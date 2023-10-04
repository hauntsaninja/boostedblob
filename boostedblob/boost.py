from __future__ import annotations

import asyncio
from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Collection,
    Deque,
    Generic,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
)

A = TypeVar("A")
T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)
R = TypeVar("R")


async def consume(iterable: AsyncIterable[Any]) -> None:
    async for _ in iterable:
        pass


class BoostExecutor:
    """BoostExecutor implements a concurrent.futures-like interface for running async tasks.

    The idea is: you want to run a limited number of tasks at a time. Moreover, if you have spare
    capacity, you want to redistribute it to tasks you're running. For instance, when downloading
    chunks of a file, we could use spare capacity to proactively download future chunks. That is,
    instead of the following serial code:
    ```
    for chunk in iterable_of_chunks:
        data = await download_func(chunk)
        write(data)
    ```
    You can instead do:
    ```
    async with BoostExecutor(4) as executor:
        async for data in executor.map_ordered(download_func, iterable_of_chunks):
            write(data)
    ```

    The number supplied to BoostExecutor is the number of units of concurrency you wish to maintain.
    That is, using ``BoostExecutor(1)`` would make the above example function approximately similar
    to the serial code.

    """

    def __init__(self, concurrency: int) -> None:
        assert concurrency > 0
        self.concurrency = concurrency
        # Okay, so this is a little tricky. We take away one unit of concurrency now, and give it
        # back when you iterate over a boostable. You can think of concurrency - 1 as the number of
        # units of "background" concurrency. When our "foreground" starts iterating over a
        # boostable, for instance, with an async for loop, it donates one unit of concurrency to the
        # cause for the duration of the iteration.
        # Why this rigmarole? To avoid deadlocks on reentrancy. If a boostable spawns more work on
        # the same executor and then blocks on that work, it's holding a unit of concurrency while
        # trying to acquire another one. If that happens enough times, we deadlock. For examples,
        # see test_composition_nested_(un)?ordered.
        # To solve this, we treat Boostable.__aiter__ as an indicator that we are entrant. We then
        # donate a unit of concurrency by releasing the semaphore. Conceptually, this is our
        # "foreground" or "re-entrant" unit of concurrency. We give this back when the Boostable is
        # done iterating. We could be more granular and do this on every Boostable.__anext__, but
        # that's a lot more overhead, makes iteration less predictable and can result in what feels
        # like unnecessary blocking. We could also do something complicated using contextvars, but
        # that would have to be something complicated and would have to use contextvars.
        self.semaphore = asyncio.Semaphore(concurrency - 1)
        self.boostables: Deque[Boostable[Any]] = Deque()

        self.waiter: Optional[asyncio.Future[None]] = None
        self.runner: Optional[asyncio.Task[None]] = None
        self.shutdown: bool = False

    async def __aenter__(self) -> BoostExecutor:
        self.runner = asyncio.create_task(self.run())
        return self

    async def __aexit__(
        self, exc_type: Optional[Type[BaseException]], exc_value: Any, traceback: Any
    ) -> None:
        self.shutdown = True
        if exc_type:
            # If there was an exception, let it propagate and don't block on the runner exiting.
            # Also cancel the runner.
            assert self.runner is not None
            self.runner.cancel()
            return
        self.notify_runner()
        assert self.runner is not None
        await self.runner

    def eagerise(self, iterator: AsyncIterator[T]) -> EageriseBoostable[T]:
        ret = EageriseBoostable(iterator, self)
        self.boostables.appendleft(ret)
        self.notify_runner()
        return ret

    def map_ordered(
        self, func: Callable[[A], Awaitable[T]], iterable: BoostUnderlying[A]
    ) -> OrderedMappingBoostable[A, T]:
        ret = OrderedMappingBoostable(func, iterable, self)
        self.boostables.appendleft(ret)
        self.notify_runner()
        return ret

    def map_unordered(
        self, func: Callable[[A], Awaitable[T]], iterable: BoostUnderlying[A]
    ) -> UnorderedMappingBoostable[A, T]:
        ret = UnorderedMappingBoostable(func, iterable, self)
        self.boostables.appendleft(ret)
        self.notify_runner()
        return ret

    def enumerate(self, iterable: BoostUnderlying[T]) -> EnumerateBoostable[T]:
        ret = EnumerateBoostable(iterable, self)
        self.boostables.appendleft(ret)
        self.notify_runner()
        return ret

    def filter(
        self, filter_fn: Optional[Callable[[T], bool]], iterable: BoostUnderlying[T]
    ) -> FilterBoostable[T]:
        ret = FilterBoostable(filter_fn, iterable, self)
        self.boostables.appendleft(ret)
        self.notify_runner()
        return ret

    def notify_runner(self) -> None:
        if self.waiter and not self.waiter.done():
            self.waiter.set_result(None)

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        exhausted_boostables: List[Boostable[Any]] = []
        not_ready_boostables: Deque[Boostable[Any]] = Deque()

        MIN_TIMEOUT = 0.01
        MAX_TIMEOUT = 0.1
        timeout = MIN_TIMEOUT

        if self.concurrency == 1:
            return

        while True:
            await self.semaphore.acquire()
            self.semaphore.release()

            while self.boostables:
                # We round robin the boostables until they're either all exhausted or not ready
                task = self.boostables[0].provide_boost()
                if isinstance(task, NotReady):
                    not_ready_boostables.append(self.boostables.popleft())
                    continue
                if isinstance(task, Exhausted):
                    exhausted_boostables.append(self.boostables.popleft())
                    continue
                assert isinstance(task, asyncio.Task)

                await asyncio.sleep(0)
                self.boostables.rotate(-1)
                if self.semaphore.locked():
                    break
            else:
                self.boostables = not_ready_boostables
                not_ready_boostables = Deque()

            if self.semaphore.locked():
                # If we broke out of the inner loop due to a lack of available concurrency, go to
                # the top of the outer loop and wait for concurrency
                continue

            if self.shutdown and not self.boostables:
                # If we've been told to shutdown and we have nothing more to boost, exit
                break

            self.waiter = loop.create_future()
            try:
                # If we still have boostables (because they're currently not ready), timeout in
                # case they become ready
                # Otherwise, (since we have available concurrency), wait for us to be notified in
                # case of more work
                await asyncio.wait_for(self.waiter, timeout if self.boostables else None)
                # If we were waiting for boostables to become ready, increase how long we'd wait
                # the next time. Otherwise, reset the timeout duration.
                timeout = min(MAX_TIMEOUT, timeout * 2) if self.boostables else MIN_TIMEOUT
            except asyncio.TimeoutError:
                pass
            self.waiter = None

        # It's somewhat unintuitive that we can shutdown an executor while Boostables are still
        # being iterated over. So wait for them to finish as a courtesy (but not a guarantee).
        for boostable in exhausted_boostables:
            await boostable.wait()

        # Yield, so that iterations over boostables are likely to have finished by the time we exit
        # the BoostExecutor context
        await asyncio.sleep(0)


class Exhausted:
    pass


class NotReady:
    pass


class Boostable(Generic[T_co]):
    """A Boostable is an async iterable with a twist.

    The twist is that it can make use of additional concurrency to compute the elements it iterates
    in parallel. Boostables also compose, so that additional concurrency is directed to the task
    that needs it most.

    See the docstring of MappingBoostable to see how this plays out in practice.

    You probably shouldn't use these classes directly, instead, instantiate them via the
    corresponding methods of the executor used to manage the Boostable.

    """

    def __init__(self, executor: BoostExecutor) -> None:
        self.executor = executor

    def provide_boost(self) -> Union[NotReady, Exhausted, asyncio.Task[Any]]:
        """Start an asyncio task to help speed up this boostable.

        Returns NotReady if we can't make use of a boost currently.
        Returns Exhausted if we're done. This causes the BoostExecutor to stop attempting to provide
        boosts to this Boostable.

        """
        raise NotImplementedError

    async def wait(self) -> None:
        """Wait on executor shutdown, if helpful.

        Waiting if we still have work pending makes executor shutdown more intuitive.
        Boostables don't really make any guarantees about their behaviour here, though.

        """
        pass

    def dequeue(self) -> Union[NotReady, Exhausted, T_co]:
        """Non-blockingly dequeue a result we have ready.

        Returns NotReady if it's not ready to dequeue.
        Returns Exhausted if we're definitely done.

        """
        raise NotImplementedError

    async def blocking_dequeue(self) -> T_co:
        """Dequeue a result, waiting if necessary.

        Raises StopAsyncIteration if exhausted.

        """
        raise NotImplementedError

    def __aiter__(self) -> AsyncIterator[T_co]:
        async def iterator() -> AsyncIterator[T_co]:
            try:
                self.executor.semaphore.release()
                while True:
                    yield await self.blocking_dequeue()
            except StopAsyncIteration:
                pass
            finally:
                await self.executor.semaphore.acquire()

        return iterator()


class MappingBoostable(Boostable[T], Generic[A, T]):
    """Represents a map over an underlying iterable.

    MappingBoostables map an async function over each element of some underlying iterable. To boost
    this operation, we dequeue another element from the underlying iterable and create an asyncio
    task to perform the async function call over that element.

    MappingBoostables also compose; that is, the underlying iterable can be another Boostable. In
    this case, we quickly (aka non-blockingly) check whether the underlying Boostable has an element
    ready. If so, we dequeue it, just as we would with an iterable, if not, we forward the boost we
    were provided to the underlying Boostable.

    MappingBoostables come in two flavours: ordered and unordered. Remember that Boostables are
    async iterables. An OrderedMappingBoostable will yield results corresponding to the order of the
    underlying iterable, whereas an UnorderedMappingBoostable will yield results based on what
    finishes first.

    Note that both kinds of MappingBoostables will, of course, dequeue from the underlying iterable
    in whatever order the underlying provides. This means we'll start tasks in order (but not
    necessarily finish them in order, which is kind of the point).
    """

    buffer: Collection[Awaitable[T]]

    def __init__(
        self,
        func: Callable[[A], Awaitable[T]],
        iterable: BoostUnderlying[A],
        executor: BoostExecutor,
    ) -> None:
        super().__init__(executor)

        if not isinstance(iterable, (Iterator, Boostable)):
            raise ValueError("Underlying iterable must be an Iterator or Boostable")

        async def wrapper(arg: A) -> T:
            async with self.executor.semaphore:
                return await func(arg)

        self.func = wrapper
        self.iterable = iterable

    async def wait(self) -> None:
        if self.buffer:
            await asyncio.wait(self.buffer)

    def provide_boost(self) -> Union[NotReady, Exhausted, asyncio.Task[Any]]:
        if not self.executor.shutdown and len(self.buffer) > 2 * self.executor.concurrency:
            # if we have a lot of stuff ready to go, apply backpressure by not accepting the boost
            # the main effect of this is to reduce memory usage
            # always accept boosts if the executor is shutting down to prevent hangs on misuse
            return NotReady()

        result = dequeue_underlying(self.iterable)
        if isinstance(result, NotReady):
            if isinstance(self.iterable, Boostable):
                # the underlying boostable doesn't have anything ready for us
                # so forward the boost to the underlying boostable
                return self.iterable.provide_boost()
            return result
        if isinstance(result, Exhausted):
            return result

        return self.enqueue(result)

    def enqueue(self, arg: A) -> asyncio.Task[T]:
        """Start and return an asyncio task based on an element from the underlying."""
        raise NotImplementedError


class OrderedMappingBoostable(MappingBoostable[A, T]):
    def __init__(
        self,
        func: Callable[[A], Awaitable[T]],
        iterable: BoostUnderlying[A],
        executor: BoostExecutor,
    ) -> None:
        super().__init__(func, iterable, executor)
        self.buffer: Deque[asyncio.Task[T]] = Deque()

    def enqueue(self, arg: A) -> asyncio.Task[T]:
        task = asyncio.create_task(self.func(arg))
        self.buffer.append(task)
        return task

    def dequeue(self) -> Union[NotReady, T]:
        if not self.buffer or not self.buffer[0].done():
            return NotReady()
        task = self.buffer.popleft()
        return task.result()

    async def blocking_dequeue(self) -> T:
        while True:
            if not self.buffer:
                arg = await blocking_dequeue_underlying(self.iterable)
                self.enqueue(arg)
            ret = self.dequeue()
            if not isinstance(ret, NotReady):
                return ret
            # note that dequeues are racy, so we can't return the result of self.buffer[0] instead,
            # we just take it as a signal that something is likely ready, and loop and try to
            # dequeue
            await self.buffer[0]


class UnorderedMappingBoostable(MappingBoostable[A, T]):
    def __init__(
        self,
        func: Callable[[A], Awaitable[T]],
        iterable: BoostUnderlying[A],
        executor: BoostExecutor,
    ) -> None:
        super().__init__(func, iterable, executor)
        self.buffer: Set[asyncio.Task[T]] = set()
        self.waiter: Optional[asyncio.Future[asyncio.Task[T]]] = None

    def done_callback(self, task: asyncio.Task[T]) -> None:
        if self.waiter and not self.waiter.done():
            self.waiter.set_result(task)

    def enqueue(self, arg: A) -> asyncio.Task[T]:
        task = asyncio.create_task(self.func(arg))
        self.buffer.add(task)
        task.add_done_callback(self.done_callback)
        return task

    def dequeue(self, hint: Optional[asyncio.Task[T]] = None) -> Union[NotReady, T]:
        # hint is a task that we suspect is dequeuable, which allows us to skip the linear check
        # against all outstanding tasks in the happy case.
        if hint is not None and hint in self.buffer and hint.done():
            task = hint
        else:
            try:
                task = next(t for t in self.buffer if t.done())
            except StopIteration:
                return NotReady()
        self.buffer.remove(task)
        return task.result()

    async def blocking_dequeue(self) -> T:
        task = None
        loop = asyncio.get_running_loop()
        while True:
            if not self.buffer:
                arg = await blocking_dequeue_underlying(self.iterable)
                task = self.enqueue(arg)
            ret = self.dequeue(hint=task)
            if not isinstance(ret, NotReady):
                return ret
            # note that dequeues are racy, so the task we get from self.waiter may already have been
            # dequeued. in the happy case, however, it's probably the task we want to return, so we
            # use it as the hint for the next dequeue.
            self.waiter = loop.create_future()
            task = await self.waiter
            self.waiter = None


class FilterBoostable(Boostable[T]):
    def __init__(
        self,
        filter_fn: Optional[Callable[[T], bool]],
        inner: BoostUnderlying[T],
        executor: BoostExecutor,
    ):
        super().__init__(executor)
        self.filter_fn = filter_fn or bool
        self.inner = inner

    def provide_boost(self) -> Union[NotReady, Exhausted, asyncio.Task[Any]]:
        if isinstance(self.inner, Boostable):
            return self.inner.provide_boost()
        return Exhausted()

    def dequeue(self) -> Union[NotReady, Exhausted, T]:
        while True:
            ret = dequeue_underlying(self.inner)
            if isinstance(ret, (NotReady, Exhausted)) or self.filter_fn(ret):
                return ret

    async def blocking_dequeue(self) -> T:
        while True:
            ret = await blocking_dequeue_underlying(self.inner)
            if self.filter_fn(ret):
                return ret


class EnumerateBoostable(Boostable[Tuple[int, T]]):
    def __init__(self, inner: BoostUnderlying[T], executor: BoostExecutor):
        super().__init__(executor)
        self.inner = inner
        self.index = 0

    def provide_boost(self) -> Union[NotReady, Exhausted, asyncio.Task[Any]]:
        if isinstance(self.inner, Boostable):
            return self.inner.provide_boost()
        return Exhausted()

    def dequeue(self) -> Union[NotReady, Exhausted, Tuple[int, T]]:
        inner_ret = dequeue_underlying(self.inner)
        if isinstance(inner_ret, (NotReady, Exhausted)):
            return inner_ret
        ret = (self.index, inner_ret)
        self.index += 1
        return ret

    async def blocking_dequeue(self) -> Tuple[int, T]:
        inner_ret = await blocking_dequeue_underlying(self.inner)
        ret = (self.index, inner_ret)
        self.index += 1
        return ret


class EageriseBoostable(Boostable[T]):
    def __init__(self, iterable: AsyncIterator[T], executor: BoostExecutor) -> None:
        super().__init__(executor)
        self.iterable = iterable
        self.buffer: Deque[asyncio.Task[T]] = Deque()
        self.done: bool = False

        self.waiter_buffer: Optional[asyncio.Future[None]] = None
        self.waiter_backpressure: Optional[asyncio.Future[None]] = None

        self.buffer_task = asyncio.create_task(self.eagerly_buffer())

    def provide_boost(self) -> Union[NotReady, Exhausted, asyncio.Task[Any]]:
        # If we always returned Exhausted, that would cause MappingBoostable to in turn return
        # Exhausted, if it couldn't currently make use of a boost.
        return Exhausted() if self.done else NotReady()

    async def wait(self) -> None:
        await self.buffer_task

    def dequeue(self) -> Union[NotReady, Exhausted, T]:
        if not self.buffer:
            return Exhausted() if self.done else NotReady()

        task = self.buffer.popleft()

        if self.waiter_backpressure:
            self.waiter_backpressure.set_result(None)
            self.waiter_backpressure = None

        return task.result()

    async def blocking_dequeue(self) -> T:
        loop = asyncio.get_running_loop()
        while True:
            ret = self.dequeue()
            if isinstance(ret, Exhausted):
                raise StopAsyncIteration
            if not isinstance(ret, NotReady):
                return ret

            self.waiter_buffer = loop.create_future()
            await self.waiter_buffer

    async def eagerly_buffer(self) -> None:
        loop = asyncio.get_running_loop()
        async with self.executor.semaphore:
            # We can't use async for because we need to preserve exceptions
            it = self.iterable.__aiter__()
            while True:
                # I guess in theory, __anext__ isn't guaranteed to be a coroutine
                # https://github.com/hauntsaninja/boostedblob/pull/12
                task: asyncio.Task[T] = asyncio.create_task(it.__anext__())  # type: ignore [arg-type]
                try:
                    await task
                except StopAsyncIteration:
                    break
                except Exception:
                    pass
                self.buffer.append(task)

                if self.waiter_buffer:
                    self.waiter_buffer.set_result(None)
                    self.waiter_buffer = None

                # apply backpressure
                if not self.executor.shutdown and len(self.buffer) > 10 * self.executor.concurrency:
                    self.executor.semaphore.release()
                    self.waiter_backpressure = loop.create_future()
                    await self.waiter_backpressure
                    await self.executor.semaphore.acquire()

            self.done = True
            if self.waiter_buffer:
                self.waiter_buffer.set_result(None)
                self.waiter_buffer = None


# see docstring of Boostable
BoostUnderlying = Union[Iterator[T], Boostable[T]]


def dequeue_underlying(iterable: BoostUnderlying[T]) -> Union[NotReady, Exhausted, T]:
    """Like ``dequeue``, but abstracts over a BoostUnderlying."""
    if isinstance(iterable, Boostable):
        return iterable.dequeue()
    if isinstance(iterable, Iterator):
        try:
            return next(iterable)
        except StopIteration:
            return Exhausted()
    raise AssertionError


async def blocking_dequeue_underlying(iterable: BoostUnderlying[T]) -> T:
    """Like ``blocking_dequeue``, but abstracts over a BoostUnderlying."""
    if isinstance(iterable, Boostable):
        return await iterable.blocking_dequeue()
    if isinstance(iterable, Iterator):
        try:
            return next(iterable)
        except StopIteration as e:
            raise StopAsyncIteration from e
    raise AssertionError


async def iter_underlying(iterable: BoostUnderlying[T]) -> AsyncIterator[T]:
    """Like ``iter``, but abstracts over a BoostUnderlying."""
    if isinstance(iterable, Boostable):
        async for x in iterable:
            yield x
    elif isinstance(iterable, Iterator):
        for x in iterable:
            yield x
    else:
        raise AssertionError
