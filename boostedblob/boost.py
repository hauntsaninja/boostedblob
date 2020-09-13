from __future__ import annotations

import asyncio
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Deque,
    Generic,
    Iterator,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
)

T = TypeVar("T")
R = TypeVar("R")


async def consume(iterable: AsyncIterator[Any]) -> None:
    async for _ in iterable:
        pass


class BoostExecutor:
    """BoostExecutor implements a concurrent.futures-like interface for running async tasks.

    The idea is: you want to run not more than some number of tasks at a time. However, if you have
    spare capacity, you want to redistribute it to tasks you're running. For instance, when
    downloading chunks of a file, we could use spare capacity to proactively download future chunks.

    Example usage:
    ```
    async with BoostExecutor() as executor:
        async for chunk in executor.map_ordered(func, iterable):
            write(chunk)
    ```

    """

    def __init__(self, concurrency: int) -> None:
        assert concurrency > 0
        self.semaphore = asyncio.Semaphore(concurrency)
        self.boostables: Deque[Boostable[Any, Any]] = Deque()

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

    def map_ordered(
        self, func: Callable[[T], Awaitable[R]], iterable: BoostUnderlying[T]
    ) -> OrderedBoostable[T, R]:
        ret = OrderedBoostable(func, iterable, self.semaphore)
        self.boostables.appendleft(ret)
        self.notify_runner()
        return ret

    def map_unordered(
        self, func: Callable[[T], Awaitable[R]], iterable: BoostUnderlying[T]
    ) -> UnorderedBoostable[T, R]:
        ret = UnorderedBoostable(func, iterable, self.semaphore)
        self.boostables.appendleft(ret)
        self.notify_runner()
        return ret

    def notify_runner(self) -> None:
        if self.waiter and not self.waiter.done():
            self.waiter.set_result(None)

    async def run(self) -> None:
        loop = asyncio.get_event_loop()
        pending: Set[Awaitable[Any]] = set()

        MIN_TIMEOUT = 0.01
        MAX_TIMEOUT = 0.1
        timeout = MIN_TIMEOUT
        while True:
            await self.semaphore.acquire()
            self.semaphore.release()

            not_ready_boostables: Deque[Boostable[Any, Any]] = Deque()
            while self.boostables:
                # We round robin the boostables until they're either all exhausted or not ready
                try:
                    boost_task = self.boostables[0].provide_boost()
                except NotReady:
                    not_ready_boostables.append(self.boostables.popleft())
                    continue
                except Exhausted:
                    self.boostables.popleft()
                    continue
                pending.add(boost_task)
                await asyncio.sleep(0)
                self.boostables.rotate(-1)
                if self.semaphore.locked():
                    break
            else:
                self.boostables = not_ready_boostables

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

        if pending:
            await asyncio.wait(pending)


class Exhausted(Exception):
    pass


class NotReady(Exception):
    pass


class Boostable(Generic[T, R]):
    """A Boostable represents some operation that could make use of additional concurrency.

    You probably shouldn't use these classes directly, instead, instantiate them via the ``map_*``
    methods of the executor that will provide these Boostables with additional concurrency.

    We represent boostable operations as an async function that is called on each element of some
    underlying iterable. To boost this operation, we dequeue another element from the underlying
    iterable and create an asyncio task to perform the function call over that element.

    Boostables also compose; that is, the underlying iterable can be another Boostable. In this
    case, we quickly (aka non-blockingly) check whether the underlying Boostable has an element
    ready. If so, we dequeue it, just as we would with an iterable, if not, we forward the boost
    we received to the underlying Boostable.

    You can treat Boostables as async iterators. An OrderedBoostable will yield results
    corresponding to the order of the underlying iterable, whereas an UnorderedBoostable will
    yield results based on what finishes first.

    Note that both kinds of Boostables will, of course, dequeue from the underlying iterable in
    whatever order the underlying provides. This means we'll start tasks in order (but not
    necessarily finish them in order, which is kind of the point). In some places, we use this
    fact to increment a shared index; it's important we do ordered shared memory things like that
    before any awaits.

    """

    def __init__(
        self,
        func: Callable[[T], Awaitable[R]],
        iterable: BoostUnderlying[T],
        semaphore: asyncio.Semaphore,
    ) -> None:
        if not isinstance(iterable, (Iterator, Boostable, EagerAsyncIterator)):
            raise ValueError(
                "Underlying iterable must be an Iterator, Boostable or EagerAsyncIterator"
            )

        async def wrapper(arg: T) -> R:
            async with semaphore:
                return await func(arg)

        self.func = wrapper
        self.iterable = iterable

    def provide_boost(self) -> asyncio.Task[Any]:
        """Start an asyncio task to help speed up this boostable.

        Raises NotReady if we can't make use of a boost currently.
        Raises Exhausted if we're done. This causes the BoostExecutor to stop attempting to
        provide boosts to this Boostable.

        """
        if isinstance(self.iterable, Boostable):
            try:
                arg = self.iterable.dequeue()
            except NotReady:
                # the underlying boostable doesn't have anything ready for us
                # so forward the boost to the underlying boostable
                return self.iterable.provide_boost()
        elif isinstance(self.iterable, EagerAsyncIterator):
            arg = self.iterable.dequeue()
        elif isinstance(self.iterable, Iterator):
            try:
                arg = next(self.iterable)
            except StopIteration:
                raise Exhausted
        else:
            raise AssertionError

        return self.enqueue(arg)

    def enqueue(self, arg: T) -> asyncio.Task[R]:
        """Start and return an asyncio task based on an element from the underlying."""
        raise NotImplementedError

    def dequeue(self) -> R:
        """Non-blockingly dequeue a result we have ready.

        Raises NotReady if it's not ready to dequeue.

        """
        raise NotImplementedError

    def __aiter__(self) -> AsyncIterator[R]:
        return self

    async def __anext__(self) -> R:
        raise NotImplementedError


class OrderedBoostable(Boostable[T, R]):
    def __init__(
        self,
        func: Callable[[T], Awaitable[R]],
        iterable: BoostUnderlying[T],
        semaphore: asyncio.Semaphore,
    ) -> None:
        super().__init__(func, iterable, semaphore)
        self.buffer: Deque[asyncio.Task[R]] = Deque()

    def enqueue(self, arg: T) -> asyncio.Task[R]:
        task = asyncio.create_task(self.func(arg))
        self.buffer.append(task)
        return task

    def dequeue(self) -> R:
        if not self.buffer:
            raise NotReady
        if not self.buffer[0].done():
            raise NotReady
        task = self.buffer.popleft()
        return task.result()

    async def __anext__(self) -> R:
        if not self.buffer:
            arg = await next_underlying(self.iterable)
            return await self.func(arg)
        return await self.buffer.popleft()


class UnorderedBoostable(Boostable[T, R]):
    def __init__(
        self,
        func: Callable[[T], Awaitable[R]],
        iterable: BoostUnderlying[T],
        semaphore: asyncio.Semaphore,
    ) -> None:
        super().__init__(func, iterable, semaphore)
        self.buffer: Set[asyncio.Task[R]] = set()
        self.waiter: Optional[asyncio.Future[asyncio.Task[R]]] = None

    def done_callback(self, task: asyncio.Task[R]) -> None:
        # this can happen if we've already dequeued. cancelling the callback doesn't seem to work.
        # TODO: can this cause __anext__ to block?
        if task not in self.buffer:
            return
        if self.waiter and not self.waiter.done():
            self.waiter.set_result(task)

    def enqueue(self, arg: T) -> asyncio.Task[R]:
        task = asyncio.create_task(self.func(arg))
        self.buffer.add(task)
        task.add_done_callback(self.done_callback)
        return task

    def dequeue(self) -> R:
        try:
            task = next(t for t in self.buffer if t.done())
        except StopIteration:
            raise NotReady
        self.buffer.remove(task)
        return task.result()

    async def __anext__(self) -> R:
        if not self.buffer:
            arg = await next_underlying(self.iterable)
            self.enqueue(arg)

        try:
            return self.dequeue()
        except NotReady:
            pass

        loop = asyncio.get_event_loop()
        self.waiter = loop.create_future()
        task = await self.waiter
        self.waiter = None

        assert task.done()
        self.buffer.remove(task)
        return task.result()


class EagerAsyncIterator(Generic[T]):
    """Like an AsyncIterator, but eager.

    AsyncIterators will only start computing their next element when awaited on.
    This allows an AsyncIterator to serve as the underlying iterable for a Boostable.
    Note a) this takes away the natural backpressure, b) instantiating one of these will use
    one unit of concurrency until it's exhausted which is not accounted for by a BoostExecutor.

    """

    def __init__(self, iterable: AsyncIterator[T]):
        async def eagerify(it: AsyncIterator[T]) -> Tuple[T, asyncio.Task[Any]]:
            val = await it.__anext__()
            next_task = asyncio.create_task(eagerify(it))
            return val, next_task

        self.next_task = asyncio.create_task(eagerify(iterable))

    def dequeue(self) -> T:
        if not self.next_task.done():
            raise NotReady
        try:
            ret, self.next_task = self.next_task.result()
            return ret
        except StopAsyncIteration:
            raise Exhausted

    def __aiter__(self) -> AsyncIterator[T]:
        return self

    async def __anext__(self) -> T:
        ret, self.next_task = await self.next_task
        return ret


# see docstring of Boostable
BoostUnderlying = Union[Iterator[T], Boostable[Any, T], EagerAsyncIterator[T]]


async def next_underlying(iterable: BoostUnderlying[T]) -> T:
    """Like ``next``, but abstracts over a BoostUnderlying."""
    if isinstance(iterable, (Boostable, EagerAsyncIterator)):
        return await iterable.__anext__()
    if isinstance(iterable, Iterator):
        try:
            return next(iterable)
        except StopIteration:
            raise StopAsyncIteration
    raise AssertionError


async def iter_underlying(iterable: BoostUnderlying[T]) -> AsyncIterator[T]:
    """Like ``iter``, but abstracts over a BoostUnderlying."""
    if isinstance(iterable, (Boostable, EagerAsyncIterator)):
        async for x in iterable:
            yield x
    elif isinstance(iterable, Iterator):
        for x in iterable:
            yield x
    else:
        raise AssertionError
