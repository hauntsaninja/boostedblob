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

    def __init__(self, concurrency: int):
        # this could be an int, if we didn't need to block mapping
        self.semaphore = asyncio.BoundedSemaphore(concurrency)
        self.boostables: Deque[Boostable[Any, Any]] = Deque()
        self.waiter: Optional[asyncio.Future[None]] = None
        self.runner: Optional[asyncio.Task[None]] = None
        self.shutdown: bool = False

    async def __aenter__(self) -> BoostExecutor:
        self.runner = asyncio.create_task(self.run())
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        self.shutdown = True
        if exc_type:
            # If there was an exception, let it propagate and don't block on the runner exiting.
            # TODO: consider doing some cancellation.
            return
        assert self.runner is not None
        await self.runner

    async def map_ordered(
        self, func: Callable[[T], Awaitable[R]], iterable: BoostUnderlying[T]
    ) -> OrderedBoostable[T, R]:
        ret = OrderedBoostable(func, iterable)
        await self.semaphore.acquire()
        self.boostables.appendleft(ret)
        self.notify_runner()
        return ret

    async def map_unordered(
        self, func: Callable[[T], Awaitable[R]], iterable: BoostUnderlying[T]
    ) -> UnorderedBoostable[T, R]:
        ret = UnorderedBoostable(func, iterable)
        await self.semaphore.acquire()
        self.boostables.appendleft(ret)
        self.notify_runner()
        return ret

    def notify_runner(self) -> None:
        if self.waiter and not self.waiter.done():
            self.waiter.set_result(None)

    async def run(self) -> None:
        loop = asyncio.get_event_loop()
        pending: Set[Awaitable[Any]] = set()
        while True:
            while not self.semaphore.locked() and self.boostables:
                # TODO: don't use up all concurrency on first boostable maybe?
                boost_task = self.boostables[0].provide_boost()
                if boost_task is None:
                    self.boostables.popleft()
                    self.semaphore.release()
                    continue
                pending.add(boost_task)
                self.boostables.rotate(-1)
                await self.semaphore.acquire()
                await asyncio.sleep(0)

            if self.shutdown and not pending:
                break

            async def wait_for_concurrency() -> None:
                nonlocal pending
                # TODO: we used to use a timeout, confirm no longer needed
                if pending:
                    # covariance complains, but it's okay. consider changing everything to Future
                    done, pending = await asyncio.wait(  # type: ignore
                        pending, return_when=asyncio.FIRST_COMPLETED
                    )
                    for _ in range(len(done)):
                        self.semaphore.release()
                else:
                    await asyncio.sleep(1)

                self.notify_runner()

            concurrency_waiter = asyncio.create_task(wait_for_concurrency())
            self.waiter = loop.create_future()
            await self.waiter
            self.waiter = None
            concurrency_waiter.cancel()


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
    yield results based on what finishes first. Both flavours of Boostable will start tasks in the
    order provided by the underlying iterable.

    """

    def __init__(self, func: Callable[[T], Awaitable[R]], iterable: BoostUnderlying[T]) -> None:
        assert isinstance(iterable, (Iterator, Boostable))
        self.func = func
        self.iterable = iterable

    def is_ready(self) -> bool:
        raise NotImplementedError

    def provide_boost(self) -> Optional[Awaitable[Any]]:
        """Start an asyncio task to help speed up this boostable.

        Returns None if we can't make use of a boost. Note this causes the BoostExecutor to stop
        attempting to provide boosts to this Boostable.

        """
        if isinstance(self.iterable, Boostable):
            try:
                arg = self.iterable.dequeue()
            except IndexError:
                # the underlying boostable doesn't have anything ready for us
                # so forward the boost to the underlying boostable
                return self.iterable.provide_boost()
        elif isinstance(self.iterable, Iterator):
            try:
                arg = next(self.iterable)
            except StopIteration:
                return None
        else:
            raise AssertionError

        return self.enqueue(arg)

    def enqueue(self, arg: T) -> Awaitable[R]:
        """Start and return an asyncio task based on an element from the underlying."""
        raise NotImplementedError

    def dequeue(self) -> R:
        """Non-blockingly dequeue a result we have ready.

        Raises IndexError if it's not ready to dequeue.

        """
        raise NotImplementedError

    def __aiter__(self) -> AsyncIterator[R]:
        return self

    async def __anext__(self) -> R:
        raise NotImplementedError


class OrderedBoostable(Boostable[T, R]):
    def __init__(self, func: Callable[[T], Awaitable[R]], iterable: BoostUnderlying[T]) -> None:
        super().__init__(func, iterable)
        self.buffer: Deque[asyncio.Task[R]] = Deque()

    def is_ready(self) -> bool:
        return bool(self.buffer) and self.buffer[0].done()

    def enqueue(self, arg: T) -> Awaitable[R]:
        task = asyncio.create_task(self.func(arg))
        self.buffer.append(task)
        return task

    def dequeue(self) -> R:
        if not self.buffer:
            raise IndexError
        if not self.buffer[0].done():
            raise IndexError
        task = self.buffer.popleft()
        return task.result()

    async def __anext__(self) -> R:
        if not self.buffer:
            arg = await next_underlying(self.iterable)
            return await self.func(arg)
        return await self.buffer.popleft()


class UnorderedBoostable(Boostable[T, R]):
    def __init__(self, func: Callable[[T], Awaitable[R]], iterable: BoostUnderlying[T]) -> None:
        super().__init__(func, iterable)
        self.done_tasks: Set[asyncio.Task[R]] = set()
        self.pending_tasks: Set[asyncio.Task[R]] = set()

    def is_ready(self) -> bool:
        return bool(self.done_tasks) or any(t.done() for t in self.pending_tasks)

    def enqueue(self, arg: T) -> Awaitable[R]:
        task = asyncio.create_task(self.func(arg))
        self.pending_tasks.add(task)
        return task

    def dequeue(self) -> R:
        if self.done_tasks:
            return self.done_tasks.pop().result()

        try:
            task = next(t for t in self.pending_tasks if t.done())
            return task.result()
        except StopIteration:
            raise IndexError

    async def __anext__(self) -> R:
        if self.done_tasks:
            return self.done_tasks.pop().result()

        if not self.pending_tasks:
            arg = await next_underlying(self.iterable)
            return await self.func(arg)

        # make sure we don't lose any tasks that get enqueued while we're waiting
        # and that concurrent __anext__ calls don't wait on the same set of tasks
        pending_copy = self.pending_tasks.copy()
        self.pending_tasks.clear()

        done, pending = await asyncio.wait(pending_copy, return_when=asyncio.FIRST_COMPLETED)

        # covariance complains, but it's okay
        self.done_tasks |= done  # type: ignore
        self.pending_tasks |= pending  # type: ignore
        return self.done_tasks.pop().result()


# see docstring of Boostable
BoostUnderlying = Union[Iterator[T], "Boostable[Any, T]"]


async def next_underlying(iterable: BoostUnderlying[T]) -> T:
    """Like ``next``, but abstracts over a BoostUnderlying."""
    if isinstance(iterable, Boostable):
        return await iterable.__anext__()
    if isinstance(iterable, Iterator):
        try:
            return next(iterable)
        except StopIteration:
            raise StopAsyncIteration
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
