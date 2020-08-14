from __future__ import annotations

import asyncio
from typing import (
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
AnyIterator = Union[AsyncIterator[T], Iterator[T]]


def async_iteratorise(it: AnyIterator[T]) -> AsyncIterator[T]:
    if isinstance(it, AsyncIterator):
        return it

    async def ret() -> AsyncIterator[T]:
        assert isinstance(it, Iterator)
        for x in it:
            yield x

    return ret()


class BoostManager:
    def __init__(self, tasks: AnyIterator[Callable[[BoostManager], Awaitable]], concurrency: int):
        self.tasks = async_iteratorise(tasks)
        self.concurrency = concurrency
        self.applicants: Deque[Boostable] = Deque()

    def apply_for_boost(self, applicant: Boostable) -> None:
        self.applicants.append(applicant)

    async def run(self) -> None:
        pending: Set[Awaitable] = set()
        next_task = asyncio.create_task(self.tasks.__anext__())
        while True:
            try:
                while self.concurrency:
                    task = await next_task
                    # TODO: consider kwarg, type with callback protocol
                    pending.add(task(self))
                    next_task = asyncio.create_task(self.tasks.__anext__())
                    self.concurrency -= 1
            except StopAsyncIteration:
                pass

            while self.concurrency and self.applicants:
                boost_task = self.applicants[0].provide_boost()
                if boost_task is None:
                    self.applicants.popleft()
                    continue
                pending.add(boost_task)
                self.applicants.rotate(-1)
                self.concurrency -= 1

            if not pending:
                break

            # if we have available concurrency, wait with a timeout, so that we can potentially
            # enqueue more tasks
            # TODO: consider backing off here
            timeout = 0.2 if self.concurrency else None
            # covariance complains, but it's okay
            done, pending = await asyncio.wait(  # type: ignore
                pending, return_when=asyncio.FIRST_COMPLETED, timeout=timeout
            )
            await asyncio.gather(*done)  # surface exceptions
            self.concurrency += len(done)


class Boostable(Generic[T]):
    def __init__(self, subtasks: Iterator[Awaitable[T]], manager: Optional[BoostManager] = None):
        assert isinstance(subtasks, Iterator)
        self.subtasks = subtasks
        self.manager = manager

    def provide_boost(self) -> Optional[Awaitable[T]]:
        raise NotImplementedError

    def __aiter__(self) -> AsyncIterator[T]:
        if self.manager is not None:
            self.manager.apply_for_boost(self)
        return self

    async def __anext__(self) -> T:
        raise NotImplementedError


class OrderedBoostable(Boostable[T]):
    def __init__(self, subtasks: Iterator[Awaitable[T]], manager: Optional[BoostManager] = None):
        super().__init__(subtasks, manager)
        self.buffer: Deque[Awaitable[T]] = Deque()

    def provide_boost(self) -> Optional[Awaitable[T]]:
        try:
            task = asyncio.create_task(next(self.subtasks))
            self.buffer.append(task)
            return task
        except StopIteration:
            return None

    async def __anext__(self) -> T:
        if not self.buffer:
            try:
                return await next(self.subtasks)
            except StopIteration:
                raise StopAsyncIteration
        return await self.buffer.popleft()


class UnorderedBoostable(Boostable[T]):
    def __init__(self, subtasks: Iterator[Awaitable[T]], manager: Optional[BoostManager] = None):
        super().__init__(subtasks, manager)
        self.buffer: Set[Awaitable[T]] = set()

    def provide_boost(self) -> Optional[Awaitable[T]]:
        try:
            task = asyncio.create_task(next(self.subtasks))
            self.buffer.add(task)
            return task
        except StopIteration:
            return None

    async def __anext__(self) -> T:
        if not self.buffer:
            try:
                return await next(self.subtasks)
            except StopIteration:
                raise StopAsyncIteration

        # make sure we don't lose any subtasks that get boosted while we're waiting
        buffer_copy = self.buffer.copy()
        self.buffer.clear()
        done, pending = await asyncio.wait(buffer_copy, return_when=asyncio.FIRST_COMPLETED)
        ret = done.pop()
        # covariance complains, but it's okay
        self.buffer |= done | pending  # type: ignore
        return await ret
