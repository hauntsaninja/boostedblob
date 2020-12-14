from __future__ import annotations

import asyncio
import random
import sys
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, List

import pytest

import boostedblob as bbb


async def pause():
    EPSILON = 0.001
    await asyncio.sleep(EPSILON)


def get_futures_fn(futures: Dict[int, asyncio.Future[int]]) -> Callable[[int], Awaitable[int]]:
    """Return a function that we can control the completion of each invocation of."""
    loop = asyncio.get_event_loop()

    async def fn(i: int) -> int:
        futures[i] = loop.create_future()
        await futures[i]
        del futures[i]
        return i

    return fn


async def identity(x: int) -> int:
    return x


async def collect(it: AsyncIterator[int], results: List[int]) -> None:
    """Collect the results of ``it`` in the ``results`` list."""
    async for i in it:
        results.append(i)


# ==============================
# map_ordered
# ==============================


@pytest.mark.asyncio
async def test_map_ordered_single():
    futures = {}
    async with bbb.BoostExecutor(1) as e:
        assert e.semaphore._value == 1  # type: ignore
        it = e.map_ordered(get_futures_fn(futures), iter([0, 1]))
        assert not futures
        await pause()
        assert e.semaphore._value == 0  # type: ignore
        assert set(futures) == {0}

        next_task = asyncio.create_task(it.__anext__())
        await pause()
        assert set(futures) == {0}

        assert not next_task.done()
        futures[0].set_result(None)
        assert not next_task.done()
        await pause()
        assert set(futures) == {1}
        assert next_task.done()
        assert (await next_task) == 0

        next_task = asyncio.create_task(it.__anext__())
        await pause()
        assert not next_task.done()
        futures[1].set_result(None)
        assert (await next_task) == 1
        assert not futures


@pytest.mark.asyncio
async def test_map_ordered():
    futures = {}
    results = []
    async with bbb.BoostExecutor(2) as e:
        assert e.semaphore._value == 2  # type: ignore
        it = e.map_ordered(get_futures_fn(futures), iter(range(4)))
        asyncio.create_task(collect(it, results))
        await pause()
        assert e.semaphore._value == 0  # type: ignore
        assert set(futures) == {0, 1}

        futures[1].set_result(None)
        await pause()
        assert results == []

        futures[0].set_result(None)
        await pause()
        assert results == [0, 1]
        assert set(futures) == {2, 3}

        futures[2].set_result(None)
        await pause()
        assert results == [0, 1, 2]

        futures[3].set_result(None)
        await pause()
        assert results == [0, 1, 2, 3]


@pytest.mark.asyncio
async def test_map_ordered_identity():
    N = 20
    results = []
    async with bbb.BoostExecutor(N // 2) as e:
        it = e.map_ordered(identity, iter(range(N)))
        await collect(it, results)
    assert results == list(range(N))

    results = []
    async with bbb.BoostExecutor(N // 2) as e:
        it = e.map_ordered(identity, iter(range(N)))
        asyncio.create_task(collect(it, results))
    assert results == list(range(N))


@pytest.mark.asyncio
async def test_map_ordered_many_reversed():
    N = 500
    futures = {}
    results = []
    async with bbb.BoostExecutor(N * 2) as e:
        it = e.map_ordered(get_futures_fn(futures), iter(range(N)))
        asyncio.create_task(collect(it, results))
        while not N - 1 in futures:
            await pause()  # take a couple dozen pauses to get everything scheduled
        for i in reversed(range(N)):
            futures[i].set_result(None)
            await pause()
        assert results == list(range(N))


@pytest.mark.asyncio
async def test_map_ordered_many_low_concurrency():
    N = 500
    futures = {}
    results = []
    async with bbb.BoostExecutor(10) as e:
        it = e.map_ordered(get_futures_fn(futures), iter(range(N)))
        asyncio.create_task(collect(it, results))
        await pause()
        for i in range(1, N):
            futures[i].set_result(None)
            await pause()
        assert results == []
        futures[0].set_result(None)
        await pause()
        assert results == list(range(N))


@pytest.mark.asyncio
async def test_map_ordered_many_random():
    N = 500
    futures = {}
    results = []
    async with bbb.BoostExecutor(N * 2) as e:
        it = e.map_ordered(get_futures_fn(futures), iter(range(N)))
        task = asyncio.create_task(collect(it, results))
        while not N - 1 in futures:
            await pause()  # take a couple dozen pauses to get everything scheduled
        shuffled = list(reversed(range(N)))
        random.shuffle(shuffled)
        for i in shuffled:
            futures[i].set_result(None)
            if random.random() < 0.3:
                await pause()
        await task
        assert results == list(range(N))


# ==============================
# map_unordered
# ==============================


@pytest.mark.asyncio
async def test_map_unordered():
    futures = {}
    results = []
    async with bbb.BoostExecutor(3) as e:
        assert e.semaphore._value == 3  # type: ignore
        it = e.map_unordered(get_futures_fn(futures), iter(range(5)))
        asyncio.create_task(collect(it, results))
        await pause()
        assert e.semaphore._value == 0  # type: ignore
        assert set(futures) == {0, 1, 2}

        futures[1].set_result(None)
        await pause()
        assert results == [1]
        assert set(futures) == {0, 2, 3}

        futures[0].set_result(None)
        await pause()
        assert results == [1, 0]
        assert set(futures) == {2, 3, 4}

        futures[2].set_result(None)
        await pause()
        assert results == [1, 0, 2]

        futures[4].set_result(None)
        futures[3].set_result(None)
        await pause()
        assert results == [1, 0, 2, 4, 3]


@pytest.mark.asyncio
async def test_map_unordered_identity():
    N = 20
    results = []
    async with bbb.BoostExecutor(N // 2) as e:
        it = e.map_unordered(identity, iter(range(N)))
        await collect(it, results)
    results.sort()
    assert results == list(range(N))

    results = []
    async with bbb.BoostExecutor(N // 2) as e:
        it = e.map_unordered(identity, iter(range(N)))
        asyncio.create_task(collect(it, results))
    results.sort()
    assert results == list(range(N))


@pytest.mark.asyncio
async def test_map_unordered_random_sleep():
    async def random_sleep(i):
        await asyncio.sleep(random.random() * 0.3)
        return i

    N = 20
    results = []
    async with bbb.BoostExecutor(N // 2) as e:
        it = e.map_unordered(random_sleep, iter(range(N)))
        await collect(it, results)
    results.sort()
    assert results == list(range(N))


@pytest.mark.asyncio
async def test_map_unordered_many_reversed():
    N = 500
    futures = {}
    results = []
    async with bbb.BoostExecutor(N * 2) as e:
        it = e.map_unordered(get_futures_fn(futures), iter(range(N)))
        asyncio.create_task(collect(it, results))
        while not N - 1 in futures:
            await pause()  # take a couple dozen pauses to get everything scheduled
        for i in reversed(range(N)):
            futures[i].set_result(None)
            await pause()
        assert results == list(reversed(range(N)))


@pytest.mark.asyncio
async def test_map_unordered_many_low_concurrency():
    N = 500
    futures = {}
    results = []
    async with bbb.BoostExecutor(10) as e:
        it = e.map_unordered(get_futures_fn(futures), iter(range(N)))
        asyncio.create_task(collect(it, results))
        await pause()
        for i in range(1, N):
            futures[i].set_result(None)
            await pause()
            assert len(results) == i
        futures[0].set_result(None)
        await pause()
        assert results == list(range(1, N)) + [0]


@pytest.mark.asyncio
async def test_map_unordered_many_random():
    N = 500
    futures = {}
    results = []
    async with bbb.BoostExecutor(N * 2) as e:
        it = e.map_unordered(get_futures_fn(futures), iter(range(N)))
        task = asyncio.create_task(collect(it, results))
        while not N - 1 in futures:
            await pause()  # take a couple dozen pauses to get everything scheduled
        shuffled = list(reversed(range(N)))
        random.shuffle(shuffled)
        for i in shuffled:
            futures[i].set_result(None)
            if random.random() < 0.3:
                await pause()
        await task
        assert sorted(results) == list(range(N))


# ==============================
# eager async iterator
# ==============================


@pytest.mark.asyncio
async def test_eager_async_iterator():
    N = 10
    results = []

    async def iterator() -> AsyncIterator[int]:
        for i in range(N):
            results.append(i)
            yield i

    eager_it = bbb.boost.EagerAsyncIterator(iterator())
    assert results == []
    await pause()
    assert results == list(range(N))
    assert [i async for i in eager_it] == list(range(N))

    results.clear()
    lazy_it = iterator()
    assert results == []
    await pause()
    assert results == []
    await lazy_it.__anext__()
    assert results == [0]


@pytest.mark.asyncio
async def test_map_eager_async_iterator():
    N = 30

    async def iterator() -> AsyncIterator[int]:
        for i in range(N):
            yield i

    loop = asyncio.get_event_loop()
    future = loop.create_future()
    started = []

    async def identity_wait(x: int) -> int:
        started.append(x)
        if not future.done():
            await future
        return x

    results = []
    async with bbb.BoostExecutor(N // 3) as e:
        it = e.map_ordered(identity_wait, bbb.boost.EagerAsyncIterator(iterator()))
        asyncio.create_task(collect(it, results))
        assert started == []
        await pause()
        assert started == [0]
        # BoostExecutor currently sleeps for a minimum of 0.01 seconds if the underlying async
        # iterator is not ready
        await asyncio.sleep(0.02)
        assert started == list(range(N // 3))
        future.set_result(None)
        await asyncio.sleep(0.02)
        assert started == list(range(N))
    assert results == list(range(N))


# ==============================
# composition
# ==============================


@pytest.mark.asyncio
async def test_composition_ordered_ordered():
    N = 500
    inner_futures = {}
    outer_futures = {}
    results = []
    async with bbb.BoostExecutor(N // 5) as e:
        inner_it = e.map_ordered(get_futures_fn(inner_futures), iter(range(N)))
        outer_it = e.map_ordered(get_futures_fn(outer_futures), inner_it)
        asyncio.create_task(collect(outer_it, results))
        await pause()

        while outer_futures or inner_futures:
            futures = random.choice([fs for fs in (outer_futures, inner_futures) if fs])
            futures[next(iter(futures))].set_result(None)
            await pause()
        assert results == list(range(N))


@pytest.mark.asyncio
async def test_composition_ordered_unordered():
    N = 500
    inner_futures = {}
    outer_futures = {}
    results = []
    async with bbb.BoostExecutor(N * 2) as e:
        inner_it = e.map_unordered(get_futures_fn(inner_futures), iter(range(N)))
        outer_it = e.map_ordered(get_futures_fn(outer_futures), inner_it)
        asyncio.create_task(collect(outer_it, results))
        while not N - 1 in inner_futures:
            await pause()  # take a couple dozen pauses to get everything scheduled
        for i in reversed(range(N)):
            if outer_futures:
                assert set(outer_futures) == {i + 1}
            inner_futures[i].set_result(None)
            await pause()
            assert set(outer_futures) == {i}
            outer_futures[i].set_result(None)
        await pause()
        assert results == list(reversed(range(N)))


@pytest.mark.asyncio
async def test_composition_unordered_unordered():
    N = 1000
    inner_futures = {}
    outer_futures = {}
    results = []
    async with bbb.BoostExecutor(N * 2) as e:
        inner_it = e.map_unordered(get_futures_fn(inner_futures), iter(range(N)))
        outer_it = e.map_unordered(get_futures_fn(outer_futures), inner_it)
        asyncio.create_task(collect(outer_it, results))
        await pause()

        while outer_futures or inner_futures:
            futures = random.choice([fs for fs in (outer_futures, inner_futures) if fs])
            futures[next(iter(futures))].set_result(None)
            await pause()
        assert sorted(results) == list(range(N))


@pytest.mark.asyncio
async def test_composition_nested_ordered():
    N = 10
    results = []
    async with bbb.BoostExecutor(3) as e:

        async def work_spawner(n):
            await pause()
            return [x async for x in e.map_ordered(identity, iter(range(n)))]

        it = e.map_ordered(work_spawner, iter(range(N)))
        asyncio.create_task(collect(it, results))
    assert list(map(len, results)) == list(range(10))


@pytest.mark.asyncio
async def test_composition_nested_unordered():
    N = 10
    results = []
    async with bbb.BoostExecutor(3) as e:

        async def work_spawner(n):
            await pause()
            return [x async for x in e.map_unordered(identity, iter(range(n)))]

        it = e.map_unordered(work_spawner, iter(range(N)))
        asyncio.create_task(collect(it, results))
    assert sorted(map(len, results)) == list(range(10))


# ==============================
# miscellaneous
# ==============================


def get_coro(t: asyncio.Task[Any]) -> Any:
    if sys.version_info >= (3, 8):
        return t.get_coro()
    return t._coro


@pytest.mark.asyncio
async def test_boost_executor_shutdown():
    async with bbb.BoostExecutor(4) as e:
        e.map_ordered(asyncio.sleep, (random.random() * 0.1 for _ in range(10)))
    assert set(get_coro(t).__name__ for t in asyncio.all_tasks()) == {
        "test_boost_executor_shutdown"
    }

    async with bbb.BoostExecutor(4) as e:
        e.map_unordered(asyncio.sleep, (random.random() * 0.1 for _ in range(10)))
    assert set(get_coro(t).__name__ for t in asyncio.all_tasks()) == {
        "test_boost_executor_shutdown"
    }


@pytest.mark.asyncio
async def test_boost_executor_exception():
    with pytest.raises(ValueError):
        async with bbb.BoostExecutor(10):
            assert set(get_coro(t).__name__ for t in asyncio.all_tasks()) == {
                "test_boost_executor_exception",
                "run",
            }
            assert len(asyncio.all_tasks()) > 1
            raise ValueError

    await pause()
    assert set(get_coro(t).__name__ for t in asyncio.all_tasks()) == {
        "test_boost_executor_exception"
    }


@pytest.mark.asyncio
async def test_map_multiple():
    N = 20
    r1 = []
    r2 = []
    r3 = []
    async with bbb.BoostExecutor(N // 2) as e:
        it1 = e.map_unordered(identity, iter(range(N)))
        t1 = asyncio.create_task(collect(it1, r1))

        it2 = e.map_ordered(identity, iter(range(N)))
        t2 = asyncio.create_task(collect(it2, r2))

        it3 = e.map_ordered(identity, iter(range(N)))
        t3 = asyncio.create_task(collect(it3, r3))

        await asyncio.gather(t1, t2, t3)
        r1.sort()
        assert r1 == r2 == r3
