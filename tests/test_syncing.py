import asyncio

import pytest

import boostedblob as bbb

from . import helpers


@pytest.mark.asyncio
@bbb.ensure_session
async def test_sync(any_dir, other_any_dir):
    await asyncio.wait(
        [
            helpers.unsafe_create_file(any_dir / "f1", b"samesize"),
            helpers.unsafe_create_file(any_dir / "f2"),
            helpers.unsafe_create_file(any_dir / "f3"),
            helpers.unsafe_create_file(any_dir / "alpha" / "f4"),
            helpers.unsafe_create_file(any_dir / "alpha" / "f5"),
            helpers.unsafe_create_file(any_dir / "alpha" / "beta" / "f6"),
            helpers.unsafe_create_file(any_dir / "alpha" / "beta" / "f7"),
            helpers.unsafe_create_file(any_dir / "alpha" / "beta" / "gamma" / "f8"),
            helpers.unsafe_create_file(any_dir / "delta" / "f9", b"samesize"),
            helpers.unsafe_create_file(any_dir / "delta" / "epsilon" / "f10"),
        ]
    )

    async def _listtree(d, base):
        return sorted([p.relative_to(base) async for p in bbb.listtree(d)])

    async with bbb.BoostExecutor(100) as e:
        # sleep since if we run sync too soon, we run into limits of mtime accuracy and end up
        # syncing more than what we need...
        await asyncio.sleep(1)
        await bbb.boost.consume(bbb.sync(any_dir, other_any_dir, e))
        assert await _listtree(any_dir, any_dir) == await _listtree(other_any_dir, other_any_dir)

        await asyncio.wait(
            (
                asyncio.create_task(bbb.remove(any_dir / "f2")),
                helpers.unsafe_create_file(any_dir / "f1", b"sizesame"),
                helpers.unsafe_create_file(any_dir / "delta" / "f9", b"differentsize"),
            )
        )

        actions = sorted(
            await bbb.syncing.sync_action_iterator(any_dir, other_any_dir), key=lambda x: x.relpath
        )
        assert actions == [
            bbb.syncing.CopyAction("delta/f9", 13),
            bbb.syncing.CopyAction("f1", 8),
            bbb.syncing.DeleteAction("f2"),
        ]
        actions = sorted(
            await bbb.syncing.sync_action_iterator(any_dir, other_any_dir, exclude="delta"),
            key=lambda x: x.relpath,
        )
        assert actions == [bbb.syncing.CopyAction("f1", 8), bbb.syncing.DeleteAction("f2")]
        actions = sorted(
            await bbb.syncing.sync_action_iterator(any_dir, other_any_dir, exclude="^f"),
            key=lambda x: x.relpath,
        )
        assert actions == [bbb.syncing.CopyAction("delta/f9", 13)]

        await bbb.boost.consume(bbb.sync(any_dir, other_any_dir, e, delete=True))
        assert await _listtree(any_dir, any_dir) == await _listtree(other_any_dir, other_any_dir)


@pytest.mark.asyncio
@bbb.ensure_session
async def test_sync_single_concurrency():
    with helpers.tmp_azure_dir() as dir1:
        with helpers.tmp_azure_dir() as dir2:
            await asyncio.wait([helpers.unsafe_create_file(dir1 / f"f{i}") for i in range(10)])

        async def _listtree(d, base):
            return sorted([p.relative_to(base) async for p in bbb.listtree(d)])

        async with bbb.BoostExecutor(1) as e:
            # sleep since if we run sync too soon, we run into limits of mtime accuracy and end up
            # syncing more than what we need...
            await asyncio.sleep(1)
            await bbb.boost.consume(bbb.sync(dir1, dir2, e))
            assert await _listtree(dir1, dir1) == await _listtree(dir2, dir2)

            await asyncio.wait(
                (
                    asyncio.create_task(bbb.remove(dir1 / "f2")),
                    helpers.unsafe_create_file(dir1 / "f1", b"sizesame"),
                    helpers.unsafe_create_file(dir1 / "delta" / "f9", b"differentsize"),
                )
            )
            assert len([x async for x in bbb.sync(dir1, dir2, e, delete=True)]) == 3
            assert await _listtree(dir1, dir1) == await _listtree(dir2, dir2)
