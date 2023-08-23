import asyncio

import blobfile
import pytest

import boostedblob as bbb

from . import helpers


@pytest.mark.asyncio
@bbb.ensure_session
async def test_copy(any_dir, other_any_dir):
    MIN_CHUNK_SIZE = 256 * 1024

    with open("/dev/random", "rb") as f:
        contents_medium = f.read(16 * MIN_CHUNK_SIZE)
    helpers.create_file(any_dir / "original_medium", contents_medium)

    contents_known_small = b"abcdefgh"
    helpers.create_file(any_dir / "original_small", contents_known_small)

    async with bbb.BoostExecutor(100) as e:
        with bbb.globals.configure(chunk_size=MIN_CHUNK_SIZE):
            await bbb.copyfile(any_dir / "original_medium", other_any_dir / "copied_medium", e)
            with blobfile.BlobFile(str(other_any_dir / "copied_medium"), "rb") as f:
                assert f.read() == contents_medium

        await bbb.copyfile(
            any_dir / "original_small",
            other_any_dir / "copied_small",
            e,
            size=len(contents_known_small),
        )
        with blobfile.BlobFile(str(other_any_dir / "copied_small"), "rb") as f:
            assert f.read() == contents_known_small


@pytest.mark.asyncio
@bbb.ensure_session
async def test_copytree(any_dir, other_any_dir):
    await asyncio.wait(
        [
            helpers.unsafe_create_file(any_dir / "f1"),
            helpers.unsafe_create_file(any_dir / "f2"),
            helpers.unsafe_create_file(any_dir / "f3"),
            helpers.unsafe_create_file(any_dir / "alpha" / "f4"),
            helpers.unsafe_create_file(any_dir / "alpha" / "f5"),
            helpers.unsafe_create_file(any_dir / "alpha" / "beta" / "f6"),
            helpers.unsafe_create_file(any_dir / "alpha" / "beta" / "f7"),
            helpers.unsafe_create_file(any_dir / "alpha" / "beta" / "gamma" / "f8"),
            helpers.unsafe_create_file(any_dir / "delta" / "f9"),
            helpers.unsafe_create_file(any_dir / "delta" / "epsilon" / "f10"),
        ]
    )

    async with bbb.BoostExecutor(100) as e:
        await bbb.copytree(any_dir, other_any_dir, e)

    async def _listtree(d, base):
        return sorted([p.relative_to(base) async for p in bbb.listtree(d)])

    assert await _listtree(any_dir, any_dir) == await _listtree(other_any_dir, other_any_dir)


@pytest.mark.asyncio
@bbb.ensure_session
async def test_copyglob():
    with helpers.tmp_azure_dir() as dir1:
        with helpers.tmp_azure_dir() as dir2:
            await asyncio.wait(
                [
                    helpers.unsafe_create_file(dir1 / "f1"),
                    helpers.unsafe_create_file(dir1 / "f2"),
                    helpers.unsafe_create_file(dir1 / "g3"),
                ]
            )

            async with bbb.BoostExecutor(100) as e:
                with pytest.raises(FileNotFoundError):
                    [p async for p in bbb.copying.copyglob_iterator(dir1 / "nope*", dir2, e)]

                copied = [p async for p in bbb.copying.copyglob_iterator(dir1 / "f*", dir2, e)]
                assert sorted([p.relative_to(dir1) for p in copied]) == ["f1", "f2"]
                contents = sorted([p.relative_to(dir2) async for p in bbb.listtree(dir2)])
                assert contents == ["f1", "f2"]


@pytest.mark.asyncio
@bbb.ensure_session
async def test_azure_copyfile_via_block_urls():
    CHUNK_SIZE = 16

    with open("/dev/random", "rb") as f:
        contents_medium = f.read(1024 * CHUNK_SIZE)

    with helpers.tmp_azure_dir() as azure_dir:
        helpers.create_file(azure_dir / "original_medium", contents_medium)

        with bbb.globals.configure(chunk_size=CHUNK_SIZE):
            async with bbb.BoostExecutor(100) as e:
                await bbb.copying._azure_cloud_copyfile_via_block_urls(
                    azure_dir / "original_medium", azure_dir / "copied_medium", e
                )
                with blobfile.BlobFile(str(azure_dir / "copied_medium"), "rb") as f:
                    assert f.read() == contents_medium
