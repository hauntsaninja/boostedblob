import asyncio
import os

import blobfile
import pytest

import boostedblob as bbb
from boostedblob.path import LocalPath

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
        if isinstance(other_any_dir, LocalPath):
            # TODO: should we need to do this?
            os.rmdir(other_any_dir)
        await bbb.copytree(any_dir, other_any_dir, e)

    async def _listtree(d, base):
        return sorted([p.relative_to(base) async for p in bbb.listtree(d)])

    assert await _listtree(any_dir, any_dir) == await _listtree(other_any_dir, other_any_dir)
