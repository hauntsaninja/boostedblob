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
