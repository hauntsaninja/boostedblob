import pytest

import boostedblob as bbb

from . import helpers


@pytest.mark.asyncio
@bbb.ensure_session
async def test_google_chunking():
    with helpers.tmp_google_dir() as google_dir:
        async with bbb.BoostExecutor(10) as e:
            contents = [b"abc", b"def", b"ghi"]
            with pytest.raises(ValueError, match="chunked incorrectly"):
                await bbb.write.write_stream(google_dir / "alpha", iter(contents), e)


@pytest.mark.asyncio
@bbb.ensure_session
async def test_read_write(any_dir):
    async with bbb.BoostExecutor(10) as e:
        # test reading and writing an empty stream
        await bbb.write.write_stream(any_dir / "empty", iter([]), e)
        stream = await bbb.read.read_stream(any_dir / "empty", e)
        async for _ in bbb.boost.iter_underlying(stream):
            raise AssertionError
