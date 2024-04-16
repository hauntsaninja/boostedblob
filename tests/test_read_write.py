import asyncio

import pytest

import boostedblob as bbb

from . import helpers


@pytest.mark.asyncio
@bbb.ensure_session
async def test_google_chunking():
    with helpers.tmp_google_dir() as google_dir:
        async with bbb.BoostExecutor(10) as e:
            contents = [b"abc", b"def", b"ghi"]
            with pytest.warns(UserWarning, match="chunked incorrectly"):
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


@pytest.mark.asyncio
@bbb.ensure_session
async def test_concurrent_write(any_dir):
    async with bbb.BoostExecutor(10) as e:
        contents = [b"abcd" * 64 * 1024, b"efgh" * 64 * 1024, b"ijkl" * 64 * 1024]
        t1 = bbb.write.write_stream(any_dir / "blob", iter(contents), e, overwrite=True)
        t2 = bbb.write.write_stream(any_dir / "blob", iter(contents), e, overwrite=True)

        if isinstance(any_dir, bbb.AzurePath):
            with pytest.raises(RuntimeError):
                # This might be flaky now, due to retrying put block list
                await asyncio.gather(t1, t2)
        else:
            await asyncio.gather(t1, t2)
            assert b"".join(contents) == await bbb.read.read_single(any_dir / "blob")


@pytest.mark.asyncio
@bbb.ensure_session
async def test_prepare_block_blob_write():
    with helpers.tmp_azure_dir() as azure_dir:
        async with bbb.BoostExecutor(10) as e:
            path = azure_dir / "blob"
            contents = [b"abcd", b"efgh"]
            await bbb.write.write_stream(path, iter(contents), e)
            await bbb.write.prepare_block_blob_write(path, _always_clear=True)
            assert b"".join(contents) == await bbb.read.read_single(path)


@pytest.mark.asyncio
@bbb.ensure_session
async def test_azure_write_unordered():
    with helpers.tmp_azure_dir() as azure_dir:
        async with bbb.BoostExecutor(10) as e:
            path = azure_dir / "blob"
            contents = [b"abc", b"defghi", b"jkl"]
            stream = []
            offset = 0
            for c in contents:
                stream.append((c, (offset, offset + len(c))))
                offset += len(c)

            await bbb.write.write_stream_unordered(path, iter(stream), e)
            assert b"".join(contents) == await bbb.read.read_single(path)


@pytest.mark.asyncio
@bbb.ensure_session
async def test_read_byte_range(any_dir):
    path = any_dir / "blob"
    helpers.create_file(path, b"1111222233334444")
    assert b"1111" == await bbb.read.read_byte_range(path, (0, 4))
    assert b"2222" == await bbb.read.read_byte_range(path, (4, 8))
    assert b"33334444" == await bbb.read.read_byte_range(path, (8, None))
    assert b"11112" == await bbb.read.read_byte_range(path, (None, 5))
    assert b"1111222233334444" == await bbb.read.read_byte_range(path, (None, None))
