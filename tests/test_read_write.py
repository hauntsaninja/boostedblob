import asyncio
import contextlib
import os
from types import SimpleNamespace

import pytest

import boostedblob as bbb
from boostedblob.request import RequestFailure

from . import helpers


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["single", "stream", "unordered"])
@pytest.mark.parametrize("overwrite", [False, True])
@pytest.mark.parametrize("collision", [None, 409, 412])
async def test_azure_writes_use_if_none_match_unless_overwriting(
    monkeypatch, operation, overwrite, collision
):
    requests = []

    @contextlib.asynccontextmanager
    async def fake_execute(request):
        async def read():
            return b""

        status = collision if collision and request.params.get("comp") != "block" else 201
        yield SimpleNamespace(status=status, reason="Precondition Failed", headers={}, read=read)

    async def fake_auth(request):
        requests.append(request)
        return SimpleNamespace(raw_execute=lambda: fake_execute(request))

    async def nonexistent(_path):
        return False

    async def prepare(_path):
        return None

    monkeypatch.setattr(bbb.write, "azure_auth_req", fake_auth)
    monkeypatch.setattr(bbb.write, "exists", nonexistent)
    monkeypatch.setattr(bbb.write, "prepare_block_blob_write", prepare)

    path = bbb.AzurePath("account", "container", "blob")
    expectation = (
        pytest.raises(FileExistsError if not overwrite else RequestFailure)
        if collision
        else contextlib.nullcontext()
    )
    with expectation:
        async with bbb.BoostExecutor(2) as executor:
            if operation == "single":
                await bbb.write.write_single(path, b"data", overwrite=overwrite)
            elif operation == "stream":
                await bbb.write.write_stream(path, iter([b"data"]), executor, overwrite=overwrite)
            else:
                await bbb.write.write_stream_unordered(
                    path, iter([(b"data", (0, 4))]), executor, overwrite=overwrite
                )

    assert requests[-1].headers.get("If-None-Match") == (None if overwrite else "*")
    assert all("If-None-Match" not in request.headers for request in requests[:-1])


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
async def test_read_write_empty(any_dir):
    async with bbb.BoostExecutor(10) as e:
        await bbb.write.write_stream(any_dir / "empty", iter([]), e)
        stream = await bbb.read.read_stream(any_dir / "empty", e)
        async for _ in bbb.boost.iter_underlying(stream):
            raise AssertionError


@pytest.mark.asyncio
@bbb.ensure_session
async def test_read_not_found(any_dir):
    async with bbb.BoostExecutor(10) as e:
        with pytest.raises(FileNotFoundError):
            stream = await bbb.read.read_stream(any_dir / "not_found", e)
            async for _buf in bbb.boost.iter_underlying(stream):
                pass


@pytest.mark.asyncio
@bbb.ensure_session
async def test_read_write_single_chunk(any_dir):
    async with bbb.BoostExecutor(10) as e:
        contents = [b"a"]
        await bbb.write.write_stream(any_dir / "small", iter(contents), e)
        stream = await bbb.read.read_stream(any_dir / "small", e)
        chunks = []
        async for buf in bbb.boost.iter_underlying(stream):
            chunks.append(buf)
        assert len(chunks) == 1
        assert chunks[0] == b"a"


@pytest.mark.asyncio
@bbb.ensure_session
async def test_read_write_many_chunks(any_dir):
    with bbb.globals.configure(chunk_size=1024):
        async with bbb.BoostExecutor(10) as e:
            # Google requires chunks to be multiples of 256 KB, except for the last chunk.
            contents = [os.urandom(256 * 1024), os.urandom(42)]
            await bbb.write.write_stream(any_dir / "big", iter(contents), e)
            stream = await bbb.read.read_stream(any_dir / "big", e)
            read_chunks = []
            async for buf in bbb.boost.iter_underlying(stream):
                read_chunks.append(buf)
            assert len(read_chunks) == 257
            assert b"".join(contents) == b"".join(read_chunks)


@pytest.mark.asyncio
@bbb.ensure_session
async def test_read_write_chunked(cloud_dir):
    with bbb.globals.configure(chunk_size=1024):
        async with bbb.BoostExecutor(10) as e:
            original_contents = os.urandom(256 * 1024 + 42)
            await bbb.write.write_chunked(cloud_dir / "chunk", original_contents, e)

            contents = await bbb.read.read_chunked(cloud_dir / "chunk", e)
            assert original_contents == contents

            contents = await bbb.read.read_single(cloud_dir / "chunk")
            assert original_contents == contents


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
