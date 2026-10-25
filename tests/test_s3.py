import asyncio
import gzip
import os
from contextlib import asynccontextmanager
from contextvars import ContextVar
from unittest.mock import AsyncMock, Mock

import pytest
from aiohttp import ClientPayloadError

import boostedblob as bbb
from boostedblob import _s3

from . import helpers


@pytest.mark.asyncio
@bbb.ensure_session
async def test_s3_multipart(monkeypatch):
    with helpers.tmp_s3_dir() as directory:
        src = directory / "nested/../a space+%2F?#/雪"
        dst = directory / "copied"
        part_size = _s3.MIN_PART_SIZE
        chunks = [b"a" * (part_size - 1), b"b" * (part_size + 2)]
        contents = b"".join(chunks)
        with bbb.globals.configure(chunk_size=part_size):
            async with bbb.BoostExecutor(2) as executor:
                await bbb.write.write_stream(src, iter(chunks), executor)
                assert helpers.read_file(src) == contents
                assert [p async for p in bbb.listtree(directory)] == [src]
                assert await bbb.read.read_byte_range(src, (0, 4)) == contents[:4]
                # Exercise multipart server-side copy without needing a multi-GiB fixture.
                monkeypatch.setattr(_s3, "MAX_PART_SIZE", part_size)
                await bbb.copyfile(src, dst, executor)
                assert helpers.read_file(dst) == contents
                with pytest.raises(FileExistsError):
                    await bbb.copyfile(src, dst, executor)
                with pytest.raises(FileExistsError):
                    await bbb.write.write_single(src, b"replacement")
                assert (await bbb.stat(src)).md5 is None


@pytest.mark.asyncio
@bbb.ensure_session
async def test_s3_failed_stream_aborts_upload():
    with helpers.tmp_s3_dir() as directory:
        path = directory / "existing"
        helpers.create_file(path, b"original")

        def interrupted():
            yield b"x" * _s3.MIN_PART_SIZE
            yield b"y" * _s3.MIN_PART_SIZE
            raise RuntimeError("interrupted stream")

        with bbb.globals.configure(chunk_size=_s3.MIN_PART_SIZE):
            async with bbb.BoostExecutor(2) as executor:
                with pytest.raises(RuntimeError, match="interrupted stream"):
                    await bbb.write.write_stream(path, interrupted(), executor, overwrite=True)
        assert helpers.read_file(path) == b"original"
        result = helpers.s3_test_client().list_multipart_uploads(
            Bucket=path.bucket, Prefix=path.blob
        )
        assert not result.get("Uploads")


@pytest.mark.asyncio
@pytest.mark.parametrize("boosted", [False, True], ids=["iterator", "boostable"])
async def test_s3_ordered_stream_parts(monkeypatch, boosted):
    path = bbb.S3Path("bucket", "key")
    parts = []
    second_finished = asyncio.Event()
    finished = []

    @asynccontextmanager
    async def multipart(path, overwrite):
        yield "upload", parts

    async def upload(path, upload_id, number, data):
        if number == 1:
            await asyncio.wait_for(second_finished.wait(), timeout=5)
        if number == 2:
            second_finished.set()
        finished.append(number)
        return {"PartNumber": number, "data": data}

    async def source(data):
        return data

    marker = ContextVar("source", default="outside")
    owner = asyncio.current_task()
    inputs = [b"a", memoryview(b"bcd"), b"", bytearray(b"e")]

    def chunks():
        token = marker.set("inside")
        try:
            for chunk in inputs:
                assert asyncio.current_task() is owner
                yield chunk
        finally:
            marker.reset(token)

    monkeypatch.setattr(_s3, "MIN_PART_SIZE", 2)
    monkeypatch.setattr(bbb.write, "multipart_upload", multipart)
    monkeypatch.setattr(bbb.write, "_s3_upload_part", upload)
    with bbb.globals.configure(chunk_size=2):
        async with bbb.BoostExecutor(2) as executor:
            stream = executor.map_ordered(source, iter(inputs)) if boosted else chunks()
            await bbb.write.write_stream(path, stream, executor, overwrite=True)
    assert marker.get() == "outside"
    assert finished == [2, 1, 3]
    assert [(p["PartNumber"], p["data"]) for p in parts] == [(1, b"ab"), (2, b"cd"), (3, b"e")]


@pytest.mark.asyncio
async def test_s3_uploads_ready_part_before_waiting_for_input(monkeypatch):
    first_uploaded = asyncio.Event()
    parts = []

    @asynccontextmanager
    async def multipart(path, overwrite):
        yield "upload", parts

    async def upload(path, upload_id, number, data):
        first_uploaded.set()
        return data

    async def source(data):
        if data == b"cd":
            await asyncio.wait_for(first_uploaded.wait(), timeout=5)
        return data

    monkeypatch.setattr(_s3, "MIN_PART_SIZE", 2)
    monkeypatch.setattr(bbb.write, "multipart_upload", multipart)
    monkeypatch.setattr(bbb.write, "_s3_upload_part", upload)
    with bbb.globals.configure(chunk_size=2):
        async with bbb.BoostExecutor(2) as executor:
            stream = executor.map_ordered(source, iter([b"ab", b"cd"]))
            await bbb.write.write_stream(
                bbb.S3Path("bucket", "key"), stream, executor, overwrite=True
            )
    assert parts == [b"ab", b"cd"]


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel", [True, False], ids=["cancelled", "failed_part"])
@bbb.ensure_session
async def test_s3_interrupted_stream_aborts_upload(monkeypatch, s3_http, cancel):
    started = asyncio.Event()
    active = set()
    calls = s3_http((200, {}, b"<Result><UploadId>upload</UploadId></Result>"), (204, {}, b""))

    async def upload(path, upload_id, number, data):
        active.add(number)
        if len(active) == 2:
            started.set()
        try:
            if number == 1 and not cancel:
                await started.wait()
                raise RuntimeError("failed part")
            await asyncio.Future()
        finally:
            assert not any(method == "DELETE" for method, _, _ in calls)
            active.remove(number)

    monkeypatch.setattr(_s3, "MIN_PART_SIZE", 1)
    monkeypatch.setattr(bbb.write, "_s3_upload_part", upload)
    with bbb.globals.configure(chunk_size=1):
        async with bbb.BoostExecutor(2) as executor:
            write = asyncio.create_task(
                bbb.write.write_stream(
                    bbb.S3Path("bucket", "key"), iter([b"abc"]), executor, overwrite=True
                )
            )
            await asyncio.wait_for(started.wait(), timeout=5)
            if cancel:
                write.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await asyncio.wait_for(write, timeout=5)
            else:
                with pytest.raises(RuntimeError, match="failed part"):
                    await asyncio.wait_for(write, timeout=5)
            assert not active
    assert [method for method, _, _ in calls] == ["HEAD", "POST", "DELETE"]
    assert calls[-1][1].query["uploadId"] == "upload"


@pytest.mark.asyncio
@bbb.ensure_session
async def test_s3_unordered_and_empty_directory():
    with helpers.tmp_s3_dir() as directory:
        size = _s3.MIN_PART_SIZE
        path = directory / "unordered"
        stream = [(b"tail", (size, size + 4)), (b"z" * size, (0, size))]
        with bbb.globals.configure(chunk_size=size):
            async with bbb.BoostExecutor(2) as executor:
                await bbb.write.write_stream_unordered(path, iter(stream), executor)
                assert helpers.read_file(path) == b"z" * size + b"tail"
                empty = directory / "empty/"
                await bbb.write.write_single(empty, b"")
                await bbb.rmtree(empty, executor)
                assert not await bbb.exists(empty)


@pytest.mark.asyncio
@bbb.ensure_session
async def test_s3_reads_stored_bytes():
    with helpers.tmp_s3_dir() as directory:
        path = directory / "content.gz"
        compressed = gzip.compress(b"original content")
        helpers.s3_test_client().put_object(
            Bucket=path.bucket, Key=path.blob, Body=compressed, ContentEncoding="gzip"
        )
        assert await bbb.read.read_single(path) == compressed
        assert await bbb.read.read_byte_range(path, (1, 4)) == compressed[1:4]


@pytest.fixture
def s3_http(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "session-token")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_CONFIG_FILE", os.devnull)
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    monkeypatch.delenv("AWS_DEFAULT_PROFILE", raising=False)
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    monkeypatch.delenv("AWS_ENDPOINT_URL_S3", raising=False)
    monkeypatch.setattr(bbb.globals.config, "backoff_initial", 0)

    def stub(*responses, head_status=403):
        # A GetObject-only caller can still discover the bucket region.
        sequence = iter([(head_status, {"x-amz-bucket-region": "us-west-2"}, b""), *responses])
        calls = []

        @asynccontextmanager
        async def send(*, method, url, headers, **kwargs):
            calls.append((method, url, headers))
            status, response_headers, body = next(sequence)
            read = (
                AsyncMock(side_effect=body)
                if isinstance(body, Exception)
                else AsyncMock(return_value=body)
            )
            yield Mock(status=status, headers=response_headers, read=read, reason="test response")

        monkeypatch.setattr(bbb.globals.config.session, "request", send)
        return calls

    return stub


@pytest.mark.asyncio
@bbb.ensure_session
async def test_s3_missing_bucket_reports_requested_path(s3_http):
    path = bbb.S3Path("missing-bucket", "key")
    s3_http(head_status=404)
    with pytest.raises(FileNotFoundError) as exc:
        await bbb.read.read_single(path)
    assert exc.value.args == (path,)


@pytest.mark.asyncio
@bbb.ensure_session
async def test_s3_write_stream_existing_destination(s3_http):
    path = bbb.S3Path("bucket", "existing")
    s3_http(
        (
            200,
            {
                "Content-Length": "0",
                "Last-Modified": "Sat, 24 Oct 2026 00:00:00 GMT",
                "ETag": '"etag"',
            },
            b"",
        )
    )

    def source():
        raise AssertionError("the source should not be read")
        yield b""

    async with bbb.BoostExecutor(1) as executor:
        with pytest.raises(FileExistsError) as exc:
            await bbb.write.write_stream(path, source(), executor)
    assert exc.value.args == (path,)


@pytest.mark.asyncio
@bbb.ensure_session
async def test_s3_paginated_listing(s3_http):
    path = bbb.S3Path("bucket", "a +%/")
    calls = s3_http(
        (
            200,
            {},
            b'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><NextContinuationToken>+/=</NextContinuationToken></ListBucketResult>',
        ),
        (
            200,
            {},
            b'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><CommonPrefixes><Prefix>a+%2B%25/dir/</Prefix></CommonPrefixes></ListBucketResult>',
        ),
    )
    assert [p async for p in bbb.listdir(path)] == [path / "dir/"]
    assert [method for method, _, _ in calls] == ["HEAD", "GET", "GET"]
    for _, url, headers in calls[1:]:
        assert url.host == "bucket.s3.us-west-2.amazonaws.com"
        assert "/us-west-2/s3/aws4_request" in headers["Authorization"]
        assert headers["X-Amz-Security-Token"] == "session-token"
        assert "prefix=a%20%2B%25%2F" in str(url)
    assert "continuation-token=%2B%2F%3D" in str(calls[-1][1])


@pytest.mark.asyncio
@bbb.ensure_session
async def test_s3_isdir_empty_first_page(s3_http):
    path = bbb.S3Path("bucket", "dir")
    s3_http(
        (
            200,
            {},
            b"<ListBucketResult><KeyCount>0</KeyCount><NextContinuationToken>next</NextContinuationToken></ListBucketResult>",
        ),
        (200, {}, b"<ListBucketResult><KeyCount>1</KeyCount></ListBucketResult>"),
    )
    assert await bbb.isdir(path)


@pytest.mark.asyncio
@bbb.ensure_session
async def test_s3_retry_read_body(s3_http):
    path = bbb.S3Path("bucket", "prefix/../literal %2F+#?")
    calls = s3_http((206, {}, ClientPayloadError()), (206, {}, b"ok"))
    assert await bbb.read.read_byte_range(path, (4, 6)) == b"ok"
    assert [method for method, _, _ in calls] == ["HEAD", "GET", "GET"]
    for _, url, headers in calls[1:]:
        assert url.raw_path == "/prefix/../literal%20%252F%2B%23%3F"
        assert headers["Range"] == "bytes=4-5"


@pytest.mark.asyncio
@bbb.ensure_session
async def test_s3_copy_embedded_error(s3_http):
    src = bbb.S3Path("bucket", "source")
    dst = bbb.S3Path("bucket", "dest")
    calls = s3_http(
        (200, {}, b"<Error><Code>SlowDown</Code></Error>"),
        (200, {}, b"<Error><Code>NoSuchKey</Code></Error>"),
    )
    async with bbb.BoostExecutor(1) as executor:
        with pytest.raises(FileNotFoundError) as exc:
            await bbb.copyfile(src, dst, executor, size=1, overwrite=True)
    assert exc.value.args == (src,)
    assert [method for method, _, _ in calls] == ["HEAD", "PUT", "PUT"]


@pytest.mark.asyncio
@pytest.mark.parametrize("streamed", [False, True], ids=["single", "stream"])
@bbb.ensure_session
async def test_s3_conditional_write(s3_http, streamed):
    path = bbb.S3Path("bucket", "dest")
    calls = s3_http(
        (404, {}, b""),
        (200, {}, b"<ListBucketResult><KeyCount>0</KeyCount></ListBucketResult>"),
        (412, {}, b""),
    )
    with pytest.raises(FileExistsError) as exc:
        if streamed:
            async with bbb.BoostExecutor(1) as executor:
                await bbb.write.write_stream(path, iter([b"new"]), executor)
        else:
            await bbb.write.write_single(path, b"new")
    assert exc.value.args == (path,)
    method, _, headers = calls[-1]
    assert method == "PUT"
    assert headers["If-None-Match"] == "*"
