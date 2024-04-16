from __future__ import annotations

import asyncio
import base64
import hashlib
import os
import random
from typing import List, Mapping, Optional, Tuple, Union

from .boost import BoostExecutor, BoostUnderlying, consume, iter_underlying
from .delete import remove
from .globals import config
from .path import (
    AzurePath,
    BasePath,
    BlobPath,
    CloudPath,
    GooglePath,
    LocalPath,
    exists,
    pathdispatch,
)
from .read import ByteRange
from .request import (
    Request,
    RequestFailure,
    azure_auth_req,
    exponential_sleep_generator,
    google_auth_req,
)
from .xml import etree

AZURE_BLOCK_COUNT_LIMIT = 50_000

# ==============================
# write_single
# ==============================


@pathdispatch
async def write_single(
    path: Union[BasePath, BlobPath, str],
    data: Union[bytes, bytearray, memoryview],
    overwrite: bool = False,
) -> None:
    """Write ``data`` to ``path`` in a single request.

    :param path: The path to write to.
    :param data: The data to write.
    :param overwrite: If False, raises if the path already exists.

    """
    raise ValueError(f"Unsupported path: {path}")


@write_single.register  # type: ignore
async def _azure_write_single(
    path: AzurePath, data: Union[bytes, bytearray, memoryview], overwrite: bool = False
) -> None:
    if not overwrite:
        if await exists(path):
            raise FileExistsError(path)

    request = Request(
        method="PUT",
        url=path.format_url("https://{account}.blob.core.windows.net/{container}/{blob}"),
        data=data,
        headers={"x-ms-blob-type": "BlockBlob"},
        success_codes=(201,),
        auth=azure_auth_req,
    )
    await request.execute_reponseless()


@write_single.register  # type: ignore
async def _google_write_single(
    path: GooglePath, data: Union[bytes, bytearray, memoryview], overwrite: bool = False
) -> None:
    if not overwrite:
        if await exists(path):
            raise FileExistsError(path)

    request = Request(
        method="POST",
        url=path.format_url(
            "https://storage.googleapis.com/upload/storage/v1/b/{bucket}/o?uploadType=media&name={blob}"
        ),
        data=data,
        headers={"Content-Type": "application/octet-stream"},
        auth=google_auth_req,
    )
    await request.execute_reponseless()


@write_single.register  # type: ignore
async def _local_write_single(
    path: LocalPath, data: Union[bytes, bytearray, memoryview], overwrite: bool = False
) -> None:
    if not overwrite:
        if await exists(path):
            raise FileExistsError(path)

    loop = asyncio.get_running_loop()
    os.makedirs(path.parent, exist_ok=True)
    with open(path, mode="wb") as f:
        await loop.run_in_executor(config.executor, f.write, data)


# ==============================
# write_stream
# ==============================


@pathdispatch
async def write_stream(
    path: Union[BasePath, BlobPath, str],
    stream: BoostUnderlying[bytes],
    executor: BoostExecutor,
    overwrite: bool = False,
) -> None:
    """Write the given stream to ``path``.

    :param path: The path to write to.
    :param executor: An executor.
    :param stream: The stream of bytes to write. Note the chunking of stream also determines how
        we chunk the writes. Writes to Google Cloud must be chunked in multiples of 256 KB.
    :param overwrite: If False, raises if the path already exists.

    """
    raise ValueError(f"Unsupported path: {path}")


@write_stream.register  # type: ignore
async def _azure_write_stream(
    path: AzurePath,
    stream: BoostUnderlying[bytes],
    executor: BoostExecutor,
    overwrite: bool = False,
) -> None:
    if overwrite:
        await prepare_block_blob_write(path)
    else:
        if await exists(path):
            raise FileExistsError(path)

    upload_id = get_upload_id()
    md5 = hashlib.md5()
    max_block_index = -1

    async def upload_chunk(index_chunk: Tuple[int, bytes]) -> None:
        block_index, chunk = index_chunk
        # https://docs.microsoft.com/en-us/rest/api/storageservices/put-block-list#remarks
        assert block_index < AZURE_BLOCK_COUNT_LIMIT

        nonlocal max_block_index
        max_block_index = max(max_block_index, block_index)

        block_id = get_block_id(upload_id, block_index)
        md5.update(chunk)

        await _azure_put_block(path, block_id, chunk)

    await consume(executor.map_ordered(upload_chunk, executor.enumerate(stream)))

    # azure does not calculate md5s for us, we have to do that manually
    # https://blogs.msdn.microsoft.com/windowsazurestorage/2011/02/17/windows-azure-blob-md5-overview/
    headers = {"x-ms-blob-content-md5": base64.b64encode(md5.digest()).decode("utf8")}
    blocklist = [get_block_id(upload_id, i) for i in range(max_block_index + 1)]
    await azure_put_block_list(path, blocklist, headers=headers)


@write_stream.register  # type: ignore
async def _google_write_stream(
    path: GooglePath,
    stream: BoostUnderlying[bytes],
    executor: BoostExecutor,
    overwrite: bool = False,
) -> None:
    if not overwrite:
        if await exists(path):
            raise FileExistsError(path)

    upload_url = await _google_start_resumable_upload(path)

    offset = 0
    is_finalised = False
    total_size = "*"
    # Unfortunately, GCS doesn't allow us to upload these chunks in parallel.
    async for chunk in iter_underlying(stream):
        start = offset
        offset += len(chunk)
        end = offset

        # GCS requires resumable uploads to be chunked in multiples of 256 KB, except for the last
        # chunk. If you upload a chunk with another size you get an HTTP 400 error, unless you tell
        # GCS that it's the last chunk. Since our interface doesn't allow us to know whether or not
        # a given chunk is actually the last chunk, we go ahead and assume that it is if it's an
        # invalid chunk size. If we continue to receive chunks, which can happen if the file is
        # being written to concurrently, then we discard these additional chunks.
        if is_finalised:
            if chunk:
                import warnings

                warnings.warn(
                    "The upload was already finalised. A likely cause is a) the file was being "
                    "written to concurrently, or b) the given stream was chunked incorrectly. "
                    "Uploads to Google Cloud need to be chunked in multiples of 256 KB "
                    "(except for the last chunk).",
                    stacklevel=2,
                )
            break
        should_finalise = len(chunk) % (256 * 1024) != 0
        if should_finalise:
            total_size = str(end)
            is_finalised = True

        request = Request(
            method="PUT",
            url=upload_url,
            data=chunk,
            headers={
                "Content-Type": "application/octet-stream",
                "Content-Range": f"bytes {start}-{end-1}/{total_size}",
            },
            success_codes=(200, 201) if is_finalised else (308,),
            auth=google_auth_req,
        )
        await request.execute_reponseless()

    if not is_finalised:
        await _google_finalise_upload(upload_url, total_size=offset)


@write_stream.register  # type: ignore
async def _local_write_stream(
    path: LocalPath,
    stream: BoostUnderlying[bytes],
    executor: BoostExecutor,
    overwrite: bool = False,
) -> None:
    if not overwrite:
        if await exists(path):
            raise FileExistsError(path)

    os.makedirs(path.parent, exist_ok=True)
    loop = asyncio.get_running_loop()
    with open(path, mode="wb") as f:
        async for data in iter_underlying(stream):
            await loop.run_in_executor(config.executor, f.write, data)


# ==============================
# write_stream_unordered
# ==============================


@pathdispatch
async def write_stream_unordered(
    path: Union[CloudPath, str],
    stream: BoostUnderlying[Tuple[bytes, ByteRange]],
    executor: BoostExecutor,
    overwrite: bool = False,
) -> None:
    """Write the given stream to ``path``.

    :param path: The path to write to.
    :param executor: An executor.
    :param stream: The stream of bytes to write, along with what range of bytes each chunk
        corresponds to. Note the chunking of stream also determines how we chunk the writes.
    :param overwrite: If False, raises if the path already exists.

    """
    raise ValueError(f"Unsupported path: {path}")


@write_stream_unordered.register  # type: ignore
async def _azure_write_stream_unordered(
    path: AzurePath,
    stream: BoostUnderlying[Tuple[bytes, ByteRange]],
    executor: BoostExecutor,
    overwrite: bool = False,
) -> None:
    # TODO: this doesn't upload an md5...
    if overwrite:
        await prepare_block_blob_write(path)
    else:
        if await exists(path):
            raise FileExistsError(path)

    upload_id = get_upload_id()
    block_list = []

    async def upload_chunk(index_chunk_byte_range: Tuple[int, Tuple[bytes, ByteRange]]) -> None:
        unordered_index, (chunk, byte_range) = index_chunk_byte_range
        # https://docs.microsoft.com/en-us/rest/api/storageservices/put-block-list#remarks
        assert unordered_index < AZURE_BLOCK_COUNT_LIMIT

        block_id = get_block_id(upload_id, unordered_index)
        block_list.append((byte_range[0], unordered_index))

        await _azure_put_block(path, block_id, chunk)

    await consume(executor.map_unordered(upload_chunk, executor.enumerate(stream)))

    # sort by start byte so the blocklist is ordered correctly
    block_list.sort()
    await azure_put_block_list(path, [get_block_id(upload_id, index) for _, index in block_list])


@write_stream_unordered.register  # type: ignore
async def _google_write_stream_unordered(
    path: GooglePath,
    stream: BoostUnderlying[Tuple[bytes, ByteRange]],
    executor: BoostExecutor,
    overwrite: bool = False,
) -> None:
    raise NotImplementedError


# ==============================
# helpers
# ==============================


def get_upload_id() -> int:
    return random.randint(0, 2**47 - 1)


def get_block_id(upload_id: int, index: int) -> str:
    assert index < 2**17
    id_plus_index = (upload_id << 17) + index
    assert id_plus_index < 2**64
    return base64.b64encode(id_plus_index.to_bytes(8, byteorder="big")).decode("utf8")


async def prepare_block_blob_write(path: AzurePath, _always_clear: bool = False) -> None:
    request = Request(
        method="GET",
        url=path.format_url("https://{account}.blob.core.windows.net/{container}/{blob}"),
        params=dict(comp="blocklist"),
        success_codes=(200, 404, 400),
        auth=azure_auth_req,
    )
    async with request.execute() as resp:
        # If the block doesn't exist, we're good to go.
        if resp.status == 404:
            return
        # Get Block List will return 400 if the blob is not a block blob.
        # In this case, we need to remove the blob to prevent errors when we Put Block
        if resp.status == 400:
            await remove(path)
            return
        data = await resp.read()

    result = etree.fromstring(data)
    blocks = [b.text for b in result.iterfind("CommittedBlocks/Block/Name")]
    if not blocks:
        return

    if not _always_clear and len(blocks) < 10_000:
        # Don't bother clearing uncommitted blocks if we have less than 10,000 committed blocks,
        # since seems unlikely we'd run into the 100,000 block limit in this case.
        return

    # In the event of multiple concurrent writers, we run the risk of ending up with uncommitted
    # blocks which could hit the uncommited block limit. Clear uncommitted blocks by performing a
    # Put Block List with all the existing blocks. This will change the last-modified timestamp and
    # the etag.

    # Make sure to preserve metadata for the file
    request = Request(
        method="HEAD",
        url=path.format_url("https://{account}.blob.core.windows.net/{container}/{blob}"),
        failure_exceptions={404: FileNotFoundError(path)},
        auth=azure_auth_req,
    )
    async with request.execute() as resp:
        metadata = resp.headers
    headers = {k: v for k, v in metadata.items() if k.startswith("x-ms-meta-")}
    RESPONSE_HEADER_TO_REQUEST_HEADER = {
        "Cache-Control": "x-ms-blob-cache-control",
        "Content-Type": "x-ms-blob-content-type",
        "Content-MD5": "x-ms-blob-content-md5",
        "Content-Encoding": "x-ms-blob-content-encoding",
        "Content-Language": "x-ms-blob-content-language",
        "Content-Disposition": "x-ms-blob-content-disposition",
    }
    for src, dst in RESPONSE_HEADER_TO_REQUEST_HEADER.items():
        if src in metadata:
            headers[dst] = metadata[src]

    request = Request(
        method="PUT",
        url=path.format_url("https://{account}.blob.core.windows.net/{container}/{blob}"),
        headers={**headers, "If-Match": metadata["etag"]},
        params=dict(comp="blocklist"),
        data={"BlockList": {"Latest": blocks}},
        success_codes=(201, 404, 412),
        auth=azure_auth_req,
    )
    await request.execute_reponseless()


async def _azure_put_block(
    path: AzurePath, block_id: str, chunk: Union[bytes, bytearray, memoryview]
) -> None:
    request = Request(
        method="PUT",
        url=path.format_url("https://{account}.blob.core.windows.net/{container}/{blob}"),
        params=dict(comp="block", blockid=block_id),
        data=chunk,
        success_codes=(201,),
        auth=azure_auth_req,
    )
    await request.execute_reponseless()


async def azure_put_block_list(
    path: AzurePath, block_list: List[str], headers: Optional[Mapping[str, str]] = None
) -> None:
    if headers is None:
        headers = {}
    request = Request(
        method="PUT",
        url=path.format_url("https://{account}.blob.core.windows.net/{container}/{blob}"),
        headers=headers,
        params=dict(comp="blocklist"),
        data={"BlockList": {"Latest": block_list}},
        success_codes=(201, 400),
        auth=azure_auth_req,
    )

    for attempt, backoff in enumerate(
        exponential_sleep_generator(
            initial=config.backoff_initial,
            maximum=config.backoff_max,
            jitter_fraction=config.backoff_jitter_fraction,
        )
    ):
        async with request.execute() as resp:
            if resp.status == 201:
                return

            assert resp.status == 400
            data = await resp.read()
            result = etree.fromstring(data)
            if result.findtext("Code") == "InvalidBlockList":
                if attempt >= 3:
                    raise RuntimeError(
                        f"Invalid block list, most likely due to concurrent writes to {path}"
                    )
                # It turns out these can actually succeed on retry.
                # We run into this when using unordered upload, maybe it means we're just
                # going too fast...
                await asyncio.sleep(backoff)
                continue

            raise RequestFailure(reason=str(resp.reason), request=request, status=resp.status)


async def _google_start_resumable_upload(path: GooglePath) -> str:
    request = Request(
        method="POST",
        url=path.format_url(
            "https://storage.googleapis.com/upload/storage/v1/b/{bucket}/o?uploadType=resumable"
        ),
        data=dict(name=path.blob),
        headers={"Content-Type": "application/json; charset=UTF-8"},
        failure_exceptions={400: FileNotFoundError(), 404: FileNotFoundError()},
        auth=google_auth_req,
    )
    async with request.execute() as resp:
        upload_url = resp.headers["Location"]
        return upload_url


async def _google_finalise_upload(upload_url: str, total_size: int) -> None:
    headers = {"Content-Type": "application/octet-stream", "Content-Range": f"bytes */{total_size}"}
    request = Request(
        method="PUT",
        url=upload_url,
        headers=headers,
        success_codes=(200, 201),
        auth=google_auth_req,
    )
    await request.execute_reponseless()
