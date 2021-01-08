from __future__ import annotations

import base64
import hashlib
import os
import random
from typing import List, Mapping, Optional, Tuple, Union

from .boost import BoostExecutor, BoostUnderlying, consume, iter_underlying
from .delete import remove
from .path import AzurePath, BasePath, CloudPath, GooglePath, LocalPath, exists, pathdispatch
from .read import ByteRange
from .request import Request, azurify_request, googlify_request

AZURE_BLOCK_COUNT_LIMIT = 50000

# ==============================
# write_single
# ==============================


@pathdispatch
async def write_single(path: Union[BasePath, str], data: bytes, overwrite: bool = False) -> None:
    """Write the given stream to ``path``.

    :param path: The path to write to.
    :param data: The data to write.
    :param overwrite: If False, raises if the path already exists.

    """
    raise ValueError(f"Unsupported path: {path}")


@write_single.register  # type: ignore
async def _azure_write_single(path: AzurePath, data: bytes, overwrite: bool = False) -> None:
    if not overwrite:
        if await exists(path):
            raise FileExistsError(path)

    request = await azurify_request(
        Request(
            method="PUT",
            url=path.format_url("https://{account}.blob.core.windows.net/{container}/{blob}"),
            data=data,
            headers={"x-ms-blob-type": "BlockBlob"},
            success_codes=(201,),
        )
    )
    await request.execute_reponseless()


@write_single.register  # type: ignore
async def _google_write_single(path: GooglePath, data: bytes, overwrite: bool = False) -> None:
    if not overwrite:
        if await exists(path):
            raise FileExistsError(path)

    request = await googlify_request(
        Request(
            method="POST",
            url=path.format_url(
                "https://storage.googleapis.com/upload/storage/v1/b/{bucket}/o?uploadType=media&name={blob}"
            ),
            data=data,
            headers={"Content-Type": "application/octet-stream"},
        )
    )
    await request.execute_reponseless()


@write_single.register  # type: ignore
async def _local_write_single(path: LocalPath, data: bytes, overwrite: bool = False) -> None:
    if not overwrite:
        if await exists(path):
            raise FileExistsError(path)

    os.makedirs(path.parent, exist_ok=True)
    with open(path, mode="wb") as f:
        f.write(data)


# ==============================
# write_stream
# ==============================


@pathdispatch
async def write_stream(
    path: Union[BasePath, str],
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
        # if the existing blob type is not compatible with the block blob we are about to write we
        # have to delete the file before writing our block blob or else we will get a 409 error when
        # putting the first block
        # if the existing blob is compatible, then in the event of multiple concurrent writers we
        # run the risk of ending up with uncommitted blocks, which could hit the uncommitted block
        # limit. rather than deal with that, just remove the file before writing which will clear
        # all uncommitted blocks
        # we could have a more elaborate upload system that does a write, then a copy, then a delete
        # but it's not obvious how to ensure that the temporary file is deleted without creating a
        # lifecycle rule on each container
        # TODO: blobfile has made some changes here, consider pulling them in
        try:
            await remove(path)
        except FileNotFoundError:
            pass
    else:
        if await exists(path):
            raise FileExistsError(path)

    upload_id = random.randint(0, 2 ** 47 - 1)
    block_index = 0
    md5 = hashlib.md5()

    async def upload_chunk(chunk: bytes) -> None:
        # mutating an index in the outer scope is a little sketchy, but it works out if we do it
        # before we await
        nonlocal block_index
        block_id = _get_block_id(upload_id, block_index)
        block_index += 1
        # https://docs.microsoft.com/en-us/rest/api/storageservices/put-block-list#remarks
        assert block_index < AZURE_BLOCK_COUNT_LIMIT
        md5.update(chunk)
        await _azure_put_block(path, block_id, chunk)

    await consume(executor.map_ordered(upload_chunk, stream))

    # azure does not calculate md5s for us, we have to do that manually
    # https://blogs.msdn.microsoft.com/windowsazurestorage/2011/02/17/windows-azure-blob-md5-overview/
    headers = {"x-ms-blob-content-md5": base64.b64encode(md5.digest()).decode("utf8")}
    blocklist = [_get_block_id(upload_id, i) for i in range(block_index)]
    await _azure_put_block_list(path, blocklist, headers=headers)


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
        # invalid chunk size. If we receive multiple chunks of invalid chunk size, we throw an
        # error.
        should_finalise = len(chunk) % (256 * 1024) != 0
        if should_finalise:
            if is_finalised:
                raise ValueError(
                    "The upload was already finalised. A likely cause is the given stream was "
                    "chunked incorrectly. Uploads to Google Cloud need to be chunked in "
                    "multiples of 256 KB (except for the last chunk)."
                )
            total_size = str(end)
            is_finalised = True

        request = await googlify_request(
            Request(
                method="PUT",
                url=upload_url,
                data=chunk,
                headers={
                    "Content-Type": "application/octet-stream",
                    "Content-Range": f"bytes {start}-{end-1}/{total_size}",
                },
                success_codes=(200, 201) if is_finalised else (308,),
            )
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
    with open(path, mode="wb") as f:
        async for data in iter_underlying(stream):
            # Note that this is a blocking write. We used to use loop.run_in_executor, but it
            # didn't seem to help much, and had some downsides. First, we run into
            # https://bugs.python.org/issue35279 on Python 3.7. Second, blocking writes provide
            # natural backpressure.
            f.write(data)


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
        try:
            await remove(path)
        except FileNotFoundError:
            pass
    else:
        if await exists(path):
            raise FileExistsError(path)

    upload_id = random.randint(0, 2 ** 47 - 1)
    iter_index = 0
    block_list = []

    async def upload_chunk(chunk_byte_range: Tuple[bytes, ByteRange]) -> None:
        # mutating an index in the outer scope is a little sketchy, but it works out if we do it
        # before we await
        nonlocal iter_index
        block_id = _get_block_id(upload_id, iter_index)
        chunk, byte_range = chunk_byte_range
        block_list.append((byte_range[0], iter_index))

        iter_index += 1
        # https://docs.microsoft.com/en-us/rest/api/storageservices/put-block-list#remarks
        assert iter_index < AZURE_BLOCK_COUNT_LIMIT

        await _azure_put_block(path, block_id, chunk)

    await consume(executor.map_unordered(upload_chunk, stream))

    # sort by start byte so the blocklist is ordered correctly
    block_list.sort()
    await _azure_put_block_list(path, [_get_block_id(upload_id, index) for _, index in block_list])


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


def _get_block_id(upload_id: int, index: int) -> str:
    assert index < 2 ** 17
    id_plus_index = (upload_id << 17) + index
    assert id_plus_index < 2 ** 64
    return base64.b64encode(id_plus_index.to_bytes(8, byteorder="big")).decode("utf8")


async def _azure_put_block(path: AzurePath, block_id: str, chunk: bytes) -> None:
    request = await azurify_request(
        Request(
            method="PUT",
            url=path.format_url("https://{account}.blob.core.windows.net/{container}/{blob}"),
            params=dict(comp="block", blockid=block_id),
            data=chunk,
            success_codes=(201,),
        )
    )
    await request.execute_reponseless()


async def _azure_put_block_list(
    path: AzurePath, block_list: List[str], headers: Optional[Mapping[str, str]] = None
) -> None:
    if headers is None:
        headers = {}
    request = await azurify_request(
        Request(
            method="PUT",
            url=path.format_url("https://{account}.blob.core.windows.net/{container}/{blob}"),
            headers=headers,
            params=dict(comp="blocklist"),
            data={"BlockList": {"Latest": block_list}},
            success_codes=(201,),
        )
    )
    await request.execute_reponseless()


async def _google_start_resumable_upload(path: GooglePath) -> str:
    request = await googlify_request(
        Request(
            method="POST",
            url=path.format_url(
                "https://storage.googleapis.com/upload/storage/v1/b/{bucket}/o?uploadType=resumable"
            ),
            data=dict(name=path.blob),
            headers={"Content-Type": "application/json; charset=UTF-8"},
            failure_exceptions={400: FileNotFoundError(), 404: FileNotFoundError()},
        )
    )
    async with request.execute() as resp:
        upload_url = resp.headers["Location"]
        return upload_url


async def _google_finalise_upload(upload_url: str, total_size: int) -> None:
    headers = {"Content-Type": "application/octet-stream", "Content-Range": f"bytes */{total_size}"}
    request = await googlify_request(
        Request(method="PUT", url=upload_url, headers=headers, success_codes=(200, 201))
    )
    await request.execute_reponseless()
