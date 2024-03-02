from __future__ import annotations

import itertools
from typing import Any, Iterator, Optional, Tuple, Union

from .boost import (
    BoostExecutor,
    BoostUnderlying,
    OrderedMappingBoostable,
    UnorderedMappingBoostable,
)
from .globals import config
from .path import (
    AzurePath,
    BasePath,
    BlobPath,
    CloudPath,
    GooglePath,
    LocalPath,
    getsize,
    pathdispatch,
)
from .request import Request, azure_auth_req, execute_retrying_read, google_auth_req

ByteRange = Tuple[int, int]
OptByteRange = Tuple[Optional[int], Optional[int]]

# ==============================
# read_byte_range
# ==============================


@pathdispatch
async def read_byte_range(path: Union[BasePath, BlobPath, str], byte_range: OptByteRange) -> bytes:
    """Read the content of ``path`` in the given byte range.

    :param path: The path to read from.
    :param byte_range: The byte range to read.
    :return: The relevant bytes.

    """
    raise ValueError(f"Unsupported path: {path}")


@read_byte_range.register  # type: ignore
async def _azure_read_byte_range(path: AzurePath, byte_range: OptByteRange) -> bytes:
    range_str = byte_range_to_str(byte_range)
    range_header = {"Range": range_str} if range_str is not None else {}
    success_codes = (206,) if range_header else (200,)
    request = Request(
        method="GET",
        url=path.format_url("https://{account}.blob.core.windows.net/{container}/{blob}"),
        headers=range_header,
        success_codes=success_codes,
        failure_exceptions={404: FileNotFoundError(path)},
        auth=azure_auth_req,
    )
    return await execute_retrying_read(request)


@read_byte_range.register  # type: ignore
async def _google_read_byte_range(path: GooglePath, byte_range: OptByteRange) -> bytes:
    range_str = byte_range_to_str(byte_range)
    range_header = {"Range": range_str} if range_str is not None else {}
    success_codes = (206,) if range_header else (200,)
    request = Request(
        method="GET",
        url=path.format_url("https://storage.googleapis.com/storage/v1/b/{bucket}/o/{blob}"),
        params=dict(alt="media"),
        headers=range_header,
        success_codes=success_codes,
        failure_exceptions={404: FileNotFoundError(path)},
        auth=google_auth_req,
    )
    return await execute_retrying_read(request)


@read_byte_range.register  # type: ignore
async def _local_read_byte_range(path: LocalPath, byte_range: OptByteRange) -> bytes:
    with open(path, "rb") as f:
        start, end = byte_range
        if start is None:
            return f.read() if end is None else f.read(end)
        f.seek(start)
        return f.read() if end is None else f.read(end - start)


# ==============================
# read_single
# ==============================


@pathdispatch
async def read_single(path: Union[BasePath, BlobPath, str]) -> bytes:
    """Read the content of ``path``.

    :param path: The path to read from.

    """
    raise ValueError(f"Unsupported path: {path}")


@read_single.register  # type: ignore
async def _cloud_read_single(path: CloudPath) -> bytes:
    return await read_byte_range(path, (None, None))


@read_single.register  # type: ignore
async def _local_read_single(path: LocalPath) -> bytes:
    with open(path, "rb") as f:
        return f.read()


# ==============================
# read_stream
# ==============================


@pathdispatch
async def read_stream(
    path: Union[BasePath, BlobPath, str], executor: BoostExecutor, size: Optional[int] = None
) -> BoostUnderlying[bytes]:
    """Read the content of ``path``.

    :param path: The path to read from.
    :param executor: An executor.
    :param size: If specified, will save a network call.
    :return: The stream of bytes, chunking determined by ``config.chunk_size``.

    """
    raise ValueError(f"Unsupported path: {path}")


@read_stream.register  # type: ignore
async def _cloud_read_stream(
    path: CloudPath, executor: BoostExecutor, size: Optional[int] = None
) -> OrderedMappingBoostable[Any, bytes]:
    if size is None:
        size = await getsize(path)

    byte_ranges = itertools.zip_longest(
        range(0, size, config.chunk_size),
        range(config.chunk_size, size, config.chunk_size),
        fillvalue=size,
    )

    # Note that we purposefully don't do
    # https://docs.aiohttp.org/en/stable/client_quickstart.html#streaming-response-content
    # Doing that would stream data as we needed it, which is a little too lazy for our purposes
    chunks = executor.map_ordered(lambda byte_range: read_byte_range(path, byte_range), byte_ranges)
    return chunks


@read_stream.register  # type: ignore
async def _local_read_stream(
    path: LocalPath, executor: BoostExecutor, size: Optional[int] = None
) -> Iterator[bytes]:
    def iterator() -> Iterator[bytes]:
        with open(path, "rb") as f:
            while True:
                data = f.read(config.chunk_size)
                if not data:
                    return
                yield data

    return iterator()


# ==============================
# read_stream_unordered
# ==============================


@pathdispatch
async def read_stream_unordered(
    path: Union[CloudPath, str], executor: BoostExecutor, size: Optional[int] = None
) -> UnorderedMappingBoostable[Any, Tuple[bytes, ByteRange]]:
    assert isinstance(path, CloudPath)

    if size is None:
        size = await getsize(path)

    byte_ranges = itertools.zip_longest(
        range(0, size, config.chunk_size),
        range(config.chunk_size, size, config.chunk_size),
        fillvalue=size,
    )

    async def read_byte_range_wrapper(byte_range: ByteRange) -> Tuple[bytes, ByteRange]:
        chunk = await read_byte_range(path, byte_range)
        return (chunk, byte_range)

    chunks = executor.map_unordered(read_byte_range_wrapper, byte_ranges)
    return chunks


# ==============================
# helpers
# ==============================


def byte_range_to_str(byte_range: OptByteRange) -> Optional[str]:
    # https://docs.microsoft.com/en-us/rest/api/storageservices/specifying-the-range-header-for-blob-service-operations
    # https://cloud.google.com/storage/docs/xml-api/get-object-download
    # oddly range requests are not mentioned in JSON API, only in the XML API
    start, end = byte_range
    if start is not None and end is not None:
        return f"bytes={start}-{end-1}"
    if start is not None:
        return f"bytes={start}-"
    if end is not None:
        if end > 0:
            return f"bytes=0-{end-1}"
        # This form is not supported by Azure
        return f"bytes=-{-int(end)}"
    return None
