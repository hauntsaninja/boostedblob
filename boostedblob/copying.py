import asyncio
import itertools
import os
import shutil
import sys
from typing import Any, AsyncIterator, Dict, Optional, Tuple, TypeVar, Union

from . import azure_auth
from .boost import BoostExecutor, consume
from .globals import config
from .listing import DirEntry, glob_scandir, scantree
from .path import (
    AzurePath,
    BasePath,
    BlobPath,
    CloudPath,
    GooglePath,
    LocalPath,
    exists,
    getsize,
    pathdispatch,
    url_format,
)
from .read import ByteRange, byte_range_to_str, read_single, read_stream, read_stream_unordered
from .request import Request, azure_auth_req, exponential_sleep_generator, google_auth_req
from .write import (
    AZURE_BLOCK_COUNT_LIMIT,
    azure_put_block_list,
    get_block_id,
    get_upload_id,
    prepare_block_blob_write,
    write_single,
    write_stream,
    write_stream_unordered,
)

# ==============================
# copyfile
# ==============================


@pathdispatch
async def copyfile(
    src: Union[BasePath, BlobPath, str],
    dst: Union[BasePath, BlobPath, str],
    executor: BoostExecutor,
    overwrite: bool = False,
    size: Optional[int] = None,
) -> None:
    """Copy a file named ``src` to a file named ``dst``.

    We specialise based on the the types of the paths. For instance, if we're copying within the
    same cloud, we perform a remote copy operation without downloading the contents locally.

    :param src: The file to copy from.
    :param dst: The file to copy to.
    :param executor: An executor.
    :param overwrite: If False, raises if the path already exists.
    :param size: If specified, will save a network call.

    """
    raise ValueError(f"Unsupported path: {src}")


@copyfile.register  # type: ignore
async def _cloudpath_copyfile(
    src: CloudPath,
    dst: Union[BasePath, BlobPath, str],
    executor: BoostExecutor,
    overwrite: bool = False,
    size: Optional[int] = None,
) -> None:
    if isinstance(dst, str):
        dst = BasePath.from_str(dst)
    elif isinstance(dst, BlobPath):
        dst = BasePath.from_str(dst.__blobpath__())

    if isinstance(dst, LocalPath):
        if size is not None and size <= config.chunk_size:
            # skip a network request for small files
            await write_single(dst, await read_single(src), overwrite=overwrite)
            return

        stream = await read_stream(src, executor, size=size)
        await write_stream(dst, stream, executor, overwrite=overwrite)
        return
    if isinstance(dst, CloudPath):
        if type(src) is type(dst):
            await cloud_copyfile(src, dst, executor, overwrite=overwrite, size=size)
            return

        if size is not None and size <= config.chunk_size:
            # skip a network request for small files
            await write_single(dst, await read_single(src), overwrite=overwrite)
            return

        if isinstance(dst, GooglePath):
            # google doesn't support unordered writes
            stream = await read_stream(src, executor, size=size)
            await write_stream(dst, stream, executor, overwrite=overwrite)
            return

        unordered_stream = await read_stream_unordered(src, executor, size=size)
        await write_stream_unordered(dst, unordered_stream, executor, overwrite=overwrite)
        return
    raise ValueError(f"Unsupported path: {dst}")


@copyfile.register  # type: ignore
async def _localpath_copyfile(
    src: LocalPath,
    dst: Union[BasePath, BlobPath, str],
    executor: BoostExecutor,
    overwrite: bool = False,
    size: Optional[int] = None,
) -> None:
    if isinstance(dst, str):
        dst = BasePath.from_str(dst)
    elif isinstance(dst, BlobPath):
        dst = BasePath.from_str(dst.__blobpath__())

    if isinstance(dst, LocalPath):
        if not overwrite:
            if await exists(dst):
                raise FileExistsError(dst)
        os.makedirs(dst.parent, exist_ok=True)
        shutil.copyfile(src, dst)
        return
    if isinstance(dst, CloudPath):
        if size is not None and size <= config.chunk_size:
            # skip a network request for small files
            await write_single(dst, await read_single(src), overwrite=overwrite)
            return

        stream = await read_stream(src, executor, size=size)
        await write_stream(dst, stream, executor, overwrite=overwrite)
        return
    raise ValueError(f"Unsupported path: {dst}")


# ==============================
# cloud_copyfile
# ==============================

CloudPathT = TypeVar("CloudPathT", bound=CloudPath)


@pathdispatch
async def cloud_copyfile(
    src: CloudPathT,
    dst: CloudPathT,
    executor: BoostExecutor,
    overwrite: bool = False,
    size: Optional[int] = None,
) -> None:
    """Copy a file named ``src` to a file named ``dst`` within the same cloud.

    Performs a remote copy operation without downloading the contents locally. Blocks until the
    copy completes.

    :param src: The file to copy from.
    :param dst: The file to copy to.
    :param executor: An executor.
    :param overwrite: If False, raises if the path already exists.
    :param size: If specified, could save a network call.

    """
    raise ValueError(f"Unsupported path: {src}")


async def _azure_put_block_from_url(
    path: AzurePath, block_id: str, copy_source: str, byte_range: ByteRange
) -> None:
    range_str = byte_range_to_str(byte_range)
    assert range_str is not None
    request = Request(
        method="PUT",
        url=path.format_url("https://{account}.blob.core.windows.net/{container}/{blob}"),
        headers={"x-ms-copy-source": copy_source, "x-ms-source-range": range_str},
        params=dict(comp="block", blockid=block_id),
        success_codes=(201,),
        auth=azure_auth_req,
    )
    await request.execute_reponseless()


async def _azure_cloud_copyfile_via_block_urls(
    src: AzurePath,
    dst: AzurePath,
    executor: BoostExecutor,
    overwrite: bool = False,
    size: Optional[int] = None,
) -> None:
    if overwrite:
        await prepare_block_blob_write(dst)
    else:
        if await exists(dst):
            raise FileExistsError(dst)

    copy_source, _ = await azure_auth.generate_signed_url(src)
    if size is None:
        size = await getsize(src)

    # TODO: could consider doing a single Put Block from URL for blobs less than 256MB

    byte_ranges = itertools.zip_longest(
        range(0, size, config.chunk_size),
        range(config.chunk_size, size, config.chunk_size),
        fillvalue=size,
    )

    upload_id = get_upload_id()
    max_block_index = -1

    async def upload_chunk(index_byte_range: Tuple[int, ByteRange]) -> None:
        unordered_index, byte_range = index_byte_range
        # https://docs.microsoft.com/en-us/rest/api/storageservices/put-block-list#remarks
        assert unordered_index < AZURE_BLOCK_COUNT_LIMIT

        nonlocal max_block_index
        max_block_index = max(max_block_index, unordered_index)

        block_id = get_block_id(upload_id, unordered_index)
        await _azure_put_block_from_url(dst, block_id, copy_source, byte_range)

    await consume(executor.map_unordered(upload_chunk, enumerate(byte_ranges)))

    blocklist = [get_block_id(upload_id, i) for i in range(max_block_index + 1)]
    await azure_put_block_list(dst, blocklist)


async def _azure_cloud_copyfile_via_copy(
    src: AzurePath, dst: AzurePath, overwrite: bool = False
) -> None:
    assert isinstance(dst, AzurePath)
    if not overwrite:
        if await exists(dst):
            raise FileExistsError(dst)

    if src.account == dst.account:
        copy_source = src.format_url("https://{account}.blob.core.windows.net/{container}/{blob}")
    else:
        copy_source, _ = await azure_auth.generate_signed_url(src)
    request = Request(
        method="PUT",
        url=dst.format_url("https://{account}.blob.core.windows.net/{container}/{blob}"),
        headers={"x-ms-copy-source": copy_source},
        success_codes=(202,),
        failure_exceptions={404: FileNotFoundError(src)},
        auth=azure_auth_req,
    )

    async with request.execute() as resp:
        copy_id = resp.headers["x-ms-copy-id"]
        copy_status = resp.headers["x-ms-copy-status"]

    try:
        # wait for potentially async copy operation to finish
        # https://docs.microsoft.com/en-us/rest/api/storageservices/get-blob
        # pending, success, aborted, failed
        sleep = exponential_sleep_generator(
            initial=config.backoff_initial,
            maximum=config.backoff_max,
            jitter_fraction=config.backoff_jitter_fraction,
        )
        while copy_status == "pending":
            await asyncio.sleep(next(sleep))
            request = Request(
                method="GET",
                url=dst.format_url("https://{account}.blob.core.windows.net/{container}/{blob}"),
                auth=azure_auth_req,
            )
            async with request.execute() as resp:
                if resp.headers["x-ms-copy-id"] != copy_id:
                    raise RuntimeError("Copy id mismatch")
                copy_status = resp.headers["x-ms-copy-status"]
    except BaseException:
        print(
            "[boostedblob] Asynchronous cloud copy operation is still pending. You can cancel "
            f"the copy manually with `az storage blob copy cancel --copy-id {copy_id} "
            f"--destination-blob {dst.blob} --destination-container {dst.container} "
            f"--account-name {dst.account}`. Note if successfully cancelled, this will produce "
            "an empty blob at the destination.",
            file=sys.stderr,
        )
        raise
    if copy_status != "success":
        raise RuntimeError(f"Invalid copy status: '{copy_status}'")


@cloud_copyfile.register  # type: ignore
async def _azure_cloud_copyfile(
    src: AzurePath,
    dst: AzurePath,
    executor: BoostExecutor,
    overwrite: bool = False,
    size: Optional[int] = None,
) -> None:
    if src.account == dst.account:
        await _azure_cloud_copyfile_via_copy(src, dst, overwrite=overwrite)
    else:
        await _azure_cloud_copyfile_via_block_urls(
            src, dst, executor, overwrite=overwrite, size=size
        )


@cloud_copyfile.register  # type: ignore
async def _google_cloud_copyfile(
    src: GooglePath,
    dst: GooglePath,
    executor: BoostExecutor,
    overwrite: bool = False,
    size: Optional[int] = None,
) -> None:
    assert isinstance(dst, GooglePath)
    if not overwrite:
        if await exists(dst):
            raise FileExistsError(dst)
    params: Dict[str, Any] = {}

    sleep = exponential_sleep_generator(
        initial=config.backoff_initial,
        maximum=config.backoff_max,
        jitter_fraction=config.backoff_jitter_fraction,
    )
    while True:
        request = Request(
            method="POST",
            url=url_format(
                (
                    "https://storage.googleapis.com/storage/v1/b/{src_bucket}/o/"
                    "{src_blob}/rewriteTo/b/{dst_bucket}/o/{dst_blob}"
                ),
                src_bucket=src.bucket,
                src_blob=src.blob,
                dst_bucket=dst.bucket,
                dst_blob=dst.blob,
            ),
            params=params,
            failure_exceptions={404: FileNotFoundError(src)},
            auth=google_auth_req,
        )
        async with request.execute() as resp:
            result = await resp.json()
        if result["done"]:
            return
        params["rewriteToken"] = result["rewriteToken"]
        await asyncio.sleep(next(sleep))


# ==============================
# copytree
# ==============================


async def copytree_iterator(
    src: BasePath, dst: Union[BasePath, BlobPath, str], executor: BoostExecutor
) -> AsyncIterator[BasePath]:
    """Copies the tree rooted at ``src`` to ``dst``.

    Yields the paths as they are copied.

    :param src: The root of the tree to copy from.
    :param dst: The root of the tree to copy to.
    :param executor: An executor.

    """
    if isinstance(dst, str):
        dst = BasePath.from_str(dst)
    elif isinstance(dst, BlobPath):
        dst = BasePath.from_str(dst.__blobpath__())

    if isinstance(src, LocalPath):
        src = LocalPath(os.path.abspath(src.path))

    async def copy_wrapper(entry: DirEntry) -> BasePath:
        assert isinstance(dst, BasePath)
        if entry.is_dir:
            # Filter out directory marker files
            return entry.path
        size = entry.stat.size if entry.stat else None
        await copyfile(
            entry.path, dst / (entry.path.relative_to(src)), executor, size=size, overwrite=True
        )
        return entry.path

    async for path in executor.map_unordered(copy_wrapper, executor.eagerise(scantree(src))):
        yield path


@pathdispatch
async def copytree(
    src: Union[BasePath, BlobPath, str],
    dst: Union[BasePath, BlobPath, str],
    executor: BoostExecutor,
) -> None:
    """Copies the tree rooted at ``src`` to ``dst``.

    :param src: The root of the tree to copy from.
    :param dst: The root of the tree to copy to.
    :param executor: An executor.

    """
    raise ValueError(f"Unsupported path: {src}")


@copytree.register  # type: ignore
async def _cloud_copytree(
    src: CloudPath, dst: Union[BasePath, BlobPath, str], executor: BoostExecutor
) -> None:
    await consume(copytree_iterator(src, dst, executor))


@copytree.register  # type: ignore
async def _local_copytree(
    src: LocalPath, dst: Union[BasePath, BlobPath, str], executor: BoostExecutor
) -> None:
    if isinstance(dst, str):
        dst = BasePath.from_str(dst)

    if isinstance(dst, CloudPath):
        await consume(copytree_iterator(src, dst, executor))
        return
    assert isinstance(dst, LocalPath)
    shutil.copytree(src, dst, dirs_exist_ok=True)


# ==============================
# copyglob
# ==============================


async def copyglob_iterator(
    src: BasePath, dst: Union[BasePath, BlobPath, str], executor: BoostExecutor
) -> AsyncIterator[BasePath]:
    """Copies files matching the glob ``src`` into the directory ``dst``.

    :param src: The glob to match against.
    :param dst: The destination directory.
    :param executor: An executor.

    """
    if isinstance(dst, str):
        dst = BasePath.from_str(dst)
    elif isinstance(dst, BlobPath):
        dst = BasePath.from_str(dst.__blobpath__())

    dst = dst.ensure_directory_like()

    async def copy_wrapper(entry: DirEntry) -> Optional[BasePath]:
        assert isinstance(dst, BasePath)
        if entry.is_dir:
            # Skip directories (or marker files for directories)
            return None
        size = entry.stat.size if entry.stat else None
        await copyfile(entry.path, dst / entry.path.name, executor, size=size, overwrite=True)
        return entry.path

    subpath_exists = False
    async for path in executor.map_unordered(copy_wrapper, executor.eagerise(glob_scandir(src))):
        if path:
            yield path
            subpath_exists = True
    if not subpath_exists:
        raise FileNotFoundError(src)
