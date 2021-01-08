import asyncio
import os
import shutil
import sys
from typing import Any, AsyncIterator, Dict, Optional, TypeVar, Union

from . import azure_auth
from .boost import BoostExecutor, EagerAsyncIterator, consume
from .globals import config
from .listing import DirEntry, glob_scandir, scantree
from .path import (
    AzurePath,
    BasePath,
    CloudPath,
    GooglePath,
    LocalPath,
    exists,
    pathdispatch,
    url_format,
)
from .read import read_single, read_stream, read_stream_unordered
from .request import Request, azurify_request, exponential_sleep_generator, googlify_request
from .write import write_single, write_stream, write_stream_unordered

# ==============================
# copyfile
# ==============================


@pathdispatch
async def copyfile(
    src: Union[BasePath, str],
    dst: Union[BasePath, str],
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
    dst: Union[BasePath, str],
    executor: BoostExecutor,
    overwrite: bool = False,
    size: Optional[int] = None,
) -> None:
    if isinstance(dst, str):
        dst = BasePath.from_str(dst)

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
            await cloudcopyfile(src, dst, overwrite=overwrite)
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
    dst: Union[BasePath, str],
    executor: BoostExecutor,
    overwrite: bool = False,
    size: Optional[int] = None,
) -> None:
    if isinstance(dst, str):
        dst = BasePath.from_str(dst)

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
# cloudcopyfile
# ==============================

CloudPathT = TypeVar("CloudPathT", bound=CloudPath)


@pathdispatch
async def cloudcopyfile(src: CloudPathT, dst: CloudPathT, overwrite: bool = False) -> None:
    """Copy a file named ``src` to a file named ``dst`` within the same cloud.

    Performs a remote copy operation without downloading the contents locally. Blocks until the
    copy completes.

    :param src: The file to copy from.
    :param dst: The file to copy to.
    :param overwrite: If False, raises if the path already exists.

    """
    raise ValueError(f"Unsupported path: {src}")


@cloudcopyfile.register  # type: ignore
async def _azure_cloudcopyfile(src: AzurePath, dst: AzurePath, overwrite: bool = False) -> None:
    assert isinstance(dst, AzurePath)
    if not overwrite:
        if await exists(dst):
            raise FileExistsError(dst)

    if src.account == dst.account:
        copy_source = src.format_url("https://{account}.blob.core.windows.net/{container}/{blob}")
    else:
        copy_source, _ = await azure_auth.generate_signed_url(src)
    request = await azurify_request(
        Request(
            method="PUT",
            url=dst.format_url("https://{account}.blob.core.windows.net/{container}/{blob}"),
            headers={"x-ms-copy-source": copy_source},
            success_codes=(202,),
            failure_exceptions={404: FileNotFoundError(src)},
        )
    )

    async with request.execute() as resp:
        copy_id = resp.headers["x-ms-copy-id"]
        copy_status = resp.headers["x-ms-copy-status"]

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
        request = await azurify_request(
            Request(
                method="GET",
                url=dst.format_url("https://{account}.blob.core.windows.net/{container}/{blob}"),
            )
        )
        async with request.execute() as resp:
            if resp.headers["x-ms-copy-id"] != copy_id:
                raise RuntimeError("Copy id mismatch")
            copy_status = resp.headers["x-ms-copy-status"]
    if copy_status != "success":
        raise RuntimeError(f"Invalid copy status: '{copy_status}'")


@cloudcopyfile.register  # type: ignore
async def _google_cloudcopyfile(src: GooglePath, dst: GooglePath, overwrite: bool = False) -> None:
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
        request = await googlify_request(
            Request(
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
            )
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
    src: BasePath, dst: Union[BasePath, str], executor: BoostExecutor
) -> AsyncIterator[BasePath]:
    """Copies the tree rooted at ``src`` to ``dst``.

    Yields the paths as they are copied.

    :param src: The root of the tree to copy from.
    :param dst: The root of the tree to copy to.
    :param executor: An executor.

    """
    if isinstance(dst, str):
        dst = BasePath.from_str(dst)

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

    async for path in executor.map_unordered(copy_wrapper, EagerAsyncIterator(scantree(src))):
        yield path


@pathdispatch
async def copytree(
    src: Union[BasePath, str], dst: Union[BasePath, str], executor: BoostExecutor
) -> None:
    """Copies the tree rooted at ``src`` to ``dst``.

    :param src: The root of the tree to copy from.
    :param dst: The root of the tree to copy to.
    :param executor: An executor.

    """
    raise ValueError(f"Unsupported path: {src}")


@copytree.register  # type: ignore
async def _cloud_copytree(
    src: CloudPath, dst: Union[BasePath, str], executor: BoostExecutor
) -> None:
    await consume(copytree_iterator(src, dst, executor))


@copytree.register  # type: ignore
async def _local_copytree(
    src: LocalPath, dst: Union[BasePath, str], executor: BoostExecutor
) -> None:
    if isinstance(dst, str):
        dst = BasePath.from_str(dst)

    if isinstance(dst, CloudPath):
        await consume(copytree_iterator(src, dst, executor))
        return
    assert isinstance(dst, LocalPath)
    kwargs = {} if sys.version_info < (3, 8) else {"dirs_exist_ok": True}
    shutil.copytree(src, dst, **kwargs)  # type: ignore[arg-type]


# ==============================
# copyglob
# ==============================


async def copyglob_iterator(
    src: BasePath, dst: Union[BasePath, str], executor: BoostExecutor
) -> AsyncIterator[BasePath]:
    """Copies files matching the glob ``src`` into the directory ``dst``.

    :param src: The glob to match against.
    :param dst: The destination directory.
    :param executor: An executor.

    """
    if isinstance(dst, str):
        dst = BasePath.from_str(dst)
    dst = dst.ensure_directory_like()

    async def copy_wrapper(entry: DirEntry) -> Optional[BasePath]:
        assert isinstance(dst, BasePath)
        if entry.is_dir:
            # Skip directories (or marker files for directories)
            return None
        size = entry.stat.size if entry.stat else None
        await copyfile(entry.path, dst / entry.path.name, executor, size=size, overwrite=True)
        return entry.path

    async for path in executor.map_unordered(copy_wrapper, EagerAsyncIterator(glob_scandir(src))):
        if path:
            yield path
