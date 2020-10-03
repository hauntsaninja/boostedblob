from __future__ import annotations

import os
from typing import Any, AsyncIterator, Iterator, Mapping, NamedTuple, Optional, Union

from .path import AzurePath, BasePath, CloudPath, GooglePath, LocalPath, Stat, isfile, pathdispatch
from .request import Request, azure_page_iterator, google_page_iterator

# ==============================
# DirEntry
# ==============================


class DirEntry(NamedTuple):
    path: BasePath
    is_dir: bool
    is_file: bool
    stat: Optional[Stat]

    @staticmethod
    def from_dirpath(path: BasePath) -> DirEntry:
        assert path.is_directory_like()
        return DirEntry(path=path, is_dir=True, is_file=False, stat=None)

    @staticmethod
    def from_path_stat(path: BasePath, stat: Stat) -> DirEntry:
        assert not path.is_directory_like()
        return DirEntry(path=path, is_dir=False, is_file=True, stat=stat)


# ==============================
# list_blobs
# ==============================


@pathdispatch
def list_blobs(path: Union[CloudPath, str], delimiter: Optional[str]) -> AsyncIterator[DirEntry]:
    """List all blobs whose prefix matches ``path``.

    :param path: The prefix we want to list.
    :param delimiter: Group blobs matching the prefix up to a delimiter. This allows us to emulate
        directories on blob storage.

    """
    raise ValueError(f"Unsupported path: {path}")


@list_blobs.register  # type: ignore
async def _azure_list_blobs(path: AzurePath, delimiter: Optional[str]) -> AsyncIterator[DirEntry]:
    prefix = path
    if delimiter:
        assert not prefix.blob or prefix.blob.endswith(delimiter)
    if not path.container and delimiter == "/":
        it = azure_page_iterator(
            Request(
                method="GET",
                url=prefix.format_url("https://{account}.blob.core.windows.net/"),
                params=dict(comp="list"),
                failure_exceptions={404: FileNotFoundError(path)},
            )
        )
        async for result in it:
            for container in result["Containers"]["Container"]:
                yield DirEntry(
                    path=AzurePath(prefix.account, container["Name"], ""),
                    is_dir=True,
                    is_file=False,
                    stat=None,
                )
        return

    params = {}
    if delimiter is not None:
        params["delimiter"] = delimiter
    it = azure_page_iterator(
        Request(
            method="GET",
            url=prefix.format_url("https://{account}.blob.core.windows.net/{container}"),
            params=dict(comp="list", restype="container", prefix=prefix.blob, **params),
        )
    )

    async for result in it:
        for entry in _azure_get_entries(prefix.account, prefix.container, result):
            yield entry


@list_blobs.register  # type: ignore
async def _google_list_blobs(path: GooglePath, delimiter: Optional[str]) -> AsyncIterator[DirEntry]:
    prefix = path
    if delimiter:
        assert not prefix.blob or prefix.blob.endswith(delimiter)
    params = {}
    if delimiter is not None:
        params["delimiter"] = delimiter
    it = google_page_iterator(
        Request(
            method="GET",
            url=prefix.format_url("https://storage.googleapis.com/storage/v1/b/{bucket}/o"),
            params=dict(prefix=prefix.blob, **params),
        )
    )

    async for result in it:
        for entry in _google_get_entries(prefix.bucket, result):
            yield entry


# ==============================
# scandir
# ==============================


@pathdispatch
def scandir(path: Union[BasePath, str]) -> AsyncIterator[DirEntry]:
    """Iterate over the entries in a directory.

    :param path: The directory we want to scan.

    """
    raise ValueError(f"Unsupported path: {path}")


@scandir.register  # type: ignore
async def _cloud_scandir(path: CloudPath) -> AsyncIterator[DirEntry]:
    dirpath = path.ensure_directory_like()
    subpath_exists = False
    async for entry in list_blobs(dirpath, delimiter="/"):
        subpath_exists = True
        if entry.path == dirpath:
            continue
        yield entry

    # If we find nothing, then run some checks so we throw the appropriate error.
    # Doing this means we avoid extra requests in the happy path.
    if not subpath_exists:
        if not await isfile(path):
            raise FileNotFoundError(path)
        raise NotADirectoryError(path)


@scandir.register  # type: ignore
async def _local_scandir(path: LocalPath) -> AsyncIterator[DirEntry]:
    path_absolute = os.path.abspath(path)
    for entry in os.scandir(path_absolute):
        # entry.path is absolute since we're passing an absolute path to os.scandir
        entry_path = LocalPath(entry.path)
        if entry.is_dir():
            yield DirEntry(path=entry_path, is_dir=True, is_file=False, stat=None)
        else:
            yield DirEntry(
                path=entry_path, is_dir=False, is_file=True, stat=Stat.from_local(entry.stat())
            )


# ==============================
# listdir
# ==============================


@pathdispatch
def listdir(path: Union[BasePath, str]) -> AsyncIterator[BasePath]:
    """Iterate over the names of entries in a directory.

    :param path: The directory we want to scan.

    """
    raise ValueError(f"Unsupported path: {path}")


@listdir.register  # type: ignore
async def _cloud_listdir(path: CloudPath) -> AsyncIterator[CloudPath]:
    async for entry in scandir(path):
        assert isinstance(entry.path, type(path))
        yield entry.path


@listdir.register  # type: ignore
async def _local_listdir(path: LocalPath) -> AsyncIterator[LocalPath]:
    for p in os.listdir(path):
        yield LocalPath(p)


# ==============================
# scantree
# ==============================


@pathdispatch
def scantree(path: Union[BasePath, str]) -> AsyncIterator[DirEntry]:
    """Iterate over file entries in the directory tree rooted at path.

    :param path: The root of the tree we want to scan.

    """
    raise ValueError(f"Unsupported path: {path}")


@scantree.register  # type: ignore
async def _cloud_scantree(path: CloudPath) -> AsyncIterator[DirEntry]:
    dirpath = path.ensure_directory_like()
    subpath_exists = False
    async for entry in list_blobs(dirpath, None):
        subpath_exists = True
        if entry.path == dirpath:
            continue
        yield entry

    # If we find nothing, then run some checks so we throw the appropriate error.
    # Doing this means we avoid extra requests in the happy path.
    if not subpath_exists:
        if not await isfile(path):
            raise FileNotFoundError(path)


@scantree.register  # type: ignore
async def _local_scantree(path: LocalPath) -> AsyncIterator[DirEntry]:
    path_absolute = os.path.abspath(path)

    def inner(current: str) -> Iterator[DirEntry]:
        entries = list(os.scandir(current))
        for entry in entries:
            if entry.is_dir():
                yield from inner(entry.path)
            else:
                yield DirEntry(
                    path=LocalPath(entry.path),
                    is_dir=False,
                    is_file=True,
                    stat=Stat.from_local(entry.stat()),
                )

    for entry in inner(path_absolute):
        yield entry


# ==============================
# listtree
# ==============================


@pathdispatch
def listtree(path: Union[BasePath, str]) -> AsyncIterator[BasePath]:
    """List files in the directory tree rooted at path.

    :param path: The directory we want to scan.

    """
    raise ValueError(f"Unsupported path: {path}")


@listtree.register  # type: ignore
async def _cloud_listtree(path: CloudPath) -> AsyncIterator[CloudPath]:
    async for entry in scantree(path):
        assert isinstance(entry.path, type(path))
        yield entry.path


@listtree.register  # type: ignore
async def _local_listtree(path: LocalPath) -> AsyncIterator[LocalPath]:
    for base, _subdirs, files in os.walk(path):
        for name in files:
            yield LocalPath(base) / name


# ==============================
# helpers
# ==============================


def _azure_get_entries(
    account: str, container: str, result: Mapping[str, Any]
) -> Iterator[DirEntry]:
    blobs = result["Blobs"]
    if blobs is None:
        return
    if "BlobPrefix" in blobs:
        if isinstance(blobs["BlobPrefix"], dict):
            blobs["BlobPrefix"] = [blobs["BlobPrefix"]]
        for bp in blobs["BlobPrefix"]:
            blob = bp["Name"]
            path = AzurePath(account, container, blob)
            yield DirEntry.from_dirpath(path)
    if "Blob" in blobs:
        if isinstance(blobs["Blob"], dict):
            blobs["Blob"] = [blobs["Blob"]]
        for b in blobs["Blob"]:
            blob = b["Name"]
            path = AzurePath(account, container, blob)
            if b["Name"].endswith("/"):
                yield DirEntry.from_dirpath(path)
            else:
                props = b["Properties"]
                yield DirEntry.from_path_stat(path, Stat.from_azure(props))


def _google_get_entries(bucket: str, result: Mapping[str, Any]) -> Iterator[DirEntry]:
    if "prefixes" in result:
        for blob in result["prefixes"]:
            path = GooglePath(bucket, blob)
            yield DirEntry.from_dirpath(path)
    if "items" in result:
        for item in result["items"]:
            blob = item["name"]
            path = GooglePath(bucket, blob)
            if item["name"].endswith("/"):
                yield DirEntry.from_dirpath(path)
            else:
                yield DirEntry.from_path_stat(path, Stat.from_google(item))
