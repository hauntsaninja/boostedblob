from __future__ import annotations

import configparser
import datetime
import os
import re
from typing import Any, AsyncIterator, Iterator, Mapping, NamedTuple, Optional, Union

from . import google_auth
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

    def __str__(self) -> str:
        size = self.stat.size if self.stat else ""
        mtime = (
            datetime.datetime.fromtimestamp(int(self.stat.mtime)).isoformat() if self.stat else ""
        )
        return f"{size:12}  {mtime:19}  {self.path}"


# ==============================
# list_blobs
# ==============================


@pathdispatch
def list_blobs(
    path: Union[CloudPath, str], delimiter: Optional[str], allow_prefix: bool = False
) -> AsyncIterator[DirEntry]:
    """List all blobs whose prefix matches ``path``.

    :param path: The prefix we want to list.
    :param delimiter: Group blobs matching the prefix up to a delimiter. This allows us to emulate
        directories on blob storage.
    :param allow_prefix: Whether to allow prefixes that do not end with a delimiter (or "/").

    """
    raise ValueError(f"Unsupported path: {path}")


@list_blobs.register  # type: ignore
async def _azure_list_blobs(
    path: AzurePath, delimiter: Optional[str], allow_prefix: bool = False
) -> AsyncIterator[DirEntry]:
    prefix = path
    if not allow_prefix:
        _delimiter = delimiter or "/"
        assert not prefix.blob or prefix.blob.endswith(_delimiter)
    if not path.container:
        if delimiter != "/":
            raise ValueError("Cannot list blobs in storage account; must specify container")
        async for entry in _azure_list_containers(prefix.account):
            yield entry
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
async def _google_list_blobs(
    path: GooglePath, delimiter: Optional[str], allow_prefix: bool = False
) -> AsyncIterator[DirEntry]:
    prefix = path
    if not allow_prefix:
        _delimiter = delimiter or "/"
        assert not prefix.blob or prefix.blob.endswith(_delimiter)
    if not path.bucket:
        if delimiter != "/":
            raise ValueError("Cannot list blobs across buckets")
        async for entry in _google_list_buckets():
            yield entry
        return

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
        raise NotADirectoryError(path)


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
# globscandir
# ==============================


@pathdispatch
def globscandir(path: Union[BasePath, str]) -> AsyncIterator[DirEntry]:
    """Iterate over a glob in a directory.

    :param path: The directory we want to glob in.

    """
    raise ValueError(f"Unsupported path: {path}")


@globscandir.register  # type: ignore
async def _azure_globscandir(path: AzurePath) -> AsyncIterator[DirEntry]:
    if "*" in path.account or "*" in path.container:
        raise ValueError("Cannot use wildcard in storage account or container")
    if "*" in os.path.dirname(path.blob):
        raise ValueError("Currently only supports wildcards inside the filename")
    if "*" not in path.name:
        path = path / "*"
    pattern = _glob_to_regex(path.name)

    blob_prefix = path.blob.split("*", maxsplit=1)[0]
    prefix = AzurePath(account=path.account, container=path.container, blob=blob_prefix)
    async for entry in list_blobs(prefix, delimiter="/", allow_prefix=True):
        assert isinstance(entry.path, type(path))
        if re.match(pattern, entry.path.name):
            yield entry


@globscandir.register  # type: ignore
async def _google_globscandir(path: GooglePath) -> AsyncIterator[DirEntry]:
    if "*" in path.bucket:
        raise ValueError("Cannot use wildcard in bucket")
    if "*" in os.path.dirname(path.blob):
        raise ValueError("Currently only supports wildcards inside the filename")
    if "*" not in path.name:
        path = path / "*"
    pattern = _glob_to_regex(path.name)

    blob_prefix = path.blob.split("*", maxsplit=1)[0]
    prefix = GooglePath(bucket=path.bucket, blob=blob_prefix)
    async for entry in list_blobs(prefix, delimiter="/", allow_prefix=True):
        assert isinstance(entry.path, type(path))
        if re.match(pattern, entry.path.name):
            yield entry


@globscandir.register  # type: ignore
async def _local_globscandir(path: LocalPath) -> AsyncIterator[DirEntry]:
    if "*" in os.path.dirname(path):
        raise ValueError("Currently only supports wildcards inside the filename")
    if "*" not in path.name:
        path = path / "*"
    pattern = _glob_to_regex(path.name)

    async for entry in scandir(path.parent):
        if re.match(pattern, entry.path.name):
            yield entry


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


async def _azure_list_containers(account: str) -> AsyncIterator[DirEntry]:
    it = azure_page_iterator(
        Request(
            method="GET",
            url=f"https://{account}.blob.core.windows.net/",
            params=dict(comp="list"),
            failure_exceptions={404: FileNotFoundError(AzurePath(account, "", ""))},
        )
    )
    async for result in it:
        result_containers = result["Containers"]
        if result_containers is None:
            raise ValueError(f"No containers found in storage account {account}")
        result_containers = result_containers["Container"]
        containers = (
            [result_containers] if isinstance(result_containers, dict) else result_containers
        )
        assert isinstance(containers, list)
        for container in containers:
            yield DirEntry(
                path=AzurePath(account, container["Name"], ""),
                is_dir=True,
                is_file=False,
                stat=None,
            )


async def _google_list_buckets(project: Optional[str] = None) -> AsyncIterator[DirEntry]:
    if project is None:
        try:
            config = configparser.ConfigParser()
            config.read(
                os.path.join(google_auth.default_gcloud_path(), "configurations/config_default")
            )
            project = config["core"]["project"]
        except (KeyError, FileNotFoundError):
            raise ValueError(
                "Could not determine project in which to list buckets; try setting it "
                "with `gsutil config`"
            )

    it = google_page_iterator(
        Request(
            method="GET",
            url="https://storage.googleapis.com/storage/v1/b",
            params=dict(project=project),
        )
    )
    async for result in it:
        buckets = result["items"]
        assert isinstance(buckets, list)
        for bucket in buckets:
            yield DirEntry(
                path=GooglePath(bucket["name"], ""), is_dir=True, is_file=False, stat=None
            )


def _glob_to_regex(pattern: str) -> str:
    tokens = (token for token in re.split("([*]+)", pattern) if token != "")
    regexp = ""
    for token in tokens:
        if token == "*":
            regexp += r"[^/]*"
        elif token == "**":
            regexp += r".*"
        else:
            regexp += re.escape(token)
    return regexp + r"/?$"
