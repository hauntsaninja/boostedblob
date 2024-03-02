from __future__ import annotations

import asyncio
import base64
import datetime
import email.utils
import functools
import os
import urllib.parse
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Optional, Protocol, TypeVar, Union, runtime_checkable

from .request import Request, azure_auth_req, google_auth_req, xml_page_iterator

T = TypeVar("T")

# ==============================
# Path classes
# ==============================


class BasePath:
    @staticmethod
    def from_str(path: str) -> BasePath:
        url = urllib.parse.urlparse(path)
        if url.scheme == "gs":
            return GooglePath.from_str(path)
        if url.scheme == "az" or (
            url.scheme == "https" and url.netloc.endswith(".blob.core.windows.net")
        ):
            return AzurePath.from_str(path)
        if url.scheme:
            raise ValueError(f"Invalid path '{path}'")
        return LocalPath(path)

    @property
    def name(self) -> str:
        """Returns the name of path, normalised to exclude any trailing slash."""
        raise NotImplementedError

    @property
    def parent(self: T) -> T:
        raise NotImplementedError

    def relative_to(self: T, other: T) -> str:
        raise NotImplementedError

    def is_relative_to(self: T, other: T) -> bool:
        try:
            self.relative_to(other)  # type: ignore[attr-defined]
            return True
        except ValueError:
            return False

    def is_directory_like(self) -> bool:
        raise NotImplementedError

    def ensure_directory_like(self: T) -> T:
        raise NotImplementedError

    def __truediv__(self: T, relative_path: str) -> T:
        raise NotImplementedError


@dataclass(frozen=True)
class LocalPath(BasePath):
    path: str

    @staticmethod
    def from_str(path: str) -> BasePath:
        url = urllib.parse.urlparse(path)
        if url.scheme:
            raise ValueError(f"Invalid path '{path}'")
        return LocalPath(path)

    @property
    def name(self) -> str:
        return os.path.basename(_strip_slash(self.path))

    @property
    def parent(self) -> LocalPath:
        parent = os.path.dirname(_strip_slash(self.path))
        if not parent:
            parent = "/" if os.path.isabs(self.path) else "."
        return LocalPath(parent)

    def relative_to(self, other: LocalPath) -> str:
        if not isinstance(other, LocalPath):
            raise ValueError(f"'{other}' is not a subpath of '{self}'")
        other = other.ensure_directory_like()
        other_path = other.path
        if other_path.startswith("./") and not self.path.startswith("./"):
            other_path = other_path[len("./") :]
        if not self.path.startswith(other_path):
            raise ValueError(f"'{other}' is not a subpath of '{self}'")
        return self.path[len(other_path) :]

    def is_directory_like(self) -> bool:
        return not self.path or self.path.endswith("/")

    def ensure_directory_like(self) -> LocalPath:
        return self if self.is_directory_like() else LocalPath(self.path + "/")

    def abspath(self) -> LocalPath:
        return LocalPath(os.path.abspath(self.path))

    def __truediv__(self, relative_path: str) -> LocalPath:
        return LocalPath(os.path.join(self.path, relative_path))

    def __str__(self) -> str:
        return self.path

    def __fspath__(self) -> str:
        return self.path


class CloudPath(BasePath):
    pass


@dataclass(frozen=True)
class AzurePath(CloudPath):
    account: str
    container: str
    blob: str

    @staticmethod
    def from_str(url: str) -> AzurePath:
        parsed_url = urllib.parse.urlparse(url)
        if parsed_url.scheme == "https":
            account, host = parsed_url.netloc.split(".", maxsplit=1)
            if host != "blob.core.windows.net":
                raise ValueError(f"Invalid URL '{url}'; unexpected host '{host}'")
        elif parsed_url.scheme == "az":
            account = parsed_url.netloc
        else:
            raise ValueError(f"Invalid URL '{url}'; expected 'https' or 'az' scheme")

        if not account:
            raise ValueError(f"Invalid URL '{url}'; expected account name")

        # split the unparsed URL
        parts = url.split("/", maxsplit=4)
        container = parts[3] if len(parts) >= 4 else ""
        blob = parts[4] if len(parts) >= 5 else ""
        return AzurePath(account=account, container=container, blob=blob)

    @property
    def name(self) -> str:
        return os.path.basename(_strip_slash(self.blob)) if self.blob else self.container

    @property
    def parent(self) -> AzurePath:
        return AzurePath(self.account, self.container, os.path.dirname(_strip_slash(self.blob)))

    def relative_to(self, other: AzurePath) -> str:
        if not isinstance(other, AzurePath):
            raise ValueError(f"'{other}' is not a subpath of '{self}'")
        other = other.ensure_directory_like()
        if (
            self.account != other.account
            or self.container != other.container
            or not self.blob.startswith(other.blob)
        ):
            raise ValueError(f"'{other}' is not a subpath of '{self}'")
        return self.blob[len(other.blob) :]

    def is_directory_like(self) -> bool:
        return not self.blob or self.blob.endswith("/")

    def ensure_directory_like(self) -> AzurePath:
        return (
            self
            if self.is_directory_like()
            else AzurePath(self.account, self.container, self.blob + "/")
        )

    def __truediv__(self, relative_path: str) -> AzurePath:
        return AzurePath(self.account, self.container, os.path.join(self.blob, relative_path))

    def __str__(self) -> str:
        return self.to_az_str()

    def to_https_str(self) -> str:
        return f"https://{self.account}.blob.core.windows.net/{self.container}/{self.blob}"

    def to_az_str(self) -> str:
        return f"az://{self.account}/{self.container}/{self.blob}"

    def format_url(self, template: str) -> str:
        return url_format(template, account=self.account, container=self.container, blob=self.blob)


@dataclass(frozen=True)
class GooglePath(CloudPath):
    bucket: str
    blob: str

    @staticmethod
    def from_str(url: str) -> GooglePath:
        parsed_url = urllib.parse.urlparse(url)
        if parsed_url.scheme != "gs":
            raise ValueError(f"Invalid URL '{url}'; expected 'gs' scheme")

        # split the unparsed URL
        parts = url.split("/", maxsplit=3)
        blob = parts[3] if len(parts) >= 4 else ""
        return GooglePath(bucket=parsed_url.netloc, blob=blob)

    @property
    def name(self) -> str:
        return os.path.basename(_strip_slash(self.blob)) if self.blob else self.bucket

    @property
    def parent(self) -> GooglePath:
        return GooglePath(self.bucket, os.path.dirname(_strip_slash(self.blob)))

    def relative_to(self, other: GooglePath) -> str:
        if not isinstance(other, GooglePath):
            raise ValueError(f"'{other}' is not a subpath of '{self}'")
        other = other.ensure_directory_like()
        if self.bucket != other.bucket or not self.blob.startswith(other.blob):
            raise ValueError(f"'{other}' is not a subpath of '{self}'")
        return self.blob[len(other.blob) :]

    def is_directory_like(self) -> bool:
        return not self.blob or self.blob.endswith("/")

    def ensure_directory_like(self) -> GooglePath:
        return self if self.is_directory_like() else GooglePath(self.bucket, self.blob + "/")

    def __truediv__(self, relative_path: str) -> GooglePath:
        return GooglePath(self.bucket, os.path.join(self.blob, relative_path))

    def __str__(self) -> str:
        return f"gs://{self.bucket}/{self.blob}"

    def format_url(self, template: str) -> str:
        return url_format(template, bucket=self.bucket, blob=self.blob)


# ==============================
# pathdispatch
# ==============================

F = TypeVar("F", bound=Callable[..., Any])


@runtime_checkable
class BlobPath(Protocol):
    def __blobpath__(self) -> str: ...


def pathdispatch(fn: F) -> F:
    """Implements the singledispatch pattern for paths.

    The extra work this does over singledispatch is that if the path argument is a str, it's
    automatically converted to the correct BasePath object and re-dispatched.

    Typing this function as we've done allows us to exactly type usage, but unfortunately it
    causes type checkers to complain about "no attribute register"

    """
    ret: F = functools.singledispatch(fn)  # type: ignore[assignment]

    @ret.register  # type: ignore
    def strdispatch(path: str, *args: Any, **kwargs: Any) -> Any:
        return ret(BasePath.from_str(path), *args, **kwargs)

    @ret.register  # type: ignore
    def blobpathdispatch(path: BlobPath, *args: Any, **kwargs: Any) -> Any:
        return ret(BasePath.from_str(path.__blobpath__()), *args, **kwargs)

    return ret


# ==============================
# stat
# ==============================


class Stat:
    size: int
    mtime: float
    ctime: float
    md5: Optional[str]
    version: Optional[str]


class AzureStat(Stat):
    __slots__ = (
        "size",
        "_mtime",
        "_raw_mtime",
        "_ctime",
        "_raw_ctime",
        "_md5",
        "_raw_md5",
        "version",
    )

    def __init__(self, item: Mapping[str, Any]) -> None:
        self.size = int(item["Content-Length"])
        self.version = item["Etag"]
        self._raw_ctime = (
            item["Creation-Time"] if "Creation-Time" in item else item["x-ms-creation-time"]
        )
        self._ctime: Optional[float] = None
        self._raw_mtime = item["Last-Modified"]
        self._mtime: Optional[float] = None
        self._raw_md5 = item.get("Content-MD5")
        self._md5: Optional[str] = ""

    @property
    def ctime(self) -> float:  # type: ignore[override]
        if self._ctime is None:
            self._ctime = _azure_parse_timestamp(self._raw_ctime)
        return self._ctime

    @property
    def mtime(self) -> float:  # type: ignore[override]
        if self._mtime is None:
            self._mtime = _azure_parse_timestamp(self._raw_mtime)
        return self._mtime

    @property
    def md5(self) -> Optional[str]:  # type: ignore[override]
        if self._md5 == "":
            self._md5 = _azure_get_md5(self._raw_md5)
        return self._md5

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Stat):
            return False
        return (
            self.size == other.size
            and self.mtime == other.mtime
            and self.ctime == other.ctime
            and self.md5 == other.md5
            and self.version == other.version
        )


class GoogleStat(Stat):
    def __init__(self, item: Mapping[str, Any]) -> None:
        self.size = int(item["size"])
        self.mtime = _google_parse_timestamp(item["updated"])
        self.ctime = _google_parse_timestamp(item["timeCreated"])
        self.md5 = _google_get_md5(item)
        self.version = item["generation"]

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Stat):
            return False
        return (
            self.size == other.size
            and self.mtime == other.mtime
            and self.ctime == other.ctime
            and self.md5 == other.md5
            and self.version == other.version
        )


class LocalStat(Stat):
    def __init__(self, item: os.stat_result) -> None:
        self.size = item.st_size
        self.mtime = item.st_mtime
        self.ctime = item.st_ctime
        self.md5 = None
        self.version = None

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Stat):
            return False
        return (
            self.size == other.size
            and self.mtime == other.mtime
            and self.ctime == other.ctime
            and self.md5 == other.md5
            and self.version == other.version
        )


@pathdispatch
async def stat(path: Union[BasePath, BlobPath, str]) -> Stat:
    raise ValueError(f"Unsupported path: {path}")


@stat.register  # type: ignore
async def _azure_stat(path: AzurePath) -> AzureStat:
    if not path.blob:
        raise FileNotFoundError(path)
    request = Request(
        method="HEAD",
        url=path.format_url("https://{account}.blob.core.windows.net/{container}/{blob}"),
        failure_exceptions={404: FileNotFoundError(path)},
        auth=azure_auth_req,
    )
    async with request.execute() as resp:
        return AzureStat(resp.headers)


@stat.register  # type: ignore
async def _google_stat(path: GooglePath) -> GoogleStat:
    if not path.blob:
        raise FileNotFoundError(path)
    request = Request(
        method="GET",
        url=path.format_url("https://storage.googleapis.com/storage/v1/b/{bucket}/o/{blob}"),
        failure_exceptions={404: FileNotFoundError(path)},
        auth=google_auth_req,
    )
    async with request.execute() as resp:
        result = await resp.json()
        return GoogleStat(result)


@stat.register  # type: ignore
async def _local_stat(path: LocalPath) -> LocalStat:
    return LocalStat(os.stat(path))


# ==============================
# getsize
# ==============================


@pathdispatch
async def getsize(path: Union[BasePath, BlobPath, str]) -> int:
    s = await stat(path)
    return s.size


# ==============================
# isdir
# ==============================


@pathdispatch
async def isdir(path: Union[BasePath, BlobPath, str], raise_on_missing: bool = False) -> bool:
    """Check whether ``path`` is a directory.

    :param path: The path that could be a directory.
    :param raise_on_missing: If True, raise FileNotFoundError if ``path`` does not exist. Otherwise,
        return False.

    """
    raise ValueError(f"Unsupported path: {path}")


@isdir.register  # type: ignore
async def _azure_isdir(path: AzurePath) -> bool:
    try:
        if path.blob:
            prefix: str = path.blob
            if not prefix.endswith("/"):
                prefix += "/"

            it = xml_page_iterator(
                Request(
                    method="GET",
                    url=path.format_url("https://{account}.blob.core.windows.net/{container}"),
                    params=dict(
                        comp="list",
                        restype="container",
                        prefix=prefix,
                        delimiter="/",
                        maxresults="1",
                    ),
                    failure_exceptions={404: FileNotFoundError()},
                    auth=azure_auth_req,
                )
            )
            async for result in it:
                blobs = result.find("Blobs")
                assert blobs is not None
                if len(blobs):
                    return blobs.find("Blob") is not None or blobs.find("BlobPrefix") is not None
            return False
        else:
            request = Request(
                method="GET",
                url=path.format_url("https://{account}.blob.core.windows.net/{container}"),
                params=dict(restype="container"),
                failure_exceptions={404: FileNotFoundError()},
                auth=azure_auth_req,
            )
            await request.execute_reponseless()
            return True
    except FileNotFoundError:
        # execute might raise FileNotFoundError if storage account doesn't exist
        return False


@isdir.register  # type: ignore
async def _google_isdir(path: GooglePath) -> bool:
    try:
        if path.blob:
            prefix: str = path.blob
            if not prefix.endswith("/"):
                prefix += "/"
            request = Request(
                method="GET",
                url=path.format_url("https://storage.googleapis.com/storage/v1/b/{bucket}/o"),
                params=dict(prefix=prefix, delimiter="/", maxResults="1"),
                failure_exceptions={404: FileNotFoundError()},
                auth=google_auth_req,
            )
            async with request.execute() as resp:
                result = await resp.json()
                return "items" in result or "prefixes" in result
        else:
            request = Request(
                method="GET",
                url=path.format_url("https://storage.googleapis.com/storage/v1/b/{bucket}"),
                failure_exceptions={404: FileNotFoundError()},
                auth=google_auth_req,
            )
            async with request.execute() as resp:
                return True
    except FileNotFoundError:
        return False


@isdir.register  # type: ignore
async def _local_isdir(path: LocalPath) -> bool:
    return os.path.isdir(path)


# ==============================
# isfile
# ==============================


@pathdispatch
async def isfile(path: Union[BasePath, BlobPath, str]) -> bool:
    raise ValueError(f"Unsupported path: {path}")


@isfile.register  # type: ignore
async def _cloud_isfile(path: CloudPath) -> bool:
    try:
        await stat(path)
        return True
    except FileNotFoundError:
        return False


@isfile.register  # type: ignore
async def _local_isfile(path: LocalPath) -> bool:
    return os.path.isfile(path)


# ==============================
# exists
# ==============================


@pathdispatch
async def exists(path: Union[BasePath, BlobPath, str]) -> int:
    raise ValueError(f"Unsupported path: {path}")


@exists.register  # type: ignore
async def _cloud_exists(path: CloudPath) -> int:
    # TODO: this is two network requests, make it one
    for fut in asyncio.as_completed([isfile(path), isdir(path)]):
        if await fut:
            return True
    return False


@exists.register  # type: ignore
async def _local_exists(path: LocalPath) -> int:
    return os.path.exists(path)


# ==============================
# helpers
# ==============================


def url_format(template: str, **data: Any) -> str:
    escaped_data = {k: urllib.parse.quote(v, safe="") for k, v in data.items()}
    return template.format(**escaped_data)


def _strip_slash(path: str) -> str:
    return path[:-1] if path.endswith("/") else path


def _azure_parse_timestamp(text: str) -> float:
    # A little more than 2x faster than:
    # strptime(text.replace("GMT", "Z"), "%a, %d %b %Y %H:%M:%S %z").timestamp()
    return email.utils.parsedate_to_datetime(text).timestamp()


def _azure_get_md5(content_md5: Optional[str]) -> Optional[str]:
    if content_md5 is None:
        return None
    return base64.b64decode(content_md5).hex()


def _google_parse_timestamp(text: str) -> float:
    return datetime.datetime.strptime(text, "%Y-%m-%dT%H:%M:%S.%f%z").timestamp()


def _google_get_md5(metadata: Mapping[str, Any]) -> Optional[str]:
    if "md5Hash" in metadata:
        return base64.b64decode(metadata["md5Hash"]).hex()

    if "metadata" in metadata and "md5" in metadata["metadata"]:
        # fallback to our custom hash if this is a composite object that is lacking the
        # md5Hash field
        return metadata["metadata"]["md5"]

    return None
