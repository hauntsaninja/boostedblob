from __future__ import annotations

import functools
import os
import urllib.parse
from dataclasses import dataclass
from typing import Any, Callable, TypeVar, cast

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
        if url.scheme == "https" and url.netloc.endswith(".blob.core.windows.net"):
            return AzurePath.from_str(path)
        if url.scheme:
            raise ValueError
        return LocalPath(path)

    @property
    def name(self) -> str:
        """Returns the name of path, normalised to exclude any trailing slash."""
        raise NotImplementedError

    @property
    def parent(self: T) -> T:
        raise NotImplementedError

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
            raise ValueError
        return LocalPath(path)

    @property
    def name(self) -> str:
        return os.path.basename(_strip_slash(self.path))

    @property
    def parent(self) -> LocalPath:
        return LocalPath(os.path.dirname(self.path))

    def is_directory_like(self) -> bool:
        return not self.path or self.path.endswith("/")

    def ensure_directory_like(self) -> LocalPath:
        return self if self.is_directory_like() else LocalPath(self.path + "/")

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
        account, host = parsed_url.netloc.split(".", maxsplit=1)
        parts = parsed_url.path.split("/", maxsplit=2)
        if parsed_url.scheme != "https" or host != "blob.core.windows.net" or parts[0]:
            raise ValueError("Invalid URL")

        return AzurePath(account=account, container=parts[1], blob="/".join(parts[2:]))

    @property
    def name(self) -> str:
        return os.path.basename(_strip_slash(self.blob)) if self.blob else self.container

    @property
    def parent(self) -> AzurePath:
        return AzurePath(self.account, self.container, os.path.dirname(self.blob))

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
        return f"https://{self.account}.blob.core.windows.net/{self.container}/{self.blob}"

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
            raise ValueError("Invalid URL")

        return GooglePath(bucket=parsed_url.netloc, blob=parsed_url.path[1:])

    @property
    def name(self) -> str:
        return os.path.basename(_strip_slash(self.blob)) if self.blob else self.bucket

    @property
    def parent(self) -> GooglePath:
        return GooglePath(self.bucket, os.path.dirname(self.blob))

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


def pathdispatch(fn: F) -> F:
    """Implements the singledispatch pattern for paths.

    The extra work this does over singledispatch is that if the path argument is a str, it's
    automatically converted to the correct BasePath object and re-dispatched.

    Typing this function as we've done allows us to exactly type usage, but unfortunately it
    causes type checkers to complain about "no attribute register"

    """
    ret: F = functools.singledispatch(fn)  # type: ignore

    @ret.register  # type: ignore
    def strdispatch(path: str, *args: Any, **kwargs: Any) -> Any:
        return ret(BasePath.from_str(path), *args, **kwargs)

    return ret


# ==============================
# helpers
# ==============================


def url_format(template: str, **data: Any) -> str:
    escaped_data = {k: urllib.parse.quote(v, safe="") for k, v in data.items()}
    return template.format(**escaped_data)


def _strip_slash(path: str) -> str:
    return path[:-1] if path.endswith("/") else path
