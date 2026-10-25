import datetime
import os

from . import _s3, azure_auth, google_auth
from .path import AzurePath, BasePath, BlobPath, GooglePath, LocalPath, S3Path, pathdispatch


@pathdispatch
async def get_url(path: BasePath | BlobPath | str) -> tuple[str, datetime.datetime | None]:
    raise ValueError(f"Unsupported path: {path}")


@get_url.register  # type: ignore
async def _azure_get_url(path: AzurePath) -> tuple[str, datetime.datetime]:
    return await azure_auth.generate_signed_url(path)


@get_url.register  # type: ignore
async def _google_get_url(path: GooglePath) -> tuple[str, datetime.datetime]:
    return google_auth.generate_signed_url(path)


@get_url.register  # type: ignore
async def _s3_get_url(path: S3Path) -> tuple[str, datetime.datetime]:
    return await _s3.generate_signed_url(path)


@get_url.register  # type: ignore
async def _local_get_url(path: LocalPath) -> tuple[str, None]:
    return f"file://{os.path.abspath(path)}", None
