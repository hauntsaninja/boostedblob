import datetime
import os
from typing import Optional, Tuple, Union

from . import azure_auth, google_auth
from .path import AzurePath, BasePath, BlobPath, GooglePath, LocalPath, pathdispatch


@pathdispatch
async def get_url(path: Union[BasePath, BlobPath, str]) -> Tuple[str, Optional[datetime.datetime]]:
    raise ValueError(f"Unsupported path: {path}")


@get_url.register  # type: ignore
async def _azure_get_url(path: AzurePath) -> Tuple[str, datetime.datetime]:
    return await azure_auth.generate_signed_url(path)


@get_url.register  # type: ignore
async def _google_get_url(path: GooglePath) -> Tuple[str, datetime.datetime]:
    return google_auth.generate_signed_url(path)


@get_url.register  # type: ignore
async def _local_get_url(path: LocalPath) -> Tuple[str, None]:
    return f"file://{os.path.abspath(path)}", None
