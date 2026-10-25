from __future__ import annotations

import asyncio
import random

import blobfile

import boostedblob as bbb
from boostedblob import BasePath, S3Path

from .tmp_dir import s3_test_client


def create_file(path: BasePath, contents: bytes = b"asdf") -> None:
    if isinstance(path, S3Path):
        s3_test_client().put_object(Bucket=path.bucket, Key=path.blob, Body=contents)
        return
    with blobfile.BlobFile(str(path), "wb") as f:
        f.write(contents)


def read_file(path: BasePath) -> bytes:
    if isinstance(path, S3Path):
        response = s3_test_client().get_object(Bucket=path.bucket, Key=path.blob)
        with response["Body"] as body:
            return body.read()
    with blobfile.BlobFile(str(path), "rb") as f:
        return f.read()


def unsafe_create_file(path: BasePath, contents: bytes | None = None) -> asyncio.Task[None]:
    if contents is None:
        n = random.randrange(1, 512)
        contents = random.getrandbits(n * 8).to_bytes(n, "little")
    return asyncio.create_task(bbb.write.write_single(path, contents, overwrite=True))
