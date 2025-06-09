from __future__ import annotations

import asyncio
import random

import blobfile

import boostedblob as bbb
from boostedblob import BasePath


def create_file(path: BasePath, contents: bytes = b"asdf") -> None:
    with blobfile.BlobFile(str(path), "wb") as f:
        f.write(contents)


def unsafe_create_file(path: BasePath, contents: bytes | None = None) -> asyncio.Task[None]:
    if contents is None:
        n = random.randrange(1, 512)
        contents = random.getrandbits(n * 8).to_bytes(n, "little")
    return asyncio.create_task(bbb.write.write_single(path, contents, overwrite=True))
