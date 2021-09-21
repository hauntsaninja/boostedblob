import asyncio

import blobfile

import boostedblob as bbb
from boostedblob import BasePath


def create_file(path: BasePath, contents: bytes = b"asdf") -> None:
    with blobfile.BlobFile(str(path), "wb") as f:
        f.write(contents)


def unsafe_create_file(path: BasePath, contents: bytes = b"asdf") -> asyncio.Task[None]:
    return asyncio.create_task(bbb.write.write_single(path, contents, overwrite=True))
