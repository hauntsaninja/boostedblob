import blobfile

import boostedblob as bbb
from boostedblob import BasePath


def create_file(path: BasePath, contents: bytes = b"asdf") -> None:
    with blobfile.BlobFile(str(path), "wb") as f:
        f.write(contents)


async def unsafe_create_file(path: BasePath, contents: bytes = b"asdf") -> None:
    await bbb.write.write_single(path, contents, overwrite=True)
