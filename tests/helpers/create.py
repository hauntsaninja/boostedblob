import blobfile

from boostedblob.path import BasePath


def create_file(path: BasePath, contents: bytes = b"asdf") -> None:
    with blobfile.BlobFile(str(path), "wb") as f:
        f.write(contents)
