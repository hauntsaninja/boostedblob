import contextlib
import random
import string
import tempfile

import blobfile
import pytest

from boostedblob import AzurePath, GooglePath, LocalPath

GOOGLE_TEST_BASE = GooglePath("shantanutest", "")
AZURE_TEST_BASE = AzurePath("shantanutest", "container", "")


@contextlib.contextmanager
def tmp_local_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield LocalPath(tmpdir)


@contextlib.contextmanager
def tmp_azure_dir():
    random_id = "".join(random.choice(string.ascii_lowercase) for _ in range(16))
    try:
        tmpdir = AZURE_TEST_BASE / random_id
        blobfile.makedirs(str(tmpdir))
        yield tmpdir
    finally:
        try:
            blobfile.rmtree(str(tmpdir))
        except NotADirectoryError:
            pass


@contextlib.contextmanager
def tmp_google_dir():
    random_id = "".join(random.choice(string.ascii_lowercase) for _ in range(16))
    try:
        tmpdir = GOOGLE_TEST_BASE / random_id
        blobfile.makedirs(str(tmpdir))
        yield tmpdir
    finally:
        try:
            blobfile.rmtree(str(tmpdir))
        except NotADirectoryError:
            pass


@pytest.fixture(params=["azure", "google", "local"])
def any_dir(request):
    if request.param == "azure":
        ctx = tmp_azure_dir()
    elif request.param == "google":
        ctx = tmp_google_dir()
    elif request.param == "local":
        ctx = tmp_local_dir()
    else:
        raise AssertionError
    with ctx as any_dir:
        yield any_dir