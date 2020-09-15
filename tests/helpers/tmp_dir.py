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


def tmp_dir(kind):
    if kind == "azure":
        return tmp_azure_dir()
    if kind == "google":
        return tmp_google_dir()
    if kind == "local":
        return tmp_local_dir()
    raise AssertionError


@pytest.fixture(params=["azure", "google", "local"])
def any_dir(request):
    with tmp_dir(request.param) as any_dir:
        yield any_dir


@pytest.fixture(params=["azure", "google", "local"])
def other_any_dir(request):
    with tmp_dir(request.param) as any_dir:
        yield any_dir
