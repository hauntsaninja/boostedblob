import contextlib
import functools
import os
import random
import string
import tempfile
import uuid

import blobfile
import pytest

from boostedblob import AzurePath, BasePath, GooglePath, LocalPath, S3Path


def google_test_base() -> GooglePath:
    locations = (
        BasePath.from_str(loc)
        for loc in os.environ.get("BBB_TEST_LOCATIONS", "gs://shantanutest").split(",")
    )
    test_base = next((loc for loc in locations if isinstance(loc, GooglePath)), None)
    if test_base is None:
        pytest.skip("No test location found for GooglePath. Set BBB_TEST_LOCATIONS to include one.")
    assert test_base is not None
    return test_base


def azure_test_base() -> AzurePath:
    locations = (
        BasePath.from_str(loc)
        for loc in os.environ.get("BBB_TEST_LOCATIONS", "az://shantanutest/container").split(",")
    )
    test_base = next((loc for loc in locations if isinstance(loc, AzurePath)), None)
    if test_base is None:
        pytest.skip("No test location found for AzurePath. Set BBB_TEST_LOCATIONS to include one.")
    assert test_base is not None
    return test_base


@contextlib.contextmanager
def tmp_local_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield LocalPath(tmpdir)


@contextlib.contextmanager
def tmp_azure_dir():
    random_id = "".join(random.choice(string.ascii_lowercase) for _ in range(16))
    tmpdir = azure_test_base() / random_id
    try:
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
    tmpdir = google_test_base() / random_id
    try:
        blobfile.makedirs(str(tmpdir))
        yield tmpdir
    finally:
        try:
            blobfile.rmtree(str(tmpdir))
        except NotADirectoryError:
            pass


@functools.cache
def s3_test_client():
    from botocore.session import get_session  # type: ignore[import-untyped]

    return get_session().create_client("s3")


@contextlib.contextmanager
def tmp_s3_dir():
    locations = (
        BasePath.from_str(loc) for loc in os.environ.get("BBB_TEST_LOCATIONS", "").split(",")
    )
    base = next((loc for loc in locations if isinstance(loc, S3Path)), None)
    if base is None:
        pytest.skip("Set BBB_TEST_LOCATIONS to an S3 prefix to run S3 integration tests.")
    path = base / uuid.uuid4().hex
    prefix = path.ensure_directory_like().blob
    client = s3_test_client()
    client.put_object(Bucket=path.bucket, Key=prefix, Body=b"")
    try:
        yield path
    finally:
        pages = client.get_paginator("list_objects_v2").paginate(Bucket=path.bucket, Prefix=prefix)
        for page in pages:
            objects = [{"Key": item["Key"]} for item in page.get("Contents", [])]
            if objects:
                response = client.delete_objects(Bucket=path.bucket, Delete={"Objects": objects})
                assert not response.get("Errors"), response["Errors"]


def tmp_dir(kind):
    if kind == "azure":
        return tmp_azure_dir()
    if kind == "google":
        return tmp_google_dir()
    if kind == "s3":
        return tmp_s3_dir()
    if kind == "local":
        return tmp_local_dir()
    raise AssertionError


@pytest.fixture(params=["azure", "google", "s3", "local"])
def any_dir(request):
    with tmp_dir(request.param) as any_dir:
        yield any_dir


@pytest.fixture(params=["azure", "google", "s3", "local"])
def other_any_dir(request):
    with tmp_dir(request.param) as any_dir:
        yield any_dir


@pytest.fixture(params=["azure", "google", "s3"])
def cloud_dir(request):
    with tmp_dir(request.param) as cloud_dir:
        yield cloud_dir
