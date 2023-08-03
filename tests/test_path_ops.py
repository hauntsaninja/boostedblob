import hashlib
import time

import pytest

import boostedblob as bbb
from boostedblob import AzurePath, BasePath, CloudPath, GooglePath

from . import helpers


def get_base(d: BasePath) -> BasePath:
    while d != d.parent:
        d = d.parent
    return d


@pytest.mark.asyncio
@bbb.ensure_session
async def test_isdir_isfile_exists(any_dir):
    assert (await bbb.isdir(any_dir)) is True
    assert (await bbb.isfile(any_dir)) is False
    assert (await bbb.exists(any_dir)) is True

    assert (await bbb.isdir(any_dir / "alpha")) is False
    assert (await bbb.isfile(any_dir / "alpha")) is False
    assert (await bbb.exists(any_dir / "alpha")) is False

    helpers.create_file(any_dir / "alpha" / "bravo")

    assert (await bbb.isdir(any_dir / "alpha")) is True
    assert (await bbb.isfile(any_dir / "alpgha")) is False
    assert (await bbb.exists(any_dir / "alpha")) is True

    assert (await bbb.isdir(any_dir / "alpha" / "bravo")) is False
    assert (await bbb.isfile(any_dir / "alpha" / "bravo")) is True
    assert (await bbb.exists(any_dir / "alpha" / "bravo")) is True

    # test container / bucket
    assert (await bbb.isdir(get_base(any_dir))) is True
    assert (await bbb.isfile(get_base(any_dir))) is False
    assert (await bbb.exists(get_base(any_dir))) is True

    if isinstance(any_dir, AzurePath):
        assert (await bbb.isdir(AzurePath("does", "not", "exist"))) is False
        assert (await bbb.exists(AzurePath("does", "not", "exist"))) is False
    if isinstance(any_dir, GooglePath):
        assert (await bbb.isdir(GooglePath("also", "missing"))) is False
        assert (await bbb.exists(GooglePath("also", "missing"))) is False


@pytest.mark.asyncio
@bbb.ensure_session
async def test_stat(any_dir):
    if isinstance(any_dir, CloudPath):
        with pytest.raises(FileNotFoundError):
            await bbb.stat(any_dir)

    with pytest.raises(FileNotFoundError):
        await bbb.stat(any_dir / "alpha")

    now = time.time() - 1
    helpers.create_file(any_dir / "alpha", contents=b"this is twenty bytes")

    stat = await bbb.stat(any_dir / "alpha")
    assert stat.size == 20
    assert stat.size == await bbb.getsize(any_dir / "alpha")
    assert stat.mtime >= now
    assert stat.ctime >= now
    if isinstance(any_dir, CloudPath):
        assert stat.md5 == hashlib.md5(b"this is twenty bytes").hexdigest()
    else:
        assert stat.md5 is None

    helpers.create_file(any_dir / "charlie" / "delta" / "echo")
    if isinstance(any_dir, CloudPath):
        with pytest.raises(FileNotFoundError):
            await bbb.stat(any_dir / "charlie")
        with pytest.raises(FileNotFoundError):
            await bbb.stat(any_dir / "charlie" / "delta")

    base = get_base(any_dir)
    if isinstance(any_dir, CloudPath):
        # test container / bucket
        with pytest.raises(FileNotFoundError):
            await bbb.stat(base)


@pytest.mark.asyncio
@bbb.ensure_session
async def test_stat_blobpath(any_dir):
    now = time.time() - 1
    helpers.create_file(any_dir / "alpha", contents=b"this is twenty bytes")

    class BlobPath:
        def __blobpath__(self) -> str:
            return str(any_dir / "alpha")

    stat = await bbb.stat(BlobPath())
    assert stat.size == 20
    assert stat.size == await bbb.getsize(any_dir / "alpha")
    assert stat.mtime >= now
    assert stat.ctime >= now
