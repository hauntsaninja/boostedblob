import asyncio

import pytest

import boostedblob as bbb
from boostedblob.path import AzurePath, GooglePath

from . import helpers


@pytest.mark.asyncio
@bbb.ensure_session
async def test_listdir(any_dir):
    assert [p async for p in bbb.listdir(any_dir)] == []

    helpers.create_file(any_dir / "alpha")
    helpers.create_file(any_dir / "bravo")

    assert sorted([p.name async for p in bbb.listdir(any_dir)]) == ["alpha", "bravo"]

    helpers.create_file(any_dir / "charlie" / "delta" / "echo")

    assert sorted([p.name async for p in bbb.listdir(any_dir)]) == ["alpha", "bravo", "charlie"]
    assert [p.name async for p in bbb.listdir(any_dir / "charlie")] == ["delta"]
    assert [p.name async for p in bbb.listdir(any_dir / "charlie" / "delta")] == ["echo"]

    with pytest.raises(FileNotFoundError):
        [p.name async for p in bbb.listdir(any_dir / "foxtrot")]
    with pytest.raises(FileNotFoundError):
        [p.name async for p in bbb.listdir(any_dir / "alp")]
    with pytest.raises(NotADirectoryError):
        [p.name async for p in bbb.listdir(any_dir / "alpha")]


@pytest.mark.asyncio
@bbb.ensure_session
async def test_listtree(any_dir):
    async def _listtree(d):
        return sorted([p.relative_to(any_dir) async for p in bbb.listtree(d)])

    assert await _listtree(any_dir) == []

    helpers.create_file(any_dir / "alpha")
    helpers.create_file(any_dir / "bravo")
    helpers.create_file(any_dir / "charlie" / "delta" / "echo")

    assert await _listtree(any_dir) == ["alpha", "bravo", "charlie/delta/echo"]
    assert [p.name async for p in bbb.listdir(any_dir / "charlie")] == ["delta"]

    with pytest.raises(FileNotFoundError):
        [p.name async for p in bbb.listdir(any_dir / "foxtrot")]
    with pytest.raises(FileNotFoundError):
        [p.name async for p in bbb.listdir(any_dir / "alp")]
    with pytest.raises(NotADirectoryError):
        [p.name async for p in bbb.listdir(any_dir / "alpha")]


@pytest.mark.asyncio
@bbb.ensure_session
async def test_azure_list_containers():
    with helpers.tmp_azure_dir() as az_dir:
        account = AzurePath(az_dir.account, "", "")
        assert az_dir.container in [p.name async for p in bbb.listdir(account)]


@pytest.mark.asyncio
@bbb.ensure_session
async def test_google_list_buckets():
    with helpers.tmp_google_dir() as google_dir:
        bucket = GooglePath("", "")
        assert google_dir.bucket in [p.name async for p in bbb.listdir(bucket)]


@pytest.mark.asyncio
@bbb.ensure_session
async def test_glob_scandir(any_dir):
    assert [p async for p in bbb.listdir(any_dir)] == []

    await asyncio.wait(
        [
            helpers.unsafe_create_file(any_dir / "A.X"),
            helpers.unsafe_create_file(any_dir / "A.Y"),
            helpers.unsafe_create_file(any_dir / "B.X"),
            helpers.unsafe_create_file(any_dir / "B.Y"),
        ]
    )

    async def glob(pattern):
        return sorted([p.path.name async for p in bbb.listing.glob_scandir(pattern)])

    assert await glob(any_dir / "*X") == ["A.X", "B.X"]
    assert await glob(any_dir / "A*") == ["A.X", "A.Y"]
    assert await glob(any_dir / "A*X") == ["A.X"]
    assert await glob(any_dir) == ["A.X", "A.Y", "B.X", "B.Y"]

    if isinstance(any_dir, AzurePath):
        assert any_dir.container in (await glob(AzurePath(any_dir.account, "*", "")))
    if isinstance(any_dir, GooglePath):
        assert any_dir.bucket in (await glob(GooglePath("*", "")))


# TODO: test scandir
# TODO: test scantree
# make sure to test prefix and directories at bucket and directory level
