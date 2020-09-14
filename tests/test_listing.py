import pytest

import boostedblob as bbb

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


# TODO: test scandir
# TODO: test scantree
# make sure to test prefix and directories at bucket and directory level
