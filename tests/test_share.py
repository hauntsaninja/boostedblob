import urllib.request

import pytest

import boostedblob as bbb

from . import helpers


@pytest.mark.asyncio
@bbb.ensure_session
async def test_get_url(any_dir):
    if isinstance(any_dir, bbb.GooglePath):
        pytest.skip("Needs authorisation not present on my dev machine")
    helpers.create_file(any_dir / "here is a file", b"with some stuff")
    url = (await bbb.share.get_url(any_dir / "here is a file"))[0]
    with urllib.request.urlopen(url) as f:
        assert f.read() == b"with some stuff"
