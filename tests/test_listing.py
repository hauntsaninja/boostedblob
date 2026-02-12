import asyncio
from typing import AsyncIterator

import pytest

import boostedblob as bbb
from boostedblob import listing
from boostedblob.path import AzurePath, GooglePath
from boostedblob.xml import etree

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
async def test_azure_list_containers_empty_first_page_then_container(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Verifies that container listing does not fail on an empty page when NextMarker is present.
    Ensures pagination continues and containers from subsequent pages are returned.
    """

    page1 = etree.fromstring(b"""<?xml version="1.0" encoding="utf-8"?>
<EnumerationResults ServiceEndpoint="https://acct.blob.core.windows.net/">
  <Containers />
  <NextMarker>/acct/$root</NextMarker>
</EnumerationResults>""")
    page2 = etree.fromstring(b"""<?xml version="1.0" encoding="utf-8"?>
<EnumerationResults ServiceEndpoint="https://acct.blob.core.windows.net/">
  <Containers>
    <Container><Name>foo</Name></Container>
  </Containers>
  <NextMarker />
</EnumerationResults>""")

    async def fake_xml_page_iterator(_request):
        yield page1
        yield page2

    monkeypatch.setattr(listing, "xml_page_iterator", fake_xml_page_iterator)

    entries = [p async for p in listing._azure_list_containers("acct")]
    assert [p.path.name for p in entries] == ["foo"]


@pytest.mark.asyncio
@bbb.ensure_session
async def test_azure_list_containers_all_pages_empty_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Verifies that listing raises a clear ValueError when pagination completes without finding any containers.
    Confirms the “no containers found” error is deferred until all pages are exhausted.
    """

    page1 = etree.fromstring(b"""<?xml version="1.0" encoding="utf-8"?>
<EnumerationResults ServiceEndpoint="https://acct.blob.core.windows.net/">
  <Containers />
  <NextMarker>page2</NextMarker>
</EnumerationResults>""")
    page2 = etree.fromstring(b"""<?xml version="1.0" encoding="utf-8"?>
<EnumerationResults ServiceEndpoint="https://acct.blob.core.windows.net/">
  <Containers />
  <NextMarker />
</EnumerationResults>""")

    async def fake_xml_page_iterator(_request):
        yield page1
        yield page2

    monkeypatch.setattr(listing, "xml_page_iterator", fake_xml_page_iterator)

    with pytest.raises(ValueError, match="No containers found in storage account acct"):
        [p async for p in listing._azure_list_containers("acct")]


@pytest.mark.asyncio
@bbb.ensure_session
async def test_azure_list_containers_first_page_has_container(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Verifies the non-paginated success path where the first page already includes containers.
    Ensures those containers are returned without requiring continuation handling.
    """

    page = etree.fromstring(b"""<?xml version="1.0" encoding="utf-8"?>
<EnumerationResults ServiceEndpoint="https://acct.blob.core.windows.net/">
  <Containers>
    <Container><Name>foo</Name></Container>
  </Containers>
  <NextMarker />
</EnumerationResults>""")

    async def fake_xml_page_iterator(_request):
        yield page

    monkeypatch.setattr(listing, "xml_page_iterator", fake_xml_page_iterator)

    entries = [p async for p in listing._azure_list_containers("acct")]
    assert [p.path.name for p in entries] == ["foo"]


@pytest.mark.asyncio
@bbb.ensure_session
async def test_azure_list_containers_multiple_pages(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Verifies that containers are accumulated across multiple paginated responses.
    Ensures entries from both early and continued pages are yielded in order.
    """

    page1 = etree.fromstring(b"""<?xml version="1.0" encoding="utf-8"?>
<EnumerationResults ServiceEndpoint="https://acct.blob.core.windows.net/">
  <Containers>
    <Container><Name>alpha</Name></Container>
  </Containers>
  <NextMarker>page2</NextMarker>
</EnumerationResults>""")
    page2 = etree.fromstring(b"""<?xml version="1.0" encoding="utf-8"?>
<EnumerationResults ServiceEndpoint="https://acct.blob.core.windows.net/">
  <Containers>
    <Container><Name>bravo</Name></Container>
  </Containers>
  <NextMarker />
</EnumerationResults>""")

    async def fake_xml_page_iterator(_request):
        yield page1
        yield page2

    monkeypatch.setattr(listing, "xml_page_iterator", fake_xml_page_iterator)

    entries = [p async for p in listing._azure_list_containers("acct")]
    assert [p.path.name for p in entries] == ["alpha", "bravo"]


@pytest.mark.asyncio
@bbb.ensure_session
async def test_azure_account_level_listdir_uses_container_listing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Verifies that ``bbb.listdir`` on an account-level Azure path delegates to ``_azure_list_containers``.
    Confirms account-level listing returns container names from that code path.
    """

    containers = ["alpha", "bravo"]

    async def fake_list_containers(account: str) -> AsyncIterator[bbb.listing.DirEntry]:
        assert account == "acct"
        for container in containers:
            yield bbb.listing.DirEntry.from_dirpath(AzurePath("acct", container, ""))

    monkeypatch.setattr(listing, "_azure_list_containers", fake_list_containers)

    paths = [p async for p in bbb.listdir(AzurePath("acct", "", ""))]
    assert [p.name for p in paths] == containers


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
