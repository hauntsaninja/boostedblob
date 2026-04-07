from typing import Any

import pytest

from boostedblob.path import AzurePath, BasePath, GooglePath, LocalPath

PATHS: dict[type[BasePath], Any] = {
    AzurePath: AzurePath("shantanutest", "container", "blob"),
    GooglePath: GooglePath("shantanutest", "blob"),
    LocalPath: LocalPath("blob"),
}


def test_path_str():
    assert str(PATHS[AzurePath]) == "az://shantanutest/container/blob"
    assert str(PATHS[GooglePath]) == "gs://shantanutest/blob"
    assert str(PATHS[LocalPath]) == "blob"


def test_path_from_str():
    for path in PATHS.values():
        assert BasePath.from_str(str(path)) == path
        assert BasePath.from_str(str(BasePath.from_str(str(path)))) == path

    with pytest.raises(ValueError):
        BasePath.from_str("s3://bucket/blob")

    for path_cls in PATHS.keys():
        for path in PATHS.values():
            if isinstance(path, path_cls):
                assert path_cls.from_str(str(path)) == path
            else:
                with pytest.raises(ValueError):
                    path_cls.from_str(str(path))

    with pytest.raises(ValueError, match="expected account name"):
        AzurePath.from_str("az://")
    assert AzurePath.from_str("az://a") == AzurePath("a", "", "")
    assert AzurePath.from_str("az://a/") == AzurePath("a", "", "")
    assert AzurePath.from_str("az://a/b") == AzurePath("a", "b", "")
    assert AzurePath.from_str("az://a/b/") == AzurePath("a", "b", "")
    assert AzurePath.from_str("az://a/b/c") == AzurePath("a", "b", "c")
    assert AzurePath.from_str("az://a/b/c/") == AzurePath("a", "b", "c/")
    assert AzurePath.from_str("az://a/b/c/d") == AzurePath("a", "b", "c/d")
    assert AzurePath.from_str("az://a/c/evilblob;").blob == "evilblob;"

    with pytest.raises(ValueError):
        AzurePath.from_str("https://")
    with pytest.raises(ValueError):
        AzurePath.from_str("https://a/b/c")
    assert AzurePath.from_str("https://a.blob.core.windows.net") == AzurePath("a", "", "")
    assert AzurePath.from_str("https://a.blob.core.windows.net/") == AzurePath("a", "", "")
    assert AzurePath.from_str("https://a.blob.core.windows.net/b") == AzurePath("a", "b", "")
    assert AzurePath.from_str("https://a.blob.core.windows.net/b/") == AzurePath("a", "b", "")
    assert AzurePath.from_str("https://a.blob.core.windows.net/b/c") == AzurePath("a", "b", "c")
    assert AzurePath.from_str("https://a.blob.core.windows.net/b/c/") == AzurePath("a", "b", "c/")
    assert AzurePath.from_str("https://a.blob.core.windows.net/b/c/d") == AzurePath("a", "b", "c/d")
    assert AzurePath.from_str("https://a.blob.core.windows.net/c/evilblob;").blob == "evilblob;"

    assert GooglePath.from_str("gs://") == GooglePath("", "")
    assert GooglePath.from_str("gs://a") == GooglePath("a", "")
    assert GooglePath.from_str("gs://a/") == GooglePath("a", "")
    assert GooglePath.from_str("gs://a/b") == GooglePath("a", "b")
    assert GooglePath.from_str("gs://a/b/") == GooglePath("a", "b/")
    assert GooglePath.from_str("gs://a/b/c") == GooglePath("a", "b/c")
    assert GooglePath.from_str("gs://a/evilblob;").blob == "evilblob;"

    assert BasePath.from_str("az://shantanutest/container/blob") == PATHS[AzurePath]
    assert BasePath.from_str(PATHS[AzurePath].to_az_str()) == PATHS[AzurePath]
    assert BasePath.from_str(PATHS[AzurePath].to_https_str()) == PATHS[AzurePath]


def test_path_methods():
    for path in PATHS.values():
        assert path.name == "blob"

        subpath = path / "subblob"
        assert subpath.name == "subblob"
        assert subpath.parent.name == "blob"

        subsubpath = path / "subblob/subsubsub"
        assert subsubpath.name == "subsubsub"
        assert subsubpath.parent.name == "subblob"
        assert subsubpath.parent.parent.name == "blob"

        assert path.ensure_directory_like().parent == path.parent

        assert subsubpath.relative_to(path) == "subblob/subsubsub"
        assert subsubpath.relative_to(path.parent) == "blob/subblob/subsubsub"
        assert subpath.relative_to(path) == "subblob"
        assert subpath.relative_to(path.parent) == "blob/subblob"

        assert subsubpath.is_relative_to(path)
        assert not path.is_relative_to(subsubpath)

        for other_path in PATHS.values():
            if not isinstance(other_path, type(path)):
                with pytest.raises(ValueError):
                    subsubpath.relative_to(other_path)
                assert not subsubpath.is_relative_to(other_path)


def test_azure_path_concat():
    path = AzurePath.from_str("az://a") / "container"
    assert path.container == "container"
    assert str(path) == "az://a/container/"

    path = AzurePath.from_str("az://a") / "container/blob"
    assert path.container == "container"
    assert path.blob == "blob"
    assert str(path) == "az://a/container/blob"

    path = AzurePath.from_str("az://a/container") / "blob"
    assert path.container == "container"
    assert path.blob == "blob"
    assert str(path) == "az://a/container/blob"

    path = AzurePath.from_str("az://a/container/blob") / ""
    assert path.container == "container"
    assert path.blob == "blob/"
    assert str(path) == "az://a/container/blob/"


def test_local_path_parent():
    assert LocalPath("asdf").parent == LocalPath(".")
    assert LocalPath(".").parent == LocalPath(".")
    assert LocalPath("/").parent == LocalPath("/")


def test_path_directory_like():
    for path in PATHS.values():
        assert not path.is_directory_like()
        assert str(path.ensure_directory_like()).endswith("/")
        assert path.ensure_directory_like().is_directory_like()
        assert not str(path.ensure_directory_like().ensure_directory_like()).endswith("//")
        # name always strips slash
        assert not path.ensure_directory_like().name.endswith("/")

    # containers and buckets are already directory like
    assert PATHS[AzurePath].parent.blob == ""
    assert PATHS[AzurePath].parent.is_directory_like()
    assert PATHS[AzurePath].parent.ensure_directory_like().blob == ""

    assert PATHS[GooglePath].parent.blob == ""
    assert PATHS[GooglePath].parent.is_directory_like()
    assert PATHS[GooglePath].parent.ensure_directory_like().blob == ""


def test_azure_path_multi_cloud():
    """Test that AzurePath URL construction and parsing respects the active cloud config."""
    from boostedblob.azure_cloud import AZURE_PUBLIC_CLOUD, AZURE_US_GOV_CLOUD
    from boostedblob.globals import configure
    import urllib.parse

    path = AzurePath("myacct", "mycontainer", "myblob")

    # Default (public cloud)
    assert path.to_https_str() == "https://myacct.blob.core.windows.net/mycontainer/myblob"
    assert path.blob_url() == "https://myacct.blob.core.windows.net/mycontainer/myblob"
    assert path.container_url() == "https://myacct.blob.core.windows.net/mycontainer"
    assert path.account_url() == "https://myacct.blob.core.windows.net"

    # US Government cloud
    with configure(azure_cloud=AZURE_US_GOV_CLOUD):
        assert (
            path.to_https_str()
            == "https://myacct.blob.core.usgovcloudapi.net/mycontainer/myblob"
        )
        assert (
            path.blob_url() == "https://myacct.blob.core.usgovcloudapi.net/mycontainer/myblob"
        )
        assert path.container_url() == "https://myacct.blob.core.usgovcloudapi.net/mycontainer"
        assert path.account_url() == "https://myacct.blob.core.usgovcloudapi.net"

        parsed = AzurePath.from_str(
            "https://myacct.blob.core.usgovcloudapi.net/mycontainer/myblob"
        )
        assert parsed == path

        url = urllib.parse.urlparse(
            "https://myacct.blob.core.usgovcloudapi.net/mycontainer/myblob"
        )
        assert AzurePath.is_cloud_path(url)

    # Mismatched cloud should fail clearly
    with configure(azure_cloud=AZURE_PUBLIC_CLOUD):
        url = urllib.parse.urlparse("https://myacct.blob.core.usgovcloudapi.net/mycontainer/myblob")
        assert not AzurePath.is_cloud_path(url)
        with pytest.raises(ValueError, match="unexpected host"):
            AzurePath.from_str("https://myacct.blob.core.usgovcloudapi.net/mycontainer/myblob")

    # az:// scheme works regardless of cloud config
    assert AzurePath.from_str("az://myacct/mycontainer/myblob") == path


def test_non_azure_path_parsing_does_not_resolve_cloud(monkeypatch):
    import boostedblob.globals as globals_mod

    monkeypatch.setattr(globals_mod, "get_cloud_config", lambda: (_ for _ in ()).throw(AssertionError))
    monkeypatch.setattr(globals_mod.config, "_azure_cloud_cache", None)
    monkeypatch.setattr(globals_mod.config, "_azure_cloud_override", None)

    assert BasePath.from_str("local.txt") == LocalPath("local.txt")
    assert BasePath.from_str("gs://bucket/blob") == GooglePath("bucket", "blob")
    assert BasePath.from_str("az://acct/container/blob") == AzurePath("acct", "container", "blob")
    with pytest.raises(ValueError, match="Invalid path"):
        BasePath.from_str("https://example.com")
    with pytest.raises(ValueError, match="unexpected host"):
        AzurePath.from_str("https://example.com")
