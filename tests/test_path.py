from typing import Any, Dict, Type

import pytest

from boostedblob.path import AzurePath, BasePath, GooglePath, LocalPath

PATHS: Dict[Type[BasePath], Any] = {
    AzurePath: AzurePath("shantanutest", "container", "blob"),
    GooglePath: GooglePath("shantanutest", "blob"),
    LocalPath: LocalPath("blob"),
}


def test_path_str():
    assert str(PATHS[AzurePath]) == "https://shantanutest.blob.core.windows.net/container/blob"
    assert str(PATHS[GooglePath]) == "gs://shantanutest/blob"
    assert str(PATHS[LocalPath]) == "blob"


def test_path_from_str():
    for path in PATHS.values():
        assert BasePath.from_str(str(path)) == path

    with pytest.raises(ValueError):
        BasePath.from_str("s3://bucket/blob")

    for path_cls in PATHS.keys():
        for path in PATHS.values():
            if isinstance(path, path_cls):
                assert path_cls.from_str(str(path)) == path
            else:
                with pytest.raises(ValueError):
                    path_cls.from_str(str(path))


def test_path_methods():
    for path in PATHS.values():
        assert path.name == "blob"

        assert not path.is_directory_like()
        assert str(path.ensure_directory_like()).endswith("/")
        assert path.ensure_directory_like().is_directory_like()
        assert not str(path.ensure_directory_like().ensure_directory_like()).endswith("//")
        # name always strips slash
        assert not path.ensure_directory_like().name.endswith("/")

        subpath = path / "subblob"
        assert subpath.name == "subblob"
        assert subpath.parent.name == "blob"

    # containers and buckets are already directory like
    assert PATHS[AzurePath].parent.blob == ""
    assert PATHS[AzurePath].parent.is_directory_like()
    assert PATHS[AzurePath].parent.ensure_directory_like().blob == ""

    assert PATHS[GooglePath].parent.blob == ""
    assert PATHS[GooglePath].parent.is_directory_like()
    assert PATHS[GooglePath].parent.ensure_directory_like().blob == ""
