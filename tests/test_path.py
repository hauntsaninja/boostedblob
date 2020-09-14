from typing import Dict, Type

import pytest

from boostedblob.path import AzurePath, BasePath, GooglePath, LocalPath

PATHS: Dict[Type[BasePath], BasePath] = {
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

        assert not path.has_trailing_slash()
        assert str(path.with_trailing_slash()).endswith("/")
        assert path.with_trailing_slash().has_trailing_slash()
        assert not str(path.with_trailing_slash().with_trailing_slash()).endswith("//")
        # name always strips slash
        assert not path.with_trailing_slash().name.endswith("/")

        subpath = path / "subblob"
        assert subpath.name == "subblob"
        assert subpath.parent.name == "blob"
