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

        subpath = path / "subblob"
        assert subpath.name == "subblob"
        assert subpath.parent.name == "blob"

        subsubpath = path / "subblob/subsubsub"
        assert subsubpath.name == "subsubsub"
        assert subsubpath.parent.name == "subblob"
        assert subsubpath.parent.parent.name == "blob"

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
