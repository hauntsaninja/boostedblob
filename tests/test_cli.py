import contextlib
import datetime
import io
from typing import Any, List, Tuple
from unittest.mock import MagicMock

import pytest

import boostedblob.cli
from boostedblob import BasePath

from . import helpers


def run_bbb(argv: List[Any]) -> str:
    """Wrapper around boostedblob.cli.run_bbb that converts arguments and captures output."""
    output = io.StringIO()
    output.buffer = MagicMock()  # type: ignore
    output.buffer.write = MagicMock(side_effect=lambda b: output.write(b.decode("utf-8")))
    with contextlib.redirect_stdout(output):
        boostedblob.cli.run_bbb(list(map(str, argv)))
    return output.getvalue()


def test_cli():
    with helpers.tmp_local_dir() as local_dir:
        with helpers.tmp_azure_dir() as remote_dir:

            def normalise(output: str) -> List[str]:
                return sorted(
                    BasePath.from_str(p).relative_to(remote_dir) for p in output.splitlines()
                )

            def normalise_long(output: str) -> List[Tuple[str, str, str]]:
                def parse_line(line: str) -> Tuple[str, str, str]:
                    size = line[:12]
                    mtime = line[12 + 2 : 12 + 2 + 19]
                    path = line[12 + 2 + 19 + 2 :]

                    size = size.strip()
                    if size:
                        float(size.split()[0])
                    mtime = mtime.strip()
                    if mtime:
                        datetime.datetime.fromisoformat(mtime)
                        mtime = "mtime"
                    path = BasePath.from_str(path).relative_to(remote_dir)
                    return (size, mtime, path)

                return sorted(
                    parse_line(line) for line in output.splitlines() if not line.startswith("TOTAL")
                )

            f3_contents = b"f3_contents"

            helpers.create_file(local_dir / "f1")
            helpers.create_file(local_dir / "f2")
            helpers.create_file(local_dir / "f3", f3_contents)
            helpers.create_file(local_dir / "d1" / "f4", f3_contents)

            assert run_bbb(["share", local_dir / "f1"]).startswith("file://")

            assert run_bbb(["ls", remote_dir]) == ""
            with pytest.raises(ValueError):
                run_bbb(["cp", local_dir / "f1", local_dir / "f2", remote_dir / "missing"])
            run_bbb(["cp", local_dir / "f1", local_dir / "f2", remote_dir])
            assert normalise(run_bbb(["ls", remote_dir])) == ["f1", "f2"]

            # ls a file prints that file
            assert normalise(run_bbb(["ls", remote_dir / "f1"])) == ["f1"]
            assert normalise_long(run_bbb(["ls", "-l", "-n", remote_dir / "f1"])) == [
                ("4", "mtime", "f1")
            ]
            with pytest.raises(FileNotFoundError):
                run_bbb(["ls", remote_dir / "f999"])

            run_bbb(["rm", remote_dir / "f1", remote_dir / "f2"])
            assert run_bbb(["ls", remote_dir]) == ""

            run_bbb(["cp", local_dir / "f3", remote_dir / "file3"])
            assert run_bbb(["cat", remote_dir / "file3"]) == f3_contents.decode("utf-8")
            run_bbb(["rm", remote_dir / "file3"])

            run_bbb(["cptree", local_dir, remote_dir])
            assert normalise(run_bbb(["ls", remote_dir])) == ["d1/", "f1", "f2", "f3"]
            assert normalise_long(run_bbb(["ls", "-l", remote_dir])) == [
                ("", "", "d1/"),
                ("11.0 B", "mtime", "f3"),
                ("4.0 B", "mtime", "f1"),
                ("4.0 B", "mtime", "f2"),
            ]

            run_bbb(["rmtree", remote_dir / "d1"])
            helpers.create_file(remote_dir / "f5")
            run_bbb(["sync", "--delete", local_dir, remote_dir])

            assert normalise(run_bbb(["lstree", remote_dir])) == ["d1/f4", "f1", "f2", "f3"]
            assert normalise_long(run_bbb(["lstree", "-l", "-n", remote_dir])) == [
                ("11", "mtime", "d1/f4"),
                ("11", "mtime", "f3"),
                ("4", "mtime", "f1"),
                ("4", "mtime", "f2"),
            ]

            run_bbb(["rmtree", remote_dir])
            assert run_bbb(["lstree", remote_dir]) == ""
