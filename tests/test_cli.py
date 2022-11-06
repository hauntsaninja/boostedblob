import contextlib
import datetime
import io
import os
import subprocess
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

            def normalise_abs(output: str) -> List[str]:
                return sorted(
                    BasePath.from_str(p).relative_to(remote_dir) for p in output.splitlines()
                )

            def normalise_rel(output: str) -> List[str]:
                return sorted(p for p in output.splitlines())

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
                    parse_line(line)
                    for line in output.splitlines()
                    if not line.startswith("Listed")
                )

            g3_contents = b"g3_contents"

            helpers.create_file(local_dir / "f1")
            helpers.create_file(local_dir / "f2")
            helpers.create_file(local_dir / "g3", g3_contents)
            helpers.create_file(local_dir / "d1" / "f4", g3_contents)

            assert run_bbb(["share", local_dir / "f1"]).startswith("file://")

            assert run_bbb(["ls", remote_dir]) == ""
            with pytest.raises(NotADirectoryError):
                run_bbb(["cp", local_dir / "f1", local_dir / "f2", remote_dir / "missing"])
            run_bbb(["cp", local_dir / "f1", local_dir / "f2", remote_dir])
            assert normalise_abs(run_bbb(["ls", remote_dir])) == ["f1", "f2"]
            assert normalise_rel(run_bbb(["ls", remote_dir, "--relative"])) == ["f1", "f2"]

            # ls a file prints that file
            assert normalise_abs(run_bbb(["ls", remote_dir / "f1"])) == ["f1"]
            assert normalise_rel(run_bbb(["ls", remote_dir / "f1", "--relative"])) == ["f1"]
            assert normalise_rel(run_bbb(["ls", local_dir / "f1", "--relative"])) == ["f1"]
            assert normalise_long(run_bbb(["ls", "-l", "--machine", remote_dir / "f1"])) == [
                ("4", "mtime", "f1")
            ]
            with pytest.raises(FileNotFoundError):
                run_bbb(["ls", remote_dir / "f999"])

            run_bbb(["rm", remote_dir / "f1", remote_dir / "f2"])
            assert run_bbb(["ls", remote_dir / "*"]) == ""

            run_bbb(["cp", local_dir / "g3", remote_dir / "file3"])
            assert run_bbb(["cat", remote_dir / "file3"]) == g3_contents.decode("utf-8")
            run_bbb(["rm", remote_dir / "*3"])

            run_bbb(["cp", local_dir / "f*", remote_dir])
            assert normalise_long(run_bbb(["ls", "-l", remote_dir / "f*"])) == [
                ("4.0 B", "mtime", "f1"),
                ("4.0 B", "mtime", "f2"),
            ]
            run_bbb(["rmtree", remote_dir / "f*"])
            assert (
                run_bbb(["ls", "-l", remote_dir]) == "Listed 0 files summing to 0 bytes (0.0 B)\n"
            )

            run_bbb(["cptree", local_dir, remote_dir])
            contents = ["d1/", "f1", "f2", "g3"]
            assert normalise_abs(run_bbb(["ls", remote_dir])) == contents
            assert normalise_rel(run_bbb(["ls", remote_dir, "--relative"])) == contents
            assert normalise_rel(run_bbb(["ls", local_dir, "--relative"])) == contents
            assert normalise_long(run_bbb(["ls", "-l", remote_dir])) == [
                ("", "", "d1/"),
                ("11.0 B", "mtime", "g3"),
                ("4.0 B", "mtime", "f1"),
                ("4.0 B", "mtime", "f2"),
            ]

            run_bbb(["rmtree", remote_dir / "d1"])
            helpers.create_file(remote_dir / "f5")
            run_bbb(["sync", "--delete", local_dir, remote_dir])

            contents = ["d1/f4", "f1", "f2", "g3"]
            assert normalise_abs(run_bbb(["lstree", remote_dir])) == contents
            assert normalise_rel(run_bbb(["lstree", remote_dir, "--relative"])) == contents
            assert normalise_rel(run_bbb(["lstree", local_dir, "--relative"])) == contents
            assert normalise_long(run_bbb(["lstree", "-l", "--machine", remote_dir])) == [
                ("11", "mtime", "d1/f4"),
                ("11", "mtime", "g3"),
                ("4", "mtime", "f1"),
                ("4", "mtime", "f2"),
            ]
            assert normalise_long(run_bbb(["lstree", "-l", "--machine", remote_dir / "f1"])) == [
                ("4", "mtime", "f1")
            ]

            run_bbb(["rmtree", remote_dir])
            with pytest.raises(FileNotFoundError):
                run_bbb(["lstree", remote_dir])


def test_complete():
    with helpers.tmp_azure_dir() as azure_dir:
        helpers.create_file(azure_dir / "somefile")
        https_path_str = str(azure_dir.ensure_directory_like().to_https_str())
        az_path_str = str(azure_dir.ensure_directory_like().to_az_str())

        # test a normal completion
        expected_output = [f"{https_path_str}somefile"]
        output = run_bbb(["complete", "command", "zsh", 2, "bbb", "ls", https_path_str])
        assert output.splitlines() == expected_output
        output = run_bbb(
            ["complete", "command", "bash", 7 + len(https_path_str), f"bbb ls {https_path_str}"]
        )
        assert [f"https:{p}" for p in output.splitlines()] == expected_output

        # test a completion with an az:// url
        expected_output = [f"{az_path_str}somefile"]
        output = run_bbb(["complete", "command", "zsh", 2, "bbb", "ls", az_path_str])
        assert output.splitlines() == expected_output
        output = run_bbb(
            ["complete", "command", "bash", 7 + len(az_path_str), f"bbb ls {az_path_str}"]
        )
        assert [f"az:{p}" for p in output.splitlines()] == expected_output

        # if BBB_DEBUG is on then we print a stacktrace, so just skip this test in that case
        if not os.environ.get("BBB_DEBUG"):
            assert run_bbb(["complete", "command", "zsh", 2, "bbb", "ls", "az://"]) == ""

    subprocess.check_call(["bash", "-c", run_bbb(["complete", "init", "bash"])])
    subprocess.check_call(["zsh", "-ic", run_bbb(["complete", "init", "zsh"])])
