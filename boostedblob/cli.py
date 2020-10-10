import argparse
import asyncio
import datetime
import functools
import sys
from typing import Any, Awaitable, Callable, Dict, List, TypeVar

import boostedblob as bbb

F = TypeVar("F", bound=Callable[..., Any])
T = TypeVar("T")


def syncify(fn: Callable[..., Awaitable[T]]) -> Callable[..., T]:
    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            import uvloop

            uvloop.install()
        except ImportError:
            pass
        return asyncio.run(fn(*args, **kwargs))

    return wrapper


def cli_decorate(fn: F) -> F:
    return syncify(bbb.ensure_session(fn))  # type: ignore


DEFAULT_CONCURRENCY = 100


@cli_decorate
async def ls(path: str, long: bool = False) -> None:
    if long:
        async for d in bbb.scandir(path):
            size = d.stat.size if d.stat else ""
            mtime = datetime.datetime.fromtimestamp(int(d.stat.mtime)).isoformat() if d.stat else ""
            print(f"{size:12}  {mtime:19}  {d.path}")
    else:
        async for p in bbb.listdir(path):
            print(p)


@cli_decorate
async def lstree(path: str, long: bool = False) -> None:
    if long:
        async for d in bbb.scantree(path):
            assert d.stat is not None
            size = d.stat.size
            mtime = datetime.datetime.fromtimestamp(int(d.stat.mtime)).isoformat()
            print(f"{size:12}  {mtime:19}  {d.path}")
    else:
        async for p in bbb.listtree(path):
            print(p)


@cli_decorate
async def cat(path: str, concurrency: int = DEFAULT_CONCURRENCY) -> None:
    loop = asyncio.get_event_loop()
    async with bbb.BoostExecutor(concurrency) as executor:
        stream = await bbb.read.read_stream(path, executor)
        async for data in bbb.boost.iter_underlying(stream):
            await loop.run_in_executor(None, sys.stdout.buffer.write, data)


@cli_decorate
async def cp(srcs: List[str], dst: str, concurrency: int = DEFAULT_CONCURRENCY) -> None:
    dst_obj = bbb.BasePath.from_str(dst)
    dst_is_dirlike = dst_obj.is_directory_like() or await bbb.isdir(dst_obj)

    async with bbb.BoostExecutor(concurrency) as executor:
        if len(srcs) > 1 and not dst_is_dirlike:
            raise ValueError(f"{dst_obj} is not a directory")

        async def copy_wrapper(src: str) -> None:
            src_obj = bbb.BasePath.from_str(src)
            dst_file_obj = dst_obj / src_obj.name if dst_is_dirlike else dst_obj
            await bbb.copyfile(src_obj, dst_file_obj, executor, overwrite=True)

        await bbb.boost.consume(executor.map_unordered(copy_wrapper, iter(srcs)))


@cli_decorate
async def cptree(
    src: str, dst: str, quiet: bool = False, concurrency: int = DEFAULT_CONCURRENCY
) -> None:
    src_obj = bbb.BasePath.from_str(src)
    async with bbb.BoostExecutor(concurrency) as executor:
        async for p in bbb.copying.copytree_iterator(src_obj, dst, executor):
            if not quiet:
                print(p)


@cli_decorate
async def rm(paths: List[str], concurrency: int = DEFAULT_CONCURRENCY) -> None:
    async with bbb.BoostExecutor(concurrency) as executor:
        await bbb.boost.consume(executor.map_unordered(bbb.remove, iter(paths)))


@cli_decorate
async def rmtree(path: str, quiet: bool = False, concurrency: int = DEFAULT_CONCURRENCY) -> None:
    path_obj = bbb.BasePath.from_str(path)
    async with bbb.BoostExecutor(concurrency) as executor:
        if isinstance(path_obj, bbb.CloudPath):
            async for p in bbb.delete.rmtree_iterator(path_obj, executor):
                if not quiet:
                    print(p)
        else:
            await bbb.rmtree(path_obj, executor)


@cli_decorate
async def share(path: str) -> None:
    url, expiration = await bbb.share.get_url(path)
    print(url)
    if expiration is not None:
        print(f"Expires on: {expiration.isoformat()}")


def parse_options(args: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", action="version", version=f"boostedblob {bbb.__version__}")
    subparsers = parser.add_subparsers(required=True)

    concurrency_kwargs: Dict[str, Any] = dict(
        type=int,
        metavar="N",
        default=DEFAULT_CONCURRENCY,
        help="Number of concurrent requests to use",
    )

    ls_desc = """\
`bbb ls` lists the immediate contents of a directory (both files and
subdirectories).

To see the contents of the current directory:
$ bbb ls .

To additionally see size and mtime of files in the current directory:
$ bbb ls -l .
"""
    lstree_desc = """\
`bbb lstree` lists all files present anywhere in the given directory tree.
Note that `bbb lstree` only lists files and will not list subdirectories.

To see all files in your bucket:
$ bbb lstree gs://my_bucket/

To additionally see size and mtime of files:
$ bbb lstree -l gs://my_bucket/
"""
    cat_desc = """\
`bbb cat` copies the contents of a given file to stdout.

Example:
$ bbb cat boostedblob/boost.py
"""
    cp_desc = """\
`bbb cp` copies a file to another location. If the location is a directory, the
file will be copied there under the same name, otherwise it will be copied to
the name provided. If the destination file already exists, it will be
overwritten.

Copy a file into a directory:
$ bbb cp frogs.txt my_directory

Copy multiple files into a directory:
$ bbb cp frogs.txt worms.txt my_directory/

Copy a file to a file with a different name:
$ bbb cp frogs.txt renamed_frogs.txt
"""
    cptree_desc = """\
`bbb cptree` copies an entire directory tree. The rule here is simple: provide
two directory paths, and bbb will create a copy of the files with the same
structure under the destination directory. If any of the files already exist,
they will be overwritten.

Create an exact copy of a directory somewhere else:
$ bbb cptree boostedblob gs://tmp/boostedblob
"""
    rm_desc = """\
`bbb rm` deletes the given files.

Example:
$ bbb rm frog.txt worm.txt
"""
    rmtree_desc = """\
`bbb rmtree` deletes an entire directory tree.

Example:
$ bbb rmtree boostedblob
"""
    share_desc = """\
`bbb share` prints a link you can use to open a file in a browser.

Example:
$ bbb share gs://bucket/frogs.txt
"""

    subparser = subparsers.add_parser(
        "ls",
        help="List contents of a directory",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=ls_desc,
    )
    subparser.set_defaults(command=ls)
    subparser.add_argument("path", help="Path of directory to list")
    subparser.add_argument(
        "-l", "--long", action="store_true", help="List information about each file"
    )

    subparser = subparsers.add_parser(
        "lstree",
        aliases=["lsr"],
        help="List all files in a directory tree",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=lstree_desc,
    )
    subparser.set_defaults(command=lstree)
    subparser.add_argument("path", help="Root of directory tree to list")
    subparser.add_argument(
        "-l", "--long", action="store_true", help="List information about each file"
    )

    subparser = subparsers.add_parser(
        "cat",
        help="Print the contents of a file",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=cat_desc,
    )
    subparser.set_defaults(command=cat)
    subparser.add_argument("path", help="File whose contents to print")
    subparser.add_argument("--concurrency", **concurrency_kwargs)

    subparser = subparsers.add_parser(
        "cp",
        help="Copy files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=cp_desc,
    )
    subparser.set_defaults(command=cp)
    subparser.add_argument("srcs", nargs="+", help="File(s) to copy from")
    subparser.add_argument("dst", help="File or directory to copy to")
    subparser.add_argument("--concurrency", **concurrency_kwargs)

    subparser = subparsers.add_parser(
        "cptree",
        aliases=["cpr"],
        help="Copy a directory tree",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=cptree_desc,
    )
    subparser.set_defaults(command=cptree)
    subparser.add_argument("src", help="Directory to copy from")
    subparser.add_argument("dst", help="Directory to copy to")
    subparser.add_argument("-q", "--quiet", action="store_true")
    subparser.add_argument("--concurrency", **concurrency_kwargs)

    subparser = subparsers.add_parser(
        "rm",
        help="Remove files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=rm_desc,
    )
    subparser.set_defaults(command=rm)
    subparser.add_argument("paths", nargs="+", help="File(s) to delete")
    subparser.add_argument("--concurrency", **concurrency_kwargs)

    subparser = subparsers.add_parser(
        "rmtree",
        aliases=["rmr"],
        help="Remove a directory tree",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=rmtree_desc,
    )
    subparser.set_defaults(command=rmtree)
    subparser.add_argument("path", help="Directory to delete")
    subparser.add_argument("-q", "--quiet", action="store_true")
    subparser.add_argument("--concurrency", **concurrency_kwargs)

    subparser = subparsers.add_parser(
        "share",
        help="Get a shareable link to a file",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=share_desc,
    )
    subparser.set_defaults(command=share)
    subparser.add_argument("path", help="Path to share")

    return parser.parse_args(args)


def run_bbb(argv: List[str]) -> None:
    args = parse_options(argv)
    command = args.__dict__.pop("command")
    try:
        command(**args.__dict__)
    except Exception as e:
        print(f"ERROR: {type(e).__name__}: {e}", file=sys.stderr)
        raise
