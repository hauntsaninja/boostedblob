import argparse
import asyncio
import functools
import sys
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, List, TypeVar

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
    path_obj = bbb.BasePath.from_str(path)
    if "*" in path:
        async for entry in bbb.listing.globscandir(path_obj):
            if long:
                print(entry)
            else:
                print(entry.path)
        return

    try:
        it = bbb.scandir(path_obj) if long else bbb.listdir(path_obj)
        assert isinstance(it, AsyncIterator)
        async for entry in it:
            print(entry)
    except NotADirectoryError:
        if long:
            stat = await bbb.stat(path_obj)
            print(bbb.listing.DirEntry.from_path_stat(path_obj, stat))
        else:
            print(path_obj)


@cli_decorate
async def lstree(path: str, long: bool = False) -> None:
    try:
        it = bbb.scantree(path) if long else bbb.listtree(path)
        assert isinstance(it, AsyncIterator)
        async for entry in it:
            print(entry)
    except NotADirectoryError:
        path_obj = bbb.BasePath.from_str(path)
        if long:
            stat = await bbb.stat(path_obj)
            print(bbb.listing.DirEntry.from_path_stat(path_obj, stat))
        else:
            print(path_obj)


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
        if len(paths) == 1 and "*" in paths[0]:
            it = bbb.boost.EagerAsyncIterator(bbb.listing.globscandir(paths[0]))
            await bbb.boost.consume(executor.map_unordered(lambda x: bbb.remove(x.path), it))
        else:
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


@cli_decorate
async def sync(
    src: str,
    dst: str,
    delete: bool = False,
    quiet: bool = False,
    concurrency: int = DEFAULT_CONCURRENCY,
) -> None:
    src_obj = bbb.BasePath.from_str(src)
    dst_obj = bbb.BasePath.from_str(dst)

    src_is_dirlike = src_obj.is_directory_like() or await bbb.isdir(src_obj)
    if not src_is_dirlike:
        raise ValueError(f"{src_obj} is not a directory")
    async with bbb.BoostExecutor(concurrency) as executor:
        async for p in bbb.sync(src_obj, dst_obj, executor, delete=delete):
            if not quiet:
                print(p)


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

To check whether a file exists:
$ bbb ls schrodingers_file.txt
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
    sync_desc = """\
`bbb sync` synchronises two directory trees. Provide two directory paths, and
bbb will change the destination so that it better mirrors the source.
Specifically, bbb will copy over or replace files in the destination that are
missing or have changed in the source. If --delete is specified, bbb will delete
files in the destination that are not present in the source.

Example:
$ bbb sync gs://tmp/boostedblob boostedblob

Using --delete and re-syncing will delete spurious_file.txt:
$ touch boostedblob/spurious_file.txt
$ bbb sync --delete gs://tmp/boostedblob boostedblob
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

    subparser = subparsers.add_parser(
        "sync",
        help="Sync a directory tree",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=sync_desc,
    )
    subparser.set_defaults(command=sync)
    subparser.add_argument("src", help="Directory to sync from")
    subparser.add_argument("dst", help="Directory to sync to")
    subparser.add_argument(
        "--delete", action="store_true", help="Delete destination files that don't exist in source"
    )
    subparser.add_argument("-q", "--quiet", action="store_true")
    subparser.add_argument("--concurrency", **concurrency_kwargs)

    if not args:
        parser.error("missing subcommand, see `bbb --help`")
    return parser.parse_args(args)


def run_bbb(argv: List[str]) -> None:
    args = parse_options(argv)
    command = args.__dict__.pop("command")
    try:
        command(**args.__dict__)
    except Exception as e:
        print(f"ERROR: {type(e).__name__}: {e}", file=sys.stderr)
        raise
