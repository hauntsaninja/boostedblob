import argparse
import asyncio
import functools
import sys
from typing import Any, Awaitable, Callable, List, TypeVar

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
async def ls(path: str) -> None:
    async for p in bbb.listdir(path):
        print(p)


@cli_decorate
async def lstree(path: str) -> None:
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
async def cptree(src: str, dst: str, concurrency: int = DEFAULT_CONCURRENCY) -> None:
    src_obj = bbb.BasePath.from_str(src)
    async with bbb.BoostExecutor(concurrency) as executor:
        async for p in bbb.copying.copytree_iterator(src_obj, dst, executor):
            print(p)


@cli_decorate
async def rm(paths: List[str], concurrency: int = DEFAULT_CONCURRENCY) -> None:
    async with bbb.BoostExecutor(concurrency) as executor:
        await bbb.boost.consume(executor.map_unordered(bbb.remove, iter(paths)))


@cli_decorate
async def rmtree(path: str, concurrency: int = DEFAULT_CONCURRENCY) -> None:
    path_obj = bbb.BasePath.from_str(path)
    async with bbb.BoostExecutor(concurrency) as executor:
        if isinstance(path_obj, bbb.CloudPath):
            async for p in bbb.delete.rmtree_iterator(path_obj, executor):
                print(p)
        else:
            await bbb.rmtree(path_obj, executor)


def parse_options(args: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparser = subparsers.add_parser("ls", help="List files in a directory")
    subparser.add_argument("path", help="Path of directory to list")

    subparser = subparsers.add_parser("lstree", help="List all files in a directory tree")
    subparser.add_argument("path", help="Root of directory tree to list")

    subparser = subparsers.add_parser("cat", help="Print the contents of a file")
    subparser.add_argument("path", help="File whose contents to print")
    subparser.add_argument("--concurrency", default=DEFAULT_CONCURRENCY)

    subparser = subparsers.add_parser("cp", help="Copy files")
    subparser.add_argument("srcs", nargs="+", help="File(s) to copy from")
    subparser.add_argument("dst", help="File or directory to copy to")
    subparser.add_argument("--concurrency", default=DEFAULT_CONCURRENCY)

    subparser = subparsers.add_parser("cptree", help="Copy a directory tree")
    subparser.add_argument("src", help="Directory to copy from")
    subparser.add_argument("dst", help="Directory to copy to")
    subparser.add_argument("--concurrency", default=DEFAULT_CONCURRENCY)

    subparser = subparsers.add_parser("rm", help="Remove files")
    subparser.add_argument("paths", help="File(s) to delete")
    subparser.add_argument("--concurrency", default=DEFAULT_CONCURRENCY)

    subparser = subparsers.add_parser("rmtree", help="Remove a directory tree")
    subparser.add_argument("path", help="Directory to delete")
    subparser.add_argument("--concurrency", default=DEFAULT_CONCURRENCY)

    return parser.parse_args(args)


def run_bbb() -> None:
    args = parse_options(sys.argv[1:])
    command = args.__dict__.pop("command")
    try:
        eval(command)(**args.__dict__)
    except Exception as e:
        print(f"ERROR: {type(e).__name__}: {e}")
        sys.exit(1)
