import argparse
import asyncio
import functools
import inspect
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


cli_fns = {}


def register_cli(fn: F) -> F:
    cli_fns[fn.__name__] = fn
    return fn


def cli_decorate(fn: F) -> F:
    return functools.wraps(fn)(register_cli(syncify(bbb.ensure_session(fn))))  # type: ignore


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
async def cp(src: str, dst: str, concurrency: int = DEFAULT_CONCURRENCY) -> None:
    async with bbb.BoostExecutor(concurrency) as executor:
        src_obj = bbb.BasePath.from_str(src)
        dst_obj = bbb.BasePath.from_str(dst)
        if dst_obj.is_directory_like() or await bbb.isdir(dst_obj):
            dst_obj /= src_obj.name
        await bbb.copyfile(src_obj, dst_obj, executor, overwrite=True)


@cli_decorate
async def cptree(src: str, dst: str, concurrency: int = DEFAULT_CONCURRENCY) -> None:
    src_obj = bbb.BasePath.from_str(src)
    async with bbb.BoostExecutor(concurrency) as executor:
        async for p in bbb.copying.copytree_iterator(src_obj, dst, executor):
            print(p)


@cli_decorate
async def rm(path: str) -> None:
    await bbb.remove(path)


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

    for fn in cli_fns.values():
        subparser = subparsers.add_parser(fn.__name__)
        sig = inspect.signature(fn)
        for param in sig.parameters.values():
            typ = eval(param.annotation) if isinstance(param.annotation, str) else param.annotation
            if param.default == param.empty:
                subparser.add_argument(param.name, type=typ)
            else:
                subparser.add_argument(f"--{param.name}", default=param.default, type=typ)

    return parser.parse_args(args)


def run_bbb() -> None:
    args = parse_options(sys.argv[1:])
    command = args.__dict__.pop("command")
    try:
        cli_fns[command](**args.__dict__)
    except Exception as e:
        print(f"ERROR: {type(e).__name__}: {e}")
