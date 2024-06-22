import argparse
import asyncio
import datetime
import functools
import os
import shlex
import subprocess
import sys
import tempfile
from typing import Any, AsyncIterator, Callable, Coroutine, Dict, List, Optional, TypeVar, cast

import boostedblob as bbb

T = TypeVar("T")


def syncify(fn: Callable[..., Coroutine[T, None, None]]) -> Callable[..., T]:
    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            import uvloop

            if sys.version_info < (3, 9) or (
                tuple(map(int, cast(Any, uvloop).__version__.split("."))) >= (0, 15, 0)
            ):
                uvloop.install()
        except ImportError:
            pass
        return asyncio.run(fn(*args, **kwargs))

    return wrapper


def sync_with_session(fn: Callable[..., Coroutine[T, None, None]]) -> Callable[..., T]:
    return syncify(bbb.ensure_session(fn))


DEFAULT_CONCURRENCY = int(os.environ.get("BBB_DEFAULT_CONCURRENCY", 32))


def is_glob(path: str) -> bool:
    return "*" in path


def glob_parent(path: str) -> str:
    return path.split("*", 1)[0].rsplit("/", 1)[0]


def format_size(num: float, suffix: str = "B") -> str:
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            unit += suffix
            return f"{num:.1f} {unit:<3}"
        num /= 1024.0
    return f"{num:.1f} Yi{suffix}"


def format_path_relative(path: bbb.BasePath, relative_to: Optional[bbb.BasePath]) -> str:
    if relative_to is None:
        return str(path)
    if relative_to == path:
        return path.name
    return path.relative_to(relative_to)


def format_long_entry(
    entry: bbb.listing.DirEntry, human_readable: bool, relative_to: Optional[bbb.BasePath]
) -> str:
    size = (
        (format_size(entry.stat.size) if human_readable else entry.stat.size) if entry.stat else ""
    )
    mtime = datetime.datetime.fromtimestamp(int(entry.stat.mtime)).isoformat() if entry.stat else ""
    path = format_path_relative(entry.path, relative_to)
    return f"{size:>12}  {mtime:19}  {path}"


async def print_long(
    it: AsyncIterator[bbb.listing.DirEntry],
    human_readable: bool,
    relative_to: Optional[bbb.BasePath],
) -> None:
    total = 0
    num_files = 0
    async for entry in it:
        if entry.is_file:
            num_files += 1
        if entry.stat:
            total += entry.stat.size
        print(format_long_entry(entry, human_readable=human_readable, relative_to=relative_to))
    if human_readable:
        human_total = format_size(total).strip()
        print(f"Listed {num_files} files summing to {total} bytes ({human_total})")


@sync_with_session
async def ls(path: str, long: bool = False, machine: bool = False, relative: bool = False) -> None:
    path_obj = bbb.BasePath.from_str(path)
    if isinstance(path_obj, bbb.LocalPath):
        path_obj = path_obj.abspath()

    if is_glob(path):
        relative_to = bbb.BasePath.from_str(glob_parent(path)) if relative else None
        it = bbb.listing.glob_scandir(path_obj)
        if long:
            await print_long(it, human_readable=not machine, relative_to=relative_to)
        else:
            async for entry in it:
                print(format_path_relative(entry.path, relative_to))
        return

    relative_to = path_obj if relative else None
    try:
        if long:
            await print_long(
                bbb.scandir(path_obj),
                human_readable=not machine,
                relative_to=path_obj if relative else None,
            )
        else:
            async for p in bbb.listdir(path_obj):
                print(format_path_relative(p, relative_to))
    except NotADirectoryError:
        if long:
            stat = await bbb.stat(path_obj)
            entry = bbb.listing.DirEntry.from_path_stat(path_obj, stat)
            print(format_long_entry(entry, human_readable=not machine, relative_to=relative_to))
        else:
            print(format_path_relative(path_obj, relative_to))


@sync_with_session
async def lstree(
    path: str, long: bool = False, machine: bool = False, relative: bool = False
) -> None:
    path_obj = bbb.BasePath.from_str(path)
    if isinstance(path_obj, bbb.LocalPath):
        path_obj = path_obj.abspath()
    relative_to = path_obj if relative else None

    try:
        if long:
            await print_long(
                bbb.scantree(path), human_readable=not machine, relative_to=relative_to
            )
        else:
            async for p in bbb.listtree(path):
                print(format_path_relative(p, relative_to))
    except NotADirectoryError:
        if long:
            stat = await bbb.stat(path_obj)
            entry = bbb.listing.DirEntry.from_path_stat(path_obj, stat)
            print(format_long_entry(entry, human_readable=not machine, relative_to=relative_to))
        else:
            print(format_path_relative(path_obj, relative_to))


@sync_with_session
async def _dud1(path: str) -> None:
    sizes: Dict[bbb.BasePath, int] = {}

    total_count = 0

    async def _dud0(entry: bbb.listing.DirEntry) -> None:
        nonlocal total_count
        if entry.is_file:
            sizes[entry.path] = entry.stat.size if entry.stat else 0
            total_count += 1
            return
        sizes[entry.path] = 0
        async for e in bbb.scantree(entry.path):
            if e.stat:
                sizes[entry.path] += e.stat.size
                total_count += 1

    def print_sizes() -> int:
        sorted_sizes = sorted(sizes.items(), key=lambda x: x[1])
        for subpath, size in sorted_sizes:
            print(f"{format_size(size).strip():>12}  {subpath}")
        return len(sorted_sizes)

    def clear_lines(num_lines: int) -> None:
        erase_in_line = "\x1b[2K"
        cursor_up = "\x1b[1A"
        clear_scrollback = "\x1b[3J"
        print("\r" + f"{cursor_up}{erase_in_line}" * num_lines, end="")
        print(clear_scrollback, end="")

    def print_spinner(message: str, _pos: Any = [0]) -> None:  # noqa: B006
        clocks = ["🕛", "🕐", "🕑", "🕒", "🕓", "🕔", "🕕", "🕖", "🕗", "🕘", "🕙", "🕚"]
        print(f"{clocks[_pos[0]]} {message}")
        _pos[0] = (_pos[0] + 1) % len(clocks)

    loop = asyncio.get_running_loop()
    finished = loop.create_future()

    async def live_update() -> None:
        last_printed_len = 0
        while True:
            clear_lines(last_printed_len)
            last_printed_len = print_sizes()
            print_spinner(f"Summed {total_count} files so far...")
            last_printed_len += 1
            try:
                await asyncio.wait_for(asyncio.shield(finished), 0.25)
                break
            except asyncio.TimeoutError:
                pass
        clear_lines(last_printed_len)

    printer = asyncio.create_task(live_update())

    async with bbb.BoostExecutor(DEFAULT_CONCURRENCY) as executor:
        await bbb.boost.consume(executor.map_unordered(_dud0, executor.eagerise(bbb.scandir(path))))

    finished.set_result(None)
    await printer

    print_sizes()
    total_size = format_size(sum(sizes.values())).strip()
    print(f"Listed {total_count} files summing to {total_size}")


@sync_with_session
async def cat(path: str, concurrency: int = DEFAULT_CONCURRENCY) -> None:
    loop = asyncio.get_running_loop()
    async with bbb.BoostExecutor(concurrency) as executor:
        stream = await bbb.read.read_stream(path, executor)
        async for data in bbb.boost.iter_underlying(stream):
            await loop.run_in_executor(None, sys.stdout.buffer.write, data)


@sync_with_session
async def cp(
    srcs: List[str], dst: str, quiet: bool = False, concurrency: int = DEFAULT_CONCURRENCY
) -> None:
    dst_obj = bbb.BasePath.from_str(dst)
    dst_is_dirlike = dst_obj.is_directory_like() or await bbb.isdir(dst_obj)

    async with bbb.BoostExecutor(concurrency) as executor:
        if len(srcs) > 1 and not dst_is_dirlike:
            raise NotADirectoryError(dst_obj)

        async def copy_wrapper(src: str) -> None:
            src_obj = bbb.BasePath.from_str(src)
            if is_glob(src):
                if not dst_is_dirlike:
                    raise NotADirectoryError(dst_obj)
                async for path in bbb.copying.copyglob_iterator(src_obj, dst_obj, executor):
                    if not quiet:
                        print(path)
                return

            dst_file_obj = dst_obj / src_obj.name if dst_is_dirlike else dst_obj
            await bbb.copyfile(src_obj, dst_file_obj, executor, overwrite=True)
            if not quiet:
                print(src_obj)

        await bbb.boost.consume(executor.map_unordered(copy_wrapper, iter(srcs)))


@sync_with_session
async def cptree(
    src: str, dst: str, quiet: bool = False, concurrency: int = DEFAULT_CONCURRENCY
) -> None:
    src_obj = bbb.BasePath.from_str(src)
    async with bbb.BoostExecutor(concurrency) as executor:
        async for p in bbb.copying.copytree_iterator(src_obj, dst, executor):
            if not quiet:
                print(p)


@sync_with_session
async def rm(paths: List[str], quiet: bool = False, concurrency: int = DEFAULT_CONCURRENCY) -> None:
    async with bbb.BoostExecutor(concurrency) as executor:

        async def remove_wrapper(path: str) -> None:
            path_obj = bbb.BasePath.from_str(path)
            if is_glob(path):
                async for p in bbb.delete.glob_remove(path_obj, executor):
                    if not quiet:
                        print(p)
                return
            await bbb.remove(path_obj)
            if not quiet:
                print(path_obj)

        await bbb.boost.consume(executor.map_unordered(remove_wrapper, iter(paths)))


@sync_with_session
async def rmtree(path: str, quiet: bool = False, concurrency: int = DEFAULT_CONCURRENCY) -> None:
    path_obj = bbb.BasePath.from_str(path)
    async with bbb.BoostExecutor(concurrency) as executor:
        if is_glob(path):
            # this will fail if the glob matches a directory, which is a little contra the spirit of
            # rmtree. but maybe the best way to do that (and least likely to result in accidents) is
            # through recursive wildcards
            async for p in bbb.delete.glob_remove(path_obj, executor):
                if not quiet:
                    print(p)
        elif isinstance(path_obj, bbb.CloudPath):
            async for p in bbb.delete.rmtree_iterator(path_obj, executor):
                if not quiet:
                    print(p)
        else:
            await bbb.rmtree(path_obj, executor)


@sync_with_session
async def _xxx_recoverprefix(
    prefix: str, restore_dt: str, dry_run: bool = True, concurrency: int = DEFAULT_CONCURRENCY
) -> None:
    from ._recover import _recoverprefix

    async with bbb.BoostExecutor(concurrency) as executor:
        await _recoverprefix(prefix, restore_dt, executor=executor, dry_run=dry_run)


@sync_with_session
async def share(path: str) -> None:
    url, expiration = await bbb.share.get_url(path)
    print(url)
    if expiration is not None:
        print(f"Expires on: {expiration.isoformat()}")


@sync_with_session
async def sync(
    src: str,
    dst: str,
    delete: bool = False,
    exclude: Optional[str] = None,
    quiet: bool = False,
    concurrency: int = DEFAULT_CONCURRENCY,
) -> None:
    src_obj = bbb.BasePath.from_str(src)
    dst_obj = bbb.BasePath.from_str(dst)

    src_is_dirlike = src_obj.is_directory_like() or await bbb.isdir(src_obj)
    if not src_is_dirlike:
        raise ValueError(f"{src_obj} is not a directory")
    async with bbb.BoostExecutor(concurrency) as executor:
        async for p in bbb.sync(src_obj, dst_obj, executor, delete=delete, exclude=exclude):
            if not quiet:
                print(p)


@sync_with_session
async def edit(path: str, read_only: bool = False) -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        path_obj = bbb.BasePath.from_str(path)
        local = bbb.LocalPath(tmpdir) / path_obj.name
        async with bbb.BoostExecutor(DEFAULT_CONCURRENCY) as executor:
            try:
                await bbb.copyfile(path_obj, local, executor)
            except FileNotFoundError:
                print("File not found, creating new file...")
                with open(local, "w"):
                    pass
            pre_stat = await bbb.stat(local)
            editor = shlex.split(os.environ.get("EDITOR") or "vi")
            subprocess.check_call([*editor, local])
            post_stat = await bbb.stat(local)
            if pre_stat != post_stat:
                if read_only:
                    raise RuntimeError(
                        "File was edited, but read-only mode is enabled. "
                        "Edits were not persisted."
                    )
                else:
                    await bbb.copyfile(local, path_obj, executor, overwrite=True)
                    print(f"Updated {path_obj}")
            else:
                if not read_only:
                    print("File unmodified, skipping reupload...")


def complete_init(shell: str) -> None:
    if shell == "zsh":
        # zsh uses index-1 based arrays, so adjust CURRENT
        # the (f) things splits the output on newlines into an array
        # -U disables prefix matching
        # -S '' prevents zsh from inserting a space after the completion
        init_script = """\
_bbb_complete() {
    compadd -U  -S '' ${(f)"$(bbb complete command zsh $(( $CURRENT - 1)) $words)"}
}
compdef _bbb_complete bbb
"""
    elif shell == "bash":
        # use COMP_LINE instead of COMP_WORDS because bash uncustomisably splits words on colons
        # -o nospace prevents bash from inserting a space after the completion
        # life would be simpler if we did COMP_WORDBREAKS=${COMP_WORDBREAKS//:}
        init_script = """\
_bbb_complete() {
    local completions="$(bbb complete command bash $COMP_POINT "$COMP_LINE")"
    COMPREPLY=( $(compgen -W "$completions" '') )
}
complete -o nospace -F _bbb_complete bbb
"""
    else:
        raise ValueError(f"Unrecognised shell {shell}")
    print(init_script)


@sync_with_session
async def complete_command(shell: str, index: int, partial_command: List[str]) -> None:
    if shell == "bash":
        # the entire command is passed as a single argument and index is an index into that string
        assert len(partial_command) == 1
        command_str = partial_command[0]
        partial_command = command_str.split()
        if index == len(command_str):
            index = len(partial_command) - (0 if command_str[index - 1] == " " else 1)
        else:
            return  # TODO: support bash completion in the middle of a command

    if index <= 1:
        # TODO: support completion of subcommands
        return

    # assume we're trying to complete a path; just add a wildcard
    word_to_complete = partial_command[index] if index < len(partial_command) else ""
    path_to_complete = word_to_complete.lstrip("'\"")
    if not path_to_complete.endswith("*"):
        path_to_complete += "*"

    try:
        async for entry in bbb.listing.glob_scandir(path_to_complete):
            # bash won't let you complete before a colon without setting COMP_WORDBREAKS globally
            # instead, we just make sure our completion matches the part before the colon
            # to do that, we special case az:// paths
            if isinstance(entry.path, bbb.AzurePath):
                if path_to_complete.startswith("az://"):
                    path_str = entry.path.to_az_str()
                else:
                    path_str = entry.path.to_https_str()
            else:
                path_str = str(entry.path)

            # bash won't split on a colon inside a quoted argument, though
            if shell == "bash" and not word_to_complete.startswith(("'", '"')):
                try:
                    path_str = path_str[path_str.index(":") + 1 :]
                except ValueError:
                    pass
            print(path_str)
    except Exception:
        # ignore errors, usually
        if os.environ.get("BBB_DEBUG"):
            raise


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

To show relative paths:
$ bbb ls --relative .

Aliases:
bbb ll == bbb ls -l
bbb ll -s == bbb ls --long --relative
"""
    lstree_desc = """\
`bbb lstree` lists all files present anywhere in the given directory tree.
Note that `bbb lstree` only lists files and will not list subdirectories
(unless they are marked by marker files).

To see all files in your bucket:
$ bbb lstree gs://my_bucket/

To additionally see size and mtime of files:
$ bbb lstree -l gs://my_bucket/

To make output more easily machine readable:
$ bbb lstree --machine .

To show relative paths:
$ bbb lstree --relative .

Aliases:
bbb lsr == bbb lstree
bbb llr == bbb lstree -l == bbb lsr -l == bbb du
bbb lsr -s == bbb lstree --relative
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

Aliases:
bbb cpr == bbb cptree
"""
    edit_desc = """\
`bbb edit` lets you edit a file in a text editor. It makes a local copy of the
file, opens it in an editor (determined by the $EDITOR environment variable),
then overwrites the original file with the edited local copy.

Example:
$ bbb edit gs://bucket/frogs.txt
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
files in the destination that are not present in the source. If --exclude (-x)
is specified, bbb will not copy or delete any files whose relative path matches
the given regex at any position (i.e. using the semantics of Python's
re.search). Consider using regex anchors (^$) to ensure you're excluding exactly
what you intend.

Example:
$ bbb sync gs://tmp/boostedblob boostedblob

Using --delete and re-syncing will delete spurious_file.txt:
$ touch boostedblob/spurious_file.txt
$ bbb sync --delete gs://tmp/boostedblob boostedblob

Sync all files except those with .txt extension
$ bbb sync gs://tmp/boostedblob boostedblob -x '\\.txt$'
"""
    complete_desc = """\
To enable tab completion for bash, add the following to your bashrc:
eval "$(bbb complete init bash)"

To enable tab completion for zsh, add the following to your zshrc:
eval "$(bbb complete init zsh)"
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
    subparser.add_argument(
        "--machine", action="store_true", help="Make output more easily machine readable"
    )
    subparser.add_argument("-s", "--relative", action="store_true", help="Show relative paths")

    subparser = subparsers.add_parser(
        "ll", formatter_class=argparse.RawDescriptionHelpFormatter, description=ls_desc
    )
    subparser.set_defaults(command=functools.partial(ls, long=True))
    subparser.add_argument("path", help="Path of directory to list")
    subparser.add_argument(
        "--machine", action="store_true", help="Make output more easily machine readable"
    )
    subparser.add_argument("-s", "--relative", action="store_true", help="Show relative paths")

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
    subparser.add_argument(
        "--machine", action="store_true", help="Make output more easily machine readable"
    )
    subparser.add_argument("-s", "--relative", action="store_true", help="Show relative paths")

    subparser = subparsers.add_parser(
        "llr",
        aliases=["du"],
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=lstree_desc,
    )
    subparser.set_defaults(command=functools.partial(lstree, long=True))
    subparser.add_argument("path", help="Root of directory tree to list")
    subparser.add_argument(
        "--machine", action="store_true", help="Make output more easily machine readable"
    )
    subparser.add_argument("-s", "--relative", action="store_true", help="Show relative paths")

    subparser = subparsers.add_parser("_dud1")
    subparser.set_defaults(command=_dud1)
    subparser.add_argument("path", help="Path of directory to list")

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
    subparser.add_argument("-q", "--quiet", action="store_true")
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
        "edit",
        help="Edit a file in a local editor",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=edit_desc,
    )
    subparser.set_defaults(command=edit)
    subparser.add_argument("path")
    subparser.add_argument("-r", "--read-only", action="store_true", help="Open file read-only")

    subparser = subparsers.add_parser(
        "rm",
        help="Remove files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=rm_desc,
    )
    subparser.set_defaults(command=rm)
    subparser.add_argument("paths", nargs="+", help="File(s) to delete")
    subparser.add_argument("-q", "--quiet", action="store_true")
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
    subparser.add_argument(
        "-x",
        "--exclude",
        help=(
            "Only copy or delete files whose relative path don't match this Python regular "
            "expression at any position"
        ),
    )
    subparser.add_argument("-q", "--quiet", action="store_true")
    subparser.add_argument("--concurrency", **concurrency_kwargs)

    subparser = subparsers.add_parser("_xrp")
    subparser.set_defaults(command=_xxx_recoverprefix)
    subparser.add_argument("prefix", help="Prefix of globs to recover")
    subparser.add_argument("restore_dt", help="GMT datetime to recover to")
    subparser.add_argument("--dry-run", type=lambda x: not x or x[0].lower() != "f", default=True)

    subparser = subparsers.add_parser(
        "complete",
        help="Tab complete a command",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=complete_desc,
    )
    subsubparsers = subparser.add_subparsers(required=True)
    subsubparser = subsubparsers.add_parser(
        "init", help="Print shell script to initialise tab completion"
    )
    subsubparser.set_defaults(command=complete_init)
    subsubparser.add_argument("shell", choices=["bash", "zsh"])

    subsubparser = subsubparsers.add_parser("command", help="Get a completion for a command")
    subsubparser.set_defaults(command=complete_command)
    subsubparser.add_argument("shell", help="Shell to complete for")
    subsubparser.add_argument("index", type=int, help="Index into partial_command to complete")
    subsubparser.add_argument(
        "partial_command", nargs=argparse.REMAINDER, help="Command to complete"
    )

    if not args:
        parser.print_help()
        print()
        parser.error("missing subcommand, see `bbb --help`")
    return parser.parse_args(args)


def run_bbb(argv: List[str]) -> None:
    try:
        args = parse_options(argv)
        command = args.__dict__.pop("command")
        command(**args.__dict__)
    except Exception as e:
        print(f"ERROR: {type(e).__name__}: {e}", file=sys.stderr)
        raise
