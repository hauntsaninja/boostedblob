import asyncio
import os
import re
import sys
from dataclasses import dataclass
from typing import AsyncIterator, Iterator, List, Optional, Tuple, Union

from .boost import BoostExecutor
from .copying import copyfile
from .delete import remove
from .listing import DirEntry, scantree
from .path import BasePath, LocalPath, Stat


@dataclass(frozen=True)
class Action:
    relpath: str


@dataclass(frozen=True)
class CopyAction(Action):
    size: Optional[int]


@dataclass(frozen=True)
class DeleteAction(Action):
    pass


async def sync_action_iterator(
    src: BasePath, dst: BasePath, exclude: Optional[str] = None
) -> Iterator[Action]:
    """Yields the actions to take to sync the tree rooted at ``src`` to ``dst``.

    :param src: The root of the tree to copy from.
    :param dst: The root of the tree to copy to.

    """
    if isinstance(src, LocalPath):
        src = LocalPath(os.path.abspath(src.path))
    if isinstance(dst, LocalPath):
        dst = LocalPath(os.path.abspath(dst.path))

    try:
        exclude_pattern = re.compile(exclude) if exclude else None
    except re.error as e:
        raise ValueError(
            f"Failed to compile exclude pattern {repr(exclude)}: {e}\n"
            "Hint: exclude patterns should be Python regular expressions, not globs."
        ) from None

    async def collect_tree(tree: BasePath) -> List[Tuple[str, DirEntry]]:
        try:
            # Note that if you create marker files to mark directories, scantree will return
            # those. Filter by is_file to avoid syncing these files that represent directories.
            files = [(p.path.relative_to(tree), p) async for p in scantree(tree) if p.is_file]
            return [p for p in files if exclude_pattern is None or not exclude_pattern.search(p[0])]
        except FileNotFoundError:
            return []

    # We block on tree collection, rather than streaming actions, for fear that the list
    # operations might start to reflect our changes, causing weird things to happen.
    src_files, dst_files = await asyncio.gather(collect_tree(src), collect_tree(dst))
    return sync_files_action_iterator(src_files, dst_files)


def sync_files_action_iterator(
    src_files: List[Tuple[str, DirEntry]], dst_files: List[Tuple[str, DirEntry]]
) -> Iterator[Action]:
    src_files.sort(key=lambda p: p[0])
    dst_files.sort(key=lambda p: p[0])

    i = 0
    j = 0
    while i < len(src_files) or j < len(dst_files):
        if i < len(src_files) and (j == len(dst_files) or src_files[i][0] < dst_files[j][0]):
            src_stat = src_files[i][1].stat
            size = src_stat.size if src_stat else None
            yield CopyAction(src_files[i][0], size)
            i += 1
            continue
        if j < len(dst_files) and (i == len(src_files) or src_files[i][0] > dst_files[j][0]):
            yield DeleteAction(dst_files[j][0])
            j += 1
            continue
        if src_files[i][0] == dst_files[j][0]:
            if should_copy(src_files[i][1].stat, dst_files[j][1].stat):
                src_stat = src_files[i][1].stat
                size = src_stat.size if src_stat else None
                yield CopyAction(src_files[i][0], size)
            i += 1
            j += 1
            continue
        raise AssertionError


# ==============================
# sync
# ==============================


async def sync(
    src: Union[str, BasePath],
    dst: Union[str, BasePath],
    executor: BoostExecutor,
    delete: bool = False,
    exclude: Optional[str] = None,
) -> AsyncIterator[BasePath]:
    """Syncs the tree rooted at ``src`` to ``dst``.

    Yields the paths as they are copied or deleted.

    :param src: The root of the tree to sync from.
    :param dst: The root of the tree to sync to.
    :param executor: An executor.
    :param delete: Whether to delete files present in the destination but missing in the source.

    """
    src_obj = src if isinstance(src, BasePath) else BasePath.from_str(src)
    src_obj = src_obj.ensure_directory_like()
    dst_obj = dst if isinstance(dst, BasePath) else BasePath.from_str(dst)
    dst_obj = dst_obj.ensure_directory_like()

    if src_obj.is_relative_to(dst_obj) or dst_obj.is_relative_to(src_obj):
        raise ValueError("Cannot sync overlapping directories")

    async def copy_wrapper(relpath: str, size: Optional[int]) -> Optional[BasePath]:
        src_file = src_obj / relpath
        dst_file = dst_obj / relpath
        try:
            await copyfile(src_file, dst_file, executor, size=size, overwrite=True)
            return src_file
        except FileNotFoundError as e:
            print(
                "[boostedblob] File disappeared while syncing, ignoring. Likely due to "
                f"concurrent deletion: {e}",
                file=sys.stderr,
            )
        return None

    async def delete_wrapper(relpath: str) -> BasePath:
        dst_file = dst_obj / relpath
        await remove(dst_file)
        return dst_file

    async def action_wrapper(action: Action) -> Optional[BasePath]:
        if isinstance(action, CopyAction):
            return await copy_wrapper(action.relpath, action.size)
        if isinstance(action, DeleteAction):
            if delete:
                return await delete_wrapper(action.relpath)
        return None

    actions = executor.map_unordered(
        action_wrapper, await sync_action_iterator(src_obj, dst_obj, exclude=exclude)
    )
    async for path in actions:
        if path is not None:
            yield path


def should_copy(src_stat: Optional[Stat], dst_stat: Optional[Stat]) -> bool:
    if src_stat is None and dst_stat is None:
        return False
    if src_stat is None or dst_stat is None:
        # We'll only run into this if a cloud directory has a file marker. For most cases in which
        # we're asked to overwrite directories with files or vice versa, we'll either get an
        # IsADirectoryError / NotADirectoryError from local directories or no error because
        # directories are an illusion in cloud storage.
        raise IsADirectoryError(
            "Sync does not support overwriting directories with files or vice versa. "
            "Please rmtree your destination and try again"
        )
    if src_stat.size != dst_stat.size:
        return True
    if src_stat.md5 and dst_stat.md5:
        return src_stat.md5 != dst_stat.md5
    # Round mtime, since different stores have different precisions
    if int(src_stat.mtime) >= int(dst_stat.mtime):
        return True
    # If hashes are unavailable, sizes are the same and the dst file has a newer mtime
    # than the src file, we take our chances.
    return False
