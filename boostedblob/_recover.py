"""

Experimental code to recover deleted or overwritten data in Azure.

This has been used and should work, but isn't tested to the degree something like this should be.

"""
import datetime
import urllib.parse
from typing import Any, DefaultDict, Dict, List, Union

from .boost import BoostExecutor
from .path import AzurePath
from .request import Request, azure_page_iterator, azurify_request


async def _listtree_versions_snapshots(
    prefix: Union[str, AzurePath]
) -> Dict[AzurePath, List[Dict[str, Any]]]:
    if isinstance(prefix, str):
        prefix = AzurePath.from_str(prefix)

    # Azure has both "snapshots" and "versions"
    # They are very similar, because Azure likes to keep things confusing

    # We assume one of the following two situations:
    # 1) Soft-delete is enabled, but versioning is not (preferred)
    # 2) Soft-delete and versioning is enabled
    # https://docs.microsoft.com/en-us/azure/storage/blobs/soft-delete-blob-overview
    # https://docs.microsoft.com/en-us/azure/storage/blobs/soft-delete-blob-overview#blob-soft-delete-and-versioning

    it = azure_page_iterator(
        Request(
            method="GET",
            url=prefix.format_url("https://{account}.blob.core.windows.net/{container}"),
            params=dict(
                comp="list",
                restype="container",
                prefix=prefix.blob,
                include="deleted,snapshots,versions",
            ),
        )
    )

    results = DefaultDict[AzurePath, List[Dict[str, Any]]](list)
    async for result in it:
        blobs = result["Blobs"]
        if blobs is None:
            continue
        if "Blob" in blobs:
            if isinstance(blobs["Blob"], dict):
                blobs["Blob"] = [blobs["Blob"]]
            for b in blobs["Blob"]:
                results[AzurePath(prefix.account, prefix.container, b["Name"])].append(b)
    return results


async def _undelete(path: AzurePath) -> AzurePath:
    request = await azurify_request(
        Request(
            method="PUT",
            url=path.format_url("https://{account}.blob.core.windows.net/{container}/{blob}"),
            params=dict(comp="undelete"),
            success_codes=(200,),
        )
    )
    await request.execute_reponseless()
    return path


async def _promote_candidate(path: AzurePath, candidate: Dict[str, Any]) -> None:
    if "Snapshot" in candidate:
        source = "snapshot=" + urllib.parse.quote(candidate["Snapshot"])
    elif "VersionId" in candidate:
        source = "versionid=" + urllib.parse.quote(candidate["VersionId"])
    else:
        raise AssertionError
    url = path.format_url("https://{account}.blob.core.windows.net/{container}/{blob}")
    request = await azurify_request(
        Request(
            method="PUT",
            url=url,
            headers={"x-ms-copy-source": url + "?" + source},
            success_codes=(202,),
        )
    )
    async with request.execute() as resp:
        copy_status = resp.headers["x-ms-copy-status"]
        assert copy_status == "success"


async def _delete_snapshot(path: AzurePath, snapshot: str) -> None:
    request = await azurify_request(
        Request(
            method="DELETE",
            url=path.format_url("https://{account}.blob.core.windows.net/{container}/{blob}"),
            params={"snapshot": snapshot},
            success_codes=(202,),
        )
    )
    await request.execute_reponseless()


async def _recover_candidate(
    path: AzurePath, candidate: Dict[str, Any], alternatives: List[Dict[str, Any]]
) -> None:
    # https://docs.microsoft.com/en-us/azure/storage/blobs/soft-delete-blob-overview#restoring-soft-deleted-objects
    await _undelete(path)
    await _promote_candidate(path, candidate)
    if "Snapshot" in candidate:
        # undelete will undelete all snapshots, so re-delete all previously deleted snapshots
        # don't do anything for versions, because we assume someone else will take care of them
        # yes, yes, awaiting in a loop
        if candidate.get("Deleted") == "true":
            await _delete_snapshot(path, candidate["Snapshot"])
        for b in alternatives:
            if "Snapshot" in b and b.get("Deleted") == "true":
                await _delete_snapshot(path, b["Snapshot"])


async def _determine_candidate_and_recover(
    path: AzurePath, restore_ts: str, versions_snapshots: List[Dict[str, Any]], dry_run: bool = True
) -> str:
    def _to_ts(b: Dict[str, Any]) -> str:
        return b.get("Snapshot") or b.get("VersionId") or "9999-99-99"

    versions_snapshots.sort(key=_to_ts, reverse=True)
    candidate_index = next(
        (i for i in range(len(versions_snapshots)) if _to_ts(versions_snapshots[i]) <= restore_ts),
        None,
    )
    should_recover = (
        candidate_index is not None
        and versions_snapshots[candidate_index].get("IsCurrentVersion") != "true"
    )

    def _to_ts_desc(b: Dict[str, Any]) -> str:
        return repr(b.get("Snapshot") or b.get("VersionId") or "current")

    if should_recover:
        assert candidate_index is not None
        candidate_desc = _to_ts_desc(versions_snapshots[candidate_index])
        desc = f"Recovering {path} to {candidate_desc}."
    else:
        desc = f"Not attempting to recover {path}."

    alternatives = [b for i, b in enumerate(versions_snapshots) if i != candidate_index]
    alternatives_desc = ", ".join(map(_to_ts_desc, alternatives))
    desc += f" Alternatives include {alternatives_desc}."

    if dry_run or not should_recover:
        return desc

    assert candidate_index is not None
    await _recover_candidate(path, versions_snapshots[candidate_index], alternatives)
    return desc


async def _recoverprefix(
    prefix: Union[str, AzurePath],
    restore_dt: Union[str, datetime.datetime],
    executor: BoostExecutor,
    dry_run: bool = True,
) -> None:
    """Attempt to recover blobs with a prefix to prevous snapshots or versions.

    For each blob, attempts to restore to the most recent snapshot or version that is older than or
    equal to restore_dt. Note that this isn't quite a point-in-time restore, e.g. it won't delete
    blobs that have since been added.

    Please try to avoid blindly trusting this!
    """
    if restore_dt == "current":
        # mostly just useful for seeing the snapshots / versions that exist
        restore_dt = datetime.datetime.now(datetime.timezone.utc)
    if isinstance(restore_dt, str):
        restore_ts = restore_dt
    else:
        assert restore_dt.tzinfo is datetime.timezone.utc
        restore_ts = restore_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    versions_snapshots = await _listtree_versions_snapshots(prefix)
    async for desc in executor.map_unordered(
        lambda p_vs: _determine_candidate_and_recover(
            path=p_vs[0], restore_ts=restore_ts, versions_snapshots=p_vs[1], dry_run=dry_run
        ),
        iter(versions_snapshots.items()),
    ):
        print(desc)

    print("This code is experimental, please verify that it actually does what you want.")
    if dry_run:
        print("Dry run, no actions actually taken.")
