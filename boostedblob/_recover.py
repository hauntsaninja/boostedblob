"""

Experimental code to recover deleted or overwritten data in Azure.

This has been used and should work, but isn't tested to the degree something like this should be.

"""

import datetime
import urllib.parse
from typing import Any, DefaultDict, Dict, List, Union

from .boost import BoostExecutor
from .path import AzurePath
from .request import Request, azure_auth_req, xml_page_iterator
from .xml import etree


def _xml_to_dict(element: etree.Element) -> Any:
    # This is a hack, but works fine here
    if len(element) == 0:
        return element.text or ""
    else:
        return {e.tag: _xml_to_dict(e) for e in element}


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

    it = xml_page_iterator(
        Request(
            method="GET",
            url=prefix.format_url("https://{account}.blob.core.windows.net/{container}"),
            params=dict(
                comp="list",
                restype="container",
                prefix=prefix.blob,
                include="deleted,snapshots,versions",
            ),
            auth=azure_auth_req,
        )
    )

    results = DefaultDict[AzurePath, List[Dict[str, Any]]](list)
    async for result in it:
        blobs = result.find("Blobs")
        assert blobs is not None
        for b in blobs.iterfind("Blob"):
            b_dict = _xml_to_dict(b)
            name: str = b_dict["Name"]
            results[AzurePath(prefix.account, prefix.container, name)].append(b_dict)
    return results


async def _undelete(path: AzurePath) -> AzurePath:
    request = Request(
        method="PUT",
        url=path.format_url("https://{account}.blob.core.windows.net/{container}/{blob}"),
        params=dict(comp="undelete"),
        success_codes=(200,),
        auth=azure_auth_req,
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
    request = Request(
        method="PUT",
        url=url,
        headers={"x-ms-copy-source": url + "?" + source},
        success_codes=(202,),
        auth=azure_auth_req,
    )
    async with request.execute() as resp:
        copy_status = resp.headers["x-ms-copy-status"]
        assert copy_status == "success"


async def _delete_snapshot(path: AzurePath, snapshot: str) -> None:
    request = Request(
        method="DELETE",
        url=path.format_url("https://{account}.blob.core.windows.net/{container}/{blob}"),
        params={"snapshot": snapshot},
        success_codes=(202,),
        auth=azure_auth_req,
    )
    await request.execute_reponseless()


async def _recover_snapshot(
    path: AzurePath, candidate: Dict[str, Any], alternatives: List[Dict[str, Any]]
) -> None:
    # https://docs.microsoft.com/en-us/azure/storage/blobs/soft-delete-blob-overview#restoring-soft-deleted-objects
    await _undelete(path)
    if "Snapshot" in candidate:
        await _promote_candidate(path, candidate)
    # undelete will undelete all snapshots, so re-delete all previously deleted snapshots
    # (yes, yes, awaiting in a loop)
    if "Snapshot" in candidate and candidate.get("Deleted") == "true":
        await _delete_snapshot(path, candidate["Snapshot"])
    for b in alternatives:
        if "Snapshot" in b and b.get("Deleted") == "true":
            await _delete_snapshot(path, b["Snapshot"])


async def _determine_candidate_and_recover(
    path: AzurePath, restore_ts: str, versions_snapshots: List[Dict[str, Any]], dry_run: bool = True
) -> str:
    if any("VersionId" in b for b in versions_snapshots):
        # If using versioning, I think all blobs should have versions
        assert all("VersionId" in b for b in versions_snapshots)
        versions = versions_snapshots

        # VersionId marks the time that version of the blob was created
        versions.sort(key=lambda b: b["VersionId"], reverse=True)
        candidate_index = next(
            (i for i in range(len(versions)) if versions[i]["VersionId"] <= restore_ts), None
        )
        should_recover = (
            candidate_index is not None
            and versions[candidate_index].get("IsCurrentVersion") != "true"
        )

        if should_recover:
            assert candidate_index is not None
            candidate_desc = repr(versions[candidate_index]["VersionId"])
            desc = f"Recovering {path} to {candidate_desc}"
        else:
            desc = f"Not attempting to recover {path}"

        alternatives = [b for i, b in enumerate(versions) if i != candidate_index]
        alternatives_desc = ", ".join(repr(b["VersionId"]) for b in alternatives)
        if alternatives:
            desc += f" Alternatives include {alternatives_desc}"

        if dry_run or not should_recover:
            return desc

        assert candidate_index is not None
        await _promote_candidate(path, versions[candidate_index])
        return desc
    else:
        # If using snapshotting, all except one of them should be snapshots
        assert sum(1 for b in versions_snapshots if "Snapshot" not in b) == 1
        snapshots = versions_snapshots

        # Snapshot marks the time that version of the blob was obsoleted (I think)
        # Note the logic and sorting is different from the versioning case
        snapshots.sort(key=lambda b: b.get("Snapshot", "9999-99-99"))

        candidate_index = next(
            i
            for i in range(len(snapshots))
            if snapshots[i].get("Snapshot", "9999-99-99") >= restore_ts
        )
        should_recover = (
            "Snapshot" in snapshots[candidate_index]
            or snapshots[candidate_index].get("Deleted") == "true"
        )

        def _to_ts_desc(b: Dict[str, Any]) -> str:
            if "Snapshot" in b:
                return b["Snapshot"]
            assert candidate_index is not None
            deleted_time = snapshots[candidate_index]["Properties"]["DeletedTime"]
            return datetime.datetime.strptime(deleted_time, "%a, %d %b %Y %H:%M:%S GMT").strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )

        if should_recover:
            candidate_desc = repr(_to_ts_desc(snapshots[candidate_index]))
            desc = f"Recovering {path} to {candidate_desc}"
        else:
            desc = f"Not attempting to recover {path}"

        alternatives = [b for i, b in enumerate(snapshots) if i != candidate_index]
        alternatives_desc = ", ".join(repr(_to_ts_desc(b)) for b in alternatives)
        if alternatives:
            desc += f" Alternatives include {alternatives_desc}"

        if dry_run or not should_recover:
            return desc

        assert candidate_index is not None
        await _recover_snapshot(path, snapshots[candidate_index], alternatives)
        return desc


async def _recoverprefix(
    prefix: Union[str, AzurePath],
    restore_dt: Union[str, datetime.datetime],
    executor: BoostExecutor,
    dry_run: bool = True,
) -> None:
    """Attempt to recover blobs with a prefix to prevous snapshots or versions.

    For each blob, attempts to restore to the most recent version or snapshot that is older than or
    equal to restore_dt. Note that this isn't a point-in-time restore, e.g. it won't delete
    blobs that were added after restore_dt, or it may restore blobs that were actually only deleted
    after restore_dt.

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

    print()
    print("This code is experimental, please verify that it actually does what you want.")
    if dry_run:
        print("Dry run, no actions actually taken.")
