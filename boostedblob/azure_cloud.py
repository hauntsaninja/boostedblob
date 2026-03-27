"""Azure cloud configuration with ARM metadata discovery.

Supports multiple Azure clouds (Public, US Government, airgapped) by discovering
endpoints from the ARM cloud metadata endpoint, following the same pattern as
azure-ai-ml's ARM_CLOUD_METADATA_URL.

Configuration resolution order:
    1. ARM_CLOUD_METADATA_URL env var set → fetch metadata, select cloud by AZURE_CLOUD
    2. AZURE_CLOUD env var set (no metadata URL) → use built-in preset
    3. Default → public cloud preset (no network call)
"""

from __future__ import annotations

import json
import urllib.request
from dataclasses import dataclass

# Storage OAuth audience is the same across all Azure clouds
STORAGE_AUDIENCE = "https://storage.azure.com"

ARM_METADATA_API_VERSION = "2019-05-01"


@dataclass(frozen=True)
class AzureCloudConfig:
    """Azure cloud endpoint configuration."""

    storage_endpoint_suffix: str
    login_endpoint: str
    arm_endpoint: str

    def blob_endpoint_url(self, account: str) -> str:
        return f"https://{account}.blob.{self.storage_endpoint_suffix}"

    def storage_scope(self, account: str | None = None) -> str:
        if account:
            return f"https://{account}.blob.{self.storage_endpoint_suffix}/.default"
        return f"{STORAGE_AUDIENCE}/.default"


def _strip_trailing_slash(url: str) -> str:
    return url.rstrip("/")


# Built-in presets matching ARM metadata for known clouds
AZURE_PUBLIC_CLOUD = AzureCloudConfig(
    storage_endpoint_suffix="core.windows.net",
    login_endpoint="https://login.microsoftonline.com",
    arm_endpoint="https://management.azure.com",
)

AZURE_US_GOV_CLOUD = AzureCloudConfig(
    storage_endpoint_suffix="core.usgovcloudapi.net",
    login_endpoint="https://login.microsoftonline.us",
    arm_endpoint="https://management.usgovcloudapi.net",
)

_CLOUD_PRESETS: dict[str, AzureCloudConfig] = {
    "AzureCloud": AZURE_PUBLIC_CLOUD,
    "AzureUSGovernment": AZURE_US_GOV_CLOUD,
}


def _fetch_arm_cloud_metadata(metadata_url: str) -> list[dict]:
    """Fetch cloud definitions from the ARM metadata endpoint (no auth required)."""
    req = urllib.request.Request(metadata_url, headers={"User-Agent": "boostedblob"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def _cloud_config_from_metadata(cloud_entry: dict) -> AzureCloudConfig:
    """Extract an AzureCloudConfig from a single ARM metadata cloud entry."""
    return AzureCloudConfig(
        storage_endpoint_suffix=cloud_entry["suffixes"]["storage"],
        login_endpoint=_strip_trailing_slash(cloud_entry["authentication"]["loginEndpoint"]),
        arm_endpoint=_strip_trailing_slash(cloud_entry["resourceManager"]),
    )


def _select_cloud_from_metadata(
    metadata: list[dict], cloud_name: str | None
) -> dict:
    """Select a cloud entry from the metadata array by name."""
    if cloud_name:
        for entry in metadata:
            if entry.get("name") == cloud_name:
                return entry
        available = [e.get("name", "unknown") for e in metadata]
        raise ValueError(
            f"Cloud '{cloud_name}' not found in ARM metadata. "
            f"Available clouds: {available}"
        )
    # Default to first entry
    if not metadata:
        raise ValueError("ARM metadata response contained no cloud definitions")
    return metadata[0]


def get_cloud_config() -> AzureCloudConfig:
    """Resolve cloud configuration from environment variables.

    Resolution order:
        1. ARM_CLOUD_METADATA_URL set → fetch metadata, select by AZURE_CLOUD or first entry
        2. AZURE_CLOUD set (no metadata URL) → use built-in preset
        3. Default → AZURE_PUBLIC_CLOUD
    """
    import os

    metadata_url = os.environ.get("ARM_CLOUD_METADATA_URL")
    cloud_name = os.environ.get("AZURE_CLOUD")

    if metadata_url:
        metadata = _fetch_arm_cloud_metadata(metadata_url)
        cloud_entry = _select_cloud_from_metadata(metadata, cloud_name)
        return _cloud_config_from_metadata(cloud_entry)

    if cloud_name:
        if cloud_name in _CLOUD_PRESETS:
            return _CLOUD_PRESETS[cloud_name]
        raise ValueError(
            f"Unknown cloud '{cloud_name}'. Known clouds: {list(_CLOUD_PRESETS.keys())}. "
            f"Set ARM_CLOUD_METADATA_URL to discover endpoints for custom clouds."
        )

    return AZURE_PUBLIC_CLOUD
