"""Tests for Azure cloud configuration and ARM metadata discovery."""

from __future__ import annotations

import json
import os
from unittest.mock import patch

import pytest

from boostedblob.azure_cloud import (
    AZURE_PUBLIC_CLOUD,
    AZURE_US_GOV_CLOUD,
    AzureCloudConfig,
    _cloud_config_from_metadata,
    _select_cloud_from_metadata,
    get_cloud_config,
)


# Sample ARM metadata response (matches real response format)
SAMPLE_ARM_METADATA = [
    {
        "name": "AzureCloud",
        "authentication": {
            "loginEndpoint": "https://login.microsoftonline.com/",
            "audiences": ["https://management.core.windows.net/", "https://management.azure.com/"],
        },
        "resourceManager": "https://management.azure.com/",
        "suffixes": {
            "storage": "core.windows.net",
            "keyVaultDns": "vault.azure.net",
        },
    },
    {
        "name": "AzureUSGovernment",
        "authentication": {
            "loginEndpoint": "https://login.microsoftonline.us",
            "audiences": ["https://management.core.usgovcloudapi.net"],
        },
        "resourceManager": "https://management.usgovcloudapi.net",
        "suffixes": {
            "storage": "core.usgovcloudapi.net",
            "keyVaultDns": "vault.usgovcloudapi.net",
        },
    },
]


class TestAzureCloudConfig:
    def test_blob_endpoint_url(self):
        assert (
            AZURE_PUBLIC_CLOUD.blob_endpoint_url("myacct")
            == "https://myacct.blob.core.windows.net"
        )
        assert (
            AZURE_US_GOV_CLOUD.blob_endpoint_url("myacct")
            == "https://myacct.blob.core.usgovcloudapi.net"
        )

    def test_storage_scope_generic(self):
        assert AZURE_PUBLIC_CLOUD.storage_scope() == "https://storage.azure.com/.default"
        assert AZURE_US_GOV_CLOUD.storage_scope() == "https://storage.azure.com/.default"

    def test_storage_scope_account_specific(self):
        assert (
            AZURE_PUBLIC_CLOUD.storage_scope("myacct")
            == "https://myacct.blob.core.windows.net/.default"
        )
        assert (
            AZURE_US_GOV_CLOUD.storage_scope("myacct")
            == "https://myacct.blob.core.usgovcloudapi.net/.default"
        )


class TestPresets:
    def test_public_cloud_values(self):
        assert AZURE_PUBLIC_CLOUD.storage_endpoint_suffix == "core.windows.net"
        assert AZURE_PUBLIC_CLOUD.login_endpoint == "https://login.microsoftonline.com"
        assert AZURE_PUBLIC_CLOUD.arm_endpoint == "https://management.azure.com"

    def test_us_gov_cloud_values(self):
        assert AZURE_US_GOV_CLOUD.storage_endpoint_suffix == "core.usgovcloudapi.net"
        assert AZURE_US_GOV_CLOUD.login_endpoint == "https://login.microsoftonline.us"
        assert AZURE_US_GOV_CLOUD.arm_endpoint == "https://management.usgovcloudapi.net"


class TestCloudConfigFromMetadata:
    def test_parses_public_cloud(self):
        config = _cloud_config_from_metadata(SAMPLE_ARM_METADATA[0])
        assert config.storage_endpoint_suffix == "core.windows.net"
        assert config.login_endpoint == "https://login.microsoftonline.com"
        assert config.arm_endpoint == "https://management.azure.com"

    def test_parses_us_gov_cloud(self):
        config = _cloud_config_from_metadata(SAMPLE_ARM_METADATA[1])
        assert config.storage_endpoint_suffix == "core.usgovcloudapi.net"
        assert config.login_endpoint == "https://login.microsoftonline.us"
        assert config.arm_endpoint == "https://management.usgovcloudapi.net"

    def test_strips_trailing_slash(self):
        entry = {
            "name": "TestCloud",
            "authentication": {"loginEndpoint": "https://login.example.com/"},
            "resourceManager": "https://management.example.com/",
            "suffixes": {"storage": "core.example.net"},
        }
        config = _cloud_config_from_metadata(entry)
        assert config.login_endpoint == "https://login.example.com"
        assert config.arm_endpoint == "https://management.example.com"


class TestSelectCloudFromMetadata:
    def test_select_by_name(self):
        entry = _select_cloud_from_metadata(SAMPLE_ARM_METADATA, "AzureUSGovernment")
        assert entry["name"] == "AzureUSGovernment"

    def test_select_first_when_no_name(self):
        entry = _select_cloud_from_metadata(SAMPLE_ARM_METADATA, None)
        assert entry["name"] == "AzureCloud"

    def test_raises_on_unknown_name(self):
        with pytest.raises(ValueError, match="not found in ARM metadata"):
            _select_cloud_from_metadata(SAMPLE_ARM_METADATA, "AzureNonExistent")

    def test_raises_on_empty_metadata(self):
        with pytest.raises(ValueError, match="no cloud definitions"):
            _select_cloud_from_metadata([], None)


class TestGetCloudConfig:
    def test_default_is_public_cloud(self):
        with patch.dict(os.environ, {}, clear=True):
            # Remove any cloud env vars
            for key in ["ARM_CLOUD_METADATA_URL", "AZURE_CLOUD"]:
                os.environ.pop(key, None)
            config = get_cloud_config()
        assert config == AZURE_PUBLIC_CLOUD

    def test_azure_cloud_selects_preset(self):
        with patch.dict(os.environ, {"AZURE_CLOUD": "AzureUSGovernment"}, clear=False):
            os.environ.pop("ARM_CLOUD_METADATA_URL", None)
            config = get_cloud_config()
        assert config == AZURE_US_GOV_CLOUD

    def test_azure_cloud_unknown_raises(self):
        with patch.dict(os.environ, {"AZURE_CLOUD": "UnknownCloud"}, clear=False):
            os.environ.pop("ARM_CLOUD_METADATA_URL", None)
            with pytest.raises(ValueError, match="Unknown cloud"):
                get_cloud_config()

    def test_arm_metadata_url_fetches_and_selects(self):
        with patch.dict(
            os.environ,
            {
                "ARM_CLOUD_METADATA_URL": "https://management.example.com/metadata/endpoints?api-version=2019-05-01",
                "AZURE_CLOUD": "AzureUSGovernment",
            },
            clear=False,
        ):
            with patch(
                "boostedblob.azure_cloud._fetch_arm_cloud_metadata",
                return_value=SAMPLE_ARM_METADATA,
            ) as mock_fetch:
                config = get_cloud_config()

            mock_fetch.assert_called_once_with(
                "https://management.example.com/metadata/endpoints?api-version=2019-05-01"
            )
            assert config.storage_endpoint_suffix == "core.usgovcloudapi.net"

    def test_arm_metadata_url_defaults_to_first_cloud(self):
        with patch.dict(
            os.environ,
            {"ARM_CLOUD_METADATA_URL": "https://example.com/metadata"},
            clear=False,
        ):
            os.environ.pop("AZURE_CLOUD", None)
            with patch(
                "boostedblob.azure_cloud._fetch_arm_cloud_metadata",
                return_value=SAMPLE_ARM_METADATA,
            ):
                config = get_cloud_config()

            assert config.storage_endpoint_suffix == "core.windows.net"

    def test_arm_metadata_url_takes_priority_over_preset(self):
        """ARM_CLOUD_METADATA_URL should be used even if AZURE_CLOUD matches a preset."""
        custom_metadata = [
            {
                "name": "AzureUSGovernment",
                "authentication": {"loginEndpoint": "https://custom.login.example"},
                "resourceManager": "https://custom.arm.example",
                "suffixes": {"storage": "custom.storage.example"},
            }
        ]
        with patch.dict(
            os.environ,
            {
                "ARM_CLOUD_METADATA_URL": "https://example.com/metadata",
                "AZURE_CLOUD": "AzureUSGovernment",
            },
            clear=False,
        ):
            with patch(
                "boostedblob.azure_cloud._fetch_arm_cloud_metadata",
                return_value=custom_metadata,
            ):
                config = get_cloud_config()

            # Should use metadata values, not built-in preset
            assert config.storage_endpoint_suffix == "custom.storage.example"
            assert config.login_endpoint == "https://custom.login.example"
