from __future__ import annotations

import os
import sys
import urllib.error
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from boostedblob import azure_auth
from boostedblob.azure_cloud import (
    AZURE_PUBLIC_CLOUD,
    AZURE_US_GOV_CLOUD,
    _arm_endpoint_from_metadata_url,
    _cloud_config_from_metadata,
    _fetch_arm_cloud_metadata,
    _select_cloud_from_metadata,
    get_cloud_config,
)
from boostedblob.globals import Config, config, configure

SAMPLE_ARM_METADATA = [
    {
        "name": "AzureCloud",
        "authentication": {
            "loginEndpoint": "https://login.microsoftonline.com/",
            "audiences": ["https://management.core.windows.net/", "https://management.azure.com/"],
        },
        "resourceManager": "https://management.azure.com/",
        "suffixes": {"storage": "core.windows.net", "keyVaultDns": "vault.azure.net"},
    },
    {
        "name": "AzureUSGovernment",
        "authentication": {
            "loginEndpoint": "https://login.microsoftonline.us",
            "audiences": ["https://management.core.usgovcloudapi.net"],
        },
        "resourceManager": "https://management.usgovcloudapi.net",
        "suffixes": {"storage": "core.usgovcloudapi.net", "keyVaultDns": "vault.usgovcloudapi.net"},
    },
]


class TestAzureCloudHelpers:
    def test_arm_endpoint_from_metadata_url(self):
        assert (
            _arm_endpoint_from_metadata_url(
                "https://management.azure.com/metadata/endpoints?api-version=2019-05-01"
            )
            == "https://management.azure.com"
        )
        assert (
            _arm_endpoint_from_metadata_url(
                "https://management.usgovcloudapi.net/metadata/endpoints?api-version=2019-05-01"
            )
            == "https://management.usgovcloudapi.net"
        )

    def test_fetch_arm_cloud_metadata_uses_supplied_url(self):
        response = MagicMock()
        response.read.return_value = b'[{"name":"AzureCloud","authentication":{"loginEndpoint":"https://login.microsoftonline.com/"},"resourceManager":"https://management.azure.com/","suffixes":{"storage":"core.windows.net"}}]'
        response.__enter__.return_value = response
        response.__exit__.return_value = False

        with patch("urllib.request.urlopen", return_value=response) as mock_urlopen:
            metadata = _fetch_arm_cloud_metadata(
                "https://management.azure.com/metadata/endpoints?api-version=2019-05-01"
            )

        request = mock_urlopen.call_args.args[0]
        assert request.full_url == (
            "https://management.azure.com/metadata/endpoints?api-version=2019-05-01"
        )
        assert metadata[0]["name"] == "AzureCloud"

    def test_fetch_arm_cloud_metadata_network_error(self):
        with patch(
            "urllib.request.urlopen",
            side_effect=urllib.error.URLError("connection refused"),
        ):
            with pytest.raises(RuntimeError, match="Failed to fetch ARM cloud metadata"):
                _fetch_arm_cloud_metadata("https://management.invalid")

    def test_fetch_arm_cloud_metadata_invalid_json(self):
        response = MagicMock()
        response.read.return_value = b"{"
        response.__enter__.return_value = response
        response.__exit__.return_value = False

        with patch("urllib.request.urlopen", return_value=response):
            with pytest.raises(RuntimeError, match="was not valid JSON"):
                _fetch_arm_cloud_metadata("https://management.azure.com")

    def test_authority_host(self):
        assert AZURE_PUBLIC_CLOUD.authority_host == "login.microsoftonline.com"
        assert AZURE_US_GOV_CLOUD.authority_host == "login.microsoftonline.us"


class TestCloudConfigFromMetadata:
    def test_parses_public_cloud(self):
        cloud = _cloud_config_from_metadata(SAMPLE_ARM_METADATA[0])
        assert cloud == AZURE_PUBLIC_CLOUD

    def test_parses_us_gov_cloud(self):
        cloud = _cloud_config_from_metadata(SAMPLE_ARM_METADATA[1])
        assert cloud == AZURE_US_GOV_CLOUD

    def test_strips_trailing_slash(self):
        cloud = _cloud_config_from_metadata(SAMPLE_ARM_METADATA[0])
        assert cloud.login_endpoint == "https://login.microsoftonline.com"
        assert cloud.arm_endpoint == "https://management.azure.com"

    def test_missing_required_field_raises_clear_error(self):
        bad_entry = {
            "name": "AzureCloud",
            "authentication": {},
            "resourceManager": "https://management.azure.com/",
            "suffixes": {"storage": "core.windows.net"},
        }
        with pytest.raises(ValueError, match="missing required field"):
            _cloud_config_from_metadata(bad_entry)


class TestSelectCloudFromMetadata:
    def test_select_by_name(self):
        entry = _select_cloud_from_metadata(SAMPLE_ARM_METADATA, "AzureUSGovernment")
        assert entry["name"] == "AzureUSGovernment"

    def test_select_first_when_no_name(self):
        entry = _select_cloud_from_metadata(SAMPLE_ARM_METADATA, None)
        assert entry["name"] == "AzureCloud"

    def test_select_by_resource_manager_when_no_name(self):
        entry = _select_cloud_from_metadata(
            SAMPLE_ARM_METADATA, None, "https://management.usgovcloudapi.net/"
        )
        assert entry["name"] == "AzureUSGovernment"

    def test_raises_on_unknown_name(self):
        with pytest.raises(ValueError, match="not found in ARM metadata"):
            _select_cloud_from_metadata(SAMPLE_ARM_METADATA, "AzureNonExistent")

    def test_raises_on_empty_metadata(self):
        with pytest.raises(ValueError, match="no cloud definitions"):
            _select_cloud_from_metadata([], None)

    def test_raises_when_resource_manager_does_not_match(self):
        with pytest.raises(
            ValueError,
            match="ARM endpoint 'https://management.example.com' not found in ARM metadata",
        ):
            _select_cloud_from_metadata(
                SAMPLE_ARM_METADATA, None, "https://management.example.com/"
            )


class TestGetCloudConfig:
    def test_default_is_public_cloud(self):
        with patch.dict(os.environ, {}, clear=True):
            assert get_cloud_config() == AZURE_PUBLIC_CLOUD

    def test_azure_cloud_selects_preset(self):
        with patch.dict(os.environ, {"AZURE_CLOUD": "AzureUSGovernment"}, clear=True):
            assert get_cloud_config() == AZURE_US_GOV_CLOUD

    def test_azure_cloud_unknown_raises(self):
        with patch.dict(os.environ, {"AZURE_CLOUD": "UnknownCloud"}, clear=True):
            with pytest.raises(ValueError, match="Unknown cloud"):
                get_cloud_config()

    def test_arm_metadata_url_fetches_and_selects(self):
        with patch.dict(
            os.environ,
            {
                "ARM_CLOUD_METADATA_URL": "https://management.example.com/metadata/endpoints?api-version=2019-05-01",
                "AZURE_CLOUD": "AzureUSGovernment",
            },
            clear=True,
        ):
            with patch(
                "boostedblob.azure_cloud._fetch_arm_cloud_metadata",
                return_value=SAMPLE_ARM_METADATA,
            ) as mock_fetch:
                cloud = get_cloud_config()

        mock_fetch.assert_called_once_with(
            "https://management.example.com/metadata/endpoints?api-version=2019-05-01"
        )
        assert cloud == AZURE_US_GOV_CLOUD

    def test_arm_metadata_url_takes_priority_over_preset(self):
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
                "ARM_CLOUD_METADATA_URL": "https://management.example.com/metadata/endpoints?api-version=2019-05-01",
                "AZURE_CLOUD": "AzureUSGovernment",
            },
            clear=True,
        ):
            with patch(
                "boostedblob.azure_cloud._fetch_arm_cloud_metadata",
                return_value=custom_metadata,
            ):
                cloud = get_cloud_config()

        assert cloud.storage_endpoint_suffix == "custom.storage.example"
        assert cloud.login_endpoint == "https://custom.login.example"

    def test_arm_metadata_url_matches_resource_manager_when_name_unset(self):
        with patch.dict(
            os.environ,
            {
                "ARM_CLOUD_METADATA_URL": "https://management.usgovcloudapi.net/metadata/endpoints?api-version=2019-05-01"
            },
            clear=True,
        ):
            with patch(
                "boostedblob.azure_cloud._fetch_arm_cloud_metadata",
                return_value=SAMPLE_ARM_METADATA,
            ) as mock_fetch:
                cloud = get_cloud_config()

        mock_fetch.assert_called_once_with(
            "https://management.usgovcloudapi.net/metadata/endpoints?api-version=2019-05-01"
        )
        assert cloud == AZURE_US_GOV_CLOUD

    def test_arm_metadata_url_without_matching_resource_manager_raises(self):
        with patch.dict(
            os.environ,
            {
                "ARM_CLOUD_METADATA_URL": "https://management.example.com/metadata/endpoints?api-version=2019-05-01"
            },
            clear=True,
        ):
            with patch(
                "boostedblob.azure_cloud._fetch_arm_cloud_metadata",
                return_value=SAMPLE_ARM_METADATA,
            ):
                with pytest.raises(
                    ValueError,
                    match="ARM endpoint 'https://management.example.com' not found in ARM metadata",
                ):
                    get_cloud_config()


class TestLazyConfigResolution:
    def test_config_does_not_resolve_cloud_at_construction(self):
        with patch("boostedblob.globals.get_cloud_config", return_value=AZURE_PUBLIC_CLOUD) as mock_get:
            cfg = Config()
            assert mock_get.call_count == 0

            assert cfg.azure_cloud == AZURE_PUBLIC_CLOUD
            assert mock_get.call_count == 1

            assert cfg.azure_cloud == AZURE_PUBLIC_CLOUD
            assert mock_get.call_count == 1

    def test_configure_azure_cloud_works_synchronously(self):
        original = config.azure_cloud
        with configure(azure_cloud=AZURE_US_GOV_CLOUD):
            assert config.azure_cloud == AZURE_US_GOV_CLOUD
        assert config.azure_cloud == original


class TestAzureIdentityAuthority:
    @pytest.mark.asyncio
    async def test_get_access_token_uses_authority_host(self, monkeypatch):
        calls: dict[str, str] = {}

        class FakeDefaultAzureCredential:
            def __init__(self, *, authority: str):
                calls["authority"] = authority

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

            async def get_token(self, scope: str):
                calls["scope"] = scope
                return SimpleNamespace(token="token", expires_on=123)

        azure_module = ModuleType("azure")
        identity_module = ModuleType("azure.identity")
        aio_module = ModuleType("azure.identity.aio")
        aio_module.DefaultAzureCredential = FakeDefaultAzureCredential
        identity_module.aio = aio_module
        azure_module.identity = identity_module

        monkeypatch.setitem(sys.modules, "azure", azure_module)
        monkeypatch.setitem(sys.modules, "azure.identity", identity_module)
        monkeypatch.setitem(sys.modules, "azure.identity.aio", aio_module)
        monkeypatch.setattr(azure_auth, "load_credentials", lambda: {"_azure_auth": "azure-identity"})

        async def fake_can_access_account(*args, **kwargs):
            return True

        monkeypatch.setattr(azure_auth, "can_access_account", fake_can_access_account)

        with configure(azure_cloud=AZURE_US_GOV_CLOUD):
            auth, expires_on = await azure_auth.get_access_token(
                azure_auth.azure_cache_key("acct", None)
            )

        assert calls["authority"] == "login.microsoftonline.us"
        assert calls["scope"] == "https://storage.azure.com/.default"
        assert auth == (azure_auth.OAUTH_TOKEN, "token")
        assert expires_on == 123


class TestAzureCacheKey:
    def test_cache_key_differs_by_cloud(self):
        with configure(azure_cloud=AZURE_PUBLIC_CLOUD):
            public_key = azure_auth.azure_cache_key("acct", "container")
        with configure(azure_cloud=AZURE_US_GOV_CLOUD):
            gov_key = azure_auth.azure_cache_key("acct", "container")

        assert public_key != gov_key
