import pytest

import boostedblob as bbb

from . import helpers


@pytest.mark.asyncio
@bbb.ensure_session
async def test_get_storage_account_key():
    from boostedblob import azure_auth

    creds = azure_auth.load_credentials()
    await azure_auth.get_storage_account_key(helpers.azure_test_base().account, creds)


def test_format_size():
    assert bbb.listing.format_size(1023) == "1023.0 B  "
    assert bbb.listing.format_size(1024) == "1.0 KiB"
    assert bbb.listing.format_size(1024 * 1024 + 1) == "1.0 MiB"
    assert bbb.listing.format_size(1024 * 1024 * 1076) == "1.1 GiB"
    assert bbb.listing.format_size(2 ** 81) == "2.0 YiB"
