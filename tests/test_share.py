import datetime
import urllib.parse
import urllib.request

import pytest

import boostedblob as bbb

from . import helpers


@pytest.mark.asyncio
@bbb.ensure_session
async def test_get_url(any_dir, monkeypatch):
    if isinstance(any_dir, bbb.GooglePath):
        pytest.skip("Needs authorisation not present on my dev machine")
    if isinstance(any_dir, bbb.S3Path):
        # The signature must work even when the caller's default region differs from the bucket.
        region = helpers.s3_test_client().head_bucket(Bucket=any_dir.bucket)["BucketRegion"]
        monkeypatch.setenv(
            "AWS_DEFAULT_REGION", "us-east-1" if region != "us-east-1" else "us-west-2"
        )
    helpers.create_file(any_dir / "here is a file", b"with some stuff")
    url, expiration = await bbb.share.get_url(any_dir / "here is a file")
    if isinstance(any_dir, bbb.S3Path):
        query = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)
        signed_at = datetime.datetime.strptime(query["X-Amz-Date"][0], "%Y%m%dT%H%M%S%z")
        assert expiration == signed_at + datetime.timedelta(seconds=int(query["X-Amz-Expires"][0]))
    with urllib.request.urlopen(url) as f:
        assert f.read() == b"with some stuff"
