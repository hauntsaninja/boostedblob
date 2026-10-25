from __future__ import annotations

import asyncio
import datetime
from functools import partial
from typing import TYPE_CHECKING, Any, Mapping, Sequence
from urllib.parse import quote, urlencode
from weakref import WeakKeyDictionary

import aiohttp
from yarl import URL

from .globals import config
from .request import (
    RawRequest,
    Request,
    RequestFailure,
    execute_retrying_read,
    exponential_sleep_generator,
)
from .xml import etree

if TYPE_CHECKING:
    from .path import S3Path

MIN_PART_SIZE = 5 * 2**20
MAX_PART_SIZE = 5 * 2**30
MAX_PARTS = 10_000


class _Auth:
    def __init__(self) -> None:
        from aiobotocore.session import get_session  # type: ignore[import-untyped]
        from botocore.configprovider import (  # type: ignore[import-untyped]
            ConfiguredEndpointProvider,
        )
        from botocore.endpoint_provider import EndpointProvider  # type: ignore[import-untyped]

        self.session = get_session()
        loader = self.session.get_component("data_loader")
        self.endpoints = EndpointProvider(
            loader.load_service_model("s3", "endpoint-rule-set-1"), loader.load_data("partitions")
        )
        self.custom_endpoint = None
        if not self.session.get_config_variable("ignore_configured_endpoint_urls"):
            self.custom_endpoint = ConfiguredEndpointProvider(
                self.session.full_config, self.session.get_scoped_config(), "s3"
            ).provide()
        s3_config = self.session.get_config_variable("s3") or {}
        style = s3_config.get("addressing_style")
        self.path_style = style == "path" or (bool(self.custom_endpoint) and style != "virtual")
        self.use_fips = self.session.get_config_variable("use_fips_endpoint") or False
        self.dual_stack = self.session.get_config_variable("use_dualstack_endpoint") or False
        self.default_region = self.session.get_config_variable("region") or "us-east-1"
        self.buckets: dict[str, tuple[str, str]] = {}
        self.credentials_lock = asyncio.Lock()
        self.bucket_lock = asyncio.Lock()

    async def credentials(self) -> Any:
        from botocore.exceptions import NoCredentialsError  # type: ignore[import-untyped]

        async with self.credentials_lock:
            credentials = await self.session.get_credentials()
        if credentials is None:
            raise NoCredentialsError()
        return await credentials.get_frozen_credentials()

    def endpoint(self, bucket: str, region: str) -> str:
        params = {"Bucket": bucket} if bucket else {}
        if self.custom_endpoint:
            params["Endpoint"] = self.custom_endpoint
        endpoint = self.endpoints.resolve_endpoint(
            Region=region,
            ForcePathStyle=self.path_style,
            UseFIPS=self.use_fips,
            UseDualStack=self.dual_stack,
            **params,
        )
        return endpoint.url.rstrip("/")

    async def bucket_endpoint(self, path: S3Path) -> tuple[str, str]:
        bucket = path.bucket
        if bucket in self.buckets:
            return self.buckets[bucket]
        async with self.bucket_lock:
            if bucket in self.buckets:
                return self.buckets[bucket]
            region = self.default_region
            url = self.endpoint(bucket, region)
            if bucket:
                # HeadBucket supplies the region even when ListBucket is denied.
                head = Request(
                    method="HEAD",
                    url=url + "/",
                    auth=partial(self.sign, region=region),
                    success_codes=(200, 301, 307, 400, 403),
                    failure_exceptions={404: FileNotFoundError(path)},
                )
                async with head.execute() as response:
                    region = response.headers.get("x-amz-bucket-region", region)
                url = self.endpoint(bucket, region)
            self.buckets[bucket] = url, region
        return self.buckets[bucket]

    async def sign(self, request: Request, *, region: str) -> RawRequest:
        from botocore.auth import S3SigV4Auth  # type: ignore[import-untyped]
        from botocore.awsrequest import AWSRequest  # type: ignore[import-untyped]

        assert not isinstance(request.data, dict)
        url = request.url
        if request.params:
            url += "?" + urlencode(request.params, quote_via=quote)
        aws = AWSRequest(method=request.method, url=url, headers=request.headers, data=request.data)
        signer = S3SigV4Auth(await self.credentials(), "s3", region)
        if request.data:
            # SigV4 hashes the entire payload, which can be several GiB for a single PUT.
            await asyncio.get_running_loop().run_in_executor(config.executor, signer.add_auth, aws)
        else:
            signer.add_auth(aws)
        # Requoting the path or query here would invalidate the signature or change the object key.
        return RawRequest(
            method=request.method,
            url=URL(url, encoded=True),
            headers={**aws.headers, "Accept-Encoding": "identity"},
            data=request.data,
            allow_redirects=False,
            auto_decompress=False,
        )


_auth: WeakKeyDictionary[aiohttp.ClientSession, _Auth] = WeakKeyDictionary()


def _get_auth() -> _Auth:
    session = config.session
    if session not in _auth:
        _auth[session] = _Auth()
    return _auth[session]


async def request(
    method: str,
    path: S3Path,
    *,
    params: Mapping[str, str] | None = None,
    headers: Mapping[str, str] | None = None,
    data: bytes | None = None,
    success_codes: Sequence[int] = (200,),
    failure_exceptions: Mapping[int, Exception] | None = None,
) -> Request:
    auth = _get_auth()
    endpoint, region = await auth.bucket_endpoint(path)
    return Request(
        method=method,
        url=endpoint + "/" + quote(path.blob),
        params=params or {},
        headers=headers or {},
        data=data,
        success_codes=success_codes,
        failure_exceptions={
            403: PermissionError(path),
            404: FileNotFoundError(path),
            **(failure_exceptions or {}),
        },
        auth=partial(auth.sign, region=region),
    )


async def generate_signed_url(path: S3Path) -> tuple[str, datetime.datetime]:
    from botocore.auth import SIGV4_TIMESTAMP, S3SigV4QueryAuth
    from botocore.awsrequest import AWSRequest

    if not path.blob:
        raise IsADirectoryError(path)
    auth = _get_auth()
    endpoint, region = await auth.bucket_endpoint(path)
    aws = AWSRequest(method="GET", url=endpoint + "/" + quote(path.blob))
    expires = 3600
    S3SigV4QueryAuth(await auth.credentials(), "s3", region, expires=expires).add_auth(aws)
    signed_at = datetime.datetime.strptime(aws.context["timestamp"], SIGV4_TIMESTAMP)
    return aws.url, signed_at.replace(tzinfo=datetime.timezone.utc) + datetime.timedelta(
        seconds=expires
    )


async def xml_result(request: Request) -> etree.Element:
    # S3 can report a failed copy or multipart completion inside an HTTP 200 response.
    for attempt, backoff in enumerate(
        exponential_sleep_generator(
            config.backoff_initial, config.backoff_max, config.backoff_jitter_fraction
        )
    ):
        result = etree.fromstring(await execute_retrying_read(request))
        if result.tag.rsplit("}", 1)[-1] != "Error":
            return result
        code = result.findtext("{*}Code")
        if code == "NoSuchKey" and 404 in request.failure_exceptions:
            raise request.failure_exceptions[404]
        if (
            code not in {"SlowDown", "InternalError", "ServiceUnavailable"}
            or attempt >= config.retry_limit
        ):
            raise RequestFailure(f"{code}: {result.findtext('{*}Message')}", request, status=200)
        await asyncio.sleep(backoff)
    raise AssertionError
