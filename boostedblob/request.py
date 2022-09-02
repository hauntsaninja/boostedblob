import asyncio
import contextlib
import datetime
import json
import random
import socket
import sys
import time
import urllib.parse
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Dict, Iterator, Mapping, Optional, Sequence, Tuple

import aiohttp

from .globals import config
from .xml import dict_to_xml, etree


class MissingSession(Exception):
    pass


@dataclass(frozen=True)
class Request:
    method: str
    url: str
    params: Mapping[str, str] = field(default_factory=dict)
    # data can be large, so don't put it in the repr
    data: Any = field(repr=False, default_factory=lambda: None)
    # headers can contain authorisation details, so don't put them in the repr
    headers: Mapping[str, str] = field(repr=False, default_factory=dict)
    success_codes: Sequence[int] = field(repr=False, default_factory=lambda: (200,))
    retry_codes: Sequence[int] = field(
        repr=False, default_factory=lambda: (408, 429, 500, 502, 503, 504)
    )
    failure_exceptions: Mapping[int, Exception] = field(repr=False, default_factory=dict)

    @contextlib.asynccontextmanager
    async def execute(self) -> AsyncIterator[aiohttp.ClientResponse]:
        """Execute the request, retrying as appropriate.

        Example usage:
        ```
        async with request.execute() as resp:
            print(resp.status)
        ```

        """

        for attempt, backoff in enumerate(
            exponential_sleep_generator(
                initial=config.backoff_initial,
                maximum=config.backoff_max,
                jitter_fraction=config.backoff_jitter_fraction,
            )
        ):
            async with contextlib.AsyncExitStack() as stack:
                try:
                    resp = await stack.enter_async_context(self._raw_execute())
                except aiohttp.ClientConnectionError as e:
                    if isinstance(e, aiohttp.ClientConnectorError):
                        # azure accounts have unique urls and it's hard to tell apart
                        # an invalid hostname from a network error
                        url = urllib.parse.urlparse(self.url)
                        hostname = url.hostname
                        if hostname and hostname.endswith(".blob.core.windows.net"):
                            if await _bad_hostname_check(hostname):
                                raise FileNotFoundError(hostname) from None
                    error = RequestFailure(reason=type(e).__name__ + ": " + str(e), request=self)
                else:
                    if resp.status in self.success_codes:
                        yield resp
                        return

                    assert resp.reason is not None
                    reason = repr(resp.reason)
                    if config.debug_mode:
                        body = (await resp.read()).decode("utf-8")
                        reason += "\nBody:\n" + body + "\nHeaders:\n" + str(resp.headers)
                    error = RequestFailure(reason=reason, request=self, status=resp.status)
                    if resp.status not in self.retry_codes:
                        raise self.failure_exceptions.get(resp.status, error)

            if attempt >= config.retry_limit:
                raise error

            if config.debug_mode or attempt + 1 >= 3:
                print(
                    f"[boostedblob] Error when executing request on attempt {attempt + 1}, "
                    f"sleeping for {backoff:.1f}s before retrying. Details:\n{error}",
                    file=sys.stderr,
                )
            await asyncio.sleep(backoff)

    async def execute_reponseless(self) -> None:
        """Helper to execute the request when we don't have further need of the response."""
        async with self.execute():
            pass

    @contextlib.asynccontextmanager
    async def _raw_execute(self) -> AsyncIterator[aiohttp.ClientResponse]:
        """Actually execute the request, with no extra fluff."""
        if config.session is None:
            raise MissingSession(
                "No session available, use `async with session_context()` or `@ensure_session`"
            )
        ctx = config.session.request(
            method=self.method,
            url=self.url,
            params=self.params,
            data=self.data,
            headers=self.headers,
            allow_redirects=False,
            timeout=aiohttp.ClientTimeout(
                connect=config.connect_timeout, sock_read=config.read_timeout
            ),
            # Figuring out that some requests break because aiohttp adds some headers
            # automatically was not fun
            skip_auto_headers={"Content-Type"},
        )
        if config.debug_mode:
            print(f"[boostedblob] Making request: {self}", file=sys.stderr)
            now = time.time()
        async with ctx as resp:
            if config.debug_mode:
                duration = time.time() - now
                print(
                    f"[boostedblob] Completed request, took {duration:.3f}s: {self}",
                    file=sys.stderr,
                )
            yield resp


class RequestFailure(Exception):
    def __init__(self, reason: str, request: Request, status: Optional[int] = None):
        self.reason = reason
        self.request = request
        self.status = status

    def __str__(self) -> str:
        return f"Reason: {self.reason}\nRequest: {self.request}\nStatus: {self.status}"


async def execute_retrying_read(request: Request) -> bytes:
    # Retrying aiohttp.ServerTimeoutError is pretty straightforward
    # ClientPayloadError might be an aiohttp bug, see:
    # https://github.com/aio-libs/aiohttp/issues/4581
    # https://github.com/aio-libs/aiohttp/issues/3904#issuecomment-737094416
    for attempt, backoff in enumerate(
        exponential_sleep_generator(
            initial=config.backoff_initial,
            maximum=config.backoff_max,
            jitter_fraction=config.backoff_jitter_fraction,
        )
    ):
        try:
            async with request.execute() as resp:
                return await resp.read()
        except (aiohttp.ServerTimeoutError, aiohttp.ClientPayloadError) as error:
            if attempt >= config.retry_limit:
                raise

            if config.debug_mode or attempt + 1 >= 3:
                print(
                    f"[boostedblob] Error when executing request on attempt {attempt + 1}, "
                    f"sleeping for {backoff:.1f}s before retrying. Details:\n{error}",
                    file=sys.stderr,
                )
            await asyncio.sleep(backoff)
    raise AssertionError


# ==============================
# cloud specific utilities
# ==============================


async def azurify_request(request: Request, auth: Optional[Tuple[str, str]] = None) -> Request:
    """Return a Request that can be submitted to Azure Blob."""
    u = urllib.parse.urlparse(request.url)
    account = u.netloc.split(".")[0]
    parts = u.path.split("/", maxsplit=2)
    container = parts[1] if len(parts) >= 2 else None
    if auth is None:
        auth = await config.azure_access_token_manager.get_token(key=(account, container))
    assert auth is not None
    kind, token = auth

    headers = dict(request.headers)
    # https://docs.microsoft.com/en-us/rest/api/storageservices/previous-azure-storage-service-versions
    headers["x-ms-version"] = "2021-06-08"
    headers["x-ms-date"] = datetime.datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")

    from .azure_auth import OAUTH_TOKEN, SHARED_KEY, sign_request_with_shared_key

    data = request.data
    if data is not None and not isinstance(data, (bytes, bytearray)):
        data = dict_to_xml(data)

    result = Request(
        method=request.method,
        url=request.url,
        params=request.params,
        headers=headers,
        data=data,
        success_codes=request.success_codes,
        retry_codes=request.retry_codes,
        failure_exceptions=request.failure_exceptions,
    )

    # mutate headers to add the authorization header to result
    if kind == SHARED_KEY:
        # make sure we are signing a request that includes changes made in this function
        # i.e., the request has the x-ms-* headers added and data bytes-ified.
        assert "x-ms-date" in headers
        headers["Authorization"] = sign_request_with_shared_key(result, token)
    elif kind == OAUTH_TOKEN:
        headers["Authorization"] = f"Bearer {token}"

    return result


async def googlify_request(request: Request, access_token: Optional[str] = None) -> Request:
    """Return a Request that can be submitted to GCS."""
    if access_token is None:
        access_token = await config.google_access_token_manager.get_token(key="")

    headers = dict(request.headers)
    headers["Authorization"] = f"Bearer {access_token}"

    data = request.data
    if data is not None and not isinstance(data, (bytes, bytearray)):
        data = json.dumps(data).encode("utf8")

    return Request(
        method=request.method,
        url=request.url,
        params=request.params,
        headers=headers,
        data=data,
        success_codes=request.success_codes,
        retry_codes=request.retry_codes,
        failure_exceptions=request.failure_exceptions,
    )


async def azure_page_iterator(request: Request) -> AsyncIterator[etree.Element]:
    params = dict(request.params)
    while True:
        request = Request(
            method=request.method,
            url=request.url,
            params=params,
            data=request.data,
            headers=request.headers,
            success_codes=request.success_codes,
            retry_codes=request.retry_codes,
            failure_exceptions=request.failure_exceptions,
        )

        request = await azurify_request(request)
        body = await execute_retrying_read(request)

        result = etree.fromstring(body)
        yield result

        next_marker = result.findtext("NextMarker")
        if not next_marker:
            break
        params["marker"] = next_marker


async def google_page_iterator(request: Request) -> AsyncIterator[Dict[str, Any]]:
    params = dict(request.params)
    while True:
        request = Request(
            method=request.method,
            url=request.url,
            params=params,
            headers=request.headers,
            success_codes=(200, 404),
            retry_codes=request.retry_codes,
            failure_exceptions=request.failure_exceptions,
        )
        request = await googlify_request(request)
        async with request.execute() as resp:
            if resp.status == 404:
                return
            result = await resp.json()
        yield result
        if "nextPageToken" not in result:
            break
        params["pageToken"] = result["nextPageToken"]


# ==============================
# helpers
# ==============================


def exponential_sleep_generator(
    initial: float, maximum: float, jitter_fraction: float, multiplier: float = 2
) -> Iterator[float]:
    """Yields amounts to sleep by.

    https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
    Full jitter is ``jitter_fraction == 1``
    Equal jitter is ``jitter_fraction == 0.5``
    No jitter is ``jitter_fraction == 0``
    """

    base = initial
    while True:
        sleep = base * (1 - jitter_fraction) + base * random.random() * jitter_fraction
        yield sleep
        base *= multiplier
        if base > maximum:
            base = maximum


_hostname_check_cache: Dict[str, Tuple[float, bool]] = {}


async def _bad_hostname_check(hostname: str) -> bool:
    """Return True if the hostname likely does not exist.

    If the hostname exists, or there is uncertainty, return False.

    """

    async def inner(hostname: str) -> bool:
        loop = asyncio.get_running_loop()
        try:
            await loop.getaddrinfo(hostname, None, family=socket.AF_INET)
            # no errors encountered, the hostname exists
            return False
        except OSError as e:
            if e.errno != socket.EAI_NONAME:
                # we got some sort of other socket error, so it's unclear if the host exists or not
                return False
            if sys.platform == "linux":
                # on linux we appear to get EAI_NONAME if the host does not exist and EAI_AGAIN if
                # there is a temporary failure in resolution
                return True
            # it's not clear on other platforms how to differentiate a temporary name resolution
            # failure from a permanent one, EAI_NONAME seems to be returned for either case if we
            # cannot look up the hostname, but we can look up google, then it's likely the hostname
            # does not exist
            try:
                await loop.getaddrinfo("www.google.com", None, family=socket.AF_INET)
            except OSError:
                # if we can't resolve google, then the network is likely down and we don't know if
                # the hostname exists or not
                return False
            # in this case, we could resolve google, but not the original hostname likely the
            # hostname does not exist (though this is definitely not a foolproof check)
            return True

    # maybe this cache is a little bit overkill...
    now = time.time()
    if hostname in _hostname_check_cache and _hostname_check_cache[hostname][0] >= now:
        return _hostname_check_cache[hostname][1]
    ret = await inner(hostname)
    _hostname_check_cache[hostname] = (now + 10, ret)
    return ret
