from __future__ import annotations

import asyncio
import contextlib
import functools
import sys
import time
from dataclasses import dataclass
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, Iterator, Optional, Tuple, TypeVar

import aiohttp

from . import azure_auth, google_auth

MB = 2 ** 20


F = TypeVar("F", bound=Callable[..., Any])


class TokenManager:
    """Automatically refresh a token when it expires."""

    def __init__(self, get_token_fn: Callable[[str], Awaitable[Tuple[Any, float]]]) -> None:
        self._get_token_fn = get_token_fn
        self._tokens: Dict[str, Any] = {}
        self._expiration: Optional[float] = None

    async def get_token(self, key: str) -> Any:
        now = time.time()
        if (
            self._expiration is None
            or (now + config.token_early_expiration_seconds) > self._expiration
        ):
            if key in self._tokens:
                del self._tokens[key]

        if key not in self._tokens:
            self._tokens[key], self._expiration = await self._get_token_fn(key)
        return self._tokens[key]


@dataclass
class Config:
    session: Optional[aiohttp.ClientSession] = None

    chunk_size: int = 32 * MB

    connect_timeout: float = 10.0
    read_timeout: float = 30.0
    backoff_initial: float = 0.1
    backoff_max: float = 60.0
    backoff_jitter_fraction: float = 0.9
    retry_limit: int = 100

    token_early_expiration_seconds: int = 300
    azure_access_token_manager: TokenManager = TokenManager(azure_auth.get_access_token)
    azure_sas_token_manager: TokenManager = TokenManager(azure_auth.get_sas_token)
    google_access_token_manager: TokenManager = TokenManager(google_auth.get_access_token)


config: Config = Config()


@contextlib.contextmanager
def configure(**kwargs: Any) -> Iterator[None]:
    original = {k: getattr(config, k) for k in kwargs}
    config.__dict__.update(**kwargs)
    try:
        yield
    finally:
        config.__dict__.update(**original)


@contextlib.asynccontextmanager
async def session_context() -> AsyncIterator[None]:
    if config.session is None:
        ignore_aiohttp_ssl_eror()  # TODO: put this in a better place
        connector = aiohttp.TCPConnector(limit=0)
        async with aiohttp.ClientSession(connector=connector) as session:
            with configure(session=session):
                yield
    else:
        yield


def ensure_session(fn: F) -> F:
    @functools.wraps(fn)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        async with session_context():
            return await fn(*args, **kwargs)

    return wrapper  # type: ignore


def ignore_aiohttp_ssl_eror() -> None:  # pragma: no cover
    """Ignore aiohttp #3535 / cpython #13548 issue with SSL data after close.

    Copied from https://github.com/aio-libs/aiohttp/issues/3535#issuecomment-483268542

    """
    if sys.version_info >= (3, 7, 4):
        return

    loop = asyncio.get_event_loop()
    orig_handler = loop.get_exception_handler()

    import ssl
    from asyncio.sslproto import SSLProtocol

    def ignore_ssl_error(loop: asyncio.AbstractEventLoop, context: Dict[str, Any]) -> None:
        if context.get("message") in ("SSL error in data received", "Fatal error on transport"):
            # validate we have the right exception, transport and protocol
            exception = context.get("exception")
            protocol = context.get("protocol")
            if (
                isinstance(exception, ssl.SSLError)
                and exception.reason == "KRB5_S_INIT"
                and isinstance(protocol, SSLProtocol)
            ):
                return
        if orig_handler is not None:
            orig_handler(loop, context)
        else:
            loop.default_exception_handler(context)

    loop.set_exception_handler(ignore_ssl_error)
