from __future__ import annotations

import asyncio
import contextlib
import functools
import os
import sys
import time
from dataclasses import dataclass
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    Generic,
    Iterator,
    Optional,
    Tuple,
    TypeVar,
)

import aiohttp

from . import azure_auth, google_auth

MB = 2 ** 20


T = TypeVar("T")
F = TypeVar("F", bound=Callable[..., Any])


class TokenManager(Generic[T]):
    """Automatically refresh a token when it expires."""

    def __init__(self, get_token_fn: Callable[[T], Awaitable[Tuple[Any, float]]]) -> None:
        self._get_token_fn = get_token_fn
        self._tokens: Dict[T, Any] = {}
        self._expirations: Dict[T, float] = {}

    async def get_token(self, key: T) -> Any:
        now = time.time()
        expiration = self._expirations.get(key)
        if expiration is None or (now + config.token_early_expiration_seconds) > expiration:
            self._tokens[key], self._expirations[key] = await self._get_token_fn(key)
            assert self._expirations[key] is not None

        assert key in self._tokens
        return self._tokens[key]


@dataclass
class Config:
    session: Optional[aiohttp.ClientSession] = None

    debug_mode: bool = bool(os.environ.get("BBB_DEBUG"))

    chunk_size: int = 16 * MB

    connect_timeout: float = 30.0
    read_timeout: float = 300.0
    backoff_initial: float = 0.1
    backoff_max: float = 60.0
    backoff_jitter_fraction: float = 0.9
    retry_limit: int = 15

    token_early_expiration_seconds: int = 300
    azure_access_token_manager = TokenManager[Tuple[str, Optional[str]]](
        azure_auth.get_access_token
    )
    azure_sas_token_manager = TokenManager[Tuple[str, Optional[str]]](azure_auth.get_sas_token)
    google_access_token_manager = TokenManager[str](google_auth.get_access_token)


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
        set_event_loop_exception_handler()  # there could be a better place for this
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


def set_event_loop_exception_handler() -> None:
    loop = asyncio.get_event_loop()

    def handler(
        loop: asyncio.AbstractEventLoop, context: Dict[str, Any]
    ) -> None:  # pragma: no cover
        message = context["message"]
        exception = context.get("exception", Exception)
        if sys.version_info < (3, 7, 4) and message in (
            "SSL error in data received",
            "Fatal error on transport",
        ):
            # Ignore aiohttp #3535 / cpython #13548 issue with SSL data after close
            # Adapted from https://github.com/aio-libs/aiohttp/issues/3535#issuecomment-483268542
            import ssl
            from asyncio.sslproto import SSLProtocol

            protocol = context.get("protocol")
            if (
                isinstance(exception, ssl.SSLError)
                and exception.reason == "KRB5_S_INIT"
                and isinstance(protocol, SSLProtocol)
            ):
                return

        # Our main interest here is minimising the other various errors and tracebacks that
        # drown out the politely formatted errors from cli.py when things go wrong
        if "exception was never retrieved" in message:
            from .request import MissingSession

            # While closing down, we remove the global session, causing other requests to fail. This
            # just causes noise, so ignore if that's the exception.
            if not isinstance(exception, MissingSession):
                print(
                    f"ERROR (while closing down): {type(exception).__name__}: {exception}",
                    file=sys.stderr,
                )
        else:
            print(
                f"ERROR (from event loop): {type(exception).__name__}: {message}", file=sys.stderr
            )
        if loop.get_debug():
            loop.default_exception_handler(context)

    loop.set_exception_handler(handler)
