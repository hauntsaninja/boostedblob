from __future__ import annotations

import asyncio
import contextlib
import functools
import json
import os
import sys
import time
from dataclasses import dataclass, field
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

MB = 2**20


T = TypeVar("T")
F = TypeVar("F", bound=Callable[..., Any])


class TokenManager(Generic[T]):
    """Automatically refresh a token when it expires."""

    def __init__(self, get_token_fn: Callable[[T], Awaitable[Tuple[Any, float]]]) -> None:
        self._get_token_fn = get_token_fn
        self._tokens: Dict[T, Any] = {}
        self._expirations: Dict[T, float] = {}

    async def get_token(self, key: T) -> Any:
        if not self._tokens:
            self.load_state()

        now = time.time()
        expiration = self._expirations.get(key)
        if expiration is None or (now + config.token_early_expiration_seconds) > expiration:
            self._tokens[key], self._expirations[key] = await self._get_token_fn(key)
            assert self._expirations[key] is not None
            self.dump_state()

        assert key in self._tokens
        return self._tokens[key]

    def get_cache_file(self) -> str:
        qn = self._get_token_fn.__module__ + "." + self._get_token_fn.__qualname__
        return os.path.expanduser(f"~/.config/bbb/{qn}.json")

    def load_state(self) -> None:
        if os.environ.get("BBB_DISABLE_CACHE"):
            return
        try:
            cache_file = self.get_cache_file()
            with open(cache_file, "r") as f:
                state = json.load(f)
            if state.get("__version__") != 1:
                os.remove(cache_file)
                raise RuntimeError("Outdated format")
        except Exception as e:
            if config.debug_mode:
                print(f"[boostedblob] Error while loading token cache: {e}", file=sys.stderr)
            return

        for t in state["tokens"]:
            key: Any = t["key"]
            key = tuple(key) if isinstance(key, list) else key
            self._tokens[key] = t["token"]
            self._expirations[key] = t["expiration"]

    def dump_state(self) -> None:
        if os.environ.get("BBB_DISABLE_CACHE"):
            return
        now = time.time()
        state = {
            "__version__": 1,
            "tokens": [
                {
                    "key": k,
                    "token": self._tokens[k],
                    # lie about token expiry, so there's a limit on how long bbb will used a
                    # cached token
                    "expiration": min(exp, now + config.token_early_expiration_seconds * 2),
                }
                for k, exp in self._expirations.items()
                if exp > now  # only save tokens that have not expired
            ],
        }
        try:
            cache_file = self.get_cache_file()
            os.makedirs(os.path.dirname(cache_file), exist_ok=True)
            with open(cache_file, "w") as f:
                json.dump(state, f)
        except Exception as e:
            if config.debug_mode:
                print(f"[boostedblob] Error while dumping token cache: {e}", file=sys.stderr)


@dataclass
class Config:
    session: Optional[aiohttp.ClientSession] = None

    debug_mode: bool = bool(os.environ.get("BBB_DEBUG"))
    storage_account_key_fallback: bool = bool(os.environ.get("BBB_SA_KEY_FALLBACK"))

    chunk_size: int = 16 * MB

    connect_timeout: float = 20.0
    read_timeout: float = 60.0
    backoff_initial: float = 0.1
    backoff_max: float = 60.0
    backoff_jitter_fraction: float = 0.9
    retry_limit: int = 25

    token_early_expiration_seconds: int = 300

    azure_access_token_manager: TokenManager[Tuple[str, Optional[str]]] = field(
        default_factory=lambda: TokenManager(azure_auth.get_access_token)
    )
    azure_sas_token_manager: TokenManager[Tuple[str, Optional[str]]] = field(
        default_factory=lambda: TokenManager(azure_auth.get_sas_token)
    )
    google_access_token_manager: TokenManager[str] = field(
        default_factory=lambda: TokenManager(google_auth.get_access_token)
    )


config: Config = Config()


@contextlib.contextmanager
def configure(**kwargs: Any) -> Iterator[None]:
    original = {k: getattr(config, k) for k in kwargs}
    config.__dict__.update(**kwargs)
    try:
        yield
    finally:
        config.__dict__.update(**original)


def _create_session() -> aiohttp.ClientSession:
    # NB: We currently seem to leak file descriptors. E.g., if you run with -X dev, you'll see
    # "ResourceWarning: unclosed resource <TCPTransport ...>"
    # This can be fixed by changing the following line to:
    # connector = aiohttp.TCPConnector(limit=0, force_close=True, enable_cleanup_closed=True)
    # Both additional arguments are apparently necessary. Unfortunately this negates the
    # benefits of reusing connections. If you only have a single aiohttp.ClientSession, as is
    # recommended, this isn't really a problem.
    # Also note:
    # https://docs.aiohttp.org/en/stable/client_advanced.html#graceful-shutdown
    # https://github.com/aio-libs/aiohttp/issues/1925#issuecomment-715977247
    # While the sleep suggested doesn't work, it does indicate that this is a problem for
    # aiohttp in general.
    connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=60)
    return aiohttp.ClientSession(connector=connector)


@contextlib.asynccontextmanager
async def session_context() -> AsyncIterator[None]:
    if config.session is None:
        set_event_loop_exception_handler()  # there could be a better place for this
        session = _create_session()
        async with session:
            with configure(session=session):
                yield
    else:
        yield


def ensure_session(fn: F) -> F:
    @functools.wraps(fn)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        async with session_context():
            return await fn(*args, **kwargs)

    return wrapper  # type: ignore[return-value]


def set_event_loop_exception_handler() -> None:
    loop = asyncio.get_running_loop()

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
