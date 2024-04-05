from __future__ import annotations

import asyncio
import contextlib
import functools
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
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
        self._locks: Dict[asyncio.AbstractEventLoop, asyncio.Lock] = {}

    async def get_token(self, key: T) -> Any:
        if not self._tokens:
            self.load_state()

        running_loop = asyncio.get_running_loop()
        if running_loop not in self._locks:
            self._locks[running_loop] = asyncio.Lock()
        lock = self._locks[running_loop]

        now = time.time()
        expiration = self._expirations.get(key)

        if expiration is None or (now + config.token_early_expiration_seconds) > expiration:
            async with lock:
                refresh_expiration = self._expirations.get(key)
                if refresh_expiration == expiration:
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
            with open(cache_file) as f:
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
    # Avoid the default executor because https://bugs.python.org/issue35279
    executor: ThreadPoolExecutor = field(default_factory=lambda: ThreadPoolExecutor(max_workers=32))

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
    request_reauth_seconds: int = 300

    azure_access_token_manager: TokenManager[Tuple[str, Optional[str]]] = field(
        default_factory=lambda: TokenManager(azure_auth.get_access_token)
    )
    azure_sas_token_manager: TokenManager[Tuple[str, Optional[str]]] = field(
        default_factory=lambda: TokenManager(azure_auth.get_sas_token)
    )
    google_access_token_manager: TokenManager[str] = field(
        default_factory=lambda: TokenManager(google_auth.get_access_token)
    )

    _sessions: Dict[asyncio.AbstractEventLoop, aiohttp.ClientSession] = field(
        default_factory=dict, init=False
    )

    def _get_session(self) -> Optional[aiohttp.ClientSession]:
        return self._sessions.get(asyncio.get_running_loop())

    def _set_session(self, session: Optional[aiohttp.ClientSession]) -> None:
        running_loop = asyncio.get_running_loop()
        if session is None:
            self._sessions.pop(running_loop, None)
            return
        # In case we're doing repeated asyncio.run, clean up old sessions
        for l in list(self._sessions):
            if l.is_closed():
                del self._sessions[l]
        if hasattr(session, "_loop"):  # hasattr in case aiohttp changes
            assert session._loop is running_loop
        self._sessions[running_loop] = session

    async def _close_session(self) -> None:
        session = self._sessions.pop(asyncio.get_running_loop(), None)
        if session:
            await session.close()

    @property
    def session(self) -> aiohttp.ClientSession:
        loop = asyncio.get_running_loop()
        try:
            return self._sessions[loop]
        except KeyError:
            set_event_loop_exception_handler()  # there could be a better place for this
            session = _create_session()
            assert session._loop is loop
            self._set_session(session)
            return session

    @session.setter
    def session(self, session: aiohttp.ClientSession) -> None:
        import warnings

        warnings.warn(
            "Setting config.session is deprecated. Use config.configure(session=...) instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        self._set_session(session)


config: Config = Config()


@contextlib.contextmanager
def configure(**kwargs: Any) -> Iterator[None]:
    old_session = None
    if "session" in kwargs:
        old_session = config._get_session()
        config._set_session(kwargs.pop("session"))

    original = {k: getattr(config, k) for k in kwargs}
    config.__dict__.update(**kwargs)
    try:
        yield
    finally:
        config.__dict__.update(**original)
        config._set_session(old_session)


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
    connector = aiohttp.TCPConnector(limit=1024, ttl_dns_cache=60)
    return aiohttp.ClientSession(connector=connector)


@contextlib.asynccontextmanager
async def session_context() -> AsyncIterator[None]:
    preexisting_session = bool(config._get_session())
    try:
        yield
    finally:
        if not preexisting_session:
            await config._close_session()


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

        print(f"ERROR (from event loop): {type(exception).__name__}: {message}", file=sys.stderr)
        if loop.get_debug():
            loop.default_exception_handler(context)

    loop.set_exception_handler(handler)
