"""
Centralised source transport — retry, rate limit, circuit breaker, fallback.

Replaces ad-hoc retry loops and scattered ``_try_get_ak_df`` helpers with a
single ``SourceClient`` that applies a declared ``SourcePolicy`` to every call.
"""

from __future__ import annotations

import json
import logging
import random
import threading
import time
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

logger = logging.getLogger(__name__)

# ── value types ──────────────────────────────────────────────────────────

RETRYABLE_HTTP = frozenset({429, 502, 503})


def _network_exception_types() -> tuple[type[Exception], ...]:
    """Collect the exception types real HTTP clients actually raise.

    The built-in ``ConnectionError`` / ``TimeoutError`` are **siblings** of the
    libraries' classes, not superclasses — ``requests`` and ``curl_cffi`` each
    define their own tree rooted at ``OSError``::

        requests.exceptions.ConnectionError -> RequestException -> OSError
        requests.exceptions.Timeout         -> RequestException -> OSError
        curl_cffi.requests.errors.RequestsError -> Exception
        built-in ConnectionError                          -> OSError
        built-in TimeoutError                            -> OSError

    so ``except (TimeoutError, ConnectionError)`` catches a ``RemoteDisconnected``
    wrapped by ``requests`` **not at all**. That failure then lands in the
    ``except Exception`` branch documented as "non-retryable: schema drift":
    one attempt, no backoff, and ``circuit.record_failure()`` never called, so
    the breaker never trips. Measured 2026-10-01: 2520 doomed requests at 0.8 s
    intervals, 34 minutes to conclude "source down".

    Only the **network** classes are listed, deliberately excluding
    ``requests.exceptions.HTTPError`` and its umbrella ``RequestException``:
    HTTP status handling is a separate mechanism in ``call()`` (it reads
    ``status_code`` off the returned response) and folding it in here would
    retry 4xx responses.
    """
    found: list[type[Exception]] = [TimeoutError, ConnectionError]

    try:
        from requests.exceptions import ConnectionError as RequestsConnectionError
        from requests.exceptions import Timeout as RequestsTimeout

        found.extend([RequestsConnectionError, RequestsTimeout])
    except ImportError:  # pragma: no cover - requests 是 akshare 的硬依赖
        pass

    try:
        from curl_cffi.requests.errors import RequestsError

        found.append(RequestsError)
    except ImportError:  # pragma: no cover - curl_cffi 是 akshare 的传递依赖
        pass

    return tuple(found)


RETRYABLE_EXCEPTIONS = _network_exception_types()

_SENTINEL = object()  # unique marker for "no return value"


class CircuitState(StrEnum):
    CLOSED = "closed"          # normal operation
    OPEN = "open"              # failing — skip calls
    HALF_OPEN = "half_open"    # probe after cooldown


# ── data classes ─────────────────────────────────────────────────────────


@dataclass(frozen=True)
class SourcePolicy:
    """Declared behaviour for one data source / host pair."""

    source: str
    host: str
    timeout_seconds: float
    max_attempts: int
    base_delay_seconds: float
    max_delay_seconds: float
    min_interval_seconds: float
    circuit_failures: int = 5
    circuit_cooldown_seconds: float = 120.0
    # 额外视为「瞬时故障」的异常类型。默认只有网络类；某些源的**瞬时**上游故障
    # 会被上游库表达成非网络异常（见 POLICIES["legu"]），不声明就会落进
    # `except Exception` 的「schema drift，不可重试」分支，max_attempts 形同虚设。
    retryable_exceptions: tuple[type[Exception], ...] = RETRYABLE_EXCEPTIONS


@dataclass
class FetchMetadata:
    """Observability data collected during one fetch attempt chain."""

    source_name: str
    attempt_count: int = 1
    total_duration_ms: float = 0.0
    fallback_used: bool = False
    circuit_breaker_triggered: bool = False
    http_status_code: int | None = None
    error: str | None = None


@dataclass
class SourceResponse:
    """Result of a ``SourceClient.call`` or ``call_with_fallback``."""

    success: bool
    data: Any
    metadata: FetchMetadata


# ── per-host circuit state ───────────────────────────────────────────────


class _CircuitBreaker:
    """Shared per-host circuit breaker state."""

    def __init__(self, policy: SourcePolicy) -> None:
        self._policy = policy
        self._internal_state = CircuitState.CLOSED
        self._failure_count = 0
        self._last_failure_time = 0.0
        self._lock = threading.Lock()

    @property
    def state(self) -> CircuitState:
        self._maybe_transition()
        return self._internal_state

    @state.setter
    def state(self, value: CircuitState) -> None:
        self._internal_state = value

    def _maybe_transition(self) -> None:
        if self._internal_state is CircuitState.OPEN:
            elapsed = time.monotonic() - self._last_failure_time
            if elapsed >= self._policy.circuit_cooldown_seconds:
                self._internal_state = CircuitState.HALF_OPEN
                logger.info(
                    "🟡 Circuit breaker HALF_OPEN for %s (cooldown elapsed)",
                    self._policy.source,
                )

    def record_failure(self) -> None:
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.monotonic()
            if self._failure_count >= self._policy.circuit_failures:
                self.state = CircuitState.OPEN
                logger.warning(
                    "🔴 Circuit breaker OPEN for %s after %d failures",
                    self._policy.source, self._failure_count,
                )

    def record_success(self) -> None:
        with self._lock:
            self._failure_count = 0
            if self.state is not CircuitState.CLOSED:
                self.state = CircuitState.CLOSED
                logger.info("🟢 Circuit breaker CLOSED for %s", self._policy.source)

    def may_attempt(self) -> bool:
        """Check whether a call may proceed (accounting for cooldown)."""
        with self._lock:
            return self.state in (CircuitState.CLOSED, CircuitState.HALF_OPEN)

    def reset(self) -> None:
        with self._lock:
            self._failure_count = 0
            self.state = CircuitState.CLOSED
            self._last_failure_time = 0.0


# ── per-host rate limiter ────────────────────────────────────────────────


class _RateLimiter:
    """Enforce minimum interval between calls to the same host."""

    def __init__(self, policy: SourcePolicy) -> None:
        self._min_interval = policy.min_interval_seconds
        self._last_call_time = 0.0
        self._lock = threading.Lock()

    def wait_if_needed(self) -> None:
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_call_time
            if elapsed < self._min_interval:
                sleep_for = self._min_interval - elapsed
                time.sleep(sleep_for)
            self._last_call_time = time.monotonic()


# ── main client ──────────────────────────────────────────────────────────


class SourceClient:
    """Transport abstraction for pipeline data sources.

    Usage::

        client = SourceClient()
        resp = client.call("eastmoney", my_fetch_fn, arg1, arg2)
        if resp.success:
            use(resp.data)

    For multi-tier fallback::

        resp = client.call_with_fallback(
            "primary_source", primary_fn,
            fallback_sources=[("fallback_source", fallback_fn)],
        )
    """

    def __init__(
        self,
        policies: dict[str, SourcePolicy] | None = None,
    ) -> None:
        self._policies = dict(POLICIES)
        if policies:
            self._policies.update(policies)

        self._circuits: dict[str, _CircuitBreaker] = {}
        self._rate_limiters: dict[str, _RateLimiter] = {}
        self._sessions: dict[str, Any] = {}
        self._closed = False

        for name, policy in self._policies.items():
            self._circuits[name] = _CircuitBreaker(policy)
            self._rate_limiters[name] = _RateLimiter(policy)

    # ── public API ─────────────────────────────────────────────────────

    def call(
        self,
        source_name: str,
        operation: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> SourceResponse:
        """Execute *operation* under the *source_name* policy.

        The call is subject to retry, circuit breaker and rate-limiter
        defined by the named policy.
        """
        policy = self._policies.get(source_name)
        if policy is None:
            raise ValueError(f"unknown source: {source_name}")

        circuit = self._circuits[source_name]
        limiter = self._rate_limiters[source_name]

        # Circuit breaker gate
        if not circuit.may_attempt():
            meta = FetchMetadata(
                source_name=source_name,
                attempt_count=0,
                circuit_breaker_triggered=True,
                error="circuit breaker open",
            )
            return SourceResponse(success=False, data=None, metadata=meta)

        start = time.monotonic()
        last_error: str | None = None
        http_status: int | None = None

        for attempt in range(1, policy.max_attempts + 1):
            limiter.wait_if_needed()

            try:
                result = operation(*args, **kwargs)

                # Detect HTTP response objects with status_code attribute
                status = getattr(result, "status_code", None)
                if status is not None:
                    http_status = int(status)
                    if http_status in RETRYABLE_HTTP:
                        raise RuntimeError(f"HTTP {http_status}")
                    if http_status in (400, 401, 403, 404):
                        # Non-retryable — fail immediately
                        meta = FetchMetadata(
                            source_name=source_name,
                            attempt_count=attempt,
                            http_status_code=http_status,
                            error=f"HTTP {http_status}",
                        )
                        return SourceResponse(success=False, data=None, metadata=meta)
                    result.raise_for_status()

                circuit.record_success()
                elapsed_ms = (time.monotonic() - start) * 1000
                meta = FetchMetadata(
                    source_name=source_name,
                    attempt_count=attempt,
                    total_duration_ms=round(elapsed_ms, 1),
                )
                return SourceResponse(success=True, data=result, metadata=meta)

            except policy.retryable_exceptions as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                logger.debug(
                    "Attempt %d/%d for %s failed: %s",
                    attempt, policy.max_attempts, source_name, last_error,
                )
                http_status = _extract_http_status(exc)
                if attempt < policy.max_attempts:
                    _backoff_sleep(attempt, policy)
                continue

            except RuntimeError as exc:
                last_error = str(exc)
                http_status = _extract_http_status(exc)
                if http_status in RETRYABLE_HTTP:
                    logger.debug(
                        "Attempt %d/%d for %s failed: HTTP %d",
                        attempt, policy.max_attempts, source_name, http_status,
                    )
                    if attempt < policy.max_attempts:
                        _backoff_sleep(attempt, policy)
                    continue
                # Non-retryable runtime error
                meta = FetchMetadata(
                    source_name=source_name,
                    attempt_count=attempt,
                    http_status_code=http_status,
                    error=last_error,
                )
                return SourceResponse(success=False, data=None, metadata=meta)

            except Exception as exc:
                # Non-retryable failure (schema drift, removed API, etc.)
                last_error = f"{type(exc).__name__}: {exc}"
                logger.debug(
                    "Attempt %d/%d for %s failed (non-retryable): %s",
                    attempt, policy.max_attempts, source_name, last_error,
                )
                meta = FetchMetadata(
                    source_name=source_name,
                    attempt_count=attempt,
                    http_status_code=http_status,
                    error=last_error,
                )
                return SourceResponse(success=False, data=None, metadata=meta)

        # All attempts exhausted
        circuit.record_failure()
        elapsed_ms = (time.monotonic() - start) * 1000
        meta = FetchMetadata(
            source_name=source_name,
            attempt_count=policy.max_attempts,
            total_duration_ms=round(elapsed_ms, 1),
            http_status_code=http_status,
            error=last_error or "all attempts exhausted",
        )
        return SourceResponse(success=False, data=None, metadata=meta)

    def call_with_fallback(
        self,
        primary_source: str,
        primary_operation: Callable[..., Any],
        *,
        fallback_sources: list[tuple[str, Callable[..., Any]]] | None = None,
        **kwargs: Any,
    ) -> SourceResponse:
        """Try *primary_source* first, then each fallback in order.

        The first successful response is returned.  If all sources fail the
        last error response is returned.
        """
        resp = self.call(primary_source, primary_operation, **kwargs)
        if resp.success:
            return resp

        if not fallback_sources:
            return resp

        for fb_source, fb_operation in fallback_sources:
            fb_resp = self.call(fb_source, fb_operation, **kwargs)
            if fb_resp.success:
                fb_resp.metadata.fallback_used = True
                return fb_resp

        return resp

    def close(self) -> None:
        """Release resources (sessions, etc.).  Safe to call multiple times."""
        if self._closed:
            return
        self._closed = True
        for session in self._sessions.values():
            with suppress(Exception):
                session.close()
        self._sessions.clear()
        for circuit in self._circuits.values():
            circuit.reset()

    # ── session management for curl_cffi ───────────────────────────────

    def get_session(self, source_name: str) -> Any:
        """Return a cached requests.Session-like object for *source_name*.

        Sessions are created once per source name and reused for the lifetime
        of the client.  A ``curl_cffi.requests.Session`` is created for
        eastmoney (with browser impersonation), otherwise a plain
        ``requests.Session``.
        """
        cached = self._sessions.get(source_name)
        if cached is not None:
            return cached

        policy = self._policies.get(source_name)
        if policy is None:
            raise ValueError(f"unknown source: {source_name}")

        # curl_cffi 与 requests 的 Session 是两个不同的类，两条分支都合法，
        # 因此显式声明为 Any（否则 mypy 按 try 分支推断，在 except 分支报错）
        session: Any
        try:
            from curl_cffi import requests as curl_requests

            session = curl_requests.Session()
            session.impersonate = "chrome110"
        except ImportError:
            import requests as std_requests

            session = std_requests.Session()
            session.headers.update(
                {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}
            )
        self._sessions[source_name] = session
        return session


# ── helpers ──────────────────────────────────────────────────────────────


def _backoff_sleep(attempt: int, policy: SourcePolicy) -> None:
    """Sleep with exponential backoff + jitter."""
    delay = min(
        policy.base_delay_seconds * (2 ** (attempt - 1)),
        policy.max_delay_seconds,
    )
    jitter = random.uniform(0.75, 1.25)
    time.sleep(delay * jitter)


def _extract_http_status(exc: Exception) -> int | None:
    """Try to extract HTTP status code from an exception message/args."""
    msg = str(exc)
    for prefix in ("HTTP ", "http status "):
        if msg.startswith(prefix):
            try:
                return int(msg.split()[1])
            except (IndexError, ValueError):
                pass
    # Check if the exception has a status_code attribute
    for attr in ("status_code", "code", "status"):
        val = getattr(exc, attr, None)
        if isinstance(val, int):
            return val
    return None


# ── default policies ─────────────────────────────────────────────────────


# ── shared pipeline client ────────────────────────────────────────────────

_DEFAULT_CLIENT: SourceClient | None = None


def get_default_client() -> SourceClient:
    """Return the shared pipeline ``SourceClient`` singleton."""
    global _DEFAULT_CLIENT
    if _DEFAULT_CLIENT is None:
        _DEFAULT_CLIENT = SourceClient()
    return _DEFAULT_CLIENT


def reset_default_client() -> None:
    """Close and clear the shared client (for testing)."""
    global _DEFAULT_CLIENT
    if _DEFAULT_CLIENT is not None:
        _DEFAULT_CLIENT.close()
        _DEFAULT_CLIENT = None


POLICIES: dict[str, SourcePolicy] = {
    "eastmoney": SourcePolicy(
        source="eastmoney", host="eastmoney.com",
        timeout_seconds=15, max_attempts=3,
        base_delay_seconds=1, max_delay_seconds=30,
        min_interval_seconds=0.8,
    ),
    "legu": SourcePolicy(
        source="legu", host="legulegu.com",
        timeout_seconds=15, max_attempts=3,
        base_delay_seconds=1, max_delay_seconds=20,
        min_interval_seconds=0.5,
        # 乐咕把 504 / 反爬页当 HTML 返回，而 akshare 抓 `get_cookie_csrf` 时既不
        # `raise_for_status()` 也不判 None，于是同一场瞬时故障有三种面貌：
        #   AttributeError: 'NoneType' object has no attribute 'attrs'  (缺 _csrf meta)
        #   json.JSONDecodeError                                            (错误页不是 JSON)
        #   TypeError                                                       (None 不可下标)
        # 实测 2026-09-29 站点持续 504，三源（PE/PB/股债利差）同时以
        # AttributeError 失败，且因为不可重试，max_attempts=3 一次都没跑。
        retryable_exceptions=RETRYABLE_EXCEPTIONS
        + (AttributeError, KeyError, TypeError, json.JSONDecodeError),
    ),
    "ths": SourcePolicy(
        source="ths", host="10jqka.com.cn",
        timeout_seconds=15, max_attempts=3,
        base_delay_seconds=1, max_delay_seconds=20,
        min_interval_seconds=0.8,
    ),
    "sina": SourcePolicy(
        source="sina", host="sina.com.cn",
        timeout_seconds=15, max_attempts=2,
        base_delay_seconds=1, max_delay_seconds=10,
        min_interval_seconds=0.5,
    ),
    # 同花顺官方 Financial-API（公测期）：QPS 限 2，4001 走指数退避
    "hithink": SourcePolicy(
        source="hithink", host="fuyao.aicubes.cn",
        timeout_seconds=15, max_attempts=3,
        base_delay_seconds=1, max_delay_seconds=30,
        min_interval_seconds=0.5,
    ),
}
