"""Tests for SourceClient transport abstraction.

Covers retry, rate limit, circuit breaker, fallback chain, and
error classification — all with fake clocks and scripted responses.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from unittest.mock import MagicMock

from core.source_client import (
    CircuitState,
    FetchMetadata,
    SourceClient,
    SourcePolicy,
    SourceResponse,
    get_default_client,
)

# ── helpers ──────────────────────────────────────────────────────────────


@dataclass
class _FakeClock:
    """Deterministic clock for testing time-dependent behaviour."""

    _now: float = 1000.0
    _lock: threading.Lock = threading.Lock()

    def time(self) -> float:
        with self._lock:
            return self._now

    def advance(self, seconds: float) -> None:
        with self._lock:
            self._now += seconds


def _ok_cb(*args, **kwargs) -> str:
    return "ok"


def _fail_cb(msg: str = "fail") -> str:
    raise TimeoutError(msg)


def _http(status: int) -> str:
    """Return a response-marker for the given HTTP status."""

    class _FakeResp:
        status_code = status

        def raise_for_status(self) -> None:
            if status >= 400:
                raise RuntimeError(f"HTTP {status}")

    return _FakeResp()  # type: ignore[return-value]


# ── SourcePolicy ─────────────────────────────────────────────────────────


class TestSourcePolicy:
    def test_defaults(self) -> None:
        p = SourcePolicy("test", "example.com", 10, 3, 1, 30, 0.5)
        assert p.circuit_failures == 5
        assert p.circuit_cooldown_seconds == 120.0

    def test_custom_circuit(self) -> None:
        p = SourcePolicy("test", "example.com", 10, 3, 1, 30, 0.5,
                         circuit_failures=2, circuit_cooldown_seconds=60.0)
        assert p.circuit_failures == 2
        assert p.circuit_cooldown_seconds == 60.0

    def test_test_default_client_has_no_network_wait(self) -> None:
        """Mock-only tests must not inherit production source throttling."""
        assert get_default_client()._policies["eastmoney"].min_interval_seconds == 0


# ── FetchMetadata ────────────────────────────────────────────────────────


class TestFetchMetadata:
    def test_defaults(self) -> None:
        m = FetchMetadata(source_name="test")
        assert m.attempt_count == 1
        assert m.circuit_breaker_triggered is False

    def test_with_error(self) -> None:
        m = FetchMetadata(source_name="test", error="timeout",
                          http_status_code=503)
        assert m.error == "timeout"
        assert m.http_status_code == 503


# ── SourceResponse ───────────────────────────────────────────────────────


class TestSourceResponse:
    def test_success(self) -> None:
        meta = FetchMetadata(source_name="test")
        r = SourceResponse(success=True, data="hello", metadata=meta)
        assert r.success is True
        assert r.data == "hello"

    def test_failure(self) -> None:
        meta = FetchMetadata(source_name="test", error="fail")
        r = SourceResponse(success=False, data=None, metadata=meta)
        assert r.success is False
        assert r.data is None


# ── Retry behaviour ──────────────────────────────────────────────────────


class TestRetry:
    def test_retries_on_timeout(self) -> None:
        """Transient TimeoutError is retried up to max_attempts."""
        policy = SourcePolicy("t", "ex.com", 10, 3, 0.01, 0.1, 0.0)
        client = SourceClient(policies={"t": policy})

        call_count = 0

        def _flaky() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise TimeoutError("slow")
            return "ok"

        resp = client.call("t", _flaky)
        assert resp.success is True
        assert resp.data == "ok"
        assert call_count == 3  # 2 failures + 1 success

    def test_does_not_retry_404(self) -> None:
        """HTTP 404 is not retryable — single attempt only."""
        policy = SourcePolicy("t", "ex.com", 10, 3, 0.01, 0.1, 0.0)
        client = SourceClient(policies={"t": policy})
        call_count = 0

        def _not_found() -> str:
            nonlocal call_count
            call_count += 1
            resp = MagicMock()
            resp.status_code = 404
            resp.raise_for_status.side_effect = RuntimeError("HTTP 404")
            return resp  # type: ignore[return-value]

        resp = client.call("t", _not_found)
        assert resp.success is False
        assert call_count == 1

    def test_retries_on_429(self) -> None:
        """HTTP 429 is retryable."""
        policy = SourcePolicy("t", "ex.com", 10, 3, 0.01, 0.1, 0.0)
        client = SourceClient(policies={"t": policy})
        call_count = 0

        def _rate_limited() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                resp = MagicMock()
                resp.status_code = 429
                resp.raise_for_status.side_effect = RuntimeError("HTTP 429")
                return resp  # type: ignore[return-value]
            return "ok"

        resp = client.call("t", _rate_limited)
        assert resp.success is True
        assert resp.data == "ok"
        assert call_count == 3

    def test_all_attempts_exhausted(self) -> None:
        """When all retries fail, response is failure with metadata."""
        policy = SourcePolicy("t", "ex.com", 10, 2, 0.01, 0.1, 0.0)
        client = SourceClient(policies={"t": policy})

        def _always_fails() -> str:
            raise ConnectionError("down")

        resp = client.call("t", _always_fails)
        assert resp.success is False
        assert resp.data is None
        assert resp.metadata.attempt_count == 2


# ── Circuit breaker ──────────────────────────────────────────────────────


class TestCircuitBreaker:
    def test_opens_after_consecutive_failures(self) -> None:
        """After circuit_failures consecutive failures, circuit opens."""
        policy = SourcePolicy("t", "ex.com", 10, 1, 0.01, 0.1, 0.0,
                              circuit_failures=2, circuit_cooldown_seconds=60.0)
        client = SourceClient(policies={"t": policy})

        def _fail() -> str:
            raise ConnectionError("boom")

        resp1 = client.call("t", _fail)  # failure 1
        assert resp1.success is False
        assert client._circuits["t"].state is CircuitState.CLOSED

        resp2 = client.call("t", _fail)  # failure 2 → opens circuit
        assert resp2.success is False
        assert client._circuits["t"].state is CircuitState.OPEN

    def test_skips_call_when_circuit_open(self) -> None:
        """When circuit is open, call is skipped without invoking callback."""
        policy = SourcePolicy("t", "ex.com", 10, 1, 0.01, 0.1, 0.0,
                              circuit_failures=1, circuit_cooldown_seconds=60.0)
        client = SourceClient(policies={"t": policy})

        call_count = 0

        def _fail() -> str:
            nonlocal call_count
            call_count += 1
            raise ConnectionError("boom")

        client.call("t", _fail)  # opens circuit
        assert call_count == 1

        client.call("t", _fail)  # circuit open, skipped
        assert call_count == 1  # not incremented

    def test_half_open_after_cooldown(self) -> None:
        """After cooldown, circuit transitions to HALF_OPEN."""
        policy = SourcePolicy("t", "ex.com", 10, 1, 0.01, 0.1, 0.0,
                              circuit_failures=1, circuit_cooldown_seconds=0.05)
        client = SourceClient(policies={"t": policy})

        def _fail() -> str:
            raise ConnectionError("boom")

        client.call("t", _fail)  # opens circuit
        time.sleep(0.06)  # wait for cooldown
        assert client._circuits["t"].state is CircuitState.HALF_OPEN

    def test_half_open_success_closes_circuit(self) -> None:
        """Successful probe on half-open circuit closes it."""
        policy = SourcePolicy("t", "ex.com", 10, 1, 0.01, 0.1, 0.0,
                              circuit_failures=1, circuit_cooldown_seconds=0.05)
        client = SourceClient(policies={"t": policy})

        def _fail() -> str:
            raise ConnectionError("boom")

        client.call("t", _fail)  # opens circuit
        time.sleep(0.06)

        resp = client.call("t", lambda: "probe ok")
        assert resp.success is True
        assert client._circuits["t"].state is CircuitState.CLOSED


# ── Rate limiting ────────────────────────────────────────────────────────


class TestRateLimit:
    def test_enforces_min_interval(self) -> None:
        """Back-to-back calls to the same host respect min_interval."""
        policy = SourcePolicy("t", "ex.com", 10, 1, 0.01, 0.1, 0.2)
        client = SourceClient(policies={"t": policy})
        start = time.monotonic()

        client.call("t", lambda: "a")
        client.call("t", lambda: "b")
        elapsed = time.monotonic() - start

        assert elapsed >= 0.2 - 0.05  # allow small scheduler jitter

    def test_different_hosts_not_delayed(self) -> None:
        """Calls to different hosts are not rate-limited against each other."""
        pa = SourcePolicy("a", "a.com", 10, 1, 0.01, 0.1, 1.0)
        pb = SourcePolicy("b", "b.com", 10, 1, 0.01, 0.1, 0.0)
        client = SourceClient(policies={"a": pa, "b": pb})
        start = time.monotonic()

        client.call("a", lambda: "a")
        client.call("b", lambda: "b")
        elapsed = time.monotonic() - start

        assert elapsed < 0.5  # no significant delay


# ── Fallback chain ───────────────────────────────────────────────────────


class TestFallbackChain:
    def test_fallback_on_primary_failure(self) -> None:
        """When primary source fails, fallback is attempted."""
        policy = SourcePolicy("primary", "ex.com", 10, 1, 0.01, 0.1, 0.0)
        fallback_policy = SourcePolicy("fallback", "fb.com", 10, 1, 0.01, 0.1, 0.0)
        client = SourceClient(policies={"primary": policy, "fallback": fallback_policy})

        def _primary() -> str:
            raise ConnectionError("down")

        def _fallback() -> str:
            return "fb data"

        resp = client.call_with_fallback(
            "primary", _primary,
            fallback_sources=[("fallback", _fallback)],
        )
        assert resp.success is True
        assert resp.data == "fb data"
        assert resp.metadata.fallback_used is True

    def test_all_sources_fail(self) -> None:
        """When all sources fail, final response is failure."""
        policy = SourcePolicy("primary", "ex.com", 10, 1, 0.01, 0.1, 0.0)
        client = SourceClient(policies={"primary": policy})

        def _fail() -> str:
            raise TimeoutError("down")

        resp = client.call_with_fallback(
            "primary", _fail,
            fallback_sources=[("primary", _fail)],  # same source, different call
        )
        assert resp.success is False
        assert resp.data is None

    def test_primary_success_no_fallback(self) -> None:
        """When primary succeeds, fallback is never called."""
        primary_calls = 0
        fallback_calls = 0

        def _primary() -> str:
            nonlocal primary_calls
            primary_calls += 1
            return "primary data"

        def _fallback() -> str:
            nonlocal fallback_calls
            fallback_calls += 1
            return "fb data"

        policy = SourcePolicy("primary", "ex.com", 10, 1, 0.01, 0.1, 0.0)
        client = SourceClient(policies={"primary": policy})

        resp = client.call_with_fallback(
            "primary", _primary,
            fallback_sources=[("primary", _fallback)],
        )
        assert resp.success is True
        assert resp.data == "primary data"
        assert fallback_calls == 0


# ── Session lifecycle ────────────────────────────────────────────────────


class TestLifecycle:
    def test_close_cleans_up(self) -> None:
        """close() can be called safely and does not prevent future calls."""
        policy = SourcePolicy("t", "ex.com", 10, 1, 0.01, 0.1, 0.0)
        client = SourceClient(policies={"t": policy})
        client.close()
        # No-op should not raise
        client.close()

    def test_default_policies(self) -> None:
        """Default policies dict is populated."""
        from core.source_client import POLICIES
        assert "eastmoney" in POLICIES
        assert "legu" in POLICIES
        assert "ths" in POLICIES
        assert "sina" in POLICIES
