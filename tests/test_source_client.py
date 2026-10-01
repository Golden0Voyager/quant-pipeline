"""Tests for SourceClient transport abstraction.

Covers retry, rate limit, circuit breaker, fallback chain, and
error classification — all with fake clocks and scripted responses.
"""

from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass
from unittest.mock import MagicMock

import pytest

from core.source_client import (
    POLICIES,
    RETRYABLE_EXCEPTIONS,
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
            return resp

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
                return resp
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


class TestPolicyScopedRetryableExceptions:
    """A source that signals transient failure with a non-network exception type.

    legulegu serves its 504 / anti-bot page as HTML, and akshare scrapes it
    without ``raise_for_status()`` or a None guard, so the transient upstream
    condition surfaces as ``AttributeError: 'NoneType' object has no
    attribute 'attrs'`` (the missing ``_csrf`` meta tag) or
    ``json.JSONDecodeError``. The generic ``except Exception`` branch treats
    both as permanent schema drift, so the policy's three attempts collapse
    to one and a site-wide 504 becomes an instant hard failure.
    """

    def test_policy_declared_exception_type_is_retried(self) -> None:
        policy = SourcePolicy(
            "t", "ex.com", 10, 3, 0.01, 0.1, 0.0,
            retryable_exceptions=(AttributeError,),
        )
        client = SourceClient(policies={"t": policy})
        call_count = 0

        def _flaky() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise AttributeError("'NoneType' object has no attribute 'attrs'")
            return "ok"

        resp = client.call("t", _flaky)
        assert resp.success is True
        assert resp.data == "ok"
        assert call_count == 3

    def test_undeclared_exception_type_still_fails_fast(self) -> None:
        """A policy that says nothing keeps today's one-shot behaviour."""
        policy = SourcePolicy("t", "ex.com", 10, 3, 0.01, 0.1, 0.0)
        client = SourceClient(policies={"t": policy})
        call_count = 0

        def _always_fails() -> str:
            nonlocal call_count
            call_count += 1
            raise AttributeError("boom")

        resp = client.call("t", _always_fails)
        assert resp.success is False
        assert call_count == 1
        assert resp.metadata.attempt_count == 1

    def test_legu_policy_declares_the_scrape_failure_types(self) -> None:
        """The production legu policy must name both observed failure shapes."""
        legu = POLICIES["legu"]
        assert AttributeError in legu.retryable_exceptions
        assert json.JSONDecodeError in legu.retryable_exceptions

    def test_no_other_policy_widens_its_retry_set(self) -> None:
        """Widening is a per-source decision; the rest keep the network-only set."""
        widened = {
            name
            for name, policy in POLICIES.items()
            if set(policy.retryable_exceptions) - set(RETRYABLE_EXCEPTIONS)
        }
        assert widened == {"legu"}


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


# ── Real-library network exceptions ───────────────────────────────────────


class TestRealLibraryNetworkExceptions:
    """真实 HTTP 客户端抛的异常必须与内建异常同等对待。

    背景（2026-10-01 实测）：``RETRYABLE_EXCEPTIONS`` 原本只含**内建**
    ``ConnectionError`` / ``TimeoutError``，而 ``requests`` 与 ``curl_cffi``
    抛的是各自库的异常类——它们只继承 ``OSError``，与内建 ``ConnectionError``
    是兄弟而非子类::

        requests.exceptions.ConnectionError -> RequestException -> OSError
        内建 ConnectionError                              -> OSError
        issubclass(...) = False

    于是真实网络故障不命中 ``except policy.retryable_exceptions``，直接掉进
    ``except Exception`` 的「非可重试：schema drift」分支：**只请求 1 次**、
    无退避，且 ``circuit.record_failure()`` 一次都不调 → **熔断器永不计数**。

    现场后果（``update_concept_board_backfill``，2026-10-01 20:13）：504 个板块
    × 5 个交易日 = 2520 次注定失败的请求打在一个正在拒接的 host 上，耗时 34
    分钟才判定「源端故障」。

    本组测试用**真实库的异常类**驱动 ``call()``，锁住三件事：重试、退避、
    熔断计数。旧实现下三个断言全部失败。
    """

    # 真实库里最高频的网络故障形态：RemoteDisconnected 被 requests 包成
    # requests.exceptions.ConnectionError（实测日志即此形态）。
    @staticmethod
    def _network_exceptions() -> dict[str, BaseException]:
        import http.client

        import requests
        from curl_cffi.requests.errors import RequestsError

        return {
            "requests.ConnectionError": requests.exceptions.ConnectionError(
                ("Connection aborted.", http.client.RemoteDisconnected(
                    "Remote end closed connection without response"))
            ),
            "requests.ConnectTimeout": requests.exceptions.ConnectTimeout("timed out"),
            "requests.ReadTimeout": requests.exceptions.ReadTimeout("timed out"),
            "curl_cffi.RequestsError": RequestsError(
                "curl: (56) Connection closed abruptly"
            ),
        }

    @pytest.mark.parametrize("label", list(_network_exceptions()))
    def test_network_exceptions_are_declared_retryable(self, label: str) -> None:
        """真实库的异常类型必须在 ``RETRYABLE_EXCEPTIONS`` 里被显式覆盖。

        这是**类型层面**的断言，不依赖 ``call()`` 的行为，因此能在任何重试 /
        熔断逻辑被改动时依然钉住「这些异常被认定为瞬时故障」这一前提。
        """
        cls = type(self._network_exceptions()[label])
        assert any(issubclass(cls, retryable) for retryable in RETRYABLE_EXCEPTIONS), (
            f"{label} 未被认定为可重试 —— 它只继承 OSError，与内建 "
            f"ConnectionError 是兄弟类；真实网络故障会掉进 "
            f"except Exception 的「不可重试」分支"
        )

    @pytest.mark.parametrize("label", list(_network_exceptions()))
    def test_network_exception_is_retried_to_max_attempts(self, label: str) -> None:
        """一次瞬时网络故障必须重试满 max_attempts 次，而不是只试 1 次。"""
        exc = self._network_exceptions()[label]
        policy = SourcePolicy("t", "ex.com", 10, 3, 0.0, 0.0, 0.0)
        client = SourceClient(policies={"t": policy})
        state = {"n": 0}

        def _flaky() -> str:
            state["n"] += 1
            if state["n"] < 3:
                raise exc
            return "ok"

        resp = client.call("t", _flaky)
        assert resp.success is True, f"{label}: 两次瞬时故障后应成功"
        assert state["n"] == 3, (
            f"{label}: 实际请求 {state['n']} 次，应为 3 —— 说明该异常"
            f"未被识别为可重试"
        )

    def test_network_failure_counts_toward_circuit_breaker(self) -> None:
        """连续的网络故障必须计入熔断器，达到阈值即开闸。

        旧实现下 ``record_failure()`` 从不被调用，``_failure_count`` 恒为 0，
        熔断器永不打开——这正是 2026-10-01 那 2520 次请求未被掐断的原因。
        """
        exc = self._network_exceptions()["requests.ConnectionError"]
        policy = SourcePolicy("t", "ex.com", 10, 3, 0.0, 0.0, 0.0, circuit_failures=3)
        client = SourceClient(policies={"t": policy})

        def _always_down() -> str:
            raise exc

        for _ in range(3):
            assert client.call("t", _always_down).success is False

        assert client._circuits["t"].state is CircuitState.OPEN, (
            "连续 3 次真实网络故障（circuit_failures=3）后熔断器未打开 —— "
            "record_failure() 没被调用，重试与熔断机制整体失效"
        )

    def test_network_failure_is_not_labelled_as_schema_drift(self) -> None:
        """瞬时网络故障不得被当成「schema drift / 不可重试」。

        ``error`` 文案是对上游归因的唯一线索；瞬时故障报成契约漂移，会把运维
        引向「接口改了」而不是「源在限流」。
        """
        exc = self._network_exceptions()["requests.ConnectionError"]
        policy = SourcePolicy("t", "ex.com", 10, 3, 0.0, 0.0, 0.0)
        client = SourceClient(policies={"t": policy})

        def _down() -> str:
            raise exc

        resp = client.call("t", _down)
        assert resp.metadata.attempt_count == 3, (
            f"attempt_count={resp.metadata.attempt_count}，应为 3"
        )


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
