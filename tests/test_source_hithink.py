"""Tests for the HiThink (同花顺官方 API) fallback source.

Covers the ApiResponse envelope contract (code=0 / 4001 / 2003),
thscode mapping, DataFrame standardisation, and the
SmartMoneyLoaderProvider fallback wiring — all with fake sessions,
no real network.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, cast
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from core import source_hithink
from core.source_hithink import HithinkClient, HithinkError, to_thscode

_SH = ZoneInfo("Asia/Shanghai")


def _ms(y: int, m: int, d: int) -> int:
    """Asia/Shanghai 零点的毫秒时间戳（与 hithink date_ms 约定一致）。"""
    return int(datetime(y, m, d, tzinfo=_SH).timestamp() * 1000)


def _bar(y: int, m: int, d: int, close: float) -> dict[str, Any]:
    return {
        "date_ms": _ms(y, m, d),
        "volume": 1000.0,
        "turnover": 1000.0 * close,  # hithink turnover = 成交额
        "open_price": close - 0.1,
        "high_price": close + 0.5,
        "low_price": close - 0.5,
        "close_price": close,
    }


def _envelope(code: int, data: dict | None = None, message: str = "") -> dict:
    return {"code": code, "message": message, "request_id": "r-1", "data": data or {}}


class _FakeResp:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._payload


class _FakeSession:
    """Scripted session: each get() pops the next queued payload."""

    def __init__(self, payloads: list[dict]) -> None:
        self._payloads = list(payloads)
        self.trust_env = True
        self.calls: list[dict] = []

    def get(self, url: str, params: dict | None = None,
            headers: dict | None = None, timeout: int | None = None) -> _FakeResp:
        self.calls.append({"url": url, "params": params, "headers": headers})
        if not self._payloads:
            raise AssertionError("unexpected extra request")
        return _FakeResp(self._payloads.pop(0))


def _client(payloads: list[dict]) -> tuple[HithinkClient, _FakeSession]:
    session = _FakeSession(payloads)
    # 测试替身只实现用到的 get()，与 requests.Session 无继承关系，故显式 cast
    return HithinkClient(api_key="test-key", session=cast(Any, session)), session


# ── to_thscode ───────────────────────────────────────────────────────────


class TestToThscode:
    @pytest.mark.parametrize("symbol,expected", [
        ("600519", "600519.SH"),
        ("900901", "900901.SH"),
        ("000001", "000001.SZ"),
        ("200002", "200002.SZ"),
        ("300750", "300750.SZ"),
        ("920001", "920001.BJ"),
    ])
    def test_supported(self, symbol: str, expected: str) -> None:
        assert to_thscode(symbol) == expected

    @pytest.mark.parametrize("symbol", [
        "430047",      # 老北交所代码 hithink 实测不支持（code=1002）
        "830799",
        "sh000300",    # 带前缀指数代码
        "600519.SH",   # 已带后缀
        "",
    ])
    def test_unsupported(self, symbol: str) -> None:
        assert to_thscode(symbol) is None


# ── HithinkClient envelope handling ──────────────────────────────────────


class TestFetchDailyBars:
    def test_success_returns_standardized_df(self) -> None:
        client, session = _client([_envelope(0, {
            "item": [_bar(2026, 8, 13, 10.0), _bar(2026, 8, 14, 11.0)],
            "thscode": "600519.SH",
        })])

        df = client.fetch_daily_bars("600519", start_date="20260813", end_date="2026-08-14")

        assert list(df["date"].dt.strftime("%Y-%m-%d")) == ["2026-08-13", "2026-08-14"]
        assert df["close"].tolist() == [10.0, 11.0]
        # hithink turnover（成交额）→ amount
        assert df["amount"].tolist() == [10000.0, 11000.0]
        # 第二行 pct_change = (11/10 - 1) * 100
        assert df["pct_change"].iloc[1] == pytest.approx(10.0)
        assert df["amplitude"].iloc[1] == pytest.approx((11.5 - 10.5) / 10.0 * 100)
        assert (df["data_source"] == "hithink").all()
        # 请求契约：毫秒时间戳 + X-api-key + forward 复权
        params = session.calls[0]["params"]
        assert params["thscode"] == "600519.SH"
        assert params["adjust"] == "forward"
        assert isinstance(params["start"], int) and params["start"] > 10**12
        assert session.calls[0]["headers"]["X-api-key"] == "test-key"

    def test_empty_items_returns_empty_df(self) -> None:
        client, _ = _client([_envelope(0, {"item": []})])
        assert client.fetch_daily_bars("600519").empty

    def test_unsupported_symbol_returns_empty_without_request(self) -> None:
        client, session = _client([])
        assert client.fetch_daily_bars("430047").empty
        assert session.calls == []

    def test_rate_limit_4001_raises_retryable_429(self) -> None:
        client, _ = _client([_envelope(4001, message="rate limit")])
        with pytest.raises(RuntimeError, match="HTTP 429"):
            client.fetch_daily_bars("600519")

    def test_forbidden_2003_disables_client(self) -> None:
        client, session = _client([_envelope(2003, message="capability revoked")])
        with pytest.raises(HithinkError, match="2003"):
            client.fetch_daily_bars("600519")
        assert not client.available
        # 停用后不再发请求，直接失败
        with pytest.raises(HithinkError, match="停用"):
            client.fetch_daily_bars("600519")
        assert len(session.calls) == 1

    def test_other_business_error_raises(self) -> None:
        client, _ = _client([_envelope(1002, message="Unknown thscode")])
        with pytest.raises(HithinkError, match="1002"):
            client.fetch_daily_bars("600519")

    def test_missing_api_key(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("HITHINK_FINANCE_API_KEY", raising=False)
        client = HithinkClient(session=cast(Any, _FakeSession([])))
        assert not client.available
        with pytest.raises(HithinkError, match="未配置"):
            client.fetch_daily_bars("600519")


# ── SmartMoneyLoaderProvider fallback wiring ─────────────────────────────


def _hithink_df(dates: list[str]) -> pd.DataFrame:
    return pd.DataFrame({
        "date": pd.to_datetime(dates),
        "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.5,
        "volume": 1000.0, "amount": 10000.0,
        "pct_change": 1.0, "amplitude": 2.0,
        "data_source": "hithink",
    })


def _make_provider(monkeypatch: pytest.MonkeyPatch,
                   hithink_df: pd.DataFrame) -> Any:
    """SmartMoneyLoaderProvider with mocked DataLoader + hithink client."""
    from providers import SmartMoneyLoaderProvider

    provider = SmartMoneyLoaderProvider(use_cache=False)
    client = MagicMock()
    client.available = True
    client.fetch_daily_bars.return_value = hithink_df
    monkeypatch.setattr(source_hithink, "get_hithink_client", lambda: client)
    return provider, client


class TestProviderFallback:
    def test_get_daily_bars_falls_back_when_loader_empty(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        provider, client = _make_provider(monkeypatch, _hithink_df(["2026-08-25"]))
        provider._loader.get_daily_bars.return_value = pd.DataFrame()

        df = provider.get_daily_bars("600519", "20260801", "20260826")

        assert not df.empty
        assert (df["data_source"] == "hithink").all()
        client.fetch_daily_bars.assert_called_once()

    def test_get_daily_bars_skips_fallback_when_loader_ok(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        provider, client = _make_provider(monkeypatch, _hithink_df(["2026-08-25"]))
        provider._loader.get_daily_bars.return_value = pd.DataFrame({
            "date": pd.to_datetime(["2026-08-25"]), "close": [1.0],
            "data_source": ["akshare"],
        })

        df = provider.get_daily_bars("600519", "20260801", "20260826")

        assert df["data_source"].iloc[0] == "akshare"
        client.fetch_daily_bars.assert_not_called()

    def test_get_daily_bars_returns_empty_when_hithink_unavailable(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        provider, client = _make_provider(monkeypatch, pd.DataFrame())
        client.available = False
        provider._loader.get_daily_bars.return_value = pd.DataFrame()

        df = provider.get_daily_bars("600519", "20260801", "20260826")

        assert df.empty
        client.fetch_daily_bars.assert_not_called()

    def test_incremental_update_fills_gap_via_hithink(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        new_rows = _hithink_df(["2026-08-26"])
        provider, _ = _make_provider(monkeypatch, new_rows)
        existing = pd.DataFrame({
            "ts_code": ["600519"],
            "trade_date": ["2026-08-25"],
            "close": [10.0],
            "turnover_rate": [0.5],
        })
        # 全链失败：loader 原样返回 existing（无新行）
        provider._loader.incremental_update.return_value = existing.copy()

        df = provider.incremental_update("600519", existing)

        assert len(df) == 2
        # 既有行归一化为 date 列，hithink 新行追加在后
        assert list(df["date"].dt.strftime("%Y-%m-%d")) == ["2026-08-25", "2026-08-26"]
        assert df["data_source"].iloc[-1] == "hithink"
        # turnover_rate 归一为 turnover，避免双列并存写 NULL 的历史事故
        assert "turnover" in df.columns and "turnover_rate" not in df.columns

    def test_incremental_update_skips_fallback_when_loader_fetched(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        provider, client = _make_provider(monkeypatch, _hithink_df(["2026-08-26"]))
        existing = pd.DataFrame({"trade_date": ["2026-08-25"], "close": [10.0]})
        provider._loader.incremental_update.return_value = pd.DataFrame({
            "date": pd.to_datetime(["2026-08-25", "2026-08-26"]),
            "close": [10.0, 10.5], "data_source": ["akshare", "akshare"],
        })

        df = provider.incremental_update("600519", existing)

        assert len(df) == 2
        client.fetch_daily_bars.assert_not_called()
