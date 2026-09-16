"""Tests for tasks/global_assets.py — 增量拉取、单只重试与高成功率容忍。"""
from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd

from tasks import global_assets as ga

MODULE = "tasks.global_assets"


def _bars_df() -> pd.DataFrame:
    return pd.DataFrame([{
        "trade_date": "2026-09-12",
        "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.5,
        "adj_close": 100.5, "volume": 1000,
    }])


def _make_db_loader(latest: str | None = "2026-09-10", df: pd.DataFrame | None = None):
    db = MagicMock()
    db.get_global_assets_latest_date.return_value = latest
    db.save_global_assets_bars_batch.side_effect = lambda records: len(records)
    loader = MagicMock()
    loader.fetch_global_assets_bars.return_value = (
        df if df is not None else _bars_df()
    )
    return db, loader


class TestUpdateGlobalAssets:
    def test_incremental_start_from_latest_with_overlap(self):
        """库内有数据 → 从最新日前回退 5 天续拉（INSERT OR REPLACE 幂等）。"""
        db, loader = _make_db_loader(latest="2026-09-10")
        with patch(f"{MODULE}.ProviderFactory") as factory, \
             patch(f"{MODULE}.GLOBAL_ASSETS", ["AAPL"]):
            factory.get_db.return_value = db
            factory.get_loader.return_value = loader
            result = ga.update_global_assets()
        assert result.status == "success"
        loader.fetch_global_assets_bars.assert_called_once_with(
            "AAPL", start_date="2026-09-05"
        )

    def test_full_lookback_when_no_history(self):
        """库内无数据 → 全量回填 730 天。"""
        db, loader = _make_db_loader(latest=None)
        expected_start = (
            datetime.now() - pd.Timedelta(days=730)
        ).strftime("%Y-%m-%d")
        with patch(f"{MODULE}.ProviderFactory") as factory, \
             patch(f"{MODULE}.GLOBAL_ASSETS", ["AAPL"]):
            factory.get_db.return_value = db
            factory.get_loader.return_value = loader
            result = ga.update_global_assets()
        assert result.status == "success"
        loader.fetch_global_assets_bars.assert_called_once_with(
            "AAPL", start_date=expected_start
        )

    def test_empty_response_retried_once(self):
        """空响应（yfinance 限流假报）→ 重试一次；第二次有数则成功。"""
        db, loader = _make_db_loader()
        loader.fetch_global_assets_bars.side_effect = [pd.DataFrame(), _bars_df()]
        with patch(f"{MODULE}.ProviderFactory") as factory, \
             patch(f"{MODULE}.GLOBAL_ASSETS", ["JNJ"]), \
             patch(f"{MODULE}.time.sleep") as mock_sleep:
            factory.get_db.return_value = db
            factory.get_loader.return_value = loader
            result = ga.update_global_assets()
        assert result.status == "success"
        assert loader.fetch_global_assets_bars.call_count == 2
        mock_sleep.assert_called_once_with(10.0)

    def test_persistent_empty_marks_failed_but_tolerated(self):
        """重试后仍空 → 计入失败；成功率 ≥90% 时整体 success 并保留失败清单。"""
        db, loader = _make_db_loader()
        symbols = [f"S{i}" for i in range(10)]

        def fake_fetch(symbol, start_date=None):
            return pd.DataFrame() if symbol == "S9" else _bars_df()

        loader.fetch_global_assets_bars.side_effect = fake_fetch
        with patch(f"{MODULE}.ProviderFactory") as factory, \
             patch(f"{MODULE}.GLOBAL_ASSETS", symbols), \
             patch(f"{MODULE}.time.sleep"):
            factory.get_db.return_value = db
            factory.get_loader.return_value = loader
            result = ga.update_global_assets()
        assert result.status == "success"
        assert result.saved == 9
        assert result.metadata["failed_symbols"] == ["S9"]

    def test_low_success_rate_degraded(self):
        """成功率 <90% → degraded（管道仍会判失败）。"""
        db, loader = _make_db_loader()
        symbols = [f"S{i}" for i in range(10)]

        def fake_fetch(symbol, start_date=None):
            return pd.DataFrame() if symbol in {"S8", "S9"} else _bars_df()

        loader.fetch_global_assets_bars.side_effect = fake_fetch
        with patch(f"{MODULE}.ProviderFactory") as factory, \
             patch(f"{MODULE}.GLOBAL_ASSETS", symbols), \
             patch(f"{MODULE}.time.sleep"):
            factory.get_db.return_value = db
            factory.get_loader.return_value = loader
            result = ga.update_global_assets()
        assert result.status == "degraded"
        assert "S8" in result.error and "S9" in result.error
