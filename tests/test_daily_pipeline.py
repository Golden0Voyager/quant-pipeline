"""Tests for daily_pipeline.py - comprehensive mock-based tests."""
from __future__ import annotations

import json
import os
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

import daily_pipeline


# ===========================================================================
# Helpers
# ===========================================================================
def _bars_df(dates: list[str]) -> pd.DataFrame:
    return pd.DataFrame({"trade_date": dates, "open": [10.0] * len(dates), "close": [10.5] * len(dates)})


def _bars_df_with_source(dates: list[str], source: str = "akshare") -> pd.DataFrame:
    return pd.DataFrame({"trade_date": dates, "data_source": [source] * len(dates), "open": [10.0] * len(dates), "close": [10.5] * len(dates)})


def _mock_db_path(db_path_str: str) -> MagicMock:
    db = MagicMock()
    db.db_path = db_path_str
    return db


def _init_health_tables(conn: sqlite3.Connection):
    for tbl in [
        "stock_list", "daily_bars", "fund_flow", "fundamentals",
        "chip_distribution", "historical_valuation", "margin_trading",
        "dragon_tiger", "block_trade", "sector_fund_flow",
    ]:
        conn.execute(f"CREATE TABLE {tbl} (ts_code TEXT, trade_date TEXT)")
    conn.execute("CREATE TABLE indicators (ts_code TEXT, trade_date TEXT, macd_hist REAL, rsi6 REAL)")
    conn.execute("CREATE TABLE shareholder_count (ts_code TEXT, report_date TEXT)")
    conn.execute("INSERT INTO stock_list (ts_code) VALUES ('000001.SZ')")
    conn.execute("INSERT INTO daily_bars (ts_code, trade_date) VALUES ('000001.SZ', '2024-06-21')")
    conn.execute("INSERT INTO indicators (ts_code, trade_date, macd_hist, rsi6) VALUES ('000001.SZ', '2024-06-21', 0.1, 50.0)")
    conn.commit()


@pytest.fixture
def health_db(tmp_path: Path) -> str:
    db_path = str(tmp_path / "quant_core.db")
    conn = sqlite3.connect(db_path)
    _init_health_tables(conn)
    conn.close()
    return db_path


@pytest.fixture
def weekday_mock() -> None:
    with patch("daily_pipeline.datetime") as m:
        m.now.return_value = datetime(2026, 6, 22, 15, 30)
        m.side_effect = lambda *a, **kw: datetime(*a, **kw)
        yield


# ===========================================================================
# _update_single_bar
# ===========================================================================
class TestUpdateSingleBar:
    def test_normal_incremental_success(self):
        db = MagicMock()
        loader = MagicMock()
        db.get_daily_bars.return_value = _bars_df(["2024-01-02"])
        loader.incremental_update.return_value = _bars_df(["2024-01-02", "2024-01-03"])
        with patch("daily_pipeline.time.sleep"), patch("daily_pipeline.logger"):
            r = daily_pipeline._update_single_bar(db, loader, "000001.SZ", watchlist_symbols=set(), backfill_file=Path("/tmp/bf.txt"), backfilled_symbols=set())
        assert r == "success"
        db.save_daily_bars.assert_called_once()

    def test_no_new_data_skipped(self):
        db = MagicMock()
        loader = MagicMock()
        db.get_daily_bars.return_value = _bars_df(["2024-01-02"])
        loader.incremental_update.return_value = _bars_df(["2024-01-02"])
        with patch("daily_pipeline.time.sleep"), patch("daily_pipeline.logger"):
            r = daily_pipeline._update_single_bar(db, loader, "000001.SZ", watchlist_symbols=set(), backfill_file=Path("/tmp/bf.txt"), backfilled_symbols=set())
        assert r == "skipped"
        db.save_daily_bars.assert_not_called()

    def test_full_no_existing_success(self):
        db = MagicMock()
        loader = MagicMock()
        db.get_daily_bars.return_value = pd.DataFrame()
        loader.get_daily_bars.return_value = _bars_df(["2024-01-02"])
        with patch("daily_pipeline.time.sleep"), patch("daily_pipeline.logger"):
            r = daily_pipeline._update_single_bar(db, loader, "000001.SZ", watchlist_symbols=set(), backfill_file=Path("/tmp/bf.txt"), backfilled_symbols=set())
        assert r == "success"
        db.save_daily_bars.assert_called_once()

    def test_empty_bars_skipped(self):
        db = MagicMock()
        loader = MagicMock()
        db.get_daily_bars.return_value = pd.DataFrame()
        loader.get_daily_bars.return_value = pd.DataFrame()
        with patch("daily_pipeline.time.sleep"), patch("daily_pipeline.logger"):
            r = daily_pipeline._update_single_bar(db, loader, "000001.SZ", watchlist_symbols=set(), backfill_file=Path("/tmp/bf.txt"), backfilled_symbols=set())
        assert r == "skipped"
        db.save_daily_bars.assert_not_called()

    def test_yfinance_only_failed(self):
        db = MagicMock()
        loader = MagicMock()
        db.get_daily_bars.return_value = pd.DataFrame()
        loader.get_daily_bars.return_value = _bars_df_with_source(["2024-01-02"], "yfinance")
        with patch("daily_pipeline.time.sleep"), patch("daily_pipeline.logger"):
            r = daily_pipeline._update_single_bar(db, loader, "000001.SZ", watchlist_symbols=set(), backfill_file=Path("/tmp/bf.txt"), backfilled_symbols=set())
        assert r == "failed"
        db.save_daily_bars.assert_not_called()

    def test_mixed_source_allowed(self):
        db = MagicMock()
        loader = MagicMock()
        db.get_daily_bars.return_value = pd.DataFrame()
        df = pd.DataFrame({"trade_date": ["2024-01-02", "2024-01-03"], "data_source": ["akshare", "yfinance"], "open": [10.0, 10.5], "close": [10.5, 11.0]})
        loader.get_daily_bars.return_value = df
        with patch("daily_pipeline.time.sleep"), patch("daily_pipeline.logger"):
            r = daily_pipeline._update_single_bar(db, loader, "000001.SZ", watchlist_symbols=set(), backfill_file=Path("/tmp/bf.txt"), backfilled_symbols=set())
        assert r == "success"
        db.save_daily_bars.assert_called_once()

    def test_watchlist_backfill_success(self, tmp_path):
        db = MagicMock()
        loader = MagicMock()
        bf = tmp_path / "backfilled.txt"
        loader.get_daily_bars.return_value = _bars_df_with_source(["2024-01-02"], "akshare")
        with patch("daily_pipeline.time.sleep"), patch("daily_pipeline.logger"):
            r = daily_pipeline._update_single_bar(db, loader, "000001.SZ", watchlist_symbols={"000001.SZ"}, backfill_file=bf, backfilled_symbols=set())
        assert r == "success"
        db.save_daily_bars.assert_called_once()
        assert bf.read_text().strip() == "000001.SZ"

    def test_watchlist_backfill_yfinance_failed(self, tmp_path):
        db = MagicMock()
        loader = MagicMock()
        loader.get_daily_bars.return_value = _bars_df_with_source(["2024-01-02"], "yfinance")
        with patch("daily_pipeline.time.sleep"), patch("daily_pipeline.logger"):
            r = daily_pipeline._update_single_bar(db, loader, "000001.SZ", watchlist_symbols={"000001.SZ"}, backfill_file=tmp_path / "bf.txt", backfilled_symbols=set())
        assert r == "failed"
        db.save_daily_bars.assert_not_called()

    def test_watchlist_backfill_empty_failed(self):
        db = MagicMock()
        loader = MagicMock()
        loader.get_daily_bars.return_value = pd.DataFrame()
        with patch("daily_pipeline.time.sleep"), patch("daily_pipeline.logger"):
            r = daily_pipeline._update_single_bar(db, loader, "000001.SZ", watchlist_symbols={"000001.SZ"}, backfill_file=Path("/tmp/bf.txt"), backfilled_symbols=set())
        assert r == "failed"
        db.save_daily_bars.assert_not_called()

    def test_exception_retry_exhausted(self):
        db = MagicMock()
        loader = MagicMock()
        db.get_daily_bars.side_effect = Exception("DB error")
        with patch("daily_pipeline.time.sleep"), patch("daily_pipeline.logger"):
            r = daily_pipeline._update_single_bar(db, loader, "000001.SZ", watchlist_symbols=set(), backfill_file=Path("/tmp/bf.txt"), backfilled_symbols=set())
        assert r == "failed"

    def test_watchlist_init_from_db(self, tmp_path):
        db = MagicMock()
        loader = MagicMock()
        db.watchlist_get_all.return_value = pd.DataFrame({"ts_code": ["000001.SZ"]})
        db.get_daily_bars.return_value = pd.DataFrame()
        loader.get_daily_bars.return_value = _bars_df(["2024-01-02"])
        with patch("daily_pipeline.time.sleep"), patch("daily_pipeline.logger"):
            r = daily_pipeline._update_single_bar(db, loader, "000002.SZ", backfill_file=tmp_path / "bf.txt", backfilled_symbols=set())
        assert r == "success"

    def test_watchlist_get_all_exception(self, tmp_path):
        db = MagicMock()
        loader = MagicMock()
        db.watchlist_get_all.side_effect = Exception("db err")
        db.get_daily_bars.return_value = pd.DataFrame()
        loader.get_daily_bars.return_value = _bars_df(["2024-01-02"])
        with patch("daily_pipeline.time.sleep"), patch("daily_pipeline.logger"):
            r = daily_pipeline._update_single_bar(db, loader, "000001.SZ", backfill_file=tmp_path / "bf.txt", backfilled_symbols=set())
        assert r == "success"

    def test_backfilled_init_from_file(self, tmp_path):
        db = MagicMock()
        loader = MagicMock()
        (tmp_path / "watchlist_backfilled.txt").write_text("000001.SZ\n")
        db.get_daily_bars.return_value = pd.DataFrame()
        loader.get_daily_bars.return_value = _bars_df(["2024-01-02"])
        with patch.object(daily_pipeline, "SHARED_DATA_DIR", tmp_path), patch("daily_pipeline.time.sleep"), patch("daily_pipeline.logger"):
            r = daily_pipeline._update_single_bar(db, loader, "000002.SZ")
        assert r == "success"

    def test_backfilled_read_error_recovers(self, tmp_path):
        db = MagicMock()
        loader = MagicMock()
        bf = tmp_path / "watchlist_backfilled.txt"
        bf.write_text("000001.SZ\n")
        bf.chmod(0o200)
        db.get_daily_bars.return_value = pd.DataFrame()
        loader.get_daily_bars.return_value = _bars_df(["2024-01-02"])
        with patch.object(daily_pipeline, "SHARED_DATA_DIR", tmp_path), patch("daily_pipeline.time.sleep"), patch("daily_pipeline.logger"):
            r = daily_pipeline._update_single_bar(db, loader, "000002.SZ")
        bf.chmod(0o644)
        assert r == "success"


# ===========================================================================
# update_indicators
# ===========================================================================
class TestUpdateIndicators:
    @pytest.fixture
    def indicator_db(self, tmp_path: Path) -> str:
        db_path = str(tmp_path / "ind.db")
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE daily_bars (ts_code TEXT, trade_date TEXT)")
        conn.execute("CREATE TABLE indicators (ts_code TEXT, trade_date TEXT, macd_hist REAL, rsi6 REAL)")
        conn.execute("INSERT INTO daily_bars (ts_code, trade_date) VALUES ('000001.SZ', '2024-01-02')")
        conn.commit()
        conn.close()
        return db_path

    def test_smart_detection(self, indicator_db: str):
        db = _mock_db_path(indicator_db)
        engine = MagicMock()
        engine.calculate_all_indicators.return_value = pd.DataFrame({"macd_hist": [0.1]})
        bars_60 = _bars_df([f"2024-01-{d:02d}" for d in range(1, 62)])  # 61 rows
        db.get_daily_bars.return_value = bars_60
        with patch("daily_pipeline.logger"):
            r = daily_pipeline.update_indicators(db, engine)
        assert r["success"] == 1

    def test_with_explicit_symbols(self, indicator_db: str):
        db = _mock_db_path(indicator_db)
        engine = MagicMock()
        engine.calculate_all_indicators.return_value = pd.DataFrame({"macd_hist": [0.1]})
        bars_60 = _bars_df([f"2024-01-{d:02d}" for d in range(1, 62)])
        db.get_daily_bars.return_value = bars_60
        with patch("daily_pipeline.logger"):
            r = daily_pipeline.update_indicators(db, engine, symbols_to_update=["000001.SZ"])
        assert r["success"] == 1

    def test_empty_symbols(self, indicator_db: str):
        db = _mock_db_path(indicator_db)
        engine = MagicMock()
        with patch("daily_pipeline.logger"):
            r = daily_pipeline.update_indicators(db, engine, symbols_to_update=[])
        assert r["total"] == 0

    def test_insufficient_data(self, indicator_db: str):
        db = _mock_db_path(indicator_db)
        engine = MagicMock()
        bars_30 = _bars_df([f"2024-01-{d:02d}" for d in range(1, 31)])
        db.get_daily_bars.return_value = bars_30
        with patch("daily_pipeline.logger"):
            r = daily_pipeline.update_indicators(db, engine, symbols_to_update=["000001.SZ"])
        assert r["insufficient"] == 1

    def test_rename_trade_date(self, indicator_db: str):
        db = _mock_db_path(indicator_db)
        engine = MagicMock()
        bars_60 = pd.DataFrame({"trade_date": [f"2024-01-{d:02d}" for d in range(1, 62)], "close": [10.0] * 61})
        db.get_daily_bars.return_value = bars_60
        engine.calculate_all_indicators.return_value = pd.DataFrame({"macd_hist": [0.1]})
        with patch("daily_pipeline.logger"):
            r = daily_pipeline.update_indicators(db, engine, symbols_to_update=["000001.SZ"])
        assert r["success"] == 1
        df_arg = engine.calculate_all_indicators.call_args[0][0]
        assert "date" in df_arg.columns

    def test_exception_during_calc(self, indicator_db: str):
        db = _mock_db_path(indicator_db)
        engine = MagicMock()
        bars_60 = _bars_df([f"2024-01-{d:02d}" for d in range(1, 62)])
        db.get_daily_bars.return_value = bars_60
        engine.calculate_all_indicators.side_effect = ValueError("calc error")
        with patch("daily_pipeline.logger"):
            r = daily_pipeline.update_indicators(db, engine, symbols_to_update=["000001.SZ"])
        assert r["failed"] == 1


# ===========================================================================
# update_fundamentals
# ===========================================================================
class TestUpdateFundamentals:
    def test_normal(self):
        db = MagicMock()
        loader = MagicMock()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "success": True,
            "result": {
                "data": [
                    {"SECURITY_CODE": "000001", "TRADE_DATE": "2026-06-30",
                     "PE_TTM": 10.0, "PB_MRQ": 1.5, "PS_TTM": 2.0,
                     "PEG_CAR": 1.2, "TOTAL_MARKET_CAP": 1e9},
                    {"SECURITY_CODE": "600000", "TRADE_DATE": "2026-06-30",
                     "PE_TTM": 8.0, "PB_MRQ": 0.8, "PS_TTM": 1.0,
                     "PEG_CAR": None, "TOTAL_MARKET_CAP": 5e9},
                ],
                "count": 2,
            },
        }
        with (
            patch("daily_pipeline.logger"),
            patch("requests.Session") as mock_session_cls,
            patch("daily_pipeline.datetime") as mock_dt,
        ):
            mock_session_cls.return_value.get.return_value = mock_resp
            mock_dt.now.return_value = datetime(2026, 6, 30, 9, 0, 0)
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw) if a else mock_dt.now()
            r = daily_pipeline.update_fundamentals(db, loader)
        assert r["saved"] == 2
        assert r["total"] == 2

    def test_empty(self):
        db = MagicMock()
        loader = MagicMock()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"success": True, "result": {"data": [], "count": 0}}
        with (
            patch("daily_pipeline.logger"),
            patch("requests.Session") as mock_session_cls,
            patch("daily_pipeline.datetime") as mock_dt,
        ):
            mock_session_cls.return_value.get.return_value = mock_resp
            mock_dt.now.return_value = datetime(2026, 6, 30, 9, 0, 0)
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw) if a else mock_dt.now()
            r = daily_pipeline.update_fundamentals(db, loader)
        assert r["saved"] == 0
        assert r["total"] == 0
        db.save_fundamentals.assert_not_called()

    def test_http_error(self):
        db = MagicMock()
        loader = MagicMock()
        mock_session = MagicMock()
        mock_session.get.side_effect = ConnectionError("HTTP error")
        with (
            patch("daily_pipeline.logger"),
            patch("requests.Session", return_value=mock_session),
            patch("daily_pipeline.datetime") as mock_dt,
        ):
            mock_dt.now.return_value = datetime(2026, 6, 30, 9, 0, 0)
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw) if a else mock_dt.now()
            r = daily_pipeline.update_fundamentals(db, loader)
        assert r["saved"] == 0
        assert r["total"] == 0

    def test_empty_code_skipped(self):
        db = MagicMock()
        loader = MagicMock()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "success": True,
            "result": {
                "data": [
                    {"SECURITY_CODE": "", "TRADE_DATE": "2026-06-30",
                     "PE_TTM": None, "PB_MRQ": None, "PS_TTM": None,
                     "PEG_CAR": None, "TOTAL_MARKET_CAP": None},
                    {"SECURITY_CODE": "000001", "TRADE_DATE": "2026-06-30",
                     "PE_TTM": 10.0, "PB_MRQ": 1.5, "PS_TTM": 2.0,
                     "PEG_CAR": None, "TOTAL_MARKET_CAP": 1e9},
                ],
                "count": 2,
            },
        }
        with (
            patch("daily_pipeline.logger"),
            patch("requests.Session") as mock_session_cls,
            patch("daily_pipeline.datetime") as mock_dt,
        ):
            mock_session_cls.return_value.get.return_value = mock_resp
            mock_dt.now.return_value = datetime(2026, 6, 30, 9, 0, 0)
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw) if a else mock_dt.now()
            r = daily_pipeline.update_fundamentals(db, loader)
        assert r["saved"] == 1


# ===========================================================================
# update_fund_flow
# ===========================================================================
class TestUpdateFundFlow:
    def test_normal(self):
        db = MagicMock()
        loader = MagicMock()
        loader.get_market_fund_flow.return_value = pd.DataFrame({"code": ["000001.SZ"], "main_net_inflow": [1e8], "main_net_inflow_pct": [0.05], "super_large_net_inflow": [5e7], "super_large_net_inflow_pct": [0.03], "large_net_inflow": [5e7], "large_net_inflow_pct": [0.02]})
        with patch("daily_pipeline.logger"):
            r = daily_pipeline.update_fund_flow(db, loader)
        assert r["saved"] == 1

    def test_empty(self):
        db = MagicMock()
        loader = MagicMock()
        loader.get_market_fund_flow.return_value = pd.DataFrame()
        with patch("daily_pipeline.logger"):
            r = daily_pipeline.update_fund_flow(db, loader)
        assert r["saved"] == 0

    def test_loader_error(self):
        db = MagicMock()
        loader = MagicMock()
        loader.get_market_fund_flow.side_effect = RuntimeError("fail")
        with patch("daily_pipeline.logger"):
            r = daily_pipeline.update_fund_flow(db, loader)
        assert r["error"] == "fail"

    def test_empty_code_skipped(self):
        db = MagicMock()
        loader = MagicMock()
        loader.get_market_fund_flow.return_value = pd.DataFrame({"code": [""], "main_net_inflow": [1e8], "main_net_inflow_pct": [0.05], "super_large_net_inflow": [5e7], "super_large_net_inflow_pct": [0.03], "large_net_inflow": [5e7], "large_net_inflow_pct": [0.02]})
        with patch("daily_pipeline.logger"):
            r = daily_pipeline.update_fund_flow(db, loader)
        assert r["saved"] == 0


# ===========================================================================
# retry_failed
# ===========================================================================
class TestRetryFailed:
    def test_no_file(self, tmp_path: Path):
        db = MagicMock()
        loader = MagicMock()
        with patch.object(daily_pipeline, "SHARED_DATA_DIR", tmp_path), patch("daily_pipeline.logger"):
            r = daily_pipeline.retry_failed(db, loader)
        assert r["total"] == 0

    def test_empty_file(self, tmp_path: Path):
        db = MagicMock()
        (tmp_path / "retry_queue.txt").write_text("")
        with patch.object(daily_pipeline, "SHARED_DATA_DIR", tmp_path), patch("daily_pipeline.logger"):
            r = daily_pipeline.retry_failed(db, MagicMock())
        assert r["total"] == 0

    def test_normal(self, tmp_path: Path):
        db = MagicMock()
        loader = MagicMock()
        db.get_daily_bars.return_value = _bars_df(["2024-01-02"])
        loader.incremental_update.return_value = _bars_df(["2024-01-02", "2024-01-03"])
        (tmp_path / "retry_queue.txt").write_text("000001.SZ\n000002.SZ\n")
        with patch.object(daily_pipeline, "SHARED_DATA_DIR", tmp_path), patch("daily_pipeline.time.sleep"), patch("daily_pipeline.logger"):
            r = daily_pipeline.retry_failed(db, loader)
        assert r["success"] == 2

    def test_some_still_fail(self, tmp_path: Path):
        (tmp_path / "retry_queue.txt").write_text("000001.SZ\n000002.SZ\n")
        with patch.object(daily_pipeline, "_update_single_bar") as mock_update:
            mock_update.side_effect = ["failed", "success"]
            with patch.object(daily_pipeline, "SHARED_DATA_DIR", tmp_path), patch("daily_pipeline.logger"):
                r = daily_pipeline.retry_failed(MagicMock(), MagicMock())
        assert r["success"] == 1
        assert r["failed"] == 1
        remaining = (tmp_path / "retry_queue.txt").read_text().strip()
        assert remaining in ("000001.SZ", "000002.SZ")


# ===========================================================================
# health_check
# ===========================================================================
class TestHealthCheck:
    def test_healthy(self, health_db: str):
        db = _mock_db_path(health_db)
        with patch("daily_pipeline.datetime") as m:
            m.now.return_value = datetime(2024, 6, 21)
            m.side_effect = lambda *a, **kw: datetime(*a, **kw)
            with patch("daily_pipeline.logger"):
                r = daily_pipeline.health_check(db)
        assert r["issues"] == []

    def test_low_coverage(self, tmp_path: Path):
        db_path = str(tmp_path / "test.db")
        conn = sqlite3.connect(db_path)
        for tbl in ["stock_list", "daily_bars", "fund_flow", "fundamentals",
                     "chip_distribution", "historical_valuation", "margin_trading",
                     "dragon_tiger", "block_trade", "sector_fund_flow"]:
            conn.execute(f"CREATE TABLE {tbl} (ts_code TEXT, trade_date TEXT)")
        conn.execute("CREATE TABLE indicators (ts_code TEXT, trade_date TEXT, macd_hist REAL, rsi6 REAL)")
        conn.execute("CREATE TABLE shareholder_count (ts_code TEXT, report_date TEXT)")
        conn.execute("INSERT INTO stock_list (ts_code) VALUES ('000001.SZ')")
        conn.execute("INSERT INTO stock_list (ts_code) VALUES ('000002.SZ')")
        conn.execute("INSERT INTO daily_bars (ts_code, trade_date) VALUES ('000001.SZ', '2024-06-20')")
        conn.commit()
        conn.close()
        db = _mock_db_path(db_path)
        with patch("daily_pipeline.datetime") as m:
            m.now.return_value = datetime(2024, 6, 21)
            m.side_effect = lambda *a, **kw: datetime(*a, **kw)
            with patch("daily_pipeline.logger"):
                r = daily_pipeline.health_check(db)
        assert any("覆盖率" in i for i in r["issues"])

    def test_null_indicators(self, tmp_path: Path):
        db_path = str(tmp_path / "test.db")
        conn = sqlite3.connect(db_path)
        for tbl in ["stock_list", "daily_bars", "fund_flow", "fundamentals",
                     "chip_distribution", "historical_valuation", "margin_trading",
                     "dragon_tiger", "block_trade", "sector_fund_flow"]:
            conn.execute(f"CREATE TABLE {tbl} (ts_code TEXT, trade_date TEXT)")
        conn.execute("CREATE TABLE indicators (ts_code TEXT, trade_date TEXT, macd_hist REAL, rsi6 REAL)")
        conn.execute("CREATE TABLE shareholder_count (ts_code TEXT, report_date TEXT)")
        conn.execute("INSERT INTO stock_list (ts_code) VALUES ('000001.SZ')")
        conn.execute("INSERT INTO daily_bars (ts_code, trade_date) VALUES ('000001.SZ', '2024-06-20')")
        conn.execute("INSERT INTO indicators (ts_code, trade_date) VALUES ('000001.SZ', '2024-06-20')")
        conn.commit()
        conn.close()
        db = _mock_db_path(db_path)
        with patch("daily_pipeline.datetime") as m:
            m.now.return_value = datetime(2024, 6, 21)
            m.side_effect = lambda *a, **kw: datetime(*a, **kw)
            with patch("daily_pipeline.logger"):
                r = daily_pipeline.health_check(db)
        assert any("指标" in i for i in r["issues"])

    def test_stale_data(self, health_db: str):
        db = _mock_db_path(health_db)
        with patch("daily_pipeline.datetime") as m:
            m.now.return_value = datetime(2024, 7, 1)
            m.side_effect = lambda *a, **kw: datetime(*a, **kw)
            with patch("daily_pipeline.logger"):
                r = daily_pipeline.health_check(db)
        assert any("未更新" in i for i in r["issues"])

    def test_output_smoke(self, health_db: str):
        db = _mock_db_path(health_db)
        with patch("daily_pipeline.datetime") as m:
            m.now.return_value = datetime(2024, 6, 21)
            m.side_effect = lambda *a, **kw: datetime(*a, **kw)
            with patch("daily_pipeline.logger"):
                r = daily_pipeline.health_check(db)
        assert "report" in r


# ===========================================================================
# run_all
# ===========================================================================
class TestRunAll:
    def test_skip_weekend(self):
        db = MagicMock()
        loader = MagicMock()
        engine = MagicMock()
        with patch("daily_pipeline._should_update", return_value=False), patch("daily_pipeline.logger"):
            r = daily_pipeline.run_all(db, loader, engine)
        assert r == {"status": "skipped", "reason": "非交易日"}

    def test_normal_run(self, tmp_path: Path, weekday_mock):
        db = MagicMock()
        db.db_path = str(tmp_path / "quant_core.db")
        loader = MagicMock()
        engine = MagicMock()
        engine.calculate_all_indicators.return_value = pd.DataFrame({"macd_hist": [0.1]})
        db.get_stock_list.return_value = pd.DataFrame({"code": ["000001.SZ"]})
        db.get_daily_bars.return_value = _bars_df([f"2024-01-{d:02d}" for d in range(1, 62)])
        loader.incremental_update.return_value = _bars_df([f"2024-01-{d:02d}" for d in range(1, 62)])
        loader.get_market_valuation.return_value = pd.DataFrame()
        loader.get_market_fund_flow.return_value = pd.DataFrame()
        db.watchlist_get_all.return_value = pd.DataFrame()
        conn = sqlite3.connect(db.db_path)
        conn.execute("CREATE TABLE daily_bars (ts_code TEXT, trade_date TEXT)")
        conn.execute("CREATE TABLE indicators (ts_code TEXT, trade_date TEXT)")
        conn.execute("INSERT INTO daily_bars (ts_code, trade_date) VALUES ('000001.SZ', '2024-01-01')")
        conn.commit()
        conn.close()
        with patch.object(daily_pipeline, "SHARED_DATA_DIR", tmp_path), \
             patch("daily_pipeline.time.sleep"), patch("daily_pipeline.logger"), \
             patch.object(daily_pipeline, "_update_single_bar", return_value="success"), \
             patch.object(daily_pipeline, "update_margin_trading", return_value={"saved": 0, "total": 0}), \
             patch.object(daily_pipeline, "update_dragon_tiger", return_value={"saved": 0, "total": 0}), \
             patch.object(daily_pipeline, "update_block_trade", return_value={"saved": 0, "total": 0}), \
             patch.object(daily_pipeline, "update_sector_fund_flow", return_value={"saved": 0, "total": 0}), \
             patch.object(daily_pipeline, "update_shareholder_count", return_value={"saved": 0, "total": 0}), \
             patch.object(daily_pipeline, "retry_failed", return_value={"success": 0, "failed": 0, "total": 0}), \
             patch.object(daily_pipeline, "health_check", return_value={"issues": []}):
            r = daily_pipeline.run_all(db, loader, engine)
        assert "bars" in r
        assert "indicators" in r
        assert "health" in r


# ===========================================================================
# main() / CLI
# ===========================================================================
class TestMain:
    def test_all(self, weekday_mock):
        with patch.object(sys, "argv", ["daily_pipeline.py"]), \
             patch("daily_pipeline.ProviderFactory") as f, \
             patch("daily_pipeline.run_all") as fn:
            f.configure.return_value = None
            f.get_db.return_value = MagicMock()
            f.get_loader.return_value = MagicMock()
            f.get_indicator_engine.return_value = MagicMock()
            daily_pipeline.main()
            fn.assert_called_once()

    def test_health_check(self, weekday_mock):
        with patch.object(sys, "argv", ["daily_pipeline.py", "--task", "health_check"]), \
             patch("daily_pipeline.ProviderFactory") as f, \
             patch("daily_pipeline.health_check") as fn:
            f.configure.return_value = None
            f.get_db.return_value = MagicMock()
            f.get_loader.return_value = MagicMock()
            f.get_indicator_engine.return_value = MagicMock()
            daily_pipeline.main()
            fn.assert_called_once()

    def test_update_bars_with_limit(self, weekday_mock):
        with patch.object(sys, "argv", ["daily_pipeline.py", "--task", "update_bars", "--limit", "5"]), \
             patch("daily_pipeline.ProviderFactory") as f, \
             patch("daily_pipeline.update_bars") as fn:
            f.configure.return_value = None
            f.get_db.return_value = db = MagicMock()
            f.get_loader.return_value = loader = MagicMock()
            f.get_indicator_engine.return_value = MagicMock()
            daily_pipeline.main()
            fn.assert_called_once_with(db, loader, limit=5, resume=False)

    def test_with_force_and_resume(self, weekday_mock):
        with patch.object(sys, "argv", ["daily_pipeline.py", "--force", "--resume"]), \
             patch("daily_pipeline.ProviderFactory") as f, \
             patch("daily_pipeline.run_all") as fn:
            f.configure.return_value = None
            f.get_db.return_value = db = MagicMock()
            f.get_loader.return_value = loader = MagicMock()
            f.get_indicator_engine.return_value = engine = MagicMock()
            daily_pipeline.main()
            fn.assert_called_once_with(db, loader, engine, resume=True)

    def test_task_retry(self, weekday_mock):
        with patch.object(sys, "argv", ["daily_pipeline.py", "--task", "retry"]), \
             patch("daily_pipeline.ProviderFactory") as f, \
             patch("daily_pipeline.retry_failed") as fn:
            f.configure.return_value = None
            f.get_db.return_value = db = MagicMock()
            f.get_loader.return_value = loader = MagicMock()
            f.get_indicator_engine.return_value = MagicMock()
            daily_pipeline.main()
            fn.assert_called_once_with(db, loader)


# ===========================================================================
# Direct akshare tasks (patch daily_pipeline.ak)
# ===========================================================================
class TestDirectAkShareTasks:
    def test_margin_trading_sse_only(self):
        db = MagicMock()
        df = pd.DataFrame({"标的证券代码": ["000001.SZ"], "融资余额": [1e9], "融资买入额": [1e8], "融资偿还额": [5e7], "融券余量": [1e5], "融券卖出量": [1e4], "融资融券余额": [1.1e9]})
        with patch.object(daily_pipeline, "ak") as ak:
            ak.stock_margin_detail_sse.return_value = df
            ak.stock_margin_detail_szse.return_value = pd.DataFrame()
            with patch("daily_pipeline.datetime") as m:
                m.now.return_value = datetime(2024, 6, 21)
                m.side_effect = lambda *a, **kw: datetime(*a, **kw)
                with patch.object(daily_pipeline, "timedelta") as td:
                    td.return_value = timedelta(days=1)
                    with patch("daily_pipeline.logger"):
                        r = daily_pipeline.update_margin_trading(db)
        assert r["saved"] == 1

    def test_margin_trading_ak_none(self):
        db = MagicMock()
        with patch.object(daily_pipeline, "ak", None), patch("daily_pipeline.logger"):
            r = daily_pipeline.update_margin_trading(db)
        assert r["saved"] == 0

    def test_dragon_tiger_normal(self):
        db = MagicMock()
        df = pd.DataFrame({"代码": ["000001.SZ"], "收盘价": [10.5], "涨跌幅": [0.02], "龙虎榜净买额": [1e8], "龙虎榜买入额": [2e8], "龙虎榜卖出额": [1e8], "换手率": [0.05], "流通市值": [1e9], "上榜原因": ["连续三日"]})
        with patch.object(daily_pipeline, "ak") as ak:
            ak.stock_lhb_detail_em.return_value = df
            with patch("daily_pipeline.datetime") as m:
                m.now.return_value = datetime(2024, 6, 21)
                m.side_effect = lambda *a, **kw: datetime(*a, **kw)
                with patch.object(daily_pipeline, "timedelta") as td:
                    td.return_value = timedelta(days=1)
                    with patch("daily_pipeline.logger"):
                        r = daily_pipeline.update_dragon_tiger(db)
        assert r["saved"] == 1

    def test_dragon_tiger_empty(self):
        db = MagicMock()
        with patch.object(daily_pipeline, "ak") as ak:
            ak.stock_lhb_detail_em.return_value = pd.DataFrame()
            with patch("daily_pipeline.datetime") as m:
                m.now.return_value = datetime(2024, 6, 21)
                m.side_effect = lambda *a, **kw: datetime(*a, **kw)
                with patch.object(daily_pipeline, "timedelta") as td:
                    td.return_value = timedelta(days=1)
                    with patch("daily_pipeline.logger"):
                        r = daily_pipeline.update_dragon_tiger(db)
        assert r["saved"] == 0

    def test_block_trade_normal(self):
        db = MagicMock()
        df = pd.DataFrame({"证券代码": ["000001.SZ"], "成交价": [10.0], "收盘价": [10.5], "折溢率": [-0.05], "成交量": [1e6], "成交额": [1e7], "买方营业部": ["A"], "卖方营业部": ["B"]})
        with patch.object(daily_pipeline, "ak") as ak:
            ak.stock_dzjy_mrmx.return_value = df
            with patch("daily_pipeline.datetime") as m:
                m.now.return_value = datetime(2024, 6, 21)
                m.side_effect = lambda *a, **kw: datetime(*a, **kw)
                with patch.object(daily_pipeline, "timedelta") as td:
                    td.return_value = timedelta(days=1)
                    with patch("daily_pipeline.logger"):
                        r = daily_pipeline.update_block_trade(db)
        assert r["saved"] == 1

    def test_block_trade_empty(self):
        db = MagicMock()
        with patch.object(daily_pipeline, "ak") as ak:
            ak.stock_dzjy_mrmx.return_value = pd.DataFrame()
            with patch("daily_pipeline.datetime") as m:
                m.now.return_value = datetime(2024, 6, 21)
                m.side_effect = lambda *a, **kw: datetime(*a, **kw)
                with patch.object(daily_pipeline, "timedelta") as td:
                    td.return_value = timedelta(days=1)
                    with patch("daily_pipeline.logger"):
                        r = daily_pipeline.update_block_trade(db)
        assert r["saved"] == 0

    def test_sector_fund_flow_normal(self):
        db = MagicMock()
        df = pd.DataFrame({"行业": ["银行"], "主力净流入-净额": [1e9], "主力净流入-净占比": [0.02], "超大单净流入-净额": [5e8], "大单净流入-净额": [5e8], "中单净流入-净额": [-3e8], "小单净流入-净额": [-7e8]})
        with patch.object(daily_pipeline, "ak") as ak:
            ak.stock_sector_fund_flow_hist.return_value = df
            with patch("daily_pipeline.datetime") as m:
                m.now.return_value = datetime(2024, 6, 21)
                m.side_effect = lambda *a, **kw: datetime(*a, **kw)
                with patch.object(daily_pipeline, "timedelta") as td:
                    td.return_value = timedelta(days=1)
                    with patch("daily_pipeline.logger"):
                        r = daily_pipeline.update_sector_fund_flow(db)
        assert r["saved"] == 1

    def test_shareholder_count_normal(self):
        db = MagicMock()
        df = pd.DataFrame({"证券代码": ["000001.SZ"], "本期股东人数": [50000], "股东人数增幅": [-0.05], "本期人均持股数量": [20000]})
        with patch.object(daily_pipeline, "ak") as ak:
            ak.stock_hold_num_cninfo.return_value = df
            with patch("daily_pipeline.datetime") as m:
                m.now.return_value = datetime(2024, 7, 15)
                m.side_effect = lambda *a, **kw: datetime(*a, **kw)
                with patch("daily_pipeline.logger"):
                    r = daily_pipeline.update_shareholder_count(db)
        assert r["saved"] == 1

    def test_shareholder_count_empty_code(self):
        db = MagicMock()
        df = pd.DataFrame({"证券代码": [""], "本期股东人数": [None], "股东人数增幅": [None], "本期人均持股数量": [None]})
        with patch.object(daily_pipeline, "ak") as ak:
            ak.stock_hold_num_cninfo.return_value = df
            with patch("daily_pipeline.datetime") as m:
                m.now.return_value = datetime(2024, 7, 15)
                m.side_effect = lambda *a, **kw: datetime(*a, **kw)
                with patch("daily_pipeline.logger"):
                    r = daily_pipeline.update_shareholder_count(db)
        assert r["saved"] == 0

    def test_shareholder_count_period_selection(self):
        db = MagicMock()
        df = pd.DataFrame({"证券代码": ["000001.SZ"], "本期股东人数": [50000], "股东人数增幅": [-0.05], "本期人均持股数量": [20000]})
        with patch.object(daily_pipeline, "ak") as ak:
            ak.stock_hold_num_cninfo.return_value = df
            cases = [
                (datetime(2024, 1, 15), "20230930"),
                (datetime(2024, 6, 1), "20240331"),
                (datetime(2024, 9, 1), "20240630"),
                (datetime(2024, 11, 15), "20240930"),
            ]
            for now_dt, expected_period in cases:
                with patch("daily_pipeline.datetime") as m:
                    m.now.return_value = now_dt
                    m.side_effect = lambda *a, **kw: datetime(*a, **kw)
                    with patch("daily_pipeline.logger"):
                        daily_pipeline.update_shareholder_count(db)
                ak.stock_hold_num_cninfo.assert_called_with(date=expected_period)


# ===========================================================================
# update_bars: resume & limit
# ===========================================================================
class TestUpdateBarsAdvanced:
    def test_resume_continues(self, tmp_path: Path):
        db = MagicMock()
        loader = MagicMock()
        db.get_stock_list.return_value = pd.DataFrame({"code": [f"{i:06d}.SZ" for i in range(5)]})
        db.get_daily_bars.return_value = _bars_df(["2024-01-02"])
        loader.incremental_update.return_value = _bars_df(["2024-01-02"])
        db.watchlist_get_all.return_value = pd.DataFrame()
        (tmp_path / "progress.json").write_text(json.dumps({"last_symbol": "000002.SZ"}))
        with patch.object(daily_pipeline, "SHARED_DATA_DIR", tmp_path), \
             patch("daily_pipeline.time.sleep"), patch("daily_pipeline.logger"):
            r = daily_pipeline.update_bars(db, loader, resume=True)
        assert r["total"] == 5

    def test_limit_zero(self):
        db = MagicMock()
        db.get_stock_list.return_value = pd.DataFrame({"code": [f"{i:06d}.SZ" for i in range(3)]})
        loader = MagicMock()
        db.get_daily_bars.return_value = _bars_df(["2024-01-02"])
        loader.incremental_update.return_value = _bars_df(["2024-01-02"])
        db.watchlist_get_all.return_value = pd.DataFrame()
        with patch.object(daily_pipeline, "SHARED_DATA_DIR", Path("/tmp/x")), \
             patch("daily_pipeline.time.sleep"), patch("daily_pipeline.logger"):
            r = daily_pipeline.update_bars(db, loader, limit=0)
        assert r["total"] == 3


# ===========================================================================
# Ultra-safe mode
# ===========================================================================
def test_ultra_safe_config():
    with patch.dict(os.environ, {"ULTRA_SAFE": "1"}, clear=False):
        import importlib
        importlib.reload(daily_pipeline)
        assert daily_pipeline.BATCH_SIZE == 30
        assert daily_pipeline.MAX_RETRY == 2
        assert daily_pipeline.RETRY_DELAY == 3.0


# ===========================================================================
# update_bars: data-source filter branch
# ===========================================================================
def test_update_bars_resume_skips_existing(tmp_path: Path):
    """Resume skips symbols before the last checkpoint."""
    db = MagicMock()
    loader = MagicMock()
    db.get_stock_list.return_value = pd.DataFrame({"code": [f"{i:06d}.SZ" for i in range(5)]})
    db.get_daily_bars.return_value = _bars_df(["2024-01-02"])
    loader.incremental_update.return_value = _bars_df(["2024-01-02", "2024-01-03"])
    db.watchlist_get_all.return_value = pd.DataFrame()
    (tmp_path / "progress.json").write_text(json.dumps({"last_symbol": "000003.SZ"}))
    with patch.object(daily_pipeline, "SHARED_DATA_DIR", tmp_path), \
         patch("daily_pipeline.time.sleep"), patch("daily_pipeline.logger"):
        r = daily_pipeline.update_bars(db, loader, resume=True)
    assert r["success"] > 0


# ===========================================================================
# Edge case coverage
# ===========================================================================
def test_update_bars_empty_stock_list():
    db = MagicMock()
    db.get_stock_list.return_value = pd.DataFrame()
    with patch("daily_pipeline.logger"):
        r = daily_pipeline.update_bars(db, MagicMock())
    assert r["total"] == 0


def test_update_fundamentals_save_error():
    db = MagicMock()
    db.save_fundamentals.side_effect = ValueError("save failed")
    loader = MagicMock()
    loader.get_market_valuation.return_value = pd.DataFrame({
        "code": ["000001.SZ"], "pe_ttm": [10.0], "pb": [1.5],
        "ps_ttm": [2.0], "peg": [None], "market_cap": [1e9],
    })
    with patch("daily_pipeline.logger"):
        r = daily_pipeline.update_fundamentals(db, loader)
    assert r["saved"] == 0


def test_update_fund_flow_save_error():
    db = MagicMock()
    db.save_fund_flow.side_effect = ValueError("save failed")
    loader = MagicMock()
    loader.get_market_fund_flow.return_value = pd.DataFrame({
        "code": ["000001.SZ"], "main_net_inflow": [1e8],
        "main_net_inflow_pct": [0.05], "super_large_net_inflow": [5e7],
        "super_large_net_inflow_pct": [0.03], "large_net_inflow": [5e7],
        "large_net_inflow_pct": [0.02],
    })
    with patch("daily_pipeline.logger"):
        r = daily_pipeline.update_fund_flow(db, loader)
    assert r["saved"] == 0


def test_is_trading_day_weekday():
    with patch("daily_pipeline.datetime") as m:
        m.now.return_value = datetime(2026, 6, 22)
        assert daily_pipeline._is_trading_day()


def test_is_trading_day_weekend():
    with patch("daily_pipeline.datetime") as m:
        m.now.return_value = datetime(2026, 6, 27)
        assert not daily_pipeline._is_trading_day()


# ===========================================================================
# New data-layer repair tasks
# ===========================================================================
def test_update_historical_valuation():
    db = MagicMock()
    db.get_fundamentals_batch.return_value = pd.DataFrame({
        "ts_code": ["000001", "000002"],
        "trade_date": ["2026-06-30", "2026-06-30"],
        "pe_ttm": [10.0, 12.0],
        "pb": [1.0, 1.5],
        "ps_ttm": [2.0, 2.5],
        "dividend_yield": [0.03, 0.02],
    })
    with patch("daily_pipeline.logger"):
        r = daily_pipeline.update_historical_valuation(db)
    assert r["saved"] == 2
    db.save_historical_valuation.assert_called()


def test_update_sector_industry():
    db = MagicMock()
    db.db_path = ":memory:"
    db.get_stock_list.return_value = pd.DataFrame({
        "code": ["000001", "000002"],
        "industry": ["银行", "银行"],
    })
    db.get_fundamentals_batch.return_value = pd.DataFrame({
        "ts_code": ["000001", "000002"],
        "pe_ttm": [10.0, 12.0],
        "pb": [1.0, 1.5],
        "ps_ttm": [2.0, 2.5],
        "roe": [0.12, 0.10],
        "revenue_growth": [0.20, 0.15],
        "profit_growth": [0.18, 0.12],
        "market_cap": [1e9, 2e9],
    })
    with patch("daily_pipeline.logger"):
        r = daily_pipeline.update_sector_industry(db)
    assert r["saved"] == 1
    db.save_sector_industry.assert_called_once()
# ===========================================================================
# Sector fund flow tests
# ===========================================================================

@patch("daily_pipeline.ak")
def test_fetch_sector_fund_flow_primary(mock_ak: MagicMock):
    mock_df = pd.DataFrame({
        "行业": ["银行", "医药"],
        "主力净流入-净额": [1e8, 5e7],
        "主力净流入-净占比": [0.5, 0.3],
        "超大单净流入-净额": [5e7, 2e7],
        "大单净流入-净额": [3e7, 1e7],
        "中单净流入-净额": [-2e7, -1e7],
        "小单净流入-净额": [-1e7, -5e6],
    })
    mock_ak.stock_sector_fund_flow_hist.return_value = mock_df
    result = daily_pipeline._fetch_sector_fund_flow_primary("2026-07-01")
    assert result is not None
    assert len(result) == 2
    assert result.iloc[0]["sector_name"] == "银行"


@patch("daily_pipeline.ak")
def test_fetch_sector_fund_flow_primary_empty(mock_ak: MagicMock):
    mock_ak.stock_sector_fund_flow_hist.return_value = pd.DataFrame()
    result = daily_pipeline._fetch_sector_fund_flow_primary("2026-07-01")
    assert result is None


@patch("daily_pipeline.ak")
def test_fetch_sector_fund_flow_fallback(mock_ak: MagicMock):
    mock_df = pd.DataFrame({
        "行业": ["银行", "医药"],
        "净额": [1e8, 5e7],
        "行业-涨跌幅": [0.5, -0.3],
        "流入资金": [2e8, 1e8],
        "流出资金": [1e8, 5e7],
    })
    mock_ak.stock_fund_flow_industry.return_value = mock_df
    result = daily_pipeline._fetch_sector_fund_flow_fallback("2026-07-01")
    assert result is not None
    assert len(result) == 2
    assert result.iloc[0]["data_source"] == "akshare_fallback"


@patch("daily_pipeline.ak")
def test_fetch_sector_fund_flow_fallback_empty(mock_ak: MagicMock):
    mock_ak.stock_fund_flow_industry.return_value = pd.DataFrame()
    result = daily_pipeline._fetch_sector_fund_flow_fallback("2026-07-01")
    assert result is None


@patch("daily_pipeline.ak")
def test_update_sector_fund_flow_primary(mock_ak: MagicMock):
    db = MagicMock()
    mock_df = pd.DataFrame({
        "行业": ["银行"],
        "主力净流入-净额": [1e8],
        "主力净流入-净占比": [0.5],
        "超大单净流入-净额": [5e7],
        "大单净流入-净额": [3e7],
        "中单净流入-净额": [-2e7],
        "小单净流入-净额": [-1e7],
    })
    mock_ak.stock_sector_fund_flow_hist.return_value = mock_df
    with patch("daily_pipeline.logger"):
        r = daily_pipeline.update_sector_fund_flow(db)
    assert r["saved"] == 1
    assert r["source"] == "primary"


@patch("daily_pipeline.ak")
def test_update_sector_fund_flow_fallback(mock_ak: MagicMock):
    """When primary fails, should fall back to secondary source."""
    db = MagicMock()
    # Primary fails
    mock_ak.stock_sector_fund_flow_hist.side_effect = Exception("东财被墙")
    # Fallback succeeds
    mock_df = pd.DataFrame({
        "行业": ["银行"],
        "净额": [1e8],
        "行业-涨跌幅": [0.5],
        "流入资金": [2e8],
        "流出资金": [1e8],
    })
    mock_ak.stock_fund_flow_industry.return_value = mock_df
    with patch("daily_pipeline.logger"):
        r = daily_pipeline.update_sector_fund_flow(db)
    assert r["saved"] == 1
    assert r["source"] == "fallback"


@patch("daily_pipeline.ak")
def test_update_sector_fund_flow_both_fail(mock_ak: MagicMock):
    """When both sources fail, should return empty result."""
    db = MagicMock()
    mock_ak.stock_sector_fund_flow_hist.side_effect = Exception("东财被墙")
    mock_ak.stock_fund_flow_industry.side_effect = Exception("新浪也挂了")
    with patch("daily_pipeline.logger"):
        r = daily_pipeline.update_sector_fund_flow(db)
    assert r["total"] == 0
    assert "error" in r


# ===========================================================================
# update_historical_valuation edge cases
# ===========================================================================

def test_update_historical_valuation_empty():
    db = MagicMock()
    db.get_fundamentals_batch.return_value = pd.DataFrame()
    with patch("daily_pipeline.logger"):
        r = daily_pipeline.update_historical_valuation(db)
    assert r["saved"] == 0
    assert r["total"] == 0


# ===========================================================================
# update_sector_industry edge cases
# ===========================================================================

def test_update_sector_industry_no_fundamentals():
    db = MagicMock()
    db.get_stock_list.return_value = pd.DataFrame({"code": [], "industry": []})
    db.get_fundamentals_batch.return_value = pd.DataFrame()
    with patch("daily_pipeline.logger"):
        r = daily_pipeline.update_sector_industry(db)
    assert r["saved"] == 0 or r["total"] == 0


# ===========================================================================
# BJ filtering in update_bars
# ===========================================================================

def test_update_bars_filters_bj():
    """Verify that BJ stocks are excluded from update_bars stock list."""
    db = MagicMock()
    loader = MagicMock()

    # Create a stock list with a BJ code (880001)
    stocks_df = pd.DataFrame({
        "code": ["000001", "880001", "000002"],
        "name": ["平安银行", "BJ Test", "万科A"],
    })
    db.get_stock_list.return_value = stocks_df
    db.get_daily_bars.return_value = pd.DataFrame()

    with patch("daily_pipeline.is_beijing_stock", side_effect=lambda s: s == "880001"), \
         patch("daily_pipeline.logger"):
        from daily_pipeline import ProgressTracker
        # Clear any previous progress
        ProgressTracker.clear()
        r = daily_pipeline.update_bars(db, loader)

    # Should only process 2 stocks (skip the BJ one)
    assert r["total"] == 2
    assert r["success"] + r["failed"] + r["skipped"] == 2


def test_update_bars_includes_bj_when_configured():
    """Verify that BJ stocks are included in update_bars when INCLUDE_BJ is set."""
    db = MagicMock()
    loader = MagicMock()

    stocks_df = pd.DataFrame({
        "code": ["000001", "880001", "000002"],
        "name": ["平安银行", "BJ Test", "万科A"],
    })
    db.get_stock_list.return_value = stocks_df
    db.get_daily_bars.return_value = pd.DataFrame()

    with patch.dict(os.environ, {"INCLUDE_BJ": "1"}), \
         patch("daily_pipeline.is_beijing_stock", side_effect=lambda s: s == "880001"), \
         patch("daily_pipeline.logger"):
        from daily_pipeline import ProgressTracker
        ProgressTracker.clear()
        r = daily_pipeline.update_bars(db, loader)

    # Should process all 3 stocks (include the BJ one)
    assert r["total"] == 3
    assert r["success"] + r["failed"] + r["skipped"] == 3
