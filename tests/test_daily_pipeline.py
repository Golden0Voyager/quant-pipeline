"""Tests for daily_pipeline.py - comprehensive mock-based tests."""
from __future__ import annotations

import json
import os
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

import daily_pipeline
from core.refresh import CrossSourceCheckConfig, RefreshOrchestrator
from core.refresh_audit import CrossSourceTolerance
from core.refresh_cross_source import XueqiuCrossSourceVerifier
from core.task_result import ErrorKind, TaskResult


# ===========================================================================
# Fixtures
# ===========================================================================
@pytest.fixture(autouse=True)
def mock_pipeline_lock():
    """Bypass flock logic so tests don't fail when the daemon is running."""
    with patch("daily_pipeline._acquire_lock"), \
         patch("daily_pipeline._release_lock"), \
         patch("core.lock.TaskLock.acquire", return_value=True), \
         patch("core.lock.TaskLock.release"):
        yield


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

    def test_existing_latest_expected_trade_date_skipped_without_fetch(self):
        db = MagicMock()
        loader = MagicMock()
        db.get_latest_bar_date.return_value = "2026-07-17"
        with patch("tasks.bars.get_expected_latest_trading_day", return_value="2026-07-17"), \
             patch("daily_pipeline.time.sleep"), patch("daily_pipeline.logger"):
            r = daily_pipeline._update_single_bar(
                db,
                loader,
                "000001.SZ",
                watchlist_symbols=set(),
                backfill_file=Path("/tmp/bf.txt"),
                backfilled_symbols=set(),
            )
        assert r == "skipped"
        db.get_daily_bars.assert_not_called()
        loader.incremental_update.assert_not_called()

    def test_stale_unchanged_incremental_counts_failed(self):
        db = MagicMock()
        loader = MagicMock()
        db.get_latest_bar_date.return_value = "2026-07-16"
        existing = _bars_df(["2026-07-16"])
        db.get_daily_bars.return_value = existing
        loader.incremental_update.return_value = existing
        with patch("tasks.bars.get_expected_latest_trading_day", return_value="2026-07-17"), \
             patch("daily_pipeline.time.sleep"), patch("daily_pipeline.logger"):
            r = daily_pipeline._update_single_bar(
                db,
                loader,
                "000001.SZ",
                watchlist_symbols=set(),
                backfill_file=Path("/tmp/bf.txt"),
                backfilled_symbols=set(),
            )
        assert r == "failed"
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
        # 插入 61 行日线数据，满足 smart detection HAVING COUNT(*) >= 60
        dates_61 = [f"2024-01-{d:02d}" for d in range(1, 32)] + [f"2024-02-{d:02d}" for d in range(1, 31)]
        for d in dates_61:
            conn.execute("INSERT INTO daily_bars (ts_code, trade_date) VALUES ('000001.SZ', ?)", (d,))
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
# update_chip_distribution_em
# ===========================================================================
class TestUpdateChipDistributionEm:
    def test_aborts_after_nine_consecutive_fetch_failures(self):
        db = MagicMock()
        symbols = [f"00000{i}.SZ" for i in range(1, 12)]

        with patch("tasks.index_chain._get_chip_em_target_symbols", return_value=symbols), \
             patch("tasks.index_chain._fetch_cyq_em", return_value=None) as fetch, \
             patch("tasks.index_chain.time.sleep"), \
             patch("tasks.index_chain.logger"):
            r = daily_pipeline.update_chip_distribution_em(db)

        assert r["aborted"] is True
        assert r["abort_reason"] == "consecutive_failures"
        assert r["failed"] == 9
        assert r["total"] == len(symbols)
        assert r["processed"] == 9
        assert fetch.call_count == 9

    def test_empty_target_result_has_consistent_status_fields(self):
        db = MagicMock()

        with patch("tasks.index_chain._get_chip_em_target_symbols", return_value=[]), \
             patch("tasks.index_chain.logger"):
            r = daily_pipeline.update_chip_distribution_em(db)

        assert r == {
            "success": 0,
            "failed": 0,
            "skipped": 0,
            "total": 0,
            "processed": 0,
            "aborted": False,
        }


# ===========================================================================
# fetch_chip_em_record_for_refresh（Task 7 收盘刷新 helper）
# ===========================================================================
class TestFetchChipEmRecordForRefresh:
    """只抽目标日一行，绝不调用随机目标选择器，不写库。"""

    @staticmethod
    def _em_df(dates: list[str], profit: float = 0.5, avg: float = 10.0) -> pd.DataFrame:
        n = len(dates)
        return pd.DataFrame({
            "trade_date": dates,
            "profit_ratio": [profit] * n,
            "avg_cost": [avg] * n,
            "cost_90_low": [9.0] * n,
            "cost_90_high": [11.0] * n,
            "concentration_90": [0.2] * n,
            "cost_70_low": [9.5] * n,
            "cost_70_high": [10.5] * n,
            "concentration_70": [0.1] * n,
        })

    def test_returns_only_target_date_record(self):
        from tasks.index_chain import fetch_chip_em_record_for_refresh

        df = self._em_df(["2026-07-23", "2026-07-24", "2026-07-27"])
        record, reason = fetch_chip_em_record_for_refresh(
            "000001.SZ", "2026-07-27", fetch=lambda symbol: df
        )

        assert reason is None
        assert record["ts_code"] == "000001.SZ"
        assert record["trade_date"] == "2026-07-27"
        assert record["profit_ratio"] == 0.5
        assert record["avg_cost"] == 10.0
        # EM 源没有 chip_concentration 列，不得捆绑写入
        assert "chip_concentration" not in record

    def test_fetch_failure_reason(self):
        from tasks.index_chain import fetch_chip_em_record_for_refresh

        record, reason = fetch_chip_em_record_for_refresh(
            "000001.SZ", "2026-07-27", fetch=lambda symbol: None
        )
        assert record is None
        assert reason == "fetch_failed"

    def test_missing_target_date(self):
        from tasks.index_chain import fetch_chip_em_record_for_refresh

        df = self._em_df(["2026-07-23", "2026-07-24"])
        record, reason = fetch_chip_em_record_for_refresh(
            "000001.SZ", "2026-07-27", fetch=lambda symbol: df
        )
        assert record is None
        assert reason == "missing_target"

    def test_all_zero_target_row_is_invalid(self):
        from tasks.index_chain import fetch_chip_em_record_for_refresh

        df = self._em_df(["2026-07-27"], profit=0.0, avg=0.0)
        record, reason = fetch_chip_em_record_for_refresh(
            "000001.SZ", "2026-07-27", fetch=lambda symbol: df
        )
        assert record is None
        assert reason == "invalid"

    def test_nan_target_row_is_invalid(self):
        from tasks.index_chain import fetch_chip_em_record_for_refresh

        df = self._em_df(["2026-07-27"], profit=float("nan"))
        record, reason = fetch_chip_em_record_for_refresh(
            "000001.SZ", "2026-07-27", fetch=lambda symbol: df
        )
        assert record is None
        assert reason == "invalid"

    def test_never_calls_random_target_selector(self):
        """刷新模式下绝不调用 _get_chip_em_target_symbols（随机兼底）。"""
        from tasks.index_chain import fetch_chip_em_record_for_refresh

        df = self._em_df(["2026-07-27"])
        with patch(
            "tasks.index_chain._get_chip_em_target_symbols",
            side_effect=AssertionError("random selector must not be called"),
        ):
            record, reason = fetch_chip_em_record_for_refresh(
                "000001.SZ", "2026-07-27", fetch=lambda symbol: df
            )
        assert reason is None
        assert record["trade_date"] == "2026-07-27"


# ===========================================================================
# New derived data task field mapping
# ===========================================================================
class TestDerivedDataTaskMappings:
    def test_restricted_share_uses_start_and_end_date_parameters(self):
        import tasks.corporate_actions as corporate_actions

        mock_ak = MagicMock()
        mock_ak.stock_restricted_release_detail_em.return_value = pd.DataFrame(
            {
                "股票代码": ["000001"],
                "股票简称": ["平安银行"],
                "解禁时间": ["2026-07-15"],
                "限售股类型": ["首发原股东限售股份"],
                "实际解禁数量": [100.5],
                "解禁数量": [200.5],
            }
        )

        with patch.object(corporate_actions, "ak", mock_ak), \
             patch.object(corporate_actions, "get_expected_latest_trading_day", return_value="2026-07-19"):
            records = corporate_actions._fetch_restricted_share()

        assert records[0]["ts_code"] == "000001"
        assert records[0]["name"] == "平安银行"
        assert records[0]["release_date"] == "2026-07-15"
        assert records[0]["total_shares"] == 200.5
        assert records[0]["market_type"] == "首发原股东限售股份"
        # 单次区间查询：起始 = 最近交易日回溯 30 天，结束 = 最近交易日
        call_kwargs = mock_ak.stock_restricted_release_detail_em.call_args_list[0].kwargs
        assert call_kwargs == {"start_date": "20260619", "end_date": "20260719"}

    def test_ah_premium_maps_actual_akshare_columns(self):
        import tasks.finance_flow as finance_flow

        mock_ak = MagicMock()
        mock_ak.stock_zh_ah_spot_em.return_value = pd.DataFrame(
            {
                "A股代码": ["000001"],
                "H股代码": ["00001"],
                "名称": ["平安银行"],
                "最新价-RMB": [10.1],
                "最新价-HKD": [11.2],
                "溢价": [5.5],
            }
        )

        with patch.object(finance_flow, "ak", mock_ak), \
             patch.object(finance_flow, "get_expected_latest_trading_day", return_value="2026-07-19"):
            records = finance_flow._fetch_ah_premium()

        assert records == [
            {
                "trade_date": "2026-07-19",
                "ts_code": "000001",
                "h_code": "00001",
                "name": "平安银行",
                "h_price": 11.2,
                "a_price": 10.1,
                "premium": 5.5,
                "data_source": "akshare",
            }
        ]

    def test_sector_valuation_uses_current_date_and_actual_columns(self):
        import tasks.sector_derivatives as sector_derivatives

        mock_ak = MagicMock()
        mock_ak.stock_industry_pe_ratio_cninfo.return_value = pd.DataFrame(
            {
                "行业名称": ["银行"],
                "变动日期": ["2026-07-19"],
                "静态市盈率-加权平均": [6.1],
                "市净率": [0.7],
                "总市值-静态": [123456.0],
            }
        )

        with patch.object(sector_derivatives, "ak", mock_ak), \
             patch.object(sector_derivatives, "get_expected_latest_trading_day", return_value="2026-07-19"):
            records = sector_derivatives._fetch_sector_valuation()

        mock_ak.stock_industry_pe_ratio_cninfo.assert_called_once_with(
            symbol="证监会行业分类", date="20260719"
        )
        assert records == [
            {
                "sector_name": "银行",
                "trade_date": "2026-07-19",
                "pe": 6.1,
                "pb": 0.7,
                "total_mv": 123456.0,
                "data_source": "akshare",
            }
        ]

    def test_parse_chinese_quarter_label(self):
        import tasks.china_macro as china_macro

        assert china_macro._parse_quarter("2024年第一季度") == "2024-Q1"
        assert china_macro._parse_quarter("2024年第四季度") == "2024-Q4"


# ===========================================================================
# update_fundamentals
# ===========================================================================
class TestUpdateFundamentals:
    def test_normal(self):
        db = MagicMock()
        db.save_fundamentals_batch.return_value = 2
        db.count_fundamentals_for_date.return_value = 0
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
        db.save_fundamentals_batch.return_value = 2
        with (
            patch("daily_pipeline.logger"),
            patch("tasks.valuation_chain.get_default_client") as mock_gc,
            patch("daily_pipeline.datetime") as mock_dt,
        ):
            mock_gc.return_value.get_session.return_value.get.return_value = mock_resp
            mock_dt.now.return_value = datetime(2026, 6, 30, 9, 0, 0)
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw) if a else mock_dt.now()
            r = daily_pipeline.update_fundamentals(db, loader)
        assert r["saved"] == 2
        assert r["total"] == 2

    def test_empty(self):
        db = MagicMock()
        db.count_fundamentals_for_date.return_value = 0
        loader = MagicMock()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"success": True, "result": {"data": [], "count": 0}}
        with (
            patch("daily_pipeline.logger"),
            patch("tasks.valuation_chain.get_default_client") as mock_gc,
            patch("daily_pipeline.datetime") as mock_dt,
        ):
            mock_gc.return_value.get_session.return_value.get.return_value = mock_resp
            mock_dt.now.return_value = datetime(2026, 6, 30, 9, 0, 0)
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw) if a else mock_dt.now()
            r = daily_pipeline.update_fundamentals(db, loader)
        assert r["saved"] == 0
        assert r["total"] == 0
        db.save_fundamentals.assert_not_called()

    def test_http_error(self):
        db = MagicMock()
        db.count_fundamentals_for_date.return_value = 0
        loader = MagicMock()
        with (
            patch("daily_pipeline.logger"),
            patch("tasks.valuation_chain.get_default_client") as mock_gc,
            patch("daily_pipeline.datetime") as mock_dt,
        ):
            mock_gc.return_value.get_session.return_value.get.side_effect = ConnectionError("HTTP error")
            mock_dt.now.return_value = datetime(2026, 6, 30, 9, 0, 0)
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw) if a else mock_dt.now()
            r = daily_pipeline.update_fundamentals(db, loader)
        assert r["saved"] == 0
        assert r["total"] == 0

    def test_empty_code_skipped(self):
        db = MagicMock()
        db.save_fundamentals_batch.return_value = 1
        db.count_fundamentals_for_date.return_value = 0
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
            patch("tasks.valuation_chain.get_default_client") as mock_gc,
            patch("daily_pipeline.datetime") as mock_dt,
        ):
            mock_gc.return_value.get_session.return_value.get.return_value = mock_resp
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
        db.save_fund_flow_batch.return_value = 1
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

    def test_all_nan_rows_skipped(self):
        """All-NaN numeric fields should be skipped — save_fund_flow not called."""
        db = MagicMock()
        loader = MagicMock()
        loader.get_market_fund_flow.return_value = pd.DataFrame({
            "code": ["000001.SZ"],
            "main_net_inflow": [float("nan")],
            "main_net_inflow_pct": [float("nan")],
            "super_large_net_inflow": [float("nan")],
            "super_large_net_inflow_pct": [float("nan")],
            "large_net_inflow": [float("nan")],
            "large_net_inflow_pct": [float("nan")],
        })
        with patch("daily_pipeline.logger"):
            r = daily_pipeline.update_fund_flow(db, loader)
        assert r["saved"] == 0
        db.save_fund_flow.assert_not_called()


# ===========================================================================
# retry_failed
# ===========================================================================
class TestRetryFailed:
    def _make_progress(self, tmp_path: Path, failed: list[str]):
        import json
        prog = tmp_path / "progress.json"
        prog.write_text(json.dumps({"failed_queue": failed, "task": "retry"}))

    def test_no_progress(self, tmp_path: Path):
        db = MagicMock()
        loader = MagicMock()
        with patch.object(daily_pipeline.ProgressTracker, "FILE", tmp_path / "progress.json"), patch("daily_pipeline.logger"):
            r = daily_pipeline.retry_failed(db, loader)
        assert r["total"] == 0

    def test_empty_queue(self, tmp_path: Path):
        self._make_progress(tmp_path, [])
        db = MagicMock()
        with patch.object(daily_pipeline.ProgressTracker, "FILE", tmp_path / "progress.json"), patch("daily_pipeline.logger"):
            r = daily_pipeline.retry_failed(db, MagicMock())
        assert r["total"] == 0

    def test_normal(self, tmp_path: Path):
        self._make_progress(tmp_path, ["000001.SZ", "000002.SZ"])
        db = MagicMock()
        loader = MagicMock()
        db.get_daily_bars.return_value = _bars_df(["2024-01-02"])
        loader.incremental_update.return_value = _bars_df(["2024-01-02", "2024-01-03"])
        with patch.object(daily_pipeline.ProgressTracker, "FILE", tmp_path / "progress.json"), patch("daily_pipeline.time.sleep"), patch("daily_pipeline.logger"):
            r = daily_pipeline.retry_failed(db, loader)
        assert r["success"] == 2

    def test_some_still_fail(self, tmp_path: Path):
        self._make_progress(tmp_path, ["000001.SZ", "000002.SZ"])
        with patch("tasks.utility._update_single_bar") as mock_update:
            mock_update.side_effect = ["failed", "success"]
            with patch.object(daily_pipeline.ProgressTracker, "FILE", tmp_path / "progress.json"), patch("daily_pipeline.logger"):
                r = daily_pipeline.retry_failed(MagicMock(), MagicMock())
        assert r["success"] == 1
        assert r["failed"] == 1
        # 确认 progress.json 仍保留剩余失败的记录
        import json
        saved = json.loads((tmp_path / "progress.json").read_text())
        assert "000001.SZ" in saved["failed_queue"]
        assert "000002.SZ" not in saved["failed_queue"]


# ===========================================================================
# health_check
# ===========================================================================
class TestHealthCheck:
    def test_healthy(self, health_db: str):
        db = _mock_db_path(health_db)
        with patch("tasks.utility.get_expected_latest_trading_day", return_value="2024-06-20"), \
             patch("tasks.utility.logger"):
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
        with patch("tasks.utility.get_expected_latest_trading_day", return_value="2024-06-25"), \
             patch("tasks.utility.logger"):
            r = daily_pipeline.health_check(db)
        assert any("未更新" in i for i in r["issues"])

    def test_output_smoke(self, health_db: str):
        db = _mock_db_path(health_db)
        with patch("tasks.utility.get_expected_latest_trading_day", return_value="2024-06-20"), \
             patch("tasks.utility.logger"):
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

    def test_target_cadences_skip_monthly_and_quarterly(self, tmp_path: Path, weekday_mock):
        db = MagicMock()
        db.db_path = str(tmp_path / "quant_core.db")
        loader = MagicMock()
        engine = MagicMock()
        with patch("daily_pipeline._should_update", return_value=True), \
             patch("daily_pipeline.logger"), \
             patch("daily_pipeline._safe_task", return_value={"status": "ok"}) as safe_task:
            r = daily_pipeline.run_all(db, loader, engine, target_cadences={daily_pipeline.Cadence.TRADING_DAY, daily_pipeline.Cadence.DAILY})
        # 股票列表是月度任务，应被跳过
        assert r["stock_list"]["status"] == "skipped"
        # 季度财务是季度任务，也应被跳过
        assert r["quarterly_financials"]["status"] == "skipped"
        # 日线行情是交易日任务，应该执行
        assert safe_task.call_args_list[0].args[0] == "update_bars"

    def test_trading_day_cadence_runs_market_snapshot_with_canonical_name(
        self, tmp_path: Path, weekday_mock
    ):
        db = MagicMock()
        db.db_path = str(tmp_path / "quant_core.db")
        loader = MagicMock()
        engine = MagicMock()

        with patch(
            "daily_pipeline._should_update", return_value=True
        ), patch(
            "daily_pipeline.logger"
        ), patch(
            "daily_pipeline._safe_task", return_value={"status": "ok"}
        ) as safe_task:
            daily_pipeline.run_all(
                db,
                loader,
                engine,
                target_cadences={daily_pipeline.Cadence.TRADING_DAY},
            )

        safe_task.assert_any_call(
            "update_market_snapshot",
            daily_pipeline.update_market_snapshot,
            db,
        )

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
             patch("daily_pipeline._should_update", return_value=True), \
             patch.object(
                 daily_pipeline,
                 "_safe_task",
                 return_value={"status": "ok"},
             ) as safe_task:
            r = daily_pipeline.run_all(db, loader, engine)
        assert "bars" in r
        assert "indicators" in r
        assert "health" in r
        assert safe_task.call_count > 20

    def test_chip_tasks_run_after_all_data_update_tasks(self, tmp_path: Path, weekday_mock):
        db = MagicMock()
        db.db_path = str(tmp_path / "quant_core.db")
        loader = MagicMock()
        engine = MagicMock()

        with patch("daily_pipeline._should_update", return_value=True), \
             patch("daily_pipeline._safe_task", return_value={"status": "ok"}) as safe_task, \
             patch("daily_pipeline.logger"):
            daily_pipeline.run_all(db, loader, engine)

        task_names = [call.args[0] for call in safe_task.call_args_list]

        assert task_names.index("update_chip_distribution") > task_names.index("update_sector_derivatives")
        assert task_names.index("update_chip_distribution_em") > task_names.index("update_chip_distribution")
        assert task_names.index("retry") > task_names.index("update_chip_distribution_em")
        assert task_names.index("health_check") > task_names.index("retry")


# ===========================================================================
# main() / CLI
# ===========================================================================
class TestMain:
    @pytest.fixture(autouse=True)
    def _passthrough_safe_task(self):
        # 单任务模式现经 _safe_task 包装（注入 _task_run_id / 写审计父行）；
        # CLI 分发测试只关心路由与参数，这里透传以保持断言语义
        with patch(
            "daily_pipeline._safe_task",
            side_effect=lambda name, fn, *a, **kw: fn(*a, **kw),
        ):
            yield

    @pytest.mark.parametrize("status", ["success", "no_data"])
    def test_direct_task_success_status_does_not_exit(self, weekday_mock, status):
        mock_fn = MagicMock(
            return_value={"status": status, "saved": 0, "reason": "current"}
        )
        with patch.object(
            sys, "argv", ["daily_pipeline.py", "--task", "update_bars"]
        ), patch("daily_pipeline.ProviderFactory") as factory, patch.dict(
            "daily_pipeline._TASK_CALLABLES", {"update_bars": mock_fn}
        ):
            factory.get_db.return_value = db = MagicMock()
            factory.get_loader.return_value = loader = MagicMock()
            factory.get_indicator_engine.return_value = MagicMock()
            daily_pipeline.main()
        mock_fn.assert_called_once_with(
            db, loader, limit=None, resume=False, symbols=None, force=False
        )

    def test_direct_task_degraded_status_exits_one(self, weekday_mock):
        mock_fn = MagicMock(
            return_value={
                "status": "degraded",
                "saved": 0,
                "error": "1 failures",
            }
        )
        with patch.object(
            sys, "argv", ["daily_pipeline.py", "--task", "update_bars"]
        ), patch("daily_pipeline.ProviderFactory") as factory, patch.dict(
            "daily_pipeline._TASK_CALLABLES", {"update_bars": mock_fn}
        ):
            factory.get_db.return_value = db = MagicMock()
            factory.get_loader.return_value = loader = MagicMock()
            factory.get_indicator_engine.return_value = MagicMock()
            with pytest.raises(SystemExit) as exc_info:
                daily_pipeline.main()
        assert exc_info.value.code == 1
        mock_fn.assert_called_once_with(
            db, loader, limit=None, resume=False, symbols=None, force=False
        )

    def test_all(self, weekday_mock):
        with patch.object(sys, "argv", ["daily_pipeline.py"]), \
             patch("daily_pipeline.ProviderFactory") as f, \
             patch("daily_pipeline.run_all", return_value={"bars": {"status": "ok"}}) as fn:
            f.configure.return_value = None
            f.get_db.return_value = MagicMock()
            f.get_loader.return_value = MagicMock()
            f.get_indicator_engine.return_value = MagicMock()
            daily_pipeline.main()
            fn.assert_called_once()

    def test_health_check(self, weekday_mock):
        with patch.object(sys, "argv", ["daily_pipeline.py", "--task", "health_check"]), \
             patch("daily_pipeline.ProviderFactory") as f, \
             patch.dict("daily_pipeline._TASK_CALLABLES", {"health_check": MagicMock()}) as fn_dict:
            f.configure.return_value = None
            f.get_db.return_value = MagicMock()
            f.get_loader.return_value = MagicMock()
            f.get_indicator_engine.return_value = MagicMock()
            daily_pipeline.main()
            fn_dict["health_check"].assert_called_once()

    def test_main_does_not_create_magicmock_file(self, weekday_mock, tmp_path):
        """main() 不应在 ProviderFactory.get_db() 为 MagicMock 时生成垃圾 SQLite 文件。"""
        db_mock = MagicMock()
        db_mock.db_path = str(tmp_path / "test.db")
        # 记录当前 cwd 下已有的 MagicMock 文件，避免被预先存在的文件（如之前的测试残留）干扰
        pre_existing = {p for p in Path.cwd().iterdir() if p.is_file() and "MagicMock" in p.name}
        with patch.object(sys, "argv", ["daily_pipeline.py"]), \
             patch("daily_pipeline.ProviderFactory") as f, \
             patch("daily_pipeline.run_all", return_value={"bars": {"status": "ok"}}):
            f.configure.return_value = None
            f.get_db.return_value = db_mock
            f.get_loader.return_value = MagicMock()
            f.get_indicator_engine.return_value = MagicMock()
            daily_pipeline.main()
            magicmock_files = list(tmp_path.glob("*MagicMock*"))
            new_magicmock = {p for p in Path.cwd().iterdir() if p.is_file() and "MagicMock" in p.name} - pre_existing
            assert not magicmock_files
            assert not new_magicmock

    def test_update_bars_with_limit(self, weekday_mock):
        mock_fn = MagicMock()
        with patch.object(sys, "argv", ["daily_pipeline.py", "--task", "update_bars", "--limit", "5"]), \
             patch("daily_pipeline.ProviderFactory") as f, \
             patch.dict("daily_pipeline._TASK_CALLABLES", {"update_bars": mock_fn}):
            f.configure.return_value = None
            f.get_db.return_value = db = MagicMock()
            f.get_loader.return_value = loader = MagicMock()
            f.get_indicator_engine.return_value = MagicMock()
            daily_pipeline.main()
            mock_fn.assert_called_once_with(db, loader, limit=5, resume=False, symbols=None, force=False)

    def test_with_force_and_resume(self, weekday_mock):
        with patch.object(sys, "argv", ["daily_pipeline.py", "--force", "--resume"]), \
             patch("daily_pipeline.ProviderFactory") as f, \
             patch("daily_pipeline.run_all", return_value={"bars": {"status": "ok"}}) as fn:
            f.configure.return_value = None
            f.get_db.return_value = db = MagicMock()
            f.get_loader.return_value = loader = MagicMock()
            f.get_indicator_engine.return_value = engine = MagicMock()
            daily_pipeline.main()
            fn.assert_called_once_with(db, loader, engine, resume=True, force=True)

    def test_task_retry(self, weekday_mock):
        mock_fn = MagicMock()
        with patch.object(sys, "argv", ["daily_pipeline.py", "--task", "retry"]), \
             patch("daily_pipeline.ProviderFactory") as f, \
             patch.dict("daily_pipeline._TASK_CALLABLES", {"retry": mock_fn}):
            f.configure.return_value = None
            f.get_db.return_value = db = MagicMock()
            f.get_loader.return_value = loader = MagicMock()
            f.get_indicator_engine.return_value = MagicMock()
            daily_pipeline.main()
            mock_fn.assert_called_once_with(db, loader)


# ===========================================================================
# run_close_refresh / --refresh-today
# ===========================================================================
class _RecordingRefreshStore:
    """Recording fake for the refresh run store — 不落盘、不联网。"""

    def __init__(self):
        self.calls: list[tuple[str, dict]] = []

    def start_run(self, **kwargs):
        self.calls.append(("start", kwargs))

    def record_task_result(self, **kwargs):
        self.calls.append(("task", kwargs))

    def finish_run(self, **kwargs):
        self.calls.append(("finish", kwargs))


def _shanghai_now(mock_dt, hour: int, minute: int = 0) -> None:
    mock_dt.now.return_value = datetime(
        2026, 7, 28, hour, minute, tzinfo=ZoneInfo("Asia/Shanghai")
    )


def _empty_orchestrator(store: _RecordingRefreshStore) -> RefreshOrchestrator:
    return RefreshOrchestrator(specs=(), adapters={}, store=store)


class TestRunCloseRefresh:
    def test_context_uses_expected_trading_day_and_shanghai_clock(self, tmp_path):
        captured: list = []

        class FakeOrchestrator:
            def run(self, context):
                captured.append(context)
                return TaskResult.success("refresh_today", saved=0)

        with patch("daily_pipeline.get_expected_latest_trading_day", return_value="2026-07-27") as expected, \
             patch("daily_pipeline.datetime") as mock_dt:
            _shanghai_now(mock_dt, 16, 30)
            result = daily_pipeline.run_close_refresh(
                str(tmp_path / "refresh.db"),
                symbols=["000001.SZ"],
                orchestrator=FakeOrchestrator(),
            )
        expected.assert_called_once_with(now=mock_dt.now.return_value)
        # 目标日计算与 started_at 共用同一次上海时钟读取
        mock_dt.now.assert_called_once_with(daily_pipeline._SHANGHAI_TZ)
        assert result.exit_failure is False
        ctx = captured[0]
        assert ctx.target_date == "2026-07-27"
        assert ctx.symbols == ("000001.SZ",)
        assert ctx.bypass_cache is True
        assert ctx.run_id
        assert ctx.started_at is mock_dt.now.return_value
        assert ctx.started_at.tzinfo is not None

    @pytest.mark.parametrize(
        ("symbols", "expected"),
        [
            (None, None),
            ([], ()),
            (["000001.SZ"], ("000001.SZ",)),
        ],
    )
    def test_symbols_preserves_none_vs_empty_distinction(self, tmp_path, symbols, expected):
        """symbols=[] 必须映射为 ()（no-op），不得坍缩为 None（全市场）。"""
        captured: list = []

        class FakeOrchestrator:
            def run(self, context):
                captured.append(context)
                return TaskResult.success("refresh_today", saved=0)

        with patch("daily_pipeline.get_expected_latest_trading_day", return_value="2026-07-27"), \
             patch("daily_pipeline.datetime") as mock_dt:
            _shanghai_now(mock_dt, 16, 30)
            daily_pipeline.run_close_refresh(
                str(tmp_path / "refresh.db"),
                symbols=symbols,
                orchestrator=FakeOrchestrator(),
            )
        ctx = captured[0]
        if expected is None:
            # None 才是全市场哨兵，() 只是空作用域
            assert ctx.symbols is None
        else:
            assert ctx.symbols == expected

    def test_pre_close_fails_without_force_and_never_starts_run(self, tmp_path):
        store = _RecordingRefreshStore()
        with patch("daily_pipeline.get_expected_latest_trading_day", return_value="2026-07-27"), \
             patch("daily_pipeline.datetime") as mock_dt:
            _shanghai_now(mock_dt, 10, 0)
            result = daily_pipeline.run_close_refresh(
                str(tmp_path / "refresh.db"), orchestrator=_empty_orchestrator(store)
            )
        assert result.exit_failure is True
        assert "16:00" in (result.error or "")
        assert store.calls == []

    def test_pre_close_force_allows_run(self, tmp_path):
        store = _RecordingRefreshStore()
        with patch("daily_pipeline.get_expected_latest_trading_day", return_value="2026-07-27"), \
             patch("daily_pipeline.datetime") as mock_dt:
            _shanghai_now(mock_dt, 10, 0)
            result = daily_pipeline.run_close_refresh(
                str(tmp_path / "refresh.db"),
                force=True,
                orchestrator=_empty_orchestrator(store),
            )
        assert result.exit_failure is False
        assert [name for name, _ in store.calls] == ["start", "finish"]

    def test_post_close_dispatches_orchestrator(self, tmp_path):
        store = _RecordingRefreshStore()
        with patch("daily_pipeline.get_expected_latest_trading_day", return_value="2026-07-27"), \
             patch("daily_pipeline.datetime") as mock_dt:
            _shanghai_now(mock_dt, 16, 30)
            result = daily_pipeline.run_close_refresh(
                str(tmp_path / "refresh.db"), orchestrator=_empty_orchestrator(store)
            )
        assert result.exit_failure is False
        assert store.calls[0][0] == "start"
        assert store.calls[0][1]["target_date"] == "2026-07-27"
        assert store.calls[-1][0] == "finish"

    def test_default_wiring_requests_uncached_loader(self, tmp_path):
        with patch("daily_pipeline.ProviderFactory") as factory:
            factory.get_loader.return_value = MagicMock()
            orchestrator = daily_pipeline._build_refresh_orchestrator(str(tmp_path / "audit.db"))
        factory.get_loader.assert_called_once_with(use_cache=False)
        assert isinstance(orchestrator, RefreshOrchestrator)

    def test_cli_orchestrator_wires_every_refresh_adapter(self, tmp_path):
        """CLI 构建的编排器必须装配全部 29 个适配器，_topological_specs 不再缺适配器。"""
        from core.task_registry import refreshable_trading_tasks

        class FakeDb:
            """只提供 db_path 的手写 db fake（构造期不联网、不落盘）。"""

            def __init__(self, db_path: str):
                self.db_path = db_path

        db_path = str(tmp_path / "refresh.db")
        with patch("daily_pipeline.ProviderFactory") as factory:
            factory.get_db.return_value = FakeDb(db_path)
            factory.get_loader.return_value = MagicMock()
            factory.get_indicator_engine.return_value = MagicMock()
            orchestrator = daily_pipeline._build_refresh_orchestrator(db_path)

        expected = {spec.name for spec in refreshable_trading_tasks()}
        assert set(orchestrator._adapters) == expected
        # 修复前 _topological_specs 会对全部 29 个 spec 抛 "missing adapter"
        ordered = orchestrator._topological_specs()
        assert {spec.name for spec in ordered} == expected


# ===========================================================================
# 跨源抽样校验开关（REFRESH_CROSS_SOURCE，默认关闭）
# ===========================================================================
_CROSS_SOURCE_ENV_KEYS = (
    "REFRESH_CROSS_SOURCE",
    "REFRESH_CROSS_SOURCE_TASK",
    "REFRESH_CROSS_SOURCE_SAMPLE_SIZE",
    "REFRESH_CROSS_SOURCE_PRICE_TOL",
    "REFRESH_CROSS_SOURCE_VOLUME_TOL",
    "REFRESH_CROSS_SOURCE_REPORT_ONLY",
)


class _FakeCrossSourceVerifier:
    """手写 CrossSourceVerifier 协议 fake — 只读、不联网。"""

    source_name = "fake_backup"
    supported_boards = frozenset({"sh_main"})

    def primary_quotes(self, symbols, target_date):
        return {}

    def reference_quotes(self, symbols, target_date):
        return {}


class TestCrossSourceOptIn:
    """_build_refresh_orchestrator 的装配分支：关闭 / 开启+注入 verifier / 开启默认 verifier。"""

    @pytest.fixture(autouse=True)
    def _clean_cross_source_env(self, monkeypatch):
        for key in _CROSS_SOURCE_ENV_KEYS:
            monkeypatch.delenv(key, raising=False)

    def _build(self, tmp_path, **kwargs):
        with patch("daily_pipeline.ProviderFactory") as factory:
            factory.get_loader.return_value = MagicMock()
            return daily_pipeline._build_refresh_orchestrator(
                str(tmp_path / "audit.db"), **kwargs
            )

    @pytest.mark.parametrize("flag", [None, "0", "false"])
    def test_disabled_builds_without_cross_source(self, tmp_path, monkeypatch, flag):
        """默认（或显式关闭）不装配跨源校验 — 与现状行为完全一致。"""
        if flag is not None:
            monkeypatch.setenv("REFRESH_CROSS_SOURCE", flag)
        orchestrator = self._build(tmp_path)
        assert orchestrator._cross_source is None
        assert orchestrator._verifier is None

    def test_enabled_with_injected_verifier_uses_env_config(self, tmp_path, monkeypatch):
        monkeypatch.setenv("REFRESH_CROSS_SOURCE", "1")
        monkeypatch.setenv("REFRESH_CROSS_SOURCE_TASK", "update_indicators")
        monkeypatch.setenv("REFRESH_CROSS_SOURCE_SAMPLE_SIZE", "7")
        monkeypatch.setenv("REFRESH_CROSS_SOURCE_PRICE_TOL", "0.01")
        monkeypatch.setenv("REFRESH_CROSS_SOURCE_VOLUME_TOL", "0.1")
        fake = _FakeCrossSourceVerifier()
        orchestrator = self._build(tmp_path, cross_source_verifier=fake)
        assert orchestrator._verifier is fake
        cfg = orchestrator._cross_source
        assert isinstance(cfg, CrossSourceCheckConfig)
        assert cfg.task_name == "update_indicators"
        assert cfg.sample_size == 7
        assert cfg.tolerance == CrossSourceTolerance(price=0.01, volume=0.1)
        # 未设 REFRESH_CROSS_SOURCE_REPORT_ONLY 时默认观察模式（安全灰度）
        assert cfg.report_only is True

    def test_enabled_with_verifier_falls_back_to_defaults(self, tmp_path, monkeypatch):
        """只开开关时使用默认 task/sample/容差（容差为临时值，待真实数据调参）。"""
        monkeypatch.setenv("REFRESH_CROSS_SOURCE", "true")
        fake = _FakeCrossSourceVerifier()
        orchestrator = self._build(tmp_path, cross_source_verifier=fake)
        cfg = orchestrator._cross_source
        assert cfg.task_name == "update_bars"
        assert cfg.sample_size == 30
        assert cfg.tolerance == CrossSourceTolerance(price=0.005, volume=0.05)
        assert cfg.report_only is True

    def test_enabled_without_injected_verifier_builds_default_xueqiu(
        self, tmp_path, monkeypatch
    ):
        """开关打开且未注入 verifier：装配默认雪球 verifier，取代步骤 1 的 RuntimeError。"""
        monkeypatch.setenv("REFRESH_CROSS_SOURCE", "1")
        db_path = str(tmp_path / "audit.db")
        with patch("daily_pipeline.ProviderFactory") as factory:
            factory.get_loader.return_value = MagicMock()
            orchestrator = daily_pipeline._build_refresh_orchestrator(db_path)
        verifier = orchestrator._verifier
        assert isinstance(verifier, XueqiuCrossSourceVerifier)
        assert verifier._db_path == db_path
        cfg = orchestrator._cross_source
        assert isinstance(cfg, CrossSourceCheckConfig)
        # 首次启用默认先观察不降级：report_only 默认 True
        assert cfg.report_only is True

    def test_report_only_env_zero_switches_to_enforce(self, tmp_path, monkeypatch):
        """显式 REFRESH_CROSS_SOURCE_REPORT_ONLY=0 才进入 enforce（重试+降级）模式。"""
        monkeypatch.setenv("REFRESH_CROSS_SOURCE", "1")
        monkeypatch.setenv("REFRESH_CROSS_SOURCE_REPORT_ONLY", "0")
        orchestrator = self._build(tmp_path)
        assert orchestrator._cross_source.report_only is False

    def test_injected_verifier_takes_precedence_over_default(
        self, tmp_path, monkeypatch
    ):
        """测试接缝保留：注入的 verifier 优先于默认雪球 verifier。"""
        monkeypatch.setenv("REFRESH_CROSS_SOURCE", "1")
        fake = _FakeCrossSourceVerifier()
        orchestrator = self._build(tmp_path, cross_source_verifier=fake)
        assert orchestrator._verifier is fake
        assert not isinstance(orchestrator._verifier, XueqiuCrossSourceVerifier)

    def test_falsy_injected_verifier_is_not_replaced(self, tmp_path, monkeypatch):
        """注入判断必须是 is None 身份检查：假值但有效的 verifier 不得被
        默认雪球 verifier 静默替换。"""
        monkeypatch.setenv("REFRESH_CROSS_SOURCE", "1")

        class _FalsyVerifier(_FakeCrossSourceVerifier):
            def __bool__(self) -> bool:
                return False

        fake = _FalsyVerifier()
        orchestrator = self._build(tmp_path, cross_source_verifier=fake)
        assert orchestrator._verifier is fake

    def test_run_close_refresh_threads_verifier_to_builder(self, tmp_path):
        fake = _FakeCrossSourceVerifier()
        built = MagicMock()
        built.run.return_value = TaskResult.success("refresh_today", saved=0)
        with patch("daily_pipeline._build_refresh_orchestrator", return_value=built) as build, \
             patch("daily_pipeline.get_expected_latest_trading_day", return_value="2026-07-27"), \
             patch("daily_pipeline.datetime") as mock_dt:
            _shanghai_now(mock_dt, 16, 30)
            daily_pipeline.run_close_refresh(
                str(tmp_path / "refresh.db"),
                cross_source_verifier=fake,
            )
        build.assert_called_once_with(
            str(tmp_path / "refresh.db"), cross_source_verifier=fake
        )


class TestRefreshTodayCLI:
    def test_no_flags_still_runs_legacy_all(self, weekday_mock):
        with patch.object(sys, "argv", ["daily_pipeline.py"]), \
             patch("daily_pipeline.ProviderFactory") as f, \
             patch("daily_pipeline.run_close_refresh") as refresh, \
             patch("daily_pipeline.run_all", return_value={"bars": {"status": "ok"}}) as run_all_mock:
            f.get_db.return_value = MagicMock()
            f.get_loader.return_value = MagicMock()
            f.get_indicator_engine.return_value = MagicMock()
            daily_pipeline.main()
        run_all_mock.assert_called_once()
        refresh.assert_not_called()

    def test_task_conflicts_with_refresh_today(self, capsys):
        with patch.object(sys, "argv", ["daily_pipeline.py", "--refresh-today", "--task", "update_bars"]), \
             pytest.raises(SystemExit) as exc_info:
            daily_pipeline.main()
        assert exc_info.value.code == 2
        assert "--task 不能与 --refresh-today 同时使用" in capsys.readouterr().err

    def test_resume_conflicts_with_refresh_today(self, capsys):
        with patch.object(sys, "argv", ["daily_pipeline.py", "--refresh-today", "--resume"]), \
             pytest.raises(SystemExit) as exc_info:
            daily_pipeline.main()
        assert exc_info.value.code == 2
        assert "--resume 不能与 --refresh-today 同时使用" in capsys.readouterr().err

    def test_refresh_today_dispatches_and_never_calls_run_all(self):
        ok = TaskResult.success("refresh_today", saved=0)
        with patch.object(sys, "argv", ["daily_pipeline.py", "--refresh-today"]), \
             patch("daily_pipeline.ProviderFactory"), \
             patch("daily_pipeline.run_close_refresh", return_value=ok) as refresh, \
             patch("daily_pipeline.run_all") as run_all_mock:
            daily_pipeline.main()
        refresh.assert_called_once_with(os.environ["QUANT_DB_PATH"], symbols=None, force=False)
        run_all_mock.assert_not_called()

    def test_refresh_today_forwards_force(self):
        ok = TaskResult.success("refresh_today", saved=0)
        with patch.object(sys, "argv", ["daily_pipeline.py", "--refresh-today", "--force"]), \
             patch("daily_pipeline.ProviderFactory"), \
             patch("daily_pipeline.run_close_refresh", return_value=ok) as refresh:
            daily_pipeline.main()
        refresh.assert_called_once_with(os.environ["QUANT_DB_PATH"], symbols=None, force=True)

    def test_refresh_today_acquires_global_pipeline_lock(self):
        ok = TaskResult.success("refresh_today", saved=0)
        with patch.object(sys, "argv", ["daily_pipeline.py", "--refresh-today"]), \
             patch("daily_pipeline._acquire_lock") as lock, \
             patch("daily_pipeline.ProviderFactory"), \
             patch("daily_pipeline.run_close_refresh", return_value=ok):
            daily_pipeline.main()
        lock.assert_called_once_with()

    @pytest.mark.parametrize(
        "result",
        [
            TaskResult.degraded("refresh_today", ErrorKind.DATA_QUALITY, "partial"),
            TaskResult.failed("refresh_today", ErrorKind.NETWORK, "down"),
            TaskResult.aborted("refresh_today", error="cancelled"),
        ],
    )
    def test_refresh_today_failure_exits_nonzero(self, result):
        with patch.object(sys, "argv", ["daily_pipeline.py", "--refresh-today"]), \
             patch("daily_pipeline.ProviderFactory"), \
             patch("daily_pipeline.run_close_refresh", return_value=result), \
             pytest.raises(SystemExit) as exc_info:
            daily_pipeline.main()
        assert exc_info.value.code == 1

    def test_refresh_today_interrupt_exits_nonzero(self):
        """收盘刷新路径被中断必须以非零退出码结束，不得静默返回 0。"""
        with patch.object(sys, "argv", ["daily_pipeline.py", "--refresh-today"]), \
             patch("daily_pipeline.ProviderFactory"), \
             patch("daily_pipeline.run_close_refresh", side_effect=KeyboardInterrupt), \
             pytest.raises(SystemExit) as exc_info:
            daily_pipeline.main()
        assert exc_info.value.code == 1

    def test_empty_symbols_string_errors_and_never_refreshes(self, capsys):
        """显式 --symbols 解析为空必须报错退出，禁止静默升级为全市场刷新。"""
        with patch.object(sys, "argv", ["daily_pipeline.py", "--refresh-today", "--symbols", ","]), \
             patch("daily_pipeline.ProviderFactory"), \
             patch("daily_pipeline.run_close_refresh") as refresh, \
             pytest.raises(SystemExit) as exc_info:
            daily_pipeline.main()
        assert exc_info.value.code == 2
        assert "--symbols" in capsys.readouterr().err
        refresh.assert_not_called()

    def test_blank_symbols_file_errors_and_never_refreshes(self, tmp_path, capsys):
        """--symbols 指向只含空行的文件同样必须报错，不得回退全市场。"""
        symbols_file = tmp_path / "symbols.txt"
        symbols_file.write_text("\n   \n\n", encoding="utf-8")
        with patch.object(sys, "argv", ["daily_pipeline.py", "--refresh-today", "--symbols", str(symbols_file)]), \
             patch("daily_pipeline.ProviderFactory"), \
             patch("daily_pipeline.run_close_refresh") as refresh, \
             pytest.raises(SystemExit) as exc_info:
            daily_pipeline.main()
        assert exc_info.value.code == 2
        assert "--symbols" in capsys.readouterr().err
        refresh.assert_not_called()

    def test_empty_string_symbols_errors_and_never_refreshes(self, capsys):
        """--symbols ""（空字符串，如 shell 变量未赋值）同样必须报错，不得升级为全市场。"""
        with patch.object(sys, "argv", ["daily_pipeline.py", "--refresh-today", "--symbols", ""]), \
             patch("daily_pipeline.ProviderFactory"), \
             patch("daily_pipeline.run_close_refresh") as refresh, \
             pytest.raises(SystemExit) as exc_info:
            daily_pipeline.main()
        assert exc_info.value.code == 2
        assert "--symbols" in capsys.readouterr().err
        refresh.assert_not_called()

    def test_valid_symbols_still_scope_refresh(self):
        """回归：正常 --symbols 列表仍按作用域传入 run_close_refresh。"""
        ok = TaskResult.success("refresh_today", saved=0)
        with patch.object(sys, "argv", ["daily_pipeline.py", "--refresh-today", "--symbols", "000001.SZ,600000.SH"]), \
             patch("daily_pipeline.ProviderFactory"), \
             patch("daily_pipeline.run_close_refresh", return_value=ok) as refresh:
            daily_pipeline.main()
        refresh.assert_called_once_with(
            os.environ["QUANT_DB_PATH"],
            symbols=["000001.SZ", "600000.SH"],
            force=False,
        )


# ===========================================================================
# Direct akshare tasks (patch daily_pipeline.ak)
# ===========================================================================
class TestDirectAkShareTasks:
    def test_margin_trading_sse_only(self):
        db = MagicMock()
        db.save_margin_trading_batch.return_value = 1
        df = pd.DataFrame({"标的证券代码": ["000001.SZ"], "融资余额": [1e9], "融资买入额": [1e8], "融资偿还额": [5e7], "融券余量": [1e5], "融券卖出量": [1e4], "融资融券余额": [1.1e9]})
        with patch("tasks.market_flow.ak") as mock_ak:
            mock_ak.stock_margin_detail_sse.return_value = df
            mock_ak.stock_margin_detail_szse.return_value = pd.DataFrame()
            with patch("daily_pipeline.logger"):
                r = daily_pipeline.update_margin_trading(db)
        assert r["saved"] == 1

    def test_margin_trading_ak_none(self):
        db = MagicMock()
        with patch("tasks.market_flow.ak", None), patch("daily_pipeline.logger"):
            r = daily_pipeline.update_margin_trading(db)
        assert r["saved"] == 0

    def test_dragon_tiger_normal(self):
        db = MagicMock()
        db.save_dragon_tiger_batch.return_value = 1
        df = pd.DataFrame({"代码": ["000001.SZ"], "收盘价": [10.5], "涨跌幅": [0.02], "龙虎榜净买额": [1e8], "龙虎榜买入额": [2e8], "龙虎榜卖出额": [1e8], "换手率": [0.05], "流通市值": [1e9], "上榜原因": ["连续三日"]})
        with patch("tasks.market_flow.ak") as mock_ak:
            mock_ak.stock_lhb_detail_em.return_value = df
            with patch("daily_pipeline.logger"):
                r = daily_pipeline.update_dragon_tiger(db)
        assert r["saved"] == 1

    def test_dragon_tiger_empty(self):
        db = MagicMock()
        with patch("tasks.market_flow.ak") as mock_ak:
            mock_ak.stock_lhb_detail_em.return_value = pd.DataFrame()
            with patch("daily_pipeline.logger"):
                r = daily_pipeline.update_dragon_tiger(db)
        assert r["saved"] == 0

    def test_block_trade_normal(self):
        db = MagicMock()
        db.save_block_trade_batch.return_value = 1
        df = pd.DataFrame({"证券代码": ["000001.SZ"], "成交价": [10.0], "收盘价": [10.5], "折溢率": [-0.05], "成交量": [1e6], "成交额": [1e7], "买方营业部": ["A"], "卖方营业部": ["B"]})
        with patch("tasks.market_flow.ak") as mock_ak:
            mock_ak.stock_dzjy_mrmx.return_value = df
            with patch("daily_pipeline.logger"):
                r = daily_pipeline.update_block_trade(db)
        assert r["saved"] == 1

    def test_block_trade_empty(self):
        db = MagicMock()
        with patch("tasks.market_flow.ak") as mock_ak:
            mock_ak.stock_dzjy_mrmx.return_value = pd.DataFrame()
            with patch("daily_pipeline.logger"):
                r = daily_pipeline.update_block_trade(db)
        assert r["saved"] == 0

    def test_sector_fund_flow_normal(self):
        db = MagicMock()
        db.save_sector_fund_flow_batch.return_value = 1
        df = pd.DataFrame({"行业": ["银行"], "主力净流入-净额": [1e9], "主力净流入-净占比": [0.02], "超大单净流入-净额": [5e8], "流入资金": [5e8], "流出资金": [-3e8]})
        with patch("tasks.market_flow.ak") as mock_ak:
            mock_ak.stock_fund_flow_industry.return_value = df
            with patch("daily_pipeline.logger"):
                r = daily_pipeline.update_sector_fund_flow(db)
        assert r["saved"] == 1

    def test_shareholder_count_normal(self):
        db = MagicMock()
        db.save_shareholder_count_batch.return_value = 1
        df = pd.DataFrame({"证券代码": ["000001.SZ"], "本期股东人数": [50000], "股东人数增幅": [-0.05], "本期人均持股数量": [20000]})
        with patch("tasks.financials.ak") as mock_ak:
            mock_ak.stock_hold_num_cninfo.return_value = df
            with patch("daily_pipeline.logger"):
                r = daily_pipeline.update_shareholder_count(db)
        assert r["saved"] == 1

    def test_shareholder_count_empty_code(self):
        db = MagicMock()
        df = pd.DataFrame({"证券代码": [""], "本期股东人数": [None], "股东人数增幅": [None], "本期人均持股数量": [None]})
        with patch("tasks.financials.ak") as mock_ak:
            mock_ak.stock_hold_num_cninfo.return_value = df
            with patch("daily_pipeline.logger"):
                r = daily_pipeline.update_shareholder_count(db)
        assert r["saved"] == 0

    def test_shareholder_count_period_selection(self):
        db = MagicMock()
        df = pd.DataFrame({"证券代码": ["000001.SZ"], "本期股东人数": [50000], "股东人数增幅": [-0.05], "本期人均持股数量": [20000]})
        with patch("tasks.financials.ak") as mock_ak:
            mock_ak.stock_hold_num_cninfo.return_value = df
            cases = [
                (datetime(2024, 1, 15), "20230930"),
                (datetime(2024, 6, 1), "20240331"),
                (datetime(2024, 9, 1), "20240630"),
                (datetime(2024, 11, 15), "20240930"),
            ]
            for now_dt, expected_period in cases:
                with patch("tasks.financials.datetime") as m:
                    m.now.return_value = now_dt
                    m.side_effect = lambda *a, **kw: datetime(*a, **kw)
                    with patch("daily_pipeline.logger"):
                        daily_pipeline.update_shareholder_count(db)
                mock_ak.stock_hold_num_cninfo.assert_called_with(date=expected_period)


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
    import importlib

    with patch.dict(os.environ, {"ULTRA_SAFE": "1"}, clear=False):
        import core.config as cfg
        import tasks.bars
        importlib.reload(cfg)
        importlib.reload(tasks.bars)
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
    db.save_fundamentals_batch.side_effect = ValueError("batch save failed")
    db.count_fundamentals_for_date.return_value = 0
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
    db.save_fund_flow_batch.side_effect = ValueError("batch save failed")
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
    with patch("core.utils.datetime") as m:
        m.now.return_value = datetime(2026, 6, 22)
        assert daily_pipeline._is_trading_day()


def test_is_trading_day_weekend():
    with patch("core.utils.datetime") as m:
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

@patch("tasks.market_flow.ak")
def test_fetch_sector_fund_flow(mock_ak: MagicMock):
    mock_df = pd.DataFrame({
        "行业": ["银行", "医药"],
        "主力净流入-净额": [1e8, 5e7],
        "主力净流入-净占比": [0.5, 0.3],
        "超大单净流入-净额": [5e7, 2e7],
        "流入资金": [3e7, 1e7],
        "流出资金": [-2e7, -1e7],
    })
    mock_ak.stock_fund_flow_industry.return_value = mock_df
    result = daily_pipeline._fetch_sector_fund_flow("2026-07-01")
    assert result is not None
    assert len(result) == 2
    assert result.iloc[0]["sector_name"] == "银行"


@patch("tasks.market_flow.ak")
def test_fetch_sector_fund_flow_empty(mock_ak: MagicMock):
    mock_ak.stock_fund_flow_industry.return_value = pd.DataFrame()
    result = daily_pipeline._fetch_sector_fund_flow("2026-07-01")
    assert result is None


@patch("tasks.market_flow.ak")
def test_update_sector_fund_flow_success(mock_ak: MagicMock):
    db = MagicMock()
    db.save_sector_fund_flow_batch.return_value = 1
    mock_df = pd.DataFrame({
        "行业": ["银行"],
        "主力净流入-净额": [1e8],
        "主力净流入-净占比": [0.5],
        "超大单净流入-净额": [5e8],
        "流入资金": [3e7],
        "流出资金": [-2e7],
    })
    mock_ak.stock_fund_flow_industry.return_value = mock_df
    with patch("daily_pipeline.logger"):
        r = daily_pipeline.update_sector_fund_flow(db)
    assert r["saved"] == 1
    assert r["source"] == "ths"


@patch("tasks.market_flow.ak")
def test_update_sector_fund_flow_empty_df(mock_ak: MagicMock):
    """When the single source returns empty, should return zero saved."""
    db = MagicMock()
    mock_ak.stock_fund_flow_industry.return_value = pd.DataFrame()
    with patch("daily_pipeline.logger"):
        r = daily_pipeline.update_sector_fund_flow(db)
    assert r["saved"] == 0
    assert r["total"] == 0


@patch("tasks.market_flow.ak")
def test_update_sector_fund_flow_akshare_error(mock_ak: MagicMock):
    """When the single source raises, should return error dict."""
    db = MagicMock()
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


def test_update_historical_valuation_deduplicates_symbol_date():
    db = MagicMock()
    db.get_fundamentals_batch.return_value = pd.DataFrame({
        "ts_code": ["000001", "000001", "000002"],
        "trade_date": ["2026-07-17", "2026-07-17", "2026-07-17"],
        "pe_ttm": [10.0, 10.1, 12.0],
        "pb": [1.0, 1.1, 1.5],
        "ps_ttm": [2.0, 2.1, 2.5],
        "dividend_yield": [0.03, 0.031, 0.02],
    })

    with patch("daily_pipeline.logger"):
        r = daily_pipeline.update_historical_valuation(db)

    assert r["saved"] == 2
    assert r["total"] == 2
    assert db.save_historical_valuation.call_count == 2


def test_update_historical_valuation_uses_batch_save_when_available():
    class _BatchDB:
        def __init__(self) -> None:
            self.save_historical_valuation = MagicMock()
            self.save_historical_valuation_batch = MagicMock(return_value=2)

        def get_fundamentals_batch(self) -> pd.DataFrame:
            return pd.DataFrame({
                "ts_code": ["000001", "000002"],
                "trade_date": ["2026-07-17", "2026-07-17"],
                "pe_ttm": [10.0, 12.0],
                "pb": [1.0, 1.5],
                "ps_ttm": [2.0, 2.5],
                "dividend_yield": [0.03, 0.02],
            })

    db = _BatchDB()

    with patch("daily_pipeline.logger"):
        r = daily_pipeline.update_historical_valuation(db)

    assert r["saved"] == 2
    db.save_historical_valuation_batch.assert_called_once()
    db.save_historical_valuation.assert_not_called()


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

    with patch("core.utils.is_beijing_stock", side_effect=lambda s: s == "880001"), \
         patch("daily_pipeline.logger"), \
         patch.dict(os.environ, {"INCLUDE_BJ": "0"}):
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


# ===========================================================================
# Bug regression: --symbols mode / exit code
# ===========================================================================

def test_update_bars_symbols_mode_no_crash():
    """update_bars with --symbols must not crash (regression for UnboundLocalError)."""
    db = MagicMock()
    loader = MagicMock()
    loader.incremental_update.return_value = pd.DataFrame()
    db.get_daily_bars.return_value = pd.DataFrame()

    with patch("core.utils.is_beijing_stock", return_value=False), \
         patch("daily_pipeline.logger"), \
         patch("tasks.bars.logger"):
        from daily_pipeline import ProgressTracker
        ProgressTracker.clear()
        r = daily_pipeline.update_bars(db, loader, symbols=["000001", "600000"])

    assert r["total"] == 2
    ProgressTracker.clear()


def test_update_bars_with_mock_db_path_does_not_create_magicmock_file():
    db = MagicMock()
    loader = MagicMock()
    db.get_stock_list.return_value = pd.DataFrame({"code": ["000001"]})
    db.get_daily_bars.return_value = _bars_df(["2024-01-02"])
    db.watchlist_get_all.return_value = pd.DataFrame()
    loader.incremental_update.return_value = _bars_df(["2024-01-02"])
    pre_existing = {p for p in Path.cwd().iterdir() if p.is_file() and "MagicMock" in p.name}

    with patch("daily_pipeline.time.sleep"), patch("daily_pipeline.logger"):
        r = daily_pipeline.update_bars(db, loader)

    new_magicmock = {p for p in Path.cwd().iterdir() if p.is_file() and "MagicMock" in p.name} - pre_existing
    assert r["total"] == 1
    assert not new_magicmock


def test_run_all_detects_crashed_task():
    """run_all() must detect crashed tasks in results."""
    from daily_pipeline import run_all

    db = MagicMock()
    loader = MagicMock()
    engine = MagicMock()

    # crashed=True on failed status
    with patch("daily_pipeline._safe_task",
               return_value={"status": "failed", "error": "simulated crash"}), \
         patch("daily_pipeline._should_update", return_value=True), \
         patch("daily_pipeline.logger"), \
         patch("time.sleep"):
        results = run_all(db, loader, engine)

    assert results.get("crashed") is True, "run_all should set crashed=True for failed"

    # crashed=True on status field via degraded/failed/aborted
    with patch("daily_pipeline._safe_task",
               return_value={"status": "failed", "error": "API unavailable"}), \
         patch("daily_pipeline._should_update", return_value=True), \
         patch("daily_pipeline.logger"), \
         patch("time.sleep"):
        results2 = run_all(db, loader, engine)
    assert results2.get("crashed") is True, \
        "run_all should set crashed=True when task returns error field"

    # crashed=True on degraded status
    with patch("daily_pipeline._safe_task",
               return_value={"status": "degraded", "error": "completed with errors"}), \
         patch("daily_pipeline._should_update", return_value=True), \
         patch("daily_pipeline.logger"), \
         patch("time.sleep"):
        results3 = run_all(db, loader, engine)
    assert results3.get("crashed") is True, \
        "run_all should set crashed=True when task completed with errors"


def test_safe_task_marks_error_result_completed_with_errors():
    from core.runner import safe_task

    with patch("core.runner.logger"):
        result = safe_task("broken_task", lambda **kw: {"saved": 0, "total": 0, "error": "Broken pipe"})

    assert result["status"] == "failed"
    assert result["error"] == "Broken pipe"


def test_safe_task_marks_failed_result_completed_with_errors():
    from core.runner import safe_task

    with patch("core.runner.logger"):
        result = safe_task("partial_task", lambda **kw: {"success": 0, "failed": 2, "total": 10})

    assert result["status"] == "degraded"


def test_main_exits_one_when_run_all_returns_crashed():
    """main() must exit with code 1 when run_all returns crashed=True."""
    with patch.object(sys, "argv",
                      ["daily_pipeline.py", "--task", "all", "--force"]), \
         patch("daily_pipeline._should_update", return_value=True), \
         patch("daily_pipeline.run_all",
               return_value={"crashed": True, "bars": {"status": "crashed"}}), \
         patch("daily_pipeline._acquire_lock"), \
         patch("daily_pipeline._release_lock"), \
         patch("daily_pipeline.ProviderFactory"), \
         patch("daily_pipeline.logger"), \
         pytest.raises(SystemExit) as exc_info:
        daily_pipeline.main()
    assert exc_info.value.code == 1


# ===========================================================================
# 国际数据维度 (v3.1): gold_price / crude_oil / fx_rate / global_index / us_treasury
# ===========================================================================

@patch("tasks.macro.ak")
def test_update_gold_price_success(mock_ak: MagicMock):
    db = MagicMock()
    db.save_gold_price_batch.return_value = 2
    mock_ak.spot_golden_benchmark_sge.return_value = pd.DataFrame({
        "交易时间": ["2026-07-11 早盘", "2026-07-10 晚盘"],
        "晚盘价": [897.58, 895.0],
        "早盘价": [891.66, 890.0],
    })
    with patch("daily_pipeline.logger"):
        r = daily_pipeline.update_gold_price(db)
    assert r["saved"] == 2
    db.save_gold_price_batch.assert_called_once()
    rec = db.save_gold_price_batch.call_args[0][0][0]
    assert rec["evening_price"] == 897.58
    assert rec["trading_time"] == "2026-07-11 早盘"


@patch("tasks.macro.ak")
def test_update_gold_price_empty(mock_ak: MagicMock):
    db = MagicMock()
    mock_ak.spot_golden_benchmark_sge.return_value = pd.DataFrame()
    with patch("daily_pipeline.logger"):
        r = daily_pipeline.update_gold_price(db)
    assert r["saved"] == 0
    db.save_gold_price_batch.assert_not_called()


@patch("tasks.macro.ak")
def test_update_crude_oil_success(mock_ak: MagicMock):
    db = MagicMock()
    db.save_crude_oil_batch.return_value = 2

    def _oil(symbol: str) -> pd.DataFrame:
        is_cl = symbol == "CL"
        return pd.DataFrame({
            "名称": ["WTI原油" if is_cl else "Brent原油"],
            "最新价": [73.5 if is_cl else 75.2],
            "人民币报价": [3600.0 if is_cl else 3720.0],
            "涨跌额": [-1.0], "涨跌幅": [-1.3], "开盘价": [74.0],
            "最高价": [75.0], "最低价": [73.0], "昨日结算价": [74.5],
        })

    mock_ak.futures_foreign_commodity_realtime.side_effect = _oil
    with patch("daily_pipeline.logger"):
        r = daily_pipeline.update_crude_oil(db)
    assert r["saved"] == 2
    db.save_crude_oil_batch.assert_called_once()
    recs = db.save_crude_oil_batch.call_args[0][0]
    assert {x["contract"] for x in recs} == {"CL", "OIL"}
    # 验证真实映射：昨结价 -> pre_settle，涨跌额/涨跌幅来自 API 实际列
    cl = next(x for x in recs if x["contract"] == "CL")
    assert cl["pre_settle"] == 74.5
    assert cl["change"] == -1.0
    assert cl["change_pct"] == -1.3
    assert cl["latest_price"] == 73.5


@patch("tasks.macro.ak")
def test_update_crude_oil_empty(mock_ak: MagicMock):
    db = MagicMock()
    mock_ak.futures_foreign_commodity_realtime.return_value = pd.DataFrame()
    with patch("daily_pipeline.logger"):
        r = daily_pipeline.update_crude_oil(db)
    assert r["saved"] == 0
    db.save_crude_oil_batch.assert_not_called()


@patch("tasks.macro.ak")
def test_update_usd_success(mock_ak: MagicMock):
    db = MagicMock()
    db.save_usd_batch.return_value = 1
    mock_ak.currency_boc_sina.return_value = pd.DataFrame({
        "日期": ["2026-07-10"],
        "中行汇买价": [676.44], "中行钞买价": [676.44],
        "中行钞卖价/汇卖价": [679.29], "央行中间价": [679.89], "中行折算价": [679.89],
    })
    with patch("daily_pipeline.logger"):
        r = daily_pipeline.update_usd(db)
    assert r["saved"] == 1
    rec = db.save_usd_batch.call_args[0][0][0]
    assert rec["currency"] == "美元"
    assert rec["cash_sell_price"] == 679.29  # 真实列名: 中行钞卖价/汇卖价


@patch("tasks.macro.ak")
def test_update_usd_empty(mock_ak: MagicMock):
    db = MagicMock()
    mock_ak.currency_boc_sina.return_value = pd.DataFrame()
    with patch("daily_pipeline.logger"):
        r = daily_pipeline.update_usd(db)
    assert r["saved"] == 0
    db.save_usd_batch.assert_not_called()


@patch("tasks.macro.ak")
def test_update_global_index_success(mock_ak: MagicMock):
    db = MagicMock()
    db.save_global_index_batch.return_value = 3
    mock_ak.index_global_spot_em.return_value = pd.DataFrame({
        "代码": ["KS11", "BVSP", "N225"],
        "名称": ["韩国KOSPI", "巴西BOVESPA", "日经225"],
        "最新价": [7475.94, 177866.38, 39000.0],
        "涨跌额": [184.03, 5124.26, 100.0], "涨跌幅": [2.52, 2.97, 0.3],
        "开盘价": [7552.49, 172760.66, 38900.0], "最高价": [7704.93, 177866.38, 39100.0],
        "最低价": [7429.51, 172760.66, 38800.0], "昨收价": [7291.91, 172742.12, 38900.0],
        "振幅": [3.78, 2.96, 0.5],
        "最新行情时间": ["2026-07-10 14:29:59", "2026-07-11 04:08:30", "2026-07-10 14:00:00"],
    })
    with patch("daily_pipeline.logger"):
        r = daily_pipeline.update_global_index(db)
    assert r["saved"] == 3
    rec = db.save_global_index_batch.call_args[0][0][0]
    assert rec["index_code"] == "KS11"
    assert rec["quote_time"] == "2026-07-10 14:29:59"  # 真实列名: 最新行情时间


@patch("tasks.macro.ak")
def test_update_global_index_empty(mock_ak: MagicMock):
    db = MagicMock()
    mock_ak.index_global_spot_em.return_value = pd.DataFrame()
    with patch("daily_pipeline.logger"):
        r = daily_pipeline.update_global_index(db)
    assert r["saved"] == 0
    db.save_global_index_batch.assert_not_called()


@patch("tasks.macro.ak")
def test_update_global_index_sina_fallback(mock_ak: MagicMock):
    db = MagicMock()
    db.save_global_index_batch.return_value = 2
    mock_ak.index_global_spot_em.side_effect = Exception("connection reset")
    mock_ak.index_global_name_table.return_value = pd.DataFrame({
        "指数名称": ["英国富时100指数", "日经225指数"],
        "代码": ["UKX", "NKY"],
    })
    mock_ak.index_global_hist_sina.side_effect = [
        pd.DataFrame({
            "date": ["2026-07-13", "2026-07-14"],
            "open": [10450.0, 10498.7],
            "high": [10520.0, 10552.56],
            "low": [10420.0, 10422.98],
            "close": [10498.29, 10529.39],
            "volume": [0, 0],
        }),
        pd.DataFrame({
            "date": ["2026-07-13", "2026-07-14"],
            "open": [38900.0, 39000.0],
            "high": [39100.0, 39200.0],
            "low": [38800.0, 38900.0],
            "close": [39000.0, 39100.0],
            "volume": [0, 0],
        }),
    ]
    with patch("daily_pipeline.logger"):
        r = daily_pipeline.update_global_index(db)
    assert r["saved"] == 2
    recs = db.save_global_index_batch.call_args[0][0]
    codes = {rec["index_code"] for rec in recs}
    assert codes == {"FTSE", "N225"}
    n225 = [rec for rec in recs if rec["index_code"] == "N225"][0]
    assert n225["trade_date"] == "2026-07-14"
    assert n225["latest_price"] == 39100.0
    assert n225["change_amount"] == 100.0
    assert n225["change_pct"] == round(100.0 / 39000.0 * 100, 4)
    assert n225["data_source"] == "akshare_sina_fallback"


@patch("tasks.macro.ak")
def test_update_us_treasury_success(mock_ak: MagicMock):
    db = MagicMock()
    db.save_us_treasury_batch.return_value = 1
    mock_ak.bond_zh_us_rate.return_value = pd.DataFrame({
        "日期": ["2026-07-10"],
        "中国国债收益率2年": [1.26], "中国国债收益率5年": [1.44],
        "中国国债收益率10年": [1.74], "中国国债收益率30年": [2.25],
        "中国国债收益率10年-2年": [0.48],
        "美国国债收益率2年": [4.21], "美国国债收益率5年": [4.30],
        "美国国债收益率10年": [4.56], "美国国债收益率30年": [5.06],
        "美国国债收益率10年-2年": [0.35],
    })
    with patch("daily_pipeline.logger"):
        r = daily_pipeline.update_us_treasury(db)
    assert r["saved"] == 1
    rec = db.save_us_treasury_batch.call_args[0][0][0]
    assert rec["us_10y"] == 4.56
    assert rec["cn_10y"] == 1.74
    assert rec["spread_10y_2y"] == 0.35  # 真实列名: 美国国债收益率10年-2年


@patch("tasks.macro.ak")
def test_update_us_treasury_empty(mock_ak: MagicMock):
    db = MagicMock()
    mock_ak.bond_zh_us_rate.return_value = pd.DataFrame()
    with patch("daily_pipeline.logger"):
        r = daily_pipeline.update_us_treasury(db)
    assert r["saved"] == 0
    db.save_us_treasury_batch.assert_not_called()


# ===========================================================================
# A股补充数据 (v3.1): north_flow / index_daily / limit_up_down / dividend_summary
# ===========================================================================

@patch("tasks.macro.ak")
def test_update_north_flow_success(mock_ak: MagicMock):
    db = MagicMock()
    db.save_north_flow_batch.return_value = 2
    mock_ak.stock_hsgt_fund_flow_summary_em.return_value = pd.DataFrame({
        "交易日": ["2026-07-10", "2026-07-10"],
        "板块": ["沪股通", "深股通"],
        "资金方向": ["北向", "北向"],
        "成交净买额": [1.5, -0.8],
    })
    with patch("daily_pipeline.logger"):
        r = daily_pipeline.update_north_flow(db)
    assert r["saved"] == 2
    db.save_north_flow_batch.assert_called_once()
    rec = db.save_north_flow_batch.call_args[0][0][0]
    assert rec["market"] == "沪股通"
    assert rec["net_buy_amount"] == 1.5


@patch("tasks.macro.ak")
def test_update_north_flow_empty(mock_ak: MagicMock):
    db = MagicMock()
    mock_ak.stock_hsgt_fund_flow_summary_em.return_value = pd.DataFrame()
    with patch("daily_pipeline.logger"):
        r = daily_pipeline.update_north_flow(db)
    assert r["saved"] == 0
    db.save_north_flow_batch.assert_not_called()


@patch("tasks.macro.ak")
def test_update_north_flow_filters_south(mock_ak: MagicMock):
    """确认方向='南向'的行被过滤掉。"""
    db = MagicMock()
    db.save_north_flow_batch.return_value = 1
    mock_ak.stock_hsgt_fund_flow_summary_em.return_value = pd.DataFrame({
        "交易日": ["2026-07-10", "2026-07-10"],
        "板块": ["沪股通", "港股通(沪)"],
        "资金方向": ["北向", "南向"],
        "成交净买额": [1.5, -33.4],
    })
    with patch("daily_pipeline.logger"):
        r = daily_pipeline.update_north_flow(db)
    assert r["saved"] == 1  # 只有北向那行
    recs = db.save_north_flow_batch.call_args[0][0]
    assert len(recs) == 1
    assert recs[0]["market"] == "沪股通"


def _mk_north_hold_resp(payload: dict) -> MagicMock:
    """构造一个类 requests.Response 的 mock。"""
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.json.return_value = payload
    return resp


@patch("tasks.macro.requests")
def test_update_north_hold_success(mock_requests: MagicMock):
    """东财个股北向持仓：解析字段 + 日期截断 + 写入。"""
    db = MagicMock()
    db.save_north_hold_batch.return_value = 2
    payload = {
        "result": {
            "pages": 1,
            "count": 2,
            "data": [
                {
                    "SECURITY_CODE": "600519", "SECURITY_NAME": "贵州茅台",
                    "TRADE_DATE": "2026-06-30 00:00:00", "CLOSE_PRICE": 1185.49,
                    "HOLD_SHARES": 53711656, "HOLD_MARKET_CAP": 63674631071,
                    "HOLD_SHARES_RATIO": 4.29, "FREE_SHARES_RATIO": 4.30,
                    "TOTAL_SHARES_RATIO": 4.30,
                },
                {
                    "SECURITY_CODE": "300750", "SECURITY_NAME": "宁德时代",
                    "TRADE_DATE": "2026-06-30 00:00:00", "CLOSE_PRICE": 393.01,
                    "HOLD_SHARES": 894158187, "HOLD_MARKET_CAP": 351413000000,
                    "HOLD_SHARES_RATIO": 20.28, "FREE_SHARES_RATIO": 21.00,
                    "TOTAL_SHARES_RATIO": 19.33,
                },
            ],
        }
    }
    mock_requests.get.return_value = _mk_north_hold_resp(payload)
    with patch("daily_pipeline.logger"):
        r = daily_pipeline.update_north_hold(db)
    assert r["saved"] == 2
    db.save_north_hold_batch.assert_called_once()
    recs = db.save_north_hold_batch.call_args[0][0]
    assert recs[0]["ts_code"] == "600519"
    assert recs[0]["trade_date"] == "2026-06-30"  # 截断到 10 位
    assert recs[0]["hold_shares"] == 53711656
    assert recs[0]["data_source"] == "eastmoney"


@patch("tasks.macro.requests")
def test_update_north_hold_empty(mock_requests: MagicMock):
    db = MagicMock()
    payload = {"result": {"pages": 1, "count": 0, "data": []}}
    mock_requests.get.return_value = _mk_north_hold_resp(payload)
    with patch("daily_pipeline.logger"):
        r = daily_pipeline.update_north_hold(db)
    assert r["saved"] == 0
    db.save_north_hold_batch.assert_not_called()


@patch("tasks.macro.requests")
def test_update_north_hold_network_error_graceful(mock_requests: MagicMock):
    """网络/解析异常时优雅降级，不抛出、不写库。"""
    import requests as _real_requests
    db = MagicMock()
    # 保留真实异常类，使 `except (requests.RequestException, ValueError)` 合法
    mock_requests.RequestException = _real_requests.RequestException
    mock_requests.get.side_effect = _real_requests.RequestException("boom")
    with patch("daily_pipeline.logger"):
        r = daily_pipeline.update_north_hold(db)
    assert r["saved"] == 0
    db.save_north_hold_batch.assert_not_called()


@patch("tasks.index_chain.ak")
def test_update_index_daily_success(mock_ak: MagicMock):
    db = MagicMock()
    db.save_index_daily_batch.return_value = 4

    sample_df = pd.DataFrame({
        "date": ["2026-07-10"],
        "open": [3400.0], "high": [3420.0], "low": [3390.0],
        "close": [3410.0], "volume": [500000.0],
    })

    def _index(symbol: str) -> pd.DataFrame:
        return sample_df

    mock_ak.stock_zh_index_daily_tx.side_effect = _index
    with patch("daily_pipeline.logger"):
        r = daily_pipeline.update_index_daily(db)
    assert r["saved"] == 4
    rec = db.save_index_daily_batch.call_args[0][0][0]
    assert rec["index_code"] == "sh000001"
    assert rec["close"] == 3410.0


@patch("tasks.index_chain.ak")
def test_update_index_daily_empty(mock_ak: MagicMock):
    db = MagicMock()
    mock_ak.stock_zh_index_daily_tx.return_value = pd.DataFrame()
    with patch("daily_pipeline.logger"):
        r = daily_pipeline.update_index_daily(db)
    assert r["saved"] == 0
    db.save_index_daily_batch.assert_not_called()


@patch("tasks.macro.ak")
def test_update_limit_up_down_success(mock_ak: MagicMock):
    db = MagicMock()
    db.save_limit_up_down_batch.return_value = 3
    mock_ak.stock_zt_pool_em.return_value = pd.DataFrame({
        "代码": ["600519", "000858"],
        "名称": ["贵州茅台", "五粮液"],
        "涨跌幅": [10.0, 9.98],
        "最新价": [1500.0, 120.0],
        "换手率": [0.5, 0.8],
        "连板数": [1, 2],
        "所属行业": ["白酒", "白酒"],
    })
    mock_ak.stock_zt_pool_dtgc_em.return_value = pd.DataFrame({
        "代码": ["300750"],
        "名称": ["宁德时代"],
        "涨跌幅": [-9.99],
        "最新价": [180.0],
        "换手率": [2.5],
        "所属行业": ["电池"],
    })
    with patch("daily_pipeline.logger"):
        r = daily_pipeline.update_limit_up_down(db)
    assert r["saved"] == 3
    recs = db.save_limit_up_down_batch.call_args[0][0]
    up = [x for x in recs if x["limit_type"] == "涨停"]
    down = [x for x in recs if x["limit_type"] == "跌停"]
    assert len(up) == 2
    assert len(down) == 1
    assert up[0]["board_count"] == 1
    assert down[0]["ts_code"] == "300750"


@patch("tasks.macro.ak")
def test_update_limit_up_down_empty(mock_ak: MagicMock):
    db = MagicMock()
    mock_ak.stock_zt_pool_em.return_value = pd.DataFrame()
    mock_ak.stock_zt_pool_dtgc_em.return_value = pd.DataFrame()
    with patch("daily_pipeline.logger"):
        r = daily_pipeline.update_limit_up_down(db)
    assert r["saved"] == 0
    db.save_limit_up_down_batch.assert_not_called()


@patch("tasks.macro.ak")
def test_update_dividend_summary_success(mock_ak: MagicMock):
    db = MagicMock()
    db.save_dividend_summary_batch.return_value = 2
    mock_ak.stock_history_dividend.return_value = pd.DataFrame({
        "代码": ["000001", "000002"],
        "名称": ["平安银行", "万科A"],
        "上市日期": [datetime(1991, 4, 3).date(), datetime(1991, 1, 29).date()],
        "累计股息": [150.0, 220.0],
        "年均股息": [4.5, 6.8],
        "分红次数": [30, 35],
        "融资总额": [0.0, 0.0],
        "融资次数": [0, 0],
    })
    with patch("daily_pipeline.logger"):
        r = daily_pipeline.update_dividend_summary(db)
    assert r["saved"] == 2
    rec = db.save_dividend_summary_batch.call_args[0][0][0]
    assert rec["ts_code"] == "000001"
    assert rec["dividend_count"] == 30


@patch("tasks.macro.ak")
def test_update_dividend_summary_empty(mock_ak: MagicMock):
    db = MagicMock()
    mock_ak.stock_history_dividend.return_value = pd.DataFrame()
    with patch("daily_pipeline.logger"):
        r = daily_pipeline.update_dividend_summary(db)
    assert r["saved"] == 0
    db.save_dividend_summary_batch.assert_not_called()


# ===========================================================================
# Global macro: CLI branch coverage (Category 1)
# ===========================================================================

class TestGlobalMacroCli:
    @pytest.fixture(autouse=True)
    def _passthrough_safe_task(self):
        with patch(
            "daily_pipeline._safe_task",
            side_effect=lambda name, fn, *a, **kw: fn(*a, **kw),
        ):
            yield

    @pytest.mark.parametrize(
        "task_name,func_name",
        [
            ("update_north_flow", "update_north_flow"),
            ("update_index_daily", "update_index_daily"),
            ("update_limit_up_down", "update_limit_up_down"),
            ("update_dividend_summary", "update_dividend_summary"),
            ("update_gold_price", "update_gold_price"),
            ("update_crude_oil", "update_crude_oil"),
            ("update_usd", "update_usd"),
            ("update_global_index", "update_global_index"),
            ("update_us_treasury", "update_us_treasury"),
        ],
    )
    def test_new_global_macro_tasks(self, weekday_mock, task_name: str, func_name: str):
        with patch.object(sys, "argv", ["daily_pipeline.py", "--task", task_name]), \
             patch("daily_pipeline.ProviderFactory") as f, \
             patch(f"daily_pipeline.{func_name}") as fn, \
             patch.dict("daily_pipeline._TASK_CALLABLES", {task_name: fn}):
            f.configure.return_value = None
            f.get_db.return_value = db = MagicMock()
            f.get_loader.return_value = MagicMock()
            f.get_indicator_engine.return_value = MagicMock()
            daily_pipeline.main()
            fn.assert_called_once_with(db)


# ===========================================================================
# Global macro: ak=None early-return (Category 2)
# ===========================================================================

class TestGlobalMacroAkNone:
    def test_global_macro_ak_none(self):
        db = MagicMock()
        with patch("tasks.macro.ak", None), \
             patch("tasks.index_chain.ak", None), \
             patch("daily_pipeline.logger"):
            for fn in [
                daily_pipeline.update_north_flow,
                daily_pipeline.update_index_daily,
                daily_pipeline.update_limit_up_down,
                daily_pipeline.update_dividend_summary,
                daily_pipeline.update_gold_price,
                daily_pipeline.update_crude_oil,
                daily_pipeline.update_usd,
                daily_pipeline.update_global_index,
                daily_pipeline.update_us_treasury,
            ]:
                r = fn(db)
                assert r["saved"] == 0
                assert "error" in r


# ===========================================================================
# Global macro: exception handling (Category 3)
# ===========================================================================

class TestGlobalMacroFetchException:
    def test_north_flow_fetch_exception(self):
        db = MagicMock()
        with patch("tasks.macro.ak") as mock_ak, patch("daily_pipeline.logger"):
            mock_ak.stock_hsgt_fund_flow_summary_em.side_effect = ValueError("API error")
            r = daily_pipeline.update_north_flow(db)
        assert r["saved"] == 0

    def test_index_daily_fetch_exception(self):
        db = MagicMock()
        with patch("tasks.index_chain.ak") as mock_ak, patch("daily_pipeline.logger"):
            mock_ak.stock_zh_index_daily_tx.side_effect = RuntimeError("index fetch failed")
            r = daily_pipeline.update_index_daily(db)
        assert r["saved"] == 0

    def test_limit_up_down_fetch_exception(self):
        db = MagicMock()
        with patch("tasks.macro.ak") as mock_ak, patch("daily_pipeline.logger"):
            mock_ak.stock_zt_pool_em.side_effect = ValueError("zt_pool error")
            r = daily_pipeline.update_limit_up_down(db)
        assert r["saved"] == 0

    def test_limit_down_fetch_exception(self):
        db = MagicMock()
        db.save_limit_up_down_batch.return_value = 1
        with patch("tasks.macro.ak") as mock_ak, patch("daily_pipeline.logger"):
            mock_ak.stock_zt_pool_em.return_value = pd.DataFrame({
                "代码": ["000001"], "名称": ["平安银行"], "涨跌幅": [10.0],
                "最新价": [10.0], "换手率": [0.5], "连板数": [1], "所属行业": ["银行"],
            })
            mock_ak.stock_zt_pool_dtgc_em.side_effect = RuntimeError("zt_pool_dtgc error")
            r = daily_pipeline.update_limit_up_down(db)
        # limit_up succeeded (1 record) but limit_down failed internally; update returns non-zero saved, no crash
        assert r["saved"] == 1

    def test_dividend_summary_fetch_exception(self):
        db = MagicMock()
        with patch("tasks.macro.ak") as mock_ak, patch("daily_pipeline.logger"):
            mock_ak.stock_history_dividend.side_effect = ValueError("dividend error")
            r = daily_pipeline.update_dividend_summary(db)
        assert r["saved"] == 0

    def test_gold_price_fetch_exception(self):
        db = MagicMock()
        db.save_gold_price_batch.return_value = 0
        with patch("tasks.macro.ak") as mock_ak, patch("daily_pipeline.logger"):
            mock_ak.spot_golden_benchmark_sge.side_effect = ValueError("gold API error")
            r = daily_pipeline.update_gold_price(db)
        assert r["saved"] == 0

    def test_crude_oil_fetch_exception(self):
        db = MagicMock()
        with patch("tasks.macro.ak") as mock_ak, patch("daily_pipeline.logger"):
            mock_ak.futures_foreign_commodity_realtime.side_effect = RuntimeError("oil fetch failed")
            r = daily_pipeline.update_crude_oil(db)
        assert r["saved"] == 0

    def test_usd_fetch_exception(self):
        db = MagicMock()
        with patch("tasks.macro.ak") as mock_ak, patch("daily_pipeline.logger"):
            mock_ak.currency_boc_sina.side_effect = ValueError("fx error")
            r = daily_pipeline.update_usd(db)
        assert r["saved"] == 0

    def test_global_index_fetch_exception(self):
        db = MagicMock()
        with patch("tasks.macro.ak") as mock_ak, patch("daily_pipeline.logger"):
            mock_ak.index_global_spot_em.side_effect = RuntimeError("global index error")
            r = daily_pipeline.update_global_index(db)
        assert r["saved"] == 0

    def test_us_treasury_fetch_exception(self):
        db = MagicMock()
        with patch("tasks.macro.ak") as mock_ak, patch("daily_pipeline.logger"):
            mock_ak.bond_zh_us_rate.side_effect = ValueError("treasury error")
            r = daily_pipeline.update_us_treasury(db)
        assert r["saved"] == 0


# ===========================================================================
# Global macro: update_* except blocks via db.save_*_batch exceptions (Category 3b)
# ===========================================================================

class TestGlobalMacroDbSaveException:
    """Cover update_* except blocks by making db.save_*_batch raise."""

    def _make_records(self, count: int = 1) -> list[dict]:
        return [{"x": i} for i in range(count)]

    def test_north_flow_db_exception(self):
        db = MagicMock()
        db.save_north_flow_batch.side_effect = RuntimeError("db error")
        with patch("tasks.macro.ak") as mock_ak, patch("daily_pipeline.logger"):
            mock_ak.stock_hsgt_fund_flow_summary_em.return_value = pd.DataFrame({
                "交易日": ["2026-07-10"], "板块": ["沪股通"],
                "资金方向": ["北向"], "成交净买额": [1.0],
            })
            r = daily_pipeline.update_north_flow(db)
        assert r["saved"] == 0
        assert "error" in r

    def test_index_daily_db_exception(self):
        db = MagicMock()
        db.save_index_daily_batch.side_effect = RuntimeError("db error")
        sample_df = pd.DataFrame({"date": ["2026-07-10"], "open": [3400.0], "high": [3420.0],
                                   "low": [3390.0], "close": [3410.0], "volume": [500000.0]})
        with patch("tasks.index_chain.ak") as mock_ak, patch("daily_pipeline.logger"):
            mock_ak.stock_zh_index_daily_tx.return_value = sample_df
            r = daily_pipeline.update_index_daily(db)
        assert r["saved"] == 0
        assert "error" in r

    def test_limit_up_down_db_exception(self):
        db = MagicMock()
        db.save_limit_up_down_batch.side_effect = RuntimeError("db error")
        up_df = pd.DataFrame({"代码": ["000001"], "名称": ["平安银行"], "涨跌幅": [10.0],
                              "最新价": [10.0], "换手率": [0.5], "连板数": [1], "所属行业": ["银行"]})
        with patch("tasks.macro.ak") as mock_ak, patch("daily_pipeline.logger"):
            mock_ak.stock_zt_pool_em.return_value = up_df
            mock_ak.stock_zt_pool_dtgc_em.return_value = pd.DataFrame()
            r = daily_pipeline.update_limit_up_down(db)
        assert r["saved"] == 0
        assert "error" in r

    def test_dividend_summary_db_exception(self):
        db = MagicMock()
        db.save_dividend_summary_batch.side_effect = RuntimeError("db error")
        with patch("tasks.macro.ak") as mock_ak, patch("daily_pipeline.logger"):
            mock_ak.stock_history_dividend.return_value = pd.DataFrame({
                "代码": ["000001"], "名称": ["平安银行"], "上市日期": [datetime(1991, 4, 3).date()],
                "累计股息": [150.0], "年均股息": [4.5], "分红次数": [30],
                "融资总额": [0.0], "融资次数": [0],
            })
            r = daily_pipeline.update_dividend_summary(db)
        assert r["saved"] == 0
        assert "error" in r

    def test_gold_price_db_exception(self):
        db = MagicMock()
        db.save_gold_price_batch.side_effect = RuntimeError("db error")
        with patch("tasks.macro.ak") as mock_ak, patch("daily_pipeline.logger"):
            mock_ak.spot_golden_benchmark_sge.return_value = pd.DataFrame({
                "交易时间": ["2026-07-11 早盘"], "晚盘价": [897.58], "早盘价": [891.66],
            })
            r = daily_pipeline.update_gold_price(db)
        assert r["saved"] == 0
        assert "error" in r

    def test_crude_oil_db_exception(self):
        db = MagicMock()
        db.save_crude_oil_batch.side_effect = RuntimeError("db error")
        def _oil(symbol: str) -> pd.DataFrame:
            return pd.DataFrame({
                "最新价": [73.5], "涨跌额": [-1.0], "涨跌幅": [-1.3], "开盘价": [74.0],
                "最高价": [75.0], "最低价": [73.0], "昨日结算价": [74.5],
            })
        with patch("tasks.macro.ak") as mock_ak, patch("daily_pipeline.logger"):
            mock_ak.futures_foreign_commodity_realtime.side_effect = _oil
            r = daily_pipeline.update_crude_oil(db)
        assert r["saved"] == 0
        assert "error" in r

    def test_usd_db_exception(self):
        db = MagicMock()
        db.save_usd_batch.side_effect = RuntimeError("db error")
        with patch("tasks.macro.ak") as mock_ak, patch("daily_pipeline.logger"):
            mock_ak.currency_boc_sina.return_value = pd.DataFrame({
                "日期": ["2026-07-10"], "中行汇买价": [676.44], "中行钞买价": [676.44],
                "中行钞卖价/汇卖价": [679.29], "央行中间价": [679.89], "中行折算价": [679.89],
            })
            r = daily_pipeline.update_usd(db)
        assert r["saved"] == 0
        assert "error" in r

    def test_global_index_db_exception(self):
        db = MagicMock()
        db.save_global_index_batch.side_effect = RuntimeError("db error")
        with patch("tasks.macro.ak") as mock_ak, patch("daily_pipeline.logger"):
            mock_ak.index_global_spot_em.return_value = pd.DataFrame({
                "代码": ["KS11"], "名称": ["韩国KOSPI"], "最新价": [7475.94],
                "涨跌额": [184.03], "涨跌幅": [2.52], "开盘价": [7552.49],
                "最高价": [7704.93], "最低价": [7429.51], "昨收价": [7291.91],
                "振幅": [3.78], "最新行情时间": ["2026-07-10 14:29:59"],
            })
            r = daily_pipeline.update_global_index(db)
        assert r["saved"] == 0
        assert "error" in r

    def test_us_treasury_db_exception(self):
        db = MagicMock()
        db.save_us_treasury_batch.side_effect = RuntimeError("db error")
        with patch("tasks.macro.ak") as mock_ak, patch("daily_pipeline.logger"):
            mock_ak.bond_zh_us_rate.return_value = pd.DataFrame({
                "日期": ["2026-07-10"], "中国国债收益率2年": [1.26],
                "中国国债收益率5年": [1.44], "中国国债收益率10年": [1.74],
                "中国国债收益率30年": [2.25], "中国国债收益率10年-2年": [0.48],
                "美国国债收益率2年": [4.21], "美国国债收益率5年": [4.30],
                "美国国债收益率10年": [4.56], "美国国债收益率30年": [5.06],
                "美国国债收益率10年-2年": [0.35],
            })
            r = daily_pipeline.update_us_treasury(db)
        assert r["saved"] == 0
        assert "error" in r


# ===========================================================================
# _should_update
# ===========================================================================
class TestShouldUpdate:
    def test_weekend(self):
        with patch("core.utils.datetime") as m:
            m.now.return_value = datetime(2026, 6, 27)  # Saturday
            m.side_effect = lambda *a, **kw: datetime(*a, **kw)
            with patch("daily_pipeline.logger"):
                assert not daily_pipeline._should_update()

    def test_during_trading(self):
        with patch("core.utils.datetime") as m:
            m.now.return_value = datetime(2026, 6, 22, 10, 0)  # Monday 10am
            m.side_effect = lambda *a, **kw: datetime(*a, **kw)
            with patch("daily_pipeline.logger"):
                assert not daily_pipeline._should_update()

    def test_settlement_window(self):
        with patch("core.utils.datetime") as m:
            m.now.return_value = datetime(2026, 6, 22, 15, 0)  # 15:00
            m.side_effect = lambda *a, **kw: datetime(*a, **kw)
            with patch("daily_pipeline.logger"):
                assert not daily_pipeline._should_update()

    def test_after_hours_ok(self):
        with patch("core.utils.datetime") as m:
            m.now.return_value = datetime(2026, 6, 22, 16, 0)
            m.side_effect = lambda *a, **kw: datetime(*a, **kw)
            with patch("daily_pipeline.logger"):
                assert daily_pipeline._should_update()


# ===========================================================================
# _infer_market
# ===========================================================================
class TestInferMarket:
    @pytest.mark.parametrize(
        "code,expected",
        [
            ("688001", "star"),
            ("600000", "sh"),
            ("900001", "sh"),
            ("300750", "gem"),
            ("301000", "gem"),
            ("002000", "sme"),
            ("003000", "sme"),
            ("000001", "sz"),
            ("001000", "sz"),
            ("430001", "bj"),
            ("830001", "bj"),
            ("920001", "bj"),
            ("999999", "sz"),  # fallback
        ],
    )
    def test_market_inference(self, code, expected):
        assert daily_pipeline._infer_market(code) == expected


# ===========================================================================
# update_stock_list
# ===========================================================================
class TestUpdateStockList:
    def test_normal(self):
        db = MagicMock()
        db.save_stock_list.return_value = None
        with patch("tasks.core_chain.ak") as mock_ak:
            mock_ak.stock_info_a_code_name.return_value = pd.DataFrame({
                "code": ["000001", "600000", "688001"],
                "name": ["平安银行", "浦发银行", "科创板测试"],
            })
            with patch("daily_pipeline.logger"):
                r = daily_pipeline.update_stock_list(db)
        assert r["saved"] == 3
        db.save_stock_list.assert_called_once()

    def test_ak_none(self):
        db = MagicMock()
        with patch("tasks.core_chain.ak", None), patch("daily_pipeline.logger"):
            r = daily_pipeline.update_stock_list(db)
        assert r["saved"] == 0
        assert "error" in r

    def test_empty_response(self):
        db = MagicMock()
        with patch("tasks.core_chain.ak") as mock_ak:
            mock_ak.stock_info_a_code_name.return_value = pd.DataFrame()
            with patch("daily_pipeline.logger"):
                r = daily_pipeline.update_stock_list(db)
        assert r["saved"] == 0

    def test_ak_exception(self):
        db = MagicMock()
        with patch("tasks.core_chain.ak") as mock_ak:
            mock_ak.stock_info_a_code_name.side_effect = RuntimeError("fail")
            with patch("daily_pipeline.logger"):
                r = daily_pipeline.update_stock_list(db)
        assert r["saved"] == 0


# ===========================================================================
# AkShareMonitor
# ===========================================================================
class TestAkShareMonitor:
    def test_record_and_success_rate(self):
        m = daily_pipeline.AkShareMonitor()
        m.record(True, "000001")
        m.record(False, "000002")
        assert m.get_success_rate(window=2) == 0.5

    def test_empty_records_rate(self):
        m = daily_pipeline.AkShareMonitor()
        m.records.clear()
        assert m.get_success_rate() == 1.0

    def test_recommended_multiplier(self):
        m = daily_pipeline.AkShareMonitor()
        m.records.clear()
        assert m.get_recommended_sleep_multiplier() == 1.0
        # Simulate low rate
        m.records = [{"success": False} for _ in range(20)]
        assert m.get_recommended_sleep_multiplier() >= 2.0

    def test_should_abort_no_attempts(self):
        m = daily_pipeline.AkShareMonitor()
        abort, _ = m.should_abort()
        assert not abort

    def test_should_abort_consecutive(self):
        m = daily_pipeline.AkShareMonitor()
        m.record(False, "000001")
        m.record(False, "000002")
        m.record(False, "000003")
        abort, _ = m.should_abort()
        assert abort

    def test_should_abort_low_rate(self):
        m = daily_pipeline.AkShareMonitor()
        m.records = [{"success": False} for _ in range(25)]
        m.current_run_attempts = 20
        # 中止判定使用本轮滑动窗口（ABORT_WINDOW 次），非整轮累计均值
        for _ in range(m.ABORT_WINDOW):
            m.current_run_recent.append(False)
        abort, _ = m.should_abort()
        assert abort

    def test_should_abort_late_run_failures_not_diluted(self):
        """前期大量成功不应摊薄后期的全面故障。"""
        m = daily_pipeline.AkShareMonitor()
        for i in range(3000):
            m.record(True, f"{i:06d}")
        m.current_run_consecutive_failures = 0
        for i in range(m.ABORT_WINDOW):
            m.record(False, f"fail{i:04d}")
            m.current_run_consecutive_failures = 0  # 模拟哨兵化解连续失败规则
        abort, _ = m.should_abort()
        assert abort

    def test_log_status(self):
        m = daily_pipeline.AkShareMonitor()
        with patch("daily_pipeline.logger"):
            m.log_status()  # should not raise


# ===========================================================================
# update_market_snapshot
# ===========================================================================
class TestUpdateMarketSnapshot:
    def test_skipped_when_already_run(self, tmp_path: Path):
        db = MagicMock()
        db.db_path = str(tmp_path / "test.db")
        db.get_last_task_run.return_value = "2026-06-30"
        # Setup tables
        conn = sqlite3.connect(db.db_path)
        conn.execute("CREATE TABLE fundamentals (ts_code TEXT, trade_date TEXT)")
        conn.execute("CREATE TABLE stock_list (code TEXT, market TEXT)")
        conn.execute("INSERT INTO stock_list VALUES ('000001', 'sz')")
        conn.execute("INSERT INTO fundamentals VALUES ('000001', '2026-06-30')")
        conn.commit()
        conn.close()
        with patch("daily_pipeline.datetime") as m:
            m.now.return_value = datetime(2026, 6, 30, 16, 0)
            m.side_effect = lambda *a, **kw: datetime(*a, **kw)
            with patch("daily_pipeline.logger"):
                r = daily_pipeline.update_market_snapshot(db)
        assert r.get("skipped") or r["saved"] == 0

    def test_no_xueqiu_token(self, tmp_path: Path):
        db = MagicMock()
        db.db_path = str(tmp_path / "test.db")
        db.get_last_task_run.return_value = None
        conn = sqlite3.connect(db.db_path)
        conn.execute("CREATE TABLE fundamentals (ts_code TEXT, trade_date TEXT)")
        conn.execute("CREATE TABLE stock_list (code TEXT, market TEXT)")
        conn.execute("INSERT INTO stock_list VALUES ('000001', 'sz')")
        conn.commit()
        conn.close()
        with patch("daily_pipeline.datetime") as m:
            m.now.return_value = datetime(2026, 6, 30, 16, 0)
            m.side_effect = lambda *a, **kw: datetime(*a, **kw)
            with patch("daily_pipeline.logger"), \
                 patch("smartmoney_hunter.xueqiu._get_token", return_value=None):
                r = daily_pipeline.update_market_snapshot(db)
        assert r.get("skipped") or r["total"] == 0

    def test_empty_stock_list(self, tmp_path: Path):
        db = MagicMock()
        db.db_path = str(tmp_path / "test.db")
        db.get_last_task_run.return_value = None
        conn = sqlite3.connect(db.db_path)
        conn.execute("CREATE TABLE fundamentals (ts_code TEXT, trade_date TEXT)")
        conn.execute("CREATE TABLE stock_list (code TEXT, market TEXT)")
        conn.commit()
        conn.close()
        with patch("daily_pipeline.logger"):
            r = daily_pipeline.update_market_snapshot(db)
        assert r["total"] == 0


# ===========================================================================
# update_quarterly_financials
# ===========================================================================
class TestUpdateQuarterlyFinancials:
    def test_ak_none(self):
        db = MagicMock()
        loader = MagicMock()
        with patch.object(daily_pipeline, "ak", None), patch("daily_pipeline.logger"):
            r = daily_pipeline.update_quarterly_financials(db, loader)
        assert r["saved"] == 0

    def test_empty_stock_list(self):
        db = MagicMock()
        loader = MagicMock()
        db.get_stock_list.return_value = pd.DataFrame()
        with patch("tasks.financials.ak"), patch("daily_pipeline.logger"):
            r = daily_pipeline.update_quarterly_financials(db, loader)
        assert r["total"] == 0

    def test_normal_with_mock(self):
        db = MagicMock()
        loader = MagicMock()
        db.get_stock_list.return_value = pd.DataFrame({"code": ["000001"]})
        db.get_distinct_codes.return_value = set()
        with patch("tasks.financials.ak") as mock_ak, \
             patch("daily_pipeline.logger"), \
             patch("daily_pipeline.time.sleep"):
            df = pd.DataFrame({
                "指标": ["营业总收入", "归母净利润"],
                "当前值": ["2024-12-31", "2024-12-31"],
                "value": [1e10, 1e9],
            })
            mock_ak.stock_financial_abstract.return_value = df
            db.save_quarterly_financials_batch.return_value = 1
            r = daily_pipeline.update_quarterly_financials(db, loader)
        assert r["total"] >= 0


# ===========================================================================
# update_industry
# ===========================================================================
class TestUpdateIndustry:
    def test_all_have_industry(self, tmp_path: Path):
        db = MagicMock()
        db.db_path = str(tmp_path / "test.db")
        conn = sqlite3.connect(db.db_path)
        conn.execute("CREATE TABLE stock_list (code TEXT, market TEXT, industry TEXT)")
        conn.execute("INSERT INTO stock_list VALUES ('000001', 'sz', '银行')")
        conn.commit()
        conn.close()
        with patch("daily_pipeline.logger"):
            r = daily_pipeline.update_industry(db)
        assert r["total"] == 0

    def test_f10_returns_industry(self, tmp_path: Path):
        db = MagicMock()
        db.db_path = str(tmp_path / "test.db")
        conn = sqlite3.connect(db.db_path)
        conn.execute("CREATE TABLE stock_list (code TEXT, market TEXT, industry TEXT, updated_at TEXT)")
        conn.execute("INSERT INTO stock_list VALUES ('000001', 'sz', NULL, NULL)")
        conn.commit()
        conn.close()
        with patch("daily_pipeline.logger"), \
             patch("requests.Session") as mock_session_cls:
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.json.return_value = {"jbzl": {"sshy": "银行"}}
            mock_session_cls.return_value.get.return_value = mock_resp
            r = daily_pipeline.update_industry(db)
        assert r["total"] == 1

    def test_f10_blocked_falls_back_to_akshare(self, tmp_path: Path):
        """When 3 F10 attempts fail, should fall back to AkShare."""
        db = MagicMock()
        db.db_path = str(tmp_path / "test.db")
        conn = sqlite3.connect(db.db_path)
        conn.execute("CREATE TABLE stock_list (code TEXT, market TEXT, industry TEXT, updated_at TEXT)")
        conn.execute("INSERT INTO stock_list VALUES ('000001', 'sz', NULL, NULL)")
        conn.commit()
        conn.close()
        with patch("daily_pipeline.logger"), \
             patch("requests.Session") as mock_session_cls, \
             patch("tasks.financials.ak") as mock_ak, \
             patch("daily_pipeline.time.sleep"):
            # F10 returns 403 for all attempts
            mock_resp = MagicMock()
            mock_resp.status_code = 403
            mock_session_cls.return_value.get.return_value = mock_resp
            # AkShare fallback
            mock_ak.stock_individual_info_em.return_value = pd.DataFrame({
                0: ["行业"], 1: ["银行"],
            })
            r = daily_pipeline.update_industry(db)
        assert r["total"] == 1


# ===========================================================================
# CLI: additional main() task branches
# ===========================================================================
class TestRunRegistryTaskSafeTaskWrap:
    def test_single_task_routes_through_safe_task(self):
        """单任务模式必须经 safe_task：审计父行 + _task_run_id 注入。"""
        mock_fn = MagicMock(return_value={"status": "success", "saved": 1})
        db = MagicMock()
        with patch("daily_pipeline._safe_task") as safe_task, \
             patch.dict("daily_pipeline._TASK_CALLABLES", {"update_concept_member": mock_fn}):
            daily_pipeline._run_registry_task("update_concept_member", db)
            safe_task.assert_called_once_with("update_concept_member", mock_fn, db)

    def test_daily_core_not_double_wrapped(self):
        """编排器 update_daily_core 内部任务已各自经 safe_task，本身不再包一层。"""
        mock_fn = MagicMock(return_value={})
        db, loader, engine = MagicMock(), MagicMock(), MagicMock()
        with patch("daily_pipeline._safe_task") as safe_task, \
             patch.dict("daily_pipeline._TASK_CALLABLES", {"update_daily_core": mock_fn}):
            daily_pipeline._run_registry_task("update_daily_core", db, loader, engine)
            safe_task.assert_not_called()
            mock_fn.assert_called_once_with(db, loader, engine, resume=False, force=False)


class TestMainMoreTasks:
    @pytest.fixture(autouse=True)
    def _passthrough_safe_task(self):
        with patch(
            "daily_pipeline._safe_task",
            side_effect=lambda name, fn, *a, **kw: fn(*a, **kw),
        ):
            yield

    def test_task_update_stock_list(self, weekday_mock):
        with patch.object(sys, "argv", ["daily_pipeline.py", "--task", "update_stock_list"]), \
             patch("daily_pipeline.ProviderFactory") as f, \
             patch("daily_pipeline.update_stock_list") as fn, \
             patch.dict("daily_pipeline._TASK_CALLABLES", {"update_stock_list": fn}):
            f.configure.return_value = None
            f.get_db.return_value = MagicMock()
            f.get_loader.return_value = MagicMock()
            f.get_indicator_engine.return_value = MagicMock()
            daily_pipeline.main()
            fn.assert_called_once()

    def test_task_update_indicators(self, weekday_mock):
        with patch.object(sys, "argv", ["daily_pipeline.py", "--task", "update_indicators"]), \
             patch("daily_pipeline.ProviderFactory") as f, \
             patch("daily_pipeline.update_indicators") as fn, \
             patch.dict("daily_pipeline._TASK_CALLABLES", {"update_indicators": fn}):
            f.configure.return_value = None
            f.get_db.return_value = MagicMock()
            f.get_loader.return_value = MagicMock()
            f.get_indicator_engine.return_value = MagicMock()
            daily_pipeline.main()
            fn.assert_called_once()

    def test_task_update_fundamentals(self, weekday_mock):
        with patch.object(sys, "argv", ["daily_pipeline.py", "--task", "update_fundamentals"]), \
             patch("daily_pipeline.ProviderFactory") as f, \
             patch("daily_pipeline.update_fundamentals") as fn, \
             patch.dict("daily_pipeline._TASK_CALLABLES", {"update_fundamentals": fn}):
            f.configure.return_value = None
            f.get_db.return_value = MagicMock()
            f.get_loader.return_value = MagicMock()
            f.get_indicator_engine.return_value = MagicMock()
            daily_pipeline.main()
            fn.assert_called_once()

    def test_task_update_fund_flow(self, weekday_mock):
        with patch.object(sys, "argv", ["daily_pipeline.py", "--task", "update_fund_flow"]), \
             patch("daily_pipeline.ProviderFactory") as f, \
             patch("daily_pipeline.update_fund_flow") as fn, \
             patch.dict("daily_pipeline._TASK_CALLABLES", {"update_fund_flow": fn}):
            f.configure.return_value = None
            f.get_db.return_value = MagicMock()
            f.get_loader.return_value = MagicMock()
            f.get_indicator_engine.return_value = MagicMock()
            daily_pipeline.main()
            fn.assert_called_once()

    def test_task_update_market_snapshot(self, weekday_mock):
        with patch.object(sys, "argv", ["daily_pipeline.py", "--task", "update_market_snapshot"]), \
             patch("daily_pipeline.ProviderFactory") as f, \
             patch("daily_pipeline.update_market_snapshot") as fn, \
             patch.dict("daily_pipeline._TASK_CALLABLES", {"update_market_snapshot": fn}):
            f.configure.return_value = None
            f.get_db.return_value = MagicMock()
            f.get_loader.return_value = MagicMock()
            f.get_indicator_engine.return_value = MagicMock()
            daily_pipeline.main()
            fn.assert_called_once()

    def test_task_update_margin_trading(self, weekday_mock):
        with patch.object(sys, "argv", ["daily_pipeline.py", "--task", "update_margin_trading"]), \
             patch("daily_pipeline.ProviderFactory") as f, \
             patch("daily_pipeline.update_margin_trading") as fn, \
             patch.dict("daily_pipeline._TASK_CALLABLES", {"update_margin_trading": fn}):
            f.configure.return_value = None
            f.get_db.return_value = MagicMock()
            f.get_loader.return_value = MagicMock()
            f.get_indicator_engine.return_value = MagicMock()
            daily_pipeline.main()
            fn.assert_called_once()

    def test_task_update_dragon_tiger(self, weekday_mock):
        with patch.object(sys, "argv", ["daily_pipeline.py", "--task", "update_dragon_tiger"]), \
             patch("daily_pipeline.ProviderFactory") as f, \
             patch("daily_pipeline.update_dragon_tiger") as fn, \
             patch.dict("daily_pipeline._TASK_CALLABLES", {"update_dragon_tiger": fn}):
            f.configure.return_value = None
            f.get_db.return_value = MagicMock()
            f.get_loader.return_value = MagicMock()
            f.get_indicator_engine.return_value = MagicMock()
            daily_pipeline.main()
            fn.assert_called_once()

    def test_task_update_block_trade(self, weekday_mock):
        with patch.object(sys, "argv", ["daily_pipeline.py", "--task", "update_block_trade"]), \
             patch("daily_pipeline.ProviderFactory") as f, \
             patch("daily_pipeline.update_block_trade") as fn, \
             patch.dict("daily_pipeline._TASK_CALLABLES", {"update_block_trade": fn}):
            f.configure.return_value = None
            f.get_db.return_value = MagicMock()
            f.get_loader.return_value = MagicMock()
            f.get_indicator_engine.return_value = MagicMock()
            daily_pipeline.main()
            fn.assert_called_once()

    def test_task_update_sector_fund_flow(self, weekday_mock):
        with patch.object(sys, "argv", ["daily_pipeline.py", "--task", "update_sector_fund_flow"]), \
             patch("daily_pipeline.ProviderFactory") as f, \
             patch("daily_pipeline.update_sector_fund_flow") as fn, \
             patch.dict("daily_pipeline._TASK_CALLABLES", {"update_sector_fund_flow": fn}):
            f.configure.return_value = None
            f.get_db.return_value = MagicMock()
            f.get_loader.return_value = MagicMock()
            f.get_indicator_engine.return_value = MagicMock()
            daily_pipeline.main()
            fn.assert_called_once()

    def test_task_update_historical_valuation(self, weekday_mock):
        with patch.object(sys, "argv", ["daily_pipeline.py", "--task", "update_historical_valuation"]), \
             patch("daily_pipeline.ProviderFactory") as f, \
             patch("daily_pipeline.update_historical_valuation") as fn, \
             patch.dict("daily_pipeline._TASK_CALLABLES", {"update_historical_valuation": fn}):
            f.configure.return_value = None
            f.get_db.return_value = MagicMock()
            f.get_loader.return_value = MagicMock()
            f.get_indicator_engine.return_value = MagicMock()
            daily_pipeline.main()
            fn.assert_called_once()

    def test_task_update_sector_industry(self, weekday_mock):
        with patch.object(sys, "argv", ["daily_pipeline.py", "--task", "update_sector_industry"]), \
             patch("daily_pipeline.ProviderFactory") as f, \
             patch("daily_pipeline.update_sector_industry") as fn, \
             patch.dict("daily_pipeline._TASK_CALLABLES", {"update_sector_industry": fn}):
            f.configure.return_value = None
            f.get_db.return_value = MagicMock()
            f.get_loader.return_value = MagicMock()
            f.get_indicator_engine.return_value = MagicMock()
            daily_pipeline.main()
            fn.assert_called_once()

    def test_task_update_shareholder_count(self, weekday_mock):
        with patch.object(sys, "argv", ["daily_pipeline.py", "--task", "update_shareholder_count"]), \
             patch("daily_pipeline.ProviderFactory") as f, \
             patch("daily_pipeline.update_shareholder_count") as fn, \
             patch.dict("daily_pipeline._TASK_CALLABLES", {"update_shareholder_count": fn}):
            f.configure.return_value = None
            f.get_db.return_value = MagicMock()
            f.get_loader.return_value = MagicMock()
            f.get_indicator_engine.return_value = MagicMock()
            daily_pipeline.main()
            fn.assert_called_once()

    def test_task_update_quarterly_financials(self, weekday_mock):
        with patch.object(sys, "argv", ["daily_pipeline.py", "--task", "update_quarterly_financials"]), \
             patch("daily_pipeline.ProviderFactory") as f, \
             patch("daily_pipeline.update_quarterly_financials") as fn, \
             patch.dict("daily_pipeline._TASK_CALLABLES", {"update_quarterly_financials": fn}):
            f.configure.return_value = None
            f.get_db.return_value = MagicMock()
            f.get_loader.return_value = MagicMock()
            f.get_indicator_engine.return_value = MagicMock()
            daily_pipeline.main()
            fn.assert_called_once()

    def test_task_update_industry(self, weekday_mock):
        with patch.object(sys, "argv", ["daily_pipeline.py", "--task", "update_industry"]), \
             patch("daily_pipeline.ProviderFactory") as f, \
             patch("daily_pipeline.update_industry") as fn, \
             patch.dict("daily_pipeline._TASK_CALLABLES", {"update_industry": fn}):
            f.configure.return_value = None
            f.get_db.return_value = MagicMock()
            f.get_loader.return_value = MagicMock()
            f.get_indicator_engine.return_value = MagicMock()
            daily_pipeline.main()
            fn.assert_called_once()

    def test_invalid_task(self, weekday_mock):
        with patch.object(sys, "argv", ["daily_pipeline.py", "--task", "nonexistent_task"]), \
             patch("daily_pipeline.ProviderFactory") as f, \
             patch("daily_pipeline.logger"):
            f.configure.return_value = None
            f.get_db.return_value = MagicMock()
            f.get_loader.return_value = MagicMock()
            f.get_indicator_engine.return_value = MagicMock()
            with pytest.raises(SystemExit):
                daily_pipeline.main()

    def test_keyboard_interrupt(self, weekday_mock):
        with patch.object(sys, "argv", ["daily_pipeline.py"]), \
             patch("daily_pipeline.ProviderFactory") as f, \
             patch("daily_pipeline.run_all", side_effect=KeyboardInterrupt), \
             patch("daily_pipeline.logger"):
            f.configure.return_value = None
            f.get_db.return_value = MagicMock()
            f.get_loader.return_value = MagicMock()
            f.get_indicator_engine.return_value = MagicMock()
            daily_pipeline.main()  # should handle gracefully


# ===========================================================================
# _lower_process_priority
# ===========================================================================
def test_lower_process_priority():
    daily_pipeline._lower_process_priority()  # should not raise


# ===========================================================================
# margin_trading: _safe_fetch_margin_detail with ValueErrors
# ===========================================================================
class TestMarginTradingSafety:
    def test_safe_fetch_length_mismatch(self):
        from tasks.market_flow import _safe_fetch_margin_detail

        def bad_fetcher(date=None):
            raise ValueError("Length mismatch: Expected axis has 10 elements")
        with patch("daily_pipeline.logger"):
            r = _safe_fetch_margin_detail(bad_fetcher, "20240701", "sh")
        assert r is None

    def test_safe_fetch_other_value_error(self):
        from tasks.market_flow import _safe_fetch_margin_detail

        def bad_fetcher(date=None):
            raise ValueError("Some other error")
        with pytest.raises(ValueError):
            _safe_fetch_margin_detail(bad_fetcher, "20240701", "sh")

    def test_safe_fetch_none_return(self):
        from tasks.market_flow import _safe_fetch_margin_detail

        def none_fetcher(date=None):
            return None
        with patch("daily_pipeline.logger"):
            r = _safe_fetch_margin_detail(none_fetcher, "20240701", "sh")
        assert r is None


# ===========================================================================
# margin_trading: both exchanges + date fallback
# ===========================================================================
class TestMarginTradingFull:
    def test_both_exchanges(self):
        db = MagicMock()
        db.save_margin_trading_batch.return_value = 2
        sse_df = pd.DataFrame({
            "标的证券代码": ["000001.SZ"], "融资余额": [1e9], "融资买入额": [1e8],
            "融资偿还额": [5e7], "融券余量": [1e5], "融券卖出量": [1e4], "融资融券余额": [1.1e9],
        })
        szse_df = pd.DataFrame({
            "证券代码": ["600000.SH"], "融资余额": [2e9], "融资买入额": [2e8],
            "融券余量": [2e5], "融券卖出量": [2e4], "融资融券余额": [2.2e9],
        })
        with patch("tasks.market_flow.ak") as ak:
            ak.stock_margin_detail_sse.return_value = sse_df
            ak.stock_margin_detail_szse.return_value = szse_df
            with patch("daily_pipeline.datetime") as m:
                m.now.return_value = datetime(2024, 6, 21)
                m.side_effect = lambda *a, **kw: datetime(*a, **kw)
                with patch.object(daily_pipeline, "timedelta") as td:
                    td.return_value = timedelta(days=1)
                    with patch("daily_pipeline.logger"):
                        r = daily_pipeline.update_margin_trading(db)
        assert r["saved"] == 2


# ===========================================================================
# update_fundamentals: already has data, skip
# ===========================================================================
def test_update_fundamentals_already_has_data():
    db = MagicMock()
    db.count_fundamentals_for_date.return_value = 5500  # > MIN_FUNDAMENTALS_STOCK_COUNT
    loader = MagicMock()
    with patch("daily_pipeline.datetime") as mock_dt, patch("daily_pipeline.logger"):
        mock_dt.now.return_value = datetime(2026, 6, 30, 9, 0, 0)
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw) if a else mock_dt.now()
        r = daily_pipeline.update_fundamentals(db, loader)
    assert r.get("skipped") or r["saved"] == 0


# ===========================================================================
# run_all: include_optional=False
# ===========================================================================
class TestRunAllOptions:
    def test_without_optional(self, tmp_path: Path, weekday_mock):
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
        conn.commit()
        conn.close()
        with patch.object(daily_pipeline, "SHARED_DATA_DIR", tmp_path), \
             patch("daily_pipeline.time.sleep"), patch("daily_pipeline.logger"), \
             patch("daily_pipeline._should_update", return_value=True), \
             patch.object(
                 daily_pipeline,
                 "_safe_task",
                 return_value={"status": "ok"},
             ) as safe_task:
            r = daily_pipeline.run_all(db, loader, engine)
        assert "bars" in r
        assert safe_task.call_count > 20


# ===========================================================================
# update_bars: abort path
# ===========================================================================
def test_update_bars_abort_on_monitor():
    """When AkShareMonitor says abort, update_bars should return early."""
    db = MagicMock()
    loader = MagicMock()
    stocks_df = pd.DataFrame({"code": ["000001", "000002"]})
    db.get_stock_list.return_value = stocks_df
    db.get_daily_bars.return_value = _bars_df(["2024-01-02"])
    loader.incremental_update.return_value = _bars_df(["2024-01-03"])
    db.watchlist_get_all.return_value = pd.DataFrame()
    with patch.object(daily_pipeline, "AkShareMonitor") as mock_monitor_cls, \
         patch.object(daily_pipeline, "SHARED_DATA_DIR", Path("/tmp/x")), \
         patch("daily_pipeline.time.sleep"), patch("daily_pipeline.logger"):
        monitor = MagicMock()
        monitor.should_abort.return_value = (True, "test abort")
        monitor.get_recommended_sleep_multiplier.return_value = 1.0
        mock_monitor_cls.return_value = monitor
        r = daily_pipeline.update_bars(db, loader)
    assert r["total"] == 2


# ===========================================================================
# update_bars: parallel mode
# ===========================================================================
def test_update_bars_parallel_mode():
    db = MagicMock()
    loader = MagicMock()
    stocks_df = pd.DataFrame({"code": ["000001", "000002"]})
    db.get_stock_list.return_value = stocks_df
    db.get_daily_bars.return_value = _bars_df(["2024-01-02"])
    loader.incremental_update.return_value = _bars_df(["2024-01-02"])
    db.watchlist_get_all.return_value = pd.DataFrame()
    with patch.object(daily_pipeline, "SHARED_DATA_DIR", Path("/tmp/x")), \
         patch.object(daily_pipeline, "PARALLEL_WORKERS", 2), \
         patch("daily_pipeline.time.sleep"), patch("daily_pipeline.logger"):
        r = daily_pipeline.update_bars(db, loader)
    assert r["total"] == 2


# ===========================================================================
# health_check: edge with unreachable db
# ===========================================================================
def test_health_check_db_error():
    db = MagicMock()
    db.db_path = "/nonexistent/path/test.db"
    with patch("daily_pipeline.logger"):
        r = daily_pipeline.health_check(db)
    assert "issues" in r
    assert "report" in r


# ===========================================================================
# get_expected_latest_trading_day
# ===========================================================================
def test_get_expected_latest_trading_day_weekday():
    from core.calendar import get_expected_latest_trading_day

    with patch("core.calendar.datetime") as m:
        m.now.return_value = datetime(2026, 6, 22, 16, 0)  # Monday 16:00
        m.side_effect = lambda *a, **kw: datetime(*a, **kw)
        result = get_expected_latest_trading_day()
        assert result == "2026-06-22"  # same day after hours


def test_get_expected_latest_trading_day_monday_before_market():
    from core.calendar import get_expected_latest_trading_day

    with patch("core.calendar.datetime") as m:
        m.now.return_value = datetime(2026, 6, 22, 9, 0)  # Monday before 15:30
        m.side_effect = lambda *a, **kw: datetime(*a, **kw)
        result = get_expected_latest_trading_day()
        assert result == "2026-06-19"  # previous Friday


# ===========================================================================
# 收盘刷新 helper（Task 9）：fetch_limit_pool_records 只抓取不落库
# ===========================================================================


def test_fetch_limit_pool_records_merges_up_and_down():
    """涨停池 + 跌停池合并为 legacy 形状；limit_type 互异。"""
    import tasks.macro as macro

    fake_ak = MagicMock()
    fake_ak.stock_zt_pool_em.return_value = pd.DataFrame(
        [{"代码": "600000", "名称": "浦发银行", "涨跌幅": 10.0, "最新价": 11.0,
          "换手率": 2.0, "连板数": 2, "所属行业": "银行"}]
    )
    fake_ak.stock_zt_pool_dtgc_em.return_value = pd.DataFrame(
        [{"代码": "000001", "名称": "平安银行", "涨跌幅": -10.0, "最新价": 9.0,
          "换手率": 1.0, "所属行业": "银行"}]
    )
    with patch.object(macro, "ak", fake_ak):
        records = macro.fetch_limit_pool_records("2026-07-27")

    fake_ak.stock_zt_pool_em.assert_called_once_with(date="20260727")
    fake_ak.stock_zt_pool_dtgc_em.assert_called_once_with(date="20260727")
    assert len(records) == 2
    by_type = {r["limit_type"]: r for r in records}
    assert by_type["涨停"]["ts_code"] == "600000"
    assert by_type["涨停"]["board_count"] == 2
    assert by_type["跌停"]["ts_code"] == "000001"
    assert by_type["跌停"]["board_count"] is None
    assert all(r["trade_date"] == "2026-07-27" for r in records)


def test_fetch_limit_pool_records_authoritative_empty():
    """两池均权威空 → 返回 []（空池 ≠ 源失败）。"""
    import tasks.macro as macro

    fake_ak = MagicMock()
    fake_ak.stock_zt_pool_em.return_value = pd.DataFrame()
    fake_ak.stock_zt_pool_dtgc_em.return_value = pd.DataFrame()
    with patch.object(macro, "ak", fake_ak):
        assert macro.fetch_limit_pool_records("2026-07-27") == []


def test_fetch_limit_pool_records_propagates_source_error():
    """任一池源异常直接上抛，不得吞掉后当空池处理。"""
    import tasks.macro as macro

    fake_ak = MagicMock()
    fake_ak.stock_zt_pool_em.side_effect = ConnectionError("em down")
    with patch.object(macro, "ak", fake_ak), pytest.raises(ConnectionError):
        macro.fetch_limit_pool_records("2026-07-27")
