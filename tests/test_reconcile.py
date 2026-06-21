"""Tests for reconcile_with_akshare.py."""
from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

import reconcile_with_akshare as rwa


# ===========================================================================
# format_eta
# ===========================================================================
class TestFormatEta:
    def test_seconds(self):
        assert rwa.format_eta(30) == "30s"

    def test_minutes(self):
        assert rwa.format_eta(120) == "2.0m"

    def test_hours(self):
        assert rwa.format_eta(7200) == "2.0h"


# ===========================================================================
# no_proxy
# ===========================================================================
class TestNoProxy:
    def test_basic(self):
        os.environ["http_proxy"] = "http://p:8080"
        try:
            with rwa.no_proxy():
                assert "http_proxy" not in os.environ
            assert os.environ["http_proxy"] == "http://p:8080"
        finally:
            os.environ.pop("http_proxy", None)

    def test_no_env(self):
        saved = os.environ.pop("http_proxy", None)
        try:
            with rwa.no_proxy():
                pass
        finally:
            if saved:
                os.environ["http_proxy"] = saved


# ===========================================================================
# ReconcileProgress
# ===========================================================================
class TestReconcileProgress:
    def test_load_no_file(self, tmp_path: Path):
        rwa.ReconcileProgress.FILE = tmp_path / "no.json"
        assert rwa.ReconcileProgress.load() == (0, "")

    def test_load_json_error(self, tmp_path: Path):
        f = tmp_path / "bad.json"
        f.write_text("{bad json")
        rwa.ReconcileProgress.FILE = f
        assert rwa.ReconcileProgress.load() == (0, "")

    def test_save_and_load(self, tmp_path: Path):
        f = tmp_path / "progress.json"
        rwa.ReconcileProgress.FILE = f
        rwa.ReconcileProgress.save(42, "000001.SZ")
        assert rwa.ReconcileProgress.load() == (42, "000001.SZ")

    def test_clear(self, tmp_path: Path):
        f = tmp_path / "progress.json"
        f.write_text("{}")
        rwa.ReconcileProgress.FILE = f
        rwa.ReconcileProgress.clear()
        assert not f.exists()

    def test_clear_no_file(self, tmp_path: Path):
        f = tmp_path / "no.json"
        rwa.ReconcileProgress.FILE = f
        rwa.ReconcileProgress.clear()


# ===========================================================================
# Retry file functions
# ===========================================================================
class TestRetryFunctions:
    def test_load_no_file(self, tmp_path: Path):
        with patch.object(rwa, "RETRY_FILE", tmp_path / "no.txt"):
            assert rwa.load_retry_symbols() == []

    def test_save_and_load(self, tmp_path: Path):
        f = tmp_path / "retry.txt"
        with patch.object(rwa, "RETRY_FILE", f):
            rwa.save_retry_symbol("000001.SZ")
            assert rwa.load_retry_symbols() == ["000001.SZ"]

    def test_dedup(self, tmp_path: Path):
        f = tmp_path / "retry.txt"
        with patch.object(rwa, "RETRY_FILE", f):
            rwa.save_retry_symbol("000001.SZ")
            rwa.save_retry_symbol("000001.SZ")
            assert len(rwa.load_retry_symbols()) == 1

    def test_load_dedup(self, tmp_path: Path):
        f = tmp_path / "retry.txt"
        f.write_text("000001.SZ\n000002.SZ\n000001.SZ\n")
        with patch.object(rwa, "RETRY_FILE", f):
            syms = rwa.load_retry_symbols()
            assert len(syms) == 2

    def test_clear(self, tmp_path: Path):
        f = tmp_path / "retry.txt"
        f.write_text("000001.SZ\n")
        with patch.object(rwa, "RETRY_FILE", f):
            rwa.clear_retry_file()
        assert not f.exists()


# ===========================================================================
# get_akshare_data
# ===========================================================================
def _ak_df(dates: list[str], close: float = 10.0) -> pd.DataFrame:
    """Post-processed AkShare DataFrame (English columns)."""
    return pd.DataFrame({
        "date": dates,
        "open": [9.8] * len(dates),
        "close": [close] * len(dates),
        "high": [10.2] * len(dates),
        "low": [9.7] * len(dates),
        "volume": [1000000] * len(dates),
        "amount": [10000000] * len(dates),
        "turnover_rate": [0.5] * len(dates),
        "pct_change": [0.2] * len(dates),
        "amplitude": [0.15] * len(dates),
    })


class TestGetAkshareData:
    def test_ak_none(self):
        with patch.object(rwa, "ak", None), patch("reconcile_with_akshare.logger"):
            df = rwa.get_akshare_data("000001.SZ", "2024-01-01", "2024-01-10")
        assert df.empty

    def test_success(self):
        with patch.object(rwa, "ak") as ak:
            ak.stock_zh_a_hist.return_value = pd.DataFrame({
                "日期": ["2024-01-02"], "开盘": [10.0], "收盘": [10.5],
                "最高": [11.0], "最低": [9.5], "成交量": [1000000],
                "成交额": [10500000], "换手率": [0.5], "涨跌幅": [2.0],
                "振幅": [1.5],
            })
            with patch("reconcile_with_akshare.no_proxy"):
                df = rwa.get_akshare_data("000001.SZ", "2024-01-01", "2024-01-10")
        assert not df.empty
        assert "date" in df.columns
        assert df.iloc[0]["close"] == 10.5

    def test_empty_result(self):
        with patch.object(rwa, "ak") as ak:
            ak.stock_zh_a_hist.return_value = pd.DataFrame()
            with patch("reconcile_with_akshare.no_proxy"), patch("reconcile_with_akshare.logger"):
                df = rwa.get_akshare_data("000001.SZ", "2024-01-01", "2024-01-10")
        assert df.empty

    def test_retry_then_success(self):
        good_df = pd.DataFrame({
            "日期": ["2024-01-02"], "开盘": [10.0], "收盘": [10.5],
            "最高": [11.0], "最低": [9.5], "成交量": [1000000],
            "成交额": [10500000], "换手率": [0.5], "涨跌幅": [2.0],
            "振幅": [1.5],
        })
        with patch.object(rwa, "ak") as ak:
            ak.stock_zh_a_hist.side_effect = [Exception("timeout"), good_df]
            with patch("reconcile_with_akshare.no_proxy"), \
                 patch("reconcile_with_akshare.time.sleep"), \
                 patch("reconcile_with_akshare.logger"):
                df = rwa.get_akshare_data("000001.SZ", "2024-01-01", "2024-01-10")
        assert not df.empty

    def test_all_retries_fail(self):
        with patch.object(rwa, "ak") as ak:
            ak.stock_zh_a_hist.side_effect = Exception("fail")
            with patch("reconcile_with_akshare.no_proxy"), \
                 patch("reconcile_with_akshare.time.sleep"), \
                 patch("reconcile_with_akshare.logger"):
                df = rwa.get_akshare_data("000001.SZ", "2024-01-01", "2024-01-10")
        assert df.empty


# ===========================================================================
# compare_and_repair
# ===========================================================================
@pytest.fixture
def db_conn(tmp_path: Path):
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE daily_bars (
            ts_code TEXT, trade_date TEXT, open REAL, close REAL, high REAL, low REAL,
            volume REAL, amount REAL, turnover_rate REAL, pct_change REAL, amplitude REAL,
            data_source TEXT, updated_at TEXT
        )
    """)
    yield conn
    conn.close()


def _insert_bars(conn: sqlite3.Connection, symbol: str, dates: list[str],
                 close: float = 10.0, data_source: str | None = None):
    for d in dates:
        conn.execute(
            "INSERT INTO daily_bars VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)",
            (symbol, d, 9.8, close, 10.2, 9.7, 1e6, 1e7, 0.5, 0.2, 0.15, data_source),
        )
    conn.commit()


class TestCompareAndRepair:
    def test_empty_db(self, db_conn: sqlite3.Connection):
        result = rwa.compare_and_repair(db_conn, "000001.SZ")
        assert result["skipped"] == 1

    def test_ak_empty(self, db_conn: sqlite3.Connection):
        _insert_bars(db_conn, "000001.SZ", ["2024-01-02"])
        with patch.object(rwa, "get_akshare_data", return_value=pd.DataFrame()), \
             patch("reconcile_with_akshare.logger"):
            result = rwa.compare_and_repair(db_conn, "000001.SZ")
        assert result["failed"] == 1

    def test_no_date_overlap(self, db_conn: sqlite3.Connection):
        _insert_bars(db_conn, "000001.SZ", ["2024-01-02"])
        ak_df = _ak_df(["2024-02-01"])
        with patch.object(rwa, "get_akshare_data", return_value=ak_df), patch("reconcile_with_akshare.logger"):
            result = rwa.compare_and_repair(db_conn, "000001.SZ")
        assert result["diff"] == 1
        assert result["matched"] == -1  # merged empty → len(merged) - total_diff = 0 - 1

    def test_fast_skip(self, db_conn: sqlite3.Connection):
        dates = [f"2024-01-{d:02d}" for d in range(1, 10)]
        _insert_bars(db_conn, "000001.SZ", dates)
        ak_df = _ak_df(dates)
        with patch.object(rwa, "get_akshare_data", return_value=ak_df), patch("reconcile_with_akshare.logger"):
            result = rwa.compare_and_repair(db_conn, "000001.SZ")
        assert result["diff"] == 0
        assert result["matched"] > 0

    def test_fast_skip_with_backfill(self, db_conn: sqlite3.Connection):
        dates = [f"2024-01-{d:02d}" for d in range(1, 10)]
        _insert_bars(db_conn, "000001.SZ", dates, data_source=None)
        ak_df = _ak_df(dates)
        with patch.object(rwa, "get_akshare_data", return_value=ak_df), patch("reconcile_with_akshare.logger"):
            result = rwa.compare_and_repair(db_conn, "000001.SZ", backfill_source=True)
        assert result["diff"] == 0
        assert result["fixed"] > 0

    def test_dry_run(self, db_conn: sqlite3.Connection):
        dates = [f"2024-01-{d:02d}" for d in range(1, 10)]
        _insert_bars(db_conn, "000001.SZ", dates, close=10.0)
        ak_df = _ak_df(dates, close=9.5)
        with patch.object(rwa, "get_akshare_data", return_value=ak_df), patch("reconcile_with_akshare.logger"):
            result = rwa.compare_and_repair(db_conn, "000001.SZ", dry_run=True)
        assert result["diff"] > 0
        assert result["fixed"] == 0

    def test_full_check_finds_diffs(self, db_conn: sqlite3.Connection):
        dates = [f"2024-01-{d:02d}" for d in range(1, 10)]
        _insert_bars(db_conn, "000001.SZ", dates, close=10.0)
        ak_df = _ak_df(dates, close=9.5)
        with patch.object(rwa, "get_akshare_data", return_value=ak_df), patch("reconcile_with_akshare.logger"):
            result = rwa.compare_and_repair(db_conn, "000001.SZ", full_check=True)
        assert result["diff"] > 0

    def test_smart_repair_update(self, db_conn: sqlite3.Connection):
        dates = [f"2024-01-{d:02d}" for d in range(1, 12)]
        _insert_bars(db_conn, "000001.SZ", dates, close=10.0)
        # Only first 3 dates differ
        ak_data = dict.fromkeys(dates[:3], 9.5)
        ak_data.update(dict.fromkeys(dates[3:], 10.0))
        ak_df = _ak_df(dates)
        ak_df["close"] = [ak_data[d] for d in dates]
        with patch.object(rwa, "get_akshare_data", return_value=ak_df), patch("reconcile_with_akshare.logger"):
            result = rwa.compare_and_repair(db_conn, "000001.SZ", full_check=True, smart_repair=True)
        assert result["fixed"] > 0

    def test_delete_insert(self, db_conn: sqlite3.Connection):
        dates = [f"2024-01-{d:02d}" for d in range(1, 20)]
        _insert_bars(db_conn, "000001.SZ", dates, close=10.0)
        ak_df = _ak_df(dates, close=9.5)
        with patch.object(rwa, "get_akshare_data", return_value=ak_df), patch("reconcile_with_akshare.logger"):
            result = rwa.compare_and_repair(db_conn, "000001.SZ", smart_repair=True)
        assert result["fixed"] > 0

    def test_with_since_filter(self, db_conn: sqlite3.Connection):
        all_dates = [f"2024-01-{d:02d}" for d in range(1, 12)]
        _insert_bars(db_conn, "000001.SZ", all_dates)
        # Only return recent dates from AkShare
        recent_dates = [f"2024-01-{d:02d}" for d in range(8, 12)]
        ak_df = _ak_df(recent_dates)
        with patch.object(rwa, "get_akshare_data", return_value=ak_df), patch("reconcile_with_akshare.logger"):
            result = rwa.compare_and_repair(db_conn, "000001.SZ", since="2024-01-08")
        assert result["total"] == 4  # since filter limits DB rows too
        assert result["matched"] == 4  # all 4 overlap with ak
        assert result["diff"] == 0

    def test_fast_skip_no_backfill_source(self, db_conn: sqlite3.Connection):
        dates = [f"2024-01-{d:02d}" for d in range(1, 10)]
        _insert_bars(db_conn, "000001.SZ", dates, data_source=None)
        ak_df = _ak_df(dates)
        with patch.object(rwa, "get_akshare_data", return_value=ak_df), patch("reconcile_with_akshare.logger"):
            result = rwa.compare_and_repair(db_conn, "000001.SZ", backfill_source=False)
        assert result["fixed"] == 0

    def test_less_than_5_rows(self, db_conn: sqlite3.Connection):
        _insert_bars(db_conn, "000001.SZ", ["2024-01-02", "2024-01-03"])
        ak_df = _ak_df(["2024-01-02", "2024-01-03"])
        with patch.object(rwa, "get_akshare_data", return_value=ak_df), patch("reconcile_with_akshare.logger"):
            result = rwa.compare_and_repair(db_conn, "000001.SZ")
        assert result["diff"] == 0


# ===========================================================================
# update_indicators_for_symbols
# ===========================================================================
class TestUpdateIndicators:
    def test_import_error(self):
        import_paths = {
            "smartmoney_hunter.database": None,
            "smartmoney_hunter.indicators": None,
        }
        with patch.dict("sys.modules", import_paths, clear=False), \
             patch("reconcile_with_akshare.logger"):
            result = rwa.update_indicators_for_symbols("/tmp/nonexistent.db", ["000001.SZ"])
        assert result["success"] == 0
        assert result["failed"] == 0

    def test_normal(self, db_conn: sqlite3.Connection):
        db = MagicMock()
        calc = MagicMock()
        db.get_daily_bars.return_value = _ak_df(
            [f"2024-01-{d:02d}" for d in range(1, 62)], close=10.0
        )
        calc.calculate_all_indicators.return_value = pd.DataFrame({"macd": [0.1]})
        modules = {
            "smartmoney_hunter": MagicMock(),
            "smartmoney_hunter.database": MagicMock(DatabaseManager=MagicMock(return_value=db)),
            "smartmoney_hunter.indicators": MagicMock(IndicatorCalculator=MagicMock(return_value=calc)),
        }
        with patch.dict("sys.modules", modules, clear=False), \
             patch("reconcile_with_akshare.logger"):
            result = rwa.update_indicators_for_symbols("/tmp/fake.db", ["000001.SZ"])
        assert result["success"] == 1

    def test_empty_bars(self):
        db = MagicMock()
        calc = MagicMock()
        db.get_daily_bars.return_value = pd.DataFrame()
        modules = {
            "smartmoney_hunter": MagicMock(),
            "smartmoney_hunter.database": MagicMock(DatabaseManager=MagicMock(return_value=db)),
            "smartmoney_hunter.indicators": MagicMock(IndicatorCalculator=MagicMock(return_value=calc)),
        }
        with patch.dict("sys.modules", modules, clear=False), \
             patch("reconcile_with_akshare.logger"):
            result = rwa.update_indicators_for_symbols("/tmp/fake.db", ["000001.SZ"])
        assert result["success"] == 0

    def test_calc_error(self):
        db = MagicMock()
        calc = MagicMock()
        db.get_daily_bars.return_value = _ak_df(
            [f"2024-01-{d:02d}" for d in range(1, 62)], close=10.0
        )
        calc.calculate_all_indicators.side_effect = ValueError("calc error")
        modules = {
            "smartmoney_hunter": MagicMock(),
            "smartmoney_hunter.database": MagicMock(DatabaseManager=MagicMock(return_value=db)),
            "smartmoney_hunter.indicators": MagicMock(IndicatorCalculator=MagicMock(return_value=calc)),
        }
        with patch.dict("sys.modules", modules, clear=False), \
             patch("reconcile_with_akshare.logger"):
            result = rwa.update_indicators_for_symbols("/tmp/fake.db", ["000001.SZ"])
        assert result["failed"] == 1


# ===========================================================================
# main() / CLI
# ===========================================================================
class TestMain:
    def test_retry_failed_empty(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE daily_bars (ts_code TEXT, trade_date TEXT)")
        conn.commit()
        conn.close()
        with patch.object(sys, "argv", ["reconcile.py", "--retry-failed", "--db-path", str(db_path)]), \
             patch("reconcile_with_akshare.logger"):
            rwa.main()

    def test_retry_failed_with_symbols(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE daily_bars (ts_code TEXT, trade_date TEXT)")
        conn.execute("INSERT INTO daily_bars (ts_code, trade_date) VALUES ('000001.SZ', '2024-01-02')")
        conn.commit()
        conn.close()
        retry_file = tmp_path / "reconcile_retry.txt"
        retry_file.write_text("000001.SZ\n")
        with patch.object(sys, "argv", ["reconcile.py", "--retry-failed", "--db-path", str(db_path)]), \
             patch.object(rwa, "RETRY_FILE", retry_file), \
             patch.object(rwa, "ReconcileProgress"), \
             patch.object(rwa, "compare_and_repair") as mock_car, \
             patch("reconcile_with_akshare.logger"):
            mock_car.return_value = {"total": 1, "matched": 1, "diff": 0, "fixed": 0,
                                      "skipped": 0, "failed": 0, "elapsed": 0.1}
            rwa.main()
            mock_car.assert_called_once()

    def test_symbols_param(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE daily_bars (ts_code TEXT, trade_date TEXT)")
        conn.execute("INSERT INTO daily_bars (ts_code) VALUES ('000001.SZ')")
        conn.execute("INSERT INTO daily_bars (ts_code) VALUES ('600000.SH')")
        conn.commit()
        conn.close()
        with patch.object(sys, "argv", [
            "reconcile.py", "--symbols", "000001.SZ,600000.SH,BADCODE",
            "--db-path", str(db_path),
        ]), \
             patch.object(rwa, "compare_and_repair") as mock_car, \
             patch("reconcile_with_akshare.logger"):
            mock_car.return_value = {"total": 1, "matched": 1, "diff": 0, "fixed": 0,
                                      "skipped": 0, "failed": 0, "elapsed": 0.1}
            rwa.main()
            assert mock_car.call_count == 2

    def test_resume(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE daily_bars (ts_code TEXT, trade_date TEXT)")
        for i in range(5):
            conn.execute("INSERT INTO daily_bars (ts_code) VALUES (?)", (f"{i:06d}.SZ",))
        conn.commit()
        conn.close()
        with patch.object(sys, "argv", [
            "reconcile.py", "--resume", "--db-path", str(db_path),
        ]), \
             patch.object(rwa, "ReconcileProgress") as mock_prog, \
             patch.object(rwa, "compare_and_repair") as mock_car, \
             patch("reconcile_with_akshare.logger"):
            mock_prog.load.return_value = (2, "000002.SZ")
            mock_car.return_value = {"total": 1, "matched": 1, "diff": 0, "fixed": 0,
                                      "skipped": 0, "failed": 0, "elapsed": 0.1}
            rwa.main()

    def test_limit(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE daily_bars (ts_code TEXT, trade_date TEXT)")
        for i in range(10):
            conn.execute("INSERT INTO daily_bars (ts_code) VALUES (?)", (f"{i:06d}.SZ",))
        conn.commit()
        conn.close()
        with patch.object(sys, "argv", [
            "reconcile.py", "--limit", "3", "--db-path", str(db_path),
        ]), \
             patch.object(rwa, "compare_and_repair") as mock_car, \
             patch("reconcile_with_akshare.logger"):
            mock_car.return_value = {"total": 1, "matched": 1, "diff": 0, "fixed": 0,
                                      "skipped": 0, "failed": 0, "elapsed": 0.1}
            rwa.main()
            assert mock_car.call_count == 3

    def test_update_indicators_flag(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE daily_bars (ts_code TEXT, trade_date TEXT)")
        conn.execute("INSERT INTO daily_bars (ts_code, trade_date) VALUES ('000001.SZ', '2024-01-02')")
        conn.commit()
        conn.close()
        with patch.object(sys, "argv", [
            "reconcile.py", "--update-indicators", "--db-path", str(db_path),
        ]), \
             patch.object(rwa, "compare_and_repair") as mock_car, \
             patch.object(rwa, "update_indicators_for_symbols") as mock_ind, \
             patch("reconcile_with_akshare.logger"):
            mock_car.return_value = {"total": 1, "matched": 0, "diff": 1, "fixed": 1,
                                      "skipped": 0, "failed": 0, "elapsed": 0.1}
            rwa.main()
            mock_ind.assert_called_once()
