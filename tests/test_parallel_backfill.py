"""Tests for parallel_backfill.py."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import parallel_backfill
from parallel_backfill import copy_schema, prepare_worker_db


# ===========================================================================
# copy_schema
# ===========================================================================
class TestCopySchema:
    def test_basic(self, tmp_path: Path):
        src = tmp_path / "src.db"
        dst = tmp_path / "dst.db"
        conn = sqlite3.connect(str(src))
        conn.execute("CREATE TABLE stock_list (code TEXT)")
        conn.execute("CREATE TABLE daily_bars (ts_code TEXT, trade_date TEXT)")
        conn.execute("INSERT INTO stock_list (code) VALUES ('000001.SZ')")
        conn.execute("INSERT INTO daily_bars VALUES ('000001.SZ', '2024-01-02')")
        conn.commit()
        conn.close()
        copy_schema(src, dst)
        assert dst.exists()
        conn2 = sqlite3.connect(str(dst))
        rows = conn2.execute("SELECT * FROM stock_list").fetchall()
        assert len(rows) == 0  # data cleared
        conn2.close()

    def test_overwrites_existing(self, tmp_path: Path):
        src = tmp_path / "src.db"
        dst = tmp_path / "dst.db"
        dst.write_text("junk")
        conn = sqlite3.connect(str(src))
        conn.execute("CREATE TABLE t (c TEXT)")
        conn.commit()
        conn.close()
        copy_schema(src, dst)
        conn = sqlite3.connect(str(dst))
        conn.execute("SELECT * FROM t")
        conn.close()


# ===========================================================================
# prepare_worker_db
# ===========================================================================
class TestPrepareWorkerDb:
    def test_creates_worker_db(self, tmp_path: Path):
        master = tmp_path / "quant_core.db"
        conn = sqlite3.connect(str(master))
        conn.execute("CREATE TABLE stock_list (code TEXT)")
        conn.execute("INSERT INTO stock_list (code) VALUES ('000001.SZ')")
        conn.execute("INSERT INTO stock_list (code) VALUES ('600000.SH')")
        conn.commit()
        conn.close()
        with patch.object(parallel_backfill, "MASTER_DB", master):
            worker_db = prepare_worker_db(1, ["000001.SZ"])
            conn2 = sqlite3.connect(str(worker_db))
            rows = conn2.execute("SELECT * FROM stock_list").fetchall()
            assert len(rows) == 1
            assert rows[0][0] == "000001.SZ"
            conn2.close()
            worker_db.unlink()

    def test_multiple_stocks(self, tmp_path: Path):
        master = tmp_path / "quant_core.db"
        conn = sqlite3.connect(str(master))
        conn.execute("CREATE TABLE stock_list (code TEXT)")
        for i in range(10):
            conn.execute("INSERT INTO stock_list (code) VALUES (?)", (f"{i:06d}.SZ",))
        conn.commit()
        conn.close()
        stocks = [f"{i:06d}.SZ" for i in range(3, 8)]
        with patch.object(parallel_backfill, "MASTER_DB", master):
            worker_db = prepare_worker_db(2, stocks)
            conn2 = sqlite3.connect(str(worker_db))
            rows = conn2.execute("SELECT * FROM stock_list ORDER BY code").fetchall()
            assert len(rows) == 5
            conn2.close()
            worker_db.unlink()


# ===========================================================================
# get_remaining_stocks
# ===========================================================================
class TestGetRemainingStocks:
    def test_all_remaining(self, tmp_path: Path):
        master = tmp_path / "quant_core.db"
        conn = sqlite3.connect(str(master))
        conn.execute("CREATE TABLE stock_list (code TEXT)")
        conn.execute("CREATE TABLE daily_bars (ts_code TEXT, trade_date TEXT)")
        for i in range(5):
            conn.execute("INSERT INTO stock_list (code) VALUES (?)", (f"{i:06d}.SZ",))
        conn.commit()
        conn.close()
        with patch.object(parallel_backfill, "MASTER_DB", master):
            remaining = parallel_backfill.get_remaining_stocks()
        assert len(remaining) == 5

    def test_some_done(self, tmp_path: Path):
        master = tmp_path / "quant_core.db"
        conn = sqlite3.connect(str(master))
        conn.execute("CREATE TABLE stock_list (code TEXT)")
        conn.execute("CREATE TABLE daily_bars (ts_code TEXT, trade_date TEXT)")
        for i in range(5):
            conn.execute("INSERT INTO stock_list (code) VALUES (?)", (f"{i:06d}.SZ",))
        conn.execute("INSERT INTO daily_bars (ts_code) VALUES ('000003.SZ')")
        conn.execute("INSERT INTO daily_bars (ts_code) VALUES ('000004.SZ')")
        conn.commit()
        conn.close()
        with patch.object(parallel_backfill, "MASTER_DB", master):
            remaining = parallel_backfill.get_remaining_stocks()
        assert len(remaining) == 3


# ===========================================================================
# merge_worker_dbs
# ===========================================================================
class TestMergeWorkerDbs:
    def test_merge_one(self, tmp_path: Path):
        master = tmp_path / "quant_core.db"
        conn = sqlite3.connect(str(master))
        conn.execute("CREATE TABLE stock_list (code TEXT)")
        conn.execute("CREATE TABLE daily_bars (ts_code TEXT, trade_date TEXT, open REAL, high REAL, low REAL, close REAL, volume REAL, amount REAL, turnover_rate REAL, pct_change REAL, amplitude REAL, data_source TEXT, updated_at TEXT)")
        conn.commit()
        conn.close()
        worker = tmp_path / "quant_core_worker0.db"
        conn2 = sqlite3.connect(str(worker))
        conn2.execute("CREATE TABLE daily_bars (ts_code TEXT, trade_date TEXT, open REAL, high REAL, low REAL, close REAL, volume REAL, amount REAL, turnover_rate REAL, pct_change REAL, amplitude REAL, data_source TEXT, updated_at TEXT)")
        conn2.execute("INSERT INTO daily_bars (ts_code, trade_date, close) VALUES ('000001.SZ', '2024-01-02', 10.5)")
        conn2.commit()
        conn2.close()
        with patch.object(parallel_backfill, "MASTER_DB", master):
            parallel_backfill.merge_worker_dbs([0])
        conn3 = sqlite3.connect(str(master))
        rows = conn3.execute("SELECT * FROM daily_bars").fetchall()
        assert len(rows) == 1
        conn3.close()

    def test_merge_skip_missing(self, tmp_path: Path):
        master = tmp_path / "quant_core.db"
        conn = sqlite3.connect(str(master))
        conn.execute("CREATE TABLE stock_list (code TEXT)")
        conn.execute("CREATE TABLE daily_bars (ts_code TEXT, trade_date TEXT, open REAL, high REAL, low REAL, close REAL, volume REAL, amount REAL, turnover_rate REAL, pct_change REAL, amplitude REAL, data_source TEXT, updated_at TEXT)")
        conn.commit()
        conn.close()
        with patch.object(parallel_backfill, "MASTER_DB", master):
            parallel_backfill.merge_worker_dbs([99])
        conn2 = sqlite3.connect(str(master))
        rows = conn2.execute("SELECT * FROM daily_bars").fetchall()
        assert len(rows) == 0
        conn2.close()


# ===========================================================================
# cleanup
# ===========================================================================
class TestCleanup:
    def test_default(self, tmp_path: Path):
        for wid in range(2):
            (tmp_path / f"quant_core_worker{wid}.db").write_text("data")
            (tmp_path / f"progress_worker{wid}.json").write_text("{}")
        with patch.object(parallel_backfill, "MASTER_DB", tmp_path / "quant_core.db"), \
             patch.object(parallel_backfill, "PIPELINE_DIR", tmp_path):
            parallel_backfill.cleanup([0, 1])
        assert not (tmp_path / "quant_core_worker0.db").exists()
        assert not (tmp_path / "progress_worker0.json").exists()
        assert not (tmp_path / "quant_core_worker1.db").exists()
        assert not (tmp_path / "progress_worker1.json").exists()

    def test_skip_missing(self, tmp_path: Path):
        with patch.object(parallel_backfill, "MASTER_DB", tmp_path / "quant_core.db"), \
             patch.object(parallel_backfill, "PIPELINE_DIR", tmp_path):
            parallel_backfill.cleanup([0])


# ===========================================================================
# run_worker (subprocess mock)
# ===========================================================================
class TestRunWorker:
    def test_with_existing_progress(self, tmp_path: Path):
        master = tmp_path / "quant_core.db"
        conn = sqlite3.connect(str(master))
        conn.execute("CREATE TABLE stock_list (code TEXT)")
        conn.commit()
        conn.close()
        (tmp_path / "progress_worker0.json").write_text('{"done": []}')
        with patch.object(parallel_backfill, "MASTER_DB", master), \
             patch.object(parallel_backfill, "PIPELINE_DIR", tmp_path), \
             patch("subprocess.Popen") as mock_popen:
            proc = MagicMock()
            proc.returncode = 0
            mock_popen.return_value = proc
            parallel_backfill.run_worker(0, ["000001.SZ"], 1)
        assert not (tmp_path / "progress_worker0.json").exists()

    def test_failure(self, tmp_path: Path):
        master = tmp_path / "quant_core.db"
        conn = sqlite3.connect(str(master))
        conn.execute("CREATE TABLE stock_list (code TEXT)")
        conn.commit()
        conn.close()
        with patch.object(parallel_backfill, "MASTER_DB", master), \
             patch.object(parallel_backfill, "PIPELINE_DIR", tmp_path), \
             patch("subprocess.Popen") as mock_popen:
            proc = MagicMock()
            proc.returncode = 1
            mock_popen.return_value = proc
            parallel_backfill.run_worker(1, ["000002.SZ"], 2)
            mock_popen.assert_called_once()


# ===========================================================================
# main()
# ===========================================================================
class TestMain:
    def test_missing_master_db(self, tmp_path: Path):
        with patch.object(parallel_backfill, "MASTER_DB", tmp_path / "nonexistent.db"), \
             patch.object(sys, "argv", ["parallel_backfill.py"]), \
             pytest.raises(SystemExit):
            parallel_backfill.main()

    def test_merge_only_no_files(self, tmp_path: Path):
        master = tmp_path / "quant_core.db"
        conn = sqlite3.connect(str(master))
        conn.execute("CREATE TABLE stock_list (code TEXT)")
        conn.execute("CREATE TABLE daily_bars (ts_code TEXT, trade_date TEXT, open REAL, high REAL, low REAL, close REAL, volume REAL, amount REAL, turnover_rate REAL, pct_change REAL, amplitude REAL, data_source TEXT, updated_at TEXT)")
        conn.commit()
        conn.close()
        with patch.object(parallel_backfill, "MASTER_DB", master), \
             patch.object(sys, "argv", ["parallel_backfill.py", "--merge-only"]), patch("parallel_backfill.print"):
            parallel_backfill.main()

    def test_merge_only_with_data(self, tmp_path: Path):
        master = tmp_path / "quant_core.db"
        conn = sqlite3.connect(str(master))
        conn.execute("CREATE TABLE stock_list (code TEXT)")
        conn.execute("CREATE TABLE daily_bars (ts_code TEXT, trade_date TEXT, open REAL, high REAL, low REAL, close REAL, volume REAL, amount REAL, turnover_rate REAL, pct_change REAL, amplitude REAL, data_source TEXT, updated_at TEXT)")
        conn.commit()
        conn.close()
        worker = tmp_path / "quant_core_worker0.db"
        conn2 = sqlite3.connect(str(worker))
        conn2.execute("CREATE TABLE daily_bars (ts_code TEXT, trade_date TEXT, open REAL, high REAL, low REAL, close REAL, volume REAL, amount REAL, turnover_rate REAL, pct_change REAL, amplitude REAL, data_source TEXT, updated_at TEXT)")
        conn2.execute("INSERT INTO daily_bars (ts_code, trade_date, close) VALUES ('000001.SZ', '2024-01-02', 10.5)")
        conn2.commit()
        conn2.close()
        with patch.object(parallel_backfill, "MASTER_DB", master), \
             patch.object(sys, "argv", ["parallel_backfill.py", "--merge-only", "--no-cleanup"]), \
             patch("parallel_backfill.print"):
            parallel_backfill.main()
        conn3 = sqlite3.connect(str(master))
        rows = conn3.execute("SELECT * FROM daily_bars").fetchall()
        assert len(rows) == 1
        conn3.close()

    def test_all_done(self, tmp_path: Path):
        master = tmp_path / "quant_core.db"
        conn = sqlite3.connect(str(master))
        conn.execute("CREATE TABLE stock_list (code TEXT)")
        conn.execute("CREATE TABLE daily_bars (ts_code TEXT, trade_date TEXT)")
        for i in range(3):
            conn.execute("INSERT INTO stock_list (code) VALUES (?)", (f"{i:06d}.SZ",))
            conn.execute("INSERT INTO daily_bars (ts_code, trade_date) VALUES (?, '2024-01-02')", (f"{i:06d}.SZ",))
        conn.commit()
        conn.close()
        with patch.object(parallel_backfill, "MASTER_DB", master), \
             patch.object(sys, "argv", ["parallel_backfill.py"]), \
             patch("parallel_backfill.print"):
            parallel_backfill.main()

    def test_normal_run_with_cleanup(self, tmp_path: Path):
        master = tmp_path / "quant_core.db"
        conn = sqlite3.connect(str(master))
        conn.execute("CREATE TABLE stock_list (code TEXT)")
        conn.execute("CREATE TABLE daily_bars (ts_code TEXT, trade_date TEXT)")
        conn.execute("INSERT INTO stock_list (code) VALUES ('000001.SZ')")
        conn.commit()
        conn.close()
        with patch.object(parallel_backfill, "MASTER_DB", master), \
             patch.object(sys, "argv", ["parallel_backfill.py"]), \
             patch.object(parallel_backfill, "Process") as mock_process, \
             patch("parallel_backfill.print"):
            proc = MagicMock()
            mock_process.return_value = proc
            parallel_backfill.main()
            mock_process.assert_called()


class TestMainRemaining:
    """Cover the last few main() branches."""
    def test_merge_only_with_cleanup(self, tmp_path: Path):
        master = tmp_path / "quant_core.db"
        conn = sqlite3.connect(str(master))
        conn.execute("CREATE TABLE stock_list (code TEXT)")
        conn.execute("CREATE TABLE daily_bars (ts_code TEXT, trade_date TEXT, open REAL, high REAL, low REAL, close REAL, volume REAL, amount REAL, turnover_rate REAL, pct_change REAL, amplitude REAL, data_source TEXT, updated_at TEXT)")
        conn.commit()
        conn.close()
        worker = tmp_path / "quant_core_worker0.db"
        conn2 = sqlite3.connect(str(worker))
        conn2.execute("CREATE TABLE daily_bars (ts_code TEXT, trade_date TEXT, open REAL, high REAL, low REAL, close REAL, volume REAL, amount REAL, turnover_rate REAL, pct_change REAL, amplitude REAL, data_source TEXT, updated_at TEXT)")
        conn2.execute("INSERT INTO daily_bars (ts_code, trade_date, close) VALUES ('000001.SZ', '2024-01-02', 10.5)")
        conn2.commit()
        conn2.close()
        with patch.object(parallel_backfill, "MASTER_DB", master), \
             patch.object(sys, "argv", ["parallel_backfill.py", "--merge-only"]), \
             patch("parallel_backfill.print"):
            parallel_backfill.main()
        assert not worker.exists()

    def test_indicator_after_merge(self, tmp_path: Path):
        master = tmp_path / "quant_core.db"
        conn = sqlite3.connect(str(master))
        conn.execute("CREATE TABLE stock_list (code TEXT)")
        conn.execute("CREATE TABLE daily_bars (ts_code TEXT, trade_date TEXT)")
        conn.execute("INSERT INTO stock_list (code) VALUES ('000001.SZ')")
        conn.commit()
        conn.close()
        with patch.object(parallel_backfill, "MASTER_DB", master), \
             patch.object(sys, "argv", ["parallel_backfill.py", "--no-cleanup"]), \
             patch.object(parallel_backfill, "Process") as mock_process, \
             patch("parallel_backfill.print"):
            proc = MagicMock()
            mock_process.return_value = proc
            parallel_backfill.main()

