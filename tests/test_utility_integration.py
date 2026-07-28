"""Integration tests for tasks.utility.py — health_check and retry_failed.

Uses tmp_path for real SQLite databases to bypass the is_real_db_path guard.
Tests verify:
- health_check: early exits, happy path, low coverage, high null rate
- retry_failed: empty queue, all succeed, some fail
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from core.task_result import TaskStatus, normalize_task_result
from tasks.utility import health_check, retry_failed

# ===========================================================================
# Helpers
# ===========================================================================


def _create_minimal_db(db_path: str, coverage_pct: float = 100.0) -> None:
    """Create a real SQLite database with all tables health_check queries."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("CREATE TABLE stock_list (ts_code TEXT)")
    cursor.execute(
        "CREATE TABLE daily_bars (ts_code TEXT, trade_date TEXT, close REAL)"
    )
    cursor.execute(
        "CREATE TABLE indicators (ts_code TEXT, trade_date TEXT, "
        "macd_hist REAL, rsi6 REAL)"
    )
    for t in [
        "fund_flow",
        "fundamentals",
        "chip_distribution",
        "historical_valuation",
        "margin_trading",
        "dragon_tiger",
        "block_trade",
        "sector_fund_flow",
    ]:
        cursor.execute(f"CREATE TABLE {t} (ts_code TEXT, trade_date TEXT)")
    cursor.execute(
        "CREATE TABLE shareholder_count (ts_code TEXT, report_date TEXT)"
    )

    total_stocks = 100
    for i in range(total_stocks):
        cursor.execute(
            "INSERT INTO stock_list VALUES (?)", [f"{i:06d}"]
        )

    bars_count = int(total_stocks * coverage_pct / 100)
    for i in range(bars_count):
        cursor.execute(
            "INSERT INTO daily_bars VALUES (?, ?, ?)",
            [f"{i:06d}", "2026-07-17", 10.0],
        )

    for i in range(bars_count):
        macd = 0.5 if i < bars_count * 0.9 else None
        rsi = 50.0 if i < bars_count * 0.9 else None
        cursor.execute(
            "INSERT INTO indicators VALUES (?, ?, ?, ?)",
            [f"{i:06d}", "2026-07-17", macd, rsi],
        )

    cursor.execute(
        "INSERT INTO fund_flow VALUES ('000001', '2026-07-17')"
    )
    for t in [
        "fundamentals",
        "chip_distribution",
        "historical_valuation",
        "margin_trading",
        "dragon_tiger",
        "block_trade",
        "sector_fund_flow",
    ]:
        cursor.execute(f"INSERT INTO {t} VALUES ('000001', '2026-07-17')")
    cursor.execute(
        "INSERT INTO shareholder_count VALUES ('000001', '2026-07-17')"
    )

    conn.commit()
    conn.close()


@pytest.fixture
def real_db(tmp_path: Path):
    """DatabaseInterface mock backed by a real SQLite file (coverage=100%)."""
    db_path = tmp_path / "quant_core.db"
    _create_minimal_db(str(db_path))
    db = MagicMock()
    db.db_path = str(db_path)
    return db


# ===========================================================================
# health_check
# ===========================================================================


class TestHealthCheck:
    """Integration tests for the health_check function."""

    def test_no_real_db_path(self):
        """is_real_db_path returns False → early return with issue."""
        db = MagicMock()
        result = health_check(db)
        assert result["issues"]
        assert any("不是有效路径" in i for i in result["issues"])
        assert result.get("coverage_pct") is None

    def test_db_is_directory(self, tmp_path: Path):
        """db_path points to a directory → sqlite3.OperationalError."""
        dir_path = tmp_path / "is_a_dir"
        dir_path.mkdir()
        db = MagicMock()
        db.db_path = str(dir_path)
        result = health_check(db)
        assert any("无法打开数据库" in i for i in result["issues"])

    def test_happy_path(self, real_db):
        """Full DB with 100% coverage → no issues."""
        with patch(
            "tasks.utility.get_expected_latest_trading_day",
            return_value="2026-07-17",
        ):
            result = health_check(real_db)
        assert result["issues"] == []
        assert result["coverage_pct"] == 100.0
        assert result["latest_bar"] == "2026-07-17"
        assert result["db_size_mb"] > 0

    def test_low_coverage(self, tmp_path: Path):
        """Only 10% of stocks have bars → coverage issue flagged."""
        db_path = tmp_path / "low_cov.db"
        _create_minimal_db(str(db_path), coverage_pct=10.0)
        db = MagicMock()
        db.db_path = str(db_path)
        result = health_check(db)
        assert any("日线覆盖率过低" in i for i in result["issues"])
        assert result["coverage_pct"] < 80

    def test_high_null_rate(self, tmp_path: Path):
        """80% of indicators are NULL → null rate issue flagged."""
        db_path = tmp_path / "high_null.db"
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE stock_list (ts_code TEXT)")
        cursor.execute(
            "CREATE TABLE daily_bars (ts_code TEXT, trade_date TEXT, close REAL)"
        )
        cursor.execute(
            "CREATE TABLE indicators (ts_code TEXT, trade_date TEXT, "
            "macd_hist REAL, rsi6 REAL)"
        )
        for t in [
            "fund_flow",
            "fundamentals",
            "chip_distribution",
            "historical_valuation",
            "margin_trading",
            "dragon_tiger",
            "block_trade",
            "sector_fund_flow",
        ]:
            cursor.execute(f"CREATE TABLE {t} (ts_code TEXT, trade_date TEXT)")
        cursor.execute(
            "CREATE TABLE shareholder_count (ts_code TEXT, report_date TEXT)"
        )

        for i in range(100):
            cursor.execute(
                "INSERT INTO stock_list VALUES (?)", [f"{i:06d}"]
            )
            cursor.execute(
                "INSERT INTO daily_bars VALUES (?, ?, ?)",
                [f"{i:06d}", "2026-07-17", 10.0],
            )
            macd = 0.5 if i < 20 else None
            rsi = 50.0 if i < 20 else None
            cursor.execute(
                "INSERT INTO indicators VALUES (?, ?, ?, ?)",
                [f"{i:06d}", "2026-07-17", macd, rsi],
            )

        cursor.execute(
            "INSERT INTO fund_flow VALUES ('000001', '2026-07-17')"
        )
        for t in [
            "fundamentals",
            "chip_distribution",
            "historical_valuation",
            "margin_trading",
            "dragon_tiger",
            "block_trade",
            "sector_fund_flow",
        ]:
            cursor.execute(f"INSERT INTO {t} VALUES ('000001', '2026-07-17')")
        cursor.execute(
            "INSERT INTO shareholder_count VALUES ('000001', '2026-07-17')"
        )
        conn.commit()
        conn.close()

        db = MagicMock()
        db.db_path = str(db_path)
        result = health_check(db)
        assert any("技术指标空值率过高" in i for i in result["issues"])


# ===========================================================================
# retry_failed
# ===========================================================================


class TestRetryFailed:
    """Tests for the retry_failed function with mocked ProgressTracker."""

    def test_retry_failed_no_progress_file(self):
        """ProgressTracker.load() returns None → early return."""
        db = MagicMock()
        loader = MagicMock()
        with patch(
            "tasks.utility.ProgressTracker.load", return_value=None
        ), patch("tasks.utility.ProgressTracker.clear") as mock_clear:
            result = retry_failed(db, loader)
        assert result["status"] == "no_data"
        assert result["reason"] == "retry queue empty"
        assert result["saved"] == 0
        assert result["attempted"] == result["total"] == 0
        assert normalize_task_result("retry_failed", result).status is TaskStatus.NO_DATA
        mock_clear.assert_called_once()

    def test_retry_failed_empty_queue(self):
        """Failed_queue is empty list → early return."""
        db = MagicMock()
        loader = MagicMock()
        with patch(
            "tasks.utility.ProgressTracker.load",
            return_value={"task": "retry", "failed_queue": []},
        ), patch("tasks.utility.ProgressTracker.clear") as mock_clear:
            result = retry_failed(db, loader)
        assert result["status"] == "no_data"
        assert result["reason"] == "retry queue empty"
        assert result["saved"] == 0
        assert result["attempted"] == result["total"] == 0
        assert normalize_task_result("retry_failed", result).status is TaskStatus.NO_DATA
        mock_clear.assert_called_once()

    def test_retry_failed_all_succeed(self):
        """All retried symbols succeed → ProgressTracker cleared."""
        db = MagicMock()
        loader = MagicMock()
        with patch(
            "tasks.utility.ProgressTracker.load",
            return_value={
                "task": "retry",
                "failed_queue": ["000001", "000002", "000003"],
            },
        ), patch("tasks.utility.ProgressTracker.clear") as mock_clear, patch(
            "tasks.utility.ProgressTracker.save"
        ) as mock_save, patch(
            "tasks.utility._update_single_bar", return_value="success"
        ):
            result = retry_failed(db, loader)
        assert result["status"] == "success"
        assert result["saved"] == result["success"] == 3
        assert result["attempted"] == result["total"] == 3
        assert normalize_task_result("retry_failed", result).status is TaskStatus.SUCCESS
        mock_clear.assert_called_once()
        mock_save.assert_not_called()

    def test_retry_failed_some_fail(self):
        """Mixed results → ProgressTracker.save() with remaining failures."""
        db = MagicMock()
        loader = MagicMock()

        def fake_update(db_, loader_, symbol):
            return "success" if symbol in ("000001", "000003") else "error"

        with patch(
            "tasks.utility.ProgressTracker.load",
            return_value={
                "task": "retry",
                "failed_queue": ["000001", "000002", "000003", "000004"]
            },
        ), patch("tasks.utility.ProgressTracker.clear") as mock_clear, patch(
            "tasks.utility.ProgressTracker.save"
        ) as mock_save, patch(
            "tasks.utility._update_single_bar", side_effect=fake_update
        ):
            result = retry_failed(db, loader)
        assert result["status"] == "degraded"
        assert result["saved"] == result["success"] == 2
        assert result["attempted"] == result["total"] == 4
        assert result["error"] == "2 failures"
        assert normalize_task_result("retry_failed", result).status is TaskStatus.DEGRADED
        mock_save.assert_called_once()
        mock_clear.assert_not_called()

    @pytest.mark.parametrize("progress_task", [None, "update_bars"])
    def test_retry_failed_preserves_scan_checkpoint(self, progress_task):
        """scan/legacy checkpoint 不能由 retry_failed 消费或清理。"""
        data = {
            "failed_queue": ["000001.SZ", "000002.SZ"],
            "last_symbol": "000002.SZ",
            "processed": 2,
        }
        if progress_task is not None:
            data["task"] = progress_task

        with patch(
            "tasks.utility.ProgressTracker.load", return_value=data
        ), patch(
            "tasks.utility.ProgressTracker.clear"
        ) as mock_clear, patch(
            "tasks.utility.ProgressTracker.save"
        ) as mock_save, patch(
            "tasks.utility._update_single_bar"
        ) as mock_update:
            result = retry_failed(MagicMock(), MagicMock())

        assert result["status"] == "no_data"
        assert result["reason"]
        mock_update.assert_not_called()
        mock_save.assert_not_called()
        mock_clear.assert_not_called()

    def test_retry_failed_deduplicates_and_records_attempted(self):
        """重复 symbol 仅尝试一次，checkpoint processed 写实际尝试数。"""
        with patch(
            "tasks.utility.ProgressTracker.load",
            return_value={
                "task": "retry",
                "failed_queue": ["000001.SZ", "000001.SZ", "000002.SZ"],
            },
        ), patch(
            "tasks.utility.ProgressTracker.clear"
        ) as mock_clear, patch(
            "tasks.utility.ProgressTracker.save"
        ) as mock_save, patch(
            "tasks.utility._update_single_bar",
            side_effect=["failed", "success"],
        ) as mock_update:
            result = retry_failed(MagicMock(), MagicMock())

        assert [call.args[2] for call in mock_update.call_args_list] == [
            "000001.SZ",
            "000002.SZ",
        ]
        assert result["attempted"] == 2
        mock_save.assert_called_once_with(
            task="retry",
            last_symbol="000002.SZ",
            processed=2,
            total=2,
            failed_queue=["000001.SZ"],
        )
        mock_clear.assert_not_called()
