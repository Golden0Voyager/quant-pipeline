"""统一上海市场时钟模块测试。"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

import pytest

from core.calendar import get_expected_latest_trading_day
from core.market_time import (
    PHASE_POST_CLOSE,
    PHASE_PRE_OPEN,
    PHASE_SESSION,
    PHASE_SETTLEMENT,
    SHANGHAI,
    has_post_close_completion,
    is_fetch_window,
    market_phase,
    shanghai_now,
    shanghai_today,
)


def _sh(hour: int, minute: int = 0) -> datetime:
    """2026-07-31（周五，交易日）上海时间。"""
    return datetime(2026, 7, 31, hour, minute, tzinfo=SHANGHAI)


class TestMarketPhase:
    @pytest.mark.parametrize(
        "now,expected",
        [
            (_sh(0, 0), PHASE_PRE_OPEN),
            (_sh(9, 14), PHASE_PRE_OPEN),
            (_sh(9, 15), PHASE_SESSION),  # 集合竞价开始即视为盘中
            (_sh(14, 59), PHASE_SESSION),
            (_sh(15, 0), PHASE_SETTLEMENT),
            (_sh(15, 59), PHASE_SETTLEMENT),
            (_sh(16, 0), PHASE_POST_CLOSE),
            (_sh(23, 59), PHASE_POST_CLOSE),
        ],
    )
    def test_boundaries(self, now: datetime, expected: str):
        assert market_phase(now) == expected

    def test_non_shanghai_tz_converted(self):
        """本机 +0700 时钟 08:40 = 上海 09:40 → 盘中（旧实现的漏防窗口）。"""
        local = datetime(2026, 7, 31, 8, 40, tzinfo=ZoneInfo("Asia/Bangkok"))
        assert market_phase(local) == PHASE_SESSION

    def test_naive_input_interpreted_as_shanghai(self):
        assert market_phase(datetime(2026, 7, 31, 10, 0)) == PHASE_SESSION

    def test_fetch_window(self):
        assert is_fetch_window(_sh(8, 0)) is True
        assert is_fetch_window(_sh(10, 0)) is False
        assert is_fetch_window(_sh(15, 30)) is False
        assert is_fetch_window(_sh(16, 0)) is True


class TestShanghaiClock:
    def test_shanghai_now_is_aware(self):
        now = shanghai_now()
        assert now.tzinfo is not None
        assert now.utcoffset().total_seconds() == 8 * 3600

    def test_shanghai_today_format(self):
        assert len(shanghai_today()) == 10


class TestExpectedLatestTradingDayShanghai:
    """expected 翻转与放行窗口统一在上海 16:00。"""

    def test_before_close_settled_is_previous_day(self):
        assert get_expected_latest_trading_day(_sh(15, 30)) == "2026-07-30"

    def test_after_close_settled_is_today(self):
        assert get_expected_latest_trading_day(_sh(16, 0)) == "2026-07-31"

    def test_machine_tz_does_not_shift_result(self):
        # 本机 +0700 的 15:30 = 上海 16:30 → 期望应为今天
        local = datetime(2026, 7, 31, 15, 30, tzinfo=ZoneInfo("Asia/Bangkok"))
        assert get_expected_latest_trading_day(local.astimezone(SHANGHAI)) == "2026-07-31"


class TestHasPostCloseCompletion:
    TASK = "update_fundamentals"
    TARGET = "2026-07-31"  # 收盘定型 16:00 SH = 08:00 UTC

    def _make_db(self, tmp_path, finished_at: str | None, status: str = "success") -> str:
        db_path = str(tmp_path / "audit.db")
        conn = sqlite3.connect(db_path)
        conn.execute(
            "CREATE TABLE ingestion_runs ("
            " run_id TEXT PRIMARY KEY, task_name TEXT, status TEXT, finished_at TEXT)"
        )
        if finished_at is not None:
            conn.execute(
                "INSERT INTO ingestion_runs VALUES ('r1', ?, ?, ?)",
                (self.TASK, status, finished_at),
            )
        conn.commit()
        conn.close()
        return db_path

    def test_post_close_success_counts(self, tmp_path):
        db = self._make_db(tmp_path, "2026-07-31T08:05:00+00:00")
        assert has_post_close_completion(db, self.TASK, self.TARGET) is True

    def test_intraday_finish_does_not_count(self, tmp_path):
        # 完成于上海 14:00（06:00 UTC）→ 盘中写入，不算收盘后完成
        db = self._make_db(tmp_path, "2026-07-31T06:00:00+00:00")
        assert has_post_close_completion(db, self.TASK, self.TARGET) is False

    def test_failed_run_does_not_count(self, tmp_path):
        db = self._make_db(tmp_path, "2026-07-31T09:00:00+00:00", status="failed")
        assert has_post_close_completion(db, self.TASK, self.TARGET) is False

    def test_naive_utc_timestamp_supported(self, tmp_path):
        # provider 兜底路径写 naive utcnow().isoformat()
        db = self._make_db(tmp_path, "2026-07-31T08:05:00")
        assert has_post_close_completion(db, self.TASK, self.TARGET) is True

    def test_no_data_status_counts(self, tmp_path):
        db = self._make_db(tmp_path, "2026-07-31T08:05:00+00:00", status="no_data")
        assert has_post_close_completion(db, self.TASK, self.TARGET) is True

    def test_missing_table_fails_open(self, tmp_path):
        db_path = str(tmp_path / "empty.db")
        sqlite3.connect(db_path).close()
        assert has_post_close_completion(db_path, self.TASK, self.TARGET) is False

    def test_unopenable_db_fails_open(self, tmp_path):
        assert (
            has_post_close_completion(str(tmp_path / "no/such/dir.db"), self.TASK, self.TARGET)
            is False
        )

    def test_other_task_not_matched(self, tmp_path):
        db = self._make_db(tmp_path, "2026-07-31T08:05:00+00:00")
        assert has_post_close_completion(db, "update_market_snapshot", self.TARGET) is False

    def test_bad_target_date_fails_open(self, tmp_path):
        db = self._make_db(tmp_path, "2026-07-31T08:05:00+00:00")
        assert has_post_close_completion(db, self.TASK, "not-a-date") is False

    def test_utc_helper_consistency(self):
        # 目标日 16:00 上海 == 08:00 UTC（防止未来有人改错时区换算）
        close_local = datetime(2026, 7, 31, 16, 0, tzinfo=SHANGHAI)
        assert close_local.astimezone(UTC).hour == 8
