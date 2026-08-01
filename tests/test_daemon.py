"""scripts/daemon.py 单元测试：退避、flock PID 文件、日志截断、交易日跳过。"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from scripts import daemon


class TestBackoff:
    def test_backoff_grows_exponentially(self):
        assert daemon._backoff_seconds(1) == 30
        assert daemon._backoff_seconds(2) == 60
        assert daemon._backoff_seconds(3) == 120

    def test_backoff_caps_at_one_hour(self):
        assert daemon._backoff_seconds(10) == 3600
        assert daemon._backoff_seconds(100) == 3600


class TestPidfileLock:
    def test_acquire_and_release(self, tmp_path: Path):
        pidfile = tmp_path / "daemon.pid"
        with patch.object(daemon, "PIDFILE", pidfile):
            fd = daemon._try_acquire_pidfile_lock()
            assert fd is not None
            # 持有期间第二次获取必须失败
            assert daemon._try_acquire_pidfile_lock() is None
            fd.close()
            # 释放后可重新获取
            fd2 = daemon._try_acquire_pidfile_lock()
            assert fd2 is not None
            fd2.close()

    def test_is_running_follows_lock(self, tmp_path: Path):
        pidfile = tmp_path / "daemon.pid"
        with patch.object(daemon, "PIDFILE", pidfile):
            assert daemon.is_running() is False
            fd = daemon._try_acquire_pidfile_lock()
            assert daemon.is_running() is True
            fd.close()
            assert daemon.is_running() is False

    def test_is_running_without_file(self, tmp_path: Path):
        with patch.object(daemon, "PIDFILE", tmp_path / "nonexistent.pid"):
            assert daemon.is_running() is False


class TestLogTruncation:
    def test_oversized_log_is_truncated_to_tail(self, tmp_path: Path):
        log_file = tmp_path / "daemon.log"
        log_file.write_bytes(b"x" * (11 * 1024 * 1024))
        with patch.object(daemon, "LOG_FILE", log_file):
            daemon._truncate_log_if_oversized()
        data = log_file.read_bytes()
        assert len(data) <= daemon.LOG_KEEP_BYTES

    def test_small_log_untouched(self, tmp_path: Path):
        log_file = tmp_path / "daemon.log"
        log_file.write_bytes(b"hello\n")
        with patch.object(daemon, "LOG_FILE", log_file):
            daemon._truncate_log_if_oversized()
        assert log_file.read_bytes() == b"hello\n"


class TestTradingDayGate:
    def test_non_trading_day_skips(self):
        with patch("scripts.daemon._is_trading_day_now", return_value=False):
            assert daemon._should_run_pipeline() is False

    def test_trading_day_runs(self):
        with patch("scripts.daemon._is_trading_day_now", return_value=True):
            assert daemon._should_run_pipeline() is True

    def test_calendar_failure_fails_open(self):
        """交易日历不可用时必须放行（fail-open），不能因为判断失败而停跑。"""
        with patch("scripts.daemon._is_trading_day_now", side_effect=Exception("boom")):
            assert daemon._should_run_pipeline() is True
