"""Tests for daily_pipeline.py - ProgressTracker and AkShareMonitor."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

import daily_pipeline


@pytest.fixture
def shared_data_dir(tmp_path: Path) -> Path:
    data_dir = tmp_path / "shared_data"
    data_dir.mkdir()
    return data_dir


class TestProgressTracker:
    @pytest.fixture(autouse=True)
    def setup(self, shared_data_dir: Path):
        self.data_dir = shared_data_dir

    def with_patches(self):
        prog_file = self.data_dir / "progress.json"
        return patch.object(daily_pipeline, "SHARED_DATA_DIR", self.data_dir), patch.object(
            daily_pipeline.ProgressTracker, "FILE", prog_file
        )

    def test_save_and_load(self):
        p1, p2 = self.with_patches()
        with p1, p2:
            file = daily_pipeline.ProgressTracker.FILE
            daily_pipeline.ProgressTracker.save(
                task="test_task",
                last_symbol="000001.SZ",
                processed=50,
                total=100,
                failed_queue=["600001.SH"],
            )
            assert file.exists()

            data = daily_pipeline.ProgressTracker.load()
            assert data is not None
            assert data["task"] == "test_task"
            assert data["last_symbol"] == "000001.SZ"
            assert data["processed"] == 50
            assert data["total"] == 100
            assert data["failed_queue"] == ["600001.SH"]

    def test_load_nonexistent(self):
        p1, p2 = self.with_patches()
        with p1, p2:
            result = daily_pipeline.ProgressTracker.load()
            assert result is None

    def test_load_corrupted(self):
        p1, p2 = self.with_patches()
        with p1, p2:
            prog_file = daily_pipeline.ProgressTracker.FILE
            prog_file.parent.mkdir(parents=True, exist_ok=True)
            prog_file.write_text("invalid json{{{")
            result = daily_pipeline.ProgressTracker.load()
            assert result is None

    def test_clear(self):
        p1, p2 = self.with_patches()
        with p1, p2:
            prog_file = daily_pipeline.ProgressTracker.FILE
            prog_file.parent.mkdir(parents=True, exist_ok=True)
            prog_file.write_text('{"done": true}')
            assert prog_file.exists()

            daily_pipeline.ProgressTracker.clear()
            assert not prog_file.exists()

    def test_clear_nonexistent(self):
        p1, p2 = self.with_patches()
        with p1, p2:
            daily_pipeline.ProgressTracker.clear()

    def test_find_resume_index(self):
        stock_codes = ["000001.SZ", "000002.SZ", "000003.SZ", "000004.SZ"]
        idx = daily_pipeline.ProgressTracker.find_resume_index(stock_codes, "000002.SZ")
        assert idx == 2  # resume from index after last_symbol

    def test_find_resume_index_not_found(self):
        stock_codes = ["000001.SZ", "000002.SZ", "000003.SZ"]
        idx = daily_pipeline.ProgressTracker.find_resume_index(stock_codes, "999999.SZ")
        assert idx == 0


class TestAkShareMonitor:
    @pytest.fixture(autouse=True)
    def setup(self, shared_data_dir: Path):
        self.data_dir = shared_data_dir

    def make_monitor(self):
        monitor_file = self.data_dir / "akshare_monitor.json"
        with patch.object(daily_pipeline, "SHARED_DATA_DIR", self.data_dir), \
             patch.object(daily_pipeline.AkShareMonitor, "FILE", monitor_file):
            monitor = daily_pipeline.AkShareMonitor()
            return monitor

    def test_init_without_existing_file(self):
        monitor = self.make_monitor()
        assert monitor.records == []
        assert monitor.current_run_attempts == 0

    def test_record_and_success_rate(self):
        monitor = self.make_monitor()

        monitor.record(True, "000001.SZ")
        assert monitor.current_run_attempts == 1
        assert monitor.current_run_consecutive_failures == 0
        assert monitor.get_success_rate() == 1.0

        monitor.record(False, "000002.SZ")
        assert monitor.current_run_attempts == 2
        assert monitor.current_run_consecutive_failures == 1
        assert monitor.get_success_rate() == 0.5

    def test_get_success_rate_empty(self):
        monitor = self.make_monitor()
        assert monitor.get_success_rate() == 1.0

    def test_recommended_sleep_multiplier(self):
        monitor = self.make_monitor()
        assert monitor.get_recommended_sleep_multiplier() == 1.0

        for _ in range(3):
            monitor.record(True, "000001.SZ")
        assert monitor.get_recommended_sleep_multiplier() == 1.0

    def test_recommended_sleep_multiplier_low_rate(self):
        monitor = self.make_monitor()

        for _ in range(10):
            monitor.record(True, "000001.SZ")
        for _ in range(10):
            monitor.record(False, "000001.SZ")

        rate = monitor.get_success_rate()
        multiplier = monitor.get_recommended_sleep_multiplier()
        if rate < 0.3:
            assert multiplier == 3.0
        elif rate < 0.5:
            assert multiplier == 2.0
        elif rate < 0.8:
            assert multiplier == 1.5
        else:
            assert multiplier == 1.0

    def test_should_abort_no_attempts(self):
        monitor = self.make_monitor()
        should, reason = monitor.should_abort()
        assert not should
        assert reason == ""

    def test_should_abort_consecutive_failures(self):
        monitor = self.make_monitor()
        for _ in range(3):
            monitor.record(False, "000001.SZ")

        should, reason = monitor.should_abort()
        assert should
        assert "连续" in reason

    def test_should_abort_low_rate(self):
        """本次运行 ≥ 20 次请求且成功率 < 20% → 中止。"""
        monitor = self.make_monitor()

        # 20 次请求：17 失败 + 末尾 3 成功（收尾连续失败归零，确保命中的是成功率规则）
        for i, ok in enumerate([False] * 17 + [True] * 3):
            monitor.record(ok, f"{i:06d}")

        should, reason = monitor.should_abort()
        assert should
        assert "成功率" in reason

    def test_should_abort_ignores_stale_history(self):
        """持久化的历史失败记录不影响本次运行的中止判定。"""
        monitor = self.make_monitor()

        # 模拟前几天攒下的大量失败记录
        for i in range(30):
            monitor.records.append({"timestamp": "", "success": False, "symbol": f"{i:06d}"})

        # 本次运行只有少量请求且未连续 3 次失败
        monitor.record(False, "000001.SZ")
        monitor.record(True, "000002.SZ")
        monitor.record(False, "000003.SZ")
        monitor.record(False, "000004.SZ")

        should, reason = monitor.should_abort()
        assert not should
