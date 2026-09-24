"""core.run_state 运行状态标记。

这些断言对应 2026-09-17 那次部分运行：31 个任务全 success、无任何告警，
只留下至今仍在的数据空洞。核心断言是「下一轮必须能发现上一轮没结束」。
"""
from __future__ import annotations

from unittest.mock import MagicMock

from core.run_state import (
    COMPLETE_PREFIX,
    IN_PROGRESS_PREFIX,
    RUN_STATE_TASK,
    describe_run_state,
    mark_run_completed,
    mark_run_started,
    read_run_state,
)


class _RunStateDb:
    """承载 task_runs 语义的假库。

    用 MagicMock 作底（与仓库其它测试一致：``__getattr__ -> Any`` 使其在 mypy
    下仍可传入 ``db: DatabaseInterface`` 形参），只把两个方法接到真实 dict 上。
    """

    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.db = MagicMock()
        self.db.record_task_run.side_effect = self._record
        self.db.get_last_task_run.side_effect = self.store.get

    def _record(self, task_name: str, run_date: str) -> None:
        self.store[task_name] = run_date


def test_no_marker_on_fresh_database():
    fake = _RunStateDb()
    assert read_run_state(fake.db) is None
    assert "首次运行" in describe_run_state(fake.db)


def test_mark_started_writes_in_progress_and_returns_none_when_clean():
    fake = _RunStateDb()
    assert mark_run_started(fake.db, "2026-09-17") is None
    assert fake.store[RUN_STATE_TASK] == f"{IN_PROGRESS_PREFIX}2026-09-17"


def test_abandoned_run_from_a_previous_day_is_reported():
    """回归：上一轮开始后没有完成记录 → 下一轮必须报告（09-17 场景）。"""
    fake = _RunStateDb()
    mark_run_started(fake.db, "2026-09-17")  # 之后进程被杀，没有 mark_run_completed

    assert mark_run_started(fake.db, "2026-09-18") == "2026-09-17"
    assert fake.store[RUN_STATE_TASK] == f"{IN_PROGRESS_PREFIX}2026-09-18"


def test_restarting_on_the_same_day_is_not_reported_as_abandoned():
    """同日重跑（人工重试）不算中断，否则会误报。"""
    fake = _RunStateDb()
    mark_run_started(fake.db, "2026-09-18")
    assert mark_run_started(fake.db, "2026-09-18") is None


def test_completed_run_from_a_previous_day_is_not_reported():
    fake = _RunStateDb()
    mark_run_started(fake.db, "2026-09-17")
    mark_run_completed(fake.db, "2026-09-17")
    assert fake.store[RUN_STATE_TASK] == f"{COMPLETE_PREFIX}2026-09-17"

    assert mark_run_started(fake.db, "2026-09-18") is None


def test_completion_overwrites_in_progress():
    fake = _RunStateDb()
    mark_run_started(fake.db, "2026-09-18")
    mark_run_completed(fake.db, "2026-09-18")
    assert read_run_state(fake.db) == ("complete", "2026-09-18")


def test_unrecognised_value_is_ignored():
    """旧库/人工写入的异物不应让巡检崩溃。"""
    fake = _RunStateDb()
    fake.store[RUN_STATE_TASK] = "garbage"
    assert read_run_state(fake.db) is None

    fake.store[RUN_STATE_TASK] = IN_PROGRESS_PREFIX
    assert read_run_state(fake.db) is None  # 前缀后无日期


def test_describe_run_state_labels_both_states():
    fake = _RunStateDb()
    mark_run_started(fake.db, "2026-09-17")
    assert "进行中" in describe_run_state(fake.db)
    mark_run_completed(fake.db, "2026-09-17")
    assert "已完整结束" in describe_run_state(fake.db)
