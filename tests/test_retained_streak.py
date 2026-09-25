"""连续「保留旧数据」检测的门禁。

规则而非例举：只报**当前仍在持续**的退化、一天只认**最后一次有结论的裁决**、
按**上海运行日**聚合、没有运行的日子跳过而不打断计数。
"""
from __future__ import annotations

import sqlite3

import pytest

from core.retained_streak import (
    RETAINED_STREAK_THRESHOLD,
    RetainedStreak,
    retained_streaks,
)


@pytest.fixture
def conn() -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:")
    connection.execute(
        "CREATE TABLE ingestion_runs ("
        " run_id TEXT PRIMARY KEY, task_name TEXT, status TEXT,"
        " finished_at TEXT, error_kind TEXT)"
    )
    return connection


def _insert(
    conn: sqlite3.Connection,
    task_name: str,
    status: str,
    finished_at: str | None,
    error_kind: str | None = None,
) -> None:
    """造一行审计记录；run_id 取当前行数，保证唯一。"""
    n = conn.execute("SELECT COUNT(*) FROM ingestion_runs").fetchone()[0]
    conn.execute(
        "INSERT INTO ingestion_runs VALUES (?, ?, ?, ?, ?)",
        (f"run-{n}", task_name, status, finished_at, error_kind),
    )
    conn.commit()


def _days(*days: str) -> list[str]:
    """若干运行日，每天固定 10:00（上海 18:00）一条 retained。"""
    return [f"{day}T10:00:00+00:00" for day in days]


def test_default_threshold_is_three():
    """阈值 3 是刻意的，不要顺手改小。

    单次 retained 正常（``core.runner`` 内建网络重试已吸收一次源端瞬断），两次仍可能
    是连续两次抖动；连续 3 个运行日才说明这个任务已连续三天没产出数据。改小会让正常
    抖动变告警（重演「连续 8 次无人处理」的噪音机理），改大则让静默退化更久。
    """
    assert RETAINED_STREAK_THRESHOLD == 3


def test_three_consecutive_days_is_a_streak(conn):
    for stamp in _days("2026-09-21", "2026-09-22", "2026-09-23"):
        _insert(conn, "update_concept_board", "retained", stamp, "network")

    streaks = retained_streaks(conn.cursor())

    assert len(streaks) == 1
    streak = streaks[0]
    assert streak.task_name == "update_concept_board"
    assert streak.days == 3
    assert streak.first_day == "2026-09-21"
    assert streak.last_day == "2026-09-23"
    assert streak.error_kind == "network"


def test_two_consecutive_days_is_below_the_threshold(conn):
    for stamp in _days("2026-09-22", "2026-09-23"):
        _insert(conn, "update_fund_flow", "retained", stamp, "network")

    assert retained_streaks(conn.cursor()) == []


def test_threshold_is_adjustable(conn):
    for stamp in _days("2026-09-22", "2026-09-23"):
        _insert(conn, "update_fund_flow", "retained", stamp, "network")

    assert len(retained_streaks(conn.cursor(), threshold=2)) == 1


def test_non_positive_threshold_is_rejected(conn):
    with pytest.raises(ValueError):
        retained_streaks(conn.cursor(), threshold=0)


def test_no_rows_at_all_is_not_a_streak(conn):
    assert retained_streaks(conn.cursor()) == []


def test_missing_table_raises_so_the_caller_can_decide(conn):
    """表不存在时抛 ``OperationalError``，由调用方决定跳过还是失败。

    ``health_check`` 选择「跳过 + 报告里写明」；把它默默吞成空列表会让「审计表没迁移」
    也看不出来。
    """
    conn.execute("DROP TABLE ingestion_runs")

    with pytest.raises(sqlite3.OperationalError):
        retained_streaks(conn.cursor())


# ── 一天只认最后一次有结论的裁决 ──────────────────────────────────────


def test_latest_verdict_of_the_day_decides_that_day(conn):
    """当天先 retained 后 success：这一天结束时数据已经更新了，不算退化。"""
    _insert(conn, "update_x", "retained", "2026-09-21T05:00:00+00:00", "network")
    _insert(conn, "update_x", "success", "2026-09-21T13:00:00+00:00")

    assert retained_streaks(conn.cursor()) == []


def test_recovery_clears_the_streak(conn):
    """连续 3 天 retained 之后恢复了 → 不再告警。

    否则一旦恢复，旧噪音会天天重报，真正的新退化就被淹没了。
    """
    for stamp in _days("2026-09-21", "2026-09-22", "2026-09-23"):
        _insert(conn, "update_x", "retained", stamp, "network")
    _insert(conn, "update_x", "success", "2026-09-24T10:00:00+00:00")

    assert retained_streaks(conn.cursor()) == []


def test_running_placeholder_does_not_break_the_day(conn):
    """残留的 running 父行不带结论，不能把崩溃日误判成「非 retained」。"""
    _insert(conn, "update_x", "retained", "2026-09-23T05:00:00+00:00", "network")
    _insert(conn, "update_x", "running", "2026-09-23T06:00:00+00:00")

    for stamp in _days("2026-09-21", "2026-09-22"):
        _insert(conn, "update_x", "retained", stamp, "network")

    streaks = retained_streaks(conn.cursor())
    assert len(streaks) == 1
    assert streaks[0].days == 3


@pytest.mark.parametrize("status", ["failed", "degraded", "no_data", "aborted", "success"])
def test_any_other_verdict_breaks_the_streak(conn, status):
    """只有 retained 计入连续；任何别的裁决都打断计数（它是「有结论」的一天）。"""
    _insert(conn, "update_x", status, "2026-09-23T10:00:00+00:00")  # 最新一天
    for stamp in _days("2026-09-21", "2026-09-22"):
        _insert(conn, "update_x", "retained", stamp, "network")

    assert retained_streaks(conn.cursor()) == []


def test_streak_must_be_the_most_recent_days(conn):
    """中间断过的不算连续：retained/retained/failed/retained/retained 只有 2 天。"""
    for day in ("2026-09-16", "2026-09-17"):
        _insert(conn, "update_x", "retained", f"{day}T10:00:00+00:00", "network")
    _insert(conn, "update_x", "failed", "2026-09-18T10:00:00+00:00", "internal")
    for day in ("2026-09-21", "2026-09-22"):
        _insert(conn, "update_x", "retained", f"{day}T10:00:00+00:00", "network")

    assert retained_streaks(conn.cursor()) == []


# ── 按上海运行日聚合 ──────────────────────────────────────────────────


def test_days_without_runs_are_skipped_not_counted(conn):
    """没有运行的日子跳过而不打断计数。

    本管线靠手动触发，「某天没跑」归 ``core.day_coverage`` / ``core.run_state``；
    在这里把它当成「恢复」会让周末与漏跑日把真实退化切成几段。
    """
    for day in ("2026-09-21", "2026-09-23", "2026-09-25"):
        _insert(conn, "update_x", "retained", f"{day}T10:00:00+00:00", "network")

    streaks = retained_streaks(conn.cursor())

    assert len(streaks) == 1
    assert streaks[0].days == 3
    assert (streaks[0].first_day, streaks[0].last_day) == ("2026-09-21", "2026-09-25")


def test_runs_are_grouped_by_shanghai_day(conn):
    """UTC 16:00 之后的运行属于**上海次日**——按 UTC 日切会把连续计数割断。"""
    _insert(conn, "update_x", "retained", "2026-09-22T05:00:00+00:00", "network")
    _insert(conn, "update_x", "retained", "2026-09-23T05:00:00+00:00", "network")
    # UTC 09-24 20:00 = 上海 09-25 04:00
    _insert(conn, "update_x", "retained", "2026-09-24T20:00:00+00:00", "network")

    streaks = retained_streaks(conn.cursor())

    assert len(streaks) == 1
    assert streaks[0].last_day == "2026-09-25"


def test_naive_timestamps_are_read_as_utc(conn):
    """naive 时间戳按 UTC 解释（``core.runner`` 用 ``datetime.now(UTC)`` 落库）。"""
    _insert(conn, "update_x", "retained", "2026-09-22T05:00:00", "network")
    _insert(conn, "update_x", "retained", "2026-09-23T05:00:00", "network")
    _insert(conn, "update_x", "retained", "2026-09-24T20:00:00", "network")

    streaks = retained_streaks(conn.cursor())

    assert len(streaks) == 1
    assert streaks[0].last_day == "2026-09-25"


@pytest.mark.parametrize("bad", [None, "", "not-a-date", "2026-13-45T99:00:00", 12345])
def test_unparseable_timestamps_are_skipped(conn, bad):
    """解析失败的行直接跳过（而不是猜一个日期），因此不能凭它凑出第 3 天。"""
    _insert(conn, "update_x", "retained", "2026-09-21T10:00:00+00:00", "network")
    _insert(conn, "update_x", "retained", "2026-09-22T10:00:00+00:00", "network")
    _insert(conn, "update_x", "retained", bad, "network")

    assert retained_streaks(conn.cursor()) == []


def test_blank_task_name_is_skipped(conn):
    _insert(conn, "update_x", "retained", "2026-09-21T10:00:00+00:00", "network")
    _insert(conn, "update_x", "retained", "2026-09-22T10:00:00+00:00", "network")
    _insert(conn, "", "retained", "2026-09-23T10:00:00+00:00", "network")

    assert retained_streaks(conn.cursor()) == []


# ── 多任务 ────────────────────────────────────────────────────────────


def test_streaks_are_per_task(conn):
    for day in ("2026-09-21", "2026-09-22", "2026-09-23"):
        _insert(conn, "update_sick", "retained", f"{day}T10:00:00+00:00", "network")
        _insert(conn, "update_healthy", "success", f"{day}T11:00:00+00:00")

    streaks = retained_streaks(conn.cursor())

    assert [s.task_name for s in streaks] == ["update_sick"]


def test_streaks_are_sorted_by_days_then_name(conn):
    for day in ("2026-09-21", "2026-09-22", "2026-09-23", "2026-09-24", "2026-09-25"):
        _insert(conn, "update_sick", "retained", f"{day}T10:00:00+00:00", "network")
    for day in ("2026-09-21", "2026-09-22", "2026-09-23"):
        _insert(conn, "update_bad", "retained", f"{day}T10:00:00+00:00", "network")
        _insert(conn, "update_also_bad", "retained", f"{day}T10:00:00+00:00", "network")

    streaks = retained_streaks(conn.cursor())

    assert [(s.task_name, s.days) for s in streaks] == [
        ("update_sick", 5),
        ("update_also_bad", 3),
        ("update_bad", 3),
    ]


# ── 告警文案 ──────────────────────────────────────────────────────────


def test_describe_names_the_task_the_days_and_the_reason():
    """文案要能独立读懂：任务名、连续天数、日期区间、最近 error_kind 缺一不可。

    它是 ``issues`` 的内容，也是收尾通知里唯一能让运维判断「去查什么」的线索。
    """
    text = RetainedStreak(
        "update_concept_board", 5, "2026-09-21", "2026-09-25", "network"
    ).describe()

    assert "update_concept_board" in text
    assert "连续 5 个运行日" in text
    assert "2026-09-21 ~ 2026-09-25" in text
    assert "network" in text


def test_describe_without_error_kind_still_reads_well():
    text = RetainedStreak("update_x", 3, "2026-09-21", "2026-09-23").describe()

    assert "update_x" in text
    assert "error_kind" not in text
