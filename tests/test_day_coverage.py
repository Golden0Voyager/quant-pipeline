"""整日缺席检测的门禁。

规则而非例举：探针表必须与任务注册表一致（都来自 ``TRADING_DAY`` 任务）、判定只在
「**全部**探针表皆空」时成立、探针表不可用时不得报出假空洞。
"""
from __future__ import annotations

import sqlite3

import pytest

import core.day_coverage as day_coverage
from core.day_coverage import PROBE_TABLES, missing_trading_days, scan_window
from core.known_gaps import AUDIT_ERA_START
from core.task_registry import TASK_REGISTRY, Cadence

# 单元测试用的小探针集：判定逻辑与具体表无关，不必拉起生产表结构。
PROBES = (("fundamentals", "trade_date"), ("fund_flow", "trade_date"))


@pytest.fixture
def conn() -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:")
    for table, column in PROBES:
        connection.execute(f"CREATE TABLE {table} ({column} TEXT)")
    return connection


def _insert(conn: sqlite3.Connection, table: str, column: str, days: list[str]) -> None:
    conn.executemany(f"INSERT INTO {table} ({column}) VALUES (?)", [(d,) for d in days])
    conn.commit()


def test_scan_window_excludes_the_latest_trading_day(monkeypatch):
    """最新交易日本身归新鲜度巡检。

    把它算进窗口，管线当天还没跑完时就会天天报假空洞——而假空洞会让人开始无视
    整条巡检，这是本仓库已经吃过一次的教训（08 月连续 8 次告警无人处理）。
    """
    monkeypatch.setattr(
        day_coverage,
        "trading_days_between",
        lambda s, e: ["2026-08-03", "2026-08-04", "2026-08-05"],
    )
    assert scan_window(AUDIT_ERA_START, "2026-08-05") == ("2026-08-03", "2026-08-04")


def test_scan_window_is_none_when_fewer_than_two_trading_days(monkeypatch):
    """不足两个交易日时无从判断，调用方应跳过并说明（而不是猜）。"""
    monkeypatch.setattr(day_coverage, "trading_days_between", lambda s, e: ["2026-08-03"])
    assert scan_window(AUDIT_ERA_START, "2026-08-03") is None

    monkeypatch.setattr(day_coverage, "trading_days_between", lambda s, e: [])
    assert scan_window(AUDIT_ERA_START, "2026-08-03") is None


def test_all_probes_empty_is_a_missing_day(conn, monkeypatch):
    monkeypatch.setattr(
        day_coverage, "trading_days_between", lambda s, e: ["2026-08-03", "2026-08-04"]
    )
    _insert(conn, "fundamentals", "trade_date", ["2026-08-04"])

    assert missing_trading_days(
        conn.cursor(), start="2026-08-03", end="2026-08-04", probe_tables=PROBES
    ) == ["2026-08-03"]


def test_one_probe_having_rows_is_not_a_missing_day(conn, monkeypatch):
    """部分表有行 = 部分运行，不是整日缺席（那条归 ``run_state`` 与股息率巡检）。"""
    monkeypatch.setattr(day_coverage, "trading_days_between", lambda s, e: ["2026-08-03"])
    _insert(conn, "fund_flow", "trade_date", ["2026-08-03"])

    assert (
        missing_trading_days(
            conn.cursor(), start="2026-08-03", end="2026-08-04", probe_tables=PROBES
        )
        == []
    )


def test_unusable_probes_produce_no_false_holes(conn, monkeypatch):
    """探针表不存在（测试库 / 未迁移库）时必须跳过。

    若此时把所有交易日都报成空洞，真空洞会被这批噪音淹没——那正是这条巡检要避免的
    事，所以宁可漏报。
    """
    monkeypatch.setattr(
        day_coverage, "trading_days_between", lambda s, e: ["2026-08-03", "2026-08-04"]
    )

    assert (
        missing_trading_days(
            conn.cursor(),
            start="2026-08-03",
            end="2026-08-04",
            probe_tables=(("no_such_table", "trade_date"),),
        )
        == []
    )


def test_missing_days_are_outside_the_scan_window(conn, monkeypatch):
    """窗口外的日期不得进入结果（探针表在窗口外有行也不能掩盖窗口内）。"""
    monkeypatch.setattr(
        day_coverage, "trading_days_between", lambda s, e: ["2026-08-04", "2026-08-05"]
    )
    _insert(conn, "fundamentals", "trade_date", ["2026-08-03"])  # 窗口外

    assert missing_trading_days(
        conn.cursor(), start="2026-08-04", end="2026-08-05", probe_tables=PROBES
    ) == ["2026-08-04", "2026-08-05"]


def test_probe_tables_are_all_written_by_trading_day_tasks():
    """探针表必须都来自 ``TRADING_DAY`` 任务。

    否则「每个交易日都该有行」这个前提就不成立，巡检会开始报假空洞。这条门禁让探针集
    与任务注册表绑在一起：改了 cadence 或表声明，这里会先红。
    """
    written = {
        table
        for spec in TASK_REGISTRY
        if spec.cadence == Cadence.TRADING_DAY
        for table in spec.tables
    }
    assert {table for table, _ in PROBE_TABLES} <= written


def test_probe_tables_are_unique_and_name_a_date_column():
    tables = [table for table, _ in PROBE_TABLES]
    assert len(tables) == len(set(tables))
    assert all(column for _, column in PROBE_TABLES)
