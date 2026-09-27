"""整日缺席回补入口的门禁。

三件事：日期解析/筛选语义；「仍缺才回补」的幂等行为；以及**分类覆盖**——
可回补、部分可回补与不可回补的探针表必须正好拼成 ``day_coverage.PROBE_TABLES``，
不允许出现「没说过能不能补」的表。
"""
from __future__ import annotations

import sqlite3

import pytest

import core.backfill as backfill
import core.day_coverage as day_coverage
import daily_pipeline
from core.day_coverage import PROBE_TABLES
from core.known_gaps import declared_missing_days

# 钉住日历：``trading_days_between`` 只读本地缓存，CI 上不存在，不钉就永远返回空。
CALENDAR = [
    "2026-08-03",
    "2026-08-04",
    "2026-08-19",
    "2026-08-21",
    "2026-09-02",
    "2026-09-14",
    "2026-09-16",
]


@pytest.fixture
def conn(monkeypatch) -> sqlite3.Connection:
    """内存库：建出全部探针表（默认探针集是真实表名，不建就一律「不可判定」）。"""
    monkeypatch.setattr(
        day_coverage,
        "trading_days_between",
        lambda s, e: [d for d in CALENDAR if s <= d <= e],
    )
    connection = sqlite3.connect(":memory:")
    for table, column in PROBE_TABLES:
        connection.execute(f"CREATE TABLE {table} ({column} TEXT)")
    return connection


def _fill(conn: sqlite3.Connection, table: str, day: str) -> None:
    conn.execute(f"INSERT INTO {table} (trade_date) VALUES (?)", (day,))
    conn.commit()


# ── parse_days ────────────────────────────────────────────────────────


def test_parse_days_accepts_commas_and_spaces():
    days, rejected = backfill.parse_days("2026-09-16, 2026-08-03  2026-09-16")
    assert days == ["2026-08-03", "2026-09-16"]  # 去重 + 旧到新
    assert rejected == []


def test_parse_days_rejects_malformed_items():
    days, rejected = backfill.parse_days("2026/08/03,tomorrow")
    assert days == []
    assert [item for item, _ in rejected] == ["2026/08/03", "tomorrow"]


# ── is_still_absent ───────────────────────────────────────────────────


def test_still_absent_when_every_probe_table_is_empty(conn):
    assert backfill.is_still_absent(conn.cursor(), "2026-08-03") is True


def test_not_absent_when_a_single_probe_table_has_rows(conn):
    """部分表有行 = 部分运行，不是整日缺席（不得被当成回补目标）。"""
    _fill(conn, "index_daily", "2026-08-03")
    assert backfill.is_still_absent(conn.cursor(), "2026-08-03") is False


def test_not_absent_for_a_day_outside_the_calendar(conn):
    """非交易日「无法判定」，不得当成缺（否则会去回补一个不存在的交易日）。"""
    assert backfill.is_still_absent(conn.cursor(), "2026-08-15") is False


# ── resolve_backfill_days ─────────────────────────────────────────────


def test_explicit_spec_keeps_only_still_absent_days(conn):
    _fill(conn, "block_trade", "2026-08-04")
    plan = backfill.resolve_backfill_days(conn.cursor(), "2026-08-04,2026-08-03")

    assert plan.source == "explicit"
    assert plan.days == ("2026-08-03",)
    assert dict(plan.skipped)["2026-08-04"] == "该日已有数据（或不是交易日），无需回补"


def test_explicit_spec_reports_malformed_items_as_skipped(conn):
    plan = backfill.resolve_backfill_days(conn.cursor(), "2026/08/03")

    assert plan.days == ()
    assert "不是合法的 YYYY-MM-DD 日期" in dict(plan.skipped)["2026/08/03"]


def test_no_spec_falls_back_to_the_declared_missing_days(conn):
    plan = backfill.resolve_backfill_days(conn.cursor(), None)

    assert plan.source == "declared"
    assert plan.days == tuple(sorted(declared_missing_days()))


def test_declared_days_already_filled_are_skipped(conn):
    _fill(conn, "index_daily", "2026-09-16")
    plan = backfill.resolve_backfill_days(conn.cursor(), "")

    assert "2026-09-16" not in plan.days
    assert dict(plan.skipped)["2026-09-16"] == "该日已有数据（或不是交易日），无需回补"


def test_blank_spec_is_treated_as_auto_discovery(conn):
    """``--backfill-days`` 不带值（empty string）必须等价于「自动取登记册」。"""
    assert backfill.resolve_backfill_days(conn.cursor(), "").source == "declared"
    assert backfill.resolve_backfill_days(conn.cursor(), "   ").source == "declared"


# ── 分类覆盖门禁 ──────────────────────────────────────────────────────


def test_classification_covers_every_probe_table_exactly_once():
    """每张探针表都必须被明确归类为可回补/部分可回补/不可回补——不允许沉默跳过。

    探针集增删时这里先红，逼着同时更新分类（否则新表会悄悄落在两不管地带，
    而「为什么这张补不回来」将无从回答）。
    """
    backfillable = {table for table, _ in backfill.BACKFILLABLE_TABLES}
    partial = {table for table, _, _ in backfill.PARTIAL_BACKFILLABLE_TABLES}
    not_backfillable = {table for table, _ in backfill.NOT_BACKFILLABLE_TABLES}
    probes = {table for table, _ in PROBE_TABLES}

    assert backfillable | partial | not_backfillable == probes
    assert backfillable & partial == set()
    assert backfillable & not_backfillable == set()
    assert partial & not_backfillable == set()


def test_backfillable_tables_map_to_real_backfill_tasks():
    """分类表里写的任务名必须真的在回补注册表里，避免文档与实现分叉。

    部分可回补的表仍要尝试（仅受源端窗口限制），所以其任务也必须在注册表里。
    """
    declared = {task for _, task in backfill.BACKFILLABLE_TABLES}
    declared |= {task for _, task, _ in backfill.PARTIAL_BACKFILLABLE_TABLES}
    assert declared == set(daily_pipeline._BACKFILL_TASK_CALLABLES)


def test_partial_backfillable_entries_all_carry_a_reason():
    assert all(reason.strip() for _, _, reason in backfill.PARTIAL_BACKFILLABLE_TABLES)
    assert len({table for table, _, _ in backfill.PARTIAL_BACKFILLABLE_TABLES}) == len(
        backfill.PARTIAL_BACKFILLABLE_TABLES
    )


def test_not_backfillable_entries_all_carry_a_reason():
    assert all(reason.strip() for _, reason in backfill.NOT_BACKFILLABLE_TABLES)
    assert len({table for table, _ in backfill.NOT_BACKFILLABLE_TABLES}) == len(
        backfill.NOT_BACKFILLABLE_TABLES
    )
