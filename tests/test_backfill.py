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

# backfill 引用了 day_coverage.date_column，这里同把 re-export 钉住，
# 避免「幂等门悄悄换了判定来源」这类分叉。
assert backfill.date_column is day_coverage.date_column

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


# ── resolve_backfill_targets（--backfill-table）────────────────────


def test_targets_single_table_x_declared_days(conn):
    """只给表名：那些表 × 登记册缺失日，source=declared。"""
    plan = backfill.resolve_backfill_targets(conn.cursor(), "limit_up_down")

    assert plan.source == "declared"
    assert plan.targets == tuple(
        ("limit_up_down", day) for day in sorted(declared_missing_days())
    )


def test_targets_single_day_x_all_backfillable_tables(conn):
    """只给日期：全部可回补表 × 那些日期，source=explicit。"""
    plan = backfill.resolve_backfill_targets(conn.cursor(), "2026-09-14")

    assert plan.source == "explicit"
    tables = {table for table, _ in plan.targets}
    assert tables == {
        table for table, _ in backfill.BACKFILLABLE_TABLES
    } | {table for table, _, _ in backfill.PARTIAL_BACKFILLABLE_TABLES}
    assert {day for _, day in plan.targets} == {"2026-09-14"}


def test_targets_mixed_tables_and_days(conn):
    """表名与日期可混写；幂等门在 (表, 日) 粒度：该表当日有行才跳过。"""
    _fill(conn, "index_daily", "2026-08-03")
    plan = backfill.resolve_backfill_targets(
        conn.cursor(), "limit_up_down index_daily 2026-08-03"
    )

    assert plan.source == "explicit"
    # index_daily 当日已有行 → 整对跳过；limit_up_down 同日仍要补
    assert ("limit_up_down", "2026-08-03") in plan.targets
    assert (("index_daily", "2026-08-03"), "该表当日已有数据，无需回补") in plan.skipped


def test_targets_unknown_token_is_skipped_not_fatal(conn):
    plan = backfill.resolve_backfill_targets(conn.cursor(), "limit_up_down 2026/99/99")

    assert ("2026/99/99", "既不是回补表名，也不是合法的 YYYY-MM-DD 日期") in plan.skipped
    # 形似日期的非法项也算「给了日期」：不得静默落到 declared 分支
    # （否则会突然回补全表 × 登记册日）。全非法 ⇒ 空计划、无操作。
    assert plan.source == "explicit"
    assert plan.targets == ()


def test_targets_bare_and_none_mean_declared_x_all_tables(conn):
    for spec in ("", "   ", None):
        plan = backfill.resolve_backfill_targets(conn.cursor(), spec)
        assert plan.source == "declared"
        tables = {table for table, _ in plan.targets}
        assert tables == {
            table for table, _ in backfill.BACKFILLABLE_TABLES
        } | {table for table, _, _ in backfill.PARTIAL_BACKFILLABLE_TABLES}


def test_targets_probeday_granularity_not_whole_day(conn):
    """与 ``--backfill-days`` 的关键差异：整天有行不影响单表缺口。

    08-03 除 limit_up_down 外全部有行——整天级判定会跳过该日；
    表格级判定只跳过已有行的表，limit_up_down 仍是回补目标。
    """
    for table, _ in PROBE_TABLES:
        if table != "limit_up_down":
            _fill(conn, table, "2026-08-03")
    plan = backfill.resolve_backfill_targets(conn.cursor(), "2026-08-03")

    assert plan.targets == (("limit_up_down", "2026-08-03"),)


def test_targets_unknown_date_hint_not_mistaken_for_day_intent(conn):
    """纯表名输入里混入非日期的乱写 token：仍走 declared（没给日期）。"""
    plan = backfill.resolve_backfill_targets(conn.cursor(), "limit_up_down oops")

    assert plan.source == "declared"
    assert {day for _, day in plan.targets} == set(declared_missing_days())


def test_table_has_rows_missing_table_is_not_treated_as_filled(conn):
    """表不存在 = 无法判定，不得当成「已有行」而把可补的表悄悄跳过。"""
    bare = sqlite3.connect(":memory:")
    try:
        assert backfill.table_has_rows(bare.cursor(), "index_daily", "2026-08-03") is None
    finally:
        bare.close()


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


def test_backfill_task_by_table_matches_backfill_callables():
    """``--backfill-table`` 的表名清单必须与任务注册表互相印证：

    表名集合 = 可回补 + 部分可回补；每个表名都能在 ``_BACKFILL_TASK_CALLABLES``
    里找到执行体。两边任一分叉这里先红。
    """
    by_table = backfill.backfill_task_by_table()
    assert set(by_table) == {
        table for table, _ in backfill.BACKFILLABLE_TABLES
    } | {table for table, _, _ in backfill.PARTIAL_BACKFILLABLE_TABLES}
    assert set(by_table.values()) <= set(daily_pipeline._BACKFILL_TASK_CALLABLES)
    assert by_table["limit_up_down"] == "update_limit_up_down"


def test_probe_tables_all_have_date_column_lookup():
    """``date_column`` 必须认识全部探针表——``--backfill-table`` 的幂等门靠它查行。"""
    for table, column in PROBE_TABLES:
        assert backfill.date_column(table) == column
    assert backfill.date_column("no_such_table") is None


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
