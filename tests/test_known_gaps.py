"""core.known_gaps 登记册的不变量。

登记册是「显式声明」而不是「静默豁免」，所以它必须有门禁：条目的增删都应该是
一次有意识的决定，而不是为了让报告变绿而悄悄删掉几行。
"""
from __future__ import annotations

from core.known_gaps import (
    AUDIT_ERA_START,
    KNOWN_GAPS,
    describe_known_gap,
    is_known_gap,
    known_gap_dates,
)


def test_no_duplicate_entries():
    """同一 (表, 列, 日期) 不应重复登记。"""
    keys = [(g.table, g.column, g.date) for g in KNOWN_GAPS]
    assert len(keys) == len(set(keys))


def test_every_gap_has_a_cause():
    """必须写明成因，否则登记册退化成一份无解释的豁免名单。"""
    for gap in KNOWN_GAPS:
        assert gap.cause.strip(), f"{gap.table}.{gap.column}@{gap.date} 缺少成因"


def test_all_gaps_are_inside_the_audit_era():
    """审计期之前没有逐任务轨迹、无法归因，不应出现在登记册里。"""
    for gap in KNOWN_GAPS:
        assert gap.date >= AUDIT_ERA_START, f"{gap.date} 早于审计期起点 {AUDIT_ERA_START}"


def test_declared_dividend_yield_gaps_are_pinned():
    """钉住已声明的 6 个股息率空洞。

    若有人为了消除告警而删除条目，这里会变红——删条目必须先说服这个测试，
    也就是必须给出「这些洞已不再是洞」的证据（例如真的回补成功）。
    """
    assert known_gap_dates("fundamentals", "dividend_yield") == frozenset({
        "2026-08-10",
        "2026-08-12",
        "2026-08-20",
        "2026-08-25",
        "2026-09-03",
        "2026-09-17",
    })


def test_is_known_gap_matches_only_declared_triples():
    assert is_known_gap("fundamentals", "dividend_yield", "2026-09-17") is True
    # 同日期但不同列 → 未声明
    assert is_known_gap("fundamentals", "roe", "2026-09-17") is False
    # 同列但日期不在册 → 未声明（这是 health_check 用来判定「新增空洞」的依据）
    assert is_known_gap("fundamentals", "dividend_yield", "2026-09-22") is False


def test_known_gap_dates_are_scoped_to_the_requested_column():
    assert known_gap_dates("daily_bars", "turnover_rate") == frozenset()


def test_describe_known_gap_returns_cause_or_none():
    cause = describe_known_gap("fundamentals", "dividend_yield", "2026-09-17")
    assert cause is not None and "部分运行" in cause
    assert describe_known_gap("fundamentals", "dividend_yield", "2026-09-22") is None
