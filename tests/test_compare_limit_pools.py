"""scripts/compare_limit_pools.py 纯函数的门禁。

脚本唯一职责是离线对比东财与同花顺涨跌停池；网络侧只做编排（拿两家的行、
打印），可钉住的是纯函数：板价归类与集合差异摘要。分桶语义直接决定
docs 里「口径差异 vs 数据分歧」的结论，分叉了结论就跟着错。
"""
from __future__ import annotations

from scripts.compare_limit_pools import (
    _PCT_BUCKETS,
    _by_code,
    _codes,
    board_price_for,
    classify_pct_gap,
    describe_set_diff,
)

_EM_ROW = {
    "trade_date": "2026-09-15",
    "ts_code": "300001",
    "name": "某股",
    "pct_change": 20.0,
    "close_price": 12.0,
    "turnover_rate": 5.0,
    "limit_type": "涨停",
    "board_count": 1,
    "industry": "行业",
    "data_source": "akshare",
}


def _ht_row(code: str, pct: float | None) -> dict:
    row = dict(_EM_ROW)
    row["ts_code"] = code
    row["pct_change"] = pct
    row["data_source"] = "hithink"
    return dict(row)


# ── board_price_for ──────────────────────────────────────────────────


def test_board_price_matches_nearest_band():
    assert board_price_for(10.0) == 10.0
    assert board_price_for(-10.0) == -10.0
    assert board_price_for(19.98) == 20.0
    assert board_price_for(4.98) == 5.0
    assert board_price_for(30.0) == 30.0


# ── classify_pct_gap ─────────────────────────────────────────────────


def test_both_at_board_and_equal_is_both_board():
    assert classify_pct_gap(20.0, 20.01) == "both_board"


def test_both_at_board_but_different_values_is_field_mismatch():
    """都贴板但数值互异 = 数据分歧（不是口径差异）。

    两点都落在 ±20 档容差内（偏差 0.25 ≤ 0.3），但互差 0.5 > 0.3。
    """
    assert classify_pct_gap(19.75, 20.25) == "field_mismatch"


def test_em_at_board_ht_off_board_is_criterion_gap():
    """只有东财贴板：同花顺口径疑似含破板/未封住。"""
    assert classify_pct_gap(20.0, 15.0) == "em_only_board"


def test_ht_at_board_em_off_board_is_criterion_gap():
    assert classify_pct_gap(15.0, 10.0) == "ht_only_board"


def test_neither_at_board_is_both_off_board():
    assert classify_pct_gap(15.0, 14.0) == "both_off_board"


def test_missing_pct_is_unknown():
    assert classify_pct_gap(None, 20.0) == "unknown"
    assert classify_pct_gap(20.0, None) == "unknown"


# ── describe_set_diff ────────────────────────────────────────────────


def test_set_diff_counts_and_only_lists():
    em = [_EM_ROW, _ht_row("300002", 10.0)]
    ht = [_ht_row("300001", 20.0), _ht_row("300003", -10.0)]
    summary = describe_set_diff(em, ht, "涨停")

    assert summary["em_count"] == 2
    assert summary["ht_count"] == 2
    assert summary["shared"] == 1
    assert summary["only_em"] == ["300002"]
    assert summary["only_ht"] == ["300003"]
    assert summary["jaccard"] == 0.333  # 1 / 3


def test_set_diff_buckets_shared_codes():
    em = [
        _ht_row("300001", 20.0),  # both_board（与 HT 相同）
        _ht_row("300002", 10.0),  # em_only_board（HT 17.0 离 ±20 档远，不贴板）
        _ht_row("300003", -9.8),  # field_mismatch（HT -10.2，都贴 -10 档但互异 >0.3）
    ]
    ht = [
        _ht_row("300001", 20.0),
        _ht_row("300002", 17.0),
        _ht_row("300003", -10.2),
    ]
    summary = describe_set_diff(em, ht, "跌停")

    buckets = summary["pct_gap_buckets"]
    assert buckets["both_board"] == ["300001"]
    assert buckets["em_only_board"] == ["300002"]
    assert buckets["field_mismatch"] == ["300003"]


def test_set_diff_empty_inputs_yield_neutral_summary():
    summary = describe_set_diff([], [], "涨停")
    assert summary["em_count"] == 0
    assert summary["shared"] == 0
    assert summary["jaccard"] == 1.0
    assert summary["pct_gap_buckets"] == {}


def test_every_bucket_is_listed_by_the_report_order():
    """报告打印顺序的桶清单必须覆盖 classify 的全部可能取值。"""
    assert set(_PCT_BUCKETS) == {
        "both_board",
        "field_mismatch",
        "em_only_board",
        "ht_only_board",
        "both_off_board",
        "unknown",
    }


# ── 行聚合 helper ────────────────────────────────────────────────────


def test_codes_skips_empty_and_strips():
    rows = [{"ts_code": " 300001 "}, {"ts_code": ""}, {}, {"ts_code": None}]
    assert _codes(rows) == {"300001"}


def test_by_code_keeps_last_duplicate():
    rows = [_ht_row("300001", 20.0), _ht_row("300001", 10.0)]
    assert _by_code(rows)["300001"]["pct_change"] == 10.0
