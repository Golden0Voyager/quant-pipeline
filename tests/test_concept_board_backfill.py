"""概念板块缺口回补：缺口计算的边界。

实测背景（2026-09-29）：`concept_board` 只由 `push2` 实时快照写入，接口不留
历史，线路一断该天就永久丢失。库内 31 个交易日 / 应有 50 个，日期跳跃即为
「那天没拍成」的痕迹。本模块负责算出该补哪些天。
"""

from __future__ import annotations

import logging

from tasks.concept_board_backfill import find_missing_days

# 真实日历：2026-09-29 是周二，09-28 周一，**09-25 是中秋节休市、不在日历里**
# （已对生产库只读核实：get_recent_trading_days("2026-09-29", 6) 不含它）。
#
# ⚠️ 改动这份列表的人注意：**不要加进 2026-09-25**。整个模块没有任何「节假日」
# 逻辑——休市日不进窗口，靠的是钉住的日历里根本没有它。加进去等于凭空造出一个
# 不存在的交易日，回补会为它白跑 504 次请求。下面不再有用例守这条，因为任何
# 断言只要日历里有它就必然通过，守不住；守它的责任在这个列表上。
_TRADING_DAYS = [
    "2026-09-29", "2026-09-28", "2026-09-24", "2026-09-23", "2026-09-22",
    "2026-09-21", "2026-09-18", "2026-09-17", "2026-09-16", "2026-09-15",
    "2026-09-14",
]

# lookback=10 / expected=09-29 时，去掉 expected 之后的窗口内容（9 天）
_ALL_IN_WINDOW = [
    "2026-09-28", "2026-09-24", "2026-09-23", "2026-09-22", "2026-09-21",
    "2026-09-18", "2026-09-17", "2026-09-16", "2026-09-15",
]

# 每个用例都**显式**传 declared，不走生产的 declared_missing_days()。
# 默认 None 会读 core/known_gaps.py 的真实内容，而 2026-09-16 就在本窗口内——
# 哪天那条登记被清掉（2026-09-27 的部分恢复已经把 4 张表补齐了），
# 这些断言就会因为一个跟它们无关的理由变红。生产登记册由 tests/test_known_gaps.py
# 单独钉住，不该顺带决定这里的期望值。
_NO_DECLARED: frozenset[str] = frozenset()


def _calendar(monkeypatch):
    """把交易日历钉死，使边界不依赖宿主机缓存。"""
    import tasks.concept_board_backfill as mod

    monkeypatch.setattr(
        mod, "get_recent_trading_days",
        lambda end, count: [d for d in _TRADING_DAYS if d <= end][:count],
    )


def test_no_gap_returns_empty(monkeypatch):
    _calendar(monkeypatch)
    assert find_missing_days(
        _ALL_IN_WINDOW, expected="2026-09-29", lookback_days=10, declared=_NO_DECLARED
    ) == []


def test_missing_days_are_returned_in_ascending_order(monkeypatch):
    _calendar(monkeypatch)
    # 库内实测形态：只有零星几天。窗口 9 天里已有 3 天 → 缺 6 天。
    have = ["2026-09-21", "2026-09-18", "2026-09-17"]
    got = find_missing_days(have, expected="2026-09-29", lookback_days=10, declared=_NO_DECLARED)
    assert got == [
        "2026-09-15", "2026-09-16", "2026-09-22", "2026-09-23", "2026-09-24", "2026-09-28",
    ]


def test_expected_day_is_never_included(monkeypatch):
    """回补绝不写 expected 当天——那道闸防的是 NULL 抹掉快照的涨跌家数。"""
    _calendar(monkeypatch)
    got = find_missing_days(
        [], expected="2026-09-29", lookback_days=10, declared=_NO_DECLARED
    )
    assert got, "窗口内应有缺口"
    assert "2026-09-29" not in got


def test_declared_missing_days_are_skipped(monkeypatch):
    """已登记的整日缺席是不可回补的，反复尝试只是浪费 504 次请求。

    ``have`` 传空：09-24 若同时出现在 ``have`` 里，「它没被返回」就是 ``present``
    过滤的功劳，与 ``declared`` 无关——那样这条用例就白测了。
    """
    _calendar(monkeypatch)
    got = find_missing_days(
        [], expected="2026-09-29", lookback_days=10,
        declared=frozenset({"2026-09-28", "2026-09-24"}),
    )
    assert "2026-09-28" not in got and "2026-09-24" not in got
    assert got == [
        "2026-09-15", "2026-09-16", "2026-09-17", "2026-09-18",
        "2026-09-21", "2026-09-22", "2026-09-23",
    ]


def test_declared_default_comes_from_the_gap_registry(monkeypatch):
    """**不传** ``declared`` 时必须真去查 ``declared_missing_days()``。

    这是主任务（Task 3）实际走的路径——它不传 ``declared``。缺了这条查询，六个
    已核实不可回补的整日缺席会各被请求 504 次，全部拿回空集。生产登记册的**内容**
    由 ``tests/test_known_gaps.py`` 钉住；这里钉的是「默认路径确实去查它」，
    因此打桩返回值而不是读真实登记册。
    """
    _calendar(monkeypatch)
    import tasks.concept_board_backfill as mod

    monkeypatch.setattr(
        mod, "declared_missing_days",
        lambda: frozenset({"2026-09-28", "2026-09-16"}),
    )
    got = find_missing_days([], expected="2026-09-29", lookback_days=10)
    assert got == [
        "2026-09-15", "2026-09-17", "2026-09-18", "2026-09-21",
        "2026-09-22", "2026-09-23", "2026-09-24",
    ]


def test_lookback_window_is_truncated(monkeypatch):
    """窗口是成本上界：只看最近 lookback_days 个交易日。"""
    _calendar(monkeypatch)
    got = find_missing_days([], expected="2026-09-29", lookback_days=2, declared=_NO_DECLARED)
    assert got == ["2026-09-28"]


def test_history_records_carry_em_hist_source_and_null_counts():
    """历史 K 线没有涨跌家数，必须显式写 None 并标记来源，供下游区分。"""
    import pandas as pd

    from tasks.concept_board_backfill import HISTORY_SOURCE, _records_from_hist_df

    df = pd.DataFrame({
        "日期": ["2026-09-28", "2026-09-28"],
        "开盘": [100.0, 100.0],
        "收盘": [102.0, 102.0],
        "最高": [103.0, 103.0],
        "最低": [99.0, 99.0],
        "成交量": [1000.0, 1000.0],
        "成交额": [1.0e8, 1.0e8],
        "涨跌幅": [2.0, 2.0],
    })
    got = _records_from_hist_df(df, concept_code="BK0425", concept_name="算力")
    assert len(got) == 2
    row = got[0]
    assert row["trade_date"] == "2026-09-28"
    assert row["concept_code"] == "BK0425"
    assert row["concept_name"] == "算力"
    assert row["close"] == 102.0
    assert row["pct_change"] == 2.0
    assert row["data_source"] == HISTORY_SOURCE
    assert row["up_count"] is None
    assert row["down_count"] is None


def test_history_records_drop_unknown_columns(caplog):
    """源端列名漂移时只写能识别的列并告警，不抛异常——否则整轮回补全废。"""
    import pandas as pd

    from tasks.concept_board_backfill import _records_from_hist_df

    df = pd.DataFrame({"日期": ["2026-09-28"], "收盘": [102.0], "某个新字段": [1.0]})
    with caplog.at_level(logging.WARNING):
        got = _records_from_hist_df(df, concept_code="BK0425", concept_name="算力")
    assert len(got) == 1
    assert got[0]["close"] == 102.0
    assert got[0]["pct_change"] is None
    assert "列名" in caplog.text


def test_fetch_day_skips_failed_board_and_keeps_the_rest(monkeypatch):
    """单个板块失败不中断其余——一次失败不该让 504 个概念白跑。"""
    import pandas as pd

    import tasks.concept_board_backfill as mod

    boards = [("BK0001", "甲"), ("BK0002", "乙"), ("BK0003", "丙")]

    def fake_hist(symbol, **_):
        if symbol == "乙":
            raise ConnectionError("Connection closed abruptly")
        return pd.DataFrame({"日期": ["2026-09-28"], "收盘": [100.0]})

    monkeypatch.setattr(mod.ak, "stock_board_concept_hist_em", fake_hist)
    got = mod.fetch_day_records("2026-09-28", boards=boards)
    assert {r["concept_name"] for r in got} == {"甲", "丙"}


def test_fetch_day_returns_empty_when_all_boards_fail(monkeypatch):
    import tasks.concept_board_backfill as mod

    def boom(*_a, **_k):
        raise ConnectionError("down")

    monkeypatch.setattr(mod.ak, "stock_board_concept_hist_em", boom)
    assert mod.fetch_day_records("2026-09-28", boards=[("BK0001", "甲")]) == []
