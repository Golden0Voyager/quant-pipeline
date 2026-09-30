"""概念板块缺口回补：缺口计算的边界。

实测背景（2026-09-29）：`concept_board` 只由 `push2` 实时快照写入，接口不留
历史，线路一断该天就永久丢失。库内 31 个交易日 / 应有 50 个，日期跳跃即为
「那天没拍成」的痕迹。本模块负责算出该补哪些天。
"""

from __future__ import annotations

from tasks.concept_board_backfill import find_missing_days

# 真实日历：2026-09-29 是周二，09-28 周一，**09-25 是中秋节休市、不在日历里**
# （已对生产库只读核实：get_recent_trading_days("2026-09-29", 6) 不含它）。
# 夹具里混入 09-25 会让回补任务去补一个根本不存在的交易日——504 次白跑的请求。
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
    assert got == sorted(got)


def test_expected_day_is_never_included(monkeypatch):
    """回补绝不写 expected 当天——那道闸防的是 NULL 抹掉快照的涨跌家数。"""
    _calendar(monkeypatch)
    got = find_missing_days(
        [], expected="2026-09-29", lookback_days=10, declared=_NO_DECLARED
    )
    assert got, "窗口内应有缺口"
    assert "2026-09-29" not in got


def test_declared_missing_days_are_skipped(monkeypatch):
    """已登记的整日缺席是不可回补的，反复尝试只是浪费 504 次请求。"""
    _calendar(monkeypatch)
    got = find_missing_days(
        ["2026-09-24"], expected="2026-09-29", lookback_days=10,
        declared=frozenset({"2026-09-28", "2026-09-24"}),
    )
    assert "2026-09-28" not in got and "2026-09-24" not in got
    assert got == [
        "2026-09-15", "2026-09-16", "2026-09-17", "2026-09-18",
        "2026-09-21", "2026-09-22", "2026-09-23",
    ]


def test_lookback_window_is_truncated(monkeypatch):
    """窗口是成本上界：只看最近 lookback_days 个交易日。"""
    _calendar(monkeypatch)
    got = find_missing_days([], expected="2026-09-29", lookback_days=2, declared=_NO_DECLARED)
    assert got == ["2026-09-28"]


def test_holiday_is_never_treated_as_a_gap(monkeypatch):
    """09-25 是中秋节休市，钉住的日历里没有它，就绝不能被要求回补。"""
    _calendar(monkeypatch)
    assert find_missing_days(
        _ALL_IN_WINDOW, expected="2026-09-29", lookback_days=10, declared=_NO_DECLARED
    ) == []
    assert "2026-09-25" not in find_missing_days(
        [], expected="2026-09-29", lookback_days=10, declared=_NO_DECLARED)
    assert "2026-09-25" not in find_missing_days(
        [], expected="2026-09-29", lookback_days=11, declared=_NO_DECLARED)
