"""概念板块缺口回补：缺口计算的边界。

实测背景（2026-09-29）：`concept_board` 只由 `push2` 实时快照写入，接口不留
历史，线路一断该天就永久丢失。库内 31 个交易日 / 应有 50 个，日期跳跃即为
「那天没拍成」的痕迹。本模块负责算出该补哪些天。
"""

from __future__ import annotations

import logging
from types import SimpleNamespace

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

# ``ak.stock_board_concept_hist_em`` 实际返回的 11 列——本机读 akshare 源码逐字读出
# （它先建 11 列再按固定顺序 select），2026-09-29 记录。本模块只映射其中 9 列，
# 剩下 ``振幅``/``涨跌额`` 是**正常多出来的**，不是漂移。
_HIST_FRAME_COLUMNS = [
    "日期", "开盘", "收盘", "最高", "最低", "涨跌幅",
    "涨跌额", "成交量", "成交额", "振幅", "换手率",
]


class _FakeResponse:
    """``SourceResponse`` 的最小替身：只带 ``_fetch_board_list`` 读的那几个属性。"""

    def __init__(
        self, *, success: bool, data: object = None, error: str | None = None
    ) -> None:
        self.success = success
        self.data = data
        self.metadata = SimpleNamespace(error=error)


class _FakeClient:
    """记录每次 ``call`` 传进来的 operation——用来钉住「委托给谁」。"""

    def __init__(self, resp: _FakeResponse) -> None:
        self.resp = resp
        self.ops: list[object] = []

    def call(self, _source: str, op: object, *_a: object, **_k: object) -> _FakeResponse:
        self.ops.append(op)
        return self.resp


def _stub_client(monkeypatch, mod, resp: _FakeResponse) -> _FakeClient:
    client = _FakeClient(resp)
    monkeypatch.setattr(mod, "get_default_client", lambda: client)
    return client


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


def test_history_records_ignore_unmapped_columns_without_warning(caplog):
    """**多**出来的列不是漂移，东财正常就多给 ``振幅``/``涨跌额``。

    旧判据是「帧里有我们没映射的列就告警」，配上真实列集就是**每次健康响应都命中**：
    一次 504×9 的回补刷 4536 条一模一样的噪音，真漂移发生时反而被埋在里面。
    真正要报的是反方向——**已映射的列消失了**（见下一条）。
    """
    import pandas as pd

    from tasks.concept_board_backfill import _HIST_VALUE_COLS, _records_from_hist_df

    values: dict[str, list[object]] = {c: [1.0] for c in _HIST_FRAME_COLUMNS}
    values["日期"] = ["2026-09-28"]
    df = pd.DataFrame(values)
    with caplog.at_level(logging.WARNING):
        got = _records_from_hist_df(df, concept_code="BK0425", concept_name="算力")
    assert len(got) == 1
    assert not caplog.records, f"健康响应当告警: {caplog.text}"
    # 键集与 _HIST_VALUE_COLS 的一致性交给这一条断言：以后往那张表加一列却忘了
    # 写进记录（或反之），这里就红，不必靠人手对着两张列表数。
    assert set(got[0]) == {
        "trade_date", "concept_code", "concept_name",
        "up_count", "down_count", "data_source",
    } | set(_HIST_VALUE_COLS)


def test_history_records_warn_when_a_mapped_column_disappears(caplog):
    """反方向才是信号：源端少给了我们已映射的列，那几列只能写 None。"""
    import pandas as pd

    from tasks.concept_board_backfill import _records_from_hist_df

    df = pd.DataFrame({"日期": ["2026-09-28"], "收盘": [102.0]})
    with caplog.at_level(logging.WARNING):
        got = _records_from_hist_df(df, concept_code="BK0425", concept_name="算力")
    assert len(got) == 1
    assert got[0]["close"] == 102.0
    assert got[0]["pct_change"] is None
    assert "缺少" in caplog.text


def test_turnover_is_mapped_from_the_source_column():
    """``换手率`` 有源有列、``concept_board.turnover REAL`` 有表有列（providers.py:373），
    映射表漏了它 → 回补行的 turnover 恒 NULL，白丢一个可用字段。"""
    import pandas as pd

    from tasks.concept_board_backfill import _records_from_hist_df

    df = pd.DataFrame({"日期": ["2026-09-28"], "收盘": [102.0], "换手率": [1.25]})
    got = _records_from_hist_df(df, concept_code="BK0425", concept_name="算力")
    assert got[0]["turnover"] == 1.25


def test_records_are_dropped_when_no_value_column_is_recognised():
    """一列值都没认出来 → 这个板块**一条都不产出**。

    「宽松映射」只该容忍多出来的列，不该容忍全部缺失。照旧放行的话，源端把
    ``涨跌幅`` 改名后 504 个板块每个仍返回 1 条记录，而 pct_change/turnover/…
    全是 None——``save_concept_board_batch`` 照收，任务报 success，而
    ``find_missing_days`` 下次看到那天「已经有了」就再也不会回来。那正是
    2026-08-12 P2-12 静默永久空洞的形状。
    """
    import pandas as pd

    from tasks.concept_board_backfill import _records_from_hist_df

    df = pd.DataFrame({
        "日期": ["2026-09-28"],
        "振幅": ["2.0"],
        "涨跌额": ["0.6"],
    })
    assert _records_from_hist_df(df, concept_code="BK0425", concept_name="算力") == []
    # 只有一个日期列、连行都不该有——不是「键齐但值全 None」。
    assert _records_from_hist_df(
        pd.DataFrame({"日期": ["2026-09-28"]}), concept_code="BK0425", concept_name="算力"
    ) == []


def test_missing_date_row_is_dropped_instead_of_written_as_the_string_none():
    """``str(None)`` 是 ``'None'``、``str(nan)`` 是 ``'nan'``，都 truthy，
    ``if not day`` 抓不住。``concept_board.trade_date`` 虽是 ``DATE NOT NULL``，
    SQLite 不做类型检查，``'None'`` 会真的落库并污染 ``find_missing_days`` 的 have。"""
    import numpy as np
    import pandas as pd

    from tasks.concept_board_backfill import _records_from_hist_df

    df = pd.DataFrame({"日期": [None, "2026-09-28"], "收盘": [1.0, 2.0]})
    got = _records_from_hist_df(df, concept_code="BK0425", concept_name="算力")
    assert [r["trade_date"] for r in got] == ["2026-09-28"]

    nan_df = pd.DataFrame({"日期": [np.nan], "收盘": [1.0]})
    assert _records_from_hist_df(nan_df, concept_code="BK0425", concept_name="算力") == []


def test_fetch_day_drops_rows_that_are_not_the_requested_day(monkeypatch):
    """第二道日期闸：只留 ``trade_date == day``。

    ``find_missing_days`` 的 ``day < expected`` 只约束**请求哪些天**，管不到
    **回来的是哪几天**。源端若无视 ``start_date``/``end_date`` 把整段历史吐回来，
    Task 3 就会照写，而 ``INSERT OR REPLACE`` 会用历史行的 NULL
    ``up_count``/``down_count`` 盖掉快照那天的真值——模块 docstring 硬约束 1
    要防的正是这个。
    """
    import pandas as pd

    import tasks.concept_board_backfill as mod

    def fake_hist(**_):
        # 含 expected 当天（09-29）——它绝不能被写进表。
        return pd.DataFrame({
            "日期": ["2026-09-25", "2026-09-28", "2026-09-29"],
            "收盘": [1.0, 2.0, 3.0],
        })

    monkeypatch.setattr(mod.ak, "stock_board_concept_hist_em", fake_hist)
    got = mod.fetch_day_records("2026-09-28", boards=[("BK0001", "甲")])
    assert [r["trade_date"] for r in got] == ["2026-09-28"]


def test_fetch_board_list_returns_none_when_the_source_is_down(monkeypatch):
    """源端不可用必须与「确实没有板块」能分开。

    两者都返回 ``[]`` 时，调用方分不出「东财挂了」（该日回补整体作废、值得重试）
    与「今天没板块」（无事可做）——P2-11/P2-12 立起来的正是这条区分。
    """
    import tasks.concept_board_backfill as mod

    _stub_client(monkeypatch, mod, _FakeResponse(
        success=False, error="circuit breaker open",
    ))
    assert mod._fetch_board_list() is None


def test_fetch_board_list_returns_empty_list_when_there_are_no_boards(monkeypatch):
    import tasks.concept_board_backfill as mod

    _stub_client(monkeypatch, mod, _FakeResponse(success=True, data=[]))
    assert mod._fetch_board_list() == []


def test_fetch_board_list_maps_the_snapshot_shape_to_code_name_pairs(monkeypatch):
    import tasks.concept_board_backfill as mod

    _stub_client(monkeypatch, mod, _FakeResponse(success=True, data=[
        {"concept_code": "BK0425", "concept_name": "算力"},
        {"concept_code": "BK0426", "concept_name": "光伏"},
    ]))
    assert mod._fetch_board_list() == [("BK0425", "算力"), ("BK0426", "光伏")]


def test_fetch_board_list_reuses_the_snapshot_fetcher(monkeypatch):
    """回补的板块全集必须和快照同一个（``fs=m:90+t:3``），且要有
    ``push2`` → ``push2delay`` 的 failover——本机东财 WAF 会间歇性掐掉编号子域名
    （``tasks/concept_board.py:225`` 记着这条）。两件事都由「调那个函数本尊」保证，
    所以这里直接把它钉住：将来谁改回自己调 ``stock_board_concept_name_em``，
    这条就红。"""
    import tasks.concept_board as cb
    import tasks.concept_board_backfill as mod

    client = _stub_client(monkeypatch, mod, _FakeResponse(success=True, data=[]))
    mod._fetch_board_list()
    assert client.ops == [cb._fetch_concept_list_em]


def test_fetch_day_makes_no_history_request_when_the_board_list_is_unavailable(monkeypatch):
    """板块列表拿不到就别发 504 次历史请求。

    这里调的是 ``fetch_day_records(day)`` 的**默认参数**形态（``boards=None`` → 内部
    取列表）。它不是生产的调用路径：Task 3 自己调 ``_fetch_board_list()`` 判完
    ``None``/``[]`` 之后，永远显式传 ``boards=``。这条用例守的是公开默认值的可用性。
    """
    import tasks.concept_board_backfill as mod

    def boom(*_a, **_k):
        raise AssertionError("板块列表不可用时不该发历史请求")

    _stub_client(monkeypatch, mod, _FakeResponse(success=False, error="down"))
    monkeypatch.setattr(mod.ak, "stock_board_concept_hist_em", boom)
    assert mod.fetch_day_records("2026-09-28") == []


def test_fetch_day_skips_failed_board_and_keeps_the_rest(monkeypatch):
    """单个板块失败不中断其余——一次失败不该让 504 个概念白跑。"""
    import pandas as pd

    import tasks.concept_board_backfill as mod

    boards = [("BK0001", "甲"), ("BK0002", "乙"), ("BK0003", "丙")]

    # 按**代码**（"BK0002"）而不是名称（"乙"）判定失败：akshare 的
    # stock_board_concept_hist_em 收到 `BK\d+` 时直接当板块代码用，收到名称才会
    # 自己去查一遍名称→代码（`__stock_board_concept_name_em()`，每个板块一次请求）。
    # 传代码既是对的（concept_code 存的也是 f12 代码），也让 504 次请求不翻倍。
    def fake_hist(symbol, **_):
        if symbol == "BK0002":
            raise ConnectionError("Connection closed abruptly")
        return pd.DataFrame({"日期": ["2026-09-28"], "收盘": [100.0]})

    monkeypatch.setattr(mod.ak, "stock_board_concept_hist_em", fake_hist)
    got = mod.fetch_day_records("2026-09-28", boards=boards)
    assert {r["concept_name"] for r in got} == {"甲", "丙"}


def test_history_records_are_dropped_when_the_date_column_itself_is_renamed(caplog):
    """``日期`` 自己被改名时必须弃掉该板块，而不是让它混进「值列全漂移」的告警里。

    这是东财漂移的**第二种形态**：值列都认得出来、唯独日期列改名。两条守卫的
    返回值都是 ``[]``（``row.get("trade_date")`` 拿不到就是 ``None``，被下一行的
    ``raw_day is None`` 挡掉），所以真正可观测的差别只有**告警指向**：留着这道
    守卫，说的是「缺日期列」；删掉它，落到的却是「值列全部漂移」——一个与事实
    相反的诊断。删掉本守卫 → 本用例红。
    """
    import pandas as pd

    from tasks.concept_board_backfill import _records_from_hist_df

    df = pd.DataFrame({"时间": ["2026-09-28"], "收盘": [102.0]})
    with caplog.at_level(logging.WARNING):
        got = _records_from_hist_df(df, concept_code="BK0425", concept_name="算力")
    assert got == []
    assert "缺少日期列" in caplog.text


def test_fetch_day_skips_board_with_unmappable_payload_and_keeps_the_rest(monkeypatch, caplog):
    """载荷不是 DataFrame 时，按板块失败处理——不能炸掉整个 days 循环。

    ``SourceClient.call`` 对任何非 HTTP 返回都判 ``success=True``（core/source_client.py:267），
    于是 ``df.empty`` 上的 ``AttributeError`` 抛在 ``client.call`` 的 try **之外**。
    模块 docstring 硬约束 2 要求「单个概念失败即跳过并计数，不中断其余」，而
    ``sector_derivatives._retry`` 什么都吞所以那边免疫，这里没人吞：异常会一路
    冒出 ``fetch_day_records``，丢掉已累积的记录**和**剩余所有待补的日子。
    """
    import pandas as pd

    import tasks.concept_board_backfill as mod

    def fake_hist(symbol, **_):
        if symbol == "BK0002":
            # 东财线路偶尔吐 JSON 错误信封而不是 DataFrame——不抛异常，
            # 于是 SourceClient 会把它当成功结果原样交回来。
            return {"code": 500, "data": None}
        return pd.DataFrame({"日期": ["2026-09-28"], "收盘": [100.0]})

    monkeypatch.setattr(mod.ak, "stock_board_concept_hist_em", fake_hist)
    with caplog.at_level(logging.WARNING):
        got = mod.fetch_day_records("2026-09-28", boards=[("BK0001", "甲"), ("BK0002", "乙")])
    assert [r["concept_name"] for r in got] == ["甲"]
    assert "乙" in caplog.text


def test_fetch_day_returns_empty_when_all_boards_fail(monkeypatch):
    import tasks.concept_board_backfill as mod

    def boom(*_a, **_k):
        raise ConnectionError("down")

    monkeypatch.setattr(mod.ak, "stock_board_concept_hist_em", boom)
    assert mod.fetch_day_records("2026-09-28", boards=[("BK0001", "甲")]) == []


# ===========================================================================
# 主任务：状态语义 + 写入上界
# ===========================================================================


def _db():
    from unittest.mock import MagicMock

    db = MagicMock()
    db.save_concept_board_batch = MagicMock(return_value=504)
    return db


def _one_board(monkeypatch, mod):
    """给 ``_fetch_board_list`` 打桩，否则下面几条用例会真的发网络请求。

    主任务是**先取列表再逐日抓取**，所以任何走到 days 循环的用例都必须桩掉它——
    否则测试在离线时红、在联网时绿，而更糟的是本机东财通不通会改变 ``None`` 与
    ``[]`` 的走向，让断言偶尔以另一种原因成立。**hermetic，不依赖真实源端状态。**
    """
    monkeypatch.setattr(mod, "_fetch_board_list", lambda: [("BK0001", "甲")])


def _no_declared_gaps(monkeypatch, mod):
    """把整日缺席登记册钉成空，否则下面几条的期望值会随**生产**登记册变动。

    主任务不传 ``declared``（那正是生产路径），``find_missing_days`` 于是去读
    ``core/known_gaps.py`` 的真实内容。今天 09-28/09-25 都不在册，所以这些用例今天
    会过；但哪天 09-28 真被登记成整日缺席——**那恰恰是本功能存在的理由**——下面
    六条会集体变红，且与它们各自要测的东西毫无关系。同本文件既有的 ``_NO_DECLARED``
    约定（见其上方注释：生产登记册由 tests/test_known_gaps.py 单独钉住）。
    """
    monkeypatch.setattr(mod, "declared_missing_days", lambda: _NO_DECLARED)


def test_no_gap_makes_no_network_call_and_no_write(monkeypatch):
    """无缺口必须零请求零写入——这是日常手动跑时的常态路径。"""
    import tasks.concept_board_backfill as mod

    monkeypatch.setattr(mod, "get_recent_trading_days",
                        lambda end, count: ["2026-09-29", "2026-09-28"])
    monkeypatch.setattr(mod, "get_expected_latest_trading_day", lambda: "2026-09-29")
    _no_declared_gaps(monkeypatch, mod)
    monkeypatch.setattr(
        mod, "_stored_dates", lambda db: {"2026-09-28"}
    )
    def boom(*_a, **_k):
        raise AssertionError("不应发起任何网络请求")
    monkeypatch.setattr(mod, "fetch_day_records", boom)
    # 列表也要桩：它同样是一次网络请求，零请求的断言覆盖它。
    monkeypatch.setattr(mod, "_fetch_board_list", boom)

    db = _db()
    result = mod.update_concept_board_backfill(db)
    assert result["status"] == "success"
    assert result["saved"] == 0
    assert result["requested_days"] == 0
    assert not db.save_concept_board_batch.called


def test_all_days_backfilled_is_success(monkeypatch):
    import tasks.concept_board_backfill as mod

    _one_board(monkeypatch, mod)
    monkeypatch.setattr(mod, "get_recent_trading_days",
                        lambda end, count: ["2026-09-29", "2026-09-28"])
    monkeypatch.setattr(mod, "get_expected_latest_trading_day", lambda: "2026-09-29")
    _no_declared_gaps(monkeypatch, mod)
    monkeypatch.setattr(mod, "_stored_dates", lambda db: set())
    monkeypatch.setattr(
        mod, "fetch_day_records",
        lambda day, boards=None: [{"trade_date": day, "concept_code": "BK1"}],
    )
    result = mod.update_concept_board_backfill(_db())
    assert result["status"] == "success"
    assert result["saved"] == 504
    assert result["backfilled_days"] == ["2026-09-28"]


def test_partial_backfill_is_degraded(monkeypatch):
    import tasks.concept_board_backfill as mod

    _one_board(monkeypatch, mod)
    monkeypatch.setattr(mod, "get_recent_trading_days",
                        lambda end, count: ["2026-09-29", "2026-09-28", "2026-09-25"])
    monkeypatch.setattr(mod, "get_expected_latest_trading_day", lambda: "2026-09-29")
    _no_declared_gaps(monkeypatch, mod)
    monkeypatch.setattr(mod, "_stored_dates", lambda db: set())
    monkeypatch.setattr(
        mod, "fetch_day_records",
        lambda day, boards=None: ([{"trade_date": day, "concept_code": "BK1"}]
                                  if day == "2026-09-28" else []),
    )
    result = mod.update_concept_board_backfill(_db())
    assert result["status"] == "degraded"
    assert result["backfilled_days"] == ["2026-09-28"]
    assert result["failed_days"] == ["2026-09-25"]


def test_nothing_backfilled_when_source_is_down_is_retained_network(monkeypatch):
    """一天都没补上 = 源整体不可用，标 network 让 safe_task 的 30s 重试生效。

    注意这条与上面两条**不是同一件事**：这里列表拿到了（``None`` vs ``[]`` 的分界已过），
    失败发生在**逐日抓取**阶段——每一天都抓到 0 行。
    """
    import tasks.concept_board_backfill as mod

    _one_board(monkeypatch, mod)
    monkeypatch.setattr(mod, "get_recent_trading_days",
                        lambda end, count: ["2026-09-29", "2026-09-28"])
    monkeypatch.setattr(mod, "get_expected_latest_trading_day", lambda: "2026-09-29")
    _no_declared_gaps(monkeypatch, mod)
    monkeypatch.setattr(mod, "_stored_dates", lambda db: set())
    monkeypatch.setattr(mod, "fetch_day_records", lambda day, boards=None: [])
    result = mod.update_concept_board_backfill(_db())
    assert result["status"] == "retained"
    assert result["error_kind"] == "network"
    assert result["retained_old_data"] is True


def test_board_list_source_down_is_retained_network_not_no_data(monkeypatch):
    """``_fetch_board_list()`` 返回 ``None`` = 源挂了 = **可重试**故障。

    折叠成 ``if not boards:`` 就会把它报成 ``no_data``（源正常但无数据），
    ``safe_task`` 的 30s 重试不触发——一次网络抖动被静默吞成「本来就没数据」。
    """
    import tasks.concept_board_backfill as mod

    monkeypatch.setattr(mod, "get_recent_trading_days",
                        lambda end, count: ["2026-09-29", "2026-09-28"])
    monkeypatch.setattr(mod, "get_expected_latest_trading_day", lambda: "2026-09-29")
    _no_declared_gaps(monkeypatch, mod)
    monkeypatch.setattr(mod, "_stored_dates", lambda db: set())
    monkeypatch.setattr(mod, "_fetch_board_list", lambda: None)

    def boom(*_a, **_k):
        raise AssertionError("列表不可用时不该开始逐日抓取")

    monkeypatch.setattr(mod, "fetch_day_records", boom)
    result = mod.update_concept_board_backfill(_db())
    assert result["status"] == "retained"
    assert result["error_kind"] == "network"
    assert result["retained_old_data"] is True
    assert result["requested_days"] == 1


def test_board_list_legitimately_empty_is_no_data_not_a_network_failure(monkeypatch):
    """``_fetch_board_list()`` 返回 ``[]`` = 源正常但确实没板块 = **事实**，不是故障。

    这是 review 第二轮抓到的反向错误：一个**合法**的空结果被映射成
    ``retained`` + ``error_kind=network``，于是 ``safe_task`` 把它当网络问题白等
    30s 重试，而重试多少次结果都一样。``no_data`` 才是正确裁定。
    """
    import tasks.concept_board_backfill as mod

    monkeypatch.setattr(mod, "get_recent_trading_days",
                        lambda end, count: ["2026-09-29", "2026-09-28"])
    monkeypatch.setattr(mod, "get_expected_latest_trading_day", lambda: "2026-09-29")
    _no_declared_gaps(monkeypatch, mod)
    monkeypatch.setattr(mod, "_stored_dates", lambda db: set())
    monkeypatch.setattr(mod, "_fetch_board_list", lambda: [])

    def boom(*_a, **_k):
        raise AssertionError("没有板块时不该发任何历史请求")

    monkeypatch.setattr(mod, "fetch_day_records", boom)
    result = mod.update_concept_board_backfill(_db())
    assert result["status"] == "no_data"
    assert "error_kind" not in result, "源正常，不该标成网络故障"
    assert result["saved"] == 0


def test_backfill_never_writes_expected_or_later(monkeypatch):
    """写入上界闸门：源端若返回 expected 当天（或更晚）的行，必须被丢弃。"""
    import tasks.concept_board_backfill as mod

    _one_board(monkeypatch, mod)
    monkeypatch.setattr(mod, "get_recent_trading_days",
                        lambda end, count: ["2026-09-29", "2026-09-28"])
    monkeypatch.setattr(mod, "get_expected_latest_trading_day", lambda: "2026-09-29")
    _no_declared_gaps(monkeypatch, mod)
    monkeypatch.setattr(mod, "_stored_dates", lambda db: set())
    # 源端好心（或错乱）地把 09-29 也塞了回来
    monkeypatch.setattr(
        mod, "fetch_day_records",
        lambda day, boards=None: [
            {"trade_date": day, "concept_code": "BK1"},
            {"trade_date": "2026-09-29", "concept_code": "BK2"},
        ],
    )
    db = _db()
    mod.update_concept_board_backfill(db)
    written = db.save_concept_board_batch.call_args[0][0]
    assert {r["trade_date"] for r in written} == {"2026-09-28"}
