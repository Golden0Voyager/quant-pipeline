"""
概念板块缺口回补
────────────────
``concept_board`` 由 ``update_concept_board`` 经 ``push2`` **实时快照**写入，而快照
接口只给「此刻」、不留历史：某天抓取失败 → 该天永久不可恢复。库内形态即此 —
2026-09-29 实测已有 31 个交易日、跨度内应有 50 个，日期跳跃（09-28, 09-20,
09-18, 09-17…）标的是「那天拍成了」。

本模块用东财**历史**接口 ``stock_board_concept_hist_em`` 补回这些日子，与
``tasks/sector_derivatives.py`` 已验证的行业板块模式对称（该任务让 ``sector_daily``
的日期保持连续、从未真正缺过）。

两条硬约束
──────────
1. **严格不写 ``expected`` 当天。** 快照已写该日且带 ``up_count``/``down_count``，
   而历史 K 线没有这两列；``providers.save_concept_board_batch`` 用
   ``INSERT OR REPLACE``，回补一旦覆盖到 ``expected`` 就会用 NULL 抹掉真值。
2. **只补缺口，不做全量重写。** 单个概念失败即跳过并计数，不中断其余——
   回补是尽力而为，一次失败不该让 504 个概念白跑。
"""

from __future__ import annotations

import logging
from collections.abc import Iterable

import pandas as pd

from core.calendar import (  # noqa: F401 — get_expected_latest_trading_day 重新导出
    get_expected_latest_trading_day,
    get_recent_trading_days,
)
from core.known_gaps import declared_missing_days
from core.source_client import get_default_client
from tasks.concept_board import _fetch_concept_list_em

try:
    import akshare as ak
except ImportError:
    ak = None

logger = logging.getLogger(__name__)

# 上面那句 noqa 的理由：主任务（Task 3）在本模块内直接调 get_expected_latest_trading_day()
# 算 expected，测试也打桩本模块的这个属性——五个 Task 3 用例的
# monkeypatch.setattr(mod, "get_expected_latest_trading_day", ...) 要靠这个名字解析到它，
# 所以它必须绑在模块对象上。同 daily_pipeline.py:37 的 timedelta（那里
# tests/test_daily_pipeline.py:4468 真的 patch 了它）。
#
# 窗口默认值的含义：504 个概念 × **可用**缺口天数 = 请求数。有效窗口比 lookback_days
# 少一天——get_recent_trading_days 给的是「含 expected 在内」的 N 个交易日
# （core/calendar.py 用 `d <= end_date`），而 expected 永不入窗（上面硬约束 1），
# 于是默认 10 对应 9 个可用交易日 = 4536 次请求、约 22 分钟。这是成本上界，
# 超出窗口的缺口不会被自动发现——回补是手动任务，这是它的代价。
DEFAULT_LOOKBACK_DAYS = 10


def find_missing_days(
    have: Iterable[str],
    *,
    expected: str,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    declared: frozenset[str] | None = None,
) -> list[str]:
    """返回需要回补的交易日（升序）。**严格不含 expected 当天。**

    窗口 = 最近 ``lookback_days`` 个交易日 ∩ ``< expected``；再减去库内已有的
    与 ``declared``（已登记的整日缺席，源端不可回补）。

    Args:
        have: 库内 ``concept_board`` 已有的 ``trade_date`` 集合。
        expected: ``get_expected_latest_trading_day()``。
        lookback_days: 窗口大小，单位是**交易日**；``get_recent_trading_days`` 返回的
            N 天**含** ``expected``，而它永不入窗，故实际可用窗口是 N-1 天。
        declared: 已登记的整日缺席日期；None 时取 ``declared_missing_days()``。
    """
    skip = declared_missing_days() if declared is None else declared
    present = set(have)
    recent = get_recent_trading_days(expected, lookback_days)
    return sorted(
        day
        for day in recent
        if day < expected and day not in present and day not in skip
    )


# 历史行与快照行靠这个值区分：快照有涨跌家数，历史没有。
HISTORY_SOURCE = "em_hist"

# 列名按同族接口 stock_board_industry_hist_em 推定（tasks/sector_derivatives.py
# 已在生产验证）。待验证项见 spec：东财线路自 2026-09-29 起分钟级抖动，该接口的
# 真实列名尚未实测，因此这里用「能识别多少写多少」而不是硬断言。
_HIST_COL_MAP = {
    "日期": "trade_date",
    "开盘": "open",
    "收盘": "close",
    "最高": "high",
    "最低": "low",
    "成交量": "volume",
    "成交额": "amount",
    "涨跌幅": "pct_change",
    # 换手率：源给、``concept_board.turnover REAL`` 也存（providers.py:373），
    # 之前漏映射 → 回补行的 turnover 恒 NULL，白丢一个可用字段。
    "换手率": "turnover",
}
# 记录键集固定，缺列写 None。行与行之间键不一致会让下游按 key 取值踩 KeyError。
# ⚠️ 这张表与上面那张字典、以及下面 records.append 的键集是**三处必须同步**的：
#    tests/test_concept_board_backfill.py 的键集断言直接引用本表，改一处就红。
# open/close/high/low/volume/amount 映射着但 ``save_concept_board_batch`` 的 INSERT
# 不写它们——保留是为了与 tasks/sector_derivatives.py 的先例一致（sector_daily 真存），
# 将来 concept_board 加列时它们已经就位。
_HIST_VALUE_COLS = (
    "open", "close", "high", "low", "volume", "amount", "pct_change", "turnover",
)


def _records_from_hist_df(
    df: pd.DataFrame, *, concept_code: str, concept_name: str
) -> list[dict]:
    """东财历史 K 线 → concept_board 记录。``up_count``/``down_count`` 恒为 None。

    ``df is None`` 这道守卫静态上不可达（``SourceClient.call`` 只在 operation 返回时
    才给 ``success=True``，而该 operation 返回 DataFrame 或抛异常），但它是**第三手**
    不可信载荷前唯一的闸：真走到时，代价是 ``df.empty`` 的 AttributeError 抛在
    ``client.call`` 之外，会中断当天余下 500 个板块。为一个词点代价不值的取舍：
    留下。返回类型写非 Optional 是因为那才是**约定**，不是对未类型化上游的断言。
    """
    if df is None or df.empty:
        return []
    df = df.rename(columns=_HIST_COL_MAP)
    if "trade_date" not in df.columns:
        logger.warning("⚠️ 概念板块历史缺少日期列，跳过该板块")
        return []
    # 一列值都没认出来 → 这个板块**一条都不产出**（同 tasks/sector_derivatives.py:110）。
    # 「宽松映射」只该容忍**多**出来的列，不该容忍**全**缺失：照旧放行的话，源端把
    # 「涨跌幅」改名后 504 个板块每个仍返回 1 条记录，而 pct_change/turnover/… 全是
    # None——``save_concept_board_batch`` 照收、任务报 success，而 ``find_missing_days``
    # 下次看到那天「已经有了」就再也不会回来。那正是 2026-08-12 P2-12 的空洞形状。
    available = [c for c in _HIST_VALUE_COLS if c in df.columns]
    if not available:
        logger.warning(
            f"⚠️ 概念板块历史值列全部漂移，实到列 {list(df.columns)}，跳过该板块"
        )
        return []
    # 告警方向是「**已映射的列消失了**」，不是「来了没映射的列」：东财正常就多给
    # 振幅/涨跌额（该接口实测 11 列，见本机 akshare 源码），按反向判据则每次健康
    # 响应都告警——一次 504×9 的回补刷 4536 条一模一样的噪音，真漂移反而被淹没。
    missing = [c for c in _HIST_VALUE_COLS if c not in df.columns]
    if missing:
        logger.warning(f"⚠️ 概念板块历史缺少已映射列 {missing}，这几列写 None")
    records: list[dict] = []
    for _, row in df.iterrows():
        raw_day = row.get("trade_date")
        # pd.isna 守卫：str(None) 是 'None'、str(nan) 是 'nan'，两个都 truthy，
        # `if not day` 抓不住，而 concept_board.trade_date 是 DATE NOT NULL 也不管用
        # ——SQLite 不做类型检查，'None' 会真的落库并污染 find_missing_days 的 have。
        if raw_day is None or pd.isna(raw_day):
            continue
        day = str(raw_day).strip()[:10]
        if not day:
            continue
        # 键集固定：未识别的列写 None 而不是省略，否则同一批记录里有的行有
        # pct_change、有的行没有，下游按 key 取值会踩 KeyError。
        records.append({
            "trade_date": day,
            "concept_code": concept_code,
            "concept_name": concept_name,
            "open": row.get("open"),
            "close": row.get("close"),
            "high": row.get("high"),
            "low": row.get("low"),
            "volume": row.get("volume"),
            "amount": row.get("amount"),
            "pct_change": row.get("pct_change"),
            "turnover": row.get("turnover"),
            "up_count": None,
            "down_count": None,
            "data_source": HISTORY_SOURCE,
        })
    return records


def _fetch_board_list() -> list[tuple[str, str]] | None:
    """概念板块列表，返回 ``(code, name)``。

    **委托**给 ``tasks.concept_board._fetch_concept_list_em``，不自己再实现一遍：
    那个函数已经解决了本机反复踩到的两件事——①东财 WAF 会间歇性掐掉
    ``push2.eastmoney.com``（``tasks/concept_board.py:225`` 记着这条），它按短页翻完
    ``push2`` → ``push2delay``；②它用 ``fs=m:90+t:3``，与实时快照同一个板块全集。
    本模块若自己调 ``stock_board_concept_name_em``，拿到的是单主机 + 另一套筛选，
    回补的板块范围会和快照写入的那批对不上。
    跨模块 import 一个下划线名是本仓库既有做法（``tasks/utility.py:44`` 从
    ``tasks.bars`` import ``_detect_suspended_symbols`` 等）：下划线只表示「模块私有」，
    不表示「模块禁入」；两者同属 ``tasks/``、依赖方向单一、无环。

    Returns:
        ``None`` = 源端不可用（网络/熔断/协议错）；``[]`` = 源端正常但确实没有板块。
        两者必须能分开：前者是**故障**（该日回补整体作废、值得重试），后者是**事实**
        （无事可做）。旧实现两者都返回 ``[]``，调用方分不出「东财挂了」和「没板块」。
    """
    resp = get_default_client().call("eastmoney", _fetch_concept_list_em)
    if not resp.success:
        logger.warning(f"⚠️ 概念板块列表获取失败: {resp.metadata.error}")
        return None
    items: list[dict] = resp.data
    if not items:
        return []
    return [(item["concept_code"], item["concept_name"]) for item in items]


def fetch_day_records(
    day: str, *, boards: list[tuple[str, str]] | None = None
) -> list[dict]:
    """取某个交易日的全部概念板块记录。单个板块失败即跳过，不中断其余。

    第二道日期闸：只留 ``trade_date == day`` 的行。``find_missing_days`` 的
    ``day < expected`` 只约束**请求哪些天**，管不到**回来的是哪几天**——源端若无视
    ``start_date``/``end_date`` 把整段历史（含 expected 当天）吐回来，Task 3 就会
    照写，而 ``INSERT OR REPLACE`` 会用历史行的 NULL ``up_count``/``down_count``
    盖掉快照那天的真值：模块 docstring 硬约束 1 要防的正是这个。
    """
    if ak is None:
        logger.error("❌ akshare 未安装")
        return []
    if boards is None:
        boards = _fetch_board_list()
    if boards is None:
        logger.warning(f"⚠️ 概念板块列表不可用，{day} 整日跳过")
        return []
    if not boards:
        return []
    compact = day.replace("-", "")
    records: list[dict] = []
    for code, name in boards:
        resp = get_default_client().call(
            "eastmoney",
            lambda c=code: ak.stock_board_concept_hist_em(
                symbol=c, period="daily",
                start_date=compact, end_date=compact, adjust="",
            ),
        )
        if not resp.success:
            logger.warning(f"⚠️ 概念 {name}({code}) 历史获取失败: {resp.metadata.error}")
            continue
        board_records = _records_from_hist_df(
            resp.data, concept_code=code, concept_name=name
        )
        kept = [r for r in board_records if r["trade_date"] == day]
        if len(kept) != len(board_records):
            logger.warning(
                f"⚠️ 概念 {name}({code}) 历史混入 {len(board_records) - len(kept)} "
                f"条非 {day} 的行（源端可能无视了 start/end），已丢弃"
            )
        records.extend(kept)
    return records
