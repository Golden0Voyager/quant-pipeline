"""已声明、已接受、且经核实不可回补的数据空洞登记册。

为什么需要它
────────────
某些日期上的某个字段永久性地没有被写入。以 ``fundamentals.dividend_yield``
为例，2026-08 至 2026-09 有 6 个交易日整列为 NULL。这些空洞**回补不了**，
三条路都断掉（2026-09-24 逐一核实）：

1. 雪球行情接口（唯一写入方 ``update_market_snapshot``）只返回**实时**值，
   历史日期的股息率拿不到；
2. ``historical_valuation.dividend_yield`` 是 ``fundamentals`` 的副本，
   同样为空（2,824,046 行里只有 2026-06 之后有值）；
3. ``dividend_summary`` 只有无日期的汇总值（``avg_annual_dividend``），
   不是逐日快照。

不登记的后果有两层，第二层才是真正致命的：

* 表面：每次巡检都重报同一批旧空洞，噪音无法归零 —— 2026-08-08 至
  08-25 的 ``dividend_yield 非空率过低: 0.0%`` 连续告警 8 次无人处理，
  正是这个机制在起作用；
* 实质：**新增**空洞会淹没在这批噪音里而无人分辨。

所以这里是「显式声明」而非「静默豁免」：每条都写明日期、影响范围、成因。
``tasks/utility.py::health_check`` 只对**不在册**的空洞告警。
"""

from __future__ import annotations

from dataclasses import dataclass

# ``ingestion_runs`` 开始逐轮记录审计的日期。早于此的自建库回填时代（bulk
# backfill 只写 pe_ttm/pb/ps_ttm/market_cap 四列）没有逐任务审计轨迹，无法
# 归因到具体哪一轮跑漏了什么，故不纳入巡检范围。
AUDIT_ERA_START = "2026-07-24"


@dataclass(frozen=True)
class KnownGap:
    """一条已声明、已接受的数据空洞。

    Attributes
    ----------
    table:
        受影响的表名。
    column:
        受影响且整列为空的列名。
    date:
        受影响的交易日（``YYYY-MM-DD``）。
    cause:
        已核实的成因，含为何不可回补。
    """

    table: str
    column: str
    date: str
    cause: str


# 每条都可在生产库 + ingestion_runs 里复核：
#   SELECT COUNT(*), COUNT(dividend_yield) FROM fundamentals WHERE trade_date = '<date>';
#   以及该日期在 ingestion_runs 里 update_market_snapshot 的记录。
KNOWN_GAPS: tuple[KnownGap, ...] = (
    KnownGap(
        "fundamentals",
        "dividend_yield",
        "2026-08-10",
        "当日仅 26 个任务运行（非完整管道），update_market_snapshot 未执行",
    ),
    KnownGap(
        "fundamentals",
        "dividend_yield",
        "2026-08-12",
        "抓到 5203 条报价但每条 dividend_yield 均为 None，实际写入 0 行；"
        "旧版仍返回 success，空写被掩盖",
    ),
    KnownGap(
        "fundamentals",
        "dividend_yield",
        "2026-08-20",
        "当日 38 个任务运行（非完整管道），update_market_snapshot 未执行",
    ),
    KnownGap(
        "fundamentals",
        "dividend_yield",
        "2026-08-25",
        "任务执行但雪球返回 0 条报价（saved_rows=0），旧版仍返回 success，空写被掩盖",
    ),
    KnownGap(
        "fundamentals",
        "dividend_yield",
        "2026-09-03",
        "当日 48 个任务运行但 update_market_snapshot 未执行",
    ),
    KnownGap(
        "fundamentals",
        "dividend_yield",
        "2026-09-17",
        "部分运行中断：31 个任务（含 update_fundamentals，无 update_market_snapshot），"
        "且当日 health_check 亦未运行，故无人发现",
    ),
)


def known_gap_dates(table: str, column: str) -> frozenset[str]:
    """返回 *table*.*column* 上全部已声明的空洞日期。"""
    return frozenset(g.date for g in KNOWN_GAPS if g.table == table and g.column == column)


def is_known_gap(table: str, column: str, date: str) -> bool:
    """*table*.*column* 在 *date* 上的空洞是否已被声明。"""
    return date in known_gap_dates(table, column)


def describe_known_gap(table: str, column: str, date: str) -> str | None:
    """返回该空洞的已核实成因；未登记时返回 ``None``。"""
    for gap in KNOWN_GAPS:
        if gap.table == table and gap.column == column and gap.date == date:
            return gap.cause
    return None
