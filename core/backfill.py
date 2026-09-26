"""整日缺席回补入口
──────────────────
``core/day_coverage.py`` 会发现「整天什么都没写」的交易日，``core/known_gaps.py``
把它们登记为已接受的空洞。本模块是那个缺口的**补写入口**：把指定的历史交易日
交给能按日期取数的任务重跑一遍。

关键事实：只有一部分表补得回来
──────────────────────────────
回补能力取决于**源端是否支持历史日期**，与写入方签名无关（2026-09-26 逐一核对源端）：

* **可回补**（源端带日期参数，或返回全历史可按日期选行）：

  ================= ==========================================
  表                 源
  ================= ==========================================
  ``index_daily``     ``stock_zh_index_daily_tx``（全历史，按日期选行）
  ``limit_up_down``   ``stock_zt_pool_em`` / ``stock_zt_pool_dtgc_em``（``date=``）
  ``block_trade``     ``stock_dzjy_mrmx``（``start_date``/``end_date``）
  ``sector_valuation`` ``stock_industry_pe_ratio_cninfo``（``date=``）
  ================= ==========================================

* **补不回来**（源端只给实时值，或表本身由缺失的上游派生）：

  ========================== ==================================================
  表                          原因
  ========================== ==================================================
  ``fundamentals``            雪球行情只给实时值
  ``historical_valuation``    是 ``fundamentals`` 的副本，同样只给实时值
  ``ah_premium``              ``stock_zh_ah_spot_em`` 只给实时值
  ``fund_flow``               ``loader.get_market_fund_flow()`` 只给实时值
  ``sector_fund_flow``        同花顺 ``stock_fund_flow_industry`` 只有即时快照
  ``sector_industry``         由 ``fundamentals`` + ``stock_list`` 派生，上游本身就缺
  ========================== ==================================================

``tests/test_backfill.py`` 有一条门禁：上面两组必须正好覆盖 ``day_coverage.PROBE_TABLES``
——探针表增删时这张分类表必须同步，不允许留下「没说过能不能补」的表。

（``sector_daily`` 不在探针表里：它的源返回全历史、写入方顺手覆盖了历史日期，
实测 6 个缺席日都有 90 行，从未真正缺过。``index_futures_basis`` 同理不在探针集内。）
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from datetime import datetime

from core.day_coverage import missing_trading_days
from core.known_gaps import declared_missing_days

# 可回补的「探针表 → 负责任务」映射。任务名不是 daily 路径上的任务名时（如
# ``sector_valuation`` 日常由复合任务 ``update_sector_derivatives`` 写入），
# 用回补专用的审计名，见 daily_pipeline._BACKFILL_TASK_CALLABLES。
BACKFILLABLE_TABLES: tuple[tuple[str, str], ...] = (
    ("index_daily", "update_index_daily"),
    ("limit_up_down", "update_limit_up_down"),
    ("block_trade", "update_block_trade"),
    ("sector_valuation", "update_sector_valuation"),
)

# 已核实**不可回补**的探针表及原因（供巡检报告与文档引用；不要在代码里假装它们可补）。
NOT_BACKFILLABLE_TABLES: tuple[tuple[str, str], ...] = (
    ("fundamentals", "雪球行情只给实时值"),
    ("historical_valuation", "fundamentals 的副本，同样只给实时值"),
    ("ah_premium", "stock_zh_ah_spot_em 只给实时值"),
    ("fund_flow", "get_market_fund_flow() 只给实时值"),
    ("sector_fund_flow", "同花顺 stock_fund_flow_industry 只有即时快照"),
    ("sector_industry", "由 fundamentals + stock_list 派生，上游本身缺失"),
)


@dataclass(frozen=True)
class BackfillPlan:
    """一次回补要处理的日期，以及被跳过的日期及原因。

    Attributes
    ----------
    days:
        真正要回补的交易日，按旧到新排列。
    skipped:
        ``(日期, 跳过原因)``，按旧到新排列。显式指定但已经补上/格式不对的日期会落这里。
    source:
        ``"explicit"``（命令行给了日期）或 ``"declared"``（取登记册里的缺失日）。
    """

    days: tuple[str, ...] = ()
    skipped: tuple[tuple[str, str], ...] = field(default_factory=tuple)
    source: str = "declared"


def parse_days(spec: str) -> tuple[list[str], list[tuple[str, str]]]:
    """解析逗号/空白分隔的日期串，返回 ``(合法日期, 非法项及原因)``。

    去重并保序（旧到新）。日期必须严格是 ``YYYY-MM-DD``。
    """
    raw = [part for part in spec.replace(",", " ").split() if part]
    days: list[str] = []
    rejected: list[tuple[str, str]] = []
    seen: set[str] = set()
    for item in raw:
        try:
            parsed = datetime.strptime(item, "%Y-%m-%d").date().isoformat()
        except ValueError:
            rejected.append((item, "不是合法的 YYYY-MM-DD 日期"))
            continue
        if parsed in seen:
            continue
        seen.add(parsed)
        days.append(parsed)
    days.sort()
    return days, rejected


def is_still_absent(cursor: sqlite3.Cursor, day: str) -> bool:
    """该交易日在生产库里是否**仍然**整日缺席（全部探针表皆空）。

    非交易日、或探针表不可用时返回 ``False``——「无法判定」不得当成「缺」，
    否则会去回补一个根本不存在的交易日。
    """
    return day in missing_trading_days(cursor, start=day, end=day)


def resolve_backfill_days(cursor: sqlite3.Cursor, spec: str | None) -> BackfillPlan:
    """把命令行输入解析成回补计划。

    ``spec`` 为空/空白（如只写了 ``--backfill-days``）→ 取 ``core/known_gaps.py``
    的已登记缺失日；否则按 ``spec`` 里显式给出的日期。两种来源都只会保留
    **仍然缺席**的日期：已经补上的直接跳过（幂等，避免无谓的网络请求）。
    """
    explicit = bool(spec and spec.strip())
    if explicit:
        requested, rejected = parse_days(spec or "")
        source = "explicit"
    else:
        requested, rejected = sorted(declared_missing_days()), []
        source = "declared"

    days: list[str] = []
    skipped: list[tuple[str, str]] = list(rejected)
    for day in requested:
        if is_still_absent(cursor, day):
            days.append(day)
        else:
            skipped.append((day, "该日已有数据（或不是交易日），无需回补"))
    days.sort()
    skipped.sort()
    return BackfillPlan(days=tuple(days), skipped=tuple(skipped), source=source)
