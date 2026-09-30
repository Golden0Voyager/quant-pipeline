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

from core.calendar import get_expected_latest_trading_day, get_recent_trading_days
from core.known_gaps import declared_missing_days

logger = logging.getLogger(__name__)

# 窗口默认值的含义：504 个概念 × 缺口天数 = 请求数，10 天即 5040 次、约 25 分钟。
# 这是成本上界，超出窗口的缺口不会被自动发现——回补是手动任务，这是它的代价。
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
        lookback_days: 窗口大小，单位是**交易日**。
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


def resolve_expected() -> str:
    """独立成函数便于测试打桩。"""
    return get_expected_latest_trading_day()
