"""
董监高/大股东增减持数据更新任务
─────────────────────────────
来源: stock_cgxq_em() (东方财富) — 接口已于 akshare 新版中移除。

当前任务返回 ``SOURCE_REMOVED`` 状态，避免 AttributeError。
"""

from __future__ import annotations

import logging

from interface import DatabaseInterface

logger = logging.getLogger(__name__)

SOURCE = "akshare"
TASK_NAME = "update_insider_trading"


def update_insider_trading(db: DatabaseInterface) -> dict:
    """返回 SOURCE_REMOVED — akshare 已移除 stock_cgxq_em 接口。"""
    logger.info("\n" + "=" * 60)
    logger.info("📋 任务: 更新董监高增减持数据")
    logger.info("=" * 60)
    logger.warning(
        "⚠️ akshare 已移除 stock_cgxq_em 接口，增减持数据暂无法获取"
    )
    return {
        "status": "failed",
        "error_kind": "source_removed",
        "error": "akshare 已移除 stock_cgxq_em 接口，无等效替代",
        "saved": 0,
    }
