"""
工具函数模块
────────────
提供管线使用的辅助工具函数。
"""

from __future__ import annotations

import contextlib
import logging
import os
import time
from datetime import datetime

from smartmoney_hunter.market_utils import is_beijing_stock

logger = logging.getLogger(__name__)


def should_skip_beijing(symbol: str) -> bool:
    """判断是否根据环境变量配置跳过北交所股票。"""
    include_bj = os.getenv("INCLUDE_BJ", "0").lower() in ("1", "true", "yes")
    if include_bj:
        return False
    return is_beijing_stock(symbol)


def infer_market(code: str) -> str:
    """根据股票代码前缀精确推断板块市场标识。"""
    if code.startswith("688"):
        return "star"
    if code.startswith(("4", "8", "920")):
        return "bj"
    if code.startswith("6") or code.startswith("90"):
        return "sh"
    if code.startswith(("300", "301")):
        return "gem"
    if code.startswith(("002", "003")):
        return "sme"
    if code.startswith(("000", "001")):
        return "sz"
    return "sz"


def lower_process_priority() -> None:
    """降低当前进程的优先级（nice +10）。"""
    with contextlib.suppress(OSError):
        os.nice(10)
    with contextlib.suppress(OSError, AttributeError):
        os.setpriority(os.PRIO_PROCESS, 0, 10)


def is_trading_day() -> bool:
    """判断今天是否为 A 股交易日（简化版，排除周末）。"""
    today = datetime.now()
    return today.weekday() < 5  # 周一到周五


def should_update() -> bool:
    """判断是否需要更新：周末跳过、盘中跳过、15:00~16:00 结算窗口跳过。"""
    now = datetime.now()
    if now.weekday() >= 5:
        logger.info("今天是周末，跳过更新")
        return False
    if 9 <= now.hour < 15:
        logger.info(f"当前时间 {now.hour}:{now.minute:02d}，盘中不执行（15:00 收盘后自动允许）")
        return False
    if now.hour == 15:
        logger.warning(
            f"当前时间 {now.hour}:{now.minute:02d}，收盘结算窗口（15:00~16:00），"
            "数据源可能不稳定。等到 16:00 后再运行，或使用 --force 跳过此检查"
        )
        return False
    return True


def sleep_with_progress(seconds: float, label: str = "等待") -> None:
    """带进度显示的 sleep。"""
    for i in range(int(seconds)):
        print(f"\r  {label}: {i + 1}/{int(seconds)}s", end="", flush=True)
        time.sleep(1)
    print()
