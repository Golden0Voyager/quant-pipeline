"""
任务安全执行模块
────────────────
提供 safe_task 包装器和 TaskTimer 计时器。
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from contextlib import contextmanager
from typing import Any

logger = logging.getLogger(__name__)


@contextmanager
def task_timer(name: str):
    """
    任务计时上下文管理器。

    用法：
        with task_timer("my_task") as t:
            result = do_work()
        print(t["elapsed"]())  # 耗时秒数
    """
    start = time.time()
    yield {"name": name, "start": start, "elapsed": lambda: time.time() - start}


def safe_task(name: str, fn: Callable, *args: Any, **kwargs: Any) -> dict[str, Any]:
    """
    安全执行单个任务，异常时记录日志不影响后续任务。

    返回包含 status, error, elapsed 的结构化结果。
    """
    task_start = time.time()
    try:
        logger.info(f"\n{'=' * 60}\n▶ 开始任务: {name}\n{'=' * 60}")
        result = fn(*args, **kwargs)
        elapsed = time.time() - task_start
        if isinstance(result, dict) and _task_result_has_errors(result):
            result.setdefault("status", "completed_with_errors")
            logger.warning(f"⚠️ 任务 {name} 完成但存在错误，耗时 {elapsed:.1f}s")
        else:
            logger.info(f"✅ 任务 {name} 完成，耗时 {elapsed:.1f}s")
        return result
    except Exception as e:
        elapsed = time.time() - task_start
        logger.error(f"❌ 任务 {name} 异常终止 (耗时 {elapsed:.1f}s): {e}", exc_info=True)
        return {"error": str(e), "status": "crashed"}


def _task_result_has_errors(result: dict[str, Any]) -> bool:
    """Return True when a task completed but reported a non-empty error state."""
    if result.get("error") or result.get("aborted"):
        return True
    failed = result.get("failed")
    if isinstance(failed, int | float) and failed > 0:
        return True
    failed_symbols = result.get("failed_symbols")
    return isinstance(failed_symbols, list) and len(failed_symbols) > 0
