"""
任务安全执行模块
────────────────
提供 safe_task 包装器、TaskTimer 计时器和 TaskResult 结果归一化。
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from contextlib import contextmanager
from typing import Any

from core.task_result import ErrorKind, TaskResult, TaskStatus, normalize_task_result

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

    返回包含 status, error, elapsed 的结构化结果（``TaskResult.to_dict()``）。
    """
    task_start = time.time()
    try:
        logger.info(f"\n{'=' * 60}\n▶ 开始任务: {name}\n{'=' * 60}")
        raw = fn(*args, **kwargs)
        result = normalize_task_result(
            name, raw if isinstance(raw, dict | TaskResult) else {}
        )
        result.metadata["elapsed_seconds"] = round(time.time() - task_start, 3)

        if result.status in (TaskStatus.SUCCESS, TaskStatus.NO_DATA):
            logger.info(f"✅ 任务 {name} 完成，耗时 {result.metadata['elapsed_seconds']:.1f}s")
        else:
            logger.warning(
                f"⚠️ 任务 {name} [{result.status}] 耗时 "
                f"{result.metadata['elapsed_seconds']:.1f}s"
                + (f": {result.error}" if result.error else "")
            )
        return result.to_dict()
    except Exception as e:
        elapsed = time.time() - task_start
        logger.error(f"❌ 任务 {name} 异常终止 (耗时 {elapsed:.1f}s): {e}", exc_info=True)
        result = TaskResult.failed(
            name, ErrorKind.INTERNAL, str(e)[:2000],
        )
        result.metadata["elapsed_seconds"] = round(elapsed, 3)
        return result.to_dict()


def _task_result_has_errors(result: dict[str, Any]) -> bool:
    """Return True when a task completed but reported a non-empty error state.

    .. deprecated::
       Use ``normalize_task_result`` and check ``TaskResult.exit_failure``
       instead. Kept for existing callers during migration.
    """
    if result.get("error") or result.get("aborted"):
        return True
    failed = result.get("failed")
    if isinstance(failed, int | float) and failed > 0:
        return True
    failed_symbols = result.get("failed_symbols")
    return isinstance(failed_symbols, list) and len(failed_symbols) > 0
