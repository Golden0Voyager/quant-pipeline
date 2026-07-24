"""
任务安全执行模块
────────────────
提供 safe_task 包装器、TaskTimer 计时器和 TaskResult 结果归一化。
"""

from __future__ import annotations

import inspect
import logging
import time
import uuid
from collections.abc import Callable
from contextlib import contextmanager
from datetime import UTC, datetime
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


def _accepts_task_run_id(fn: Callable[..., Any]) -> bool:
    """Return whether *fn* safely accepts ``_task_run_id`` as a keyword."""
    try:
        parameters = inspect.signature(fn).parameters
    except (TypeError, ValueError):
        return False

    run_id_parameter = parameters.get("_task_run_id")
    if (
        run_id_parameter is not None
        and run_id_parameter.kind is not inspect.Parameter.POSITIONAL_ONLY
    ):
        return True
    return any(
        parameter.kind is inspect.Parameter.VAR_KEYWORD
        for parameter in parameters.values()
    )


def safe_task(name: str, fn: Callable, *args: Any, **kwargs: Any) -> dict[str, Any]:
    """
    安全执行单个任务，异常时记录日志不影响后续任务。

    返回包含 status, error, elapsed 的结构化结果（``TaskResult.to_dict()``），
    并把结果写入 ``ingestion_runs`` 审计表（如果第一个参数提供 ``record_ingestion_run``）。
    """
    task_start = time.time()
    run_id = str(uuid.uuid4())
    db = args[0] if args and hasattr(args[0], "record_ingestion_run") else None

    def _record(result: TaskResult) -> None:
        if db is None:
            return
        try:
            elapsed = result.metadata.get("elapsed_seconds", 0)
            finished_at = datetime.now(UTC)
            started_at = datetime.fromtimestamp(
                time.time() - elapsed, tz=UTC
            )
            result.metadata["run_id"] = run_id
            result.metadata["started_at"] = started_at.isoformat(timespec="seconds")
            result.metadata["finished_at"] = finished_at.isoformat(timespec="seconds")
            db.record_ingestion_run(result.to_dict())
        except Exception as exc:
            logger.warning("⚠️ 写入 ingestion_runs 审计表失败: %s", exc)

    # Fixed-signature legacy tasks intentionally run without this internal
    # keyword; compatible callbacks share the ID for PIT / audit writes.
    if _accepts_task_run_id(fn):
        kwargs.setdefault("_task_run_id", run_id)

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
        _record(result)
        return result.to_dict()
    except Exception as e:
        elapsed = time.time() - task_start
        logger.error(f"❌ 任务 {name} 异常终止 (耗时 {elapsed:.1f}s): {e}", exc_info=True)
        result = TaskResult.failed(
            name, ErrorKind.INTERNAL, str(e)[:2000],
        )
        result.metadata["elapsed_seconds"] = round(elapsed, 3)
        _record(result)
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
