"""
core/parallel_runner.py
───────────────────────
流水线并发执行模块：提供多任务并发调度、线程隔离与异常容错。
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any

from core.runner import safe_task

logger = logging.getLogger(__name__)


@dataclass
class ParallelTask:
    """定义一个可并发执行的任务项。"""

    name: str
    fn: Callable[..., Any]
    args: tuple[Any, ...] = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)


def run_parallel_tasks(
    tasks: list[ParallelTask],
    max_workers: int = 3,
    thread_name_prefix: str = "pipeline-worker",
    runner_fn: Callable[..., Any] | None = None,
) -> dict[str, dict[str, Any]]:
    """并发安全执行一组任务，收集并返回标准化后的任务结果字典。

    参数：
        tasks: 待执行的 ParallelTask 列表
        max_workers: 最大并发线程数（<=1 则回退为串行执行）
        thread_name_prefix: 线程名称前缀
        runner_fn: 可选的任务安全执行包装器（默认 safe_task）

    返回：
        dict[task_name, result_dict]
    """
    if not tasks:
        return {}

    runner = runner_fn if runner_fn is not None else safe_task
    results: dict[str, dict[str, Any]] = {}

    # 串行模式降级分支
    if max_workers <= 1 or len(tasks) == 1:
        for task in tasks:
            results[task.name] = runner(
                task.name, task.fn, *task.args, **task.kwargs
            )
        return results

    # 并发执行模式
    workers = min(max_workers, len(tasks))
    logger.info(f"⚡ 启动并发阶段：共 {len(tasks)} 个任务，{workers} 个工作线程")

    with ThreadPoolExecutor(
        max_workers=workers, thread_name_prefix=thread_name_prefix
    ) as executor:
        future_to_task = {
            executor.submit(
                runner, task.name, task.fn, *task.args, **task.kwargs
            ): task
            for task in tasks
        }

        for future in as_completed(future_to_task):
            task = future_to_task[future]
            try:
                task_res = future.result()
                results[task.name] = task_res
            except Exception as e:
                logger.error(
                    f"❌ 并发线程异常未捕获 {task.name}: {e}", exc_info=True
                )
                results[task.name] = {
                    "status": "failed",
                    "error": str(e),
                    "task_name": task.name,
                }

    return results
