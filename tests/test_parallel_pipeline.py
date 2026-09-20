"""
tests/test_parallel_pipeline.py
───────────────────────────────
测试流水线并发调度器 (core/parallel_runner.py) 的并发调度、线程安全与异常隔离。
"""

import sys
import time
from typing import Any
from unittest.mock import MagicMock, patch

import daily_pipeline
from core.parallel_runner import ParallelTask, run_parallel_tasks
from core.task_result import TaskResult


def dummy_task_fast(val: int) -> dict[str, Any]:
    time.sleep(0.01)
    return {"status": "success", "fetched": val, "saved": val}


def dummy_task_slow(val: int) -> dict[str, Any]:
    time.sleep(0.05)
    return {"status": "success", "fetched": val, "saved": val}


def dummy_task_failing() -> dict[str, Any]:
    raise RuntimeError("network failure simulation")


def dummy_task_taskresult() -> TaskResult:
    return TaskResult.success("custom_tr", fetched=10, saved=10)


def test_run_parallel_tasks_all_success():
    """并发执行多个成功任务，所有结果正确返回。"""
    tasks = [
        ParallelTask(name="task_1", fn=dummy_task_fast, args=(10,)),
        ParallelTask(name="task_2", fn=dummy_task_slow, args=(20,)),
        ParallelTask(name="task_3", fn=dummy_task_fast, args=(30,)),
    ]

    start = time.time()
    results = run_parallel_tasks(tasks, max_workers=3)
    elapsed = time.time() - start

    assert len(results) == 3
    assert results["task_1"]["status"] == "success"
    assert results["task_1"]["saved"] == 10
    assert results["task_2"]["status"] == "success"
    assert results["task_2"]["saved"] == 20
    assert results["task_3"]["status"] == "success"
    assert results["task_3"]["saved"] == 30

    # 并发耗时应接近最慢任务（~0.05s），而不是三者之和（~0.07s）
    assert elapsed < 0.15


def test_run_parallel_tasks_exception_isolation():
    """单个并发任务崩溃不影响其他任务，错误被捕获并标准化为 failed。"""
    tasks = [
        ParallelTask(name="task_ok", fn=dummy_task_fast, args=(1,)),
        ParallelTask(name="task_bad", fn=dummy_task_failing),
        ParallelTask(name="task_ok2", fn=dummy_task_fast, args=(2,)),
    ]

    results = run_parallel_tasks(tasks, max_workers=3)

    assert results["task_ok"]["status"] == "success"
    assert results["task_ok2"]["status"] == "success"
    assert results["task_bad"]["status"] == "failed"
    assert "network failure simulation" in results["task_bad"].get("error", "")


def test_run_parallel_tasks_taskresult_support():
    """支持直接返回 TaskResult 对象的任务。"""
    tasks = [
        ParallelTask(name="task_tr", fn=dummy_task_taskresult),
    ]

    results = run_parallel_tasks(tasks, max_workers=1)
    assert results["task_tr"]["status"] == "success"
    assert results["task_tr"]["saved"] == 10


def test_run_parallel_tasks_sequential_fallback():
    """max_workers=1 时回退为确定性串行执行。"""
    execution_order = []

    def task_a():
        execution_order.append("A")
        return {"status": "success"}

    def task_b():
        execution_order.append("B")
        return {"status": "success"}

    tasks = [
        ParallelTask(name="task_a", fn=task_a),
        ParallelTask(name="task_b", fn=task_b),
    ]

    results = run_parallel_tasks(tasks, max_workers=1)
    assert execution_order == ["A", "B"]
    assert results["task_a"]["status"] == "success"
    assert results["task_b"]["status"] == "success"


def test_run_parallel_tasks_empty_list():
    """空任务列表安全返回空字典。"""
    results = run_parallel_tasks([])
    assert results == {}


def test_run_all_parallel_execution(tmp_path):
    """测试 daily_pipeline.run_all 在默认并行模式下完整调度所有阶段。"""
    db = MagicMock()
    db.db_path = str(tmp_path / "quant_core.db")
    loader = MagicMock()
    engine = MagicMock()

    with patch("daily_pipeline._should_update", return_value=True), \
         patch("daily_pipeline._safe_task", return_value={"status": "ok"}) as safe_task, \
         patch("daily_pipeline.logger"):
        results = daily_pipeline.run_all(db, loader, engine, parallel_workers=4)

    assert results.get("crashed") is False
    assert "bars" in results
    assert "indicators" in results
    assert "global_assets" in results
    assert "gold_price" in results
    assert "fundamentals" in results
    assert "restricted_share" in results
    assert "chip_distribution_em" in results
    assert "health" in results
    assert safe_task.call_count > 25


def test_run_all_stage2_serial_stage4_parallel(tmp_path):
    """stage2 维持串行；stage4 写表不相交、已恢复并发（max_workers=workers）。"""
    db = MagicMock()
    db.db_path = str(tmp_path / "quant_core.db")

    with patch("daily_pipeline._should_update", return_value=True), \
         patch("daily_pipeline._safe_task", return_value={"status": "ok"}), \
         patch("daily_pipeline.run_parallel_tasks", return_value={}) as runner, \
         patch("daily_pipeline.logger"):
        daily_pipeline.run_all(db, MagicMock(), MagicMock(), parallel_workers=3)

    assert [call.kwargs["max_workers"] for call in runner.call_args_list] == [1, 3]


def test_run_all_sequential_flag(tmp_path):
    """测试 daily_pipeline.run_all 在 sequential=True 模式下顺序调度所有任务。"""
    db = MagicMock()
    db.db_path = str(tmp_path / "quant_core.db")
    loader = MagicMock()
    engine = MagicMock()

    with patch("daily_pipeline._should_update", return_value=True), \
         patch("daily_pipeline._safe_task", return_value={"status": "ok"}) as safe_task, \
         patch("daily_pipeline.logger"):
        results = daily_pipeline.run_all(db, loader, engine, sequential=True)

    assert results.get("crashed") is False
    assert "bars" in results
    assert "indicators" in results
    assert "global_assets" in results
    assert safe_task.call_count > 25


def test_main_cli_parallel_flags():
    """测试 main() CLI 参数解析 --sequential 与 --parallel-workers。"""
    with patch.object(sys, "argv", ["daily_pipeline.py", "--sequential", "--parallel-workers", "2"]), \
         patch("daily_pipeline.ProviderFactory") as f, \
         patch("daily_pipeline.update_daily_core", return_value={"bars": {"status": "ok"}}) as fn:
        f.configure.return_value = None
        f.get_db.return_value = db = MagicMock()
        f.get_loader.return_value = loader = MagicMock()
        f.get_indicator_engine.return_value = engine = MagicMock()
        daily_pipeline.main()
        fn.assert_called_once_with(db, loader, engine, resume=False, force=False, sequential=True, parallel_workers=2)
