"""回补任务的接线门禁：它必须是手动任务，一不小心接进日常管道就是 15 分钟。

`core/runner.py` 的 cadence 过滤**不跳过** `ON_DEMAND` 任务，所以只靠注册表
的 cadence 字段并不能保证它不进 `run_all`——必须由门禁钉住。
"""

from __future__ import annotations

import ast
from pathlib import Path

from core.task_registry import CATCH_UP_TASK_ORDER, lookup_task

TASK = "update_concept_board_backfill"
DAILY_PIPELINE = Path(__file__).resolve().parents[1] / "daily_pipeline.py"


def test_task_is_registered_as_on_demand():
    spec = lookup_task(TASK)
    assert spec is not None
    assert spec.cadence.value == "on_demand"
    assert spec.tables == ("concept_board",)
    assert spec.display_label


def test_task_is_absent_from_catch_up_order():
    """`compute_catch_up_tasks` 的 claimed 集合只允许一张表被一个任务认领。

    进了这个列表且排在 update_concept_board 之后，「补齐缺失」按钮会永远选到
    快照任务 → 只补今天 → 面板仍滞后 → 死循环。
    """
    assert TASK not in CATCH_UP_TASK_ORDER


def _run_all_task_names() -> set[str]:
    """静态扫出 `run_all` 函数体里出现的全部任务名字符串。

    比「只扫 `stage*_raw_tasks` 赋值」宽，是为了覆盖 run_all 里的**全部**派发
    形态，而不只是并发阶段那两个列表：串行分支与第三/第五阶段都是直接
    `_run_task("update_xxx", ...)` 调用（daily_pipeline.py:685-741、843-850、
    898-901），只认列表名的话，把任务加进串行分支就能绕过门禁。

    同时按**函数**而不是模块扫描：`update_daily_core` / `weekly_backfill` /
    `monthly_repair` 都在本文件里合法地引用别处注册的任务名，按模块扫会把
    本文件自己的 import 与派发表一起算进来，噪声大到看不出信号。
    """
    tree = ast.parse(DAILY_PIPELINE.read_text(encoding="utf-8"))
    fn = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "run_all"
    )
    return {
        node.value
        for node in ast.walk(fn)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    }


def test_task_is_not_wired_into_any_daily_stage():
    assert TASK not in _run_all_task_names(), (
        f"{TASK} 出现在 run_all 里：core/runner.py 的 cadence 过滤"
        "不跳过 ON_DEMAND 任务，一旦接入就会每晚执行 4536 次请求"
    )


def test_task_is_dispatchable_via_task_callables():
    from daily_pipeline import _TASK_CALLABLES
    assert TASK in _TASK_CALLABLES


def test_lookback_flag_is_registered():
    """`--lookback` 必须真的出现在 CLI 上（parser 局部于 main()，用 --help 探针）。"""
    import subprocess
    import sys

    root = Path(__file__).resolve().parents[1]
    out = subprocess.run(
        [sys.executable, str(root / "daily_pipeline.py"), "--help"],
        capture_output=True, text=True, cwd=root, timeout=120,
    ).stdout
    assert "--lookback" in out
    assert "交易日" in out, "帮助文本必须写明单位是交易日，否则 --lookback 10 会被读成 10 天"


def test_run_registry_task_forwards_lookback(monkeypatch):
    """--lookback 的值必须真的透传到任务，否则改窗口只会是个摆设。"""
    from unittest.mock import MagicMock

    from daily_pipeline import _TASK_CALLABLES, _run_registry_task

    seen = {}

    def _capture(db, **kwargs):
        seen.update(kwargs)
        return {"status": "success", "saved": 0}

    monkeypatch.setitem(_TASK_CALLABLES, TASK, _capture)
    _run_registry_task(TASK, MagicMock(), None, None, lookback_days=30)
    assert seen.get("lookback_days") == 30


def test_single_task_dispatch_goes_through_safe_task(monkeypatch):
    """`--task` 必须经 safe_task 派发，不能直调任务函数。

    整条状态设计都挂在这条路径上：只有 `core/runner.py:64` 的 `safe_task` 会
    ① 注入 `_task_run_id`、② 写 `ingestion_runs` 审计行、③ 在
    `status in (FAILED, RETAINED) and error_kind == NETWORK` 时隔 30s 重试
    （runner.py:139-149）。直调则三样全没有——本任务最常见的结局恰恰是
    `retained` + `error_kind=network`（源整体不可用，见 Task 3 的状态表），
    直调会让那 30s 重试彻底落空，22 分钟的一轮白跑且不留痕。
    """
    from unittest.mock import MagicMock

    import daily_pipeline
    from daily_pipeline import _TASK_CALLABLES

    calls: list[tuple[str, object, dict[str, object]]] = []

    def _fake_safe_task(name, fn, db, **kwargs):
        calls.append((name, fn, kwargs))
        return {"status": "success", "saved": 0}

    monkeypatch.setitem(_TASK_CALLABLES, TASK, lambda db, **kw: {"saved": 0})
    monkeypatch.setattr(daily_pipeline, "_safe_task", _fake_safe_task)
    daily_pipeline._run_registry_task(TASK, MagicMock(), None, None, lookback_days=10)

    assert len(calls) == 1, "任务没有经 safe_task 派发（--lookback 分支可能缺失）"
    name, fn, kwargs = calls[0]
    assert name == TASK
    assert fn is _TASK_CALLABLES[TASK]
    assert kwargs == {"lookback_days": 10}, (
        f"safe_task 收到的 kwargs 不对：{kwargs}；"
        "lookback_days 丢失则 --lookback 是个摆设"
    )


def test_tui_dropdown_offers_the_backfill():
    """TUI 单任务下拉必须能点到它，否则「手动回补」在 TUI 上不存在入口。

    成员与组序的唯一来源是 registry 的 `TASK_GROUPS`（`single_task.py:83-97`
    从它派生），`_SINGLE_TASK_LABELS` 只是**标签查表**。只加标签而不进组，
    派生出来的下拉里根本没有这个任务——加标签这件事会静默无效。
    """
    from tui.widgets.single_task import SingleTaskWidget

    entries = [
        (label, task)
        for _, tasks in SingleTaskWidget._SINGLE_TASK_GROUPS
        for label, task in tasks
    ]
    labels = {task: label for label, task in entries}
    assert TASK in labels, f"{TASK} 不在 TUI 单任务下拉里（只加标签不进组是无效的）"
    assert "概念板块缺口回补" in labels[TASK]
