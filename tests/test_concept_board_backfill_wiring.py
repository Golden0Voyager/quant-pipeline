"""回补任务的接线门禁：它必须是手动任务，一不小心接进日常管道就是每晚
4536 次请求、约 22 分钟（默认窗口 10 → 9 个可用交易日 × 504 个概念）。

`core/runner.py` 的 cadence 过滤**不跳过** `ON_DEMAND` 任务，所以只靠注册表
的 cadence 字段并不能保证它不进 `run_all`——必须由门禁钉住。
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

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


def test_task_name_is_absent_from_run_all():
    """`run_all` 的函数体里不得出现这个任务名字符串。

    门禁的机制是**字面量扫描**，所以它保证的是「没有按名字把它接进 run_all」，
    而不是「run_all 不可能跑到它」——非字面量的组内派发（按 cadence 过滤出的
    列表再逐个跑）能绕过它。今天不是活风险：`run_all` 是纯字面量派发，且每处
    遍历注册表的派发都按 WEEKLY/MONTHLY 过滤，ON_DEMAND 匹配不上；真正兜底的是
    cadence 字段本身。名字照实写，免得门禁显得比它实际保证的更强。
    """
    assert TASK not in _run_all_task_names(), (
        f"{TASK} 出现在 run_all 里：core/runner.py 的 cadence 过滤"
        "不跳过 ON_DEMAND 任务，一旦接入就会每晚执行 4536 次请求"
    )


def test_task_is_dispatchable_via_task_callables():
    from daily_pipeline import _TASK_CALLABLES
    assert TASK in _TASK_CALLABLES


def test_lookback_flag_is_registered(monkeypatch, capsys):
    """`--lookback` 必须真的出现在 CLI 上（parser 局部于 main()）。

    **在进程内调 `main()`，不派生子进程。** 早先的写法是
    `subprocess.run([sys.executable, "daily_pipeline.py", "--help"])`，它在
    CI 上返回了空 stdout（本地同一条命令恒为 1700~2500 字节，随 COLUMNS 变化）。
    **根因未查明。** 已排除：`--help` 在 `parse_args()` 处 `SystemExit(0)`，
    早于 `_acquire_lock()`（`daily_pipeline.py:1310` vs `:1341`），故与 `/tmp`
    的 flock 无关——实测持锁状态下同一条命令仍输出正常字节；清空环境变量后
    本地亦正常。

    子进程写法真正的问题是它把「CLI 是否正常」与「子进程能否在测试环境里
    启动」耦在一起，而后者失败时只剩一句 `assert '--lookback' in ''`——退出码
    与 stderr 都被丢掉。进程内调用走仓库为这件事建的隔离
    （`tests/conftest.py::_isolate_pipeline_lock`），没有子进程可逃逸；本用例
    另显式 patch 一次，不依赖那个 autouse fixture 的存在。
    """
    import sys

    import daily_pipeline

    # conftest 的 autouse fixture 已经 patch 了这两处；这里再显式 patch 一次，
    # 使本用例自带隔离、不依赖那个 fixture 的存在。
    monkeypatch.setattr(daily_pipeline, "_acquire_lock", lambda: None)
    monkeypatch.setattr(daily_pipeline, "global_lock_held", lambda: False)
    monkeypatch.setattr(sys, "argv", ["daily_pipeline.py", "--help"])

    with pytest.raises(SystemExit) as excinfo:
        daily_pipeline.main()

    out = capsys.readouterr().out
    assert excinfo.value.code == 0, (
        f"main() 退出码 {excinfo.value.code}；stdout 长度={len(out)}，前 400 字={out[:400]!r}"
    )
    assert "--lookback" in out, (
        f"帮助文本里没有 --lookback；stdout 长度={len(out)}，前 400 字={out[:400]!r}"
    )
    assert "交易日" in out, "帮助文本必须写明单位是交易日，否则 --lookback 10 会被读成 10 天"
    # 排他闸使有效窗口比 lookback 少一天（get_recent_trading_days 含 expected
    # 再被丢掉），运维必须从 --help 就看得出 --lookback 1 补不了任何一天。
    assert "不补任何一天" in out, "帮助文本必须说明 --lookback 1 不补任何一天"


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


def _drive_main_for_task(monkeypatch, *extra_argv: str) -> list[tuple[str, dict]]:
    """按 `tests/test_daily_pipeline.py::TestMain` 的既定模式驱动 `main()`。

    打桩 `ProviderFactory` 与 `sys.argv`、用 recorder 替掉 `_run_registry_task`，
    返回它每次被调用时的 `(任务名, kwargs)`。这里刻意**不**替 `_safe_task`：
    本组门禁只关心 main() 往派发层递了什么，safe_task 那一段由
    `test_single_task_dispatch_goes_through_safe_task` 单独守。
    """
    import sys
    from unittest.mock import MagicMock, patch

    import daily_pipeline

    calls: list[tuple[str, dict]] = []

    def _record(task_name, db, loader, engine, **kwargs):
        calls.append((task_name, kwargs))
        return {"status": "success", "saved": 0}

    # main() 的路由前提：任务名必须在 _TASK_CALLABLES 里（不 patch 整张表，
    # 免得把派发条件也一并抹掉）
    monkeypatch.setitem(
        daily_pipeline._TASK_CALLABLES, TASK, lambda db, **kw: {"saved": 0}
    )
    with patch.object(sys, "argv", ["daily_pipeline.py", "--task", TASK, *extra_argv]), \
         patch("daily_pipeline.ProviderFactory") as factory, \
         patch("daily_pipeline._run_registry_task", side_effect=_record):
        factory.get_db.return_value = MagicMock()
        factory.get_loader.return_value = MagicMock()
        factory.get_indicator_engine.return_value = MagicMock()
        daily_pipeline.main()
    return calls


def test_main_forwards_lookback_to_the_dispatcher(monkeypatch):
    """`main()` 必须把 `--lookback` 递给 `_run_registry_task`。

    这是窗口参数的两段传递里**上半段**。下半段（派发层 → 任务）由
    `test_run_registry_task_forwards_lookback` 守着；两段都缺，删掉
    `daily_pipeline.py:1439` 的 `lookback_days=args.lookback` 只会让上半段静默
    失效：`_run_registry_task` 用自己的默认值 10，于是 `--lookback 60` 照样只补
    9 天，操作员以为 23 天的债清了而实际一天没补——所以必须有这道门。
    """
    calls = _drive_main_for_task(monkeypatch, "--lookback", "60")
    assert [name for name, _ in calls] == [TASK]
    assert calls[0][1].get("lookback_days") == 60, (
        f"main() 没把 --lookback 递给派发层：{calls[0][1]}；"
        "下半段门禁只证明派发层会转发，证明不了 main() 真的递了"
    )


def test_main_without_lookback_uses_the_task_module_default(monkeypatch):
    """不带 `--lookback` 时，argparse 的默认值必须就是 `DEFAULT_LOOKBACK_DAYS`。

    TUI 从不传 `--lookback`（`tui/app.py:571-573` 只给 `--task` 与 `--force`），
    所以**每一次 TUI 点击用的都是这个默认值**。它若与任务模块的常量各写一份
    字面量，改窗口就只改到一半，且没有任何东西会红。
    """
    from tasks.concept_board_backfill import DEFAULT_LOOKBACK_DAYS

    calls = _drive_main_for_task(monkeypatch)
    assert calls[0][1].get("lookback_days") == DEFAULT_LOOKBACK_DAYS, (
        "CLI 默认窗口与 tasks.concept_board_backfill.DEFAULT_LOOKBACK_DAYS 不一致，"
        "TUI 每次点击都走这个默认值"
    )


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

    断言的是 `_build_single_tasks()` 的返回值，即 `on_mount` 原样交给
    `Select(options=...)` 的那一份，而不是渲染前的中间态 `_SINGLE_TASK_GROUPS`：
    后者看不到 `_build_single_tasks` 里任何新增的展开/过滤，选项在那里被筛掉时
    门禁仍然绿。
    """
    from tui.widgets.single_task import SingleTaskWidget

    options = SingleTaskWidget._build_single_tasks()
    labels = {task: label for label, task in options}
    assert TASK in labels, f"{TASK} 不在 TUI 单任务下拉里（只加标签不进组是无效的）"
    assert "概念板块缺口回补" in labels[TASK]
