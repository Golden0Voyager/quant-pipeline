# 三层次数据管道 + 元数据收敛 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将管道重构为每日抓取/每周补全/每月修复三层次入口，并把 tui.py 的 6 份任务/表元数据平行映射收敛到 core 单一来源。

**Architecture:** 方案 A（扩展现有管道）：`daily_pipeline.py` 新增 `weekly_backfill` / `monthly_repair` 两个 tier 入口，`all` 收窄为每日层别名；元数据移入 `core/task_registry.py` 与新模块 `core/freshness.py`，tui.py 改为纯消费方。

**Tech Stack:** Python 3.12、SQLite、Textual TUI、pytest（asyncio auto 模式）。

**Spec:** `docs/superpowers/specs/2026-08-01-three-tier-pipeline-design.md`

## Global Constraints

- 包管理 `uv pip install <pkg>`；运行 `uv run --extra dev pytest ...`（本 worktree 的 uv run 需带 `--extra dev` 才有 pytest）。
- 一文件一提交；commit message 中英双语（英文块在前）。
- 全部新逻辑 TDD：先写失败测试，看红，再实现。
- `TaskSpec` 不新增字段；registry 的 `callable=None` 占位设计不动
  （避免 registry 反向 import tasks 造成重依赖；`_TASK_CALLABLES` 不从
  registry 派生，改为加键集合一致性测试兜底——这是对 spec §3 末行的
  有意偏离，效果等价）。
- 市场时间一律 `core.market_time.shanghai_now()`，禁止 naive `datetime.now()` 做市场判断。
- 不修改 `safe_task` / refresh / 熔断现有语义。

## File Structure

- `core/task_registry.py` — 新增 `TABLE_LABELS`、`TABLE_LABELS_CN`、`TASK_GROUPS`、`CATCH_UP_TASK_ORDER` 常量与 `task_to_table()` 派生视图（自 tui.py 一次性搬入）。
- `core/freshness.py` — 新建。表新鲜度判定与补全计算：`WEEKLY_TABLES`/`MONTHLY_TABLES`/`QUARTERLY_TABLES`/`DELAYED_PUBLISH_TABLES` 常量、`normalize_date`、`get_latest_dates`、`get_daily_bars_coverage`、`date_status`、`status_for_table`、`compute_catch_up_tasks`（自 tui.py 搬入并改为 registry 驱动）。
- `daily_pipeline.py` — `daily` 正名与 `all` 别名、`weekly_backfill()`、`monthly_repair()`、`_run_repair_script()`、main() 接线、health_check fast 透传。
- `tasks/utility.py` — `health_check(db, fast=False)` 增加 page_count 估算模式。
- `tui.py` — 删除已搬走的映射/函数，改为 import core；新增「每周补全」「每月修复」两个 action。
- `tests/test_task_registry.py`（存在则扩展，否则新建）、`tests/test_freshness.py`（新建）、`tests/test_daily_pipeline.py`、`tests/test_tui.py`、`tests/test_health_check.py`（按现有文件名实况选择）。

---

### Task 1: registry 常量与 task_to_table 派生视图

**Files:**
- Modify: `core/task_registry.py`（在 `table_date_columns()` 之后追加）
- Test: `tests/test_task_registry.py`（不存在则新建）

**Interfaces:**
- Produces:
  - `core.task_registry.TABLE_LABELS: dict[str, str]`（英文/缩写标签，值自 tui.py:1557 的 `TABLE_LABELS` 逐字搬入）
  - `core.task_registry.TABLE_LABELS_CN: dict[str, str]`（自 tui.py:1614 逐字搬入）
  - `core.task_registry.TASK_GROUPS: dict[str, list[str]]`（自 tui.py:1420 逐字搬入）
  - `core.task_registry.CATCH_UP_TASK_ORDER: tuple[str, ...]`（自 tui.py:1906 `_CATCH_UP_TASK_ORDER` 逐字搬入并改名）
  - `core.task_registry.task_to_table() -> dict[str, list[str]]`

- [ ] **Step 1: 写失败测试**

```python
# tests/test_task_registry.py
"""core.task_registry 派生视图与展示元数据测试。"""
from __future__ import annotations

from core.task_registry import (
    CATCH_UP_TASK_ORDER,
    TABLE_LABELS,
    TABLE_LABELS_CN,
    TASK_GROUPS,
    TASK_REGISTRY,
    task_to_table,
    table_date_columns,
)


def test_task_to_table_matches_specs():
    mapping = task_to_table()
    for spec in TASK_REGISTRY:
        if spec.tables:
            assert list(spec.tables) == mapping[spec.name]
    assert mapping["update_bars"] == ["daily_bars"]


def test_labels_cover_all_date_tables():
    for table in table_date_columns():
        assert table in TABLE_LABELS, f"{table} 缺英文标签"
        assert table in TABLE_LABELS_CN, f"{table} 缺中文标签"


def test_task_groups_reference_registered_tasks():
    registered = {spec.name for spec in TASK_REGISTRY}
    for group, tasks in TASK_GROUPS.items():
        assert isinstance(group, str) and tasks
        for task in tasks:
            assert task in registered, f"{group} 组引用了未注册任务 {task}"


def test_catch_up_order_subset_of_registered():
    registered = {spec.name for spec in TASK_REGISTRY}
    for task in CATCH_UP_TASK_ORDER:
        assert task in registered
```

- [ ] **Step 2: 运行确认失败**

Run: `uv run --extra dev pytest tests/test_task_registry.py -q`
Expected: ImportError（常量尚不存在）

- [ ] **Step 3: 实现**

在 `core/task_registry.py` 的 `table_date_columns()` 函数后追加：

```python
def task_to_table() -> dict[str, list[str]]:
    """Derived view: ``{task_name: [tables]}`` for tasks that write tables."""
    return {spec.name: list(spec.tables) for spec in TASK_REGISTRY if spec.tables}


# ── presentation metadata (single source; consumed by tui.py) ─────────

# 以下常量自 tui.py 一次性搬入（2026-08-01 元数据收敛），键为表名/组名。
TABLE_LABELS: dict[str, str] = {
    # 逐字搬入 tui.py 原 TABLE_LABELS 的全部 42 个条目
}
TABLE_LABELS_CN: dict[str, str] = {
    # 逐字搬入 tui.py 原 TABLE_LABELS_CN 的全部条目
}
TASK_GROUPS: dict[str, list[str]] = {
    # 逐字搬入 tui.py 原 TASK_GROUPS 的全部条目
}
CATCH_UP_TASK_ORDER: tuple[str, ...] = (
    # 逐字搬入 tui.py 原 _CATCH_UP_TASK_ORDER 的全部条目
)
```

把 tui.py 对应四个对象的条目**原样复制**进上述骨架（顺序、注释一并保留）。
若 `test_labels_cover_all_date_tables` 对个别表失败，以 tui.py 原映射为准
保留该表标签，并在测试中对该表显式豁免注释说明（不得静默删表）。

- [ ] **Step 4: 运行确认通过**

Run: `uv run --extra dev pytest tests/test_task_registry.py -q`
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add core/task_registry.py && git commit -m "feat: centralize task/table presentation metadata in registry

将 TABLE_LABELS/TABLE_LABELS_CN/TASK_GROUPS/CATCH_UP_TASK_ORDER 自 tui.py 一次性搬入 core/task_registry，新增 task_to_table 派生视图，作为元数据单一来源。"
git add tests/test_task_registry.py && git commit -m "test: cover registry presentation metadata and task_to_table

覆盖标签覆盖度、分组引用合法性、catch-up 顺序注册性与 task_to_table 派生一致性。"
```

---

### Task 2: core/freshness.py 新鲜度与补全计算

**Files:**
- Create: `core/freshness.py`
- Test: `tests/test_freshness.py`

**Interfaces:**
- Consumes: `core.task_registry.table_date_columns()`、`task_to_table()`、`CATCH_UP_TASK_ORDER`（Task 1）
- Produces（签名与 tui.py 现状完全一致，TUI 无缝切换）:
  - `WEEKLY_TABLES: set[str]`、`MONTHLY_TABLES: set[str]`、`QUARTERLY_TABLES: set[str]`、`DELAYED_PUBLISH_TABLES: set[str]`（自 tui.py 逐字搬入）
  - `normalize_date(value: object) -> str | None`
  - `get_latest_dates(db_path: str) -> dict[str, str | None]`
  - `get_daily_bars_coverage(db_path: str, expected_date: str) -> tuple[int, int]`
  - `date_status(latest: str | None, expected: str) -> str`
  - `status_for_table(table: str, latest: str | None, expected_date: str, coverage: tuple[int, int] | None) -> str`
  - `compute_catch_up_tasks(latest_dates: dict[str, str | None], expected_date: str) -> list[str]`

- [ ] **Step 1: 写失败测试**

把 `tests/test_tui.py` 中 `TestComputeCatchUpTasks` 整个类、`test_get_latest_dates`、
`test_get_daily_bars_coverage`（如存在）**复制**到 `tests/test_freshness.py`，
import 改为 `from core.freshness import ...`，并新增：

```python
def test_date_status_boundaries():
    from core.freshness import date_status
    assert date_status(None, "2026-07-31") == "无数据"
    assert date_status("2026-07-31", "2026-07-31") == "最新"
    # 其余分支以 tui._date_status 现有返回值为基准逐条断言


def test_status_for_table_weekly_and_delayed():
    from core.freshness import status_for_table
    # stock_pledge 为按周更新表：任何日期都返回「按周更新」
    assert status_for_table("stock_pledge", "2020-01-01", "2026-07-31", None) == "按周更新"
    # T+1 表在只差一天时返回「T+1」而非滞后
    assert status_for_table("fx_rate", "2026-07-30", "2026-07-31", None) == "T+1"
```

注：`date_status`/`status_for_table` 的具体期望值以实现时从 tui.py 原函数
读出的行为为准，测试先按原行为写死，保证搬运零语义变化。

- [ ] **Step 2: 运行确认失败**

Run: `uv run --extra dev pytest tests/test_freshness.py -q`
Expected: ImportError: core.freshness

- [ ] **Step 3: 实现**

创建 `core/freshness.py`：

```python
"""
表新鲜度判定与补全计算
──────────────────────
数据完整度面板与每周补全层共用的单一实现（2026-08-01 自 tui.py 收敛）。
判定语义与 tui.py 原实现完全一致，仅数据源从本地常量切换为
core.task_registry 派生视图。
"""
from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path

from core.task_registry import (
    CATCH_UP_TASK_ORDER,
    table_date_columns,
    task_to_table,
)

# 以下四个集合逐字搬入 tui.py 原 WEEKLY_TABLES / MONTHLY_TABLES /
# QUARTERLY_TABLES / DELAYED_PUBLISH_TABLES
WEEKLY_TABLES: set[str] = { ... }
MONTHLY_TABLES: set[str] = { ... }
QUARTERLY_TABLES: set[str] = { ... }
DELAYED_PUBLISH_TABLES: set[str] = { ... }
```

随后逐字搬入 tui.py 的 `_normalize_date`（改名 `normalize_date`，保留原名别名
一行 `_normalize_date = normalize_date` 不需要——tui 侧统一改 import）、
`get_latest_dates`（内部 `TABLE_DATE_COLUMNS` 引用改为 `table_date_columns()`）、
`get_daily_bars_coverage`、`_date_status`（改名 `date_status`）、
`DataCompletenessWidget._get_status_for_table` 的判定主体（改名
`status_for_table`，静态方法、同签名）、`compute_catch_up_tasks`
（内部 `DataCompletenessWidget._get_status_for_table` 改 `status_for_table`，
`TASK_TO_TABLE` 改 `task_to_table()`，`_CATCH_UP_TASK_ORDER` 改
`CATCH_UP_TASK_ORDER`）。

- [ ] **Step 4: 运行确认通过**

Run: `uv run --extra dev pytest tests/test_freshness.py -q`
Expected: all passed

- [ ] **Step 5: Commit**

```bash
git add core/freshness.py && git commit -m "feat: add core.freshness as single source for staleness judgement

新建 core/freshness.py，表新鲜度集合、日期归一化、最新日期查询、状态判定与 compute_catch_up_tasks 自 tui.py 搬入并改为 registry 驱动，供 TUI 与每周补全层共用。"
git add tests/test_freshness.py && git commit -m "test: cover freshness judgement and catch-up computation

测试自 test_tui.py 平移并补充 date_status 边界与按周/T+1 表判定，锁定搬运零语义变化。"
```

---

### Task 3: tui.py 切换为消费方

**Files:**
- Modify: `tui.py`（删除已搬走对象，改 import）
- Test: `tests/test_tui.py`（import 同步更新；行为断言不变）

**Interfaces:**
- Consumes: Task 1/2 的全部产物。tui.py 中 `TABLE_DATE_COLUMNS` 改为
  `table_date_columns()` 的模块级一次性求值（`TABLE_DATE_COLUMNS = table_date_columns()`，
  保持 tui 内部引用零改动，只换来源）；`TABLE_LABELS`/`TABLE_LABELS_CN`/`TASK_GROUPS`
  直接 from import；`TASK_TO_TABLE` 改 `task_to_table()` 一次性求值；
  `_get_status_for_table` 委托 `status_for_table`；`compute_catch_up_tasks`
  改 from import。

- [ ] **Step 1: 先跑基线**

Run: `uv run --extra dev pytest tests/test_tui.py -q`
Expected: 全绿（基线，记录耗时与数量）

- [ ] **Step 2: 改 tui.py**

- 删除：`TABLE_DATE_COLUMNS` dict 字面量（873 行起）、`WEEKLY_TABLES`/
  `MONTHLY_TABLES`/`QUARTERLY_TABLES`/`DELAYED_PUBLISH_TABLES`、
  `_normalize_date`、`get_latest_dates`、`get_daily_bars_coverage`、
  `_date_status`、`TASK_GROUPS`、`_SINGLE_TASK_GROUPS` 中与 `TASK_GROUPS`
  重复的定义、`TASK_TO_TABLE`、`TABLE_LABELS`、`TABLE_LABELS_CN`、
  `_CATCH_UP_TASK_ORDER`、`compute_catch_up_tasks`。
- 新增 import：

```python
from core.freshness import (
    DELAYED_PUBLISH_TABLES,
    MONTHLY_TABLES,
    QUARTERLY_TABLES,
    WEEKLY_TABLES,
    compute_catch_up_tasks,
    date_status as _date_status,
    get_daily_bars_coverage,
    get_latest_dates,
    normalize_date as _normalize_date,
    status_for_table,
)
from core.task_registry import (
    CATCH_UP_TASK_ORDER as _CATCH_UP_TASK_ORDER,
    TABLE_LABELS,
    TABLE_LABELS_CN,
    TASK_GROUPS,
    table_date_columns,
    task_to_table,
)

TABLE_DATE_COLUMNS: dict[str, str] = table_date_columns()
```

- `DataCompletenessWidget.TASK_TO_TABLE = task_to_table()`（类属性赋值改派生）。
- `DataCompletenessWidget._get_status_for_table` 方法体改为
  `return status_for_table(table, latest, expected_date, coverage)`（保留
  方法壳，tui 内部既有调用零改动）。
- `_SINGLE_TASK_GROUPS`（下拉分组，含中文组名与 task 标签对）若与
  `TASK_GROUPS` 结构不同则**保留在 tui.py**（纯 UI 结构），但改为从
  `TASK_GROUPS` 派生：组序与成员以 `TASK_GROUPS` 为准，标签对从
  现有下拉构造逻辑生成。若派生代价高于收益，保留原样并在注释中说明
  「以 TASK_GROUPS 为准的唯一结构差异是标签对，属 UI 层」。
- tui.py 内部对搬走函数的引用全部指向新 import（别名保持原名，
  调用点零改动）。

- [ ] **Step 3: 同步 tests/test_tui.py 的 import**

测试里 `from tui import ...` 搬走对象的，改为从 core 导入或经 tui 别名
导入（tui 仍 re-export 的不变）；`TestComputeCatchUpTasks` 已在 Task 2
平移到 test_freshness.py，test_tui.py 中删除原类。

- [ ] **Step 4: 全量 tui 测试**

Run: `uv run --extra dev pytest tests/test_tui.py tests/test_freshness.py tests/test_task_registry.py -q`
Expected: 全绿（与基线数量一致，平移的测试在 test_freshness.py 中计数）

- [ ] **Step 5: Commit（两个文件分别提交）**

```bash
git add tui.py && git commit -m "refactor: consume task/table metadata from core registry and freshness

tui.py 删除 6 份平行映射约 300 行，TABLE_DATE_COLUMNS/标签/分组/catch-up 全部改由 core.task_registry 与 core.freshness 供给，面板语义不变。"
git add tests/test_tui.py && git commit -m "test: align tui tests with metadata centralization

compute_catch_up 测试平移至 test_freshness.py，import 同步到 core 来源，行为断言不变。"
```

---

### Task 4: 每日层正名（`daily` 入口，`all` 收窄为别名）

**Files:**
- Modify: `daily_pipeline.py`（`update_daily_core` 定义处、main() 的
  `task in ("all", "update_daily_core")` 分支与 `_TASK_CALLABLES` 注册行）
- Test: `tests/test_daily_pipeline.py`

**Interfaces:**
- Produces: CLI `--task daily`；`--task all`、`--task update_daily_core` 为其别名，
  三者均执行 `update_daily_core(db, loader, engine, resume, force)`
  （TRADING_DAY + DAILY + ON_DEMAND）。

- [ ] **Step 1: 写失败测试**

```python
class TestDailyTierEntry:
    def test_task_daily_dispatches_to_daily_core(self, weekday_mock):
        with patch.object(sys, "argv", ["daily_pipeline.py", "--task", "daily"]), \
             patch("daily_pipeline.ProviderFactory") as f, \
             patch("daily_pipeline.update_daily_core") as fn, \
             patch("daily_pipeline._acquire_lock"), \
             patch("daily_pipeline._release_lock", create=True), \
             patch.dict("daily_pipeline._TASK_CALLABLES", {"update_daily_core": fn}), \
             patch("daily_pipeline.logger"):
            f.configure.return_value = None
            f.get_db.return_value = MagicMock()
            f.get_loader.return_value = MagicMock()
            f.get_indicator_engine.return_value = MagicMock()
            fn.return_value = {"crashed": False}
            daily_pipeline.main()
            fn.assert_called_once()

    def test_task_all_is_daily_alias(self, weekday_mock):
        """all 收窄为每日层别名：不再跑 WEEKLY/MONTHLY/QUARTERLY 任务。"""
        with patch.object(sys, "argv", ["daily_pipeline.py", "--task", "all"]), \
             patch("daily_pipeline.ProviderFactory") as f, \
             patch("daily_pipeline.update_daily_core") as fn, \
             patch("daily_pipeline._acquire_lock"), \
             patch("daily_pipeline._release_lock", create=True), \
             patch.dict("daily_pipeline._TASK_CALLABLES", {"update_daily_core": fn}), \
             patch("daily_pipeline.logger"):
            f.configure.return_value = None
            f.get_db.return_value = MagicMock()
            f.get_loader.return_value = MagicMock()
            f.get_indicator_engine.return_value = MagicMock()
            fn.return_value = {"crashed": False}
            daily_pipeline.main()
            fn.assert_called_once()
```

（`_release_lock` 是否存在以实现时 daily_pipeline.py 实况为准；
不存在则去掉对应 patch。）

- [ ] **Step 2: 运行确认失败**

Run: `uv run --extra dev pytest tests/test_daily_pipeline.py -q -k DailyTierEntry`
Expected: `--task daily` 用例失败（未知任务退出码 1）

- [ ] **Step 3: 实现**

`daily_pipeline.py` main() 中：

```python
        if task in ("all", "daily", "update_daily_core"):
            _acquire_lock()
```

调度分支中 `elif task == "update_daily_core":` 改为：

```python
        elif task in ("all", "daily", "update_daily_core"):
            results = update_daily_core(db, loader, engine, resume=args.resume, force=args.force)
            if results.get("crashed"):
                sys.exit(1)
```

原 `if task == "all": results = run_all(...)` 分支删除（`all` 不再直调
`run_all` 全量）。docstring 用法区更新三入口说明。
注意：`test_main_exits_one_when_run_all_returns_crashed` 等以
`--task all` + patch `run_all` 的既有测试需同步改为 patch
`update_daily_core`（行为变更的预期内更新，逐条核对，不得改断言语义
以外的部分）。

- [ ] **Step 4: 运行确认通过**

Run: `uv run --extra dev pytest tests/test_daily_pipeline.py -q`
Expected: all passed

- [ ] **Step 5: Commit**

```bash
git add daily_pipeline.py && git commit -m "feat: make daily the canonical daily-tier entry, all its alias

--task daily 正名化每日层（TRADING_DAY+DAILY+ON_DEMAND）；--task all 收窄为其别名，不再陪跑周/月/季低频任务；update_daily_core 保留兼容别名。"
git add tests/test_daily_pipeline.py && git commit -m "test: cover daily tier entry and all-alias narrowing

覆盖 --task daily 与 --task all 均派发 update_daily_core；同步 all 语义变更影响的既有测试。"
```

---

### Task 5: 每周补全层 `weekly_backfill`

**Files:**
- Modify: `daily_pipeline.py`（新增函数 + main() 注册）
- Test: `tests/test_daily_pipeline.py`

**Interfaces:**
- Consumes: `core.freshness.compute_catch_up_tasks`、`get_latest_dates`、
  `core.calendar.get_expected_latest_trading_day`、`_run_registry_task`、
  `TASK_REGISTRY`、`Cadence`
- Produces: `weekly_backfill(db, loader, engine, force=False) -> dict`
  （含 `crashed` 键）；CLI `--task weekly_backfill`

- [ ] **Step 1: 写失败测试**

```python
class TestWeeklyBackfill:
    def _mocks(self):
        return MagicMock(), MagicMock(), MagicMock()

    def test_runs_weekly_cadence_tasks_then_catch_up_then_retry(self, weekday_mock):
        db, loader, engine = self._mocks()
        weekly = [s.name for s in daily_pipeline.TASK_REGISTRY
                  if s.cadence is daily_pipeline.Cadence.WEEKLY]
        calls: list[str] = []

        def fake_run(task_name, db, loader=None, engine=None, **kw):
            calls.append(task_name)
            return {"status": "ok"}

        with patch("daily_pipeline._run_registry_task", side_effect=fake_run), \
             patch("daily_pipeline.get_latest_dates", return_value={}), \
             patch("daily_pipeline.get_expected_latest_trading_day", return_value="2026-07-31"), \
             patch("daily_pipeline.compute_catch_up_tasks", return_value=["update_bars"]), \
             patch("daily_pipeline.notify_all"), \
             patch("daily_pipeline.logger"):
            results = daily_pipeline.weekly_backfill(db, loader, engine)

        # WEEKLY 任务全部执行；随后补全任务；retry 在补全之后；health 垫底
        for name in weekly:
            assert name in calls
        assert calls.index("update_bars") > max(calls.index(n) for n in weekly)
        assert calls.index("retry") > calls.index("update_bars")
        assert calls[-1] == "health_check"
        assert results["crashed"] is False

    def test_catch_up_failure_does_not_abort_tier(self, weekday_mock):
        db, loader, engine = self._mocks()
        with patch("daily_pipeline._run_registry_task", return_value={"status": "ok"}), \
             patch("daily_pipeline.get_latest_dates", side_effect=Exception("db gone")), \
             patch("daily_pipeline.get_expected_latest_trading_day", return_value="2026-07-31"), \
             patch("daily_pipeline.notify_all"), \
             patch("daily_pipeline.logger"):
            results = daily_pipeline.weekly_backfill(db, loader, engine)
        # 补全判定失败只跳过补全，不影响 WEEKLY 主体与 retry
        assert "retry" in results
        assert results["crashed"] is False

    def test_failed_task_marks_crashed_and_notifies(self, weekday_mock):
        db, loader, engine = self._mocks()
        with patch("daily_pipeline._run_registry_task",
                   return_value={"status": "failed", "error": "x"}), \
             patch("daily_pipeline.get_latest_dates", return_value={}), \
             patch("daily_pipeline.get_expected_latest_trading_day", return_value="2026-07-31"), \
             patch("daily_pipeline.compute_catch_up_tasks", return_value=[]), \
             patch("daily_pipeline.notify_all") as mock_notify, \
             patch("daily_pipeline.logger"):
            results = daily_pipeline.weekly_backfill(db, loader, engine)
        assert results["crashed"] is True
        assert mock_notify.call_args.args[0] == "error"
```

- [ ] **Step 2: 运行确认失败**

Run: `uv run --extra dev pytest tests/test_daily_pipeline.py -q -k WeeklyBackfill`
Expected: AttributeError: weekly_backfill

- [ ] **Step 3: 实现**

`daily_pipeline.py` 顶部 import 追加
`from core.freshness import compute_catch_up_tasks, get_latest_dates`，
新增：

```python
def weekly_backfill(
    db: DatabaseInterface,
    loader: DataLoaderInterface,
    engine: IndicatorEngineInterface,
    force: bool = False,
) -> dict:
    """每周数据补全层：WEEKLY 任务 → 补齐缺漏（stale 驱动）→ retry → health。"""
    start_time = time.time()
    logger.info("\n🧩 每周数据补全层启动 (WEEKLY + 补齐缺漏 + retry)")
    results: dict[str, Any] = {}

    # 1. WEEKLY cadence 任务（registry 顺序即声明顺序）
    for spec in TASK_REGISTRY:
        if spec.cadence is Cadence.WEEKLY:
            results[spec.name] = _run_registry_task(
                spec.name, db, loader, engine, force=force
            )

    # 2. 补齐缺漏：与完整度面板同一套 stale 语义，不限 cadence；
    #    判定失败（DB 不可读等）只跳过补全，不影响主体
    try:
        latest_dates = get_latest_dates(str(db.db_path))
        expected = get_expected_latest_trading_day()
        for task_name in compute_catch_up_tasks(latest_dates, expected):
            if task_name not in results:
                results[task_name] = _run_registry_task(
                    task_name, db, loader, engine, force=force
                )
    except Exception as e:
        logger.warning("⚠️ 补齐缺漏判定失败，跳过补全步骤: %s", e)

    # 3. 失败股票重抓 + 4. 健康报告
    results["retry"] = _run_registry_task("retry", db, loader, engine)
    results["health"] = _run_registry_task("health_check", db, loader, engine)

    db.close()
    elapsed = time.time() - start_time
    failed_tasks = sorted(
        k for k, v in results.items()
        if isinstance(v, dict)
        and v.get("status") in {"degraded", "failed", "aborted"}
    )
    results["crashed"] = bool(failed_tasks)
    if failed_tasks:
        notify_all("error", "每周补全完成（含失败任务）",
                   f"耗时 {elapsed / 60:.1f}min，失败任务: {', '.join(failed_tasks)}")
    else:
        notify_all("info", "每周补全全部完成", f"耗时 {elapsed / 60:.1f}min")
    return results
```

main()：`if task in ("all", "daily", "update_daily_core", "weekly_backfill", "monthly_repair"):`
走全局锁；调度分支加：

```python
        elif task == "weekly_backfill":
            results = weekly_backfill(db, loader, engine, force=args.force)
            if results.get("crashed"):
                sys.exit(1)
```

- [ ] **Step 4: 运行确认通过**

Run: `uv run --extra dev pytest tests/test_daily_pipeline.py -q -k "WeeklyBackfill"`
Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add daily_pipeline.py && git commit -m "feat: add weekly_backfill tier for completeness-driven backfill

每周补全层：WEEKLY cadence 任务 → compute_catch_up_tasks 驱动的缺漏补齐（判定失败仅跳过）→ retry → health，复用 _run_registry_task 统一派发与 notify_all 汇总。"
git add tests/test_daily_pipeline.py && git commit -m "test: cover weekly_backfill ordering and failure isolation

覆盖 WEEKLY→补全→retry→health 顺序、补全判定失败不中断、失败任务 crashed 汇总与 error 通知。"
```

---

### Task 6: 每月修复层 `monthly_repair`

**Files:**
- Modify: `daily_pipeline.py`
- Test: `tests/test_daily_pipeline.py`

**Interfaces:**
- Produces:
  - `_run_repair_script(script: str) -> dict`（子进程运行 scripts/ 下脚本，
    返回 `{"status": "ok", ...}` 或 `{"status": "failed", "error": ...}`）
  - `monthly_repair(db, loader, engine, force=False) -> dict`
  - CLI `--task monthly_repair`

- [ ] **Step 1: 写失败测试**

```python
class TestMonthlyRepair:
    def _mocks(self):
        return MagicMock(), MagicMock(), MagicMock()

    def test_runs_monthly_quarterly_tasks_then_repair_chain(self, weekday_mock):
        db, loader, engine = self._mocks()
        monthly = [s.name for s in daily_pipeline.TASK_REGISTRY
                   if s.cadence in (daily_pipeline.Cadence.MONTHLY,
                                    daily_pipeline.Cadence.QUARTERLY)]
        calls: list[str] = []
        scripts: list[str] = []

        with patch("daily_pipeline._run_registry_task",
                   side_effect=lambda n, *a, **k: (calls.append(n), {"status": "ok"})[1]), \
             patch("daily_pipeline._run_repair_script",
                   side_effect=lambda s: (scripts.append(s), {"status": "ok"})[1]), \
             patch("daily_pipeline.notify_all"), patch("daily_pipeline.logger"):
            results = daily_pipeline.monthly_repair(db, loader, engine)

        for name in monthly:
            assert name in calls
        assert scripts == ["backup_database.py", "reconcile_with_akshare.py",
                           "validate_and_vacuum.py"]
        assert calls[-1] == "health_check"
        assert results["crashed"] is False

    def test_backup_failure_aborts_repair_chain(self, weekday_mock):
        db, loader, engine = self._mocks()
        scripts: list[str] = []
        with patch("daily_pipeline._run_registry_task", return_value={"status": "ok"}), \
             patch("daily_pipeline._run_repair_script",
                   side_effect=lambda s: (scripts.append(s),
                                          {"status": "failed", "error": "disk full"})[1]), \
             patch("daily_pipeline.notify_all") as mock_notify, patch("daily_pipeline.logger"):
            results = daily_pipeline.monthly_repair(db, loader, engine)
        assert scripts == ["backup_database.py"]  # 不允许无备份修复
        assert results["crashed"] is True
        assert mock_notify.call_args.args[0] == "error"

    def test_reconcile_failure_continues_chain(self, weekday_mock):
        db, loader, engine = self._mocks()
        scripts: list[str] = []

        def fake_script(s):
            scripts.append(s)
            if s == "reconcile_with_akshare.py":
                return {"status": "failed", "error": "mismatch"}
            return {"status": "ok"}

        with patch("daily_pipeline._run_registry_task", return_value={"status": "ok"}), \
             patch("daily_pipeline._run_repair_script", side_effect=fake_script), \
             patch("daily_pipeline.notify_all"), patch("daily_pipeline.logger"):
            results = daily_pipeline.monthly_repair(db, loader, engine)
        assert scripts == ["backup_database.py", "reconcile_with_akshare.py",
                           "validate_and_vacuum.py"]
        assert results["crashed"] is True
```

- [ ] **Step 2: 运行确认失败**

Run: `uv run --extra dev pytest tests/test_daily_pipeline.py -q -k MonthlyRepair`
Expected: AttributeError: monthly_repair

- [ ] **Step 3: 实现**

`daily_pipeline.py` 顶部确认 `import subprocess`（无则添加），新增：

```python
_REPAIR_CHAIN: tuple[str, ...] = (
    "backup_database.py",
    "reconcile_with_akshare.py",
    "validate_and_vacuum.py",
)


def _run_repair_script(script: str) -> dict:
    """以子进程运行 scripts/ 下的修复脚本，返回 safe_task 兼容结果。"""
    path = Path(__file__).parent / "scripts" / script
    try:
        proc = subprocess.run(
            [sys.executable, str(path)],
            capture_output=True, text=True, timeout=3600,
        )
    except (OSError, subprocess.TimeoutExpired) as e:
        return {"status": "failed", "error": f"{script}: {e}"}
    if proc.returncode != 0:
        return {"status": "failed",
                "error": f"{script} exited {proc.returncode}: {proc.stderr[-500:]}"}
    return {"status": "ok", "output_tail": proc.stdout[-500:]}


def monthly_repair(
    db: DatabaseInterface,
    loader: DataLoaderInterface,
    engine: IndicatorEngineInterface,
    force: bool = False,
) -> dict:
    """每月数据修复层：MONTHLY/QUARTERLY 任务 → 备份→对账→vacuum → health。"""
    start_time = time.time()
    logger.info("\n🛠️ 每月数据修复层启动 (MONTHLY + QUARTERLY + 修复链)")
    results: dict[str, Any] = {}

    for spec in TASK_REGISTRY:
        if spec.cadence in (Cadence.MONTHLY, Cadence.QUARTERLY):
            results[spec.name] = _run_registry_task(
                spec.name, db, loader, engine, force=force
            )

    # 修复链：backup 失败则中止后续（不允许无备份修复），其余失败继续并汇总
    for script in _REPAIR_CHAIN:
        step = _run_repair_script(script)
        results[f"repair:{script}"] = step
        if script == "backup_database.py" and step.get("status") != "ok":
            logger.error("❌ 备份失败，中止修复链后续步骤")
            break

    results["health"] = _run_registry_task("health_check", db, loader, engine)

    db.close()
    elapsed = time.time() - start_time
    failed_tasks = sorted(
        k for k, v in results.items()
        if isinstance(v, dict)
        and v.get("status") in {"degraded", "failed", "aborted"}
    )
    results["crashed"] = bool(failed_tasks)
    if failed_tasks:
        notify_all("error", "每月修复完成（含失败步骤）",
                   f"耗时 {elapsed / 60:.1f}min，失败: {', '.join(failed_tasks)}")
    else:
        notify_all("info", "每月修复全部完成", f"耗时 {elapsed / 60:.1f}min")
    return results
```

main() 调度分支加：

```python
        elif task == "monthly_repair":
            results = monthly_repair(db, loader, engine, force=args.force)
            if results.get("crashed"):
                sys.exit(1)
```

- [ ] **Step 4: 运行确认通过**

Run: `uv run --extra dev pytest tests/test_daily_pipeline.py -q -k "MonthlyRepair"`
Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add daily_pipeline.py && git commit -m "feat: add monthly_repair tier orchestrating repair chain

每月修复层：MONTHLY/QUARTERLY 任务后编排 backup→reconcile→vacuum 子进程修复链（备份失败中止后续，其余失败继续并汇总），health 垫底并接 notify_all。"
git add tests/test_daily_pipeline.py && git commit -m "test: cover monthly_repair chain orchestration

覆盖任务→修复链→health 顺序、备份失败中止链、对账失败继续链三种路径。"
```

---

### Task 7: health_check fast 模式

**Files:**
- Modify: `tasks/utility.py:124`（`health_check` 签名与计数段）
- Modify: `daily_pipeline.py`（`run_all` 末尾 health 调用）
- Test: `tests/test_daily_pipeline.py`（health_check 测试所在文件，按现状）

**Interfaces:**
- Produces: `health_check(db: DatabaseInterface, fast: bool = False) -> dict`；
  fast=True 时 12 张表的 `COUNT(*)` 改 `PRAGMA page_count` 估算
  （`page_count * page_size / 500`，与 tui.get_all_table_counts 同口径），
  覆盖率/最新日期段不受影响。`--task health_check` 手动入口默认精确。

- [ ] **Step 1: 写失败测试**

```python
def test_health_check_fast_uses_page_estimate(tmp_path):
    """fast 模式不得对 daily_bars 执行 COUNT(*) 全表扫描。"""
    db_file = tmp_path / "h.db"
    conn = sqlite3.connect(str(db_file))
    for tbl in ("stock_list", "daily_bars", "indicators", "fund_flow",
                "fundamentals", "chip_distribution", "historical_valuation",
                "margin_trading", "dragon_tiger", "block_trade",
                "sector_fund_flow", "shareholder_count"):
        conn.execute(f"CREATE TABLE {tbl} (ts_code TEXT, trade_date TEXT)")
    conn.commit()
    conn.close()
    db = MagicMock()
    db.db_path = str(db_file)
    with patch("tasks.utility.get_expected_latest_trading_day", return_value="2026-07-31"), \
         patch("tasks.utility.logger"):
        result = health_check(db, fast=True)
    assert "report" in result
    assert result.get("status") != "failed"
```

（精确性断言：fast 报告中的行数允许与精确值不同——估算口径；
另补一条 `fast=False` 现行行为不变的回归断言。）

- [ ] **Step 2: 运行确认失败**

Run: `uv run --extra dev pytest tests/test_daily_pipeline.py -q -k health_check_fast`
Expected: TypeError: unexpected keyword 'fast'

- [ ] **Step 3: 实现**

`tasks/utility.py`：`def health_check(db: DatabaseInterface, fast: bool = False) -> dict:`。

**先读 `tui.py` 的 `get_all_table_counts(fast=True)` 确认估算口径**
（page_count 是库级指标，不是表级）。按该口径实现 fast 模式：

- 逐表 `COUNT(*)` 循环保持 `fast=False` 时不变；
- `fast=True` 时跳过逐表 COUNT 循环，改为在报告中输出一行全库估算
  （与 tui 面板同口径），逐表行数行标注 `(估算)` 或省略，
  覆盖率与最新日期段（都是 MAX 索引查询，本身很快）保持不变。
- 测试断言随之调整：fast 报告含估算行、不含逐表精确计数、
  结果结构（status/issues/report 键）与精确模式一致。

实现原则：语义诚实优先——page_count 给不出表级估算就不虚构，
不为凑测试写假数字。

`daily_pipeline.py` run_all 末尾：
`results["health"] = _run_task("health_check", health_check, db)` 改为
`results["health"] = _run_task("health_check", health_check, db, fast=True)`；
weekly/monthly 层的 `_run_registry_task("health_check", ...)` 调用经
`_run_registry_task` 增加 `health_check` 特判透传 `fast=True`
（参照现有 update_bars 特判模式新增一行分支）。

- [ ] **Step 4: 运行确认通过**

Run: `uv run --extra dev pytest tests/test_daily_pipeline.py -q -k health_check`
Expected: all passed

- [ ] **Step 5: Commit（两文件分别提交）**

```bash
git add tasks/utility.py && git commit -m "feat: add fast page-estimate mode to health_check

批量三层次入口的 health_check 改 page_count 估算，避免千万行大表 COUNT(*) 全表扫描；手动 --task health_check 保持精确口径。"
git add daily_pipeline.py && git commit -m "perf: use fast health_check in tier batch entries

run_all/weekly/monthly 层末尾的健康检查统一 fast=True；手动单任务入口不受影响。"
```

---

### Task 8: TUI 两个新入口按钮

**Files:**
- Modify: `tui.py`（BINDINGS、action、下拉/分组区入口）
- Test: `tests/test_tui.py`

**Interfaces:**
- Consumes: `_run_or_schedule(action_name, *args)`（既有）、
  `daily_pipeline.py --task weekly_backfill|monthly_repair`（Task 5/6）

- [ ] **Step 1: 写失败测试**

```python
@pytest.mark.asyncio
async def test_action_weekly_backfill_routes_to_schedule():
    app = PipelineApp()
    args_expected = (sys.executable,
                     str(Path(sys.modules["tui"].__file__).parent / "daily_pipeline.py"),
                     "--task", "weekly_backfill")
    with patch.object(app, "_run_or_schedule") as mock_sched:
        await app.action_weekly_backfill()
    mock_sched.assert_called_once_with("每周补全", *args_expected)


@pytest.mark.asyncio
async def test_action_monthly_repair_routes_to_schedule():
    app = PipelineApp()
    args_expected = (sys.executable,
                     str(Path(sys.modules["tui"].__file__).parent / "daily_pipeline.py"),
                     "--task", "monthly_repair")
    with patch.object(app, "_run_or_schedule") as mock_sched:
        await app.action_monthly_repair()
    mock_sched.assert_called_once_with("每月修复", *args_expected)
```

- [ ] **Step 2: 运行确认失败**

Run: `uv run --extra dev pytest tests/test_tui.py -q -k "weekly_backfill_routes or monthly_repair_routes"`
Expected: AttributeError: action_weekly_backfill

- [ ] **Step 3: 实现**

`tui.py` PipelineApp：

```python
    async def action_weekly_backfill(self) -> None:
        pipeline_path = str(Path(__file__).parent / "daily_pipeline.py")
        self._run_or_schedule(
            "每周补全", sys.executable, pipeline_path, "--task", "weekly_backfill",
        )

    async def action_monthly_repair(self) -> None:
        pipeline_path = str(Path(__file__).parent / "daily_pipeline.py")
        self._run_or_schedule(
            "每月修复", sys.executable, pipeline_path, "--task", "monthly_repair",
        )
```

BINDINGS 追加（键位避开现有占用，参照 BINDINGS 列表选未用键）：

```python
        Binding("w", "weekly_backfill", "Weekly Backfill", show=False),
        Binding("m", "monthly_repair", "Monthly Repair", show=False),
```

并在 HelpScreen 的快捷键说明与任务下拉的运维分组（`_UTILS` 或等效位置）
各加一行入口文案。

- [ ] **Step 4: 运行确认通过 + 键位无冲突回归**

Run: `uv run --extra dev pytest tests/test_tui.py -q`
Expected: all passed（含既有 binding 测试；若键位冲突按失败信息换键）

- [ ] **Step 5: Commit**

```bash
git add tui.py && git commit -m "feat: add weekly backfill and monthly repair entries to TUI

新增 W/M 快捷键与下拉入口，复用 _run_or_schedule 弹窗与 _run_and_report 完成报告。"
git add tests/test_tui.py && git commit -m "test: cover weekly/monthly action routing

覆盖两个新 action 以正确动作名与 CLI 参数路由到 _run_or_schedule。"
```

---

### Task 9: 文档同步

**Files:**
- Modify: `daily_pipeline.py` docstring 用法区（Task 4 已含，复核即可）
- Modify: `scripts/launchd/README.md` 分工表
- Modify: `AGENTS.md`（如三入口属新约定）

- [ ] **Step 1: 更新文档**

- `scripts/launchd/README.md` 分工表加一行：launchd 每日 20:30 跑的
  `--task all` 现为每日层别名；每周/每月层为手动触发
  （`uv run python daily_pipeline.py --task weekly_backfill|monthly_repair`
  或 TUI W/M 键）。
- `AGENTS.md`「Key Conventions」如合适加一行三层次入口说明。

- [ ] **Step 2: Commit（每文件单独提交）**

```bash
git add scripts/launchd/README.md && git commit -m "docs: document three-tier entries in launchd README

分工表说明 all 为每日层别名、每周/每月层手动触发方式。"
git add AGENTS.md && git commit -m "docs: note three-tier pipeline entries in conventions

记录 daily/weekly_backfill/monthly_repair 三层次入口约定。"
```

---

### Task 10: 全量回归与收尾

- [ ] **Step 1: 全量校验**

Run: `uv run --extra dev ruff check .`
Run: `uv run --extra dev mypy --explicit-package-base . 2>&1 | tail -3`（与 main 对比无新增错误）
Run: `uv run --extra dev pytest -q --ignore=tests/test_tui_tmux.py`
Expected: ruff 干净；mypy 无新增；pytest 全绿

- [ ] **Step 2: 防漂移核对**

确认 `test_phase2_event_tables_are_registered_everywhere` 等防漂移测试
在元数据收敛后仍然有效（断言方向已随 Task 3 调整），无被静默删除的覆盖。

## Self-Review 记录

- spec §1 三入口 → Task 4/5/6；§1.5 TUI → Task 8；§2 效率 → Task 7 +
  Task 4 收窄；§3 元数据收敛 → Task 1/2/3（`_TASK_CALLABLES` 派生一项
  有意偏离，Global Constraints 已说明理由与替代）；§4 错误处理 →
  Task 5/6 测试覆盖；§5 测试 → 各 Task TDD + Task 10；§6 兼容 →
  Task 4 别名与文档。
