# Bars Resume Result Contract Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 修复日线断点把 retry 队列误当成全市场扫描断点，以及成功任务被统一结果层误判失败的问题。

**Architecture:** 结果契约在任务生产者边界补齐，不放宽 `normalize_task_result` 的零行保护。`update_bars` 根据进度记录的 `task` 分流：扫描断点继续原列表，retry 断点只处理失败队列；`retry_failed` 使用同一套显式状态语义。

**Tech Stack:** Python 3.12、pytest、unittest.mock、pandas、Ruff、uv

## Global Constraints

- 所有命令使用 `rtk` 前缀；Python 与测试通过 `rtk uv run python <script-or-module>` 执行。
- 不改变 AkShare 优先级、禁用 yfinance 写入策略、限流或重试参数。
- 保留旧式 `success`、`failed`、`skipped`、`total`、`failed_symbols` 字段。
- Git 一文件一提交，提交信息英文在前、中文在后。
- 不改动用户已有的 `.DS_Store` 未跟踪文件。

---

## File Map

- `tests/test_bars_integration.py`：覆盖任务结果契约和 retry 断点路由。
- `tasks/bars.py`：实现进度类型分流及显式结构化结果。
- `tests/test_utility_integration.py`：覆盖 retry 空队列、成功和部分失败状态。
- `tasks/utility.py`：为 `retry_failed` 补齐结果契约。
- `tests/test_daily_pipeline.py`：锁定独立 CLI 的成功与降级退出码。

### Task 1: Update Bars Contract and Retry Resume Routing

**Files:**

- Modify: `tests/test_bars_integration.py`
- Modify: `tasks/bars.py`

**Interfaces:**

- Consumes: `ProgressTracker.load() -> dict[str, Any] | None`
- Produces: `update_bars(db, loader, limit=None, resume=False, symbols=None, force=False) -> dict`，包含 `status`、`saved`、`attempted` 和旧式计数字段。

- [ ] **Step 1: Write failing tests**

在 `TestBarsResume` 中增加 retry 断点测试，构造：

```python
{
    "task": "retry",
    "date": datetime.now().strftime("%Y-%m-%d"),
    "last_symbol": "000003.SZ",
    "processed": 3,
    "total": 3,
    "failed_queue": ["000002.SZ"],
}
```

补丁 `_update_single_bar` 返回 `"success"`，断言它只以 `000002.SZ` 调用一次，
结果为：

```python
assert result["status"] == "success"
assert result["saved"] == 1
assert result["attempted"] == 1
assert result["failed"] == 0
```

在完成路径测试中增加：

```python
normalised = normalize_task_result("update_bars", result)
assert normalised.status is TaskStatus.SUCCESS
```

- [ ] **Step 2: Verify RED**

Run:

```bash
rtk uv run python -m pytest -q \
  tests/test_bars_integration.py::TestBarsResume \
  tests/test_bars_integration.py::TestProgressFileManagement::test_all_succeed_clears_progress
```

Expected: FAIL，因为 retry 记录仍被作为全市场末尾断点，且结果缺少 `status/saved`。

- [ ] **Step 3: Commit the test file**

```bash
rtk git add tests/test_bars_integration.py
rtk git commit -m "test: cover retry queue resume contract" \
  -m "测试：覆盖失败队列续传结果契约"
```

- [ ] **Step 4: Implement minimal bars fix**

在 `tasks/bars.py` 增加私有结果构造器：

```python
def _bars_result(
    *,
    success: int,
    failed: int,
    skipped: int,
    total: int,
    attempted: int,
    failed_symbols: list[str],
) -> dict[str, Any]:
    status = "degraded" if failed else "success"
    result = {
        "status": status,
        "saved": success,
        "attempted": attempted,
        "success": success,
        "failed": failed,
        "skipped": skipped,
        "total": total,
        "failed_symbols": failed_symbols,
    }
    if failed:
        result["error"] = f"{failed} failures"
    return result
```

在恢复初始化中：

```python
if progress and progress.get("task") == "retry":
    retry_codes = [
        code for code in progress.get("failed_queue", [])
        if code in set(stock_codes)
    ]
    stock_codes = retry_codes
    total = len(stock_codes)
    progress = None
    logger.info("🔄 断点续传：仅重试失败队列 (%d 只)", total)
```

旧进度没有 `task` 时按扫描断点兼容；未知非空任务类型忽略其扫描位置。所有正常、
智能跳过、空列表和熔断返回点都提供显式状态，其中熔断为 `aborted`。

- [ ] **Step 5: Verify GREEN**

Run:

```bash
rtk uv run python -m pytest -q \
  tests/test_bars_integration.py \
  tests/test_bars_parallel_checkpoint.py \
  tests/test_bars_edge.py
```

Expected: PASS。

- [ ] **Step 6: Commit the production file**

```bash
rtk git add tasks/bars.py
rtk git commit -m "fix: route bars resume by progress task" \
  -m "修复：按进度任务类型路由日线续传"
```

### Task 2: Retry Failed Result Contract

**Files:**

- Modify: `tests/test_utility_integration.py`
- Modify: `tasks/utility.py`

**Interfaces:**

- Consumes: `_update_single_bar(db, loader, symbol) -> Literal["success", "skipped", "failed"]`
- Produces: `retry_failed(db, loader) -> dict`，显式区分 `no_data`、`success`、`degraded`。

- [ ] **Step 1: Write failing tests**

对已有空队列、全部成功、部分失败用例增加：

```python
assert result["status"] == "no_data"       # 空队列
assert result["status"] == "success"       # 全部成功
assert result["saved"] == result["success"]
assert result["attempted"] == result["total"]
assert result["status"] == "degraded"      # 仍有失败
assert result["error"] == "1 failures"
```

并对每个结果执行：

```python
assert normalize_task_result("retry_failed", result).status is expected_status
```

- [ ] **Step 2: Verify RED**

Run:

```bash
rtk uv run python -m pytest -q tests/test_utility_integration.py -k retry_failed
```

Expected: FAIL，旧结果没有显式状态。

- [ ] **Step 3: Commit the test file**

```bash
rtk git add tests/test_utility_integration.py
rtk git commit -m "test: define retry failed result statuses" \
  -m "测试：定义失败重试任务结果状态"
```

- [ ] **Step 4: Implement minimal utility fix**

空队列返回：

```python
{
    "status": "no_data",
    "reason": "retry queue empty",
    "saved": 0,
    "attempted": 0,
    "success": 0,
    "failed": 0,
    "total": 0,
}
```

非空队列返回：

```python
result = {
    "status": "degraded" if still_failed else "success",
    "saved": success,
    "attempted": len(symbols),
    "success": success,
    "failed": len(still_failed),
    "total": len(symbols),
}
if still_failed:
    result["error"] = f"{len(still_failed)} failures"
```

- [ ] **Step 5: Verify GREEN**

Run:

```bash
rtk uv run python -m pytest -q tests/test_utility_integration.py -k retry_failed
```

Expected: PASS。

- [ ] **Step 6: Commit the production file**

```bash
rtk git add tasks/utility.py
rtk git commit -m "fix: return explicit retry task outcomes" \
  -m "修复：返回明确的失败重试任务结果"
```

### Task 3: CLI Exit Regression and Full Verification

**Files:**

- Modify: `tests/test_daily_pipeline.py`

**Interfaces:**

- Consumes: `_run_registry_task(task_name, db, loader, engine, symbols=None, limit=None, resume=False, force=False) -> dict | TaskResult`
- Verifies: `main()` 对 `success/no_data` 正常返回，对 `degraded` 抛出 `SystemExit(1)`。

- [ ] **Step 1: Add CLI regression tests**

在 `TestMain` 中使用注册任务返回值覆盖两条路径：

```python
@pytest.mark.parametrize("status", ["success", "no_data"])
def test_direct_task_success_status_does_not_exit(self, weekday_mock, status):
    mock_fn = MagicMock(
        return_value={"status": status, "saved": 0, "reason": "current"}
    )
    with patch.object(
        sys, "argv", ["daily_pipeline.py", "--task", "update_bars"]
    ), patch("daily_pipeline.ProviderFactory") as factory, patch.dict(
        "daily_pipeline._TASK_CALLABLES", {"update_bars": mock_fn}
    ):
        factory.get_db.return_value = MagicMock()
        factory.get_loader.return_value = MagicMock()
        factory.get_indicator_engine.return_value = MagicMock()
        daily_pipeline.main()

def test_direct_task_degraded_status_exits_one(self, weekday_mock):
    mock_fn = MagicMock(
        return_value={
            "status": "degraded",
            "saved": 0,
            "error": "1 failures",
        }
    )
    with patch.object(
        sys, "argv", ["daily_pipeline.py", "--task", "update_bars"]
    ), patch("daily_pipeline.ProviderFactory") as factory, patch.dict(
        "daily_pipeline._TASK_CALLABLES", {"update_bars": mock_fn}
    ):
        factory.get_db.return_value = MagicMock()
        factory.get_loader.return_value = MagicMock()
        factory.get_indicator_engine.return_value = MagicMock()
        with pytest.raises(SystemExit) as exc_info:
            daily_pipeline.main()
    assert exc_info.value.code == 1
```

- [ ] **Step 2: Run CLI tests**

Run:

```bash
rtk uv run python -m pytest -q tests/test_daily_pipeline.py::TestMain
```

Expected: PASS；该层已有正确退出实现，新测试用于锁定修复后的任务返回值兼容性。

- [ ] **Step 3: Commit the CLI test file**

```bash
rtk git add tests/test_daily_pipeline.py
rtk git commit -m "test: lock direct task exit semantics" \
  -m "测试：锁定独立任务退出码语义"
```

- [ ] **Step 4: Run targeted verification**

```bash
rtk uv run python -m pytest -q \
  tests/test_bars_integration.py \
  tests/test_bars_parallel_checkpoint.py \
  tests/test_bars_edge.py \
  tests/test_utility_integration.py \
  tests/test_task_result.py \
  tests/test_daily_pipeline.py::TestMain
```

Expected: PASS。

- [ ] **Step 5: Run full verification**

```bash
rtk uv run python -m pytest -q
rtk uv run ruff check .
rtk git diff --check
```

Expected: pytest 与 Ruff 返回 0，diff 无空白错误。

- [ ] **Step 6: Check side effects**

```bash
rtk find . -maxdepth 1 -type f -name '*MagicMock*'
rtk git status --short
```

Expected: 没有 MagicMock 文件；只保留已知 `.DS_Store` 和本次计划内状态。
