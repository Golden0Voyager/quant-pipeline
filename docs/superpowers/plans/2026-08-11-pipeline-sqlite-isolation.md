# Pipeline SQLite Isolation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate cross-thread SQLite failures in daily runs and ensure the TUI attributes failed tasks only to the command that produced them.

**Architecture:** Existing task implementations receive a shared database provider and combine network fetch with SQLite writes. The daily pipeline will therefore run both current parallel stages on one database-writing lane. TUI audit lookup gains an inclusive `finished_at` lower bound, captured immediately before spawning a command. Audit inserts retry only SQLite lock errors with bounded backoff.

**Tech Stack:** Python 3, SQLite, pytest, Textual, `unittest.mock`.

## Global Constraints

- Use `uv run python`; never call `python` or `pip` directly.
- Do not make the shared `DatabaseManager` connection cross-thread by adding `check_same_thread=False`.
- Preserve prior data and explicit failed/degraded status for remote-source failures.
- Commit one changed file at a time with bilingual messages.

---

### Task 1: Serialize database-writing pipeline stages

**Files:**
- Modify: `daily_pipeline.py:562,625`
- Test: `tests/test_parallel_pipeline.py`

**Interfaces:**
- Consumes: `run_parallel_tasks(tasks, max_workers, runner_fn)`.
- Produces: stage 2 and stage 4 task results without a shared provider crossing worker threads.

- [ ] **Step 1: Write the failing test**

```python
def test_run_all_serializes_shared_db_parallel_stages(tmp_path):
    db = MagicMock()
    db.db_path = str(tmp_path / "quant_core.db")
    with patch("daily_pipeline._should_update", return_value=True), \
         patch("daily_pipeline._safe_task", return_value={"status": "ok"}), \
         patch("daily_pipeline.run_parallel_tasks") as runner:
        daily_pipeline.run_all(db, MagicMock(), MagicMock(), parallel_workers=3)
    assert [call.kwargs["max_workers"] for call in runner.call_args_list] == [1, 1]
```

- [ ] **Step 2: Run the focused test and verify it fails**

Run: `rtk uv run python -m pytest tests/test_parallel_pipeline.py::test_run_all_serializes_shared_db_parallel_stages -q`

Expected: FAIL because both stages receive `parallel_workers` rather than `1`.

- [ ] **Step 3: Write the minimal implementation**

```python
# Shared task implementations own SQLite writes; do not pass `db` into
# multiple worker threads until fetch and commit are separated.
stage2_ran = run_parallel_tasks(stage2_ptasks, max_workers=1, runner_fn=_safe_task)
stage4_ran = run_parallel_tasks(stage4_ptasks, max_workers=1, runner_fn=_safe_task)
```

- [ ] **Step 4: Run the focused test and verify it passes**

Run: `rtk uv run python -m pytest tests/test_parallel_pipeline.py::test_run_all_serializes_shared_db_parallel_stages -q`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
rtk git add daily_pipeline.py
rtk git commit -m "fix: serialize shared SQLite pipeline stages" -m "修复：串行执行共享 SQLite 的管道阶段"
```

### Task 2: Scope TUI failure details to the current command

**Files:**
- Modify: `tui/services/db_queries.py:137-171`
- Modify: `tui/app.py:112-131`
- Test: `tests/test_tui.py`

**Interfaces:**
- Consumes: `get_recent_failed_tasks(db_path, limit=10, finished_after=None)`.
- Produces: only audit records with `finished_at >= finished_after` when a command start time is supplied.

- [ ] **Step 1: Write the failing tests**

```python
def test_get_recent_failed_tasks_filters_before_command_start(tmp_path):
    # Seed one old and one new failed ingestion_runs row.
    rows = get_recent_failed_tasks(str(db_path), finished_after="2026-08-11T22:00:00")
    assert [row["task_name"] for row in rows] == ["update_chip_distribution_em"]

@pytest.mark.asyncio
async def test_run_and_report_passes_command_start_to_failed_query():
    app = PipelineApp()
    async with app.run_test():
        with patch.object(app, "_run_in_background", new=AsyncMock(return_value=1)), \
             patch("tui.get_recent_failed_tasks", return_value=[] ) as failed:
            await app._run_and_report("单任务: update_chip_distribution_em", "cmd")
    assert failed.call_args.kwargs["finished_after"]
```

- [ ] **Step 2: Run the focused tests and verify they fail**

Run: `rtk uv run python -m pytest tests/test_tui.py -k 'filters_before_command_start or passes_command_start' -q`

Expected: FAIL because the query has no `finished_after` parameter and the app does not pass one.

- [ ] **Step 3: Write the minimal implementation**

```python
def get_recent_failed_tasks(db_path: str, limit: int = 10, *, finished_after: str | None = None) -> list[dict[str, str]]:
    where = "WHERE status IN ('failed', 'degraded', 'aborted')"
    params: list[object] = []
    if finished_after is not None:
        where += " AND finished_at >= ?"
        params.append(finished_after)
    params.append(limit)
```

```python
started_at = datetime.now().isoformat(timespec="seconds")
rc = await self._run_in_background(*args)
failed = failed_getter(db_path, limit=5, finished_after=started_at)
```

- [ ] **Step 4: Run the focused tests and verify they pass**

Run: `rtk uv run python -m pytest tests/test_tui.py -k 'filters_before_command_start or passes_command_start' -q`

Expected: PASS.

- [ ] **Step 5: Commit one file at a time**

```bash
rtk git add tui/services/db_queries.py
rtk git commit -m "fix: filter TUI failed tasks by command start" -m "修复：按命令开始时间过滤 TUI 失败任务"
rtk git add tui/app.py
rtk git commit -m "fix: scope TUI failure details to current run" -m "修复：TUI 失败详情限定为本次运行"
```

### Task 3: Retry locked audit writes without changing task outcome

**Files:**
- Modify: `providers.py:104-110,663-725`
- Test: `tests/test_providers_extended2.py`

**Interfaces:**
- Consumes: `SmartMoneyDBProvider.record_ingestion_run(result)`.
- Produces: one audit row after a transient `sqlite3.OperationalError('database is locked')`; re-raises all other SQLite errors.

- [ ] **Step 1: Write the failing tests**

```python
def test_record_ingestion_run_retries_transient_database_lock(provider):
    conn = MagicMock()
    conn.execute.side_effect = [sqlite3.OperationalError("database is locked"), None]
    with patch.object(provider, "_connect_for_audit", return_value=_context_manager(conn)), \
         patch("providers.time.sleep") as sleep:
        provider.record_ingestion_run({"task_name": "x", "status": "success"})
    assert conn.execute.call_count == 2
    sleep.assert_called_once()

def test_record_ingestion_run_does_not_retry_non_lock_error(provider):
    conn = MagicMock()
    conn.execute.side_effect = sqlite3.OperationalError("no such table: ingestion_runs")
    with pytest.raises(sqlite3.OperationalError, match="no such table"):
        provider.record_ingestion_run({"task_name": "x", "status": "success"})
```

- [ ] **Step 2: Run the focused tests and verify they fail**

Run: `rtk uv run python -m pytest tests/test_providers_extended2.py -k 'ingestion_run_retries or ingestion_run_does_not_retry' -q`

Expected: the transient-lock test FAILS after one attempt.

- [ ] **Step 3: Write the minimal implementation**

```python
def _connect_for_audit(self) -> sqlite3.Connection:
    conn = sqlite3.connect(str(self._db.db_path), timeout=30.0)
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

for attempt in range(3):
    try:
        with self._connect_for_audit() as conn:
            conn.execute(sql, values)
            conn.commit()
        return
    except sqlite3.OperationalError as exc:
        if "locked" not in str(exc).lower() or attempt == 2:
            raise
        time.sleep(0.25 * (2 ** attempt))
```

- [ ] **Step 4: Run the focused tests and verify they pass**

Run: `rtk uv run python -m pytest tests/test_providers_extended2.py -k 'ingestion_run_retries or ingestion_run_does_not_retry' -q`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
rtk git add providers.py
rtk git commit -m "fix: retry transient SQLite audit locks" -m "修复：重试短暂的 SQLite 审计锁"
```

### Task 4: Verify the repair

**Files:**
- Test: `tests/test_parallel_pipeline.py`, `tests/test_tui.py`, `tests/test_providers_extended2.py`

- [ ] **Step 1: Run focused regressions**

Run: `rtk uv run python -m pytest tests/test_parallel_pipeline.py tests/test_tui.py tests/test_providers_extended2.py -q`

Expected: PASS.

- [ ] **Step 2: Run repository gates**

Run: `rtk uv run ruff check . && rtk uv run python -m pytest -m unit`

Expected: Ruff clean and unit tests pass.

- [ ] **Step 3: Verify the patch and worktree state**

Run: `rtk git diff --check && rtk git status --short`

Expected: no whitespace errors and only the documented implementation commits.
