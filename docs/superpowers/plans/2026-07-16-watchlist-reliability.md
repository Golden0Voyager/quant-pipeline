# Watchlist Reliability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make watchlist reconciliation failures visible and make the full test suite isolated from production logs, real network calls, and leaked coroutines.

**Architecture:** Extend `WatchlistSyncResult` with explicit success/error state and keep file parsing plus SQLite reconciliation inside one failure boundary. Tests use a temporary `QUANT_DB_PATH`, orchestration tests replace `_safe_task`, and coroutine-producing mocks close their inputs instead of suppressing warnings.

**Tech Stack:** Python 3.12, SQLite, Textual, pytest, unittest.mock.

## Global Constraints

- Preserve existing watchlist add/reactivate/deactivate behavior.
- Failed syncs must never produce a success notification.
- Status changes must update `updated_at`.
- Tests must not access external data APIs or `~/Code/quant_data/logs`.
- Do not hide coroutine warnings with pytest filters.
- Use `rtk` for shell verification commands.

---

### Task 1: Expose reconciliation failures and update audit timestamps

**Files:**
- Modify: `tui.py:515-617,1099-1107`
- Test: `tests/test_tui.py:641-876`

**Interfaces:**
- Produces: `WatchlistSyncResult(added, reactivated, deactivated, files, success, error)`.

- [ ] Add failing tests that use a database without a `watchlist` table and assert `success is False`, `error` is populated, and `_sync_watchlists()` sends a warning rather than a success notification.
- [ ] Add failing tests asserting reactivation and deactivation change `updated_at` from an old fixed value.
- [ ] Run the focused tests and confirm they fail for missing fields or unchanged timestamps.
- [ ] Extend `WatchlistSyncResult` with `success: bool` and `error: str | None`.
- [ ] Move TXT enumeration/read/parse into the guarded block, return explicit failure results, and update both status SQL statements with `updated_at=?`.
- [ ] Replace `_sync_watchlists()` exception suppression with explicit logging and `severity="warning"` failure notification.
- [ ] Run all `sync_watchlists` tests and confirm they pass.

### Task 2: Isolate tests from production state and real network calls

**Files:**
- Modify: `tests/conftest.py:1-40`
- Modify: `tests/test_daily_pipeline.py:591-620,2380-2408`

**Interfaces:**
- Produces: a test environment whose database and logs live below a temporary directory, and orchestration tests that do not execute task implementations.

- [ ] Add a failing test or assertion that records the production log mtime and proves a selected `run_all` orchestration test does not change it.
- [ ] In `tests/conftest.py`, create a temporary root before project imports and set `QUANT_DB_PATH` to `<temp-root>/quant_core.db`.
- [ ] Update both normal `run_all` tests to patch `daily_pipeline._safe_task` with a deterministic successful dictionary instead of executing real tasks.
- [ ] Run the affected tests while network access is unavailable and confirm they pass quickly without touching the production log.

### Task 3: Remove coroutine warning suppression and close mocked coroutines

**Files:**
- Modify: `tests/test_tui.py:445-458,842-895`
- Modify: `pyproject.toml:46-57`

**Interfaces:**
- Produces: warning-clean Textual tests without global RuntimeWarning filters.

- [ ] Remove the three `PipelineApp` RuntimeWarning filters from `pyproject.toml` and run the relevant tests to observe the un-awaited coroutine warnings.
- [ ] Add a shared test helper that closes coroutine arguments captured by mocked `asyncio.create_task` or `_create_background_task` calls.
- [ ] Apply it to run-later, on-mount, and stop-daemon tests; remove the duplicate on-mount test if it tests the same behavior.
- [ ] Run `tests/test_tui.py` with `-W error::RuntimeWarning` and confirm it passes without warning suppression.

### Task 4: Final verification

**Files:**
- Verify all changed files.

**Interfaces:**
- Produces: reviewable evidence that the fixes are complete.

- [ ] Run `rtk .venv/bin/ruff check .` and `rtk git diff --check`.
- [ ] Run `rtk uv run python -m pytest -q` and confirm zero warnings.
- [ ] Run `rtk uv run python -m pytest --cov -q` and confirm coverage remains at least 84%.
- [ ] Confirm no `*MagicMock*` files exist and the production log mtime did not change during isolated tests.
- [ ] Review `rtk git diff -- tui.py tests/conftest.py tests/test_tui.py tests/test_daily_pipeline.py pyproject.toml` for scope.
