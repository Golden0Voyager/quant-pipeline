# Watchlist Reconciliation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep TXT-managed watchlist rows synchronized with the complete current contents of `~/Code/quant_agents/watchlists/*.txt`, using soft deactivation and safe reactivation.

**Architecture:** `tui.sync_watchlists_from_files()` remains the synchronization boundary and performs one SQLite transaction that inserts, reactivates, and deactivates only `source_scan='watchlist_sync'` rows. Downstream consumers explicitly request `status='tracking'`, and TUI startup schedules synchronization regardless of whether a pipeline process is already running.

**Tech Stack:** Python 3.12, SQLite, Textual, pytest, unittest.mock.

## Global Constraints

- A missing watchlist directory must log a warning and make no database changes.
- An existing directory with no TXT files, or TXT files with no valid codes, explicitly deactivates all `watchlist_sync` rows.
- Records from other `source_scan` values must never be modified.
- Removed records use `status='inactive'`; present records use `status='tracking'`.
- All reconciliation writes occur in one transaction and roll back together on failure.
- Shell verification commands must be prefixed with `rtk`.

---

### Task 1: Reconcile TXT contents with watchlist rows

**Files:**
- Modify: `tui.py:531-585`
- Test: `tests/test_tui.py:641-688`

**Interfaces:**
- Consumes: `WATCHLIST_DIR`, `_code_to_ts_code(code: str) -> str | None`, SQLite table columns `ts_code`, `added_date`, `source_scan`, `status`.
- Produces: `WatchlistSyncResult(added: int, reactivated: int, deactivated: int, files: int)` and `sync_watchlists_from_files(db_path: str) -> WatchlistSyncResult`.

- [ ] **Step 1: Add failing tests for removal, reactivation, empty input, and source isolation**

Add a helper and tests to `tests/test_tui.py`:

```python
def _create_watchlist_db(path: str) -> None:
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE watchlist ("
        "ts_code TEXT PRIMARY KEY, added_date TEXT, "
        "source_scan TEXT, status TEXT)"
    )
    conn.commit()
    conn.close()


def test_sync_watchlists_deactivates_removed_txt_symbols(tmp_path, monkeypatch):
    from tui import sync_watchlists_from_files

    watch_dir = tmp_path / "watchlists"
    watch_dir.mkdir()
    (watch_dir / "main.txt").write_text("600000\n", encoding="utf-8")
    db_path = str(tmp_path / "test.db")
    _create_watchlist_db(db_path)
    conn = sqlite3.connect(db_path)
    conn.executemany(
        "INSERT INTO watchlist VALUES (?, '2026-07-16', 'watchlist_sync', 'tracking')",
        [("600000.SH",), ("000001.SZ",)],
    )
    conn.commit()
    conn.close()
    monkeypatch.setattr("tui.WATCHLIST_DIR", watch_dir)

    result = sync_watchlists_from_files(db_path)

    assert result.deactivated == 1
    conn = sqlite3.connect(db_path)
    statuses = dict(conn.execute("SELECT ts_code, status FROM watchlist"))
    conn.close()
    assert statuses == {"600000.SH": "tracking", "000001.SZ": "inactive"}


def test_sync_watchlists_reactivates_returning_symbol(tmp_path, monkeypatch):
    from tui import sync_watchlists_from_files

    watch_dir = tmp_path / "watchlists"
    watch_dir.mkdir()
    (watch_dir / "main.txt").write_text("000001\n", encoding="utf-8")
    db_path = str(tmp_path / "test.db")
    _create_watchlist_db(db_path)
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO watchlist VALUES "
        "('000001.SZ', '2026-07-01', 'watchlist_sync', 'inactive')"
    )
    conn.commit()
    conn.close()
    monkeypatch.setattr("tui.WATCHLIST_DIR", watch_dir)

    result = sync_watchlists_from_files(db_path)

    assert result.reactivated == 1
    conn = sqlite3.connect(db_path)
    status = conn.execute(
        "SELECT status FROM watchlist WHERE ts_code='000001.SZ'"
    ).fetchone()[0]
    conn.close()
    assert status == "tracking"


def test_sync_watchlists_empty_directory_deactivates_only_sync_source(tmp_path, monkeypatch):
    from tui import sync_watchlists_from_files

    watch_dir = tmp_path / "watchlists"
    watch_dir.mkdir()
    db_path = str(tmp_path / "test.db")
    _create_watchlist_db(db_path)
    conn = sqlite3.connect(db_path)
    conn.executemany(
        "INSERT INTO watchlist VALUES (?, '2026-07-16', ?, 'tracking')",
        [("600000.SH", "watchlist_sync"), ("000001.SZ", "manual")],
    )
    conn.commit()
    conn.close()
    monkeypatch.setattr("tui.WATCHLIST_DIR", watch_dir)

    result = sync_watchlists_from_files(db_path)

    assert result.deactivated == 1
    conn = sqlite3.connect(db_path)
    statuses = dict(conn.execute("SELECT ts_code, status FROM watchlist"))
    conn.close()
    assert statuses == {"600000.SH": "inactive", "000001.SZ": "tracking"}
```

- [ ] **Step 2: Run the new tests and verify RED**

Run:

```bash
rtk uv run python -m pytest -q \
  tests/test_tui.py::test_sync_watchlists_deactivates_removed_txt_symbols \
  tests/test_tui.py::test_sync_watchlists_reactivates_returning_symbol \
  tests/test_tui.py::test_sync_watchlists_empty_directory_deactivates_only_sync_source
```

Expected: FAIL because the current function returns a two-item tuple and never deactivates or reactivates rows.

- [ ] **Step 3: Implement the transactional reconciliation**

Add near `_code_to_ts_code` in `tui.py`:

```python
from typing import NamedTuple


class WatchlistSyncResult(NamedTuple):
    added: int
    reactivated: int
    deactivated: int
    files: int
```

Replace `sync_watchlists_from_files()` with logic equivalent to:

```python
def sync_watchlists_from_files(db_path: str) -> WatchlistSyncResult:
    watch_dir = Path(WATCHLIST_DIR)
    if not watch_dir.is_dir():
        logger.warning(f"自选股目录不存在: {watch_dir}")
        return WatchlistSyncResult(0, 0, 0, 0)

    txt_files = sorted(watch_dir.glob("*.txt"))
    desired_codes: set[str] = set()
    for fpath in txt_files:
        for raw_line in fpath.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            code = line.split("#")[0].split()[0].strip()
            ts_code = _code_to_ts_code(code)
            if ts_code:
                desired_codes.add(ts_code)

    conn = None
    try:
        conn = sqlite3.connect(db_path, timeout=5.0)
        cur = conn.cursor()
        existing = {
            row[0]: row[1]
            for row in cur.execute(
                "SELECT ts_code, status FROM watchlist "
                "WHERE source_scan = 'watchlist_sync'"
            )
        }
        today = datetime.now().strftime("%Y-%m-%d")
        added = reactivated = deactivated = 0

        for ts_code in sorted(desired_codes):
            if ts_code not in existing:
                cur.execute(
                    "INSERT OR IGNORE INTO watchlist "
                    "(ts_code, added_date, source_scan, status) "
                    "VALUES (?, ?, 'watchlist_sync', 'tracking')",
                    (ts_code, today),
                )
                added += max(cur.rowcount, 0)
            elif existing[ts_code] != "tracking":
                cur.execute(
                    "UPDATE watchlist SET status='tracking' "
                    "WHERE ts_code=? AND source_scan='watchlist_sync'",
                    (ts_code,),
                )
                reactivated += max(cur.rowcount, 0)

        removed_codes = set(existing) - desired_codes
        if removed_codes:
            placeholders = ",".join("?" for _ in removed_codes)
            cur.execute(
                f"UPDATE watchlist SET status='inactive' "
                f"WHERE source_scan='watchlist_sync' "
                f"AND status!='inactive' AND ts_code IN ({placeholders})",
                tuple(sorted(removed_codes)),
            )
            deactivated = max(cur.rowcount, 0)

        conn.commit()
        logger.info(
            "自选股同步完成: 新增 %s, 恢复 %s, 停用 %s, 来源 %s 个文件",
            added, reactivated, deactivated, len(txt_files),
        )
        return WatchlistSyncResult(added, reactivated, deactivated, len(txt_files))
    except Exception as exc:
        if conn is not None:
            conn.rollback()
        logger.warning(f"自选股同步失败: {exc}")
        return WatchlistSyncResult(0, 0, 0, len(txt_files))
    finally:
        if conn is not None:
            conn.close()
```

- [ ] **Step 4: Update existing sync tests for the named result**

Replace tuple unpacking such as:

```python
added, files = sync_watchlists_from_files(db_path)
```

with:

```python
result = sync_watchlists_from_files(db_path)
assert result.added == 2
assert result.files == 1
```

For the missing-directory test, also assert `result.deactivated == 0` to prove the safety rule.

- [ ] **Step 5: Run all synchronization tests and verify GREEN**

Run:

```bash
rtk uv run python -m pytest -q tests/test_tui.py -k 'code_to_ts_code or sync_watchlists'
```

Expected: all selected tests PASS with no warnings.

### Task 2: Restrict downstream processing to active watchlist rows

**Files:**
- Modify: `tasks/bars.py:157-160,391-398`
- Modify: `tasks/index_chain.py:135-140`
- Test: `tests/test_daily_pipeline.py`

**Interfaces:**
- Consumes: `DatabaseInterface.watchlist_get_all(status: str | None = None) -> pd.DataFrame`.
- Produces: bars and online chip tasks that ignore `inactive` watchlist rows.

- [ ] **Step 1: Write failing tests for active-status filtering**

Add to `tests/test_daily_pipeline.py`:

```python
def test_update_bars_requests_only_tracking_watchlist_rows():
    db = MagicMock()
    loader = MagicMock()
    db.get_stock_list.return_value = pd.DataFrame({"code": ["000001"]})
    db.watchlist_get_all.return_value = pd.DataFrame(columns=["ts_code"])
    db.get_latest_bar_date.return_value = datetime.now().strftime("%Y-%m-%d")

    with patch("tasks.bars.should_skip_beijing", return_value=False), \
         patch("tasks.bars.ProgressTracker.clear"), \
         patch("tasks.bars.logger"):
        daily_pipeline.update_bars(db, loader)

    db.watchlist_get_all.assert_called_once_with(status="tracking")
```

Add a focused SQLite test for `_get_chip_em_target_symbols()` that creates active and inactive watchlist rows and asserts only the active symbol appears when fallback sources are empty.

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
rtk uv run python -m pytest -q \
  tests/test_daily_pipeline.py::test_update_bars_requests_only_tracking_watchlist_rows
```

Expected: FAIL because `watchlist_get_all()` is currently called without a status.

- [ ] **Step 3: Implement active filtering**

In both watchlist reads in `tasks/bars.py`, use:

```python
watchlist_df = db.watchlist_get_all(status="tracking")
```

In `tasks/index_chain.py`, replace the watchlist query with:

```sql
SELECT DISTINCT ts_code
FROM watchlist
WHERE status = 'tracking'
ORDER BY ts_code
```

- [ ] **Step 4: Run focused downstream tests and verify GREEN**

Run:

```bash
rtk uv run python -m pytest -q \
  tests/test_daily_pipeline.py -k 'watchlist or chip_em_target'
```

Expected: all selected tests PASS.

### Task 3: Always schedule synchronization when TUI mounts

**Files:**
- Modify: `tui.py:1046-1065,1067-1075`
- Test: `tests/test_tui.py`

**Interfaces:**
- Consumes: `PipelineApp._create_background_task(coro)` and `sync_watchlists_from_files()`.
- Produces: TUI startup that schedules synchronization whether or not pipeline processes exist, plus notifications showing added/reactivated/deactivated counts.

- [ ] **Step 1: Write failing mount and notification tests**

Add to `tests/test_tui.py`:

```python
@pytest.mark.asyncio
async def test_on_mount_schedules_watchlist_sync_without_running_processes():
    app = PipelineApp()
    with patch("tui.find_running_pipeline_processes", return_value=[]), \
         patch.object(app, "_create_background_task") as create_task:
        await app.on_mount()

    create_task.assert_called_once()
    coro = create_task.call_args.args[0]
    coro.close()


@pytest.mark.asyncio
async def test_sync_watchlists_notification_reports_all_changes():
    app = PipelineApp()
    result = WatchlistSyncResult(added=1, reactivated=2, deactivated=3, files=4)
    with patch("tui.sync_watchlists_from_files", return_value=result), \
         patch.object(app, "notify") as notify:
        await app._sync_watchlists()

    message = notify.call_args.args[0]
    assert "新增 1" in message
    assert "恢复 2" in message
    assert "停用 3" in message
    assert "4 个文件" in message
```

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
rtk uv run python -m pytest -q \
  tests/test_tui.py::test_on_mount_schedules_watchlist_sync_without_running_processes \
  tests/test_tui.py::test_sync_watchlists_notification_reports_all_changes
```

Expected: first test FAILS because `on_mount()` returns before scheduling; second fails because the current function expects a two-item tuple.

- [ ] **Step 3: Move synchronization before the early return and update notification text**

At the beginning of `PipelineApp.on_mount()` schedule synchronization before checking processes:

```python
async def on_mount(self) -> None:
    self._create_background_task(self._sync_watchlists())
    processes = find_running_pipeline_processes()
    if not processes:
        return
    # existing process confirmation logic follows
```

Remove the old scheduling call at the bottom. Update `_sync_watchlists()` to use named fields:

```python
result = sync_watchlists_from_files(str(DEFAULT_DB_PATH))
if result.files or result.deactivated:
    self.notify(
        f"自选股同步完成: 新增 {result.added}, 恢复 {result.reactivated}, "
        f"停用 {result.deactivated}, 来源 {result.files} 个文件",
        timeout=4.0,
    )
```

- [ ] **Step 4: Run TUI tests and verify GREEN**

Run:

```bash
rtk uv run python -m pytest -q tests/test_tui.py
```

Expected: all tests PASS with no un-awaited coroutine warnings.

### Task 4: Final verification

**Files:**
- Verify all modified files.

**Interfaces:**
- Consumes: completed Tasks 1-3.
- Produces: merge-ready implementation evidence.

- [ ] **Step 1: Run targeted feature tests**

```bash
rtk uv run python -m pytest -q tests/test_tui.py tests/test_daily_pipeline.py -k 'watchlist or chip_em_target'
```

Expected: all selected tests PASS without warnings.

- [ ] **Step 2: Run static checks**

```bash
rtk .venv/bin/ruff check .
rtk git diff --check
```

Expected: both commands exit 0.

- [ ] **Step 3: Run the full test suite**

```bash
rtk uv run python -m pytest -q
```

Expected: all tests PASS with no new warnings and no files named `<MagicMock ...>` created in the repository root.

- [ ] **Step 4: Review the diff for scope**

```bash
rtk git diff -- tui.py tasks/bars.py tasks/index_chain.py tests/test_tui.py tests/test_daily_pipeline.py
```

Expected: changes are limited to reconciliation, active filtering, TUI scheduling, and their regression tests.
