# Ingestion Audit Integrity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make PIT snapshot writes and ingestion audit records transactionally referential, while allowing databases with either known migration-006 checksum to upgrade safely.

**Architecture:** `safe_task` creates the audit parent before callbacks that receive `_task_run_id`; `SmartMoneyDBProvider` upserts that parent and enables SQLite foreign keys before PIT writes. Published migration 006 is restored unchanged, while migration 008 repairs historical orphans and bridges the two known 006 checksums before MigrationEngine performs its final checksum verification.

**Tech Stack:** Python 3.12, SQLite, pytest, Ruff, `inspect.signature`

## Global Constraints

- Use `rtk` for every shell command and `uv` for Python execution.
- Never run `pip`; use the existing `uv` environment.
- Do not launch `pipe-tui` or any production data pipeline.
- Do not change data-source priority, AkShare calls, task order, retries, rate limiting, or TUI behavior.
- Preserve one-file-per-commit with an English subject and a Chinese body.
- Preserve fixed-signature task compatibility and the existing effective caller-provided run-ID behavior.
- Restore migration 006 to SHA-256 `a7830c97017b503c6d7b10412c901a4a4d365210704f8582521dfbe395e1e4eb`.
- Migration 008 may bridge only the published 006 checksum above and transitional checksum `8273ec2643335baacddcd6478d4fab032348e7cfd2346d03669dadf12a6e78b2`.
- Preserve the uncommitted lock-decorated regression test already present in `tests/test_core_infrastructure.py`; replace the redundant uncommitted `inspect.unwrap` implementation with the strict parameter-kind implementation below.

---

### Task 1: Pre-create audit parents and tighten callback signature detection

**Files:**
- Modify: `tests/test_core_infrastructure.py`
- Modify: `core/runner.py`

**Interfaces:**
- Consumes: `safe_task(name, fn, *args, **kwargs)` and `db.record_ingestion_run(result)`
- Produces: `_accepts_task_run_id(fn) -> bool`, provisional `running` audit write before PIT-capable callbacks, fail-closed behavior when that write fails

- [ ] **Step 1: Preserve the existing lock-decorated regression and add RED tests**

Keep `test_lock_decorated_fixed_signature_callback_does_not_receive_run_id`.
Add these tests to `TestSafeTask`:

```python
def test_var_positional_run_id_name_does_not_accept_keyword(self):
    def positional(*_task_run_id: object) -> dict[str, int]:
        return {"saved": 1}

    result = safe_task("positional", positional)

    assert result["status"] == "success"

def test_pit_capable_callback_records_parent_before_execution(self):
    db = MagicMock()
    observed: dict[str, object] = {}

    def supported(
        db: object,
        _task_run_id: str | None = None,
    ) -> dict[str, int]:
        observed["run_id"] = _task_run_id
        observed["calls_before_callback"] = db.record_ingestion_run.call_count
        observed["first_status"] = (
            db.record_ingestion_run.call_args_list[0].args[0]["status"]
        )
        return {"saved": 1}

    result = safe_task("pit", supported, db)

    assert result["status"] == "success"
    assert observed["calls_before_callback"] == 1
    assert observed["first_status"] == "running"
    assert db.record_ingestion_run.call_count == 2
    first = db.record_ingestion_run.call_args_list[0].args[0]
    final = db.record_ingestion_run.call_args_list[1].args[0]
    assert first["metadata"]["run_id"] == observed["run_id"]
    assert final["metadata"]["run_id"] == observed["run_id"]

def test_failed_parent_write_prevents_pit_callback(self):
    db = MagicMock()
    db.record_ingestion_run.side_effect = RuntimeError("audit locked")
    called = False

    def supported(
        db: object,
        _task_run_id: str | None = None,
    ) -> dict[str, int]:
        nonlocal called
        called = True
        return {"saved": 1}

    result = safe_task("pit", supported, db)

    assert called is False
    assert result["status"] == "failed"
    assert result["error_kind"] == "database"
    assert "audit parent" in result["error"]
```

- [ ] **Step 2: Run RED**

Run:

```bash
rtk uv run python -m pytest \
  tests/test_core_infrastructure.py::TestSafeTask::test_var_positional_run_id_name_does_not_accept_keyword \
  tests/test_core_infrastructure.py::TestSafeTask::test_pit_capable_callback_records_parent_before_execution \
  tests/test_core_infrastructure.py::TestSafeTask::test_failed_parent_write_prevents_pit_callback -q
```

Expected: the variadic positional callback fails from an unexpected keyword; the callback observes zero audit calls; the audit failure does not prevent callback execution.

- [ ] **Step 3: Implement strict signature support**

Use standard `inspect.signature(fn)` without an explicit `inspect.unwrap`.
The helper must be:

```python
def _accepts_task_run_id(fn: Callable[..., Any]) -> bool:
    """Return whether *fn* safely accepts ``_task_run_id`` as a keyword."""
    try:
        parameters = inspect.signature(fn).parameters
    except (TypeError, ValueError):
        return False

    run_id_parameter = parameters.get("_task_run_id")
    if run_id_parameter is not None and run_id_parameter.kind in {
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        inspect.Parameter.KEYWORD_ONLY,
    }:
        return True
    return any(
        parameter.kind is inspect.Parameter.VAR_KEYWORD
        for parameter in parameters.values()
    )
```

- [ ] **Step 4: Implement provisional audit recording**

Capture one `started_at = datetime.now(UTC)` when `safe_task` begins.
Add a private nested writer that returns success instead of swallowing:

```python
def _write_audit(payload: dict[str, Any]) -> bool:
    if db is None:
        return True
    try:
        db.record_ingestion_run(payload)
    except Exception as exc:
        logger.warning("⚠️ 写入 ingestion_runs 审计表失败: %s", exc)
        return False
    return True
```

After effective run-ID calculation, when the callback accepts a run ID and
`db` is present, write this provisional payload before calling the callback:

```python
running = TaskResult.success(name, saved=0)
running_payload = running.to_dict()
running_payload["status"] = "running"
running_payload["metadata"] = {
    "run_id": effective_run_id,
    "started_at": started_at.isoformat(timespec="seconds"),
    "finished_at": started_at.isoformat(timespec="seconds"),
}
```

If `_write_audit(running_payload)` is false, return:

```python
failure = TaskResult.failed(
    name,
    ErrorKind.DATABASE,
    "failed to create ingestion audit parent",
)
failure.metadata["run_id"] = effective_run_id
failure.metadata["elapsed_seconds"] = round(time.time() - task_start, 3)
return failure.to_dict()
```

Do not invoke the callback on this path.

Final recording must add the same run ID, fixed `started_at`, current
`finished_at`, and call `_write_audit`. Ordinary callbacks that do not accept
run IDs receive no provisional write and retain the existing final write.

- [ ] **Step 5: Run GREEN**

Run:

```bash
rtk uv run python -m pytest tests/test_core_infrastructure.py::TestSafeTask -q
```

Expected: all `TestSafeTask` tests pass, including the preserved lock-decorated test.

- [ ] **Step 6: Commit each file separately**

```bash
rtk git add core/runner.py
rtk git commit -m "fix: pre-create PIT audit parent records" -m "修复：在 PIT 写入前创建审计父记录"

rtk git add tests/test_core_infrastructure.py
rtk git commit -m "test: cover safe_task audit write ordering" -m "测试：覆盖 safe_task 审计写入顺序"
```

---

### Task 2: Upsert audit state and enforce SQLite foreign keys

**Files:**
- Modify: `tests/test_providers_extended2.py`
- Modify: `providers.py`

**Interfaces:**
- Consumes: provisional and final dictionaries passed to `SmartMoneyDBProvider.record_ingestion_run`
- Produces: idempotent audit upsert and foreign-key-enforcing shared write connection

- [ ] **Step 1: Add provider integration tests**

Add tests using the existing temporary `provider` fixture:

```python
def _audit_payload(run_id: str, status: str, saved: int) -> dict:
    return {
        "task_name": "pit_task",
        "status": status,
        "saved": saved,
        "metadata": {
            "run_id": run_id,
            "started_at": "2026-07-25T00:00:00+00:00",
            "finished_at": "2026-07-25T00:00:01+00:00",
        },
    }

def test_record_ingestion_run_upserts_same_parent(provider):
    provider.record_ingestion_run(_audit_payload("run-1", "running", 0))
    provider.record_ingestion_run(_audit_payload("run-1", "success", 3))

    with sqlite3.connect(provider.db_path) as conn:
        rows = conn.execute(
            "SELECT run_id, status, saved_rows FROM ingestion_runs WHERE run_id = ?",
            ("run-1",),
        ).fetchall()

    assert rows == [("run-1", "success", 3)]

def test_shared_write_connection_enables_foreign_keys(provider):
    conn = provider._get_write_conn()
    assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1

def test_pit_write_without_audit_parent_is_rejected(provider):
    saved = provider.save_index_member_history_batch(
        [{
            "index_code": "000300",
            "index_name": "沪深300",
            "ts_code": "000001.SZ",
            "weight": 1.0,
            "source": "akshare",
        }],
        run_id="missing-parent",
        valid_from="2026-07-25",
    )

    assert saved == 0
    with sqlite3.connect(provider.db_path) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM index_member_history "
            "WHERE snapshot_run_id = 'missing-parent'"
        ).fetchone()[0]
    assert count == 0
```

- [ ] **Step 2: Run RED**

Run:

```bash
rtk uv run python -m pytest \
  tests/test_providers_extended2.py::test_record_ingestion_run_upserts_same_parent \
  tests/test_providers_extended2.py::test_shared_write_connection_enables_foreign_keys \
  tests/test_providers_extended2.py::test_pit_write_without_audit_parent_is_rejected -q
```

Expected: duplicate run ID raises or does not update; foreign keys report 0; missing-parent PIT row is accepted.

- [ ] **Step 3: Enable foreign keys**

In `SmartMoneyDBProvider._get_write_conn`, execute:

```python
conn.execute("PRAGMA foreign_keys=ON")
```

before assigning `self._write_conn`.

- [ ] **Step 4: Upsert audit records**

Change `record_ingestion_run` to use:

```sql
INSERT INTO ingestion_runs (...) VALUES (...)
ON CONFLICT(run_id) DO UPDATE SET
    task_name = excluded.task_name,
    source = excluded.source,
    status = excluded.status,
    started_at = excluded.started_at,
    finished_at = excluded.finished_at,
    requested_date = excluded.requested_date,
    data_date = excluded.data_date,
    attempts = excluded.attempts,
    fetched_rows = excluded.fetched_rows,
    accepted_rows = excluded.accepted_rows,
    rejected_rows = excluded.rejected_rows,
    saved_rows = excluded.saved_rows,
    schema_fingerprint = excluded.schema_fingerprint,
    error_kind = excluded.error_kind,
    error_message = excluded.error_message,
    metadata_json = excluded.metadata_json
```

Keep the existing metadata-first run ID and timestamp fallback.

- [ ] **Step 5: Run GREEN**

Run:

```bash
rtk uv run python -m pytest tests/test_providers_extended2.py -q
```

Expected: all provider integration tests pass.

- [ ] **Step 6: Commit each file separately**

```bash
rtk git add providers.py
rtk git commit -m "fix: upsert audit state and enforce foreign keys" -m "修复：更新审计状态并强制外键约束"

rtk git add tests/test_providers_extended2.py
rtk git commit -m "test: cover audit persistence and PIT foreign keys" -m "测试：覆盖审计持久化与 PIT 外键"
```

---

### Task 3: Restore migration 006 and add checksum-safe migration 008

**Files:**
- Modify: `tests/test_migrations.py`
- Modify: `migrations/006_reconcile_ingestion_audit.py`
- Create: `migrations/008_reconcile_orphan_ingestion_runs.py`

**Interfaces:**
- Consumes: `schema_migrations`, `ingestion_runs`, `concept_member_history`, `index_member_history`
- Produces: immutable migration 006 and migration 008 supporting two explicitly known historical 006 checksums

- [ ] **Step 1: Add migration-008 regression helpers and tests**

Add constants matching the design:

```python
_PUBLISHED_006_CHECKSUM = (
    "a7830c97017b503c6d7b10412c901a4a4d365210704f8582521dfbe395e1e4eb"
)
_TRANSITIONAL_006_CHECKSUM = (
    "8273ec2643335baacddcd6478d4fab032348e7cfd2346d03669dadf12a6e78b2"
)
```

Use the real migrations directory in every test:

```python
def _real_migrations_dir() -> Path:
    return Path(__file__).resolve().parent.parent / "migrations"


def _prepare_version_7_db(
    db_path: str,
    recorded_checksum: str,
    *,
    orphan_run_id: str | None = None,
) -> MigrationEngine:
    engine = MigrationEngine(
        db_path=str(db_path),
        migrations_dir=_real_migrations_dir(),
    )
    engine.apply_pending(target_version=7)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            "UPDATE schema_migrations SET checksum = ? WHERE version = 6",
            (recorded_checksum,),
        )
        if orphan_run_id is not None:
            conn.execute(
                """INSERT INTO index_member_history
                   (index_code, index_name, ts_code, weight, valid_from,
                    valid_to, source, snapshot_run_id)
                   VALUES ('000300', '沪深300', '000001.SZ', 1.0,
                           '2026-07-25', NULL, 'test', ?)""",
                (orphan_run_id,),
            )
        conn.commit()
    return engine
```

Add these tests:

```python
@pytest.mark.parametrize("recorded_checksum", [
    "a7830c97017b503c6d7b10412c901a4a4d365210704f8582521dfbe395e1e4eb",
    "8273ec2643335baacddcd6478d4fab032348e7cfd2346d03669dadf12a6e78b2",
])
def test_migration_008_bridges_known_006_checksums_and_seeds_orphan(
    tmp_db,
    recorded_checksum,
):
    engine = _prepare_version_7_db(
        str(tmp_db),
        recorded_checksum,
        orphan_run_id="orphan-run",
    )

    result = engine.apply_pending()

    assert [item["version"] for item in result] == [8]
    with sqlite3.connect(str(tmp_db)) as conn:
        parent = conn.execute(
            "SELECT status FROM ingestion_runs WHERE run_id = 'orphan-run'"
        ).fetchone()
        stored = conn.execute(
            "SELECT checksum FROM schema_migrations WHERE version = 6"
        ).fetchone()[0]
        violations = conn.execute("PRAGMA foreign_key_check").fetchall()
    assert parent == ("reconciled",)
    assert stored == _PUBLISHED_006_CHECKSUM
    assert violations == []
```

Also add:

```python
def test_migration_008_is_idempotent(tmp_db):
    engine = _prepare_version_7_db(
        str(tmp_db),
        _PUBLISHED_006_CHECKSUM,
        orphan_run_id="orphan-run",
    )

    first = engine.apply_pending()
    second = engine.apply_pending()

    assert [item["version"] for item in first] == [8]
    assert second == []
    with sqlite3.connect(str(tmp_db)) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM ingestion_runs WHERE run_id = 'orphan-run'"
        ).fetchone()[0]
    assert count == 1

def test_migration_008_rejects_unknown_006_checksum(tmp_db):
    engine = _prepare_version_7_db(str(tmp_db), "unknown")

    with pytest.raises(MigrationError, match="unknown migration 006 checksum"):
        engine.apply_pending()
```

- [ ] **Step 2: Run RED**

Run:

```bash
rtk uv run python -m pytest tests/test_migrations.py -k migration_008 -q
```

Expected: tests fail because migration 008 does not exist and migration 006 still has the transitional content.

- [ ] **Step 3: Restore migration 006 exactly**

Restore `migrations/006_reconcile_ingestion_audit.py` to its content at
commit `e153235`. Verify:

```bash
rtk shasum -a 256 migrations/006_reconcile_ingestion_audit.py
```

Expected:

```text
a7830c97017b503c6d7b10412c901a4a4d365210704f8582521dfbe395e1e4eb
```

- [ ] **Step 4: Create migration 008**

Create `migrations/008_reconcile_orphan_ingestion_runs.py` with:

```python
"""Migration 008: reconcile orphaned PIT run IDs and bridge migration 006."""

import hashlib
import logging
from datetime import UTC, datetime
from pathlib import Path

logger = logging.getLogger(__name__)

PUBLISHED_006_CHECKSUM = (
    "a7830c97017b503c6d7b10412c901a4a4d365210704f8582521dfbe395e1e4eb"
)
TRANSITIONAL_006_CHECKSUM = (
    "8273ec2643335baacddcd6478d4fab032348e7cfd2346d03669dadf12a6e78b2"
)
KNOWN_006_CHECKSUMS = {PUBLISHED_006_CHECKSUM, TRANSITIONAL_006_CHECKSUM}


def apply(conn):
    _validate_006_checksum(conn)
    _seed_orphan_run_ids(conn)
    _reconcile_006_checksum(conn)
    logger.info("  ✅ 008: orphaned ingestion run IDs reconciled")


def _validate_006_checksum(conn):
    row = conn.execute(
        "SELECT checksum FROM schema_migrations WHERE version = 6 AND success = 1"
    ).fetchone()
    if row is None or row[0] not in KNOWN_006_CHECKSUMS:
        actual = None if row is None else row[0]
        raise RuntimeError(f"unknown migration 006 checksum: {actual}")


def _seed_orphan_run_ids(conn):
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    history_tables = {
        "index_member_history",
        "concept_member_history",
    } & tables
    orphan_ids: set[str] = set()
    for table in history_tables:
        rows = conn.execute(
            f"""SELECT DISTINCT h.snapshot_run_id
                FROM {table} AS h
                LEFT JOIN ingestion_runs AS r ON r.run_id = h.snapshot_run_id
                WHERE h.snapshot_run_id IS NOT NULL AND r.run_id IS NULL"""
        ).fetchall()
        orphan_ids.update(row[0] for row in rows if row[0])

    now = datetime.now(UTC).isoformat(timespec="seconds")
    conn.executemany(
        """INSERT OR IGNORE INTO ingestion_runs
           (run_id, task_name, source, status, started_at, finished_at,
            metadata_json)
           VALUES (?, 'reconcile', 'migration-008', 'reconciled', ?, ?, '{}')""",
        [(run_id, now, now) for run_id in sorted(orphan_ids)],
    )


def _reconcile_006_checksum(conn):
    migration = Path(__file__).resolve().parent / "006_reconcile_ingestion_audit.py"
    checksum = hashlib.sha256(migration.read_bytes()).hexdigest()
    if checksum != PUBLISHED_006_CHECKSUM:
        raise RuntimeError(f"migration 006 file is not immutable: {checksum}")
    conn.execute(
        "UPDATE schema_migrations SET checksum = ? WHERE version = 6",
        (checksum,),
    )
```

- [ ] **Step 5: Run GREEN**

Run:

```bash
rtk uv run python -m pytest tests/test_migrations.py -q
```

Expected: all migration tests pass.

- [ ] **Step 6: Commit each file separately**

```bash
rtk git add migrations/006_reconcile_ingestion_audit.py
rtk git commit -m "fix: restore immutable migration 006" -m "修复：恢复不可变的 migration 006"

rtk git add migrations/008_reconcile_orphan_ingestion_runs.py
rtk git commit -m "fix: add checksum-safe orphan reconciliation" -m "修复：新增校验和安全的孤立记录对账"

rtk git add tests/test_migrations.py
rtk git commit -m "test: cover migration 008 upgrade compatibility" -m "测试：覆盖 migration 008 升级兼容性"
```

---

### Task 4: Final integration verification

**Files:**
- Verify only; no planned source modifications

**Interfaces:**
- Consumes: Tasks 1–3
- Produces: fresh proof of pipeline compatibility, persistence integrity, migration safety, and clean test side effects

- [ ] **Step 1: Run focused integration suites**

```bash
rtk uv run python -m pytest \
  tests/test_core_infrastructure.py \
  tests/test_pipeline_resilience.py \
  tests/test_providers_extended2.py \
  tests/test_migrations.py \
  tests/test_daily_pipeline.py -q
```

Expected: exit code 0.

- [ ] **Step 2: Run the complete suite**

```bash
rtk uv run python -m pytest -q
```

Expected: exit code 0 with no failures.

- [ ] **Step 3: Run static checks**

```bash
rtk uv run ruff check .
rtk git diff --check
```

Expected: both exit 0.

- [ ] **Step 4: Verify migration and side-effect state**

```bash
rtk shasum -a 256 migrations/006_reconcile_ingestion_audit.py
rtk proxy find . -maxdepth 1 -type f -name '<MagicMock*' -print
rtk git status --short --branch
```

Expected: migration 006 hash is the published value; artifact search is empty;
working tree is clean on `fix/safe-task-run-id-compatibility`.

- [ ] **Step 5: Run read-only production compatibility checks**

Using SQLite read-only mode, verify:

```sql
SELECT version, checksum, success
FROM schema_migrations
WHERE version IN (6, 7, 8)
ORDER BY version;
```

Before production launch, version 008 may still be pending; version 006 must
contain one of the two known checksums. Do not apply migrations to production
as part of verification.
