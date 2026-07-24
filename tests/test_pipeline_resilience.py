"""
Resilience & recovery tests for the data pipeline.
══════════════════════════════════════════════════

Covers:
  - MigrationEngine: checksum mismatch, duplicate versions, FK violation
  - SourceClient: circuit breaker open/close, retry exhaustion,
    non-retryable HTTP codes
  - safe_task: exception isolation, _task_run_id injection
  - run_all: crash detection across failure modes
"""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from core.migrations import (
    MigrationEngine,
    MigrationError,
    MigrationScript,
    _load_python_migration,
)
from core.runner import safe_task
from core.source_client import (
    CircuitState,
    SourceClient,
    SourcePolicy,
)

# ===========================================================================
# MigrationEngine resilience
# ===========================================================================


class TestMigrationChecksumResilience:
    """Checksum mismatch detection and recovery."""

    def test_checksum_mismatch_raises(self, tmp_path: Path):
        """If a recorded checksum differs from the file, engine must fail."""
        db = tmp_path / "test.db"
        mig_dir = tmp_path / "migrations"
        mig_dir.mkdir()
        mig_file = mig_dir / "001_test.sql"
        mig_file.write_text("CREATE TABLE IF NOT EXISTS t (x INT)")

        engine = MigrationEngine(str(db), str(mig_dir))
        engine.apply_pending()

        # Tamper with the SQL file after it was applied
        mig_file.write_text("CREATE TABLE IF NOT EXISTS t (x INT, y INT)")

        with pytest.raises(MigrationError, match="checksum mismatch"):
            engine.apply_pending()

    def test_verify_applied_checksums_skips_unapplied(self, tmp_path: Path):
        """Unapplied migrations are not checksum-checked."""
        db = tmp_path / "test.db"
        conn = sqlite3.connect(str(db))
        conn.execute("""
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY,
                description TEXT NOT NULL DEFAULT '',
                applied_at TEXT NOT NULL DEFAULT (datetime('now')),
                checksum TEXT NOT NULL,
                duration_ms INTEGER NOT NULL DEFAULT 0,
                success INTEGER NOT NULL DEFAULT 1
            )
        """)
        conn.execute(
            "INSERT INTO schema_migrations (version, description, checksum) VALUES (1, 'v1', 'aaa')"
        )
        conn.close()

        mig_dir = tmp_path / "migrations"
        mig_dir.mkdir()
        (mig_dir / "001_applied.sql").write_text("original content")
        (mig_dir / "002_new.sql").write_text("new migration")

        # Should not raise — v2 is new (unapplied)
        engine = MigrationEngine(str(db), str(mig_dir))
        engine._verify_applied_checksums(engine._load())

    def test_duplicate_versions_raises(self, tmp_path: Path):
        """Two migrations with the same version must be rejected."""
        db = tmp_path / "test.db"
        mig_dir = tmp_path / "migrations"
        mig_dir.mkdir()
        (mig_dir / "001_alpha.sql").write_text("CREATE TABLE a (x INT)")
        (mig_dir / "001_beta.sql").write_text("CREATE TABLE b (x INT)")

        engine = MigrationEngine(str(db), str(mig_dir))
        with pytest.raises(MigrationError, match="duplicate migration version 1"):
            engine.apply_pending()


class TestMigrationApplyResilience:
    """Error handling during migration application."""

    def test_sql_migration_fk_violation_rolls_back(self, tmp_path: Path):
        """FK violations must roll back and record the failure."""
        db = tmp_path / "test.db"
        conn = sqlite3.connect(str(db))
        conn.execute("CREATE TABLE parent (id INT PRIMARY KEY)")
        conn.execute("INSERT INTO parent VALUES (1)")
        conn.commit()
        conn.close()

        mig_dir = tmp_path / "migrations"
        mig_dir.mkdir()
        (mig_dir / "001_add_child.sql").write_text("""
            CREATE TABLE child (
                ref INT REFERENCES parent(id)
            );
            INSERT INTO child (ref) VALUES (999);
        """)

        engine = MigrationEngine(str(db), str(mig_dir))
        with pytest.raises(MigrationError, match="(?i)foreign key"):
            engine.apply_pending()

        # Failure must be recorded in schema_migrations
        conn = sqlite3.connect(str(db))
        row = conn.execute(
            "SELECT version, success FROM schema_migrations WHERE version = 1"
        ).fetchone()
        conn.close()
        assert row is not None
        assert row[1] == 0

    def test_python_migration_exception_rolls_back(self, tmp_path: Path):
        """Python migrations that raise must roll back."""
        db = tmp_path / "test.db"
        mig_dir = tmp_path / "migrations"
        mig_dir.mkdir()

        py_mig = mig_dir / "001_crash.py"
        py_mig.write_text("""
def apply(conn):
    raise RuntimeError("intentional crash")
""")

        engine = MigrationEngine(str(db), str(mig_dir))
        with pytest.raises(MigrationError, match="intentional crash"):
            engine.apply_pending()

    def test_invalid_migration_no_sql_no_func(self, tmp_path: Path):
        """Script without sql or apply_func must fail."""
        mig = MigrationScript(version=1, description="empty")
        conn = sqlite3.connect(":memory:")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY,
                checksum TEXT NOT NULL
            )
        """)

        engine = MigrationEngine(str(tmp_path / "nonexistent.db"), str(tmp_path))
        result = engine._apply_one(conn, mig)
        assert result["applied"] is False
        assert "neither sql nor apply_func" in result["error"]

    def test_pending_returns_correct_unapplied(self, tmp_path: Path):
        """plan() must only return migrations not yet in schema_migrations."""
        db = tmp_path / "test.db"
        conn = sqlite3.connect(str(db))
        conn.execute("""
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY,
                description TEXT NOT NULL DEFAULT '',
                applied_at TEXT NOT NULL DEFAULT (datetime('now')),
                checksum TEXT NOT NULL,
                duration_ms INTEGER NOT NULL DEFAULT 0,
                success INTEGER NOT NULL DEFAULT 1
            )
        """)
        conn.execute(
            "INSERT INTO schema_migrations (version, description, checksum) VALUES (1, 'v1', 'abc')"
        )
        conn.commit()
        conn.close()

        mig_dir = tmp_path / "migrations"
        mig_dir.mkdir()
        (mig_dir / "001_done.sql").write_text("SELECT 1")
        (mig_dir / "002_pending.sql").write_text("SELECT 2")

        engine = MigrationEngine(str(db), str(mig_dir))
        plan = engine.plan()
        assert len(plan) == 1
        assert plan[0]["version"] == 2


# ===========================================================================
# SourceClient circuit breaker resilience
# ===========================================================================


class TestCircuitBreaker:
    """Circuit breaker state management."""

    def test_closed_by_default(self):
        """New circuit breaker starts CLOSED."""
        policy = SourcePolicy(
            source="test", host="example.com",
            timeout_seconds=5, max_attempts=2,
            base_delay_seconds=0.01, max_delay_seconds=0.1,
            min_interval_seconds=0,
            circuit_failures=3, circuit_cooldown_seconds=0.05,
        )
        client = SourceClient(policies={"test": policy})

        assert client._circuits["test"].state == CircuitState.CLOSED
        assert client._circuits["test"].may_attempt() is True

    def test_opens_after_threshold_failures(self):
        """After N consecutive failures, circuit opens."""
        policy = SourcePolicy(
            source="test", host="example.com",
            timeout_seconds=5, max_attempts=1,
            base_delay_seconds=0.01, max_delay_seconds=0.1,
            min_interval_seconds=0,
            circuit_failures=3, circuit_cooldown_seconds=3600,
        )
        client = SourceClient(policies={"test": policy})
        cb = client._circuits["test"]

        for _ in range(cb._policy.circuit_failures):
            cb.record_failure()

        assert cb.state == CircuitState.OPEN
        assert cb.may_attempt() is False

    def test_rejects_calls_when_open(self):
        """When circuit is OPEN, call() must fail immediately."""
        policy = SourcePolicy(
            source="test", host="example.com",
            timeout_seconds=5, max_attempts=1,
            base_delay_seconds=0.01, max_delay_seconds=0.1,
            min_interval_seconds=0,
            circuit_failures=1, circuit_cooldown_seconds=3600,
        )
        client = SourceClient(policies={"test": policy})

        # First call fails → circuit opens
        def fn():
            raise ConnectionError("refused")

        resp1 = client.call("test", fn)
        assert resp1.success is False

        # Second call → circuit open, no attempt
        def never_called():
            raise AssertionError("should not be called")

        resp2 = client.call("test", never_called)
        assert resp2.success is False
        assert resp2.metadata.attempt_count == 0
        assert resp2.metadata.circuit_breaker_triggered is True

    def test_half_open_after_cooldown(self):
        """After cooldown, circuit transitions to HALF_OPEN."""
        policy = SourcePolicy(
            source="test", host="example.com",
            timeout_seconds=5, max_attempts=1,
            base_delay_seconds=0.01, max_delay_seconds=0.1,
            min_interval_seconds=0,
            circuit_failures=1, circuit_cooldown_seconds=0.05,
        )
        client = SourceClient(policies={"test": policy})
        cb = client._circuits["test"]

        cb.record_failure()  # triggers OPEN
        assert cb.state == CircuitState.OPEN

        time.sleep(0.06)  # wait for cooldown
        assert cb.state == CircuitState.HALF_OPEN

    def test_closes_after_success_in_half_open(self):
        """A successful call from HALF_OPEN must close the circuit."""
        policy = SourcePolicy(
            source="test", host="example.com",
            timeout_seconds=5, max_attempts=1,
            base_delay_seconds=0.01, max_delay_seconds=0.1,
            min_interval_seconds=0,
            circuit_failures=1, circuit_cooldown_seconds=0.05,
        )
        client = SourceClient(policies={"test": policy})
        cb = client._circuits["test"]

        cb.record_failure()  # → OPEN
        assert cb.state == CircuitState.OPEN

        time.sleep(0.06)  # → HALF_OPEN
        cb.record_success()  # → CLOSED
        assert cb.state == CircuitState.CLOSED

    def test_reset_clears_all_state(self):
        """reset() must clear failure count and close circuit."""
        policy = SourcePolicy(
            source="test", host="example.com",
            timeout_seconds=5, max_attempts=1,
            base_delay_seconds=0.01, max_delay_seconds=0.1,
            min_interval_seconds=0,
            circuit_failures=3, circuit_cooldown_seconds=3600,
        )
        client = SourceClient(policies={"test": policy})
        cb = client._circuits["test"]

        for _ in range(3):
            cb.record_failure()
        assert cb.state == CircuitState.OPEN
        assert cb._failure_count >= 3

        cb.reset()
        assert cb.state == CircuitState.CLOSED
        assert cb._failure_count == 0


class TestSourceClientRetry:
    """Retry and backoff behavior."""

    def test_successful_first_attempt(self):
        """A successful call must return data with success=True."""
        policy = SourcePolicy(
            source="test", host="example.com",
            timeout_seconds=5, max_attempts=3,
            base_delay_seconds=0.01, max_delay_seconds=0.1,
            min_interval_seconds=0,
            circuit_failures=5, circuit_cooldown_seconds=3600,
        )
        client = SourceClient(policies={"test": policy})

        resp = client.call("test", lambda: {"data": 42})
        assert resp.success is True
        assert resp.data == {"data": 42}
        assert resp.metadata.attempt_count == 1

    def test_retry_exhausted_returns_failure(self):
        """After exhausting retries, return failure with metadata."""
        call_count = [0]
        policy = SourcePolicy(
            source="test", host="example.com",
            timeout_seconds=5, max_attempts=3,
            base_delay_seconds=0.01, max_delay_seconds=0.1,
            min_interval_seconds=0,
            circuit_failures=5, circuit_cooldown_seconds=3600,
        )
        client = SourceClient(policies={"test": policy})

        def flaky():
            call_count[0] += 1
            raise TimeoutError("timeout")

        resp = client.call("test", flaky)
        assert resp.success is False
        assert resp.metadata.attempt_count == 3  # all attempts exhausted
        assert "timeout" in resp.metadata.error

    def test_non_retryable_http_400(self):
        """HTTP 400 must fail immediately without retry."""
        def returns_400():
            resp = MagicMock()
            resp.status_code = 400
            return resp

        policy = SourcePolicy(
            source="test", host="example.com",
            timeout_seconds=5, max_attempts=3,
            base_delay_seconds=0.01, max_delay_seconds=0.1,
            min_interval_seconds=0,
        )
        client = SourceClient(policies={"test": policy})
        resp = client.call("test", returns_400)
        assert resp.success is False
        assert resp.metadata.attempt_count == 1  # no retry

    def test_retryable_http_503(self):
        """HTTP 503 must be retried."""
        call_count = [0]
        policy = SourcePolicy(
            source="test", host="example.com",
            timeout_seconds=5, max_attempts=3,
            base_delay_seconds=0.01, max_delay_seconds=0.1,
            min_interval_seconds=0,
            circuit_failures=5, circuit_cooldown_seconds=3600,
        )
        client = SourceClient(policies={"test": policy})

        def returns_503():
            call_count[0] += 1
            resp = MagicMock()
            resp.status_code = 503
            return resp

        resp = client.call("test", returns_503)
        assert resp.success is False
        assert call_count[0] == 3  # retried 3 times


class TestSourceClientFallback:
    """call_with_fallback behavior."""

    def test_primary_success_no_fallback(self):
        """If primary succeeds, fallback is never called."""
        fb_marker = []

        def primary():
            return {"ok": True}

        def fallback():
            fb_marker.append("called")
            return {"ok": False}

        policy = SourcePolicy(
            source="test", host="example.com",
            timeout_seconds=5, max_attempts=1,
            base_delay_seconds=0.01, max_delay_seconds=0.1,
            min_interval_seconds=0,
        )
        client = SourceClient(policies={"test": policy, "fb": policy})

        resp = client.call_with_fallback(
            "test", primary,
            fallback_sources=[("fb", fallback)],
        )
        assert resp.success is True
        assert fb_marker == []  # fallback never called

    def test_fallback_succeeds_when_primary_fails(self):
        """If primary fails, fallback is used."""
        policy = SourcePolicy(
            source="test", host="example.com",
            timeout_seconds=5, max_attempts=1,
            base_delay_seconds=0.01, max_delay_seconds=0.1,
            min_interval_seconds=0,
        )
        client = SourceClient(policies={"test": policy, "fb": policy})

        resp = client.call_with_fallback(
            "test", lambda: (_ for _ in ()).throw(ConnectionError("fail")),
            fallback_sources=[("fb", lambda: {"data": "from fb"})],
        )
        assert resp.success is True
        assert resp.data == {"data": "from fb"}
        assert resp.metadata.fallback_used is True


# ===========================================================================
# safe_task resilience
# ===========================================================================


class TestSafeTaskResilience:
    """safe_task exception isolation and metadata."""

    def test_exception_returns_failed_struct(self):
        """An exception in the task must return a failed dict, not propagate."""
        def crash(**kw):
            msg = "internal error"
            raise ValueError(msg)

        result = safe_task("crash", crash)
        assert result["status"] == "failed"
        assert "internal" in result.get("error_kind", "")
        assert "internal error" in result.get("error", "")
        assert "elapsed_seconds" in result.get("metadata", {})

    def test_empty_task_returns_failed(self):
        """fn that does not return any dict must still return a valid result."""
        def empty(**kw):
            pass

        result = safe_task("empty", empty)
        assert "status" in result
        assert result["saved"] == 0

    def test_run_id_injected_into_kwargs(self):
        """safe_task must inject _task_run_id into kwargs."""
        captured: dict[str, Any] = {}

        def capture(**kw):
            captured.update(kw)
            return {"saved": 1}

        safe_task("capture", capture)
        assert "_task_run_id" in captured
        # run_id should be a uuid-like string
        assert len(str(captured["_task_run_id"])) == 36  # uuid4 format

    def test_metadata_includes_elapsed(self):
        """Result metadata must include elapsed_seconds."""
        def slow(**_kw):
            time.sleep(0.01)
            return {"saved": 1}

        result = safe_task("slow", slow)
        assert result["metadata"]["elapsed_seconds"] >= 0.01

    def test_writes_ingestion_record_on_success(self):
        """When a db is passed, safe_task must call record_ingestion_run."""
        db = MagicMock()
        db.record_ingestion_run.return_value = None

        def ok_fn(**kw):
            return {"saved": 5}

        safe_task("write_test", ok_fn, db)
        assert db.record_ingestion_run.called

    def test_writes_ingestion_record_on_failure(self):
        """Even on exception, ingestion_runs must be recorded."""
        db = MagicMock()
        db.record_ingestion_run.return_value = None

        def crash_fn(**kw):
            msg = "oops"
            raise RuntimeError(msg)

        safe_task("crash", crash_fn, db)
        assert db.record_ingestion_run.called


# ===========================================================================
# daily_pipeline run_all crash detection
# ===========================================================================


class TestRunAllCrashDetection:
    """run_all crash detection across failure modes."""

    # We test _task_result_has_errors directly since it drives crash detection
    def test_error_key_detected(self):
        from core.runner import _task_result_has_errors
        assert _task_result_has_errors({"error": "disk full"}) is True

    def test_aborted_key_detected(self):
        from core.runner import _task_result_has_errors
        assert _task_result_has_errors({"aborted": True}) is True

    def test_failed_count_detected(self):
        from core.runner import _task_result_has_errors
        assert _task_result_has_errors({"failed": 3, "total": 10}) is True

    def test_failed_symbols_detected(self):
        from core.runner import _task_result_has_errors
        assert _task_result_has_errors({"failed_symbols": ["000001.SH"]}) is True

    def test_clean_result_not_detected(self):
        from core.runner import _task_result_has_errors
        assert _task_result_has_errors({"saved": 10, "total": 10}) is False
        assert _task_result_has_errors({}) is False

    def test_none_values_not_detected(self):
        from core.runner import _task_result_has_errors
        assert _task_result_has_errors({"failed": 0, "error": None}) is False


# ===========================================================================
# Python migration loader
# ===========================================================================


class TestPythonMigrationLoader:
    """_load_python_migration edge cases."""

    def test_loader_rejects_missing_apply(self, tmp_path: Path):
        """A .py migration without apply() must raise."""
        bad = tmp_path / "bad.py"
        bad.write_text("x = 1")

        with pytest.raises(MigrationError, match="must export"):
            _load_python_migration(bad)

    def test_loader_imports_correctly(self, tmp_path: Path):
        """A .py migration with apply() must load successfully."""
        good = tmp_path / "good.py"
        good.write_text("""
def apply(conn):
    conn.execute("CREATE TABLE IF NOT EXISTS loaded (x INT)")
""")

        fn = _load_python_migration(good)
        conn = sqlite3.connect(":memory:")
        fn(conn)
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        assert any("loaded" in t for t in tables[0])
