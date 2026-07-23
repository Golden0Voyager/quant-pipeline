"""Tests for versioned schema migration engine."""
from __future__ import annotations

import os
import sqlite3
import tempfile
from pathlib import Path

import pytest

from core.migrations import (
    MigrationEngine,
    MigrationError,
    MigrationScript,
    run_migrations,
)

# ── fixtures ───────────────────────────────────────────────────────────


@pytest.fixture
def tmp_db():
    """Yield a path to an empty temp SQLite database (cleaned up after)."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    yield path
    if Path(path).exists():
        Path(path).unlink()


@pytest.fixture
def tmp_migrations_dir(tmp_path: Path) -> Path:
    """Yield an empty temp migrations directory."""
    d = tmp_path / "migrations"
    d.mkdir()
    return d


# ═══════════════════════════════════════════════════════════════════════
# MigrationScript
# ═══════════════════════════════════════════════════════════════════════


class TestMigrationScript:
    """Minimal dataclass structure."""

    def test_sql_migration(self):
        m = MigrationScript(version=1, description="test", sql="CREATE TABLE x (a INT);")
        assert m.version == 1
        assert m.sql is not None
        assert m.apply_func is None

    def test_python_migration(self):
        def f(conn):
            pass
        m = MigrationScript(version=2, description="py", apply_func=f)
        assert m.apply_func is not None
        assert m.sql is None

    def test_checksum_empty_by_default(self):
        m = MigrationScript(version=3, description="bare")
        assert m.checksum == ""


# ═══════════════════════════════════════════════════════════════════════
# MigrationEngine — basic behaviour
# ═══════════════════════════════════════════════════════════════════════


class TestMigrationEnginePlan:
    """plan() returns pending migrations."""

    def test_plan_on_empty_db(self, tmp_db, tmp_migrations_dir):
        # Write an SQL migration
        (tmp_migrations_dir / "001_create_a.sql").write_text(
            "CREATE TABLE a (x INT);"
        )
        engine = MigrationEngine(
            db_path=str(tmp_db),
            migrations_dir=str(tmp_migrations_dir),
        )
        pending = engine.plan()
        assert len(pending) == 1
        assert pending[0]["version"] == 1

    def test_plan_after_apply(self, tmp_db, tmp_migrations_dir):
        (tmp_migrations_dir / "001_create_a.sql").write_text(
            "CREATE TABLE a (x INT);"
        )
        engine = MigrationEngine(
            db_path=str(tmp_db),
            migrations_dir=str(tmp_migrations_dir),
        )
        engine.apply_pending()
        pending = engine.plan()
        assert len(pending) == 0

    def test_plan_skip_applied(self, tmp_db, tmp_migrations_dir):
        (tmp_migrations_dir / "001_t1.sql").write_text("CREATE TABLE t1 (x INT);")
        (tmp_migrations_dir / "002_t2.sql").write_text("CREATE TABLE t2 (x INT);")
        engine = MigrationEngine(
            db_path=str(tmp_db),
            migrations_dir=str(tmp_migrations_dir),
        )
        engine.apply_pending(target_version=1)
        pending = engine.plan()
        assert [p["version"] for p in pending] == [2]

    def test_plan_empty_dir(self, tmp_db, tmp_migrations_dir):
        engine = MigrationEngine(
            db_path=str(tmp_db),
            migrations_dir=str(tmp_migrations_dir),
        )
        assert engine.plan() == []

    def test_plan_nonexistent_dir(self, tmp_db):
        engine = MigrationEngine(
            db_path=str(tmp_db),
            migrations_dir="/nonexistent/migrations",
        )
        assert engine.plan() == []


class TestMigrationEngineApply:
    """apply_pending() executes and records migrations."""

    def test_apply_sql(self, tmp_db, tmp_migrations_dir):
        (tmp_migrations_dir / "001_create_a.sql").write_text(
            "CREATE TABLE a (x INT);"
        )
        engine = MigrationEngine(
            db_path=str(tmp_db),
            migrations_dir=str(tmp_migrations_dir),
        )
        results = engine.apply_pending()
        assert len(results) == 1
        assert results[0]["applied"] is True
        assert results[0]["error"] is None
        # Verify table exists
        with sqlite3.connect(str(tmp_db)) as conn:
            tables = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
            assert ("a",) in tables

    def test_apply_python_migration(self, tmp_db, tmp_migrations_dir):
        (tmp_migrations_dir / "001_create_b.py").write_text("""
def apply(conn):
    conn.execute("CREATE TABLE b (y TEXT)")
""")
        engine = MigrationEngine(
            db_path=str(tmp_db),
            migrations_dir=str(tmp_migrations_dir),
        )
        results = engine.apply_pending()
        assert results[0]["applied"] is True
        with sqlite3.connect(str(tmp_db)) as conn:
            tables = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
            assert ("b",) in tables

    def test_apply_tracking_table_created(self, tmp_db, tmp_migrations_dir):
        (tmp_migrations_dir / "001_dummy.sql").write_text(
            "CREATE TABLE dummy (id INT);"
        )
        engine = MigrationEngine(
            db_path=str(tmp_db),
            migrations_dir=str(tmp_migrations_dir),
        )
        engine.apply_pending()
        with sqlite3.connect(str(tmp_db)) as conn:
            rows = conn.execute(
                "SELECT version, description, success FROM schema_migrations"
            ).fetchall()
            assert rows == [(1, "dummy", 1)]

    def test_apply_tracking_includes_checksum(self, tmp_db, tmp_migrations_dir):
        path = tmp_migrations_dir / "001_ck.sql"
        path.write_text("CREATE TABLE ck (x INT);")
        engine = MigrationEngine(
            db_path=str(tmp_db),
            migrations_dir=str(tmp_migrations_dir),
        )
        engine.apply_pending()
        with sqlite3.connect(str(tmp_db)) as conn:
            row = conn.execute(
                "SELECT checksum FROM schema_migrations WHERE version=1"
            ).fetchone()
            assert len(row[0]) == 64  # SHA-256 hex

    def test_apply_in_order(self, tmp_db, tmp_migrations_dir):
        (tmp_migrations_dir / "001_first.sql").write_text(
            "CREATE TABLE first (x INT);"
        )
        (tmp_migrations_dir / "002_second.sql").write_text(
            "CREATE TABLE second (x INT);"
        )
        engine = MigrationEngine(
            db_path=str(tmp_db),
            migrations_dir=str(tmp_migrations_dir),
        )
        results = engine.apply_pending()
        assert len(results) == 2
        assert results[0]["version"] == 1
        assert results[1]["version"] == 2

    def test_idempotent(self, tmp_db, tmp_migrations_dir):
        (tmp_migrations_dir / "001_idem.sql").write_text(
            "CREATE TABLE idem (x INT);"
        )
        engine = MigrationEngine(
            db_path=str(tmp_db),
            migrations_dir=str(tmp_migrations_dir),
        )
        engine.apply_pending()
        engine.apply_pending()  # second apply should be a no-op
        with sqlite3.connect(str(tmp_db)) as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM schema_migrations"
            ).fetchone()[0]
            assert count == 1

    def test_dry_run(self, tmp_db, tmp_migrations_dir):
        (tmp_migrations_dir / "001_dry.sql").write_text(
            "CREATE TABLE dry_run_tbl (x INT);"
        )
        engine = MigrationEngine(
            db_path=str(tmp_db),
            migrations_dir=str(tmp_migrations_dir),
        )
        results = engine.apply_pending(dry_run=True)
        assert len(results) == 1
        assert results[0]["applied"] is False
        # Table should NOT have been created
        with sqlite3.connect(str(tmp_db)) as conn:
            tables = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
            assert ("dry_run_tbl",) not in tables

    def test_target_version(self, tmp_db, tmp_migrations_dir):
        (tmp_migrations_dir / "001_t.sql").write_text("CREATE TABLE t1 (x INT);")
        (tmp_migrations_dir / "002_t.sql").write_text("CREATE TABLE t2 (x INT);")
        engine = MigrationEngine(
            db_path=str(tmp_db),
            migrations_dir=str(tmp_migrations_dir),
        )
        results = engine.apply_pending(target_version=1)
        assert len(results) == 1
        assert results[0]["version"] == 1
        with sqlite3.connect(str(tmp_db)) as conn:
            tables = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
            assert ("t1",) in tables
            assert ("t2",) not in tables


class TestMigrationEngineErrors:
    """Error handling behaviour."""

    def test_duplicate_version_raises(self, tmp_db, tmp_migrations_dir):
        (tmp_migrations_dir / "001_dup.sql").write_text("CREATE TABLE dup1 (x INT);")
        (tmp_migrations_dir / "001_dup2.sql").write_text("CREATE TABLE dup2 (x INT);")
        engine = MigrationEngine(
            db_path=str(tmp_db),
            migrations_dir=str(tmp_migrations_dir),
        )
        with pytest.raises(MigrationError, match="duplicate migration version 1"):
            engine.apply_pending()

    def test_python_migration_no_apply_raises(self, tmp_db, tmp_migrations_dir):
        (tmp_migrations_dir / "001_bad.py").write_text(
            "# no apply function\n"
        )
        engine = MigrationEngine(
            db_path=str(tmp_db),
            migrations_dir=str(tmp_migrations_dir),
        )
        with pytest.raises(MigrationError, match="must export an 'apply"):
            engine.apply_pending()

    def test_sql_error_records_failure(self, tmp_db, tmp_migrations_dir):
        # Invalid SQL
        (tmp_migrations_dir / "001_badsql.sql").write_text(
            "CREATE TABLE;;;"
        )
        engine = MigrationEngine(
            db_path=str(tmp_db),
            migrations_dir=str(tmp_migrations_dir),
        )
        with pytest.raises(MigrationError):
            engine.apply_pending()
        # The failure should be recorded
        with sqlite3.connect(str(tmp_db)) as conn:
            row = conn.execute(
                "SELECT version, success FROM schema_migrations"
            ).fetchone()
            assert row is not None
            assert row[1] == 0

    def test_non_numeric_file_skipped(self, tmp_db, tmp_migrations_dir):
        (tmp_migrations_dir / "README.md").write_text("# notes")
        (tmp_migrations_dir / "001_real.sql").write_text("CREATE TABLE real_tbl (x INT);")
        engine = MigrationEngine(
            db_path=str(tmp_db),
            migrations_dir=str(tmp_migrations_dir),
        )
        results = engine.apply_pending()
        assert len(results) == 1
        assert results[0]["version"] == 1


# ═══════════════════════════════════════════════════════════════════════
# run_migrations convenience
# ═══════════════════════════════════════════════════════════════════════


class TestRunMigrations:
    """Shortcut function."""

    def test_run_migrations_default_dir(self, tmp_db, tmp_migrations_dir):
        # Create a migration in the custom dir and pass it explicitly
        (tmp_migrations_dir / "001_conv.sql").write_text(
            "CREATE TABLE conv (x INT);"
        )
        results = run_migrations(
            db_path=str(tmp_db),
            migrations_dir=str(tmp_migrations_dir),
        )
        assert len(results) == 1
        assert results[0]["applied"] is True

    def test_dry_run_flag(self, tmp_db, tmp_migrations_dir):
        (tmp_migrations_dir / "001_dry2.sql").write_text(
            "CREATE TABLE dry2 (x INT);"
        )
        results = run_migrations(
            db_path=str(tmp_db),
            migrations_dir=str(tmp_migrations_dir),
            dry_run=True,
        )
        assert results[0]["applied"] is False


# ═══════════════════════════════════════════════════════════════════════
# Validation — migration 001 (SQL)
# ═══════════════════════════════════════════════════════════════════════


class TestMigration001:
    """Ingestion audit table."""

    def test_creates_task_run_log(self, tmp_db):
        migrations_dir = Path(__file__).resolve().parent.parent / "migrations"
        if not migrations_dir.is_dir():
            pytest.skip("migrations/ directory not found")

        engine = MigrationEngine(
            db_path=str(tmp_db),
            migrations_dir=str(migrations_dir),
        )
        engine.apply_pending(target_version=1)

        with sqlite3.connect(str(tmp_db)) as conn:
            tables = {
                r[0]
                for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            assert "task_run_log" in tables
            # Verify columns
            cols = {r[1] for r in conn.execute("PRAGMA table_info(task_run_log)").fetchall()}
            for expected in ("task_name", "run_date", "status", "saved",
                             "error_kind", "error_msg", "elapsed_ms"):
                assert expected in cols, f"missing column {expected}"


# ═══════════════════════════════════════════════════════════════════════
# Validation — migration 002 (Python)
# ═══════════════════════════════════════════════════════════════════════


class TestMigration002:
    """Phase-2 data cleanup."""

    def test_rebuilds_cb_tables(self, tmp_db):
        migrations_dir = Path(__file__).resolve().parent.parent / "migrations"
        if not migrations_dir.is_dir():
            pytest.skip("migrations/ directory not found")

        # Prepare corrupted cb_quotation (no updated_at)
        with sqlite3.connect(str(tmp_db)) as conn:
            conn.execute("""
                CREATE TABLE cb_quotation (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts_code TEXT
                )
            """)
            conn.commit()

        engine = MigrationEngine(
            db_path=str(tmp_db),
            migrations_dir=str(migrations_dir),
        )
        engine.apply_pending(target_version=2)

        with sqlite3.connect(str(tmp_db)) as conn:
            cols = {
                r[1] for r in conn.execute("PRAGMA table_info(cb_quotation)")
            }
            assert "updated_at" in cols
            assert "ts_code" in cols
            assert "premium" in cols

    def test_rebuilds_cb_redeem(self, tmp_db):
        migrations_dir = Path(__file__).resolve().parent.parent / "migrations"
        if not migrations_dir.is_dir():
            pytest.skip("migrations/ directory not found")

        with sqlite3.connect(str(tmp_db)) as conn:
            conn.execute("""
                CREATE TABLE cb_redeem (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts_code TEXT
                )
            """)
            conn.commit()

        engine = MigrationEngine(
            db_path=str(tmp_db),
            migrations_dir=str(migrations_dir),
        )
        engine.apply_pending(target_version=2)

        with sqlite3.connect(str(tmp_db)) as conn:
            cols = {
                r[1] for r in conn.execute("PRAGMA table_info(cb_redeem)")
            }
            assert "updated_at" in cols
            assert "redeem_flag" in cols

    def test_does_not_drop_data_on_reapply(self, tmp_db):
        """A second apply should not drop+recreate if schema is fine."""
        migrations_dir = Path(__file__).resolve().parent.parent / "migrations"
        if not migrations_dir.is_dir():
            pytest.skip("migrations/ directory not found")

        # Create the proper schema first
        with sqlite3.connect(str(tmp_db)) as conn:
            conn.execute("""
                CREATE TABLE cb_quotation (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts_code TEXT NOT NULL,
                    bond_name TEXT,
                    price REAL,
                    premium REAL,
                    double_low REAL,
                    expire_date TEXT,
                    data_source TEXT,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(ts_code)
                )
            """)
            conn.execute(
                "INSERT INTO cb_quotation (ts_code, bond_name) VALUES ('123001.SH', 'test')"
            )
            conn.commit()

        engine = MigrationEngine(
            db_path=str(tmp_db),
            migrations_dir=str(migrations_dir),
        )
        engine.apply_pending(target_version=2)

        with sqlite3.connect(str(tmp_db)) as conn:
            count = conn.execute("SELECT COUNT(*) FROM cb_quotation").fetchone()[0]
            assert count == 1  # data preserved


# ═══════════════════════════════════════════════════════════════════════
# Validation — migration 003 (SQL)
# ═══════════════════════════════════════════════════════════════════════


class TestMigration003:
    """Point-in-time tables."""

    def test_creates_pt_tables(self, tmp_db):
        migrations_dir = Path(__file__).resolve().parent.parent / "migrations"
        if not migrations_dir.is_dir():
            pytest.skip("migrations/ directory not found")

        engine = MigrationEngine(
            db_path=str(tmp_db),
            migrations_dir=str(migrations_dir),
        )
        engine.apply_pending(target_version=3)

        with sqlite3.connect(str(tmp_db)) as conn:
            tables = {
                r[0]
                for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            for expected in ("financial_history_pt", "concept_member_pt", "index_member_pt"):
                assert expected in tables, f"missing table {expected}"
