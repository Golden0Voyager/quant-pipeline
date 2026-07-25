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

_PUBLISHED_006_CHECKSUM = (
    "a783c28347a05f415f4f6b4dd15f068cde964194657cea3c1573523085af65e0"
)
_TRANSITIONAL_006_CHECKSUM = (
    "8273ec2643335baacddcd6478d4fab032348e7cfd2346d03669dadf12a6e78b2"
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

    def test_sql_migration_rolls_back_partial_script(self, tmp_db, tmp_migrations_dir):
        (tmp_migrations_dir / "001_partial.sql").write_text(
            "CREATE TABLE sql_partial (value TEXT);\n"
            "INSERT INTO missing_table VALUES ('boom');\n"
        )
        engine = MigrationEngine(
            db_path=str(tmp_db),
            migrations_dir=str(tmp_migrations_dir),
        )

        with pytest.raises(MigrationError, match="migration 1 .* failed"):
            engine.apply_pending()

        with sqlite3.connect(str(tmp_db)) as conn:
            table = conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'sql_partial'"
            ).fetchone()
            failure = conn.execute(
                "SELECT success FROM schema_migrations WHERE version = 1"
            ).fetchone()
        assert table is None
        assert failure == (0,)

    def test_python_migration_executescript_rolls_back_partial_script(
        self,
        tmp_db,
        tmp_migrations_dir,
    ):
        (tmp_migrations_dir / "001_partial.py").write_text(
            """def apply(conn):
    conn.executescript(\"\"\"
        CREATE TABLE python_partial (value TEXT);
        INSERT INTO python_partial VALUES ('written');
    \"\"\")
    raise RuntimeError('injected failure')
"""
        )
        engine = MigrationEngine(
            db_path=str(tmp_db),
            migrations_dir=str(tmp_migrations_dir),
        )

        with pytest.raises(MigrationError, match="migration 1 .* failed"):
            engine.apply_pending()

        with sqlite3.connect(str(tmp_db)) as conn:
            table = conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'python_partial'"
            ).fetchone()
            failure = conn.execute(
                "SELECT success FROM schema_migrations WHERE version = 1"
            ).fetchone()
        assert table is None
        assert failure == (0,)

    def test_python_migration_executescript_preserves_semicolons_and_trigger_body(
        self,
        tmp_db,
        tmp_migrations_dir,
    ):
        (tmp_migrations_dir / "001_script.py").write_text(
            """def apply(conn):
    conn.executescript(\"\"\"
        CREATE TABLE messages (value TEXT NOT NULL);
        CREATE TABLE events (value TEXT NOT NULL);
        CREATE TRIGGER record_message AFTER INSERT ON messages
        BEGIN
            INSERT INTO events (value) VALUES ('trigger; value');
        END;
        INSERT INTO messages (value) VALUES ('payload; value');
    \"\"\")
"""
        )
        engine = MigrationEngine(
            db_path=str(tmp_db),
            migrations_dir=str(tmp_migrations_dir),
        )

        result = engine.apply_pending()

        assert [item["version"] for item in result] == [1]
        with sqlite3.connect(str(tmp_db)) as conn:
            messages = conn.execute("SELECT value FROM messages").fetchall()
            events = conn.execute("SELECT value FROM events").fetchall()
        assert messages == [("payload; value",)]
        assert events == [("trigger; value",)]

    def test_python_migration_acquires_immediate_write_lock(
        self,
        tmp_db,
        tmp_migrations_dir,
    ):
        observer_path = repr(str(tmp_db))
        (tmp_migrations_dir / "001_immediate.py").write_text(
            f"""import sqlite3

def apply(conn):
    observer = sqlite3.connect({observer_path}, timeout=0)
    try:
        observer.execute('BEGIN IMMEDIATE')
    except sqlite3.OperationalError as exc:
        assert 'locked' in str(exc).lower() or 'busy' in str(exc).lower()
    else:
        observer.rollback()
        raise RuntimeError('migration did not acquire an immediate write lock')
    finally:
        observer.close()
    conn.execute('CREATE TABLE lock_proof (value TEXT)')
"""
        )
        engine = MigrationEngine(
            db_path=str(tmp_db),
            migrations_dir=str(tmp_migrations_dir),
        )

        result = engine.apply_pending()

        assert [item["version"] for item in result] == [1]
        with sqlite3.connect(str(tmp_db)) as conn:
            table = conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'lock_proof'"
            ).fetchone()
        assert table == ("lock_proof",)

    @pytest.mark.parametrize(
        ("filename", "content", "table_name"),
        [
            (
                "001_python_execute.py",
                """def apply(conn):
    conn.execute('CREATE TABLE python_execute_control (value TEXT)')
    conn.execute('COMMIT')
""",
                "python_execute_control",
            ),
            (
                "001_python_script.py",
                """def apply(conn):
    conn.executescript(\"\"\"
        CREATE TABLE python_script_control (value TEXT);
        -- an explicit transaction escape
        COMMIT;
    \"\"\")
""",
                "python_script_control",
            ),
            (
                "001_python_bom_control.py",
                """def apply(conn):
    conn.execute('CREATE TABLE python_bom_control (value TEXT)')
    conn.execute('\\ufeffCOMMIT')
""",
                "python_bom_control",
            ),
            (
                "001_sql_control.sql",
                """CREATE TABLE sql_control (value TEXT);
-- an explicit transaction escape
COMMIT;
""",
                "sql_control",
            ),
            (
                "001_sql_block_comment_control.sql",
                """CREATE TABLE sql_block_comment_control (value TEXT);
/* an explicit transaction escape */ COMMIT;
""",
                "sql_block_comment_control",
            ),
            (
                "001_sql_bom_control.sql",
                """CREATE TABLE sql_bom_control (value TEXT);
\ufeffCOMMIT;
""",
                "sql_bom_control",
            ),
        ],
    )
    def test_migration_rejects_transaction_control_statements(
        self,
        tmp_db,
        tmp_migrations_dir,
        filename,
        content,
        table_name,
    ):
        (tmp_migrations_dir / filename).write_text(content)
        engine = MigrationEngine(
            db_path=str(tmp_db),
            migrations_dir=str(tmp_migrations_dir),
        )

        with pytest.raises(MigrationError, match="must not control transactions"):
            engine.apply_pending()

        with sqlite3.connect(str(tmp_db)) as conn:
            table = conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
                (table_name,),
            ).fetchone()
            failure = conn.execute(
                "SELECT success FROM schema_migrations WHERE version = 1"
            ).fetchone()
        assert table is None
        assert failure == (0,)

    def test_sql_migration_executes_semicolon_free_trailing_statement(
        self,
        tmp_db,
        tmp_migrations_dir,
    ):
        (tmp_migrations_dir / "001_trailing.sql").write_text(
            "CREATE TABLE semicolon_free_trailing (value TEXT)"
        )
        engine = MigrationEngine(
            db_path=str(tmp_db),
            migrations_dir=str(tmp_migrations_dir),
        )

        result = engine.apply_pending()

        assert [item["version"] for item in result] == [1]
        with sqlite3.connect(str(tmp_db)) as conn:
            table = conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' "
                "AND name = 'semicolon_free_trailing'"
            ).fetchone()
        assert table == ("semicolon_free_trailing",)

    def test_success_tracking_failure_rolls_back_migration(self, tmp_db, tmp_migrations_dir):
        (tmp_migrations_dir / "001_tracking.sql").write_text(
            "CREATE TABLE tracking_rollback (value TEXT);"
        )
        engine = MigrationEngine(
            db_path=str(tmp_db),
            migrations_dir=str(tmp_migrations_dir),
        )
        engine.plan()
        with sqlite3.connect(str(tmp_db)) as conn:
            conn.execute(
                """CREATE TRIGGER fail_success_tracking
                   BEFORE INSERT ON schema_migrations
                   WHEN NEW.version = 1 AND NEW.success = 1
                   BEGIN
                       SELECT RAISE(ABORT, 'injected success tracking failure');
                   END"""
            )
            conn.commit()

        with pytest.raises(MigrationError, match="injected success tracking failure"):
            engine.apply_pending()

        with sqlite3.connect(str(tmp_db)) as conn:
            table = conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'tracking_rollback'"
            ).fetchone()
            failure = conn.execute(
                "SELECT success FROM schema_migrations WHERE version = 1"
            ).fetchone()
        assert table is None
        assert failure == (0,)

    def test_sql_migration_acquires_immediate_write_lock(
        self,
        tmp_db,
        tmp_migrations_dir,
        monkeypatch,
    ):
        (tmp_migrations_dir / "001_sql_lock.sql").write_text(
            "CREATE TABLE sql_lock_proof (value TEXT);"
        )
        original_connect = sqlite3.connect
        observed_locks: list[bool] = []

        class LockCheckingConnection(sqlite3.Connection):
            def execute(self, sql, *args, **kwargs):
                result = super().execute(sql, *args, **kwargs)
                if sql.strip().upper() == "BEGIN IMMEDIATE":
                    observer = original_connect(str(tmp_db), timeout=0)
                    try:
                        with pytest.raises(sqlite3.OperationalError, match="locked|busy"):
                            observer.execute("BEGIN IMMEDIATE")
                    finally:
                        observer.close()
                    observed_locks.append(True)
                return result

        def connect(*args, **kwargs):
            kwargs["factory"] = LockCheckingConnection
            return original_connect(*args, **kwargs)

        monkeypatch.setattr("core.migrations.sqlite3.connect", connect)
        engine = MigrationEngine(
            db_path=str(tmp_db),
            migrations_dir=str(tmp_migrations_dir),
        )

        result = engine.apply_pending()

        assert [item["version"] for item in result] == [1]
        assert observed_locks == [True]


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
    """Original ingestion audit table (task_run_log)."""

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
            assert "ingestion_runs" not in tables


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
# Validation — migration 003 (original SQL — legacy PIT tables)
# ═══════════════════════════════════════════════════════════════════════


class TestMigration003:
    """Original point-in-time tables (legacy schema)."""

    def test_creates_legacy_tables(self, tmp_db):
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
                assert expected in tables, f"missing legacy table {expected}"
            for novel in ("quarterly_financials_history", "concept_member_history"):
                assert novel not in tables, f"new table {novel} should not exist yet"


# ═══════════════════════════════════════════════════════════════════════
# Validation — migration 006+007 (reconciliation → new PIT tables)
# ═══════════════════════════════════════════════════════════════════════


class TestMigration007:
    """Reconciled interval-based PIT tables (after 006+007)."""

    def test_reconciles_to_new_tables(self, tmp_db):
        migrations_dir = Path(__file__).resolve().parent.parent / "migrations"
        if not migrations_dir.is_dir():
            pytest.skip("migrations/ directory not found")

        engine = MigrationEngine(
            db_path=str(tmp_db),
            migrations_dir=str(migrations_dir),
        )
        engine.apply_pending()

        with sqlite3.connect(str(tmp_db)) as conn:
            tables = {
                r[0]
                for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            for expected in (
                "ingestion_runs",
                "ingestion_rejections",
                "quarterly_financials_history",
                "quarterly_financials",
                "concept_member_history",
                "index_member_history",
            ):
                assert expected in tables, f"missing table {expected}"
            for legacy in ("task_run_log", "financial_history_pt", "concept_member_pt", "index_member_pt"):
                assert legacy not in tables, f"legacy table {legacy} should have been dropped"


# ═══════════════════════════════════════════════════════════════════════
# Validation — migration 008 (orphan reconciliation and 006 checksum bridge)
# ═══════════════════════════════════════════════════════════════════════


@pytest.mark.parametrize(
    "recorded_checksum",
    [_PUBLISHED_006_CHECKSUM, _TRANSITIONAL_006_CHECKSUM],
)
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


@pytest.mark.parametrize(
    "recorded_checksum",
    [_PUBLISHED_006_CHECKSUM, _TRANSITIONAL_006_CHECKSUM],
)
def test_migration_008_rolls_back_orphan_seed_when_reconcile_fails(
    tmp_db,
    recorded_checksum,
):
    engine = _prepare_version_7_db(
        str(tmp_db),
        recorded_checksum,
        orphan_run_id="orphan-run",
    )
    with sqlite3.connect(str(tmp_db)) as conn:
        conn.execute(
            """CREATE TRIGGER fail_008_checksum_reconcile
               BEFORE UPDATE OF checksum ON schema_migrations
               WHEN OLD.version = 6
               BEGIN
                   SELECT RAISE(ABORT, 'injected checksum reconcile failure');
               END"""
        )
        conn.commit()

    with pytest.raises(MigrationError, match="injected checksum reconcile failure"):
        engine.apply_pending()

    with sqlite3.connect(str(tmp_db)) as conn:
        parent_count = conn.execute(
            "SELECT COUNT(*) FROM ingestion_runs WHERE run_id = 'orphan-run'"
        ).fetchone()[0]
        stored = conn.execute(
            "SELECT checksum FROM schema_migrations WHERE version = 6"
        ).fetchone()[0]
        version_8 = conn.execute(
            "SELECT success FROM schema_migrations WHERE version = 8"
        ).fetchone()
    assert parent_count == 0
    assert stored == recorded_checksum
    assert version_8 == (0,)
