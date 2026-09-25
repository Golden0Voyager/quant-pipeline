"""
Versioned schema migration engine.
═══════════════════════════════

Replaces ad-hoc ``_ensure_tables`` / ``_migrate_phase2_tables`` calls in
providers.py with a tracked, versioned migration system.

Convention
──────────
Migration files live in ``migrations/`` and are named
``<version>_<description>.sql`` (pure SQL) or
``<version>_<description>.py`` (Python script exporting ``apply(conn)``).

A ``schema_migrations`` tracking table records every applied migration
along with its checksum and duration, so the engine never re-applies a
migration that has already run.
"""

from __future__ import annotations

import hashlib
import importlib.util
import logging
import sqlite3
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from core.db_pragmas import apply_write_pragmas

logger = logging.getLogger(__name__)


# ── errors ─────────────────────────────────────────────────────────────


class MigrationError(RuntimeError):
    """Raised when a migration fails to apply."""


# ── types ──────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class MigrationScript:
    """A single, versioned schema migration.

    Attributes
    ----------
    version:
        Monotonically-increasing version number.
    description:
        Human-readable summary (the file-name stem minus the version
        prefix).
    source_path:
        Path to the migration file (``.sql`` or ``.py``).
    checksum:
        SHA-256 of the file content.  Used to detect tampering or drift
        after a migration has been applied.
    sql:
        Inline SQL to execute.  Mutually exclusive with *apply_func*.
    apply_func:
        Python callable ``(sqlite3.Connection) -> None`` for complex
        migrations that cannot be expressed as pure SQL.
    """

    version: int
    description: str
    source_path: str | None = None
    checksum: str = ""
    sql: str | None = None
    apply_func: Callable | None = None

    _SQLLite3_Connection: Any = field(default=None, repr=False, compare=False)


_TRANSACTION_CONTROL_KEYWORDS = frozenset({
    "BEGIN",
    "COMMIT",
    "END",
    "ROLLBACK",
    "SAVEPOINT",
    "RELEASE",
})


def _iter_sql_statements(script: str) -> Iterator[str]:
    """Yield complete SQLite statements without invoking ``executescript``."""
    statement = ""
    for character in script:
        statement += character
        if sqlite3.complete_statement(statement):
            if statement.strip():
                yield statement
            statement = ""
    if statement.strip():
        yield statement


def _first_sql_keyword(statement: str) -> str:
    """Return the first keyword, ignoring leading whitespace and comments."""
    index = 0
    while index < len(statement):
        while index < len(statement) and (
            statement[index].isspace() or statement[index] == "\ufeff"
        ):
            index += 1
        if statement.startswith("--", index):
            newline = statement.find("\n", index + 2)
            if newline == -1:
                return ""
            index = newline + 1
            continue
        if statement.startswith("/*", index):
            comment_end = statement.find("*/", index + 2)
            if comment_end == -1:
                return ""
            index = comment_end + 2
            continue
        break

    keyword_end = index
    while keyword_end < len(statement) and statement[keyword_end].isalpha():
        keyword_end += 1
    return statement[index:keyword_end].upper()


def _reject_transaction_control(statement: str) -> None:
    keyword = _first_sql_keyword(statement)
    if keyword in _TRANSACTION_CONTROL_KEYWORDS:
        raise MigrationError(f"migration scripts must not control transactions: {keyword}")


class _TransactionalMigrationConnection:
    """Connection facade that keeps Python migration scripts in the outer transaction.

    ``sqlite3.Connection.executescript`` commits an active transaction before
    executing its script.  Python migrations receive this facade instead, so
    their scripts are split with SQLite's own statement-completeness parser
    and each statement is executed on the already-open raw connection.
    """

    def __init__(self, conn: Any) -> None:
        self.__conn = conn

    def execute(self, *args: Any, **kwargs: Any) -> Any:
        _reject_transaction_control(args[0])
        return self.__conn.execute(*args, **kwargs)

    def executemany(self, *args: Any, **kwargs: Any) -> Any:
        _reject_transaction_control(args[0])
        return self.__conn.executemany(*args, **kwargs)

    def executescript(self, script: str) -> None:
        for statement in _iter_sql_statements(script):
            self.execute(statement)

    def commit(self) -> None:
        raise MigrationError("Python migrations must not commit the outer transaction")

    def rollback(self) -> None:
        raise MigrationError("Python migrations must not roll back the outer transaction")

# ── engine ─────────────────────────────────────────────────────────────


class MigrationEngine:
    """Versioned migration engine for a single SQLite database.

    Parameters
    ----------
    db_path:
        Filesystem path to the SQLite database.
    migrations_dir:
        Directory containing ``<version>_<description>.{sql,py}`` files.
    """

    def __init__(
        self,
        db_path: str,
        migrations_dir: str | Path,
    ) -> None:
        self._db_path = db_path
        self._migrations_dir = Path(migrations_dir)

    # ── public API ──────────────────────────────────────────────────

    def plan(self) -> list[dict[str, Any]]:
        """Return an ordered list of pending migrations (no side effects)."""
        return [
            {"version": m.version, "description": m.description, "checksum": m.checksum}
            for m in self._pending(self._load())
        ]

    def apply_pending(
        self,
        dry_run: bool = False,
        target_version: int | None = None,
    ) -> list[dict[str, Any]]:
        """Apply all pending migrations up to *target_version*.

        Args:
            dry_run:
                When true, report what *would* be applied without
                executing anything.
            target_version:
                Apply only migrations whose version <= this value.
                ``None`` means apply everything pending.

        Returns:
            A list of result dicts, one per applied migration::

                {
                    "version": int,
                    "description": str,
                    "applied": bool,    # True = committed
                    "duration_ms": int,
                    "error": str | None,
                }

        Raises:
            MigrationError:
                When a single migration fails and the engine cannot
                continue (earlier migrations are rolled back; later
                ones are skipped).
        """
        import sqlite3

        all_migrations = self._load()

        conn = sqlite3.connect(str(self._db_path), timeout=30.0)
        apply_write_pragmas(conn, busy_timeout_ms=30000)

        try:
            self._ensure_tracking_table(conn)
            applied = self._applied_versions(conn)
            results: list[dict[str, Any]] = []

            for mig in all_migrations:
                if mig.version in applied:
                    continue
                if target_version is not None and mig.version > target_version:
                    continue

                if dry_run:
                    results.append({
                        "version": mig.version,
                        "description": mig.description,
                        "applied": False,
                        "duration_ms": 0,
                        "error": None,
                    })
                    continue

                result = self._apply_one(conn, mig)
                results.append(result)
                if result["error"]:
                    raise MigrationError(
                        f"migration {mig.version} ({mig.description}) failed: "
                        f"{result['error']}"
                    )

            conn.commit()

            # Verify checksums after applying pending migrations so that
            # reconciliation migrations (e.g. 004, 005) can update recorded
            # checksums for edited base migrations before we enforce integrity.
            self._verify_applied_checksums(all_migrations)

            return results
        finally:
            conn.close()

    # ── internal helpers ────────────────────────────────────────────

    @staticmethod
    def _ensure_tracking_table(conn: Any) -> None:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version     INTEGER PRIMARY KEY,
                description TEXT NOT NULL,
                applied_at  TEXT NOT NULL DEFAULT (datetime('now')),
                checksum    TEXT NOT NULL,
                duration_ms INTEGER NOT NULL DEFAULT 0,
                success     INTEGER NOT NULL DEFAULT 1
            )
        """)
        conn.commit()

    @staticmethod
    def _applied_versions(conn: Any) -> set[int]:
        rows = conn.execute(
            "SELECT version FROM schema_migrations WHERE success = 1"
        ).fetchall()
        return {r[0] for r in rows}

    def _verify_applied_checksums(
        self,
        migrations: list[MigrationScript],
    ) -> None:
        """Verify that already-applied migrations have not drifted.

        A checksum mismatch means the migration file was edited after it was
        applied, which breaks reproducibility and rollback guarantees.
        """
        import sqlite3

        conn = sqlite3.connect(str(self._db_path), timeout=10.0)
        try:
            self._ensure_tracking_table(conn)
            rows = conn.execute(
                "SELECT version, checksum FROM schema_migrations WHERE success = 1"
            ).fetchall()
            recorded = dict(rows)
            for mig in migrations:
                if mig.version in recorded and recorded[mig.version] != mig.checksum:
                    raise MigrationError(
                        f"checksum mismatch for migration {mig.version} "
                        f"({mig.description}): file has changed since it was applied"
                    )
        finally:
            conn.close()

    def _load(self) -> list[MigrationScript]:
        if not self._migrations_dir.is_dir():
            logger.warning("migrations directory %s not found", self._migrations_dir)
            return []

        migrations: list[MigrationScript] = []
        for entry in sorted(self._migrations_dir.iterdir()):
            if entry.suffix not in (".sql", ".py"):
                continue
            parts = entry.stem.split("_", 1)
            try:
                version = int(parts[0])
            except (ValueError, IndexError):
                logger.warning("skipping migration with non-numeric prefix: %s", entry.name)
                continue
            description = parts[1] if len(parts) > 1 else entry.stem
            checksum = self._checksum(entry)

            if entry.suffix == ".sql":
                sql = entry.read_text(encoding="utf-8")
                migrations.append(MigrationScript(
                    version=version,
                    description=description,
                    source_path=str(entry),
                    checksum=checksum,
                    sql=sql,
                ))
            elif entry.suffix == ".py":
                migrations.append(MigrationScript(
                    version=version,
                    description=description,
                    source_path=str(entry),
                    checksum=checksum,
                    apply_func=_load_python_migration(entry),
                ))

        # Safety: reject duplicate versions
        seen: set[int] = set()
        for m in migrations:
            if m.version in seen:
                raise MigrationError(f"duplicate migration version {m.version}")
            seen.add(m.version)

        return migrations

    def _pending(self, all_migs: list[MigrationScript]) -> list[MigrationScript]:
        import sqlite3
        try:
            conn = sqlite3.connect(str(self._db_path), timeout=10.0)
            try:
                self._ensure_tracking_table(conn)
                applied = self._applied_versions(conn)
                return [m for m in all_migs if m.version not in applied]
            finally:
                conn.close()
        except sqlite3.Error:
            # DB not reachable / not initialised — all migrations are pending
            return all_migs

    def _apply_one(
        self,
        conn: Any,
        migration: MigrationScript,
    ) -> dict[str, Any]:
        logger.info("  ⏳ applying migration %03d: %s", migration.version, migration.description)
        start = time.time()

        try:
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute("BEGIN IMMEDIATE")
            migration_conn = _TransactionalMigrationConnection(conn)

            if migration.sql is not None:
                migration_conn.executescript(migration.sql)
            elif migration.apply_func is not None:
                migration.apply_func(migration_conn)
            else:
                raise MigrationError(
                    f"migration {migration.version} has neither sql nor apply_func"
                )

            # Post-migration integrity checks
            fk_violations = conn.execute("PRAGMA foreign_key_check").fetchall()
            if fk_violations:
                raise MigrationError(f"foreign key violations: {fk_violations}")
            quick = conn.execute("PRAGMA quick_check").fetchone()[0]
            if quick != "ok":
                raise MigrationError(f"PRAGMA quick_check failed: {quick}")

            duration_ms = int((time.time() - start) * 1000)
            conn.execute(
                "INSERT OR REPLACE INTO schema_migrations "
                "(version, description, applied_at, checksum, duration_ms, success) "
                "VALUES (?, ?, datetime('now'), ?, ?, 1)",
                (migration.version, migration.description, migration.checksum, duration_ms),
            )
            conn.commit()
            logger.info("  ✅ migration %03d applied in %dms", migration.version, duration_ms)
            return {
                "version": migration.version,
                "description": migration.description,
                "applied": True,
                "duration_ms": duration_ms,
                "error": None,
            }
        except Exception as exc:
            conn.rollback()
            duration_ms = int((time.time() - start) * 1000)
            logger.error(
                "  ❌ migration %03d failed after %dms: %s",
                migration.version, duration_ms, exc,
            )
            # Record the failure so operators can see it
            try:
                conn.execute(
                    "INSERT OR REPLACE INTO schema_migrations "
                    "(version, description, applied_at, checksum, duration_ms, success) "
                    "VALUES (?, ?, datetime('now'), ?, ?, 0)",
                    (migration.version, migration.description, migration.checksum, duration_ms),
                )
                conn.commit()
            except Exception:
                pass
            return {
                "version": migration.version,
                "description": migration.description,
                "applied": False,
                "duration_ms": duration_ms,
                "error": str(exc),
            }

    @staticmethod
    def _checksum(path: Path) -> str:
        return hashlib.sha256(path.read_bytes()).hexdigest()


# ── python migration loader ────────────────────────────────────────────


def _load_python_migration(path: Path) -> Callable:
    """Dynamically import a ``.py`` migration and return its ``apply`` function."""
    spec = importlib.util.spec_from_file_location(
        f"_migration_{path.stem}", str(path),
    )
    if spec is None or spec.loader is None:
        raise MigrationError(f"cannot load migration module: {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    if not hasattr(mod, "apply"):
        raise MigrationError(
            f"python migration {path.name} must export an 'apply(conn)' function"
        )
    return mod.apply


# ── convenience ────────────────────────────────────────────────────────


def run_migrations(
    db_path: str,
    migrations_dir: str | Path = "",
    dry_run: bool = False,
    target_version: int | None = None,
) -> list[dict[str, Any]]:
    """Shortcut: create engine, apply pending, return results."""
    if not migrations_dir:
        # Default to <project_root>/migrations/
        migrations_dir = Path(__file__).resolve().parent.parent / "migrations"
    engine = MigrationEngine(db_path=db_path, migrations_dir=migrations_dir)
    return engine.apply_pending(dry_run=dry_run, target_version=target_version)
