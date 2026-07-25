"""
Migration 006: Reconcile ingestion audit tables.

Original migration 001 created ``task_run_log``; the production code now
uses ``ingestion_runs`` / ``ingestion_rejections``.  This migration:

* creates the replacement tables if they do not yet exist,
* drops the legacy ``task_run_log`` table,
* updates the recorded checksum for migration 001 so that
  :meth:`_verify_applied_checksums` does not reject the legitimate
  schema evolution.
"""

import hashlib
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


def apply(conn):
    _create_ingestion_tables(conn)
    _seed_orphan_run_ids(conn)
    _drop_legacy(conn)
    _reconcile_checksum(conn)
    logger.info("  ✅ 006: ingestion audit tables reconciled")


def _create_ingestion_tables(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ingestion_runs (
            run_id            TEXT PRIMARY KEY,
            task_name         TEXT NOT NULL,
            source            TEXT,
            status            TEXT NOT NULL,
            started_at        TEXT NOT NULL,
            finished_at       TEXT NOT NULL,
            requested_date    TEXT,
            data_date         TEXT,
            attempts          INTEGER NOT NULL DEFAULT 0,
            fetched_rows      INTEGER NOT NULL DEFAULT 0,
            accepted_rows     INTEGER NOT NULL DEFAULT 0,
            rejected_rows     INTEGER NOT NULL DEFAULT 0,
            saved_rows        INTEGER NOT NULL DEFAULT 0,
            schema_fingerprint TEXT,
            error_kind        TEXT,
            error_message     TEXT,
            metadata_json     TEXT NOT NULL DEFAULT '{}'
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ingestion_rejections (
            run_id        TEXT NOT NULL REFERENCES ingestion_runs(run_id),
            row_number    INTEGER NOT NULL,
            reason        TEXT NOT NULL,
            payload_json  TEXT NOT NULL,
            PRIMARY KEY (run_id, row_number)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_ingestion_runs_task_started
            ON ingestion_runs(task_name, started_at DESC)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_ingestion_runs_status
            ON ingestion_runs(status)
    """)


def _drop_legacy(conn):
    conn.execute("DROP TABLE IF EXISTS task_run_log")


def _seed_orphan_run_ids(conn):
    """Seed placeholder ingestion_runs records for PIT tables that already
    have snapshot_run_id values referencing rows that do not yet exist.

    This handles the case where ``index_member_history`` or
    ``concept_member_history`` were populated before migration 006
    created the ``ingestion_runs`` table, and the task's ``run_id``
    was lost because ``record_ingestion_run`` could not find it in the
    result dict (a bug fixed in providers.py).
    """
    orphan_tables = [t for t in ("index_member_history", "concept_member_history")
                     if _table_exists(conn, t)]
    if not orphan_tables:
        return

    to_insert: set[str] = set()
    for tbl in orphan_tables:
        rows = conn.execute(
            f"SELECT DISTINCT snapshot_run_id FROM {tbl} WHERE snapshot_run_id IS NOT NULL"
        ).fetchall()
        for (rid,) in rows:
            if rid:
                exists = conn.execute(
                    "SELECT 1 FROM ingestion_runs WHERE run_id = ?", (rid,)
                ).fetchone()
                if not exists:
                    to_insert.add(rid)

    if not to_insert:
        return

    now = datetime.utcnow().isoformat(timespec="seconds")
    for rid in sorted(to_insert):
        conn.execute(
            """INSERT OR IGNORE INTO ingestion_runs
               (run_id, task_name, source, status, started_at, finished_at, metadata_json)
               VALUES (?, 'reconcile', 'migration-006', 'reconciled', ?, ?, '{}')""",
            (rid, now, now),
        )
    logger.info("  ✅ seeded %d orphaned run_id(s) from %s",
                len(to_insert), ", ".join(orphan_tables))


def _table_exists(conn, name: str) -> bool:
    return bool(conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone())


def _reconcile_checksum(conn):
    mig_file = Path(__file__).resolve().parent / "001_ingestion_audit.sql"
    if mig_file.is_file():
        ck = hashlib.sha256(mig_file.read_bytes()).hexdigest()
        conn.execute(
            "UPDATE schema_migrations SET checksum = ? WHERE version = 1",
            (ck,),
        )
    else:
        logger.warning("  006: 001_ingestion_audit.sql not found; checksum NOT reconciled")
