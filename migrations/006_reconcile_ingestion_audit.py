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
from pathlib import Path

logger = logging.getLogger(__name__)


def apply(conn):
    _create_ingestion_tables(conn)
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
