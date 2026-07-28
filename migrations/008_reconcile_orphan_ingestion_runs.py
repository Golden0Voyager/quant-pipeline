"""Migration 008: reconcile orphaned PIT run IDs and bridge migration 006."""

import hashlib
import logging
from datetime import UTC, datetime
from pathlib import Path

logger = logging.getLogger(__name__)

PUBLISHED_006_CHECKSUM = (
    "a783c28347a05f415f4f6b4dd15f068cde964194657cea3c1573523085af65e0"
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
