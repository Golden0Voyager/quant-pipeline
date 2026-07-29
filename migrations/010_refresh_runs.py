"""Migration 010: persist close-refresh run audit records."""

from __future__ import annotations


def apply(conn):
    """Create close-refresh run and per-task audit tables."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS refresh_runs (
            run_id       TEXT PRIMARY KEY,
            target_date  TEXT NOT NULL,
            started_at   TEXT NOT NULL,
            finished_at  TEXT,
            status       TEXT NOT NULL CHECK (
                status IN (
                    'running', 'success', 'no_data', 'degraded', 'failed',
                    'aborted'
                )
            ),
            symbols_json TEXT NOT NULL DEFAULT '[]'
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS refresh_task_runs (
            run_id         TEXT NOT NULL REFERENCES refresh_runs(run_id)
                           ON DELETE CASCADE,
            task_name       TEXT NOT NULL,
            policy_kind     TEXT NOT NULL,
            requested_date  TEXT NOT NULL,
            as_of_date      TEXT,
            status          TEXT NOT NULL CHECK (
                status IN ('success', 'no_data', 'degraded', 'failed', 'aborted')
            ),
            fetched         INTEGER NOT NULL DEFAULT 0 CHECK (
                typeof(fetched) = 'integer' AND fetched >= 0
            ),
            validated       INTEGER NOT NULL DEFAULT 0 CHECK (
                typeof(validated) = 'integer' AND validated >= 0
            ),
            replaced        INTEGER NOT NULL DEFAULT 0 CHECK (
                typeof(replaced) = 'integer' AND replaced >= 0
            ),
            retained        INTEGER NOT NULL DEFAULT 0 CHECK (
                typeof(retained) = 'integer' AND retained >= 0
            ),
            failed          INTEGER NOT NULL DEFAULT 0 CHECK (
                typeof(failed) = 'integer' AND failed >= 0
            ),
            metadata_json   TEXT NOT NULL DEFAULT '{}',
            PRIMARY KEY (run_id, task_name)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_refresh_runs_target_date
        ON refresh_runs(target_date)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_refresh_task_runs_status
        ON refresh_task_runs(status)
    """)
