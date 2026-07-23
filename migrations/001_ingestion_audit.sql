-- ═══════════════════════════════════════════════════════════════════════
-- Migration 001: Ingestion audit tracking
-- ═══════════════════════════════════════════════════════════════════════
-- Creates the task_run_log table that records every pipeline task
-- execution with status, row counts and timing — replacing ad-hoc
-- record_task_run / get_last_task_run patterns.

CREATE TABLE IF NOT EXISTS task_run_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    task_name   TEXT NOT NULL,
    run_date    TEXT NOT NULL,          -- YYYY-MM-DD
    status      TEXT NOT NULL DEFAULT 'success',
    attempted   INTEGER NOT NULL DEFAULT 0,
    fetched     INTEGER NOT NULL DEFAULT 0,
    accepted    INTEGER NOT NULL DEFAULT 0,
    rejected    INTEGER NOT NULL DEFAULT 0,
    saved       INTEGER NOT NULL DEFAULT 0,
    source      TEXT,
    error_kind  TEXT,
    error_msg   TEXT,
    elapsed_ms  INTEGER NOT NULL DEFAULT 0,
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_task_run_log_name_date
    ON task_run_log(task_name, run_date DESC);

CREATE INDEX IF NOT EXISTS idx_task_run_log_date
    ON task_run_log(run_date DESC);
