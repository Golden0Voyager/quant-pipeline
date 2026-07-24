"""
Migration 007: Reconcile point-in-time tracking tables.

Original migration 003 created ``financial_history_pt``, ``concept_member_pt``
and ``index_member_pt``.  The downstream code now expects the interval-based
PIT tables (``quarterly_financials_history``, ``quarterly_financials``,
``concept_member_history``, ``index_member_history``).

This migration:

* creates the replacement tables if they do not yet exist,
* drops the legacy point-in-time tables,
* updates the recorded checksum for migration 003.
"""

import hashlib
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def apply(conn):
    _create_pit_tables(conn)
    _drop_legacy(conn)
    _reconcile_checksum(conn)
    logger.info("  ✅ 007: PIT tables reconciled")


def _create_pit_tables(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS quarterly_financials_history (
            ts_code          TEXT NOT NULL,
            report_period    TEXT NOT NULL,
            publish_date     TEXT NOT NULL,
            revenue          REAL,
            net_profit       REAL,
            deduct_profit    REAL,
            operating_cashflow REAL,
            rd_expense       REAL,
            gross_margin     REAL,
            net_margin       REAL,
            roe              REAL,
            debt_ratio       REAL,
            revenue_growth   REAL,
            profit_growth    REAL,
            eps              REAL,
            bps              REAL,
            free_cashflow    REAL,
            rd_expense_pct   REAL,
            inventory_turnover REAL,
            receivable_days  REAL,
            roic             REAL,
            data_source      TEXT,
            updated_at       DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (ts_code, report_period, publish_date)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_quarterly_financials_history_code
            ON quarterly_financials_history(ts_code, report_period DESC)
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS quarterly_financials (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_code          TEXT NOT NULL,
            report_period    TEXT NOT NULL,
            revenue          REAL,
            net_profit       REAL,
            operating_cashflow REAL,
            roe              REAL,
            gross_margin     REAL,
            net_margin       REAL,
            revenue_growth   REAL,
            profit_growth    REAL,
            debt_ratio       REAL,
            eps              REAL,
            bps              REAL,
            data_source      TEXT DEFAULT 'akshare',
            updated_at       DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(ts_code, report_period)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_quarterly_financials_code
            ON quarterly_financials(ts_code, report_period DESC)
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS concept_member_history (
            concept_code     TEXT NOT NULL,
            concept_name     TEXT NOT NULL,
            ts_code          TEXT NOT NULL,
            valid_from       TEXT NOT NULL,
            valid_to         TEXT,
            source           TEXT NOT NULL,
            snapshot_run_id  TEXT NOT NULL REFERENCES ingestion_runs(run_id),
            PRIMARY KEY (concept_code, ts_code, valid_from)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_concept_member_history_interval
            ON concept_member_history(concept_code, ts_code, valid_to)
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS index_member_history (
            index_code       TEXT NOT NULL,
            index_name       TEXT NOT NULL,
            ts_code          TEXT NOT NULL,
            weight           REAL,
            valid_from       TEXT NOT NULL,
            valid_to         TEXT,
            source           TEXT NOT NULL,
            snapshot_run_id  TEXT NOT NULL REFERENCES ingestion_runs(run_id),
            PRIMARY KEY (index_code, ts_code, valid_from)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_index_member_history_interval
            ON index_member_history(index_code, ts_code, valid_to)
    """)


def _drop_legacy(conn):
    for tbl in ("financial_history_pt", "concept_member_pt", "index_member_pt"):
        conn.execute(f"DROP TABLE IF EXISTS {tbl}")


def _reconcile_checksum(conn):
    mig_file = Path(__file__).resolve().parent / "003_point_in_time_tables.sql"
    if mig_file.is_file():
        ck = hashlib.sha256(mig_file.read_bytes()).hexdigest()
        conn.execute(
            "UPDATE schema_migrations SET checksum = ? WHERE version = 3",
            (ck,),
        )
    else:
        logger.warning("  007: 003_point_in_time_tables.sql not found; checksum NOT reconciled")
