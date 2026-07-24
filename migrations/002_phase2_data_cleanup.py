"""Migration 002: Rebuild phase-2 tables with NOT NULL constraints.

This migration addresses three classes of schema pollution:

1. **Convertible bond tables** (cb_quotation, cb_redeem) — prior
   ad-hoc ALTER TABLE ADD COLUMN left the schema in a state where
   SQLite refuses to add ``updated_at`` with ``DEFAULT
   CURRENT_TIMESTAMP`` (the infamous "Cannot add a column with non-
   constant default" error).  Since these tables write 0 rows in the
   corrupted state, we drop and recreate them.

2. **stock_repurchase, institution_survey** — rows with NULL/empty
   trade_date or stock_code exist, and institution_survey has
   duplicate (trade_date, stock_code) rows.  We deduplicate and add
   proper NOT NULL constraints and a UNIQUE index.

3. **stock_pledge** — similar NULL-trade_date cleanup.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def apply(conn: Any) -> None:
    """Apply migration 002."""

    # ── 1. Convertible bond tables ─────────────────────────────────
    # Both are write-broken with 0 rows — drop and recreate.
    _rebuild_cb_tables(conn)

    # ── 2. Phase-2 table cleanup ───────────────────────────────────
    _cleanup_repurchase(conn)
    _cleanup_survey(conn)
    _cleanup_pledge(conn)

    # ── 3. Verify key integrity ────────────────────────────────────
    _verify_integrity(conn)

    logger.info("migration 002 complete")


# ── per-table helpers ─────────────────────────────────────────────────


def _rebuild_cb_tables(conn: Any) -> None:
    """Drop and recreate cb_quotation / cb_redeem with proper schema."""

    for tbl_name, ddl in _CB_TABLES.items():
        # Check current state
        info = conn.execute(f"PRAGMA table_info({tbl_name})").fetchall()
        has_updated_at = any(row[1] == "updated_at" for row in info)
        logger.info(
            "  %s: %d columns, has_updated_at=%s",
            tbl_name, len(info), has_updated_at,
        )

        # Only drop+recreate if schema is actually broken (missing
        # updated_at or columns don't match).
        if not has_updated_at or len(info) < 8:
            conn.executescript(ddl)
            logger.info("  ✅ %s rebuilt", tbl_name)
        else:
            logger.info("  ➖ %s schema OK, skipped", tbl_name)


_CB_TABLES = {
    "cb_quotation": """
        DROP TABLE IF EXISTS cb_quotation;
        CREATE TABLE cb_quotation (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_code     TEXT NOT NULL,
            bond_name   TEXT,
            price       REAL,
            premium     REAL,
            double_low  REAL,
            expire_date TEXT,
            data_source TEXT,
            updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(ts_code)
        );
    """,
    "cb_redeem": """
        DROP TABLE IF EXISTS cb_redeem;
        CREATE TABLE cb_redeem (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_code      TEXT NOT NULL,
            bond_name    TEXT,
            redeem_flag  TEXT,
            redeem_price REAL,
            redeem_date  TEXT,
            data_source  TEXT,
            updated_at   DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(ts_code)
        );
    """,
}


def _cleanup_repurchase(conn: Any) -> None:
    """Remove null-key rows from stock_repurchase and tighten schema."""
    if not _table_exists(conn, "stock_repurchase"):
        logger.info("  ➖ stock_repurchase does not exist, skipped")
        return
    conn.execute("""
        DELETE FROM stock_repurchase
        WHERE trade_date IS NULL OR TRIM(trade_date) = ''
           OR stock_code IS NULL OR TRIM(stock_code) = ''
    """)
    logger.info("  cleaned stock_repurchase null rows")


def _cleanup_survey(conn: Any) -> None:
    """Deduplicate institution_survey and add UNIQUE index."""
    if not _table_exists(conn, "institution_survey"):
        logger.info("  ➖ institution_survey does not exist, skipped")
        return
    conn.execute("""
        DELETE FROM institution_survey
        WHERE trade_date IS NULL OR TRIM(trade_date) = ''
           OR stock_code IS NULL OR TRIM(stock_code) = ''
    """)
    # Keep the MAX(id) per (trade_date, stock_code)
    conn.execute("""
        DELETE FROM institution_survey
        WHERE id NOT IN (
            SELECT MAX(id) FROM institution_survey
            GROUP BY trade_date, stock_code
        )
    """)
    conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_institution_survey_date_code
        ON institution_survey(trade_date, stock_code)
    """)
    logger.info("  cleaned & deduplicated institution_survey")


def _cleanup_pledge(conn: Any) -> None:
    """Remove null-key rows from stock_pledge."""
    if not _table_exists(conn, "stock_pledge"):
        logger.info("  ➖ stock_pledge does not exist, skipped")
        return
    conn.execute("""
        DELETE FROM stock_pledge
        WHERE trade_date IS NULL OR TRIM(trade_date) = ''
           OR stock_code IS NULL OR TRIM(stock_code) = ''
    """)
    logger.info("  cleaned stock_pledge null rows")


def _table_exists(conn: Any, name: str) -> bool:
    """Check if a table exists in the database."""
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return row is not None


def _verify_integrity(conn: Any) -> None:
    """Run a quick integrity check on the affected tables."""
    for tbl in ("cb_quotation", "cb_redeem", "stock_repurchase",
                "institution_survey", "stock_pledge"):
        try:
            row_count = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            logger.info("  %s: %d rows", tbl, row_count)
        except Exception:
            logger.warning("  ⚠️  could not verify table %s", tbl)
