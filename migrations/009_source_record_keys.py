"""Migration 009: add deterministic source-record keys."""

from __future__ import annotations

import logging

from core import source_record_key as source_key_module
from core.source_record_key import (
    INSTITUTION_SURVEY_SOURCE_KEY_FIELDS,
    STOCK_REPURCHASE_SOURCE_KEY_FIELDS,
)

logger = logging.getLogger(__name__)


def apply(conn):
    _rebuild_stock_repurchase(conn)
    _rebuild_institution_survey(conn)
    logger.info("  ✅ 009: source record keys reconciled")


def _rebuild_stock_repurchase(conn):
    columns = _columns(conn, "stock_repurchase")
    if not columns:
        logger.info("  ➖ stock_repurchase does not exist, skipped")
        return

    rows = _read_rows(
        conn,
        "stock_repurchase",
        (
            "id",
            "trade_date",
            "stock_code",
            "stock_name",
            "repurchase_amount",
            "repurchase_price",
            "repurchase_price_lower",
            "repurchase_price_upper",
            "repurchase_quantity",
            "progress_status",
        ),
    )
    conn.execute("DROP TABLE IF EXISTS stock_repurchase__v9")
    conn.execute("""
        CREATE TABLE stock_repurchase__v9 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date TEXT NOT NULL,
            stock_code TEXT NOT NULL,
            stock_name TEXT,
            repurchase_amount REAL,
            repurchase_price REAL,
            repurchase_price_lower REAL,
            repurchase_price_upper REAL,
            repurchase_quantity INTEGER,
            progress_status TEXT,
            source_record_key TEXT NOT NULL UNIQUE
        )
    """)
    conn.executemany(
        """INSERT INTO stock_repurchase__v9
           (id, trade_date, stock_code, stock_name, repurchase_amount,
            repurchase_price, repurchase_price_lower, repurchase_price_upper,
            repurchase_quantity, progress_status, source_record_key)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [_stock_repurchase_tuple(row) for row in rows],
    )
    conn.execute("DROP TABLE stock_repurchase")
    conn.execute("ALTER TABLE stock_repurchase__v9 RENAME TO stock_repurchase")


def _rebuild_institution_survey(conn):
    columns = _columns(conn, "institution_survey")
    if not columns:
        logger.info("  ➖ institution_survey does not exist, skipped")
        return

    rows = _read_rows(
        conn,
        "institution_survey",
        (
            "id",
            "trade_date",
            "stock_code",
            "stock_name",
            "survey_org",
            "survey_type",
            "survey_count",
        ),
    )
    conn.execute("DROP TABLE IF EXISTS institution_survey__v9")
    conn.execute("""
        CREATE TABLE institution_survey__v9 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date TEXT NOT NULL,
            stock_code TEXT NOT NULL,
            stock_name TEXT,
            survey_org TEXT,
            survey_type TEXT,
            survey_count INTEGER,
            source_record_key TEXT NOT NULL UNIQUE
        )
    """)
    conn.executemany(
        """INSERT INTO institution_survey__v9
           (id, trade_date, stock_code, stock_name, survey_org, survey_type,
            survey_count, source_record_key)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        [_institution_survey_tuple(row) for row in rows],
    )
    conn.execute("DROP TABLE institution_survey")
    conn.execute("ALTER TABLE institution_survey__v9 RENAME TO institution_survey")


def _columns(conn, table):
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}


def _read_rows(conn, table, target_columns):
    existing_columns = _columns(conn, table)
    selected_columns = [column for column in target_columns if column in existing_columns]
    if not selected_columns:
        return []
    rows = conn.execute(
        f"SELECT {', '.join(selected_columns)} FROM {table} ORDER BY id"
    ).fetchall()
    return [
        dict(zip(selected_columns, row, strict=True))
        for row in rows
        if row[selected_columns.index("trade_date")] and row[selected_columns.index("stock_code")]
    ]


def _stock_repurchase_tuple(row):
    record = {
        "trade_date": row.get("trade_date"),
        "stock_code": row.get("stock_code"),
        "stock_name": row.get("stock_name"),
        "repurchase_amount": row.get("repurchase_amount"),
        "repurchase_price": row.get("repurchase_price"),
        "repurchase_price_lower": row.get("repurchase_price_lower"),
        "repurchase_price_upper": row.get("repurchase_price_upper"),
        "repurchase_quantity": row.get("repurchase_quantity"),
        "progress_status": row.get("progress_status"),
    }
    return (
        row.get("id"),
        record["trade_date"],
        record["stock_code"],
        record["stock_name"],
        record["repurchase_amount"],
        record["repurchase_price"],
        record["repurchase_price_lower"],
        record["repurchase_price_upper"],
        record["repurchase_quantity"],
        record["progress_status"],
        source_key_module.source_record_key(record, STOCK_REPURCHASE_SOURCE_KEY_FIELDS),
    )


def _institution_survey_tuple(row):
    record = {
        "trade_date": row.get("trade_date"),
        "stock_code": row.get("stock_code"),
        "stock_name": row.get("stock_name"),
        "survey_org": row.get("survey_org"),
        "survey_type": row.get("survey_type"),
        "survey_count": row.get("survey_count"),
    }
    return (
        row.get("id"),
        record["trade_date"],
        record["stock_code"],
        record["stock_name"],
        record["survey_org"],
        record["survey_type"],
        record["survey_count"],
        source_key_module.source_record_key(record, INSTITUTION_SURVEY_SOURCE_KEY_FIELDS),
    )
