"""Safety tests for atomic close-refresh SQLite replacements."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from core.refresh_store import (
    CompositeReplacement,
    DateSnapshotReplacement,
    KeyedUpsertReplacement,
    RefreshValidationError,
    RunSnapshotReplacement,
    SQLiteRefreshStore,
)


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    path = tmp_path / "refresh.db"
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            PRAGMA foreign_keys = ON;

            CREATE TABLE quotes (
                ts_code TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                close REAL NOT NULL CHECK(close >= 0),
                source TEXT NOT NULL,
                PRIMARY KEY (ts_code, trade_date)
            );
            INSERT INTO quotes VALUES
                ('000001.SZ', '2026-07-24', 11.0, 'close'),
                ('000001.SZ', '2026-07-27', 10.0, 'intraday'),
                ('600000.SH', '2026-07-27', 20.0, 'intraday');

            CREATE TABLE events (
                event_date TEXT NOT NULL,
                stock_code TEXT NOT NULL,
                detail TEXT NOT NULL,
                UNIQUE(event_date, stock_code)
            );
            INSERT INTO events VALUES
                ('2026-07-24', '000001', 'historical'),
                ('2026-07-27', '000001', 'intraday');

            CREATE TABLE current_snapshot (
                code TEXT PRIMARY KEY,
                value REAL NOT NULL CHECK(value >= 0)
            );
            INSERT INTO current_snapshot VALUES
                ('OLD1', 1.0),
                ('OLD2', 2.0);

            CREATE TABLE sector_daily (
                sector_code TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                close REAL NOT NULL CHECK(close >= 0),
                PRIMARY KEY (sector_code, trade_date)
            );
            INSERT INTO sector_daily VALUES ('BK001', '2026-07-27', 10.0);

            CREATE TABLE sector_valuation (
                sector_code TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                pe REAL NOT NULL CHECK(pe >= 0),
                PRIMARY KEY (sector_code, trade_date)
            );
            INSERT INTO sector_valuation VALUES ('BK001', '2026-07-27', 15.0);

            CREATE TABLE parent_codes (
                code TEXT PRIMARY KEY
            );
            INSERT INTO parent_codes VALUES ('VALID');

            CREATE TABLE child_snapshot (
                code TEXT PRIMARY KEY,
                parent_code TEXT NOT NULL REFERENCES parent_codes(code)
            );
            INSERT INTO child_snapshot VALUES ('OLD', 'VALID');
            """
        )
    return path


@pytest.fixture
def store(db_path: Path) -> SQLiteRefreshStore:
    return SQLiteRefreshStore(db_path)


def _rows(db_path: Path, sql: str) -> list[tuple[object, ...]]:
    with sqlite3.connect(db_path) as conn:
        return conn.execute(sql).fetchall()


def _quotes_request(
    rows: tuple[tuple[object, ...], ...],
    *,
    minimum_coverage: float | None = None,
) -> DateSnapshotReplacement:
    return DateSnapshotReplacement(
        table="quotes",
        columns=("ts_code", "trade_date", "close", "source"),
        rows=rows,
        date_column="trade_date",
        date_value="2026-07-27",
        natural_keys=("ts_code", "trade_date"),
        required_fields=("ts_code", "trade_date", "close", "source"),
        minimum_coverage=minimum_coverage,
    )


def test_date_snapshot_replaces_complete_partition_and_preserves_history(
    store: SQLiteRefreshStore,
    db_path: Path,
) -> None:
    request = _quotes_request(
        (
            ("000001.SZ", "2026-07-27", 10.8, "close"),
            ("300001.SZ", "2026-07-27", 30.8, "close"),
        )
    )

    result = store.replace_date_snapshot(request)

    assert result.replaced == 2
    assert result.tables == ("quotes",)
    assert _rows(
        db_path,
        "SELECT ts_code, trade_date, close, source FROM quotes ORDER BY trade_date, ts_code",
    ) == [
        ("000001.SZ", "2026-07-24", 11.0, "close"),
        ("000001.SZ", "2026-07-27", 10.8, "close"),
        ("300001.SZ", "2026-07-27", 30.8, "close"),
    ]


@pytest.mark.parametrize(
    "rows",
    [
        (
            ("000001.SZ", "2026-07-27", 10.8, "close"),
            ("000001.SZ", "2026-07-27", 10.9, "close"),
        ),
        (("000001.SZ", "2026-07-26", 10.8, "close"),),
    ],
    ids=["duplicate-natural-key", "wrong-target-partition"],
)
def test_validation_failure_preserves_old_partition(
    store: SQLiteRefreshStore,
    db_path: Path,
    rows: tuple[tuple[object, ...], ...],
) -> None:
    before = _rows(db_path, "SELECT * FROM quotes ORDER BY trade_date, ts_code")

    with pytest.raises(RefreshValidationError):
        store.replace_date_snapshot(_quotes_request(rows))

    assert _rows(db_path, "SELECT * FROM quotes ORDER BY trade_date, ts_code") == before


def test_required_fields_reject_null_or_blank_values_before_delete(
    store: SQLiteRefreshStore,
    db_path: Path,
) -> None:
    before = _rows(db_path, "SELECT * FROM quotes ORDER BY trade_date, ts_code")
    request = _quotes_request(
        (
            ("000001.SZ", "2026-07-27", 10.8, ""),
            ("600000.SH", "2026-07-27", None, "close"),
        )
    )

    with pytest.raises(RefreshValidationError, match="required field"):
        store.replace_date_snapshot(request)

    assert _rows(db_path, "SELECT * FROM quotes ORDER BY trade_date, ts_code") == before


def test_minimum_coverage_failure_preserves_old_partition(
    store: SQLiteRefreshStore,
    db_path: Path,
) -> None:
    before = _rows(db_path, "SELECT * FROM quotes ORDER BY trade_date, ts_code")
    request = _quotes_request(
        (("000001.SZ", "2026-07-27", 10.8, "close"),),
        minimum_coverage=0.8,
    )

    with pytest.raises(RefreshValidationError, match="coverage"):
        store.replace_date_snapshot(request)

    assert _rows(db_path, "SELECT * FROM quotes ORDER BY trade_date, ts_code") == before


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("table", 'quotes"; DROP TABLE quotes; --'),
        ("columns", ("ts_code", "trade_date", "close", "source; DELETE")),
        ("date_column", "trade_date OR 1=1"),
        ("natural_keys", ("ts_code", "trade_date DESC")),
        ("required_fields", ("source)",)),
    ],
)
def test_sql_identifier_rejection_never_executes_adapter_text(
    store: SQLiteRefreshStore,
    db_path: Path,
    field: str,
    value: object,
) -> None:
    values: dict[str, object] = {
        "table": "quotes",
        "columns": ("ts_code", "trade_date", "close", "source"),
        "rows": (("000001.SZ", "2026-07-27", 10.8, "close"),),
        "date_column": "trade_date",
        "date_value": "2026-07-27",
        "natural_keys": ("ts_code", "trade_date"),
        "required_fields": ("source",),
    }
    values[field] = value

    with pytest.raises(RefreshValidationError, match="identifier"):
        store.replace_date_snapshot(DateSnapshotReplacement(**values))

    assert _rows(db_path, "SELECT count(*) FROM quotes") == [(3,)]


def test_keyed_upsert_updates_matches_and_keeps_unmentioned_rows(
    store: SQLiteRefreshStore,
    db_path: Path,
) -> None:
    request = KeyedUpsertReplacement(
        table="events",
        columns=("event_date", "stock_code", "detail"),
        rows=(
            ("2026-07-27", "000001", "close"),
            ("2026-07-27", "600000", "new"),
        ),
        natural_keys=("event_date", "stock_code"),
        required_fields=("event_date", "stock_code", "detail"),
    )

    result = store.upsert_keyed_snapshot(request)

    assert result.replaced == 2
    assert _rows(db_path, "SELECT * FROM events ORDER BY event_date, stock_code") == [
        ("2026-07-24", "000001", "historical"),
        ("2026-07-27", "000001", "close"),
        ("2026-07-27", "600000", "new"),
    ]


def test_run_snapshot_atomically_replaces_the_current_snapshot(
    store: SQLiteRefreshStore,
    db_path: Path,
) -> None:
    request = RunSnapshotReplacement(
        table="current_snapshot",
        columns=("code", "value"),
        rows=(("NEW1", 10.0), ("NEW2", 20.0)),
        natural_keys=("code",),
        required_fields=("code", "value"),
    )

    result = store.replace_run_snapshot(request)

    assert result.replaced == 2
    assert _rows(db_path, "SELECT * FROM current_snapshot ORDER BY code") == [
        ("NEW1", 10.0),
        ("NEW2", 20.0),
    ]


def test_formal_table_constraint_failure_rolls_back_date_replacement(
    store: SQLiteRefreshStore,
    db_path: Path,
) -> None:
    before = _rows(db_path, "SELECT * FROM quotes ORDER BY trade_date, ts_code")
    request = _quotes_request(
        (
            ("000001.SZ", "2026-07-27", 10.8, "close"),
            ("600000.SH", "2026-07-27", -1.0, "close"),
        )
    )

    with pytest.raises(sqlite3.IntegrityError):
        store.replace_date_snapshot(request)

    assert _rows(db_path, "SELECT * FROM quotes ORDER BY trade_date, ts_code") == before


def test_fresh_connection_enforces_foreign_keys_and_rolls_back(
    store: SQLiteRefreshStore,
    db_path: Path,
) -> None:
    request = RunSnapshotReplacement(
        table="child_snapshot",
        columns=("code", "parent_code"),
        rows=(("NEW", "MISSING"),),
        natural_keys=("code",),
        required_fields=("code", "parent_code"),
    )

    with pytest.raises(sqlite3.IntegrityError):
        store.replace_run_snapshot(request)

    assert _rows(db_path, "SELECT * FROM child_snapshot") == [("OLD", "VALID")]


def test_composite_failure_rolls_back_every_table(
    store: SQLiteRefreshStore,
    db_path: Path,
) -> None:
    request = CompositeReplacement(
        replacements=(
            DateSnapshotReplacement(
                table="sector_daily",
                columns=("sector_code", "trade_date", "close"),
                rows=(("BK001", "2026-07-27", 11.0),),
                date_column="trade_date",
                date_value="2026-07-27",
                natural_keys=("sector_code", "trade_date"),
                required_fields=("sector_code", "trade_date", "close"),
            ),
            DateSnapshotReplacement(
                table="sector_valuation",
                columns=("sector_code", "trade_date", "pe"),
                rows=(("BK001", "2026-07-27", -1.0),),
                date_column="trade_date",
                date_value="2026-07-27",
                natural_keys=("sector_code", "trade_date"),
                required_fields=("sector_code", "trade_date", "pe"),
            ),
        )
    )

    with pytest.raises(sqlite3.IntegrityError):
        store.replace_composite(request)

    assert _rows(db_path, "SELECT * FROM sector_daily") == [
        ("BK001", "2026-07-27", 10.0)
    ]
    assert _rows(db_path, "SELECT * FROM sector_valuation") == [
        ("BK001", "2026-07-27", 15.0)
    ]
