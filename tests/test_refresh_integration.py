"""End-to-end acceptance tests for the close-refresh feature.

Drives hand-rolled fake adapters through the REAL ``RefreshOrchestrator``
and the REAL ``SQLiteRefreshStore`` against a temporary SQLite database
preloaded with intraday target-date rows and historical rows. No network,
no production database, no store mocks.
"""

from __future__ import annotations

import importlib.util
import json
import sqlite3
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from core.refresh import (
    RefreshAdapter,
    RefreshAdapterResult,
    RefreshContext,
    RefreshOrchestrator,
)
from core.refresh_store import (
    CompositeReplacement,
    DateSnapshotReplacement,
    KeyedUpsertReplacement,
    SQLiteRefreshStore,
)
from core.task_registry import (
    DateStrategy,
    TaskSpec,
    refreshable_trading_tasks,
)
from core.task_result import TaskResult, TaskStatus

_TARGET = "2026-07-27"
_HISTORY = "2026-07-24"

_BAR_COLUMNS = (
    "ts_code",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "source",
)
_SECTOR_DAILY_COLUMNS = ("sector_name", "trade_date", "close")
_SECTOR_VALUATION_COLUMNS = ("sector_name", "trade_date", "pe")
_BASIS_COLUMNS = ("trade_date", "futures_code", "basis")

# The authoritative close snapshot published by the fake bars adapter.
# 300750.SZ existed intraday but is absent from the close feed: keyed
# upsert intentionally retains rows for symbols it did not fetch, so the
# intraday row survives (ghost-row cleanup is a separate concern, not
# bars' responsibility).
_CLOSE_ROWS: tuple[tuple[object, ...], ...] = (
    ("000001.SZ", _TARGET, 10.0, 10.6, 9.9, 10.5, 1_000.0, 10_500.0, "close"),
    ("600000.SH", _TARGET, 20.0, 20.4, 19.8, 20.2, 2_000.0, 40_400.0, "close"),
)


def _apply_refresh_audit_migration(conn: sqlite3.Connection) -> None:
    """Create refresh_runs / refresh_task_runs from the real migration 010."""
    path = Path(__file__).resolve().parent.parent / "migrations" / "010_refresh_runs.py"
    spec = importlib.util.spec_from_file_location("migration_010_refresh_runs", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.apply(conn)


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    """Temp DB preloaded with intraday target-date rows and historical rows."""
    path = tmp_path / "refresh_e2e.db"
    with sqlite3.connect(path) as conn:
        conn.executescript(
            f"""
            PRAGMA foreign_keys = ON;

            CREATE TABLE daily_bars (
                ts_code TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL NOT NULL,
                amount REAL NOT NULL,
                source TEXT NOT NULL,
                PRIMARY KEY (ts_code, trade_date)
            );
            INSERT INTO daily_bars VALUES
                ('000001.SZ', '{_HISTORY}', 9.0, 9.5, 8.9, 9.4, 900.0, 8460.0, 'close'),
                ('600000.SH', '{_HISTORY}', 19.0, 19.5, 18.9, 19.4, 1900.0, 36860.0, 'close'),
                ('300750.SZ', '{_HISTORY}', 180.0, 185.0, 179.0, 184.0, 300.0, 55200.0, 'close'),
                ('000001.SZ', '{_TARGET}', 10.0, 10.3, 9.9, 10.1, 500.0, 5050.0, 'intraday'),
                ('600000.SH', '{_TARGET}', 20.0, 20.1, 19.8, 19.9, 800.0, 15920.0, 'intraday'),
                ('300750.SZ', '{_TARGET}', 184.0, 186.0, 183.0, 185.0, 100.0, 18500.0, 'intraday');

            CREATE TABLE sector_daily (
                sector_name TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                close REAL NOT NULL CHECK (close >= 0),
                PRIMARY KEY (sector_name, trade_date)
            );
            INSERT INTO sector_daily VALUES
                ('银行', '{_HISTORY}', 100.0),
                ('银行', '{_TARGET}', 101.0);

            CREATE TABLE sector_valuation (
                sector_name TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                pe REAL NOT NULL CHECK (pe >= 0),
                PRIMARY KEY (sector_name, trade_date)
            );
            INSERT INTO sector_valuation VALUES
                ('银行', '{_HISTORY}', 5.1),
                ('银行', '{_TARGET}', 5.2);

            CREATE TABLE index_futures_basis (
                trade_date TEXT NOT NULL,
                futures_code TEXT NOT NULL,
                basis REAL NOT NULL CHECK (basis >= -100),
                PRIMARY KEY (trade_date, futures_code)
            );
            INSERT INTO index_futures_basis VALUES
                ('{_HISTORY}', 'IF2608', -1.5),
                ('{_TARGET}', 'IF2608', -1.2);
            """
        )
        _apply_refresh_audit_migration(conn)
    return path


@pytest.fixture
def store(db_path: Path) -> SQLiteRefreshStore:
    return SQLiteRefreshStore(db_path)


def _literal_dump(
    db_path: Path,
    table: str,
    columns: tuple[str, ...],
    *,
    exclude_date: str | None = None,
) -> tuple[tuple[str, ...], ...]:
    """Dump rows as SQLite literals so comparisons are byte-for-byte."""
    quoted = ", ".join(f"quote({column})" for column in columns)
    order = ", ".join(columns)
    query = f"SELECT {quoted} FROM {table}"
    params: tuple[object, ...] = ()
    if exclude_date is not None:
        query += " WHERE trade_date <> ?"
        params = (exclude_date,)
    query += f" ORDER BY {order}"
    with sqlite3.connect(db_path) as conn:
        return tuple(tuple(row) for row in conn.execute(query, params))


def _target_bar_rows(db_path: Path) -> tuple[tuple[object, ...], ...]:
    with sqlite3.connect(db_path) as conn:
        return tuple(
            tuple(row)
            for row in conn.execute(
                "SELECT ts_code, trade_date, open, high, low, close, volume,"
                " amount, source FROM daily_bars WHERE trade_date = ?"
                " ORDER BY ts_code",
                (_TARGET,),
            )
        )


def _run_rows(db_path: Path) -> tuple[tuple[object, ...], ...]:
    with sqlite3.connect(db_path) as conn:
        return tuple(
            tuple(row)
            for row in conn.execute(
                "SELECT run_id, target_date, status, finished_at FROM refresh_runs"
            )
        )


def _task_audit_rows(db_path: Path) -> dict[str, dict[str, Any]]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        return {
            row["task_name"]: dict(row)
            for row in conn.execute("SELECT * FROM refresh_task_runs")
        }


# ── fake adapters (real store, no mocks) ───────────────────────────────


@dataclass
class GenericAdapter:
    """Registry-policy-conformant fake for tasks not under test."""

    spec: TaskSpec
    failed_symbols: tuple[str, ...] = ()
    changed_symbols: tuple[str, ...] = ()
    contexts: list[RefreshContext] = field(default_factory=list)

    def refresh(self, context: RefreshContext) -> RefreshAdapterResult:
        self.contexts.append(context)
        policy = self.spec.refresh_policy
        assert policy is not None
        metadata: dict[str, Any] = {}
        as_of_date: str | None = context.target_date
        if policy.date_strategy is DateStrategy.RUN_SNAPSHOT:
            metadata["run_id"] = context.run_id
            as_of_date = None
        fetched = 2
        validated = fetched - len(self.failed_symbols)
        return RefreshAdapterResult(
            task_name=self.spec.name,
            as_of_date=as_of_date,
            fetched=fetched,
            validated=validated,
            replaced=validated,
            retained=len(self.failed_symbols),
            failed_symbols=self.failed_symbols,
            changed_symbols=self.changed_symbols,
            metadata=metadata,
        )


@dataclass
class PublishingBarsAdapter:
    """Publishes the close rows through the real atomic keyed upsert."""

    store: SQLiteRefreshStore
    rows: tuple[tuple[object, ...], ...] = _CLOSE_ROWS
    contexts: list[RefreshContext] = field(default_factory=list)

    def refresh(self, context: RefreshContext) -> RefreshAdapterResult:
        self.contexts.append(context)
        # Mirrors the real BarsRefreshAdapter: per-symbol upsert keyed on
        # (ts_code, trade_date); never deletes rows it did not fetch.
        self.store.upsert_keyed_snapshot(
            KeyedUpsertReplacement(
                table="daily_bars",
                columns=_BAR_COLUMNS,
                rows=self.rows,
                natural_keys=("ts_code", "trade_date"),
                required_fields=("open", "high", "low", "close", "volume", "amount"),
            )
        )
        return RefreshAdapterResult(
            task_name="update_bars",
            as_of_date=context.target_date,
            fetched=len(self.rows),
            validated=len(self.rows),
            replaced=len(self.rows),
            retained=0,
            failed_symbols=(),
            changed_symbols=tuple(str(row[0]) for row in self.rows),
            metadata={},
        )


@dataclass
class FailingAdapter:
    """Simulates a dead upstream source for every attempt."""

    task_name: str
    calls: int = 0

    def refresh(self, context: RefreshContext) -> RefreshAdapterResult:
        self.calls += 1
        raise ConnectionError(f"{self.task_name} source unavailable")


@dataclass
class CompositeSectorAdapter:
    """Publishes the three sector tables in one all-or-nothing transaction."""

    store: SQLiteRefreshStore
    basis_value: float = -1.0
    contexts: list[RefreshContext] = field(default_factory=list)

    def refresh(self, context: RefreshContext) -> RefreshAdapterResult:
        self.contexts.append(context)
        target = context.target_date
        self.store.replace_composite(
            CompositeReplacement(
                replacements=(
                    DateSnapshotReplacement(
                        table="sector_daily",
                        columns=_SECTOR_DAILY_COLUMNS,
                        rows=(("银行", target, 102.5),),
                        date_column="trade_date",
                        date_value=target,
                        natural_keys=("sector_name", "trade_date"),
                        required_fields=("close",),
                    ),
                    DateSnapshotReplacement(
                        table="sector_valuation",
                        columns=_SECTOR_VALUATION_COLUMNS,
                        rows=(("银行", target, 5.3),),
                        date_column="trade_date",
                        date_value=target,
                        natural_keys=("sector_name", "trade_date"),
                    ),
                    DateSnapshotReplacement(
                        table="index_futures_basis",
                        columns=_BASIS_COLUMNS,
                        rows=((target, "IF2608", self.basis_value),),
                        date_column="trade_date",
                        date_value=target,
                        natural_keys=("trade_date", "futures_code"),
                        required_fields=("basis",),
                    ),
                )
            )
        )
        return RefreshAdapterResult(
            task_name="update_sector_derivatives",
            as_of_date=target,
            fetched=3,
            validated=3,
            replaced=3,
            retained=0,
            failed_symbols=(),
            changed_symbols=(),
            metadata={},
        )


# ── run plumbing ────────────────────────────────────────────────────────


def _context(
    *,
    run_id: str = "refresh-e2e-1",
    symbols: tuple[str, ...] | None = None,
) -> RefreshContext:
    # 08:05 UTC == 16:05 Asia/Shanghai: past the close gate.
    return RefreshContext(
        target_date=_TARGET,
        started_at=datetime(2026, 7, 27, 8, 5, tzinfo=UTC),
        run_id=run_id,
        symbols=symbols,
    )


def _build_adapters(
    specs: tuple[TaskSpec, ...],
    overrides: dict[str, RefreshAdapter] | None = None,
) -> dict[str, RefreshAdapter]:
    adapters: dict[str, RefreshAdapter] = {
        spec.name: GenericAdapter(spec) for spec in specs
    }
    if overrides:
        adapters.update(overrides)
    return adapters


def _run(
    store: SQLiteRefreshStore,
    adapters: dict[str, RefreshAdapter],
    *,
    specs: tuple[TaskSpec, ...],
    symbols: tuple[str, ...] | None = None,
) -> TaskResult:
    orchestrator = RefreshOrchestrator(
        specs=specs,
        adapters=adapters,
        store=store,
        clock=lambda: datetime(2026, 7, 27, 9, 0, tzinfo=UTC),
    )
    return orchestrator.run(_context(symbols=symbols))


# ── acceptance: close rows upsert fetched symbols only ────────────────────


def test_close_rows_upsert_fetched_symbols_and_retain_absent(
    db_path: Path, store: SQLiteRefreshStore
) -> None:
    specs = refreshable_trading_tasks()
    bars = PublishingBarsAdapter(store)
    adapters = _build_adapters(specs, {"update_bars": bars})

    result = _run(store, adapters, specs=specs)

    assert result.status is TaskStatus.SUCCESS
    assert result.exit_failure is False
    # Fetched symbols are overwritten from intraday to close values; the
    # intraday-only 300750.SZ row is retained because keyed upsert never
    # deletes rows for symbols absent from the close feed.
    assert _target_bar_rows(db_path) == (
        _CLOSE_ROWS[0],
        ("300750.SZ", _TARGET, 184.0, 186.0, 183.0, 185.0, 100.0, 18500.0, "intraday"),
        _CLOSE_ROWS[1],
    )
    sources = {row[-1] for row in _target_bar_rows(db_path)}
    assert sources == {"close", "intraday"}


def test_historical_rows_are_byte_for_byte_unchanged(
    db_path: Path, store: SQLiteRefreshStore
) -> None:
    before = {
        "daily_bars": _literal_dump(db_path, "daily_bars", _BAR_COLUMNS, exclude_date=_TARGET),
        "sector_daily": _literal_dump(db_path, "sector_daily", _SECTOR_DAILY_COLUMNS, exclude_date=_TARGET),
        "sector_valuation": _literal_dump(db_path, "sector_valuation", _SECTOR_VALUATION_COLUMNS, exclude_date=_TARGET),
        "index_futures_basis": _literal_dump(db_path, "index_futures_basis", _BASIS_COLUMNS, exclude_date=_TARGET),
    }
    specs = refreshable_trading_tasks()
    adapters = _build_adapters(
        specs,
        {
            "update_bars": PublishingBarsAdapter(store),
            "update_sector_derivatives": CompositeSectorAdapter(store),
        },
    )

    result = _run(store, adapters, specs=specs)

    assert result.status is TaskStatus.SUCCESS
    after = {
        "daily_bars": _literal_dump(db_path, "daily_bars", _BAR_COLUMNS, exclude_date=_TARGET),
        "sector_daily": _literal_dump(db_path, "sector_daily", _SECTOR_DAILY_COLUMNS, exclude_date=_TARGET),
        "sector_valuation": _literal_dump(db_path, "sector_valuation", _SECTOR_VALUATION_COLUMNS, exclude_date=_TARGET),
        "index_futures_basis": _literal_dump(db_path, "index_futures_basis", _BASIS_COLUMNS, exclude_date=_TARGET),
    }
    assert after == before


# ── acceptance: failures preserve old data ─────────────────────────────


def test_source_failure_retains_old_rows_and_blocks_dependents(
    db_path: Path, store: SQLiteRefreshStore
) -> None:
    before = _literal_dump(db_path, "daily_bars", _BAR_COLUMNS)
    specs = refreshable_trading_tasks()
    bars = FailingAdapter("update_bars")
    derived = GenericAdapter(next(s for s in specs if s.name == "update_indicators"))
    adapters = _build_adapters(
        specs, {"update_bars": bars, "update_indicators": derived}
    )

    result = _run(store, adapters, specs=specs)

    # One retry then failure; every daily_bars row (intraday included) intact.
    assert bars.calls == 2
    assert _literal_dump(db_path, "daily_bars", _BAR_COLUMNS) == before
    assert result.status is TaskStatus.DEGRADED
    assert result.exit_failure is True
    assert "update_bars" in result.metadata["failed_tasks"]

    audits = _task_audit_rows(db_path)
    bars_audit = audits["update_bars"]
    assert bars_audit["status"] == "failed"
    assert json.loads(bars_audit["metadata_json"])["retained_old_data"] is True
    # Dependents never run against a failed upstream: blocked, adapter unused.
    assert derived.contexts == []
    indicators_audit = audits["update_indicators"]
    assert indicators_audit["status"] == "failed"
    assert "update_bars" in json.loads(indicators_audit["metadata_json"])["blocked_by"]


def test_store_validation_failure_retains_old_rows(
    db_path: Path, store: SQLiteRefreshStore
) -> None:
    before = _literal_dump(db_path, "daily_bars", _BAR_COLUMNS)
    specs = refreshable_trading_tasks()
    # Rows with an empty required field: the real store must refuse to
    # publish them (keyed upsert validates required_fields before staging).
    poisoned = tuple(
        (*row[:5], None, *row[6:]) for row in _CLOSE_ROWS
    )
    bars = PublishingBarsAdapter(store, rows=poisoned)
    adapters = _build_adapters(specs, {"update_bars": bars})

    result = _run(store, adapters, specs=specs)

    assert _literal_dump(db_path, "daily_bars", _BAR_COLUMNS) == before
    assert result.status is TaskStatus.DEGRADED
    assert result.exit_failure is True
    audits = _task_audit_rows(db_path)
    assert audits["update_bars"]["status"] == "failed"
    assert json.loads(audits["update_bars"]["metadata_json"])["retained_old_data"] is True


# ── acceptance: derived tasks receive only changed symbols ─────────────


def test_derived_tasks_receive_only_changed_symbols(
    db_path: Path, store: SQLiteRefreshStore
) -> None:
    specs = refreshable_trading_tasks()
    bars = PublishingBarsAdapter(store)
    adapters = _build_adapters(specs, {"update_bars": bars})

    result = _run(store, adapters, specs=specs)

    assert result.status is TaskStatus.SUCCESS
    changed = tuple(str(row[0]) for row in _CLOSE_ROWS)
    for derived_name in (
        "update_indicators",
        "update_chip_distribution",
        "update_chip_distribution_em",
    ):
        adapter = adapters[derived_name]
        assert isinstance(adapter, GenericAdapter)
        assert [context.symbols for context in adapter.contexts] == [changed]
    # A dependent that does not support per-symbol scope stays full-market.
    snapshot = adapters["update_market_snapshot"]
    assert isinstance(snapshot, GenericAdapter)
    assert [context.symbols for context in snapshot.contexts] == [None]


# ── acceptance: composite tasks roll back together ─────────────────────


def test_composite_success_replaces_all_three_target_partitions(
    db_path: Path, store: SQLiteRefreshStore
) -> None:
    specs = refreshable_trading_tasks()
    adapters = _build_adapters(
        specs, {"update_sector_derivatives": CompositeSectorAdapter(store)}
    )

    result = _run(store, adapters, specs=specs)

    assert result.status is TaskStatus.SUCCESS
    with sqlite3.connect(db_path) as conn:
        close = conn.execute(
            "SELECT close FROM sector_daily WHERE trade_date = ?", (_TARGET,)
        ).fetchone()
        pe = conn.execute(
            "SELECT pe FROM sector_valuation WHERE trade_date = ?", (_TARGET,)
        ).fetchone()
        basis = conn.execute(
            "SELECT basis FROM index_futures_basis WHERE trade_date = ?", (_TARGET,)
        ).fetchone()
    assert (close, pe, basis) == ((102.5,), (5.3,), (-1.0,))


def test_composite_component_failure_rolls_back_every_table(
    db_path: Path, store: SQLiteRefreshStore
) -> None:
    before = {
        "sector_daily": _literal_dump(db_path, "sector_daily", _SECTOR_DAILY_COLUMNS),
        "sector_valuation": _literal_dump(db_path, "sector_valuation", _SECTOR_VALUATION_COLUMNS),
        "index_futures_basis": _literal_dump(db_path, "index_futures_basis", _BASIS_COLUMNS),
    }
    specs = refreshable_trading_tasks()
    # basis=-999 violates the CHECK constraint only at publish time, after
    # sector_daily and sector_valuation were already written inside the
    # same transaction: the rollback must undo all three tables.
    composite = CompositeSectorAdapter(store, basis_value=-999.0)
    adapters = _build_adapters(specs, {"update_sector_derivatives": composite})

    result = _run(store, adapters, specs=specs)

    after = {
        "sector_daily": _literal_dump(db_path, "sector_daily", _SECTOR_DAILY_COLUMNS),
        "sector_valuation": _literal_dump(db_path, "sector_valuation", _SECTOR_VALUATION_COLUMNS),
        "index_futures_basis": _literal_dump(db_path, "index_futures_basis", _BASIS_COLUMNS),
    }
    assert after == before
    assert len(composite.contexts) == 2  # one retry, both rolled back
    assert result.status is TaskStatus.DEGRADED
    assert result.exit_failure is True
    audits = _task_audit_rows(db_path)
    assert audits["update_sector_derivatives"]["status"] == "failed"


# ── acceptance: all 31 tasks persist audited results ───────────────────


def test_all_31_tasks_persist_run_and_task_audit_rows(
    db_path: Path, store: SQLiteRefreshStore
) -> None:
    specs = refreshable_trading_tasks()
    # 31：update_north_flow 已下线（北向日度流向 2024-08 起停止披露），
    # update_placement_announcements 为定增公告台账（2026-09 接入），
    # update_stock_comment / update_hot_rank 为东财情绪快照（2026-09 接入）
    assert len(specs) == 31
    adapters = _build_adapters(
        specs,
        {
            "update_bars": PublishingBarsAdapter(store),
            "update_sector_derivatives": CompositeSectorAdapter(store),
        },
    )

    result = _run(store, adapters, specs=specs)

    assert result.status is TaskStatus.SUCCESS
    audits = _task_audit_rows(db_path)
    assert set(audits) == {spec.name for spec in specs}
    assert len(audits) == 31
    assert {row["status"] for row in audits.values()} == {"success"}
    assert {row["requested_date"] for row in audits.values()} == {_TARGET}
    runs = _run_rows(db_path)
    assert len(runs) == 1
    run_id, target_date, status, finished_at = runs[0]
    assert (run_id, target_date, status) == ("refresh-e2e-1", _TARGET, "success")
    assert finished_at is not None


# ── acceptance: degraded outcome is a nonzero exit ─────────────────────


def test_degraded_run_yields_nonzero_exit_semantics(
    db_path: Path, store: SQLiteRefreshStore
) -> None:
    specs = refreshable_trading_tasks()
    fund_flow_spec = next(s for s in specs if s.name == "update_fund_flow")
    degraded = GenericAdapter(fund_flow_spec, failed_symbols=("000002.SZ",))
    adapters = _build_adapters(specs, {"update_fund_flow": degraded})

    result = _run(store, adapters, specs=specs)

    # daily_pipeline maps exit_failure to sys.exit(1); a degraded aggregate
    # therefore terminates the CLI with a nonzero code.
    assert result.status is TaskStatus.DEGRADED
    assert result.exit_failure is True
    assert result.metadata["degraded_tasks"] == ("update_fund_flow",)
    audits = _task_audit_rows(db_path)
    assert audits["update_fund_flow"]["status"] == "degraded"
    assert audits["update_fund_flow"]["failed"] == 1
    assert _run_rows(db_path)[0][2] == "degraded"
