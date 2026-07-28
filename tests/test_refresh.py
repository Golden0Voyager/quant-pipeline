"""Behavior tests for the close-refresh orchestrator."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from core.refresh import RefreshAdapterResult, RefreshContext, RefreshOrchestrator
from core.task_registry import (
    Cadence,
    DateStrategy,
    EmptyPolicy,
    RefreshKind,
    RefreshPolicy,
    TaskSpec,
)
from core.task_result import TaskStatus


def _spec(
    name: str,
    *,
    tables: tuple[str, ...] | None = None,
    dependencies: tuple[str, ...] = (),
    supports_symbols: bool = False,
) -> TaskSpec:
    task_tables = tables or (f"{name}_table",)
    return TaskSpec(
        name=name,
        callable=None,
        tables=task_tables,
        cadence=Cadence.TRADING_DAY,
        date_columns=dict.fromkeys(task_tables, "trade_date"),
        empty_policy=EmptyPolicy.ALLOW,
        primary_source="test",
        refresh_policy=RefreshPolicy(
            RefreshKind.REMOTE_DATE_SNAPSHOT,
            DateStrategy.EXACT_TARGET,
            dict.fromkeys(task_tables, ("code", "trade_date")),
            dict.fromkeys(task_tables, ("code", "trade_date")),
            dependencies=dependencies,
            supports_symbols=supports_symbols,
        ),
    )


def _adapter_result(
    task_name: str,
    *,
    changed_symbols: tuple[str, ...] = (),
    failed_symbols: tuple[str, ...] = (),
    metadata: Mapping[str, Any] | None = None,
) -> RefreshAdapterResult:
    return RefreshAdapterResult(
        task_name=task_name,
        as_of_date="2026-07-27",
        fetched=1,
        validated=1,
        replaced=1,
        retained=len(failed_symbols),
        failed_symbols=failed_symbols,
        changed_symbols=changed_symbols,
        metadata=metadata or {},
    )


@dataclass
class RecordingStore:
    calls: list[tuple[str, dict[str, Any]]] = field(default_factory=list)

    def start_run(self, **kwargs: Any) -> None:
        self.calls.append(("start", kwargs))

    def record_task_result(self, **kwargs: Any) -> None:
        self.calls.append(("task", kwargs))

    def finish_run(self, **kwargs: Any) -> None:
        self.calls.append(("finish", kwargs))


@dataclass
class RecordingAdapter:
    task_name: str
    calls: list[RefreshContext]
    result: RefreshAdapterResult | None = None
    failures_before_success: int = 0

    def refresh(self, context: RefreshContext) -> RefreshAdapterResult:
        self.calls.append(context)
        if len(self.calls) <= self.failures_before_success:
            raise ConnectionError(f"{self.task_name} unavailable")
        return self.result or _adapter_result(self.task_name)


def _context(
    *,
    hour_utc: int = 8,
    minute: int = 0,
    symbols: tuple[str, ...] | None = None,
    allow_pre_close: bool = False,
) -> RefreshContext:
    return RefreshContext(
        target_date="2026-07-27",
        started_at=datetime(2026, 7, 27, hour_utc, minute, tzinfo=UTC),
        run_id="refresh-1",
        symbols=symbols,
        allow_pre_close=allow_pre_close,
    )


def test_pre_close_gate_uses_shanghai_time_and_does_not_start_run() -> None:
    store = RecordingStore()
    calls: list[RefreshContext] = []
    orchestrator = RefreshOrchestrator(
        specs=(_spec("bars"),),
        adapters={"bars": RecordingAdapter("bars", calls)},
        store=store,
    )

    result = orchestrator.run(_context(hour_utc=7, minute=59))

    assert result.status is TaskStatus.FAILED
    assert result.exit_failure is True
    assert "16:00" in (result.error or "")
    assert calls == []
    assert store.calls == []


def test_force_equivalent_context_allows_pre_close_and_preserves_target_date() -> None:
    store = RecordingStore()
    calls: list[RefreshContext] = []
    orchestrator = RefreshOrchestrator(
        specs=(_spec("bars"),),
        adapters={"bars": RecordingAdapter("bars", calls)},
        store=store,
        clock=lambda: datetime(2026, 7, 27, 8, 30, tzinfo=UTC),
    )

    result = orchestrator.run(_context(hour_utc=7, minute=59, allow_pre_close=True))

    assert result.status is TaskStatus.SUCCESS
    assert calls[0].target_date == "2026-07-27"
    assert store.calls[0] == (
        "start",
        {
            "run_id": "refresh-1",
            "target_date": "2026-07-27",
            "started_at": "2026-07-27T07:59:00+00:00",
            "symbols": None,
        },
    )


def test_dependencies_are_topological_and_shared_table_writers_are_serial() -> None:
    order: list[str] = []

    @dataclass
    class OrderedAdapter:
        name: str

        def refresh(self, context: RefreshContext) -> RefreshAdapterResult:
            order.append(self.name)
            return _adapter_result(self.name)

    base = _spec("base", tables=("shared",))
    overlay = _spec(
        "overlay",
        tables=("shared",),
        dependencies=("base",),
    )
    derived = _spec("derived", dependencies=("overlay",))
    orchestrator = RefreshOrchestrator(
        specs=(derived, overlay, base),
        adapters={
            "base": OrderedAdapter("base"),
            "overlay": OrderedAdapter("overlay"),
            "derived": OrderedAdapter("derived"),
        },
        store=RecordingStore(),
    )

    result = orchestrator.run(_context())

    assert result.status is TaskStatus.SUCCESS
    assert order == ["base", "overlay", "derived"]


def test_failed_dependency_is_blocked_but_independent_task_continues() -> None:
    store = RecordingStore()
    base_calls: list[RefreshContext] = []
    dependent_calls: list[RefreshContext] = []
    independent_calls: list[RefreshContext] = []
    orchestrator = RefreshOrchestrator(
        specs=(
            _spec("dependent", dependencies=("base",)),
            _spec("independent"),
            _spec("base"),
        ),
        adapters={
            "base": RecordingAdapter(
                "base",
                base_calls,
                failures_before_success=2,
            ),
            "dependent": RecordingAdapter("dependent", dependent_calls),
            "independent": RecordingAdapter("independent", independent_calls),
        },
        store=store,
    )

    result = orchestrator.run(_context())

    assert result.status is TaskStatus.DEGRADED
    assert result.exit_failure is True
    assert len(base_calls) == 2
    assert dependent_calls == []
    assert len(independent_calls) == 1
    assert result.metadata["task_statuses"] == {
        "base": "failed",
        "dependent": "failed",
        "independent": "success",
    }
    task_records = {
        kwargs["task_name"]: kwargs
        for call, kwargs in store.calls
        if call == "task"
    }
    assert task_records["dependent"]["metadata"]["blocked_by"] == ("base",)
    assert store.calls[-1][1]["status"] == "degraded"


def test_symbol_scope_is_forwarded_only_to_supported_tasks() -> None:
    supported_calls: list[RefreshContext] = []
    full_market_calls: list[RefreshContext] = []
    store = RecordingStore()
    symbols = ("000001.SZ", "600000.SH")
    orchestrator = RefreshOrchestrator(
        specs=(
            _spec("supported", supports_symbols=True),
            _spec("full_market"),
        ),
        adapters={
            "supported": RecordingAdapter("supported", supported_calls),
            "full_market": RecordingAdapter("full_market", full_market_calls),
        },
        store=store,
    )

    result = orchestrator.run(_context(symbols=symbols))

    assert result.status is TaskStatus.SUCCESS
    assert supported_calls[0].symbols == symbols
    assert full_market_calls[0].symbols is None
    records = [
        kwargs for call, kwargs in store.calls if call == "task"
    ]
    assert records[1]["metadata"]["symbols_ignored"] is True


def test_changed_symbols_flow_to_supported_dependent_task() -> None:
    base_calls: list[RefreshContext] = []
    derived_calls: list[RefreshContext] = []
    orchestrator = RefreshOrchestrator(
        specs=(
            _spec("base", supports_symbols=True),
            _spec(
                "derived",
                dependencies=("base",),
                supports_symbols=True,
            ),
        ),
        adapters={
            "base": RecordingAdapter(
                "base",
                base_calls,
                result=_adapter_result(
                    "base",
                    changed_symbols=("000001.SZ", "600000.SH"),
                ),
            ),
            "derived": RecordingAdapter("derived", derived_calls),
        },
        store=RecordingStore(),
    )

    result = orchestrator.run(_context(symbols=("000001.SZ", "300001.SZ")))

    assert result.status is TaskStatus.SUCCESS
    assert derived_calls[0].symbols == ("000001.SZ",)


def test_adapter_failure_is_retried_once_then_success_is_audited() -> None:
    calls: list[RefreshContext] = []
    store = RecordingStore()
    orchestrator = RefreshOrchestrator(
        specs=(_spec("bars"),),
        adapters={
            "bars": RecordingAdapter(
                "bars",
                calls,
                failures_before_success=1,
            )
        },
        store=store,
    )

    result = orchestrator.run(_context())

    assert result.status is TaskStatus.SUCCESS
    assert len(calls) == 2
    record = next(kwargs for call, kwargs in store.calls if call == "task")
    assert record["metadata"]["attempts"] == 2


def test_failed_symbols_and_dead_source_make_run_degraded_and_retain_old_data() -> None:
    store = RecordingStore()
    partial_calls: list[RefreshContext] = []
    dead_calls: list[RefreshContext] = []
    orchestrator = RefreshOrchestrator(
        specs=(_spec("partial"), _spec("dead")),
        adapters={
            "partial": RecordingAdapter(
                "partial",
                partial_calls,
                result=_adapter_result(
                    "partial",
                    failed_symbols=("600000.SH",),
                ),
            ),
            "dead": RecordingAdapter(
                "dead",
                dead_calls,
                result=RefreshAdapterResult(
                    task_name="dead",
                    as_of_date=None,
                    fetched=0,
                    validated=0,
                    replaced=0,
                    retained=12,
                    failed_symbols=(),
                    changed_symbols=(),
                    metadata={
                        "source_status": "dead_source",
                        "reason": "endpoint retired",
                    },
                ),
            ),
        },
        store=store,
    )

    result = orchestrator.run(_context())

    assert result.status is TaskStatus.DEGRADED
    assert result.exit_failure is True
    assert result.saved == 1
    assert result.metadata["task_statuses"] == {
        "partial": "degraded",
        "dead": "degraded",
    }
    records = {
        kwargs["task_name"]: kwargs
        for call, kwargs in store.calls
        if call == "task"
    }
    assert records["partial"]["retained"] == 1
    assert records["dead"]["retained"] == 12
    assert records["dead"]["failed"] == 1
