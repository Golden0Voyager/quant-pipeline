"""Behavior tests for the close-refresh orchestrator."""

from __future__ import annotations

import logging
import sqlite3
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta, timezone
from typing import Any

import pytest

from core.refresh import (
    CrossSourceCheckConfig,
    RefreshAdapterResult,
    RefreshContext,
    RefreshOrchestrator,
)
from core.refresh_audit import (
    CROSS_SOURCE_BOARDS,
    CrossSourceTolerance,
    RefreshAudit,
    RefreshAuditError,
    stratified_cross_source_sample,
)
from core.refresh_store import ResumedTaskOutcome
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
class RecordCrashStore(RecordingStore):
    finish_error: bool = False

    def record_task_result(self, **kwargs: Any) -> None:
        self.calls.append(("task", kwargs))
        raise sqlite3.OperationalError("database is locked")

    def finish_run(self, **kwargs: Any) -> None:
        if self.finish_error:
            raise sqlite3.OperationalError("finish failed")
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


def _outcome(
    task_name: str,
    *,
    status: str,
    replaced: int = 1,
    fetched: int = 1,
    validated: int = 1,
    failed: int = 0,
    metadata: Mapping[str, Any] | None = None,
) -> ResumedTaskOutcome:
    return ResumedTaskOutcome(
        task_name=task_name,
        policy_kind="remote_date_snapshot",
        as_of_date="2026-07-27",
        status=status,
        fetched=fetched,
        validated=validated,
        replaced=replaced,
        retained=0,
        failed=failed,
        metadata=metadata or {},
    )


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


def test_started_at_is_normalized_to_utc_in_the_audit_row() -> None:
    """refresh_runs.started_at 落库必须与 finished_at 同为 UTC。

    ``context.started_at`` 由 daily_pipeline 以**上海 aware 时钟**创建（闸门、
    目标交易日、started_at 共用一个时间基准），若直接 ``isoformat()`` 会写成
    ``+08:00``，使同一行的两个时间戳偏移不一致（历史遗留）。编排器只归一化
    落库字符串为 UTC，``context.started_at`` 本身仍按上海语义供闸门使用。
    """
    store = RecordingStore()
    orchestrator = RefreshOrchestrator(
        specs=(_spec("bars"),),
        adapters={"bars": RecordingAdapter("bars", [])},
        store=store,
    )
    shanghai = timezone(timedelta(hours=8))
    context = RefreshContext(
        target_date="2026-07-27",
        # 16:05 +08:00 == 08:05 UTC：已过 16:00 收盘闸门。
        started_at=datetime(2026, 7, 27, 16, 5, tzinfo=shanghai),
        run_id="refresh-1",
        symbols=None,
    )

    result = orchestrator.run(context)

    assert result.status is TaskStatus.SUCCESS
    # 关键：落库为 UTC（+00:00）而非 +08:00。
    assert store.calls[0][1]["started_at"] == "2026-07-27T08:05:00+00:00"


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


def test_empty_upstream_changed_set_keeps_full_market_scope_for_dependent() -> None:
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
            "base": RecordingAdapter("base", base_calls),
            "derived": RecordingAdapter("derived", derived_calls),
        },
        store=RecordingStore(),
    )

    result = orchestrator.run(_context())

    assert result.status is TaskStatus.SUCCESS
    assert derived_calls[0].symbols is None


def test_nonempty_upstream_changed_set_narrows_full_market_dependent() -> None:
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

    result = orchestrator.run(_context())

    assert result.status is TaskStatus.SUCCESS
    assert derived_calls[0].symbols == ("000001.SZ", "600000.SH")


def test_store_error_mid_run_returns_failed_result_and_finishes_run_failed() -> None:
    store = RecordCrashStore()
    calls: list[RefreshContext] = []
    orchestrator = RefreshOrchestrator(
        specs=(_spec("bars"),),
        adapters={"bars": RecordingAdapter("bars", calls)},
        store=store,
    )

    result = orchestrator.run(_context())

    assert result.status is TaskStatus.FAILED
    assert result.exit_failure is True
    assert result.metadata["error_type"] == "OperationalError"
    finishes = [kwargs for call, kwargs in store.calls if call == "finish"]
    assert [kwargs["status"] for kwargs in finishes] == ["failed"]


def test_finish_run_error_does_not_mask_failed_outcome() -> None:
    store = RecordCrashStore(finish_error=True)
    calls: list[RefreshContext] = []
    orchestrator = RefreshOrchestrator(
        specs=(_spec("bars"),),
        adapters={"bars": RecordingAdapter("bars", calls)},
        store=store,
    )

    result = orchestrator.run(_context())

    assert result.status is TaskStatus.FAILED
    assert result.metadata["error_type"] == "OperationalError"


class InterruptingAdapter:
    """Adapter fake：refresh 中抛 KeyboardInterrupt，模拟人工中断。"""

    def refresh(self, context: RefreshContext) -> RefreshAdapterResult:
        raise KeyboardInterrupt


def test_keyboard_interrupt_finishes_run_aborted_and_propagates() -> None:
    """中断不得把运行行留在 'running'：落 'aborted' 后原样重抛。"""
    store = RecordingStore()
    orchestrator = RefreshOrchestrator(
        specs=(_spec("bars"),),
        adapters={"bars": InterruptingAdapter()},
        store=store,
    )

    with pytest.raises(KeyboardInterrupt):
        orchestrator.run(_context())

    assert store.calls[0][0] == "start"
    finishes = [kwargs for call, kwargs in store.calls if call == "finish"]
    assert [kwargs["status"] for kwargs in finishes] == ["aborted"]


def test_finish_run_error_during_interrupt_does_not_mask_interrupt() -> None:
    """收尾时 store 再报错也不得吞掉或替换原始中断信号。"""
    store = RecordCrashStore(finish_error=True)
    orchestrator = RefreshOrchestrator(
        specs=(_spec("bars"),),
        adapters={"bars": InterruptingAdapter()},
        store=store,
    )

    with pytest.raises(KeyboardInterrupt):
        orchestrator.run(_context())


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


class FailingAudit(RefreshAudit):
    """Audit fake：适配器发布后必抛，模拟发布后审计失败。

    继承 RefreshAudit 而不是靠 duck typing：orchestrator 的形参是具体类而非
    Protocol，不继承就无法通过类型检查。
    """

    def validate_task(self, spec, context, adapter_result):
        raise RefreshAuditError("bars coverage 0.500 is below minimum 0.800")


def test_publish_then_audit_failure_does_not_claim_retained_old_data() -> None:
    """已发布行后审计才失败 → 不得谎称 retained_old_data，如实记部分写入。"""
    store = RecordingStore()
    calls: list[RefreshContext] = []
    orchestrator = RefreshOrchestrator(
        specs=(_spec("bars"),),
        adapters={"bars": RecordingAdapter("bars", calls)},
        store=store,
        audit=FailingAudit(),
    )

    result = orchestrator.run(_context())

    assert result.status is TaskStatus.DEGRADED
    record = _task_record(store)
    assert record["status"] == "failed"
    metadata = record["metadata"]
    assert metadata["retained_old_data"] is False
    assert metadata["partial_write"] is True
    assert metadata["published_rows"] == 1


def test_failure_before_any_write_claims_retained_old_data() -> None:
    """两次尝试均在写入前失败 → retained_old_data=True 仍成立。"""
    store = RecordingStore()
    orchestrator = RefreshOrchestrator(
        specs=(_spec("bars"),),
        adapters={
            "bars": RecordingAdapter("bars", [], failures_before_success=2),
        },
        store=store,
    )

    result = orchestrator.run(_context())

    assert result.status is TaskStatus.DEGRADED
    record = _task_record(store)
    assert record["status"] == "failed"
    assert record["metadata"]["retained_old_data"] is True
    assert "partial_write" not in record["metadata"]


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


_CROSS_SYMBOLS = (
    "600000.SH",
    "000001.SZ",
    "300001.SZ",
    "688001.SH",
    "830799.BJ",
)


def _quotes(
    overrides: Mapping[str, dict[str, float]] | None = None,
) -> dict[str, dict[str, float]]:
    quotes = {
        symbol: {"close": 10.0, "volume": 1000.0} for symbol in _CROSS_SYMBOLS
    }
    quotes.update(overrides or {})
    return quotes


@dataclass
class FakeVerifier:
    primary: dict[str, dict[str, float]]
    reference: dict[str, dict[str, float]]
    source_name: str = "em_backup"
    supported_boards: frozenset[str] = frozenset(CROSS_SOURCE_BOARDS)
    primary_calls: list[tuple[str, ...]] = field(default_factory=list)
    reference_calls: list[tuple[str, ...]] = field(default_factory=list)
    reference_error: Exception | None = None

    def primary_quotes(
        self,
        symbols: Sequence[str],
        target_date: str,
    ) -> Mapping[str, Mapping[str, float]]:
        self.primary_calls.append(tuple(symbols))
        return {
            symbol: self.primary[symbol]
            for symbol in symbols
            if symbol in self.primary
        }

    def reference_quotes(
        self,
        symbols: Sequence[str],
        target_date: str,
    ) -> Mapping[str, Mapping[str, float]]:
        if self.reference_error is not None:
            raise self.reference_error
        self.reference_calls.append(tuple(symbols))
        return {
            symbol: self.reference[symbol]
            for symbol in symbols
            if symbol in self.reference
        }


@dataclass
class CrossCheckedAdapter:
    """Full-market adapter whose targeted retry republishes reference values."""

    task_name: str
    verifier: FakeVerifier
    changed: tuple[str, ...] = _CROSS_SYMBOLS
    fix_on_retry: bool = True
    calls: list[RefreshContext] = field(default_factory=list)

    def refresh(self, context: RefreshContext) -> RefreshAdapterResult:
        self.calls.append(context)
        if context.symbols is None:
            return _adapter_result(self.task_name, changed_symbols=self.changed)
        if self.fix_on_retry:
            for symbol in context.symbols:
                self.verifier.primary[symbol] = dict(self.verifier.reference[symbol])
        return _adapter_result(
            self.task_name,
            changed_symbols=tuple(context.symbols),
        )


def _cross_orchestrator(
    adapter: CrossCheckedAdapter,
    verifier: FakeVerifier,
    store: RecordingStore,
    *,
    report_only: bool = False,
) -> RefreshOrchestrator:
    return RefreshOrchestrator(
        specs=(_spec("bars", supports_symbols=True),),
        adapters={"bars": adapter},
        store=store,
        cross_source=CrossSourceCheckConfig(
            task_name="bars",
            tolerance=CrossSourceTolerance(price=0.01, volume=0.10),
            report_only=report_only,
        ),
        verifier=verifier,
    )


def _task_record(store: RecordingStore) -> dict[str, Any]:
    return next(kwargs for call, kwargs in store.calls if call == "task")


def test_cross_source_sample_is_persisted_in_task_metadata_and_deterministic() -> None:
    samples: list[tuple[str, ...]] = []
    for _ in range(2):
        store = RecordingStore()
        verifier = FakeVerifier(primary=_quotes(), reference=_quotes())
        adapter = CrossCheckedAdapter("bars", verifier)

        result = _cross_orchestrator(adapter, verifier, store).run(_context())

        assert result.status is TaskStatus.SUCCESS
        assert len(adapter.calls) == 1
        summary = _task_record(store)["metadata"]["cross_source"]
        assert summary["source"] == "em_backup"
        assert summary["mismatched"] == ()
        samples.append(summary["sampled"])

    assert samples[0] == samples[1]
    expected = stratified_cross_source_sample(
        _CROSS_SYMBOLS,
        target_date="2026-07-27",
        supported_boards=frozenset(CROSS_SOURCE_BOARDS),
    )
    assert samples[0] == expected.symbols


def test_cross_source_mismatch_retries_only_mismatched_symbols_once() -> None:
    store = RecordingStore()
    verifier = FakeVerifier(
        primary=_quotes(
            {
                "300001.SZ": {"close": 10.5, "volume": 1000.0},
                "688001.SH": {"close": 10.0, "volume": 1500.0},
            }
        ),
        reference=_quotes(),
    )
    adapter = CrossCheckedAdapter("bars", verifier)

    result = _cross_orchestrator(adapter, verifier, store).run(_context())

    assert result.status is TaskStatus.SUCCESS
    assert len(adapter.calls) == 2
    assert adapter.calls[0].symbols is None
    assert adapter.calls[1].symbols == ("300001.SZ", "688001.SH")
    summary = _task_record(store)["metadata"]["cross_source"]
    assert summary["mismatched"] == ("300001.SZ", "688001.SH")
    assert summary["retried_symbols"] == ("300001.SZ", "688001.SH")
    assert summary["still_mismatched"] == ()


def test_cross_source_retry_failure_degrades_without_full_market_rerun() -> None:
    store = RecordingStore()
    verifier = FakeVerifier(
        primary=_quotes({"300001.SZ": {"close": 10.5, "volume": 1000.0}}),
        reference=_quotes(),
    )
    adapter = CrossCheckedAdapter("bars", verifier, fix_on_retry=False)

    result = _cross_orchestrator(adapter, verifier, store).run(_context())

    assert result.status is TaskStatus.DEGRADED
    assert result.metadata["task_statuses"] == {"bars": "degraded"}
    # Exactly one targeted retry; no third attempt and never a full-market
    # rerun after the initial fetch.
    assert len(adapter.calls) == 2
    assert adapter.calls[1].symbols == ("300001.SZ",)
    record = _task_record(store)
    assert record["status"] == "degraded"
    # Accepted old data is retained: the record keeps the original counts.
    assert record["replaced"] == 1
    summary = record["metadata"]["cross_source"]
    assert summary["still_mismatched"] == ("300001.SZ",)


def test_cross_source_verifier_error_degrades_and_retains_old_data() -> None:
    store = RecordingStore()
    verifier = FakeVerifier(
        primary=_quotes(),
        reference=_quotes(),
        reference_error=ConnectionError("backup source down"),
    )
    adapter = CrossCheckedAdapter("bars", verifier)

    result = _cross_orchestrator(adapter, verifier, store).run(_context())

    assert result.status is TaskStatus.DEGRADED
    # No targeted retry when the comparison itself failed: one full fetch,
    # never a full-market delete or rerun.
    assert len(adapter.calls) == 1
    record = _task_record(store)
    assert record["status"] == "degraded"
    assert record["replaced"] == 1
    summary = record["metadata"]["cross_source"]
    assert "backup source down" in summary["error"]


def test_cross_source_excludes_unsupported_backup_markets_explicitly() -> None:
    store = RecordingStore()
    verifier = FakeVerifier(
        primary=_quotes(),
        reference=_quotes(),
        supported_boards=frozenset({"shanghai", "shenzhen", "chinext", "star"}),
    )
    adapter = CrossCheckedAdapter("bars", verifier)

    result = _cross_orchestrator(adapter, verifier, store).run(_context())

    assert result.status is TaskStatus.SUCCESS
    summary = _task_record(store)["metadata"]["cross_source"]
    assert summary["excluded_boards"] == ("beijing",)
    assert "830799.BJ" not in summary["sampled"]
    for batch in verifier.reference_calls:
        assert "830799.BJ" not in batch
    for batch in verifier.primary_calls:
        assert "830799.BJ" not in batch


def test_cross_source_config_requires_matching_verifier() -> None:
    with pytest.raises(ValueError, match="cross-source"):
        RefreshOrchestrator(
            specs=(_spec("bars"),),
            adapters={"bars": RecordingAdapter("bars", [])},
            store=RecordingStore(),
            cross_source=CrossSourceCheckConfig(
                task_name="bars",
                tolerance=CrossSourceTolerance(price=0.01, volume=0.10),
            ),
        )


def test_cross_source_report_only_mismatch_observes_without_degrade_or_retry(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """report_only 下发现不一致：只记录 + 告警，绝不降级、绝不定向重试。"""
    store = RecordingStore()
    verifier = FakeVerifier(
        primary=_quotes({"300001.SZ": {"close": 10.5, "volume": 1000.0}}),
        reference=_quotes(),
    )
    adapter = CrossCheckedAdapter("bars", verifier)

    with caplog.at_level(logging.WARNING, logger="core.refresh"):
        result = _cross_orchestrator(
            adapter, verifier, store, report_only=True
        ).run(_context())

    assert result.status is TaskStatus.SUCCESS
    assert result.metadata["task_statuses"] == {"bars": "success"}
    # 只有初始全市场一次拉取：观察模式绝不触发定向重试
    assert len(adapter.calls) == 1
    record = _task_record(store)
    assert record["status"] == "success"
    summary = record["metadata"]["cross_source"]
    assert summary["report_only"] is True
    assert summary["mismatched"] == ("300001.SZ",)
    assert "retried_symbols" not in summary
    warnings = [rec.message for rec in caplog.records if rec.levelno == logging.WARNING]
    assert any("300001.SZ" in message for message in warnings)


def test_cross_source_report_only_compare_error_recorded_not_degraded(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """report_only 下比对异常：记录 error + 告警，任务状态保持成功。"""
    store = RecordingStore()
    verifier = FakeVerifier(
        primary=_quotes(),
        reference=_quotes(),
        reference_error=ConnectionError("backup source down"),
    )
    adapter = CrossCheckedAdapter("bars", verifier)

    with caplog.at_level(logging.WARNING, logger="core.refresh"):
        result = _cross_orchestrator(
            adapter, verifier, store, report_only=True
        ).run(_context())

    assert result.status is TaskStatus.SUCCESS
    assert len(adapter.calls) == 1
    record = _task_record(store)
    assert record["status"] == "success"
    summary = record["metadata"]["cross_source"]
    assert summary["report_only"] is True
    assert "backup source down" in summary["error"]
    warnings = [rec.message for rec in caplog.records if rec.levelno == logging.WARNING]
    assert any("backup source down" in message for message in warnings)


def test_cross_source_report_only_splits_unverifiable_from_mismatched(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """停牌/无数据样本归入 unverifiable，真实分歧归入 mismatched，两桶分开
    记录并分别告警；report_only 下两者都绝不触发降级或重试。"""
    store = RecordingStore()
    reference = _quotes()
    reference.pop("688001.SH")  # 停牌：参考源无数据
    verifier = FakeVerifier(
        primary=_quotes({"300001.SZ": {"close": 10.5, "volume": 1000.0}}),
        reference=reference,
    )
    adapter = CrossCheckedAdapter("bars", verifier)

    with caplog.at_level(logging.WARNING, logger="core.refresh"):
        result = _cross_orchestrator(
            adapter, verifier, store, report_only=True
        ).run(_context())

    assert result.status is TaskStatus.SUCCESS
    assert len(adapter.calls) == 1
    summary = _task_record(store)["metadata"]["cross_source"]
    assert summary["mismatched"] == ("300001.SZ",)
    assert summary["unverifiable"] == ("688001.SH",)
    assert "reference_dead" not in summary
    assert "retried_symbols" not in summary
    warnings = [rec.message for rec in caplog.records if rec.levelno == logging.WARNING]
    assert any("unverifiable" in m and "688001.SH" in m for m in warnings)
    assert any("mismatch" in m and "300001.SZ" in m for m in warnings)


def test_cross_source_enforce_unverifiable_only_never_retries_or_degrades() -> None:
    """enforce 下仅部分样本不可校验且无真实分歧：不重试、不降级。"""
    store = RecordingStore()
    reference = _quotes()
    reference.pop("688001.SH")
    reference.pop("830799.BJ")
    verifier = FakeVerifier(primary=_quotes(), reference=reference)
    adapter = CrossCheckedAdapter("bars", verifier)

    result = _cross_orchestrator(adapter, verifier, store).run(_context())

    assert result.status is TaskStatus.SUCCESS
    assert len(adapter.calls) == 1
    record = _task_record(store)
    assert record["status"] == "success"
    summary = record["metadata"]["cross_source"]
    assert summary["mismatched"] == ()
    assert summary["unverifiable"] == ("688001.SH", "830799.BJ")
    # 未触发重试时两个复核桶也必须预置为空元组，消费方永不 KeyError
    assert summary["still_mismatched"] == ()
    assert summary["still_unverifiable"] == ()
    assert "retried_symbols" not in summary
    assert "reference_dead" not in summary


def test_cross_source_enforce_retry_targets_only_real_mismatches() -> None:
    """enforce 下定向重试只针对真实分歧股票，unverifiable 绝不参与重试。"""
    store = RecordingStore()
    reference = _quotes()
    reference.pop("688001.SH")
    verifier = FakeVerifier(
        primary=_quotes({"300001.SZ": {"close": 10.5, "volume": 1000.0}}),
        reference=reference,
    )
    adapter = CrossCheckedAdapter("bars", verifier)

    result = _cross_orchestrator(adapter, verifier, store).run(_context())

    assert result.status is TaskStatus.SUCCESS
    assert len(adapter.calls) == 2
    assert adapter.calls[1].symbols == ("300001.SZ",)
    summary = _task_record(store)["metadata"]["cross_source"]
    assert summary["mismatched"] == ("300001.SZ",)
    assert summary["unverifiable"] == ("688001.SH",)
    assert summary["retried_symbols"] == ("300001.SZ",)
    assert summary["still_mismatched"] == ()


@dataclass
class ReferenceVanishingAdapter(CrossCheckedAdapter):
    """定向重试期间参考源对重试股票失去数据（复核时变为不可校验）。"""

    def refresh(self, context: RefreshContext) -> RefreshAdapterResult:
        if context.symbols is not None:
            for symbol in context.symbols:
                self.verifier.reference.pop(symbol, None)
        return super().refresh(context)


def test_cross_source_enforce_recheck_unverifiable_is_not_still_mismatched() -> None:
    """enforce 复核：重试后参考源失去数据的股票归入 still_unverifiable，
    绝不算 still_mismatched，也绝不因此降级（重试行已过 validate_task）。"""
    store = RecordingStore()
    verifier = FakeVerifier(
        primary=_quotes({"300001.SZ": {"close": 10.5, "volume": 1000.0}}),
        reference=_quotes(),
    )
    adapter = ReferenceVanishingAdapter("bars", verifier, fix_on_retry=False)

    result = _cross_orchestrator(adapter, verifier, store).run(_context())

    assert result.status is TaskStatus.SUCCESS
    assert result.metadata["task_statuses"] == {"bars": "success"}
    # 恰好一次定向重试，绝无全市场重拉
    assert len(adapter.calls) == 2
    assert adapter.calls[1].symbols == ("300001.SZ",)
    record = _task_record(store)
    assert record["status"] == "success"
    # 旧数据保留：记录保持原始发布计数
    assert record["replaced"] == 1
    summary = record["metadata"]["cross_source"]
    assert summary["retried_symbols"] == ("300001.SZ",)
    assert summary["still_mismatched"] == ()
    assert summary["still_unverifiable"] == ("300001.SZ",)


def test_cross_source_enforce_dead_reference_degrades_without_retry(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """死参考源守卫：非空样本零覆盖 ⇒ enforce 下降级且绝不定向重试。"""
    store = RecordingStore()
    verifier = FakeVerifier(primary=_quotes(), reference={})
    adapter = CrossCheckedAdapter("bars", verifier)

    with caplog.at_level(logging.WARNING, logger="core.refresh"):
        result = _cross_orchestrator(adapter, verifier, store).run(_context())

    assert result.status is TaskStatus.DEGRADED
    assert len(adapter.calls) == 1
    record = _task_record(store)
    assert record["status"] == "degraded"
    # 旧数据保留：记录保持原始发布计数，绝无全市场重拉或删除
    assert record["replaced"] == 1
    summary = record["metadata"]["cross_source"]
    assert summary["reference_dead"] is True
    assert summary["mismatched"] == ()
    assert len(summary["unverifiable"]) == len(summary["sampled"])
    warnings = [rec.message for rec in caplog.records if rec.levelno == logging.WARNING]
    assert any("no data for any sampled symbol" in m for m in warnings)


def test_execute_logs_per_task_progress_and_run_bounds(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """每完成一个任务必须通报序号/总数与耗时，否则长任务看起来像卡死。

    历史事故：07-30 三次 --refresh-today 全因界面长时间无输出被 SIGTERM，
    26 分钟的工作全部作废。TUI 的实时日志面板 tail 的是同一日志文件。
    """
    orchestrator = RefreshOrchestrator(
        specs=(_spec("base"), _spec("overlay", dependencies=("base",))),
        adapters={
            "base": RecordingAdapter("base", []),
            "overlay": RecordingAdapter("overlay", []),
        },
        store=RecordingStore(),
    )

    with caplog.at_level(logging.INFO, logger="core.refresh"):
        result = orchestrator.run(_context())

    assert result.status is TaskStatus.SUCCESS
    messages = [rec.message for rec in caplog.records]
    assert any("开始收盘刷新" in m and "共 2 个任务" in m for m in messages)
    assert any("收盘刷新任务 [1/2]: base" in m for m in messages)
    assert any("收盘刷新任务 [2/2]: overlay" in m for m in messages)
    assert any("✅ 任务 [1/2] base 完成" in m for m in messages)
    assert any("✅ 任务 [2/2] overlay 完成" in m for m in messages)
    assert any("🏁 收盘刷新结束：状态 success" in m for m in messages)


def test_blocked_task_logs_blocked_not_failed(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """被依赖阻塞的任务须标注 [blocked]，与真正失败区分开。"""
    orchestrator = RefreshOrchestrator(
        specs=(_spec("dependent", dependencies=("base",)), _spec("base")),
        adapters={
            "base": RecordingAdapter("base", [], failures_before_success=2),
            "dependent": RecordingAdapter("dependent", []),
        },
        store=RecordingStore(),
    )

    with caplog.at_level(logging.INFO, logger="core.refresh"):
        result = orchestrator.run(_context())

    assert result.status is TaskStatus.DEGRADED
    messages = [rec.message for rec in caplog.records]
    assert any("[blocked]" in m and "dependent" in m for m in messages)
    assert any("[failed]" in m and "base" in m for m in messages)


def test_pre_close_gate_logs_rejection_reason(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """闸门拒绝必须落日志：否则日志里只有一个无从解释的退出码 1。"""
    orchestrator = RefreshOrchestrator(
        specs=(_spec("bars"),),
        adapters={"bars": RecordingAdapter("bars", [])},
        store=RecordingStore(),
    )

    with caplog.at_level(logging.WARNING, logger="core.refresh"):
        result = orchestrator.run(_context(hour_utc=7, minute=59))

    assert result.status is TaskStatus.FAILED
    warnings = [rec.message for rec in caplog.records if rec.levelno == logging.WARNING]
    assert any("收盘刷新被拒" in m and "16:00" in m for m in warnings)


def test_cross_source_report_only_dead_reference_records_flag_without_degrade(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """report_only 下死参考源：只记 reference_dead + 告警，任务保持成功。"""
    store = RecordingStore()
    verifier = FakeVerifier(primary=_quotes(), reference={})
    adapter = CrossCheckedAdapter("bars", verifier)

    with caplog.at_level(logging.WARNING, logger="core.refresh"):
        result = _cross_orchestrator(
            adapter, verifier, store, report_only=True
        ).run(_context())

    assert result.status is TaskStatus.SUCCESS
    assert len(adapter.calls) == 1
    record = _task_record(store)
    assert record["status"] == "success"
    summary = record["metadata"]["cross_source"]
    assert summary["reference_dead"] is True
    assert summary["mismatched"] == ()
    warnings = [rec.message for rec in caplog.records if rec.levelno == logging.WARNING]
    assert any("no data for any sampled symbol" in m for m in warnings)


def test_resume_skips_settled_tasks_and_reruns_failures() -> None:
    """续跑沿用已落定的任务（适配器不再被调用），但失败任务必须重跑。"""
    store = RecordingStore()
    bars_calls: list[RefreshContext] = []
    flow_calls: list[RefreshContext] = []
    orchestrator = RefreshOrchestrator(
        specs=(_spec("bars"), _spec("flow")),
        adapters={
            "bars": RecordingAdapter("bars", bars_calls),
            "flow": RecordingAdapter("flow", flow_calls),
        },
        store=store,
    )
    resume = (
        _outcome("bars", status="success", replaced=1234),
        _outcome("flow", status="failed"),
    )

    result = orchestrator.run(_context(), resume_from=resume)

    assert bars_calls == []
    assert len(flow_calls) == 1
    task_records = {
        kwargs["task_name"]: kwargs for call, kwargs in store.calls if call == "task"
    }
    # 沿用的任务仍要重新落一遍审计行，metadata 标记来源
    assert task_records["bars"]["status"] == "success"
    assert task_records["bars"]["replaced"] == 1234
    assert task_records["bars"]["metadata"]["resumed"] is True
    # 重跑的任务不带 resumed 标记
    assert task_records["flow"]["status"] == "success"
    assert "resumed" not in task_records["flow"]["metadata"]
    assert result.status is TaskStatus.SUCCESS


def test_resume_degraded_task_keeps_run_degraded() -> None:
    """沿用的 degraded 任务必须让聚合结果照样 degraded，不得被洗成成功。"""
    orchestrator = RefreshOrchestrator(
        specs=(_spec("bars"),),
        adapters={"bars": RecordingAdapter("bars", [])},
        store=RecordingStore(),
    )

    result = orchestrator.run(
        _context(), resume_from=(_outcome("bars", status="degraded"),)
    )

    assert result.status is TaskStatus.DEGRADED
    assert result.exit_failure is True


def test_resume_satisfies_dependent_dependency() -> None:
    """沿用的成功任务要能被依赖看到：下游不会因「依赖失败」被误阻塞。"""
    base_calls: list[RefreshContext] = []
    dependent_calls: list[RefreshContext] = []
    orchestrator = RefreshOrchestrator(
        specs=(_spec("dependent", dependencies=("base",)), _spec("base")),
        adapters={
            "base": RecordingAdapter("base", base_calls),
            "dependent": RecordingAdapter("dependent", dependent_calls),
        },
        store=RecordingStore(),
    )

    result = orchestrator.run(
        _context(), resume_from=(_outcome("base", status="success"),)
    )

    assert base_calls == []
    assert len(dependent_calls) == 1
    assert result.status is TaskStatus.SUCCESS


def test_resume_logs_skip_line(caplog: pytest.LogCaptureFixture) -> None:
    """沿用旧结果必须明写 [resumed]，否则日志会像「任务凭空消失」。"""
    orchestrator = RefreshOrchestrator(
        specs=(_spec("bars"),),
        adapters={"bars": RecordingAdapter("bars", [])},
        store=RecordingStore(),
    )

    with caplog.at_level(logging.INFO, logger="core.refresh"):
        orchestrator.run(_context(), resume_from=(_outcome("bars", status="success"),))

    messages = [rec.message for rec in caplog.records]
    assert any("[resumed]" in m and "bars" in m for m in messages)
    assert any("🔁 续跑" in m for m in messages)
