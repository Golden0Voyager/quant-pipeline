"""Dependency-aware orchestration for safe close-refresh runs."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, time
from typing import Any, Literal, Protocol
from zoneinfo import ZoneInfo

from core.refresh_audit import RefreshAudit, RefreshAuditError, RefreshAuditReport
from core.task_registry import TaskSpec
from core.task_result import ErrorKind, TaskResult, TaskStatus

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_CLOSE_TIME = time(16, 0)


@dataclass(frozen=True, slots=True)
class RefreshContext:
    """Immutable identity and scope for one close-refresh run."""

    target_date: str
    started_at: datetime
    run_id: str
    symbols: tuple[str, ...] | None = None
    bypass_cache: Literal[True] = True
    refresh_mode: Literal["close_refresh"] = "close_refresh"
    allow_pre_close: bool = False

    def __post_init__(self) -> None:
        if not self.target_date:
            raise ValueError("target_date must not be empty")
        if self.started_at.tzinfo is None:
            raise ValueError("started_at must be timezone-aware")
        if not self.run_id:
            raise ValueError("run_id must not be empty")
        if self.bypass_cache is not True:
            raise ValueError("close refresh must bypass cache")


@dataclass(frozen=True, slots=True)
class RefreshAdapterResult:
    """Structured result returned by one refresh adapter."""

    task_name: str
    as_of_date: str | None
    fetched: int
    validated: int
    replaced: int
    retained: int
    failed_symbols: tuple[str, ...]
    changed_symbols: tuple[str, ...]
    metadata: Mapping[str, Any]


class RefreshAdapter(Protocol):
    """Task-specific close-refresh boundary injected into the orchestrator."""

    def refresh(self, context: RefreshContext) -> RefreshAdapterResult:
        """Fetch, validate, and atomically publish one task."""
        ...


class RefreshRunStore(Protocol):
    """Audit persistence surface supplied by ``SQLiteRefreshStore``."""

    def start_run(
        self,
        *,
        run_id: str,
        target_date: str,
        started_at: str,
        symbols: tuple[str, ...] | None = None,
    ) -> None: ...

    def record_task_result(
        self,
        *,
        run_id: str,
        task_name: str,
        policy_kind: str,
        requested_date: str,
        as_of_date: str | None,
        status: str,
        fetched: int,
        validated: int,
        replaced: int,
        retained: int,
        failed: int,
        metadata: Mapping[str, Any] | None = None,
    ) -> None: ...

    def finish_run(
        self,
        *,
        run_id: str,
        finished_at: str,
        status: str,
    ) -> None: ...


@dataclass(slots=True)
class _TaskExecution:
    task_result: TaskResult
    adapter_result: RefreshAdapterResult | None = None
    attempts: int = 0
    audit_report: RefreshAuditReport | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


class RefreshOrchestrator:
    """Run refresh adapters in dependency order and persist their audit trail."""

    def __init__(
        self,
        *,
        specs: Sequence[TaskSpec],
        adapters: Mapping[str, RefreshAdapter],
        store: RefreshRunStore,
        audit: RefreshAudit | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._specs = tuple(specs)
        self._adapters = dict(adapters)
        self._store = store
        self._audit = audit or RefreshAudit()
        self._clock = clock or (lambda: datetime.now(UTC))

    def run(self, context: RefreshContext) -> TaskResult:
        """Execute one dependency-aware close-refresh run."""
        gate_failure = self._pre_close_failure(context)
        if gate_failure is not None:
            return gate_failure

        try:
            ordered_specs = self._topological_specs()
        except ValueError as exc:
            return TaskResult.failed(
                "refresh_today",
                ErrorKind.INTERNAL,
                str(exc),
            )

        self._store.start_run(
            run_id=context.run_id,
            target_date=context.target_date,
            started_at=context.started_at.isoformat(),
            symbols=context.symbols,
        )

        executions: dict[str, _TaskExecution] = {}
        for spec in ordered_specs:
            policy = spec.refresh_policy
            if policy is None:
                continue
            blocked_by = tuple(
                dependency
                for dependency in policy.dependencies
                if dependency in executions
                and executions[dependency].task_result.status
                in {TaskStatus.FAILED, TaskStatus.ABORTED}
            )
            if blocked_by:
                execution = self._blocked_execution(spec.name, blocked_by)
            else:
                execution = self._run_task(
                    spec,
                    self._task_context(spec, context, executions),
                    symbols_ignored=context.symbols is not None
                    and not policy.supports_symbols,
                )
            executions[spec.name] = execution
            self._record_task(context, spec, execution)

        aggregate = self._aggregate(context, executions)
        self._store.finish_run(
            run_id=context.run_id,
            finished_at=self._aware_now().isoformat(),
            status=str(aggregate.status),
        )
        return aggregate

    @staticmethod
    def _pre_close_failure(context: RefreshContext) -> TaskResult | None:
        shanghai_time = context.started_at.astimezone(_SHANGHAI).timetz().replace(
            tzinfo=None
        )
        if shanghai_time >= _CLOSE_TIME or context.allow_pre_close:
            return None
        return TaskResult.failed(
            "refresh_today",
            ErrorKind.DATA_QUALITY,
            "close refresh is blocked before 16:00 Asia/Shanghai; "
            "explicit force authorization is required",
        )

    def _topological_specs(self) -> tuple[TaskSpec, ...]:
        by_name: dict[str, TaskSpec] = {}
        for spec in self._specs:
            if spec.name in by_name:
                raise ValueError(f"duplicate refresh task {spec.name!r}")
            if spec.refresh_policy is None:
                raise ValueError(f"refresh task {spec.name!r} has no refresh policy")
            by_name[spec.name] = spec

        for spec in self._specs:
            policy = spec.refresh_policy
            assert policy is not None
            missing = set(policy.dependencies) - set(by_name)
            if missing:
                raise ValueError(
                    f"task {spec.name!r} has missing dependencies: {sorted(missing)}"
                )
            if spec.name not in self._adapters:
                raise ValueError(f"task {spec.name!r} has no refresh adapter")

        remaining = list(self._specs)
        ordered: list[TaskSpec] = []
        completed: set[str] = set()
        while remaining:
            ready_index = next(
                (
                    index
                    for index, spec in enumerate(remaining)
                    if self._dependencies(spec) <= completed
                ),
                None,
            )
            if ready_index is None:
                cycle = sorted(spec.name for spec in remaining)
                raise ValueError(f"refresh dependency cycle detected: {cycle}")
            ready = remaining.pop(ready_index)
            ordered.append(ready)
            completed.add(ready.name)
        return tuple(ordered)

    def _task_context(
        self,
        spec: TaskSpec,
        context: RefreshContext,
        executions: Mapping[str, _TaskExecution],
    ) -> RefreshContext:
        policy = spec.refresh_policy
        assert policy is not None
        if not policy.supports_symbols:
            return replace(context, symbols=None)

        if not policy.dependencies:
            return context

        symbols = context.symbols
        changed: list[str] = []
        for dependency in policy.dependencies:
            adapter_result = executions[dependency].adapter_result
            if adapter_result is not None:
                changed.extend(adapter_result.changed_symbols)
        changed = list(dict.fromkeys(changed))
        if symbols is None:
            return replace(context, symbols=tuple(changed))
        changed_set = set(changed)
        return replace(
            context,
            symbols=tuple(symbol for symbol in symbols if symbol in changed_set),
        )

    def _run_task(
        self,
        spec: TaskSpec,
        context: RefreshContext,
        *,
        symbols_ignored: bool,
    ) -> _TaskExecution:
        adapter = self._adapters[spec.name]
        last_error: BaseException | None = None
        for attempt in (1, 2):
            try:
                adapter_result = adapter.refresh(context)
                audit_report = self._audit.validate_task(
                    spec,
                    context,
                    adapter_result,
                )
            except Exception as exc:
                last_error = exc
                continue

            metadata = dict(adapter_result.metadata)
            metadata["attempts"] = attempt
            if symbols_ignored:
                metadata["symbols_ignored"] = True
            degraded = bool(adapter_result.failed_symbols) or audit_report.degraded
            if degraded:
                task_result = TaskResult.degraded(
                    spec.name,
                    ErrorKind.DATA_QUALITY,
                    self._degraded_reason(adapter_result, audit_report),
                    saved=adapter_result.replaced,
                    attempted=adapter_result.fetched,
                    fetched=adapter_result.fetched,
                    accepted=adapter_result.validated,
                    rejected=len(adapter_result.failed_symbols),
                    source=spec.primary_source,
                )
            else:
                task_result = TaskResult.success(
                    spec.name,
                    saved=adapter_result.replaced,
                    attempted=adapter_result.fetched,
                    fetched=adapter_result.fetched,
                    accepted=adapter_result.validated,
                    rejected=0,
                    source=spec.primary_source,
                    data_date=adapter_result.as_of_date,
                )
            task_result.metadata = metadata
            return _TaskExecution(
                task_result=task_result,
                adapter_result=adapter_result,
                attempts=attempt,
                audit_report=audit_report,
                metadata=metadata,
            )

        assert last_error is not None
        error_kind = (
            ErrorKind.DATA_QUALITY
            if isinstance(last_error, RefreshAuditError)
            else ErrorKind.NETWORK
            if isinstance(last_error, (ConnectionError, TimeoutError))
            else ErrorKind.INTERNAL
        )
        task_result = TaskResult.failed(
            spec.name,
            error_kind,
            str(last_error),
            source=spec.primary_source,
        )
        metadata = {
            "attempts": 2,
            "retained_old_data": True,
            "error_type": type(last_error).__name__,
        }
        if symbols_ignored:
            metadata["symbols_ignored"] = True
        task_result.metadata = metadata
        return _TaskExecution(
            task_result=task_result,
            attempts=2,
            metadata=metadata,
        )

    @staticmethod
    def _blocked_execution(
        task_name: str,
        blocked_by: tuple[str, ...],
    ) -> _TaskExecution:
        task_result = TaskResult.failed(
            task_name,
            ErrorKind.DATA_QUALITY,
            f"blocked by failed dependencies: {', '.join(blocked_by)}",
        )
        metadata = {"blocked_by": blocked_by, "retained_old_data": True}
        task_result.metadata = metadata
        return _TaskExecution(task_result=task_result, metadata=metadata)

    def _record_task(
        self,
        context: RefreshContext,
        spec: TaskSpec,
        execution: _TaskExecution,
    ) -> None:
        policy = spec.refresh_policy
        assert policy is not None
        adapter_result = execution.adapter_result
        task_result = execution.task_result
        failed = (
            len(adapter_result.failed_symbols)
            if adapter_result
            else max(task_result.rejected, 1)
        )
        if task_result.status is TaskStatus.DEGRADED and failed == 0:
            failed = 1
        self._store.record_task_result(
            run_id=context.run_id,
            task_name=spec.name,
            policy_kind=str(policy.kind),
            requested_date=context.target_date,
            as_of_date=adapter_result.as_of_date if adapter_result else None,
            status=str(task_result.status),
            fetched=adapter_result.fetched if adapter_result else task_result.fetched,
            validated=adapter_result.validated if adapter_result else 0,
            replaced=adapter_result.replaced if adapter_result else task_result.saved,
            retained=adapter_result.retained if adapter_result else 0,
            failed=failed,
            metadata=execution.metadata,
        )

    @staticmethod
    def _aggregate(
        context: RefreshContext,
        executions: Mapping[str, _TaskExecution],
    ) -> TaskResult:
        task_results = [execution.task_result for execution in executions.values()]
        statuses = {
            name: str(execution.task_result.status)
            for name, execution in executions.items()
        }
        metadata = {
            "run_id": context.run_id,
            "target_date": context.target_date,
            "task_statuses": statuses,
            "failed_tasks": tuple(
                name
                for name, execution in executions.items()
                if execution.task_result.status is TaskStatus.FAILED
            ),
            "degraded_tasks": tuple(
                name
                for name, execution in executions.items()
                if execution.task_result.status is TaskStatus.DEGRADED
            ),
        }
        counts = {
            "attempted": sum(result.attempted for result in task_results),
            "fetched": sum(result.fetched for result in task_results),
            "accepted": sum(result.accepted for result in task_results),
            "rejected": sum(result.rejected for result in task_results),
            "saved": sum(result.saved for result in task_results),
        }
        if any(result.exit_failure for result in task_results):
            return TaskResult(
                task_name="refresh_today",
                status=TaskStatus.DEGRADED,
                attempted=counts["attempted"],
                fetched=counts["fetched"],
                accepted=counts["accepted"],
                rejected=counts["rejected"],
                saved=counts["saved"],
                error_kind=ErrorKind.DATA_QUALITY,
                error="one or more close-refresh tasks did not complete successfully",
                metadata=metadata,
                data_date=context.target_date,
            )
        return TaskResult.success(
            "refresh_today",
            attempted=counts["attempted"],
            fetched=counts["fetched"],
            accepted=counts["accepted"],
            rejected=counts["rejected"],
            saved=counts["saved"],
            metadata=metadata,
            data_date=context.target_date,
        )

    @staticmethod
    def _degraded_reason(
        result: RefreshAdapterResult,
        audit_report: RefreshAuditReport,
    ) -> str:
        if audit_report.dead_source:
            return str(result.metadata.get("reason", "source is unavailable"))
        return f"{len(result.failed_symbols)} symbols failed; old data retained"

    def _aware_now(self) -> datetime:
        now = self._clock()
        if now.tzinfo is None:
            raise ValueError("refresh clock must return a timezone-aware datetime")
        return now

    @staticmethod
    def _dependencies(spec: TaskSpec) -> set[str]:
        policy = spec.refresh_policy
        if policy is None:
            return set()
        return set(policy.dependencies)
