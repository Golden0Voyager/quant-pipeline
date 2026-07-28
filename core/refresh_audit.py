"""Task-level data-quality audit for close-refresh results."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from typing import TYPE_CHECKING, Any

from core.task_registry import DateStrategy, TaskSpec

if TYPE_CHECKING:
    from core.refresh import RefreshAdapterResult, RefreshContext

type AuditRow = Mapping[str, Any]
type AuditRows = Mapping[str, Sequence[AuditRow]]


class RefreshAuditError(ValueError):
    """Raised when a close-refresh result violates its declared policy."""


@dataclass(frozen=True, slots=True)
class RefreshAuditReport:
    """Successful task audit summary."""

    validated: int
    degraded: bool = False
    dead_source: bool = False


class RefreshAudit:
    """Validate dates, keys, fields, market invariants, and source state."""

    def validate_task(
        self,
        spec: TaskSpec,
        context: RefreshContext,
        result: RefreshAdapterResult,
        *,
        rows_by_table: AuditRows | None = None,
        baseline_counts: Mapping[str, int] | None = None,
    ) -> RefreshAuditReport:
        """Validate one adapter result against its registry policy."""
        policy = spec.refresh_policy
        if policy is None:
            raise RefreshAuditError(f"{spec.name} has no refresh policy")
        if result.task_name != spec.name:
            raise RefreshAuditError(
                f"adapter returned {result.task_name!r} for task {spec.name!r}"
            )
        self._validate_counts(result)

        metadata = result.metadata
        if self._is_dead_source(metadata):
            return self._validate_dead_source(result)

        self._validate_date(policy.date_strategy, policy.lookback_days, context, result)

        raw_rows = rows_by_table
        if raw_rows is None:
            candidate = metadata.get("audit_rows")
            if isinstance(candidate, Mapping):
                raw_rows = candidate
        raw_baselines = baseline_counts
        if raw_baselines is None:
            candidate = metadata.get("baseline_counts")
            if isinstance(candidate, Mapping):
                raw_baselines = candidate

        rows = raw_rows or {}
        baselines = raw_baselines or {}
        for table, table_rows in rows.items():
            if table not in policy.natural_keys:
                raise RefreshAuditError(
                    f"task {spec.name} supplied undeclared audit table {table!r}"
                )
            self._validate_rows(
                table=table,
                rows=table_rows,
                natural_keys=policy.natural_keys[table],
                required_fields=policy.required_fields.get(table, ()),
                date_column=spec.date_columns.get(table),
                accepted_date=result.as_of_date,
            )
            self._validate_coverage(
                table=table,
                incoming=len(table_rows),
                baseline=baselines.get(table),
                minimum=policy.minimum_coverage,
            )

        if policy.minimum_coverage is not None and not rows:
            expected = metadata.get("expected_count")
            if isinstance(expected, int):
                self._validate_coverage(
                    table=spec.name,
                    incoming=result.validated,
                    baseline=expected,
                    minimum=policy.minimum_coverage,
                )

        return RefreshAuditReport(validated=result.validated)

    @staticmethod
    def _validate_counts(result: RefreshAdapterResult) -> None:
        counts = {
            "fetched": result.fetched,
            "validated": result.validated,
            "replaced": result.replaced,
            "retained": result.retained,
        }
        for label, value in counts.items():
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise RefreshAuditError(f"{label} must be a nonnegative integer")
        if result.validated > result.fetched:
            raise RefreshAuditError("validated count exceeds fetched count")
        if result.replaced > result.validated:
            raise RefreshAuditError("replaced count exceeds validated count")

    @classmethod
    def _validate_date(
        cls,
        strategy: DateStrategy,
        lookback_days: int,
        context: RefreshContext,
        result: RefreshAdapterResult,
    ) -> None:
        if strategy is DateStrategy.RUN_SNAPSHOT:
            if result.metadata.get("run_id") != context.run_id:
                raise RefreshAuditError(
                    "run snapshot run_id does not match the current refresh run"
                )
            return

        if result.as_of_date is None:
            raise RefreshAuditError("dated refresh result is missing as_of_date")
        target = cls._parse_date(context.target_date, "target date")
        as_of = cls._parse_date(result.as_of_date, "as_of_date")

        if strategy is DateStrategy.EXACT_TARGET:
            if as_of != target:
                raise RefreshAuditError(
                    f"as_of_date {result.as_of_date} does not match target date "
                    f"{context.target_date}"
                )
            return

        age = (target - as_of).days
        if age < 0 or age > lookback_days:
            raise RefreshAuditError(
                f"as_of_date {result.as_of_date} is outside the "
                f"{lookback_days}-day lookback window"
            )

    @staticmethod
    def _parse_date(raw: str, label: str) -> date:
        for fmt in ("%Y-%m-%d", "%Y%m%d"):
            try:
                return datetime.strptime(raw, fmt).date()
            except (TypeError, ValueError):
                continue
        raise RefreshAuditError(f"{label} must be YYYY-MM-DD or YYYYMMDD")

    @classmethod
    def _validate_rows(
        cls,
        *,
        table: str,
        rows: Sequence[AuditRow],
        natural_keys: tuple[str, ...],
        required_fields: tuple[str, ...],
        date_column: str | None,
        accepted_date: str | None,
    ) -> None:
        seen: set[tuple[Any, ...]] = set()
        accepted = (
            cls._parse_date(accepted_date, "as_of_date")
            if accepted_date is not None
            else None
        )
        for row_number, row in enumerate(rows, start=1):
            for field in required_fields:
                if cls._is_empty(row.get(field)):
                    raise RefreshAuditError(
                        f"{table} row {row_number} required field {field!r} is empty"
                    )

            key = tuple(row.get(field) for field in natural_keys)
            if any(cls._is_empty(value) for value in key):
                raise RefreshAuditError(
                    f"{table} row {row_number} natural key contains an empty value"
                )
            if key in seen:
                raise RefreshAuditError(
                    f"{table} contains duplicate natural key {key!r}"
                )
            seen.add(key)

            if date_column and accepted is not None and date_column in row:
                row_date = cls._parse_date(str(row[date_column]), date_column)
                if row_date != accepted:
                    raise RefreshAuditError(
                        f"{table} row {row_number} is outside accepted date "
                        f"{accepted_date}"
                    )

            cls._validate_ohlc(table, row_number, row)
            cls._validate_nonnegative(table, row_number, row)

    @staticmethod
    def _validate_ohlc(table: str, row_number: int, row: AuditRow) -> None:
        fields = ("open", "high", "low", "close")
        if not all(field in row and row[field] is not None for field in fields):
            return
        try:
            open_, high, low, close = (float(row[field]) for field in fields)
        except (TypeError, ValueError) as exc:
            raise RefreshAuditError(
                f"{table} row {row_number} has nonnumeric OHLC values"
            ) from exc
        if high < max(open_, close) or low > min(open_, close) or high < low:
            raise RefreshAuditError(
                f"{table} row {row_number} violates OHLC invariants"
            )

    @staticmethod
    def _validate_nonnegative(table: str, row_number: int, row: AuditRow) -> None:
        for field in ("volume", "amount"):
            value = row.get(field)
            if value is None:
                continue
            try:
                negative = float(value) < 0
            except (TypeError, ValueError) as exc:
                raise RefreshAuditError(
                    f"{table} row {row_number} has nonnumeric {field}"
                ) from exc
            if negative:
                raise RefreshAuditError(
                    f"{table} row {row_number} has negative {field}"
                )

    @staticmethod
    def _validate_coverage(
        *,
        table: str,
        incoming: int,
        baseline: int | None,
        minimum: float | None,
    ) -> None:
        if minimum is None or baseline is None:
            return
        if baseline < 0:
            raise RefreshAuditError(f"{table} coverage baseline must be nonnegative")
        coverage = incoming / max(baseline, 1)
        if coverage < minimum:
            raise RefreshAuditError(
                f"{table} coverage {coverage:.3f} is below minimum {minimum:.3f}"
            )

    @staticmethod
    def _is_dead_source(metadata: Mapping[str, Any]) -> bool:
        return (
            metadata.get("source_status") == "dead_source"
            or metadata.get("dead_source") is True
        )

    @staticmethod
    def _validate_dead_source(
        result: RefreshAdapterResult,
    ) -> RefreshAuditReport:
        if result.replaced:
            raise RefreshAuditError("dead source result cannot claim replacement")
        if result.retained <= 0:
            raise RefreshAuditError("dead source result must retain old data")
        reason = result.metadata.get("reason")
        if not isinstance(reason, str) or not reason.strip():
            raise RefreshAuditError("dead source metadata requires a reason")
        return RefreshAuditReport(
            validated=result.validated,
            degraded=True,
            dead_source=True,
        )

    @staticmethod
    def _is_empty(value: object) -> bool:
        return value is None or isinstance(value, str) and not value.strip()

