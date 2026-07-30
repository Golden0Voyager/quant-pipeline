"""Task-level data-quality audit for close-refresh results."""

from __future__ import annotations

import math
import random
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from typing import TYPE_CHECKING, Any

from core.task_registry import DateStrategy, TaskSpec

if TYPE_CHECKING:
    from core.refresh import RefreshAdapterResult, RefreshContext

type AuditRow = Mapping[str, Any]
type AuditRows = Mapping[str, Sequence[AuditRow]]

# Canonical board order for stratified cross-source sampling. Prefix rules
# follow AGENTS.md: 6/9 → sh, 0/2/3 → sz, 4/8/920 → bj, with ChiNext (30x)
# and STAR (68x) carved out as their own strata.
CROSS_SOURCE_BOARDS: tuple[str, ...] = (
    "shanghai",
    "shenzhen",
    "chinext",
    "star",
    "beijing",
)


def classify_board(symbol: str) -> str | None:
    """Map a symbol to its board, or None when the code is not an A-share."""
    code = symbol.split(".", 1)[0]
    if not code or not code.isdigit():
        return None
    if code.startswith("920") or code[0] in "48":
        return "beijing"
    if code.startswith("68"):
        return "star"
    if code[0] in "69":
        return "shanghai"
    if code.startswith("30"):
        return "chinext"
    if code[0] in "023":
        return "shenzhen"
    return None


@dataclass(frozen=True, slots=True)
class CrossSourceSample:
    """Deterministic stratified sample plus the explicitly excluded boards."""

    symbols: tuple[str, ...]
    excluded_boards: tuple[str, ...]


def stratified_cross_source_sample(
    candidates: Sequence[str],
    *,
    target_date: str,
    supported_boards: Collection[str],
    sample_size: int = 30,
) -> CrossSourceSample:
    """Draw ~sample_size symbols spread over the backup source's boards.

    The seed is derived from target_date, so the same date always yields the
    same sample. Boards the backup source does not support are excluded
    explicitly and reported, never sampled by luck.
    """
    if sample_size <= 0:
        raise ValueError("sample_size must be positive")
    unknown = set(supported_boards) - set(CROSS_SOURCE_BOARDS)
    if unknown:
        raise ValueError(f"unknown boards in supported_boards: {sorted(unknown)}")

    by_board: dict[str, list[str]] = {board: [] for board in CROSS_SOURCE_BOARDS}
    for symbol in dict.fromkeys(candidates):
        board = classify_board(symbol)
        if board is not None:
            by_board[board].append(symbol)

    excluded = tuple(
        board for board in CROSS_SOURCE_BOARDS if board not in supported_boards
    )
    populated = tuple(
        board
        for board in CROSS_SOURCE_BOARDS
        if board in supported_boards and by_board[board]
    )
    if not populated:
        return CrossSourceSample((), excluded)

    rng = random.Random(f"cross-source-sample:{target_date}")
    base, extra = divmod(sample_size, len(populated))
    chosen: list[str] = []
    for index, board in enumerate(populated):
        quota = base + (1 if index < extra else 0)
        pool = sorted(by_board[board])
        chosen.extend(rng.sample(pool, min(quota, len(pool))))
    return CrossSourceSample(tuple(chosen), excluded)


@dataclass(frozen=True, slots=True)
class CrossSourceTolerance:
    """Separately configured relative tolerances for price and volume."""

    price: float
    volume: float

    def __post_init__(self) -> None:
        for label, value in (("price", self.price), ("volume", self.volume)):
            if (
                not isinstance(value, (int, float))
                or isinstance(value, bool)
                or not math.isfinite(value)
                or value < 0
            ):
                raise ValueError(
                    f"{label} tolerance must be a finite nonnegative number"
                )


@dataclass(frozen=True, slots=True)
class CrossSourceComparison:
    """Split comparison outcome: real disagreements vs reference-missing."""

    mismatched: tuple[str, ...]
    unverifiable: tuple[str, ...]


def compare_cross_source_quotes(
    symbols: Sequence[str],
    primary: Mapping[str, Mapping[str, Any]],
    reference: Mapping[str, Mapping[str, Any]],
    tolerance: CrossSourceTolerance,
) -> CrossSourceComparison:
    """Partition sampled symbols into real mismatches and unverifiable ones.

    A symbol the reference source returned no data for (suspended or simply
    not covered) is *unverifiable*: the backup offers no evidence either way,
    so it must not be reported as a disagreement. A symbol present in the
    reference but missing from primary IS a real mismatch: the backup has
    evidence for a row we failed to publish. Values present on both sides
    are compared under the configured tolerances.
    """
    mismatched: list[str] = []
    unverifiable: list[str] = []
    for symbol in symbols:
        theirs = reference.get(symbol)
        if theirs is None:
            unverifiable.append(symbol)
            continue
        ours = primary.get(symbol)
        if ours is None:
            mismatched.append(symbol)
            continue
        close_ok = _within_tolerance(
            ours.get("close"), theirs.get("close"), tolerance.price
        )
        volume_ok = _within_tolerance(
            ours.get("volume"), theirs.get("volume"), tolerance.volume
        )
        if not close_ok or not volume_ok:
            mismatched.append(symbol)
    return CrossSourceComparison(tuple(mismatched), tuple(unverifiable))


def cross_source_mismatches(
    symbols: Sequence[str],
    primary: Mapping[str, Mapping[str, Any]],
    reference: Mapping[str, Mapping[str, Any]],
    tolerance: CrossSourceTolerance,
) -> tuple[str, ...]:
    """Return sampled symbols whose close or volume disagrees across sources.

    Flat compatibility view over ``compare_cross_source_quotes``: a missing
    or unreadable quote on either side still counts as flagged here, because
    absent evidence must not pass a data-quality check. Callers that need to
    treat reference-missing symbols differently should use the split helper.
    """
    comparison = compare_cross_source_quotes(symbols, primary, reference, tolerance)
    flagged = set(comparison.mismatched) | set(comparison.unverifiable)
    return tuple(symbol for symbol in symbols if symbol in flagged)


def _within_tolerance(ours: Any, theirs: Any, tolerance: float) -> bool:
    try:
        ours_value = float(ours)
        theirs_value = float(theirs)
    except (TypeError, ValueError):
        return False
    if not math.isfinite(ours_value) or not math.isfinite(theirs_value):
        return False
    if theirs_value == 0:
        return ours_value == 0
    return abs(ours_value - theirs_value) <= tolerance * abs(theirs_value)


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
        if result.retained < 0:
            raise RefreshAuditError("dead source retained count must be nonnegative")
        # retained == 0 is honest only when the adapter attests the baseline
        # table itself is empty; anything else means old data was lost.
        if result.retained == 0 and result.metadata.get("baseline_empty") is not True:
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

