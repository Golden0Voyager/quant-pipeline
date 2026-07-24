"""Unified task outcome types.

Provides ``TaskStatus``, ``ErrorKind``, ``TaskResult`` and
``normalize_task_result`` so every pipeline task returns a consistent,
observable result that distinguishes success, legitimate empty data,
degraded partial results, hard failures and aborted runs.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class TaskStatus(StrEnum):
    SUCCESS = "success"
    NO_DATA = "no_data"
    DEGRADED = "degraded"
    FAILED = "failed"
    ABORTED = "aborted"


class ErrorKind(StrEnum):
    NETWORK = "network"
    RATE_LIMIT = "rate_limit"
    SOURCE_REMOVED = "source_removed"
    SCHEMA_DRIFT = "schema_drift"
    DATA_QUALITY = "data_quality"
    DATABASE = "database"
    INTERNAL = "internal"


@dataclass
class TaskResult:
    """Normalised outcome of one pipeline task invocation.

    Factory methods (``.success()``, ``.no_data()``, ``.degraded()``,
    ``.failed()``) are the preferred construction path — they enforce the
    invariants required by each status.
    """

    task_name: str
    status: TaskStatus
    attempted: int = 0
    fetched: int = 0
    accepted: int = 0
    rejected: int = 0
    saved: int = 0
    source: str | None = None
    data_date: str | None = None
    error_kind: ErrorKind | None = None
    error: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    # ── computed properties ──────────────────────────────────────────

    @property
    def exit_failure(self) -> bool:
        """True when the pipeline should exit with a non-zero code."""
        return self.status in {
            TaskStatus.DEGRADED,
            TaskStatus.FAILED,
            TaskStatus.ABORTED,
        }

    # ── factory methods ──────────────────────────────────────────────

    @classmethod
    def success(
        cls,
        task_name: str,
        *,
        saved: int,
        attempted: int = 0,
        fetched: int = 0,
        accepted: int = 0,
        rejected: int = 0,
        source: str | None = None,
        data_date: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> TaskResult:
        return cls(
            task_name=task_name,
            status=TaskStatus.SUCCESS,
            attempted=attempted,
            fetched=fetched,
            accepted=accepted,
            rejected=rejected,
            saved=saved,
            source=source,
            data_date=data_date,
            metadata=metadata or {},
        )

    @classmethod
    def no_data(
        cls,
        task_name: str,
        *,
        reason: str,
        attempted: int = 0,
        source: str | None = None,
        data_date: str | None = None,
    ) -> TaskResult:
        return cls(
            task_name=task_name,
            status=TaskStatus.NO_DATA,
            attempted=attempted,
            source=source,
            data_date=data_date,
            metadata={"reason": reason},
        )

    @classmethod
    def degraded(
        cls,
        task_name: str,
        error_kind: ErrorKind,
        error: str,
        *,
        saved: int = 0,
        fetched: int = 0,
        accepted: int = 0,
        rejected: int = 0,
        source: str | None = None,
    ) -> TaskResult:
        return cls(
            task_name=task_name,
            status=TaskStatus.DEGRADED,
            fetched=fetched,
            accepted=accepted,
            rejected=rejected,
            saved=saved,
            source=source,
            error_kind=error_kind,
            error=error,
        )

    @classmethod
    def failed(
        cls,
        task_name: str,
        error_kind: ErrorKind,
        error: str,
        *,
        fetched: int = 0,
        rejected: int = 0,
        source: str | None = None,
    ) -> TaskResult:
        return cls(
            task_name=task_name,
            status=TaskStatus.FAILED,
            fetched=fetched,
            rejected=rejected,
            source=source,
            error_kind=error_kind,
            error=error,
        )

    # ── serialisation ────────────────────────────────────────────────

    def to_dict(self) -> dict[str, Any]:
        """Return a plain dict for legacy CLI consumers."""
        return {
            "task_name": self.task_name,
            "status": str(self.status),
            "attempted": self.attempted,
            "fetched": self.fetched,
            "accepted": self.accepted,
            "rejected": self.rejected,
            "saved": self.saved,
            "source": self.source,
            "data_date": self.data_date,
            "error_kind": str(self.error_kind) if self.error_kind else None,
            "error": self.error,
            "metadata": self.metadata,
        }


# ── legacy dict normalisation ─────────────────────────────────────────


def normalize_task_result(
    task_name: str,
    value: TaskResult | dict[str, Any],
) -> TaskResult:
    """Convert a legacy dictionary or pass through a ``TaskResult``.

    Compatibility rules
    -------------------
    * ``{"saved": 0}`` (unqualified) → ``FAILED / DATA_QUALITY``.
    * ``{"saved": 0, "status": "no_data"}`` → ``NO_DATA``.
    * ``{"failed": N}`` with N > 0 → ``DEGRADED``.
    * An explicit ``"status"`` key is trusted when it matches one of the
      known status names.
    * An ``"error_kind"`` string value is mapped to the corresponding
      ``ErrorKind`` member when available.
    """
    if isinstance(value, TaskResult):
        return value

    # Fast path: explicit status we can trust ---------------------------------
    explicit_status = value.get("status")
    if explicit_status and isinstance(explicit_status, str):
        try:
            status = TaskStatus(explicit_status)
            saved = value.get("saved", 0)
            if status is TaskStatus.NO_DATA:
                return TaskResult.no_data(
                    task_name,
                    reason=value.get("reason", "unspecified"),
                    attempted=value.get("attempted", 0),
                    source=value.get("source"),
                    data_date=value.get("data_date"),
                )
            if status is TaskStatus.SUCCESS:
                return TaskResult.success(
                    task_name,
                    saved=saved if isinstance(saved, int) else 0,
                    attempted=value.get("attempted", 0),
                    fetched=value.get("fetched", 0),
                    accepted=value.get("accepted", 0),
                    rejected=value.get("rejected", 0),
                    source=value.get("source"),
                    data_date=value.get("data_date"),
                )
            if status is TaskStatus.FAILED:
                return TaskResult.failed(
                    task_name,
                    error_kind=_parse_error_kind(value),
                    error=value.get("error", "unspecified"),
                    fetched=value.get("fetched", 0),
                    rejected=value.get("rejected", 0),
                    source=value.get("source"),
                )
            if status is TaskStatus.DEGRADED:
                return TaskResult.degraded(
                    task_name,
                    error_kind=_parse_error_kind(value),
                    error=value.get("error", "partial"),
                    saved=value.get("saved", 0),
                    fetched=value.get("fetched", 0),
                    accepted=value.get("accepted", 0),
                    rejected=value.get("rejected", 0),
                    source=value.get("source"),
                )
        except ValueError:
            pass  # unknown status string → fall through to heuristics

    # Heuristic detection from legacy fields ----------------------------------
    error = value.get("error")
    if error:
        return TaskResult.failed(
            task_name,
            error_kind=_parse_error_kind(value),
            error=str(error),
            fetched=value.get("fetched", 0),
            rejected=value.get("rejected", 0),
        )

    saved = value.get("saved", 0)
    failed_count = value.get("failed", 0)

    if isinstance(failed_count, (int, float)) and failed_count > 0:
        return TaskResult.degraded(
            task_name,
            error_kind=_parse_error_kind(value),
            error=value.get("error", f"{failed_count} failures"),
            saved=int(saved) if isinstance(saved, (int, float)) else 0,
            fetched=value.get("fetched", 0),
            accepted=value.get("accepted", 0),
            rejected=value.get("rejected", 0),
        )

    if isinstance(saved, (int, float)) and saved > 0:
        return TaskResult.success(
            task_name,
            saved=int(saved),
            attempted=value.get("attempted", 0),
            fetched=value.get("fetched", 0),
            accepted=value.get("accepted", 0),
            rejected=value.get("rejected", 0),
            source=value.get("source"),
            data_date=value.get("data_date"),
        )

    # Everything else — unqualified zero → failure
    return TaskResult.failed(
        task_name,
        error_kind=_parse_error_kind(value),
        error=value.get("error", "zero rows without explanation"),
        fetched=value.get("fetched", 0),
        rejected=value.get("rejected", 0),
    )


def _parse_error_kind(value: dict[str, Any]) -> ErrorKind:
    """Map a string ``error_kind`` field to an ``ErrorKind`` member."""
    raw = value.get("error_kind")
    if raw and isinstance(raw, str):
        try:
            return ErrorKind(raw)
        except ValueError:
            pass
    return ErrorKind.DATA_QUALITY
