"""Atomic SQLite writes for validated close-refresh snapshots."""

from __future__ import annotations

import json
import re
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

from core.task_result import TaskStatus

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_TERMINAL_STATUSES = frozenset(status.value for status in TaskStatus)

type Row = tuple[object, ...]
type Rows = tuple[Row, ...]


def _stable_json(value: object) -> str:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _stable_metadata_json(metadata: Mapping[str, Any] | None) -> str:
    return _stable_json({} if metadata is None else dict(metadata))


class RefreshValidationError(ValueError):
    """Raised when fetched refresh rows are unsafe to publish."""


class RefreshStateError(RuntimeError):
    """Raised when a refresh audit lifecycle transition is invalid."""


def _validate_terminal_status(status: str, *, scope: str) -> None:
    if status not in _TERMINAL_STATUSES:
        raise RefreshValidationError(
            f"invalid terminal {scope} status: {status!r}"
        )


def _validate_audit_counters(values: Mapping[str, object]) -> None:
    for name, value in values.items():
        if type(value) is not int or value < 0:
            raise RefreshValidationError(
                f"{name} must be a non-negative integer, got {value!r}"
            )


@dataclass(frozen=True, slots=True)
class DateSnapshotReplacement:
    """A complete replacement for one business-date partition."""

    table: str
    columns: tuple[str, ...]
    rows: Rows
    date_column: str
    date_value: object
    natural_keys: tuple[str, ...]
    required_fields: tuple[str, ...] = ()
    minimum_coverage: float | None = None
    allow_empty: bool = False


@dataclass(frozen=True, slots=True)
class KeyedUpsertReplacement:
    """A non-destructive snapshot merged by declared natural keys."""

    table: str
    columns: tuple[str, ...]
    rows: Rows
    natural_keys: tuple[str, ...]
    required_fields: tuple[str, ...] = ()
    minimum_coverage: float | None = None
    allow_empty: bool = False


@dataclass(frozen=True, slots=True)
class RunSnapshotReplacement:
    """A complete replacement for a table whose boundary is the current run."""

    table: str
    columns: tuple[str, ...]
    rows: Rows
    natural_keys: tuple[str, ...]
    required_fields: tuple[str, ...] = ()
    minimum_coverage: float | None = None
    allow_empty: bool = False


type Replacement = (
    DateSnapshotReplacement | KeyedUpsertReplacement | RunSnapshotReplacement
)


@dataclass(frozen=True, slots=True)
class CompositeReplacement:
    """Multiple replacements that must commit or roll back together."""

    replacements: tuple[Replacement, ...]


@dataclass(frozen=True, slots=True)
class ReplaceResult:
    """Summary of rows published by one atomic store operation."""

    replaced: int
    tables: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _StagedReplacement:
    request: Replacement
    staging_table: str


class SQLiteRefreshStore:
    """Publish already-fetched rows without exposing arbitrary SQL predicates."""

    def __init__(self, db_path: str | Path, *, timeout: float = 30.0) -> None:
        if not isinstance(db_path, (str, Path)):
            raise TypeError("db_path must be a string or pathlib.Path")
        if not str(db_path):
            raise ValueError("db_path must not be empty")
        self._db_path = Path(db_path)
        self._timeout = timeout

    def start_run(
        self,
        *,
        run_id: str,
        target_date: str,
        started_at: str,
        symbols: tuple[str, ...] | None = None,
    ) -> None:
        """Persist the start of one close-refresh run."""
        conn = self._connect()
        try:
            conn.execute(
                """INSERT INTO refresh_runs
                   (run_id, target_date, started_at, status, symbols_json)
                   VALUES (?, ?, ?, 'running', ?)""",
                (
                    run_id,
                    target_date,
                    started_at,
                    _stable_json(symbols),
                ),
            )
        finally:
            conn.close()

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
    ) -> None:
        """Persist the final result for one task in a refresh run."""
        _validate_terminal_status(status, scope="task")
        _validate_audit_counters({
            "fetched": fetched,
            "validated": validated,
            "replaced": replaced,
            "retained": retained,
            "failed": failed,
        })
        conn = self._connect()
        try:
            cursor = conn.execute(
                """INSERT INTO refresh_task_runs
                   (run_id, task_name, policy_kind, requested_date, as_of_date,
                    status, fetched, validated, replaced, retained, failed,
                    metadata_json)
                   SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                   WHERE EXISTS (
                       SELECT 1 FROM refresh_runs
                       WHERE run_id = ? AND status = 'running'
                   )""",
                (
                    run_id,
                    task_name,
                    policy_kind,
                    requested_date,
                    as_of_date,
                    status,
                    fetched,
                    validated,
                    replaced,
                    retained,
                    failed,
                    _stable_metadata_json(metadata),
                    run_id,
                ),
            )
            if cursor.rowcount != 1:
                raise RefreshStateError(
                    f"refresh run {run_id!r} does not exist or is not running"
                )
        finally:
            conn.close()

    def finish_run(
        self,
        *,
        run_id: str,
        finished_at: str,
        status: str,
    ) -> None:
        """Persist the completion state for one close-refresh run."""
        _validate_terminal_status(status, scope="run")
        conn = self._connect()
        try:
            cursor = conn.execute(
                """UPDATE refresh_runs
                   SET finished_at = ?, status = ?
                   WHERE run_id = ? AND status = 'running'""",
                (finished_at, status, run_id),
            )
            if cursor.rowcount != 1:
                raise RefreshStateError(
                    f"refresh run {run_id!r} does not exist or is not running"
                )
        finally:
            conn.close()

    def replace_date_snapshot(
        self,
        request: DateSnapshotReplacement,
    ) -> ReplaceResult:
        """Replace exactly one validated date partition."""
        return self._replace_atomically((request,))

    def upsert_keyed_snapshot(
        self,
        request: KeyedUpsertReplacement,
    ) -> ReplaceResult:
        """Insert or update declared keys without deleting unmentioned rows."""
        return self._replace_atomically((request,))

    def replace_run_snapshot(
        self,
        request: RunSnapshotReplacement,
    ) -> ReplaceResult:
        """Replace the entire current snapshot after staging succeeds."""
        return self._replace_atomically((request,))

    def replace_composite(self, request: CompositeReplacement) -> ReplaceResult:
        """Publish all component replacements in one transaction."""
        if not request.replacements:
            raise RefreshValidationError("composite replacement must not be empty")
        table_keys = []
        for replacement in request.replacements:
            self._validate_identifier(replacement.table)
            table_keys.append(replacement.table.casefold())
        if len(table_keys) != len(set(table_keys)):
            raise RefreshValidationError("composite replacement contains a duplicate table")
        return self._replace_atomically(request.replacements)

    def _replace_atomically(
        self,
        requests: tuple[Replacement, ...],
    ) -> ReplaceResult:
        for request in requests:
            self._validate_request(request)

        conn = self._connect()
        try:
            staged = tuple(self._stage(conn, request) for request in requests)
            conn.execute("BEGIN IMMEDIATE")
            try:
                for item in staged:
                    old_count = self._existing_count(conn, item.request)
                    self._validate_coverage(item.request, old_count)
                for item in staged:
                    self._publish_staged(conn, item)
                conn.commit()
            except BaseException:
                conn.rollback()
                raise
        finally:
            conn.close()

        return ReplaceResult(
            replaced=sum(len(request.rows) for request in requests),
            tables=tuple(request.table for request in requests),
        )

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            str(self._db_path),
            timeout=self._timeout,
            isolation_level=None,
        )
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _validate_request(self, request: Replacement) -> None:
        columns = request.columns
        self._validate_identifier(request.table)
        for identifier in columns:
            self._validate_identifier(identifier)
        for identifier in request.natural_keys:
            self._validate_identifier(identifier)
        for identifier in request.required_fields:
            self._validate_identifier(identifier)
        if isinstance(request, DateSnapshotReplacement):
            self._validate_identifier(request.date_column)

        if not columns:
            raise RefreshValidationError("columns must not be empty")
        if len(columns) != len(set(columns)):
            raise RefreshValidationError("columns must be unique")
        if not request.natural_keys:
            raise RefreshValidationError("natural keys must not be empty")
        if type(request.allow_empty) is not bool:
            raise RefreshValidationError("allow_empty must be a boolean")
        if not request.rows and not request.allow_empty:
            raise RefreshValidationError(
                "empty rows require explicit allow_empty=True authorization"
            )

        declared = set(columns)
        expected_fields = set(request.natural_keys) | set(request.required_fields)
        if isinstance(request, DateSnapshotReplacement):
            expected_fields.add(request.date_column)
        missing = expected_fields - declared
        if missing:
            raise RefreshValidationError(
                f"declared fields are absent from columns: {sorted(missing)}"
            )

        indexes = {column: index for index, column in enumerate(columns)}
        seen_keys: set[tuple[object, ...]] = set()
        for row_number, row in enumerate(request.rows, start=1):
            if len(row) != len(columns):
                raise RefreshValidationError(
                    f"row {row_number} has {len(row)} values; expected {len(columns)}"
                )
            for field in request.required_fields:
                if self._is_empty(row[indexes[field]]):
                    raise RefreshValidationError(
                        f"row {row_number} required field {field!r} is empty"
                    )

            key = tuple(row[indexes[field]] for field in request.natural_keys)
            if any(self._is_empty(value) for value in key):
                raise RefreshValidationError(
                    f"row {row_number} natural key contains an empty value"
                )
            if key in seen_keys:
                raise RefreshValidationError(f"duplicate natural key: {key!r}")
            seen_keys.add(key)

            if (
                isinstance(request, DateSnapshotReplacement)
                and row[indexes[request.date_column]] != request.date_value
            ):
                raise RefreshValidationError(
                    f"row {row_number} is outside target partition "
                    f"{request.date_column}={request.date_value!r}"
                )

        coverage = request.minimum_coverage
        if coverage is not None and not 0 <= coverage <= 1:
            raise RefreshValidationError("minimum coverage must be between 0 and 1")

    def _stage(
        self,
        conn: sqlite3.Connection,
        request: Replacement,
    ) -> _StagedReplacement:
        table = self._quoted(request.table)
        columns = self._column_list(request.columns)

        staging_name = f"_refresh_stage_{uuid4().hex}"
        staging = self._quoted(staging_name)
        conn.execute(
            f"CREATE TEMP TABLE {staging} AS "
            f"SELECT {columns} FROM {table} WHERE 0"
        )
        if request.rows:
            placeholders = ", ".join("?" for _ in request.columns)
            conn.executemany(
                f"INSERT INTO {staging} ({columns}) VALUES ({placeholders})",
                request.rows,
            )

        staged_count = conn.execute(f"SELECT COUNT(*) FROM {staging}").fetchone()[0]
        if staged_count != len(request.rows):
            raise RefreshValidationError(
                f"staging row count mismatch: expected {len(request.rows)}, "
                f"got {staged_count}"
            )
        return _StagedReplacement(request=request, staging_table=staging_name)

    def _existing_count(
        self,
        conn: sqlite3.Connection,
        request: Replacement,
    ) -> int:
        table = self._quoted(request.table)
        if isinstance(request, DateSnapshotReplacement):
            date_column = self._quoted(request.date_column)
            row = conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE {date_column} = ?",
                (request.date_value,),
            ).fetchone()
        else:
            row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        return int(row[0])

    @staticmethod
    def _validate_coverage(request: Replacement, old_count: int) -> None:
        minimum = request.minimum_coverage
        if minimum is None or not request.rows and request.allow_empty:
            return
        baseline = max(old_count, 1)
        coverage = len(request.rows) / baseline
        if coverage < minimum:
            raise RefreshValidationError(
                f"incoming coverage {coverage:.3f} is below minimum {minimum:.3f}"
            )

    def _publish_staged(
        self,
        conn: sqlite3.Connection,
        staged: _StagedReplacement,
    ) -> None:
        request = staged.request
        table = self._quoted(request.table)
        staging = self._quoted(staged.staging_table)
        columns = self._column_list(request.columns)

        if isinstance(request, DateSnapshotReplacement):
            date_column = self._quoted(request.date_column)
            conn.execute(
                f"DELETE FROM {table} WHERE {date_column} = ?",
                (request.date_value,),
            )
            conn.execute(
                f"INSERT INTO {table} ({columns}) "
                f"SELECT {columns} FROM {staging}"
            )
            return

        if isinstance(request, RunSnapshotReplacement):
            conn.execute(f"DELETE FROM {table}")
            conn.execute(
                f"INSERT INTO {table} ({columns}) "
                f"SELECT {columns} FROM {staging}"
            )
            return

        keys = self._column_list(request.natural_keys)
        update_columns = tuple(
            column for column in request.columns if column not in request.natural_keys
        )
        if update_columns:
            assignments = ", ".join(
                f"{self._quoted(column)} = excluded.{self._quoted(column)}"
                for column in update_columns
            )
            conflict_action = f"DO UPDATE SET {assignments}"
        else:
            conflict_action = "DO NOTHING"
        conn.execute(
            f"INSERT INTO {table} ({columns}) "
            f"SELECT {columns} FROM {staging} WHERE 1 "
            f"ON CONFLICT ({keys}) {conflict_action}"
        )

    @staticmethod
    def _validate_identifier(identifier: str) -> None:
        if not isinstance(identifier, str) or not _IDENTIFIER.fullmatch(identifier):
            raise RefreshValidationError(f"invalid SQL identifier: {identifier!r}")

    @classmethod
    def _quoted(cls, identifier: str) -> str:
        cls._validate_identifier(identifier)
        return f'"{identifier}"'

    @classmethod
    def _column_list(cls, columns: tuple[str, ...]) -> str:
        return ", ".join(cls._quoted(column) for column in columns)

    @staticmethod
    def _is_empty(value: object) -> bool:
        return value is None or isinstance(value, str) and not value.strip()
