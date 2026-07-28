"""Tests for typed task result types (TaskStatus, ErrorKind, TaskResult)."""

from __future__ import annotations

import pytest

from core.task_result import ErrorKind, TaskResult, TaskStatus, normalize_task_result


class TestTaskStatusContract:
    """Status enum factory behaviour."""

    def test_success_status(self):
        assert TaskResult.success("x", saved=10).status is TaskStatus.SUCCESS

    def test_no_data_requires_reason(self):
        result = TaskResult.no_data("x", reason="market holiday")
        assert result.status is TaskStatus.NO_DATA
        assert result.metadata.get("reason") == "market holiday"

    def test_no_data_without_reason_raises(self):
        with pytest.raises(TypeError):
            TaskResult.no_data("x")  # type: ignore[call-arg]

    def test_failed_requires_error_kind(self):
        with pytest.raises(TypeError):
            TaskResult.failed("x", "missing date")  # type: ignore[call-arg]

    def test_failed_exit_failure(self):
        result = TaskResult.failed("x", ErrorKind.SCHEMA_DRIFT, "missing date")
        assert result.status is TaskStatus.FAILED
        assert result.exit_failure is True

    def test_degraded_exit_failure(self):
        result = TaskResult.degraded(
            "x", ErrorKind.NETWORK, "partial fetch", saved=5, rejected=1
        )
        assert result.status is TaskStatus.DEGRADED
        assert result.exit_failure is True

    def test_aborted_exit_failure(self):
        tr = TaskResult("x", status=TaskStatus.ABORTED)
        assert tr.exit_failure is True

    def test_aborted_factory_preserves_contract(self):
        result = TaskResult.aborted(
            "x",
            error="circuit breaker open",
            saved=2,
            attempted=5,
        )

        assert result.status is TaskStatus.ABORTED
        assert result.saved == 2
        assert result.attempted == 5
        assert result.error == "circuit breaker open"
        assert result.exit_failure is True

    def test_no_data_not_exit_failure(self):
        result = TaskResult.no_data("x", reason="holiday")
        assert result.exit_failure is False

    def test_success_not_exit_failure(self):
        result = TaskResult.success("x", saved=10)
        assert result.exit_failure is False

    def test_degraded_requires_error_kind(self):
        with pytest.raises(TypeError):
            TaskResult.degraded("x", "partial")  # type: ignore[call-arg]


class TestNormalizeTaskResult:
    """Legacy dict → TaskResult conversion."""

    def test_unqualified_zero_rows_is_failed(self):
        result = normalize_task_result("x", {"saved": 0})
        assert result.status is TaskStatus.FAILED
        assert result.error_kind is ErrorKind.DATA_QUALITY

    def test_zero_rows_with_no_data_status(self):
        result = normalize_task_result(
            "x", {"saved": 0, "status": "no_data", "reason": "holiday"}
        )
        assert result.status is TaskStatus.NO_DATA
        assert result.metadata.get("reason") == "holiday"

    def test_failed_count_creates_degraded(self):
        result = normalize_task_result("x", {"failed": 2, "total": 10})
        assert result.status is TaskStatus.DEGRADED

    def test_success_with_saved(self):
        result = normalize_task_result("x", {"saved": 5, "total": 5})
        assert result.status is TaskStatus.SUCCESS

    def test_empty_dict_is_failed(self):
        result = normalize_task_result("x", {})
        assert result.status is TaskStatus.FAILED

    def test_error_key_becomes_failed_internal(self):
        result = normalize_task_result("x", {"error": "timeout"})
        assert result.status is TaskStatus.FAILED

    def test_keeps_explicit_task_status(self):
        result = normalize_task_result(
            "x", {"saved": 0, "status": "success", "skipped": True}
        )
        assert result.status is TaskStatus.SUCCESS

    def test_keeps_explicit_aborted_contract(self):
        result = normalize_task_result(
            "x",
            {
                "status": "aborted",
                "saved": 2,
                "attempted": 5,
                "error": "circuit breaker open",
            },
        )

        assert result.status is TaskStatus.ABORTED
        assert result.saved == 2
        assert result.attempted == 5
        assert result.error == "circuit breaker open"

    def test_passthrough_task_result(self):
        original = TaskResult.success("x", saved=10)
        result = normalize_task_result("x", original)
        assert result is original

    def test_error_kind_propagation(self):
        result = normalize_task_result(
            "x",
            {
                "saved": 0,
                "error": "schema changed",
                "error_kind": "schema_drift",
            },
        )
        assert result.error_kind is ErrorKind.SCHEMA_DRIFT


class TestTaskResultToDict:
    """``to_dict()`` serialisation for legacy CLI consumers."""

    def test_includes_core_fields(self):
        tr = TaskResult.success("t", saved=3, data_date="2026-07-23")
        d = tr.to_dict()
        assert d["task_name"] == "t"
        assert d["status"] == "success"
        assert d["saved"] == 3
        assert d["data_date"] == "2026-07-23"

    def test_includes_optional_fields_when_set(self):
        tr = TaskResult.failed("t", ErrorKind.NETWORK, "timeout", fetched=100)
        d = tr.to_dict()
        assert d["error_kind"] == "network"
        assert d["error"] == "timeout"

    def test_excludes_none_optionals(self):
        tr = TaskResult.success("t", saved=5)
        d = tr.to_dict()
        assert d.get("error") is None
        assert d.get("error_kind") is None


class TestTaskResultCounts:
    """Count field consistency."""

    def test_attempted_defaults_to_zero(self):
        assert TaskResult.success("x", saved=1).attempted == 0

    def test_counts_accept_positive_values(self):
        tr = TaskResult.success(
            "x", saved=5, attempted=10, fetched=10, accepted=8, rejected=2
        )
        assert tr.attempted == 10
        assert tr.fetched == 10
        assert tr.accepted == 8
        assert tr.rejected == 2
        assert tr.saved == 5


class TestErrorKindEnum:
    """ErrorKind string semantics."""

    def test_network(self):
        assert str(ErrorKind.NETWORK) == "network"

    def test_schema_drift(self):
        assert str(ErrorKind.SCHEMA_DRIFT) == "schema_drift"

    def test_source_removed(self):
        assert str(ErrorKind.SOURCE_REMOVED) == "source_removed"


class TestTaskResultDataclass:
    """``TaskResult`` is a frozen-ish dataclass."""

    def test_fields_can_be_read(self):
        tr = TaskResult("t", status=TaskStatus.SUCCESS, saved=5)
        assert tr.task_name == "t"
        assert tr.saved == 5

    def test_metadata_defaults_to_empty_dict(self):
        tr = TaskResult.success("x", saved=1)
        assert tr.metadata == {}
