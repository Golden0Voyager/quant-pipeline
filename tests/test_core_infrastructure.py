"""Coverage tests for core infrastructure modules: runner.py + config.py.

``runner.py``: task_timer context manager, safe_task wrapper, _task_result_has_errors.
``config.py``: load_env_file, _detect_macos_proxy, PipelineConfig dataclass.
"""

from __future__ import annotations

import os
import subprocess
from functools import wraps
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch
from uuid import UUID

from core.runner import _task_result_has_errors, safe_task, task_timer

# ===========================================================================
# runner.py
# ===========================================================================


class TestTaskTimer:
    """``task_timer`` context manager."""

    def test_yields_info_dict(self):
        """Yields dict with name, start time, elapsed callable."""
        with task_timer("my_task") as info:
            assert info["name"] == "my_task"
            assert isinstance(info["start"], float)
            assert callable(info["elapsed"])
            elapsed = info["elapsed"]()
            assert isinstance(elapsed, float)
            assert elapsed >= 0

    def test_elapsed_increases(self):
        """elapsed() 返回的值随时间增长。"""
        with task_timer("slow") as info:
            t1 = info["elapsed"]()
        t2 = info["elapsed"]()
        assert t2 >= t1


class TestSafeTask:
    """``safe_task`` wrapper (now normalizes via ``TaskResult``)."""

    def test_success_returns_normalized_dict(self):
        """``{"saved": 5}`` → normalized with status='success'."""
        result = safe_task("test", lambda **kw: {"saved": 5})
        assert result["status"] == "success"
        assert result["saved"] == 5
        assert "elapsed_seconds" in result["metadata"]

    def test_exception_returns_failed_internal(self):
        """fn 抛异常 → FAILED/INTERNAL + to_dict. """

        def crash(**kw):
            msg = "something broke"
            raise ValueError(msg)

        result = safe_task("crash", crash)
        assert result["status"] == "failed"
        assert result["error_kind"] == "internal"
        assert "something broke" in result["error"]

    def test_error_in_result_becomes_failed(self):
        """``{"error": "..."}`` → FAILED. """
        result = safe_task("err", lambda **kw: {"error": "partial failure"})
        assert result["status"] == "failed"

    def test_failed_count_becomes_degraded(self):
        """``{"failed": 3}`` → DEGRADED. """
        result = safe_task("fail", lambda **kw: {"failed": 3, "total": 10})
        assert result["status"] == "degraded"

    def test_aborted_key_becomes_failed(self):
        """``{"aborted": True}`` → FAILED (falls through heuristics). """
        result = safe_task("abort", lambda **kw: {"aborted": True})
        assert result["status"] == "failed"

    def test_failed_symbols_becomes_failed(self):
        """``{"failed_symbols": [...]}`` → FAILED (not a recognized key). """
        result = safe_task(
            "sym", lambda **kw: {"failed_symbols": ["000001"], "total": 5}
        )
        assert result["status"] == "failed"

    def test_success_results_always_include_status(self):
        """All normalized results include ``status``. """
        result = safe_task("ok", lambda **kw: {"saved": 10})
        assert result["status"] == "success"

    # ── New regression tests (Task 2 plan) ──────────────────────────

    def test_unqualified_zero_saved_becomes_failed(self):
        """``{"saved": 0}`` → FAILED (not success). """
        result = safe_task("zero", lambda **kw: {"saved": 0})
        assert result["status"] == "failed"

    def test_zero_saved_with_no_data_status(self):
        """``{"saved": 0, "status": "no_data"}`` → NO_DATA, not failure. """
        result = safe_task(
            "holiday",
            lambda **kw: {"saved": 0, "status": "no_data", "reason": "market holiday"},
        )
        assert result["status"] == "no_data"

    def test_partial_saved_with_rejected_becomes_degraded(self):
        """Partial success with non-zero rejected → DEGRADED. """
        result = safe_task(
            "partial",
            lambda **kw: {"saved": 8, "rejected": 2, "status": "degraded",
                          "error_kind": "data_quality", "error": "2 records invalid"},
        )
        assert result["status"] == "degraded"

    def test_metadata_includes_elapsed_seconds(self):
        """``metadata["elapsed_seconds"]`` is a positive float. """
        result = safe_task("timed", lambda **kw: {"saved": 1})
        assert isinstance(result["metadata"]["elapsed_seconds"], float)
        assert result["metadata"]["elapsed_seconds"] >= 0

    # ── 任务级网络错误重试 ─────────────────────────────────────────

    def test_network_failure_retried_once_then_success(self):
        """error_kind=network 的失败自动重试一次，第二次成功则整体成功。"""
        fn = MagicMock(side_effect=[
            {"error": "curl: (28) Operation timed out", "error_kind": "network"},
            {"saved": 3},
        ])
        with patch("core.runner.time.sleep") as mock_sleep:
            result = safe_task("flaky", fn)
        assert result["status"] == "success"
        assert fn.call_count == 2
        mock_sleep.assert_called_once_with(30.0)

    def test_network_failure_exhausted_stays_failed(self):
        """重试后仍网络失败 → failed，fn 共调用 2 次。"""
        fn = MagicMock(return_value={"error": "connection reset", "error_kind": "network"})
        with patch("core.runner.time.sleep"):
            result = safe_task("down", fn)
        assert result["status"] == "failed"
        assert result["error_kind"] == "network"
        assert fn.call_count == 2

    def test_safe_task_retries_retained_network_once(self):
        """retained + network 也按网络错误重试一次，最终仍保留 retained。"""
        fn = MagicMock(return_value={
            "status": "retained",
            "reason": "source down",
            "error_kind": "network",
        })
        with patch("core.runner.time.sleep") as mock_sleep:
            result = safe_task("retained", fn)
        assert result["status"] == "retained"
        assert fn.call_count == 2
        mock_sleep.assert_called_once_with(30.0)

    def test_safe_task_retained_logs_warning_not_success(self, caplog):
        """retained 结果记录 warning，且不会记录 ✅ 成功日志。"""
        fn = MagicMock(return_value={
            "status": "retained",
            "reason": "source down",
            "error_kind": "network",
        })
        with caplog.at_level("INFO", logger="core.runner"), patch("core.runner.time.sleep"):
            result = safe_task("retained", fn)
        assert result["status"] == "retained"
        assert any(
            record.levelname == "WARNING" and "retained" in record.message
            for record in caplog.records
        )
        assert not any("✅" in record.message for record in caplog.records)

    def test_non_network_failure_not_retried(self):
        """非 network 的失败（如 internal/data_quality）不重试。"""
        fn = MagicMock(return_value={"error": "partial failure"})
        with patch("core.runner.time.sleep") as mock_sleep:
            result = safe_task("err", fn)
        assert result["status"] == "failed"
        assert fn.call_count == 1
        mock_sleep.assert_not_called()

    def test_exception_not_retried(self):
        """fn 抛异常归为 internal，不触发网络重试。"""
        fn = MagicMock(side_effect=ValueError("boom"))
        with patch("core.runner.time.sleep") as mock_sleep:
            result = safe_task("crash", fn)
        assert result["status"] == "failed"
        assert fn.call_count == 1
        mock_sleep.assert_not_called()

    def test_fixed_signature_callback_does_not_receive_internal_run_id(self):
        """A callback with a fixed signature must remain callable."""

        def fixed() -> dict[str, int]:
            return {"saved": 1}

        result = safe_task("fixed", fixed)

        assert result["status"] == "success"
        assert result["saved"] == 1

    def test_explicit_run_id_parameter_receives_generated_uuid(self):
        """A callback that declares _task_run_id receives the scheduler UUID."""
        captured: dict[str, str | None] = {}

        def supported(_task_run_id: str | None = None) -> dict[str, int]:
            captured["run_id"] = _task_run_id
            return {"saved": 1}

        result = safe_task("supported", supported)

        assert result["status"] == "success"
        assert UUID(captured["run_id"] or "").version == 4

    def test_var_keyword_callback_receives_generated_run_id(self):
        """A generic **kwargs callback keeps the existing injection behavior."""
        captured: dict[str, object] = {}

        def generic(**kwargs: object) -> dict[str, int]:
            captured.update(kwargs)
            return {"saved": 1}

        result = safe_task("generic", generic)

        assert result["status"] == "success"
        assert UUID(str(captured["_task_run_id"])).version == 4

    def test_wrapped_fixed_signature_callback_does_not_receive_run_id(self):
        """Signature inspection follows functools.wraps to the original task."""

        def fixed() -> dict[str, int]:
            return {"saved": 1}

        @wraps(fixed)
        def wrapped(*args: object, **kwargs: object) -> dict[str, int]:
            return fixed(*args, **kwargs)

        result = safe_task("wrapped", wrapped)

        assert result["status"] == "success"
        assert result["saved"] == 1

    def test_caller_provided_run_id_is_preserved(self):
        """safe_task must not overwrite a supported caller-provided run ID."""
        captured: dict[str, str | None] = {}
        db = MagicMock()

        def supported(
            db: Any,
            _task_run_id: str | None = None,
        ) -> dict[str, int]:
            captured["run_id"] = _task_run_id
            return {"saved": 1}

        result = safe_task(
            "provided", supported, db, _task_run_id="provided-run-id"
        )

        assert result["status"] == "success"
        assert captured["run_id"] == "provided-run-id"
        recorded = db.record_ingestion_run.call_args.args[0]
        assert recorded["metadata"]["run_id"] == captured["run_id"]

    def test_lock_decorated_fixed_signature_callback_does_not_receive_run_id(self):
        """@skip_if_task_locked wrapped fixed-signature callback must not get _task_run_id."""
        from core.lock import skip_if_task_locked

        @skip_if_task_locked("decorated")
        def fixed(db: object) -> dict[str, int]:
            return {"saved": 1}

        result = safe_task("decorated", fixed, object())

        assert result["status"] == "success"
        assert result["saved"] == 1

    def test_var_positional_run_id_name_does_not_accept_keyword(self):
        def positional(*_task_run_id: object) -> dict[str, int]:
            return {"saved": 1}

        result = safe_task("positional", positional)

        assert result["status"] == "success"

    def test_pit_capable_callback_records_parent_before_execution(self):
        db = MagicMock()
        observed: dict[str, object] = {}

        def supported(
            db: Any,
            _task_run_id: str | None = None,
        ) -> dict[str, int]:
            observed["run_id"] = _task_run_id
            observed["calls_before_callback"] = db.record_ingestion_run.call_count
            observed["first_status"] = (
                db.record_ingestion_run.call_args_list[0].args[0]["status"]
            )
            return {"saved": 1}

        result = safe_task("pit", supported, db)

        assert result["status"] == "success"
        assert observed["calls_before_callback"] == 1
        assert observed["first_status"] == "running"
        assert db.record_ingestion_run.call_count == 2
        first = db.record_ingestion_run.call_args_list[0].args[0]
        final = db.record_ingestion_run.call_args_list[1].args[0]
        assert first["metadata"]["run_id"] == observed["run_id"]
        assert final["metadata"]["run_id"] == observed["run_id"]

    def test_failed_parent_write_prevents_pit_callback(self):
        db = MagicMock()
        db.record_ingestion_run.side_effect = RuntimeError("audit locked")
        called = False

        def supported(
            db: Any,
            _task_run_id: str | None = None,
        ) -> dict[str, int]:
            nonlocal called
            called = True
            return {"saved": 1}

        result = safe_task("pit", supported, db)

        assert called is False
        assert result["status"] == "failed"
        assert result["error_kind"] == "database"
        assert "audit parent" in result["error"]

    def test_uninspectable_callback_runs_without_internal_run_id(self):
        """Optional metadata injection must not block an uninspectable callback."""

        class UninspectableCallable:
            __signature__ = "invalid"

            def __call__(self) -> dict[str, int]:
                return {"saved": 1}

        result = safe_task("uninspectable", UninspectableCallable())

        assert result["status"] == "success"
        assert result["saved"] == 1


class TestTaskResultHasErrors:
    """``_task_result_has_errors`` helper."""

    def test_empty_dict(self):
        assert _task_result_has_errors({}) is False

    def test_error_key(self):
        assert _task_result_has_errors({"error": "fail"}) is True

    def test_aborted_key(self):
        assert _task_result_has_errors({"aborted": True}) is True

    def test_failed_positive_int(self):
        assert _task_result_has_errors({"failed": 1}) is True

    def test_failed_positive_float(self):
        assert _task_result_has_errors({"failed": 0.5}) is True

    def test_failed_zero(self):
        assert _task_result_has_errors({"failed": 0}) is False

    def test_failed_none(self):
        assert _task_result_has_errors({"failed": None}) is False

    def test_failed_symbols_non_empty(self):
        assert _task_result_has_errors({"failed_symbols": ["000001"]}) is True

    def test_failed_symbols_empty(self):
        assert _task_result_has_errors({"failed_symbols": []}) is False

    def test_failed_symbols_none(self):
        assert _task_result_has_errors({"failed_symbols": None}) is False


# ===========================================================================
# config.py — load_env_file
# ===========================================================================


class TestLoadEnvFile:
    """``load_env_file`` 函数。

    所有修改 ``os.environ`` 的操作都在 ``patch.dict(os.environ)``
    上下文中执行，退出后自动恢复。
    """

    def test_no_file(self):
        """不存在的文件 → 无错误。"""
        from core.config import load_env_file
        load_env_file("/tmp/nonexistent_env_file_xyz")

    def test_loads_key_value(self, tmp_path: Path):
        """解析 KEY=VALUE 行。"""
        from core.config import load_env_file
        env_file = tmp_path / ".env"
        env_file.write_text("FOO=bar\nBAZ=qux\n")
        with patch.dict(os.environ):
            load_env_file(str(env_file))
            assert os.environ.get("FOO") == "bar"
            assert os.environ.get("BAZ") == "qux"

    def test_skips_comments_and_blanks(self, tmp_path: Path):
        """跳过 # 注释行和空行。"""
        from core.config import load_env_file
        env_file = tmp_path / ".env"
        env_file.write_text("# this is a comment\n\nKEY=val\n")
        with patch.dict(os.environ):
            load_env_file(str(env_file))
            assert os.environ.get("KEY") == "val"

    def test_skips_lines_without_equals(self, tmp_path: Path):
        """跳过不含 = 的行。"""
        from core.config import load_env_file
        env_file = tmp_path / ".env"
        env_file.write_text("NOPE\nKEY=val\n")
        with patch.dict(os.environ):
            load_env_file(str(env_file))
            assert os.environ.get("KEY") == "val"

    def test_does_not_override_existing(self, tmp_path: Path):
        """已有环境变量时不被覆盖。"""
        from core.config import load_env_file
        with patch.dict(os.environ, {"EXISTING_KEY": "original"}):
            env_file = tmp_path / ".env"
            env_file.write_text("EXISTING_KEY=override\n")
            load_env_file(str(env_file))
            assert os.environ["EXISTING_KEY"] == "original"

    def test_strips_quotes(self, tmp_path: Path):
        """去除值两侧的引号。"""
        from core.config import load_env_file
        env_file = tmp_path / ".env"
        env_file.write_text('QUOTED="hello"')
        with patch.dict(os.environ):
            load_env_file(str(env_file))
            assert os.environ.get("QUOTED") == "hello"


# ===========================================================================
# config.py — _detect_macos_proxy
# ===========================================================================


class TestDetectMacosProxy:
    """``_detect_macos_proxy`` 函数。"""

    def test_already_set(self):
        """代理变量已存在 → 不执行 scutil。"""
        from core.config import _detect_macos_proxy
        with (
            patch.dict(os.environ, {"HTTP_PROXY": "http://existing:8080"}, clear=False),
            patch("core.config.subprocess.run") as mock_run,
        ):
            _detect_macos_proxy()
            mock_run.assert_not_called()

    def test_scutil_fails_gracefully(self):
        """subprocess.run 异常 → 静默捕获。"""
        from core.config import _detect_macos_proxy
        with (
            patch.dict(os.environ, {}, clear=True),
            patch("core.config.subprocess.run", side_effect=FileNotFoundError("no scutil")),
        ):
            _detect_macos_proxy()  # Should not raise

    def test_scutil_no_http_enable(self):
        """scutil 返回 HTTPEnable: 0 → 不设代理。"""
        from core.config import _detect_macos_proxy
        fake_out = "HTTPEnable : 0"
        with patch.dict(os.environ, {}, clear=True):
            with patch.object(subprocess, "run") as mock_run:
                mock_run.return_value.stdout = fake_out
                _detect_macos_proxy()
            assert "HTTP_PROXY" not in os.environ

    def test_scutil_sets_proxy_vars(self):
        """scutil 返回 HTTPEnable: 1 → 设置代理环境变量。"""
        from core.config import _detect_macos_proxy
        fake_out = (
            "HTTPEnable : 1\n"
            "HTTPProxy : 127.0.0.1\n"
            "HTTPPort : 8080\n"
        )
        with patch.dict(os.environ, {}, clear=True):
            with patch.object(subprocess, "run") as mock_run:
                mock_run.return_value.stdout = fake_out
                _detect_macos_proxy()
            assert os.environ.get("HTTP_PROXY") == "http://127.0.0.1:8080"
            assert os.environ.get("HTTPS_PROXY") == "http://127.0.0.1:8080"
            # 境内数据源必须全部直连（urllib no_proxy 按 endswith 匹配裸域名）
            no_proxy = os.environ.get("NO_PROXY", "")
            for domain in (
                "eastmoney.com", "sina.com.cn", "sse.com.cn",
                "szse.cn", "jin10.com", "csindex.com.cn", "cninfo.com.cn",
            ):
                assert domain in no_proxy


# ===========================================================================
# config.py — PipelineConfig
# ===========================================================================


class TestPipelineConfig:
    """``PipelineConfig`` dataclass。"""

    def test_defaults(self):
        """默认值不为空。"""
        from core.config import PipelineConfig
        cfg = PipelineConfig()
        assert cfg.db_path.endswith("quant_core.db")
        assert cfg.lookback_days >= 0
        assert cfg.parallel_workers >= 1

    def test_from_env(self):
        """from_env 返回 PipelineConfig 实例。"""
        from core.config import PipelineConfig
        cfg = PipelineConfig.from_env()
        assert isinstance(cfg, PipelineConfig)
        assert cfg.db_path.endswith("quant_core.db")

    def test_ultra_safe_default(self):
        """ULTRA_SAFE 未设置时 ultra_safe=False。"""
        from core.config import PipelineConfig
        with patch.dict(os.environ, {}, clear=True):
            cfg = PipelineConfig()
            assert cfg.ultra_safe is False
