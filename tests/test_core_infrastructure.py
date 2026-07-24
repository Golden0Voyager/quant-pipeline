"""Coverage tests for core infrastructure modules: runner.py + config.py.

``runner.py``: task_timer context manager, safe_task wrapper, _task_result_has_errors.
``config.py``: load_env_file, _detect_macos_proxy, PipelineConfig dataclass.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from unittest.mock import patch

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
            assert "push2.eastmoney.com" in os.environ.get("NO_PROXY", "")


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
