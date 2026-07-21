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
    """``safe_task`` wrapper."""

    def test_success_returns_result(self):
        """fn 正常返回 → 返回 fn 的结果。"""
        result = safe_task("test", lambda: {"saved": 5})
        assert result["saved"] == 5

    def test_exception_returns_error(self):
        """fn 抛异常 → 返回 error dict + status='crashed'。"""

        def crash():
            msg = "something broke"
            raise ValueError(msg)

        result = safe_task("crash", crash)
        assert result["status"] == "crashed"
        assert "something broke" in result["error"]

    def test_error_in_result_sets_completed_with_errors(self):
        """结果中有 ``error`` 键 → status='completed_with_errors'。"""
        result = safe_task("err", lambda: {"error": "partial failure"})
        assert result["status"] == "completed_with_errors"

    def test_failed_gt_zero_sets_completed_with_errors(self):
        """结果中 failed > 0 → status='completed_with_errors'。"""
        result = safe_task("fail", lambda: {"failed": 3, "total": 10})
        assert result["status"] == "completed_with_errors"

    def test_aborted_sets_completed_with_errors(self):
        """结果中 aborted=True → status='completed_with_errors'。"""
        result = safe_task("abort", lambda: {"aborted": True})
        assert result["status"] == "completed_with_errors"

    def test_failed_symbols_non_empty_sets_completed_with_errors(self):
        """结果中 failed_symbols 非空列表 → status='completed_with_errors'。"""
        result = safe_task(
            "sym", lambda: {"failed_symbols": ["000001"], "total": 5}
        )
        assert result["status"] == "completed_with_errors"

    def test_clean_result_no_status_override(self):
        """正常结果无 error/aborted/failed → 不添加 status。"""
        result = safe_task("ok", lambda: {"saved": 10})
        assert "status" not in result


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
