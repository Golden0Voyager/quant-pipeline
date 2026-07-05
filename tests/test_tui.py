import asyncio
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from tui import (
    PipelineApp,
    get_active_stock_count,
    get_daemon_status,
    get_db_size,
    get_launchd_status,
    get_subprocess_env,
    parse_progress,
)


@pytest.mark.asyncio
async def test_app_title():
    app = PipelineApp()
    async with app.run_test():
        assert app.title == "SmartMoney Pipeline Manager"

@pytest.mark.asyncio
async def test_widgets_present():
    app = PipelineApp()
    async with app.run_test():
        assert app.query_one("#status-dashboard") is not None
        assert app.query_one("#operations") is not None
        assert app.query_one("#scraping-progress") is not None
        assert app.query_one("#live-logs") is not None


def test_get_active_stock_count_empty(tmp_path):
    db_file = tmp_path / "test_empty.db"
    count = get_active_stock_count(str(db_file))
    assert count == 0

def test_get_active_stock_count_with_table(tmp_path):
    import sqlite3
    db_file = tmp_path / "test_active.db"
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE stock_list (code TEXT, market TEXT)")
    cursor.execute("INSERT INTO stock_list (code, market) VALUES ('000001', 'SZ')")
    cursor.execute("INSERT INTO stock_list (code, market) VALUES ('600000', 'SH')")
    conn.commit()
    conn.close()

    count = get_active_stock_count(str(db_file))
    assert count == 2


def test_get_db_size(tmp_path):
    db_file = tmp_path / "test.db"
    db_file.write_bytes(b"\x00" * 1024 * 1024 * 2) # 2MB
    size_str = get_db_size(str(db_file))
    assert size_str == "2.00 MB"

def test_get_db_size_not_exists():
    assert get_db_size("non_existent_file.db") == "0.00 MB"

def test_get_daemon_status_inactive(tmp_path):
    pid_file = tmp_path / "daemon.pid"
    status, pid = get_daemon_status(str(pid_file))
    assert status == "Stopped"
    assert pid is None

def test_get_daemon_status_running(tmp_path):
    import os
    pid_file = tmp_path / "daemon.pid"
    current_pid = os.getpid()
    pid_file.write_text(str(current_pid))
    status, pid = get_daemon_status(str(pid_file))
    assert status == "Running"
    assert pid == current_pid

def test_get_daemon_status_invalid_pid(tmp_path):
    pid_file = tmp_path / "daemon.pid"
    pid_file.write_text("not_a_pid")
    status, pid = get_daemon_status(str(pid_file))
    assert status == "Stopped"
    assert pid is None

def test_get_daemon_status_stale_pid(tmp_path):
    pid_file = tmp_path / "daemon.pid"
    pid_file.write_text("999999")
    status, pid = get_daemon_status(str(pid_file))
    assert status == "Stopped"
    assert pid is None

@pytest.mark.asyncio
async def test_get_launchd_status_active():
    mock_res = MagicMock()
    mock_res.stdout = "com.smartmoney.update\nother.service"
    with patch("subprocess.run", return_value=mock_res) as mock_run:
        assert await get_launchd_status() is True
        mock_run.assert_called_once()
        args, kwargs = mock_run.call_args
        assert args == (["launchctl", "list"],)
        assert kwargs["capture_output"] is True
        assert kwargs["text"] is True
        assert "NO_PROXY" in kwargs["env"]
        assert "DISABLE_YFINANCE_FALLBACK" in kwargs["env"]

@pytest.mark.asyncio
async def test_get_launchd_status_inactive():
    mock_res = MagicMock()
    mock_res.stdout = "other.service"
    with patch("subprocess.run", return_value=mock_res):
        assert await get_launchd_status() is False

@pytest.mark.asyncio
async def test_get_launchd_status_exception():
    with patch("subprocess.run", side_effect=OSError):
        assert await get_launchd_status() is False

def test_get_subprocess_env():
    env = get_subprocess_env()
    assert isinstance(env, dict)
    assert env.get("NO_PROXY") == "push2his.eastmoney.com,*.eastmoney.com,*.sina.com,*.sina.cn"
    assert env.get("DISABLE_YFINANCE_FALLBACK") == "1"


@pytest.mark.asyncio
async def test_key_bindings():
    from tui import PipelineApp
    app = PipelineApp()
    async with app.run_test():
        # Verify action exists
        assert app.check_action("run_pipeline", ()) is True
        assert app.check_action("resume_pipeline", ()) is True
        assert app.check_action("start_daemon", ()) is True
        assert app.check_action("stop_daemon", ()) is True
        assert app.check_action("run_health", ()) is True


@pytest.mark.asyncio
async def test_run_in_background_success():
    app = PipelineApp()
    mock_proc = MagicMock()
    mock_proc.wait = MagicMock(return_value=asyncio.Future())
    mock_proc.wait.return_value.set_result(0)
    mock_proc.returncode = 0

    with patch("asyncio.create_subprocess_exec", return_value=mock_proc) as mock_exec:
        await app._run_in_background("arg1", "arg2")
        mock_exec.assert_called_once()
        args, kwargs = mock_exec.call_args
        assert args == ("arg1", "arg2")
        assert "env" in kwargs
        assert kwargs["env"]["DISABLE_YFINANCE_FALLBACK"] == "1"
        assert kwargs["stdout"] == asyncio.subprocess.DEVNULL
        assert kwargs["stderr"] == asyncio.subprocess.DEVNULL
        mock_proc.wait.assert_called_once()


@pytest.mark.asyncio
async def test_run_in_background_non_zero_exit():
    app = PipelineApp()
    mock_proc = MagicMock()
    mock_proc.wait = MagicMock(return_value=asyncio.Future())
    mock_proc.wait.return_value.set_result(0)
    mock_proc.returncode = 127

    mock_logger = MagicMock()
    with patch("asyncio.create_subprocess_exec", return_value=mock_proc), \
         patch("logging.getLogger", return_value=mock_logger):
        await app._run_in_background("arg1", "arg2")
        mock_logger.error.assert_called_once_with("Subprocess arg1 arg2 exited with code 127")


@pytest.mark.asyncio
async def test_run_in_background_exception():
    app = PipelineApp()
    mock_logger = MagicMock()
    with patch("asyncio.create_subprocess_exec", side_effect=OSError("Spawn failed")), \
         patch("logging.getLogger", return_value=mock_logger):
        await app._run_in_background("arg1")
        mock_logger.exception.assert_called_once_with("Exception running subprocess arg1")


@pytest.mark.asyncio
async def test_action_handlers_use_run_in_background():
    app = PipelineApp()
    expected_pipeline_path = str(Path(sys.modules["tui"].__file__).parent / "daily_pipeline.py")
    expected_manager_path = str(Path(sys.modules["tui"].__file__).parent / "manager.sh")

    with patch.object(app, "_run_in_background", new_callable=MagicMock) as mock_run_bg, \
         patch("asyncio.create_task") as mock_create_task:

        await app.action_run_pipeline()
        mock_create_task.assert_called_once()
        mock_run_bg.assert_called_once_with(
            sys.executable, expected_pipeline_path, "--task", "all", "--force"
        )

        mock_run_bg.reset_mock()
        mock_create_task.reset_mock()
        await app.action_resume_pipeline()
        mock_create_task.assert_called_once()
        mock_run_bg.assert_called_once_with(
            sys.executable, expected_pipeline_path, "--task", "update_bars", "--resume", "--force"
        )

        mock_run_bg.reset_mock()
        mock_create_task.reset_mock()
        await app.action_start_daemon()
        mock_create_task.assert_called_once()
        mock_run_bg.assert_called_once_with(
            expected_manager_path, "daemon-resume"
        )

        mock_run_bg.reset_mock()
        mock_create_task.reset_mock()
        await app.action_stop_daemon()
        mock_create_task.assert_called_once()
        mock_run_bg.assert_called_once_with(
            expected_manager_path, "daemon-stop"
        )

        mock_run_bg.reset_mock()
        mock_create_task.reset_mock()
        await app.action_run_health()
        mock_create_task.assert_called_once()
        mock_run_bg.assert_called_once_with(
            sys.executable, expected_pipeline_path, "--task", "health_check", "--force"
        )


def test_parse_progress_valid(tmp_path):
    progress_file = tmp_path / "progress.json"
    progress_file.write_text("""{
        "task": "update_bars",
        "date": "2026-07-05",
        "start_time": "2026-07-05 12:00:00",
        "last_symbol": "SZ000001",
        "processed": 100,
        "total": 1000,
        "failed_queue": ["SH600000"]
    }""", encoding="utf-8")
    data = parse_progress(str(progress_file))
    assert data is not None
    assert data["processed"] == 100
    assert data["total"] == 1000
    assert len(data["failed_queue"]) == 1


def test_parse_progress_not_exists():
    assert parse_progress("non_existent_file.json") is None


def test_parse_progress_invalid_json(tmp_path):
    progress_file = tmp_path / "progress.json"
    progress_file.write_text("{invalid_json}", encoding="utf-8")
    assert parse_progress(str(progress_file)) is None
