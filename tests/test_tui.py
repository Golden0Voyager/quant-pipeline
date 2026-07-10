import asyncio
import os
import sqlite3
import sys
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from tui import (
    PipelineApp,
    find_latest_log_file,
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
        assert app.query_one("#single-task") is not None
        assert app.query_one("#scraping-progress") is not None
        assert app.query_one("#live-logs") is not None


def test_get_active_stock_count_empty(tmp_path):
    db_file = tmp_path / "test_empty.db"
    count = get_active_stock_count(str(db_file))
    assert count == 0

def test_get_active_stock_count_with_table(tmp_path):
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
    pid_file = tmp_path / "daemon.pid"
    current_pid = os.getpid()
    pid_file.write_text(str(current_pid))

    mock_res = MagicMock()
    mock_res.stdout = "python daily_pipeline.py --task update_bars"
    with patch("subprocess.run", return_value=mock_res):
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
        assert app.check_action("stop_pipeline", ()) is True
        assert app.check_action("start_daemon", ()) is True
        assert app.check_action("stop_daemon", ()) is True
        assert app.check_action("run_health", ()) is True


@pytest.mark.asyncio
async def test_logs_widget_supports_selection():
    from tui import LogsWidget, PipelineApp
    app = PipelineApp()
    async with app.run_test():
        logs_widget = app.query_one("#live-logs", LogsWidget)
        assert logs_widget.ALLOW_SELECT is True


@pytest.mark.asyncio
async def test_run_in_background_tracks_current_process():
    app = PipelineApp()
    mock_proc = MagicMock()
    mock_proc.wait = MagicMock(return_value=asyncio.Future())
    mock_proc.wait.return_value.set_result(0)
    mock_proc.returncode = 0

    with patch("asyncio.create_subprocess_exec", return_value=mock_proc) as mock_exec:
        await app._run_in_background("arg1", "arg2")
        assert app._current_process is None
        mock_exec.assert_called_once()
        args, kwargs = mock_exec.call_args
        assert args == ("arg1", "arg2")
        assert "env" in kwargs


@pytest.mark.asyncio
async def test_stop_current_process_terminates_running_process():
    app = PipelineApp()
    mock_proc = MagicMock()
    mock_proc.pid = 99999  # fake PID, triggers ProcessLookupError → fallback to terminate()
    wait_future = asyncio.Future()
    wait_future.set_result(0)
    mock_proc.wait = MagicMock(return_value=wait_future)
    mock_proc.returncode = None
    app._current_process = mock_proc

    await app._stop_current_process()

    mock_proc.terminate.assert_called_once()
    mock_proc.wait.assert_called_once()
    assert app._current_process is None


@pytest.mark.asyncio
async def test_stop_current_process_kills_on_timeout():
    app = PipelineApp()
    mock_proc = MagicMock()
    mock_proc.pid = 99999
    pending = asyncio.Future()
    done = asyncio.Future()
    done.set_result(0)
    mock_proc.wait = MagicMock(side_effect=[pending, done])
    mock_proc.returncode = None
    app._current_process = mock_proc

    await app._stop_current_process()

    mock_proc.terminate.assert_called_once()
    mock_proc.kill.assert_called_once()
    assert app._current_process is None


@pytest.mark.asyncio
async def test_action_stop_pipeline_no_process():
    app = PipelineApp()
    mock_logger = MagicMock()
    with patch("logging.getLogger", return_value=mock_logger):
        await app.action_stop_pipeline()
        mock_logger.info.assert_called_once_with("没有正在运行的任务可停止")


@pytest.mark.asyncio
async def test_action_stop_pipeline_stops_process():
    app = PipelineApp()
    mock_proc = MagicMock()
    mock_proc.pid = 99999
    wait_future = asyncio.Future()
    wait_future.set_result(0)
    mock_proc.wait = MagicMock(return_value=wait_future)
    mock_proc.returncode = None
    app._current_process = mock_proc
    mock_logger = MagicMock()

    with patch("logging.getLogger", return_value=mock_logger):
        await app.action_stop_pipeline()
        mock_proc.terminate.assert_called_once()
        mock_logger.info.assert_called_with("已停止进程: %s", [99999])


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
    expected_daemon_path = str(Path(sys.modules["tui"].__file__).parent / "scripts" / "daemon.py")

    with patch.object(app, "_run_in_background", new_callable=MagicMock) as mock_run_bg, \
         patch("asyncio.create_task") as mock_create_task, \
         patch.object(app, "push_screen") as mock_push_screen:

        await app.action_run_pipeline()
        assert mock_push_screen.call_count == 1
        _screen, callback = mock_push_screen.call_args[0]
        callback("run-now")
        mock_create_task.assert_called_once()
        mock_run_bg.assert_called_once_with(
            sys.executable, expected_pipeline_path, "--task", "all", "--force"
        )

        mock_run_bg.reset_mock()
        mock_create_task.reset_mock()
        mock_push_screen.reset_mock()
        await app.action_resume_pipeline()
        assert mock_push_screen.call_count == 1
        _screen, callback = mock_push_screen.call_args[0]
        callback("run-now")
        mock_create_task.assert_called_once()
        mock_run_bg.assert_called_once_with(
            sys.executable, expected_pipeline_path, "--task", "update_bars", "--resume", "--force"
        )

        mock_run_bg.reset_mock()
        mock_create_task.reset_mock()
        await app.action_start_daemon()
        mock_create_task.assert_called_once()
        mock_run_bg.assert_called_once_with(
            sys.executable, expected_daemon_path, "start", "--resume"
        )

        mock_run_bg.reset_mock()
        mock_create_task.reset_mock()
        await app.action_stop_daemon()
        mock_create_task.assert_called_once()
        mock_run_bg.assert_called_once_with(
            sys.executable, expected_daemon_path, "stop"
        )

        mock_run_bg.reset_mock()
        mock_create_task.reset_mock()
        mock_push_screen.reset_mock()
        await app.action_run_health()
        assert mock_push_screen.call_count == 1
        _screen, callback = mock_push_screen.call_args[0]
        callback("run-now")
        mock_create_task.assert_called_once()
        mock_run_bg.assert_called_once_with(
            sys.executable, expected_pipeline_path, "--task", "health_check", "--force"
        )


def test_format_chinese_magnitude():
    from tui import format_chinese_magnitude
    assert format_chinese_magnitude(123) == "123"
    assert format_chinese_magnitude(12_345) == "1.2万"
    assert format_chinese_magnitude(123_456_789) == "1.23亿"


def test_get_expected_latest_trading_day_is_weekday():
    from tui import _get_expected_latest_trading_day
    result = _get_expected_latest_trading_day()
    from datetime import datetime
    dt = datetime.strptime(result, "%Y-%m-%d")
    assert dt.weekday() < 5


def test_date_status():
    from tui import _date_status
    assert _date_status("2026-07-07", "2026-07-07") == ("[green]●[/green]", "最新")
    assert _date_status(None, "2026-07-07") == ("[red]●[/red]", "无数据")
    assert _date_status("2026-07-06", "2026-07-07") == ("[yellow]●[/yellow]", "略滞后")
    assert _date_status("2026-07-01", "2026-07-07") == ("[red]●[/red]", "滞后")


def test_normalize_date():
    from tui import _normalize_date
    assert _normalize_date(None) is None
    assert _normalize_date("2026-07-07") == "2026-07-07"
    assert _normalize_date("20260630") == "2026-06-30"
    assert _normalize_date("20260331") == "2026-03-31"
    assert _normalize_date("not-a-date") == "not-a-date"


def test_get_daily_bars_coverage(tmp_path):
    from tui import get_daily_bars_coverage
    db_file = tmp_path / "test.db"
    conn = sqlite3.connect(db_file)
    conn.execute("CREATE TABLE daily_bars (ts_code TEXT, trade_date TEXT)")
    conn.execute("INSERT INTO daily_bars VALUES ('000001.SZ', '2026-07-09')")
    conn.execute("INSERT INTO daily_bars VALUES ('600000.SH', '2026-07-08')")
    conn.execute("INSERT INTO daily_bars VALUES ('000002.SZ', '2026-07-01')")
    conn.commit()
    conn.close()

    up_to_date, total = get_daily_bars_coverage(str(db_file), "2026-07-10")
    assert total == 3
    # 2026-07-09 >= 2026-07-08 (expect -2) → up to date
    # 2026-07-08 >= 2026-07-08 → up to date
    # 2026-07-01 <  2026-07-08 → lagging
    assert up_to_date == 2

    # non-existent DB
    assert get_daily_bars_coverage("/nonexistent/test.db", "2026-07-10") == (0, 0)


def test_seconds_until_safe():
    from datetime import datetime

    from tui import _seconds_until_safe
    before = datetime(2026, 7, 10, 14, 59, 0)
    with patch("tui.datetime") as m:
        m.now.return_value = before
        m.side_effect = lambda *a, **kw: datetime(*a, **kw)
        assert _seconds_until_safe() == 3660


@pytest.mark.asyncio
async def test_confirm_run_screen_dismiss():
    from unittest.mock import MagicMock

    from textual.widgets import Button

    from tui import ConfirmRunScreen
    screen = ConfirmRunScreen("全量更新")
    for btn_id in ("run-now", "run-later", "cancel"):
        mock_dismiss = MagicMock()
        screen.dismiss = mock_dismiss
        # Simulate the button press like Textual does
        btn = Button(id=btn_id)
        screen.on_button_pressed(Button.Pressed(btn))
        mock_dismiss.assert_called_once_with(btn_id)


def test_get_latest_dates(tmp_path):
    from tui import get_latest_dates
    db_file = tmp_path / "test.db"
    conn = sqlite3.connect(db_file)
    conn.execute("CREATE TABLE daily_bars (trade_date TEXT)")
    conn.execute("CREATE TABLE indicators (trade_date TEXT)")
    conn.execute("INSERT INTO daily_bars (trade_date) VALUES ('2026-07-07')")
    conn.execute("INSERT INTO indicators (trade_date) VALUES ('2026-07-06')")
    conn.commit()
    conn.close()

    result = get_latest_dates(str(db_file))
    assert result.get("daily_bars") == "2026-07-07"
    assert result.get("indicators") == "2026-07-06"


@pytest.mark.asyncio
async def test_data_completeness_shows_freshness_and_dates():
    from tui import DataCompletenessWidget, PipelineApp
    app = PipelineApp()
    async with app.run_test():
        widget = app.query_one("#data-completeness", DataCompletenessWidget)
        widget._counts = {
            "stock_list": 5000,
            "daily_bars": 7000000,
            "indicators": 6500000,
            "fundamentals": 5000,
        }
        widget._latest_dates = {
            "daily_bars": "2026-07-09",
            "indicators": "2026-07-09",
            "fundamentals": "2026-07-08",
        }
        captured = []
        with patch.object(widget._content, "update", side_effect=captured.append):
            widget._rebuild_content()
        text = "\n".join(captured)
        assert "期望最新日期" in text
        assert "[green]●[/green]" in text or "[yellow]●[/yellow]" in text
        assert "2026-07-09" in text


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


def test_find_latest_log_file(tmp_path):
    # Create mock log files
    log1 = tmp_path / "smartmoney_20260704.log"
    log1.touch()
    os.utime(log1, (time.time() - 100, time.time() - 100))

    log2 = tmp_path / "smartmoney_20260705.log"
    log2.touch()

    latest = find_latest_log_file(str(tmp_path))
    assert Path(latest).name == "smartmoney_20260705.log"

    # Clean up and test fallback to daemon.log
    log1.unlink()
    log2.unlink()
    daemon_log = tmp_path / "daemon.log"
    daemon_log.touch()

    latest_fallback = find_latest_log_file(str(tmp_path))
    assert Path(latest_fallback).name == "daemon.log"

    # Test when no logs exist
    daemon_log.unlink()
    assert find_latest_log_file(str(tmp_path)) is None
