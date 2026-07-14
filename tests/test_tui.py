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
        btn = Button(id=btn_id)
        screen.on_button_pressed(Button.Pressed(btn))
        mock_dismiss.assert_called_once_with(btn_id)


def test_seconds_until_safe_after_sixteen():
    from datetime import datetime, timedelta

    from tui import _seconds_until_safe
    after = datetime(2026, 7, 10, 17, 30, 0)
    with patch("tui.datetime") as m:
        m.now.return_value = after
        m.side_effect = lambda *a, **kw: datetime(*a, **kw)
        result = _seconds_until_safe()
        target = after.replace(hour=16, minute=0, second=0, microsecond=0) + timedelta(days=1)
        expected = int((target - after).total_seconds())
        assert result == expected


@pytest.mark.asyncio
async def test_run_or_schedule_run_later():
    from tui import PipelineApp
    app = PipelineApp()
    async with app.run_test():
        args = ("python", "test_script.py")
        with patch.object(app, "push_screen") as mock_push_screen:
            app._run_or_schedule("测试任务", *args)
            assert mock_push_screen.call_count == 1
            _, callback = mock_push_screen.call_args[0]
            with patch.object(app, "_background_tasks", new_callable=set), \
                 patch("tui._seconds_until_safe", return_value=1), \
                 patch("asyncio.create_task") as mock_create_task:
                callback("run-later")
                mock_create_task.assert_called_once()


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
            "chip_distribution": 1000000,
            "chip_distribution_em": 2000,
        }
        widget._latest_dates = {
            "daily_bars": "2026-07-09",
            "indicators": "2026-07-09",
            "fundamentals": "2026-07-08",
            "chip_distribution": "2026-07-09",
            "chip_distribution_em": "2026-07-09",
        }
        captured = []
        with patch.object(widget._content, "update", side_effect=captured.append):
            widget._rebuild_content()
        text = "\n".join(captured)
        assert "期望最新日期" in text
        assert "2026-07-09" in text
        assert "daily_bars" in text or "Daily Bars" in text


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


# ===========================================================================
# _code_to_ts_code
# ===========================================================================
def test_code_to_ts_code_sh():
    from tui import _code_to_ts_code
    assert _code_to_ts_code("600000") == "600000.SH"
    assert _code_to_ts_code("900001") == "900001.SH"


def test_code_to_ts_code_sz():
    from tui import _code_to_ts_code
    assert _code_to_ts_code("000001") == "000001.SZ"
    assert _code_to_ts_code("002000") == "002000.SZ"
    assert _code_to_ts_code("300750") == "300750.SZ"


def test_code_to_ts_code_bj():
    from tui import _code_to_ts_code
    assert _code_to_ts_code("430001") == "430001.BJ"
    assert _code_to_ts_code("830001") == "830001.BJ"
    assert _code_to_ts_code("920001") == "920001.BJ"


def test_code_to_ts_code_invalid():
    from tui import _code_to_ts_code
    assert _code_to_ts_code("abc") is None
    assert _code_to_ts_code("") is None
    assert _code_to_ts_code("   ") is None


# ===========================================================================
# sync_watchlists_from_files
# ===========================================================================
def test_sync_watchlists_no_dir(tmp_path, monkeypatch):
    from tui import sync_watchlists_from_files
    monkeypatch.setattr("tui.WATCHLIST_DIR", tmp_path / "nonexistent")
    added, files = sync_watchlists_from_files(":memory:")
    assert added == 0
    assert files == 0


def test_sync_watchlists_empty_dir(tmp_path, monkeypatch):
    from tui import sync_watchlists_from_files
    watch_dir = tmp_path / "watchlists"
    watch_dir.mkdir()
    monkeypatch.setattr("tui.WATCHLIST_DIR", watch_dir)
    added, files = sync_watchlists_from_files(":memory:")
    assert added == 0
    assert files == 0


def test_sync_watchlists_with_file(tmp_path, monkeypatch):
    from tui import sync_watchlists_from_files
    watch_dir = tmp_path / "watchlists"
    watch_dir.mkdir()
    (watch_dir / "my_watchlist.txt").write_text("600000  # 浦发银行\n000001 # 平安银行\n")
    db_path = str(tmp_path / "test.db")
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE watchlist (ts_code TEXT, added_date TEXT, source_scan TEXT, status TEXT)")
    conn.commit()
    conn.close()
    monkeypatch.setattr("tui.WATCHLIST_DIR", watch_dir)
    added, files = sync_watchlists_from_files(db_path)
    assert added == 2
    assert files == 1


def test_sync_watchlists_with_comments(tmp_path, monkeypatch):
    from tui import sync_watchlists_from_files
    watch_dir = tmp_path / "watchlists"
    watch_dir.mkdir()
    (watch_dir / "watch.txt").write_text("# Header comment\n300750  # 宁德时代\n")
    db_path = str(tmp_path / "test.db")
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE watchlist (ts_code TEXT, added_date TEXT, source_scan TEXT, status TEXT)")
    conn.commit()
    conn.close()
    monkeypatch.setattr("tui.WATCHLIST_DIR", watch_dir)
    added, _ = sync_watchlists_from_files(db_path)
    assert added == 1


# ===========================================================================
# find_running_pipeline_processes
# ===========================================================================
def test_find_running_pipeline_no_processes():
    from tui import find_running_pipeline_processes
    with patch("tui.Path.exists", return_value=False), \
         patch("subprocess.run") as mock_run:
        mock_run.return_value.stdout = ""
        procs = find_running_pipeline_processes()
    assert procs == []


def test_find_running_pipeline_from_pidfile():
    from tui import find_running_pipeline_processes
    with patch("tui.Path.exists", return_value=True), \
         patch("tui.Path.read_text", return_value="12345\n"), \
         patch("os.kill", return_value=None), \
         patch("subprocess.run") as mock_run:
        mock_run.side_effect = [
            MagicMock(stdout="12345  01:23:45 python daily_pipeline.py --task all"),
            MagicMock(stdout=""),
        ]
        procs = find_running_pipeline_processes()
    assert len(procs) == 1
    assert procs[0]["pid"] == 12345


# ===========================================================================
# get_all_table_counts fast mode
# ===========================================================================
def test_get_all_table_counts_fast(tmp_path):
    from tui import get_all_table_counts
    db_file = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_file))
    conn.execute("CREATE TABLE daily_bars (ts_code TEXT, trade_date TEXT)")
    conn.commit()
    conn.close()
    result = get_all_table_counts(str(db_file), fast=True)
    assert "_estimated_total" in result
    assert "_db_bytes" in result


def test_get_all_table_counts_no_db():
    from tui import get_all_table_counts
    assert get_all_table_counts("/nonexistent/path.db") == {}


# ===========================================================================
# PipelineApp: action handlers
# ===========================================================================
@pytest.mark.asyncio
async def test_action_run_reconcile_new():
    app = PipelineApp()
    with patch.object(app, "_create_background_task") as mock_bg, \
         patch.object(app, "notify") as mock_notify:
        await app.action_run_reconcile()
        mock_bg.assert_called_once()
        mock_notify.assert_called()


@pytest.mark.asyncio
async def test_on_mount_no_processes_new():
    app = PipelineApp()
    with patch("tui.find_running_pipeline_processes", return_value=[]), \
         patch.object(app, "_create_background_task") as mock_bg:
        await app.on_mount()
        # on_mount returns early when no processes → _sync_watchlists not called
        mock_bg.assert_not_called()


@pytest.mark.asyncio
async def test_action_stop_pipeline_and_daemon():
    app = PipelineApp()
    mock_proc = MagicMock()
    mock_proc.pid = 99999
    mock_proc.returncode = None
    wait_future = asyncio.Future()
    wait_future.set_result(0)
    mock_proc.wait = MagicMock(return_value=wait_future)
    app._current_process = mock_proc
    with patch("tui.find_running_pipeline_processes", return_value=[]), \
         patch("tui.get_daemon_status", return_value=("Running", 88888)), \
         patch.object(app, "_create_background_task") as mock_bg, \
         patch("logging.getLogger", return_value=MagicMock()):
        await app.action_stop_pipeline()
        mock_bg.assert_called()


@pytest.mark.asyncio
async def test_stop_daemon_process_success_new():
    app = PipelineApp()
    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.communicate = MagicMock(return_value=(b"daemon stopped", b""))
    with (
        patch("asyncio.create_subprocess_exec", return_value=mock_proc) as mock_exec,
        patch("logging.getLogger", return_value=MagicMock()),
    ):
        await app._stop_daemon_process()
        mock_exec.assert_called_once()


@pytest.mark.asyncio
async def test_stop_daemon_process_timeout_new():
    app = PipelineApp()
    mock_proc = MagicMock()
    mock_proc.communicate = MagicMock(side_effect=asyncio.TimeoutError)
    with (
        patch("asyncio.create_subprocess_exec", return_value=mock_proc),
        patch("logging.getLogger", return_value=MagicMock()),
    ):
        await app._stop_daemon_process()


@pytest.mark.asyncio
async def test_action_copy_panel_new():
    app = PipelineApp()
    async with app.run_test():
        with patch.object(app, "copy_to_clipboard") as mock_copy, \
             patch.object(app, "notify") as mock_notify:
            app._copy_panel_index = 0
            app.action_copy_panel()
            assert mock_notify.called or mock_copy.called


@pytest.mark.asyncio
async def test_action_run_single_task_new():
    app = PipelineApp()
    with patch.object(app, "_run_or_schedule") as mock_run:
        await app.action_run_single_task("update_bars")
        mock_run.assert_called_once()


# ===========================================================================
# DataCompletenessWidget: _get_updating_table
# ===========================================================================
def test_get_updating_table_active():
    import time

    from tui import DataCompletenessWidget
    data = {"task": "update_bars", "processed": 10}
    with patch("os.path.getmtime", return_value=time.time() - 30), \
         patch("tui.parse_progress", return_value=data):
        result = DataCompletenessWidget._get_updating_table()
        assert result == "daily_bars"


def test_get_updating_table_stale():
    import time

    from tui import DataCompletenessWidget
    with patch("os.path.getmtime", return_value=time.time() - 120):
        result = DataCompletenessWidget._get_updating_table()
        assert result is None


def test_get_updating_table_no_file():
    from tui import DataCompletenessWidget
    with patch("os.path.getmtime", side_effect=OSError):
        result = DataCompletenessWidget._get_updating_table()
        assert result is None


# ===========================================================================
# PipelineApp on_mount: yes/no dialog branches
# ===========================================================================
@pytest.mark.asyncio
async def test_on_mount_with_processes_yes_stop():
    """on_mount with processes → user presses Y → kills processes + syncs watchlists."""
    app = PipelineApp()
    fake_procs = [{"pid": 12345, "elapsed": "01:00", "command": "daily_pipeline.py"}]

    async def fake_push_screen(*args, **kwargs):
        return True

    with patch("tui.find_running_pipeline_processes", return_value=fake_procs), \
         patch.object(app, "push_screen_wait", side_effect=fake_push_screen), \
         patch("os.kill") as mock_kill, \
         patch.object(app, "_create_background_task") as mock_bg, \
         patch.object(app, "notify"):
        await app.on_mount()
        mock_kill.assert_called_once()  # SIGTERM
        mock_bg.assert_called_once()  # watchlist sync


@pytest.mark.asyncio
async def test_on_mount_with_processes_no_keep():
    """on_mount with processes → user presses N → keeps processes running + syncs watchlists."""
    app = PipelineApp()
    fake_procs = [{"pid": 12345, "elapsed": "01:00", "command": "daily_pipeline.py"}]

    async def fake_push_screen(*args, **kwargs):
        return False

    with patch("tui.find_running_pipeline_processes", return_value=fake_procs), \
         patch.object(app, "push_screen_wait", side_effect=fake_push_screen), \
         patch("os.kill") as mock_kill, \
         patch.object(app, "_create_background_task") as mock_bg, \
         patch.object(app, "notify") as mock_notify:
        await app.on_mount()
        mock_kill.assert_not_called()
        mock_notify.assert_called_with(
            "Background processes kept running",
            severity="information", timeout=3.0,
        )
        mock_bg.assert_called_once()  # still syncs watchlists


@pytest.mark.asyncio
async def test_sync_watchlists_with_files_notify():
    """_sync_watchlists with files > 0 calls notify."""
    app = PipelineApp()
    with patch("tui.sync_watchlists_from_files", return_value=(5, 3)), \
         patch.object(app, "notify") as mock_notify:
        await app._sync_watchlists()
        mock_notify.assert_called_once()
        args = mock_notify.call_args[0]
        assert "新增 5" in str(args)


@pytest.mark.asyncio
async def test_sync_watchlists_no_files_no_notify():
    """_sync_watchlists with files=0 does NOT call notify."""
    app = PipelineApp()
    with patch("tui.sync_watchlists_from_files", return_value=(0, 0)), \
         patch.object(app, "notify") as mock_notify:
        await app._sync_watchlists()
        mock_notify.assert_not_called()


@pytest.mark.asyncio
async def test_sync_watchlists_exception_suppressed():
    """_sync_watchlists exceptions are suppressed."""
    app = PipelineApp()
    with patch("tui.sync_watchlists_from_files", side_effect=RuntimeError("boom")):
        await app._sync_watchlists()  # should not raise


# ===========================================================================
# ConfirmStopScreen: on_key "n"
# ===========================================================================
def test_confirm_stop_screen_on_key_n():
    from unittest.mock import MagicMock

    from tui import ConfirmStopScreen
    screen = ConfirmStopScreen([{"pid": 1, "elapsed": "00:01", "command": "test"}])
    screen.dismiss = MagicMock()
    event = MagicMock()
    event.key = "n"
    screen.on_key(event)
    screen.dismiss.assert_called_once_with(False)


def test_confirm_stop_screen_on_key_y():
    from unittest.mock import MagicMock

    from tui import ConfirmStopScreen
    screen = ConfirmStopScreen([{"pid": 1, "elapsed": "00:01", "command": "test"}])
    screen.dismiss = MagicMock()
    event = MagicMock()
    event.key = "Y"
    screen.on_key(event)
    screen.dismiss.assert_called_once_with(True)


def test_confirm_stop_screen_other_key():
    from unittest.mock import MagicMock

    from tui import ConfirmStopScreen
    screen = ConfirmStopScreen([{"pid": 1, "elapsed": "00:01", "command": "test"}])
    screen.dismiss = MagicMock()
    event = MagicMock()
    event.key = "x"
    screen.on_key(event)
    screen.dismiss.assert_not_called()


# ===========================================================================
# _run_or_schedule: cancel + run-later
# ===========================================================================
@pytest.mark.asyncio
async def test_run_or_schedule_cancel():
    """_run_or_schedule with cancel callback → no background task created."""
    app = PipelineApp()
    with patch.object(app, "push_screen") as mock_push, \
         patch.object(app, "_create_background_task") as mock_bg, \
         patch.object(app, "notify") as mock_notify:
        app._run_or_schedule("测试", "python", "test.py")
        _, callback = mock_push.call_args[0]
        callback("cancel")
        mock_bg.assert_not_called()
        mock_notify.assert_not_called()


@pytest.mark.asyncio
async def test_run_or_schedule_none():
    """_run_or_schedule with None callback → no background task created."""
    app = PipelineApp()
    with patch.object(app, "push_screen") as mock_push, \
         patch.object(app, "_create_background_task") as mock_bg:
        app._run_or_schedule("测试", "python", "test.py")
        _, callback = mock_push.call_args[0]
        callback(None)
        mock_bg.assert_not_called()


# ===========================================================================
# LogsWidget: colorize_line all branches
# ===========================================================================
def test_colorize_line_error():
    from tui import LogsWidget
    w = LogsWidget()
    result = w.colorize_line("2026-07-12 | ERROR | Something broke")
    assert "[red]❌" in result
    assert "Something broke" in result


def test_colorize_line_warn():
    from tui import LogsWidget
    w = LogsWidget()
    result = w.colorize_line("2026-07-12 | WARN | Connection slow")
    assert "[yellow]⚠️" in result


def test_colorize_line_warning():
    from tui import LogsWidget
    w = LogsWidget()
    result = w.colorize_line("2026-07-12 | WARNING | Disk full")
    assert "[yellow]⚠️" in result


def test_colorize_line_success():
    from tui import LogsWidget
    w = LogsWidget()
    result = w.colorize_line("2026-07-12 | SUCCESS | Done")
    assert "[bold green]✅" in result


def test_colorize_line_info():
    from tui import LogsWidget
    w = LogsWidget()
    result = w.colorize_line("2026-07-12 | INFO | Processing")
    assert "[#e2e8f0]" in result


def test_colorize_line_unknown_level():
    from tui import LogsWidget
    w = LogsWidget()
    result = w.colorize_line("2026-07-12 | DEBUG | Debug msg")
    assert "Debug msg" in result
    assert "❌" not in result
    assert "⚠️" not in result


def test_colorize_line_equals_in_body():
    from tui import LogsWidget
    w = LogsWidget()
    result = w.colorize_line("2026-07-12 | INFO | x=y=z")
    assert "x-y-z" in result
    assert "=" not in result


def test_colorize_line_no_pipe():
    from tui import LogsWidget
    w = LogsWidget()
    result = w.colorize_line("plain text with = signs")
    assert "plain text with - signs" in result


def test_colorize_line_empty():
    from tui import LogsWidget
    w = LogsWidget()
    result = w.colorize_line("")
    assert result == ""


# ===========================================================================
# LogsWidget: tail_log error + no log
# ===========================================================================
def test_tail_log_no_latest():
    """tail_log when find_latest_log_file returns None."""
    from tui import LogsWidget
    w = LogsWidget()
    with patch("tui.find_latest_log_file", return_value=None), \
         patch.object(w, "write") as mock_write:
        w.tail_log()
        mock_write.assert_not_called()


def test_tail_log_error():
    """tail_log when an exception occurs."""
    from tui import LogsWidget
    w = LogsWidget()
    with patch("tui.find_latest_log_file", side_effect=OSError("permission denied")), \
         patch.object(w, "write") as mock_write:
        w.tail_log()
        mock_write.assert_called_once()
        assert "Error tailing log" in mock_write.call_args[0][0]


def test_tail_log_switch_log_file(tmp_path):
    """tail_log when log file changes → closes old handle, opens new."""
    from tui import LogsWidget
    w = LogsWidget()
    log1 = tmp_path / "log1.log"
    log1.write_text("line 1\n")
    log2 = tmp_path / "log2.log"
    log2.write_text("line 2\n")

    old_fh = MagicMock()
    w.file_handle = old_fh
    w.active_log = str(log1)

    with patch("tui.find_latest_log_file", return_value=str(log2)), \
         patch.object(w, "write") as mock_write:
        w.tail_log()
        old_fh.close.assert_called_once()
        assert w.active_log == str(log2)
        # Should have written the "Bound to log" message
        assert any("Bound to log" in str(call) for call in mock_write.call_args_list)


# ===========================================================================
# LogsWidget: copy_recent_logs
# ===========================================================================
def test_copy_recent_logs_from_active_file(tmp_path):
    """copy_recent_logs reads from active log file."""
    from tui import LogsWidget
    w = LogsWidget()
    log_file = tmp_path / "test.log"
    log_file.write_text("line1\nline2\nline3\n")
    w.active_log = str(log_file)
    result = w.copy_recent_logs(line_count=2)
    assert "line2" in result
    assert "line3" in result
    assert "line1" not in result


def test_copy_recent_logs_fallback_to_lines():
    """copy_recent_logs falls back to RichLog lines when no active file."""
    from tui import LogsWidget
    w = LogsWidget()
    w.active_log = None
    w.lines = ["rendered line 1", "rendered line 2"]
    result = w.copy_recent_logs(line_count=1)
    assert "rendered line 2" in result


def test_copy_recent_logs_empty_fallback():
    """copy_recent_logs returns empty string when all fallbacks fail."""
    from tui import LogsWidget
    w = LogsWidget()
    w.active_log = "/nonexistent/path.log"
    w.lines = []
    result = w.copy_recent_logs()
    assert result == ""


# ===========================================================================
# LogsWidget: on_unmount
# ===========================================================================
def test_logs_widget_on_unmount_closes_handle():
    """on_unmount closes file handle if open."""
    from tui import LogsWidget
    w = LogsWidget()
    mock_fh = MagicMock()
    w.file_handle = mock_fh
    w.on_unmount()
    mock_fh.close.assert_called_once()
    assert w.file_handle is None


def test_logs_widget_on_unmount_no_handle():
    """on_unmount does nothing if no file handle."""
    from tui import LogsWidget
    w = LogsWidget()
    w.file_handle = None
    w.on_unmount()  # should not raise


# ===========================================================================
# format_count: all branches
# ===========================================================================
def test_format_count_millions():
    from tui import format_count
    assert format_count(7_500_000) == "7.50M"


def test_format_count_thousands():
    from tui import format_count
    assert format_count(5_500) == "5.5K"


def test_format_count_small():
    from tui import format_count
    assert format_count(42) == "42"


# ===========================================================================
# get_db_size: GB branch
# ===========================================================================
def test_get_db_size_gb(tmp_path):
    from tui import get_db_size
    db_file = tmp_path / "big.db"
    # Write just over 1GB of bytes
    db_file.write_bytes(b"\x00" * (1024 * 1024 * 1024 + 1024 * 1024))
    size_str = get_db_size(str(db_file))
    assert "GB" in size_str


# ===========================================================================
# DataCompletenessWidget: _rebuild_content edge cases
# ===========================================================================
def test_rebuild_content_estimated_mode():
    """_rebuild_content in estimated mode (fast counts, all zeros)."""
    from tui import DataCompletenessWidget
    w = DataCompletenessWidget()
    w._content = MagicMock()
    w._counts = {"_estimated_total": 1000, "_db_bytes": 500000}
    for k in DataCompletenessWidget.TABLE_LABELS:
        w._counts[k] = 0
    w._latest_dates = {}
    with patch("tui._get_expected_latest_trading_day", return_value="2026-07-10"), \
         patch("tui.get_db_size", return_value="1.23 MB"):
        w._rebuild_content()
        call_args = w._content.update.call_args[0][0]
        assert "行数加载中" in call_args
        assert "计算中" in call_args


def test_rebuild_content_updating_table():
    """_rebuild_content with an updating table."""
    from tui import DataCompletenessWidget
    w = DataCompletenessWidget()
    w._content = MagicMock()
    w._counts = {"daily_bars": 1000, "indicators": 500, "stock_list": 100}
    w._latest_dates = {"daily_bars": "2026-07-10", "indicators": "2026-07-09"}
    w._daily_coverage = (950, 1000)
    with patch.object(DataCompletenessWidget, "_get_updating_table", return_value="daily_bars"), \
         patch("tui._get_expected_latest_trading_day", return_value="2026-07-10"), \
         patch("tui.get_db_size", return_value="1.00 MB"):
        w._rebuild_content()
        call_args = w._content.update.call_args[0][0]
        assert "更新中" in call_args


def test_rebuild_content_monthly_table():
    """_rebuild_content with institutional_holdings (monthly table)."""
    from tui import DataCompletenessWidget
    w = DataCompletenessWidget()
    w._content = MagicMock()
    w._counts = {"daily_bars": 1000, "institutional_holdings": 5000}
    w._latest_dates = {"daily_bars": "2026-07-10", "institutional_holdings": "2026-06-30"}
    w._daily_coverage = (950, 1000)
    with patch.object(DataCompletenessWidget, "_get_updating_table", return_value=None), \
         patch("tui._get_expected_latest_trading_day", return_value="2026-07-10"), \
         patch("tui.get_db_size", return_value="1.00 MB"):
        w._rebuild_content()
        call_args = w._content.update.call_args[0][0]
        assert "按月更新" in call_args


def test_rebuild_content_quarterly_table():
    """_rebuild_content with shareholder_count (quarterly table)."""
    from tui import DataCompletenessWidget
    w = DataCompletenessWidget()
    w._content = MagicMock()
    w._counts = {"daily_bars": 1000, "shareholder_count": 5000}
    w._latest_dates = {"daily_bars": "2026-07-10", "shareholder_count": "2026-03-31"}
    w._daily_coverage = (950, 1000)
    with patch.object(DataCompletenessWidget, "_get_updating_table", return_value=None), \
         patch("tui._get_expected_latest_trading_day", return_value="2026-07-10"), \
         patch("tui.get_db_size", return_value="1.00 MB"):
        w._rebuild_content()
        call_args = w._content.update.call_args[0][0]
        assert "按季更新" in call_args


def test_rebuild_content_delayed_table():
    """_rebuild_content with fx_rate (delayed publish table)."""
    from tui import DataCompletenessWidget
    w = DataCompletenessWidget()
    w._content = MagicMock()
    w._counts = {"daily_bars": 1000, "fx_rate": 500}
    w._latest_dates = {"daily_bars": "2026-07-10", "fx_rate": "2026-07-08"}
    w._daily_coverage = (950, 1000)
    with patch.object(DataCompletenessWidget, "_get_updating_table", return_value=None), \
         patch("tui._get_expected_latest_trading_day", return_value="2026-07-10"), \
         patch("tui.get_db_size", return_value="1.00 MB"):
        w._rebuild_content()
        call_args = w._content.update.call_args[0][0]
        assert "延迟发布" in call_args


def test_rebuild_content_no_date_table():
    """_rebuild_content with dividend_summary (NO_DATE_TABLES)."""
    from tui import DataCompletenessWidget
    w = DataCompletenessWidget()
    w._content = MagicMock()
    w._counts = {"daily_bars": 1000, "dividend_summary": 500}
    w._latest_dates = {"daily_bars": "2026-07-10"}
    w._daily_coverage = (950, 1000)
    with patch.object(DataCompletenessWidget, "_get_updating_table", return_value=None), \
         patch("tui._get_expected_latest_trading_day", return_value="2026-07-10"), \
         patch("tui.get_db_size", return_value="1.00 MB"):
        w._rebuild_content()
        call_args = w._content.update.call_args[0][0]
        assert "最新" not in call_args.split("Dividends")[1].split("\n")[0]


def test_rebuild_content_stock_list_table():
    """_rebuild_content with stock_list (special '只' suffix)."""
    from tui import DataCompletenessWidget
    w = DataCompletenessWidget()
    w._content = MagicMock()
    w._counts = {"daily_bars": 1000, "stock_list": 5528}
    w._latest_dates = {"daily_bars": "2026-07-10"}
    w._daily_coverage = (950, 1000)
    with patch.object(DataCompletenessWidget, "_get_updating_table", return_value=None), \
         patch("tui._get_expected_latest_trading_day", return_value="2026-07-10"), \
         patch("tui.get_db_size", return_value="1.00 MB"):
        w._rebuild_content()
        call_args = w._content.update.call_args[0][0]
        assert "只" in call_args


def test_rebuild_content_daily_bars_with_bar():
    """_rebuild_content daily_bars with coverage bar."""
    from tui import DataCompletenessWidget
    w = DataCompletenessWidget()
    w._content = MagicMock()
    w._counts = {"daily_bars": 7000000, "indicators": 4000000}
    w._latest_dates = {"daily_bars": "2026-07-10", "indicators": "2026-07-10"}
    w._daily_coverage = (5000, 5528)
    with patch.object(DataCompletenessWidget, "_get_updating_table", return_value=None), \
         patch("tui._get_expected_latest_trading_day", return_value="2026-07-10"), \
         patch("tui.get_db_size", return_value="2.50 GB"):
        w._rebuild_content()
        call_args = w._content.update.call_args[0][0]
        assert "█" in call_args
        assert "%" in call_args
        assert "2.50 GB" in call_args
        assert "700.00万" in call_args or "7.00M" in call_args


def test_rebuild_content_indicators_with_bar():
    """_rebuild_content indicators with percentage bar."""
    from tui import DataCompletenessWidget
    w = DataCompletenessWidget()
    w._content = MagicMock()
    w._counts = {"daily_bars": 1000, "indicators": 750, "stock_list": 0}
    w._latest_dates = {"daily_bars": "2026-07-10", "indicators": "2026-07-10"}
    w._daily_coverage = (950, 1000)
    with patch.object(DataCompletenessWidget, "_get_updating_table", return_value=None), \
         patch("tui._get_expected_latest_trading_day", return_value="2026-07-10"), \
         patch("tui.get_db_size", return_value="1.00 MB"):
        w._rebuild_content()
        call_args = w._content.update.call_args[0][0]
        # indicators should have a bar with %
        indicators_section = call_args.split("Indicators")[1]
        assert "█" in indicators_section
        assert "%" in indicators_section


def test_rebuild_content_default_else_branch():
    """_rebuild_content default else branch for regular tables."""
    from tui import DataCompletenessWidget
    w = DataCompletenessWidget()
    w._content = MagicMock()
    w._counts = {"daily_bars": 1000, "fund_flow": 5000}
    w._latest_dates = {"daily_bars": "2026-07-10", "fund_flow": "2026-07-09"}
    w._daily_coverage = (950, 1000)
    with patch.object(DataCompletenessWidget, "_get_updating_table", return_value=None), \
         patch("tui._get_expected_latest_trading_day", return_value="2026-07-10"), \
         patch("tui.get_db_size", return_value="1.00 MB"):
        w._rebuild_content()
        call_args = w._content.update.call_args[0][0]
        assert "Fund Flow" in call_args
        assert "2026-07-09" in call_args


# ===========================================================================
# DataCompletenessWidget: _mini_bar
# ===========================================================================
def test_mini_bar_zero():
    from tui import DataCompletenessWidget
    bar, pct = DataCompletenessWidget._mini_bar(0)
    assert pct == 0
    assert bar.count("█") == 0
    assert bar.count("░") == 10


def test_mini_bar_fifty():
    from tui import DataCompletenessWidget
    bar, pct = DataCompletenessWidget._mini_bar(50)
    assert pct == 50
    assert bar.count("█") == 5
    assert bar.count("░") == 5


def test_mini_bar_hundred():
    from tui import DataCompletenessWidget
    bar, pct = DataCompletenessWidget._mini_bar(100)
    assert pct == 100
    assert bar.count("█") == 10
    assert bar.count("░") == 0


def test_mini_bar_over_100():
    from tui import DataCompletenessWidget
    bar, pct = DataCompletenessWidget._mini_bar(150)
    assert pct == 100
    assert bar.count("█") == 10


def test_mini_bar_negative():
    from tui import DataCompletenessWidget
    bar, pct = DataCompletenessWidget._mini_bar(-10)
    assert pct == 0
    assert bar.count("█") == 0


def test_mini_bar_33():
    from tui import DataCompletenessWidget
    bar, pct = DataCompletenessWidget._mini_bar(33.3)
    assert pct == 33
    assert bar.count("█") == 3


# ===========================================================================
# DataCompletenessWidget: _rebuild_from_cache
# ===========================================================================
def test_rebuild_from_cache():
    """_rebuild_from_cache just calls _rebuild_content."""
    from tui import DataCompletenessWidget
    w = DataCompletenessWidget()
    w._content = MagicMock()
    with patch.object(w, "_rebuild_content") as mock_rebuild:
        w._rebuild_from_cache()
        mock_rebuild.assert_called_once()


# ===========================================================================
# DataCompletenessWidget: _rebuild_content empty counts
# ===========================================================================
def test_rebuild_content_empty_counts():
    """_rebuild_content with empty counts dict."""
    from tui import DataCompletenessWidget
    w = DataCompletenessWidget()
    w._content = MagicMock()
    w._counts = {}
    w._latest_dates = {}
    w._daily_coverage = (0, 0)
    w._rebuild_content()
    w._content.update.assert_called_once()
    assert "等待数据库连接" in w._content.update.call_args[0][0]


# ===========================================================================
# find_running_pipeline_processes: skip_ppid_check
# ===========================================================================
def test_find_running_pipeline_skip_ppid_check():
    """With skip_ppid_check=True, ppid filter is bypassed."""
    from tui import find_running_pipeline_processes
    with patch("tui.Path.exists", side_effect=[False]), \
         patch("subprocess.run") as mock_run:
        mock_run.side_effect = [
            MagicMock(stdout="12345\n"),
            MagicMock(stdout="12345  01:23:45 python daily_pipeline.py --task all"),
        ]
        procs = find_running_pipeline_processes(skip_ppid_check=True)
    assert len(procs) == 1
    assert procs[0]["pid"] == 12345


def test_find_running_pipeline_pgrep_exception():
    """pgrep fails → returns empty list gracefully."""
    from tui import find_running_pipeline_processes
    with patch("tui.Path.exists", return_value=False), \
         patch("subprocess.run", side_effect=OSError):
        procs = find_running_pipeline_processes()
    assert procs == []


# ===========================================================================
# _stop_current_process: killpg + ProcessLookupError
# ===========================================================================
@pytest.mark.asyncio
async def test_stop_current_process_killpg():
    """_stop_current_process uses os.killpg when available."""
    app = PipelineApp()
    mock_proc = MagicMock()
    mock_proc.pid = 12345
    mock_proc.returncode = None
    wait_future = asyncio.Future()
    wait_future.set_result(0)
    mock_proc.wait = MagicMock(return_value=wait_future)
    app._current_process = mock_proc

    with patch("os.killpg") as mock_killpg, \
         patch("os.getpgid", return_value=12345), \
         patch.object(app, "notify"):
        await app._stop_current_process()
        mock_killpg.assert_called_once_with(12345, 15)
        assert app._current_process is None


@pytest.mark.asyncio
async def test_stop_current_process_already_exited():
    """_stop_current_process when ProcessLookupError is raised on killpg."""
    app = PipelineApp()
    mock_proc = MagicMock()
    mock_proc.pid = 12345
    mock_proc.returncode = None
    wait_future = asyncio.Future()
    wait_future.set_result(0)
    mock_proc.wait = MagicMock(return_value=wait_future)
    app._current_process = mock_proc

    with patch("os.killpg", side_effect=ProcessLookupError), \
         patch("os.getpgid", return_value=12345), \
         patch.object(mock_proc, "terminate") as mock_terminate, \
         patch.object(app, "notify"):
        await app._stop_current_process()
        mock_terminate.assert_called_once()
        assert app._current_process is None


# ===========================================================================
# _stop_daemon_process: non-zero exit
# ===========================================================================
@pytest.mark.asyncio
async def test_stop_daemon_process_nonzero_exit():
    """_stop_daemon_process when daemon.py exits non-zero."""
    app = PipelineApp()
    mock_proc = MagicMock()
    mock_proc.returncode = 1
    mock_proc.communicate = MagicMock(return_value=asyncio.Future())
    mock_proc.communicate.return_value.set_result((b"error output", b""))
    mock_logger = MagicMock()

    with patch("asyncio.create_subprocess_exec", return_value=mock_proc), \
         patch("logging.getLogger", return_value=mock_logger):
        await app._stop_daemon_process()
        mock_logger.warning.assert_called_once()


# ===========================================================================
# _stop_daemon_process: exception
# ===========================================================================
@pytest.mark.asyncio
async def test_stop_daemon_process_exception():
    """_stop_daemon_process when create_subprocess_exec raises."""
    app = PipelineApp()
    mock_logger = MagicMock()

    with patch("asyncio.create_subprocess_exec", side_effect=OSError("exec failed")), \
         patch("logging.getLogger", return_value=mock_logger):
        await app._stop_daemon_process()
        mock_logger.exception.assert_called_once()


# ===========================================================================
# action_copy_panel: cycle through all 4 panels
# ===========================================================================
@pytest.mark.asyncio
async def test_action_copy_panel_cycle_all():
    """action_copy_panel cycles through all 4 panels."""
    app = PipelineApp()
    async with app.run_test():
        logs = app.query_one("#live-logs")
        with patch.object(logs, "copy_recent_logs", return_value="some log text"), \
             patch.object(app, "copy_to_clipboard") as mock_copy, \
             patch.object(app, "notify") as mock_notify:
            # Call 4 times to cycle through all panels
            for i in range(4):
                app._copy_panel_index = i
                app.action_copy_panel()
            assert mock_copy.call_count == 4
            assert mock_notify.call_count == 4


@pytest.mark.asyncio
async def test_action_copy_panel_empty_text_skip():
    """action_copy_panel skips panels with empty text."""
    app = PipelineApp()
    async with app.run_test():
        dc = app.query_one("#data-completeness")
        dc._content = MagicMock()
        # Mock all panels to return empty text
        with patch.object(app, "notify") as mock_notify:
            app._copy_panel_index = 0
            # Force empty content
            dc._content._Static__content = ""
            app.action_copy_panel()
            assert "内容为空" in mock_notify.call_args[0][0]


# ===========================================================================
# action_stop_pipeline: with pgrep processes
# ===========================================================================
@pytest.mark.asyncio
async def test_action_stop_pipeline_with_pgrep_procs():
    """action_stop_pipeline kills processes found via pgrep."""
    app = PipelineApp()
    app._current_process = None
    mock_logger = MagicMock()
    fake_procs = [{"pid": 99999}]

    with patch("tui.find_running_pipeline_processes", return_value=fake_procs), \
         patch("tui.get_daemon_status", return_value=("Stopped", None)), \
         patch("os.kill") as mock_kill, \
         patch("logging.getLogger", return_value=mock_logger), \
         patch.object(app, "notify"):
        await app.action_stop_pipeline()
        mock_kill.assert_called_once_with(99999, 15)
        mock_logger.info.assert_called()


# ===========================================================================
# ProgressWidget: with actual progress data
# ===========================================================================
def test_progress_widget_with_data():
    """ProgressWidget.update_progress with full progress data."""
    from tui import ProgressWidget
    w = ProgressWidget()
    progress_data = {
        "task": "update_bars",
        "processed": 500,
        "total": 1000,
        "last_symbol": "000001.SZ",
        "failed_queue": ["err1", "err2"],
    }
    with patch("tui.parse_progress", return_value=progress_data), \
         patch.object(w, "update") as mock_update:
        w.update_progress()
        mock_update.assert_called_once()
        text = mock_update.call_args[0][0]
        assert "50.0%" in text
        assert "000001.SZ" in text
        assert "2" in text  # failed count


# ===========================================================================
# _get_updating_table: progress parsed, task matched
# ===========================================================================
def test_get_updating_table_matched():
    """_get_updating_table returns table name for mapped task."""
    import time

    from tui import DataCompletenessWidget
    data = {"task": "update_indicators", "processed": 10}
    with patch("os.path.getmtime", return_value=time.time() - 30), \
         patch("tui.parse_progress", return_value=data):
        result = DataCompletenessWidget._get_updating_table()
        assert result == "indicators"


def test_get_updating_table_unmapped_task():
    """_get_updating_table returns None for unmapped task."""
    import time

    from tui import DataCompletenessWidget
    data = {"task": "unknown_task", "processed": 10}
    with patch("os.path.getmtime", return_value=time.time() - 30), \
         patch("tui.parse_progress", return_value=data):
        result = DataCompletenessWidget._get_updating_table()
        assert result is None


# ===========================================================================
# get_latest_dates: with YYYYMMDD normalization
# ===========================================================================
def test_get_latest_dates_normalized(tmp_path):
    """get_latest_dates normalizes YYYYMMDD to YYYY-MM-DD."""
    from tui import get_latest_dates
    db_file = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_file))
    conn.execute("CREATE TABLE margin_trading (trade_date TEXT, ts_code TEXT)")
    conn.execute("INSERT INTO margin_trading VALUES ('20260710', '000001.SZ')")
    conn.commit()
    conn.close()

    result = get_latest_dates(str(db_file))
    assert result.get("margin_trading") == "2026-07-10"


# ===========================================================================
# _date_status: exception path
# ===========================================================================
def test_date_status_invalid_date():
    """_date_status with non-parseable date returns '滞后'."""
    from tui import _date_status
    emoji, status = _date_status("not-a-date", "2026-07-10")
    assert status == "滞后"
    assert "red" in emoji
