import pytest
from unittest.mock import MagicMock, patch

from tui import (
    PipelineApp,
    get_active_stock_count,
    get_daemon_status,
    get_db_size,
    get_launchd_status,
    get_subprocess_env,
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


