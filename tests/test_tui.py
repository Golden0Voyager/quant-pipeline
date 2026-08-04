import asyncio
import os
import sqlite3
import sys
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tui import (
    CopyPanelScreen,
    HelpScreen,
    LogCleanupScreen,
    PipelineApp,
    find_latest_log_file,
    get_active_stock_count,
    get_daemon_status,
    get_db_size,
    get_launchd_status,
    get_subprocess_env,
    load_theme,
    parse_progress,
    save_theme,
)

_PROJECT_ROOT = Path(__file__).resolve().parent.parent


class TestActionRunCatchUp:
    def _sh(self, y, m, d, hh, mm=0):
        from datetime import datetime as dt
        from zoneinfo import ZoneInfo

        return dt(y, m, d, hh, mm, tzinfo=ZoneInfo("Asia/Shanghai"))

    @pytest.mark.asyncio
    async def test_refused_during_session_on_trading_day(self):
        app = PipelineApp()
        with patch("core.market_time.shanghai_now",
                   return_value=self._sh(2026, 7, 31, 10, 30)), \
             patch("core.calendar.is_trading_day", return_value=True), \
             patch.object(app, "_run_task_group") as run_group, \
             patch.object(app, "notify") as notify:
            await app.action_run_catch_up()
        run_group.assert_not_called()
        assert "盘中" in notify.call_args.args[0]

    @pytest.mark.asyncio
    async def test_post_close_runs_computed_tasks(self):
        app = PipelineApp()
        with patch("core.market_time.shanghai_now",
                   return_value=self._sh(2026, 7, 31, 17, 0)), \
             patch("core.calendar.is_trading_day", return_value=True), \
             patch("tui.get_expected_latest_trading_day", return_value="2026-07-31"), \
             patch("tui.get_latest_dates", return_value={}), \
             patch("tui.compute_catch_up_tasks",
                   return_value=["update_bars", "update_fund_flow"]) as compute, \
             patch.object(app, "_create_background_task") as bg, \
             patch.object(app, "_run_task_group",
                          side_effect=_close_coro) as run_group, \
             patch.object(app, "notify"):
            await app.action_run_catch_up()
        compute.assert_called_once_with({}, "2026-07-31")
        run_group.assert_called_once_with("补齐缺失", ["update_bars", "update_fund_flow"])
        bg.assert_called_once()

    @pytest.mark.asyncio
    async def test_nothing_stale_notifies_and_skips(self):
        app = PipelineApp()
        with patch("core.market_time.shanghai_now",
                   return_value=self._sh(2026, 8, 1, 10, 0)), \
             patch("core.calendar.is_trading_day", return_value=False), \
             patch("tui.get_expected_latest_trading_day", return_value="2026-07-31"), \
             patch("tui.get_latest_dates", return_value={}), \
             patch("tui.compute_catch_up_tasks", return_value=[]), \
             patch.object(app, "_run_task_group") as run_group, \
             patch.object(app, "notify") as notify:
            await app.action_run_catch_up()
        run_group.assert_not_called()
        assert "无缺失" in notify.call_args.args[0]


def _close_coro(coro, **_kwargs):
    """Close a coroutine captured by a mocked task scheduler."""
    if asyncio.iscoroutine(coro):
        coro.close()
    return MagicMock()


@pytest.mark.asyncio
@patch("tui.find_running_pipeline_processes", return_value=[])
async def test_app_title(mock_find):
    app = PipelineApp()
    async with app.run_test():
        assert app.title == "SmartMoney Pipeline Manager"

@pytest.mark.asyncio
async def test_widgets_present():
    from textual.widgets import Footer
    app = PipelineApp()
    async with app.run_test():
        assert app.query_one("#status-dashboard") is not None
        assert app.query_one("#single-task") is not None
        assert app.query_one("#scraping-progress") is not None
        assert app.query_one("#live-logs") is not None
        assert app.query_one(Footer) is not None


@pytest.mark.asyncio
async def test_dashboard_widget_caches_stock_count():
    from tui import DashboardWidget
    app = PipelineApp()
    async with app.run_test():
        widget = app.query_one("#status-dashboard", DashboardWidget)
        with patch("tui.get_active_stock_count", return_value=1234) as mock_count:
            # 重置缓存时间戳，确保本次会触发查询
            widget._last_stocks_update = 0.0
            await widget.update_status()
            await widget.update_status()
            # 60 秒内应该只查一次
            assert mock_count.call_count == 1


@pytest.mark.asyncio
async def test_data_completeness_toggles_active_task_class():
    from tui import DataCompletenessWidget
    app = PipelineApp()
    async with app.run_test():
        widget = app.query_one("#data-completeness", DataCompletenessWidget)
        widget._counts = {"daily_bars": 1}
        widget._latest_dates = {"daily_bars": "2026-07-14"}
        with patch.object(widget, "_get_updating_table", return_value="daily_bars"):
            widget._rebuild_content()
            assert "active-task" in widget.classes
        with patch.object(widget, "_get_updating_table", return_value=None):
            widget._rebuild_content()
            assert "active-task" not in widget.classes


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

def test_get_subprocess_env_preserves_operator_db_path():
    """操作者显式 export 的 QUANT_DB_PATH 不得被 TUI 覆盖回默认库。"""
    with patch.dict(os.environ, {"QUANT_DB_PATH": "/tmp/custom_operator.db"}):
        env = get_subprocess_env()
    assert env["QUANT_DB_PATH"] == "/tmp/custom_operator.db"


def test_get_subprocess_env_defaults_db_path():
    """未设置 QUANT_DB_PATH 时才补默认值。"""
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("QUANT_DB_PATH", None)
        env = get_subprocess_env()
    from tui import DEFAULT_DB_PATH
    assert env["QUANT_DB_PATH"] == str(DEFAULT_DB_PATH)


def test_get_subprocess_env():
    env = get_subprocess_env()
    assert isinstance(env, dict)
    # 境内数据源必须全部直连（2026-07-30 事故：sse/szse/jin10/sina.com.cn
    # 不在白名单，系统代理关闭时全部以 ProxyError / SSL EOF 失败）
    no_proxy = env.get("NO_PROXY", "")
    for domain in (
        "eastmoney.com", "sina.com.cn", "sse.com.cn",
        "szse.cn", "jin10.com", "csindex.com.cn", "cninfo.com.cn",
    ):
        assert domain in no_proxy
    # requests/urllib 优先读小写 no_proxy，须与大写完全一致，
    # 否则操作者 shell 继承来的小写变量会遮蔽我们的白名单
    assert env["no_proxy"] == env["NO_PROXY"]
    # NO_PROXY 按后缀匹配、不支持 glob，条目必须与 core/config.py 完全一致（集合相等）
    expected_domains = {
        "localhost", "127.0.0.1",
        "eastmoney.com",
        "sina.com", "sina.cn", "sina.com.cn",
        "sse.com.cn", "szse.cn",
        "jin10.com", "csindex.com.cn", "cninfo.com.cn",
    }
    assert set(no_proxy.split(",")) == expected_domains
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
async def test_run_in_background_refuses_when_task_running():
    """有任务在跑时拒绝启动新任务，且不得杀掉正在运行的子进程。"""
    app = PipelineApp()
    running_proc = MagicMock()
    running_proc.pid = 12345
    running_proc.returncode = None
    app._current_process = running_proc

    with patch("asyncio.create_subprocess_exec") as mock_exec:
        result = await app._run_in_background("arg1")

    assert result is None
    mock_exec.assert_not_called()
    running_proc.terminate.assert_not_called()
    running_proc.kill.assert_not_called()
    # 运行中的子进程保持被跟踪，不得被顶掉
    assert app._current_process is running_proc


@pytest.mark.asyncio
async def test_run_in_background_wait_queues_until_slot_free():
    """wait=True 时在执行槽上排队：槽被占不启动，槽释放后自动执行。"""
    app = PipelineApp()
    mock_proc = MagicMock()
    wait_future = asyncio.Future()
    wait_future.set_result(0)
    mock_proc.wait = MagicMock(return_value=wait_future)
    mock_proc.returncode = 0

    await app._task_slot.acquire()  # 模拟另一任务占用执行槽
    with patch("asyncio.create_subprocess_exec", return_value=mock_proc) as mock_exec:
        queued = asyncio.ensure_future(app._run_in_background("arg1", wait=True))
        await asyncio.sleep(0.05)
        mock_exec.assert_not_called()  # 排队中，不得启动

        app._task_slot.release()
        result = await asyncio.wait_for(queued, timeout=2.0)

    assert result == 0
    mock_exec.assert_called_once()


@pytest.mark.asyncio
async def test_run_in_background_refuses_when_slot_locked():
    """wait=False 时执行槽被占（即使子进程尚未注册）也拒绝启动。"""
    app = PipelineApp()
    await app._task_slot.acquire()
    try:
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            result = await app._run_in_background("arg1")
        assert result is None
        mock_exec.assert_not_called()
    finally:
        app._task_slot.release()


@pytest.mark.asyncio
async def test_run_task_group_uses_wait_semantics():
    """分组队列必须以 wait=True 调用执行器，排队而非拒绝/杀进程。"""
    app = PipelineApp()
    calls: list[dict] = []

    async def fake_run(*args, **kwargs):
        calls.append(kwargs)
        return 0

    with patch.object(app, "_run_in_background", side_effect=fake_run):
        await app._run_task_group("测试组", ["update_south_flow", "update_usd"])

    assert len(calls) == 2
    assert all(kw.get("wait") is True for kw in calls)


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
    with patch("tui.find_running_pipeline_processes", return_value=[]), \
         patch("tui.get_daemon_status", return_value=("Stopped", None)), \
         patch("logging.getLogger", return_value=mock_logger):
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

    with patch("tui.find_running_pipeline_processes", return_value=[]), \
         patch("tui.get_daemon_status", return_value=("Stopped", None)), \
         patch("logging.getLogger", return_value=mock_logger):
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
    expected_pipeline_path = str(_PROJECT_ROOT / "daily_pipeline.py")
    expected_daemon_path = str(_PROJECT_ROOT / "scripts" / "daemon.py")

    with patch.object(app, "_run_in_background", new_callable=MagicMock) as mock_run_bg, \
         patch.object(app, "_run_and_report", new_callable=MagicMock) as mock_run_report, \
         patch("asyncio.create_task", side_effect=_close_coro) as mock_create_task, \
         patch.object(app, "push_screen") as mock_push_screen:

        await app.action_run_pipeline()
        assert mock_push_screen.call_count == 1
        _screen, callback = mock_push_screen.call_args[0]
        callback("run-now")
        mock_create_task.assert_called_once()
        mock_run_report.assert_called_once_with(
            "每日更新", sys.executable, expected_pipeline_path, "--task", "all", "--force"
        )

        mock_run_report.reset_mock()
        mock_create_task.reset_mock()
        mock_push_screen.reset_mock()
        await app.action_resume_pipeline()
        assert mock_push_screen.call_count == 1
        _screen, callback = mock_push_screen.call_args[0]
        callback("run-now")
        mock_create_task.assert_called_once()
        mock_run_report.assert_called_once_with(
            "断点续传", sys.executable, expected_pipeline_path, "--task", "update_bars", "--resume", "--force"
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

        mock_run_report.reset_mock()
        mock_create_task.reset_mock()
        mock_push_screen.reset_mock()
        await app.action_run_health()
        assert mock_push_screen.call_count == 1
        _screen, callback = mock_push_screen.call_args[0]
        callback("run-now")
        mock_create_task.assert_called_once()
        mock_run_report.assert_called_once_with(
            "健康检查", sys.executable, expected_pipeline_path, "--task", "health_check", "--force"
        )


def test_format_chinese_magnitude():
    from tui import format_chinese_magnitude
    assert format_chinese_magnitude(123) == "123"
    assert format_chinese_magnitude(12_345) == "1.2万"
    assert format_chinese_magnitude(123_456_789) == "1.23亿"


def test_get_expected_latest_trading_day_is_weekday():
    from core.calendar import get_expected_latest_trading_day
    result = get_expected_latest_trading_day()
    from datetime import datetime
    dt = datetime.strptime(result, "%Y-%m-%d")
    assert dt.weekday() < 5


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
                 patch("asyncio.create_task", side_effect=_close_coro) as mock_create_task:
                callback("run-later")
                mock_create_task.assert_called_once()


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
        with patch.object(widget._content, "update", side_effect=captured.append), \
             patch("tui.get_expected_latest_trading_day", return_value="2026-07-09"):
            widget._rebuild_content()
        text = "\n".join(captured)
        # 顶部摘要行使用简洁标签 "期望 " + 期望日期
        assert "期望" in text
        assert "2026-07-09" in text
        # 新设计使用 hex 颜色 + 图标 + 状态文本，检查任一状态颜色标签
        from tui import DataCompletenessWidget
        expected_colors = [f"[{color}]" for _icon, color in DataCompletenessWidget.STATUS_STYLES.values()]
        assert any(tag in text for tag in expected_colors)


def test_status_for_table_with_timestamp():
    from tui import DataCompletenessWidget
    # chip_distribution 等表返回带时间戳的日期，只落后 1 天应判定为略滞后
    status = DataCompletenessWidget._get_status_for_table(
        "chip_distribution", "2026-07-13", "2026-07-14", None
    )
    assert status == "略滞后"


def test_phase2_event_tables_are_registered_everywhere():
    from tui import TABLE_DATE_COLUMNS, DataCompletenessWidget

    tables = {
        "stock_repurchase": "update_stock_repurchase",
        "institution_survey": "update_institution_survey",
        "stock_pledge": "update_stock_pledge",
        "option_sentiment": "update_option_sentiment",
    }
    # get_all_table_counts 现在从 TABLE_LABELS.keys() 派生表名，
    # 不再硬编码常量；改为运行时验证表名包含关系
    from tui import TABLE_LABELS as _TABLE_LABELS
    for table, task in tables.items():
        assert table in _TABLE_LABELS, f"{table} not in TABLE_LABELS"
        assert TABLE_DATE_COLUMNS[table] == "trade_date"
        assert DataCompletenessWidget.TASK_TO_TABLE[task] == [table]
        assert table in DataCompletenessWidget.TABLE_LABELS
        assert table in DataCompletenessWidget.TABLE_LABELS_CN




@pytest.mark.asyncio
async def test_data_completeness_sorts_by_freshness():
    from tui import DataCompletenessWidget, PipelineApp
    app = PipelineApp()
    async with app.run_test():
        widget = app.query_one("#data-completeness", DataCompletenessWidget)
        # 用真实表名构造各状态：最新、延迟发布、略滞后、滞后、按月、按季
        widget._counts = {
            "daily_bars": 7000000,      # 最新
            "fx_rate": 13,              # 延迟发布
            "margin_trading": 46607,    # 略滞后
            "global_index": 56,         # 滞后
            "institutional_holdings": 32149,  # 按月更新
            "quarterly_financials": 5531,     # 按季更新
        }
        widget._latest_dates = {
            "daily_bars": "2026-07-14",
            "fx_rate": "2026-07-14",
            "margin_trading": "2026-07-13",
            "global_index": "2026-07-10",
            "institutional_holdings": "2026-06-30",
            "quarterly_financials": "2026-03-31",
        }
        captured = []
        with patch.object(widget._content, "update", side_effect=captured.append), patch("tui.get_expected_latest_trading_day", return_value="2026-07-14"):
            widget._rebuild_content()
        text = "\n".join(captured)
        # UI 渲染用中文标签（TABLE_LABELS_CN），按位置验证排序：最新 → T+1 → 略滞后 → 滞后 → 按月 → 按季
        positions = {
            "日线行情": text.find("日线行情"),
            "汇率": text.find("汇率"),
            "融资融券": text.find("融资融券"),
            "全球指数": text.find("全球指数"),
            "机构持仓": text.find("机构持仓"),
            "季度财务": text.find("季度财务"),
        }
        assert -1 not in positions.values(), f"标签缺失: {positions}"
        assert (
            positions["日线行情"]
            < positions["融资融券"]
            < positions["汇率"]
            < positions["全球指数"]
            < positions["机构持仓"]
            < positions["季度财务"]
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
    result = sync_watchlists_from_files(":memory:")
    assert result.added == 0
    assert result.deactivated == 0
    assert result.files == 0


def test_sync_watchlists_empty_dir(tmp_path, monkeypatch):
    from tui import sync_watchlists_from_files
    watch_dir = tmp_path / "watchlists"
    watch_dir.mkdir()
    monkeypatch.setattr("tui.WATCHLIST_DIR", watch_dir)
    result = sync_watchlists_from_files(":memory:")
    assert result.added == 0
    assert result.files == 0


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
    result = sync_watchlists_from_files(db_path)
    assert result.added == 2
    assert result.files == 1


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
    result = sync_watchlists_from_files(db_path)
    assert result.added == 1


def _create_watchlist_db(path: str) -> None:
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE watchlist ("
        "ts_code TEXT PRIMARY KEY, added_date TEXT, "
        "source_scan TEXT, status TEXT, "
        "updated_at TEXT DEFAULT CURRENT_TIMESTAMP)"
    )
    conn.commit()
    conn.close()


def test_sync_watchlists_deactivates_removed_txt_symbols(tmp_path, monkeypatch):
    from tui import sync_watchlists_from_files

    watch_dir = tmp_path / "watchlists"
    watch_dir.mkdir()
    (watch_dir / "main.txt").write_text("600000\n", encoding="utf-8")
    db_path = str(tmp_path / "test.db")
    _create_watchlist_db(db_path)
    conn = sqlite3.connect(db_path)
    conn.executemany(
        "INSERT INTO watchlist (ts_code, added_date, source_scan, status, updated_at) "
        "VALUES (?, '2026-07-16', 'watchlist_sync', 'tracking', '2026-01-01 00:00:00')",
        [("600000.SH",), ("000001.SZ",)],
    )
    conn.commit()
    conn.close()
    monkeypatch.setattr("tui.WATCHLIST_DIR", watch_dir)

    result = sync_watchlists_from_files(db_path)

    assert result.deactivated == 1
    conn = sqlite3.connect(db_path)
    statuses = dict(conn.execute("SELECT ts_code, status FROM watchlist"))
    removed_updated_at = conn.execute(
        "SELECT updated_at FROM watchlist WHERE ts_code='000001.SZ'"
    ).fetchone()[0]
    conn.close()
    assert statuses == {"600000.SH": "tracking", "000001.SZ": "inactive"}
    assert removed_updated_at != "2026-01-01 00:00:00"


def test_sync_watchlists_reactivates_returning_symbol(tmp_path, monkeypatch):
    from tui import sync_watchlists_from_files

    watch_dir = tmp_path / "watchlists"
    watch_dir.mkdir()
    (watch_dir / "main.txt").write_text("000001\n", encoding="utf-8")
    db_path = str(tmp_path / "test.db")
    _create_watchlist_db(db_path)
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO watchlist (ts_code, added_date, source_scan, status, updated_at) VALUES "
        "('000001.SZ', '2026-07-01', 'watchlist_sync', 'inactive', '2026-01-01 00:00:00')"
    )
    conn.commit()
    conn.close()
    monkeypatch.setattr("tui.WATCHLIST_DIR", watch_dir)

    result = sync_watchlists_from_files(db_path)

    assert result.reactivated == 1
    conn = sqlite3.connect(db_path)
    status, updated_at = conn.execute(
        "SELECT status, updated_at FROM watchlist WHERE ts_code='000001.SZ'"
    ).fetchone()
    conn.close()
    assert status == "tracking"
    assert updated_at != "2026-01-01 00:00:00"


def test_sync_watchlists_empty_directory_deactivates_only_sync_source(tmp_path, monkeypatch):
    from tui import sync_watchlists_from_files

    watch_dir = tmp_path / "watchlists"
    watch_dir.mkdir()
    db_path = str(tmp_path / "test.db")
    _create_watchlist_db(db_path)
    conn = sqlite3.connect(db_path)
    conn.executemany(
        "INSERT INTO watchlist (ts_code, added_date, source_scan, status) "
        "VALUES (?, '2026-07-16', ?, 'tracking')",
        [("600000.SH", "watchlist_sync"), ("000001.SZ", "manual")],
    )
    conn.commit()
    conn.close()
    monkeypatch.setattr("tui.WATCHLIST_DIR", watch_dir)

    result = sync_watchlists_from_files(db_path)

    assert result.deactivated == 1
    conn = sqlite3.connect(db_path)
    statuses = dict(conn.execute("SELECT ts_code, status FROM watchlist"))
    conn.close()
    assert statuses == {"600000.SH": "inactive", "000001.SZ": "tracking"}


def test_sync_watchlists_reports_database_failure(tmp_path, monkeypatch):
    from tui import sync_watchlists_from_files

    watch_dir = tmp_path / "watchlists"
    watch_dir.mkdir()
    (watch_dir / "main.txt").write_text("600000\n", encoding="utf-8")
    db_path = str(tmp_path / "missing_table.db")
    sqlite3.connect(db_path).close()
    monkeypatch.setattr("tui.WATCHLIST_DIR", watch_dir)

    result = sync_watchlists_from_files(db_path)

    assert result.success is False
    assert result.error
    assert "watchlist" in result.error


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


def test_find_running_pipeline_skips_non_python_lookalikes():
    """pgrep -f 会命中 vim/tail 等命令行含 daily_pipeline.py 的进程，必须过滤。"""
    from tui import find_running_pipeline_processes
    with patch("tui.Path.exists", return_value=False), \
         patch("subprocess.run") as mock_run:
        mock_run.side_effect = [
            MagicMock(stdout="999\n"),                           # pgrep
            MagicMock(stdout=" 1\n"),                            # ps ppid
            MagicMock(stdout="999 01:23 vim daily_pipeline.py"),  # ps command
        ]
        procs = find_running_pipeline_processes()
    assert procs == []


def test_find_running_pipeline_keeps_python_processes():
    """python 解释器启动的 daily_pipeline.py 进程必须保留。"""
    from tui import find_running_pipeline_processes
    with patch("tui.Path.exists", return_value=False), \
         patch("subprocess.run") as mock_run:
        mock_run.side_effect = [
            MagicMock(stdout="888\n"),                                        # pgrep
            MagicMock(stdout=" 1\n"),                                         # ps ppid
            MagicMock(stdout="888 01:23 python daily_pipeline.py --task all"),  # ps command
        ]
        procs = find_running_pipeline_processes()
    assert len(procs) == 1
    assert procs[0]["pid"] == 888


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
# PipelineApp: action_run_reconcile, action_copy_panel, on_mount no processes
# ===========================================================================
@pytest.mark.asyncio
async def test_action_run_reconcile():
    app = PipelineApp()
    # reconcile 现在统一走 _run_or_schedule 弹窗确认，验证 push_screen 被调用
    with patch.object(app, "push_screen") as mock_push:
        await app.action_run_reconcile()
        mock_push.assert_called_once()


@pytest.mark.asyncio
async def test_on_mount_no_processes():
    app = PipelineApp()
    with patch("tui.find_running_pipeline_processes", return_value=[]), \
         patch.object(app, "_create_background_task", side_effect=_close_coro) as mock_bg:
        await app.on_mount()
        # 没有后台进程时仍应调度自选股同步
        mock_bg.assert_called_once()


@pytest.mark.asyncio
async def test_sync_watchlists_notification_reports_all_changes():
    from tui import WatchlistSyncResult
    app = PipelineApp()
    result = WatchlistSyncResult(
        added=1,
        reactivated=2,
        deactivated=3,
        files=4,
        success=True,
        error=None,
    )
    with patch("tui.sync_watchlists_from_files", return_value=result), \
         patch.object(app, "notify") as notify:
        await app._sync_watchlists()

    message = notify.call_args.args[0]
    assert "新增 1" in message
    assert "恢复 2" in message
    assert "停用 3" in message
    assert "4 个文件" in message


@pytest.mark.asyncio
async def test_sync_watchlists_notification_reports_failure():
    from tui import WatchlistSyncResult

    app = PipelineApp()
    result = WatchlistSyncResult(
        added=0,
        reactivated=0,
        deactivated=0,
        files=1,
        success=False,
        error="database unavailable",
    )
    with patch("tui.sync_watchlists_from_files", return_value=result), \
         patch.object(app, "notify") as notify:
        await app._sync_watchlists()

    assert "database unavailable" in notify.call_args.args[0]
    assert notify.call_args.kwargs["severity"] == "warning"


@pytest.mark.asyncio
async def test_action_stop_pipeline_also_stops_daemon():
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
         patch.object(app, "_create_background_task", side_effect=_close_coro) as mock_bg, \
         patch("logging.getLogger", return_value=MagicMock()):
        await app.action_stop_pipeline()
        mock_bg.assert_called()  # should schedule daemon stop


# ===========================================================================
# PipelineApp: _stop_daemon_process and action_copy_panel
# ===========================================================================
@pytest.mark.asyncio
async def test_stop_daemon_process_success():
    app = PipelineApp()
    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.communicate = MagicMock(return_value=(b"daemon stopped", b""))
    with patch("asyncio.create_subprocess_exec", return_value=mock_proc) as mock_exec, \
         patch("logging.getLogger", return_value=MagicMock()):
        await app._stop_daemon_process()
        mock_exec.assert_called_once()


@pytest.mark.asyncio
async def test_stop_daemon_process_timeout():
    app = PipelineApp()
    mock_proc = MagicMock()
    mock_proc.communicate = MagicMock(side_effect=asyncio.TimeoutError)
    with patch("asyncio.create_subprocess_exec", return_value=mock_proc), \
         patch("logging.getLogger", return_value=MagicMock()):
        await app._stop_daemon_process()  # should handle timeout gracefully


@pytest.mark.asyncio
async def test_action_copy_panel_opens_screen():
    app = PipelineApp()
    async with app.run_test():
        with patch.object(app, "push_screen") as mock_push:
            app.action_copy_panel()
            mock_push.assert_called_once()
            screen, callback = mock_push.call_args[0]
            assert isinstance(screen, CopyPanelScreen)


@pytest.mark.asyncio
async def test_copy_panel_screen_dismiss():
    screen = CopyPanelScreen()
    for btn_id, _key, _label in CopyPanelScreen.PANELS:
        mock_dismiss = MagicMock()
        screen.dismiss = mock_dismiss
        # on_list_view_selected 只访问 event.item.id，用一个轻量 stub 即可
        item = MagicMock()
        item.id = btn_id
        event = MagicMock()
        event.item = item
        screen.on_list_view_selected(event)
        mock_dismiss.assert_called_once_with(btn_id)


@pytest.mark.asyncio
async def test_copy_panel_screen_composes_four_items() -> None:
    from textual.widgets import ListItem
    app = PipelineApp()
    with patch("tui.find_running_pipeline_processes", return_value=[]):
        async with app.run_test() as pilot:
            screen = CopyPanelScreen()
            app.push_screen(screen)
            await pilot.pause()
            items = list(screen.query(ListItem))
            assert len(items) == 4
            expected_ids = {panel_id for panel_id, _key, _label in CopyPanelScreen.PANELS}
            assert {item.id for item in items} == expected_ids
            # 每个列表项都应该有可见高度，确保不会被布局裁剪
            for item in items:
                assert item.region.height > 0


@pytest.mark.asyncio
async def test_action_run_single_task():
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
        assert result == ["daily_bars"]


def test_new_tables_have_date_column_mappings():
    from tui import TABLE_DATE_COLUMNS

    expected = {
        "south_flow": "trade_date",
        "ah_premium": "trade_date",
        "etf_daily": "trade_date",
        "cb_index": "trade_date",
        "restricted_share": "release_date",
        "earnings_forecast": "end_date",
        "sector_daily": "trade_date",
        "sector_valuation": "trade_date",
        "index_futures_basis": "trade_date",
        "macro_monthly": "date",
        "macro_quarterly": "date",
    }

    for table, date_column in expected.items():
        assert TABLE_DATE_COLUMNS[table] == date_column

    # macro_daily 已由 money_market 接管，无生产者，不得再出现在监控映射中
    assert "macro_daily" not in TABLE_DATE_COLUMNS


def test_stock_pledge_reports_weekly_not_stale():
    """质押数据每周五发布，一周内的数据不得标记为滞后。"""
    from tui import DataCompletenessWidget

    status = DataCompletenessWidget._get_status_for_table(
        "stock_pledge", "2026-07-24", "2026-07-30", None
    )
    assert status == "按周更新"


def test_healthy_statuses_include_weekly():
    """健康度统计必须含“按周更新”，否则新鲜的质押数据会从健康计数中静默流失。"""
    from tui import _HEALTHY_STATUSES

    assert "按周更新" in _HEALTHY_STATUSES
    for status in ("最新", "T+1", "按月更新", "按季更新"):
        assert status in _HEALTHY_STATUSES


def test_stock_pledge_weekly_goes_stale_after_10_days():
    """超过 10 个自然日未更新的质押数据应落入 _date_status 判定，不再显示“按周更新”。"""
    from tui import DataCompletenessWidget, _date_status

    status = DataCompletenessWidget._get_status_for_table(
        "stock_pledge", "2026-07-10", "2026-07-30", None
    )
    assert status != "按周更新"
    assert status == _date_status("2026-07-10", "2026-07-30")


def test_stock_pledge_weekly_malformed_date_keeps_badge():
    """日期解析失败时保守地保留“按周更新”标记。"""
    from tui import DataCompletenessWidget

    status = DataCompletenessWidget._get_status_for_table(
        "stock_pledge", "not-a-date", "2026-07-30", None
    )
    assert status == "按周更新"


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
# Theme switching and HelpScreen
# ===========================================================================
@pytest.mark.asyncio
async def test_action_toggle_theme():
    """验证主题在 BUILTIN_THEMES 中轮换并持久化。"""
    from textual.theme import BUILTIN_THEMES
    app = PipelineApp()
    async with app.run_test():
        with patch("tui.save_theme") as mock_save:
            themes_sorted = sorted(BUILTIN_THEMES)
            initial_idx = themes_sorted.index(app._theme_name) if app._theme_name in themes_sorted else -1
            await app.action_toggle_theme()
            next_idx = (initial_idx + 1) % len(themes_sorted)
            assert app._theme_name == themes_sorted[next_idx]
            assert app.theme == themes_sorted[next_idx]
            mock_save.assert_called_once_with(themes_sorted[next_idx])

            await app.action_toggle_theme()
            after_idx = (next_idx + 1) % len(themes_sorted)
            assert app._theme_name == themes_sorted[after_idx]
            assert app.theme == themes_sorted[after_idx]
            assert mock_save.call_args.args[0] == themes_sorted[after_idx]


@pytest.mark.asyncio
async def test_action_toggle_theme_wraparound_index_display():
    """最后一个主题环绕到第一个时，通知里的索引应显示 1/N 而非 (N+1)/N。"""
    from textual.theme import BUILTIN_THEMES
    app = PipelineApp()
    async with app.run_test():
        themes_sorted = sorted(BUILTIN_THEMES)
        app._theme_name = themes_sorted[-1]
        with patch("tui.save_theme"), patch.object(app, "notify") as mock_notify:
            await app.action_toggle_theme()
        assert app._theme_name == themes_sorted[0]
        msg = mock_notify.call_args.args[0]
        assert f"(1/{len(themes_sorted)})" in msg


@pytest.mark.asyncio
async def test_help_screen_bindings():
    screen = HelpScreen()
    assert any(binding.key == "escape" for binding in screen.BINDINGS)
    assert any(binding.key == "q" for binding in screen.BINDINGS)


@pytest.mark.asyncio
async def test_show_help_opens_help_screen():
    app = PipelineApp()
    async with app.run_test():
        with patch.object(app, "push_screen") as mock_push:
            await app.action_show_help()
            mock_push.assert_called_once()
            screen = mock_push.call_args[0][0]
            assert isinstance(screen, HelpScreen)


def test_load_save_theme(tmp_path, monkeypatch):
    monkeypatch.setattr("tui.TUI_CONFIG_PATH", tmp_path / "tui.json")
    assert load_theme() == "textual-dark"
    save_theme("textual-light")
    assert load_theme() == "textual-light"


@pytest.mark.asyncio
async def test_log_cleanup_screen_buttons():
    app = PipelineApp()
    with patch("tui.find_running_pipeline_processes", return_value=[]):
        async with app.run_test() as pilot:
            await pilot.press("l")
            screen = app.screen
            assert isinstance(screen, LogCleanupScreen)
            labels = [str(b.label) for b in screen.query("Button")]
            assert "全部清理" in labels
            assert "保留最近 7 天" in labels
            assert "保留最近 30 天" in labels
            assert "取消 (Esc)" in labels


@pytest.mark.asyncio
async def test_action_clean_logs_opens_screen():
    app = PipelineApp()
    async with app.run_test():
        with patch.object(app, "push_screen") as mock_push:
            await app.action_clean_logs()
            mock_push.assert_called_once()
            assert isinstance(mock_push.call_args[0][0], LogCleanupScreen)


@pytest.mark.asyncio
async def test_task_group_widget_present():
    from tui import TaskGroupWidget
    app = PipelineApp()
    async with app.run_test():
        tabs = app.query_one("#task-tabs")
        assert tabs is not None
        group_widget = app.query_one("#task-groups", TaskGroupWidget)
        assert group_widget is not None


@pytest.mark.asyncio
async def test_task_group_buttons():
    from tui import TASK_GROUPS, TaskGroupWidget
    app = PipelineApp()
    async with app.run_test():
        widget = app.query_one("#task-groups", TaskGroupWidget)
        buttons = list(widget.query("Button"))
        # 分组按钮 + 「补齐缺失」按钮
        assert len(buttons) == len(TASK_GROUPS) + 1
        for key in TASK_GROUPS:
            assert widget.query_one(f"#group-{key}") is not None
        assert widget.query_one("#group-catchup") is not None


@pytest.mark.asyncio
async def test_action_run_task_group():
    from tui import TASK_GROUPS
    app = PipelineApp()
    async with app.run_test() as pilot:
        with patch.object(app, "_run_in_background", return_value=0) as mock_run:
            await app.action_run_task_group("valuation")
            # 让后台任务有机会执行
            await pilot.pause()
        expected_tasks = TASK_GROUPS["valuation"]
        assert mock_run.call_count == len(expected_tasks)
        for i, task in enumerate(expected_tasks):
            args = mock_run.call_args_list[i][0]
            assert "--task" in args
            assert task in args


@pytest.mark.asyncio
async def test_action_run_task_group_unknown():
    app = PipelineApp()
    async with app.run_test() as pilot:
        with patch.object(app, "notify") as mock_notify:
            await app.action_run_task_group("nonexistent")
            await pilot.pause()
        assert any("未知任务分组" in str(call) for call in mock_notify.call_args_list)


# ===========================================================================
# 收盘刷新（--refresh-today）：绑定、确认弹窗、启动命令与结构化状态
# ===========================================================================
def _screen_labels_text(screen) -> str:
    """提取弹窗内所有 Label 的原始 markup 文本（沿用 _Static__content 惯例）。"""
    from textual.widgets import Label
    return " ".join(
        str(getattr(label, "_Static__content", "")) for label in screen.query(Label)
    )


@pytest.mark.asyncio
async def test_refresh_today_binding_and_action_exist():
    """收盘刷新是独立的绑定 + action，与全量更新/断点续传分开。"""
    app = PipelineApp()
    async with app.run_test():
        assert callable(getattr(app, "action_refresh_today", None))
        refresh_bindings = [b for b in PipelineApp.BINDINGS if b.action == "refresh_today"]
        assert len(refresh_bindings) == 1
        # 不与现有绑定共用按键
        other_keys = {b.key for b in PipelineApp.BINDINGS if b.action != "refresh_today"}
        assert refresh_bindings[0].key not in other_keys


@pytest.mark.asyncio
async def test_refresh_today_confirmation_displays_date_tasks_and_scope():
    """确认弹窗必须显示上海目标交易日、28 个任务范围与可选股票范围输入。"""
    from zoneinfo import ZoneInfo

    from textual.widgets import Input

    from tui import ConfirmRefreshTodayScreen

    app = PipelineApp()
    with patch("tui.find_running_pipeline_processes", return_value=[]):
        async with app.run_test() as pilot:
            with patch(
                "tui.get_expected_latest_trading_day", return_value="2026-07-28"
            ) as mock_day:
                screen = ConfirmRefreshTodayScreen()
            # 目标日期必须用上海时区的 aware now 计算，不依赖本机时区
            now_arg = mock_day.call_args.kwargs["now"]
            assert now_arg.tzinfo == ZoneInfo("Asia/Shanghai")

            app.push_screen(screen)
            await pilot.pause()
            text = _screen_labels_text(screen)
            assert "2026-07-28" in text
            assert "28" in text
            # 可选股票范围输入框 + 确认/取消按钮
            assert screen.query_one("#refresh-symbols", Input) is not None
            assert screen.query_one("#refresh-confirm") is not None
            assert screen.query_one("#refresh-cancel") is not None


@pytest.mark.asyncio
async def test_refresh_today_confirmation_dismiss_values():
    """确认返回输入的股票范围（可为空字符串），取消返回 None。"""
    from textual.widgets import Button, Input

    from tui import ConfirmRefreshTodayScreen

    app = PipelineApp()
    with patch("tui.find_running_pipeline_processes", return_value=[]):
        async with app.run_test() as pilot:
            screen = ConfirmRefreshTodayScreen()
            app.push_screen(screen)
            await pilot.pause()

            screen.dismiss = MagicMock()
            screen.query_one("#refresh-symbols", Input).value = " 600000,000001 "
            screen.on_button_pressed(Button.Pressed(Button(id="refresh-confirm")))
            screen.dismiss.assert_called_once_with("600000,000001")

            screen.dismiss = MagicMock()
            screen.on_button_pressed(Button.Pressed(Button(id="refresh-cancel")))
            screen.dismiss.assert_called_once_with(None)


@pytest.mark.asyncio
async def test_action_refresh_today_accept_launches_exact_command():
    """确认后精确启动 daily_pipeline.py --refresh-today，无额外 flag。"""
    from tui import ConfirmRefreshTodayScreen

    app = PipelineApp()
    expected_pipeline_path = str(
        _PROJECT_ROOT / "daily_pipeline.py"
    )
    with patch.object(app, "_run_refresh_today") as mock_refresh, \
         patch("asyncio.create_task", side_effect=_close_coro) as mock_create_task, \
         patch.object(app, "push_screen") as mock_push_screen:
        await app.action_refresh_today()
        assert mock_push_screen.call_count == 1
        screen, callback = mock_push_screen.call_args[0]
        assert isinstance(screen, ConfirmRefreshTodayScreen)

        callback("")
        mock_create_task.assert_called_once()
        mock_refresh.assert_called_once_with(
            sys.executable, expected_pipeline_path, "--refresh-today"
        )


@pytest.mark.asyncio
async def test_action_refresh_today_accept_with_symbols_scope():
    """输入股票范围时追加 --symbols，且仅追加这一个参数对。"""
    app = PipelineApp()
    expected_pipeline_path = str(
        _PROJECT_ROOT / "daily_pipeline.py"
    )
    with patch.object(app, "_run_refresh_today") as mock_refresh, \
         patch("asyncio.create_task", side_effect=_close_coro), \
         patch.object(app, "push_screen") as mock_push_screen:
        await app.action_refresh_today()
        _screen, callback = mock_push_screen.call_args[0]
        callback("600000,000001")
        mock_refresh.assert_called_once_with(
            sys.executable, expected_pipeline_path, "--refresh-today",
            "--symbols", "600000,000001",
        )


@pytest.mark.asyncio
async def test_action_refresh_today_cancel_launches_nothing():
    """取消时不得启动任何子进程，也不做延迟调度。"""
    app = PipelineApp()
    with patch.object(app, "_run_refresh_today") as mock_refresh, \
         patch.object(app, "_run_in_background", new_callable=MagicMock) as mock_run_bg, \
         patch("asyncio.create_task", side_effect=_close_coro) as mock_create_task, \
         patch.object(app, "push_screen") as mock_push_screen:
        await app.action_refresh_today()
        _screen, callback = mock_push_screen.call_args[0]
        callback(None)
        mock_refresh.assert_not_called()
        mock_run_bg.assert_not_called()
        mock_create_task.assert_not_called()


@pytest.mark.asyncio
async def test_run_refresh_today_delegates_and_reports_states(tmp_path):
    """后台执行只透传参数给 _run_in_background，结束后汇总审计状态。"""
    app = PipelineApp()
    records = [
        {"task_name": "update_bars", "state": "committed"},
        {"task_name": "update_fund_flow", "state": "retained"},
        {"task_name": "update_indicators", "state": "blocked"},
    ]
    with patch("tui.DEFAULT_DB_PATH", tmp_path / "missing.db"), \
         patch.object(app, "_run_in_background", return_value=1) as mock_run_bg, \
         patch("tui.get_latest_refresh_task_states", return_value=records), \
         patch.object(app, "notify") as mock_notify:
        await app._run_refresh_today("python", "daily_pipeline.py", "--refresh-today")
        mock_run_bg.assert_called_once_with(
            "python", "daily_pipeline.py", "--refresh-today"
        )
    message = mock_notify.call_args.args[0]
    # 展示必须区分「已提交覆盖」与「保留旧数据」
    assert "已提交覆盖 1" in message
    assert "保留旧数据 1" in message
    assert mock_notify.call_args.kwargs["severity"] == "warning"


def _create_refresh_audit_db_with_prior_run(db_path: str) -> None:
    """建审计表并写入上一次运行的记录（2 个任务均为保留旧数据）。"""
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE refresh_runs (run_id TEXT PRIMARY KEY, target_date TEXT, "
        "started_at TEXT, finished_at TEXT, status TEXT, symbols_json TEXT)"
    )
    conn.execute(
        "CREATE TABLE refresh_task_runs (run_id TEXT, task_name TEXT, "
        "policy_kind TEXT, requested_date TEXT, as_of_date TEXT, status TEXT, "
        "fetched INTEGER, validated INTEGER, replaced INTEGER, "
        "retained INTEGER, failed INTEGER, metadata_json TEXT)"
    )
    conn.execute(
        "INSERT INTO refresh_runs VALUES "
        "('run-prior', '2026-07-27', '2026-07-27T16:05:00+08:00', "
        "'2026-07-27T16:40:00+08:00', 'failed', '[]')"
    )
    conn.execute(
        "INSERT INTO refresh_task_runs VALUES ('run-prior', 'update_bars', "
        "'full_replace', '2026-07-27', NULL, 'failed', 0, 0, 0, 50, 1, "
        "'{\"retained_old_data\": true}')"
    )
    conn.execute(
        "INSERT INTO refresh_task_runs VALUES ('run-prior', 'update_fund_flow', "
        "'full_replace', '2026-07-27', NULL, 'failed', 0, 0, 0, 30, 1, "
        "'{\"retained_old_data\": true}')"
    )
    conn.commit()
    conn.close()


def _insert_new_refresh_run(db_path: str) -> None:
    """模拟本次子进程退出前写入的新 run（1 个任务已提交覆盖）。"""
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO refresh_runs VALUES "
        "('run-current', '2026-07-28', '2026-07-28T16:05:00+08:00', "
        "'2026-07-28T16:40:00+08:00', 'success', '[]')"
    )
    conn.execute(
        "INSERT INTO refresh_task_runs VALUES ('run-current', 'update_bars', "
        "'full_replace', '2026-07-28', '2026-07-28', 'success', 100, 100, 100, "
        "0, 0, '{}')"
    )
    conn.commit()
    conn.close()


@pytest.mark.asyncio
async def test_run_refresh_today_crash_before_persist_no_stale_summary(tmp_path):
    """子进程在写入自身 refresh_runs 行之前崩溃：不得把上一次运行汇总为本次。

    例如 CLI 16:00 闸门拒绝、--symbols 参数错误、导入失败——审计库里只有
    上一次 run 的记录，通知必须退回纯退出码告警。
    """
    db_path = tmp_path / "audit.db"
    _create_refresh_audit_db_with_prior_run(str(db_path))
    app = PipelineApp()
    with patch("tui.DEFAULT_DB_PATH", db_path), \
         patch.object(app, "_run_in_background", return_value=2) as mock_run_bg, \
         patch.object(app, "notify") as mock_notify:
        await app._run_refresh_today("python", "daily_pipeline.py", "--refresh-today")
        mock_run_bg.assert_called_once_with(
            "python", "daily_pipeline.py", "--refresh-today"
        )
    assert mock_notify.call_count == 1
    message = mock_notify.call_args.args[0]
    # 上一次 run 的保留/覆盖计数不得出现在本次通知中
    assert "保留旧数据" not in message
    assert "已提交覆盖" not in message
    # 退回既有的非零退出码告警路径
    assert "退出码 2" in message
    assert mock_notify.call_args.kwargs["severity"] == "warning"


@pytest.mark.asyncio
async def test_run_refresh_today_summarizes_run_newer_than_boundary(tmp_path):
    """子进程写入了严格晚于启动边界的新 run：正常汇总该 run（回归保护）。"""
    db_path = tmp_path / "audit.db"
    _create_refresh_audit_db_with_prior_run(str(db_path))
    app = PipelineApp()

    def _fake_run(*args):
        _insert_new_refresh_run(str(db_path))
        return 0

    with patch("tui.DEFAULT_DB_PATH", db_path), \
         patch.object(app, "_run_in_background", side_effect=_fake_run), \
         patch.object(app, "notify") as mock_notify:
        await app._run_refresh_today("python", "daily_pipeline.py", "--refresh-today")
    message = mock_notify.call_args.args[0]
    # 只汇总新 run：1 个已提交覆盖，上一次 run 的 2 个保留任务不得混入
    assert "已提交覆盖 1" in message
    assert "保留旧数据" not in message
    assert mock_notify.call_args.kwargs["severity"] == "information"


def test_classify_refresh_task_state_distinguishes_retained_from_committed():
    """结构化状态归类覆盖设计文档要求的六态区分。"""
    from tui import REFRESH_STATE_LABELS, classify_refresh_task_state

    assert classify_refresh_task_state(None) == "pending"

    committed = {
        "status": "success", "fetched": 100, "validated": 100,
        "replaced": 100, "retained": 0, "metadata": {},
    }
    retained = {
        "status": "failed", "fetched": 0, "validated": 0,
        "replaced": 0, "retained": 50,
        "metadata": {"retained_old_data": True},
    }
    fetched_not_validated = {
        "status": "failed", "fetched": 80, "validated": 0,
        "replaced": 0, "retained": 50,
        "metadata": {"retained_old_data": True},
    }
    degraded = {
        "status": "degraded", "fetched": 100, "validated": 90,
        "replaced": 90, "retained": 10, "metadata": {},
    }
    blocked = {
        "status": "failed", "fetched": 0, "validated": 0,
        "replaced": 0, "retained": 0,
        "metadata": {"blocked_by": ["update_bars"], "retained_old_data": True},
    }

    assert classify_refresh_task_state(committed) == "committed"
    assert classify_refresh_task_state(retained) == "retained"
    assert classify_refresh_task_state(fetched_not_validated) == "fetched_not_validated"
    assert classify_refresh_task_state(degraded) == "degraded"
    assert classify_refresh_task_state(blocked) == "blocked"

    # 保留旧数据与已提交覆盖必须是不同的展示标签
    labels = [REFRESH_STATE_LABELS[state] for state in (
        "pending", "fetched_not_validated", "committed",
        "retained", "degraded", "blocked",
    )]
    assert len(set(labels)) == 6
    assert REFRESH_STATE_LABELS["committed"] != REFRESH_STATE_LABELS["retained"]


def test_get_latest_refresh_task_states_reads_latest_run(tmp_path):
    """从 refresh_runs/refresh_task_runs 审计表读取最近一次运行并归类。"""
    from tui import get_latest_refresh_task_states

    db_path = str(tmp_path / "audit.db")
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE refresh_runs (run_id TEXT PRIMARY KEY, target_date TEXT, "
        "started_at TEXT, finished_at TEXT, status TEXT, symbols_json TEXT)"
    )
    conn.execute(
        "CREATE TABLE refresh_task_runs (run_id TEXT, task_name TEXT, "
        "policy_kind TEXT, requested_date TEXT, as_of_date TEXT, status TEXT, "
        "fetched INTEGER, validated INTEGER, replaced INTEGER, "
        "retained INTEGER, failed INTEGER, metadata_json TEXT)"
    )
    conn.execute(
        "INSERT INTO refresh_runs VALUES "
        "('run-old', '2026-07-27', '2026-07-27T16:05:00+08:00', NULL, 'success', '[]')"
    )
    conn.execute(
        "INSERT INTO refresh_runs VALUES "
        "('run-new', '2026-07-28', '2026-07-28T16:05:00+08:00', NULL, 'degraded', '[]')"
    )
    conn.execute(
        "INSERT INTO refresh_task_runs VALUES ('run-old', 'update_bars', 'full_replace', "
        "'2026-07-27', '2026-07-27', 'success', 10, 10, 10, 0, 0, '{}')"
    )
    conn.execute(
        "INSERT INTO refresh_task_runs VALUES ('run-new', 'update_bars', 'full_replace', "
        "'2026-07-28', '2026-07-28', 'success', 100, 100, 100, 0, 0, '{}')"
    )
    conn.execute(
        "INSERT INTO refresh_task_runs VALUES ('run-new', 'update_fund_flow', 'full_replace', "
        "'2026-07-28', NULL, 'failed', 0, 0, 0, 50, 1, "
        "'{\"retained_old_data\": true}')"
    )
    conn.commit()
    conn.close()

    records = get_latest_refresh_task_states(db_path)
    states = {r["task_name"]: r["state"] for r in records}
    # 只读最近一次 run，且区分覆盖/保留
    assert states == {"update_bars": "committed", "update_fund_flow": "retained"}

    # 表不存在 / 库不存在时返回空，不抛异常
    assert get_latest_refresh_task_states(str(tmp_path / "missing.db")) == []


def test_format_refresh_summary_counts_states():
    from tui import format_refresh_summary

    records = [
        {"state": "committed"},
        {"state": "committed"},
        {"state": "retained"},
        {"state": "degraded"},
    ]
    summary = format_refresh_summary(records)
    assert "已提交覆盖 2" in summary
    assert "保留旧数据 1" in summary
    assert "部分降级 1" in summary


# ===========================================================================
# Single Task 下拉中的收盘刷新哨兵：路由到 action_refresh_today
# ===========================================================================
def test_single_task_dropdown_contains_refresh_today_sentinel():
    """下拉必须包含收盘刷新哨兵项，且紧跟在全量更新之后。"""
    from tui import SingleTaskWidget

    options = SingleTaskWidget._build_single_tasks()
    values = [value for _label, value in options]
    assert "__refresh_today__" in values
    assert values.index("__refresh_today__") == values.index("all") + 1
    labels = {value: label for label, value in options}
    assert "收盘刷新" in labels["__refresh_today__"]


def test_validate_against_registry_ignores_refresh_today_sentinel(caplog):
    """哨兵不是注册任务，不得进入未注册任务警告名单。"""
    import logging

    from tui import SingleTaskWidget

    # 强制所有任务判为未注册以触发警告路径，但哨兵不在校验名单里
    with patch("core.task_registry.lookup_task", return_value=None), \
         caplog.at_level(logging.WARNING):
        SingleTaskWidget._build_single_tasks()
    assert any("unregistered" in record.getMessage() for record in caplog.records)
    assert "__refresh_today__" not in caplog.text


@pytest.mark.asyncio
async def test_select_refresh_today_sentinel_routes_to_action_refresh_today():
    """选中哨兵走收盘刷新确认流程，绝不落入 --task 单任务路径。"""
    from textual.widgets import Select

    from tui import SingleTaskWidget

    app = PipelineApp()
    with patch("tui.find_running_pipeline_processes", return_value=[]):
        async with app.run_test():
            panel = app.query_one("#single-task", SingleTaskWidget)
            select = panel.query_one("#task-select", Select)
            with patch.object(
                app, "action_refresh_today", new_callable=AsyncMock
            ) as mock_refresh, patch.object(
                app, "action_run_single_task", new_callable=AsyncMock
            ) as mock_single:
                await panel.on_select_changed(
                    Select.Changed(select, "__refresh_today__")
                )
            mock_refresh.assert_awaited_once()
            mock_single.assert_not_called()
            # 处理后依旧重置回提示状态
            assert select.is_blank()


@pytest.mark.asyncio
async def test_select_changed_normal_task_and_separator_routing_regression():
    """普通任务仍走单任务路径；分隔符两条路径都不触发。"""
    from textual.widgets import Select

    from tui import SingleTaskWidget

    app = PipelineApp()
    with patch("tui.find_running_pipeline_processes", return_value=[]):
        async with app.run_test():
            panel = app.query_one("#single-task", SingleTaskWidget)
            select = panel.query_one("#task-select", Select)
            with patch.object(
                app, "action_refresh_today", new_callable=AsyncMock
            ) as mock_refresh, patch.object(
                app, "action_run_single_task", new_callable=AsyncMock
            ) as mock_single:
                await panel.on_select_changed(Select.Changed(select, "update_bars"))
                mock_single.assert_awaited_once_with("update_bars")
                mock_refresh.assert_not_called()

                mock_single.reset_mock()
                await panel.on_select_changed(Select.Changed(select, "__sep__行情"))
                mock_single.assert_not_called()
                mock_refresh.assert_not_called()


# ===========================================================================
# LogsWidget: colorize_line / copy_recent_logs
# ===========================================================================
def test_colorize_line_preserves_equals_signs():
    """日志内容不得被篡改：= 不得被替换成 -（如 task=update_bars）。"""
    from tui import LogsWidget
    w = LogsWidget()
    out = w.colorize_line("2026-08-01 12:00:00 | INFO | task=update_bars saved=100")
    assert "task=update_bars" in out
    assert "saved=100" in out


def test_colorize_line_levels():
    from tui import LogsWidget
    w = LogsWidget()
    assert "❌" in w.colorize_line("ts | ERROR | boom")
    assert "⚠️" in w.colorize_line("ts | WARNING | careful")
    assert "✅" in w.colorize_line("ts | SUCCESS | done")


def test_copy_recent_logs_reads_bound_file(tmp_path):
    from tui import LogsWidget
    log = tmp_path / "x.log"
    log.write_text("line1\nline2\nline3\n")
    w = LogsWidget()
    w.active_log = str(log)
    assert w.copy_recent_logs(2) == "line2\nline3\n"


def test_copy_recent_logs_without_file_returns_string():
    from tui import LogsWidget
    w = LogsWidget()
    w.active_log = "/nonexistent/x.log"
    assert isinstance(w.copy_recent_logs(), str)


# ===========================================================================
# get_recent_failed_tasks
# ===========================================================================
def test_get_recent_failed_tasks_filters_and_orders(tmp_path):
    from tui import get_recent_failed_tasks
    db_file = tmp_path / "t.db"
    conn = sqlite3.connect(str(db_file))
    conn.execute(
        "CREATE TABLE ingestion_runs (run_id TEXT PRIMARY KEY, task_name TEXT,"
        " status TEXT, finished_at TEXT, error_kind TEXT, error_message TEXT)"
    )
    conn.executemany(
        "INSERT INTO ingestion_runs VALUES (?,?,?,?,?,?)",
        [
            ("1", "update_bars", "success", "2026-08-01T10:00:00", None, None),
            ("2", "update_bars", "failed", "2026-08-01T11:00:00", "internal", "boom"),
            ("3", "update_indicators", "degraded", "2026-08-01T12:00:00", "data_quality", "partial"),
            ("4", "retry", "aborted", "2026-08-01T09:00:00", None, "circuit"),
        ],
    )
    conn.commit()
    conn.close()
    result = get_recent_failed_tasks(str(db_file))
    # success 行被过滤；按 finished_at 倒序
    assert [r["task_name"] for r in result] == ["update_indicators", "update_bars", "retry"]
    assert result[0]["error_message"] == "partial"


def test_get_recent_failed_tasks_missing_table(tmp_path):
    from tui import get_recent_failed_tasks
    db_file = tmp_path / "t.db"
    conn = sqlite3.connect(str(db_file))
    conn.execute("CREATE TABLE x (a TEXT)")
    conn.commit()
    conn.close()
    assert get_recent_failed_tasks(str(db_file)) == []


def test_get_recent_failed_tasks_no_db():
    from tui import get_recent_failed_tasks
    assert get_recent_failed_tasks("/nonexistent/x.db") == []


# ===========================================================================
# _notify_and_log / _run_and_report
# ===========================================================================
def test_notify_and_log_writes_notify_and_logs_widget():
    """关键结果既弹通知也写 Logs 面板（通知几秒即逝，面板可回看）。"""
    app = PipelineApp()
    logs = MagicMock()
    with patch.object(app, "notify") as mock_notify, \
         patch.object(app, "query_one", return_value=logs):
        app._notify_and_log("hello-result", severity="warning")
    mock_notify.assert_called_once()
    logs.write.assert_called_once()
    assert "hello-result" in logs.write.call_args.args[0]


def test_notify_and_log_tolerates_missing_widget():
    """Logs 面板未挂载时只弹通知，不抛异常。"""
    app = PipelineApp()
    with patch.object(app, "notify") as mock_notify, \
         patch.object(app, "query_one", side_effect=Exception("not mounted")):
        app._notify_and_log("hello")
    mock_notify.assert_called_once()


@pytest.mark.asyncio
async def test_run_and_report_failure_lists_failed_tasks():
    """运行失败时报告退出码并列出失败任务名（来自审计表）。"""
    app = PipelineApp()
    async with app.run_test():
        with patch.object(app, "_run_in_background", new=AsyncMock(return_value=1)), \
             patch("tui.get_recent_failed_tasks", return_value=[
                 {"task_name": "update_bars", "status": "failed",
                  "finished_at": "t", "error_kind": "", "error_message": "boom"},
             ]), \
             patch.object(app, "_notify_and_log") as mock_nal:
            rc = await app._run_and_report("全量更新", "python", "daily_pipeline.py")
        assert rc == 1
        msg = mock_nal.call_args.args[0]
        assert "update_bars" in msg
        assert mock_nal.call_args.kwargs.get("severity") == "error"


@pytest.mark.asyncio
async def test_run_and_report_success_notifies_info():
    app = PipelineApp()
    async with app.run_test():
        with patch.object(app, "_run_in_background", new=AsyncMock(return_value=0)), \
             patch.object(app, "_notify_and_log") as mock_nal:
            rc = await app._run_and_report("全量更新", "python", "daily_pipeline.py")
        assert rc == 0
        assert mock_nal.call_args.kwargs.get("severity") == "information"


# ===========================================================================
# ProgressWidget 失败明细
# ===========================================================================
def test_progress_widget_shows_failed_symbols_inline():
    """失败队列前 5 只内联展示，超出显示「等 N 只」。"""
    from tui import ProgressWidget
    w = ProgressWidget()
    progress = {
        "task": "update_bars", "processed": 10, "total": 100,
        "last_symbol": "000001.SZ",
        "failed_queue": [f"{i:06d}.SZ" for i in range(7)],
    }
    captured: dict[str, str] = {}
    with patch("tui.parse_progress", return_value=progress), \
         patch.object(w, "update", side_effect=lambda t: captured.setdefault("text", t)), \
         patch.object(w, "add_class"), patch.object(w, "remove_class"):
        w.update_progress()
    assert "000004.SZ" in captured["text"]
    assert "000006.SZ" not in captured["text"]
    assert "等 7 只" in captured["text"]


# ===========================================================================
# 弹窗键盘一致性
# ===========================================================================
def test_confirm_stop_screen_escape_cancels():
    from tui import ConfirmStopScreen
    screen = ConfirmStopScreen([])
    with patch.object(screen, "dismiss") as mock_dismiss:
        screen.on_key(MagicMock(key="escape"))
    mock_dismiss.assert_called_once_with(False)


def test_confirm_run_screen_keyboard_shortcuts():
    from tui import ConfirmRunScreen
    screen = ConfirmRunScreen("测试")
    for key, expected in (("y", "run-now"), ("l", "run-later"), ("escape", "cancel")):
        with patch.object(screen, "dismiss") as mock_dismiss:
            screen.on_key(MagicMock(key=key))
        mock_dismiss.assert_called_once_with(expected)


def test_log_cleanup_screen_escape_cancels():
    screen = LogCleanupScreen()
    with patch.object(screen, "dismiss") as mock_dismiss:
        screen.on_key(MagicMock(key="escape"))
    mock_dismiss.assert_called_once_with("cancel")


# ===========================================================================
# 延迟调度去重
# ===========================================================================
@pytest.mark.asyncio
async def test_run_or_schedule_run_later_dedupes_same_action():
    """同一动作重复选择「稍后运行」不得重复排队。"""
    app = PipelineApp()
    async with app.run_test():
        args = ("python", "test_script.py")
        with patch.object(app, "push_screen") as mock_push_screen:
            app._run_or_schedule("测试任务", *args)
            _, callback = mock_push_screen.call_args[0]
            with patch("tui._seconds_until_safe", return_value=3600), \
                 patch.object(app, "_create_background_task") as mock_bg, \
                 patch.object(app, "notify") as mock_notify:
                callback("run-later")
                callback("run-later")
                assert mock_bg.call_count == 1
                assert "已在延迟队列" in mock_notify.call_args.args[0]


# ===========================================================================
# 每周补全 / 每月修复入口路由
# ===========================================================================
@pytest.mark.asyncio
async def test_action_weekly_backfill_routes_to_schedule():
    app = PipelineApp()
    args_expected = (sys.executable,
                     str(_PROJECT_ROOT / "daily_pipeline.py"),
                     "--task", "weekly_backfill")
    with patch.object(app, "_run_or_schedule") as mock_sched:
        await app.action_weekly_backfill()
    mock_sched.assert_called_once_with("每周补全", *args_expected)


@pytest.mark.asyncio
async def test_action_monthly_repair_routes_to_schedule():
    app = PipelineApp()
    args_expected = (sys.executable,
                     str(_PROJECT_ROOT / "daily_pipeline.py"),
                     "--task", "monthly_repair")
    with patch.object(app, "_run_or_schedule") as mock_sched:
        await app.action_monthly_repair()
    mock_sched.assert_called_once_with("每月修复", *args_expected)


# ===========================================================================
# 单任务下拉成员与 TASK_GROUPS 一致性（防漂移）
# ===========================================================================
def test_single_task_dropdown_membership_matches_task_groups():
    """下拉任务集合必须等于 registry TASK_GROUPS 成员集合（标签可不同，成员不可漂移）。"""
    from core.task_registry import TASK_GROUPS
    from tui import SingleTaskWidget
    dropdown = {
        task
        for _, tasks in SingleTaskWidget._SINGLE_TASK_GROUPS
        for _, task in tasks
    }
    members = {t for tasks in TASK_GROUPS.values() for t in tasks}
    assert dropdown == members


def test_single_task_dropdown_contains_concept_board():
    """概念板块必须有单任务入口（2026-08-02 用户报告：无法强制更新）。"""
    from tui import SingleTaskWidget
    all_tasks = [
        task
        for _, tasks in SingleTaskWidget._SINGLE_TASK_GROUPS
        for _, task in tasks
    ]
    assert "update_concept_board" in all_tasks
    assert "update_concept_member" in all_tasks


def test_db_queries_cache_and_invalidation(tmp_path):
    import time

    from tui.services.db_queries import (
        clear_db_queries_cache,
        get_active_stock_count,
        get_all_table_counts,
    )

    clear_db_queries_cache()
    db_file = tmp_path / "test_counts.db"
    conn = sqlite3.connect(str(db_file))
    conn.execute("CREATE TABLE stock_list (ts_code TEXT)")
    conn.execute("INSERT INTO stock_list VALUES ('000001.SZ')")
    conn.commit()
    conn.close()

    assert get_active_stock_count(str(db_file)) == 1
    counts1 = get_all_table_counts(str(db_file), fast=False)
    assert counts1.get("stock_list") == 1

    time.sleep(0.01)
    conn = sqlite3.connect(str(db_file))
    conn.execute("INSERT INTO stock_list VALUES ('600000.SH')")
    conn.commit()
    conn.close()

    assert get_active_stock_count(str(db_file)) == 2
    counts2 = get_all_table_counts(str(db_file), fast=False)
    assert counts2.get("stock_list") == 2

