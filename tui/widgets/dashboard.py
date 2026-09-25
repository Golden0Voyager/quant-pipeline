"""系统状态概览组件 (DashboardWidget)。"""

from __future__ import annotations

import asyncio
import time

from textual.widgets import Static

import tui
from tui.config import DAEMON_PID_PATH, DEFAULT_DB_PATH
from tui.services.db_queries import get_active_stock_count
from tui.services.formatting import get_db_size
from tui.services.process import get_daemon_status, get_launchd_status


# can_focus=True:让状态看板成为 AUTO_FOCUS="*" 按 DOM 顺序命中的第一个
# 可聚焦控件(它排在 LogsWidget 之前)。否则启动自动聚焦会落到可滚动的
# LogsWidget 上,Screen.focus 的 scroll_to_center 在窄屏(.narrow 溢出滚动)
# 下把 #main-grid 滚到上限,Dashboard 的边框标题("Dashboard")滚出视口 ——
# tests/test_tui_tmux.py::test_copy_panel_opens_and_closes 第 80 行因此失败。
# 看板位于内容顶端,聚焦它时滚动量恒为 0,标题始终可见。
class DashboardWidget(Static, can_focus=True):
    """显示数据库大小、股票数、守护进程及调度器状态的概览面板。"""

    async def on_mount(self) -> None:
        self.border_title = "Dashboard"
        # 缓存股票数量，避免每次刷新都查询数据库
        self._cached_stocks: int = 0
        self._last_stocks_update: float = 0.0
        self._stocks_cache_ttl: float = 60.0
        await self.update_status()
        self.set_interval(10.0, self.update_status)

    async def update_status(self) -> None:
        db_path = str(getattr(tui, "DEFAULT_DB_PATH", DEFAULT_DB_PATH))
        get_size_fn = getattr(tui, "get_db_size", get_db_size)
        db_size = get_size_fn(db_path)
        active_stocks = await self._get_active_stock_count_cached()
        daemon_getter = getattr(tui, "get_daemon_status", get_daemon_status)
        daemon_status, daemon_pid = await asyncio.to_thread(
            daemon_getter, DAEMON_PID_PATH
        )
        launchd_getter = getattr(tui, "get_launchd_status", get_launchd_status)
        launchd_active = await launchd_getter()

        daemon_str = (
            f"[bold green]Running[/bold green] [gray](PID: {daemon_pid})[/gray]"
            if daemon_status == "Running"
            else "[bold red]Stopped[/bold red]"
        )
        launchd_str = (
            "[bold green]Active[/bold green]"
            if launchd_active
            else "[bold red]Inactive[/bold red]"
        )

        # 标签压到 8 列内：80 列终端下左栏 ~27 列，长标签会把值挤到下一行
        # （5.47 / GB 分家、Stopped 孤行）。值+单位必须同行。
        text = (
            f" • [bold gray]DB      [/bold gray][cyan]{db_size}[/cyan]\n"
            f" • [bold gray]Stocks  [/bold gray][cyan]{active_stocks}[/cyan]\n"
            f" • [bold gray]Daemon  [/bold gray]{daemon_str}\n"
            f" • [bold gray]Sched   [/bold gray]{launchd_str}\n"
        )
        self.update(text)

    async def _get_active_stock_count_cached(self) -> int:
        now = time.time()
        if now - self._last_stocks_update > self._stocks_cache_ttl:
            db_path = str(getattr(tui, "DEFAULT_DB_PATH", DEFAULT_DB_PATH))
            count_getter = getattr(
                tui, "get_active_stock_count", get_active_stock_count
            )
            self._cached_stocks = await asyncio.to_thread(
                count_getter, db_path
            )
            self._last_stocks_update = now
        return self._cached_stocks
