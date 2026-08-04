"""抓取/执行进度展示组件 (ProgressWidget)。"""

from __future__ import annotations

import asyncio

from rich.markup import escape
from textual.widgets import Static

import tui
from tui.config import PROGRESS_JSON_PATH
from tui.services.process import parse_progress


class ProgressWidget(Static):
    """显示当前正在执行任务的进度条与失败详情。"""

    def on_mount(self) -> None:
        self.border_title = "Progress"
        self.update_progress()
        self.set_interval(2.0, self._update_progress_async)

    async def _update_progress_async(self) -> None:
        """异步读取 progress.json 并更新显示，避免阻塞事件循环。"""
        parser = getattr(tui, "parse_progress", parse_progress)
        progress = await asyncio.to_thread(parser, str(PROGRESS_JSON_PATH))
        self._render_progress(progress)

    def update_progress(self) -> None:
        """同步入口（供挂载时初次调用和测试使用）。"""
        parser = getattr(tui, "parse_progress", parse_progress)
        progress = parser(str(PROGRESS_JSON_PATH))
        self._render_progress(progress)

    def _render_progress(self, progress: dict | None) -> None:
        """根据 progress 字典渲染进度面板。"""
        if not progress:
            self.update(" [dim]当前无运行中的任务[/dim]")
            self.remove_class("active-task")
            return

        processed = progress.get("processed", 0)
        total = progress.get("total", 0)
        last_symbol = progress.get("last_symbol", "")
        failed_queue = progress.get("failed_queue", [])
        failed_count = len(failed_queue)

        pct = (processed / total * 100) if total > 0 else 0
        bar_length = 20
        filled = (
            min(bar_length, max(0, int(bar_length * processed / total)))
            if total > 0
            else 0
        )
        bar = "█" * filled + "░" * (bar_length - filled)

        # 失败明细内联：前 5 只列出，超出显示「等 N 只」，避免只见计数不见对象
        failed_detail = ""
        if failed_count:
            shown = ", ".join(str(s) for s in failed_queue[:5])
            suffix = f" 等 {failed_count} 只" if failed_count > 5 else ""
            failed_detail = f"\n [dim]失败明细：[/dim][red]{escape(shown)}{suffix}[/red]"

        text = (
            f" [yellow]{progress.get('task')}[/yellow]  "
            f"[bold #e2e8f0]{pct:.1f}%[/bold #e2e8f0] "
            f"([cyan]{processed}[/cyan]/[cyan]{total}[/cyan])  "
            f"[bold #c084fc]{bar}[/bold #c084fc]\n"
            f" [dim]当前股票：[/dim][cyan]{last_symbol}[/cyan]  "
            f"[dim]失败：[/dim][bold red]{failed_count}[/bold red]"
            f"{failed_detail}\n"
        )
        self.update(text)
        self.add_class("active-task")
