"""实时日志展示与滚动组件 (LogsWidget)。"""

from __future__ import annotations

import contextlib
import os
from pathlib import Path
from typing import TextIO

from rich.markup import escape
from textual.widgets import RichLog

from tui.config import LOGS_DIR_PATH
from tui.services.process import find_latest_log_file


class LogsWidget(RichLog):
    """实时日志面板：跟踪最新日志文件输出，支持高亮和最近内容提取。"""

    ALLOW_SELECT = True

    def __init__(self, *args, **kwargs) -> None:
        kwargs.setdefault("markup", True)
        kwargs.setdefault("max_lines", 1000)
        kwargs.setdefault("wrap", True)
        super().__init__(*args, **kwargs)

    def on_mount(self) -> None:
        self.border_title = "Live Logs"
        self.active_log: str | None = None
        self.file_handle: TextIO | None = None
        self.set_interval(1.0, self.tail_log)

    def on_unmount(self) -> None:
        if self.file_handle:
            with contextlib.suppress(Exception):
                self.file_handle.close()
            self.file_handle = None

    def copy_recent_logs(self, line_count: int = 500) -> str:
        """返回最近 N 行原始日志文本；优先从当前绑定的日志文件读取。"""
        if self.active_log and Path(self.active_log).exists():
            try:
                with open(self.active_log, encoding="utf-8", errors="ignore") as fh:
                    lines = fh.readlines()
                return "".join(lines[-line_count:])
            except Exception:
                pass
        # 兜底：读取控件中已渲染的文本
        try:
            texts = [str(line) for line in self.lines[-line_count:]]
            return "\n".join(texts)
        except Exception:
            return ""

    def colorize_line(self, line: str) -> str:
        line = line.strip()
        parts = line.split("|", 2)
        if len(parts) >= 3:
            level = parts[1].strip()
            body = parts[2].strip()

            body = escape(body)

            if "ERROR" in level:
                return f"[red]❌ {body}[/red]"
            elif "WARN" in level or "WARNING" in level:
                return f"[yellow]⚠️  {body}[/yellow]"
            elif "SUCCESS" in level:
                return f"[bold green]✅ {body}[/bold green]"
            elif "INFO" in level:
                return f"[#e2e8f0]{body}[/#e2e8f0]"
            return body
        else:
            return escape(line)

    def tail_log(self) -> None:
        try:
            latest = find_latest_log_file(str(LOGS_DIR_PATH))
            if not latest:
                return

            if latest != self.active_log:
                # 先读完旧文件剩余内容，防止日志轮转时丢失最后一批输出
                if self.file_handle:
                    try:
                        remaining = self.file_handle.readlines()
                        for line in remaining:
                            self.write(self.colorize_line(line))
                    finally:
                        self.file_handle.close()
                fh = open(latest, encoding="utf-8", errors="ignore")  # noqa: SIM115
                # Seek to end on open
                fh.seek(0, os.SEEK_END)
                self.file_handle = fh
                self.active_log = latest
                self.write(f"--- Bound to log: {os.path.basename(latest)} ---")

            if self.file_handle:
                lines = self.file_handle.readlines()
                for line in lines:
                    self.write(self.colorize_line(line))
        except Exception as e:
            self.write(f"[red]Error tailing log: {escape(str(e))}[/red]")
