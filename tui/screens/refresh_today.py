"""收盘刷新确认弹窗。"""

from __future__ import annotations

from datetime import datetime

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Input, Label

from core.calendar import get_expected_latest_trading_day
from core.task_registry import refreshable_trading_tasks
from tui.config import _SHANGHAI_TZ


class ConfirmRefreshTodayScreen(ModalScreen[str | None]):
    """弹窗：确认收盘刷新。

    显示上海目标交易日、刷新任务范围（28 个交易日任务）与可选股票范围；
    确认返回股票范围字符串（可为空），取消返回 None。
    16:00 安全闸门由 CLI 负责，弹窗不重复实现。
    """

    def __init__(self) -> None:
        super().__init__()
        # 与 CLI 共用同一计算：上海时区 aware now + get_expected_latest_trading_day
        import sys

        tui_mod = sys.modules.get("tui")
        exp_fn = (
            getattr(tui_mod, "get_expected_latest_trading_day", get_expected_latest_trading_day)
            if tui_mod
            else get_expected_latest_trading_day
        )
        self._target_date = exp_fn(now=datetime.now(_SHANGHAI_TZ))
        self._task_count = len(refreshable_trading_tasks())

    def compose(self) -> ComposeResult:
        with Vertical(id="refresh-dialog"):
            yield Label("[bold]收盘刷新确认[/bold]")
            yield Label("")
            yield Label(f"目标交易日（上海时间）: [yellow]{self._target_date}[/yellow]")
            yield Label(f"刷新范围: 全部 {self._task_count} 个交易日任务")
            yield Label("[dim]可选股票范围（逗号分隔，留空为全市场）[/dim]")
            yield Input(placeholder="如 600000,000001（留空=全市场）", id="refresh-symbols")
            with Horizontal(id="refresh-buttons"):
                yield Button("确认刷新", variant="primary", id="refresh-confirm")
                yield Button("取消", variant="error", id="refresh-cancel")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "refresh-confirm":
            self.dismiss(self.query_one("#refresh-symbols", Input).value.strip())
        else:
            self.dismiss(None)

    def on_key(self, event) -> None:
        if event.key == "escape":
            self.dismiss(None)
