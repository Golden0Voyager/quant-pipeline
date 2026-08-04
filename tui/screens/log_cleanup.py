"""日志清理选择弹窗。"""

from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import Grid, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Label


class LogCleanupScreen(ModalScreen[str]):
    """弹窗：选择日志清理策略。"""

    def compose(self) -> ComposeResult:
        with Vertical(id="logclean-dialog"):
            yield Label("[bold]清理日志文件[/bold]")
            yield Label("")
            yield Label("[dim]当前正在写入的活跃日志会被保留[/dim]")
            yield Label("")
            with Grid(id="logclean-buttons"):
                yield Button("全部清理", variant="error", id="all")
                yield Button("保留最近 7 天", variant="primary", id="keep-7")
                yield Button("保留最近 30 天", variant="default", id="keep-30")
                yield Button("取消 (Esc)", variant="default", id="cancel")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        self.dismiss(event.button.id)

    def on_key(self, event) -> None:
        if event.key == "escape":
            self.dismiss("cancel")
