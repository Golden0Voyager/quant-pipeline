"""任务执行确认弹窗（立即/稍后/取消）。"""

from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Label


class ConfirmRunScreen(ModalScreen[str]):
    """弹窗：确认是否启动任务，可选择立即、稍后或取消。"""

    def __init__(self, action_name: str) -> None:
        super().__init__()
        self._action_name = action_name

    def compose(self) -> ComposeResult:
        with Vertical(id="confirm-dialog"):
            yield Label(f"[bold]是否启动「{self._action_name}」？[/bold]")
            yield Label("")
            with Horizontal(id="confirm-buttons"):
                yield Button("立即运行 (Y)", variant="primary", id="run-now")
                yield Button("稍后运行 (L)", variant="default", id="run-later")
                yield Button("取消 (Esc)", variant="error", id="cancel")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        self.dismiss(event.button.id)

    def on_key(self, event) -> None:
        if event.key.lower() == "y":
            self.dismiss("run-now")
        elif event.key.lower() == "l":
            self.dismiss("run-later")
        elif event.key == "escape":
            self.dismiss("cancel")
