"""检测到后台进程时的确认终止弹窗。"""

from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Label


class ConfirmStopScreen(ModalScreen[bool]):
    """弹窗：检测到后台进程，询问是否终止。"""

    def __init__(self, processes: list[dict[str, str | int]]) -> None:
        super().__init__()
        self._processes = processes

    def compose(self) -> ComposeResult:
        with Vertical(id="confirm-dialog"):
            yield Label("[bold red]⚠️  检测到后台进程[/bold red]")
            for p in self._processes:
                yield Label(
                    f"  PID {p['pid']}  |  已运行 {p['elapsed']}  |  {p['command']}"
                )
            yield Label("")
            yield Label("终止这些进程？")
            with Horizontal(id="confirm-buttons"):
                yield Button("终止并继续 (Y)", variant="error", id="stop-yes")
                yield Button("保留运行 (N)", variant="primary", id="stop-no")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        self.dismiss(event.button.id == "stop-yes")

    def on_key(self, event) -> None:
        if event.key.lower() == "y":
            self.dismiss(True)
        elif event.key.lower() == "n" or event.key == "escape":
            self.dismiss(False)
