"""快捷键帮助弹窗。"""

from __future__ import annotations

from typing import TYPE_CHECKING

from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Vertical
from textual.screen import ModalScreen
from textual.widgets import Label

if TYPE_CHECKING:
    pass


class HelpScreen(ModalScreen[None]):
    """弹窗：显示快捷键帮助。"""

    BINDINGS = [
        Binding("escape", "dismiss", "Close"),
        Binding("q", "dismiss", "Close"),
    ]

    def compose(self) -> ComposeResult:
        with Vertical(id="help-dialog"):
            yield Label("[bold]快捷键帮助[/bold]")
            yield Label("")
            # 从 app.BINDINGS 动态生成，避免手动维护漂移
            bindings = getattr(self.app, "BINDINGS", [])
            for binding in bindings:
                if binding.key in ("ctrl+c", "q"):
                    continue
                show_marker = "" if binding.show else " [dim](隐藏)[/dim]"
                yield Label(
                    f"[bold]{binding.key.upper()}[/bold] — {binding.description}{show_marker}"
                )
            yield Label("")
            yield Label("[bold]Ctrl+C / Q[/bold] — Quit")
            yield Label("")
            yield Label("[dim]按 Esc 或 Q 关闭[/dim]")

    async def action_dismiss(self, result: None = None) -> None:
        self.dismiss(result)
