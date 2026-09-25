"""检测到后台进程时的确认终止弹窗。"""

from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Label

_MAX_CMD_WIDTH = 48


def _short_command(cmd: str) -> str:
    """超长命令行截断：保留解释器与脚本 basename + 末参，避免 80 列弹窗溢出。"""
    if len(cmd) <= _MAX_CMD_WIDTH:
        return cmd
    parts = cmd.split()
    if not parts:
        return cmd[: _MAX_CMD_WIDTH - 1] + "…"
    names = [p.rsplit("/", 1)[-1] for p in parts]
    # 日志里脚本名比解释器路径更有辨识度，优先保 daily_pipeline.py 这类名字
    core = " ".join(names)
    if len(core) <= _MAX_CMD_WIDTH:
        return core
    if len(names) == 1:
        # 防 names[1] IndexError（单段超长无空格命令）
        return names[0][: _MAX_CMD_WIDTH - 1] + "…"
    short = f"{names[0]} {names[1]} … {names[-1]}"
    if len(short) > _MAX_CMD_WIDTH:
        return short[: _MAX_CMD_WIDTH - 1] + "…"
    return short


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
                    f"  PID {p['pid']}  |  {p['elapsed']}  |  {_short_command(str(p['command']))}"
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
