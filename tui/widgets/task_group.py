"""任务分组执行面板 (TaskGroupWidget)。"""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from textual.app import ComposeResult
from textual.containers import Grid
from textual.widgets import Button, Static

if TYPE_CHECKING:
    from tui.app import PipelineApp


class TaskGroupWidget(Static):
    """任务分组面板：一键顺序执行同组任务。"""

    GROUP_LABELS: dict[str, str] = {
        "core": "核心行情",
        "fund": "资金面",
        "valuation": "估值/财务",
        "macro": "宏观/全球",
        "sector_index": "行业/大盘",
        "derivatives": "ETF/可转债/港通",
        "events": "事件信号",
    }

    def compose(self) -> ComposeResult:
        with Grid(id="group-buttons"):
            for key, label in self.GROUP_LABELS.items():
                yield Button(label, id=f"group-{key}", variant="primary")
            yield Button("⚡ 补齐缺失", id="group-catchup", variant="warning")

    async def on_button_pressed(self, event: Button.Pressed) -> None:
        button_id = event.button.id
        if button_id is None:
            return
        group_key = button_id.replace("group-", "")
        app = cast("PipelineApp", self.app)
        if group_key == "catchup":
            await app.action_run_catch_up()
            return
        await app.action_run_task_group(group_key)
