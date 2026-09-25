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

    # 按钮文案压到 5 字内（测试钉住 ≤5；ETF/债 恰为 5 宽）：
    # 2 列布局下单格 ~13 列，CJK 过长易腰斩（核心行/情）。
    # 完整组名仍见 Single 任务下拉的分隔标题。
    GROUP_LABELS: dict[str, str] = {
        "core": "行情",
        "fund": "资金",
        "valuation": "估值",
        "macro": "宏观",
        "sector_index": "行业",
        "derivatives": "ETF/债",
        "events": "事件",
    }

    def compose(self) -> ComposeResult:
        with Grid(id="group-buttons"):
            for key, label in self.GROUP_LABELS.items():
                yield Button(label, id=f"group-{key}", variant="primary")
            yield Button("补齐缺失", id="group-catchup", variant="warning")

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
