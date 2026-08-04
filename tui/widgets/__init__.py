"""TUI 页面组件导出。"""

from tui.widgets.completeness import DataCompletenessWidget
from tui.widgets.dashboard import DashboardWidget
from tui.widgets.logs import LogsWidget
from tui.widgets.progress import ProgressWidget
from tui.widgets.single_task import (
    _SINGLE_TASK_GROUP_NAMES,
    _SINGLE_TASK_LABELS,
    SingleTaskWidget,
    _build_single_task_groups,
)
from tui.widgets.task_group import TaskGroupWidget

__all__ = [
    "DashboardWidget",
    "DataCompletenessWidget",
    "LogsWidget",
    "ProgressWidget",
    "SingleTaskWidget",
    "TaskGroupWidget",
    "_SINGLE_TASK_GROUP_NAMES",
    "_SINGLE_TASK_LABELS",
    "_build_single_task_groups",
]
