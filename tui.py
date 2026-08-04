"""SmartMoney Pipeline Textual TUI entry point & backward-compatibility facade.

This module re-exports everything from the modular `tui` package for backward
compatibility with existing callers and tests.
"""

from core.freshness import (
    DELAYED_PUBLISH_TABLES,
    MONTHLY_TABLES,
    QUARTERLY_TABLES,
    WEEKLY_TABLES,
    get_daily_bars_coverage,
    get_latest_dates,
    status_for_table,
)
from core.freshness import date_status as _date_status
from core.freshness import normalize_date as _normalize_date
from core.task_registry import (
    CATCH_UP_TASK_ORDER as _CATCH_UP_TASK_ORDER,
)
from core.task_registry import (
    TABLE_LABELS,
    TABLE_LABELS_CN,
    TASK_GROUPS,
    lookup_task,
    panel_date_columns,
    refreshable_trading_tasks,
    task_to_table,
)
from tui.app import PipelineApp
from tui.config import (
    _SHANGHAI_TZ,
    DAEMON_PID_PATH,
    DEFAULT_DB_PATH,
    DEFAULT_THEME,
    LOGS_DIR_PATH,
    PIPELINE_PID_PATH,
    PROGRESS_JSON_PATH,
    TUI_CONFIG_PATH,
    WATCHLIST_DIR,
    load_theme,
    save_theme,
)
from tui.screens import (
    ConfirmRefreshTodayScreen,
    ConfirmRunScreen,
    ConfirmStopScreen,
    CopyPanelScreen,
    HelpScreen,
    LogCleanupScreen,
)
from tui.services import (
    _HEALTHY_STATUSES,
    NO_DATE_TABLES,
    REFRESH_STATE_LABELS,
    TABLE_DATE_COLUMNS,
    WatchlistSyncResult,
    _code_to_ts_code,
    _ljust_vis,
    _seconds_until_safe,
    _vis_width,
    classify_refresh_task_state,
    find_latest_log_file,
    find_running_pipeline_processes,
    format_chinese_magnitude,
    format_count,
    format_refresh_summary,
    get_active_stock_count,
    get_all_table_counts,
    get_daemon_status,
    get_db_size,
    get_latest_refresh_run_id,
    get_latest_refresh_task_states,
    get_launchd_status,
    get_recent_failed_tasks,
    get_subprocess_env,
    parse_progress,
    sync_watchlists_from_files,
)
from tui.widgets import (
    _SINGLE_TASK_GROUP_NAMES,
    _SINGLE_TASK_LABELS,
    DashboardWidget,
    DataCompletenessWidget,
    LogsWidget,
    ProgressWidget,
    SingleTaskWidget,
    TaskGroupWidget,
    _build_single_task_groups,
)

__all__ = [
    "ConfirmRefreshTodayScreen",
    "ConfirmRunScreen",
    "ConfirmStopScreen",
    "CopyPanelScreen",
    "DAEMON_PID_PATH",
    "DEFAULT_DB_PATH",
    "DEFAULT_THEME",
    "DELAYED_PUBLISH_TABLES",
    "DashboardWidget",
    "DataCompletenessWidget",
    "HelpScreen",
    "LOGS_DIR_PATH",
    "LogCleanupScreen",
    "LogsWidget",
    "MONTHLY_TABLES",
    "NO_DATE_TABLES",
    "PIPELINE_PID_PATH",
    "PROGRESS_JSON_PATH",
    "PipelineApp",
    "ProgressWidget",
    "QUARTERLY_TABLES",
    "REFRESH_STATE_LABELS",
    "SingleTaskWidget",
    "TABLE_DATE_COLUMNS",
    "TABLE_LABELS",
    "TABLE_LABELS_CN",
    "TASK_GROUPS",
    "TUI_CONFIG_PATH",
    "TaskGroupWidget",
    "WATCHLIST_DIR",
    "WEEKLY_TABLES",
    "WatchlistSyncResult",
    "_CATCH_UP_TASK_ORDER",
    "_HEALTHY_STATUSES",
    "_SHANGHAI_TZ",
    "_SINGLE_TASK_GROUP_NAMES",
    "_SINGLE_TASK_LABELS",
    "_build_single_task_groups",
    "_code_to_ts_code",
    "_date_status",
    "_ljust_vis",
    "_normalize_date",
    "_seconds_until_safe",
    "_vis_width",
    "classify_refresh_task_state",
    "find_latest_log_file",
    "find_running_pipeline_processes",
    "format_chinese_magnitude",
    "format_count",
    "format_refresh_summary",
    "get_active_stock_count",
    "get_all_table_counts",
    "get_daily_bars_coverage",
    "get_daemon_status",
    "get_db_size",
    "get_latest_dates",
    "get_latest_refresh_run_id",
    "get_latest_refresh_task_states",
    "get_launchd_status",
    "get_recent_failed_tasks",
    "get_subprocess_env",
    "load_theme",
    "lookup_task",
    "main",
    "panel_date_columns",
    "parse_progress",
    "refreshable_trading_tasks",
    "save_theme",
    "status_for_table",
    "sync_watchlists_from_files",
    "task_to_table",
]


def main() -> None:
    """TUI CLI 入口点。"""
    app = PipelineApp()
    app.run()


if __name__ == "__main__":
    main()
