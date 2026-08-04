import asyncio
import contextlib
import glob
import json
import logging
import os
import signal
import sqlite3
import subprocess
import sys
import time
import unicodedata
from datetime import datetime, timedelta
from pathlib import Path
from typing import Literal, NamedTuple, TextIO, cast
from zoneinfo import ZoneInfo

from rich.markup import escape
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Grid, Horizontal, Vertical, VerticalScroll
from textual.screen import ModalScreen
from textual.widgets import (
    Button,
    Footer,
    Header,
    Input,
    Label,
    ListItem,
    ListView,
    RichLog,
    Select,
    Static,
    TabbedContent,
    TabPane,
)

from core.calendar import get_expected_latest_trading_day
from core.freshness import (
    DELAYED_PUBLISH_TABLES,  # noqa: F401  # re-export，兼容 from tui import
    MONTHLY_TABLES,  # noqa: F401  # re-export，兼容 from tui import
    QUARTERLY_TABLES,  # noqa: F401  # re-export，兼容 from tui import
    WEEKLY_TABLES,  # noqa: F401  # re-export，兼容 from tui import
    compute_catch_up_tasks,
    get_daily_bars_coverage,
    get_latest_dates,
    status_for_table,
)
from core.freshness import date_status as _date_status  # noqa: F401  # re-export，兼容 from tui import
from core.freshness import normalize_date as _normalize_date  # noqa: F401  # re-export，兼容 from tui import
from core.log_cleanup import cleanup_logs
from core.task_registry import (
    CATCH_UP_TASK_ORDER as _CATCH_UP_TASK_ORDER,  # noqa: F401  # re-export，兼容 from tui import
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

logger = logging.getLogger(__name__)

# 共享 CSS 变量：蓝/玫瑰主题色，供 PipelineApp 与各 ModalScreen 共用
_SHARED_CSS = """
$blue-normal: #1d4ed8;
$blue-hover: #3b82f6;
$blue-focus: #60a5fa;

$rose-normal: #be123c;
$rose-hover: #e11d48;
$rose-focus: #fb7185;
"""

DEFAULT_DB_PATH = Path.home() / "Code/quant_data/quant_core.db"
DAEMON_PID_PATH = "/tmp/smartmoney_daemon.pid"
PIPELINE_PID_PATH = "/tmp/daily_pipeline.pid"
PROGRESS_JSON_PATH = Path.home() / "Code/quant_data/progress.json"
LOGS_DIR_PATH = Path.home() / "Code/quant_data/logs"
WATCHLIST_DIR = Path.home() / "Code/quant_agents/watchlists"
TUI_CONFIG_PATH = Path(
    os.environ.get("QUANT_TUI_CONFIG_PATH", Path.home() / ".config/quant_pipeline/tui.json")
)

# 收盘刷新的目标交易日按上海时区计算，与 CLI（Task 5）保持一致
_SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")


def _load_tui_config() -> dict:
    """加载 TUI 配置（主题等）。"""
    try:
        if TUI_CONFIG_PATH.exists():
            return json.loads(TUI_CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}


def _save_tui_config(config: dict) -> None:
    """保存 TUI 配置。"""
    try:
        TUI_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
        TUI_CONFIG_PATH.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


def load_theme() -> str:
    """返回保存的主题名称，默认 dark。"""
    return _load_tui_config().get("theme", "textual-dark")


def save_theme(theme_name: str) -> None:
    """保存主题名称。"""
    config = _load_tui_config()
    config["theme"] = theme_name
    _save_tui_config(config)


def find_latest_log_file(logs_dir: str) -> str | None:
    files = glob.glob(os.path.join(logs_dir, "smartmoney_*.log"))
    if not files:
        daemon_log = os.path.join(logs_dir, "daemon.log")
        return daemon_log if os.path.exists(daemon_log) else None
    return max(files, key=os.path.getmtime)

def parse_progress(progress_path: str) -> dict | None:
    p = Path(progress_path)
    if not p.exists():
        return None
    try:
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def get_db_size(db_path: str) -> str:
    p = Path(db_path)
    if p.exists():
        bytes_size = p.stat().st_size
        mb = bytes_size / (1024 * 1024)
        if mb >= 1024:
            return f"{mb / 1024:.2f} GB"
        return f"{mb:.2f} MB"
    return "0.00 MB"

def get_daemon_status(pid_path: str) -> tuple[str, int | None]:
    p = Path(pid_path)
    if not p.exists():
        return "Stopped", None
    try:
        pid = int(p.read_text(encoding="utf-8").strip())
        os.kill(pid, 0)
        res = subprocess.run(["ps", "-p", str(pid), "-o", "command="], capture_output=True, text=True, timeout=2.0)
        if "daily_pipeline.py" in res.stdout or "daemon.py" in res.stdout:
            return "Running", pid
        return "Stopped", None
    except (ValueError, OSError, subprocess.SubprocessError):
        return "Stopped", None


def find_running_pipeline_processes(
    skip_ppid_check: bool = False,
) -> list[dict[str, str | int]]:
    """查找所有正在运行的 daily_pipeline.py 进程（非 daemon）。

    Args:
        skip_ppid_check: 为 True 时不过滤 TUI 子进程（用于 X 键停止场景）。
    """
    processes: list[dict[str, str | int]] = []
    # 1. 检查 pidfile
    pidfile = Path(PIPELINE_PID_PATH)
    if pidfile.exists():
        try:
            pid = int(pidfile.read_text().strip())
            os.kill(pid, 0)
            res = subprocess.run(
                ["ps", "-p", str(pid), "-o", "pid=,etime=,command="],
                capture_output=True, text=True, timeout=2.0,
            )
            if "daily_pipeline.py" in res.stdout and "python" in res.stdout.lower():
                parts = res.stdout.strip().split(None, 2)
                processes.append({
                    "pid": int(parts[0]),
                    "elapsed": parts[1] if len(parts) > 1 else "unknown",
                    "command": parts[2] if len(parts) > 2 else "daily_pipeline.py",
                })
        except (ValueError, OSError, subprocess.SubprocessError):
            pass

    # 2. 扫描所有 daily_pipeline.py 进程（兜底，覆盖 pidfile 之前的旧进程）
    try:
        res = subprocess.run(
            ["pgrep", "-f", "daily_pipeline\\.py"],
            capture_output=True, text=True, timeout=2.0,
        )
        for line in res.stdout.strip().splitlines():
            pid = int(line.strip())
            # 跳过已在列表中的进程
            if any(p["pid"] == pid for p in processes):
                continue
            # 跳过 TUI 自身的子进程（ppid 是 TUI）
            # X 键停止场景（skip_ppid_check=True）不过滤，确保能杀 TUI 子进程
            if not skip_ppid_check:
                try:
                    ppid_res = subprocess.run(
                        ["ps", "-p", str(pid), "-o", "ppid="],
                        capture_output=True, text=True, timeout=2.0,
                    )
                    ppid = int(ppid_res.stdout.strip())
                    if ppid == os.getpid():
                        continue
                except Exception:
                    continue
            res2 = subprocess.run(
                ["ps", "-p", str(pid), "-o", "pid=,etime=,command="],
                capture_output=True, text=True, timeout=2.0,
            )
            if res2.stdout.strip():
                parts = res2.stdout.strip().split(None, 2)
                command = parts[2] if len(parts) > 2 else ""
                # pgrep -f 匹配整条命令行：vim/tail/grep 等打开过该文件的进程
                # 也会命中，只认 python 解释器启动的管道进程，避免误报/误杀
                if "daily_pipeline.py" not in command or "python" not in command.lower():
                    continue
                processes.append({
                    "pid": int(parts[0]),
                    "elapsed": parts[1] if len(parts) > 1 else "unknown",
                    "command": command,
                })
    except (ValueError, OSError, subprocess.SubprocessError):
        pass

    return processes


class ConfirmStopScreen(ModalScreen[bool]):
    """弹窗：检测到后台进程，询问是否终止。"""

    CSS = """
    ConfirmStopScreen {
        align: center middle;
        background: $background 60%;
    }
    #confirm-dialog {
        width: 70;
        height: auto;
        max-height: 20;
        background: $surface;
        border: round $error;
        padding: 1 2;
    }
    #confirm-dialog Label {
        width: 100%;
    }
    #confirm-buttons {
        margin-top: 1;
        width: 100%;
        height: auto;
        align: center middle;
    }
    """

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


class ConfirmRunScreen(ModalScreen[str]):
    """弹窗：确认是否启动任务，可选择立即、稍后或取消。"""

    CSS = """
    ConfirmRunScreen {
        align: center middle;
        background: $background 60%;
    }
    #confirm-dialog {
        width: 66;
        height: auto;
        max-height: 14;
        background: $surface;
        border: round $primary;
        padding: 1 2;
    }
    #confirm-dialog Label {
        width: 100%;
        text-align: center;
    }
    #confirm-buttons {
        margin-top: 1;
        width: 100%;
        height: auto;
        align: center middle;
    }
    """

    def __init__(self, action_name: str) -> None:
        super().__init__()
        self._action_name = action_name

    def compose(self) -> ComposeResult:
        with Vertical(id="confirm-dialog"):
            yield Label("[bold]确认运行[/bold]")
            yield Label("")
            yield Label(f"[yellow]{self._action_name}[/yellow]")
            yield Label("")
            with Horizontal(id="confirm-buttons"):
                yield Button("立即运行 (Y)", variant="primary", id="run-now")
                yield Button("稍后自动运行 (L)", variant="default", id="run-later")
                yield Button("取消 (Esc)", variant="error", id="cancel")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        self.dismiss(event.button.id)

    def on_key(self, event) -> None:
        key = event.key.lower()
        if key == "y":
            self.dismiss("run-now")
        elif key == "l":
            self.dismiss("run-later")
        elif key in ("escape", "n"):
            self.dismiss("cancel")


class ConfirmRefreshTodayScreen(ModalScreen[str | None]):
    """弹窗：确认收盘刷新。

    显示上海目标交易日、刷新任务范围（28 个交易日任务）与可选股票范围；
    确认返回股票范围字符串（可为空），取消返回 None。
    16:00 安全闸门由 CLI 负责，弹窗不重复实现。
    """

    CSS = """
    ConfirmRefreshTodayScreen {
        align: center middle;
        background: $background 60%;
    }
    #refresh-dialog {
        width: 70;
        height: auto;
        max-height: 18;
        background: $surface;
        border: round $primary;
        padding: 1 2;
    }
    #refresh-dialog Label {
        width: 100%;
        text-align: center;
    }
    #refresh-symbols {
        width: 100%;
        margin-top: 1;
    }
    #refresh-buttons {
        margin-top: 1;
        width: 100%;
        height: auto;
        align: center middle;
    }
    """

    def __init__(self) -> None:
        super().__init__()
        # 与 CLI 共用同一计算：上海时区 aware now + get_expected_latest_trading_day
        self._target_date = get_expected_latest_trading_day(
            now=datetime.now(_SHANGHAI_TZ)
        )
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


class CopyPanelScreen(ModalScreen[str]):
    """弹窗：选择要复制的面板。"""

    CSS = _SHARED_CSS + """
    CopyPanelScreen {
        align: center middle;
        background: $background 30%;
    }
    #copy-dialog {
        width: 50;
        height: auto;
        background: $surface 95%;
        border: round $primary;
        padding: 1 2;
    }
    #copy-dialog > Label {
        width: 100%;
        text-align: center;
        margin-bottom: 0;
        color: $text;
        text-style: bold;
    }
    #copy-list {
        width: 100%;
        height: auto;
        border: none;
        background: transparent;
        padding: 0;
    }
    #copy-list ListView {
        width: 100%;
        height: auto;
        border: none;
        background: transparent;
        padding: 0;
    }
    #copy-list ListView > ListItem {
        width: 100%;
        height: auto;
        min-height: 1;
        padding: 0 1;
        margin: 0 0 1 0;
        border: none;
        background: transparent;
        color: $text;
        text-align: left;
    }
    #copy-list ListView > ListItem:hover {
        background: $blue-hover 25%;
    }
    #copy-list ListView > ListItem:focus {
        background: $blue-focus 35%;
        text-style: bold;
    }
    #copy-list ListView > ListItem > Label {
        width: 100%;
        text-align: left;
    }

    #copy-hint {
        width: 100%;
        text-align: center;
        margin-top: 0;
        color: $text;
        text-style: dim;
    }
    """

    PANELS: list[tuple[str, str, str]] = [
        ("data-completeness", "1", "📀 数据完整度"),
        ("status-dashboard", "2", "📊 Dashboard"),
        ("scraping-progress", "3", "📈 Progress"),
        ("live-logs", "4", "📋 日志"),
    ]

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._panel_ids: set[str] = {panel_id for panel_id, _key, _label in self.PANELS}
        self._key_map = {key: panel_id for panel_id, key, _label in self.PANELS}

    def compose(self) -> ComposeResult:
        with Vertical(id="copy-dialog"):
            yield Label("选择要复制的面板")
            with Vertical(id="copy-list"):
                list_items = []
                for panel_id, key, label in self.PANELS:
                    list_items.append(
                        ListItem(Label(f"[dim][{key}][/dim] {label}"), id=panel_id)
                    )
                yield ListView(*list_items, id="copy-list-view")
            yield Label("按 1-4 / Enter 选择 · Esc 取消", id="copy-hint")

    def on_list_view_selected(self, event: ListView.Selected) -> None:
        """处理 ListView 项选择。"""
        if event.item and event.item.id in self._panel_ids:
            self.dismiss(event.item.id)

    def on_key(self, event) -> None:
        if event.key in ("escape", "q"):
            self.dismiss(None)
            return
        if event.key in self._key_map:
            self.dismiss(self._key_map[event.key])


class HelpScreen(ModalScreen[None]):
    """弹窗：显示快捷键帮助。"""

    CSS = """
    HelpScreen {
        align: center middle;
        background: $background 60%;
    }
    #help-dialog {
        width: 70;
        height: auto;
        max-height: 24;
        background: $surface;
        border: round $primary;
        padding: 1 2;
    }
    #help-dialog Label {
        width: 100%;
    }
    """

    BINDINGS = [
        Binding("escape", "dismiss", "Close"),
        Binding("q", "dismiss", "Close"),
    ]

    def compose(self) -> ComposeResult:
        with Vertical(id="help-dialog"):
            yield Label("[bold]快捷键帮助[/bold]")
            yield Label("")
            # 从 PipelineApp.BINDINGS 动态生成，避免手动维护漂移
            for binding in PipelineApp.BINDINGS:
                if binding.key in ("ctrl+c", "q"):
                    continue
                show_marker = "" if binding.show else " [dim](隐藏)[/dim]"
                yield Label(f"[bold]{binding.key.upper()}[/bold] — {binding.description}{show_marker}")
            yield Label("")
            yield Label("[bold]Ctrl+C / Q[/bold] — Quit")
            yield Label("")
            yield Label("[dim]按 Esc 或 Q 关闭[/dim]")

    async def action_dismiss(self, result: None = None) -> None:
        self.dismiss(result)


class LogCleanupScreen(ModalScreen[str]):
    """弹窗：选择日志清理策略。"""

    CSS = """
    LogCleanupScreen {
        align: center middle;
        background: $background 60%;
    }
    #logclean-dialog {
        width: 56;
        height: auto;
        max-height: 16;
        background: $surface;
        border: round $warning;
        padding: 1 2;
    }
    #logclean-dialog Label {
        width: 100%;
        text-align: center;
    }
    #logclean-buttons {
        margin-top: 1;
        width: 100%;
        height: auto;
        align: center middle;
        layout: grid;
        grid-size: 2;
        grid-gutter: 1;
    }
    """

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


def _seconds_until_safe() -> int:
    """计算到下一个安全运行时间（上海时间 16:00）的秒数。"""
    now = datetime.now(_SHANGHAI_TZ)
    target = now.replace(hour=16, minute=0, second=0, microsecond=0)
    seconds = (target - now).total_seconds()
    if seconds <= 0:
        # 已过 16:00，次日 16:00
        target += timedelta(days=1)
        seconds = (target - now).total_seconds()
    return int(seconds)


# 收盘刷新结构化任务状态 → 展示标签（设计文档要求的六态区分）
REFRESH_STATE_LABELS: dict[str, str] = {
    "pending": "未运行",
    "fetched_not_validated": "已抓取未通过校验",
    "committed": "已提交覆盖",
    "retained": "保留旧数据",
    "degraded": "部分降级",
    "blocked": "被依赖任务阻塞",
}


def classify_refresh_task_state(record: dict | None) -> str:
    """将 refresh_task_runs 审计记录归类为结构化展示状态。

    关键区分：已提交覆盖（新数据已落库）与保留旧数据（旧数据原样保留）。
    """
    if record is None or record.get("status") is None:
        return "pending"
    metadata = record.get("metadata") or {}
    if metadata.get("blocked_by"):
        return "blocked"
    status = record["status"]
    if status == "success":
        return "committed"
    if status == "degraded":
        return "degraded"
    # failed / aborted / no_data：旧数据保留；区分“抓到但未通过校验”
    if record.get("fetched", 0) > 0 and record.get("validated", 0) == 0:
        return "fetched_not_validated"
    return "retained"


def get_latest_refresh_run_id(db_path: str) -> str | None:
    """读取审计库中最近一次收盘刷新的 run_id（启动前边界快照）。

    审计表不存在或库不可读时返回 None（视为无历史运行），不抛异常。
    """
    p = Path(db_path)
    if not p.exists():
        return None
    conn = None
    try:
        conn = sqlite3.connect(db_path, timeout=5.0)
        row = conn.execute(
            "SELECT run_id FROM refresh_runs ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
        return row[0] if row is not None else None
    except Exception:
        return None
    finally:
        if conn is not None:
            conn.close()


def get_latest_refresh_task_states(
    db_path: str, boundary_run_id: str | None = None
) -> list[dict]:
    """读取最近一次收盘刷新的每任务审计记录（含结构化状态归类）。

    审计表不存在或库不可读时返回空列表，不抛异常。
    boundary_run_id 为启动前捕获的边界：若最新 run 仍是边界本身（子进程
    崩溃于写入自身 refresh_runs 行之前），返回空列表，避免把上一次运行
    的结果误报为本次。
    """
    p = Path(db_path)
    if not p.exists():
        return []
    conn = None
    try:
        conn = sqlite3.connect(db_path, timeout=5.0)
        cur = conn.cursor()
        row = cur.execute(
            "SELECT run_id FROM refresh_runs ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
        if row is None:
            return []
        if boundary_run_id is not None and row[0] == boundary_run_id:
            return []
        rows = cur.execute(
            """SELECT task_name, status, fetched, validated, replaced,
                      retained, failed, metadata_json
               FROM refresh_task_runs WHERE run_id = ?""",
            (row[0],),
        ).fetchall()
        records: list[dict] = []
        for task_name, status, fetched, validated, replaced, retained, failed, metadata_json in rows:
            try:
                metadata = json.loads(metadata_json or "{}")
            except (TypeError, json.JSONDecodeError):
                metadata = {}
            record = {
                "task_name": task_name,
                "status": status,
                "fetched": fetched,
                "validated": validated,
                "replaced": replaced,
                "retained": retained,
                "failed": failed,
                "metadata": metadata,
            }
            record["state"] = classify_refresh_task_state(record)
            records.append(record)
        return records
    except Exception:
        return []
    finally:
        if conn is not None:
            conn.close()


def format_refresh_summary(records: list[dict]) -> str:
    """汇总收盘刷新结果：区分已提交覆盖与保留旧数据，附失败态计数。"""
    if not records:
        return "收盘刷新: 无任务审计记录"
    counts: dict[str, int] = {}
    for record in records:
        state = record.get("state", "pending")
        counts[state] = counts.get(state, 0) + 1
    parts = [
        f"{REFRESH_STATE_LABELS[state]} {counts[state]}"
        for state in REFRESH_STATE_LABELS
        if state in counts
    ]
    return "收盘刷新: " + "，".join(parts)


def get_subprocess_env() -> dict:
    env = os.environ.copy()
    # 与 core/config.py 保持一致：境内数据源全部直连。
    # NO_PROXY 按后缀（urllib endswith / requests 子串）匹配，不支持 glob，
    # 故一律用裸域名后缀；requests/urllib 会优先读小写 no_proxy，需同时设置，
    # 否则操作者 shell 继承来的小写变量会遮蔽此处白名单。
    no_proxy = (
        "localhost,127.0.0.1,"
        "eastmoney.com,"
        "sina.com,sina.cn,sina.com.cn,"
        "sse.com.cn,szse.cn,"
        "jin10.com,csindex.com.cn,cninfo.com.cn"
    )
    env["NO_PROXY"] = no_proxy
    env["no_proxy"] = no_proxy
    env["DISABLE_YFINANCE_FALLBACK"] = "1"
    # 操作者显式 export 的 QUANT_DB_PATH（如指向测试库）必须保留，
    # 只在未设置时补默认值，避免静默写回生产库
    env.setdefault("QUANT_DB_PATH", str(DEFAULT_DB_PATH))
    return env

def get_recent_failed_tasks(db_path: str, limit: int = 10) -> list[dict[str, str]]:
    """查询最近失败/降级/中止的任务审计记录（ingestion_runs，按完成时间倒序）。

    用于运行结束后的失败明细报告；表不存在或不可读时返回空列表。
    """
    p = Path(db_path)
    if not p.exists():
        return []
    try:
        conn = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
        try:
            rows = conn.execute(
                """
                SELECT task_name, status, finished_at, error_kind, error_message
                FROM ingestion_runs
                WHERE status IN ('failed', 'degraded', 'aborted')
                ORDER BY finished_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        finally:
            conn.close()
    except sqlite3.Error:
        return []
    return [
        {
            "task_name": r[0],
            "status": r[1],
            "finished_at": r[2],
            "error_kind": r[3] or "",
            "error_message": r[4] or "",
        }
        for r in rows
    ]


async def get_launchd_status(env: dict | None = None) -> bool:
    if env is None:
        env = get_subprocess_env()
    try:
        res = await asyncio.to_thread(
            subprocess.run,
            ["launchctl", "list"],
            capture_output=True,
            text=True,
            env=env
        )
        return "com.smartmoney.update" in res.stdout
    except Exception:
        return False

def get_all_table_counts(db_path: str, fast: bool = False) -> dict[str, int]:
    """Query row counts for all key tables.

    fast=True: 使用 PRAGMA page_count 估算（瞬间完成，大表有误差）
    fast=False: 精确 COUNT(*)（大表可能耗时 10s+）
    """
    p = Path(db_path)
    if not p.exists():
        return {}
    conn = None
    try:
        conn = sqlite3.connect(db_path, timeout=5.0)
        cur = conn.cursor()
        # 表名列表由 TABLE_LABELS 单一来源派生，防止与面板显示漂移
        tables = list(TABLE_LABELS.keys())
        result = {}
        if fast:
            # 快速估算：用 page_count * page_size 推算行数（SQLite 内部统计）
            cur.execute("PRAGMA page_count")
            page_count = cur.fetchone()[0]
            cur.execute("PRAGMA page_size")
            page_size = cur.fetchone()[0]
            db_bytes = page_count * page_size
            # 粗略估算：每行约 500 bytes（含索引开销）
            estimated_total = max(1, db_bytes // 500)
            for tbl in tables:
                result[tbl] = 0  # 先返回 0，后台精确更新
            result["_estimated_total"] = estimated_total
            result["_db_bytes"] = db_bytes
        else:
            for tbl in tables:
                try:
                    # 方括号包裹防止保留字冲突（表名来自内部常量 TABLE_LABELS）
                    cur.execute(f"SELECT COUNT(*) FROM [{tbl}]")
                    result[tbl] = cur.fetchone()[0]
                except Exception:
                    result[tbl] = 0
        return result
    except Exception:
        return {}
    finally:
        if conn is not None:
            conn.close()


# 面板新鲜度监控的 {表: 日期列} 映射，由 core.task_registry 派生（注册表
# 全集 ∪ 遗留表 institutional_holdings）；面板渲染只遍历 TABLE_LABELS。
TABLE_DATE_COLUMNS: dict[str, str] = panel_date_columns()

# 无有意义日期列的表（不显示新鲜度标记，只显示行数）
NO_DATE_TABLES: set[str] = {
}

# 健康度统计中视为“健康”的状态（含周/月/季周期性更新标记）
_HEALTHY_STATUSES: tuple[str, ...] = (
    "最新",
    "T+1",
    "按周更新",
    "按月更新",
    "按季更新",
)


def format_count(n: int) -> str:
    """Format a count into human-readable form."""
    if n >= 1_000_000:
        return f"{n / 1_000_000:.2f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(n)


def _vis_width(text: str) -> int:
    """计算字符串在终端中的可见宽度（全宽=2, 半宽=1）。

    使用 unicodedata.east_asian_width() 覆盖全角标点、CJK 扩展区等字符，
    而非仅硬编码 U+4E00-U+9FFF。
    """
    return sum(
        2 if unicodedata.east_asian_width(ch) in ('F', 'W') else 1
        for ch in text
    )


def _ljust_vis(text: str, width: int) -> str:
    """按可见宽度左对齐填充空格。"""
    return text + " " * max(0, width - _vis_width(text))


def format_chinese_magnitude(n: int) -> str:
    """将数字转为中文量级：万、亿。"""
    if n >= 100_000_000:
        return f"{n / 100_000_000:.2f}亿"
    if n >= 10_000:
        return f"{n / 10_000:.1f}万"
    return str(n)


def get_active_stock_count(db_path: str) -> int:
    p = Path(db_path)
    if not p.exists():
        return 0
    conn = None
    try:
        conn = sqlite3.connect(db_path, timeout=5.0)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM stock_list")
        count = cursor.fetchone()[0]
        return count
    except Exception:
        return 0
    finally:
        if conn is not None:
            conn.close()

class WatchlistSyncResult(NamedTuple):
    added: int
    reactivated: int
    deactivated: int
    files: int
    success: bool
    error: str | None


def _code_to_ts_code(code: str) -> str | None:
    """将纯数字股票代码转为 ts_code 格式（加交易所后缀）。"""
    code = code.strip()
    if not code.isdigit():
        return None
    # 920xxx 北交所（必须在 6/9 之前检查，否则被 "9" 误匹配为 SH）
    if code.startswith("920"):
        return f"{code}.BJ"
    if code.startswith(("6", "9")):
        return f"{code}.SH"
    if code.startswith(("0", "2", "3")):
        return f"{code}.SZ"
    if code.startswith(("4", "8")):
        return f"{code}.BJ"
    return None


def sync_watchlists_from_files(db_path: str) -> WatchlistSyncResult:
    """从文本文件同步自选股到数据库，支持新增/恢复/停用。"""
    watch_dir = Path(WATCHLIST_DIR)
    if not watch_dir.is_dir():
        error = f"自选股目录不存在: {watch_dir}"
        logger.warning(error)
        return WatchlistSyncResult(0, 0, 0, 0, False, error)

    conn = None
    txt_files: list[Path] = []
    try:
        txt_files = sorted(watch_dir.glob("*.txt"))
        desired_codes: set[str] = set()
        for fpath in txt_files:
            text = fpath.read_text(encoding="utf-8")
            for line in text.splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                code = line.split("#")[0].split()[0].strip()
                ts_code = _code_to_ts_code(code)
                if ts_code:
                    desired_codes.add(ts_code)

        conn = sqlite3.connect(db_path, timeout=5.0)
        cur = conn.cursor()

        existing: dict[str, str] = {
            row[0]: row[1]
            for row in cur.execute(
                "SELECT ts_code, status FROM watchlist "
                "WHERE source_scan = 'watchlist_sync'"
            )
        }

        today = datetime.now().strftime("%Y-%m-%d")
        updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        added = reactivated = deactivated = 0

        for ts_code in sorted(desired_codes):
            if ts_code not in existing:
                cur.execute(
                    "INSERT OR IGNORE INTO watchlist "
                    "(ts_code, added_date, source_scan, status) "
                    "VALUES (?, ?, 'watchlist_sync', 'tracking')",
                    (ts_code, today),
                )
                added += max(cur.rowcount, 0)
            elif existing[ts_code] != "tracking":
                cur.execute(
                    "UPDATE watchlist SET status='tracking', updated_at=? "
                    "WHERE ts_code=? AND source_scan='watchlist_sync'",
                    (updated_at, ts_code),
                )
                reactivated += max(cur.rowcount, 0)

        removed_codes = set(existing) - desired_codes
        if removed_codes:
            placeholders = ",".join("?" for _ in removed_codes)
            cur.execute(
                f"UPDATE watchlist SET status='inactive', updated_at=? "
                f"WHERE source_scan='watchlist_sync' "
                f"AND status!='inactive' AND ts_code IN ({placeholders})",
                (updated_at, *sorted(removed_codes)),
            )
            deactivated = max(cur.rowcount, 0)

        conn.commit()
        logger.info(
            "自选股同步完成: 新增 %s, 恢复 %s, 停用 %s, 来源 %s 个文件",
            added, reactivated, deactivated, len(txt_files),
        )
        return WatchlistSyncResult(
            added, reactivated, deactivated, len(txt_files), True, None
        )
    except Exception as exc:
        if conn is not None:
            conn.rollback()
        error = str(exc)
        logger.warning(f"自选股同步失败: {error}")
        return WatchlistSyncResult(0, 0, 0, len(txt_files), False, error)
    finally:
        if conn is not None:
            conn.close()


class DashboardWidget(Static):
    async def on_mount(self) -> None:
        self.border_title = "Dashboard"
        # 缓存股票数量，避免每次刷新都查询数据库
        self._cached_stocks: int = 0
        self._last_stocks_update: float = 0.0
        self._stocks_cache_ttl: float = 60.0
        await self.update_status()
        self.set_interval(10.0, self.update_status)

    async def update_status(self) -> None:
        db_size = get_db_size(str(DEFAULT_DB_PATH))
        active_stocks = await self._get_active_stock_count_cached()
        daemon_status, daemon_pid = await asyncio.to_thread(get_daemon_status, DAEMON_PID_PATH)
        launchd_active = await get_launchd_status()

        daemon_str = f"[bold green]Running[/bold green] [gray](PID: {daemon_pid})[/gray]" if daemon_status == "Running" else "[bold red]Stopped[/bold red]"
        launchd_str = "[bold green]Active[/bold green]" if launchd_active else "[bold red]Inactive[/bold red]"

        text = (
            f" • [bold gray]DB Size:    [/bold gray] [cyan]{db_size}[/cyan]\n"
            f" • [bold gray]Stocks:     [/bold gray] [cyan]{active_stocks}[/cyan]\n"
            f" • [bold gray]Daemon:     [/bold gray] {daemon_str}\n"
            f" • [bold gray]Scheduler:  [/bold gray] {launchd_str}\n"
        )
        self.update(text)

    async def _get_active_stock_count_cached(self) -> int:
        now = time.time()
        if now - self._last_stocks_update > self._stocks_cache_ttl:
            self._cached_stocks = await asyncio.to_thread(
                get_active_stock_count, str(DEFAULT_DB_PATH)
            )
            self._last_stocks_update = now
        return self._cached_stocks


# ── 单任务下拉：组名与双语标签（成员/组序一律来自 TASK_GROUPS 单一来源）──
_SINGLE_TASK_GROUP_NAMES: dict[str, str] = {
    "core": "核心行情",
    "fund": "资金面",
    "valuation": "估值/财务",
    "macro": "宏观/全球",
    "sector_index": "行业/大盘",
    "derivatives": "ETF/可转债/港通",
    "events": "事件信号",
}

_SINGLE_TASK_LABELS: dict[str, str] = {
    "update_bars": "日线行情 (Daily Bars)",
    "update_indicators": "技术指标 (Indicators)",
    "update_chip_distribution": "筹码分布 (Chip Dist.)",
    "update_chip_distribution_em": "筹码分布线上 (Chip EM)",
    "update_chip_distribution_em_fullmarket": "筹码分布全市场 (Full Market Chip EM)",
    "update_fundamentals": "基本面数据 (Fundamentals)",
    "update_market_snapshot": "行情快照 (Market Snapshot)",
    "update_fund_flow": "资金流向 (Fund Flow)",
    "update_sector_fund_flow": "板块资金 (Sector Fund Flow)",
    "update_north_hold": "北向持仓 (North Hold)",
    "update_margin_trading": "融资融券 (Margin Trading)",
    "update_dragon_tiger": "龙虎榜 (Dragon Tiger)",
    "update_block_trade": "大宗交易 (Block Trade)",
    "update_historical_valuation": "历史估值 (Valuation)",
    "update_quarterly_financials": "季度财务 (Quarterly Fin.)",
    "update_shareholder_count": "股东户数 (Shareholders)",
    "update_dividend_summary": "分红信息 (Dividends)",
    "update_market_valuation": "大盘估值 (Market Valuation)",
    "update_financial_history": "财务历史 (Financial History)",
    "update_china_macro": "中国宏观 (China Macro)",
    "update_gold_price": "黄金价格 (Gold Price)",
    "update_crude_oil": "原油价格 (Crude Oil)",
    "update_usd": "汇率 (USD/CNY)",
    "update_global_index": "全球指数 (Global Index)",
    "update_us_treasury": "美债收益率 (US Treasury)",
    "update_futures": "期货日线 (Futures)",
    "update_money_market": "货币市场 (Money Market)",
    "update_sector_industry": "行业分类 (Sector Industry)",
    "update_industry": "行业更新 (Industry)",
    "update_sector_derivatives": "行业板块 (Sector Derivatives)",
    "update_index_daily": "大盘指数 (Index Daily)",
    "update_limit_up_down": "涨跌停 (Limit U/D)",
    "update_concept_board": "概念板块 (Concept Board)",
    "update_concept_member": "概念成分 (Concept Members)",
    "update_index_membership": "指数成分 (Index Membership)",
    "update_etf_daily": "ETF日线 (ETF Daily)",
    "update_cb_quotation": "可转债行情 (CB Quotation)",
    "update_cb_redeem": "可转债强赎 (CB Redeem)",
    "update_cb_index": "可转债指数 (CB Index)",
    "update_south_flow": "南向资金 (South Flow)",
    "update_ah_premium": "AH溢价 (AH Premium)",
    "update_restricted_share": "限售解禁 (Restricted Share)",
    "update_earnings_forecast": "业绩预告 (Earnings Forecast)",
    "update_stock_repurchase": "股票回购 (Stock Repurchase)",
    "update_institution_survey": "机构调研 (Institution Survey)",
    "update_stock_pledge": "股票质押 (Stock Pledge)",
    "update_option_sentiment": "期权情绪 (Option Sentiment)",
}


def _build_single_task_groups() -> list[tuple[str, list[tuple[str, str]]]]:
    """从 TASK_GROUPS 派生单任务下拉分组（成员与组序的唯一来源在 registry）。"""
    def _label(task: str) -> str:
        if task in _SINGLE_TASK_LABELS:
            return _SINGLE_TASK_LABELS[task]
        spec = lookup_task(task)
        return spec.display_label if spec and spec.display_label else task

    return [
        (
            _SINGLE_TASK_GROUP_NAMES.get(group_key, group_key),
            [(_label(task), task) for task in tasks],
        )
        for group_key, tasks in TASK_GROUPS.items()
    ]


class SingleTaskWidget(Static):
    """Single task selector with a dropdown, organized by task groups."""

    # 成员与组序以 core.task_registry.TASK_GROUPS 为单一来源（防漂移测试锁定），
    # 下拉结构由模块级 _build_single_task_groups 派生
    _SINGLE_TASK_GROUPS: list[tuple[str, list[tuple[str, str]]]] = _build_single_task_groups()

    _SINGLE_TASK_UTILS: list[tuple[str, str]] = [
        ("股票列表 (Stock List)", "update_stock_list"),
        ("重试失败 (Retry Failed)", "retry"),
        ("健康检查 (Health Check)", "health_check"),
    ]

    SINGLE_TASKS: list[tuple[str, str]] = []

    @classmethod
    def _validate_against_registry(cls) -> None:
        """Assert every referenced task name exists in TASK_REGISTRY.

        Keeps TUI display labels flexible while preventing drift from
        the single source of truth for task identity.
        """
        try:
            from core.task_registry import lookup_task

            all_task_names: set[str] = set()
            for _, tasks in cls._SINGLE_TASK_GROUPS:
                for _, name in tasks:
                    all_task_names.add(name)
            for _, name in cls._SINGLE_TASK_UTILS:
                all_task_names.add(name)

            missing = [n for n in sorted(all_task_names) if lookup_task(n) is None]
            if missing:
                import logging
                logging.getLogger(__name__).warning(
                    "TUI references unregistered tasks: %s", missing
                )
        except ImportError:
            pass  # registry not available (e.g. test environment)

    @classmethod
    def _build_single_tasks(cls) -> list[tuple[str, str]]:
        """把分组定义展开为带分隔符的下拉选项列表。"""
        cls._validate_against_registry()
        # 收盘刷新是哨兵项（仿 __sep__ 惯例），不进 _SINGLE_TASK_GROUPS/_UTILS，
        # 避免被 _validate_against_registry 当作未注册任务警告；
        # 每周补全/每月修复同理（它们是 tier 入口，不在 TASK_REGISTRY）
        options: list[tuple[str, str]] = [
            ("每日更新 (Daily Update)", "all"),
            ("收盘刷新 (Close Refresh)", "__refresh_today__"),
        ]
        for group_name, tasks in cls._SINGLE_TASK_GROUPS:
            options.append((f"[dim]── {group_name} ──[/dim]", f"__sep__{group_name}"))
            options.extend(tasks)
        options.append(("[dim]── 工具 ──[/dim]", "__sep__tools"))
        options.extend(cls._SINGLE_TASK_UTILS)
        options.append(("每周补全 (Weekly Backfill)", "__weekly_backfill__"))
        options.append(("每月修复 (Monthly Repair)", "__monthly_repair__"))
        return options

    def on_mount(self) -> None:
        self.border_title = "Single Task"
        self.SINGLE_TASKS = self._build_single_tasks()
        select = Select(
            options=self.SINGLE_TASKS,
            prompt="选择一项任务...",
            id="task-select",
        )
        self.mount(select)

    async def on_select_changed(self, event: Select.Changed) -> None:
        # event.value 在 clear() 后为 Select.NULL（NoSelection 对象），
        # 只有 str 类型才是真实任务名，避免误触发导致杀进程
        value = event.value
        select = self.query_one("#task-select", Select)
        if value == "__refresh_today__":
            # 收盘刷新是独立 CLI 模式（--refresh-today 与 --task 互斥），
            # 走专属确认流程而非 _run_or_schedule 延迟调度
            await cast(PipelineApp, self.app).action_refresh_today()
        elif value == "__weekly_backfill__":
            # 每周补全层哨兵项：路由到专属 action（不带 --force）
            await cast(PipelineApp, self.app).action_weekly_backfill()
        elif value == "__monthly_repair__":
            # 每月修复层哨兵项：路由到专属 action（不带 --force）
            await cast(PipelineApp, self.app).action_monthly_repair()
        elif isinstance(value, str) and value and not value.startswith("__sep__"):
            await cast(PipelineApp, self.app).action_run_single_task(value)
        # 无论选中真实任务还是分组分隔符，都重置回提示状态
        # （会触发新的 Select.Changed 但被上面过滤掉）
        select.clear()


# ===========================================================================
# 任务分组 TASK_GROUPS 由 core.task_registry 统一供给（本文件顶部 import），
# 把相似的 single task 聚合成一键顺序执行的按钮组
# ===========================================================================


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
        app = cast(PipelineApp, self.app)
        if group_key == "catchup":
            await app.action_run_catch_up()
            return
        await app.action_run_task_group(group_key)


class DataCompletenessWidget(VerticalScroll):
    # 任务名 → 表名列表的映射（用于判断哪些表正在更新），由 core.task_registry 派生
    TASK_TO_TABLE: dict[str, list[str]] = task_to_table()

    # 表标签元数据由 core.task_registry 统一供给（模块级 import）；
    # 此处保留类属性别名，维持 DataCompletenessWidget.TABLE_LABELS 既有访问路径
    TABLE_LABELS: dict[str, str] = TABLE_LABELS
    TABLE_LABELS_CN: dict[str, str] = TABLE_LABELS_CN

    async def on_mount(self) -> None:
        self.border_title = "Data Completeness"
        self._counts: dict[str, int] = {}
        self._latest_dates: dict[str, str | None] = {}
        self._daily_coverage: tuple[int, int] = (0, 0)
        self._bg_tasks: set[asyncio.Task] = set()
        self._last_updating_table: list[str] | None = None
        # 挂载内容子组件
        self._content = Static(id="dc-content")
        await self.mount(self._content)
        # 轻量计时器：仅用缓存重建显示（读 progress.json 判断更新中，无 DB 查询）
        self.set_interval(30.0, self._rebuild_from_cache)
        # 重量计时器：后台并行重算行数/最新日期/覆盖率（查询较重，低频执行）
        self.set_interval(90.0, self._refresh_exact)
        # 先快速加载（瞬间完成），再后台精确更新
        self._counts = await asyncio.to_thread(
            get_all_table_counts, str(DEFAULT_DB_PATH), fast=True
        )
        self._rebuild_content()
        task = asyncio.create_task(self._refresh_exact())
        self._bg_tasks.add(task)
        task.add_done_callback(self._bg_tasks.discard)

    async def _refresh_exact(self) -> None:
        """后台重算行数、最新日期与覆盖率（查询较重，低频执行）。

        串行执行以避免多查询同时抢占同一块磁盘 I/O 导致争用变慢。
        """
        expected = get_expected_latest_trading_day()
        counts = await asyncio.to_thread(get_all_table_counts, str(DEFAULT_DB_PATH), fast=False)
        latest = await asyncio.to_thread(get_latest_dates, str(DEFAULT_DB_PATH))
        cov = await asyncio.to_thread(get_daily_bars_coverage, str(DEFAULT_DB_PATH), expected)
        if counts:
            self._counts = counts
        if latest:
            self._latest_dates = latest
        self._daily_coverage = cov
        self._rebuild_content()

    def _rebuild_from_cache(self) -> None:
        """仅用已缓存数据重建显示（无 DB 查询，轻量）。"""
        self._rebuild_content()

    @staticmethod
    def _get_updating_table() -> list[str] | None:
        """读取 progress.json，返回当前正在更新的表名列表，若无活跃任务则返回 None。"""
        import os
        import time
        try:
            mtime = os.path.getmtime(str(PROGRESS_JSON_PATH))
            # 如果 progress.json 超过 90 秒未更新，认为已无活跃任务
            if time.time() - mtime > 90:
                return None
            progress = parse_progress(str(PROGRESS_JSON_PATH))
            if not progress:
                return None
            task = progress.get("task", "")
            return DataCompletenessWidget.TASK_TO_TABLE.get(task)
        except OSError:
            return None

    # 数据新鲜度排序权重：数字越小越靠前。
    # 用户指定顺序：最新 → T+1 → 略滞后 → 滞后 → 按周更新 → 按月更新 → 按季更新 → 无数据
    _STATUS_ORDER: dict[str, int] = {
        "更新中": 0,
        "最新": 1,
        "T+1": 2,
        "略滞后": 3,
        "滞后": 4,
        "按周更新": 5,
        "按月更新": 6,
        "按季更新": 7,
        "无数据": 8,
    }

    # 状态 → (图标, 颜色)。使用高对比度 hex 色，确保在深色主题下清晰可辨。
    STATUS_STYLES: dict[str, tuple[str, str]] = {
        "更新中": ("↻", "#22d3ee"),
        "最新": ("●", "#10b981"),
        "T+1": ("◐", "#3b82f6"),
        "按周更新": ("◇", "#a3e635"),
        "按月更新": ("◈", "#8b5cf6"),
        "按季更新": ("◆", "#d946ef"),
        "略滞后": ("▲", "#f59e0b"),
        "滞后": ("▼", "#ef4444"),
        "无数据": ("○", "#737373"),
    }

    @classmethod
    def _get_status_for_table(
        cls,
        tbl: str,
        latest: str | None,
        expected_date: str,
        updating_tables: list[str] | None,
    ) -> str:
        """返回指定表的新鲜度状态标签（纯文本）。实现已收敛至 core.freshness.status_for_table。"""
        return status_for_table(tbl, latest, expected_date, updating_tables)

    def _rebuild_content(self) -> None:
        counts = self._counts
        latest_dates = self._latest_dates
        updating_tables = self._get_updating_table()
        if updating_tables != self._last_updating_table:
            self._last_updating_table = updating_tables
            if updating_tables:
                self.add_class("active-task")
            else:
                self.remove_class("active-task")
        if not counts:
            self._content.update(" 等待数据库连接...")
            return

        # 快速模式：显示估算值，等待后台精确更新
        estimated_total = counts.get("_estimated_total", 0)
        if estimated_total and sum(counts.get(k, 0) for k in self.TABLE_LABELS) == 0:
            db_size = get_db_size(str(DEFAULT_DB_PATH))
            lines = [
                f" - [bold]数据库：[/bold][cyan]{db_size}[/cyan]  [dim]行数加载中...[/dim]",
                "",
            ]
            for _tbl, label in self.TABLE_LABELS.items():
                lines.append(f" - [bold gray]{label}：[/bold gray][dim]计算中...[/dim]")
            self._content.update("\n".join(lines))
            return

        daily_bars = counts.get("daily_bars", 0) or 1
        expected_date = get_expected_latest_trading_day()
        daily_up_to_date, daily_total = self._daily_coverage

        total_rows = sum(v for k, v in counts.items() if not k.startswith("_"))
        db_size = get_db_size(str(DEFAULT_DB_PATH))

        stock_count = counts.get("stock_list", 0)

        lines = [
            f" [cyan]{stock_count}[/cyan] [bold]只股票[/bold]    "
            f"[bold]总数据 [/bold][cyan]{format_chinese_magnitude(total_rows)}[/cyan]  "
            f"[bold]期望 [/bold][cyan]{expected_date}[/cyan]",
        ]

        # 先计算每个表的状态与排序键，再按新鲜度排序（滞后/无数据沉底）
        items: list[tuple[int, int, str, str, int, str | None, str]] = []
        health_counts: dict[str, int] = {"healthy": 0, "degraded": 0, "critical": 0}
        for idx, (tbl, label) in enumerate(self.TABLE_LABELS.items()):
            n = counts.get(tbl, 0)
            latest = latest_dates.get(tbl)
            status = self._get_status_for_table(
                tbl, latest, expected_date, updating_tables
            )

            order = self._STATUS_ORDER.get(status, 3)
            items.append((order, idx, tbl, label, n, latest, status))

            if status in _HEALTHY_STATUSES:
                health_counts["healthy"] += 1
            elif status == "略滞后":
                health_counts["degraded"] += 1
            elif status in ("滞后", "无数据"):
                health_counts["critical"] += 1

        health_str = (
            f"[bold #10b981]●[/bold #10b981] {health_counts['healthy']}  "
            f"[bold #f59e0b]▲[/bold #f59e0b] {health_counts['degraded']}  "
            f"[bold #ef4444]▼[/bold #ef4444] {health_counts['critical']}"
        )
        lines.append(f"  健康度: {health_str}")

        items.sort(key=lambda x: (x[0], x[1]))

        max_label_w = max(_vis_width(cn) for cn in self.TABLE_LABELS_CN.values())
        max_count_w = max(len(format_count(counts.get(tbl, 0))) for tbl in self.TABLE_LABELS)

        for _, _, tbl, label, n, latest, status in items:
            if tbl == "stock_list":
                continue
            formatted = format_count(n)
            rjust_count = formatted.rjust(max_count_w)
            date_str = f"[gray]{latest or '—'}[/gray]"
            icon, color = self.STATUS_STYLES.get(status, ("●", "white"))
            status_str = f"[{color}]{icon} {status}[/{color}]"
            label_cn = self.TABLE_LABELS_CN.get(tbl, label)
            padded_label = _ljust_vis(label_cn, max_label_w)

            if tbl in ("daily_bars", "indicators"):
                if tbl == "daily_bars":
                    pct = (daily_up_to_date / daily_total * 100) if daily_total else 0
                else:
                    pct = n / daily_bars * 100 if daily_bars else 0
                bar, pct_int = self._mini_bar(pct)
                lines.append(
                    f" [dim]{padded_label}[/dim]  {status_str} [cyan]{rjust_count}[/cyan]  {date_str}  {bar} [dim]{pct_int}%[/dim]"
                )
            elif tbl in NO_DATE_TABLES:
                lines.append(
                    f" [dim]{padded_label}[/dim]  [cyan]{rjust_count}[/cyan]  [dim]无日期列[/dim]"
                )
            else:
                lines.append(
                    f" [dim]{padded_label}[/dim]  {status_str} [cyan]{rjust_count}[/cyan]  {date_str}"
                )

        self._content.update("\n".join(lines))

    @staticmethod
    def _mini_bar(pct: float) -> tuple[str, int]:
        length = 10
        pct_int = max(0, min(100, int(round(pct))))
        filled = max(0, min(length, round(length * pct_int / 100)))
        bar = "█" * filled + "░" * (length - filled)
        return f"[bold #22c55e]{bar}[/bold #22c55e]", pct_int


class ProgressWidget(Static):
    def on_mount(self) -> None:
        self.border_title = "Progress"
        self.update_progress()
        self.set_interval(2.0, self._update_progress_async)

    async def _update_progress_async(self) -> None:
        """异步读取 progress.json 并更新显示，避免阻塞事件循环。"""
        progress = await asyncio.to_thread(parse_progress, str(PROGRESS_JSON_PATH))
        self._render_progress(progress)

    def update_progress(self) -> None:
        """同步入口（供挂载时初次调用和测试使用）。"""
        progress = parse_progress(str(PROGRESS_JSON_PATH))
        self._render_progress(progress)

    def _render_progress(self, progress: dict | None) -> None:
        """根据 progress 字典渲染进度面板。"""
        if not progress:
            self.update(" [dim]当前无运行中的任务[/dim]")
            self.remove_class("active-task")
            return

        processed = progress.get("processed", 0)
        total = progress.get("total", 0)
        last_symbol = progress.get("last_symbol", "")
        failed_queue = progress.get("failed_queue", [])
        failed_count = len(failed_queue)

        pct = (processed / total * 100) if total > 0 else 0
        bar_length = 20
        filled = min(bar_length, max(0, int(bar_length * processed / total))) if total > 0 else 0
        bar = "█" * filled + "░" * (bar_length - filled)

        # 失败明细内联：前 5 只列出，超出显示「等 N 只」，避免只见计数不见对象
        failed_detail = ""
        if failed_count:
            shown = ", ".join(str(s) for s in failed_queue[:5])
            suffix = f" 等 {failed_count} 只" if failed_count > 5 else ""
            failed_detail = f"\n [dim]失败明细：[/dim][red]{escape(shown)}{suffix}[/red]"

        text = (
            f" [yellow]{progress.get('task')}[/yellow]  "
            f"[bold #e2e8f0]{pct:.1f}%[/bold #e2e8f0] "
            f"([cyan]{processed}[/cyan]/[cyan]{total}[/cyan])  "
            f"[bold #c084fc]{bar}[/bold #c084fc]\n"
            f" [dim]当前股票：[/dim][cyan]{last_symbol}[/cyan]  "
            f"[dim]失败：[/dim][bold red]{failed_count}[/bold red]"
            f"{failed_detail}\n"
        )
        self.update(text)
        self.add_class("active-task")


class LogsWidget(RichLog):
    ALLOW_SELECT = True

    def __init__(self, *args, **kwargs) -> None:
        kwargs.setdefault("markup", True)
        kwargs.setdefault("max_lines", 1000)
        kwargs.setdefault("wrap", True)
        super().__init__(*args, **kwargs)

    def on_mount(self) -> None:
        self.border_title = "Live Logs"
        self.active_log: str | None = None
        self.file_handle: TextIO | None = None
        self.set_interval(1.0, self.tail_log)

    def on_unmount(self) -> None:
        if self.file_handle:
            with contextlib.suppress(Exception):
                self.file_handle.close()
            self.file_handle = None

    def copy_recent_logs(self, line_count: int = 500) -> str:
        """返回最近 N 行原始日志文本；优先从当前绑定的日志文件读取。"""
        if self.active_log and Path(self.active_log).exists():
            try:
                with open(self.active_log, encoding="utf-8", errors="ignore") as fh:
                    lines = fh.readlines()
                return "".join(lines[-line_count:])
            except Exception:
                pass
        # 兜底：读取控件中已渲染的文本
        try:
            texts = [str(line) for line in self.lines[-line_count:]]
            return "\n".join(texts)
        except Exception:
            return ""

    def colorize_line(self, line: str) -> str:
        line = line.strip()
        parts = line.split("|", 2)
        if len(parts) >= 3:
            level = parts[1].strip()
            body = parts[2].strip()

            body = escape(body)

            if "ERROR" in level:
                return f"[red]❌ {body}[/red]"
            elif "WARN" in level or "WARNING" in level:
                return f"[yellow]⚠️  {body}[/yellow]"
            elif "SUCCESS" in level:
                return f"[bold green]✅ {body}[/bold green]"
            elif "INFO" in level:
                return f"[#e2e8f0]{body}[/#e2e8f0]"
            return body
        else:
            return escape(line)

    def tail_log(self) -> None:
        try:
            latest = find_latest_log_file(str(LOGS_DIR_PATH))
            if not latest:
                return

            if latest != self.active_log:
                # 先读完旧文件剩余内容，防止日志轮转时丢失最后一批输出
                if self.file_handle:
                    try:
                        remaining = self.file_handle.readlines()
                        for line in remaining:
                            self.write(self.colorize_line(line))
                    finally:
                        self.file_handle.close()
                fh = open(latest, encoding="utf-8", errors="ignore")  # noqa: SIM115
                # Seek to end on open
                fh.seek(0, os.SEEK_END)
                self.file_handle = fh
                self.active_log = latest
                self.write(f"--- Bound to log: {os.path.basename(latest)} ---")

            if self.file_handle:
                lines = self.file_handle.readlines()
                for line in lines:
                    self.write(self.colorize_line(line))
        except Exception as e:
            self.write(f"[red]Error tailing log: {escape(str(e))}[/red]")

class PipelineApp(App):
    TITLE = "SmartMoney Pipeline Manager"
    BINDINGS = [
        Binding("s", "run_pipeline", "Daily Update"),
        Binding("r", "resume_pipeline", "Resume"),
        Binding("u", "refresh_today", "Close Refresh"),
        Binding("x", "stop_pipeline", "Stop"),
        Binding("d", "start_daemon", "Daemon"),
        Binding("h", "run_health", "Health"),
        Binding("?", "show_help", "More"),
        Binding("z", "stop_daemon", "Stop Daemon", show=False),
        Binding("f", "run_reconcile", "Data Repair", show=False),
        Binding("c", "copy_panel", "Copy Panel", show=False),
        Binding("t", "toggle_theme", "Toggle Theme", show=False),
        Binding("l", "clean_logs", "Clean Logs", show=False),
        Binding("w", "weekly_backfill", "Weekly Backfill", show=False),
        Binding("m", "monthly_repair", "Monthly Repair", show=False),
        Binding("f5", "refresh_data", "Refresh", show=False),
        Binding("ctrl+c", "quit", "Quit", priority=True, show=False),
        Binding("q", "quit", "Quit", show=False),
    ]

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._background_tasks: set[asyncio.Task] = set()
        self._current_process: asyncio.subprocess.Process | None = None
        # 全局唯一执行槽：任何时刻只允许一个子任务在跑。
        # 分组队列用 wait=True 在槽上排队；手动按键默认拒绝，防手滑重复启动
        self._task_slot = asyncio.Lock()
        self._theme_name = load_theme()
        # 已排入延迟队列的动作名：防止连按重复调度（排队状态对操作者可见）
        self._pending_scheduled: set[str] = set()

    def _notify_and_log(
        self,
        message: str,
        *,
        severity: Literal["information", "warning", "error"] = "information",
        timeout: float = 6.0,
    ) -> None:
        """弹通知的同时写入 Logs 面板：关键结果可回看（通知几秒后即消失）。"""
        self.notify(message, severity=severity, timeout=timeout)
        try:
            logs = self.query_one("#live-logs", LogsWidget)
            stamp = datetime.now().strftime("%H:%M:%S")
            logs.write(f"[dim]{stamp}[/dim] {escape(message)}")
        except Exception:
            pass

    async def _run_and_report(self, action_name: str, *args: str) -> int | None:
        """运行子进程并报告结果：完成/失败既弹通知也写日志面板，失败列出任务名。"""
        rc = await self._run_in_background(*args)
        if rc == 0:
            self._notify_and_log(f"✅ 「{action_name}」运行完成", severity="information")
        else:
            failed = get_recent_failed_tasks(str(DEFAULT_DB_PATH), limit=5)
            detail = ""
            if failed:
                names = "、".join(f["task_name"] for f in failed)
                detail = f"；失败任务: {names}"
            self._notify_and_log(
                f"❌ 「{action_name}」运行失败 (code={rc}){detail}，详见日志",
                severity="error",
            )
        return rc

    async def on_mount(self) -> None:
        """启动时应用保存的主题、同步自选股，然后检测后台进程询问是否终止。"""
        self.theme = self._theme_name
        self._create_background_task(self._sync_watchlists())

        processes = find_running_pipeline_processes()
        if processes:
            self.push_screen(
                ConfirmStopScreen(processes),
                callback=lambda should_stop: self._on_stop_confirm(should_stop, processes),
            )

    def _on_stop_confirm(self, should_stop: bool, processes: list[dict]) -> None:
        if should_stop:
            for p in processes:
                with contextlib.suppress(OSError):
                    os.kill(int(p["pid"]), signal.SIGTERM)
            self.notify(
                f"Sent SIGTERM to {len(processes)} process(es)",
                severity="information",
                timeout=3.0,
            )
        else:
            self.notify(
                "Background processes kept running",
                severity="information",
                timeout=3.0,
            )

    async def _sync_watchlists(self) -> None:
        try:
            result = sync_watchlists_from_files(str(DEFAULT_DB_PATH))
            if not result.success:
                self.notify(
                    f"自选股同步失败: {result.error or '未知错误'}",
                    severity="warning",
                    timeout=6.0,
                )
            elif result.files or result.deactivated:
                self.notify(
                    f"自选股同步完成: 新增 {result.added}, 恢复 {result.reactivated}, "
                    f"停用 {result.deactivated}, 来源 {result.files} 个文件",
                    timeout=4.0,
                )
        except Exception as exc:
            logger.exception("自选股同步发生未处理异常")
            self.notify(
                f"自选股同步失败: {exc}",
                severity="warning",
                timeout=6.0,
            )

    def _create_background_task(self, coro) -> None:
        task = asyncio.create_task(coro)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    def _run_or_schedule(self, action_name: str, *args: str) -> None:
        """弹窗：立即运行、稍后调度到 16:00 或取消。"""

        def _on_dismiss(choice: str | None) -> None:
            if choice == "run-now":
                self._create_background_task(self._run_and_report(action_name, *args))
            elif choice == "run-later":
                if action_name in self._pending_scheduled:
                    self.notify(
                        f"「{action_name}」已在延迟队列中，请勿重复调度",
                        severity="warning",
                        timeout=4.0,
                    )
                    return
                delay = _seconds_until_safe()
                self._pending_scheduled.add(action_name)
                self.notify(
                    f"「{action_name}」已调度到安全时间后自动运行（剩余 {delay//60} 分钟）",
                    timeout=6.0,
                )
                async def _delayed():
                    try:
                        await asyncio.sleep(delay)
                        await self._run_and_report(action_name, *args)
                    finally:
                        self._pending_scheduled.discard(action_name)
                # 统一走 _create_background_task：带 done_callback 回收，任务不泄漏
                self._create_background_task(_delayed())

        self.push_screen(ConfirmRunScreen(action_name), _on_dismiss)

    CSS = _SHARED_CSS + """
    PipelineApp {
        background: $background;
    }
    #main-grid {
        layout: grid;
        grid-size: 2 4;
        grid-rows: auto auto 1fr 2fr;
        grid-columns: 35fr 65fr;
        height: 100%;
        padding: 1 2;
    }
    #single-task {
        border: round $blue-normal;
        background: $surface;
        padding: 0 2;
        border-title-align: left;
        border-title-color: #60a5fa;
    }
    #single-task:hover {
        border: round $blue-hover;
    }
    #single-task Select {
        width: 100%;
    }
    #single-task Select > SelectCurrent {
        border: round #334155;
        background: transparent;
    }
    #single-task Select:focus > SelectCurrent {
        border: round #3b82f6;
        background: transparent;
    }
    Select > .select-list {
        background: $surface;
        border: round $blue-normal;
        max-height: 14;
        overflow-y: auto;
    }
    Select > .select-list > .select-list-item:hover {
        background: $blue-normal;
    }
    Select > .select-list > .select-list-item.button {
        background: #2563eb;
    }
    #task-tabs {
        border: round $blue-normal;
        background: $surface;
        padding: 0;
    }
    #task-tabs TabPane {
        padding: 0;
    }
    #task-tabs:focus {
        border: round $blue-focus;
    }
    #group-buttons {
        layout: grid;
        grid-size: 2;
        grid-gutter: 1;
        width: 100%;
        height: auto;
        padding: 0;
    }
    #group-buttons Button {
        width: 100%;
        height: 2;
        min-height: 1;
        padding: 0 1;
        margin: 0;
        border: none;
        color: $text;
        content-align: center middle;
        text-style: none;
    }
    #group-buttons Button:hover {
        background: $blue-hover;
        text-style: bold;
    }
    #status-dashboard, #scraping-progress {
        border: round $blue-normal;
        background: $surface;
        padding: 0 1;
        border-title-align: left;
        border-title-color: #60a5fa;
        height: 1fr;
    }
    #data-completeness {
        border: round $blue-normal;
        background: $surface;
        padding: 0 1;
        border-title-align: left;
        border-title-color: #60a5fa;
        scrollbar-color: #475569 #1e293b;
        height: 1fr;
    }
    #dc-content {
        width: 100%;
        height: auto;
    }
    #status-dashboard:hover, #scraping-progress:hover {
        border: round $blue-hover;
        border-title-color: #93c5fd;
    }
    #data-completeness:hover {
        border: round $blue-hover;
        border-title-color: #93c5fd;
    }
    #status-dashboard:focus, #scraping-progress:focus {
        border: round $blue-focus;
        border-title-color: #3b82f6;
    }
    #data-completeness:focus {
        border: round $blue-focus;
        border-title-color: #3b82f6;
    }

    #live-logs {
        border: round $rose-normal;
        background: $surface;
        padding: 1 2;
        border-title-align: left;
        border-title-color: #fb7185;
        row-span: 4;
    }
    #live-logs:hover {
        border: round $rose-hover;
        border-title-color: #fecdd3;
    }
    #live-logs:focus {
        border: round $rose-focus;
        border-title-color: #e11d48;
    }
    .active-task {
        border-left: thick #f59e0b;
        border-title-color: #fbbf24;
    }

    /* 弹窗按钮统一主题 */
    ModalScreen Button {
        margin: 1 1;
        min-width: 16;
        border: none;
    }
    ModalScreen Button.-primary {
        background: $blue-normal;
        color: white;
    }
    ModalScreen Button.-primary:hover {
        background: $blue-hover;
    }
    ModalScreen Button.-primary:focus {
        background: $blue-focus;
        text-style: bold;
    }
    ModalScreen Button.-error {
        background: $rose-normal;
        color: white;
    }
    ModalScreen Button.-error:hover {
        background: $rose-hover;
    }
    ModalScreen Button.-error:focus {
        background: $rose-focus;
        text-style: bold;
    }
    ModalScreen Button.-default {
        background: #334155;
        color: white;
    }
    ModalScreen Button.-default:hover {
        background: #475569;
    }
    ModalScreen Button.-default:focus {
        background: #64748b;
        text-style: bold;
    }

    Footer {
        align: center middle;
    }
    """

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Grid(id="main-grid"):
            yield DashboardWidget(id="status-dashboard")
            yield LogsWidget(id="live-logs")
            with TabbedContent(id="task-tabs"):
                with TabPane("Single", id="tab-single"):
                    yield SingleTaskWidget(id="single-task")
                with TabPane("Groups", id="tab-groups"):
                    yield TaskGroupWidget(id="task-groups")
            yield ProgressWidget(id="scraping-progress")
            yield DataCompletenessWidget(id="data-completeness")
        yield Footer(show_command_palette=False)

    async def _stop_current_process(self) -> None:
        """终止当前正在运行的子进程及其整个进程组。"""
        logger = logging.getLogger("quant_pipeline.tui")
        proc = self._current_process
        if proc is None or proc.returncode is not None:
            self.notify("No running task to stop", severity="warning", timeout=3.0)
            return
        try:
            logger.info("⛔ TUI 停止当前任务子进程 (PID %s)", proc.pid)
            # 杀整个进程组（start_new_session=True 创建的）
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            except (ProcessLookupError, PermissionError):
                proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), timeout=5.0)
            except TimeoutError:
                logger.warning("⚠️ 子进程 (PID %s) SIGTERM 超时，升级为 SIGKILL", proc.pid)
                try:
                    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                except (ProcessLookupError, PermissionError):
                    proc.kill()
                await proc.wait()
            logger.info("✅ 子进程 (PID %s) 已退出", proc.pid)
            self.notify("Task stopped", severity="information", timeout=3.0)
        except ProcessLookupError:
            self.notify("Process already exited", severity="information", timeout=3.0)
        finally:
            self._current_process = None

    async def _run_in_background(self, *args: str, wait: bool = False) -> int | None:
        """在后台运行子进程，返回其退出码（被外部中断时返回 None）。

        wait=False（手动按键）：执行槽被占用时拒绝启动，防手滑重复触发；
        wait=True（分组队列）：在执行槽上排队，等当前任务结束后自动执行。
        """
        env = get_subprocess_env()
        logger = logging.getLogger("quant_pipeline.tui")

        # 有任务在跑就拒绝启动，X 键是唯一的停止入口——
        # 隐式顶掉运行中的任务曾把手动全量更新 SIGKILL 掉（2026-07-29 事故）
        busy = self._task_slot.locked() or (
            self._current_process is not None and self._current_process.returncode is None
        )
        if busy and not wait:
            pid = self._current_process.pid if self._current_process else "?"
            self.notify(
                f"已有任务在运行 (PID {pid})，请先按 X 停止",
                severity="warning",
                timeout=5.0,
            )
            return None

        async with self._task_slot:
            return await self._spawn_and_wait(args, env, logger)

    async def _spawn_and_wait(
        self,
        args: tuple[str, ...],
        env: dict[str, str],
        logger: logging.Logger,
    ) -> int | None:
        proc: asyncio.subprocess.Process | None = None
        try:
            proc = await asyncio.create_subprocess_exec(
                *args,
                env=env,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
                start_new_session=True,
            )
            self._current_process = proc
            await proc.wait()
            if proc.returncode != 0:
                logger.error(f"Subprocess {' '.join(args)} exited with code {proc.returncode}")
            return proc.returncode
        except Exception:
            logger.exception(f"Exception running subprocess {' '.join(args)}")
            return None
        finally:
            self._current_process = None

    async def _stop_daemon_process(self) -> None:
        """调用 daemon.py stop 干净地停止守护进程（不破坏 _stop_current_process 的副作用）。"""
        daemon_path = str(Path(__file__).parent / "scripts" / "daemon.py")
        env = get_subprocess_env()
        logger = logging.getLogger("quant_pipeline.tui")
        try:
            proc = await asyncio.create_subprocess_exec(
                sys.executable, daemon_path, "stop",
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=10.0)
            if proc.returncode != 0:
                output = stdout.decode("utf-8", errors="replace").strip()
                logger.warning("停止守护进程失败 (code=%s): %s", proc.returncode, output)
        except TimeoutError:
            logger.warning("停止守护进程超时")
        except Exception:
            logger.exception("停止守护进程异常")

    async def action_run_pipeline(self) -> None:
        pipeline_path = str(Path(__file__).parent / "daily_pipeline.py")
        self._run_or_schedule(
            "每日更新",
            sys.executable, pipeline_path, "--task", "all", "--force",
        )

    async def action_resume_pipeline(self) -> None:
        pipeline_path = str(Path(__file__).parent / "daily_pipeline.py")
        self._run_or_schedule(
            "断点续传",
            sys.executable, pipeline_path, "--task", "update_bars", "--resume", "--force",
        )

    async def action_refresh_today(self) -> None:
        """收盘刷新：确认后启动 --refresh-today。

        16:00 安全闸门与 --force 由 CLI 拥有，这里不重复实现，
        也不复用普通全量更新的延迟调度（_run_or_schedule）。
        """
        pipeline_path = str(Path(__file__).parent / "daily_pipeline.py")

        def _on_dismiss(symbols: str | None) -> None:
            if symbols is None:
                # 取消：不启动任何子进程
                return
            args = [sys.executable, pipeline_path, "--refresh-today"]
            if symbols:
                args.extend(["--symbols", symbols])
            self._create_background_task(self._run_refresh_today(*args))

        self.push_screen(ConfirmRefreshTodayScreen(), _on_dismiss)

    async def _run_refresh_today(self, *args: str) -> None:
        """后台执行收盘刷新，结束后用审计记录汇总通知（区分覆盖/保留）。

        启动前先捕获审计库最新 run 的边界，结束后只汇总严格晚于该边界
        的新 run；若子进程在写入自身 refresh_runs 行之前崩溃，则退回纯
        退出码告警，避免把上一次运行的结果误报为本次。
        """
        boundary_run_id = get_latest_refresh_run_id(str(DEFAULT_DB_PATH))
        returncode = await self._run_in_background(*args)
        records = get_latest_refresh_task_states(
            str(DEFAULT_DB_PATH), boundary_run_id=boundary_run_id
        )
        if records:
            severity = "information" if returncode == 0 else "warning"
            self.notify(format_refresh_summary(records), severity=severity, timeout=8.0)
        elif returncode is not None and returncode != 0:
            self.notify(f"收盘刷新退出码 {returncode}", severity="warning", timeout=6.0)

    async def action_start_daemon(self) -> None:
        daemon_path = str(Path(__file__).parent / "scripts" / "daemon.py")
        self._create_background_task(
            self._run_in_background(sys.executable, daemon_path, "start", "--resume")
        )

    async def action_stop_daemon(self) -> None:
        daemon_path = str(Path(__file__).parent / "scripts" / "daemon.py")
        self._create_background_task(
            self._run_in_background(sys.executable, daemon_path, "stop")
        )

    async def action_stop_pipeline(self) -> None:
        """停止当前正在运行的 pipeline / resume / health 子进程，
        以及任何后台（含 daemon 启动、或在其它终端启动）正在运行的 daily_pipeline.py 进程。
        若守护进程正在运行，也会一并停止，避免任务被重新拉起。"""
        logger = logging.getLogger("quant_pipeline.tui")
        stopped_pids: list[int] = []

        # 1. 停止 TUI 直接启动的子进程（R / S / H 键启动的任务）
        if self._current_process is not None and self._current_process.returncode is None:
            proc_pid = self._current_process.pid
            await self._stop_current_process()
            stopped_pids.append(proc_pid)

        # 2. 停止通过 pgrep 发现的其它后台 daily_pipeline.py 进程
        #    （含 daemon 启动的孙进程、TUI 子进程、在其它终端启动的进程）
        for p in find_running_pipeline_processes(skip_ppid_check=True):
            pid = int(p["pid"])
            try:
                os.kill(pid, signal.SIGTERM)
                stopped_pids.append(pid)
            except OSError:
                pass

        # 3. 若守护进程仍在运行，一并停止（否则会重新拉起任务）
        daemon_status, daemon_pid = get_daemon_status(DAEMON_PID_PATH)
        if daemon_status == "Running" and daemon_pid is not None:
            self._create_background_task(self._stop_daemon_process())
            stopped_pids.append(daemon_pid)

        if stopped_pids:
            self.notify(
                f"已发送停止信号给 {len(set(stopped_pids))} 个进程",
                severity="information",
                timeout=3.0,
            )
            logger.info("已停止进程: %s", stopped_pids)
        else:
            self.notify("没有正在运行的任务可停止", severity="warning", timeout=3.0)
            logger.info("没有正在运行的任务可停止")

    async def action_run_health(self) -> None:
        pipeline_path = str(Path(__file__).parent / "daily_pipeline.py")
        self._run_or_schedule(
            "健康检查",
            sys.executable, pipeline_path, "--task", "health_check", "--force",
        )

    async def action_weekly_backfill(self) -> None:
        """每周补全层：完整性优先的缺漏兜底。"""
        pipeline_path = str(Path(__file__).parent / "daily_pipeline.py")
        self._run_or_schedule(
            "每周补全", sys.executable, pipeline_path, "--task", "weekly_backfill",
        )

    async def action_monthly_repair(self) -> None:
        """每月修复层：正确性优先的校验修复。"""
        pipeline_path = str(Path(__file__).parent / "daily_pipeline.py")
        self._run_or_schedule(
            "每月修复", sys.executable, pipeline_path, "--task", "monthly_repair",
        )

    async def action_run_reconcile(self) -> None:
        """全量清洗：对比 AkShare 并修复差异。

        统一走 _run_or_schedule 执行槽保护，与其他 action 一致。
        """
        reconcile_path = str(Path(__file__).parent / "scripts" / "reconcile_with_akshare.py")
        self._run_or_schedule(
            "全量数据清洗",
            sys.executable, reconcile_path, "--workers", "3",
        )

    async def action_run_single_task(self, task: str) -> None:
        """运行单个数据拉取任务。"""
        pipeline_path = str(Path(__file__).parent / "daily_pipeline.py")
        self._run_or_schedule(
            f"单任务: {task}",
            sys.executable, pipeline_path, "--task", task, "--force",
        )

    async def action_run_task_group(self, group_key: str) -> None:
        """顺序执行某一任务分组内的所有 single task。"""
        tasks = TASK_GROUPS.get(group_key)
        if not tasks:
            self.notify(f"未知任务分组: {group_key}", severity="error", timeout=3.0)
            return

        label = TaskGroupWidget.GROUP_LABELS.get(group_key, group_key)
        self.notify(
            f"开始执行分组「{label}」，共 {len(tasks)} 个任务，按顺序运行",
            timeout=4.0,
        )
        self._create_background_task(self._run_task_group(label, tasks))

    async def action_run_catch_up(self) -> None:
        """补齐缺失：检测截至最近交易日的滞后表，只补缺的任务。

        目标日 = get_expected_latest_trading_day()（交易日收盘定型后 → 今天，
        否则 → 上一交易日）。分组队列以 --force 运行会绕过盘中门禁，
        因此交易日的盘中/结算窗口直接拒绝，避免把实时快照写成终值。
        """
        from core.calendar import is_trading_day as _calendar_is_trading_day
        from core.market_time import (
            PHASE_POST_CLOSE,
            PHASE_PRE_OPEN,
            market_phase,
            shanghai_now,
        )

        sh_now = shanghai_now()
        if _calendar_is_trading_day(sh_now.date()) and market_phase(sh_now) not in (
            PHASE_PRE_OPEN,
            PHASE_POST_CLOSE,
        ):
            self.notify(
                "盘中/结算窗口不可补数（上海 16:00 后数据定型再试）",
                severity="warning",
                timeout=5.0,
            )
            return

        expected = get_expected_latest_trading_day()
        latest_dates = await asyncio.to_thread(get_latest_dates, str(DEFAULT_DB_PATH))
        tasks = compute_catch_up_tasks(latest_dates, expected)
        if not tasks:
            self.notify(f"✅ 无缺失：全部数据已更新到 {expected}", timeout=4.0)
            return

        self.notify(
            f"检测到 {len(tasks)} 项滞后于 {expected}，按依赖顺序补齐: "
            + "、".join(t.removeprefix("update_") for t in tasks[:6])
            + ("…" if len(tasks) > 6 else ""),
            timeout=6.0,
        )
        self._create_background_task(self._run_task_group("补齐缺失", tasks))

    async def _run_task_group(self, label: str, tasks: list[str]) -> None:
        """在后台协程中依次执行分组任务。"""
        pipeline_path = str(Path(__file__).parent / "daily_pipeline.py")
        failed: list[str] = []
        for task in tasks:
            if self._task_slot.locked():
                self.notify(
                    f"[{label}] {task} 排队等待当前任务结束...",
                    severity="information",
                    timeout=3.0,
                )
            self.notify(
                f"[{label}] 正在运行: {task}",
                severity="information",
                timeout=2.0,
            )
            # wait=True：在执行槽上排队而非拒绝，多个分组并发点击时自动串行
            returncode = await self._run_in_background(
                sys.executable, pipeline_path, "--task", task, "--force", wait=True
            )
            # returncode 为 None 表示进程被外部中断或发生异常
            if returncode is None or returncode < 0:
                self.notify(
                    f"[{label}] 任务 {task} 被中断，分组执行停止",
                    severity="warning",
                    timeout=4.0,
                )
                failed.append(task)
                break
            if returncode != 0:
                self.notify(
                    f"[{label}] 任务 {task} 退出码 {returncode}，继续执行下一任务",
                    severity="warning",
                    timeout=3.0,
                )
                failed.append(task)
                # 单个任务失败不阻塞同组其他任务
                continue
        else:
            self.notify(
                f"[{label}] 分组全部完成（共 {len(tasks)} 个任务）",
                severity="information",
                timeout=4.0,
            )
            return

        if failed:
            self.notify(
                f"[{label}] 分组执行结束，失败/中断 {len(failed)} 个任务",
                severity="warning",
                timeout=4.0,
            )

    async def action_toggle_theme(self) -> None:
        """轮换主题并持久化。"""
        from textual.theme import BUILTIN_THEMES

        themes = sorted(BUILTIN_THEMES)
        idx = themes.index(self._theme_name) if self._theme_name in themes else -1
        new_idx = (idx + 1) % len(themes)
        new_theme = themes[new_idx]
        self._theme_name = new_theme
        self.theme = new_theme
        save_theme(new_theme)
        self.notify(f"主题已切换: {new_theme} ({new_idx + 1}/{len(themes)})", timeout=3.0)

    async def action_show_help(self) -> None:
        """显示快捷键帮助弹窗。"""
        self.push_screen(HelpScreen())

    async def action_refresh_data(self) -> None:
        """手动刷新数据完整度面板。"""
        dc = self.query_one("#data-completeness", DataCompletenessWidget)
        self._create_background_task(dc._refresh_exact())
        self.notify("数据完整度刷新中...", timeout=2.0)

    def action_copy_panel(self) -> None:
        from rich.text import Text

        def _static_plain(w: Static) -> str:
            raw = str(getattr(w, "_Static__content", ""))
            return Text.from_markup(raw).plain if raw else ""

        def _on_dismiss(choice: str | None) -> None:
            if not choice:
                return

            panel_text = ""
            label = ""
            if choice == "data-completeness":
                dc = self.query_one("#data-completeness", DataCompletenessWidget)
                panel_text = _static_plain(dc._content).strip()
                label = "📀 数据完整度"
            elif choice == "status-dashboard":
                dash = self.query_one("#status-dashboard", DashboardWidget)
                panel_text = _static_plain(dash).strip()
                label = "📊 Dashboard"
            elif choice == "scraping-progress":
                prog = self.query_one("#scraping-progress", ProgressWidget)
                panel_text = _static_plain(prog).strip()
                label = "📈 Progress"
            elif choice == "live-logs":
                logs = self.query_one("#live-logs", LogsWidget)
                panel_text = logs.copy_recent_logs(line_count=500).strip()
                label = "📋 日志"

            if not panel_text:
                self.notify(f"{label} — 内容为空", timeout=2.0)
                return

            full = f"=== {label} ===\n{panel_text}"
            self.copy_to_clipboard(full)
            self.notify(f"✓ {label} 已复制到剪贴板", timeout=3.0)

        self.push_screen(CopyPanelScreen(), _on_dismiss)

    async def action_clean_logs(self) -> None:
        """清理日志文件：弹窗选择「全部 / 保留 7 天 / 保留 30 天」。"""
        active_log = find_latest_log_file(str(LOGS_DIR_PATH))

        def _on_dismiss(choice: str | None) -> None:
            if not choice or choice == "cancel":
                return
            keep_days = 0 if choice == "all" else int(choice.removeprefix("keep-"))
            self._create_background_task(self._clean_logs(keep_days, active_log))

        self.push_screen(LogCleanupScreen(), _on_dismiss)

    async def _clean_logs(self, keep_days: int, active_log: str | None) -> None:
        """后台执行日志清理并通过通知反馈结果。"""
        exclude = {active_log} if active_log else set()
        try:
            result = await asyncio.to_thread(
                cleanup_logs, str(LOGS_DIR_PATH), keep_days, exclude
            )
        except Exception as exc:  # pragma: no cover - 防御性兜底
            self.notify(f"日志清理失败: {exc}", severity="error", timeout=5.0)
            return

        for err in result.errors:
            self.notify(f"⚠️  {err}", severity="warning", timeout=4.0)
        if result.deleted_count == 0:
            self.notify("没有需要清理的日志文件", severity="information", timeout=3.0)
            return
        size_mb = result.freed_bytes / (1024 * 1024)
        size_str = f"{size_mb:.1f} MB" if size_mb < 1024 else f"{size_mb / 1024:.2f} GB"
        self.notify(
            f"已清理 {result.deleted_count} 个日志文件，释放 {size_str}",
            severity="information",
            timeout=4.0,
        )

if __name__ == "__main__":
    app = PipelineApp()
    app.run()
