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
from datetime import datetime, timedelta
from pathlib import Path
from typing import NamedTuple, TextIO
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
from core.log_cleanup import cleanup_logs
from core.task_registry import refreshable_trading_tasks

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
            if "daily_pipeline.py" in res.stdout:
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
                processes.append({
                    "pid": int(parts[0]),
                    "elapsed": parts[1] if len(parts) > 1 else "unknown",
                    "command": parts[2] if len(parts) > 2 else "daily_pipeline.py",
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
        elif event.key.lower() == "n":
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
                yield Button("立即运行", variant="primary", id="run-now")
                yield Button("稍后自动运行", variant="default", id="run-later")
                yield Button("取消", variant="error", id="cancel")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        self.dismiss(event.button.id)


class ConfirmRefreshTodayScreen(ModalScreen[str | None]):
    """弹窗：确认收盘刷新。

    显示上海目标交易日、刷新任务范围（29 个交易日任务）与可选股票范围；
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
            yield Label("[bold]S[/bold] — 全量更新")
            yield Label("[bold]R[/bold] — 断点续传")
            yield Label("[bold]U[/bold] — 收盘刷新")
            yield Label("[bold]X[/bold] — 停止任务")
            yield Label("[bold]D[/bold] — 启动守护进程")
            yield Label("[bold]H[/bold] — 健康检查")
            yield Label("")
            yield Label("[dim]更多快捷键[/dim]")
            yield Label("[bold]Z[/bold] — 停止守护进程")
            yield Label("[bold]F[/bold] — 数据修复")
            yield Label("[bold]C[/bold] — 复制面板内容")
            yield Label("[bold]L[/bold] — 清理日志")
            yield Label("[bold]T[/bold] — 切换主题")
            yield Label("[bold]F5[/bold] — 刷新数据")
            yield Label("[bold]Ctrl+C / Q[/bold] — 退出")
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
                yield Button("取消", variant="default", id="cancel")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        self.dismiss(event.button.id)


def _seconds_until_safe() -> int:
    """计算到下一个安全运行时间（16:00）的秒数。"""
    now = datetime.now()
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
    env["NO_PROXY"] = "push2his.eastmoney.com,*.eastmoney.com,*.sina.com,*.sina.cn"
    env["DISABLE_YFINANCE_FALLBACK"] = "1"
    env["QUANT_DB_PATH"] = str(DEFAULT_DB_PATH)
    return env

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
        tables = [
            "daily_bars", "indicators", "fundamentals",
            "fund_flow", "margin_trading", "dragon_tiger",
            "block_trade", "sector_fund_flow", "shareholder_count",
            "quarterly_financials", "historical_valuation",
            "sector_industry", "stock_list",
            "institutional_holdings",
            "north_flow", "north_hold", "index_daily", "limit_up_down", "dividend_summary",
            "gold_price", "crude_oil", "fx_rate", "global_index", "us_treasury",
            "chip_distribution", "chip_distribution_em",
            "futures_daily",
            "south_flow", "ah_premium", "etf_daily",
            "cb_quotation", "cb_redeem", "cb_index",
            "restricted_share", "earnings_forecast",
            "stock_repurchase", "institution_survey", "stock_pledge", "option_sentiment",
            "sector_daily", "sector_valuation", "index_futures_basis",
            "macro_monthly", "macro_quarterly",
        ]
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
                    cur.execute(f"SELECT COUNT(*) FROM {tbl}")
                    result[tbl] = cur.fetchone()[0]
                except Exception:
                    result[tbl] = 0
        return result
    except Exception:
        return {}
    finally:
        if conn is not None:
            conn.close()


TABLE_DATE_COLUMNS: dict[str, str] = {
    "daily_bars": "trade_date",
    "indicators": "trade_date",
    "fundamentals": "trade_date",
    "fund_flow": "trade_date",
    "margin_trading": "trade_date",
    "dragon_tiger": "trade_date",
    "block_trade": "trade_date",
    "sector_fund_flow": "trade_date",
    "shareholder_count": "report_date",
    "quarterly_financials": "report_period",
    "historical_valuation": "trade_date",
    "sector_industry": "trade_date",
    "institutional_holdings": "report_date",
    "north_flow": "trade_date",
    "north_hold": "trade_date",
    "index_daily": "trade_date",
    "limit_up_down": "trade_date",
    "gold_price": "trade_date",
    "crude_oil": "trade_date",
    "fx_rate": "trade_date",
    "global_index": "trade_date",
    "us_treasury": "trade_date",
    "chip_distribution": "trade_date",
    "chip_distribution_em": "trade_date",
    "dividend_summary": "updated_at",
    "futures_daily": "trade_date",
    "south_flow": "trade_date",
    "ah_premium": "trade_date",
    "etf_daily": "trade_date",
    "cb_index": "trade_date",
    "cb_quotation": "updated_at",
    "cb_redeem": "updated_at",
    "restricted_share": "release_date",
    "earnings_forecast": "end_date",
    "stock_repurchase": "trade_date",
    "institution_survey": "trade_date",
    "stock_pledge": "trade_date",
    "option_sentiment": "trade_date",
    "sector_daily": "trade_date",
    "sector_valuation": "trade_date",
    "index_futures_basis": "trade_date",
    "macro_monthly": "date",
    "macro_quarterly": "date",
}

# 按周度更新的表（数据源每周发布一次，不按交易日衡量新鲜度）
WEEKLY_TABLES: set[str] = {
    "stock_pledge",  # 中登公司每周五更新质押比例
}

# 按月度更新的表（不按交易日衡量新鲜度）
MONTHLY_TABLES: set[str] = {
    "institutional_holdings",
    "macro_monthly",
}

# 随季报更新的表（使用 report_date/report_period，不按交易日衡量新鲜度）
QUARTERLY_TABLES: set[str] = {
    "shareholder_count",
    "quarterly_financials",
    "macro_quarterly",
    "north_hold",
    "earnings_forecast",
}

# T+1 更新的表（数据源当日尚未公布，取最近已发布日期，不按交易日衡量新鲜度）
DELAYED_PUBLISH_TABLES: set[str] = {
    "fx_rate",
    "us_treasury",
    "margin_trading",
    "dragon_tiger",
    "block_trade",
}

# 无有意义日期列的表（不显示新鲜度标记，只显示行数）
NO_DATE_TABLES: set[str] = {
}




def _normalize_date(value: object) -> str | None:
    """将日期/报告期归一化为 YYYY-MM-DD。

    部分表（margin_trading、dragon_tiger、block_trade、shareholder_count、
    quarterly_financials）的日期列以 YYYYMMDD 无横线格式存储；
    chip_distribution 等表以 YYYY-MM-DD HH:MM:SS 格式存储。需归一化后
    才能与期望日做新鲜度比较，否则会被 _date_status 误判为滞后。
    """
    if value is None:
        return None
    s = str(value).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return s


def get_latest_dates(db_path: str) -> dict[str, str | None]:
    """查询每个表最新日期/报告期（已归一化为 YYYY-MM-DD）。"""
    p = Path(db_path)
    if not p.exists():
        return {}
    conn = None
    try:
        conn = sqlite3.connect(db_path, timeout=5.0)
        cur = conn.cursor()
        result: dict[str, str | None] = {}
        for tbl, col in TABLE_DATE_COLUMNS.items():
            try:
                cur.execute(f"SELECT MAX({col}) FROM {tbl}")
                value = cur.fetchone()[0]
                result[tbl] = _normalize_date(value)
            except Exception:
                result[tbl] = None
        return result
    except Exception:
        return {}
    finally:
        if conn is not None:
            conn.close()


def get_daily_bars_coverage(db_path: str, expected_date: str) -> tuple[int, int]:
    """返回 (已更新到期望交易日的股票数, 有日线数据的股票总数)。

    用「各股最新交易日是否达到期望日」衡量覆盖率，比按行数对比更符合实际
    （新股历史不足、节假日等会导致行数天然少于理想值）。
    """
    p = Path(db_path)
    if not p.exists():
        return 0, 0
    conn = None
    try:
        conn = sqlite3.connect(db_path, timeout=5.0)
        cur = conn.cursor()
        # 容忍期望日前 2 个自然日（周末/节假日），视为已更新
        cur.execute(
            """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN max_date >= date(?, '-2 days') THEN 1 ELSE 0 END) AS up_to_date
            FROM (SELECT ts_code, MAX(trade_date) AS max_date FROM daily_bars GROUP BY ts_code)
            """,
            (expected_date,),
        )
        total, up_to_date = cur.fetchone()
        return int(up_to_date or 0), int(total or 0)
    except Exception:
        return 0, 0
    finally:
        if conn is not None:
            conn.close()


def _date_status(latest: str | None, expected: str) -> str:
    """返回日期新鲜度状态标签（纯文本，颜色由调用方根据 STATUS_STYLES 渲染）。"""
    if not latest:
        return "无数据"
    if latest == expected:
        return "最新"
    try:
        from datetime import datetime, timedelta

        latest_dt = datetime.strptime(latest, "%Y-%m-%d")
        expected_dt = datetime.strptime(expected, "%Y-%m-%d")
        # 数据日期 >= 期望日期 → 已更新到或超过预期（非交易日也有数据）
        if latest_dt >= expected_dt:
            return "最新"
        if latest_dt >= expected_dt - timedelta(days=2):
            return "略滞后"
    except Exception:
        pass
    return "滞后"


def format_count(n: int) -> str:
    """Format a count into human-readable form."""
    if n >= 1_000_000:
        return f"{n / 1_000_000:.2f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(n)


def _vis_width(text: str) -> int:
    """计算字符串在终端中的可见宽度（CJK=2, ASCII=1）。"""
    return sum(2 if "\u4e00" <= ch <= "\u9fff" else 1 for ch in text)


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
        self.set_interval(2.0, self.update_status)

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


class SingleTaskWidget(Static):
    """Single task selector with a dropdown, organized by task groups."""

    # 与 Groups tab 保持一致的分组定义
    _SINGLE_TASK_GROUPS: list[tuple[str, list[tuple[str, str]]]] = [
        (
            "核心行情",
            [
                ("日线行情 (Daily Bars)", "update_bars"),
                ("技术指标 (Indicators)", "update_indicators"),
                ("筹码分布 (Chip Dist.)", "update_chip_distribution"),
                ("筹码分布线上 (Chip EM)", "update_chip_distribution_em"),
                ("筹码分布全市场 (Full Market Chip EM)", "update_chip_distribution_em_fullmarket"),
                ("基本面数据 (Fundamentals)", "update_fundamentals"),
                ("行情快照 (Market Snapshot)", "update_market_snapshot"),
            ],
        ),
        (
            "资金面",
            [
                ("资金流向 (Fund Flow)", "update_fund_flow"),
                ("板块资金 (Sector Fund Flow)", "update_sector_fund_flow"),
                ("北向资金 (North Flow)", "update_north_flow"),
                ("北向持仓 (North Hold)", "update_north_hold"),
                ("融资融券 (Margin Trading)", "update_margin_trading"),
                ("龙虎榜 (Dragon Tiger)", "update_dragon_tiger"),
                ("大宗交易 (Block Trade)", "update_block_trade"),
            ],
        ),
        (
            "估值/财务",
            [
                ("历史估值 (Valuation)", "update_historical_valuation"),
                ("季度财务 (Quarterly Fin.)", "update_quarterly_financials"),
                ("股东户数 (Shareholders)", "update_shareholder_count"),
                ("分红信息 (Dividends)", "update_dividend_summary"),
            ],
        ),
        (
            "宏观/全球",
            [
                ("中国宏观 (China Macro)", "update_china_macro"),
                ("黄金价格 (Gold Price)", "update_gold_price"),
                ("原油价格 (Crude Oil)", "update_crude_oil"),
                ("汇率 (USD/CNY)", "update_usd"),
                ("全球指数 (Global Index)", "update_global_index"),
                ("美债收益率 (US Treasury)", "update_us_treasury"),
                ("期货日线 (Futures)", "update_futures"),
            ],
        ),
        (
            "行业/大盘",
            [
                ("行业分类 (Sector Industry)", "update_sector_industry"),
                ("行业更新 (Industry)", "update_industry"),
                ("行业板块 (Sector Derivatives)", "update_sector_derivatives"),
                ("大盘指数 (Index Daily)", "update_index_daily"),
                ("涨跌停 (Limit U/D)", "update_limit_up_down"),
            ],
        ),
        (
            "ETF/可转债/港通",
            [
                ("ETF日线 (ETF Daily)", "update_etf_daily"),
                ("可转债行情 (CB Quotation)", "update_cb_quotation"),
                ("可转债强赎 (CB Redeem)", "update_cb_redeem"),
                ("可转债指数 (CB Index)", "update_cb_index"),
                ("南向资金 (South Flow)", "update_south_flow"),
                ("AH溢价 (AH Premium)", "update_ah_premium"),
            ],
        ),
        (
            "事件信号",
            [
                ("限售解禁 (Restricted Share)", "update_restricted_share"),
                ("业绩预告 (Earnings Forecast)", "update_earnings_forecast"),
                ("股票回购 (Stock Repurchase)", "update_stock_repurchase"),
                ("机构调研 (Institution Survey)", "update_institution_survey"),
                ("股票质押 (Stock Pledge)", "update_stock_pledge"),
                ("期权情绪 (Option Sentiment)", "update_option_sentiment"),
            ],
        ),
    ]

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
        options: list[tuple[str, str]] = [("全量更新 (Full Update)", "all")]
        for group_name, tasks in cls._SINGLE_TASK_GROUPS:
            options.append((f"[dim]── {group_name} ──[/dim]", f"__sep__{group_name}"))
            options.extend(tasks)
        options.append(("[dim]── 工具 ──[/dim]", "__sep__tools"))
        options.extend(cls._SINGLE_TASK_UTILS)
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
        if isinstance(value, str) and value and not value.startswith("__sep__"):
            from typing import cast
            await cast(PipelineApp, self.app).action_run_single_task(value)
        # 无论选中真实任务还是分组分隔符，都重置回提示状态
        # （会触发新的 Select.Changed 但被上面过滤掉）
        select.clear()


# ===========================================================================
# 任务分组：把相似的 single task 聚合成一键顺序执行的按钮组
# ===========================================================================

TASK_GROUPS: dict[str, list[str]] = {
    "core": [
        "update_bars",
        "update_indicators",
        "update_fundamentals",
        "update_chip_distribution",
        "update_chip_distribution_em",
        "update_chip_distribution_em_fullmarket",
        "update_market_snapshot",
    ],
    "fund": [
        "update_fund_flow",
        "update_sector_fund_flow",
        "update_north_flow",
        "update_north_hold",
        "update_margin_trading",
        "update_dragon_tiger",
        "update_block_trade",
    ],
    "valuation": [
        "update_historical_valuation",
        "update_quarterly_financials",
        "update_shareholder_count",
        "update_dividend_summary",
    ],
    "macro": [
        "update_china_macro",
        "update_gold_price",
        "update_crude_oil",
        "update_usd",
        "update_global_index",
        "update_us_treasury",
        "update_futures",
    ],
    "sector_index": [
        "update_sector_industry",
        "update_industry",
        "update_sector_derivatives",
        "update_index_daily",
        "update_limit_up_down",
    ],
    "derivatives": [
        "update_etf_daily",
        "update_cb_quotation",
        "update_cb_redeem",
        "update_cb_index",
        "update_south_flow",
        "update_ah_premium",
    ],
    "events": [
        "update_restricted_share",
        "update_earnings_forecast",
        "update_stock_repurchase",
        "update_institution_survey",
        "update_stock_pledge",
        "update_option_sentiment",
    ],
}


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

    async def on_button_pressed(self, event: Button.Pressed) -> None:
        button_id = event.button.id
        if button_id is None:
            return
        group_key = button_id.replace("group-", "")
        from typing import cast
        await cast(PipelineApp, self.app).action_run_task_group(group_key)


class DataCompletenessWidget(VerticalScroll):
    # 任务名 → 表名列表的映射（用于判断哪些表正在更新）
    TASK_TO_TABLE: dict[str, list[str]] = {
        "update_stock_list": ["stock_list"],
        "update_bars": ["daily_bars"],
        "update_indicators": ["indicators"],
        "update_fundamentals": ["fundamentals"],
        "update_fund_flow": ["fund_flow"],
        "update_margin_trading": ["margin_trading"],
        "update_dragon_tiger": ["dragon_tiger"],
        "update_block_trade": ["block_trade"],
        "update_sector_fund_flow": ["sector_fund_flow"],
        "update_shareholder_count": ["shareholder_count"],
        "update_quarterly_financials": ["quarterly_financials"],
        "update_historical_valuation": ["historical_valuation"],
        "update_sector_industry": ["sector_industry"],
        "update_institutional_holdings": ["institutional_holdings"],
        "update_north_flow": ["north_flow"],
        "update_north_hold": ["north_hold"],
        "update_index_daily": ["index_daily"],
        "update_limit_up_down": ["limit_up_down"],
        "update_dividend_summary": ["dividend_summary"],
        "update_gold_price": ["gold_price"],
        "update_crude_oil": ["crude_oil"],
        "update_usd": ["fx_rate"],
        "update_global_index": ["global_index"],
        "update_us_treasury": ["us_treasury"],
        "update_futures": ["futures_daily"],
        "update_chip_distribution": ["chip_distribution"],
        "update_chip_distribution_em": ["chip_distribution_em"],
        "update_south_flow": ["south_flow"],
        "update_ah_premium": ["ah_premium"],
        "update_etf_daily": ["etf_daily"],
        "update_cb_quotation": ["cb_quotation"],
        "update_cb_redeem": ["cb_redeem"],
        "update_cb_index": ["cb_index"],
        "update_restricted_share": ["restricted_share"],
        "update_earnings_forecast": ["earnings_forecast"],
        "update_stock_repurchase": ["stock_repurchase"],
        "update_institution_survey": ["institution_survey"],
        "update_stock_pledge": ["stock_pledge"],
        "update_option_sentiment": ["option_sentiment"],
        "update_sector_derivatives": ["sector_daily", "sector_valuation", "index_futures_basis"],
        "update_china_macro": ["macro_monthly", "macro_quarterly"],
    }

    TABLE_LABELS: dict[str, str] = {
        # 行情核心
        "daily_bars": "Daily Bars",
        "indicators": "Indicators",
        # 基本面
        "fundamentals": "Fundamentals",
        "historical_valuation": "Valuation",
        "quarterly_financials": "Quarterly Fin.",
        "dividend_summary": "Dividends",
        # 资金面
        "fund_flow": "Fund Flow",
        "margin_trading": "Margin Trading",
        "dragon_tiger": "Dragon Tiger",
        "block_trade": "Block Trade",
        "sector_fund_flow": "Sector Flow",
        "north_flow": "North Flow",
        "north_hold": "North Hold",
        # 行业/大盘
        "sector_industry": "Industry",
        "index_daily": "Index Daily",
        "limit_up_down": "Limit U/D",
        # 股东
        "shareholder_count": "Shareholders",
        "institutional_holdings": "Inst. Holdings",
        # 筹码分布
        "chip_distribution": "Chip Dist.",
        "chip_distribution_em": "Chip EM",
        # 宏观
        "gold_price": "Gold Price",
        "crude_oil": "Crude Oil",
        "fx_rate": "USD/CNY",
        "global_index": "Global Index",
        "us_treasury": "US Treasury",
        # 期货
        "futures_daily": "Futures",
        # 衍生数据
        "south_flow": "South Flow",
        "ah_premium": "AH Premium",
        "etf_daily": "ETF Daily",
        "cb_quotation": "CB Quotation",
        "cb_redeem": "CB Redeem",
        "cb_index": "CB Index",
        "restricted_share": "Restricted Share",
        "earnings_forecast": "Earnings Forecast",
        "stock_repurchase": "Stock Repurchase",
        "institution_survey": "Institution Survey",
        "stock_pledge": "Stock Pledge",
        "option_sentiment": "Option Sentiment",
        "sector_daily": "Sector Daily",
        "sector_valuation": "Sector Val.",
        "index_futures_basis": "Futures Basis",
        # 宏观
        "macro_monthly": "Macro Monthly",
        "macro_quarterly": "Macro Quarterly",
        # 总览
        "stock_list": "Stock List",
    }

    TABLE_LABELS_CN: dict[str, str] = {
        # 行情核心
        "daily_bars": "日线行情",
        "indicators": "技术指标",
        # 基本面
        "fundamentals": "基本面数据",
        "historical_valuation": "历史估值",
        "quarterly_financials": "季度财务",
        "dividend_summary": "分红信息",
        # 资金面
        "fund_flow": "资金流向",
        "margin_trading": "融资融券",
        "dragon_tiger": "龙虎榜",
        "block_trade": "大宗交易",
        "sector_fund_flow": "板块资金",
        "north_flow": "北向资金",
        "north_hold": "北向持仓",
        # 行业/大盘
        "sector_industry": "行业分类",
        "index_daily": "大盘指数",
        "limit_up_down": "涨跌停",
        # 股东
        "shareholder_count": "股东户数",
        "institutional_holdings": "机构持仓",
        # 筹码分布
        "chip_distribution": "筹码分布",
        "chip_distribution_em": "筹码分布(EM)",
        # 宏观
        "gold_price": "黄金价格",
        "crude_oil": "原油价格",
        "fx_rate": "汇率",
        "global_index": "全球指数",
        "us_treasury": "美债收益率",
        # 期货
        "futures_daily": "期货日线",
        # 衍生数据
        "south_flow": "南向资金",
        "ah_premium": "AH溢价",
        "etf_daily": "ETF日线",
        "cb_quotation": "可转债行情",
        "cb_redeem": "可转债强赎",
        "cb_index": "可转债指数",
        "restricted_share": "限售解禁",
        "earnings_forecast": "业绩预告",
        "stock_repurchase": "股票回购",
        "institution_survey": "机构调研",
        "stock_pledge": "股票质押",
        "option_sentiment": "期权情绪",
        "sector_daily": "行业涨跌幅",
        "sector_valuation": "板块估值",
        "index_futures_basis": "基差",
        # 宏观
        "macro_monthly": "宏观(月)",
        "macro_quarterly": "宏观(季)",
        # 总览
        "stock_list": "股票列表",
    }

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
        """返回指定表的新鲜度状态标签（纯文本）。"""
        if updating_tables and tbl in updating_tables:
            return "更新中"
        if tbl in WEEKLY_TABLES and latest:
            return "按周更新"
        if tbl in MONTHLY_TABLES and latest:
            return "按月更新"
        if tbl in QUARTERLY_TABLES and latest:
            return "按季更新"
        if tbl in DELAYED_PUBLISH_TABLES and latest:
            return "T+1"
        return _date_status(latest, expected_date)

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

            if status in ("最新", "T+1", "按月更新", "按季更新"):
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
        self.set_interval(2.0, self.update_progress)

    def update_progress(self) -> None:
        progress = parse_progress(str(PROGRESS_JSON_PATH))
        if not progress:
            self.update(" [dim]当前无运行中的任务[/dim]")
            self.remove_class("active-task")
            return

        processed = progress.get("processed", 0)
        total = progress.get("total", 0)
        last_symbol = progress.get("last_symbol", "")
        failed_count = len(progress.get("failed_queue", []))

        pct = (processed / total * 100) if total > 0 else 0
        bar_length = 20
        filled = min(bar_length, max(0, int(bar_length * processed / total))) if total > 0 else 0
        bar = "█" * filled + "░" * (bar_length - filled)

        text = (
            f" [yellow]{progress.get('task')}[/yellow]  "
            f"[bold #e2e8f0]{pct:.1f}%[/bold #e2e8f0] "
            f"([cyan]{processed}[/cyan]/[cyan]{total}[/cyan])  "
            f"[bold #c084fc]{bar}[/bold #c084fc]\n"
            f" [dim]当前股票：[/dim][cyan]{last_symbol}[/cyan]  "
            f"[dim]失败：[/dim][bold red]{failed_count}[/bold red]\n"
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

            if "=" in body:
                body = body.replace("=", "-")

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
            if "=" in line:
                line = line.replace("=", "-")
            return escape(line)

    def tail_log(self) -> None:
        try:
            latest = find_latest_log_file(str(LOGS_DIR_PATH))
            if not latest:
                return

            if latest != self.active_log:
                if self.file_handle:
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
        Binding("s", "run_pipeline", "Full Update"),
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
                self._create_background_task(self._run_in_background(*args))
            elif choice == "run-later":
                delay = _seconds_until_safe()
                self.notify(
                    f"「{action_name}」已调度到安全时间后自动运行（剩余 {delay//60} 分钟）",
                    timeout=6.0,
                )
                async def _delayed():
                    await asyncio.sleep(delay)
                    self._create_background_task(self._run_in_background(*args))
                self._background_tasks.add(asyncio.create_task(_delayed()))

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
            "全量更新",
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

    async def action_run_reconcile(self) -> None:
        """全量清洗：对比 AkShare 并修复差异。"""
        if self._current_process is not None:
            self.notify("已有任务在运行，请等待完成", severity="warning", timeout=3.0)
            return
        reconcile_path = str(Path(__file__).parent / "scripts" / "reconcile_with_akshare.py")
        self.notify("全量数据清洗启动（对比 AkShare 并修复差异）", timeout=5.0)
        self._create_background_task(
            self._run_in_background(
                sys.executable, reconcile_path, "--workers", "3"
            )
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
        new_theme = themes[(idx + 1) % len(themes)]
        self._theme_name = new_theme
        self.theme = new_theme
        save_theme(new_theme)
        self.notify(f"主题已切换: {new_theme} ({idx + 2}/{len(themes)})", timeout=3.0)

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
