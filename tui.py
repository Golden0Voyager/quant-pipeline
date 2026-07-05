import asyncio
import glob
import json
import logging
import os
import sqlite3
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Grid
from textual.widgets import Footer, Header, Static, RichLog

DEFAULT_DB_PATH = Path.home() / "Code/data/quant_data/quant_core.db"
DAEMON_PID_PATH = "/tmp/smartmoney_daemon.pid"
PROGRESS_JSON_PATH = Path.home() / "Code/data/quant_data/progress.json"
LOGS_DIR_PATH = Path.home() / "Code/data/quant_data/logs"


def find_latest_log_file(logs_dir: str) -> Optional[str]:
    files = glob.glob(os.path.join(logs_dir, "smartmoney_*.log"))
    if not files:
        daemon_log = os.path.join(logs_dir, "daemon.log")
        return daemon_log if os.path.exists(daemon_log) else None
    return max(files, key=os.path.getmtime)

def parse_progress(progress_path: str) -> Optional[dict]:
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
        return f"{bytes_size / (1024 * 1024):.2f} MB"
    return "0.00 MB"

def get_daemon_status(pid_path: str) -> tuple[str, int | None]:
    p = Path(pid_path)
    if not p.exists():
        return "Stopped", None
    try:
        pid = int(p.read_text(encoding="utf-8").strip())
        os.kill(pid, 0)
        return "Running", pid
    except (ValueError, OSError):
        return "Stopped", None

def get_subprocess_env() -> dict:
    env = os.environ.copy()
    env["NO_PROXY"] = "push2his.eastmoney.com,*.eastmoney.com,*.sina.com,*.sina.cn"
    env["DISABLE_YFINANCE_FALLBACK"] = "1"
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

class DashboardWidget(Static):
    async def on_mount(self) -> None:
        await self.update_status()
        self.set_interval(2.0, self.update_status)

    async def update_status(self) -> None:
        db_size = get_db_size(str(DEFAULT_DB_PATH))
        active_stocks = await asyncio.to_thread(get_active_stock_count, str(DEFAULT_DB_PATH))
        daemon_status, daemon_pid = get_daemon_status(DAEMON_PID_PATH)
        launchd_active = await get_launchd_status()

        daemon_str = f"[green]Running (PID: {daemon_pid})[/green]" if daemon_status == "Running" else "[red]Stopped[/red]"
        launchd_str = "[green]Active[/green]" if launchd_active else "[red]Inactive[/red]"

        text = (
            "📊 SmartMoney 状态看板\n"
            "==========================\n"
            f"数据库大小:  {db_size}\n"
            f"有效股票数量: {active_stocks}\n"
            f"守护进程状态: {daemon_str}\n"
            f"定时任务状态: {launchd_str}\n"
        )
        self.update(text)


class OperationsWidget(Static):
    def on_mount(self) -> None:
        text = (
            "⚙️ 控制面板 (Operations)\n"
            "==========================\n"
            "快捷键操作：\n"
            "  • [R] 立即启动完整更新\n"
            "  • [M] 断点续续数据更新\n"
            "  • [D] 启动守护进程 (Daemon)\n"
            "  • [S] 停止守护进程 (Daemon)\n"
            "  • [H] 立即进行数据库健康检查\n"
            "  • [Q] 退出监控面板\n"
        )
        self.update(text)

class ProgressWidget(Static):
    def on_mount(self) -> None:
        self.update_progress()
        self.set_interval(2.0, self.update_progress)

    def update_progress(self) -> None:
        progress = parse_progress(str(PROGRESS_JSON_PATH))
        if not progress:
            self.update("📈 进度看板\n==========================\n当前无运行中的任务，或未生成进度文件。")
            return
        
        processed = progress.get("processed", 0)
        total = progress.get("total", 0)
        last_symbol = progress.get("last_symbol", "")
        failed_count = len(progress.get("failed_queue", []))
        
        pct = (processed / total * 100) if total > 0 else 0
        bar_length = 20
        filled = int(bar_length * processed / total) if total > 0 else 0
        bar = "█" * filled + "░" * (bar_length - filled)
        
        text = (
            "📈 数据抓取进度\n"
            "==========================\n"
            f"任务:     {progress.get('task')}\n"
            f"更新进度: [{bar}] {pct:.1f}% ({processed}/{total})\n"
            f"当前股票: {last_symbol}\n"
            f"失败数量: [red]{failed_count}[/red]\n"
        )
        self.update(text)

class LogsWidget(RichLog):
    def __init__(self, *args, **kwargs) -> None:
        kwargs.setdefault("markup", True)
        super().__init__(*args, **kwargs)

    def on_mount(self) -> None:
        self.active_log: Optional[str] = None
        self.file_handle = None
        self.set_interval(1.0, self.tail_log)

    def colorize_line(self, line: str) -> str:
        line = line.strip()
        if "INFO" in line:
            return f"[green]{line}[/green]"
        elif "WARN" in line:
            return f"[yellow]{line}[/yellow]"
        elif "ERROR" in line:
            return f"[red]{line}[/red]"
        elif "SUCCESS" in line:
            return f"[bold green]{line}[/bold green]"
        return line

    def tail_log(self) -> None:
        latest = find_latest_log_file(str(LOGS_DIR_PATH))
        if not latest:
            return

        if latest != self.active_log:
            self.active_log = latest
            if self.file_handle:
                self.file_handle.close()
            self.file_handle = open(latest, "r", encoding="utf-8", errors="ignore")
            # Seek to end on open
            self.file_handle.seek(0, os.SEEK_END)
            self.write(f"--- 绑定新日志文件: {os.path.basename(latest)} ---")

        if self.file_handle:
            lines = self.file_handle.readlines()
            for line in lines:
                self.write(self.colorize_line(line))

class PipelineApp(App):
    TITLE = "SmartMoney Pipeline Manager"
    BINDINGS = [
        Binding("r", "run_pipeline", "Run Pipeline"),
        Binding("m", "resume_pipeline", "Resume"),
        Binding("d", "start_daemon", "Start Daemon"),
        Binding("s", "stop_daemon", "Stop Daemon"),
        Binding("h", "run_health", "Health Check"),
        Binding("q", "quit", "Quit"),
    ]

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._background_tasks: set[asyncio.Task] = set()

    def _create_background_task(self, coro) -> None:
        task = asyncio.create_task(coro)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    CSS = """
    $cyan: #00ffff;
    $green: #00ff00;
    $magenta: #ff00ff;
    $yellow: #ffff00;
    $panel: #1e1e1e;

    Screen {
        background: #121212;
    }
    #main-grid {
        layout: grid;
        grid-size: 2 3;
        grid-rows: 1fr 1fr 1fr;
        grid-columns: 1fr 1fr;
        height: 100%;
        padding: 1;
    }
    #status-dashboard {
        border: double $cyan;
        background: $panel;
        padding: 1;
    }
    #operations {
        border: double $green;
        background: $panel;
        padding: 1;
    }
    #scraping-progress {
        border: double $magenta;
        background: $panel;
        padding: 1;
    }
    #live-logs {
        border: double $yellow;
        background: $panel;
        row-span: 3;
        padding: 1;
    }
    """

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Grid(id="main-grid"):
            yield DashboardWidget("Dashboard", id="status-dashboard")
            yield LogsWidget(id="live-logs")
            yield OperationsWidget("Operations", id="operations")
            yield ProgressWidget("Progress", id="scraping-progress")
        yield Footer()

    async def _run_in_background(self, *args: str) -> None:
        env = get_subprocess_env()
        logger = logging.getLogger("quant_pipeline.tui")
        try:
            proc = await asyncio.create_subprocess_exec(
                *args,
                env=env,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL
            )
            await proc.wait()
            if proc.returncode != 0:
                logger.error(f"Subprocess {' '.join(args)} exited with code {proc.returncode}")
        except Exception as e:
            logger.exception(f"Exception running subprocess {' '.join(args)}")

    async def action_run_pipeline(self) -> None:
        pipeline_path = str(Path(__file__).parent / "daily_pipeline.py")
        self._create_background_task(
            self._run_in_background(
                sys.executable, pipeline_path, "--task", "all", "--force"
            )
        )

    async def action_resume_pipeline(self) -> None:
        pipeline_path = str(Path(__file__).parent / "daily_pipeline.py")
        self._create_background_task(
            self._run_in_background(
                sys.executable, pipeline_path, "--task", "update_bars", "--resume", "--force"
            )
        )

    async def action_start_daemon(self) -> None:
        manager_path = str(Path(__file__).parent / "manager.sh")
        self._create_background_task(
            self._run_in_background(manager_path, "daemon-resume")
        )

    async def action_stop_daemon(self) -> None:
        manager_path = str(Path(__file__).parent / "manager.sh")
        self._create_background_task(
            self._run_in_background(manager_path, "daemon-stop")
        )

    async def action_run_health(self) -> None:
        pipeline_path = str(Path(__file__).parent / "daily_pipeline.py")
        self._create_background_task(
            self._run_in_background(
                sys.executable, pipeline_path, "--task", "health_check", "--force"
            )
        )

if __name__ == "__main__":
    app = PipelineApp()
    app.run()

