import asyncio
import glob
import json
import logging
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

from rich.markup import escape
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Grid
from textual.widgets import Footer, Header, RichLog, Static

DEFAULT_DB_PATH = Path.home() / "Code/data/quant_data/quant_core.db"
DAEMON_PID_PATH = "/tmp/smartmoney_daemon.pid"
PROGRESS_JSON_PATH = Path.home() / "Code/data/quant_data/progress.json"
LOGS_DIR_PATH = Path.home() / "Code/data/quant_data/logs"


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
        if "daily_pipeline.py" in res.stdout or "manager.sh" in res.stdout:
            return "Running", pid
        return "Stopped", None
    except (ValueError, OSError, subprocess.SubprocessError):
        return "Stopped", None

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
        self.border_title = "📊 SmartMoney 状态看板"
        await self.update_status()
        self.set_interval(2.0, self.update_status)

    async def update_status(self) -> None:
        db_size = get_db_size(str(DEFAULT_DB_PATH))
        active_stocks = await asyncio.to_thread(get_active_stock_count, str(DEFAULT_DB_PATH))
        daemon_status, daemon_pid = await asyncio.to_thread(get_daemon_status, DAEMON_PID_PATH)
        launchd_active = await get_launchd_status()

        daemon_str = f"[bold green]Running[/bold green] [gray](PID: {daemon_pid})[/gray]" if daemon_status == "Running" else "[bold red]Stopped[/bold red]"
        launchd_str = "[bold green]Active[/bold green]" if launchd_active else "[bold red]Inactive[/bold red]"

        text = (
            f" • [bold gray]数据库大小：[/bold gray]  [cyan]{db_size}[/cyan]\n"
            f" • [bold gray]有效股票数：[/bold gray]  [cyan]{active_stocks}[/cyan]\n"
            f" • [bold gray]守护进程：[/bold gray]    {daemon_str}\n"
            f" • [bold gray]定时任务：[/bold gray]    {launchd_str}\n"
        )
        self.update(text)


class OperationsWidget(Static):
    def on_mount(self) -> None:
        self.border_title = "⚙️ 控制面板 (Operations)"
        text = (
            " [bold #f1f5f9 on #334155] R [/]  立即启动完整更新\n"
            " [bold #f1f5f9 on #334155] M [/]  断点续传数据更新\n"
            " [bold #f1f5f9 on #334155] D [/]  启动守护进程 (Daemon)\n"
            " [bold #f1f5f9 on #334155] S [/]  停止守护进程 (Daemon)\n"
            " [bold #f1f5f9 on #334155] H [/]  立即进行数据健康检查\n"
            " [bold #f1f5f9 on #334155] Q [/]  退出系统监控面板\n"
        )
        self.update(text)


class ProgressWidget(Static):
    def on_mount(self) -> None:
        self.border_title = "📈 进度看板"
        self.update_progress()
        self.set_interval(2.0, self.update_progress)

    def update_progress(self) -> None:
        progress = parse_progress(str(PROGRESS_JSON_PATH))
        if not progress:
            self.update(" 当前无运行中的任务，或未生成进度文件。")
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
            f" • [bold gray]当前任务：[/bold gray]  [yellow]{progress.get('task')}[/yellow]\n"
            f" • [bold gray]更新进度：[/bold gray]  [bold #e2e8f0]{pct:.1f}%[/bold #e2e8f0] ([cyan]{processed}[/cyan]/[cyan]{total}[/cyan])\n"
            f"            [bold #c084fc]{bar}[/bold #c084fc]\n"
            f" • [bold gray]当前股票：[/bold gray]  [cyan]{last_symbol}[/cyan]\n"
            f" • [bold gray]失败数量：[/bold gray]  [bold red]{failed_count}[/bold red]\n"
        )
        self.update(text)


class LogsWidget(RichLog):
    def __init__(self, *args, **kwargs) -> None:
        kwargs.setdefault("markup", True)
        kwargs.setdefault("max_lines", 1000)
        super().__init__(*args, **kwargs)

    def on_mount(self) -> None:
        self.border_title = "📋 实时系统日志"
        self.active_log: str | None = None
        self.file_handle = None
        self.set_interval(1.0, self.tail_log)

    def on_unmount(self) -> None:
        if self.file_handle:
            try:  # noqa: SIM105
                self.file_handle.close()
            except Exception:
                pass
            self.file_handle = None

    def colorize_line(self, line: str) -> str:
        line = escape(line.strip())
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
                self.write(f"--- 绑定新日志文件: {os.path.basename(latest)} ---")

            if self.file_handle:
                lines = self.file_handle.readlines()
                for line in lines:
                    self.write(self.colorize_line(line))
        except Exception as e:
            self.write(f"[red]Error tailing log: {escape(str(e))}[/red]")

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
    $border-normal: #334155;
    $border-hover: #475569;
    $border-focus: #38bdf8;
    $bg-panel: #111827;
    $bg-screen: #030712;

    Screen {
        background: $bg-screen;
    }
    #main-grid {
        layout: grid;
        grid-size: 2 3;
        grid-rows: 1fr 1fr 1fr;
        grid-columns: 1fr 1fr;
        height: 100%;
        padding: 1 2;
    }
    #status-dashboard, #operations, #scraping-progress, #live-logs {
        border: round $border-normal;
        background: $bg-panel;
        padding: 1 2;
        border-title-align: left;
        border-title-color: #94a3b8;
    }
    #status-dashboard:hover, #operations:hover, #scraping-progress:hover, #live-logs:hover {
        border: round $border-hover;
        border-title-color: #f8fafc;
    }
    #status-dashboard:focus, #operations:focus, #scraping-progress:focus, #live-logs:focus {
        border: round $border-focus;
        border-title-color: #38bdf8;
    }
    #live-logs {
        row-span: 3;
    }
    """

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Grid(id="main-grid"):
            yield DashboardWidget(id="status-dashboard")
            yield LogsWidget(id="live-logs")
            yield OperationsWidget(id="operations")
            yield ProgressWidget(id="scraping-progress")
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
        except Exception:
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

