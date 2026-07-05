import os
import sqlite3
import subprocess
from pathlib import Path
from typing import Tuple, Optional
from textual.app import App, ComposeResult
from textual.containers import Grid
from textual.widgets import Footer, Header, Static

DEFAULT_DB_PATH = Path.home() / "Code/data/quant_data/quant_core.db"
DAEMON_PID_PATH = "/tmp/smartmoney_daemon.pid"

def get_db_size(db_path: str) -> str:
    p = Path(db_path)
    if p.exists():
        bytes_size = p.stat().st_size
        return f"{bytes_size / (1024 * 1024):.2f} MB"
    return "0.00 MB"

def get_daemon_status(pid_path: str) -> Tuple[str, Optional[int]]:
    p = Path(pid_path)
    if not p.exists():
        return "Stopped", None
    try:
        pid = int(p.read_text(encoding="utf-8").strip())
        os.kill(pid, 0)
        return "Running", pid
    except (ValueError, OSError):
        return "Stopped", None

def get_launchd_status() -> bool:
    try:
        res = subprocess.run(["launchctl", "list"], capture_output=True, text=True)
        return "com.smartmoney.update" in res.stdout
    except Exception:
        return False

def get_active_stock_count(db_path: str) -> int:
    p = Path(db_path)
    if not p.exists():
        return 0
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM stock_list")
        count = cursor.fetchone()[0]
        conn.close()
        return count
    except Exception:
        return 0

class DashboardWidget(Static):
    def on_mount(self) -> None:
        self.update_status()
        self.set_interval(2.0, self.update_status)

    def update_status(self) -> None:
        db_size = get_db_size(str(DEFAULT_DB_PATH))
        active_stocks = get_active_stock_count(str(DEFAULT_DB_PATH))
        daemon_status, daemon_pid = get_daemon_status(DAEMON_PID_PATH)
        launchd_active = get_launchd_status()
        
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
    pass

class ProgressWidget(Static):
    pass

class LogsWidget(Static):
    pass

class PipelineApp(App):
    TITLE = "SmartMoney Pipeline Manager"
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
            yield LogsWidget("Live Logs", id="live-logs")
            yield OperationsWidget("Operations", id="operations")
            yield ProgressWidget("Progress", id="scraping-progress")
        yield Footer()

if __name__ == "__main__":
    app = PipelineApp()
    app.run()

