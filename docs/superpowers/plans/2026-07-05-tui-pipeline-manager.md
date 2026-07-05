# Data Pipeline TUI Manager Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create a Textual-based terminal user interface (TUI) to manage the SmartMoney data pipeline, consolidate runner/daemon commands, display live progress and logs, and clean up redundant zshrc aliases.

**Architecture:** Use the Textual framework to construct a reactive grid dashboard. Use background worker threads/asyncio processes to run daily_pipeline.py and tail logs asynchronously without blocking the UI main loop. Polling daemon PIDs and progress.json files determines states.

**Tech Stack:** Python, Textual, Rich, Asyncio.

## Global Constraints

- Requires Python >=3.11.
- TUI script name: `tui.py` at `/Users/hainingyu/code/quant_pipeline/tui.py`.
- TUI tests in `tests/test_tui.py`.
- Final alias name in ~/.zshrc: `datapipe`.
- Subprocesses must receive `NO_PROXY="push2his.eastmoney.com,*.eastmoney.com,*.sina.com,*.sina.cn"` and `DISABLE_YFINANCE_FALLBACK="1"`.

---

### Task 1: Project Setup & Dependency Scaffolding

**Files:**
- Modify: `/Users/hainingyu/code/quant_pipeline/pyproject.toml`
- Create: `/Users/hainingyu/code/quant_pipeline/tui.py`
- Create: `/Users/hainingyu/code/quant_pipeline/tests/test_tui.py`

**Interfaces:**
- Produces: `PipelineApp` class in `tui.py` representing the basic Textual App structure.

- [ ] **Step 1: Write the failing test**
  Create `tests/test_tui.py` with code to verify the App loads and has the correct title:
  ```python
  import pytest
  from tui import PipelineApp

  @pytest.mark.asyncio
  async def test_app_title():
      app = PipelineApp()
      async with app.run_test() as pilot:
          assert app.title == "SmartMoney Pipeline Manager"
  ```
- [ ] **Step 2: Run test to verify it fails**
  Run: `uv run pytest tests/test_tui.py -v`
  Expected: FAIL with `ModuleNotFoundError: No module named 'tui'` (and potentially missing `textual` package).
- [ ] **Step 3: Update pyproject.toml dependencies**
  Modify `/Users/hainingyu/code/quant_pipeline/pyproject.toml` to add `textual` and `rich` to dependencies block:
  ```toml
  dependencies = [
      "akshare>=1.0.0",
      "pandas>=2.0.0",
      "numpy>=1.24.0",
      "requests>=2.31.0",
      "textual>=0.50.0",
      "rich>=13.0.0",
  ]
  ```
- [ ] **Step 4: Implement minimal TUI boilerplate**
  Create `/Users/hainingyu/code/quant_pipeline/tui.py`:
  ```python
  from textual.app import App, ComposeResult
  from textual.widgets import Header, Footer

  class PipelineApp(App):
      TITLE = "SmartMoney Pipeline Manager"
      CSS = """
      Screen {
          background: #121212;
      }
      """

      def compose(self) -> ComposeResult:
          yield Header(show_clock=True)
          yield Footer()

  if __name__ == "__main__":
      app = PipelineApp()
      app.run()
  ```
- [ ] **Step 5: Run tests and verify they pass**
  Run: `uv run pytest tests/test_tui.py -v`
  Expected: PASS
- [ ] **Step 6: Commit**
  Run:
  ```bash
  git add pyproject.toml tui.py tests/test_tui.py
  git commit -m "feat: scaffold basic TUI application structure and test"
  ```

---

### Task 2: Dashboard UI Grid Layout

**Files:**
- Modify: `/Users/hainingyu/code/quant_pipeline/tui.py`

**Interfaces:**
- Consumes: `PipelineApp` class from Task 1.
- Produces: Layout containers and empty widgets for:
  - Header
  - Status Dashboard (`DashboardWidget`)
  - Operations (`OperationsWidget`)
  - Scraping Progress (`ProgressWidget`)
  - Live Logs (`LogsWidget`)

- [ ] **Step 1: Write UI layout test**
  Add a test in `tests/test_tui.py` to check all widgets are rendered:
  ```python
  @pytest.mark.asyncio
  async def test_widgets_present():
      from tui import PipelineApp
      app = PipelineApp()
      async with app.run_test() as pilot:
          assert app.query_one("#status-dashboard") is not None
          assert app.query_one("#operations") is not None
          assert app.query_one("#scraping-progress") is not None
          assert app.query_one("#live-logs") is not None
  ```
- [ ] **Step 2: Run test to verify it fails**
  Run: `uv run pytest tests/test_tui.py::test_widgets_present -v`
  Expected: FAIL with `NoMatches` (widgets not found).
- [ ] **Step 3: Define Layout Widgets in `tui.py`**
  Modify `/Users/hainingyu/code/quant_pipeline/tui.py` to construct the layout using Textual `Static`, `Container` and custom CSS:
  ```python
  from textual.app import App, ComposeResult
  from textual.widgets import Header, Footer, Static
  from textual.containers import Container, Grid

  class DashboardWidget(Static):
      pass

  class OperationsWidget(Static):
      pass

  class ProgressWidget(Static):
      pass

  class LogsWidget(Static):
      pass

  class PipelineApp(App):
      TITLE = "SmartMoney Pipeline Manager"
      CSS = """
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
          grid-row-span: 3;
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
  ```
- [ ] **Step 4: Run tests and verify they pass**
  Run: `uv run pytest tests/test_tui.py::test_widgets_present -v`
  Expected: PASS
- [ ] **Step 5: Commit**
  Run:
  ```bash
  git add tui.py tests/test_tui.py
  git commit -m "feat: design grid layouts for dashboard widgets"
  ```

---

### Task 3: Status Panel & Database Stats Integration

**Files:**
- Modify: `/Users/hainingyu/code/quant_pipeline/tui.py`

**Interfaces:**
- Consumes: `DashboardWidget` from Task 2.
- Produces: Periodic update loops rendering:
  - Database file size.
  - Active stock counts from the DB.
  - launchd Plist statuses.
  - Running daemon status.

- [ ] **Step 1: Write status retrieval logic unit test**
  Add unit tests in `tests/test_tui.py` to mock and verify helper functions for reading DB sizes and launchd statuses:
  ```python
  from tui import get_db_size, get_daemon_status

  def test_get_db_size(tmp_path):
      db_file = tmp_path / "test.db"
      db_file.write_bytes(b"\x00" * 1024 * 1024 * 2) # 2MB
      size_str = get_db_size(str(db_file))
      assert size_str == "2.00 MB"

  def test_get_daemon_status_inactive(tmp_path):
      pid_file = tmp_path / "daemon.pid"
      # If pid file doesn't exist, status is stopped
      status, pid = get_daemon_status(str(pid_file))
      assert status == "Stopped"
      assert pid is None
  ```
- [ ] **Step 2: Run test to verify it fails**
  Run: `uv run pytest tests/test_tui.py -v`
  Expected: FAIL with `ImportError: cannot import name 'get_db_size'`
- [ ] **Step 3: Implement helper methods and status rendering**
  Modify `/Users/hainingyu/code/quant_pipeline/tui.py` to add functions and update the widget every 2 seconds:
  ```python
  import os
  import subprocess
  from pathlib import Path
  from typing import Tuple, Optional

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
          # Check if process is running on Mac/Linux
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

  class DashboardWidget(Static):
      def on_mount(self) -> None:
          self.set_interval(2.0, self.update_status)

      def update_status(self) -> None:
          db_size = get_db_size(str(DEFAULT_DB_PATH))
          daemon_status, daemon_pid = get_daemon_status(DAEMON_PID_PATH)
          launchd_active = get_launchd_status()
          
          daemon_str = f"[green]Running (PID: {daemon_pid})[/green]" if daemon_status == "Running" else "[red]Stopped[/red]"
          launchd_str = "[green]Active[/green]" if launchd_active else "[red]Inactive[/red]"
          
          text = (
              "📊 SmartMoney 状态看板\n"
              "==========================\n"
              f"数据库大小:  {db_size}\n"
              f"守护进程状态: {daemon_str}\n"
              f"定时任务状态: {launchd_str}\n"
          )
          self.update(text)
  ```
- [ ] **Step 4: Run tests and verify they pass**
  Run: `uv run pytest tests/test_tui.py -v`
  Expected: PASS
- [ ] **Step 5: Commit**
  Run:
  ```bash
  git add tui.py tests/test_tui.py
  git commit -m "feat: implement database and daemon status widgets with update loops"
  ```

---

### Task 4: Interactive Subprocess Control

**Files:**
- Modify: `/Users/hainingyu/code/quant_pipeline/tui.py`

**Interfaces:**
- Consumes: Keyboard events in `PipelineApp`.
- Produces: Asynchronous workers starting/stopping background scraper updates and daemon processes.

- [ ] **Step 1: Write keyboard bindings test**
  Add a test to verify key bindings map to actions:
  ```python
  @pytest.mark.asyncio
  async def test_key_bindings():
      from tui import PipelineApp
      app = PipelineApp()
      async with app.run_test() as pilot:
          # Verify action exists
          assert app.check_action("run_pipeline") is True
          assert app.check_action("resume_pipeline") is True
          assert app.check_action("start_daemon") is True
          assert app.check_action("stop_daemon") is True
  ```
- [ ] **Step 2: Run test to verify it fails**
  Run: `uv run pytest tests/test_tui.py::test_key_bindings -v`
  Expected: FAIL (actions not defined on App).
- [ ] **Step 3: Implement Actions and Operations Widget in `tui.py`**
  Modify `/Users/hainingyu/code/quant_pipeline/tui.py`:
  ```python
  import asyncio
  from textual.binding import Binding

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

  class PipelineApp(App):
      TITLE = "SmartMoney Pipeline Manager"
      # (CSS is preserved)

      BINDINGS = [
          Binding("r", "run_pipeline", "Run Pipeline"),
          Binding("m", "resume_pipeline", "Resume"),
          Binding("d", "start_daemon", "Start Daemon"),
          Binding("s", "stop_daemon", "Stop Daemon"),
          Binding("h", "run_health", "Health Check"),
          Binding("q", "quit", "Quit"),
      ]

      def _get_subprocess_env(self) -> dict:
          env = os.environ.copy()
          env["NO_PROXY"] = "push2his.eastmoney.com,*.eastmoney.com,*.sina.com,*.sina.cn"
          env["DISABLE_YFINANCE_FALLBACK"] = "1"
          return env

      async def action_run_pipeline(self) -> None:
          env = self._get_subprocess_env()
          # Non-blocking async execution
          asyncio.create_task(
              asyncio.create_subprocess_exec(
                  "python", "daily_pipeline.py", "--task", "all", "--force",
                  env=env
              )
          )

      async def action_resume_pipeline(self) -> None:
          env = self._get_subprocess_env()
          asyncio.create_task(
              asyncio.create_subprocess_exec(
                  "python", "daily_pipeline.py", "--task", "update_bars", "--resume", "--force",
                  env=env
              )
          )

      async def action_start_daemon(self) -> None:
          env = self._get_subprocess_env()
          asyncio.create_task(
              asyncio.create_subprocess_exec(
                  "./manager.sh", "daemon-resume",
                  env=env
              )
          )

      async def action_stop_daemon(self) -> None:
          asyncio.create_task(
              asyncio.create_subprocess_exec("./manager.sh", "daemon-stop")
          )

      async def action_run_health(self) -> None:
          asyncio.create_task(
              asyncio.create_subprocess_exec("python", "daily_pipeline.py", "--task", "health_check", "--force")
          )
  ```
- [ ] **Step 4: Run tests and verify they pass**
  Run: `uv run pytest tests/test_tui.py -v`
  Expected: PASS
- [ ] **Step 5: Commit**
  Run:
  ```bash
  git add tui.py tests/test_tui.py
  git commit -m "feat: implement keybindings and subprocess control runners"
  ```

---

### Task 5: Progress Board Parser

**Files:**
- Modify: `/Users/hainingyu/code/quant_pipeline/tui.py`

**Interfaces:**
- Consumes: `/Users/hainingyu/Code/data/quant_data/progress.json`
- Produces: Calculated percentages, success rate counts, progress bar widgets.

- [ ] **Step 1: Write test for progress parser**
  Add a test to parse and format progress status details:
  ```python
  from tui import parse_progress

  def test_parse_progress_valid(tmp_path):
      progress_file = tmp_path / "progress.json"
      progress_file.write_text("""{
          "task": "update_bars",
          "date": "2026-07-05",
          "start_time": "2026-07-05 12:00:00",
          "last_symbol": "SZ000001",
          "processed": 100,
          "total": 1000,
          "failed_queue": ["SH600000"]
      }""", encoding="utf-8")
      data = parse_progress(str(progress_file))
      assert data is not None
      assert data["processed"] == 100
      assert data["total"] == 1000
      assert len(data["failed_queue"]) == 1
  ```
- [ ] **Step 2: Run test to verify it fails**
  Run: `uv run pytest tests/test_tui.py::test_parse_progress_valid -v`
  Expected: FAIL with `ImportError` (no function `parse_progress`).
- [ ] **Step 3: Implement progress file watcher and renderer in `tui.py`**
  Modify `/Users/hainingyu/code/quant_pipeline/tui.py`:
  ```python
  import json

  PROGRESS_JSON_PATH = Path.home() / "Code/data/quant_data/progress.json"

  def parse_progress(progress_path: str) -> Optional[dict]:
      p = Path(progress_path)
      if not p.exists():
          return None
      try:
          with open(p, encoding="utf-8") as f:
              return json.load(f)
      except Exception:
          return None

  class ProgressWidget(Static):
      def on_mount(self) -> None:
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
  ```
- [ ] **Step 4: Run tests and verify they pass**
  Run: `uv run pytest tests/test_tui.py -v`
  Expected: PASS
- [ ] **Step 5: Commit**
  Run:
  ```bash
  git add tui.py tests/test_tui.py
  git commit -m "feat: parse progress.json and display progress bar inside TUI"
  ```

---

### Task 6: Async Live Log Viewer

**Files:**
- Modify: `/Users/hainingyu/code/quant_pipeline/tui.py`

**Interfaces:**
- Consumes: Daily log file `smartmoney_YYYYMMDD.log` or `daemon.log`.
- Produces: Appended line stream in scrolling widget.

- [ ] **Step 1: Write log lines finder test**
  Add a test to verify we fetch the correct latest log file:
  ```python
  from tui import find_latest_log_file

  def test_find_latest_log_file(tmp_path):
      # Create mock log files
      log1 = tmp_path / "smartmoney_20260704.log"
      log1.touch()
      log2 = tmp_path / "smartmoney_20260705.log"
      log2.touch()
      
      latest = find_latest_log_file(str(tmp_path))
      assert Path(latest).name == "smartmoney_20260705.log"
  ```
- [ ] **Step 2: Run test to verify it fails**
  Run: `uv run pytest tests/test_tui.py::test_find_latest_log_file -v`
  Expected: FAIL with `ImportError` (no function `find_latest_log_file`).
- [ ] **Step 3: Implement active log tracker and reader in `tui.py`**
  Modify `/Users/hainingyu/code/quant_pipeline/tui.py` to add log tailing background worker:
  ```python
  import glob
  from datetime import datetime
  from textual.widgets import RichLog

  LOGS_DIR_PATH = Path.home() / "Code/data/quant_data/logs"

  def find_latest_log_file(logs_dir: str) -> Optional[str]:
      files = glob.glob(os.path.join(logs_dir, "smartmoney_*.log"))
      if not files:
          daemon_log = os.path.join(logs_dir, "daemon.log")
          return daemon_log if os.path.exists(daemon_log) else None
      return max(files, key=os.path.getmtime)

  class LogsWidget(RichLog):
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
  ```
  And modify `compose` in `PipelineApp` to render `LogsWidget` instead of default `Static`:
  ```python
  # Change: yield LogsWidget("Live Logs", id="live-logs")
  # To:
  yield LogsWidget(id="live-logs")
  ```
- [ ] **Step 4: Run tests and verify they pass**
  Run: `uv run pytest tests/test_tui.py -v`
  Expected: PASS
- [ ] **Step 5: Commit**
  Run:
  ```bash
  git add tui.py tests/test_tui.py
  git commit -m "feat: integrate log tailing and formatting to logs pane"
  ```

---

### Task 7: Shell Integration & Cleanup

**Files:**
- Modify: `/Users/hainingyu/.zshrc`

**Interfaces:**
- Produces: Simplified user experience inside terminals.

- [ ] **Step 1: Check shell zshrc content**
  Search `/Users/hainingyu/.zshrc` for existing pipe- commands:
  Run: `grep -n "alias pipe-" /Users/hainingyu/.zshrc`
  Expected: Output lines 360 to 365.
- [ ] **Step 2: Clean up aliases and add datapipe**
  Modify `/Users/hainingyu/.zshrc` to remove the redundant `pipe-*` commands and add:
  ```bash
  alias datapipe='cd /Users/hainingyu/code/quant_pipeline && NO_PROXY="push2his.eastmoney.com,*.eastmoney.com,*.sina.com,*.sina.cn" DISABLE_YFINANCE_FALLBACK=1 uv run python tui.py'
  ```
- [ ] **Step 3: Verify the alias works manually**
  Run: `source ~/.zshrc`
  And execute `datapipe` in a test shell session to launch the TUI.
  Expected: TUI successfully opens.
- [ ] **Step 4: Commit**
  Run:
  ```bash
  git commit -a -m "chore: clean up pipe- aliases in zshrc and add unified datapipe alias"
  ```
