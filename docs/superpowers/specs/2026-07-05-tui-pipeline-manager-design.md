# Technical Specification: Data Pipeline TUI Manager

This document outlines the technical specification for the terminal user interface (TUI) designed to manage the SmartMoney data pipeline.

---

## 1. Overview & Goals
*   **Purpose**: Consolidate multiple CLI commands and cron/daemon scripts into a unified, visually appealing, interactive terminal interface.
*   **Core Alias**: `datapipe` (replacing all prior `pipe-*` commands).
*   **Visual Aesthetic**: Dark mode with neon-cyan and purple borders, styled tables, active progress bars, and real-time colored log streams.
*   **Dependencies**: Python `textual` and `rich`.

---

## 2. Interface Layout & Widget Architecture

The application will be built using a grid/dock layout in `Textual`:

```
+-----------------------------------------------------------------------+
| Header: SmartMoney Pipeline Manager v2.1      [DB: .../quant_core.db] |
+-----------------------------------+-----------------------------------+
| Status Dashboard                  | Live Logs                         |
| - DB Size: 2.38 GB                | [14:15:32] INFO | Pipeline init...|
| - Stock Count: 14,850             | [14:15:35] SUCCESS | Connected DB.|
| - Daemon Status: Running (PID)    | [14:15:43] WARN | Request failed. |
| - Launchd status: Active          |                                   |
+-----------------------------------+                                   |
| Operations                        |                                   |
| [R] Run Full Pipeline             |                                   |
| [M] Resume Scraping               |                                   |
| [D] Start Daemon                  |                                   |
| [S] Stop Daemon                   |                                   |
| [H] Health Check                  |                                   |
| [Q] Quit                          |                                   |
+-----------------------------------+                                   |
| Scraping Progress                 |                                   |
| [████████░░░░░░░░░] 40% (2100/52) |                                   |
| Success: 2055 | Failed: 45        |                                   |
| Speed: 18.5 stocks/sec | ETA: 3m  |                                   |
+-----------------------------------+-----------------------------------+
| Footer: q: Quit | r: Run | m: Resume | d: Start Daemon | s: Stop      |
+-----------------------------------------------------------------------+
```

---

## 3. Subprocess & Daemon Integration

The TUI must launch and interact with background tasks without freezing the UI event loop. We will use Textual's worker APIs and background threads.

### A. Manual Execution (Run / Resume)
*   **Action**: Pressing `R` or `M` runs `daily_pipeline.py` via Python's `asyncio.create_subprocess_exec` or `subprocess.Popen`.
*   **Execution Command**:
    *   **Run**: `python daily_pipeline.py --task all --force`
    *   **Resume**: `python daily_pipeline.py --task update_bars --resume --force`
*   **Progress Tracking**:
    *   The TUI will poll `~/Code/data/quant_data/progress.json` every 2 seconds.
    *   It parses the JSON to extract `processed`, `total`, `last_symbol`, and `failed_queue` to feed the **Scraping Progress** panel.

### B. Daemon Management
*   **Start Daemon (`D`)**:
    *   Triggers `manager.sh daemon-resume` (runs `daily_pipeline.py` with automatic retry/resume in the background).
    *   The daemon writes its status to `/tmp/smartmoney_daemon.pid` and outputs logs to `~/Code/data/quant_data/logs/daemon.log`.
*   **Stop Daemon (`S`)**:
    *   Triggers `manager.sh daemon-stop` (reads pidfile and terminates daemon process trees).
*   **Status Detection**:
    *   The TUI checks `/tmp/smartmoney_daemon.pid` and verifies if the process is active using `os.kill(pid, 0)`.

### C. Live Log Tailing
*   A background worker tails the active log file (determined by the current date: `~/Code/data/quant_data/logs/smartmoney_YYYYMMDD.log` or fallback to `daemon.log`).
*   New log lines are parsed, colorized using `Rich` print styles, and appended to the scrollable logs widget.

---

## 4. Environment Configuration

The TUI must load and inject necessary environment variables to all subprocesses:
```python
env_overrides = {
    "NO_PROXY": "push2his.eastmoney.com,*.eastmoney.com,*.sina.com,*.sina.cn",
    "DISABLE_YFINANCE_FALLBACK": "1",
    "QUANT_DB_PATH": "/Users/hainingyu/Code/data/quant_data/quant_core.db"
}
```

---

## 5. Shell Integration & Cleanup

Once `tui.py` is fully functional and verified:
1.  **Zsh Aliases Cleanup**: Modify `~/.zshrc` to remove the redundant `pipe-*` aliases.
2.  **Add `datapipe` Alias**:
    ```bash
    alias datapipe='cd /Users/hainingyu/code/quant_pipeline && NO_PROXY="push2his.eastmoney.com,*.eastmoney.com,*.sina.com,*.sina.cn" DISABLE_YFINANCE_FALLBACK=1 uv run python tui.py'
    ```

---

## 6. Implementation Stages
1.  **Stage 1: Setup & UI Mocking**: Write `tui.py` layout widgets using Textual.
2.  **Stage 2: Status & DB Stats**: Integrate database size calculation and launchd/daemon checks.
3.  **Stage 3: Subprocess Running & Progress Tracker**: Implement non-blocking runners for `daily_pipeline.py` and parse `progress.json` dynamically.
4.  **Stage 4: Live Log Viewer**: Build the async log tailing panel.
5.  **Stage 5: Shell Clean-up & Testing**: Update `~/.zshrc` and verify overall stability.
