#!/usr/bin/env python3
"""
SmartMoney 数据管道守护进程
────────────────────────
用法:
    python scripts/daemon.py start [--resume]
    python scripts/daemon.py stop

功能：后台循环执行 daily_pipeline.py --task update_bars
- 正常完成 → 休眠 1 小时后重试
- 异常退出 → 30 秒后自动重启
- 中断信号 → 优雅退出
"""
from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

PIDFILE = Path("/tmp/smartmoney_daemon.pid")
PIPELINE_DIR = Path(__file__).resolve().parent.parent
LOG_DIR = Path.home() / "Code/quant_data/logs"
LOG_FILE = LOG_DIR / "daemon.log"


def _log(msg: str, f):
    f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} {msg}\n")
    f.flush()


def is_running() -> bool:
    if not PIDFILE.exists():
        return False
    try:
        pid = int(PIDFILE.read_text().strip())
        os.kill(pid, 0)
        return True
    except (ValueError, OSError):
        return False


def start(resume: bool = False) -> None:
    os.nice(10)
    if is_running():
        old_pid = PIDFILE.read_text().strip()
        print(f"⚠️  守护进程已在运行 (PID: {old_pid})")
        sys.exit(1)

    # fork 进入后台
    pid = os.fork()
    if pid > 0:
        print(f"✅ 守护进程已启动 (PID: {pid})")
        print(f"   日志: {LOG_FILE}")
        sys.exit(0)

    # ── 子进程：守护循环 ──
    PIDFILE.write_text(str(os.getpid()))

    task_args = ["--task", "update_bars", "--force"]
    if resume:
        task_args.append("--resume")

    LOG_DIR.mkdir(parents=True, exist_ok=True)

    with open(LOG_FILE, "a", encoding="utf-8") as log:
        _log("🚀 守护进程启动", log)

        while True:
            _log("▶️  启动数据更新任务", log)

            proc = subprocess.run(
                [sys.executable, "daily_pipeline.py"] + task_args,
                cwd=str(PIPELINE_DIR),
                stdout=log,
                stderr=log,
            )

            if proc.returncode == 0:
                _log("✅ 任务正常完成，休眠 1 小时后重试", log)
                time.sleep(3600)
            elif proc.returncode in (130, 143):
                _log("🛑 任务被中断，守护进程退出", log)
                break
            else:
                _log(f"⚠️  任务异常退出 (code={proc.returncode})，30 秒后重启", log)
                time.sleep(30)

    PIDFILE.unlink(missing_ok=True)
    sys.exit(0)


def stop() -> None:
    if not PIDFILE.exists():
        print("ℹ️  守护进程未运行")
        return

    pid = int(PIDFILE.read_text().strip())
    try:
        os.kill(pid, signal.SIGTERM)
        time.sleep(2)
        os.kill(pid, 0)
        os.kill(pid, signal.SIGKILL)
    except OSError:
        pass

    PIDFILE.unlink(missing_ok=True)
    print("✅ 守护进程已停止")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    cmd = sys.argv[1]
    if cmd == "start":
        start(resume="--resume" in sys.argv)
    elif cmd == "stop":
        stop()
    else:
        print(f"未知命令: {cmd}")
        print(__doc__)
        sys.exit(1)
