"""进程与日志管理相关服务函数。"""

from __future__ import annotations

import asyncio
import glob
import json
import os
import subprocess
from pathlib import Path

from tui.config import DEFAULT_DB_PATH, PIPELINE_PID_PATH


def find_latest_log_file(logs_dir: str) -> str | None:
    """查找最新的日志文件。"""
    files = glob.glob(os.path.join(logs_dir, "smartmoney_*.log"))
    if not files:
        daemon_log = os.path.join(logs_dir, "daemon.log")
        return daemon_log if os.path.exists(daemon_log) else None
    return max(files, key=os.path.getmtime)


def parse_progress(progress_path: str) -> dict | None:
    """解析 progress.json 文件。"""
    p = Path(progress_path)
    if not p.exists():
        return None
    try:
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def get_daemon_status(pid_path: str | Path) -> tuple[str, int | None]:
    """查询后台守护进程运行状态。"""
    p = Path(pid_path)
    if not p.exists():
        return "Stopped", None
    try:
        pid = int(p.read_text(encoding="utf-8").strip())
        os.kill(pid, 0)
        res = subprocess.run(
            ["ps", "-p", str(pid), "-o", "command="],
            capture_output=True,
            text=True,
            timeout=2.0,
        )
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
    import sys

    tui_mod = sys.modules.get("tui")
    path_cls = getattr(tui_mod, "Path", Path) if tui_mod else Path
    subproc = getattr(tui_mod, "subprocess", subprocess) if tui_mod else subprocess
    pid_path_val = (
        getattr(tui_mod, "PIPELINE_PID_PATH", PIPELINE_PID_PATH)
        if tui_mod
        else PIPELINE_PID_PATH
    )

    processes: list[dict[str, str | int]] = []
    # 1. 检查 pidfile
    pidfile = path_cls(pid_path_val)
    if pidfile.exists():
        try:
            pid = int(pidfile.read_text().strip())
            os.kill(pid, 0)
            res = subproc.run(
                ["ps", "-p", str(pid), "-o", "pid=,etime=,command="],
                capture_output=True,
                text=True,
                timeout=2.0,
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
        res = subproc.run(
            ["pgrep", "-f", "daily_pipeline\\.py"],
            capture_output=True,
            text=True,
            timeout=2.0,
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
                        capture_output=True,
                        text=True,
                        timeout=2.0,
                    )
                    ppid = int(ppid_res.stdout.strip())
                    if ppid == os.getpid():
                        continue
                except Exception:
                    continue
            res2 = subprocess.run(
                ["ps", "-p", str(pid), "-o", "pid=,etime=,command="],
                capture_output=True,
                text=True,
                timeout=2.0,
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


def get_subprocess_env() -> dict[str, str]:
    """构造管道子进程的统一运行环境变量。"""
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


async def get_launchd_status(env: dict[str, str] | None = None) -> bool:
    """异步查询 launchd 中 com.smartmoney.update 服务是否活跃。"""
    if env is None:
        env = get_subprocess_env()
    try:
        res = await asyncio.to_thread(
            subprocess.run,
            ["launchctl", "list"],
            capture_output=True,
            text=True,
            env=env,
        )
        return "com.smartmoney.update" in res.stdout
    except Exception:
        return False
