#!/usr/bin/env python3
"""
SmartMoney 数据管道守护进程
────────────────────────
用法:
    python scripts/daemon.py start [--resume]
    python scripts/daemon.py stop

功能：后台循环执行 daily_pipeline.py --task update_bars
- 非交易日 → 跳过本轮，休眠 1 小时（交易日历不可用时放行，fail-open）
- 正常完成 → 休眠 1 小时后重试
- 异常退出 → 指数退避重启（30s 起，封顶 1 小时），连续失败 3 次外发告警
- 中断信号 → 转发给子进程进程组后优雅退出，不孤儿化正在跑的管道
- PID 文件使用 flock 持有，杜绝 PID 复用误判（与 core.lock.ProcessLock 同方案）
"""
from __future__ import annotations

import contextlib
import fcntl
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

# daemon.log 以 append 模式长期增长，超过 10MB 时启动期截断保留尾部 1MB
LOG_MAX_BYTES = 10 * 1024 * 1024
LOG_KEEP_BYTES = 1 * 1024 * 1024

# 异常重启退避：30s 起指数增长，封顶 1 小时；连续失败 3 次外发一次告警
BASE_BACKOFF_SECONDS = 30
MAX_BACKOFF_SECONDS = 3600
BACKOFF_NOTIFY_THRESHOLD = 3

if str(PIPELINE_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINE_DIR))


def _log(msg: str, f):
    f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} {msg}\n")
    f.flush()


def _backoff_seconds(failures: int) -> int:
    """连续失败 N 次后的退避秒数：30s 起指数增长，封顶 1 小时。"""
    return min(BASE_BACKOFF_SECONDS * (2 ** (failures - 1)), MAX_BACKOFF_SECONDS)


def _try_acquire_pidfile_lock():
    """以 flock 持有 PID 文件；成功返回文件对象（调用方负责持有），已被持有返回 None。"""
    fd = open(PIDFILE, "a+", encoding="utf-8")  # noqa: SIM115
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        fd.close()
        return None
    return fd


def is_running() -> bool:
    """flock 判定守护进程是否存活：PID 复用不会造成误判。"""
    if not PIDFILE.exists():
        return False
    fd = _try_acquire_pidfile_lock()
    if fd is None:
        return True
    fd.close()
    return False


def _truncate_log_if_oversized() -> None:
    """daemon.log 超过 10MB 时保留尾部 1MB（append 模式无法被按天清理覆盖）。"""
    try:
        if not LOG_FILE.exists() or LOG_FILE.stat().st_size <= LOG_MAX_BYTES:
            return
        with open(LOG_FILE, "rb") as f:
            f.seek(-LOG_KEEP_BYTES, os.SEEK_END)
            tail = f.read()
        LOG_FILE.write_bytes(tail)
    except OSError:
        pass


def _is_trading_day_now() -> bool:
    """上海时钟下的当日是否为 A 股交易日。"""
    from core.calendar import is_trading_day
    from core.market_time import shanghai_now

    return is_trading_day(shanghai_now().date())


def _should_run_pipeline() -> bool:
    """非交易日跳过本轮；日历判断失败时放行（fail-open），宁可空跑不可停跑。"""
    try:
        return _is_trading_day_now()
    except Exception:
        return True


def _notify_continuous_failures(failures: int, returncode: int, backoff: int) -> None:
    """连续失败达到阈值时外发告警；通知通道异常不影响守护循环。"""
    try:
        from core.notifications import notify_all

        notify_all(
            "error",
            "数据管道守护进程连续失败",
            f"连续 {failures} 次异常退出 (code={returncode})，已退避至 {backoff}s 重启",
        )
    except Exception:
        pass


def start(resume: bool = False) -> None:
    os.nice(10)
    # 在 fork 前完成 check-then-lock：父进程持有 flock，子进程继承并终身持有，
    # 消除"检查通过 → fork"之间两个实例同时启动的竞态窗口
    lock_fd = _try_acquire_pidfile_lock()
    if lock_fd is None:
        old_pid = PIDFILE.read_text().strip() if PIDFILE.exists() else "unknown"
        print(f"⚠️  守护进程已在运行 (PID: {old_pid})")
        sys.exit(1)

    # fork 进入后台
    pid = os.fork()
    if pid > 0:
        print(f"✅ 守护进程已启动 (PID: {pid})")
        print(f"   日志: {LOG_FILE}")
        sys.exit(0)

    # ── 子进程：守护循环（继承父进程的 flock fd，进程退出即自动释放）──
    PIDFILE.write_text(str(os.getpid()))

    task_args = ["--task", "update_bars"]
    if resume:
        task_args.append("--resume")

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    _truncate_log_if_oversized()

    # stop() 的 SIGTERM 必须转发给子进程进程组，否则正在跑的管道被孤儿化
    current: dict[str, subprocess.Popen | None] = {"proc": None}

    def _on_sigterm(signum, _frame):
        proc = current["proc"]
        if proc is not None and proc.poll() is None:
            with contextlib.suppress(OSError):
                os.killpg(proc.pid, signal.SIGTERM)
        sys.exit(128 + signum)

    signal.signal(signal.SIGTERM, _on_sigterm)

    failures = 0
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as log:
            _log("🚀 守护进程启动", log)

            while True:
                if not _should_run_pipeline():
                    _log("⏭️ 非交易日，休眠 1 小时", log)
                    time.sleep(3600)
                    continue

                _log("▶️  启动数据更新任务", log)
                # start_new_session：子进程独立进程组，stop 时可整组终止
                proc = subprocess.Popen(
                    [sys.executable, "daily_pipeline.py", *task_args],
                    cwd=str(PIPELINE_DIR),
                    stdout=log,
                    stderr=log,
                    start_new_session=True,
                )
                current["proc"] = proc
                rc = proc.wait()
                current["proc"] = None

                if rc == 0:
                    failures = 0
                    _log("✅ 任务正常完成，休眠 1 小时后重试", log)
                    time.sleep(3600)
                elif rc in (130, 143):
                    _log("🛑 任务被中断，守护进程退出", log)
                    break
                else:
                    failures += 1
                    backoff = _backoff_seconds(failures)
                    _log(
                        f"⚠️  任务异常退出 (code={rc})，连续失败 {failures} 次，"
                        f"{backoff} 秒后重启",
                        log,
                    )
                    if failures == BACKOFF_NOTIFY_THRESHOLD:
                        _notify_continuous_failures(failures, rc, backoff)
                    time.sleep(backoff)
    finally:
        PIDFILE.unlink(missing_ok=True)
    sys.exit(0)


def stop() -> None:
    if not PIDFILE.exists() or not is_running():
        print("ℹ️  守护进程未运行")
        PIDFILE.unlink(missing_ok=True)
        return

    pid = int(PIDFILE.read_text().strip())
    # 身份二次校验：防止 PID 复用后误杀无关进程（与 TUI 判定口径一致）
    res = subprocess.run(
        ["ps", "-p", str(pid), "-o", "command="],
        capture_output=True, text=True, timeout=2.0,
    )
    if "daemon.py" not in res.stdout:
        print("ℹ️  守护进程未运行（PID 已被复用）")
        PIDFILE.unlink(missing_ok=True)
        return

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
