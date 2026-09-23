"""
进程锁模块
──────────
提供 ProcessLock context manager，防止多实例同时运行。
"""

from __future__ import annotations

import atexit
import contextlib
import fcntl
import functools
import io
import logging
import os
import re
import signal
import sys
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_PIDFILE = Path("/tmp/daily_pipeline.pid")


def global_lock_held() -> bool:
    """非阻塞试探全局管道锁是否被其它进程持有（不获取、不残留）。

    供单任务路径在 acquire TaskLock 前调用：全局锁与单任务锁原本互不感知，
    ``--task all`` 运行期间单任务可并行写同一批表。
    """
    fd = open(_PIDFILE, "a+")  # noqa: SIM115
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        fd.close()
        return True
    fcntl.flock(fd, fcntl.LOCK_UN)
    fd.close()
    return False


def _task_lock_path(name: str) -> Path:
    safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", name).strip("_") or "task"
    return Path(f"/tmp/daily_pipeline_{safe_name}.lock")


class ProcessLock:
    """
    进程锁：使用 PID 文件 + fcntl 文件锁防止多实例同时运行。
    加锁失败时打印已有进程信息并退出。
    """

    _lock_file_fd: io.TextIOWrapper | None = None

    @classmethod
    def acquire(cls) -> None:
        """获取文件锁。失败时打印已有进程信息并退出。"""
        if cls._lock_file_fd is not None:
            return  # 已加锁
        cls._lock_file_fd = open(_PIDFILE, "a+", buffering=1)  # noqa: SIM115
        try:
            fcntl.flock(cls._lock_file_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            # flock 失败 ⇒ 锁确实在**某个活进程**手里（内核在进程退出时释放 flock），
            # 所以这里绝不能 unlink 锁文件：删掉它只会让本进程在**新 inode** 上
            # 加锁成功，于是两个 pipeline 同时自认持有全局锁、并发写同一个库，
            # 同时令 global_lock_held() 对其它进程永远返回 False（单任务互斥失效）。
            # PID 文件内容只有在“持有者刚 flock 成功、尚未写入”的窗口里
            # 才可能不可信，因此一律 fail-closed 拒绝启动，不做自愈。
            cls._lock_file_fd.seek(0)
            old_pid = cls._lock_file_fd.read().strip()
            cls._lock_file_fd.close()
            cls._lock_file_fd = None
            holder = f"PID {old_pid}" if old_pid.isdigit() else "PID 尚未写入的进程"
            print(f"❌ 管道已在运行 ({holder})，请勿重复启动")
            if old_pid.isdigit():
                print(f"   如需强制重启，请先执行: kill {old_pid}")
            else:
                print("   锁文件为空说明持有者正在启动中；请稍候重试")
            sys.exit(1)
        cls._lock_file_fd.truncate(0)
        cls._lock_file_fd.seek(0)
        cls._lock_file_fd.write(str(os.getpid()))
        cls._lock_file_fd.flush()
        atexit.register(cls.release)

        def _signal_handler(signum: int, _frame: object) -> None:
            cls.release()
            sys.exit(128 + signum)

        signal.signal(signal.SIGTERM, _signal_handler)
        signal.signal(signal.SIGINT, _signal_handler)

    @classmethod
    def release(cls) -> None:
        """释放文件锁。

        锁文件**不删除**，只清空 PID 内容：unlink 会让并发启动的进程在
        新 inode 上加锁成功（旧 inode 的锁仍由本进程持有到 close 为止），
        形成双持有；文件本身是稳定的锁锚点，留着才能保证互斥成立。
        清空发生在解锁**之前**，避免解锁后另一个进程刚写入 PID 就被本
        进程截断。读者（TUI / global_lock_held）按空内容、失活 PID 处理。
        """
        if cls._lock_file_fd is not None:
            try:
                cls._lock_file_fd.seek(0)
                cls._lock_file_fd.truncate(0)
                cls._lock_file_fd.flush()
                fcntl.flock(cls._lock_file_fd, fcntl.LOCK_UN)
                cls._lock_file_fd.close()
            except OSError:
                pass
            cls._lock_file_fd = None


class TaskLock:
    """Named non-blocking task lock for direct function or UI-triggered runs."""

    _fds: dict[str, io.TextIOWrapper] = {}

    @classmethod
    def acquire(cls, name: str) -> bool:
        if name in cls._fds:
            return True

        path = _task_lock_path(name)
        fd = open(path, "a+", buffering=1)  # noqa: SIM115
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            fd.close()
            return False

        fd.truncate(0)
        fd.seek(0)
        fd.write(str(os.getpid()))
        fd.flush()
        cls._fds[name] = fd
        return True

    @classmethod
    def release(cls, name: str) -> None:
        fd = cls._fds.pop(name, None)
        if fd is None:
            return
        try:
            # 与 ProcessLock 同理：不 unlink，解锁前清空 PID 即可。
            # unlink 会让并发启动的同名任务在新 inode 上加锁成功 —— 那正是
            # skip_if_task_locked 要防的重复运行。
            fd.seek(0)
            fd.truncate(0)
            fd.flush()
            fcntl.flock(fd, fcntl.LOCK_UN)
            fd.close()
        except OSError:
            pass


@contextlib.contextmanager
def task_lock(name: str) -> Iterator[bool]:
    acquired = TaskLock.acquire(name)
    try:
        yield acquired
    finally:
        if acquired:
            TaskLock.release(name)


def skip_if_task_locked(name: str) -> Callable[[Callable[..., dict[str, Any]]], Callable[..., dict[str, Any]]]:
    """Decorate a task so duplicate direct invocations are skipped, not overlapped."""
    def decorator(fn: Callable[..., dict[str, Any]]) -> Callable[..., dict[str, Any]]:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> dict[str, Any]:
            with task_lock(name) as acquired:
                if not acquired:
                    logger.warning(f"⚠️ 任务 {name} 已在运行，跳过重复启动")
                    return {
                        "status": "locked",
                        "skipped": True,
                        "reason": "task already running",
                    }
                return fn(*args, **kwargs)

        return wrapper

    return decorator
