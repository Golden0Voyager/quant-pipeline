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
            cls._lock_file_fd.seek(0)
            old_pid = cls._lock_file_fd.read().strip()
            alive = False
            if old_pid.isdigit():
                try:
                    os.kill(int(old_pid), 0)
                    alive = True
                except OSError:
                    pass
            if alive:
                print(f"❌ 管道已在运行 (PID: {old_pid})，请勿重复启动")
                print(f"   如需强制重启，请先执行: kill {old_pid}")
                sys.exit(1)
            print(f"⚠️  检测到残留锁文件（PID {old_pid} 已不存在），自动清理后启动")
            cls.release()
            # 递归重新加锁成功后直接返回，不得落入 sys.exit
            cls.acquire()
            return
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
        """释放文件锁并清理 PID 文件。"""
        if cls._lock_file_fd is not None:
            try:
                fcntl.flock(cls._lock_file_fd, fcntl.LOCK_UN)
                cls._lock_file_fd.close()
            except OSError:
                pass
            cls._lock_file_fd = None
        _PIDFILE.unlink(missing_ok=True)


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
            fcntl.flock(fd, fcntl.LOCK_UN)
            fd.close()
        except OSError:
            pass
        _task_lock_path(name).unlink(missing_ok=True)


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
