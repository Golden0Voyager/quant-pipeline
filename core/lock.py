"""
进程锁模块
──────────
提供 ProcessLock context manager，防止多实例同时运行。
"""

from __future__ import annotations

import atexit
import fcntl
import io
import logging
import os
import signal
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

_PIDFILE = Path("/tmp/daily_pipeline.pid")


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
            else:
                print(f"⚠️  检测到残留锁文件（PID {old_pid} 已不存在），自动清理后启动")
                cls.release()
                cls.acquire()
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
        """释放文件锁并清理 PID 文件。"""
        if cls._lock_file_fd is not None:
            try:
                fcntl.flock(cls._lock_file_fd, fcntl.LOCK_UN)
                cls._lock_file_fd.close()
            except OSError:
                pass
            cls._lock_file_fd = None
        _PIDFILE.unlink(missing_ok=True)
