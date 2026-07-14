"""
断点续传进度追踪模块
──────────────────
提供 ProgressTracker 类，支持原子写入和文件锁保护的进度读写。
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import logging
import os
from datetime import datetime
from typing import Any

from core.config import SHARED_DATA_DIR

logger = logging.getLogger(__name__)


class ProgressTracker:
    """
    断点续传进度追踪器。

    使用原子写入（tempfile + rename）防止写一半断电导致进度文件损坏。
    记录内容：最后处理的 symbol、已处理数量、失败队列、启动时间。
    """

    FILE = SHARED_DATA_DIR / "progress.json"
    LOCK_FILE = SHARED_DATA_DIR / "progress.lock"

    @classmethod
    @contextlib.contextmanager
    def _lock(cls, exclusive: bool = True):
        """使用文件锁保护进度文件的读写操作。"""
        lock_path = cls.LOCK_FILE
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR, 0o666)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    @classmethod
    def save(
        cls,
        task: str,
        last_symbol: str,
        processed: int,
        total: int,
        failed_queue: list[str],
    ) -> None:
        """原子写入进度文件。"""
        data = {
            "task": task,
            "date": datetime.now().strftime("%Y-%m-%d"),
            "start_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "last_symbol": last_symbol,
            "processed": processed,
            "total": total,
            "failed_queue": failed_queue,
        }
        with cls._lock(exclusive=True):
            tmp = cls.FILE.with_suffix(".tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())
            tmp.replace(cls.FILE)

    @classmethod
    def load(cls) -> dict[str, Any] | None:
        """读取进度文件。"""
        with cls._lock(exclusive=False):
            if not cls.FILE.exists():
                return None
            try:
                with open(cls.FILE, encoding="utf-8") as f:
                    return json.load(f)
            except (json.JSONDecodeError, OSError):
                logger.warning("⚠️  进度文件损坏，将从头开始")
                return None

    @classmethod
    def clear(cls) -> None:
        """清除进度文件（任务成功完成后调用）。"""
        with cls._lock(exclusive=True):
            if cls.FILE.exists():
                cls.FILE.unlink()
                logger.info("🗑️  进度文件已清除")

    @classmethod
    def find_resume_index(cls, stock_codes: list[str], last_symbol: str) -> int:
        """找到断点位置。返回应该从哪个索引开始处理。"""
        try:
            idx = stock_codes.index(last_symbol)
            return idx + 1  # 从下一个开始
        except ValueError:
            logger.warning(
                f"⚠️  断点 symbol '{last_symbol}' 不在今日股票列表中，"
                "可能因新股上市/退市导致列表变化，将从头开始"
            )
            return 0
