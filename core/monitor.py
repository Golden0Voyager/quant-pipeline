"""
AkShare 稳定性监控模块
────────────────────
根据最近请求成功率动态调整限流策略。
"""

from __future__ import annotations

import json
import logging
from collections import deque
from datetime import datetime
from typing import Any

from core.config import SHARED_DATA_DIR

logger = logging.getLogger(__name__)


class AkShareMonitor:
    """AkShare 稳定性监控器：根据最近请求成功率动态调整限流策略。"""

    FILE = SHARED_DATA_DIR / "akshare_monitor.json"
    WINDOW_SIZE = 30  # 滑动窗口大小

    FLUSH_INTERVAL = 50  # 每 N 条记录写一次磁盘

    ABORT_WINDOW = 20  # 本轮中止判定的滑动窗口大小

    def __init__(self):
        self.records = self._load()
        self.current_run_attempts = 0
        self.current_run_successes = 0
        self.current_run_consecutive_failures = 0
        # 本轮最近 N 次结果：中止判定用滑动窗口而非整轮累计，
        # 否则前期大量成功会让后期的全面故障永远压不破阈值
        self.current_run_recent: deque[bool] = deque(maxlen=self.ABORT_WINDOW)
        self._dirty_since_last_save = 0  # 自上次写入以来新增的记录数

    def _load(self) -> list[dict[str, Any]]:
        if not self.FILE.exists():
            return []
        try:
            with open(self.FILE, encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return []

    def _save(self) -> None:
        try:
            tmp = self.FILE.with_suffix(".tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self.records[-self.WINDOW_SIZE * 2:], f, ensure_ascii=False)
            tmp.replace(self.FILE)
        except Exception as e:
            logger.warning(f"⚠️ 无法写入 AkShare 监控文件: {e}")

    def record(self, success: bool, symbol: str) -> None:
        self.current_run_attempts += 1
        if success:
            self.current_run_successes += 1
            self.current_run_consecutive_failures = 0
        else:
            self.current_run_consecutive_failures += 1
        self.current_run_recent.append(success)

        self.records.append({
            "timestamp": datetime.now().isoformat(),
            "success": success,
            "symbol": symbol,
        })
        self._dirty_since_last_save += 1
        if self._dirty_since_last_save >= self.FLUSH_INTERVAL:
            self._save()
            self._dirty_since_last_save = 0

    def flush(self) -> None:
        """强制刷写缓存中的记录到磁盘。"""
        if self._dirty_since_last_save > 0:
            self._save()
            self._dirty_since_last_save = 0

    def get_success_rate(self, window: int | None = None) -> float:
        if not self.records:
            return 1.0
        window = window or self.WINDOW_SIZE
        recent = self.records[-window:]
        if not recent:
            return 1.0
        success_count = sum(1 for r in recent if r["success"])
        return success_count / len(recent)

    def get_recommended_sleep_multiplier(self) -> float:
        rate = self.get_success_rate()
        if rate >= 0.8:
            return 1.0
        elif rate >= 0.5:
            return 1.5
        elif rate >= 0.3:
            return 2.0
        else:
            return 3.0

    def should_abort(self) -> tuple[bool, str]:
        if self.current_run_attempts == 0:
            return False, ""

        if self.current_run_consecutive_failures >= 3:
            return (
                True,
                f"AkShare 在本次运行中连续 {self.current_run_consecutive_failures} 次请求失败，"
                "网络可能彻底不可用或受到强力限流阻断，已自动中止。",
            )

        # 成功率规则只看本次运行的请求：持久化的 records 跨运行/跨天，
        # 用历史失败记录判定当前中止会在 skip 为主的运行中误杀
        #（历史成功率仍用于 get_recommended_sleep_multiplier 的限流节奏）
        # 且只看本轮最近 ABORT_WINDOW 次，避免前期成功摊薄后期故障
        if len(self.current_run_recent) >= self.ABORT_WINDOW:
            rate = sum(self.current_run_recent) / len(self.current_run_recent)
            if rate < 0.2:
                return (
                    True,
                    f"AkShare 本次运行最近 {len(self.current_run_recent)} 次请求成功率仅 {rate * 100:.0f}%，"
                    "建议推迟到晚上 20:00+ 再跑",
                )
        return False, ""

    def log_status(self) -> None:
        rate = self.get_success_rate()
        multiplier = self.get_recommended_sleep_multiplier()
        if rate < 1.0:
            logger.info(
                f"📊 AkShare 最近 {min(len(self.records), self.WINDOW_SIZE)} 次成功率: "
                f"{rate * 100:.0f}%，sleep 倍率: {multiplier}x"
            )
