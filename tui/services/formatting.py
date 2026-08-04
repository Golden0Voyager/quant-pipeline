"""TUI 文本格式化与显示辅助工具。"""

from __future__ import annotations

import unicodedata
from datetime import datetime, timedelta
from pathlib import Path

from tui.config import _SHANGHAI_TZ


def _vis_width(text: str) -> int:
    """计算字符串在终端中的可见宽度（全宽=2, 半宽=1）。

    使用 unicodedata.east_asian_width() 覆盖全角标点、CJK 扩展区等字符，
    而非仅硬编码 U+4E00-U+9FFF。
    """
    return sum(
        2 if unicodedata.east_asian_width(ch) in ("F", "W") else 1
        for ch in text
    )


def _ljust_vis(text: str, width: int) -> str:
    """按可见宽度左对齐填充空格。"""
    return text + " " * max(0, width - _vis_width(text))


def format_chinese_magnitude(n: int) -> str:
    """将数字转为中文量级：万、亿。"""
    if n >= 100_000_000:
        return f"{n / 100_000_000:.2f}亿"
    if n >= 10_000:
        return f"{n / 10_000:.1f}万"
    return str(n)


def format_count(n: int) -> str:
    """Format a count into human-readable form."""
    if n >= 1_000_000:
        return f"{n / 1_000_000:.2f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(n)


def get_db_size(db_path: str) -> str:
    """获取数据库文件大小的易读表示。"""
    p = Path(db_path)
    if p.exists():
        bytes_size = p.stat().st_size
        mb = bytes_size / (1024 * 1024)
        if mb >= 1024:
            return f"{mb / 1024:.2f} GB"
        return f"{mb:.2f} MB"
    return "0.00 MB"


def _seconds_until_safe() -> int:
    """计算到下一个安全运行时间（上海时间 16:00）的秒数。"""
    import sys

    tui_mod = sys.modules.get("tui")
    dt_cls = getattr(tui_mod, "datetime", datetime) if tui_mod else datetime
    now = dt_cls.now(_SHANGHAI_TZ)
    target = now.replace(hour=16, minute=0, second=0, microsecond=0)
    seconds = (target - now).total_seconds()
    if seconds <= 0:
        # 已过 16:00，次日 16:00
        target += timedelta(days=1)
        seconds = (target - now).total_seconds()
    return int(seconds)

