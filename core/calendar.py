"""
交易日历模块
────────────
提供 A 股交易日判断，基于 AkShare 交易日历数据。
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta

import pandas as pd

from core.config import SHARED_DATA_DIR

logger = logging.getLogger(__name__)

CALENDAR_CACHE = SHARED_DATA_DIR / "trading_calendar.json"
CALENDAR_CACHE_DAYS = 90  # 缓存有效期（天）


def _fetch_trading_calendar() -> list[str]:
    """从 AkShare 获取交易日历，返回日期字符串列表 YYYY-MM-DD。"""
    try:
        import akshare as ak  # noqa: F811

        df = ak.tool_trade_date_hist_sina()
        if df is not None and not df.empty and "trade_date" in df.columns:
            df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
            return sorted(df["trade_date"].dropna().dt.strftime("%Y-%m-%d").tolist())
    except Exception as e:
        logger.warning(f"⚠️ AkShare 交易日历获取失败: {e}")
    return []


def _load_cached_calendar() -> list[str] | None:
    """读取缓存的交易日历，过期返回 None。"""
    if not CALENDAR_CACHE.exists():
        return None
    try:
        data = json.loads(CALENDAR_CACHE.read_text(encoding="utf-8"))
        cached_date = datetime.fromisoformat(data["cached_at"])
        if (datetime.now() - cached_date).days < CALENDAR_CACHE_DAYS:
            return data["trade_dates"]
    except (json.JSONDecodeError, KeyError, ValueError):
        pass
    return None


def _save_calendar_cache(trade_dates: list[str]) -> None:
    """缓存交易日历到本地文件。"""
    try:
        data = {"cached_at": datetime.now().isoformat(), "trade_dates": trade_dates}
        CALENDAR_CACHE.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    except Exception as e:
        logger.warning(f"⚠️ 交易日历缓存写入失败: {e}")


def _is_weekend(d: date) -> bool:
    """判断是否为周末（周六/周日）。"""
    return d.weekday() >= 5


def is_trading_day(d: date | None = None) -> bool:
    """
    判断指定日期是否为 A 股交易日。

    优先使用缓存的 AkShare 交易日历，
    AkShare 不可用时 fallback 到周末检查。
    """
    d = d or date.today()

    # 周末快速判断
    if _is_weekend(d):
        return False

    # 尝试获取缓存的交易日历
    trade_dates = _load_cached_calendar()
    if trade_dates is not None:
        return d.strftime("%Y-%m-%d") in trade_dates

    # 缓存不存在或过期，重新获取
    trade_dates = _fetch_trading_calendar()
    if trade_dates:
        _save_calendar_cache(trade_dates)
        return d.strftime("%Y-%m-%d") in trade_dates

    # AkShare fallback: 仅用周末判断（周一~五都认为是交易日）
    logger.warning("⚠️ AkShare 不可用，仅用周末判断交易日")
    return True


def get_expected_latest_trading_day(now: datetime | None = None) -> str:
    """获取期望的最新交易日日期 (YYYY-MM-DD)。

    周末 → 上周五；周一至周五 15:30 之前 → 前一天；15:30 之后 → 今天。
    用于判断数据新鲜度（如 TUI 的数据完整性面板）和任务调度。

    now 缺省时使用本机时钟；调用方可传入上海时区的 aware datetime，
    此时 15:30 截止点按上海时间解释，结果与主机本地时区无关。
    """
    if now is None:
        now = datetime.now()
    target = now
    if target.weekday() < 5 and (target.hour < 15 or (target.hour == 15 and target.minute < 30)):
        target -= timedelta(days=1)
    while target.weekday() >= 5:
        target -= timedelta(days=1)
    return target.strftime("%Y-%m-%d")


def get_recent_trading_days(end_date: str, count: int) -> list[str]:
    """返回不晚于 end_date 的最近若干交易日，按新到旧排列。"""
    if count <= 0:
        return []

    trade_dates = _load_cached_calendar()
    if trade_dates is None:
        trade_dates = _fetch_trading_calendar()
        if trade_dates:
            _save_calendar_cache(trade_dates)

    if trade_dates:
        return sorted((d for d in trade_dates if d <= end_date), reverse=True)[:count]

    current = datetime.strptime(end_date, "%Y-%m-%d").date()
    result: list[str] = []
    while len(result) < count:
        if not _is_weekend(current):
            result.append(current.strftime("%Y-%m-%d"))
        current -= timedelta(days=1)
    return result
