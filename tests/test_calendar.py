"""core.calendar 覆盖率测试：交易日判断与期望最新交易日计算。"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import pandas as pd

import core.calendar as cal


def test_is_weekend():
    assert cal._is_weekend(date(2026, 7, 18)) is True   # Sat
    assert cal._is_weekend(date(2026, 7, 19)) is True   # Sun
    assert cal._is_weekend(date(2026, 7, 20)) is False  # Mon


def test_is_trading_day_weekend_is_false():
    assert cal.is_trading_day(date(2026, 7, 18)) is False


def test_is_trading_day_cached_hit():
    with patch.object(cal, "_load_cached_calendar", return_value=["2026-07-20", "2026-07-21"]):
        assert cal.is_trading_day(date(2026, 7, 20)) is True
        assert cal.is_trading_day(date(2026, 7, 22)) is False


def test_is_trading_day_refetches_when_cache_missing():
    with patch.object(cal, "_load_cached_calendar", return_value=None), patch.object(
        cal, "_fetch_trading_calendar", return_value=["2026-07-20"]
    ) as mock_fetch, patch.object(cal, "_save_calendar_cache") as mock_save:
        assert cal.is_trading_day(date(2026, 7, 20)) is True
        mock_fetch.assert_called_once()
        mock_save.assert_called_once_with(["2026-07-20"])


def test_is_trading_day_fallback_when_fetch_empty():
    # 缓存缺失且 akshare 拉取为空 → fallback 仅周末判断（周一~五 True）
    with patch.object(cal, "_load_cached_calendar", return_value=None), patch.object(
        cal, "_fetch_trading_calendar", return_value=[]
    ):
        assert cal.is_trading_day(date(2026, 7, 20)) is True


def test_get_expected_weekend_rolls_back_to_friday():
    with patch.object(cal, "datetime") as mock_dt:
        mock_dt.now.return_value = datetime(2026, 7, 18, 10, 0)  # Sat
        assert cal.get_expected_latest_trading_day() == "2026-07-17"


def test_get_expected_weekday_before_1530_uses_prev_day():
    with patch.object(cal, "datetime") as mock_dt:
        mock_dt.now.return_value = datetime(2026, 7, 21, 10, 0)  # Tue 10:00 < 15:30
        assert cal.get_expected_latest_trading_day() == "2026-07-20"


def test_get_expected_weekday_after_1530_uses_today():
    with patch.object(cal, "datetime") as mock_dt:
        mock_dt.now.return_value = datetime(2026, 7, 20, 16, 0)  # Mon 16:00 > 15:30
        assert cal.get_expected_latest_trading_day() == "2026-07-20"


def test_get_expected_explicit_shanghai_now_before_1530_uses_prev_day():
    now = datetime(2026, 7, 21, 10, 0, tzinfo=ZoneInfo("Asia/Shanghai"))  # Tue 10:00 < 15:30
    assert cal.get_expected_latest_trading_day(now=now) == "2026-07-20"


def test_get_expected_explicit_shanghai_now_after_1530_uses_today():
    now = datetime(2026, 7, 20, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai"))  # Mon 16:00 > 15:30
    assert cal.get_expected_latest_trading_day(now=now) == "2026-07-20"


def test_get_expected_explicit_shanghai_now_weekend_rolls_back_to_friday():
    now = datetime(2026, 7, 18, 10, 0, tzinfo=ZoneInfo("Asia/Shanghai"))  # Sat
    assert cal.get_expected_latest_trading_day(now=now) == "2026-07-17"


def test_get_expected_explicit_now_ignores_host_clock():
    # 传入显式 now 时不得读取主机时钟，结果与主机本地时区无关
    with patch.object(cal, "datetime") as mock_dt:
        mock_dt.now.return_value = datetime(2026, 1, 1, 0, 0)  # 与传入 now 冲突的主机时钟
        now = datetime(2026, 7, 21, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai"))  # Tue 16:00
        assert cal.get_expected_latest_trading_day(now=now) == "2026-07-21"
        mock_dt.now.assert_not_called()


def test_get_expected_default_now_none_still_uses_local_clock():
    # now=None 路径保持旧行为：走本机 datetime.now()
    with patch.object(cal, "datetime") as mock_dt:
        mock_dt.now.return_value = datetime(2026, 7, 21, 16, 0)  # Tue 16:00 > 15:30
        assert cal.get_expected_latest_trading_day() == "2026-07-21"
        mock_dt.now.assert_called_once_with()


def test_get_recent_trading_days_uses_calendar_and_skips_weekend():
    cached = ["2026-07-16", "2026-07-17", "2026-07-20"]
    with patch.object(cal, "_load_cached_calendar", return_value=cached):
        assert cal.get_recent_trading_days("2026-07-20", 3) == [
            "2026-07-20",
            "2026-07-17",
            "2026-07-16",
        ]


def test_get_recent_trading_days_falls_back_to_weekdays():
    with patch.object(cal, "_load_cached_calendar", return_value=None), patch.object(
        cal, "_fetch_trading_calendar", return_value=[]
    ):
        assert cal.get_recent_trading_days("2026-07-20", 2) == [
            "2026-07-20",
            "2026-07-17",
        ]


# ───────────────────────── 交易日历获取 / 缓存 ─────────────────────────


def test_fetch_trading_calendar_returns_sorted():
    fake_ak = MagicMock()
    # trade_date 列需要是可转 datetime 的字符串，且需含 "trade_date" 列名
    fake_ak.tool_trade_date_hist_sina.return_value = pd.DataFrame(
        {"trade_date": pd.to_datetime(["2026-07-21", "2026-07-20", "2026-07-17"])}
    )
    with patch.dict("sys.modules", {"akshare": fake_ak}):
        result = cal._fetch_trading_calendar()
    assert result == ["2026-07-17", "2026-07-20", "2026-07-21"]


def test_fetch_trading_calendar_empty():
    fake_ak = MagicMock()
    fake_ak.tool_trade_date_hist_sina.return_value = pd.DataFrame()
    with patch.dict("sys.modules", {"akshare": fake_ak}):
        assert cal._fetch_trading_calendar() == []


def test_fetch_trading_calendar_raises():
    fake_ak = MagicMock()
    fake_ak.tool_trade_date_hist_sina.side_effect = RuntimeError("boom")
    with patch.dict("sys.modules", {"akshare": fake_ak}):
        assert cal._fetch_trading_calendar() == []


def test_save_and_load_calendar_cache(tmp_path: Path):
    cache = tmp_path / "trading_calendar.json"
    with patch.object(cal, "CALENDAR_CACHE", cache):
        cal._save_calendar_cache(["2026-07-20", "2026-07-21"])
        assert cache.exists()
        # 刚写入未过期，应原样返回
        assert cal._load_cached_calendar() == ["2026-07-20", "2026-07-21"]


def test_load_cached_calendar_expired(tmp_path: Path):
    cache = tmp_path / "trading_calendar.json"
    old = (datetime.now() - timedelta(days=cal.CALENDAR_CACHE_DAYS + 1)).isoformat()
    cache.write_text(
        f'{{"cached_at": "{old}", "trade_dates": ["2026-07-20"]}}', encoding="utf-8"
    )
    with patch.object(cal, "CALENDAR_CACHE", cache):
        # 超期 → 返回 None，触发重新获取
        assert cal._load_cached_calendar() is None


def test_load_cached_calendar_missing(tmp_path: Path):
    cache = tmp_path / "trading_calendar.json"  # 不存在
    with patch.object(cal, "CALENDAR_CACHE", cache):
        assert cal._load_cached_calendar() is None
