"""core.calendar 覆盖率测试：交易日判断与期望最新交易日计算。"""
from __future__ import annotations

from datetime import date, datetime
from unittest.mock import patch

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
