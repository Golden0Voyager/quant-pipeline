"""core.calendar 覆盖率测试：交易日判断与期望最新交易日计算。"""
from __future__ import annotations

import json
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


def test_get_expected_weekend_rolls_back_to_friday(pinned_trading_calendar):
    with patch("core.market_time.datetime") as mock_dt:
        mock_dt.now.return_value = datetime(2026, 7, 18, 10, 0, tzinfo=ZoneInfo("Asia/Shanghai"))  # Sat
        assert cal.get_expected_latest_trading_day() == "2026-07-17"


def test_get_expected_weekday_before_1600_uses_prev_day(pinned_trading_calendar):
    with patch("core.market_time.datetime") as mock_dt:
        mock_dt.now.return_value = datetime(2026, 7, 21, 10, 0, tzinfo=ZoneInfo("Asia/Shanghai"))  # Tue 10:00 < 16:00
        assert cal.get_expected_latest_trading_day() == "2026-07-20"


def test_get_expected_weekday_after_1600_uses_today(pinned_trading_calendar):
    with patch("core.market_time.datetime") as mock_dt:
        mock_dt.now.return_value = datetime(2026, 7, 20, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai"))  # Mon 16:00
        assert cal.get_expected_latest_trading_day() == "2026-07-20"


def test_get_expected_explicit_shanghai_now_before_1600_uses_prev_day(pinned_trading_calendar):
    now = datetime(2026, 7, 21, 10, 0, tzinfo=ZoneInfo("Asia/Shanghai"))  # Tue 10:00 < 16:00
    assert cal.get_expected_latest_trading_day(now=now) == "2026-07-20"


def test_get_expected_explicit_shanghai_now_after_1600_uses_today(pinned_trading_calendar):
    now = datetime(2026, 7, 20, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai"))  # Mon 16:00
    assert cal.get_expected_latest_trading_day(now=now) == "2026-07-20"


def test_get_expected_settlement_window_still_prev_day(pinned_trading_calendar):
    """15:00–16:00 结算窗口 expected 仍为前一日（与放行窗口 16:00 对齐）。"""
    now = datetime(2026, 7, 21, 15, 30, tzinfo=ZoneInfo("Asia/Shanghai"))
    assert cal.get_expected_latest_trading_day(now=now) == "2026-07-20"


def test_get_expected_explicit_shanghai_now_weekend_rolls_back_to_friday(pinned_trading_calendar):
    now = datetime(2026, 7, 18, 10, 0, tzinfo=ZoneInfo("Asia/Shanghai"))  # Sat
    assert cal.get_expected_latest_trading_day(now=now) == "2026-07-17"


def test_get_expected_explicit_now_ignores_host_clock(pinned_trading_calendar):
    # 传入显式 now 时不得读取主机时钟，结果与主机本地时区无关
    with patch.object(cal, "datetime") as mock_dt:
        mock_dt.now.return_value = datetime(2026, 1, 1, 0, 0)  # 与传入 now 冲突的主机时钟
        now = datetime(2026, 7, 21, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai"))  # Tue 16:00
        assert cal.get_expected_latest_trading_day(now=now) == "2026-07-21"
        mock_dt.now.assert_not_called()


def test_get_expected_default_now_none_uses_shanghai_clock(pinned_trading_calendar):
    # now=None 路径改走上海时钟（shanghai_now），与主机本地时区无关
    with patch("core.market_time.datetime") as mock_dt:
        mock_dt.now.return_value = datetime(2026, 7, 21, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
        assert cal.get_expected_latest_trading_day() == "2026-07-21"
        mock_dt.now.assert_called_once()


# ───────────────────── expected 必须落在真实交易日 ─────────────────────


def test_get_expected_skips_holiday_window(pinned_trading_calendar):
    """长假期间 expected 不得返回非交易日（旧实现的周末回退会返回假期本身）。"""
    # 端午 06-19~06-21 休市：06-22 周一开盘前应回落到 06-18
    assert (
        cal.get_expected_latest_trading_day(
            now=datetime(2026, 6, 22, 9, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
        )
        == "2026-06-18"
    )


def test_get_expected_never_returns_non_trading_day(monkeypatch):
    """对真实日历逐日扫描：任意时刻的 expected 都必须是交易日。"""
    holiday_gap = ["2026-06-17", "2026-06-18", "2026-06-22"]  # 06-19~06-21 休市
    monkeypatch.setattr(cal, "_load_calendar_covering", lambda _up_to: holiday_gap)
    # 06-19（端午，周五）收盘后：日历里没有当天 → 必须回落到 06-18
    assert (
        cal.get_expected_latest_trading_day(
            now=datetime(2026, 6, 19, 20, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
        )
        == "2026-06-18"
    )


def test_get_expected_falls_back_to_weekday_when_calendar_unavailable(monkeypatch, caplog):
    """日历不可用 → 退化为周末判断，并告警一次（不再是静默错误）。"""
    monkeypatch.setattr(cal, "_load_calendar_covering", lambda _up_to: None)
    monkeypatch.setattr(cal, "_CALENDAR_FALLBACK_WARNED", False)
    with caplog.at_level("WARNING", logger="core.calendar"):
        # 06-19 是端午（非交易日），退化路径只能按周五处理 —— 这正是需要告警的原因
        assert (
            cal.get_expected_latest_trading_day(
                now=datetime(2026, 6, 22, 9, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
            )
            == "2026-06-19"
        )
    assert any("退化为周末判断" in r.message for r in caplog.records)
    assert cal._CALENDAR_FALLBACK_WARNED is True


def test_get_expected_warns_only_once(monkeypatch, caplog):
    """expected 一次运行内被调用数十次，退化告警不得刷屏。"""
    monkeypatch.setattr(cal, "_load_calendar_covering", lambda _up_to: None)
    monkeypatch.setattr(cal, "_CALENDAR_FALLBACK_WARNED", False)
    now = datetime(2026, 6, 22, 9, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    with caplog.at_level("WARNING", logger="core.calendar"):
        for _ in range(5):
            cal.get_expected_latest_trading_day(now=now)
    assert sum("退化为周末判断" in r.message for r in caplog.records) == 1


def test_load_calendar_covering_rejects_stale_cache(monkeypatch):
    """缓存未覆盖到目标日时必须返回 None，避免拿着旧日历算 expected。"""
    monkeypatch.setattr(
        cal, "_load_cached_calendar", lambda: ["2026-07-16", "2026-07-17"]
    )
    assert cal._load_calendar_covering("2026-07-17") == ["2026-07-16", "2026-07-17"]
    assert cal._load_calendar_covering("2026-07-20") is None
    monkeypatch.setattr(cal, "_load_cached_calendar", lambda: None)
    assert cal._load_calendar_covering("2026-07-17") is None


def test_load_cached_calendar_rejects_empty_trade_dates(monkeypatch, tmp_path):
    """空 trade_dates 缓存必须判为无效：否则 is_trading_day 会把每个工作日都判为非交易日。"""
    cache_file = tmp_path / "trading_calendar.json"
    cache_file.write_text(
        json.dumps({"cached_at": datetime.now().isoformat(), "trade_dates": []}),
        encoding="utf-8",
    )
    monkeypatch.setattr(cal, "CALENDAR_CACHE", cache_file)
    assert cal._load_cached_calendar() is None


def test_is_trading_day_empty_cache_falls_back_to_fetch(monkeypatch, tmp_path):
    """缓存文件存在但 trade_dates 为空 → 视为缓存缺失，走重新获取路径。"""
    cache_file = tmp_path / "trading_calendar.json"
    cache_file.write_text(
        json.dumps({"cached_at": datetime.now().isoformat(), "trade_dates": []}),
        encoding="utf-8",
    )
    monkeypatch.setattr(cal, "CALENDAR_CACHE", cache_file)
    monkeypatch.setattr(cal, "_fetch_trading_calendar", lambda: ["2026-07-20"])
    monkeypatch.setattr(cal, "_save_calendar_cache", lambda _dates: None)
    assert cal.is_trading_day(date(2026, 7, 20)) is True


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
