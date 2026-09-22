"""futures.py 覆盖率测试：聚焦 _fetch_futures_history 解析与 update_futures 调度。

网络依赖（akshare）全部用 MagicMock 替换，不触发真实请求。
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

import tasks.futures as futures_mod
from tasks.futures import FUTURES_VARIETIES, update_futures


def _sina_df(prices: list[float], dates: list[str], **extra) -> pd.DataFrame:
    """构造模拟新浪期货返回的 DataFrame（中文列名），列长与 prices 一致。"""
    n = len(prices)
    data = {
        "日期": dates,
        "开盘价": [12.0] * n,
        "最高价": [14.0] * n,
        "最低价": [11.0] * n,
        "收盘价": list(prices),
        "成交量": [110] * n,
        "持仓量": [55] * n,
    }
    data.update(extra)
    return pd.DataFrame(data)


VARIETY = {"sina_code": "M0", "name": "豆粕"}


# ───────────────────── _fetch_futures_history ─────────────────────

def test_fetch_history_normal():
    df = _sina_df([100.0, 105.0], ["2026-07-17", "2026-07-18"])
    fake_ak = MagicMock()
    fake_ak.futures_main_sina.return_value = df
    with patch.object(futures_mod, "ak", fake_ak):
        rows = futures_mod._fetch_futures_history(VARIETY, "20260701", "20260718")
    assert len(rows) == 2
    last = rows[-1]
    assert last["symbol"] == "M"
    assert last["name"] == "豆粕"
    assert last["close"] == 105.0
    assert last["open"] == 12.0
    assert last["high"] == 14.0
    assert last["low"] == 11.0
    assert last["volume"] == 110
    assert last["hold"] == 55
    assert last["change_pct"] == pytest.approx((105.0 - 100.0) / 100.0 * 100)
    assert last["data_source"] == "akshare_sina"
    # 窗口首行无前收，涨跌幅为 None
    assert rows[0]["change_pct"] is None


def test_fetch_history_ak_none():
    with patch.object(futures_mod, "ak", None):
        assert futures_mod._fetch_futures_history(VARIETY, "20260701", "20260718") == []


def test_fetch_history_empty_df():
    fake_ak = MagicMock()
    fake_ak.futures_main_sina.return_value = pd.DataFrame()
    with patch.object(futures_mod, "ak", fake_ak):
        assert futures_mod._fetch_futures_history(VARIETY, "20260701", "20260718") == []


def test_fetch_history_none_response():
    fake_ak = MagicMock()
    fake_ak.futures_main_sina.return_value = None
    with patch.object(futures_mod, "ak", fake_ak):
        assert futures_mod._fetch_futures_history(VARIETY, "20260701", "20260718") == []


def test_fetch_history_nan_and_none_values():
    df = _sina_df(
        [100.0, float("nan")],
        ["2026-07-17", "2026-07-18"],
        开盘价=[12.0, None],
        最高价=[14.0, None],
        最低价=[11.0, None],
        成交量=[110, None],
        持仓量=[55, None],
    )
    fake_ak = MagicMock()
    fake_ak.futures_main_sina.return_value = df
    with patch.object(futures_mod, "ak", fake_ak):
        rows = futures_mod._fetch_futures_history(VARIETY, "20260701", "20260718")
    assert rows[-1]["open"] is None
    assert rows[-1]["high"] is None
    assert rows[-1]["low"] is None
    assert rows[-1]["close"] is None
    assert rows[-1]["volume"] is None
    assert rows[-1]["hold"] is None


def test_fetch_history_zero_preclose_no_division():
    df = _sina_df([0.0, 105.0], ["2026-07-17", "2026-07-18"])
    fake_ak = MagicMock()
    fake_ak.futures_main_sina.return_value = df
    with patch.object(futures_mod, "ak", fake_ak):
        rows = futures_mod._fetch_futures_history(VARIETY, "20260701", "20260718")
    assert rows[1]["change_pct"] is None


def test_fetch_history_raises():
    fake_ak = MagicMock()
    fake_ak.futures_main_sina.side_effect = RuntimeError("network boom")
    with patch.object(futures_mod, "ak", fake_ak):
        assert futures_mod._fetch_futures_history(VARIETY, "20260701", "20260718") == []


# ───────────────────────── update_futures ─────────────────────────

def test_update_futures_ak_none():
    db = MagicMock()
    with patch.object(futures_mod, "ak", None):
        res = update_futures(db)
    assert res == {"saved": 0, "error": "akshare not installed"}
    db.save_futures_daily_batch.assert_not_called()


def test_update_futures_all_empty():
    db = MagicMock()
    fake_ak = MagicMock()
    fake_ak.futures_main_sina.return_value = pd.DataFrame()
    with patch.object(futures_mod, "ak", fake_ak):
        res = update_futures(db)
    assert res == {"saved": 0, "total": 0, "error_kind": "network"}
    db.save_futures_daily_batch.assert_not_called()


def test_update_futures_full_backfill_when_no_history():
    """库内无历史：全部品种走全量回填起点 20190101。"""
    db = MagicMock()
    db.get_futures_latest_date.return_value = None
    df = _sina_df([100.0, 105.0], ["2026-07-17", "2026-07-18"])
    fake_ak = MagicMock()
    fake_ak.futures_main_sina.return_value = df
    with patch.object(futures_mod, "ak", fake_ak):
        res = update_futures(db)
    assert res["total"] == 2 * len(FUTURES_VARIETIES)
    starts = {c.kwargs.get("start_date") or c.args[1] for c in fake_ak.futures_main_sina.call_args_list}
    assert starts == {"20190101"}


def test_update_futures_incremental_window():
    """库内有历史：增量窗口起点 = 最新日 - 10 天。"""
    db = MagicMock()
    db.get_futures_latest_date.return_value = "2026-07-18"
    fake_ak = MagicMock()
    fake_ak.futures_main_sina.return_value = _sina_df([105.0], ["2026-07-18"])
    with patch.object(futures_mod, "ak", fake_ak):
        update_futures(db)
    starts = {c.kwargs.get("start_date") or c.args[1] for c in fake_ak.futures_main_sina.call_args_list}
    assert starts == {"20260708"}


def test_update_futures_mixed_results():
    """部分品种成功、部分失败 → 只保存成功的记录。"""
    db = MagicMock()
    db.get_futures_latest_date.return_value = None
    db.save_futures_daily_batch.return_value = 6
    df_valid = _sina_df([100.0, 105.0], ["2026-07-17", "2026-07-18"])

    def _side_effect(symbol, start_date=None, end_date=None, **kw):
        if symbol in ("LC0", "SI0", "PS0"):
            return df_valid
        return pd.DataFrame()

    fake_ak = MagicMock()
    fake_ak.futures_main_sina.side_effect = _side_effect
    with patch.object(futures_mod, "ak", fake_ak):
        res = update_futures(db)
    assert res["total"] == 6
    assert res["saved"] == 6
    assert fake_ak.futures_main_sina.call_count == len(FUTURES_VARIETIES)
    db.save_futures_daily_batch.assert_called_once()


def test_update_futures_saves_all_varieties():
    db = MagicMock()
    db.get_futures_latest_date.return_value = None
    db.save_futures_daily_batch.return_value = 2 * len(FUTURES_VARIETIES)
    df = _sina_df([100.0, 105.0], ["2026-07-17", "2026-07-18"])
    fake_ak = MagicMock()
    fake_ak.futures_main_sina.return_value = df
    with patch.object(futures_mod, "ak", fake_ak):
        res = update_futures(db)
    assert res["total"] == 2 * len(FUTURES_VARIETIES)
    assert res["saved"] == 2 * len(FUTURES_VARIETIES)
    assert fake_ak.futures_main_sina.call_count == len(FUTURES_VARIETIES)
    db.save_futures_daily_batch.assert_called_once()


def test_update_futures_silver_variety_registered():
    """白银（上期所 AG）在品种列表中，区别于工业硅 SI。"""
    codes = {v["sina_code"] for v in FUTURES_VARIETIES}
    assert "AG0" in codes


def test_fetch_history_skips_rows_without_date():
    """日期缺失的行应被跳过，不得落库为空 trade_date。"""
    df = _sina_df([100.0, 105.0], ["2026-07-17", None])
    fake_ak = MagicMock()
    fake_ak.futures_main_sina.return_value = df
    with patch.object(futures_mod, "ak", fake_ak):
        rows = futures_mod._fetch_futures_history(VARIETY, "20260701", "20260718")
    assert len(rows) == 1
    assert rows[0]["trade_date"] == "2026-07-17"
