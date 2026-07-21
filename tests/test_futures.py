"""futures.py 覆盖率测试：聚焦 _fetch_futures 解析逻辑与 update_futures 调度。

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
        "date": dates,
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


# ───────────────────────── _fetch_futures ─────────────────────────

def test_fetch_futures_normal():
    df = _sina_df([100.0, 105.0], ["2026-07-17", "2026-07-18"])
    fake_ak = MagicMock()
    fake_ak.futures_main_sina.return_value = df
    with patch.object(futures_mod, "ak", fake_ak):
        rec = futures_mod._fetch_futures(VARIETY, "2026-07-18")
    assert rec is not None
    assert rec["symbol"] == "M"
    assert rec["name"] == "豆粕"
    assert rec["close"] == 105.0
    assert rec["open"] == 12.0
    assert rec["high"] == 14.0
    assert rec["low"] == 11.0
    assert rec["volume"] == 110
    assert rec["hold"] == 55
    assert rec["change_pct"] == pytest.approx((105.0 - 100.0) / 100.0 * 100)
    assert rec["data_source"] == "akshare_sina"


def test_fetch_futures_ak_none():
    with patch.object(futures_mod, "ak", None):
        assert futures_mod._fetch_futures(VARIETY, "2026-07-18") is None


def test_fetch_futures_empty_df():
    fake_ak = MagicMock()
    fake_ak.futures_main_sina.return_value = pd.DataFrame()
    with patch.object(futures_mod, "ak", fake_ak):
        assert futures_mod._fetch_futures(VARIETY, "2026-07-18") is None


def test_fetch_futures_none_response():
    fake_ak = MagicMock()
    fake_ak.futures_main_sina.return_value = None
    with patch.object(futures_mod, "ak", fake_ak):
        assert futures_mod._fetch_futures(VARIETY, "2026-07-18") is None


def test_fetch_futures_single_row_no_preclose():
    df = _sina_df([100.0], ["2026-07-18"])
    fake_ak = MagicMock()
    fake_ak.futures_main_sina.return_value = df
    with patch.object(futures_mod, "ak", fake_ak):
        rec = futures_mod._fetch_futures(VARIETY, "2026-07-18")
    assert rec is not None
    # 仅 1 行时无昨收，涨跌幅应为 None
    assert rec["change_pct"] is None


def test_fetch_futures_nan_and_none_values():
    # None/NaN 放在最后一行（df.iloc[-1] 取最新行），应被归一化为 None
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
        rec = futures_mod._fetch_futures(VARIETY, "2026-07-18")
    assert rec is not None
    # NaN / None 应归一化为 None
    assert rec["open"] is None
    assert rec["high"] is None
    assert rec["low"] is None
    assert rec["volume"] is None
    assert rec["hold"] is None


def test_fetch_futures_zero_preclose_no_division():
    # 昨收为 0 时不应除零，change_pct 应为 None
    df = _sina_df([0.0, 105.0], ["2026-07-17", "2026-07-18"])
    fake_ak = MagicMock()
    fake_ak.futures_main_sina.return_value = df
    with patch.object(futures_mod, "ak", fake_ak):
        rec = futures_mod._fetch_futures(VARIETY, "2026-07-18")
    assert rec is not None
    assert rec["change_pct"] is None


def test_fetch_futures_raises():
    fake_ak = MagicMock()
    fake_ak.futures_main_sina.side_effect = RuntimeError("network boom")
    with patch.object(futures_mod, "ak", fake_ak):
        assert futures_mod._fetch_futures(VARIETY, "2026-07-18") is None


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
    assert res == {"saved": 0, "total": 0}
    db.save_futures_daily_batch.assert_not_called()


def test_update_futures_mixed_results():
    """部分品种成功、部分失败 → 只保存成功的记录。"""
    db = MagicMock()
    db.save_futures_daily_batch.return_value = 3
    df_valid = _sina_df([100.0, 105.0], ["2026-07-17", "2026-07-18"])

    # 前 3 个品种返回有效数据，其余返回空
    def _side_effect(symbol, start_date, end_date):
        # 前 3 个品种（LC0, SI0, PS0）返回数据
        if symbol in ("LC0", "SI0", "PS0"):
            return df_valid
        return pd.DataFrame()

    fake_ak = MagicMock()
    fake_ak.futures_main_sina.side_effect = _side_effect
    with patch.object(futures_mod, "ak", fake_ak):
        res = update_futures(db)
    assert res["total"] == 3
    assert res["saved"] == 3
    assert fake_ak.futures_main_sina.call_count == len(FUTURES_VARIETIES)
    db.save_futures_daily_batch.assert_called_once()


def test_update_futures_partial_save():
    """db.save 返回的数量与总记录数不同。"""
    db = MagicMock()
    db.save_futures_daily_batch.return_value = 2  # 只保存了 2 条
    df = _sina_df([100.0, 105.0], ["2026-07-17", "2026-07-18"])
    fake_ak = MagicMock()
    fake_ak.futures_main_sina.return_value = df
    with patch.object(futures_mod, "ak", fake_ak):
        res = update_futures(db)
    assert res["total"] == len(FUTURES_VARIETIES)
    assert res["saved"] == 2  # 只有 2 条入库


def test_update_futures_saves_all_varieties():
    db = MagicMock()
    db.save_futures_daily_batch.return_value = len(FUTURES_VARIETIES)
    df = _sina_df([100.0, 105.0], ["2026-07-17", "2026-07-18"])
    fake_ak = MagicMock()
    fake_ak.futures_main_sina.return_value = df
    with patch.object(futures_mod, "ak", fake_ak):
        res = update_futures(db)
    assert res["total"] == len(FUTURES_VARIETIES)
    assert res["saved"] == len(FUTURES_VARIETIES)
    # 每个品种各取一次
    assert fake_ak.futures_main_sina.call_count == len(FUTURES_VARIETIES)
    db.save_futures_daily_batch.assert_called_once()
