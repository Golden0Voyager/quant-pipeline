"""tasks/hk_tech_index.py 单元测试：新浪港股指数抓取、增量过滤与任务契约。"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd

from tasks.hk_tech_index import _fetch_hk_tech_records, update_hk_tech_index

MODULE = "tasks.hk_tech_index"


def _hk_df() -> pd.DataFrame:
    """四行恒生科技日线（含一行重叠窗口之前的旧数据；date 同 sina 接口实测格式）。"""
    return pd.DataFrame({
        "date": pd.to_datetime(
            ["2026-09-01", "2026-09-16", "2026-09-17", "2026-09-18"]
        ).date,
        "open": [4300.0, 4328.41, 4350.0, 4413.11],
        "high": [4350.0, 4413.11, 4360.0, 4420.0],
        "low": [4280.0, 4328.41, 4340.0, 4405.5],
        "close": [4340.0, 4405.5, 4355.0, 4415.0],
        "volume": [1800000000, 2001880289, 2100000000, 1900000000],
        "amount": [52000000000, 57833102902, 59000000000, 57000000000],
    })


# ===========================================================================
# _fetch_hk_tech_records
# ===========================================================================


@patch(f"{MODULE}.ak")
def test_fetch_full_backfill_when_table_empty(mock_ak: MagicMock):
    mock_ak.stock_hk_index_daily_sina.return_value = _hk_df()
    records = _fetch_hk_tech_records(None)
    assert len(records) == 4
    first, second = records[0], records[1]
    assert first["trade_date"] == "2026-09-01"
    assert first["close"] == 4340.0
    # 首日无前收，change_pct 为 None；次日按前收计算
    assert first["change_pct"] is None
    assert second["change_pct"] == round((4405.5 - 4340.0) / 4340.0 * 100, 4)
    assert first["data_source"] == "akshare_sina_hk"


@patch(f"{MODULE}.ak")
def test_fetch_incremental_window(mock_ak: MagicMock):
    """库内最新 09-17：cutoff = 09-10，旧数据被过滤，重叠窗内行保留（幂等去重）。"""
    mock_ak.stock_hk_index_daily_sina.return_value = _hk_df()
    records = _fetch_hk_tech_records("2026-09-17")
    assert [r["trade_date"] for r in records] == ["2026-09-16", "2026-09-17", "2026-09-18"]


@patch(f"{MODULE}.ak")
def test_fetch_failure_returns_empty(mock_ak: MagicMock):
    mock_ak.stock_hk_index_daily_sina.side_effect = RuntimeError("network")
    assert _fetch_hk_tech_records(None) == []


# ===========================================================================
# update_hk_tech_index
# ===========================================================================


@patch(f"{MODULE}.ak")
def test_update_hk_tech_index_success(mock_ak: MagicMock):
    db = MagicMock()
    db.get_hk_tech_latest_date.return_value = "2026-09-17"
    db.save_hk_tech_index_batch.return_value = 3
    mock_ak.stock_hk_index_daily_sina.return_value = _hk_df()
    r = update_hk_tech_index(db)
    assert r == {"saved": 3, "total": 3}
    db.save_hk_tech_index_batch.assert_called_once()
    rec = db.save_hk_tech_index_batch.call_args[0][0][-1]
    assert rec["trade_date"] == "2026-09-18"


def test_update_hk_tech_index_ak_none():
    db = MagicMock()
    with patch(f"{MODULE}.ak", None):
        r = update_hk_tech_index(db)
    assert r["saved"] == 0
    assert "error" in r


@patch(f"{MODULE}.ak")
def test_update_hk_tech_index_no_new_data(mock_ak: MagicMock):
    db = MagicMock()
    # 库内最新日晚于接口全部数据：增量窗口过滤后为零行
    db.get_hk_tech_latest_date.return_value = "2026-09-30"
    mock_ak.stock_hk_index_daily_sina.return_value = _hk_df()
    r = update_hk_tech_index(db)
    assert r == {"saved": 0, "total": 0}
    db.save_hk_tech_index_batch.assert_not_called()


@patch(f"{MODULE}.ak")
def test_update_hk_tech_index_exception(mock_ak: MagicMock):
    db = MagicMock()
    db.get_hk_tech_latest_date.side_effect = RuntimeError("boom")
    r = update_hk_tech_index(db)
    assert r["saved"] == 0
    assert "error" in r
