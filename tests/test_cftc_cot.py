"""tasks/cftc_cot.py 单元测试：宽表转长表、增量窗口与任务契约。"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd

from tasks.cftc_cot import _fetch_market_cot, _melt_market, update_cftc_cot

MODULE = "tasks.cftc_cot"


def _cot_df() -> pd.DataFrame:
    """五行宽表（含一行重叠窗口之前的旧数据），两个品种 × 多/空/净。"""
    return pd.DataFrame({
        "日期": pd.to_datetime(
            ["2026-05-01", "2026-08-18", "2026-08-25", "2026-09-01", "2026-09-08"]
        ).date,
        "纽约原油-多头仓位": [370000.0, 360000.0, 355000.0, 350118.0, 345000.0],
        "纽约原油-空头仓位": [98000.0, 100000.0, 102000.0, 105000.0, 108000.0],
        "纽约原油-净仓位": [272000.0, 260000.0, 253000.0, 245118.0, 237000.0],
        "黄金-多头仓位": [200000.0, 210000.0, 212000.0, 215000.0, 218000.0],
        "黄金-空头仓位": [92000.0, 90000.0, 88000.0, 86000.0, 84000.0],
        "黄金-净仓位": [108000.0, 120000.0, 124000.0, 129000.0, 134000.0],
    })


# ===========================================================================
# _melt_market
# ===========================================================================


def test_melt_wide_to_long():
    records = _melt_market(_cot_df(), "goods")
    assert len(records) == 5 * 2
    crude = [r for r in records if r["instrument"] == "纽约原油"]
    assert len(crude) == 5
    row = crude[-1]
    assert row["trade_date"] == "2026-09-08"
    assert row["market"] == "goods"
    assert row["long_positions"] == 345000.0
    assert row["net_positions"] == 237000.0
    assert row["data_source"] == "cftc"


def test_melt_empty_df():
    assert _melt_market(pd.DataFrame(), "goods") == []
    assert _melt_market(None, "fx") == []


# ===========================================================================
# _fetch_market_cot
# ===========================================================================


@patch(f"{MODULE}.ak")
def test_fetch_full_backfill_when_empty(mock_ak: MagicMock):
    mock_ak.macro_usa_cftc_c_holding.return_value = _cot_df()
    records = _fetch_market_cot("goods", None)
    assert len(records) == 10


@patch(f"{MODULE}.ak")
def test_fetch_incremental_overlap_window(mock_ak: MagicMock):
    """库内最新 09-01：cutoff = 12 周前（06-10），05-01 旧行被过滤。"""
    mock_ak.macro_usa_cftc_c_holding.return_value = _cot_df()
    records = _fetch_market_cot("goods", "2026-09-01")
    dates = sorted({r["trade_date"] for r in records})
    assert dates == ["2026-08-18", "2026-08-25", "2026-09-01", "2026-09-08"]


@patch(f"{MODULE}.ak")
def test_fetch_source_failure_returns_empty(mock_ak: MagicMock):
    mock_ak.macro_usa_cftc_nc_holding.side_effect = RuntimeError("network")
    assert _fetch_market_cot("fx", None) == []


# ===========================================================================
# update_cftc_cot
# ===========================================================================


@patch(f"{MODULE}.ak")
def test_update_cftc_cot_no_new_weekly_report(mock_ak: MagicMock):
    """库内最新已覆盖接口全部数据：增量过滤后为零 → 显式 skipped。"""
    db = MagicMock()
    db.get_cftc_cot_latest_date.side_effect = ["2026-12-31", "2026-12-31"]
    db.save_cftc_cot_batch.return_value = 0
    mock_ak.macro_usa_cftc_c_holding.return_value = _cot_df()
    mock_ak.macro_usa_cftc_nc_holding.return_value = _cot_df()
    r = update_cftc_cot(db)
    assert r == {"skipped": True, "reason": "no new weekly COT report"}
    db.save_cftc_cot_batch.assert_not_called()


@patch(f"{MODULE}.ak")
def test_update_cftc_cot_backfill(mock_ak: MagicMock):
    db = MagicMock()
    db.get_cftc_cot_latest_date.return_value = None
    db.save_cftc_cot_batch.side_effect = lambda records: len(records)
    mock_ak.macro_usa_cftc_c_holding.return_value = _cot_df()
    mock_ak.macro_usa_cftc_nc_holding.return_value = _cot_df()
    r = update_cftc_cot(db)
    assert r == {"saved": 20, "total": 20}
    assert db.save_cftc_cot_batch.call_count == 2


def test_update_cftc_cot_ak_none():
    db = MagicMock()
    with patch(f"{MODULE}.ak", None):
        r = update_cftc_cot(db)
    assert r["saved"] == 0
    assert "error" in r


@patch(f"{MODULE}.ak")
def test_update_cftc_cot_exception(mock_ak: MagicMock):
    db = MagicMock()
    db.get_cftc_cot_latest_date.side_effect = RuntimeError("boom")
    r = update_cftc_cot(db)
    assert r["saved"] == 0
    assert "error" in r
