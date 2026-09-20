"""tasks/lithium_spot.py 单元测试：现货+基差解析、增量窗口与任务契约。"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd

from tasks.lithium_spot import _fetch_lithium_spot, update_lithium_spot

MODULE = "tasks.lithium_spot"


def _spot_df() -> pd.DataFrame:
    """五行生意社现货数据（含一行重叠窗口之前的旧数据）。"""
    return pd.DataFrame({
        "date": ["20260914", "20260915", "20260916", "20260917", "20260918"],
        "symbol": ["LC"] * 5,
        "spot_price": [129500.0, 130000.0, 130500.0, 131000.0, 130000.0],
        "near_contract": ["LC2610"] * 5,
        "near_contract_price": [128000.0, 128500.0, 129000.0, 132280.0, 128580.0],
        "dominant_contract": ["LC2701"] * 5,
        "dominant_contract_price": [127000.0, 127500.0, 128000.0, 131180.0, 127160.0],
        "near_basis": [1500.0] * 5,
        "dom_basis": [2500.0] * 5,
        "near_basis_rate": [0.0117] * 5,
        "dom_basis_rate": [0.0197] * 5,
    })


# ===========================================================================
# _fetch_lithium_spot
# ===========================================================================


@patch(f"{MODULE}.ak")
def test_fetch_full_backfill_when_empty(mock_ak: MagicMock):
    mock_ak.futures_spot_price_daily.return_value = _spot_df()
    records = _fetch_lithium_spot(None)
    assert len(records) == 5
    row = records[-1]
    assert row["spot_date"] == "2026-09-18"
    assert row["spot_price"] == 130000.0
    assert row["dom_basis"] == 2500.0
    assert row["dom_basis_rate"] == 0.0197
    assert row["data_source"] == "sunss(生意社)"
    # 全量回填起点 2022-01-01
    assert mock_ak.futures_spot_price_daily.call_args[1]["start_day"] == "20220101"


@patch(f"{MODULE}.ak")
def test_fetch_incremental_window(mock_ak: MagicMock):
    """库内最新 09-16：cutoff = 09-06，五行均在重叠窗口内保留（幂等去重）。"""
    mock_ak.futures_spot_price_daily.return_value = _spot_df()
    records = _fetch_lithium_spot("2026-09-16")
    assert [r["spot_date"] for r in records] == [
        "2026-09-14", "2026-09-15", "2026-09-16", "2026-09-17", "2026-09-18",
    ]


@patch(f"{MODULE}.ak")
def test_fetch_source_failure_returns_empty(mock_ak: MagicMock):
    mock_ak.futures_spot_price_daily.side_effect = RuntimeError("network")
    assert _fetch_lithium_spot(None) == []


# ===========================================================================
# update_lithium_spot
# ===========================================================================


@patch(f"{MODULE}.ak")
def test_update_lithium_spot_success(mock_ak: MagicMock):
    db = MagicMock()
    db.get_lithium_spot_latest_date.return_value = "2026-09-16"
    db.save_lithium_spot_batch.side_effect = lambda records: len(records)
    mock_ak.futures_spot_price_daily.return_value = _spot_df()
    r = update_lithium_spot(db)
    assert r == {"saved": 5, "total": 5}
    db.save_lithium_spot_batch.assert_called_once()


def test_update_lithium_spot_ak_none():
    db = MagicMock()
    with patch(f"{MODULE}.ak", None):
        r = update_lithium_spot(db)
    assert r["saved"] == 0
    assert "error" in r


@patch(f"{MODULE}.ak")
def test_update_lithium_spot_no_new_data(mock_ak: MagicMock):
    db = MagicMock()
    db.get_lithium_spot_latest_date.return_value = "2026-12-31"
    mock_ak.futures_spot_price_daily.return_value = _spot_df()
    r = update_lithium_spot(db)
    assert r == {"saved": 0, "total": 0}
    db.save_lithium_spot_batch.assert_not_called()


@patch(f"{MODULE}.ak")
def test_update_lithium_spot_exception(mock_ak: MagicMock):
    db = MagicMock()
    db.get_lithium_spot_latest_date.side_effect = RuntimeError("boom")
    r = update_lithium_spot(db)
    assert r["saved"] == 0
    assert "error" in r
