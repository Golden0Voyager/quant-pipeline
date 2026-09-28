"""边缘日期解析红证测试：非法 latest_date / 统计时间不得令任务崩溃或落脏数据。

对应 review 分诊项③：
- tasks/hk_tech_index.py：库内最新日期为空串/畸形串时旧实现直接 strptime 抛 ValueError。
- tasks/money_market.py：央行资产负债表「统计时间」非 "YYYY.M" 形态时旧实现放行脏日期
  （如 "2026." → "2026--01"），白名单校验后这类行必须被跳过（全畸形时任务落 no_data）。
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd

from tasks.hk_tech_index import _fetch_hk_tech_records, update_hk_tech_index
from tasks.money_market import _fetch_central_bank_balance, update_money_market

HK_MODULE = "tasks.hk_tech_index"
MM_MODULE = "tasks.money_market"


def _hk_df() -> pd.DataFrame:
    """单行恒生科技日线（date 同 sina 接口实测格式）。"""
    return pd.DataFrame({
        "date": pd.to_datetime(["2026-09-18"]).date,
        "open": [4413.11],
        "high": [4420.0],
        "low": [4405.5],
        "close": [4415.0],
        "volume": [1900000000],
        "amount": [57000000000],
    })


def _balance_df(months: list[str]) -> pd.DataFrame:
    """央行资产负债表接口形态：统计时间为 "YYYY.M"，数值列全给 1.0。"""
    return pd.DataFrame({
        "统计时间": months,
        "总资产": [1.0] * len(months),
        "储备货币": [1.0] * len(months),
        "发行货币": [1.0] * len(months),
        "对其他存款性公司债权": [1.0] * len(months),
        "对政府债权": [1.0] * len(months),
        "政府存款": [1.0] * len(months),
        "国外资产": [1.0] * len(months),
        "外汇": [1.0] * len(months),
    })


# ===========================================================================
# tasks/hk_tech_index.py：latest_date 边缘解析
# ===========================================================================


@patch(f"{HK_MODULE}.ak")
def test_fetch_empty_latest_date_returns_no_records(mock_ak: MagicMock):
    """空串 latest_date：旧实现 strptime 抛 ValueError；必须按 no_data（空 records）返回。"""
    mock_ak.stock_hk_index_daily_sina.return_value = _hk_df()
    assert _fetch_hk_tech_records("") == []


@patch(f"{HK_MODULE}.ak")
def test_fetch_malformed_latest_date_returns_no_records(mock_ak: MagicMock):
    """非 YYYY-MM-DD 形态 latest_date：同样不得抛异常，按 no_data 返回。"""
    mock_ak.stock_hk_index_daily_sina.return_value = _hk_df()
    assert _fetch_hk_tech_records("not-a-date") == []


@patch(f"{HK_MODULE}.ak")
def test_update_empty_latest_date_returns_no_data_shape(mock_ak: MagicMock):
    """任务级：库内最新日期为空串时返回与「无新数据」分支逐字一致的 no_data 结构。"""
    db = MagicMock()
    db.get_hk_tech_latest_date.return_value = ""
    mock_ak.stock_hk_index_daily_sina.return_value = _hk_df()
    r = update_hk_tech_index(db)
    assert r == {"saved": 0, "total": 0}
    assert "error" not in r
    db.save_hk_tech_index_batch.assert_not_called()


# ===========================================================================
# tasks/money_market.py：统计时间边缘解析
# ===========================================================================


@patch(f"{MM_MODULE}.ak")
def test_balance_malformed_months_are_skipped(mock_ak: MagicMock):
    """畸形统计时间（空月、超界月、非数字）一律跳过；合法 "YYYY.M" 行保留。"""
    mock_ak.macro_china_central_bank_balance.return_value = _balance_df(
        ["2026.", "2026.13", "abc.6", "2026.6.1", "2026.6"]
    )
    records = _fetch_central_bank_balance()
    assert [r["date"] for r in records] == ["2026-06-01"]


@patch(f"{MM_MODULE}.ak")
def test_update_all_malformed_balance_months_yields_no_data(mock_ak: MagicMock):
    """全部统计时间畸形时：无行可写，任务级裁定必须为既有 no_data 路径。"""
    mock_ak.macro_china_shibor_all.return_value = pd.DataFrame()
    mock_ak.repo_rate_query.return_value = pd.DataFrame()
    mock_ak.macro_bank_china_interest_rate.return_value = pd.DataFrame()
    mock_ak.macro_china_central_bank_balance.return_value = _balance_df(
        ["2026.", "not-a-month"]
    )
    db = MagicMock()
    r = update_money_market(db)
    assert r["status"] == "no_data"
    assert r["saved"] == 0
    db.save_central_bank_balance_batch.assert_not_called()
