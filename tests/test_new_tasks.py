"""Coverage tests for the extended data tasks.

The newly-added network task modules (china_macro / convertible_bond /
corporate_actions / finance_flow / sector_derivatives / index_chain) are the
biggest coverage gaps. These tests mock ``akshare`` so no network calls are
made, while still exercising every ``_fetch_*`` helper and ``update_*`` entry
point to raise line coverage above the 85% gate (PR #28).
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

import tasks.china_macro as china_macro
import tasks.concept_board as concept_board
import tasks.convertible_bond as convertible_bond
import tasks.corporate_actions as corporate_actions
import tasks.finance_flow as finance_flow
import tasks.financials as financials
import tasks.index_chain as index_chain
import tasks.market_valuation as market_valuation
import tasks.money_market as money_market
import tasks.sector_derivatives as sector_derivatives
from core.source_client import FetchMetadata, SourceResponse

# ===========================================================================
# money_market
# ===========================================================================


def _mock_ak_money_market() -> MagicMock:
    ak = MagicMock()
    ak.macro_china_shibor_all.return_value = pd.DataFrame(
        {
            "日期": ["2024-01-02", "2024-01-03"],
            "O/N-定价": [1.5, 1.6],
            "1W-定价": [1.8, 1.9],
            "2W-定价": [2.0, 2.1],
            "1M-定价": [2.2, 2.3],
            "3M-定价": [2.5, 2.6],
            "6M-定价": [2.7, 2.8],
            "9M-定价": [2.9, 3.0],
            "1Y-定价": [3.1, 3.2],
        }
    )
    ak.repo_rate_query.return_value = pd.DataFrame(
        {
            "date": ["2024-01-02", "2024-01-03"],
            "FR001": [1.2, 1.3],
            "FR007": [1.5, 1.6],
            "FR014": [1.8, 1.9],
        }
    )
    ak.macro_bank_china_interest_rate.return_value = pd.DataFrame(
        {
            "日期": ["2024-01-17"],
            "今值": [3.45],
        }
    )
    ak.macro_china_central_bank_balance.return_value = pd.DataFrame(
        {
            "统计时间": ["2024.12"],
            "总资产": [400000],
            "储备货币": [350000],
            "发行货币": [120000],
            "对其他存款性公司债权": [150000],
            "对政府债权": [50000],
            "政府存款": [30000],
            "国外资产": [200000],
            "外汇": [180000],
        }
    )
    return ak


def test_update_money_market_runs_all_fetchers():
    ak = _mock_ak_money_market()
    db = MagicMock()
    db.save_money_market_batch.return_value = 1
    db.save_central_bank_balance_batch.return_value = 1
    with patch.object(money_market, "ak", ak):
        result = money_market.update_money_market(db)
    assert "daily_saved" in result
    assert "balance_saved" in result
    assert db.save_money_market_batch.called
    assert db.save_central_bank_balance_batch.called


def test_update_money_market_ak_none():
    db = MagicMock()
    with patch.object(money_market, "ak", None):
        result = money_market.update_money_market(db)
    assert result["saved"] == 0
    assert "error" in result


# ===========================================================================
# china_macro
# ===========================================================================


def _mock_ak_china_macro() -> MagicMock:
    ak = MagicMock()
    ak.macro_china_cpi.return_value = pd.DataFrame(
        {"月份": ["2024-01"], "全国-同比增长": [0.2], "全国-环比增长": [0.1]}
    )
    ak.macro_china_ppi.return_value = pd.DataFrame({"月份": ["2024-01"], "当月": [0.3], "当月同比增长": [0.2]})
    ak.macro_china_pmi.return_value = pd.DataFrame(
        {"月份": ["2024-01"], "制造业-指数": [50.5], "制造业-同比增长": [1.0]}
    )
    ak.macro_china_cx_pmi_yearly.return_value = pd.DataFrame({"日期": ["2024-01-01"], "今值": [51.0]})
    ak.macro_china_money_supply.return_value = pd.DataFrame(
        {
            "月份": ["2024-01"],
            "货币和准货币(M2)-数量(亿元)": [300],
            "货币和准货币(M2)-同比增长": [8.0],
            "货币(M1)-数量(亿元)": [100],
            "货币(M1)-同比增长": [3.0],
            "流通中的现金(M0)-数量(亿元)": [10],
            "流通中的现金(M0)-同比增长": [1.0],
        }
    )
    ak.macro_china_new_financial_credit.return_value = pd.DataFrame(
        {"月份": ["2024-01"], "当月": [100], "当月-同比增长": [5.0]}
    )
    ak.macro_china_consumer_goods_retail.return_value = pd.DataFrame(
        {"月份": ["2024-01"], "同比增长": [4.0], "累计-同比增长": [4.5]}
    )
    ak.macro_china_gdzctz.return_value = pd.DataFrame({"月份": ["2024-01"], "同比增长": [3.0], "自年初累计": [3.5]})
    ak.macro_china_hgjck.return_value = pd.DataFrame(
        {
            "月份": ["2024-01"],
            "当月出口额-金额": [100],
            "当月出口额-同比增长": [2.0],
            "当月进口额-金额": [90],
            "当月进口额-同比增长": [1.0],
        }
    )
    ak.macro_china_industrial_production_yoy.return_value = pd.DataFrame({"日期": ["2024-01-01"], "今值": [5.0]})
    ak.macro_china_society_electricity.return_value = pd.DataFrame(
        {"统计时间": ["2024.1"], "全社会用电量": [7000], "全社会用电量同比": [6.0]}
    )
    ak.macro_china_qyspjg.return_value = pd.DataFrame(
        {"月份": ["2024-01"], "总指数-指数值": [105], "总指数-同比增长": [1.0], "总指数-环比增长": [0.5]}
    )
    ak.macro_china_xfzxx.return_value = pd.DataFrame(
        {
            "月份": ["2024-01"],
            "消费者信心指数-指数值": [120],
            "消费者满意指数-指数值": [115],
            "消费者预期指数-指数值": [125],
        }
    )
    ak.macro_china_lpr.return_value = pd.DataFrame({"TRADE_DATE": ["2024-01-01"], "LPR1Y": [3.45], "LPR5Y": [3.95]})
    ak.macro_china_gdp.return_value = pd.DataFrame(
        {
            "季度": ["2024年第一季度"],
            "国内生产总值（亿元）": [300000],
            "GDP同比增长": [5.0],
            "GDP环比增长": [1.0],
            "第一产业（亿元）": [10000],
            "第二产业（亿元）": [120000],
            "第三产业（亿元）": [170000],
        }
    )
    return ak


def test_update_china_macro_runs_all_fetchers():
    ak = _mock_ak_china_macro()
    db = MagicMock()
    db.save_macro_monthly_batch.return_value = 1
    db.save_macro_quarterly_batch.return_value = 1
    with patch.object(china_macro, "ak", ak):
        result = china_macro.update_china_macro(db)
    assert "monthly_saved" in result
    assert "quarterly_saved" in result
    assert db.save_macro_monthly_batch.called
    assert db.save_macro_quarterly_batch.called


def test_update_china_macro_ak_none():
    db = MagicMock()
    with patch.object(china_macro, "ak", None):
        result = china_macro.update_china_macro(db)
    assert result["saved"] == 0
    assert "error" in result


# ===========================================================================
# convertible_bond
# ===========================================================================


def _mock_ak_convertible_bond() -> MagicMock:
    ak = MagicMock()
    ak.bond_cb_jsl.return_value = pd.DataFrame(
        {
            "代码": ["113050"],
            "转债名称": ["测试转债"],
            "现价": [120.0],
            "转股溢价率": [5.0],
            "双低": [125.0],
            "到期时间": ["2028-01-01"],
        }
    )
    ak.bond_cb_redeem_jsl.return_value = pd.DataFrame(
        {
            "代码": ["113050"],
            "名称": ["测试转债"],
            "强赎状态": ["Y"],
            "强赎价": [100.0],
            "最后交易日": ["2026-08-01"],
        }
    )
    ak.bond_cb_index_jsl.return_value = pd.DataFrame(
        {
            "日期": ["2024-01-01"],
            "指数代码": ["000832"],
            "指数名称": ["中证转债"],
            "开盘": [400.0],
            "收盘": [401.0],
            "最高": [402.0],
            "最低": [399.0],
            "成交量": [1e6],
        }
    )
    return ak


def test_update_convertible_bond_runs_all():
    ak = _mock_ak_convertible_bond()
    db = MagicMock()
    for m in ("save_cb_quotation_batch", "save_cb_redeem_batch", "save_cb_index_batch"):
        setattr(db, m, MagicMock(return_value=1))
    with patch.object(convertible_bond, "ak", ak):
        result = convertible_bond.update_convertible_bond(db)
    assert "quotation" in result and "redeem" in result and "index" in result
    assert db.save_cb_quotation_batch.called
    assert db.save_cb_redeem_batch.called
    assert db.save_cb_index_batch.called


def test_convertible_bond_individual_and_ak_none():
    ak = _mock_ak_convertible_bond()
    db = MagicMock()
    for m in ("save_cb_quotation_batch", "save_cb_redeem_batch", "save_cb_index_batch"):
        setattr(db, m, MagicMock(return_value=1))
    with patch.object(convertible_bond, "ak", ak):
        assert convertible_bond.update_cb_quotation(db)["saved"] == 1
        assert convertible_bond.update_cb_redeem(db)["saved"] == 1
        assert convertible_bond.update_cb_index(db)["saved"] == 1
    db2 = MagicMock()
    with patch.object(convertible_bond, "ak", None):
        assert convertible_bond.update_cb_quotation(db2)["saved"] == 0
        assert convertible_bond.update_cb_redeem(db2)["saved"] == 0
        assert convertible_bond.update_cb_index(db2)["saved"] == 0


# --- 补充边缘路径 ---


def test_cb_to_float_nan():
    """_to_float: NaN → None。"""
    assert convertible_bond._to_float(float("nan")) is None


def test_cb_to_float_invalid():
    """_to_float: 非数值 → None。"""
    assert convertible_bond._to_float("abc") is None


def test_cb_fetch_quotation_ak_none():
    """_fetch_cb_quotation: ak=None → []."""
    with patch.object(convertible_bond, "ak", None):
        assert convertible_bond._fetch_cb_quotation() == []


def test_cb_fetch_quotation_exception():
    """_fetch_cb_quotation: ak 抛异常 → []."""
    ak = MagicMock()
    ak.bond_cb_jsl.side_effect = RuntimeError("network err")
    with patch.object(convertible_bond, "ak", ak):
        assert convertible_bond._fetch_cb_quotation() == []


def test_cb_update_quotation_save_exception():
    """update_cb_quotation: 保存抛异常 → error dict。"""
    ak = MagicMock()
    ak.bond_cb_jsl.return_value = pd.DataFrame(
        {
            "代码": ["113050"],
            "转债名称": ["测试"],
            "现价": [120.0],
            "转股溢价率": [5.0],
            "双低": [125.0],
            "到期时间": ["2028-01-01"],
        }
    )
    db = MagicMock()
    db.save_cb_quotation_batch.side_effect = RuntimeError("save fail")
    with patch.object(convertible_bond, "ak", ak):
        result = convertible_bond.update_cb_quotation(db)
    assert result["saved"] == 0
    assert "error" in result


def test_cb_fetch_redeem_ak_none():
    """_fetch_cb_redeem: ak=None → []."""
    with patch.object(convertible_bond, "ak", None):
        assert convertible_bond._fetch_cb_redeem() == []


def test_cb_fetch_redeem_exception():
    """_fetch_cb_redeem: ak 抛异常 → []."""
    ak = MagicMock()
    ak.bond_cb_redeem_jsl.side_effect = RuntimeError("network err")
    with patch.object(convertible_bond, "ak", ak):
        assert convertible_bond._fetch_cb_redeem() == []


def test_cb_update_redeem_save_exception():
    """update_cb_redeem: 保存抛异常 → error dict。"""
    ak = MagicMock()
    ak.bond_cb_redeem_jsl.return_value = pd.DataFrame(
        {
            "代码": ["113050"],
            "名称": ["测试"],
            "强赎状态": ["Y"],
            "强赎价": [100.0],
            "最后交易日": ["2026-08-01"],
        }
    )
    db = MagicMock()
    db.save_cb_redeem_batch.side_effect = RuntimeError("save fail")
    with patch.object(convertible_bond, "ak", ak):
        result = convertible_bond.update_cb_redeem(db)
    assert result["saved"] == 0
    assert "error" in result


def test_cb_fetch_index_ak_none():
    """_fetch_cb_index: ak=None → []."""
    with patch.object(convertible_bond, "ak", None):
        assert convertible_bond._fetch_cb_index() == []


def test_cb_fetch_index_exception():
    """_fetch_cb_index: ak 抛异常 → []."""
    ak = MagicMock()
    ak.bond_cb_index_jsl.side_effect = RuntimeError("network err")
    with patch.object(convertible_bond, "ak", ak):
        assert convertible_bond._fetch_cb_index() == []


def test_cb_update_index_save_exception():
    """update_cb_index: 保存抛异常 → error dict。"""
    ak = MagicMock()
    ak.bond_cb_index_jsl.return_value = pd.DataFrame(
        {
            "price_dt": ["2024-01-01"],
            "price": [401.0],
            "volume": [1e6],
        }
    )
    db = MagicMock()
    db.save_cb_index_batch.side_effect = RuntimeError("save fail")
    with patch.object(convertible_bond, "ak", ak):
        result = convertible_bond.update_cb_index(db)
    assert result["saved"] == 0
    assert "error" in result


def test_cb_update_aggregate_quotation_exception():
    """update_convertible_bond: quotation 抛异常时不中断其他。"""
    ak = MagicMock()
    ak.bond_cb_jsl.side_effect = RuntimeError("quotation err")
    ak.bond_cb_redeem_jsl.return_value = pd.DataFrame(
        {
            "代码": ["113050"],
            "名称": ["测试"],
            "强赎状态": ["Y"],
            "强赎价": [100.0],
            "最后交易日": ["2026-08-01"],
        }
    )
    ak.bond_cb_index_jsl.return_value = pd.DataFrame(
        {
            "price_dt": ["2024-01-01"],
            "price": [401.0],
            "volume": [1e6],
        }
    )
    db = MagicMock()
    for m in ("save_cb_redeem_batch", "save_cb_index_batch"):
        setattr(db, m, MagicMock(return_value=1))
    with patch.object(convertible_bond, "ak", ak):
        result = convertible_bond.update_convertible_bond(db)
    assert "quotation" not in result  # 异常时不会设置 key
    assert result.get("redeem") == 1
    assert result.get("index") == 1


def test_cb_update_aggregate_one_fetcher_empty():
    """update_convertible_bond: 单个 fetcher 返回空不中断其他。"""
    ak = MagicMock()
    ak.bond_cb_jsl.return_value = pd.DataFrame(
        {
            "代码": ["113050"],
            "转债名称": ["测试"],
            "现价": [120.0],
            "转股溢价率": [5.0],
            "双低": [125.0],
            "到期时间": ["2028-01-01"],
        }
    )
    ak.bond_cb_redeem_jsl.return_value = pd.DataFrame(
        {
            "代码": ["113050"],
            "名称": ["测试"],
            "强赎状态": ["Y"],
            "强赎价": [100.0],
            "最后交易日": ["2026-08-01"],
        }
    )
    ak.bond_cb_index_jsl.return_value = pd.DataFrame()
    db = MagicMock()
    for m in ("save_cb_quotation_batch", "save_cb_redeem_batch"):
        setattr(db, m, MagicMock(return_value=1))
    # 验证 aggregate 不因单个空 fetcher 而崩溃
    with patch.object(convertible_bond, "ak", ak):
        result = convertible_bond.update_convertible_bond(db)
    assert result.get("quotation") == 1
    assert result.get("redeem") == 1


# ===========================================================================
# corporate_actions
# ===========================================================================


def _mock_ak_corporate_actions() -> MagicMock:
    ak = MagicMock()
    ak.stock_restricted_release_detail_em.return_value = pd.DataFrame(
        {
            "股票代码": ["000001"],
            "股票简称": ["平安银行"],
            "解禁时间": ["2026-07-15"],
            "限售股类型": ["首发原股东限售股份"],
            "实际解禁数量": [100.5],
            "解禁数量": [200.5],
        }
    )
    ak.stock_yjyg_em.return_value = pd.DataFrame(
        {
            "股票代码": ["000001"],
            "股票简称": ["平安银行"],
            "预告类型": ["预增"],
            "业绩变动幅度": [50.0],
            "上年同期值": [1.0e9],
        }
    )
    return ak


def test_update_corporate_actions_runs_all():
    ak = _mock_ak_corporate_actions()
    db = MagicMock()
    for m in ("save_restricted_share_batch", "save_earnings_forecast_batch"):
        setattr(db, m, MagicMock(return_value=1))
    with (
        patch.object(corporate_actions, "ak", ak),
        patch.object(corporate_actions, "get_expected_latest_trading_day", return_value="2026-07-19"),
    ):
        result = corporate_actions.update_corporate_actions(db)
    assert "restricted_share" in result and "earnings_forecast" in result
    assert db.save_restricted_share_batch.called
    assert db.save_earnings_forecast_batch.called


def test_corporate_actions_individual_and_ak_none():
    ak = _mock_ak_corporate_actions()
    db = MagicMock()
    for m in ("save_restricted_share_batch", "save_earnings_forecast_batch"):
        setattr(db, m, MagicMock(return_value=1))
    with (
        patch.object(corporate_actions, "ak", ak),
        patch.object(corporate_actions, "get_expected_latest_trading_day", return_value="2026-07-19"),
    ):
        assert corporate_actions.update_restricted_share(db)["saved"] == 1
        assert corporate_actions.update_earnings_forecast(db)["saved"] == 1
    db2 = MagicMock()
    with patch.object(corporate_actions, "ak", None):
        assert corporate_actions.update_restricted_share(db2)["saved"] == 0
        assert corporate_actions.update_earnings_forecast(db2)["saved"] == 0


# ===========================================================================
# finance_flow
# ===========================================================================


def _mock_ak_finance_flow() -> MagicMock:
    ak = MagicMock()
    ak.stock_hsgt_hist_em.return_value = pd.DataFrame(
        {
            "日期": ["2024-01-01"],
            "当日成交净买额": [1.0],
            "买入成交额": [2.0],
            "卖出成交额": [1.0],
            "历史累计净买额": [100.0],
        }
    )
    ak.stock_zh_ah_spot_em.return_value = pd.DataFrame(
        {
            "A股代码": ["000001"],
            "H股代码": ["00001"],
            "名称": ["平安银行"],
            "最新价-HKD": [11.2],
            "最新价-RMB": [10.1],
            "溢价": [5.5],
        }
    )
    ak.fund_etf_hist_em.return_value = pd.DataFrame(
        {
            "日期": ["2024-01-01"],
            "开盘": [2.6],
            "最高": [2.7],
            "最低": [2.5],
            "收盘": [2.65],
            "成交量": [1e8],
            "成交额": [2.6e8],
        }
    )
    return ak


def test_etf_update_range_ignores_non_real_db_path(
    tmp_path: Path,
    monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    db = MagicMock()

    with patch.object(
        finance_flow,
        "get_expected_latest_trading_day",
        return_value="2026-07-19",
    ):
        start_date, end_date, latest_date = finance_flow._get_etf_update_range(db)

    assert (start_date, end_date, latest_date) == ("20260619", "20260719", None)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize(
    "path_factory",
    [str, lambda path: path],
    ids=["str", "Path"],
)
def test_etf_update_range_reads_latest_date_from_real_db_path(
    tmp_path: Path,
    path_factory,
):
    db_path = tmp_path / "etf.db"
    with sqlite3.connect(db_path) as connection:
        connection.execute("CREATE TABLE etf_daily (trade_date TEXT)")
        connection.execute("INSERT INTO etf_daily VALUES ('2026-07-10')")

    db = MagicMock()
    db.db_path = path_factory(db_path)

    with patch.object(
        finance_flow,
        "get_expected_latest_trading_day",
        return_value="2026-07-19",
    ):
        result = finance_flow._get_etf_update_range(db)

    assert result == ("20260705", "20260719", "2026-07-10")


def test_update_finance_flow_runs_all():
    ak = _mock_ak_finance_flow()
    db = MagicMock()
    for m in ("save_south_flow_batch", "save_ah_premium_batch", "save_etf_daily_batch"):
        setattr(db, m, MagicMock(return_value=1))
    with (
        patch.object(finance_flow, "ak", ak),
        patch.object(finance_flow, "get_expected_latest_trading_day", return_value="2026-07-19"),
    ):
        result = finance_flow.update_finance_flow(db)
    assert "details" in result
    assert db.save_south_flow_batch.called
    assert db.save_ah_premium_batch.called
    assert db.save_etf_daily_batch.called


def test_finance_flow_individual_and_ak_none():
    ak = _mock_ak_finance_flow()
    db = MagicMock()
    for m in ("save_south_flow_batch", "save_ah_premium_batch", "save_etf_daily_batch"):
        setattr(db, m, MagicMock(return_value=1))
    with (
        patch.object(finance_flow, "ak", ak),
        patch.object(finance_flow, "get_expected_latest_trading_day", return_value="2026-07-19"),
    ):
        assert finance_flow.update_south_flow(db)["saved"] == 1
        assert finance_flow.update_ah_premium(db)["saved"] == 1
        assert finance_flow.update_etf_daily(db)["saved"] == 1
    db2 = MagicMock()
    with patch.object(finance_flow, "ak", None):
        assert finance_flow.update_south_flow(db2)["saved"] == 0
        assert finance_flow.update_ah_premium(db2)["saved"] == 0
        assert finance_flow.update_etf_daily(db2)["saved"] == 0


# ===========================================================================
# sector_derivatives
# ===========================================================================


def _mock_ak_sector_derivatives() -> MagicMock:
    ak = MagicMock()
    ak.stock_board_industry_name_em.return_value = pd.DataFrame({"板块名称": ["半导体", "银行", "白酒"]})
    ak.stock_board_industry_hist_em.return_value = pd.DataFrame(
        {
            "日期": ["2024-01-01"],
            "开盘": [100.0],
            "收盘": [101.0],
            "最高": [102.0],
            "最低": [99.0],
            "成交量": [1e6],
            "成交额": [1e8],
            "涨跌幅": [1.0],
        }
    )
    ak.stock_industry_pe_ratio_cninfo.return_value = pd.DataFrame(
        {
            "行业": ["银行"],
            "日期": ["2024-01-01"],
            "平均市盈率": [6.1],
            "平均市净率": [0.7],
            "总市值": [123456.0],
        }
    )
    ak.futures_zh_daily_sina.return_value = pd.DataFrame(
        {
            "日期": ["2024-01-01"],
            "开盘价": [3490.0],
            "最高价": [3510.0],
            "最低价": [3480.0],
            "收盘价": [3500.0],
            "成交量": [1e5],
            "持仓量": [1e4],
        }
    )
    ak.stock_zh_index_daily_tx.return_value = pd.DataFrame({"date": ["2024-01-01"], "close": [3490.0]})
    return ak


def test_update_sector_derivatives_runs_all():
    ak = _mock_ak_sector_derivatives()
    db = MagicMock()
    for m in ("save_sector_daily_batch", "save_sector_valuation_batch", "save_index_futures_basis_batch"):
        setattr(db, m, MagicMock(return_value=1))
    with patch.object(sector_derivatives, "ak", ak), patch.object(
        sector_derivatives, "get_expected_latest_trading_day", return_value="2024-01-01"
    ):
        result = sector_derivatives.update_sector_derivatives(db)
    assert "sector_daily" in result and "sector_valuation" in result and "index_futures_basis" in result
    assert db.save_sector_daily_batch.called
    assert db.save_sector_valuation_batch.called
    assert db.save_index_futures_basis_batch.called


def test_sector_derivatives_ak_none():
    db = MagicMock()
    with patch.object(sector_derivatives, "ak", None):
        result = sector_derivatives.update_sector_derivatives(db)
    assert "error" in result


# ===========================================================================
# index_chain
# ===========================================================================


def _cyq_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "trade_date": "2024-01-01",
                "profit_ratio": 0.5,
                "avg_cost": 10.0,
                "cost_90_low": 9.0,
                "cost_90_high": 11.0,
                "concentration_90": 0.3,
                "cost_70_low": 9.5,
                "cost_70_high": 10.5,
                "concentration_70": 0.2,
            }
        ]
    )


def test_update_index_daily_runs():
    ak = MagicMock()
    ak.stock_zh_index_daily_tx.return_value = pd.DataFrame(
        {
            "date": ["2024-01-01"],
            "open": [3000.0],
            "high": [3050.0],
            "low": [2980.0],
            "close": [3020.0],
            "volume": [1e8],
        }
    )
    db = MagicMock()
    db.save_index_daily_batch.return_value = 1
    with (
        patch.object(index_chain, "ak", ak),
        patch.object(index_chain, "get_expected_latest_trading_day", return_value="2026-07-19"),
    ):
        result = index_chain.update_index_daily(db)
    assert result["saved"] == 1
    assert db.save_index_daily_batch.called


def test_update_index_daily_ak_none():
    db = MagicMock()
    with patch.object(index_chain, "ak", None):
        result = index_chain.update_index_daily(db)
    assert result["saved"] == 0
    assert "error" in result


def test_update_index_daily_backfills_the_requested_historical_date():
    """回补历史日：从全历史序列里选目标日那一行，而不是永远取最后一行。"""
    ak = MagicMock()
    ak.stock_zh_index_daily_tx.return_value = pd.DataFrame(
        {
            "date": ["2026-09-11", "2026-09-14", "2026-09-15"],
            "open": [1.0, 2.0, 3.0],
            "high": [1.5, 2.5, 3.5],
            "low": [0.5, 1.5, 2.5],
            "close": [1.2, 2.2, 3.2],
            "volume": [10.0, 20.0, 30.0],
        }
    )
    db = MagicMock()
    db.save_index_daily_batch.return_value = 5
    with patch.object(index_chain, "ak", ak):
        result = index_chain.update_index_daily(db, target_date="2026-09-14")

    assert result["saved"] == 5
    rec = db.save_index_daily_batch.call_args[0][0][0]
    assert rec["trade_date"] == "2026-09-14"
    assert rec["close"] == 2.2


def test_select_index_row_keeps_the_lag_fallback_for_a_newer_target():
    """目标日晚于源端最新（当日尚未发布）→ 仍取最后一行，保留旧的源端滞后自愈。"""
    df = pd.DataFrame({"date": ["2026-09-14", "2026-09-15"], "close": [1.0, 2.0]})
    assert index_chain._select_index_row(df, "2026-09-16")["close"] == 2.0


def test_select_index_row_returns_none_for_an_absent_historical_date():
    """目标日早于源端最新但当天确实无行（停牌/非交易日）→ None，不得张冠李戴。"""
    df = pd.DataFrame({"date": ["2026-09-14", "2026-09-15"], "close": [1.0, 2.0]})
    assert index_chain._select_index_row(df, "2026-09-12") is None


def test_update_chip_distribution_em_success():
    db = MagicMock()
    db.save_chip_distribution_em_batch.return_value = 1
    # mock time.sleep 跳过限速，但保留 time.time() 返回数值（节流按耗时判断）
    mock_time = MagicMock()
    mock_time.time.return_value = 0.0
    with patch.object(index_chain, "_fetch_cyq_em", return_value=_cyq_df()), \
         patch.object(index_chain, "time", mock_time):
        result = index_chain.update_chip_distribution_em(db, symbols_to_update=["000001.SZ"])
    assert result["success"] == 1
    assert result["total"] == 1
    assert result["aborted"] is False
    assert db.save_chip_distribution_em_batch.called
    # 结果契约：成功运行不得被归一化误判为 failed（2026-08-03）
    from core.task_result import normalize_task_result
    normalised = normalize_task_result("update_chip_distribution_em", result)
    assert normalised.exit_failure is False
    assert normalised.status.value == "success"


def test_update_chip_distribution_em_all_skipped_is_no_data():
    """全跳过时归一化为 no_data 而非 failed。"""
    from core.stock_cyq_em import InsufficientDataError
    from core.task_result import TaskStatus, normalize_task_result

    db = MagicMock()
    with patch.object(
        index_chain, "_fetch_cyq_em", side_effect=InsufficientDataError("x")
    ), patch.object(index_chain.time, "sleep"):
        result = index_chain.update_chip_distribution_em(
            db, symbols_to_update=["920001.BJ"]
        )
    normalised = normalize_task_result("update_chip_distribution_em", result)
    assert normalised.status is TaskStatus.NO_DATA
    assert normalised.exit_failure is False


def test_update_chip_distribution_em_circuit_breaker():
    db = MagicMock()
    symbols = [f"00000{i}.SZ" for i in range(5)]
    # max_consecutive_failures=4 over 5 symbols aborts at the 4th failure
    # (cooldown only fires at >=5, so no time.sleep is needed here).
    with patch.object(index_chain, "_fetch_cyq_em", return_value=None):
        result = index_chain.update_chip_distribution_em(db, max_consecutive_failures=4, symbols_to_update=symbols)
    assert result["aborted"] is True
    assert result["abort_reason"] == "consecutive_failures"
    assert result["failed"] == 4
    assert result["total"] == 5
    assert result["processed"] == 4


def test_update_chip_distribution_em_insufficient_data_skips():
    """数据前置条件不满足（换手率缺失）按跳过处理：不计失败、不触发熔断。

    2026-08-02 事故：约 330 只北交所因换手率历史缺失被当作连续失败，
    触发连环冷却，尾段爬行半小时。
    """
    from core.stock_cyq_em import InsufficientDataError

    db = MagicMock()
    symbols = [f"92000{i}.BJ" for i in range(6)]
    with patch.object(
        index_chain,
        "_fetch_cyq_em",
        side_effect=InsufficientDataError("换手率有效数据仅 1/120 条"),
    ), patch.object(index_chain.time, "sleep") as mock_sleep:
        result = index_chain.update_chip_distribution_em(
            db, max_consecutive_failures=4, symbols_to_update=symbols
        )
    assert result["aborted"] is False
    assert result["failed"] == 0
    assert result["skipped"] == 6
    assert result["total"] == 6
    mock_sleep.assert_not_called()  # 无冷却、无限速休眠
    db.save_chip_distribution_em_batch.assert_not_called()


def test_fetch_cyq_em_success_and_none():
    df_in = pd.DataFrame(
        {
            "c0": ["2024-01-01"],
            "c1": [0.5],
            "c2": [10.0],
            "c3": [9.0],
            "c4": [11.0],
            "c5": [0.3],
            "c6": [9.5],
            "c7": [10.5],
            "c8": [0.2],
        }
    )
    with patch.object(index_chain, "stock_cyq_em", return_value=df_in):
        result = index_chain._fetch_cyq_em("000001.SZ")
    assert result is not None
    assert list(result.columns)[:3] == ["trade_date", "profit_ratio", "avg_cost"]
    assert result["trade_date"].iloc[0] == "2024-01-01"

    with patch.object(index_chain, "stock_cyq_em", return_value=None):
        assert index_chain._fetch_cyq_em("000001.SZ") is None

    with patch.object(index_chain, "stock_cyq_em", side_effect=RuntimeError("boom")):
        assert index_chain._fetch_cyq_em("000001.SZ") is None


def test_fetch_index_constituents():
    sina_df = pd.DataFrame({"code": ["600000", "000001", "300001", "8xxxxx"]})
    csindex_df = pd.DataFrame({"成分券代码": ["600001", "000002"]})
    with (
        patch("akshare.index_stock_cons_sina", return_value=sina_df),
        patch("akshare.index_stock_cons_csindex", return_value=csindex_df),
    ):
        sina = index_chain._fetch_index_constituents("000300", "sina")
        cs = index_chain._fetch_index_constituents("000852", "csindex")
        other = index_chain._fetch_index_constituents("x", "other")
    assert "600000" in sina and "000001" in sina and "300001" in sina
    assert "8xxxxx" not in sina
    assert "600001" in cs
    assert other == set()


def test_get_chip_em_target_symbols(tmp_path):
    import sqlite3

    db_path = tmp_path / "targets.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE IF NOT EXISTS watchlist (ts_code TEXT, status TEXT)")
    conn.execute("INSERT INTO watchlist VALUES ('000001.SZ', 'tracking')")
    conn.execute("INSERT INTO watchlist VALUES ('000002.SZ', 'dropped')")
    conn.execute("CREATE TABLE IF NOT EXISTS daily_bars (ts_code TEXT)")
    conn.execute("CREATE TABLE IF NOT EXISTS chip_distribution (ts_code TEXT)")
    conn.commit()
    conn.close()

    db = MagicMock()
    db.db_path = str(db_path)
    # 隔离 quant_agents 自选股 txt（指向空目录），使断言只反映 DB + 指数
    empty_dir = tmp_path / "empty_watchlists"
    empty_dir.mkdir()
    with patch.object(index_chain, "_fetch_index_constituents", return_value={"600000.SH", "600001.SH"}), \
         patch.object(index_chain, "_AGENTS_WATCHLIST_DIR", str(empty_dir)):
        symbols = index_chain._get_chip_em_target_symbols(db)
    assert "000001.SZ" in symbols
    assert "000002.SZ" not in symbols
    assert "600000.SH" in symbols
    assert "600001.SH" in symbols


def test_read_agents_watchlist_symbols(tmp_path):
    """txt 自选股解析：提取 6 位代码，忽略注释/空行/非法行。"""
    wl = tmp_path / "wl"
    wl.mkdir()
    (wl / "my.txt").write_text(
        "000975  # 山金国际\n002179  # 中航光电\n\n# 纯注释行\n",
        encoding="utf-8",
    )
    # 末行无换行符 + 一个非法行
    (wl / "watch.txt").write_text("300748  # 长缆科技\nnot_a_code\n600050", encoding="utf-8")
    with patch.object(index_chain, "_AGENTS_WATCHLIST_DIR", str(wl)):
        syms = index_chain._read_agents_watchlist_symbols()
    assert syms == {"000975", "002179", "300748", "600050"}


def test_read_agents_watchlist_symbols_missing_dir(tmp_path):
    """目录不存在时返回空集合，不抛异常。"""
    with patch.object(index_chain, "_AGENTS_WATCHLIST_DIR", str(tmp_path / "nope")):
        assert index_chain._read_agents_watchlist_symbols() == set()


def test_get_chip_em_target_symbols_merges_agents_watchlist(tmp_path):
    """_get_chip_em_target_symbols 合并 quant_agents txt 自选股（如非指数成分的 000975）。"""
    import sqlite3

    db_path = tmp_path / "targets2.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE IF NOT EXISTS watchlist (ts_code TEXT, status TEXT)")
    conn.execute("CREATE TABLE IF NOT EXISTS daily_bars (ts_code TEXT)")
    conn.execute("CREATE TABLE IF NOT EXISTS chip_distribution (ts_code TEXT)")
    conn.commit()
    conn.close()

    wl = tmp_path / "wl2"
    wl.mkdir()
    (wl / "my.txt").write_text("000975  # 山金国际\n", encoding="utf-8")

    db = MagicMock()
    db.db_path = str(db_path)
    with patch.object(index_chain, "_fetch_index_constituents", return_value={"600000.SH"}), \
         patch.object(index_chain, "_AGENTS_WATCHLIST_DIR", str(wl)):
        symbols = index_chain._get_chip_em_target_symbols(db)
    assert "000975" in symbols  # 非指数成分的自选股被覆盖


def test_update_chip_distribution_em_fullmarket(tmp_path):
    import sqlite3

    db_path = tmp_path / "chip_fullmarket.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE IF NOT EXISTS daily_bars (ts_code TEXT)")
    conn.execute("CREATE TABLE IF NOT EXISTS chip_distribution_em (ts_code TEXT)")
    conn.commit()
    conn.close()

    db = MagicMock()
    db.db_path = str(db_path)
    fake_result = {"success": 0, "failed": 0, "skipped": 0, "total": 0, "processed": 0, "aborted": False}
    with patch.object(index_chain, "update_chip_distribution_em", return_value=fake_result) as mock_run:
        result = index_chain.update_chip_distribution_em_fullmarket(db)
    assert mock_run.called
    assert result == fake_result


# ===========================================================================
# market_valuation
# ===========================================================================


def _mock_ak_market_valuation() -> MagicMock:
    ak = MagicMock()
    ak.stock_a_ttm_lyr.return_value = pd.DataFrame(
        {
            "date": ["2024-01-02", "2024-01-03"],
            "middlePE": [16.5, 16.3],
            "quantile": [0.45, 0.44],
            "middlePE_LYR": [15.2, 15.0],
        }
    )
    ak.stock_a_all_pb.return_value = pd.DataFrame(
        {
            "date": ["2024-01-02", "2024-01-03"],
            "middlePB": [1.8, 1.79],
            "quantile": [0.3, 0.29],
        }
    )
    ak.stock_ebs_lg.return_value = pd.DataFrame(
        {
            "日期": ["2024-01-02", "2024-01-03"],
            "沪深300": [3800.5, 3810.2],
            "股债利差": [0.06, 0.061],
            "均线": [0.058, 0.059],
        }
    )
    return ak


def test_update_market_valuation_runs_all():
    ak = _mock_ak_market_valuation()
    db = MagicMock()
    db.save_market_valuation_batch.return_value = 2
    with patch.object(market_valuation, "ak", ak):
        result = market_valuation.update_market_valuation(db)
    assert result["saved"] == 2
    assert "全市场PE" in result and "全市场PB" in result and "股债利差" in result
    assert db.save_market_valuation_batch.called


def test_update_market_valuation_ak_none():
    db = MagicMock()
    with patch.object(market_valuation, "ak", None):
        result = market_valuation.update_market_valuation(db)
    assert result["saved"] == 0
    assert "error" in result


def test_to_float_edge_cases():
    """_to_float: None/NaN/非数值 → None。"""
    assert market_valuation._to_float(None) is None
    assert market_valuation._to_float(float("nan")) is None
    assert market_valuation._to_float("not_a_number") is None
    assert market_valuation._to_float(42.0) == 42.0


def test_fetch_pe_empty_df():
    """_fetch_pe: ak 返回空 → []."""
    ak = MagicMock()
    ak.stock_a_ttm_lyr.return_value = pd.DataFrame()
    with patch.object(market_valuation, "ak", ak):
        assert market_valuation._fetch_pe() == []


def test_fetch_pb_empty_df():
    """_fetch_pb: ak 返回空 → []."""
    ak = MagicMock()
    ak.stock_a_all_pb.return_value = pd.DataFrame()
    with patch.object(market_valuation, "ak", ak):
        assert market_valuation._fetch_pb() == []


def test_fetch_ebs_empty_df():
    """_fetch_ebs: ak 返回空 → []."""
    ak = MagicMock()
    ak.stock_ebs_lg.return_value = pd.DataFrame()
    with patch.object(market_valuation, "ak", ak):
        assert market_valuation._fetch_ebs() == []


def test_update_market_valuation_all_empty():
    """全部 fetcher 返回空 → saved=0。"""
    ak = MagicMock()
    ak.stock_a_ttm_lyr.return_value = pd.DataFrame()
    ak.stock_a_all_pb.return_value = pd.DataFrame()
    ak.stock_ebs_lg.return_value = pd.DataFrame()
    db = MagicMock()
    with patch.object(market_valuation, "ak", ak):
        result = market_valuation.update_market_valuation(db)
    assert result["saved"] == 0
    assert result["全市场PE"] == 0
    assert result["全市场PB"] == 0
    assert result["股债利差"] == 0


def test_update_market_valuation_fetcher_exception():
    """fetcher 抛异常时主函数不崩。"""
    ak = MagicMock()
    ak.stock_a_ttm_lyr = MagicMock(side_effect=RuntimeError("PE error"), __name__="stock_a_ttm_lyr")
    ak.stock_a_all_pb.return_value = pd.DataFrame()
    ak.stock_ebs_lg.return_value = pd.DataFrame()
    db = MagicMock()
    with patch.object(market_valuation, "ak", ak):
        result = market_valuation.update_market_valuation(db)
    # SourceClient catches exception, _fetch_pe returns []
    assert result["全市场PE"] == 0
    assert result["saved"] == 0


def _failed_legu_response(error: str) -> SourceResponse:
    return SourceResponse(
        success=False,
        data=None,
        metadata=FetchMetadata(source_name="legu", error=error),
    )


@pytest.mark.parametrize(
    ("fetch_fn", "source_label"),
    [
        (market_valuation._fetch_pe, "全市场PE"),
        (market_valuation._fetch_pb, "全市场PB"),
        (market_valuation._fetch_ebs, "股债利差"),
    ],
)
def test_fetch_legu_failure_reason_logged(fetch_fn, source_label, caplog):
    """red-proof: 乐咕源失败时 ``resp.metadata.error`` 必须落日志。

    还原 tasks/market_valuation.py 的源提交会让本用例变红：旧实现把
    ``resp.success=False`` 静默吞成空列表，日志里没有任何 error 文本，
    任务只剩「zero rows without explanation」现场无法分诊
    （2026-09-28 乐咕全站 504 断服时实测如此）。
    """
    client = MagicMock()
    client.call.return_value = _failed_legu_response(
        "AttributeError: 'NoneType' object has no attribute 'attrs'"
    )
    with (
        patch.object(market_valuation, "get_default_client", return_value=client),
        caplog.at_level(logging.WARNING, logger=market_valuation.logger.name),
    ):
        assert fetch_fn() == []
    assert source_label in caplog.text
    assert "attrs" in caplog.text


def test_update_market_valuation_empty_logs_per_source_detail(caplog):
    """red-proof: 三源全空时日志须带分源明细，不能只报一句「无数据」。

    还原 tasks/market_valuation.py 的源提交会让本用例变红：旧日志是固定
    文案「⚠️ 大盘估值无数据」，不含「分源明细」字样与各源计数。
    """
    ak = MagicMock()
    ak.stock_a_ttm_lyr.return_value = pd.DataFrame()
    ak.stock_a_all_pb.return_value = pd.DataFrame()
    ak.stock_ebs_lg.return_value = pd.DataFrame()
    db = MagicMock()
    with (
        patch.object(market_valuation, "ak", ak),
        caplog.at_level(logging.WARNING),
    ):
        result = market_valuation.update_market_valuation(db)
    assert result["saved"] == 0
    assert "分源明细" in caplog.text
    assert "全市场PE" in caplog.text



# ===========================================================================
# concept_board
# ===========================================================================


def _mock_concept_spot_response() -> dict:
    """模拟东方财富 push2 概念板块行情 JSON 响应。"""
    return {
        "data": {
            "total": 2,
            "diff": [
                {"f3": 3.2, "f4": 100.5, "f12": "BK1001", "f14": "AI概念", "f104": 20, "f105": 3},
                {"f3": 2.1, "f4": 80.3, "f12": "BK1002", "f14": "芯片概念", "f104": 15, "f105": 5},
            ],
        }
    }


def _mock_empty_spot_response() -> dict:
    return {"data": {"total": 0, "diff": []}}


def test_update_concept_board_runs():
    """验证 update_concept_board 直调 eastmoney 接口并保存。"""
    db = MagicMock()
    db.save_concept_board_batch.return_value = 2
    with patch.object(concept_board, "get_default_client") as mock_client:
        mock_client.return_value.call.return_value = SourceResponse(
            success=True,
            data=[{"trade_date": "2026-01-01", "concept_code": "BK1001", "concept_name": "AI概念"}],
            metadata=FetchMetadata(source_name="eastmoney"),
        )
        result = concept_board.update_concept_board(db)
    assert result["board_saved"] == 2
    assert db.save_concept_board_batch.called


def test_update_concept_board_empty():
    """验证 eastmoney 接口返回空时优雅降级。"""
    db = MagicMock()
    with patch.object(concept_board, "get_default_client") as mock_client:
        mock_client.return_value.call.return_value = SourceResponse(
            success=True, data=[], metadata=FetchMetadata(source_name="eastmoney"),
        )
        result = concept_board.update_concept_board(db)
    assert result["board_saved"] == 0
    assert not db.save_concept_board_batch.called


def _mock_concept_member_push2_response() -> MagicMock:
    """模拟东方财富 push2 概念板块成分股 JSON 响应（f12=成员股票代码）。"""
    m = MagicMock()
    m.json.return_value = {
        "data": {
            "total": 2,
            "diff": [
                {"f12": "000001", "f14": "平安银行"},
                {"f12": "000002", "f14": "万科A"},
            ],
        }
    }
    m.raise_for_status = lambda: None
    return m


def test_update_concept_member_runs():
    """验证 update_concept_member 获取成分股映射并保存快照 + PIT。"""
    db = MagicMock()
    db.save_concept_member_batch.return_value = 4
    db.save_concept_member_history_batch.return_value = 4

    with patch.object(concept_board, "get_default_client") as mock_client:
        mock_client.return_value.call.side_effect = [
            # 第一次调用：概念板块列表
            SourceResponse(
                success=True,
                data=[
                    {"concept_code": "BK1001", "concept_name": "AI概念"},
                    {"concept_code": "BK1002", "concept_name": "芯片概念"},
                ],
                metadata=FetchMetadata(source_name="eastmoney"),
            ),
            # 后续调用：每个概念抓取成分股（一次成功，一次返回空列表）
            SourceResponse(
                success=True, data=["000001", "000002"],
                metadata=FetchMetadata(source_name="eastmoney"),
            ),
            SourceResponse(
                success=True, data=[],
                metadata=FetchMetadata(source_name="eastmoney"),
            ),
        ]
        result = concept_board.update_concept_member(db)

    assert result["member_saved"] == 4
    assert result["pit_saved"] == 4
    assert db.save_concept_member_batch.called
    assert db.save_concept_member_history_batch.called


def test_update_concept_member_empty_push2():
    """验证 push2 概念列表为空时优雅降级。"""
    db = MagicMock()

    with patch.object(concept_board, "get_default_client") as mock_client:
        mock_client.return_value.call.return_value = SourceResponse(
            success=True, data=[], metadata=FetchMetadata(source_name="eastmoney"),
        )
        result = concept_board.update_concept_member(db)

    assert result["member_saved"] == 0
    assert not db.save_concept_member_batch.called


def test_update_concept_member_network_fails():
    """验证成分股网络异常时返回 retained 而非 failed。"""
    db = MagicMock()
    with patch.object(concept_board, "_fetch_concept_members_em") as mock_fetch:
        mock_fetch.side_effect = Exception("network error")
        result = concept_board.update_concept_member(db)
    assert result["status"] == "retained"
    assert result["error_kind"] == "network"
    assert result["retained_old_data"] is True
    assert result["member_saved"] == 0
    assert result["pit_saved"] == 0
    assert not db.save_concept_member_batch.called






# ===========================================================================
# money_market — 边缘路径
# ===========================================================================


def test_money_market_empty_fetchers():
    """money_market: 各 fetcher 返回空 → 各项为 0。"""
    ak = MagicMock()
    ak.macro_china_shibor_all.return_value = pd.DataFrame()
    ak.repo_rate_query.return_value = pd.DataFrame()
    ak.macro_bank_china_interest_rate.return_value = pd.DataFrame()
    ak.macro_china_central_bank_balance.return_value = pd.DataFrame()
    db = MagicMock()
    db.save_money_market_batch.return_value = 0
    db.save_central_bank_balance_batch.return_value = 0
    with patch.object(money_market, "ak", ak):
        result = money_market.update_money_market(db)
    assert result["daily_saved"] == 0
    assert result["balance_saved"] == 0


def test_money_market_fetcher_exception():
    """money_market: fetcher 抛异常。"""
    ak = MagicMock()
    ak.macro_china_shibor_all.side_effect = RuntimeError("shibor fail")
    ak.repo_rate_query.return_value = pd.DataFrame()
    ak.macro_bank_china_interest_rate.return_value = pd.DataFrame()
    ak.macro_china_central_bank_balance.return_value = pd.DataFrame()
    db = MagicMock()
    with patch.object(money_market, "ak", ak):
        result = money_market.update_money_market(db)
    assert result["daily_saved"] == 0
    assert result["balance_saved"] == 0


def test_money_market_balance_exception():
    """money_market: 资产负债表抛异常。"""
    ak = _mock_ak_money_market()
    ak.macro_china_central_bank_balance.side_effect = RuntimeError("balance fail")
    db = MagicMock()
    db.save_money_market_batch.return_value = 2
    with patch.object(money_market, "ak", ak):
        result = money_market.update_money_market(db)
    assert result["daily_saved"] > 0
    assert result["balance_saved"] == 0


def test_money_market_to_float():
    """money_market._to_float: None/NaN/非数值。"""
    assert money_market._to_float(None) is None
    assert money_market._to_float(float("nan")) is None
    assert money_market._to_float("abc") is None
    assert money_market._to_float(42.5) == 42.5


def test_money_market_try_get_ak_df_exception():
    """money_market._try_get_ak_df: 异常处理。"""
    func = MagicMock(side_effect=RuntimeError("fail"), __name__="test_func")
    with patch.object(money_market, "ak", MagicMock()):
        assert money_market._try_get_ak_df(func) is None


def test_money_market_repo_date_none():
    """_fetch_repo_rates: date 列为 None 时跳过。"""
    ak = MagicMock()
    ak.repo_rate_query.return_value = pd.DataFrame({"date": [None], "FR001": [1.0]})
    with patch.object(money_market, "ak", ak):
        records = money_market._fetch_repo_rates()
    assert len(records) == 0


def test_money_market_pboc_date_none():
    """_fetch_pboc_policy_rate: 日期列为 None 时跳过。"""
    ak = MagicMock()
    ak.macro_bank_china_interest_rate.return_value = pd.DataFrame({"日期": [None], "今值": [3.0]})
    with patch.object(money_market, "ak", ak):
        records = money_market._fetch_pboc_policy_rate()
    assert len(records) == 0


# ===========================================================================
# sector_derivatives — 边缘路径
# ===========================================================================


def test_sector_derivatives_empty_board():
    """sector_derivatives: board 列表空。"""
    ak = MagicMock()
    ak.stock_board_industry_name_em.return_value = pd.DataFrame()
    ak.stock_industry_pe_ratio_cninfo.return_value = pd.DataFrame()
    with patch.object(sector_derivatives, "ak", ak):
        result = sector_derivatives.update_sector_derivatives(MagicMock())
    assert result["sector_daily"] == 0
    assert result["sector_valuation"] == 0


def test_sector_derivatives_no_matching_sectors():
    """sector_derivatives: 无匹配的主要行业板块。"""
    ak = MagicMock()
    ak.stock_board_industry_name_em.return_value = pd.DataFrame({"板块名称": ["罕见行业", "其他"]})
    with patch.object(sector_derivatives, "ak", ak):
        result = sector_derivatives._fetch_sector_daily()
    assert result == []


def test_sector_derivatives_hist_exception():
    """sector_derivatives: 板块历史数据抛异常。"""
    ak = MagicMock()
    ak.stock_board_industry_name_em.return_value = pd.DataFrame({"板块名称": ["半导体"]})
    ak.stock_board_industry_hist_em.side_effect = RuntimeError("hist fail")
    with patch.object(sector_derivatives, "ak", ak), patch.object(sector_derivatives, "_retry", return_value=None):
        result = sector_derivatives._fetch_sector_daily()
    assert result == []


def test_sector_derivatives_valuation_empty():
    """sector_derivatives: 估值返回空。"""
    ak = MagicMock()
    ak.stock_industry_pe_ratio_cninfo.return_value = pd.DataFrame()
    with (
        patch.object(sector_derivatives, "ak", ak),
        patch.object(sector_derivatives, "get_expected_latest_trading_day", return_value="2026-07-19"),
    ):
        result = sector_derivatives._fetch_sector_valuation()
    assert result == []


def test_sector_derivatives_futures_ak_none():
    """sector_derivatives: ak=None 时 futures 返回空。"""
    with patch.object(sector_derivatives, "ak", None):
        assert sector_derivatives._fetch_index_futures_basis() == []


def test_sector_derivatives_retry_all_fail():
    """sector_derivatives._retry: 全部重试失败返回 None。"""
    fn = MagicMock(side_effect=RuntimeError("fail"))
    result = sector_derivatives._retry(fn, tries=2, base_delay=0.01, label="test")
    assert result is None
    assert fn.call_count == 2


def test_sector_derivatives_retry_success():
    """sector_derivatives._retry: 成功返回结果。"""
    fn = MagicMock(return_value="ok")
    result = sector_derivatives._retry(fn, tries=3, label="test")
    assert result == "ok"
    assert fn.call_count == 1


def test_sector_derivatives_update_exception():
    """sector_derivatives: update 中 fetcher 抛异常。

    注意: 异常在 _fetch_sector_daily 内部已被捕获，外层 results[name] = 0。
    """
    ak = MagicMock()
    ak.stock_board_industry_name_em.side_effect = RuntimeError("board fail")
    db = MagicMock()
    with patch.object(sector_derivatives, "ak", ak):
        result = sector_derivatives.update_sector_derivatives(db)
    assert result["sector_daily"] == 0


def test_sector_derivatives_to_float():
    """sector_derivatives._to_float: 边缘情况。"""
    assert sector_derivatives._to_float(None) is None
    assert sector_derivatives._to_float(float("nan")) is None
    assert sector_derivatives._to_float("abc") is None
    assert sector_derivatives._to_float(42.5) == 42.5


# ===========================================================================
# financials — 边缘路径
# ===========================================================================


def test_financials_shareholder_count_ak_none():
    db = MagicMock()
    with patch.object(financials, "ak", None):
        result = financials.update_shareholder_count(db)
    assert result["saved"] == 0
    assert "error" in result


def test_financials_shareholder_count_empty():
    """报告期无股东户数数据 → skipped（合法零行）。"""
    ak = MagicMock()
    ak.stock_hold_num_cninfo.return_value = pd.DataFrame()
    db = MagicMock()
    with patch.object(financials, "ak", ak):
        result = financials.update_shareholder_count(db)
    assert result.get("skipped") is True


def test_financials_shareholder_count_exception():
    ak = MagicMock()
    ak.stock_hold_num_cninfo.side_effect = RuntimeError("ak fail")
    db = MagicMock()
    with patch.object(financials, "ak", ak):
        result = financials.update_shareholder_count(db)
    assert result["saved"] == 0
    assert "error" in result


def test_financials_quarterly_ak_none():
    db = MagicMock()
    loader = MagicMock()
    with patch.object(financials, "ak", None):
        result = financials.update_quarterly_financials(db, loader, symbols=["000001"])
    assert result["saved"] == 0
    assert "error" in result


def test_financials_quarterly_empty_stock_list():
    ak = MagicMock()
    db = MagicMock()
    db.get_stock_list.return_value = pd.DataFrame()
    loader = MagicMock()
    # symbols=[] 走逐股路径（全市场 None 模式已委托 financial_history）
    with patch.object(financials, "ak", ak):
        result = financials.update_quarterly_financials(db, loader, symbols=[])
    assert result["saved"] == 0
    assert result["total"] == 0


# ===========================================================================
# china_macro — 边缘路径
# ===========================================================================


def test_china_macro_parse_quarter_edge_cases():
    """_parse_quarter: 各种中文和数字格式。"""
    assert china_macro._parse_quarter("") is None
    assert china_macro._parse_quarter("2024年第一季度") == "2024-Q1"
    assert china_macro._parse_quarter("2024年第四季度") == "2024-Q4"
    assert china_macro._parse_quarter("2024-Q1") == "2024-Q1"  # 数字格式


def test_china_macro_empty_fetchers():
    """china_macro: 所有 fetcher 返回空。"""
    ak = MagicMock()
    for attr in [
        "macro_china_cpi",
        "macro_china_ppi",
        "macro_china_pmi",
        "macro_china_cx_pmi_yearly",
        "macro_china_money_supply",
        "macro_china_new_financial_credit",
        "macro_china_consumer_goods_retail",
        "macro_china_gdzctz",
        "macro_china_hgjck",
        "macro_china_industrial_production_yoy",
        "macro_china_society_electricity",
        "macro_china_qyspjg",
        "macro_china_xfzxx",
        "macro_china_lpr",
        "macro_china_gdp",
    ]:
        getattr(ak, attr).return_value = pd.DataFrame()
    db = MagicMock()
    db.save_macro_monthly_batch.return_value = 0
    db.save_macro_quarterly_batch.return_value = 0
    with patch.object(china_macro, "ak", ak):
        result = china_macro.update_china_macro(db)
    assert result["monthly_saved"] == 0
    assert result["quarterly_saved"] == 0


def test_china_macro_fetcher_exception():
    """china_macro: 某个 fetcher 抛异常。

    注意: 异常在 _fetch_cpi 内部已被捕获，外层 results[name] = 0。
    """
    ak = MagicMock()
    ak.macro_china_cpi.side_effect = RuntimeError("cpi fail")
    for attr in [
        "macro_china_ppi",
        "macro_china_pmi",
        "macro_china_cx_pmi_yearly",
        "macro_china_money_supply",
        "macro_china_new_financial_credit",
        "macro_china_consumer_goods_retail",
        "macro_china_gdzctz",
        "macro_china_hgjck",
        "macro_china_industrial_production_yoy",
        "macro_china_society_electricity",
        "macro_china_qyspjg",
        "macro_china_xfzxx",
        "macro_china_lpr",
        "macro_china_gdp",
    ]:
        getattr(ak, attr).return_value = pd.DataFrame()
    db = MagicMock()
    with patch.object(china_macro, "ak", ak):
        result = china_macro.update_china_macro(db)
    assert result["CPI"] == 0
    assert result["monthly_saved"] == 0


def test_china_macro_multiple_fetchers_exception():
    """china_macro: 多个 fetcher 抛异常。"""
    ak = MagicMock()
    for attr in ["macro_china_ppi", "macro_china_lpr"]:
        getattr(ak, attr).side_effect = RuntimeError(f"{attr} fail")
    for attr in [
        "macro_china_cpi",
        "macro_china_pmi",
        "macro_china_cx_pmi_yearly",
        "macro_china_money_supply",
        "macro_china_new_financial_credit",
        "macro_china_consumer_goods_retail",
        "macro_china_gdzctz",
        "macro_china_hgjck",
        "macro_china_industrial_production_yoy",
        "macro_china_society_electricity",
        "macro_china_qyspjg",
        "macro_china_xfzxx",
        "macro_china_gdp",
    ]:
        getattr(ak, attr).return_value = pd.DataFrame()
    db = MagicMock()
    with patch.object(china_macro, "ak", ak):
        result = china_macro.update_china_macro(db)
    assert result["PPI"] == 0
    assert result["LPR"] == 0
    assert result["monthly_saved"] == 0


def test_china_macro_parse_month_col_no_month():
    """_parse_month_col: 无月份列时不处理。"""
    df = pd.DataFrame({"value": [1.0]})
    result = china_macro._parse_month_col(df)
    assert list(result.columns) == ["value"]


def test_china_macro_gdp_no_quarter():
    """_fetch_gdp: 无季度列时返回空。"""
    ak = MagicMock()
    ak.macro_china_gdp.return_value = pd.DataFrame({"value": [1.0]})
    with patch.object(china_macro, "ak", ak):
        records = china_macro._fetch_gdp()
    assert records == []


def test_china_macro_lpr_no_date():
    """_fetch_lpr: date 为空时跳过。"""
    ak = MagicMock()
    ak.macro_china_lpr.return_value = pd.DataFrame({"TRADE_DATE": [None], "LPR1Y": [3.0]})
    with patch.object(china_macro, "ak", ak):
        records = china_macro._fetch_lpr()
    assert records == []


def test_china_macro_industrial_production_no_date():
    """_fetch_industrial_production: date 为空时跳过。"""
    ak = MagicMock()
    ak.macro_china_industrial_production_yoy.return_value = pd.DataFrame({"日期": [None], "今值": [5.0]})
    with patch.object(china_macro, "ak", ak):
        records = china_macro._fetch_industrial_production()
    assert records == []


def test_china_macro_caixin_pmi_no_date():
    """_fetch_caixin_pmi: date 为空时跳过。"""
    ak = MagicMock()
    ak.macro_china_cx_pmi_yearly.return_value = pd.DataFrame({"日期": [None], "今值": [50.0]})
    with patch.object(china_macro, "ak", ak):
        records = china_macro._fetch_caixin_pmi()
    assert records == []


def test_china_macro_gdp_exception():
    """_fetch_gdp: 异常处理。"""
    ak = MagicMock()
    ak.macro_china_gdp.side_effect = RuntimeError("gdp fail")
    with patch.object(china_macro, "ak", ak):
        records = china_macro._fetch_gdp()
    assert records == []


# ===========================================================================
# financials — 深入边缘路径
# ===========================================================================


def test_financials_quarterly_with_existing_filter():
    """quarterly: 过滤已有数据的股票（--symbols 逐股路径）。"""
    ak = MagicMock()
    db = MagicMock()
    db.get_distinct_codes.return_value = {"000001.SZ"}  # 已有数据
    loader = MagicMock()
    with patch.object(financials, "ak", ak):
        result = financials.update_quarterly_financials(
            db, loader, symbols=["000001.SZ", "000002.SZ", "000003.SZ"]
        )
    assert result["total"] == 3  # code-level existence filtering removed, all 3 processed


def test_financials_industry_all_classified(tmp_path: Path):
    """update_industry: 所有股票已有行业分类 → 提前返回。"""
    import sqlite3

    db_path = str(tmp_path / "industry.db")
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE stock_list (code TEXT, market TEXT, industry TEXT)")
    conn.execute("INSERT INTO stock_list VALUES ('000001', 'sz', '银行')")
    conn.commit()
    conn.close()
    db = MagicMock()
    db.db_path = db_path
    with (
        patch.object(financials, "ak", MagicMock()),
        patch("tasks.financials.logger"),
        patch("core.lock.TaskLock.acquire", return_value=True),
        patch("core.lock.TaskLock.release"),
    ):
        result = financials.update_industry(db)
    assert result["total"] == 0


def test_concept_to_float_none():
    assert concept_board._to_float(None) is None


def test_concept_to_float_invalid():
    assert concept_board._to_float("bad") is None


def test_concept_to_int_none():
    assert concept_board._to_int(None) is None


def test_concept_to_int_invalid():
    assert concept_board._to_int("bad") is None


def test_update_concept_board_request_fails():
    """验证 push2 请求异常时返回 retained（旧数据保留）。"""
    db = MagicMock()
    with patch.object(concept_board, "get_default_client") as mock_client:
        mock_client.return_value.call.return_value = SourceResponse(
            success=False, data=None, metadata=FetchMetadata(source_name="eastmoney", error="network error"),
        )
        result = concept_board.update_concept_board(db)
    assert result["status"] == "retained"
    assert result["error_kind"] == "network"
    assert result["retained_old_data"] is True
    assert result["board_saved"] == 0
    assert not db.save_concept_board_batch.called


def test_update_concept_board_contract_failure_still_failed():
    """验证合约校验失败时仍返回 failed/data_quality（不退化为 retained）。"""
    db = MagicMock()
    with patch.object(concept_board, "get_default_client") as mock_client:
        mock_client.return_value.call.return_value = SourceResponse(
            success=True,
            data=[{"trade_date": "2026-01-01", "concept_code": "BK1001", "concept_name": "AI概念"}],
            metadata=FetchMetadata(source_name="eastmoney"),
        )
        with patch.object(concept_board, "validate_records") as mock_validate:
            mock_validate.return_value = ([], ["contract violation"])
            result = concept_board.update_concept_board(db)
    assert result["status"] == "failed"
    assert result["error_kind"] == "data_quality"
    assert not db.save_concept_board_batch.called
    """验证保存接口异常时优雅降级。"""
    db = MagicMock()
    db.save_concept_board_batch.side_effect = Exception("save error")
    with patch.object(concept_board, "get_default_client") as mock_client:
        mock_client.return_value.call.return_value = SourceResponse(
            success=True,
            data=[{"trade_date": "2026-01-01", "concept_code": "BK1001", "concept_name": "AI概念"}],
            metadata=FetchMetadata(source_name="eastmoney"),
        )
        result = concept_board.update_concept_board(db)
    assert result["board_saved"] == 0


def test_update_concept_member_save_raises():
    """验证保存成分股映射异常时优雅降级。"""
    db = MagicMock()
    db.save_concept_member_batch.side_effect = Exception("save error")

    with patch.object(concept_board, "get_default_client") as mock_client:
        mock_client.return_value.call.side_effect = [
            SourceResponse(
                success=True,
                data=[{"concept_code": "BK1001", "concept_name": "AI概念"}],
                metadata=FetchMetadata(source_name="eastmoney"),
            ),
            SourceResponse(
                success=True, data=["000001"],
                metadata=FetchMetadata(source_name="eastmoney"),
            ),
        ]
        result = concept_board.update_concept_member(db)
    assert result["member_saved"] == 0


def test_fetch_concept_members_list_fetch_fails():
    """验证概念列表获取失败时上抛异常（源故障不能被静默吞掉）。"""
    with patch.object(concept_board, "get_default_client") as mock_client:
        mock_client.return_value.call.return_value = SourceResponse(
            success=False, data=None,
            metadata=FetchMetadata(source_name="eastmoney", error="network error"),
        )
        with pytest.raises(RuntimeError, match="network error"):
            concept_board._fetch_concept_members_em()


def test_update_concept_member_source_failure_returns_retained():
    """源故障时 update_concept_member 必须返回 retained（保留旧数据），而非 no_data。"""
    db = MagicMock()

    with patch.object(concept_board, "get_default_client") as mock_client:
        mock_client.return_value.call.return_value = SourceResponse(
            success=False, data=None,
            metadata=FetchMetadata(source_name="eastmoney", error="network error"),
        )
        result = concept_board.update_concept_member(db)

    assert result["status"] == "retained"
    assert result["error_kind"] == "network"
    assert result["retained_old_data"] is True
    assert result["saved"] == 0
    assert not db.save_concept_member_batch.called
    assert not db.save_concept_member_history_batch.called


def test_fetch_em_spot_empty_code_skipped():
    """验证概念代码为空时跳过该条。"""
    db = MagicMock()
    db.save_concept_board_batch.return_value = 1

    def _call_side_effect(source_name, operation, *args, **kwargs):
        try:
            data = operation(*args, **kwargs)
            return SourceResponse(success=True, data=data, metadata=FetchMetadata(source_name=source_name))
        except Exception as e:
            return SourceResponse(success=False, data=None, metadata=FetchMetadata(source_name=source_name, error=str(e)))

    with patch.object(concept_board, "get_default_client") as mock_client:
        mock_session = MagicMock()
        mock_session.get.return_value.json.return_value = {
            "data": {
                "total": 2,
                "diff": [
                    {"f3": None, "f4": None, "f12": "", "f14": "", "f104": None, "f105": None},
                    {"f3": 2.0, "f4": 50.0, "f12": "BK2001", "f14": "新能源", "f104": 10, "f105": 2},
                ],
            }
        }
        mock_session.get.return_value.raise_for_status = lambda: None
        mock_client.return_value.get_session.return_value = mock_session
        mock_client.return_value.call.side_effect = _call_side_effect
        result = concept_board.update_concept_board(db)
    assert result["board_saved"] == 1


def test_fetch_concept_members_member_fetch_fails():
    """验证单个概念成分股请求异常时优雅跳过该板块。"""
    with patch.object(concept_board, "get_default_client") as mock_client:
        mock_client.return_value.call.side_effect = [
            SourceResponse(
                success=True,
                data=[{"concept_code": "BK1001", "concept_name": "AI概念"}],
                metadata=FetchMetadata(source_name="eastmoney"),
            ),
            SourceResponse(
                success=False, data=None,
                metadata=FetchMetadata(source_name="eastmoney", error="network error"),
            ),
        ]
        members = concept_board._fetch_concept_members_em()

    assert len(members) == 0


def test_fetch_concept_members_empty_member_list():
    """验证成分股请求返回空列表时跳过。"""
    db = MagicMock()
    db.save_concept_member_batch.return_value = 0

    with patch.object(concept_board, "get_default_client") as mock_client:
        mock_client.return_value.call.side_effect = [
            SourceResponse(
                success=True,
                data=[{"concept_code": "BK1001", "concept_name": "AI概念"}],
                metadata=FetchMetadata(source_name="eastmoney"),
            ),
            SourceResponse(
                success=True, data=[],
                metadata=FetchMetadata(source_name="eastmoney"),
            ),
        ]
        result = concept_board.update_concept_member(db)

    assert result["member_saved"] == 0


def test_fetch_concept_members_push2_fails():
    """验证 push2 概念列表请求异常时上抛（源故障不能被静默吞掉）。"""
    with patch.object(concept_board, "get_default_client") as mock_client:
        mock_client.return_value.call.side_effect = Exception("push2 error")
        with pytest.raises(Exception, match="push2 error"):
            concept_board._fetch_concept_members_em()


def test_fetch_concept_members_empty_code_skip():
    """验证概念板块代码为空时跳过。"""
    with patch.object(concept_board, "get_default_client") as mock_client:
        mock_client.return_value.call.side_effect = [
            SourceResponse(
                success=True,
                data=[
                    {"concept_code": "", "concept_name": ""},
                    {"concept_code": "BK3001", "concept_name": "有效概念"},
                ],
                metadata=FetchMetadata(source_name="eastmoney"),
            ),
            SourceResponse(
                success=True, data=["000001"],
                metadata=FetchMetadata(source_name="eastmoney"),
            ),
        ]
        members = concept_board._fetch_concept_members_em()
    assert len(members) == 1


def test_fetch_em_spot_none_fields():
    """验证行情数据中数值字段为 None 时仍然创建记录。"""
    db = MagicMock()
    db.save_concept_board_batch.return_value = 1

    def _call_side_effect(source_name, operation, *args, **kwargs):
        try:
            data = operation(*args, **kwargs)
            return SourceResponse(success=True, data=data, metadata=FetchMetadata(source_name=source_name))
        except Exception as e:
            return SourceResponse(success=False, data=None, metadata=FetchMetadata(source_name=source_name, error=str(e)))

    with patch.object(concept_board, "get_default_client") as mock_client:
        mock_session = MagicMock()
        mock_session.get.return_value.json.return_value = {
            "data": {
                "total": 1,
                "diff": [
                    {"f3": None, "f4": None, "f12": "BK4001", "f14": "测试概念", "f104": None, "f105": None},
                ],
            }
        }
        mock_session.get.return_value.raise_for_status = lambda: None
        mock_client.return_value.get_session.return_value = mock_session
        mock_client.return_value.call.side_effect = _call_side_effect
        result = concept_board.update_concept_board(db)
    assert result["board_saved"] == 1


def test_fetch_em_spot_pagination():
    """验证分页逻辑：items >= 100 时自动请求下一页。"""
    db = MagicMock()
    db.save_concept_board_batch.return_value = 150

    page_items = []
    for i in range(100):
        page_items.append(
            {
                "f3": 1.0,
                "f4": 10.0,
                "f12": f"BK{i:04d}",
                "f14": f"概念{i}",
                "f104": 5,
                "f105": 2,
            }
        )
    # Second page: few items to signal end
    next_items = [
        {"f3": 2.0, "f4": 20.0, "f12": "BK2000", "f14": "末页概念", "f104": 3, "f105": 1},
    ]

    with patch.object(concept_board, "get_default_client") as mock_client:
        mock_session = MagicMock()
        mock_session.get.return_value.json.side_effect = [
            {"data": {"total": 101, "diff": page_items}},
            {"data": {"total": 101, "diff": next_items}},
        ]
        mock_session.get.return_value.raise_for_status = lambda: None
        mock_client.return_value.get_session.return_value = mock_session
        # Make call() invoke the callback so _fetch_em_spot's pagination runs
        def _call_side_effect(source, operation, *args, **kwargs):
            data = operation(*args, **kwargs)
            return SourceResponse(success=True, data=data, metadata=FetchMetadata(source_name=source))
        mock_client.return_value.call.side_effect = _call_side_effect
        result = concept_board.update_concept_board(db)
    assert mock_session.get.call_count == 2
    assert result["board_saved"] == 150


# ===========================================================================
# 收盘刷新 helper（Task 7）：valuation_chain 派生任务
# ===========================================================================

_REFRESH_TARGET = "2026-07-27"


def _create_valuation_refresh_db(db_path: str) -> None:
    """建 fundamentals + stock_list 最小表结构（临时 SQLite）。"""
    conn = sqlite3.connect(db_path)
    conn.execute(
        """CREATE TABLE fundamentals (
            ts_code TEXT, trade_date TEXT, pe_ttm REAL, pb REAL, ps_ttm REAL,
            dividend_yield REAL, roe REAL, revenue_growth REAL,
            profit_growth REAL, market_cap REAL)"""
    )
    conn.execute("CREATE TABLE stock_list (code TEXT, industry TEXT)")
    conn.commit()
    conn.close()


def _seed_refresh_fundamental(
    db_path: str,
    code: str,
    trade_date: str = _REFRESH_TARGET,
    pe: float = 10.0,
    dividend_yield: float | None = 1.5,
) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO fundamentals (ts_code, trade_date, pe_ttm, pb, ps_ttm,"
        " dividend_yield, roe, revenue_growth, profit_growth, market_cap)"
        " VALUES (?, ?, ?, 1.2, 2.4, ?, 0.1, 0.2, 0.3, 1e9)",
        (code, trade_date, pe, dividend_yield),
    )
    conn.commit()
    conn.close()


class TestFetchHistoricalValuationRowsForRefresh:
    """fetch_historical_valuation_rows_for_refresh：只读目标日分区，不写库。"""

    def test_reads_only_target_partition(self, tmp_path: Path):
        from tasks.valuation_chain import fetch_historical_valuation_rows_for_refresh

        db_path = str(tmp_path / "refresh.db")
        _create_valuation_refresh_db(db_path)
        _seed_refresh_fundamental(db_path, "000001", pe=10.0)
        _seed_refresh_fundamental(db_path, "600000", pe=20.0)
        _seed_refresh_fundamental(db_path, "000001", trade_date="2026-07-24", pe=99.0)

        rows = fetch_historical_valuation_rows_for_refresh(db_path, _REFRESH_TARGET)

        assert {row["ts_code"] for row in rows} == {"000001", "600000"}
        assert all(row["trade_date"] == _REFRESH_TARGET for row in rows)
        assert all(
            set(row) == {"ts_code", "trade_date", "pe_ttm", "pb", "ps_ttm", "dividend_yield"}
            for row in rows
        )
        by_code = {row["ts_code"]: row for row in rows}
        assert by_code["000001"]["pe_ttm"] == 10.0
        assert by_code["000001"]["dividend_yield"] == 1.5

    def test_deduplicates_codes(self, tmp_path: Path):
        from tasks.valuation_chain import fetch_historical_valuation_rows_for_refresh

        db_path = str(tmp_path / "refresh.db")
        _create_valuation_refresh_db(db_path)
        _seed_refresh_fundamental(db_path, "000001", pe=10.0)
        _seed_refresh_fundamental(db_path, "000001", pe=11.0)

        rows = fetch_historical_valuation_rows_for_refresh(db_path, _REFRESH_TARGET)

        assert len(rows) == 1

    def test_empty_partition_returns_empty(self, tmp_path: Path):
        from tasks.valuation_chain import fetch_historical_valuation_rows_for_refresh

        db_path = str(tmp_path / "refresh.db")
        _create_valuation_refresh_db(db_path)

        assert fetch_historical_valuation_rows_for_refresh(db_path, _REFRESH_TARGET) == []


class TestComputeSectorIndustryRowsForRefresh:
    """compute_sector_industry_rows_for_refresh：只聚合目标日分区，不写库。"""

    def test_aggregates_target_partition(self, tmp_path: Path):
        from tasks.valuation_chain import compute_sector_industry_rows_for_refresh

        db_path = str(tmp_path / "refresh.db")
        _create_valuation_refresh_db(db_path)
        _seed_refresh_fundamental(db_path, "000001", pe=10.0)
        _seed_refresh_fundamental(db_path, "600000", pe=20.0)
        _seed_refresh_fundamental(db_path, "600519", pe=30.0)
        # 历史分区不得参与聚合
        _seed_refresh_fundamental(db_path, "000001", trade_date="2026-07-24", pe=999.0)
        conn = sqlite3.connect(db_path)
        conn.executemany(
            "INSERT INTO stock_list (code, industry) VALUES (?, ?)",
            [("000001", "银行"), ("600000", "银行"), ("600519", "白酒")],
        )
        conn.commit()
        conn.close()

        rows = compute_sector_industry_rows_for_refresh(db_path, _REFRESH_TARGET)

        by_industry = {row["industry_name"]: row for row in rows}
        assert set(by_industry) == {"银行", "白酒"}
        assert by_industry["银行"]["avg_pe"] == pytest.approx(15.0)
        assert by_industry["白酒"]["avg_pe"] == pytest.approx(30.0)
        assert by_industry["银行"]["total_market_cap"] == pytest.approx(2e9)
        assert all(row["trade_date"] == _REFRESH_TARGET for row in rows)
        # 刷新模式不做 sector_fund_flow 模糊映射，排名置空
        assert all(row["fund_inflow_rank"] is None for row in rows)
        assert all(row["data_source"] == "derived" for row in rows)

    def test_unknown_industry_bucket(self, tmp_path: Path):
        from tasks.valuation_chain import compute_sector_industry_rows_for_refresh

        db_path = str(tmp_path / "refresh.db")
        _create_valuation_refresh_db(db_path)
        _seed_refresh_fundamental(db_path, "300001", pe=40.0)

        rows = compute_sector_industry_rows_for_refresh(db_path, _REFRESH_TARGET)

        assert [row["industry_name"] for row in rows] == ["未知行业"]

    def test_empty_partition_returns_empty(self, tmp_path: Path):
        from tasks.valuation_chain import compute_sector_industry_rows_for_refresh

        db_path = str(tmp_path / "refresh.db")
        _create_valuation_refresh_db(db_path)

        assert compute_sector_industry_rows_for_refresh(db_path, _REFRESH_TARGET) == []


# ===========================================================================
# 收盘刷新 helper（Task 9）：只抓取/归一化，不写库，源异常直接上抛
# ===========================================================================

_T9_TARGET = "2026-07-27"


def test_fetch_south_flow_records_full_history_shape():
    """南向资金全历史归一化；market 空回退"南向"；异常上抛。"""
    fake_ak = MagicMock()
    fake_ak.stock_hsgt_hist_em.return_value = pd.DataFrame(
        [
            {"日期": "2026-07-27", "板块": "港股通(沪)", "当日成交净买额": 10.0,
             "买入成交额": 60.0, "卖出成交额": 50.0, "历史累计净买额": 1000.0},
            {"日期": "2026-07-24", "板块": "", "当日成交净买额": 5.0,
             "买入成交额": 30.0, "卖出成交额": 25.0, "历史累计净买额": 990.0},
        ]
    )
    with patch.object(finance_flow, "ak", fake_ak):
        records = finance_flow.fetch_south_flow_records()

    fake_ak.stock_hsgt_hist_em.assert_called_once_with(symbol="南向资金")
    assert len(records) == 2
    assert records[0]["trade_date"] == "2026-07-27"
    assert records[0]["market"] == "港股通(沪)"
    assert records[1]["market"] == "南向"
    assert records[0]["net_buy_amount"] == 10.0

    fake_ak.stock_hsgt_hist_em.side_effect = ConnectionError("em down")
    with patch.object(finance_flow, "ak", fake_ak), pytest.raises(ConnectionError):
        finance_flow.fetch_south_flow_records()


def test_fetch_ah_premium_records_stamps_target_date():
    """A/H 溢价即时快照 → trade_date 用调用方指定日期；异常上抛。"""
    fake_ak = MagicMock()
    fake_ak.stock_zh_ah_spot_em.return_value = pd.DataFrame(
        [{"代码": "601318", "名称": "中国平安", "H股代码": "02318",
          "最新价": 50.0, "最新价-HKD": 40.0, "溢价率": 25.0}]
    )
    with patch.object(finance_flow, "ak", fake_ak):
        records = finance_flow.fetch_ah_premium_records(_T9_TARGET)

    assert len(records) == 1
    rec = records[0]
    assert rec["trade_date"] == _T9_TARGET
    assert rec["ts_code"] == "601318"
    assert rec["h_code"] == "02318"
    assert rec["premium"] == 25.0

    fake_ak.stock_zh_ah_spot_em.side_effect = ConnectionError("em down")
    with patch.object(finance_flow, "ak", fake_ak), pytest.raises(ConnectionError):
        finance_flow.fetch_ah_premium_records(_T9_TARGET)


def test_fetch_etf_daily_records_single_target_day():
    """单只 ETF 只抓目标日；命中返回单行 legacy 形状；异常上抛。"""
    fake_ak = MagicMock()
    fake_ak.fund_etf_hist_em.return_value = pd.DataFrame(
        [{"日期": "2026-07-27", "开盘": 1.0, "最高": 1.2, "最低": 0.9,
          "收盘": 1.1, "成交量": 100.0, "成交额": 110.0}]
    )
    with patch.object(finance_flow, "ak", fake_ak):
        records = finance_flow.fetch_etf_daily_records("510050", "上证50ETF", _T9_TARGET)

    fake_ak.fund_etf_hist_em.assert_called_once_with(
        symbol="510050", period="daily",
        start_date="20260727", end_date="20260727", adjust="qfq",
    )
    assert len(records) == 1
    rec = records[0]
    assert rec["ts_code"] == "510050"
    assert rec["name"] == "上证50ETF"
    assert rec["trade_date"] == _T9_TARGET
    assert rec["close"] == 1.1

    fake_ak.fund_etf_hist_em.return_value = pd.DataFrame()
    with patch.object(finance_flow, "ak", fake_ak):
        assert finance_flow.fetch_etf_daily_records("510050", "上证50ETF", _T9_TARGET) == []

    fake_ak.fund_etf_hist_em.side_effect = ConnectionError("em down")
    with patch.object(finance_flow, "ak", fake_ak), pytest.raises(ConnectionError):
        finance_flow.fetch_etf_daily_records("510050", "上证50ETF", _T9_TARGET)


def test_fetch_index_daily_records_all_indices_full_history():
    """五大指数全历史归一化（供适配器挑选回看分区）；任一指数异常上抛。"""
    fake_ak = MagicMock()
    fake_ak.stock_zh_index_daily_tx.return_value = pd.DataFrame(
        [
            {"date": "2026-07-24", "open": 1.0, "high": 2.0, "low": 0.5,
             "close": 1.5, "volume": 100.0},
            {"date": "2026-07-27", "open": 1.5, "high": 2.5, "low": 1.0,
             "close": 2.0, "volume": 200.0},
        ]
    )
    with patch.object(index_chain, "ak", fake_ak):
        records = index_chain.fetch_index_daily_records()

    assert fake_ak.stock_zh_index_daily_tx.call_count == 5
    assert len(records) == 10  # 5 指数 × 2 日
    codes = {r["index_code"] for r in records}
    assert codes == {"sh000001", "sz399001", "sz399006", "sh000688", "sh000300"}
    dates = {r["trade_date"] for r in records}
    assert dates == {"2026-07-24", "2026-07-27"}
    assert all(r["close"] is not None for r in records)

    fake_ak.stock_zh_index_daily_tx.side_effect = ConnectionError("tx down")
    with patch.object(index_chain, "ak", fake_ak), pytest.raises(ConnectionError):
        index_chain.fetch_index_daily_records()


def test_fetch_market_valuation_records_merges_by_date():
    """PE/PB/股债利差按日期合并；带 data_source 与 data_date；异常上抛。"""
    fake_ak = MagicMock()
    fake_ak.stock_a_ttm_lyr.return_value = pd.DataFrame(
        [{"date": "2026-07-27", "middlePETTM": 30.0,
          "quantileInAllHistoryMiddlePeTtm": 0.6, "middlePELYR": 32.0}]
    )
    fake_ak.stock_a_all_pb.return_value = pd.DataFrame(
        [{"date": "2026-07-27", "middlePB": 2.5,
          "quantileInAllHistoryMiddlePB": 0.4}]
    )
    fake_ak.stock_ebs_lg.return_value = pd.DataFrame(
        [{"日期": "2026-07-27", "沪深300指数": 4000.0,
          "股债利差": 0.05, "股债利差均线": 0.04}]
    )
    with patch.object(market_valuation, "ak", fake_ak):
        records = market_valuation.fetch_market_valuation_records(_T9_TARGET)

    assert len(records) == 1
    rec = records[0]
    assert rec["date"] == "2026-07-27"
    assert rec["pe_median"] == 30.0
    assert rec["pb_median"] == 2.5
    assert rec["equity_bond_spread"] == 0.05
    assert rec["data_date"] == _T9_TARGET
    assert rec["data_source"] == "legu"

    fake_ak.stock_a_ttm_lyr.side_effect = ConnectionError("legu down")
    with patch.object(market_valuation, "ak", fake_ak), pytest.raises(ConnectionError):
        market_valuation.fetch_market_valuation_records(_T9_TARGET)


def test_fetch_cb_quotation_records_stamps_updated_at():
    """可转债行情快照 → 附 updated_at；空快照返回 []；异常上抛。"""
    fake_ak = MagicMock()
    fake_ak.bond_cb_jsl.return_value = pd.DataFrame(
        [{"代码": "113001", "转债名称": "测试转债", "现价": 110.0,
          "转股溢价率": 5.0, "双低": 115.0, "到期时间": "2030-01-01"}]
    )
    stamp = "2026-07-27T16:30:00+08:00"
    with patch.object(convertible_bond, "ak", fake_ak):
        records = convertible_bond.fetch_cb_quotation_records(stamp)

    assert len(records) == 1
    rec = records[0]
    assert rec["ts_code"] == "113001"
    assert rec["price"] == 110.0
    assert rec["updated_at"] == stamp

    fake_ak.bond_cb_jsl.return_value = pd.DataFrame()
    with patch.object(convertible_bond, "ak", fake_ak):
        assert convertible_bond.fetch_cb_quotation_records(stamp) == []

    fake_ak.bond_cb_jsl.side_effect = ConnectionError("jsl down")
    with patch.object(convertible_bond, "ak", fake_ak), pytest.raises(ConnectionError):
        convertible_bond.fetch_cb_quotation_records(stamp)


def test_fetch_cb_redeem_records_stamps_updated_at():
    """可转债强赎快照 → 附 updated_at；异常上抛。"""
    fake_ak = MagicMock()
    fake_ak.bond_cb_redeem_jsl.return_value = pd.DataFrame(
        [{"代码": "113001", "名称": "测试转债", "强赎状态": "已公告强赎",
          "强赎价": 100.5, "最后交易日": "2026-08-10"}]
    )
    stamp = "2026-07-27T16:30:00+08:00"
    with patch.object(convertible_bond, "ak", fake_ak):
        records = convertible_bond.fetch_cb_redeem_records(stamp)

    assert len(records) == 1
    rec = records[0]
    assert rec["ts_code"] == "113001"
    assert rec["redeem_flag"] == "已公告强赎"
    assert rec["updated_at"] == stamp

    fake_ak.bond_cb_redeem_jsl.side_effect = ConnectionError("jsl down")
    with patch.object(convertible_bond, "ak", fake_ak), pytest.raises(ConnectionError):
        convertible_bond.fetch_cb_redeem_records(stamp)


def test_fetch_cb_index_records_full_history():
    """可转债等权指数全历史归一化；异常上抛。"""
    fake_ak = MagicMock()
    fake_ak.bond_cb_index_jsl.return_value = pd.DataFrame(
        [
            {"price_dt": "2026-07-24", "price": 2000.0, "volume": 500.0},
            {"price_dt": "2026-07-27", "price": 2010.0, "volume": 520.0},
        ]
    )
    with patch.object(convertible_bond, "ak", fake_ak):
        records = convertible_bond.fetch_cb_index_records()

    assert len(records) == 2
    assert records[1]["trade_date"] == "2026-07-27"
    assert records[1]["index_code"] == "JSL_EW"
    assert records[1]["close"] == 2010.0

    fake_ak.bond_cb_index_jsl.side_effect = ConnectionError("jsl down")
    with patch.object(convertible_bond, "ak", fake_ak), pytest.raises(ConnectionError):
        convertible_bond.fetch_cb_index_records()


def test_fetch_concept_board_records_overrides_trade_date():
    """概念板块快照 → trade_date 覆写为目标日；源异常上抛。"""
    spot = [
        {"trade_date": "2026-07-28", "concept_code": "BK0001", "concept_name": "AI",
         "pct_change": 1.0, "turnover": 2.0, "up_count": 10, "down_count": 5,
         "data_source": "em"},
    ]
    with patch.object(concept_board, "_fetch_em_spot", return_value=spot):
        records = concept_board.fetch_concept_board_records(_T9_TARGET)

    assert len(records) == 1
    assert records[0]["trade_date"] == _T9_TARGET
    assert records[0]["concept_code"] == "BK0001"

    with patch.object(
        concept_board, "_fetch_em_spot", side_effect=ConnectionError("em down")
    ), pytest.raises(ConnectionError):
        concept_board.fetch_concept_board_records(_T9_TARGET)
