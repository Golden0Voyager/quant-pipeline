"""Coverage tests for the extended data tasks.

The newly-added network task modules (china_macro / convertible_bond /
corporate_actions / finance_flow / sector_derivatives / index_chain) are the
biggest coverage gaps. These tests mock ``akshare`` so no network calls are
made, while still exercising every ``_fetch_*`` helper and ``update_*`` entry
point to raise line coverage above the 85% gate.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd

import tasks.china_macro as china_macro
import tasks.concept_board as concept_board
import tasks.convertible_bond as convertible_bond
import tasks.corporate_actions as corporate_actions
import tasks.finance_flow as finance_flow
import tasks.index_chain as index_chain
import tasks.market_valuation as market_valuation
import tasks.money_market as money_market
import tasks.sector_derivatives as sector_derivatives

# ===========================================================================
# money_market
# ===========================================================================


def _mock_ak_money_market() -> MagicMock:
    ak = MagicMock()
    ak.macro_china_shibor_all.return_value = pd.DataFrame({
        "日期": ["2024-01-02", "2024-01-03"],
        "O/N-定价": [1.5, 1.6], "1W-定价": [1.8, 1.9], "2W-定价": [2.0, 2.1],
        "1M-定价": [2.2, 2.3], "3M-定价": [2.5, 2.6], "6M-定价": [2.7, 2.8],
        "9M-定价": [2.9, 3.0], "1Y-定价": [3.1, 3.2],
    })
    ak.repo_rate_query.return_value = pd.DataFrame({
        "date": ["2024-01-02", "2024-01-03"],
        "FR001": [1.2, 1.3], "FR007": [1.5, 1.6], "FR014": [1.8, 1.9],
    })
    ak.macro_bank_china_interest_rate.return_value = pd.DataFrame({
        "日期": ["2024-01-17"], "今值": [3.45],
    })
    ak.macro_china_central_bank_balance.return_value = pd.DataFrame({
        "统计时间": ["2024.12"], "总资产": [400000], "储备货币": [350000],
        "发行货币": [120000], "对其他存款性公司债权": [150000],
        "对政府债权": [50000], "政府存款": [30000],
        "国外资产": [200000], "外汇": [180000],
    })
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
    ak.macro_china_ppi.return_value = pd.DataFrame(
        {"月份": ["2024-01"], "当月": [0.3], "当月同比增长": [0.2]}
    )
    ak.macro_china_pmi.return_value = pd.DataFrame(
        {"月份": ["2024-01"], "制造业-指数": [50.5], "制造业-同比增长": [1.0]}
    )
    ak.macro_china_cx_pmi_yearly.return_value = pd.DataFrame(
        {"日期": ["2024-01-01"], "今值": [51.0]}
    )
    ak.macro_china_money_supply.return_value = pd.DataFrame(
        {
            "月份": ["2024-01"],
            "货币和准货币(M2)-数量(亿元)": [300], "货币和准货币(M2)-同比增长": [8.0],
            "货币(M1)-数量(亿元)": [100], "货币(M1)-同比增长": [3.0],
            "流通中的现金(M0)-数量(亿元)": [10], "流通中的现金(M0)-同比增长": [1.0],
        }
    )
    ak.macro_china_new_financial_credit.return_value = pd.DataFrame(
        {"月份": ["2024-01"], "当月": [100], "当月-同比增长": [5.0]}
    )
    ak.macro_china_consumer_goods_retail.return_value = pd.DataFrame(
        {"月份": ["2024-01"], "同比增长": [4.0], "累计-同比增长": [4.5]}
    )
    ak.macro_china_gdzctz.return_value = pd.DataFrame(
        {"月份": ["2024-01"], "同比增长": [3.0], "自年初累计": [3.5]}
    )
    ak.macro_china_hgjck.return_value = pd.DataFrame(
        {
            "月份": ["2024-01"],
            "当月出口额-金额": [100], "当月出口额-同比增长": [2.0],
            "当月进口额-金额": [90], "当月进口额-同比增长": [1.0],
        }
    )
    ak.macro_china_industrial_production_yoy.return_value = pd.DataFrame(
        {"日期": ["2024-01-01"], "今值": [5.0]}
    )
    ak.macro_china_society_electricity.return_value = pd.DataFrame(
        {"统计时间": ["2024.1"], "全社会用电量": [7000], "全社会用电量同比": [6.0]}
    )
    ak.macro_china_qyspjg.return_value = pd.DataFrame(
        {"月份": ["2024-01"], "总指数-指数值": [105], "总指数-同比增长": [1.0], "总指数-环比增长": [0.5]}
    )
    ak.macro_china_xfzxx.return_value = pd.DataFrame(
        {
            "月份": ["2024-01"],
            "消费者信心指数-指数值": [120], "消费者满意指数-指数值": [115],
            "消费者预期指数-指数值": [125],
        }
    )
    ak.macro_china_lpr.return_value = pd.DataFrame(
        {"TRADE_DATE": ["2024-01-01"], "LPR1Y": [3.45], "LPR5Y": [3.95]}
    )
    ak.macro_china_gdp.return_value = pd.DataFrame(
        {
            "季度": ["2024年第一季度"], "国内生产总值（亿元）": [300000], "GDP同比增长": [5.0],
            "GDP环比增长": [1.0], "第一产业（亿元）": [10000], "第二产业（亿元）": [120000],
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
            "代码": ["113050"], "转债名称": ["测试转债"], "现价": [120.0],
            "转股溢价率": [5.0], "双低": [125.0], "到期时间": ["2028-01-01"],
        }
    )
    ak.bond_cb_redeem_jsl.return_value = pd.DataFrame(
        {
            "代码": ["113050"], "名称": ["测试转债"], "强赎状态": ["Y"],
            "强赎价": [100.0], "最后交易日": ["2026-08-01"],
        }
    )
    ak.bond_cb_index_jsl.return_value = pd.DataFrame(
        {
            "日期": ["2024-01-01"], "指数代码": ["000832"], "指数名称": ["中证转债"],
            "开盘": [400.0], "收盘": [401.0], "最高": [402.0], "最低": [399.0], "成交量": [1e6],
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


# ===========================================================================
# corporate_actions
# ===========================================================================


def _mock_ak_corporate_actions() -> MagicMock:
    ak = MagicMock()
    ak.stock_restricted_release_detail_em.return_value = pd.DataFrame(
        {
            "代码": ["000001"], "名称": ["平安银行"], "实际解禁数量": [100.5],
            "总解禁量": [200.5], "市场类型": ["深市"],
        }
    )
    ak.stock_profit_forecast_em.return_value = pd.DataFrame(
        {
            "代码": ["000001"], "名称": ["平安银行"], "报告期": ["2026-06-30"],
            "预告类型": ["预增"], "净利润变动幅度": ["50%"], "上年同期净利润": ["10亿"],
        }
    )
    return ak


def test_update_corporate_actions_runs_all():
    ak = _mock_ak_corporate_actions()
    db = MagicMock()
    for m in ("save_restricted_share_batch", "save_earnings_forecast_batch"):
        setattr(db, m, MagicMock(return_value=1))
    with patch.object(corporate_actions, "ak", ak), patch.object(
        corporate_actions, "get_expected_latest_trading_day", return_value="2026-07-19"
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
    with patch.object(corporate_actions, "ak", ak), patch.object(
        corporate_actions, "get_expected_latest_trading_day", return_value="2026-07-19"
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
            "日期": ["2024-01-01"], "板块": ["港股通"], "成交净买额": [1.0],
            "买入额": [2.0], "卖出额": [1.0], "历史净流入": [100.0],
        }
    )
    ak.stock_zh_ah_spot_em.return_value = pd.DataFrame(
        {
            "A股代码": ["000001"], "H股代码": ["00001"], "名称": ["平安银行"],
            "最新价-HKD": [11.2], "最新价-RMB": [10.1], "溢价": [5.5],
        }
    )
    ak.fund_etf_hist_em.return_value = pd.DataFrame(
        {
            "日期": ["2024-01-01"], "开盘": [2.6], "最高": [2.7], "最低": [2.5],
            "收盘": [2.65], "成交量": [1e8], "成交额": [2.6e8],
        }
    )
    return ak


def test_update_finance_flow_runs_all():
    ak = _mock_ak_finance_flow()
    db = MagicMock()
    for m in ("save_south_flow_batch", "save_ah_premium_batch", "save_etf_daily_batch"):
        setattr(db, m, MagicMock(return_value=1))
    with patch.object(finance_flow, "ak", ak), patch.object(
        finance_flow, "get_expected_latest_trading_day", return_value="2026-07-19"
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
    with patch.object(finance_flow, "ak", ak), patch.object(
        finance_flow, "get_expected_latest_trading_day", return_value="2026-07-19"
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
            "日期": ["2024-01-01"], "开盘": [100.0], "收盘": [101.0], "最高": [102.0],
            "最低": [99.0], "成交量": [1e6], "成交额": [1e8], "涨跌幅": [1.0],
        }
    )
    ak.stock_industry_pe_ratio_cninfo.return_value = pd.DataFrame(
        {
            "行业": ["银行"], "日期": ["2024-01-01"], "平均市盈率": [6.1],
            "平均市净率": [0.7], "总市值": [123456.0],
        }
    )
    ak.futures_zh_daily_sina.return_value = pd.DataFrame(
        {
            "日期": ["2024-01-01"], "开盘价": [3490.0], "最高价": [3510.0], "最低价": [3480.0],
            "收盘价": [3500.0], "成交量": [1e5], "持仓量": [1e4],
        }
    )
    ak.stock_zh_index_daily_tx.return_value = pd.DataFrame({"date": ["2024-01-01"], "close": [3490.0]})
    return ak


def test_update_sector_derivatives_runs_all():
    ak = _mock_ak_sector_derivatives()
    db = MagicMock()
    for m in ("save_sector_daily_batch", "save_sector_valuation_batch", "save_index_futures_basis_batch"):
        setattr(db, m, MagicMock(return_value=1))
    with patch.object(sector_derivatives, "ak", ak):
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
                "trade_date": "2024-01-01", "profit_ratio": 0.5, "avg_cost": 10.0,
                "cost_90_low": 9.0, "cost_90_high": 11.0, "concentration_90": 0.3,
                "cost_70_low": 9.5, "cost_70_high": 10.5, "concentration_70": 0.2,
            }
        ]
    )


def test_update_index_daily_runs():
    ak = MagicMock()
    ak.stock_zh_index_daily_tx.return_value = pd.DataFrame(
        {
            "date": ["2024-01-01"], "open": [3000.0], "high": [3050.0],
            "low": [2980.0], "close": [3020.0], "volume": [1e8],
        }
    )
    db = MagicMock()
    db.save_index_daily_batch.return_value = 1
    with patch.object(index_chain, "ak", ak), patch.object(
        index_chain, "get_expected_latest_trading_day", return_value="2026-07-19"
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


def test_update_chip_distribution_em_success():
    db = MagicMock()
    db.save_chip_distribution_em_batch.return_value = 1
    with patch.object(index_chain, "_fetch_cyq_em", return_value=_cyq_df()), patch.object(
        index_chain, "time"
    ):
        result = index_chain.update_chip_distribution_em(db, symbols_to_update=["000001.SZ"])
    assert result["success"] == 1
    assert result["total"] == 1
    assert result["aborted"] is False
    assert db.save_chip_distribution_em_batch.called


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


def test_fetch_cyq_em_success_and_none():
    df_in = pd.DataFrame(
        {
            "c0": ["2024-01-01"], "c1": [0.5], "c2": [10.0], "c3": [9.0],
            "c4": [11.0], "c5": [0.3], "c6": [9.5], "c7": [10.5], "c8": [0.2],
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
    with patch("akshare.index_stock_cons_sina", return_value=sina_df), patch(
        "akshare.index_stock_cons_csindex", return_value=csindex_df
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
    with patch.object(
        index_chain, "_fetch_index_constituents", return_value={"600000.SH", "600001.SH"}
    ):
        symbols = index_chain._get_chip_em_target_symbols(db)
    assert "000001.SZ" in symbols
    assert "000002.SZ" not in symbols
    assert "600000.SH" in symbols
    assert "600001.SH" in symbols


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
    fake_result = {
        "success": 0, "failed": 0, "skipped": 0, "total": 0, "processed": 0, "aborted": False
    }
    with patch.object(index_chain, "update_chip_distribution_em", return_value=fake_result) as mock_run:
        result = index_chain.update_chip_distribution_em_fullmarket(db)
    assert mock_run.called
    assert result == fake_result


# ===========================================================================
# market_valuation
# ===========================================================================


def _mock_ak_market_valuation() -> MagicMock:
    ak = MagicMock()
    ak.stock_a_ttm_lyr.return_value = pd.DataFrame({
        "date": ["2024-01-02", "2024-01-03"],
        "middlePE": [16.5, 16.3],
        "quantile": [0.45, 0.44],
        "middlePE_LYR": [15.2, 15.0],
    })
    ak.stock_a_all_pb.return_value = pd.DataFrame({
        "date": ["2024-01-02", "2024-01-03"],
        "middlePB": [1.8, 1.79],
        "quantile": [0.3, 0.29],
    })
    ak.stock_ebs_lg.return_value = pd.DataFrame({
        "日期": ["2024-01-02", "2024-01-03"],
        "沪深300": [3800.5, 3810.2],
        "股债利差": [0.06, 0.061],
        "均线": [0.058, 0.059],
    })
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


# ===========================================================================
# concept_board
# ===========================================================================


def _mock_ak_concept_board() -> MagicMock:
    ak = MagicMock()
    ak.stock_board_concept_summary_ths.return_value = pd.DataFrame({
        "日期": ["2024-01-02", "2024-01-02", "2024-01-03"],
        "板块名称": ["AI概念", "芯片概念", "AI概念"],
        "涨跌幅": [3.2, 2.1, -1.5],
        "成交额": [100.5, 80.3, 95.0],
        "上涨家数": [20, 15, 8],
        "下跌家数": [3, 5, 15],
    })
    ak.stock_board_concept_name_ths.return_value = pd.DataFrame({
        "板块名称": ["AI概念", "芯片概念"],
        "板块代码": ["885902", "885903"],
    })
    ak.stock_board_concept_cons_ths.return_value = pd.DataFrame({
        "代码": ["000001", "000002"],
    })
    return ak


def test_update_concept_board_runs_all():
    ak = _mock_ak_concept_board()
    db = MagicMock()
    db.save_concept_board_batch.return_value = 3
    db.save_concept_member_batch.return_value = 4
    with patch.object(concept_board, "ak", ak):
        result = concept_board.update_concept_board(db)
    assert result["board_saved"] == 3
    assert result["member_saved"] == 4
    assert db.save_concept_board_batch.called
    assert db.save_concept_member_batch.called


def test_update_concept_board_ak_none():
    db = MagicMock()
    with patch.object(concept_board, "ak", None):
        result = concept_board.update_concept_board(db)
    assert result["saved"] == 0
    assert "error" in result
