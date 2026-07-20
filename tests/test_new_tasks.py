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
import tasks.convertible_bond as convertible_bond
import tasks.corporate_actions as corporate_actions
import tasks.finance_flow as finance_flow
import tasks.index_chain as index_chain
import tasks.sector_derivatives as sector_derivatives

# ===========================================================================
# china_macro
# ===========================================================================


def _mock_ak_china_macro() -> MagicMock:
    ak = MagicMock()
    ak.macro_china_cpi.return_value = pd.DataFrame(
        {"年": [2024], "月": [1], "cpi当月同比": [0.2], "cpi当月环比": [0.1], "cpi核心当月同比": [0.3]}
    )
    ak.macro_china_ppi.return_value = pd.DataFrame(
        {"年": [2024], "月": [1], "ppi当月同比": [0.3], "ppi当月环比": [0.2]}
    )
    ak.macro_china_pmi.return_value = pd.DataFrame(
        {
            "年": [2024], "月": [1], "制造业PMI": [50.5], "制造业PMI同比增长": [1.0],
            "制造业PMI环比变化": [0.5], "制造业PMI环比": [0.2],
        }
    )
    ak.macro_china_cx_pmi_yearly.return_value = pd.DataFrame(
        {"年": [2024], "月": [1], "财新制造业PMI": [51.0]}
    )
    ak.macro_china_money_supply.return_value = pd.DataFrame(
        {
            "年": [2024], "月": [1], "M0": [10], "M0同比": [1], "M1": [20], "M1同比": [2],
            "M2": [30], "M2同比": [3],
        }
    )
    ak.macro_china_new_financial_credit.return_value = pd.DataFrame(
        {"年": [2024], "月": [1], "新增贷款": [100], "新增贷款同比": [5]}
    )
    ak.macro_china_consumer_goods_retail.return_value = pd.DataFrame(
        {"年": [2024], "月": [1], "社会消费品零售总额同比增长": [4], "社会消费品零售总额累计增长": [4.5]}
    )
    ak.macro_china_gdzctz.return_value = pd.DataFrame(
        {"年": [2024], "月": [1], "固定资产投资完成额同比增长": [3], "固定资产投资完成额累计增长": [3.5]}
    )
    ak.macro_china_hgjck.return_value = pd.DataFrame(
        {
            "年": [2024], "月": [1], "出口总额": [100], "出口总额同比增长": [2],
            "进口总额": [90], "进口总额同比增长": [1],
        }
    )
    ak.macro_china_industrial_production_yoy.return_value = pd.DataFrame(
        {"年": [2024], "月": [1], "工业增加值同比增长": [5], "工业增加值累计增长": [5.5]}
    )
    ak.macro_china_society_electricity.return_value = pd.DataFrame(
        {"年": [2024], "月": [1], "全社会用电量同比增长": [6], "全社会用电量": [7000]}
    )
    ak.macro_china_qyspjg.return_value = pd.DataFrame(
        {"年": [2024], "月": [1], "企业商品价格同比": [1], "企业商品价格环比": [0.5]}
    )
    ak.macro_china_xfzxx.return_value = pd.DataFrame(
        {
            "年": [2024], "月": [1], "消费者信心指数": [120], "消费者满意指数": [115],
            "消费者预期指数": [125],
        }
    )
    ak.macro_china_lpr.return_value = pd.DataFrame(
        {"日期": ["2024-01-01", "2024-01-01"], "利率名称": ["1年期LPR", "5年期LPR"], "利率": [3.45, 3.95]}
    )
    ak.macro_china_gdp.return_value = pd.DataFrame(
        {
            "季度": ["2024年第一季度"], "国内生产总值": [300000], "同比增长": [5.0], "环比增长": [1.0],
            "第一产业": [10000], "第二产业": [120000], "第三产业": [170000],
        }
    )
    ak.macro_china_shibor_all.return_value = pd.DataFrame(
        {
            "日期": ["2024-01-01"], "ON": [1.0], "1W": [1.5], "2W": [2.0], "1M": [2.5],
            "3M": [3.0], "6M": [3.5], "9M": [3.7], "1Y": [4.0],
        }
    )
    return ak


def test_update_china_macro_runs_all_fetchers():
    ak = _mock_ak_china_macro()
    db = MagicMock()
    db.save_macro_monthly_batch.return_value = 1
    db.save_macro_quarterly_batch.return_value = 1
    db.save_macro_daily_batch.return_value = 1
    with patch.object(china_macro, "ak", ak):
        result = china_macro.update_china_macro(db)
    assert "monthly_saved" in result
    assert "quarterly_saved" in result
    assert "daily_saved" in result
    assert db.save_macro_monthly_batch.called
    assert db.save_macro_quarterly_batch.called
    assert db.save_macro_daily_batch.called


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
