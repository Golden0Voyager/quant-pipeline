"""列名映射修复的回归与覆盖测试。

对应修复：
- finance_flow: south_flow 列名 + ETF 裸代码
- corporate_actions: restricted_share 列名 + 业绩预告改用 stock_yjyg_em
- sector_derivatives: 板块估值日期/symbol + _retry 网络重试

所有测试 mock akshare，不产生网络调用。集中放在独立文件，避免与并发开发的
test_new_tasks.py 冲突。
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd

import tasks.corporate_actions as corporate_actions
import tasks.finance_flow as finance_flow
import tasks.sector_derivatives as sector_derivatives

TRADING_DAY = "2026-07-19"


# ===========================================================================
# finance_flow — south_flow
# ===========================================================================


def test_south_flow_maps_actual_columns():
    ak = MagicMock()
    ak.stock_hsgt_hist_em.return_value = pd.DataFrame(
        {
            "日期": ["2026-07-17"], "当日成交净买额": [-59.6],
            "买入成交额": [595.5], "卖出成交额": [655.1], "历史累计净买额": [5.4],
        }
    )
    with patch.object(finance_flow, "ak", ak):
        records = finance_flow._fetch_south_flow()
    assert records == [
        {
            "trade_date": "2026-07-17", "market": "南向", "net_buy_amount": -59.6,
            "buy_amount": 595.5, "sell_amount": 655.1, "cumulative_net_buy": 5.4,
            "data_source": "akshare",
        }
    ]


def test_south_flow_ak_none_and_empty_and_error():
    with patch.object(finance_flow, "ak", None):
        assert finance_flow._fetch_south_flow() == []
    ak = MagicMock()
    ak.stock_hsgt_hist_em.return_value = pd.DataFrame()
    with patch.object(finance_flow, "ak", ak):
        assert finance_flow._fetch_south_flow() == []
    ak.stock_hsgt_hist_em.side_effect = RuntimeError("net")
    with patch.object(finance_flow, "ak", ak):
        assert finance_flow._fetch_south_flow() == []


def test_update_south_flow_paths():
    ak = MagicMock()
    ak.stock_hsgt_hist_em.return_value = pd.DataFrame(
        {
            "日期": ["2026-07-17"], "当日成交净买额": [1.0],
            "买入成交额": [2.0], "卖出成交额": [1.0], "历史累计净买额": [3.0],
        }
    )
    db = MagicMock()
    db.save_south_flow_batch.return_value = 1
    with patch.object(finance_flow, "ak", ak):
        assert finance_flow.update_south_flow(db)["saved"] == 1
    # ak=None
    with patch.object(finance_flow, "ak", None):
        assert finance_flow.update_south_flow(MagicMock())["saved"] == 0
    # 空数据
    ak.stock_hsgt_hist_em.return_value = pd.DataFrame()
    with patch.object(finance_flow, "ak", ak):
        assert finance_flow.update_south_flow(MagicMock())["saved"] == 0
    # save 抛异常
    ak.stock_hsgt_hist_em.return_value = pd.DataFrame(
        {
            "日期": ["2026-07-17"], "当日成交净买额": [1.0],
            "买入成交额": [2.0], "卖出成交额": [1.0], "历史累计净买额": [3.0],
        }
    )
    db_err = MagicMock()
    db_err.save_south_flow_batch.side_effect = RuntimeError("db")
    with patch.object(finance_flow, "ak", ak):
        r = finance_flow.update_south_flow(db_err)
    assert r["saved"] == 0 and "error" in r


# ===========================================================================
# finance_flow — etf_daily
# ===========================================================================


def _etf_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "日期": ["2026-07-17"], "开盘": [2.6], "最高": [2.7], "最低": [2.5],
            "收盘": [2.65], "成交量": [1e8], "成交额": [2.6e8],
        }
    )


def test_etf_daily_uses_bare_symbol():
    ak = MagicMock()
    ak.fund_etf_hist_em.return_value = _etf_df()
    with patch.object(finance_flow, "ak", ak):
        records = finance_flow._fetch_etf_daily("20100101", "20260717")
    # 首个 ETF 代码 510050，symbol 必须是裸 6 位码
    assert ak.fund_etf_hist_em.call_args_list[0].kwargs["symbol"] == "510050"
    assert records[0]["ts_code"] == "510050"
    assert records[0]["close"] == 2.65


def test_etf_daily_ak_none_and_per_code_error():
    with patch.object(finance_flow, "ak", None):
        assert finance_flow._fetch_etf_daily("20100101", "20260717") == []
    ak = MagicMock()
    ak.fund_etf_hist_em.side_effect = RuntimeError("net")
    with patch.object(finance_flow, "ak", ak):
        # 每只 ETF 都失败 → 跳过 → []
        assert finance_flow._fetch_etf_daily("20100101", "20260717") == []


def test_update_etf_daily_paths():
    ak = MagicMock()
    ak.fund_etf_hist_em.return_value = _etf_df()
    db = MagicMock()
    db.save_etf_daily_batch.return_value = 20
    with patch.object(finance_flow, "ak", ak), patch.object(
        finance_flow, "get_expected_latest_trading_day", return_value=TRADING_DAY
    ):
        assert finance_flow.update_etf_daily(db)["saved"] == 20
    with patch.object(finance_flow, "ak", None):
        assert finance_flow.update_etf_daily(MagicMock())["saved"] == 0
    ak.fund_etf_hist_em.return_value = pd.DataFrame()
    with patch.object(finance_flow, "ak", ak), patch.object(
        finance_flow, "get_expected_latest_trading_day", return_value=TRADING_DAY
    ):
        assert finance_flow.update_etf_daily(MagicMock())["saved"] == 0


def test_update_finance_flow_aggregate():
    ak = MagicMock()
    ak.stock_hsgt_hist_em.return_value = pd.DataFrame(
        {
            "日期": ["2026-07-17"], "当日成交净买额": [1.0],
            "买入成交额": [2.0], "卖出成交额": [1.0], "历史累计净买额": [3.0],
        }
    )
    ak.stock_zh_ah_spot_em.return_value = pd.DataFrame(
        {
            "A股代码": ["000001"], "H股代码": ["00001"], "名称": ["平安银行"],
            "最新价-HKD": [11.2], "最新价-RMB": [10.1], "溢价": [5.5],
        }
    )
    ak.fund_etf_hist_em.return_value = _etf_df()
    db = MagicMock()
    for m in ("save_south_flow_batch", "save_ah_premium_batch", "save_etf_daily_batch"):
        setattr(db, m, MagicMock(return_value=1))
    with patch.object(finance_flow, "ak", ak), patch.object(
        finance_flow, "get_expected_latest_trading_day", return_value=TRADING_DAY
    ):
        result = finance_flow.update_finance_flow(db)
    assert "details" in result
    assert result["saved"] >= 1


# ===========================================================================
# corporate_actions — restricted_share
# ===========================================================================


def test_restricted_share_maps_columns_and_range_query():
    ak = MagicMock()
    ak.stock_restricted_release_detail_em.return_value = pd.DataFrame(
        {
            "股票代码": ["300144"], "股票简称": ["宋城演艺"], "解禁时间": ["2026-07-15"],
            "限售股类型": ["股权激励限售股份"], "实际解禁数量": [13467.0], "解禁数量": [103248.0],
        }
    )
    with patch.object(corporate_actions, "ak", ak), patch.object(
        corporate_actions, "get_expected_latest_trading_day", return_value=TRADING_DAY
    ):
        records = corporate_actions._fetch_restricted_share()
    assert records[0]["ts_code"] == "300144"
    assert records[0]["name"] == "宋城演艺"
    assert records[0]["total_shares"] == 103248.0
    assert records[0]["market_type"] == "股权激励限售股份"
    assert records[0]["release_date"] == "2026-07-15"
    kwargs = ak.stock_restricted_release_detail_em.call_args_list[0].kwargs
    assert kwargs == {"start_date": "20260619", "end_date": "20260719"}


def test_restricted_share_dedup_ak_none_empty_error():
    with patch.object(corporate_actions, "ak", None):
        assert corporate_actions._fetch_restricted_share() == []
    ak = MagicMock()
    ak.stock_restricted_release_detail_em.return_value = pd.DataFrame()
    with patch.object(corporate_actions, "ak", ak), patch.object(
        corporate_actions, "get_expected_latest_trading_day", return_value=TRADING_DAY
    ):
        assert corporate_actions._fetch_restricted_share() == []
    ak.stock_restricted_release_detail_em.side_effect = RuntimeError("net")
    with patch.object(corporate_actions, "ak", ak), patch.object(
        corporate_actions, "get_expected_latest_trading_day", return_value=TRADING_DAY
    ):
        assert corporate_actions._fetch_restricted_share() == []
    # 去重：同 ts_code+解禁时间 只保留一条
    ak2 = MagicMock()
    ak2.stock_restricted_release_detail_em.side_effect = None
    ak2.stock_restricted_release_detail_em.return_value = pd.DataFrame(
        {
            "股票代码": ["300144", "300144"], "股票简称": ["宋城演艺", "宋城演艺"],
            "解禁时间": ["2026-07-15", "2026-07-15"], "限售股类型": ["A", "A"],
            "实际解禁数量": [1.0, 1.0], "解禁数量": [2.0, 2.0],
        }
    )
    with patch.object(corporate_actions, "ak", ak2), patch.object(
        corporate_actions, "get_expected_latest_trading_day", return_value=TRADING_DAY
    ):
        assert len(corporate_actions._fetch_restricted_share()) == 1


def test_update_restricted_share_paths():
    ak = MagicMock()
    ak.stock_restricted_release_detail_em.return_value = pd.DataFrame(
        {
            "股票代码": ["300144"], "股票简称": ["宋城演艺"], "解禁时间": ["2026-07-15"],
            "限售股类型": ["A"], "实际解禁数量": [1.0], "解禁数量": [2.0],
        }
    )
    db = MagicMock()
    db.save_restricted_share_batch.return_value = 1
    with patch.object(corporate_actions, "ak", ak), patch.object(
        corporate_actions, "get_expected_latest_trading_day", return_value=TRADING_DAY
    ):
        assert corporate_actions.update_restricted_share(db)["saved"] == 1
    with patch.object(corporate_actions, "ak", None):
        assert corporate_actions.update_restricted_share(MagicMock())["saved"] == 0


# ===========================================================================
# corporate_actions — earnings_forecast (stock_yjyg_em)
# ===========================================================================


def test_recent_report_periods_descending():
    with patch.object(
        corporate_actions, "get_expected_latest_trading_day", return_value=TRADING_DAY
    ):
        assert corporate_actions._recent_report_periods(2) == ["20260630", "20260331"]
        assert corporate_actions._recent_report_periods(1) == ["20260630"]


def test_earnings_forecast_uses_yjyg_and_maps_columns():
    ak = MagicMock()
    ak.stock_yjyg_em.return_value = pd.DataFrame(
        {
            "股票代码": ["688522"], "股票简称": ["纳睿雷达"], "预告类型": ["预增"],
            "业绩变动幅度": [51.34], "上年同期值": [1.5e8],
        }
    )
    with patch.object(corporate_actions, "ak", ak), patch.object(
        corporate_actions, "get_expected_latest_trading_day", return_value=TRADING_DAY
    ):
        records = corporate_actions._fetch_earnings_forecast()
    assert ak.stock_yjyg_em.call_count == 2  # 两个报告期
    first = records[0]
    assert first["ts_code"] == "688522"
    assert first["forecast_type"] == "预增"
    assert first["net_profit_change"] == 51.34
    assert first["previous_profit"] == 1.5e8
    assert first["end_date"] == "2026-06-30"


def test_earnings_forecast_ak_none_and_period_error():
    with patch.object(corporate_actions, "ak", None):
        assert corporate_actions._fetch_earnings_forecast() == []
    ak = MagicMock()
    ak.stock_yjyg_em.side_effect = RuntimeError("net")
    with patch.object(corporate_actions, "ak", ak), patch.object(
        corporate_actions, "get_expected_latest_trading_day", return_value=TRADING_DAY
    ):
        assert corporate_actions._fetch_earnings_forecast() == []
    # 空 DataFrame
    ak.stock_yjyg_em.side_effect = None
    ak.stock_yjyg_em.return_value = pd.DataFrame()
    with patch.object(corporate_actions, "ak", ak), patch.object(
        corporate_actions, "get_expected_latest_trading_day", return_value=TRADING_DAY
    ):
        assert corporate_actions._fetch_earnings_forecast() == []


def test_update_earnings_forecast_and_corporate_actions_aggregate():
    ak = MagicMock()
    ak.stock_yjyg_em.return_value = pd.DataFrame(
        {
            "股票代码": ["688522"], "股票简称": ["纳睿雷达"], "预告类型": ["预增"],
            "业绩变动幅度": [51.34], "上年同期值": [1.5e8],
        }
    )
    ak.stock_restricted_release_detail_em.return_value = pd.DataFrame(
        {
            "股票代码": ["300144"], "股票简称": ["宋城演艺"], "解禁时间": ["2026-07-15"],
            "限售股类型": ["A"], "实际解禁数量": [1.0], "解禁数量": [2.0],
        }
    )
    db = MagicMock()
    db.save_earnings_forecast_batch.return_value = 1
    db.save_restricted_share_batch.return_value = 1
    with patch.object(corporate_actions, "ak", ak), patch.object(
        corporate_actions, "get_expected_latest_trading_day", return_value=TRADING_DAY
    ):
        assert corporate_actions.update_earnings_forecast(db)["saved"] == 1
        result = corporate_actions.update_corporate_actions(db)
    assert "restricted_share" in result and "earnings_forecast" in result
    with patch.object(corporate_actions, "ak", None):
        assert corporate_actions.update_earnings_forecast(MagicMock())["saved"] == 0
        assert "error" in corporate_actions.update_corporate_actions(MagicMock())


# ===========================================================================
# sector_derivatives — _retry
# ===========================================================================


def test_retry_success_and_exhaustion():
    assert sector_derivatives._retry(lambda: 7, tries=3, base_delay=0, label="ok") == 7
    calls = {"n": 0}

    def boom():
        calls["n"] += 1
        raise ValueError("boom")

    with patch.object(sector_derivatives.time, "sleep"):
        assert sector_derivatives._retry(boom, tries=3, base_delay=0, label="t") is None
    assert calls["n"] == 3


# ===========================================================================
# sector_derivatives — sector_daily / valuation / basis
# ===========================================================================


def _sector_daily_ak() -> MagicMock:
    ak = MagicMock()
    ak.stock_board_industry_name_em.return_value = pd.DataFrame(
        {"板块名称": ["银行", "证券", "白酒"]}
    )
    ak.stock_board_industry_hist_em.return_value = pd.DataFrame(
        {
            "日期": ["2026-07-17"], "开盘": [100.0], "收盘": [101.0], "最高": [102.0],
            "最低": [99.0], "成交量": [1e6], "成交额": [1e8], "涨跌幅": [1.0],
        }
    )
    return ak


def test_sector_daily_maps_and_filters():
    ak = _sector_daily_ak()
    with patch.object(sector_derivatives, "ak", ak):
        records = sector_derivatives._fetch_sector_daily()
    assert records
    assert records[0]["sector_name"] in {"银行", "证券", "白酒"}
    assert records[0]["close"] == 101.0
    assert records[0]["pct_change"] == 1.0


def test_sector_daily_ak_none_and_empty_board():
    with patch.object(sector_derivatives, "ak", None):
        assert sector_derivatives._fetch_sector_daily() == []
    ak = MagicMock()
    ak.stock_board_industry_name_em.return_value = pd.DataFrame()
    with patch.object(sector_derivatives, "ak", ak), patch.object(
        sector_derivatives.time, "sleep"
    ):
        assert sector_derivatives._fetch_sector_daily() == []


def test_sector_valuation_uses_trading_day_and_symbol():
    ak = MagicMock()
    ak.stock_industry_pe_ratio_cninfo.return_value = pd.DataFrame(
        {
            "行业名称": ["银行"], "变动日期": ["2026-07-17"],
            "静态市盈率-加权平均": [6.1], "总市值-静态": [123456.0],
        }
    )
    with patch.object(sector_derivatives, "ak", ak), patch.object(
        sector_derivatives, "get_expected_latest_trading_day", return_value=TRADING_DAY
    ):
        records = sector_derivatives._fetch_sector_valuation()
    ak.stock_industry_pe_ratio_cninfo.assert_called_once_with(
        symbol="证监会行业分类", date="20260719"
    )
    assert records[0]["sector_name"] == "银行"
    assert records[0]["pe"] == 6.1
    assert records[0]["pb"] is None  # 该接口无 PB
    assert records[0]["total_mv"] == 123456.0


def test_sector_valuation_ak_none_and_error():
    with patch.object(sector_derivatives, "ak", None):
        assert sector_derivatives._fetch_sector_valuation() == []
    ak = MagicMock()
    ak.stock_industry_pe_ratio_cninfo.side_effect = RuntimeError("records")
    with patch.object(sector_derivatives, "ak", ak), patch.object(
        sector_derivatives, "get_expected_latest_trading_day", return_value=TRADING_DAY
    ), patch.object(sector_derivatives.time, "sleep"):
        assert sector_derivatives._fetch_sector_valuation() == []


def test_index_futures_basis_computes_basis():
    ak = MagicMock()
    ak.futures_zh_daily_sina.return_value = pd.DataFrame(
        {"date": ["2026-07-17"], "open": [3490.0], "high": [3510.0],
         "low": [3480.0], "close": [3500.0], "volume": [1e5], "hold": [1e4], "settle": [3495.0]}
    )
    ak.stock_zh_index_daily_tx.return_value = pd.DataFrame(
        {"date": ["2026-07-17"], "open": [3480.0], "close": [3490.0],
         "high": [3505.0], "low": [3475.0], "amount": [1e9]}
    )
    with patch.object(sector_derivatives, "ak", ak):
        records = sector_derivatives._fetch_index_futures_basis()
    assert records
    row = records[0]
    assert row["futures_price"] == 3500.0
    assert row["index_price"] == 3490.0
    assert row["basis"] == 10.0


def test_index_futures_basis_ak_none():
    with patch.object(sector_derivatives, "ak", None):
        assert sector_derivatives._fetch_index_futures_basis() == []


def test_update_sector_derivatives_all_and_errors():
    ak = _sector_daily_ak()
    ak.stock_industry_pe_ratio_cninfo.return_value = pd.DataFrame(
        {
            "行业名称": ["银行"], "变动日期": ["2026-07-17"],
            "静态市盈率-加权平均": [6.1], "总市值-静态": [123456.0],
        }
    )
    ak.futures_zh_daily_sina.return_value = pd.DataFrame(
        {"date": ["2026-07-17"], "open": [3490.0], "high": [3510.0],
         "low": [3480.0], "close": [3500.0], "volume": [1e5], "hold": [1e4], "settle": [3495.0]}
    )
    ak.stock_zh_index_daily_tx.return_value = pd.DataFrame(
        {"date": ["2026-07-17"], "open": [3480.0], "close": [3490.0],
         "high": [3505.0], "low": [3475.0], "amount": [1e9]}
    )
    db = MagicMock()
    for m in ("save_sector_daily_batch", "save_sector_valuation_batch", "save_index_futures_basis_batch"):
        setattr(db, m, MagicMock(return_value=1))
    with patch.object(sector_derivatives, "ak", ak), patch.object(
        sector_derivatives, "get_expected_latest_trading_day", return_value=TRADING_DAY
    ):
        result = sector_derivatives.update_sector_derivatives(db)
    assert result["sector_daily"] == 1
    assert result["sector_valuation"] == 1
    assert result["index_futures_basis"] == 1

    # ak=None → error
    with patch.object(sector_derivatives, "ak", None):
        assert "error" in sector_derivatives.update_sector_derivatives(MagicMock())

    # save 抛异常 → 对应子任务记录 error 字符串
    db_err = MagicMock()
    db_err.save_sector_daily_batch.side_effect = RuntimeError("db down")
    db_err.save_sector_valuation_batch.return_value = 1
    db_err.save_index_futures_basis_batch.return_value = 1
    with patch.object(sector_derivatives, "ak", ak), patch.object(
        sector_derivatives, "get_expected_latest_trading_day", return_value=TRADING_DAY
    ):
        result_err = sector_derivatives.update_sector_derivatives(db_err)
    assert str(result_err["sector_daily"]).startswith("error")
