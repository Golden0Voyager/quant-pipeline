"""Tests for providers.py with mocked dependencies."""
from __future__ import annotations

import pandas as pd


def test_smartmoney_db_provider():
    from providers import SmartMoneyDBProvider

    provider = SmartMoneyDBProvider(db_path="/mock/test.db")
    assert provider.db_path is not None


def test_smartmoney_loader_provider():
    from providers import SmartMoneyLoaderProvider

    provider = SmartMoneyLoaderProvider(use_cache=True)
    assert provider is not None


def test_smartmoney_indicator_provider():
    from providers import SmartMoneyIndicatorProvider

    provider = SmartMoneyIndicatorProvider()
    assert provider is not None


def test_providers_module_imports():
    import providers  # noqa: F401
    assert providers


def test_db_provider_methods():
    from providers import SmartMoneyDBProvider

    provider = SmartMoneyDBProvider()

    result = provider.get_stock_list()
    assert result is not None

    result = provider.get_daily_bars("000001.SZ")
    assert result is not None

    provider.save_daily_bars("000001.SZ", pd.DataFrame())
    provider.save_indicators("000001.SZ", pd.DataFrame())
    provider.save_fundamentals("000001.SZ", {"pe": 10})
    provider.save_fund_flow("000001.SZ", {"net_inflow": 1000000})
    provider.save_margin_trading("000001.SZ", {"margin_balance": 5000000})
    provider.save_dragon_tiger("000001.SZ", {"buy_amount": 100000})
    provider.save_block_trade("000001.SZ", {"price": 10.5})
    provider.save_sector_fund_flow("银行", {"net_inflow": 50000000})

    result = provider.get_margin_trading("000001.SZ")
    assert result is not None

    result = provider.get_dragon_tiger("000001.SZ")
    assert result is not None

    result = provider.get_block_trade("000001.SZ")
    assert result is not None

    result = provider.get_sector_fund_flow("银行")
    assert result is not None

    provider.save_historical_valuation("000001.SZ", "2026-06-30", {"pe_ttm": 10})
    provider.save_sector_industry(
        {
            "industry_name": "银行",
            "trade_date": "2026-06-30",
            "avg_pe": 5,
            "fund_inflow_rank": 3,
        }
    )
    result = provider.get_sector_industry("银行")
    assert result is not None

    result = provider.get_fundamentals_batch()
    assert result is not None

    result = provider.watchlist_get_all()
    assert result is not None

    # 全球宏观数据 new save methods
    result = provider.save_north_flow_batch([{"trade_date": "2024-01-01", "market": "沪市", "net_buy_amount": 1e9}])
    assert result is not None
    result = provider.save_index_daily_batch([{"trade_date": "2024-01-01", "index_code": "sh000001", "close": 3000}])
    assert result is not None
    result = provider.save_limit_up_down_batch([{"trade_date": "2024-01-01", "ts_code": "000001.SZ", "limit_type": "涨停"}])
    assert result is not None
    result = provider.save_dividend_summary_batch([{"ts_code": "000001.SZ", "cumulative_dividend": 1.5}])
    assert result is not None
    result = provider.save_gold_price_batch([{"trade_date": "2024-01-01", "morning_price": 890.0}])
    assert result is not None
    result = provider.save_crude_oil_batch([{"trade_date": "2024-01-01", "contract": "CL", "latest_price": 73.5}])
    assert result is not None
    result = provider.save_usd_batch([{"trade_date": "2024-01-01", "currency": "美元", "central_parity_rate": 679.89}])
    assert result is not None
    result = provider.save_global_index_batch([{"trade_date": "2024-01-01", "index_code": "N225", "latest_price": 39000}])
    assert result is not None
    result = provider.save_us_treasury_batch([{"trade_date": "2024-01-01", "us_10y": 4.56, "cn_10y": 1.74}])
    assert result is not None


def test_loader_provider_methods():
    from providers import SmartMoneyLoaderProvider

    provider = SmartMoneyLoaderProvider()

    result = provider.get_daily_bars("000001.SZ")
    assert result is not None

    existing = pd.DataFrame()
    result = provider.incremental_update("000001.SZ", existing)
    assert result is not None

    result = provider.get_market_valuation()
    assert result is not None

    result = provider.get_market_fund_flow()
    assert result is not None


def test_indicator_provider_methods():
    from providers import SmartMoneyIndicatorProvider

    provider = SmartMoneyIndicatorProvider()

    df = pd.DataFrame({"close": [10.0, 11.0, 12.0]})
    result = provider.calculate_all_indicators(df)
    assert result is not None


# ===========================================================================
# Additional provider method coverage
# ===========================================================================
def test_db_provider_close():
    from providers import SmartMoneyDBProvider
    provider = SmartMoneyDBProvider()
    provider.close()  # should not raise


def test_db_provider_get_distinct_codes():
    from providers import SmartMoneyDBProvider
    provider = SmartMoneyDBProvider()
    result = provider.get_distinct_codes("daily_bars")
    assert isinstance(result, set)


def test_db_provider_count_fundamentals_for_date():
    from providers import SmartMoneyDBProvider
    provider = SmartMoneyDBProvider()
    result = provider.count_fundamentals_for_date("2026-07-01")
    assert isinstance(result, int)


def test_db_provider_record_task_run():
    from providers import SmartMoneyDBProvider
    provider = SmartMoneyDBProvider()
    provider.record_task_run("test_task", "2026-07-01")  # should not raise


def test_db_provider_get_last_task_run():
    from providers import SmartMoneyDBProvider
    provider = SmartMoneyDBProvider()
    result = provider.get_last_task_run("test_task")
    assert result is None or isinstance(result, str)


def test_db_provider_save_stock_list():
    from providers import SmartMoneyDBProvider
    provider = SmartMoneyDBProvider()
    df = pd.DataFrame({"code": ["000001"], "name": ["平安银行"], "market": ["sz"], "industry": ["银行"]})
    provider.save_stock_list(df)  # should not raise


def test_db_provider_save_shareholder_count():
    from providers import SmartMoneyDBProvider
    provider = SmartMoneyDBProvider()
    provider.save_shareholder_count("000001.SZ", {"holder_count": 50000})
    provider.save_shareholder_count_batch([{"ts_code": "000001.SZ", "holder_count": 50000}])


def test_db_provider_save_quarterly_financials():
    from providers import SmartMoneyDBProvider
    provider = SmartMoneyDBProvider()
    provider.save_quarterly_financials("000001.SZ", {"revenue": 1e10})
    provider.save_quarterly_financials_batch([{"ts_code": "000001.SZ", "revenue": 1e10}])


def test_loader_provider_with_params():
    from providers import SmartMoneyLoaderProvider
    provider = SmartMoneyLoaderProvider(use_cache=False)
    result = provider.get_daily_bars("000001.SZ", start_date="20240101", end_date="20240131")
    assert result is not None
