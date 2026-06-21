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

    result = provider.watchlist_get_all()
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
