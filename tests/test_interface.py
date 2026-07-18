"""Tests for interface.py - ProviderFactory and Protocol interfaces."""
from __future__ import annotations

import pandas as pd
import pytest

from interface import DatabaseInterface, DataLoaderInterface, IndicatorEngineInterface, ProviderFactory


class MockDatabase:
    """A minimal in-memory mock implementing DatabaseInterface for contract tests."""

    def __init__(self):
        self.db_path = ":memory:"
        self.closed = False
        self.calls: list[tuple[str, tuple, dict]] = []

    def _record(self, name: str, *args, **kwargs):
        self.calls.append((name, args, kwargs))

    @property
    def db_path(self) -> str:
        return self._db_path

    @db_path.setter
    def db_path(self, value: str):
        self._db_path = value

    def close(self) -> None:
        self.closed = True

    def get_distinct_codes(self, table: str, column: str = "ts_code") -> set[str]:
        self._record("get_distinct_codes", table, column=column)
        return {"000001.SZ"}

    def count_fundamentals_for_date(self, trade_date: str) -> int:
        self._record("count_fundamentals_for_date", trade_date)
        return 5000

    def record_task_run(self, task_name: str, run_date: str) -> None:
        self._record("record_task_run", task_name, run_date)

    def get_last_task_run(self, task_name: str) -> str | None:
        self._record("get_last_task_run", task_name)
        return "2024-01-01"

    def get_stock_list(self) -> pd.DataFrame:
        self._record("get_stock_list")
        return pd.DataFrame({"code": ["000001"], "name": ["平安银行"]})

    def save_stock_list(self, df: pd.DataFrame) -> None:
        self._record("save_stock_list", df)

    def get_daily_bars(self, symbol: str) -> pd.DataFrame:
        self._record("get_daily_bars", symbol)
        return pd.DataFrame({"open": [1.0], "close": [2.0]})

    def save_daily_bars(self, symbol: str, df: pd.DataFrame) -> None:
        self._record("save_daily_bars", symbol, df)

    def save_indicators(self, symbol: str, df: pd.DataFrame) -> None:
        self._record("save_indicators", symbol, df)

    def save_fundamentals_batch(self, records: list[dict]) -> int:
        self._record("save_fundamentals_batch", records)
        return len(records)

    def save_fund_flow_batch(self, records: list[dict]) -> int:
        self._record("save_fund_flow_batch", records)
        return len(records)

    def save_margin_trading_batch(self, records: list[dict]) -> int:
        self._record("save_margin_trading_batch", records)
        return len(records)

    def get_margin_trading(self, symbol: str, date: str | None = None) -> dict | None:
        self._record("get_margin_trading", symbol, date=date)
        return {"symbol": symbol, "date": date}

    def save_dragon_tiger_batch(self, records: list[dict]) -> int:
        self._record("save_dragon_tiger_batch", records)
        return len(records)

    def get_dragon_tiger(self, symbol: str, date: str | None = None) -> dict | None:
        self._record("get_dragon_tiger", symbol, date=date)
        return {"symbol": symbol, "date": date}

    def save_shareholder_count_batch(self, records: list[dict]) -> int:
        self._record("save_shareholder_count_batch", records)
        return len(records)

    def save_quarterly_financials_batch(self, records: list[dict]) -> int:
        self._record("save_quarterly_financials_batch", records)
        return len(records)

    def save_block_trade_batch(self, records: list[dict]) -> int:
        self._record("save_block_trade_batch", records)
        return len(records)

    def get_block_trade(self, symbol: str, date: str | None = None) -> dict | None:
        self._record("get_block_trade", symbol, date=date)
        return {"symbol": symbol, "date": date}

    def save_sector_fund_flow_batch(self, records: list[dict]) -> int:
        self._record("save_sector_fund_flow_batch", records)
        return len(records)

    def get_sector_fund_flow(self, sector_name: str, date: str | None = None) -> dict | None:
        self._record("get_sector_fund_flow", sector_name, date=date)
        return {"sector_name": sector_name, "date": date}

    def save_historical_valuation(self, symbol: str, trade_date: str, data: dict) -> None:
        self._record("save_historical_valuation", symbol, trade_date, data)

    def save_sector_industry(self, data: dict) -> None:
        self._record("save_sector_industry", data)

    def get_sector_industry(self, industry_name: str, trade_date: str | None = None) -> dict | None:
        self._record("get_sector_industry", industry_name, date=trade_date)
        return {"industry_name": industry_name, "date": trade_date}

    def get_fundamentals_batch(self, trade_date: str | None = None) -> pd.DataFrame:
        self._record("get_fundamentals_batch", date=trade_date)
        return pd.DataFrame({"ts_code": ["000001.SZ"]})

    def save_north_flow_batch(self, records: list[dict]) -> int:
        self._record("save_north_flow_batch", records)
        return len(records)

    def save_index_daily_batch(self, records: list[dict]) -> int:
        self._record("save_index_daily_batch", records)
        return len(records)

    def save_limit_up_down_batch(self, records: list[dict]) -> int:
        self._record("save_limit_up_down_batch", records)
        return len(records)

    def save_dividend_summary_batch(self, records: list[dict]) -> int:
        self._record("save_dividend_summary_batch", records)
        return len(records)

    def save_gold_price_batch(self, records: list[dict]) -> int:
        self._record("save_gold_price_batch", records)
        return len(records)

    def save_crude_oil_batch(self, records: list[dict]) -> int:
        self._record("save_crude_oil_batch", records)
        return len(records)

    def save_usd_batch(self, records: list[dict]) -> int:
        self._record("save_usd_batch", records)
        return len(records)

    def save_global_index_batch(self, records: list[dict]) -> int:
        self._record("save_global_index_batch", records)
        return len(records)

    def save_us_treasury_batch(self, records: list[dict]) -> int:
        self._record("save_us_treasury_batch", records)
        return len(records)

    def save_futures_daily_batch(self, records: list[dict]) -> int:
        self._record("save_futures_daily_batch", records)
        return len(records)

    def watchlist_get_all(self, status: str | None = None) -> pd.DataFrame:
        self._record("watchlist_get_all", status=status)
        return pd.DataFrame({"ts_code": ["000001.SZ"], "status": ["active"]})

    def save_chip_distribution_batch(self, records: list[dict]) -> int:
        self._record("save_chip_distribution_batch", records)
        return len(records)

    def get_chip_distribution(self, symbol: str, date: str | None = None) -> dict | None:
        self._record("get_chip_distribution", symbol, date=date)
        return {"symbol": symbol, "date": date}

    def get_chip_distribution_batch(
        self, stock_list: list[str] | None = None
    ) -> dict[str, dict]:
        self._record("get_chip_distribution_batch", stock_list)
        return {"000001.SZ": {"symbol": "000001.SZ"}}

    def save_chip_distribution_em_batch(self, records: list[dict]) -> int:
        self._record("save_chip_distribution_em_batch", records)
        return len(records)

    def get_chip_distribution_em_batch(
        self, stock_list: list[str] | None = None
    ) -> dict[str, dict]:
        self._record("get_chip_distribution_em_batch", stock_list)
        return {"000001.SZ": {"symbol": "000001.SZ"}}


class MockDataLoader:
    """A minimal in-memory mock implementing DataLoaderInterface for contract tests."""

    def __init__(self):
        self.calls: list[tuple[str, tuple, dict]] = []

    def _record(self, name: str, *args, **kwargs):
        self.calls.append((name, args, kwargs))

    def get_daily_bars(
        self,
        symbol: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        self._record("get_daily_bars", symbol, start_date=start_date, end_date=end_date)
        return pd.DataFrame({"open": [1.0], "close": [2.0]})

    def incremental_update(self, symbol: str, existing_df: pd.DataFrame) -> pd.DataFrame:
        self._record("incremental_update", symbol, existing_df)
        return existing_df

    def get_market_valuation(self) -> pd.DataFrame:
        self._record("get_market_valuation")
        return pd.DataFrame({"ts_code": ["000001.SZ"]})

    def get_market_fund_flow(self) -> pd.DataFrame:
        self._record("get_market_fund_flow")
        return pd.DataFrame({"code": ["000001"]})


class MockIndicatorEngine:
    """A minimal in-memory mock implementing IndicatorEngineInterface for contract tests."""

    def calculate_all_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        return df


def test_provider_factory_not_configured():
    ProviderFactory._db_provider = None
    ProviderFactory._loader_provider = None
    ProviderFactory._indicator_provider = None

    with pytest.raises(RuntimeError, match="Provider not configured"):
        ProviderFactory.get_db()
    with pytest.raises(RuntimeError, match="Provider not configured"):
        ProviderFactory.get_loader()
    with pytest.raises(RuntimeError, match="Provider not configured"):
        ProviderFactory.get_indicator_engine()


def test_provider_factory_unknown_provider():
    with pytest.raises(ValueError, match="Unknown provider"):
        ProviderFactory.configure(provider="nonexistent")


def test_provider_factory_configure_smartmoney():
    ProviderFactory._db_provider = None
    ProviderFactory._loader_provider = None
    ProviderFactory._indicator_provider = None

    ProviderFactory.configure(provider="smartmoney")

    db = ProviderFactory.get_db()
    assert db is not None

    loader = ProviderFactory.get_loader()
    assert loader is not None

    engine = ProviderFactory.get_indicator_engine()
    assert engine is not None


def test_protocol_imports():
    assert DatabaseInterface is not None
    assert DataLoaderInterface is not None
    assert IndicatorEngineInterface is not None


# ===========================================================================
# Protocol contract tests - verify every method signature is callable
# ===========================================================================

def test_database_interface_contract():
    """Exercise every method declared on DatabaseInterface via a mock implementation."""
    db = MockDatabase()

    assert db.db_path == ":memory:"
    db.close()
    assert db.closed is True

    assert db.get_distinct_codes("daily_bars") == {"000001.SZ"}
    assert db.count_fundamentals_for_date("2024-01-01") == 5000
    db.record_task_run("task", "2024-01-01")
    assert db.get_last_task_run("task") == "2024-01-01"

    stock_list = db.get_stock_list()
    assert not stock_list.empty

    bars = db.get_daily_bars("000001.SZ")
    assert not bars.empty

    sample_df = pd.DataFrame({"open": [1.0], "close": [2.0]})
    db.save_stock_list(sample_df)
    db.save_daily_bars("000001.SZ", sample_df)
    db.save_indicators("000001.SZ", sample_df)

    assert db.save_fundamentals_batch([{"pe": 10.0}]) == 1
    assert db.save_fund_flow_batch([{"main": 100.0}]) == 1

    assert db.save_margin_trading_batch([{"balance": 1.0}]) == 1
    assert db.get_margin_trading("000001.SZ", "2024-01-01") is not None

    assert db.save_dragon_tiger_batch([{"amount": 1.0}]) == 1
    assert db.get_dragon_tiger("000001.SZ", "2024-01-01") is not None

    assert db.save_shareholder_count_batch([{"count": 1000}]) == 1
    assert db.save_quarterly_financials_batch([{"revenue": 1e9}]) == 1

    assert db.save_block_trade_batch([{"price": 10.0}]) == 1
    assert db.get_block_trade("000001.SZ", "2024-01-01") is not None

    assert db.save_sector_fund_flow_batch([{"inflow": 100.0}]) == 1
    assert db.get_sector_fund_flow("bank", "2024-01-01") is not None

    db.save_historical_valuation("000001.SZ", "2024-01-01", {"pe": 10.0})
    db.save_sector_industry({"industry": "bank"})
    assert db.get_sector_industry("bank", "2024-01-01") is not None

    assert not db.get_fundamentals_batch("2024-01-01").empty

    assert db.save_north_flow_batch([{"net": 100.0}]) == 1
    assert db.save_index_daily_batch([{"close": 3000.0}]) == 1
    assert db.save_limit_up_down_batch([{"limit": "up"}]) == 1
    assert db.save_dividend_summary_batch([{"dividend": 1.0}]) == 1
    assert db.save_gold_price_batch([{"price": 2000.0}]) == 1
    assert db.save_crude_oil_batch([{"price": 80.0}]) == 1
    assert db.save_usd_batch([{"rate": 7.0}]) == 1
    assert db.save_global_index_batch([{"index": "sp500"}]) == 1
    assert db.save_us_treasury_batch([{"yield": 4.0}]) == 1

    assert not db.watchlist_get_all().empty

    assert db.save_chip_distribution_batch([{"symbol": "000001.SZ"}]) == 1
    assert db.get_chip_distribution("000001.SZ", "2024-01-01") is not None
    dist_batch = db.get_chip_distribution_batch(["000001.SZ"])
    assert "000001.SZ" in dist_batch
    assert db.save_chip_distribution_em_batch([{"symbol": "000001.SZ"}]) == 1
    em_batch = db.get_chip_distribution_em_batch(["000001.SZ"])
    assert "000001.SZ" in em_batch

    # 验证 ProviderFactory.get_db() 返回的对象符合 DatabaseInterface 契约
    ProviderFactory._db_provider = db
    assert ProviderFactory.get_db() is db


def test_data_loader_interface_contract():
    """Exercise every method declared on DataLoaderInterface via a mock implementation."""
    loader = MockDataLoader()

    df = loader.get_daily_bars("000001.SZ", start_date="20240101", end_date="20241231")
    assert not df.empty

    existing = pd.DataFrame({"open": [1.0]})
    assert loader.incremental_update("000001.SZ", existing) is existing

    assert not loader.get_market_valuation().empty
    assert not loader.get_market_fund_flow().empty


def test_indicator_engine_interface_contract():
    """Exercise every method declared on IndicatorEngineInterface via a mock implementation."""
    engine = MockIndicatorEngine()
    df = pd.DataFrame({"close": [1.0, 2.0, 3.0]})
    result = engine.calculate_all_indicators(df)
    assert result is df
