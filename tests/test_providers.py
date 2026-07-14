"""Tests for providers.py with mocked dependencies."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd


def _make_provider_with_real_db_path(db_path: str):
    """Create a SmartMoneyDBProvider whose mocked underlying db reports a real path.

    This avoids __init__ writing MagicMock-named SQLite files into the cwd when
    _ensure_wal_mode / _ensure_chip_tables run.
    """
    from providers import SmartMoneyDBProvider

    mock_db = MagicMock()
    mock_db.db_path = db_path
    with patch("providers.DatabaseManager", return_value=mock_db):
        return SmartMoneyDBProvider(db_path=db_path)


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
    provider.close()


def test_db_provider_get_distinct_codes():
    from providers import SmartMoneyDBProvider
    provider = SmartMoneyDBProvider()
    # DatabaseManager is globally mocked in conftest, patch its get_distinct_codes
    with patch.object(provider._db, "get_distinct_codes", return_value={"000001.SZ", "600000.SH"}):
        result = provider.get_distinct_codes("daily_bars")
        assert isinstance(result, set)
        assert result == {"000001.SZ", "600000.SH"}


def test_db_provider_count_fundamentals_for_date():
    from providers import SmartMoneyDBProvider
    provider = SmartMoneyDBProvider()
    # DatabaseManager is globally mocked in conftest, patch the method
    with patch.object(provider._db, "count_fundamentals_for_date", return_value=42):
        result = provider.count_fundamentals_for_date("2026-07-01")
        assert isinstance(result, int)
        assert result == 42


def test_db_provider_record_task_run():
    from providers import SmartMoneyDBProvider
    provider = SmartMoneyDBProvider()
    provider.record_task_run("test_task", "2026-07-01")


def test_db_provider_get_last_task_run():
    from providers import SmartMoneyDBProvider
    provider = SmartMoneyDBProvider()
    # DatabaseManager is globally mocked in conftest, patch the method
    with patch.object(provider._db, "get_last_task_run", return_value=None):
        result = provider.get_last_task_run("test_task")
        assert result is None
    with patch.object(provider._db, "get_last_task_run", return_value="2026-07-01"):
        result = provider.get_last_task_run("test_task")
        assert result == "2026-07-01"


def test_db_provider_save_stock_list():
    from providers import SmartMoneyDBProvider
    provider = SmartMoneyDBProvider()
    df = pd.DataFrame({"code": ["000001"], "name": ["平安银行"], "market": ["sz"], "industry": ["银行"]})
    provider.save_stock_list(df)


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


def test_db_provider_save_fund_flow_batch():
    from providers import SmartMoneyDBProvider
    provider = SmartMoneyDBProvider()
    with patch.object(provider._db, "save_fund_flow_batch", return_value=7) as mock_method:
        result = provider.save_fund_flow_batch([{"ts_code": "000001.SZ", "net_inflow": 1e6}])
        assert result == 7
        mock_method.assert_called_once()


def test_db_provider_save_margin_trading_batch():
    from providers import SmartMoneyDBProvider
    provider = SmartMoneyDBProvider()
    with patch.object(provider._db, "save_margin_trading_batch", return_value=8) as mock_method:
        result = provider.save_margin_trading_batch([{"ts_code": "000001.SZ", "margin_balance": 5e6}])
        assert result == 8
        mock_method.assert_called_once()


def test_db_provider_get_margin_trading():
    from providers import SmartMoneyDBProvider
    provider = SmartMoneyDBProvider()
    with patch.object(provider._db, "get_margin_trading", return_value={"margin_balance": 1e6}) as mock_method:
        result = provider.get_margin_trading("000001.SZ", date="2026-07-01")
        assert result is not None
        mock_method.assert_called_once()


def test_db_provider_save_dragon_tiger_batch():
    from providers import SmartMoneyDBProvider
    provider = SmartMoneyDBProvider()
    with patch.object(provider._db, "save_dragon_tiger_batch", return_value=9) as mock_method:
        result = provider.save_dragon_tiger_batch([{"ts_code": "000001.SZ", "buy_amount": 1e5}])
        assert result == 9
        mock_method.assert_called_once()


def test_db_provider_save_shareholder_count_batch():
    from providers import SmartMoneyDBProvider
    provider = SmartMoneyDBProvider()
    with patch.object(provider._db, "save_shareholder_count_batch", return_value=10) as mock_method:
        result = provider.save_shareholder_count_batch([{"ts_code": "000001.SZ", "holder_count": 50000}])
        assert result == 10
        mock_method.assert_called_once()


def test_db_provider_save_fundamentals_batch():
    from providers import SmartMoneyDBProvider
    provider = SmartMoneyDBProvider()
    with patch.object(provider._db, "save_fundamentals_batch", return_value=15) as mock_method:
        result = provider.save_fundamentals_batch([{"ts_code": "000001.SZ", "pe": 10}])
        assert result == 15
        mock_method.assert_called_once()


def test_db_provider_save_quarterly_financials_batch():
    from providers import SmartMoneyDBProvider
    provider = SmartMoneyDBProvider()
    with patch.object(provider._db, "save_quarterly_financials_batch", return_value=11) as mock_method:
        result = provider.save_quarterly_financials_batch([{"ts_code": "000001.SZ", "revenue": 1e10}])
        assert result == 11
        mock_method.assert_called_once()


def test_db_provider_save_block_trade_batch():
    from providers import SmartMoneyDBProvider
    provider = SmartMoneyDBProvider()
    with patch.object(provider._db, "save_block_trade_batch", return_value=12) as mock_method:
        result = provider.save_block_trade_batch([{"ts_code": "000001.SZ", "price": 10.5}])
        assert result == 12
        mock_method.assert_called_once()


def test_db_provider_save_sector_fund_flow_batch():
    from providers import SmartMoneyDBProvider
    provider = SmartMoneyDBProvider()
    with patch.object(provider._db, "save_sector_fund_flow_batch", return_value=13) as mock_method:
        result = provider.save_sector_fund_flow_batch([{"sector_name": "银行", "net_inflow": 1e6}])
        assert result == 13
        mock_method.assert_called_once()


def test_db_provider_save_us_treasury_batch():
    from providers import SmartMoneyDBProvider
    provider = SmartMoneyDBProvider()
    with patch.object(provider._db, "save_us_treasury_batch", return_value=14) as mock_method:
        result = provider.save_us_treasury_batch([{"trade_date": "2024-01-01", "us_10y": 4.5}])
        assert result == 14
        mock_method.assert_called_once()


def test_db_provider_chip_distribution_methods():
    from providers import SmartMoneyDBProvider
    provider = SmartMoneyDBProvider()

    with patch.object(provider._db, "save_chip_distribution") as mock_save:
        provider.save_chip_distribution("000001.SZ", {"profit_ratio": 0.5})
        mock_save.assert_called_once()

    with patch.object(provider._db, "get_chip_distribution", return_value={"profit_ratio": 0.5}) as mock_get:
        result = provider.get_chip_distribution("000001.SZ", date="2026-07-01")
        assert result is not None
        mock_get.assert_called_once()

    with patch.object(provider._db, "get_chip_distribution_batch", return_value={"000001.SZ": {"profit_ratio": 0.5}}) as mock_batch:
        result = provider.get_chip_distribution_batch(["000001.SZ"])
        assert result is not None
        mock_batch.assert_called_once()


def test_db_provider_save_chip_distribution_batch():
    import os
    import sqlite3
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")
        open(db_path, "w").close()

        provider = _make_provider_with_real_db_path(db_path)

        # Create the table that save_chip_distribution_batch writes to
        with sqlite3.connect(db_path) as conn:
            conn.execute("""
                CREATE TABLE chip_distribution (
                    ts_code TEXT, trade_date TEXT, profit_ratio REAL, avg_cost REAL,
                    cost_90_low REAL, cost_90_high REAL, concentration_90 REAL,
                    cost_70_low REAL, cost_70_high REAL, concentration_70 REAL,
                    chip_concentration REAL
                )
            """)

        records = [
            {
                "ts_code": "000001.SZ",
                "trade_date": "2026-07-01",
                "profit_ratio": 0.5,
                "avg_cost": 10.0,
                "cost_90_low": 9.0,
                "cost_90_high": 11.0,
                "concentration_90": 0.1,
                "cost_70_low": 9.5,
                "cost_70_high": 10.5,
                "concentration_70": 0.05,
                "chip_concentration": 0.02,
            }
        ]
        result = provider.save_chip_distribution_batch(records)
        assert result >= 0


def test_db_provider_save_chip_distribution_batch_empty_and_exception():
    import os
    import sqlite3
    import tempfile
    from unittest.mock import patch

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")
        open(db_path, "w").close()

        provider = _make_provider_with_real_db_path(db_path)

        # Empty records path
        assert provider.save_chip_distribution_batch([]) == 0

        # Exception path
        with patch("providers.sqlite3.connect", side_effect=sqlite3.OperationalError("mocked")):
            result = provider.save_chip_distribution_batch([{"ts_code": "000001.SZ", "trade_date": "2026-07-01"}])
            assert result == 0


def test_db_provider_ensure_wal_mode():
    import os
    import sqlite3
    import tempfile
    from unittest.mock import patch

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")
        open(db_path, "w").close()

        provider = _make_provider_with_real_db_path(db_path)

        # Happy path: real sqlite3 connection executes the PRAGMA lines
        provider._ensure_wal_mode()

        # Exception path: sqlite3.connect raises, exception is swallowed
        with patch("providers.sqlite3.connect", side_effect=sqlite3.OperationalError("mocked")):
            provider._ensure_wal_mode()

        assert provider is not None


def test_db_provider_ensure_wal_mode_early_return():
    from unittest.mock import MagicMock, patch

    from providers import SmartMoneyDBProvider

    # Parent directory does not exist -> early return branch
    mock_db = MagicMock()
    mock_db.db_path = "/nonexistent_dir/test.db"
    with patch("providers.DatabaseManager", return_value=mock_db):
        provider = SmartMoneyDBProvider(db_path="/nonexistent_dir/test.db")
        provider._ensure_wal_mode()
        provider._ensure_chip_tables()


def test_db_provider_ensure_chip_tables_exception():
    import os
    import sqlite3
    import tempfile
    from unittest.mock import patch

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")
        open(db_path, "w").close()

        provider = _make_provider_with_real_db_path(db_path)

        with patch("providers.sqlite3.connect", side_effect=sqlite3.OperationalError("mocked")):
            provider._ensure_chip_tables()


def test_loader_provider_with_params():
    from providers import SmartMoneyLoaderProvider
    provider = SmartMoneyLoaderProvider(use_cache=False)
    result = provider.get_daily_bars("000001.SZ", start_date="20240101", end_date="20240131")
    assert result is not None
