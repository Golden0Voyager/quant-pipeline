"""Coverage for the extended batch save methods on ``SmartMoneyDBProvider``.

These methods back the new data tasks (macro / convertible bond / corporate
actions / finance flow / sector derivatives / chip distribution / historical
valuation). They are exercised against the real temp database set up by
``conftest`` so the INSERT OR REPLACE paths are actually executed.
"""
from __future__ import annotations

from providers import SmartMoneyDBProvider


def _provider() -> SmartMoneyDBProvider:
    return SmartMoneyDBProvider()


def test_extended_macro_batch_saves():
    p = _provider()
    m = p.save_macro_monthly_batch(
        [{"date": "2026-07-01", "cpi_yoy": 0.2, "data_date": "2026-07-20"}]
    )
    assert isinstance(m, int) and m >= 1
    q = p.save_macro_quarterly_batch(
        [{"date": "2026-Q2", "gdp": 300000.0, "data_date": "2026-07-20"}]
    )
    assert isinstance(q, int) and q >= 1


def test_extended_cb_batch_saves():
    p = _provider()
    a = p.save_cb_quotation_batch(
        [
            {
                "ts_code": "113050", "bond_name": "X", "price": 120.0, "premium": 5.0,
                "double_low": 125.0, "expire_date": "2028-01-01", "data_source": "akshare",
            }
        ]
    )
    assert a >= 1
    b = p.save_cb_redeem_batch(
        [
            {
                "ts_code": "113050", "bond_name": "X", "redeem_flag": "Y",
                "redeem_price": 100.0, "redeem_date": "2026-08-01", "data_source": "akshare",
            }
        ]
    )
    assert b >= 1
    c = p.save_cb_index_batch(
        [
            {
                "trade_date": "2026-07-20", "index_code": "000832", "index_name": "中证转债",
                "open": 400.0, "close": 401.0, "high": 402.0, "low": 399.0, "volume": 1e6,
                "data_source": "akshare",
            }
        ]
    )
    assert c >= 1


def test_extended_etf_and_restricted_and_forecast():
    p = _provider()
    e = p.save_etf_daily_batch(
        [
            {
                "ts_code": "510050", "name": "50ETF", "trade_date": "2026-07-20",
                "open": 2.6, "high": 2.7, "low": 2.5, "close": 2.65, "volume": 1e8,
                "amount": 2.6e8, "data_source": "akshare",
            }
        ]
    )
    assert e >= 1
    r = p.save_restricted_share_batch(
        [
            {
                "ts_code": "000001", "name": "平安银行", "release_date": "2026-07-20",
                "actual_release": 100.5, "total_shares": 200.5, "market_type": "深市",
                "data_source": "akshare",
            }
        ]
    )
    assert r >= 1
    f = p.save_earnings_forecast_batch(
        [
            {
                "ts_code": "000001", "name": "平安银行", "end_date": "2026-06-30",
                "forecast_type": "预增", "net_profit_change": "50%", "previous_profit": "10亿",
                "data_source": "akshare",
            }
        ]
    )
    assert f >= 1


def test_extended_sector_and_futures_basis():
    p = _provider()
    s = p.save_sector_daily_batch(
        [
            {
                "sector_name": "银行", "trade_date": "2026-07-20", "open": 100.0,
                "close": 101.0, "high": 102.0, "low": 99.0, "volume": 1e6, "amount": 1e8,
                "pct_change": 1.0, "data_source": "akshare",
            }
        ]
    )
    assert s >= 1
    v = p.save_sector_valuation_batch(
        [
            {
                "sector_name": "银行", "trade_date": "2026-07-20", "pe": 6.1, "pb": 0.7,
                "total_mv": 123456.0, "data_source": "akshare",
            }
        ]
    )
    assert v >= 1
    b = p.save_index_futures_basis_batch(
        [
            {
                "trade_date": "2026-07-20", "futures_code": "IF0", "futures_price": 3500.0,
                "index_price": 3490.0, "basis": 10.0, "basis_pct": 0.2865, "data_source": "akshare",
            }
        ]
    )
    assert b >= 1


def test_extended_chip_distribution_and_historical_valuation():
    import sqlite3

    p = _provider()
    # SmartMoneyDBProvider._ensure_tables() does not create historical_valuation,
    # so provision it here against the temp database used by conftest.
    conn = sqlite3.connect(p.db_path)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS historical_valuation ("
        "ts_code TEXT, trade_date TEXT, pe_ttm REAL, pb REAL, ps_ttm REAL, dividend_yield REAL)"
    )
    conn.commit()
    conn.close()
    c = p.save_chip_distribution_em_batch(
        [
            {
                "ts_code": "000001", "trade_date": "2026-07-20", "profit_ratio": 0.5,
                "avg_cost": 10.0, "cost_90_low": 9.0, "cost_90_high": 11.0,
                "concentration_90": 0.3, "cost_70_low": 9.5, "cost_70_high": 10.5,
                "concentration_70": 0.2,
            }
        ]
    )
    assert c >= 1
    # legacy chip_distribution table
    cl = p.save_chip_distribution_batch(
        [
            {
                "ts_code": "000001", "trade_date": "2026-07-20", "profit_ratio": 0.5,
                "avg_cost": 10.0, "cost_90_low": 9.0, "cost_90_high": 11.0,
                "concentration_90": 0.3, "cost_70_low": 9.5, "cost_70_high": 10.5,
                "concentration_70": 0.2, "chip_concentration": 0.9,
            }
        ]
    )
    assert cl >= 1
    h = p.save_historical_valuation_batch(
        [
            {
                "ts_code": "000001", "trade_date": "2026-06-30", "pe_ttm": 10.0,
                "pb": 1.5, "ps_ttm": 2.0, "dividend_yield": 0.03,
            }
        ]
    )
    assert h >= 1
    # rows missing required keys are dropped -> 0 changes
    assert p.save_historical_valuation_batch([{"pe_ttm": 10.0}]) == 0
    assert p.save_chip_distribution_em_batch([]) == 0
