"""Tests for providers.py with mocked dependencies."""
from __future__ import annotations

import sqlite3
from unittest.mock import patch

import pandas as pd
import pytest


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
    # 单条事件保存已改走本地 keyed-UPSERT 批量路径：缺 trade_date → 安全 no-op 返回 0
    assert provider.save_dragon_tiger("000001.SZ", {"buy_amount": 100000}) == 0
    assert provider.save_block_trade("000001.SZ", {"price": 10.5}) == 0
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
    result = provider.save_hk_tech_index_batch([{"trade_date": "2024-01-01", "close": 4400.0}])
    assert result is not None


def test_hk_tech_index_batch_roundtrip_and_idempotent():
    from providers import SmartMoneyDBProvider

    provider = SmartMoneyDBProvider()
    rows = [
        {"trade_date": "2026-09-17", "open": 4350.0, "high": 4360.0, "low": 4340.0,
         "close": 4355.0, "change_pct": -1.15, "volume": 2.1e9, "amount": 5.9e10,
         "data_source": "akshare_sina_hk"},
        {"trade_date": "2026-09-18", "open": 4413.11, "high": 4420.0, "low": 4405.5,
         "close": 4415.0, "change_pct": 1.38, "volume": 1.9e9, "amount": 5.7e10,
         "data_source": "akshare_sina_hk"},
    ]
    assert provider.save_hk_tech_index_batch(rows) == 2
    assert provider.get_hk_tech_latest_date() == "2026-09-18"
    # UNIQUE(trade_date) + INSERT OR REPLACE：重复写入不产生新行
    provider.save_hk_tech_index_batch(rows)
    with sqlite3.connect(provider.db_path) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM hk_tech_index_daily "
            "WHERE trade_date IN ('2026-09-17', '2026-09-18')"
        ).fetchone()[0]
    assert count == 2
    provider.close()


def test_us_macro_batch_persists_dgs2_icsa():
    from providers import SmartMoneyDBProvider

    provider = SmartMoneyDBProvider()
    provider.save_us_macro_batch([
        {"trade_date": "2026-09-15", "effr": 3.63, "dgs2": 3.87, "dgs3mo": 3.71,
         "dgs10": 4.22, "t10yie": 2.36, "icsa": 205000,
         "spread_10y_3m": 0.51, "real_rate_10y": 1.86, "data_source": "fred"},
    ])
    with sqlite3.connect(provider.db_path) as conn:
        row = conn.execute(
            "SELECT dgs2, icsa FROM us_macro_daily WHERE trade_date = '2026-09-15'"
        ).fetchone()
    assert row == (3.87, 205000)
    provider.close()


def test_new_batch_save_methods_return_per_call_change_count():
    from providers import SmartMoneyDBProvider

    provider = SmartMoneyDBProvider()

    first = provider.save_south_flow_batch(
        [{"trade_date": "2026-07-19", "market": "港股通", "net_buy_amount": 1.0}]
    )
    second = provider.save_ah_premium_batch(
        [{"trade_date": "2026-07-19", "ts_code": "000001", "name": "平安银行"}]
    )

    assert first == 1
    assert second == 1


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


def test_loader_provider_forwards_use_cache_to_data_loader():
    """use_cache 必须透传到本地 DataLoader 适配层（收盘刷新依赖非缓存构造）。"""
    import providers

    captured: list[bool] = []

    class RecordingDataLoader:
        def __init__(self, use_cache: bool = True):
            captured.append(use_cache)

    with patch.object(providers, "DataLoader", RecordingDataLoader):
        providers.SmartMoneyLoaderProvider(use_cache=False)
        providers.SmartMoneyLoaderProvider()

    assert captured == [False, True]


# ===========================================================================
# 事件写清理（post-merge review #5）：单条方法改走 keyed-UPSERT 批量路径；
# 批量写的结构性 DB 错误必须外抛（真临时库，同 test_market_flow 事件表 fixture）
# ===========================================================================


def _make_event_provider(tmp_path):
    """真临时 SQLite 库上的 SmartMoneyDBProvider（conftest 会 mock DatabaseManager，
    需显式重绑 db_path 后重跑建表与迁移，同 test_market_flow 的事件表 fixture）。"""
    from providers import SmartMoneyDBProvider

    db_path = tmp_path / "event_write_test.db"
    provider = SmartMoneyDBProvider(db_path=str(db_path))
    provider._db.db_path = str(db_path)
    provider._ensure_wal_mode()
    provider._ensure_tables()
    provider._run_versioned_migrations()
    return provider


def _dragon_record() -> dict:
    return {
        "trade_date": "2026-07-21",
        "close_price": 10.0,
        "pct_change": 1.0,
        "net_buy_amount": 5.0,
        "buy_amount": 8.0,
        "sell_amount": 3.0,
        "turnover_rate": 2.0,
        "market_cap": 100.0,
        "reason": "涨幅偏离",
        "data_source": "akshare",
    }


def _block_record() -> dict:
    return {
        "trade_date": "2026-07-21",
        "deal_price": 10.0,
        "close_price": 9.5,
        "discount_rate": -5.0,
        "volume": 100.0,
        "amount": 1000.0,
        "buyer_branch": "A",
        "seller_branch": "B",
        "data_source": "akshare",
    }


def test_save_dragon_tiger_singular_persists_via_batch_path(tmp_path):
    """单条 save_dragon_tiger 走本地批量 UPSERT：真实落库、生成键、不再委托外部 _db。"""
    provider = _make_event_provider(tmp_path)
    try:
        assert provider.save_dragon_tiger("600000", _dragon_record()) == 1
        provider._db.save_dragon_tiger.assert_not_called()
        with sqlite3.connect(provider.db_path) as conn:
            rows = conn.execute(
                "SELECT ts_code, source_record_key FROM dragon_tiger"
            ).fetchall()
    finally:
        provider.close()
    assert len(rows) == 1
    assert rows[0][0] == "600000"  # symbol 映射为 ts_code
    assert rows[0][1]  # source_record_key 由键构建器生成，非空


def test_save_block_trade_singular_keeps_explicit_ts_code(tmp_path):
    """单条 save_block_trade：data 里显式 ts_code 优先于 symbol，不再委托外部 _db。"""
    provider = _make_event_provider(tmp_path)
    try:
        record = {**_block_record(), "ts_code": "000001"}
        assert provider.save_block_trade("600000", record) == 1
        provider._db.save_block_trade.assert_not_called()
        with sqlite3.connect(provider.db_path) as conn:
            rows = conn.execute(
                "SELECT ts_code, source_record_key FROM block_trade"
            ).fetchall()
    finally:
        provider.close()
    assert len(rows) == 1
    assert rows[0][0] == "000001"  # 显式 ts_code 不被 symbol 覆盖
    assert rows[0][1]


def test_save_event_singular_incomplete_data_is_noop(tmp_path):
    """缺 trade_date 的单条数据被批量路径过滤：返回 0、不落库、不抛错。"""
    provider = _make_event_provider(tmp_path)
    try:
        assert provider.save_dragon_tiger("600000", {"buy_amount": 1.0}) == 0
        assert provider.save_block_trade("600000", {"deal_price": 10.5}) == 0
        with sqlite3.connect(provider.db_path) as conn:
            dragon_count = conn.execute("SELECT COUNT(*) FROM dragon_tiger").fetchone()[0]
            block_count = conn.execute("SELECT COUNT(*) FROM block_trade").fetchone()[0]
    finally:
        provider.close()
    assert dragon_count == 0
    assert block_count == 0


def test_dragon_tiger_batch_integrity_error_propagates(tmp_path, monkeypatch):
    """source_record_key 为 NULL 触发 NOT NULL 约束 → IntegrityError 必须外抛，而非静默记 0。"""
    import providers as providers_module

    provider = _make_event_provider(tmp_path)
    try:
        monkeypatch.setattr(
            providers_module, "dragon_tiger_source_key", lambda _r: None
        )
        with pytest.raises(sqlite3.IntegrityError):
            provider.save_dragon_tiger_batch(
                [{**_dragon_record(), "ts_code": "600000"}]
            )
    finally:
        provider.close()


def test_block_trade_batch_integrity_error_propagates(tmp_path, monkeypatch):
    """大宗交易批量写同理：约束失败外抛，避免调用方误判为空结果。"""
    import providers as providers_module

    provider = _make_event_provider(tmp_path)
    try:
        monkeypatch.setattr(
            providers_module, "block_trade_source_key", lambda _r: None
        )
        with pytest.raises(sqlite3.IntegrityError):
            provider.save_block_trade_batch(
                [{**_block_record(), "ts_code": "600000"}]
            )
    finally:
        provider.close()


def test_event_batch_operational_error_propagates(tmp_path):
    """表结构损坏（缺表/缺列）→ OperationalError 外抛；正常批量仍返回保存数（回归）。"""
    provider = _make_event_provider(tmp_path)
    try:
        # 回归：合法批量返回保存条数
        assert provider.save_dragon_tiger_batch(
            [{**_dragon_record(), "ts_code": "600000"}]
        ) == 1
        assert provider.save_block_trade_batch(
            [{**_block_record(), "ts_code": "600000"}]
        ) == 1
        with sqlite3.connect(provider.db_path) as conn:
            conn.execute("DROP TABLE dragon_tiger")
            conn.execute("DROP TABLE block_trade")
            conn.commit()
        with pytest.raises(sqlite3.OperationalError):
            provider.save_dragon_tiger_batch(
                [{**_dragon_record(), "ts_code": "600000"}]
            )
        with pytest.raises(sqlite3.OperationalError):
            provider.save_block_trade_batch(
                [{**_block_record(), "ts_code": "600000"}]
            )
    finally:
        provider.close()
