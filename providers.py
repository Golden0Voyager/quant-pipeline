"""
SmartMoney Provider 实现
─────────────────────────
职责：把 smartmoney_hunter 的具体类包装成 pipeline_interface 的实现。

这是唯一依赖 smartmoney_hunter 内部类的文件。
如果 smartmoney_hunter 重构，只需要改这个文件，daily_pipeline 不动。
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import pandas as pd
from smartmoney_hunter.data_loader import DataLoader
from smartmoney_hunter.database import DatabaseManager
from smartmoney_hunter.indicators import IndicatorCalculator

# ===========================================================================
# Database Provider
# ===========================================================================

class SmartMoneyDBProvider:
    """基于 DatabaseManager 的数据库 provider。"""

    def __init__(self, db_path: str | None = None):
        self._db = DatabaseManager(db_path=db_path)
        self._ensure_wal_mode()

    def _ensure_wal_mode(self) -> None:
        """启用 WAL 模式以提升并发读写性能。"""
        db_path = self._db.db_path
        if not db_path or not Path(db_path).parent.exists():
            return
        try:
            with sqlite3.connect(str(db_path), timeout=5.0) as conn:
                cursor = conn.cursor()
                cursor.execute("PRAGMA journal_mode=WAL")
                cursor.execute("PRAGMA synchronous=NORMAL")
        except Exception:
            # WAL 启用失败不应阻塞正常流程
            pass

    @property
    def db_path(self) -> str:
        return str(self._db.db_path)

    def close(self) -> None:
        self._db.close()

    def get_distinct_codes(self, table: str, column: str = "ts_code") -> set[str]:
        return self._db.get_distinct_codes(table, column)

    def count_fundamentals_for_date(self, trade_date: str) -> int:
        return self._db.count_fundamentals_for_date(trade_date)

    def record_task_run(self, task_name: str, run_date: str) -> None:
        self._db.record_task_run(task_name, run_date)

    def get_last_task_run(self, task_name: str) -> str | None:
        return self._db.get_last_task_run(task_name)

    def get_stock_list(self) -> pd.DataFrame:
        return self._db.get_stock_list()

    def save_stock_list(self, df: pd.DataFrame) -> None:
        self._db.save_stock_list(df)

    def get_daily_bars(self, symbol: str) -> pd.DataFrame:
        return self._db.get_daily_bars(symbol)

    def save_daily_bars(self, symbol: str, df: pd.DataFrame) -> None:
        self._db.save_daily_bars(symbol, df)

    def save_indicators(self, symbol: str, df: pd.DataFrame) -> None:
        self._db.save_indicators(symbol, df)

    def save_fundamentals(self, symbol: str, data: dict[str, Any]) -> None:
        self._db.save_fundamentals(symbol, data)

    def save_fundamentals_batch(self, records: list[dict[str, Any]]) -> int:
        return self._db.save_fundamentals_batch(records)

    def save_fund_flow(self, symbol: str, data: dict[str, Any]) -> None:
        self._db.save_fund_flow(symbol, data)

    def save_fund_flow_batch(self, records: list[dict[str, Any]]) -> int:
        return self._db.save_fund_flow_batch(records)

    def save_margin_trading(self, symbol: str, data: dict[str, Any]) -> None:
        self._db.save_margin_trading(symbol, data)

    def save_margin_trading_batch(self, records: list[dict[str, Any]]) -> int:
        return self._db.save_margin_trading_batch(records)

    def get_margin_trading(self, symbol: str, date: str = None) -> dict | None:
        return self._db.get_margin_trading(symbol, date)

    def save_dragon_tiger(self, symbol: str, data: dict[str, Any]) -> None:
        self._db.save_dragon_tiger(symbol, data)

    def save_dragon_tiger_batch(self, records: list[dict[str, Any]]) -> int:
        return self._db.save_dragon_tiger_batch(records)

    def get_dragon_tiger(self, symbol: str, date: str = None) -> dict | None:
        return self._db.get_dragon_tiger(symbol, date)

    def save_shareholder_count(self, symbol: str, data: dict[str, Any]) -> None:
        self._db.save_shareholder_count(symbol, data)

    def save_shareholder_count_batch(self, records: list[dict[str, Any]]) -> int:
        return self._db.save_shareholder_count_batch(records)

    def save_quarterly_financials(self, symbol: str, data: dict[str, Any]) -> None:
        self._db.save_quarterly_financials(symbol, data)

    def save_quarterly_financials_batch(self, records: list[dict[str, Any]]) -> int:
        return self._db.save_quarterly_financials_batch(records)

    def save_block_trade(self, symbol: str, data: dict[str, Any]) -> None:
        self._db.save_block_trade(symbol, data)

    def save_block_trade_batch(self, records: list[dict[str, Any]]) -> int:
        return self._db.save_block_trade_batch(records)

    def get_block_trade(self, symbol: str, date: str = None) -> dict | None:
        return self._db.get_block_trade(symbol, date)

    def save_sector_fund_flow(self, sector_name: str, data: dict[str, Any]) -> None:
        self._db.save_sector_fund_flow(sector_name, data)

    def save_sector_fund_flow_batch(self, records: list[dict[str, Any]]) -> int:
        return self._db.save_sector_fund_flow_batch(records)

    def get_sector_fund_flow(self, sector_name: str, date: str = None) -> dict | None:
        return self._db.get_sector_fund_flow(sector_name, date)

    def save_historical_valuation(self, symbol: str, trade_date: str, data: dict[str, Any]) -> None:
        self._db.save_historical_valuation(symbol, trade_date, data)

    def save_sector_industry(self, data: dict[str, Any]) -> None:
        self._db.save_sector_industry(data)

    def get_sector_industry(self, industry_name: str, trade_date: str = None) -> dict | None:
        return self._db.get_sector_industry(industry_name, trade_date)

    def get_fundamentals_batch(self, trade_date: str = None) -> pd.DataFrame:
        return self._db.get_fundamentals_batch(trade_date)

    def save_north_flow_batch(self, records: list[dict[str, Any]]) -> int:
        return self._db.save_north_flow_batch(records)

    def save_index_daily_batch(self, records: list[dict[str, Any]]) -> int:
        return self._db.save_index_daily_batch(records)

    def save_limit_up_down_batch(self, records: list[dict[str, Any]]) -> int:
        return self._db.save_limit_up_down_batch(records)

    def save_dividend_summary_batch(self, records: list[dict[str, Any]]) -> int:
        return self._db.save_dividend_summary_batch(records)

    def save_gold_price_batch(self, records: list[dict[str, Any]]) -> int:
        return self._db.save_gold_price_batch(records)

    def save_crude_oil_batch(self, records: list[dict[str, Any]]) -> int:
        return self._db.save_crude_oil_batch(records)

    def save_usd_batch(self, records: list[dict[str, Any]]) -> int:
        return self._db.save_usd_batch(records)

    def save_global_index_batch(self, records: list[dict[str, Any]]) -> int:
        return self._db.save_global_index_batch(records)

    def save_us_treasury_batch(self, records: list[dict[str, Any]]) -> int:
        return self._db.save_us_treasury_batch(records)

    def watchlist_get_all(self, status: str = None) -> pd.DataFrame:
        return self._db.watchlist_get_all(status)



# ===========================================================================
# DataLoader Provider
# ===========================================================================

class SmartMoneyLoaderProvider:
    """基于 DataLoader 的数据加载 provider。"""

    def __init__(self, use_cache: bool = True):
        self._loader = DataLoader(use_cache=use_cache)

    def get_daily_bars(
        self,
        symbol: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        return self._loader.get_daily_bars(symbol, start_date, end_date)

    def incremental_update(
        self,
        symbol: str,
        existing_df: pd.DataFrame,
    ) -> pd.DataFrame:
        return self._loader.incremental_update(symbol, existing_df)

    def get_market_valuation(self) -> pd.DataFrame:
        return self._loader.get_market_valuation()

    def get_market_fund_flow(self) -> pd.DataFrame:
        return self._loader.get_market_fund_flow()


# ===========================================================================
# Indicator Engine Provider
# ===========================================================================

class SmartMoneyIndicatorProvider:
    """基于 IndicatorCalculator 的指标计算 provider。"""

    def __init__(self):
        self._calc = IndicatorCalculator()

    def calculate_all_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        return self._calc.calculate_all_indicators(df)
