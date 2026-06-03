"""
SmartMoney Provider 实现
─────────────────────────
职责：把 smartmoney_hunter 的具体类包装成 pipeline_interface 的实现。

这是唯一依赖 smartmoney_hunter 内部类的文件。
如果 smartmoney_hunter 重构，只需要改这个文件，daily_pipeline 不动。
"""
from __future__ import annotations

from typing import Dict, Any, Optional
import pandas as pd

from smartmoney_hunter.database import DatabaseManager
from smartmoney_hunter.data_loader import DataLoader
from smartmoney_hunter.indicators import IndicatorCalculator


# ===========================================================================
# Database Provider
# ===========================================================================

class SmartMoneyDBProvider:
    """基于 DatabaseManager 的数据库 provider。"""

    def __init__(self, db_path: Optional[str] = None):
        self._db = DatabaseManager(db_path=db_path)

    @property
    def db_path(self) -> str:
        return str(self._db.db_path)

    def get_stock_list(self) -> pd.DataFrame:
        return self._db.get_stock_list()

    def get_daily_bars(self, symbol: str) -> pd.DataFrame:
        return self._db.get_daily_bars(symbol)

    def save_daily_bars(self, symbol: str, df: pd.DataFrame) -> None:
        self._db.save_daily_bars(symbol, df)

    def save_indicators(self, symbol: str, df: pd.DataFrame) -> None:
        self._db.save_indicators(symbol, df)

    def save_fundamentals(self, symbol: str, data: Dict[str, Any]) -> None:
        self._db.save_fundamentals(symbol, data)

    def save_fund_flow(self, symbol: str, data: Dict[str, Any]) -> None:
        self._db.save_fund_flow(symbol, data)

    def save_margin_trading(self, symbol: str, data: Dict[str, Any]) -> None:
        self._db.save_margin_trading(symbol, data)

    def get_margin_trading(self, symbol: str, date: str = None) -> Optional[Dict]:
        return self._db.get_margin_trading(symbol, date)

    def save_dragon_tiger(self, symbol: str, data: Dict[str, Any]) -> None:
        self._db.save_dragon_tiger(symbol, data)

    def get_dragon_tiger(self, symbol: str, date: str = None) -> Optional[Dict]:
        return self._db.get_dragon_tiger(symbol, date)

    def save_block_trade(self, symbol: str, data: Dict[str, Any]) -> None:
        self._db.save_block_trade(symbol, data)

    def get_block_trade(self, symbol: str, date: str = None) -> Optional[Dict]:
        return self._db.get_block_trade(symbol, date)

    def save_sector_fund_flow(self, sector_name: str, data: Dict[str, Any]) -> None:
        self._db.save_sector_fund_flow(sector_name, data)

    def get_sector_fund_flow(self, sector_name: str, date: str = None) -> Optional[Dict]:
        return self._db.get_sector_fund_flow(sector_name, date)


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
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
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
