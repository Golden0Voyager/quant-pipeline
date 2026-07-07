"""
数据管道抽象接口层
────────────────────
职责：定义 daily_pipeline 所需的所有抽象接口。
不依赖任何具体实现（不 import smartmoney_hunter 内部类）。

设计原则：
  - 接口即契约：daily_pipeline 只认接口，不认实现
  - 依赖倒置：具体类通过 adapter 包装后注入 pipeline
  - 可替换性：未来换数据源/换数据库只需新增 provider
"""
from __future__ import annotations

from typing import Any, Protocol

import pandas as pd

# ===========================================================================
# 数据库接口
# ===========================================================================

class DatabaseInterface(Protocol):
    """数据库操作抽象接口。"""

    @property
    def db_path(self) -> str:
        """返回数据库文件路径（字符串）。"""
        ...

    def close(self) -> None:
        """释放数据库连接资源。"""
        ...

    def get_distinct_codes(self, table: str, column: str = "ts_code") -> set[str]:
        """获取指定表中某列的不重复值，用于增量检测。"""
        ...

    def count_fundamentals_for_date(self, trade_date: str) -> int:
        """获取 fundamentals 表某交易日的记录数，用于增量检测。"""
        ...

    def record_task_run(self, task_name: str, run_date: str) -> None:
        """记录任务成功运行日期。"""
        ...

    def get_last_task_run(self, task_name: str) -> str | None:
        """获取任务最近一次成功运行的日期。"""
        ...

    def get_stock_list(self) -> pd.DataFrame:
        """获取全部股票列表，返回 DataFrame（至少包含 'code' 列）。"""
        ...

    def get_daily_bars(self, symbol: str) -> pd.DataFrame:
        """获取单只股票的日线数据。"""
        ...

    def save_daily_bars(self, symbol: str, df: pd.DataFrame) -> None:
        """保存日线数据。"""
        ...

    def save_indicators(self, symbol: str, df: pd.DataFrame) -> None:
        """保存技术指标数据。"""
        ...

    def save_fundamentals(self, symbol: str, data: dict[str, Any]) -> None:
        """保存基本面/估值数据。"""
        ...

    def save_fund_flow(self, symbol: str, data: dict[str, Any]) -> None:
        """保存资金流向数据。"""
        ...

    def save_margin_trading(self, symbol: str, data: dict[str, Any]) -> None:
        """保存融资融券数据。"""
        ...

    def get_margin_trading(self, symbol: str, date: str = None) -> dict | None:
        """获取融资融券数据。"""
        ...

    def save_dragon_tiger(self, symbol: str, data: dict[str, Any]) -> None:
        """保存龙虎榜数据。"""
        ...

    def get_dragon_tiger(self, symbol: str, date: str = None) -> dict | None:
        """获取龙虎榜数据。"""
        ...

    def save_shareholder_count(self, symbol: str, data: dict[str, Any]) -> None:
        """保存股东户数数据。"""
        ...

    def save_quarterly_financials(self, symbol: str, data: dict[str, Any]) -> None:
        """保存季度财务指标数据。"""
        ...

    def save_block_trade(self, symbol: str, data: dict[str, Any]) -> None:
        """保存大宗交易数据。"""
        ...

    def get_block_trade(self, symbol: str, date: str = None) -> dict | None:
        """获取大宗交易数据。"""
        ...

    def save_sector_fund_flow(self, sector_name: str, data: dict[str, Any]) -> None:
        """保存板块资金流向数据。"""
        ...

    def get_sector_fund_flow(self, sector_name: str, date: str = None) -> dict | None:
        """获取板块资金流向数据。"""
        ...

    def save_historical_valuation(self, symbol: str, trade_date: str, data: dict[str, Any]) -> None:
        """保存历史估值快照。"""
        ...

    def save_sector_industry(self, data: dict[str, Any]) -> None:
        """保存行业对比数据。"""
        ...

    def get_sector_industry(self, industry_name: str, trade_date: str = None) -> dict | None:
        """获取行业对比数据。"""
        ...

    def get_fundamentals_batch(self, trade_date: str = None) -> pd.DataFrame:
        """批量获取基本面数据。"""
        ...

    def watchlist_get_all(self, status: str = None) -> pd.DataFrame:
        """获取所有自选股列表。"""
        ...



# ===========================================================================
# 数据加载器接口
# ===========================================================================

class DataLoaderInterface(Protocol):
    """数据加载器抽象接口。"""

    def get_daily_bars(
        self,
        symbol: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        """获取日线 OHLCV 数据。"""
        ...

    def incremental_update(
        self,
        symbol: str,
        existing_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """增量更新：下载本地缺失的日期数据。"""
        ...

    def get_market_valuation(self) -> pd.DataFrame:
        """批量获取全市场估值数据。"""
        ...

    def get_market_fund_flow(self) -> pd.DataFrame:
        """批量获取全市场资金流向数据。"""
        ...


# ===========================================================================
# 指标计算引擎接口
# ===========================================================================

class IndicatorEngineInterface(Protocol):
    """技术指标计算引擎抽象接口。"""

    def calculate_all_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """基于 OHLCV 数据计算全部技术指标。"""
        ...


# ===========================================================================
# Provider 工厂（配置化入口）
# ===========================================================================

class ProviderFactory:
    """
    根据配置创建具体 provider 的工厂。

    当前只支持 'smartmoney' provider，未来可扩展：
      - tushare:  接入 Tushare 数据源
      - jqdata:   接入 JoinQuant 数据源
      - akdirect: 直接调用 akshare（不经过 smartmoney_hunter 封装）
    """

    _db_provider: DatabaseInterface = None
    _loader_provider: DataLoaderInterface = None
    _indicator_provider: IndicatorEngineInterface = None

    @classmethod
    def configure(
        cls,
        db_path: str | None = None,
        provider: str = "smartmoney",
    ) -> None:
        """配置全局 provider。"""
        if provider == "smartmoney":
            from providers import (
                SmartMoneyDBProvider,
                SmartMoneyIndicatorProvider,
                SmartMoneyLoaderProvider,
            )
            cls._db_provider = SmartMoneyDBProvider(db_path=db_path)
            cls._loader_provider = SmartMoneyLoaderProvider()
            cls._indicator_provider = SmartMoneyIndicatorProvider()
        else:
            raise ValueError(f"Unknown provider: {provider}")

    @classmethod
    def get_db(cls) -> DatabaseInterface:
        if cls._db_provider is None:
            raise RuntimeError("Provider not configured. Call ProviderFactory.configure() first.")
        return cls._db_provider

    @classmethod
    def get_loader(cls) -> DataLoaderInterface:
        if cls._loader_provider is None:
            raise RuntimeError("Provider not configured. Call ProviderFactory.configure() first.")
        return cls._loader_provider

    @classmethod
    def get_indicator_engine(cls) -> IndicatorEngineInterface:
        if cls._indicator_provider is None:
            raise RuntimeError("Provider not configured. Call ProviderFactory.configure() first.")
        return cls._indicator_provider
