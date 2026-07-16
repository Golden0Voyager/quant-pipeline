"""
SmartMoney Provider 实现
─────────────────────────
职责：把 smartmoney_hunter 的具体类包装成 pipeline_interface 的实现。

这是唯一依赖 smartmoney_hunter 内部类的文件。
如果 smartmoney_hunter 重构，只需要改这个文件，daily_pipeline 不动。
"""
from __future__ import annotations

import logging
import sqlite3
import threading
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
        self._ensure_tables()
        # 共享写连接 + 写锁：所有 batch 写入串行化，避免并发写导致 database is locked
        self._write_lock = threading.Lock()
        self._write_conn: sqlite3.Connection | None = None

    def _get_write_conn(self) -> sqlite3.Connection:
        """复用单个写连接（类似 DatabaseManager._connect_for_write）。

        必须在 self._write_lock 保护下使用。
        """
        if self._write_conn is None:
            conn = sqlite3.connect(str(self._db.db_path), timeout=30.0)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=30000")
            self._write_conn = conn
        return self._write_conn

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

    def _ensure_tables(self) -> None:
        """兜底 DDL：确保管道依赖的表存在。

        即使外部 _init_database 因并发/中断未能执行全部 DDL，
        这里保证 stock_list / fundamentals / chip_distribution 及其变体存在。
        """
        try:
            with sqlite3.connect(str(self._db.db_path), timeout=5.0) as conn:
                # chip_distribution_em（原有）
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS chip_distribution_em (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        ts_code TEXT NOT NULL,
                        trade_date DATE NOT NULL,
                        profit_ratio REAL,
                        avg_cost REAL,
                        cost_90_low REAL,
                        cost_90_high REAL,
                        concentration_90 REAL,
                        cost_70_low REAL,
                        cost_70_high REAL,
                        concentration_70 REAL,
                        chip_concentration REAL,
                        UNIQUE(ts_code, trade_date)
                    )
                """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_chip_distribution_em_code_date
                    ON chip_distribution_em(ts_code, trade_date DESC)
                """)
                # chip_distribution
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS chip_distribution (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        ts_code TEXT NOT NULL,
                        trade_date DATE NOT NULL,
                        profit_ratio REAL,
                        avg_cost REAL,
                        cost_90_low REAL,
                        cost_90_high REAL,
                        concentration_90 REAL,
                        cost_70_low REAL,
                        cost_70_high REAL,
                        concentration_70 REAL,
                        chip_concentration REAL,
                        UNIQUE(ts_code, trade_date)
                    )
                """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_chip_distribution_code_date
                    ON chip_distribution(ts_code, trade_date DESC)
                """)
                # stock_list
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS stock_list (
                        code TEXT PRIMARY KEY,
                        name TEXT NOT NULL,
                        market TEXT,
                        industry TEXT,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                # fundamentals——只补必要列，更多列在写入时由上层保障
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS fundamentals (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        ts_code TEXT NOT NULL,
                        trade_date DATE NOT NULL,
                        pe_ttm REAL,
                        pb REAL,
                        ps_ttm REAL,
                        dividend_yield REAL,
                        UNIQUE(ts_code, trade_date)
                    )
                """)
        except Exception:
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

    def get_latest_bar_date(self, symbol: str) -> str | None:
        """轻量查询：直接 SQL 取 MAX(trade_date)，避免全表扫描。"""
        try:
            with sqlite3.connect(str(self._db.db_path), timeout=5.0) as conn:
                cursor = conn.execute(
                    "SELECT MAX(trade_date) FROM daily_bars WHERE ts_code = ?",
                    (symbol,),
                )
                row = cursor.fetchone()
                return row[0] if row and row[0] else None
        except Exception:
            return None

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

    def save_chip_distribution(self, symbol: str, data: dict[str, Any]) -> None:
        self._db.save_chip_distribution(symbol, data)

    def get_chip_distribution(self, symbol: str, date: str | None = None) -> dict | None:
        return self._db.get_chip_distribution(symbol, date)

    def get_chip_distribution_batch(
        self, stock_list: list[str] | None = None
    ) -> dict[str, dict]:
        return self._db.get_chip_distribution_batch(stock_list)

    def save_chip_distribution_batch(self, records: list[dict[str, Any]]) -> int:
        """批量保存筹码分布数据（使用原始 SQL 因 DatabaseManager 可能缺少 batch 方法）。

        使用共享写连接 + 写锁，避免 ThreadPoolExecutor 并发写导致 database is locked。
        """
        if not records:
            return 0
        try:
            with self._write_lock:
                conn = self._get_write_conn()
                conn.executemany(
                    """
                    INSERT OR REPLACE INTO chip_distribution (
                        ts_code, trade_date, profit_ratio, avg_cost,
                        cost_90_low, cost_90_high, concentration_90,
                        cost_70_low, cost_70_high, concentration_70,
                        chip_concentration
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            r["ts_code"],
                            r["trade_date"],
                            r.get("profit_ratio"),
                            r.get("avg_cost"),
                            r.get("cost_90_low"),
                            r.get("cost_90_high"),
                            r.get("concentration_90"),
                            r.get("cost_70_low"),
                            r.get("cost_70_high"),
                            r.get("concentration_70"),
                            r.get("chip_concentration"),
                        )
                        for r in records
                    ],
                )
                conn.commit()
                return conn.total_changes
        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.warning(f"⚠️ 筹码分布批量保存失败: {e}")
            return 0

    def save_chip_distribution_em_batch(self, records: list[dict[str, Any]]) -> int:
        """批量保存东方财富筹码分布数据到 chip_distribution_em 表。"""
        if not records:
            return 0
        try:
            with self._write_lock:
                conn = self._get_write_conn()
                conn.executemany(
                    """
                    INSERT OR REPLACE INTO chip_distribution_em (
                        ts_code, trade_date, profit_ratio, avg_cost,
                        cost_90_low, cost_90_high, concentration_90,
                        cost_70_low, cost_70_high, concentration_70,
                        chip_concentration
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            r["ts_code"],
                            r["trade_date"],
                            r.get("profit_ratio"),
                            r.get("avg_cost"),
                            r.get("cost_90_low"),
                            r.get("cost_90_high"),
                            r.get("concentration_90"),
                            r.get("cost_70_low"),
                            r.get("cost_70_high"),
                            r.get("concentration_70"),
                            r.get("chip_concentration"),
                        )
                        for r in records
                    ],
                )
                conn.commit()
                return conn.total_changes
        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.warning(f"⚠️ 东方财富筹码分布批量保存失败: {e}")
            return 0

    def get_chip_distribution_em_batch(
        self, stock_list: list[str] | None = None
    ) -> dict[str, dict]:
        """批量获取多只股票最新东方财富筹码分布。"""
        return self._db.get_chip_distribution_em_batch(stock_list)



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
