"""
SmartMoney Provider 实现
─────────────────────────
职责：把 smartmoney_hunter 的具体类包装成 pipeline_interface 的实现。

这是唯一依赖 smartmoney_hunter 内部类的文件。
如果 smartmoney_hunter 重构，只需要改这个文件，daily_pipeline 不动。
"""
from __future__ import annotations

import contextlib
import logging
import sqlite3
import threading
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from smartmoney_hunter.data_loader import DataLoader
from smartmoney_hunter.database import DatabaseManager
from smartmoney_hunter.indicators import IndicatorCalculator

logger = logging.getLogger(__name__)

# ===========================================================================
# Database Provider
# ===========================================================================

class SmartMoneyDBProvider:
    """基于 DatabaseManager 的数据库 provider。"""

    def __init__(self, db_path: str | None = None):
        self._db = DatabaseManager(db_path=db_path)
        self._ensure_wal_mode()
        self._ensure_tables()
        self._migrate_phase2_tables()
        # 共享写连接 + 写锁：所有 batch 写入串行化，避免并发写导致 database is locked
        self._write_lock = threading.Lock()
        self._write_conn: sqlite3.Connection | None = None

    def _get_write_conn(self) -> sqlite3.Connection:
        """复用单个写连接（类似 DatabaseManager._connect_for_write）。

        必须在 self._write_lock 保护下使用。
        """
        if self._write_conn is None:
            conn = sqlite3.connect(str(self._db.db_path), timeout=30.0, check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=30000")
            self._write_conn = conn
        return self._write_conn

    @staticmethod
    def _commit_delta(conn: sqlite3.Connection, before_changes: int) -> int:
        """提交事务并返回本次事务产生的变更数。"""
        conn.commit()
        return conn.total_changes - before_changes

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
                # macro_monthly
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS macro_monthly (
                        date TEXT PRIMARY KEY,
                        cpi_yoy REAL, cpi_mom REAL, cpi_core_yoy REAL,
                        ppi_yoy REAL, ppi_mom REAL,
                        pmi REAL, pmi_yoy REAL, pmi_monthly_change REAL, pmi_mom REAL,
                        pmi_caixin REAL,
                        m0 REAL, m1 REAL, m2 REAL, m0_yoy REAL, m1_yoy REAL, m2_yoy REAL,
                        new_loans REAL, new_loans_yoy REAL,
                        retail_sales_yoy REAL, retail_sales_ytd_yoy REAL,
                        fixed_asset_investment_yoy REAL, fixed_asset_investment_ytd_yoy REAL,
                        export_value REAL, export_yoy REAL, import_value REAL, import_yoy REAL,
                        industrial_production_yoy REAL, industrial_production_ytd_yoy REAL,
                        electricity_consumption_yoy REAL, electricity_consumption_total REAL,
                        enterprise_goods_price_yoy REAL, enterprise_goods_price_mom REAL,
                        consumer_confidence REAL, consumer_satisfaction REAL, consumer_expectation REAL,
                        lpr_1y REAL, lpr_5y REAL,
                        data_date TEXT
                    )
                """)
                # macro_quarterly
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS macro_quarterly (
                        date TEXT PRIMARY KEY,
                        gdp REAL, gdp_yoy REAL, gdp_qoq REAL,
                        gdp_primary REAL, gdp_secondary REAL, gdp_tertiary REAL,
                        data_date TEXT
                    )
                """)
                # macro_daily
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS macro_daily (
                        date TEXT PRIMARY KEY,
                        shibor_on REAL, shibor_1w REAL, shibor_2w REAL, shibor_1m REAL,
                        shibor_3m REAL, shibor_6m REAL, shibor_9m REAL, shibor_1y REAL,
                        data_date TEXT
                    )
                """)
                # money_market（SHIBOR + 回购利率 + 基准利率，日频）
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS money_market (
                        date TEXT PRIMARY KEY,
                        shibor_on REAL, shibor_1w REAL, shibor_2w REAL,
                        shibor_1m REAL, shibor_3m REAL, shibor_6m REAL,
                        shibor_9m REAL, shibor_1y REAL,
                        fr001 REAL, fr007 REAL, fr014 REAL,
                        pboc_policy_rate REAL,
                        data_date TEXT
                    )
                """)
                # central_bank_balance（央行资产负债表，月频）
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS central_bank_balance (
                        date TEXT PRIMARY KEY,
                        total_assets REAL, reserve_money REAL,
                        currency_issue REAL, claims_on_other_deposit REAL,
                        claims_on_gov REAL, gov_deposits REAL,
                        foreign_assets REAL, fx_reserve REAL,
                        data_date TEXT
                    )
                """)
                # market_valuation（大盘估值：PE/PB中位数+股债利差，日频）
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS market_valuation (
                        date TEXT PRIMARY KEY,
                        pe_median REAL,
                        pe_quantile REAL,
                        pe_lyr_median REAL,
                        pb_median REAL,
                        pb_quantile REAL,
                        equity_bond_spread REAL,
                        ebs_ma REAL,
                        csi300_close REAL,
                        data_source TEXT DEFAULT 'legu',
                        data_date TEXT
                    )
                """)
                # concept_board（概念板块日线行情）
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS concept_board (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        trade_date DATE NOT NULL,
                        concept_code TEXT NOT NULL,
                        concept_name TEXT,
                        pct_change REAL,
                        turnover REAL,
                        up_count INTEGER,
                        down_count INTEGER,
                        data_source TEXT DEFAULT 'ths',
                        UNIQUE(trade_date, concept_code)
                    )
                """)
                # concept_member（概念板块成分股映射）
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS concept_member (
                        concept_code TEXT NOT NULL,
                        concept_name TEXT,
                        ts_code TEXT NOT NULL,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(concept_code, ts_code)
                    )
                """)
                # south_flow
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS south_flow (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        trade_date DATE NOT NULL,
                        market TEXT,
                        net_buy_amount REAL,
                        buy_amount REAL,
                        sell_amount REAL,
                        cumulative_net_buy REAL,
                        data_source TEXT,
                        UNIQUE(trade_date, market)
                    )
                """)
                # north_hold（北向资金个股持仓，季度快照）
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS north_hold (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        ts_code TEXT NOT NULL,
                        security_name TEXT,
                        trade_date DATE NOT NULL,
                        close_price REAL,
                        hold_shares REAL,
                        hold_market_cap REAL,
                        hold_shares_ratio REAL,
                        free_shares_ratio REAL,
                        total_shares_ratio REAL,
                        data_source TEXT,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(ts_code, trade_date)
                    )
                """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_north_hold_code
                    ON north_hold(ts_code, trade_date DESC)
                """)
                # ah_premium
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS ah_premium (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        trade_date DATE NOT NULL,
                        ts_code TEXT,
                        h_code TEXT,
                        name TEXT,
                        a_price REAL,
                        h_price REAL,
                        premium REAL,
                        data_source TEXT,
                        UNIQUE(trade_date, ts_code)
                    )
                """)
                # cb_quotation
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS cb_quotation (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        ts_code TEXT,
                        bond_name TEXT,
                        price REAL,
                        premium REAL,
                        double_low REAL,
                        expire_date TEXT,
                        data_source TEXT,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(ts_code)
                    )
                """)
                # cb_redeem
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS cb_redeem (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        ts_code TEXT,
                        bond_name TEXT,
                        redeem_flag TEXT,
                        redeem_price REAL,
                        redeem_date TEXT,
                        data_source TEXT,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(ts_code)
                    )
                """)
                # migration: add updated_at to existing tables
                for tbl in ("cb_quotation", "cb_redeem"):
                    with contextlib.suppress(Exception):
                        conn.execute(f"ALTER TABLE {tbl} ADD COLUMN updated_at DATETIME DEFAULT CURRENT_TIMESTAMP")
                # cb_index
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS cb_index (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        trade_date DATE NOT NULL,
                        index_code TEXT,
                        index_name TEXT,
                        open REAL,
                        close REAL,
                        high REAL,
                        low REAL,
                        volume REAL,
                        data_source TEXT,
                        UNIQUE(trade_date, index_code)
                    )
                """)
                # etf_daily
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS etf_daily (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        ts_code TEXT,
                        name TEXT,
                        trade_date DATE NOT NULL,
                        open REAL,
                        high REAL,
                        low REAL,
                        close REAL,
                        volume REAL,
                        amount REAL,
                        data_source TEXT,
                        UNIQUE(ts_code, trade_date)
                    )
                """)
                # restricted_share
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS restricted_share (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        ts_code TEXT,
                        name TEXT,
                        release_date DATE,
                        actual_release REAL,
                        total_shares REAL,
                        market_type TEXT,
                        data_source TEXT,
                        UNIQUE(ts_code, release_date)
                    )
                """)
                # earnings_forecast
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS earnings_forecast (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        ts_code TEXT,
                        name TEXT,
                        end_date TEXT,
                        forecast_type TEXT,
                        net_profit_change REAL,
                        previous_profit REAL,
                        data_source TEXT,
                        UNIQUE(ts_code, end_date)
                    )
                """)
                # sector_daily
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS sector_daily (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        sector_name TEXT,
                        trade_date DATE NOT NULL,
                        open REAL,
                        close REAL,
                        high REAL,
                        low REAL,
                        volume REAL,
                        amount REAL,
                        pct_change REAL,
                        data_source TEXT,
                        UNIQUE(sector_name, trade_date)
                    )
                """)
                # sector_valuation
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS sector_valuation (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        sector_name TEXT,
                        trade_date DATE NOT NULL,
                        pe REAL,
                        pb REAL,
                        total_mv REAL,
                        data_source TEXT,
                        UNIQUE(sector_name, trade_date)
                    )
                """)
                # index_futures_basis
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS index_futures_basis (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        trade_date DATE NOT NULL,
                        futures_code TEXT,
                        futures_price REAL,
                        index_price REAL,
                        basis REAL,
                        basis_pct REAL,
                        data_source TEXT,
                        UNIQUE(trade_date, futures_code)
                    )
                """)
                # ==================== Phase 2: 期权情绪 ====================
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS option_sentiment (
                        trade_date TEXT PRIMARY KEY,
                        qvix REAL,
                        pcr REAL,
                        put_volume INTEGER,
                        call_volume INTEGER,
                        put_oi INTEGER,
                        call_oi INTEGER,
                        implied_vol_avg REAL
                    )
                """)
                # ==================== Phase 2: 股票回购 ====================
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS stock_repurchase (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        trade_date TEXT NOT NULL,
                        stock_code TEXT NOT NULL,
                        stock_name TEXT,
                        repurchase_amount REAL,
                        repurchase_price REAL,
                        repurchase_price_lower REAL,
                        repurchase_price_upper REAL,
                        repurchase_quantity INTEGER,
                        progress_status TEXT,
                        UNIQUE(trade_date, stock_code)
                    )
                """)
                # ==================== Phase 2: 增减持 ====================
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS insider_trading (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        trade_date TEXT,
                        stock_code TEXT,
                        stock_name TEXT,
                        changer_name TEXT,
                        change_type TEXT,
                        change_quantity INTEGER,
                        change_price REAL,
                        holdings_after_change REAL,
                        UNIQUE(trade_date, stock_code, changer_name, change_type)
                    )
                """)
                # ==================== Phase 2: 机构调研 ====================
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS institution_survey (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        trade_date TEXT NOT NULL,
                        stock_code TEXT NOT NULL,
                        stock_name TEXT,
                        survey_org TEXT,
                        survey_type TEXT,
                        survey_count INTEGER,
                        UNIQUE(trade_date, stock_code)
                    )
                """)
                # ==================== Phase 2: 股权质押 ====================
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS stock_pledge (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        trade_date TEXT,
                        stock_code TEXT,
                        stock_name TEXT,
                        pledger TEXT,
                        pledge_amount REAL,
                        pledge_ratio REAL,
                        pledge_org TEXT,
                        UNIQUE(trade_date, stock_code, pledger)
                    )
                """)
        except Exception as e:
            logger.warning(f"⚠️ _ensure_tables 创建表失败: {e}")

    def _migrate_phase2_tables(self) -> None:
        """独立执行 Phase 2 兼容迁移，避免被其他兜底 DDL 的异常阻断。"""
        try:
            with sqlite3.connect(str(self._db.db_path), timeout=5.0) as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS stock_repurchase (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        trade_date TEXT NOT NULL,
                        stock_code TEXT NOT NULL,
                        stock_name TEXT,
                        repurchase_amount REAL,
                        repurchase_price REAL,
                        repurchase_price_lower REAL,
                        repurchase_price_upper REAL,
                        repurchase_quantity INTEGER,
                        progress_status TEXT,
                        UNIQUE(trade_date, stock_code)
                    )
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS institution_survey (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        trade_date TEXT NOT NULL,
                        stock_code TEXT NOT NULL,
                        stock_name TEXT,
                        survey_org TEXT,
                        survey_type TEXT,
                        survey_count INTEGER,
                        UNIQUE(trade_date, stock_code)
                    )
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS stock_pledge (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        trade_date TEXT,
                        stock_code TEXT,
                        stock_name TEXT,
                        pledger TEXT,
                        pledge_amount REAL,
                        pledge_ratio REAL,
                        pledge_org TEXT,
                        UNIQUE(trade_date, stock_code, pledger)
                    )
                """)
                conn.commit()
                existing_columns = {
                    row[1] for row in conn.execute("PRAGMA table_info(stock_repurchase)")
                }
                for column in ("repurchase_price_lower", "repurchase_price_upper"):
                    if column not in existing_columns:
                        conn.execute(f"ALTER TABLE stock_repurchase ADD COLUMN {column} REAL")
                conn.commit()

                conn.execute("""
                    DELETE FROM stock_repurchase
                    WHERE trade_date IS NULL OR TRIM(trade_date) = ''
                       OR stock_code IS NULL OR TRIM(stock_code) = ''
                """)
                conn.commit()
                conn.execute("""
                    DELETE FROM institution_survey
                    WHERE trade_date IS NULL OR TRIM(trade_date) = ''
                       OR stock_code IS NULL OR TRIM(stock_code) = ''
                """)
                conn.execute("""
                    DELETE FROM institution_survey
                    WHERE id NOT IN (
                        SELECT MAX(id) FROM institution_survey GROUP BY trade_date, stock_code
                    )
                """)
                conn.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS ux_institution_survey_date_code
                    ON institution_survey(trade_date, stock_code)
                """)
                conn.commit()
                conn.execute("""
                    DELETE FROM stock_pledge
                    WHERE trade_date IS NULL OR TRIM(trade_date) = ''
                       OR stock_code IS NULL OR TRIM(stock_code) = ''
                """)
                conn.commit()
        except Exception as e:
            logger.warning(f"⚠️ Phase 2 表迁移失败: {e}")

    @property
    def db_path(self) -> str:
        return str(self._db.db_path)

    def close(self) -> None:
        if self._write_conn is not None:
            self._write_conn.close()
            self._write_conn = None
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

    def save_indicators_batch(self, records: list[dict[str, Any]]) -> int:
        """批量保存技术指标数据（使用共享写连接 + 写锁）。

        避免 ThreadPoolExecutor 并发写入时每只股票单独开连接/提交，
        把多只股票的指标行合并为一次 executemany + 一次 commit。
        """
        if not records:
            return 0
        try:
            with self._write_lock:
                conn = self._get_write_conn()
                before_changes = conn.total_changes
                conn.executemany(
                    """
                    INSERT OR REPLACE INTO indicators (
                        ts_code, trade_date, close, volume,
                        ma5, ma10, ma20, ma60, ma120, ma250,
                        vol_ma5, vol_ma50, vol_ma60,
                        boll_upper, boll_mid, boll_lower, boll_bandwidth,
                        cyc60, chip_concentration,
                        macd_dif, macd_dea, macd_hist,
                        kdj_k, kdj_d, kdj_j,
                        rsi6, rsi12, rsi24,
                        cci
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            r.get("ts_code"),
                            r.get("trade_date"),
                            r.get("close"),
                            r.get("volume"),
                            r.get("ma5"),
                            r.get("ma10"),
                            r.get("ma20"),
                            r.get("ma60"),
                            r.get("ma120"),
                            r.get("ma250"),
                            r.get("vol_ma5"),
                            r.get("vol_ma50"),
                            r.get("vol_ma60"),
                            r.get("boll_upper"),
                            r.get("boll_mid"),
                            r.get("boll_lower"),
                            r.get("boll_bandwidth"),
                            r.get("cyc60"),
                            r.get("chip_concentration"),
                            r.get("macd_dif"),
                            r.get("macd_dea"),
                            r.get("macd_hist"),
                            r.get("kdj_k"),
                            r.get("kdj_d"),
                            r.get("kdj_j"),
                            r.get("rsi6"),
                            r.get("rsi12"),
                            r.get("rsi24"),
                            r.get("cci"),
                        )
                        for r in records
                    ],
                )
                return self._commit_delta(conn, before_changes)
        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.warning(f"⚠️ 技术指标批量保存失败: {e}")
            return 0

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

    def save_historical_valuation_batch(self, records: list[dict[str, Any]]) -> int:
        if not records:
            return 0
        rows = [
            (
                r.get("ts_code"),
                r.get("trade_date"),
                r.get("pe_ttm"),
                r.get("pb"),
                r.get("ps_ttm"),
                r.get("dividend_yield"),
            )
            for r in records
            if r.get("ts_code") and r.get("trade_date")
        ]
        if not rows:
            return 0
        with self._write_lock:
            conn = self._get_write_conn()
            before_changes = conn.total_changes
            conn.executemany(
                """
                INSERT OR REPLACE INTO historical_valuation (
                    ts_code, trade_date, pe_ttm, pb, ps_ttm, dividend_yield
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            return self._commit_delta(conn, before_changes)

    def save_sector_industry(self, data: dict[str, Any]) -> None:
        self._db.save_sector_industry(data)

    def get_sector_industry(self, industry_name: str, trade_date: str = None) -> dict | None:
        return self._db.get_sector_industry(industry_name, trade_date)

    def get_fundamentals_batch(self, trade_date: str = None) -> pd.DataFrame:
        return self._db.get_fundamentals_batch(trade_date)

    def save_north_flow_batch(self, records: list[dict[str, Any]]) -> int:
        return self._db.save_north_flow_batch(records)

    def save_north_hold_batch(self, records: list[dict[str, Any]]) -> int:
        return self._db.save_north_hold_batch(records)

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

    def save_futures_daily_batch(self, records: list[dict[str, Any]]) -> int:
        return self._db.save_futures_daily_batch(records)

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
                before_changes = conn.total_changes
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
                return self._commit_delta(conn, before_changes)
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
                before_changes = conn.total_changes
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
                return self._commit_delta(conn, before_changes)
        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.warning(f"⚠️ 东方财富筹码分布批量保存失败: {e}")
            return 0

    def save_macro_monthly_batch(self, records: list[dict[str, Any]]) -> int:
        """批量保存月度宏观数据。"""
        if not records:
            return 0
        try:
            with self._write_lock:
                conn = self._get_write_conn()
                before_changes = conn.total_changes
                conn.executemany(
                    """
                    INSERT OR REPLACE INTO macro_monthly (
                        date, cpi_yoy, cpi_mom, cpi_core_yoy,
                        ppi_yoy, ppi_mom,
                        pmi, pmi_yoy, pmi_monthly_change, pmi_mom,
                        pmi_caixin,
                        m0, m1, m2, m0_yoy, m1_yoy, m2_yoy,
                        new_loans, new_loans_yoy,
                        retail_sales_yoy, retail_sales_ytd_yoy,
                        fixed_asset_investment_yoy, fixed_asset_investment_ytd_yoy,
                        export_value, export_yoy, import_value, import_yoy,
                        industrial_production_yoy, industrial_production_ytd_yoy,
                        electricity_consumption_yoy, electricity_consumption_total,
                        enterprise_goods_price_yoy, enterprise_goods_price_mom,
                        consumer_confidence, consumer_satisfaction, consumer_expectation,
                        lpr_1y, lpr_5y,
                        data_date
                    ) VALUES (
                        ?, ?, ?, ?,
                        ?, ?,
                        ?, ?, ?, ?,
                        ?,
                        ?, ?, ?, ?, ?, ?,
                        ?, ?,
                        ?, ?,
                        ?, ?,
                        ?, ?, ?, ?,
                        ?, ?,
                        ?, ?,
                        ?, ?,
                        ?, ?, ?,
                        ?, ?,
                        ?
                    )
                    """,
                    [
                        (
                            r["date"],
                            r.get("cpi_yoy"), r.get("cpi_mom"), r.get("cpi_core_yoy"),
                            r.get("ppi_yoy"), r.get("ppi_mom"),
                            r.get("pmi"), r.get("pmi_yoy"), r.get("pmi_monthly_change"), r.get("pmi_mom"),
                            r.get("pmi_caixin"),
                            r.get("m0"), r.get("m1"), r.get("m2"), r.get("m0_yoy"), r.get("m1_yoy"), r.get("m2_yoy"),
                            r.get("new_loans"), r.get("new_loans_yoy"),
                            r.get("retail_sales_yoy"), r.get("retail_sales_ytd_yoy"),
                            r.get("fixed_asset_investment_yoy"), r.get("fixed_asset_investment_ytd_yoy"),
                            r.get("export_value"), r.get("export_yoy"), r.get("import_value"), r.get("import_yoy"),
                            r.get("industrial_production_yoy"), r.get("industrial_production_ytd_yoy"),
                            r.get("electricity_consumption_yoy"), r.get("electricity_consumption_total"),
                            r.get("enterprise_goods_price_yoy"), r.get("enterprise_goods_price_mom"),
                            r.get("consumer_confidence"), r.get("consumer_satisfaction"), r.get("consumer_expectation"),
                            r.get("lpr_1y"), r.get("lpr_5y"),
                            r.get("data_date"),
                        )
                        for r in records
                    ],
                )
                return self._commit_delta(conn, before_changes)
        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.warning(f"⚠️ 月度宏观数据保存失败: {e}")
            return 0

    def save_macro_quarterly_batch(self, records: list[dict[str, Any]]) -> int:
        """批量保存季度宏观数据。"""
        if not records:
            return 0
        try:
            with self._write_lock:
                conn = self._get_write_conn()
                before_changes = conn.total_changes
                conn.executemany(
                    "INSERT OR REPLACE INTO macro_quarterly (date, gdp, gdp_yoy, gdp_qoq, gdp_primary, gdp_secondary, gdp_tertiary, data_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        (r["date"], r.get("gdp"), r.get("gdp_yoy"), r.get("gdp_qoq"), r.get("gdp_primary"), r.get("gdp_secondary"), r.get("gdp_tertiary"), r.get("data_date"))
                        for r in records
                    ],
                )
                return self._commit_delta(conn, before_changes)
        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.warning(f"⚠️ 季度宏观数据保存失败: {e}")
            return 0

    def save_macro_daily_batch(self, records: list[dict[str, Any]]) -> int:
        """批量保存日度宏观数据。"""
        if not records:
            return 0
        try:
            with self._write_lock:
                conn = self._get_write_conn()
                before_changes = conn.total_changes
                conn.executemany(
                    "INSERT OR REPLACE INTO macro_daily (date, shibor_on, shibor_1w, shibor_2w, shibor_1m, shibor_3m, shibor_6m, shibor_9m, shibor_1y, data_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        (r["date"], r.get("shibor_on"), r.get("shibor_1w"), r.get("shibor_2w"), r.get("shibor_1m"), r.get("shibor_3m"), r.get("shibor_6m"), r.get("shibor_9m"), r.get("shibor_1y"), r.get("data_date"))
                        for r in records
                    ],
                )
                return self._commit_delta(conn, before_changes)
        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.warning(f"⚠️ 日度宏观数据保存失败: {e}")
            return 0

    def save_money_market_batch(self, records: list[dict[str, Any]]) -> int:
        """批量保存货币市场日度数据。"""
        if not records:
            return 0
        try:
            with self._write_lock:
                conn = self._get_write_conn()
                before_changes = conn.total_changes
                conn.executemany(
                    "INSERT OR REPLACE INTO money_market (date, shibor_on, shibor_1w, shibor_2w, shibor_1m, shibor_3m, shibor_6m, shibor_9m, shibor_1y, fr001, fr007, fr014, pboc_policy_rate, data_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        (r["date"], r.get("shibor_on"), r.get("shibor_1w"), r.get("shibor_2w"), r.get("shibor_1m"), r.get("shibor_3m"), r.get("shibor_6m"), r.get("shibor_9m"), r.get("shibor_1y"), r.get("fr001"), r.get("fr007"), r.get("fr014"), r.get("pboc_policy_rate"), r.get("data_date"))
                        for r in records
                    ],
                )
                return self._commit_delta(conn, before_changes)
        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.warning(f"⚠️ 货币市场数据保存失败: {e}")
            return 0

    def save_central_bank_balance_batch(self, records: list[dict[str, Any]]) -> int:
        """批量保存央行资产负债表数据。"""
        if not records:
            return 0
        try:
            with self._write_lock:
                conn = self._get_write_conn()
                before_changes = conn.total_changes
                conn.executemany(
                    "INSERT OR REPLACE INTO central_bank_balance (date, total_assets, reserve_money, currency_issue, claims_on_other_deposit, claims_on_gov, gov_deposits, foreign_assets, fx_reserve, data_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        (r["date"], r.get("total_assets"), r.get("reserve_money"), r.get("currency_issue"), r.get("claims_on_other_deposit"), r.get("claims_on_gov"), r.get("gov_deposits"), r.get("foreign_assets"), r.get("fx_reserve"), r.get("data_date"))
                        for r in records
                    ],
                )
                return self._commit_delta(conn, before_changes)
        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.warning(f"⚠️ 央行资产负债表保存失败: {e}")
            return 0

    def save_market_valuation_batch(self, records: list[dict[str, Any]]) -> int:
        """批量保存大盘估值数据。"""
        if not records:
            return 0
        try:
            with self._write_lock:
                conn = self._get_write_conn()
                before_changes = conn.total_changes
                conn.executemany(
                    "INSERT OR REPLACE INTO market_valuation (date, pe_median, pe_quantile, pe_lyr_median, pb_median, pb_quantile, equity_bond_spread, ebs_ma, csi300_close, data_source, data_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        (r["date"], r.get("pe_median"), r.get("pe_quantile"), r.get("pe_lyr_median"), r.get("pb_median"), r.get("pb_quantile"), r.get("equity_bond_spread"), r.get("ebs_ma"), r.get("csi300_close"), r.get("data_source", "legu"), r.get("data_date"))
                        for r in records
                    ],
                )
                return self._commit_delta(conn, before_changes)
        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.warning(f"⚠️ 大盘估值数据保存失败: {e}")
            return 0

    def save_concept_board_batch(self, records: list[dict[str, Any]]) -> int:
        """批量保存概念板块日频行情数据。"""
        if not records:
            return 0
        try:
            with self._write_lock:
                conn = self._get_write_conn()
                before_changes = conn.total_changes
                conn.executemany(
                    "INSERT OR REPLACE INTO concept_board (trade_date, concept_code, concept_name, pct_change, turnover, up_count, down_count, data_source) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        (r.get("trade_date"), r.get("concept_code"), r.get("concept_name"), r.get("pct_change"), r.get("turnover"), r.get("up_count"), r.get("down_count"), r.get("data_source", "ths"))
                        for r in records
                    ],
                )
                return self._commit_delta(conn, before_changes)
        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.warning(f"⚠️ 概念板块数据保存失败: {e}")
            return 0

    def save_concept_member_batch(self, records: list[dict[str, Any]]) -> int:
        """批量保存概念板块成分股映射数据。"""
        if not records:
            return 0
        try:
            with self._write_lock:
                conn = self._get_write_conn()
                before_changes = conn.total_changes
                conn.executemany(
                    "INSERT OR REPLACE INTO concept_member (concept_code, concept_name, ts_code) VALUES (?, ?, ?)",
                    [
                        (r.get("concept_code"), r.get("concept_name"), r.get("ts_code"))
                        for r in records
                    ],
                )
                return self._commit_delta(conn, before_changes)
        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.warning(f"⚠️ 概念板块成分股数据保存失败: {e}")
            return 0

    def save_south_flow_batch(self, records: list[dict[str, Any]]) -> int:
        """批量保存南向资金流向数据。"""
        if not records:
            return 0
        try:
            with self._write_lock:
                conn = self._get_write_conn()
                before_changes = conn.total_changes
                conn.executemany(
                    """
                    INSERT OR REPLACE INTO south_flow (
                        trade_date, market, net_buy_amount, buy_amount, sell_amount,
                        cumulative_net_buy, data_source
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            r["trade_date"],
                            r.get("market"),
                            r.get("net_buy_amount"),
                            r.get("buy_amount"),
                            r.get("sell_amount"),
                            r.get("cumulative_net_buy"),
                            r.get("data_source"),
                        )
                        for r in records
                    ],
                )
                return self._commit_delta(conn, before_changes)
        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.warning(f"⚠️ 南向资金流向批量保存失败: {e}")
            return 0

    def save_ah_premium_batch(self, records: list[dict[str, Any]]) -> int:
        """批量保存AH溢价率数据。"""
        if not records:
            return 0
        try:
            with self._write_lock:
                conn = self._get_write_conn()
                before_changes = conn.total_changes
                conn.executemany(
                    """
                    INSERT OR REPLACE INTO ah_premium (
                        trade_date, ts_code, h_code, name, a_price, h_price,
                        premium, data_source
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            r["trade_date"],
                            r.get("ts_code"),
                            r.get("h_code"),
                            r.get("name"),
                            r.get("a_price"),
                            r.get("h_price"),
                            r.get("premium"),
                            r.get("data_source"),
                        )
                        for r in records
                    ],
                )
                return self._commit_delta(conn, before_changes)
        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.warning(f"⚠️ AH溢价率批量保存失败: {e}")
            return 0

    def save_cb_quotation_batch(self, records: list[dict[str, Any]]) -> int:
        """批量保存可转债行情数据。"""
        if not records:
            return 0
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self._write_lock:
            conn = self._get_write_conn()
            try:
                before_changes = conn.total_changes
                conn.executemany(
                    """
                    INSERT OR REPLACE INTO cb_quotation (
                        ts_code, bond_name, price, premium,
                        double_low, expire_date, data_source, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            r["ts_code"],
                            r.get("bond_name"),
                            r.get("price"),
                            r.get("premium"),
                            r.get("double_low"),
                            r.get("expire_date"),
                            r.get("data_source"),
                            now,
                        )
                        for r in records
                    ],
                )
                return self._commit_delta(conn, before_changes)
            except Exception as e:
                err = str(e).lower()
                if "no column named updated_at" in err:
                    try:
                        conn.execute("ALTER TABLE cb_quotation ADD COLUMN updated_at DATETIME DEFAULT CURRENT_TIMESTAMP")
                        before_changes = conn.total_changes
                        conn.executemany(
                            "INSERT OR REPLACE INTO cb_quotation (ts_code, bond_name, price, premium, double_low, expire_date, data_source, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                            [(r["ts_code"], r.get("bond_name"), r.get("price"), r.get("premium"), r.get("double_low"), r.get("expire_date"), r.get("data_source"), now) for r in records],
                        )
                        return self._commit_delta(conn, before_changes)
                    except Exception as e2:
                        logger = logging.getLogger(__name__)
                        logger.warning(f"⚠️ 可转债行情保存失败（迁移后仍失败）: {e2}")
                        return 0
                logger = logging.getLogger(__name__)
                logger.warning(f"⚠️ 可转债行情批量保存失败: {e}")
                return 0

    def save_cb_redeem_batch(self, records: list[dict[str, Any]]) -> int:
        """批量保存可转债强赎数据。"""
        if not records:
            return 0
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self._write_lock:
            conn = self._get_write_conn()
            try:
                before_changes = conn.total_changes
                conn.executemany(
                    """
                    INSERT OR REPLACE INTO cb_redeem (
                        ts_code, bond_name, redeem_flag,
                        redeem_price, redeem_date, data_source, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            r["ts_code"],
                            r.get("bond_name"),
                            r.get("redeem_flag"),
                            r.get("redeem_price"),
                            r.get("redeem_date"),
                            r.get("data_source"),
                            now,
                        )
                        for r in records
                    ],
                )
                return self._commit_delta(conn, before_changes)
            except Exception as e:
                err = str(e).lower()
                if "no column named updated_at" in err:
                    try:
                        conn.execute("ALTER TABLE cb_redeem ADD COLUMN updated_at DATETIME DEFAULT CURRENT_TIMESTAMP")
                        before_changes = conn.total_changes
                        conn.executemany(
                            "INSERT OR REPLACE INTO cb_redeem (ts_code, bond_name, redeem_flag, redeem_price, redeem_date, data_source, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                            [(r["ts_code"], r.get("bond_name"), r.get("redeem_flag"), r.get("redeem_price"), r.get("redeem_date"), r.get("data_source"), now) for r in records],
                        )
                        return self._commit_delta(conn, before_changes)
                    except Exception as e2:
                        logger = logging.getLogger(__name__)
                        logger.warning(f"⚠️ 可转债强赎保存失败（迁移后仍失败）: {e2}")
                        return 0
                logger = logging.getLogger(__name__)
                logger.warning(f"⚠️ 可转债强赎批量保存失败: {e}")
                return 0

    def save_cb_index_batch(self, records: list[dict[str, Any]]) -> int:
        """批量保存可转债指数数据。"""
        if not records:
            return 0
        try:
            with self._write_lock:
                conn = self._get_write_conn()
                before_changes = conn.total_changes
                conn.executemany(
                    """
                    INSERT OR REPLACE INTO cb_index (
                        trade_date, index_code, index_name,
                        open, close, high, low, volume, data_source
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            r["trade_date"],
                            r.get("index_code"),
                            r.get("index_name"),
                            r.get("open"),
                            r.get("close"),
                            r.get("high"),
                            r.get("low"),
                            r.get("volume"),
                            r.get("data_source"),
                        )
                        for r in records
                    ],
                )
                return self._commit_delta(conn, before_changes)
        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.warning(f"⚠️ 可转债指数批量保存失败: {e}")
            return 0

    def save_etf_daily_batch(self, records: list[dict[str, Any]]) -> int:
        """批量保存ETF日线数据。"""
        if not records:
            return 0
        try:
            with self._write_lock:
                conn = self._get_write_conn()
                before_changes = conn.total_changes
                conn.executemany(
                    """
                    INSERT OR REPLACE INTO etf_daily (
                        ts_code, name, trade_date, open, high, low, close,
                        volume, amount, data_source
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            r["ts_code"],
                            r.get("name"),
                            r.get("trade_date"),
                            r.get("open"),
                            r.get("high"),
                            r.get("low"),
                            r.get("close"),
                            r.get("volume"),
                            r.get("amount"),
                            r.get("data_source"),
                        )
                        for r in records
                    ],
                )
                return self._commit_delta(conn, before_changes)
        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.warning(f"⚠️ ETF日线数据批量保存失败: {e}")
            return 0

    def save_restricted_share_batch(self, records: list[dict[str, Any]]) -> int:
        """批量保存限售解禁数据。"""
        if not records:
            return 0
        try:
            with self._write_lock:
                conn = self._get_write_conn()
                before_changes = conn.total_changes
                conn.executemany(
                    """
                    INSERT OR REPLACE INTO restricted_share (
                        ts_code, name, release_date, actual_release,
                        total_shares, market_type, data_source
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            r["ts_code"],
                            r.get("name"),
                            r.get("release_date"),
                            r.get("actual_release"),
                            r.get("total_shares"),
                            r.get("market_type"),
                            r.get("data_source"),
                        )
                        for r in records
                    ],
                )
                return self._commit_delta(conn, before_changes)
        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.warning(f"⚠️ 限售解禁批量保存失败: {e}")
            return 0

    def save_earnings_forecast_batch(self, records: list[dict[str, Any]]) -> int:
        """批量保存业绩预告数据。"""
        if not records:
            return 0
        try:
            with self._write_lock:
                conn = self._get_write_conn()
                before_changes = conn.total_changes
                conn.executemany(
                    """
                    INSERT OR REPLACE INTO earnings_forecast (
                        ts_code, name, end_date, forecast_type,
                        net_profit_change, previous_profit, data_source
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            r["ts_code"],
                            r.get("name"),
                            r.get("end_date"),
                            r.get("forecast_type"),
                            r.get("net_profit_change"),
                            r.get("previous_profit"),
                            r.get("data_source"),
                        )
                        for r in records
                    ],
                )
                return self._commit_delta(conn, before_changes)
        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.warning(f"⚠️ 业绩预告批量保存失败: {e}")
            return 0

    def save_sector_daily_batch(self, records: list[dict[str, Any]]) -> int:
        """批量保存行业板块日线数据。"""
        if not records:
            return 0
        try:
            with self._write_lock:
                conn = self._get_write_conn()
                before_changes = conn.total_changes
                conn.executemany(
                    """
                    INSERT OR REPLACE INTO sector_daily (
                        sector_name, trade_date,
                        open, close, high, low, volume, amount, pct_change, data_source
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            r["sector_name"],
                            r.get("trade_date"),
                            r.get("open"),
                            r.get("close"),
                            r.get("high"),
                            r.get("low"),
                            r.get("volume"),
                            r.get("amount"),
                            r.get("pct_change"),
                            r.get("data_source"),
                        )
                        for r in records
                    ],
                )
                return self._commit_delta(conn, before_changes)
        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.warning(f"⚠️ 行业板块日线批量保存失败: {e}")
            return 0

    def save_sector_valuation_batch(self, records: list[dict[str, Any]]) -> int:
        """批量保存行业板块估值数据。"""
        if not records:
            return 0
        try:
            with self._write_lock:
                conn = self._get_write_conn()
                before_changes = conn.total_changes
                conn.executemany(
                    """
                    INSERT OR REPLACE INTO sector_valuation (
                        sector_name, trade_date, pe, pb, total_mv, data_source
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            r["sector_name"],
                            r.get("trade_date"),
                            r.get("pe"),
                            r.get("pb"),
                            r.get("total_mv"),
                            r.get("data_source"),
                        )
                        for r in records
                    ],
                )
                return self._commit_delta(conn, before_changes)
        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.warning(f"⚠️ 行业板块估值批量保存失败: {e}")
            return 0

    def save_index_futures_basis_batch(self, records: list[dict[str, Any]]) -> int:
        """批量保存股指期货基差数据。"""
        if not records:
            return 0
        try:
            with self._write_lock:
                conn = self._get_write_conn()
                before_changes = conn.total_changes
                conn.executemany(
                    """
                    INSERT OR REPLACE INTO index_futures_basis (
                        trade_date, futures_code, futures_price, index_price,
                        basis, basis_pct, data_source
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            r["trade_date"],
                            r.get("futures_code"),
                            r.get("futures_price"),
                            r.get("index_price"),
                            r.get("basis"),
                            r.get("basis_pct"),
                            r.get("data_source"),
                        )
                        for r in records
                    ],
                )
                return self._commit_delta(conn, before_changes)
        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.warning(f"⚠️ 股指期货基差批量保存失败: {e}")
            return 0

    def get_chip_distribution_em_batch(
        self, stock_list: list[str] | None = None
    ) -> dict[str, dict]:
        """批量获取多只股票最新东方财富筹码分布。"""
        return self._db.get_chip_distribution_em_batch(stock_list)

    def save_option_sentiment_batch(self, records: list[dict[str, Any]]) -> int:
        """批量保存期权情绪数据。"""
        if not records:
            return 0
        try:
            with self._write_lock:
                conn = self._get_write_conn()
                before_changes = conn.total_changes
                conn.executemany(
                    "INSERT OR REPLACE INTO option_sentiment (trade_date, qvix, pcr, put_volume, call_volume, put_oi, call_oi, implied_vol_avg) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        (r.get("trade_date"), r.get("qvix"), r.get("pcr"),
                         r.get("put_volume"), r.get("call_volume"),
                         r.get("put_oi"), r.get("call_oi"),
                         r.get("implied_vol_avg"))
                        for r in records
                    ],
                )
                return self._commit_delta(conn, before_changes)
        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.warning(f"⚠️ 期权情绪批量保存失败: {e}")
            return 0

    def save_stock_repurchase_batch(self, records: list[dict[str, Any]]) -> int:
        """批量保存股票回购数据。"""
        valid_records = [r for r in records if r.get("trade_date") and r.get("stock_code")]
        if not valid_records:
            return 0
        try:
            with self._write_lock:
                conn = self._get_write_conn()
                before_changes = conn.total_changes
                conn.executemany(
                    "INSERT OR REPLACE INTO stock_repurchase (trade_date, stock_code, stock_name, repurchase_amount, repurchase_price, repurchase_price_lower, repurchase_price_upper, repurchase_quantity, progress_status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        (r.get("trade_date"), r.get("stock_code"), r.get("stock_name"),
                         r.get("repurchase_amount"), r.get("repurchase_price"),
                         r.get("repurchase_price_lower"), r.get("repurchase_price_upper"),
                         r.get("repurchase_quantity"), r.get("progress_status"))
                        for r in valid_records
                    ],
                )
                return self._commit_delta(conn, before_changes)
        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.warning(f"⚠️ 股票回购批量保存失败: {e}")
            return 0

    def save_insider_trading_batch(self, records: list[dict[str, Any]]) -> int:
        """批量保存增减持数据。"""
        if not records:
            return 0
        try:
            with self._write_lock:
                conn = self._get_write_conn()
                before_changes = conn.total_changes
                conn.executemany(
                    "INSERT OR REPLACE INTO insider_trading (trade_date, stock_code, stock_name, changer_name, change_type, change_quantity, change_price, holdings_after_change) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        (r.get("trade_date"), r.get("stock_code"), r.get("stock_name"),
                         r.get("changer_name"), r.get("change_type"),
                         r.get("change_quantity"), r.get("change_price"),
                         r.get("holdings_after_change"))
                        for r in records
                    ],
                )
                return self._commit_delta(conn, before_changes)
        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.warning(f"⚠️ 增减持批量保存失败: {e}")
            return 0

    def save_institution_survey_batch(self, records: list[dict[str, Any]]) -> int:
        """批量保存机构调研数据。"""
        valid_records = [r for r in records if r.get("trade_date") and r.get("stock_code")]
        if not valid_records:
            return 0
        try:
            with self._write_lock:
                conn = self._get_write_conn()
                before_changes = conn.total_changes
                conn.executemany(
                    """
                    INSERT INTO institution_survey
                        (trade_date, stock_code, stock_name, survey_org, survey_type, survey_count)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(trade_date, stock_code) DO UPDATE SET
                        stock_name = excluded.stock_name,
                        survey_org = excluded.survey_org,
                        survey_type = excluded.survey_type,
                        survey_count = excluded.survey_count
                    """,
                    [
                        (r.get("trade_date"), r.get("stock_code"), r.get("stock_name"),
                         r.get("survey_org"), r.get("survey_type"), r.get("survey_count"))
                        for r in valid_records
                    ],
                )
                return self._commit_delta(conn, before_changes)
        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.warning(f"⚠️ 机构调研批量保存失败: {e}")
            return 0

    def save_stock_pledge_batch(self, records: list[dict[str, Any]]) -> int:
        """批量保存股权质押数据。"""
        valid_records = [r for r in records if r.get("trade_date") and r.get("stock_code")]
        if not valid_records:
            return 0
        try:
            with self._write_lock:
                conn = self._get_write_conn()
                before_changes = conn.total_changes
                conn.executemany(
                    "INSERT OR REPLACE INTO stock_pledge (trade_date, stock_code, stock_name, pledger, pledge_amount, pledge_ratio, pledge_org) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    [
                        (r.get("trade_date"), r.get("stock_code"), r.get("stock_name"),
                         r.get("pledger"), r.get("pledge_amount"),
                         r.get("pledge_ratio"), r.get("pledge_org"))
                        for r in valid_records
                    ],
                )
                return self._commit_delta(conn, before_changes)
        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.warning(f"⚠️ 股权质押批量保存失败: {e}")
            return 0



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
