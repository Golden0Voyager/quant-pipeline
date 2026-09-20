"""
SmartMoney Provider 实现
─────────────────────────
职责：把 smartmoney_hunter 的具体类包装成 pipeline_interface 的实现。

这是唯一依赖 smartmoney_hunter 内部类的文件。
如果 smartmoney_hunter 重构，只需要改这个文件，daily_pipeline 不动。
"""
from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from smartmoney_hunter.data_loader import DataLoader
from smartmoney_hunter.database import DatabaseManager
from smartmoney_hunter.indicators import IndicatorCalculator

from core.source_record_key import (
    INSTITUTION_SURVEY_SOURCE_KEY_FIELDS,
    STOCK_REPURCHASE_SOURCE_KEY_FIELDS,
    block_trade_source_key,
    dragon_tiger_source_key,
    placement_source_key,
    source_record_key,
    stock_pledge_source_key,
)

logger = logging.getLogger(__name__)


def _prev_date(d: str) -> str:
    """Return YYYY-MM-DD one day before *d*."""
    from datetime import timedelta
    dt = datetime.strptime(d, "%Y-%m-%d") - timedelta(days=1)
    return dt.strftime("%Y-%m-%d")


# ===========================================================================
# Database Provider
# ===========================================================================

class SmartMoneyDBProvider:
    """基于 DatabaseManager 的数据库 provider。"""

    def __init__(self, db_path: str | None = None):
        self._db = DatabaseManager(db_path=db_path)
        self._ensure_wal_mode()
        self._ensure_tables()
        self._run_versioned_migrations()
        # 共享写连接 + 写锁：所有 batch 写入串行化，避免并发写导致 database is locked
        self._write_lock = threading.Lock()
        self._write_conn: sqlite3.Connection | None = None

    def _run_versioned_migrations(self) -> None:
        """Run the versioned migration system in-place.

        Replaces the old ``_ensure_tables`` / ``_migrate_phase2_tables``
        ad-hoc DDL with tracked, versioned migrations from ``migrations/``.

        Any migration failure is treated as a hard failure and propagated,
        so operators cannot mistake a silently-fallback database for a
        correctly-migrated one.
        """
        from core.migrations import MigrationError, run_migrations

        db_str = str(self._db.db_path)
        results = run_migrations(db_path=db_str)
        applied = [r for r in results if r.get("applied")]
        if applied:
            for r in applied:
                logger.info("  ✅ migration %03d: %s (%dms)", r["version"], r["description"], r["duration_ms"])
        errors = [r for r in results if r.get("error")]
        if errors:
            for r in errors:
                logger.error("  ❌ migration %03d failed: %s", r["version"], r["error"])
            raise MigrationError(
                f"{len(errors)} versioned migration(s) failed; see logs above"
            )

    def _get_write_conn(self) -> sqlite3.Connection:
        """复用单个写连接（类似 DatabaseManager._connect_for_write）。

        必须在 self._write_lock 保护下使用。
        """
        if self._write_conn is None:
            conn = sqlite3.connect(str(self._db.db_path), timeout=30.0, check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=30000")
            conn.execute("PRAGMA foreign_keys=ON")
            self._write_conn = conn
        return self._write_conn

    def _connect_for_audit(self) -> sqlite3.Connection:
        """Create an audit connection with SQLite foreign keys enabled."""
        conn = sqlite3.connect(str(self._db.db_path), timeout=30.0)
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

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
                        source_record_key TEXT NOT NULL UNIQUE
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
                        source_record_key TEXT NOT NULL UNIQUE
                    )
                """)
                # ==================== Phase 2: 股权质押 ====================
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS stock_pledge (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        trade_date TEXT NOT NULL,
                        stock_code TEXT NOT NULL,
                        stock_name TEXT,
                        pledger TEXT,
                        pledge_amount REAL,
                        pledge_ratio REAL,
                        pledge_org TEXT,
                        source_record_key TEXT NOT NULL
                    )
                """)
                # ==================== Phase 2: 定增公告 ====================
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS placement_announcements (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        source_record_key TEXT NOT NULL UNIQUE,
                        ts_code TEXT NOT NULL,
                        symbol TEXT,
                        name TEXT,
                        issue_method TEXT,
                        issue_date DATE,
                        data_source TEXT,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_placement_code_date
                    ON placement_announcements(ts_code, issue_date DESC)
                """)
        except Exception as e:
            logger.warning(f"⚠️ _ensure_tables 创建表失败: {e}")

    def _old_migrate_phase2_tables(self) -> None:
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
                        source_record_key TEXT NOT NULL UNIQUE
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
                        source_record_key TEXT NOT NULL UNIQUE
                    )
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS stock_pledge (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        trade_date TEXT NOT NULL,
                        stock_code TEXT NOT NULL,
                        stock_name TEXT,
                        pledger TEXT,
                        pledge_amount REAL,
                        pledge_ratio REAL,
                        pledge_org TEXT,
                        source_record_key TEXT NOT NULL
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
                conn.commit()
                conn.execute("""
                    DELETE FROM stock_pledge
                    WHERE trade_date IS NULL OR TRIM(trade_date) = ''
                       OR stock_code IS NULL OR TRIM(stock_code) = ''
                """)
                conn.commit()
        except Exception as e:
            logger.warning(f"⚠️ Phase 2 表迁移失败: {e}")
        # 版本化迁移放在兜底 DDL 的吞异常范围之外：
        # MigrationError 必须硬失败上抛，不能被当作 Phase 2 兼容问题吞掉
        self._run_versioned_migrations()

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

    def record_ingestion_run(self, result: dict[str, Any]) -> None:
        """把 ``TaskResult.to_dict()`` 写入 ``ingestion_runs`` 审计表。"""
        if not result:
            return
        metadata = result.get("metadata") or {}
        if isinstance(metadata, dict):
            metadata_json = json.dumps(metadata, ensure_ascii=False, default=str)
        else:
            metadata_json = str(metadata)

        # safe_task 把 run_id/started_at/finished_at 放在 metadata 中，
        # 同时兼容顶层 key 的调用方。
        meta = metadata if isinstance(metadata, dict) else {}
        run_id = result.get("run_id") or meta.get("run_id") or str(uuid.uuid4())
        finished_at = (
            result.get("finished_at")
            or meta.get("finished_at")
            or datetime.utcnow().isoformat(timespec="seconds")
        )
        started_at = result.get("started_at") or meta.get("started_at") or finished_at

        statement = """
                INSERT INTO ingestion_runs (
                    run_id, task_name, source, status, started_at, finished_at,
                    requested_date, data_date, attempts, fetched_rows, accepted_rows,
                    rejected_rows, saved_rows, schema_fingerprint, error_kind,
                    error_message, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                    task_name = excluded.task_name,
                    source = excluded.source,
                    status = excluded.status,
                    started_at = excluded.started_at,
                    finished_at = excluded.finished_at,
                    requested_date = excluded.requested_date,
                    data_date = excluded.data_date,
                    attempts = excluded.attempts,
                    fetched_rows = excluded.fetched_rows,
                    accepted_rows = excluded.accepted_rows,
                    rejected_rows = excluded.rejected_rows,
                    saved_rows = excluded.saved_rows,
                    schema_fingerprint = excluded.schema_fingerprint,
                    error_kind = excluded.error_kind,
                    error_message = excluded.error_message,
                    metadata_json = excluded.metadata_json
                """
        values = (
                    run_id,
                    result.get("task_name", ""),
                    result.get("source"),
                    result.get("status", ""),
                    started_at,
                    finished_at,
                    None,
                    result.get("data_date"),
                    result.get("attempted", 0),
                    result.get("fetched", 0),
                    result.get("accepted", 0),
                    result.get("rejected", 0),
                    result.get("saved", 0),
                    result.get("schema_fingerprint"),
                    result.get("error_kind"),
                    result.get("error"),
                    metadata_json,
        )
        for attempt in range(3):
            try:
                with self._connect_for_audit() as conn:
                    conn.execute(statement, values)
                    conn.commit()
                return
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc).lower() or attempt == 2:
                    raise
                delay = 0.25 * (2 ** attempt)
                logger.warning(
                    "⚠️ ingestion_runs 写入遇到数据库锁，%.2fs 后重试 (%d/3)",
                    delay,
                    attempt + 1,
                )
                time.sleep(delay)

    def record_ingestion_rejection(
        self,
        run_id: str,
        row_number: int,
        reason: str,
        payload: dict[str, Any],
    ) -> None:
        """把被拒绝的单行数据写入 ``ingestion_rejections`` 审计表。"""
        payload_json = json.dumps(payload, ensure_ascii=False, default=str)
        with self._connect_for_audit() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO ingestion_rejections
                    (run_id, row_number, reason, payload_json)
                VALUES (?, ?, ?, ?)
                """,
                (run_id, row_number, reason, payload_json),
            )
            conn.commit()

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

    def get_global_assets_latest_date(self, symbol: str) -> str | None:
        """轻量查询：直接 SQL 取 global_assets_bars 的 MAX(trade_date)。"""
        try:
            with sqlite3.connect(str(self._db.db_path), timeout=5.0) as conn:
                cursor = conn.execute(
                    "SELECT MAX(trade_date) FROM global_assets_bars WHERE ts_code = ?",
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

    def save_dragon_tiger(self, symbol: str, data: dict[str, Any]) -> int:
        """单条保存龙虎榜：改走本地 keyed-UPSERT 批量路径（011 后外部写入缺
        source_record_key 会触发 IntegrityError）。"""
        return self.save_dragon_tiger_batch(
            [{**data, "ts_code": data.get("ts_code", symbol)}]
        )

    def save_dragon_tiger_batch(self, records: list[dict[str, Any]]) -> int:
        """批量保存龙虎榜数据（稳定事件键 UPSERT，同股同日多事件共存）。"""
        valid_records = [r for r in records if r.get("trade_date") and r.get("ts_code")]
        if not valid_records:
            return 0
        try:
            with self._write_lock:
                conn = self._get_write_conn()
                before_changes = conn.total_changes
                conn.executemany(
                    """
                    INSERT INTO dragon_tiger
                        (source_record_key, ts_code, trade_date, close_price,
                         pct_change, net_buy_amount, buy_amount, sell_amount,
                         turnover_rate, market_cap, reason, data_source)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(source_record_key) DO UPDATE SET
                        ts_code = excluded.ts_code,
                        trade_date = excluded.trade_date,
                        close_price = excluded.close_price,
                        pct_change = excluded.pct_change,
                        net_buy_amount = excluded.net_buy_amount,
                        buy_amount = excluded.buy_amount,
                        sell_amount = excluded.sell_amount,
                        turnover_rate = excluded.turnover_rate,
                        market_cap = excluded.market_cap,
                        reason = excluded.reason,
                        data_source = excluded.data_source,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    [
                        (r.get("source_record_key") or dragon_tiger_source_key(r),
                         r.get("ts_code"), r.get("trade_date"), r.get("close_price"),
                         r.get("pct_change"), r.get("net_buy_amount"),
                         r.get("buy_amount"), r.get("sell_amount"),
                         r.get("turnover_rate"), r.get("market_cap"),
                         r.get("reason"), r.get("data_source", "akshare"))
                        for r in valid_records
                    ],
                )
                return self._commit_delta(conn, before_changes)
        except (sqlite3.IntegrityError, sqlite3.OperationalError):
            # 结构性/约束错误必须外抛：让调用方审计标记失败，而非静默记 0
            raise
        except Exception as e:
            logger.warning(f"⚠️ 龙虎榜批量保存失败: {e}")
            return 0

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

    def get_financial_period_coverage(self, periods: list[str]) -> dict[str, int]:
        """查询各报告期的股票覆盖数。"""
        if not periods:
            return {}
        conn = self._get_read_conn()
        try:
            placeholders = ",".join("?" for _ in periods)
            cur = conn.execute(
                f"SELECT report_period, COUNT(DISTINCT ts_code) "
                f"FROM quarterly_financials "
                f"WHERE report_period IN ({placeholders}) "
                f"GROUP BY report_period",
                periods,
            )
            return dict(cur.fetchall())
        finally:
            conn.close()

    def save_financial_history_batch(self, records: list[dict[str, Any]]) -> dict[str, int]:
        """原子写入季度财务历史，同时更新最新视图。"""
        if not records:
            return {"history_saved": 0, "latest_updated": 0}
        with self._write_lock:
            conn = self._get_write_conn()
            before = conn.total_changes
            history_saved = 0
            latest_updated = 0
            for r in records:
                ts_code = r.get("ts_code", "")
                report_period = r.get("report_period", "")
                publish_date = r.get("publish_date", "")
                if not ts_code or not report_period or not publish_date:
                    continue
                conn.execute(
                    """INSERT OR REPLACE INTO quarterly_financials_history
                       (ts_code, report_period, publish_date,
                        revenue, net_profit, deduct_profit,
                        operating_cashflow, rd_expense,
                        gross_margin, net_margin, roe, debt_ratio,
                        revenue_growth, profit_growth, data_source)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        ts_code, report_period, publish_date,
                        r.get("revenue"), r.get("net_profit"), r.get("deduct_profit"),
                        r.get("operating_cashflow"), r.get("rd_expense"),
                        r.get("gross_margin"), r.get("net_margin"),
                        r.get("roe"), r.get("debt_ratio"),
                        r.get("revenue_growth"), r.get("profit_growth"),
                        r.get("data_source", "akshare"),
                    ),
                )
                history_saved += 1
                # Refresh latest view — take the most recent publish_date per period
                conn.execute(
                    """INSERT OR REPLACE INTO quarterly_financials
                       (ts_code, report_period,
                        revenue, net_profit, operating_cashflow,
                        roe, gross_margin, net_margin,
                        revenue_growth, profit_growth, debt_ratio, eps, bps,
                        data_source)
                       SELECT ? AS ts_code, ? AS report_period,
                              revenue, net_profit, operating_cashflow,
                              roe, gross_margin, net_margin,
                              revenue_growth, profit_growth, debt_ratio, eps, bps,
                              data_source
                       FROM quarterly_financials_history
                       WHERE ts_code = ? AND report_period = ?
                       ORDER BY publish_date DESC LIMIT 1""",
                    (ts_code, report_period, ts_code, report_period),
                )
                latest_updated += 1
            self._commit_delta(conn, before)
        return {"history_saved": history_saved, "latest_updated": latest_updated}

    def _get_read_conn(self) -> sqlite3.Connection:
        """创建只读连接（不缓存，用完即关）。"""
        import sqlite3 as _sqlite3
        return _sqlite3.connect(str(self._db.db_path), timeout=10.0)

    def get_financials_as_of(
        self, symbol: str, as_of_date: str,
    ) -> dict[str, Any] | None:
        """返回给定截止日前已知的最新财务数据。"""
        conn = self._get_read_conn()
        try:
            cur = conn.execute(
                """SELECT *
                   FROM quarterly_financials_history
                   WHERE ts_code = ? AND publish_date <= ?
                   ORDER BY report_period DESC, publish_date DESC
                   LIMIT 1""",
                (symbol, as_of_date),
            )
            row = cur.fetchone()
            if row is None:
                return None
            columns = [d[0] for d in cur.description]
            return dict(zip(columns, row, strict=True))
        finally:
            conn.close()

    def save_block_trade(self, symbol: str, data: dict[str, Any]) -> int:
        """单条保存大宗交易：改走本地 keyed-UPSERT 批量路径（011 后外部写入缺
        source_record_key 会触发 IntegrityError）。"""
        return self.save_block_trade_batch(
            [{**data, "ts_code": data.get("ts_code", symbol)}]
        )

    def save_block_trade_batch(self, records: list[dict[str, Any]]) -> int:
        """批量保存大宗交易数据（稳定事件键 UPSERT，同股同日多笔交易共存）。"""
        valid_records = [r for r in records if r.get("trade_date") and r.get("ts_code")]
        if not valid_records:
            return 0
        try:
            with self._write_lock:
                conn = self._get_write_conn()
                before_changes = conn.total_changes
                conn.executemany(
                    """
                    INSERT INTO block_trade
                        (source_record_key, ts_code, trade_date, deal_price,
                         close_price, discount_rate, volume, amount,
                         buyer_branch, seller_branch, data_source)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(source_record_key) DO UPDATE SET
                        ts_code = excluded.ts_code,
                        trade_date = excluded.trade_date,
                        deal_price = excluded.deal_price,
                        close_price = excluded.close_price,
                        discount_rate = excluded.discount_rate,
                        volume = excluded.volume,
                        amount = excluded.amount,
                        buyer_branch = excluded.buyer_branch,
                        seller_branch = excluded.seller_branch,
                        data_source = excluded.data_source,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    [
                        (r.get("source_record_key") or block_trade_source_key(r),
                         r.get("ts_code"), r.get("trade_date"), r.get("deal_price"),
                         r.get("close_price"), r.get("discount_rate"),
                         r.get("volume"), r.get("amount"),
                         r.get("buyer_branch"), r.get("seller_branch"),
                         r.get("data_source", "akshare"))
                        for r in valid_records
                    ],
                )
                return self._commit_delta(conn, before_changes)
        except (sqlite3.IntegrityError, sqlite3.OperationalError):
            # 结构性/约束错误必须外抛：让调用方审计标记失败，而非静默记 0
            raise
        except Exception as e:
            logger.warning(f"⚠️ 大宗交易批量保存失败: {e}")
            return 0

    def get_block_trade(self, symbol: str, date: str = None) -> dict | None:
        return self._db.get_block_trade(symbol, date)

    def save_placement_batch(self, records: list[dict[str, Any]]) -> int:
        """批量保存定增公告数据（稳定源键 UPSERT，老公告重抓走更新）。"""
        valid_records = [r for r in records if r.get("issue_date") and r.get("ts_code")]
        if not valid_records:
            return 0
        try:
            with self._write_lock:
                conn = self._get_write_conn()
                before_changes = conn.total_changes
                conn.executemany(
                    """
                    INSERT INTO placement_announcements
                        (source_record_key, ts_code, symbol, name,
                         issue_method, issue_date, data_source)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(source_record_key) DO UPDATE SET
                        ts_code = excluded.ts_code,
                        symbol = excluded.symbol,
                        name = excluded.name,
                        issue_method = excluded.issue_method,
                        issue_date = excluded.issue_date,
                        data_source = excluded.data_source,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    [
                        (r.get("source_record_key") or placement_source_key(r),
                         r.get("ts_code"), r.get("symbol"), r.get("name"),
                         r.get("issue_method"), r.get("issue_date"),
                         r.get("data_source", "akshare"))
                        for r in valid_records
                    ],
                )
                return self._commit_delta(conn, before_changes)
        except (sqlite3.IntegrityError, sqlite3.OperationalError):
            # 结构性/约束错误必须外抛：让调用方审计标记失败，而非静默记 0
            raise
        except Exception as e:
            logger.warning(f"⚠️ 定增公告批量保存失败: {e}")
            return 0

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

    def save_usd_batch(self, records: list[dict[str, Any]]) -> int:
        return self._db.save_usd_batch(records)

    def save_global_index_batch(self, records: list[dict[str, Any]]) -> int:
        return self._db.save_global_index_batch(records)

    def save_us_treasury_batch(self, records: list[dict[str, Any]]) -> int:
        return self._db.save_us_treasury_batch(records)

    def save_us_macro_batch(self, records: list[dict[str, Any]]) -> int:
        """批量保存美国日度宏观利率（FRED）到 us_macro_daily 表。

        使用原始 SQL 因 DatabaseManager（外部包）无该方法；表由
        migrations/012_us_macro_daily.sql 创建，dgs2/icsa 两列由
        migrations/014_us_macro_extend.sql 追加。
        """
        if not records:
            return 0
        try:
            with self._write_lock:
                conn = self._get_write_conn()
                before_changes = conn.total_changes
                conn.executemany(
                    """
                    INSERT OR REPLACE INTO us_macro_daily (
                        trade_date, effr, dgs2, dgs3mo, dgs10, t10yie, icsa,
                        spread_10y_3m, real_rate_10y, data_source
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            r["trade_date"],
                            r.get("effr"),
                            r.get("dgs2"),
                            r.get("dgs3mo"),
                            r.get("dgs10"),
                            r.get("t10yie"),
                            r.get("icsa"),
                            r.get("spread_10y_3m"),
                            r.get("real_rate_10y"),
                            r.get("data_source"),
                        )
                        for r in records
                    ],
                )
                return self._commit_delta(conn, before_changes)
        except Exception as e:
            logging.getLogger(__name__).warning(f"⚠️ 美国宏观利率批量保存失败: {e}")
            return 0

    def save_hk_tech_index_batch(self, records: list[dict[str, Any]]) -> int:
        """批量保存恒生科技指数日线到 hk_tech_index_daily 表。

        使用原始 SQL 因 DatabaseManager（外部包）无该方法；表由
        migrations/015_hk_tech_index_daily.sql 创建。
        """
        if not records:
            return 0
        try:
            with self._write_lock:
                conn = self._get_write_conn()
                before_changes = conn.total_changes
                conn.executemany(
                    """
                    INSERT OR REPLACE INTO hk_tech_index_daily (
                        trade_date, open, high, low, close, change_pct,
                        volume, amount, data_source
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            r["trade_date"],
                            r.get("open"),
                            r.get("high"),
                            r.get("low"),
                            r.get("close"),
                            r.get("change_pct"),
                            r.get("volume"),
                            r.get("amount"),
                            r.get("data_source"),
                        )
                        for r in records
                    ],
                )
                return self._commit_delta(conn, before_changes)
        except Exception as e:
            logging.getLogger(__name__).warning(f"⚠️ 恒生科技指数批量保存失败: {e}")
            return 0

    def get_hk_tech_latest_date(self) -> str | None:
        """轻量查询：直接 SQL 取 hk_tech_index_daily 的 MAX(trade_date)。"""
        try:
            with sqlite3.connect(str(self._db.db_path), timeout=5.0) as conn:
                cursor = conn.execute("SELECT MAX(trade_date) FROM hk_tech_index_daily")
                row = cursor.fetchone()
                return row[0] if row and row[0] else None
        except Exception:
            return None

    def save_futures_daily_batch(self, records: list[dict[str, Any]]) -> int:
        return self._db.save_futures_daily_batch(records)

    def save_global_assets_bars_batch(self, records: list[dict[str, Any]]) -> int:
        return self._db.save_global_assets_bars_batch(records)

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

    def save_concept_member_history_batch(
        self,
        records: list[dict[str, Any]],
        run_id: str,
        valid_from: str,
    ) -> int:
        """批量保存概念板块成分股 PIT 历史快照。"""
        if not records:
            return 0
        try:
            valid_to = _prev_date(valid_from)
            with self._write_lock:
                conn = self._get_write_conn()
                before_changes = conn.total_changes
                try:
                    # close previous active records, but NOT same-day snapshots
                    # (they get upserted below; closing them would corrupt the interval)
                    conn.execute(
                        "UPDATE concept_member_history SET valid_to = ? "
                        "WHERE valid_to IS NULL AND valid_from < ?",
                        (valid_to, valid_from),
                    )
                    # insert new snapshot (upsert on PK, idempotent for same-day reruns)
                    conn.executemany(
                        """INSERT INTO concept_member_history
                           (concept_code, concept_name, ts_code, valid_from, valid_to, source, snapshot_run_id)
                           VALUES (?, ?, ?, ?, NULL, ?, ?)
                           ON CONFLICT(concept_code, ts_code, valid_from) DO UPDATE SET
                               concept_name = excluded.concept_name,
                               source = excluded.source,
                               snapshot_run_id = excluded.snapshot_run_id""",
                        [
                            (r.get("concept_code"), r.get("concept_name"), r.get("ts_code"),
                             valid_from, r.get("source", "akshare"), run_id)
                            for r in records
                        ],
                    )
                    return self._commit_delta(conn, before_changes)
                except Exception:
                    conn.rollback()
                    raise
        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.warning(f"⚠️ 概念板块成分股 PIT 历史保存失败: {e}")
            return 0

    def save_index_member_history_batch(
        self,
        records: list[dict[str, Any]],
        run_id: str,
        valid_from: str,
    ) -> int:
        """批量保存指数成分股 PIT 历史快照。"""
        if not records:
            return 0
        try:
            valid_to = _prev_date(valid_from)
            with self._write_lock:
                conn = self._get_write_conn()
                before_changes = conn.total_changes
                try:
                    # close previous active records, excluding same-day snapshots
                    # (they are upserted below; closing them would corrupt the interval)
                    conn.execute(
                        "UPDATE index_member_history SET valid_to = ? "
                        "WHERE valid_to IS NULL AND valid_from < ?",
                        (valid_to, valid_from),
                    )
                    # insert new snapshot (upsert on PK, idempotent for same-day reruns)
                    conn.executemany(
                        """INSERT INTO index_member_history
                           (index_code, index_name, ts_code, weight, valid_from, valid_to, source, snapshot_run_id)
                           VALUES (?, ?, ?, ?, ?, NULL, ?, ?)
                           ON CONFLICT(index_code, ts_code, valid_from) DO UPDATE SET
                               index_name = excluded.index_name,
                               weight = excluded.weight,
                               source = excluded.source,
                               snapshot_run_id = excluded.snapshot_run_id""",
                        [
                            (r.get("index_code"), r.get("index_name"), r.get("ts_code"),
                             r.get("weight"), valid_from, r.get("source", "akshare"), run_id)
                            for r in records
                        ],
                    )
                    return self._commit_delta(conn, before_changes)
                except Exception:
                    conn.rollback()
                    raise
        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.warning(f"⚠️ 指数成分股 PIT 历史保存失败: {e}")
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
                        conn.execute("ALTER TABLE cb_quotation ADD COLUMN updated_at DATETIME")
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
                        conn.execute("ALTER TABLE cb_redeem ADD COLUMN updated_at DATETIME")
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
                    """
                    INSERT INTO stock_repurchase
                        (source_record_key, trade_date, stock_code, stock_name,
                         repurchase_amount, repurchase_price,
                         repurchase_price_lower, repurchase_price_upper,
                         repurchase_quantity, progress_status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(source_record_key) DO NOTHING
                    """,
                    [
                        (source_record_key(r, STOCK_REPURCHASE_SOURCE_KEY_FIELDS),
                         r.get("trade_date"), r.get("stock_code"), r.get("stock_name"),
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
                        (source_record_key, trade_date, stock_code, stock_name,
                         survey_org, survey_type, survey_count)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(source_record_key) DO NOTHING
                    """,
                    [
                        (source_record_key(r, INSTITUTION_SURVEY_SOURCE_KEY_FIELDS),
                         r.get("trade_date"), r.get("stock_code"), r.get("stock_name"),
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
                    """
                    INSERT INTO stock_pledge
                        (source_record_key, trade_date, stock_code, stock_name,
                         pledger, pledge_amount, pledge_ratio, pledge_org)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(source_record_key) DO UPDATE SET
                        trade_date = excluded.trade_date,
                        stock_code = excluded.stock_code,
                        stock_name = excluded.stock_name,
                        pledger = excluded.pledger,
                        pledge_amount = excluded.pledge_amount,
                        pledge_ratio = excluded.pledge_ratio,
                        pledge_org = excluded.pledge_org
                    """,
                    [
                        (r.get("source_record_key") or stock_pledge_source_key(r),
                         r.get("trade_date"), r.get("stock_code"), r.get("stock_name"),
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
    """基于 DataLoader 的数据加载 provider。

    日线兜底链：DataLoader 内部链（东财 → 新浪 → 腾讯 → 雪球）失败后，
    由本层追加 hithink（同花顺官方 API，core/source_hithink.py）兜底。
    hithink 未配置 Key 或熔断时静默跳过，行为等同接入前。
    """

    def __init__(self, use_cache: bool = True):
        self._loader = DataLoader(use_cache=use_cache)

    def _hithink_fallback(
        self,
        symbol: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        """东财/新浪/腾讯/雪球全链失败后的 hithink 兜底取数。"""
        from core import source_hithink
        from core.source_client import get_default_client

        client = source_hithink.get_hithink_client()
        if not client.available:
            return pd.DataFrame()
        resp = get_default_client().call(
            "hithink", client.fetch_daily_bars, symbol,
            start_date=start_date, end_date=end_date,
        )
        if resp.success and resp.data is not None and not resp.data.empty:
            logger.info(f"✅ {symbol} hithink 兜底成功 ({len(resp.data)} 条)")
            return resp.data
        if not resp.success:
            logger.warning(f"⚠️ {symbol} hithink 兜底失败: {resp.metadata.error}")
        return pd.DataFrame()

    def get_daily_bars(
        self,
        symbol: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        df = self._loader.get_daily_bars(symbol, start_date, end_date)
        if df is None or df.empty:
            hithink_df = self._hithink_fallback(symbol, start_date, end_date)
            if not hithink_df.empty:
                return hithink_df
        return df

    def incremental_update(
        self,
        symbol: str,
        existing_df: pd.DataFrame,
    ) -> pd.DataFrame:
        df = self._loader.incremental_update(symbol, existing_df)
        if existing_df is None or existing_df.empty:
            return df
        if df is not None and len(df) > len(existing_df):
            return df
        # 全链未取到新数据：用 hithink 补齐缺口（最后一日的次日起至今天）
        date_col = "trade_date" if "trade_date" in existing_df.columns else "date"
        if date_col not in existing_df.columns:
            return df
        last_date = pd.to_datetime(existing_df[date_col]).max()
        today = pd.Timestamp.now().normalize()
        if pd.isna(last_date) or last_date >= today:
            return df
        hithink_df = self._hithink_fallback(
            symbol,
            start_date=(last_date + pd.Timedelta(days=1)).strftime("%Y%m%d"),
            end_date=today.strftime("%Y%m%d"),
        )
        if hithink_df.empty:
            return df
        # 归一化既有数据到 loader 列规范（trade_date→date、turnover_rate→turnover）
        # 再合并去重，与 DataLoader.incremental_update 的合并语义保持一致
        base = existing_df.copy()
        if "trade_date" in base.columns and "date" not in base.columns:
            base = base.rename(columns={"trade_date": "date"})
        if "turnover_rate" in base.columns and "turnover" not in base.columns:
            base = base.rename(columns={"turnover_rate": "turnover"})
        base["date"] = pd.to_datetime(base["date"])
        merged = pd.concat([base, hithink_df], ignore_index=True)
        merged = merged.drop_duplicates(subset=["date"], keep="last")
        merged = merged.sort_values("date").reset_index(drop=True)
        logger.info(f"✅ {symbol} hithink 增量兜底成功 (新增 {len(hithink_df)} 条)")
        return merged

    def get_market_valuation(self) -> pd.DataFrame:
        return self._loader.get_market_valuation()

    def get_market_fund_flow(self) -> pd.DataFrame:
        return self._loader.get_market_fund_flow()

    def fetch_global_assets_bars(self, symbol: str, start_date: str | None = None, end_date: str | None = None) -> pd.DataFrame:
        import yfinance as yf
        ticker = yf.Ticker(symbol)

        # yfinance doesn't take None for start/end in history the same way as strings,
        # but if we just want max:
        if start_date is None:
            df = ticker.history(period="max")
        else:
            # yfinance expects YYYY-MM-DD
            if end_date is None:
                df = ticker.history(start=start_date)
            else:
                df = ticker.history(start=start_date, end=end_date)

        if df.empty:
            return df

        df = df.reset_index()
        # Rename columns to match schema
        # Date -> trade_date, Open -> open, High -> high, Low -> low, Close -> close, Volume -> volume
        # Note: yfinance returns 'Date' or 'Datetime'
        date_col = 'Date' if 'Date' in df.columns else 'Datetime'
        if date_col not in df.columns:
            return pd.DataFrame()

        df['trade_date'] = pd.to_datetime(df[date_col]).dt.strftime('%Y-%m-%d')
        df = df.rename(columns={
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'Volume': 'volume'
        })

        # yfinance history already returns adj_close as 'Close' if auto_adjust is True (default)
        # But we can store it as adj_close just to be clear, and let close be close
        df['adj_close'] = df['close']

        # Keep only needed columns
        keep_cols = ['trade_date', 'open', 'high', 'low', 'close', 'adj_close', 'volume']
        df = df[[c for c in keep_cols if c in df.columns]]
        df['ts_code'] = symbol
        return df


# ===========================================================================
# Indicator Engine Provider
# ===========================================================================

class SmartMoneyIndicatorProvider:
    """基于 IndicatorCalculator 的指标计算 provider。"""

    def __init__(self):
        self._calc = IndicatorCalculator()

    def calculate_all_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        return self._calc.calculate_all_indicators(df)
