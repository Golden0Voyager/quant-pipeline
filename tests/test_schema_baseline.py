"""基线 schema 契约测试。

``providers.SmartMoneyDBProvider._ensure_tables`` 创建的表里有 26 张
``migrations/`` 根本没有创建过，所以它是**基线**而不是「兜底」。这层契约一直
没人断言，而全链路都是 ``CREATE TABLE IF NOT EXISTS``——谁先运行谁定 schema——
因此一旦基线漏表或被改窄，只会在运行时表现为「静默少了一张表/一个唯一约束」，
不会有任何测试变红。

这里把契约钉死：基线表必须全部存在、必须有唯一索引（``INSERT ... ON CONFLICT``
与 ``INSERT OR REPLACE`` 的命脉），关键表的列集不得收窄；同时断言建表失败是
硬失败而不是只打 WARNING。
"""
from __future__ import annotations

import sqlite3
from unittest.mock import patch

import pytest

from providers import SmartMoneyDBProvider

# providers._ensure_tables 的基线表全集（26 张不被任何 migration 创建）
BASELINE_TABLES = {
    "ah_premium",
    "cb_index",
    "cb_quotation",
    "cb_redeem",
    "central_bank_balance",
    "chip_distribution",
    "chip_distribution_em",
    "concept_board",
    "concept_member",
    "earnings_forecast",
    "etf_daily",
    "fundamentals",
    "index_futures_basis",
    "institution_survey",
    "macro_monthly",
    "macro_quarterly",
    "market_valuation",
    "money_market",
    "north_hold",
    "option_sentiment",
    "placement_announcements",
    "restricted_share",
    "sector_daily",
    "sector_valuation",
    "south_flow",
    "stock_list",
    "stock_pledge",
    "stock_repurchase",
}

# 关键表的列必须是超集（列被改窄 → UPSERT/批量写入在运行时才炸）
REQUIRED_COLUMNS = {
    "stock_list": {"code", "name", "market", "industry", "updated_at"},
    "chip_distribution_em": {
        "ts_code", "trade_date", "profit_ratio", "avg_cost",
        "cost_90_low", "cost_90_high", "concentration_90",
        "cost_70_low", "cost_70_high", "concentration_70", "chip_concentration",
    },
    "north_hold": {
        "ts_code", "trade_date", "close_price", "hold_shares",
        "hold_market_cap", "hold_shares_ratio", "free_shares_ratio",
        "total_shares_ratio", "data_source", "updated_at",
    },
    "stock_repurchase": {
        "trade_date", "stock_code", "stock_name", "repurchase_amount",
        "repurchase_price", "repurchase_price_lower", "repurchase_price_upper",
        "repurchase_quantity", "progress_status", "source_record_key",
    },
    "option_sentiment": {
        "trade_date", "qvix", "pcr", "put_volume", "call_volume",
        "put_oi", "call_oi", "implied_vol_avg",
    },
    "market_valuation": {
        "date", "pe_median", "pb_median", "equity_bond_spread",
        "ebs_ma", "csi300_close",
    },
}


@pytest.fixture
def baseline_db(tmp_path):
    """在临时库上只跑基线 DDL 并返回 (provider, db_path)。

    conftest 把 ``DatabaseManager`` mock 成固定全局路径，所以必须在构造后
    显式把 ``_db.db_path`` 绑到本用例的库，再补跑一次 ``_ensure_tables()``。
    """
    db_path = tmp_path / "baseline.db"
    provider = SmartMoneyDBProvider(db_path=str(db_path))
    provider._db.db_path = str(db_path)
    provider._ensure_tables()
    yield provider, db_path
    provider.close()


def _tables(db_path) -> set[str]:
    with sqlite3.connect(db_path) as conn:
        return {
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }


def test_baseline_tables_all_created(baseline_db):
    _, db_path = baseline_db
    missing = BASELINE_TABLES - _tables(db_path)
    assert not missing, f"基线 DDL 未创建这些表: {sorted(missing)}"


def test_every_baseline_table_has_a_unique_index(baseline_db):
    """唯一约束是 ``ON CONFLICT`` / ``INSERT OR REPLACE`` 的前提。

    基线里 stock_pledge 没有内联 UNIQUE，它的唯一性由 migration 011 的
    ``idx_stock_pledge_source_key`` 提供；因此这里只对**基线自建**的表断言，
    不看迁移补的索引，以便在有人挪走 UNIQUE 时立刻失败。
    """
    _, db_path = baseline_db
    without_unique = []
    with sqlite3.connect(db_path) as conn:
        for table in sorted(BASELINE_TABLES - {"stock_pledge"}):
            unique = [r for r in conn.execute(f"PRAGMA index_list({table})") if r[2] == 1]
            if not unique:
                without_unique.append(table)
    assert not without_unique, (
        f"这些基线表缺少唯一索引，UPSERT 会退化为重复插入: {without_unique}"
    )


@pytest.mark.parametrize("table", sorted(REQUIRED_COLUMNS))
def test_baseline_table_columns_not_narrowed(baseline_db, table):
    _, db_path = baseline_db
    with sqlite3.connect(db_path) as conn:
        columns = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
    missing = REQUIRED_COLUMNS[table] - columns
    assert not missing, f"{table} 缺少列: {sorted(missing)}"


def test_ensure_tables_fails_closed_on_ddl_error(tmp_path):
    """建表失败必须硬失败。

    旧行为只打一条 WARNING 就继续，provider 于是在缺 26 张表的状态下运行，
    把建表失败伪装成「没有数据」；而紧随其后的 ``_run_versioned_migrations``
    本来就是硬失败口径，所以硬失败不会引入新的失败场景。
    """
    db_path = tmp_path / "broken.db"
    with (
        patch(
            "providers.sqlite3.connect",
            side_effect=sqlite3.OperationalError("disk I/O error"),
        ),
        pytest.raises(RuntimeError, match="基线建表失败"),
    ):
        SmartMoneyDBProvider(db_path=str(db_path))


def test_ensure_tables_skips_missing_parent_dir():
    """库目录不存在时不建表也不报错（与 ``_ensure_wal_mode`` 口径一致）。"""
    provider = SmartMoneyDBProvider(db_path="/nonexistent_dir_for_test/quant.db")
    assert provider.db_path is not None
