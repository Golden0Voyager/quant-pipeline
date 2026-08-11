"""Coverage sprint: batch save methods, edge cases, and connection logic.

Target: providers.py coverage 66% → 85%+.

Strategy
--------
- tmp_path 真实 SQLite 数据库，无 mock
- 参数化测试批量覆盖全部 ~22 个 batch save 方法的 happy path
- 单独覆盖 empty record 路径和 exception 路径
- 覆盖 _ensure_wal_mode / _get_write_conn / _commit_delta 等底层方法
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from core.source_record_key import source_record_key
from providers import SmartMoneyDBProvider

# ═══════════════════════════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════════════════════════


@pytest.fixture
def provider(tmp_path: Path) -> SmartMoneyDBProvider:
    """创建一个指向临时数据库的 SmartMoneyDBProvider。

    ``_ensure_tables()`` 会在 __init__ 中自动创建所有必要表。
    """
    db_path = tmp_path / "quant_core_test.db"
    instance = SmartMoneyDBProvider(db_path=str(db_path))
    # conftest mocks DatabaseManager and ignores db_path; bind this fixture to
    # its own SQLite file so source-record preserving tests stay isolated.
    instance._db.db_path = str(db_path)
    instance._ensure_wal_mode()
    instance._ensure_tables()
    instance._run_versioned_migrations()
    return instance


def _audit_payload(run_id: str, status: str, saved: int) -> dict[str, Any]:
    return {
        "task_name": "pit_task",
        "status": status,
        "saved": saved,
        "metadata": {
            "run_id": run_id,
            "started_at": "2026-07-25T00:00:00+00:00",
            "finished_at": "2026-07-25T00:00:01+00:00",
        },
    }


def test_record_ingestion_run_upserts_same_parent(provider):
    provider.record_ingestion_run(_audit_payload("run-1", "running", 0))
    provider.record_ingestion_run(_audit_payload("run-1", "success", 3))

    with sqlite3.connect(provider.db_path) as conn:
        rows = conn.execute(
            "SELECT run_id, status, saved_rows FROM ingestion_runs WHERE run_id = ?",
            ("run-1",),
        ).fetchall()

    assert rows == [("run-1", "success", 3)]


def test_record_ingestion_run_retries_transient_database_lock(provider):
    """短暂写锁应重试并最终提交同一条审计记录。"""
    conn = MagicMock()
    conn.execute.side_effect = [sqlite3.OperationalError("database is locked"), None]
    audit_context = MagicMock()
    audit_context.__enter__.return_value = conn

    with patch.object(provider, "_connect_for_audit", return_value=audit_context):
        provider.record_ingestion_run(_audit_payload("lock-run", "success", 1))

    assert conn.execute.call_count == 2


def test_record_ingestion_run_does_not_retry_non_lock_error(provider):
    """表结构等非锁错误必须立即暴露，不能伪装为瞬时竞争。"""
    conn = MagicMock()
    conn.execute.side_effect = sqlite3.OperationalError("no such table: ingestion_runs")
    audit_context = MagicMock()
    audit_context.__enter__.return_value = conn

    with patch.object(provider, "_connect_for_audit", return_value=audit_context), \
         pytest.raises(sqlite3.OperationalError, match="no such table"):
        provider.record_ingestion_run(_audit_payload("schema-run", "success", 1))


def test_shared_write_connection_enables_foreign_keys(provider):
    conn = provider._get_write_conn()
    assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1


def test_pit_write_without_audit_parent_is_rejected(provider):
    saved = provider.save_index_member_history_batch(
        [
            {
                "index_code": "000300",
                "index_name": "沪深300",
                "ts_code": "000001.SZ",
                "weight": 1.0,
                "source": "akshare",
            }
        ],
        run_id="missing-parent",
        valid_from="2026-07-25",
    )

    assert saved == 0
    with sqlite3.connect(provider.db_path) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM index_member_history "
            "WHERE snapshot_run_id = 'missing-parent'"
        ).fetchone()[0]
    assert count == 0


def test_source_record_key_trims_strings_and_hashes_to_sha256():
    key1 = source_record_key(
        {
            "trade_date": "2026-07-21",
            "stock_code": "000001",
            "stock_name": " Ping An Bank ",
        },
        ("trade_date", "stock_code", "stock_name"),
    )
    key2 = source_record_key(
        {
            "trade_date": "2026-07-21",
            "stock_code": "000001",
            "stock_name": "Ping An Bank",
        },
        ("trade_date", "stock_code", "stock_name"),
    )

    assert key1 == key2
    assert len(key1) == 64


def test_source_record_key_treats_missing_values_as_null():
    assert source_record_key({"survey_org": None}, ("survey_org",)) == source_record_key(
        {},
        ("survey_org",),
    )


def test_index_history_failed_write_rolls_back_active_interval(provider):
    provider.record_ingestion_run(_audit_payload("index-old", "success", 1))
    assert provider.save_index_member_history_batch(
        [
            {
                "index_code": "000300",
                "index_name": "沪深300",
                "ts_code": "000001.SZ",
                "weight": 1.0,
                "source": "akshare",
            }
        ],
        run_id="index-old",
        valid_from="2026-07-20",
    ) == 1
    provider.record_ingestion_run(_audit_payload("index-new", "success", 1))

    assert provider.save_index_member_history_batch(
        [
            {
                "index_code": "000300",
                "index_name": "沪深300",
                "ts_code": "000002.SZ",
                "weight": 1.0,
                "source": "akshare",
            }
        ],
        run_id="missing-index-parent",
        valid_from="2026-07-21",
    ) == 0

    with sqlite3.connect(provider.db_path) as conn:
        assert conn.execute(
            "SELECT valid_to FROM index_member_history WHERE snapshot_run_id = ?",
            ("index-old",),
        ).fetchall() == [(None,)]

    assert provider.save_index_member_history_batch(
        [
            {
                "index_code": "000300",
                "index_name": "沪深300",
                "ts_code": "000002.SZ",
                "weight": 1.0,
                "source": "akshare",
            }
        ],
        run_id="index-new",
        valid_from="2026-07-25",
    ) == 2

    with sqlite3.connect(provider.db_path) as conn:
        assert conn.execute(
            "SELECT valid_to FROM index_member_history WHERE snapshot_run_id = ?",
            ("index-old",),
        ).fetchall() == [("2026-07-24",)]


def test_index_history_same_day_rerun_is_idempotent(provider):
    """同日重复运行不得触发 UNIQUE 冲突，也不得误关当天快照。"""
    payload = {
        "index_code": "000300",
        "index_name": "沪深300",
        "ts_code": "000001.SZ",
        "weight": 1.0,
        "source": "akshare",
    }
    provider.record_ingestion_run(_audit_payload("index-run-1", "success", 1))
    assert provider.save_index_member_history_batch([payload], run_id="index-run-1", valid_from="2026-07-25") == 1

    provider.record_ingestion_run(_audit_payload("index-run-2", "success", 1))
    result = provider.save_index_member_history_batch([payload], run_id="index-run-2", valid_from="2026-07-25")
    assert result >= 0

    with sqlite3.connect(provider.db_path) as conn:
        rows = conn.execute(
            "SELECT valid_to, snapshot_run_id FROM index_member_history "
            "WHERE index_code = '000300' AND valid_from = '2026-07-25'"
        ).fetchall()
    # 同日快照保持活跃（未被误关），run_id 被最新一次覆盖
    assert rows == [(None, "index-run-2")]
    assert conn.execute(
        "SELECT COUNT(*) FROM index_member_history "
        "WHERE valid_from = '2026-07-25' AND valid_to IS NULL"
    ).fetchone()[0] == 1


def test_concept_history_same_day_rerun_is_idempotent(provider):
    """概念板块同日重复运行同样必须幂等。"""
    payload = {
        "concept_code": "BK0001",
        "concept_name": "测试概念",
        "ts_code": "000001.SZ",
        "source": "akshare",
    }
    provider.record_ingestion_run(_audit_payload("concept-run-1", "success", 1))
    assert provider.save_concept_member_history_batch([payload], run_id="concept-run-1", valid_from="2026-07-25") == 1

    provider.record_ingestion_run(_audit_payload("concept-run-2", "success", 1))
    result = provider.save_concept_member_history_batch([payload], run_id="concept-run-2", valid_from="2026-07-25")
    assert result >= 0

    with sqlite3.connect(provider.db_path) as conn:
        rows = conn.execute(
            "SELECT valid_to, snapshot_run_id FROM concept_member_history "
            "WHERE concept_code = 'BK0001' AND valid_from = '2026-07-25'"
        ).fetchall()
    assert rows == [(None, "concept-run-2")]


def test_concept_history_failed_write_rolls_back_active_interval(provider):
    provider.record_ingestion_run(_audit_payload("concept-old", "success", 1))
    assert provider.save_concept_member_history_batch(
        [
            {
                "concept_code": "BK0001",
                "concept_name": "测试概念",
                "ts_code": "000001.SZ",
                "source": "akshare",
            }
        ],
        run_id="concept-old",
        valid_from="2026-07-20",
    ) == 1
    provider.record_ingestion_run(_audit_payload("concept-new", "success", 1))

    assert provider.save_concept_member_history_batch(
        [
            {
                "concept_code": "BK0001",
                "concept_name": "测试概念",
                "ts_code": "000002.SZ",
                "source": "akshare",
            }
        ],
        run_id="missing-concept-parent",
        valid_from="2026-07-21",
    ) == 0

    with sqlite3.connect(provider.db_path) as conn:
        assert conn.execute(
            "SELECT valid_to FROM concept_member_history WHERE snapshot_run_id = ?",
            ("concept-old",),
        ).fetchall() == [(None,)]

    assert provider.save_concept_member_history_batch(
        [
            {
                "concept_code": "BK0001",
                "concept_name": "测试概念",
                "ts_code": "000002.SZ",
                "source": "akshare",
            }
        ],
        run_id="concept-new",
        valid_from="2026-07-25",
    ) == 2

    with sqlite3.connect(provider.db_path) as conn:
        assert conn.execute(
            "SELECT valid_to FROM concept_member_history WHERE snapshot_run_id = ?",
            ("concept-old",),
        ).fetchall() == [("2026-07-24",)]


def test_ingestion_rejection_without_parent_is_rejected(provider):
    with pytest.raises(sqlite3.IntegrityError):
        provider.record_ingestion_rejection(
            "missing-rejection-parent",
            row_number=1,
            reason="invalid row",
            payload={"value": "bad"},
        )

    with sqlite3.connect(provider.db_path) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM ingestion_rejections WHERE run_id = ?",
            ("missing-rejection-parent",),
        ).fetchone()[0]
    assert count == 0


# ═══════════════════════════════════════════════════════════════════════════════
# 1. 底层方法
# ═══════════════════════════════════════════════════════════════════════════════


class TestConnectionEdgeCases:
    """_ensure_wal_mode / _get_write_conn / _commit_delta 测试。"""

    def test_ensure_wal_nonexistent_parent(self):
        """父目录不存在 → _ensure_wal_mode 静默返回。"""
        p = SmartMoneyDBProvider(db_path="/nonexistent_deep/test.db")
        assert p.db_path is not None

    def test_get_write_conn_returns_same_connection(self, provider):
        """_get_write_conn 返回同一个连接（复用）。"""
        conn1 = provider._get_write_conn()
        conn2 = provider._get_write_conn()
        assert conn1 is conn2

    def test_get_write_conn_has_wal_mode(self, provider):
        """_get_write_conn 连接启用 WAL 模式。"""
        conn = provider._get_write_conn()
        cursor = conn.execute("PRAGMA journal_mode")
        mode = cursor.fetchone()[0]
        assert mode.upper() == "WAL"

    def test_commit_delta_returns_correct_count(self, provider):
        """_commit_delta 返回本次事务产生的变更数。"""
        conn = provider._get_write_conn()
        before = conn.total_changes
        conn.execute("CREATE TABLE IF NOT EXISTS _delta_test (id INTEGER PRIMARY KEY, val TEXT)")
        conn.execute("INSERT INTO _delta_test VALUES (1, 'hello')")
        delta = provider._commit_delta(conn, before)
        assert delta >= 1
        # 清理
        conn.execute("DROP TABLE IF EXISTS _delta_test")

    def test_get_latest_bar_date_no_data(self, provider):
        """无 daily_bars 数据 → 返回 None。"""
        assert provider.get_latest_bar_date("000001.SZ") is None

    def test_phase2_schema_migrates_legacy_tables(self, tmp_path):
        db_path = tmp_path / "legacy.db"
        with sqlite3.connect(db_path) as conn:
            conn.execute("""
                CREATE TABLE stock_repurchase (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trade_date TEXT,
                    stock_code TEXT,
                    stock_name TEXT,
                    repurchase_amount REAL,
                    repurchase_price REAL,
                    repurchase_quantity INTEGER,
                    progress_status TEXT,
                    UNIQUE(trade_date, stock_code)
                )
            """)
            conn.execute("""
                CREATE TABLE institution_survey (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trade_date TEXT,
                    stock_code TEXT,
                    stock_name TEXT,
                    survey_org TEXT,
                    survey_type TEXT,
                    survey_count INTEGER,
                    UNIQUE(trade_date, stock_code, survey_org)
                )
            """)
            conn.execute("INSERT INTO institution_survey (trade_date, stock_code) VALUES (NULL, '')")

        migrated = SmartMoneyDBProvider(db_path=str(db_path))
        # conftest 的 DatabaseManager mock 固定使用全局路径，显式绑定本用例数据库。
        migrated._db.db_path = str(db_path)
        migrated._old_migrate_phase2_tables()
        try:
            with sqlite3.connect(db_path) as conn:
                columns = {row[1] for row in conn.execute("PRAGMA table_info(stock_repurchase)")}
                assert {"repurchase_price_lower", "repurchase_price_upper"} <= columns
                assert conn.execute("SELECT COUNT(*) FROM institution_survey").fetchone()[0] == 0

            record = {
                "trade_date": "2026-07-21",
                "stock_code": "000001",
                "stock_name": "平安银行",
                "survey_type": "电话会议",
                "survey_count": 3,
            }
            assert migrated.save_institution_survey_batch([record]) == 1
            assert migrated.save_institution_survey_batch([{**record, "survey_count": 4}]) >= 1
            with sqlite3.connect(db_path) as conn:
                rows = conn.execute(
                    "SELECT survey_count FROM institution_survey WHERE trade_date = ? AND stock_code = ?",
                    ("2026-07-21", "000001"),
                ).fetchall()
            assert rows == [(3,), (4,)]
        finally:
            migrated.close()


class TestSourceRecordStorage:
    """Repurchase and survey storage preserves distinct source records."""

    def test_stock_repurchase_keeps_distinct_same_day_records(self, provider):
        records = [
            {
                "trade_date": "2026-07-21",
                "stock_code": "000001",
                "stock_name": "Ping An Bank",
                "repurchase_amount": 100.0,
                "repurchase_price": 12.0,
                "repurchase_price_lower": 11.0,
                "repurchase_price_upper": 13.0,
                "repurchase_quantity": 10,
                "progress_status": "planned",
            },
            {
                "trade_date": "2026-07-21",
                "stock_code": "000001",
                "stock_name": "Ping An Bank",
                "repurchase_amount": 120.0,
                "repurchase_price": 12.0,
                "repurchase_price_lower": 11.0,
                "repurchase_price_upper": 13.0,
                "repurchase_quantity": 10,
                "progress_status": "planned",
            },
        ]

        assert provider.save_stock_repurchase_batch(records) >= 2

        with sqlite3.connect(provider.db_path) as conn:
            rows = conn.execute(
                """SELECT trade_date, stock_code, repurchase_amount, progress_status
                   FROM stock_repurchase
                   ORDER BY repurchase_amount"""
            ).fetchall()

        assert rows == [
            ("2026-07-21", "000001", 100.0, "planned"),
            ("2026-07-21", "000001", 120.0, "planned"),
        ]

    def test_stock_repurchase_repeated_identical_batch_is_idempotent(self, provider):
        record = {
            "trade_date": "2026-07-21",
            "stock_code": "000001",
            "stock_name": "Ping An Bank",
            "repurchase_amount": 100.0,
            "repurchase_price": 12.0,
            "repurchase_price_lower": 11.0,
            "repurchase_price_upper": 13.0,
            "repurchase_quantity": 10,
            "progress_status": "planned",
        }

        assert provider.save_stock_repurchase_batch([record]) >= 1
        assert provider.save_stock_repurchase_batch([record]) >= 0

        with sqlite3.connect(provider.db_path) as conn:
            count = conn.execute("SELECT COUNT(*) FROM stock_repurchase").fetchone()[0]

        assert count == 1

    def test_institution_survey_keeps_distinct_nullable_org_records(self, provider):
        records = [
            {
                "trade_date": "2026-07-21",
                "stock_code": "000001",
                "stock_name": "Ping An Bank",
                "survey_org": None,
                "survey_type": "call",
                "survey_count": 3,
            },
            {
                "trade_date": "2026-07-21",
                "stock_code": "000001",
                "stock_name": "Ping An Bank",
                "survey_org": None,
                "survey_type": "call",
                "survey_count": 4,
            },
        ]

        assert provider.save_institution_survey_batch(records) >= 2

        with sqlite3.connect(provider.db_path) as conn:
            rows = conn.execute(
                """SELECT trade_date, stock_code, survey_org, survey_type, survey_count
                   FROM institution_survey
                   ORDER BY survey_count"""
            ).fetchall()

        assert rows == [
            ("2026-07-21", "000001", None, "call", 3),
            ("2026-07-21", "000001", None, "call", 4),
        ]


# ═══════════════════════════════════════════════════════════════════════════════
# 2. 空记录路径 — 所有 batch save 方法在 records=[] 时应返回 0
# ═══════════════════════════════════════════════════════════════════════════════


class TestEmptyRecords:
    """全部 batch save 方法的 records=[] 边界测试。"""

    _empty_batch_methods: list[str] = [
        "save_historical_valuation_batch",
        "save_macro_monthly_batch",
        "save_macro_quarterly_batch",
        "save_macro_daily_batch",
        "save_money_market_batch",
        "save_central_bank_balance_batch",
        "save_market_valuation_batch",
        "save_concept_board_batch",
        "save_concept_member_batch",
        "save_south_flow_batch",
        "save_ah_premium_batch",
        "save_cb_quotation_batch",
        "save_cb_redeem_batch",
        "save_cb_index_batch",
        "save_etf_daily_batch",
        "save_restricted_share_batch",
        "save_earnings_forecast_batch",
        "save_sector_daily_batch",
        "save_sector_valuation_batch",
        "save_index_futures_basis_batch",
        "save_chip_distribution_batch",
        "save_chip_distribution_em_batch",
        "save_historical_valuation_batch",
    ]

    @pytest.mark.parametrize("method_name", sorted(set(_empty_batch_methods)))
    def test_empty_records_returns_zero(self, provider, method_name: str):
        """空记录 → 返回 0。"""
        method = getattr(provider, method_name)
        result = method([])
        assert result == 0


# ═══════════════════════════════════════════════════════════════════════════════
# 3. 批量 save 方法参数化测试 — 每个方法用一条记录验证 SQL 可执行
# ═══════════════════════════════════════════════════════════════════════════════

# 每个条目: (method_name, records)
BatchCase = tuple[str, str, list[dict[str, Any]]]

_BATCH_CASES: list[BatchCase] = [
    (
        "save_concept_board_batch",
        "概念板块",
        [
            {
                "trade_date": "2024-01-02",
                "concept_code": "BK0001",
                "concept_name": "测试概念",
                "pct_change": 1.5,
                "turnover": 3.0,
                "up_count": 10,
                "down_count": 2,
                "data_source": "ths",
            }
        ],
    ),
    (
        "save_concept_member_batch",
        "概念成分股",
        [
            {
                "concept_code": "BK0001",
                "concept_name": "测试概念",
                "ts_code": "000001",
            }
        ],
    ),
    (
        "save_money_market_batch",
        "货币市场",
        [
            {
                "date": "2024-01-02",
                "shibor_on": 1.5,
                "shibor_1w": 1.8,
                "shibor_2w": 2.0,
                "shibor_1m": 2.2,
                "shibor_3m": 2.5,
                "shibor_6m": 2.7,
                "shibor_9m": 2.8,
                "shibor_1y": 3.0,
                "fr001": 1.6,
                "fr007": 2.1,
                "fr014": 2.4,
                "pboc_policy_rate": 3.5,
                "data_date": "2024-01-02",
            }
        ],
    ),
    (
        "save_central_bank_balance_batch",
        "央行资产负债表",
        [
            {
                "date": "2024-01-01",
                "total_assets": 400000,
                "reserve_money": 330000,
                "currency_issue": 110000,
                "claims_on_other_deposit": 120000,
                "claims_on_gov": 15000,
                "gov_deposits": 45000,
                "foreign_assets": 210000,
                "fx_reserve": 31000,
                "data_date": "2024-01-15",
            }
        ],
    ),
    (
        "save_market_valuation_batch",
        "大盘估值",
        [
            {
                "date": "2024-01-02",
                "pe_median": 12.5,
                "pe_quantile": 0.35,
                "pe_lyr_median": 11.0,
                "pb_median": 1.5,
                "pb_quantile": 0.25,
                "equity_bond_spread": 3.2,
                "ebs_ma": 3.0,
                "csi300_close": 3500,
                "data_source": "legu",
                "data_date": "2024-01-02",
            }
        ],
    ),
    (
        "save_south_flow_batch",
        "南向资金",
        [
            {
                "trade_date": "2024-01-02",
                "market": "港股通",
                "net_buy_amount": 1.0e9,
                "buy_amount": 2.0e9,
                "sell_amount": 1.0e9,
                "cumulative_net_buy": 100.0e9,
                "data_source": "akshare",
            }
        ],
    ),
    (
        "save_ah_premium_batch",
        "AH 溢价",
        [
            {
                "trade_date": "2024-01-02",
                "ts_code": "000001",
                "h_code": "00300.HK",
                "name": "平安银行",
                "a_price": 10.5,
                "h_price": 8.0,
                "premium": 31.25,
                "data_source": "akshare",
            }
        ],
    ),
    (
        "save_cb_quotation_batch",
        "可转债行情",
        [
            {
                "ts_code": "113050",
                "bond_name": "测试转债",
                "price": 120.0,
                "premium": 5.0,
                "double_low": 125.0,
                "expire_date": "2028-01-01",
                "data_source": "akshare",
            }
        ],
    ),
    (
        "save_cb_redeem_batch",
        "可转债强赎",
        [
            {
                "ts_code": "113050",
                "bond_name": "测试转债",
                "redeem_flag": "Y",
                "redeem_price": 100.0,
                "redeem_date": "2026-08-01",
                "data_source": "akshare",
            }
        ],
    ),
    (
        "save_cb_index_batch",
        "可转债指数",
        [
            {
                "trade_date": "2024-01-02",
                "index_code": "000832",
                "index_name": "中证转债",
                "open": 400.0,
                "close": 401.0,
                "high": 402.0,
                "low": 399.0,
                "volume": 1e6,
                "data_source": "akshare",
            }
        ],
    ),
    (
        "save_etf_daily_batch",
        "ETF 日线",
        [
            {
                "ts_code": "510050",
                "name": "50ETF",
                "trade_date": "2024-01-02",
                "open": 2.6,
                "high": 2.7,
                "low": 2.5,
                "close": 2.65,
                "volume": 1e8,
                "amount": 2.6e8,
                "data_source": "akshare",
            }
        ],
    ),
    (
        "save_restricted_share_batch",
        "限售解禁",
        [
            {
                "ts_code": "000001",
                "name": "平安银行",
                "release_date": "2024-01-02",
                "actual_release": 100.5,
                "total_shares": 200.5,
                "market_type": "深市",
                "data_source": "akshare",
            }
        ],
    ),
    (
        "save_earnings_forecast_batch",
        "业绩预告",
        [
            {
                "ts_code": "000001",
                "name": "平安银行",
                "end_date": "2024-06-30",
                "forecast_type": "预增",
                "net_profit_change": 50.0,
                "previous_profit": 1.0e9,
                "data_source": "akshare",
            }
        ],
    ),
    (
        "save_sector_daily_batch",
        "行业板块日线",
        [
            {
                "sector_name": "银行",
                "trade_date": "2024-01-02",
                "open": 100.0,
                "close": 101.0,
                "high": 102.0,
                "low": 99.0,
                "volume": 1e6,
                "amount": 1e8,
                "pct_change": 1.0,
                "data_source": "akshare",
            }
        ],
    ),
    (
        "save_sector_valuation_batch",
        "行业板块估值",
        [
            {
                "sector_name": "银行",
                "trade_date": "2024-01-02",
                "pe": 6.5,
                "pb": 0.7,
                "total_mv": 123456.0,
                "data_source": "akshare",
            }
        ],
    ),
    (
        "save_index_futures_basis_batch",
        "基差数据",
        [
            {
                "trade_date": "2024-01-02",
                "futures_code": "IF0",
                "futures_price": 3500.0,
                "index_price": 3490.0,
                "basis": 10.0,
                "basis_pct": 0.2865,
                "data_source": "akshare",
            }
        ],
    ),
    (
        "save_chip_distribution_batch",
        "筹码分布",
        [
            {
                "ts_code": "000001",
                "trade_date": "2024-01-02",
                "profit_ratio": 0.5,
                "avg_cost": 10.0,
                "cost_90_low": 9.0,
                "cost_90_high": 11.0,
                "concentration_90": 0.3,
                "cost_70_low": 9.5,
                "cost_70_high": 10.5,
                "concentration_70": 0.2,
                "chip_concentration": 0.9,
            }
        ],
    ),
    (
        "save_chip_distribution_em_batch",
        "EM 筹码分布",
        [
            {
                "ts_code": "000001",
                "trade_date": "2024-01-02",
                "profit_ratio": 0.5,
                "avg_cost": 10.0,
                "cost_90_low": 9.0,
                "cost_90_high": 11.0,
                "concentration_90": 0.3,
                "cost_70_low": 9.5,
                "cost_70_high": 10.5,
                "concentration_70": 0.2,
            }
        ],
    ),
]


class TestBatchSaveHappyPath:
    """全部 batch save 方法的 happy path 参数化测试。"""

    @pytest.mark.parametrize("method_name,label,records", _BATCH_CASES, ids=[c[1] for c in _BATCH_CASES])
    def test_happy_path(self, provider, method_name: str, label: str, records: list[dict]):
        method = getattr(provider, method_name)
        result = method(records)
        assert result >= 1, f"{label}: 应返回 >= 1, 实际 {result}"
        # 重复写入（INSERT OR REPLACE）应仍正常工作
        result2 = method(records)
        assert result2 >= 0, f"{label}(重复): 应返回 >= 0, 实际 {result2}"


class TestHistoricalValuation:
    """save_historical_valuation_batch —— 需要手动创建表。"""

    def _ensure_table(self, provider):
        conn = sqlite3.connect(provider.db_path)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS historical_valuation ("
            "ts_code TEXT, trade_date TEXT, pe_ttm REAL, pb REAL, "
            "ps_ttm REAL, dividend_yield REAL"
            ")"
        )
        conn.commit()
        conn.close()

    def test_happy_path(self, provider):
        self._ensure_table(provider)
        result = provider.save_historical_valuation_batch(
            [
                {
                    "ts_code": "000001",
                    "trade_date": "2024-06-30",
                    "pe_ttm": 10.0,
                    "pb": 1.5,
                    "ps_ttm": 2.0,
                    "dividend_yield": 0.03,
                }
            ]
        )
        assert result >= 1

    def test_missing_required_keys_skipped(self, provider):
        """缺少 ts_code 的记录被跳过 → 返回 0。"""
        self._ensure_table(provider)
        result = provider.save_historical_valuation_batch(
            [{"pe_ttm": 10.0}]  # 缺少 ts_code 和 trade_date
        )
        assert result == 0

    def test_mixed_valid_and_invalid(self, provider):
        """混合有效/无效记录 → 只保存有效记录。"""
        self._ensure_table(provider)
        result = provider.save_historical_valuation_batch(
            [
                {"ts_code": "000001", "trade_date": "2024-06-30", "pe_ttm": 10.0},
                {"pe_ttm": 15.0},  # 无效：缺少 ts_code
                {"ts_code": "000002", "trade_date": "2024-06-30", "pb": 2.0},
            ]
        )
        assert result >= 2  # 有效记录2条


class TestMacroBatchSaves:
    """宏观数据批量保存独立测试。"""

    def test_macro_monthly_happy(self, provider):
        r = provider.save_macro_monthly_batch([
            {
                "date": "2024-01-01",
                "cpi_yoy": 0.2,
                "cpi_mom": 0.1,
                "cpi_core_yoy": 0.8,
                "ppi_yoy": -2.5,
                "ppi_mom": -0.4,
                "pmi": 50.1,
                "pmi_yoy": 1.2,
                "pmi_monthly_change": 0.3,
                "pmi_mom": 0.5,
                "pmi_caixin": 50.8,
                "m0": 10.0,
                "m1": 60.0,
                "m2": 280.0,
                "m0_yoy": 5.0,
                "m1_yoy": 1.5,
                "m2_yoy": 8.0,
                "new_loans": 3.0,
                "new_loans_yoy": 10.0,
                "retail_sales_yoy": 4.5,
                "retail_sales_ytd_yoy": 4.0,
                "fixed_asset_investment_yoy": 3.2,
                "fixed_asset_investment_ytd_yoy": 3.0,
                "export_value": 3000,
                "export_yoy": 5.0,
                "import_value": 2500,
                "import_yoy": 3.0,
                "industrial_production_yoy": 5.5,
                "industrial_production_ytd_yoy": 5.0,
                "electricity_consumption_yoy": 6.0,
                "electricity_consumption_total": 8000,
                "enterprise_goods_price_yoy": -2.0,
                "enterprise_goods_price_mom": -0.3,
                "consumer_confidence": 108.5,
                "consumer_satisfaction": 107.2,
                "consumer_expectation": 109.8,
                "lpr_1y": 3.45,
                "lpr_5y": 3.95,
                "data_date": "2024-01-15",
            }
        ])
        assert r >= 1

    def test_macro_quarterly_all_fields(self, provider):
        r = provider.save_macro_quarterly_batch([
            {
                "date": "2024-Q1",
                "gdp": 300000,
                "gdp_yoy": 5.3,
                "gdp_qoq": 1.5,
                "gdp_primary": 20000,
                "gdp_secondary": 120000,
                "gdp_tertiary": 160000,
                "data_date": "2024-04-15",
            }
        ])
        assert r >= 1

    def test_macro_daily_all_fields(self, provider):
        r = provider.save_macro_daily_batch([
            {
                "date": "2024-01-02",
                "shibor_on": 1.6,
                "shibor_1w": 2.0,
                "shibor_2w": 2.3,
                "shibor_1m": 2.5,
                "shibor_3m": 2.7,
                "shibor_6m": 2.9,
                "shibor_9m": 3.0,
                "shibor_1y": 3.1,
                "data_date": "2024-01-02",
            }
        ])
        assert r >= 1


# ═══════════════════════════════════════════════════════════
# 4. except Exception 路径 — 表删除后调用 batch save 触发异常
# ═══════════════════════════════════════════════════════════


class TestBatchSaveExceptionPaths:
    """模拟 SQL 错误触发各 save_*_batch 的 except Exception 分支。"""

    def _drop_table(self, provider, table: str):
        conn = sqlite3.connect(provider.db_path)
        conn.execute(f"DROP TABLE IF EXISTS {table}")
        conn.commit()
        conn.close()

    def test_concept_board_exception(self, provider):
        self._drop_table(provider, "concept_board")
        result = provider.save_concept_board_batch([
            {"trade_date": "2024-01-02", "concept_code": "BK0001",
             "concept_name": "T", "pct_change": 1.0, "turnover": 2.0,
             "up_count": 5, "down_count": 1}
        ])
        assert result == 0

    def test_concept_member_exception(self, provider):
        self._drop_table(provider, "concept_member")
        result = provider.save_concept_member_batch([
            {"concept_code": "BK0001", "concept_name": "T", "ts_code": "000001"}
        ])
        assert result == 0

    def test_money_market_exception(self, provider):
        self._drop_table(provider, "money_market")
        result = provider.save_money_market_batch([
            {"date": "2024-01-02", "shibor_on": 1.5, "data_date": "2024-01-02"}
        ])
        assert result == 0

    def test_central_bank_exception(self, provider):
        self._drop_table(provider, "central_bank_balance")
        result = provider.save_central_bank_balance_batch([
            {"date": "2024-01-01", "total_assets": 1000, "data_date": "2024-01-15"}
        ])
        assert result == 0

    def test_south_flow_exception(self, provider):
        self._drop_table(provider, "south_flow")
        result = provider.save_south_flow_batch([
            {"trade_date": "2024-01-02", "market": "港股通"}
        ])
        assert result == 0

    def test_ah_premium_exception(self, provider):
        self._drop_table(provider, "ah_premium")
        result = provider.save_ah_premium_batch([
            {"trade_date": "2024-01-02", "ts_code": "000001", "name": "T"}
        ])
        assert result == 0

    def test_cb_redeem_exception(self, provider):
        self._drop_table(provider, "cb_redeem")
        result = provider.save_cb_redeem_batch([
            {"ts_code": "113050", "bond_name": "T"}
        ])
        assert result == 0

    def test_cb_index_exception(self, provider):
        self._drop_table(provider, "cb_index")
        result = provider.save_cb_index_batch([
            {"trade_date": "2024-01-02", "index_code": "000832", "index_name": "T"}
        ])
        assert result == 0

    def test_etf_daily_exception(self, provider):
        self._drop_table(provider, "etf_daily")
        result = provider.save_etf_daily_batch([
            {"ts_code": "510050", "name": "T", "trade_date": "2024-01-02"}
        ])
        assert result == 0

    def test_restricted_share_exception(self, provider):
        self._drop_table(provider, "restricted_share")
        result = provider.save_restricted_share_batch([
            {"ts_code": "000001", "name": "T", "release_date": "2024-01-02"}
        ])
        assert result == 0

    def test_earnings_forecast_exception(self, provider):
        self._drop_table(provider, "earnings_forecast")
        result = provider.save_earnings_forecast_batch([
            {"ts_code": "000001", "name": "T", "end_date": "2024-06-30"}
        ])
        assert result == 0

    def test_sector_valuation_exception(self, provider):
        self._drop_table(provider, "sector_valuation")
        result = provider.save_sector_valuation_batch([
            {"sector_name": "银行", "trade_date": "2024-01-02"}
        ])
        assert result == 0

    def test_index_futures_basis_exception(self, provider):
        self._drop_table(provider, "index_futures_basis")
        result = provider.save_index_futures_basis_batch([
            {"trade_date": "2024-01-02", "futures_code": "IF0"}
        ])
        assert result == 0

    def test_option_sentiment_exception(self, provider):
        self._drop_table(provider, "option_sentiment")
        result = provider.save_option_sentiment_batch([
            {"trade_date": "2024-01-02"}
        ])
        assert result == 0

    def test_stock_repurchase_exception(self, provider):
        self._drop_table(provider, "stock_repurchase")
        result = provider.save_stock_repurchase_batch([
            {"trade_date": "2024-01-02", "stock_code": "000001"}
        ])
        assert result == 0

    def test_insider_trading_exception(self, provider):
        self._drop_table(provider, "insider_trading")
        result = provider.save_insider_trading_batch([
            {"trade_date": "2024-01-02", "stock_code": "000001"}
        ])
        assert result == 0

    def test_institution_survey_exception(self, provider):
        self._drop_table(provider, "institution_survey")
        result = provider.save_institution_survey_batch([
            {"trade_date": "2024-01-02", "stock_code": "000001"}
        ])
        assert result == 0

    def test_stock_pledge_exception(self, provider):
        self._drop_table(provider, "stock_pledge")
        result = provider.save_stock_pledge_batch([
            {"trade_date": "2024-01-02", "stock_code": "000001"}
        ])
        assert result == 0

    def test_chip_distribution_exception(self, provider):
        self._drop_table(provider, "chip_distribution")
        result = provider.save_chip_distribution_batch([
            {"ts_code": "000001", "trade_date": "2024-01-02"}
        ])
        assert result == 0

    def test_chip_distribution_em_exception(self, provider):
        self._drop_table(provider, "chip_distribution_em")
        result = provider.save_chip_distribution_em_batch([
            {"ts_code": "000001", "trade_date": "2024-01-02"}
        ])
        assert result == 0

    def test_macro_quarterly_exception(self, provider):
        self._drop_table(provider, "macro_quarterly")
        result = provider.save_macro_quarterly_batch([
            {"date": "2024-Q1", "gdp": 100000}
        ])
        assert result == 0

    def test_macro_daily_exception(self, provider):
        self._drop_table(provider, "macro_daily")
        result = provider.save_macro_daily_batch([
            {"date": "2024-01-02", "shibor_on": 1.5}
        ])
        assert result == 0


# ═══════════════════════════════════════════════════════════
# 5. DataLoader / IndicatorProvider
# ═══════════════════════════════════════════════════════════


class TestLoaderAndIndicator:
    """SmartMoneyLoaderProvider + SmartMoneyIndicatorProvider 方法调用。"""

    def test_loader_init_and_get_bars(self):
        from providers import SmartMoneyLoaderProvider
        p = SmartMoneyLoaderProvider()
        result = p.get_daily_bars("000001")
        assert result is not None

    def test_loader_no_cache(self):
        from providers import SmartMoneyLoaderProvider
        p = SmartMoneyLoaderProvider(use_cache=False)
        result = p.get_daily_bars("000001", start_date="2024-01-01", end_date="2024-01-31")
        assert result is not None

    def test_loader_incremental_update(self):
        from providers import SmartMoneyLoaderProvider
        p = SmartMoneyLoaderProvider()
        df = __import__("pandas").DataFrame()
        result = p.incremental_update("000001", df)
        assert result is not None

    def test_loader_market_valuation(self):
        from providers import SmartMoneyLoaderProvider
        p = SmartMoneyLoaderProvider()
        result = p.get_market_valuation()
        assert result is not None

    def test_loader_market_fund_flow(self):
        from providers import SmartMoneyLoaderProvider
        p = SmartMoneyLoaderProvider()
        result = p.get_market_fund_flow()
        assert result is not None

    def test_indicator_init_and_calculate(self):
        from providers import SmartMoneyIndicatorProvider
        p = SmartMoneyIndicatorProvider()
        result = p.calculate_all_indicators(__import__("pandas").DataFrame({"close": [10.0, 11.0]}))
        assert result is not None
