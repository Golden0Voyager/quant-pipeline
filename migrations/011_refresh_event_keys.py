"""Migration 011: deterministic source keys for refresh event tables.

dragon_tiger / block_trade 的 UNIQUE(ts_code, trade_date) 会覆盖同股同日
合法多事件；stock_pledge 的 UNIQUE(trade_date, stock_code, pledger) 在
pledger 为 NULL 时互不相等导致重复累积。本迁移为三表重建带
``source_record_key`` 的 schema，回填键，隔离（而非删除）不可调和的
重复行，并建立唯一索引。
"""

from __future__ import annotations

import logging

from core import source_record_key as source_key_module

logger = logging.getLogger(__name__)


_TABLE_SPECS = (
    {
        "table": "dragon_tiger",
        "columns": (
            "ts_code", "trade_date", "close_price", "pct_change",
            "net_buy_amount", "buy_amount", "sell_amount", "turnover_rate",
            "market_cap", "reason", "data_source", "updated_at",
        ),
        "required": ("ts_code", "trade_date"),
        "key_builder": "dragon_tiger_source_key",
        "create": """
            CREATE TABLE {name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_code TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                close_price REAL,
                pct_change REAL,
                net_buy_amount REAL,
                buy_amount REAL,
                sell_amount REAL,
                turnover_rate REAL,
                market_cap REAL,
                reason TEXT,
                data_source TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                source_record_key TEXT NOT NULL
            )
        """,
        "indexes": (
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_dragon_tiger_source_key "
            "ON dragon_tiger(source_record_key)",
            "CREATE INDEX IF NOT EXISTS idx_dragon_tiger_code_date "
            "ON dragon_tiger(ts_code, trade_date DESC)",
        ),
    },
    {
        "table": "block_trade",
        "columns": (
            "ts_code", "trade_date", "deal_price", "close_price",
            "discount_rate", "volume", "amount", "buyer_branch",
            "seller_branch", "data_source", "updated_at",
        ),
        "required": ("ts_code", "trade_date"),
        "key_builder": "block_trade_source_key",
        "create": """
            CREATE TABLE {name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_code TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                deal_price REAL,
                close_price REAL,
                discount_rate REAL,
                volume REAL,
                amount REAL,
                buyer_branch TEXT,
                seller_branch TEXT,
                data_source TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                source_record_key TEXT NOT NULL
            )
        """,
        "indexes": (
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_block_trade_source_key "
            "ON block_trade(source_record_key)",
            "CREATE INDEX IF NOT EXISTS idx_block_trade_code_date "
            "ON block_trade(ts_code, trade_date DESC)",
        ),
    },
    {
        "table": "stock_pledge",
        "columns": (
            "trade_date", "stock_code", "stock_name", "pledger",
            "pledge_amount", "pledge_ratio", "pledge_org",
        ),
        "required": ("trade_date", "stock_code"),
        "key_builder": "stock_pledge_source_key",
        "create": """
            CREATE TABLE {name} (
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
        """,
        "indexes": (
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_stock_pledge_source_key "
            "ON stock_pledge(source_record_key)",
            "CREATE INDEX IF NOT EXISTS idx_stock_pledge_code_date "
            "ON stock_pledge(stock_code, trade_date DESC)",
        ),
    },
)


def apply(conn):
    for spec in _TABLE_SPECS:
        _rebuild_event_table(conn, spec)
    logger.info("  ✅ 011: refresh event keys reconciled")


def _rebuild_event_table(conn, spec):
    table = spec["table"]
    columns = _columns(conn, table)
    if not columns:
        # 表尚不存在（历史上由外部包创建）→ 直接建新 schema
        conn.execute(spec["create"].format(name=table))
        _ensure_indexes(conn, spec)
        logger.info("  ➕ %s created with source_record_key", table)
        return
    if "source_record_key" in columns:
        # 已迁移（或由新 _ensure_tables 建表）→ 仅确保索引，保持幂等
        _ensure_indexes(conn, spec)
        return

    rows = _read_rows(conn, table, spec["columns"])
    key_builder = getattr(source_key_module, spec["key_builder"])
    survivors: dict[str, dict] = {}
    quarantined: list[tuple[dict, str | None, str]] = []
    for row in rows:  # ORDER BY id：后写入者胜出（对齐 INSERT OR REPLACE 语义）
        if any(_is_blank(row.get(column)) for column in spec["required"]):
            quarantined.append((row, None, "missing_required_fields"))
            continue
        key = key_builder(row)
        previous = survivors.get(key)
        if previous is not None:
            quarantined.append((previous, key, "duplicate_source_record_key"))
        survivors[key] = row

    if quarantined:
        _quarantine_rows(conn, spec, quarantined)

    temp_table = f"{table}__v11"
    conn.execute(f"DROP TABLE IF EXISTS {temp_table}")
    conn.execute(spec["create"].format(name=temp_table))
    column_list = ", ".join(spec["columns"])
    placeholders = ", ".join("?" for _ in range(len(spec["columns"]) + 2))
    conn.executemany(
        f"""INSERT INTO {temp_table} (id, {column_list}, source_record_key)
            VALUES ({placeholders})""",
        [
            (row["id"], *[row.get(column) for column in spec["columns"]], key)
            for key, row in survivors.items()
        ],
    )
    conn.execute(f"DROP TABLE {table}")
    conn.execute(f"ALTER TABLE {temp_table} RENAME TO {table}")
    _ensure_indexes(conn, spec)
    logger.info(
        "  ✅ %s rebuilt: %d kept, %d quarantined",
        table, len(survivors), len(quarantined),
    )


def _quarantine_rows(conn, spec, quarantined):
    table = spec["table"]
    quarantine_table = f"{table}_quarantine"
    column_defs = ",\n                ".join(spec["columns"])
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {quarantine_table} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            original_id INTEGER,
            {column_defs},
            source_record_key TEXT,
            quarantine_reason TEXT NOT NULL,
            quarantined_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    column_list = ", ".join(spec["columns"])
    placeholders = ", ".join("?" for _ in range(len(spec["columns"]) + 3))
    conn.executemany(
        f"""INSERT INTO {quarantine_table}
            (original_id, {column_list}, source_record_key, quarantine_reason)
            VALUES ({placeholders})""",
        [
            (
                row.get("id"),
                *[row.get(column) for column in spec["columns"]],
                key,
                reason,
            )
            for row, key, reason in quarantined
        ],
    )


def _ensure_indexes(conn, spec):
    for statement in spec["indexes"]:
        conn.execute(statement)


def _columns(conn, table):
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}


def _read_rows(conn, table, target_columns):
    existing_columns = _columns(conn, table)
    selected = ["id"] + [
        column for column in target_columns if column in existing_columns
    ]
    rows = conn.execute(
        f"SELECT {', '.join(selected)} FROM {table} ORDER BY id"
    ).fetchall()
    return [dict(zip(selected, row, strict=True)) for row in rows]


def _is_blank(value) -> bool:
    if value is None:
        return True
    return isinstance(value, str) and not value.strip()
