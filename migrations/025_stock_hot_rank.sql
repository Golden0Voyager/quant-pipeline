-- Migration 025: stock_hot_rank — Eastmoney top-100 popularity-rank snapshot
--
-- Sourced from emappdata.eastmoney.com/stockrank/getAllCurrentList
-- (same endpoint akshare's stock_hot_rank_em() wraps). One row per
-- (trade_date, code); the source has no date of its own, so the snapshot
-- is stamped with the Shanghai run date (RUN_SNAPSHOT semantics, same as
-- concept_board). The endpoint is intermittently WAF-blocked — task
-- failures retain the previous snapshot instead of wiping the table.

CREATE TABLE IF NOT EXISTS stock_hot_rank (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date DATE NOT NULL,
    code TEXT NOT NULL,
    name TEXT,
    rank INTEGER NOT NULL,
    rank_change REAL,
    prev_rank REAL,
    close_price REAL,
    change_pct REAL,
    data_source TEXT DEFAULT 'eastmoney',
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(trade_date, code)
);

CREATE INDEX IF NOT EXISTS idx_stock_hot_rank_code_date
    ON stock_hot_rank(code, trade_date DESC);

CREATE INDEX IF NOT EXISTS idx_stock_hot_rank_trade_date
    ON stock_hot_rank(trade_date DESC);
