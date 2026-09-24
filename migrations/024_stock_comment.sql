-- Migration 024: stock_comment — Eastmoney 千股千评 full-market snapshot
--
-- Sourced from datacenter-web.eastmoney.com report RPT_DMSK_TS_STOCKNEW
-- (same endpoint akshare's stock_comment_em() wraps). One row per
-- (trade_date, code) snapshot; the source returns its own TRADE_DATE.

CREATE TABLE IF NOT EXISTS stock_comment (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date DATE NOT NULL,
    code TEXT NOT NULL,
    name TEXT,
    close_price REAL,
    change_pct REAL,
    turnover REAL,
    pe_dynamic REAL,
    prime_cost REAL,
    org_participation REAL,
    composite_score REAL,
    rank_up REAL,
    rank INTEGER,
    focus_index REAL,
    data_source TEXT DEFAULT 'eastmoney',
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(trade_date, code)
);

CREATE INDEX IF NOT EXISTS idx_stock_comment_code_date
    ON stock_comment(code, trade_date DESC);

CREATE INDEX IF NOT EXISTS idx_stock_comment_trade_date
    ON stock_comment(trade_date DESC);
