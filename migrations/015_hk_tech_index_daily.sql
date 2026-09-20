-- Migration 015: Hang Seng Tech Index daily bars from Sina HK feed.
--
-- Yahoo delisted ^HSTECH (404 confirmed 2026-09-20), and neither the
-- Eastmoney nor Sina A-share global-index feed reliably covers it, so
-- the pipeline pulls it from ak.stock_hk_index_daily_sina instead.
-- One row per trading day; INSERT OR REPLACE idempotent upsert.

CREATE TABLE IF NOT EXISTS hk_tech_index_daily (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date DATE NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    change_pct REAL,
    volume REAL,
    amount REAL,
    data_source TEXT,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(trade_date)
);

CREATE INDEX IF NOT EXISTS idx_hk_tech_date
    ON hk_tech_index_daily(trade_date DESC);
