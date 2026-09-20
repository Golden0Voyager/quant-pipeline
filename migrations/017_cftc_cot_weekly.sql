-- Migration 017: CFTC Commitment of Traders weekly positioning.
--
-- Speculator (non-commercial) positioning for commodity futures
-- (crude, gold, silver, natgas, grains...) and FX futures (USD, EUR,
-- JPY...), sourced from akshare's CFTC wrappers (weekly, released
-- Fridays with a few days lag). Stored long-format: one row per
-- (week, market, instrument); idempotent upsert.

CREATE TABLE IF NOT EXISTS cftc_cot_weekly (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date DATE NOT NULL,
    market TEXT NOT NULL,
    instrument TEXT NOT NULL,
    long_positions REAL,
    short_positions REAL,
    net_positions REAL,
    data_source TEXT,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(trade_date, market, instrument)
);

CREATE INDEX IF NOT EXISTS idx_cftc_cot_date
    ON cftc_cot_weekly(trade_date DESC);
