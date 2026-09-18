-- Migration 012: US daily macro rates from FRED.
--
-- Fed-policy and oil-shock context (2026-09 hike cycle): effective federal
-- funds rate, 3M/10Y Treasury yields, 10Y breakeven inflation, plus derived
-- 10Y-3M spread and 10Y real rate. FRED publishes rates T+1, so the pipeline
-- backfills a trailing window (INSERT OR REPLACE idempotent, UNIQUE(trade_date)).

CREATE TABLE IF NOT EXISTS us_macro_daily (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date DATE NOT NULL,
    effr REAL,
    dgs3mo REAL,
    dgs10 REAL,
    t10yie REAL,
    spread_10y_3m REAL,
    real_rate_10y REAL,
    data_source TEXT,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(trade_date)
);

CREATE INDEX IF NOT EXISTS idx_us_macro_daily_date
    ON us_macro_daily(trade_date DESC);
