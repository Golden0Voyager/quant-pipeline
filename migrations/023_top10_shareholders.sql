-- Migration 023: top10_shareholders — quarterly top-10 shareholders per stock
--
-- Sourced from Sina Finance stock_main_stock_holder().
-- One row per (ts_code, report_date, holder_rank) snapshot.

CREATE TABLE IF NOT EXISTS top10_shareholders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_code TEXT NOT NULL,
    report_date DATE NOT NULL,
    holder_rank INTEGER NOT NULL,
    holder_name TEXT NOT NULL,
    shares_held REAL,
    share_ratio REAL,
    share_nature TEXT,
    announcement_date DATE,
    data_source TEXT DEFAULT 'sina',
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(ts_code, report_date, holder_rank)
);

CREATE INDEX IF NOT EXISTS idx_top10_code_date
    ON top10_shareholders(ts_code, report_date DESC);

CREATE INDEX IF NOT EXISTS idx_top10_report_date
    ON top10_shareholders(report_date DESC);
