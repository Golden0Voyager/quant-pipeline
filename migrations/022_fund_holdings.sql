-- Migration 021: fund_holdings — quarterly institutional stock holdings
--
-- Sourced from 东财主力数据中心 (stock_report_fund_hold).
-- Six institution types: 基金 / QFII / 社保 / 券商 / 保险 / 信托.
-- One row per (ts_code, report_date, institution_type) snapshot.

CREATE TABLE IF NOT EXISTS fund_holdings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_code TEXT NOT NULL,
    stock_name TEXT,
    report_date DATE NOT NULL,
    institution_type TEXT NOT NULL,  -- 'fund'|'qfii'|'social'|'broker'|'insurance'|'trust'
    fund_count INTEGER,             -- 持有该股的机构数量
    total_shares REAL,              -- 持股总数（股）
    hold_value REAL,                -- 持股市值（元）
    hold_ratio REAL,                -- 占流通股比例 (%)
    update_kind TEXT,               -- 增仓|减仓|持平
    update_shares REAL,             -- 持股变动数量（股）
    update_ratio REAL,              -- 持股变动比例 (%)
    data_source TEXT DEFAULT 'eastmoney',
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(ts_code, report_date, institution_type)
);

CREATE INDEX IF NOT EXISTS idx_fund_holdings_code_date
    ON fund_holdings(ts_code, report_date DESC);

CREATE INDEX IF NOT EXISTS idx_fund_holdings_report_institution
    ON fund_holdings(report_date, institution_type);
