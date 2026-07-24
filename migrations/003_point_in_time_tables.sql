-- ═══════════════════════════════════════════════════════════════════════
-- Migration 003: Point-in-time tracking tables
-- ═══════════════════════════════════════════════════════════════════════
-- These tables support temporal queries by preserving the exact
-- composition of financial data, concept memberships and index
-- constituents at arbitrary past dates.

-- ── Financial history (point-in-time) ─────────────────────────────
-- Captures quarterly financial metrics as they were known on each
-- announcement date, enabling backtesting without look-ahead bias.

CREATE TABLE IF NOT EXISTS financial_history_pt (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_code         TEXT NOT NULL,
    announcement_date TEXT NOT NULL,   -- when the data was published
    end_date        TEXT NOT NULL,      -- fiscal quarter end
    roe             REAL,
    roa             REAL,
    gross_margin    REAL,
    net_margin      REAL,
    eps             REAL,
    bvps            REAL,
    revenue         REAL,
    net_profit      REAL,
    free_cash_flow  REAL,
    data_source     TEXT,
    updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(ts_code, announcement_date, end_date)
);

CREATE INDEX IF NOT EXISTS idx_financial_history_pt_code
    ON financial_history_pt(ts_code, end_date DESC);

-- ── Concept member history (point-in-time) ────────────────────────
-- Records which stocks belonged to which concept board at each point
-- in time, so backtests see the membership that was active then.

CREATE TABLE IF NOT EXISTS concept_member_pt (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    concept_code    TEXT NOT NULL,
    concept_name    TEXT,
    ts_code         TEXT NOT NULL,
    effective_date  TEXT NOT NULL,      -- when this mapping took effect
    data_source     TEXT,
    updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(concept_code, ts_code, effective_date)
);

CREATE INDEX IF NOT EXISTS idx_concept_member_pt_code
    ON concept_member_pt(ts_code, effective_date DESC);

-- ── Index member history (point-in-time) ──────────────────────────
-- Tracks index constituent changes over time for benchmark-aware
-- analysis.

CREATE TABLE IF NOT EXISTS index_member_pt (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    index_code      TEXT NOT NULL,
    ts_code         TEXT NOT NULL,
    in_date         TEXT NOT NULL,       -- when the stock was added
    out_date        TEXT,                -- when the stock was removed
    weight          REAL,
    data_source     TEXT,
    updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(index_code, ts_code, in_date)
);

CREATE INDEX IF NOT EXISTS idx_index_member_pt_code
    ON index_member_pt(index_code, ts_code);
