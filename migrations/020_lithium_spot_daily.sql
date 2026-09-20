-- Migration 020: lithium carbonate spot price & futures basis (daily).
--
-- Sourced from 生意社 via ak.futures_spot_price_daily (var LC): spot
-- price, near/dominant contract prices and basis. The basis (现货-期货)
-- turns ahead of outright price at supply/demand inflections, which is
-- the actionable signal for lithium-miner equity holdings.

CREATE TABLE IF NOT EXISTS lithium_spot_daily (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    spot_date DATE NOT NULL,
    spot_price REAL,
    near_contract TEXT,
    near_contract_price REAL,
    dom_contract TEXT,
    dom_contract_price REAL,
    dom_basis REAL,
    dom_basis_rate REAL,
    data_source TEXT,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(spot_date)
);

CREATE INDEX IF NOT EXISTS idx_lithium_spot_date
    ON lithium_spot_daily(spot_date DESC);
