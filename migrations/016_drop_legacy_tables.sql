-- Migration 016: drop four legacy dead tables.
--
-- Clean-up rationale (verified against production, quant_hunter has no readers):
--   * macro_daily      — v3.1 SHIBOR table, 2327 rows all NULL, no writer since
--                        money_market took over SHIBOR + repo rates.
--   * north_flow       — empty; HKEX stopped publishing daily northbound flow
--                        in 2024-08 and the task was decommissioned.
--   * crude_oil        — old realtime snapshot (89 rows); superseded by
--                        global_assets_bars (CL=F / BZ=F). update_crude_oil is
--                        decommissioned in the same change.
--   * insider_trading  — empty; akshare removed stock_cgxq_em, the orphan task
--                        file is deleted and never wired into the pipeline.
--
-- The inline CREATE TABLE IF NOT EXISTS blocks in providers.py are removed in
-- the same commit series so ensure_tables cannot resurrect these tables after
-- the DROP (ensure_tables runs before migrations).

DROP TABLE IF EXISTS macro_daily;
DROP TABLE IF EXISTS north_flow;
DROP TABLE IF EXISTS crude_oil;
DROP TABLE IF EXISTS insider_trading;
