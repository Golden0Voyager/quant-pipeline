-- Migration 014: extend us_macro_daily with 2Y yield and jobless claims.
--
-- DGS2 anchors the policy-rate expectation path, and ICSA (initial
-- claims, weekly) is the earliest labour-market loosening signal in the
-- Fed reaction function. ICSA is weekly, so the daily table holds it
-- sparsely (filled only on release days).
--
-- SQLite refuses ADD COLUMN with a non-constant default, so the
-- columns are plain nullable REAL — the writer already uses
-- INSERT OR REPLACE with explicit column lists.

ALTER TABLE us_macro_daily ADD COLUMN dgs2 REAL;
ALTER TABLE us_macro_daily ADD COLUMN icsa REAL;
