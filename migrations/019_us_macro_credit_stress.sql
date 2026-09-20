-- Migration 019: extend us_macro_daily with credit & financial-stress series.
--
-- HY/IG corporate OAS (credit-stress gauges in the hike cycle), the
-- St. Louis Fed Financial Stress Index (weekly), and the 5Y breakeven
-- inflation rate. Same FRED single-series pipeline as the existing
-- columns; STLFSI4 is weekly with Monday observation dates that land
-- on business-day rows naturally (no ICSA-style remapping needed).
--
-- SQLite refuses ADD COLUMN with a non-constant default, so the
-- columns are plain nullable REAL — the writer already uses
-- INSERT OR REPLACE with explicit column lists.

ALTER TABLE us_macro_daily ADD COLUMN hy_oas REAL;
ALTER TABLE us_macro_daily ADD COLUMN ig_oas REAL;
ALTER TABLE us_macro_daily ADD COLUMN stlfi REAL;
ALTER TABLE us_macro_daily ADD COLUMN t5yie REAL;
