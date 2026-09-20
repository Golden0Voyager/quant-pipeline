-- Migration 018: EIA weekly petroleum status indicators.
--
-- Five validated weekly series from the EIA Petroleum Status Report,
-- fetched via the APIv2 backward-compat route (/v2/seriesid/<v1 id>):
--   PET.WCESTUS1.W  U.S. commercial crude stocks excl. SPR (MBBL)
--   PET.WCSSTUS1.W  U.S. Strategic Petroleum Reserve stocks (MBBL)
--   PET.WGTSTUS1.W  U.S. total motor gasoline stocks (MBBL)
--   PET.WCRFPUS2.W  U.S. field production of crude oil (MBBL/D)
--   PET.WPULEUS3.W  Refinery utilization (%)
-- Long format: one row per (week, series). The task re-upserts a
-- trailing window weekly revisions stay corrected.

CREATE TABLE IF NOT EXISTS eia_petroleum_weekly (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    week_date DATE NOT NULL,
    series_id TEXT NOT NULL,
    series_name TEXT,
    value REAL,
    units TEXT,
    data_source TEXT,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(week_date, series_id)
);

CREATE INDEX IF NOT EXISTS idx_eia_petroleum_date
    ON eia_petroleum_weekly(week_date DESC);
