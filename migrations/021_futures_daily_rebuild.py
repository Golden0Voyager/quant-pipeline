"""Migration 021: rebuild futures_daily after the trade_date semantics bug.

The legacy snapshot writer read `latest.get("date", ...)` while Sina's
futures_main_sina actually returns the Chinese column 「日期」, so the
fallback stored the PIPELINE RUN DATE as trade_date for every row. The
whole table therefore held per-run snapshots keyed by run day — worthless
as a price series. The rewrite fetches full history windows (first run
backfills from 2019) with the correct column, so the clean fix is to
delete the poisoned rows and let the next run rebuild.

futures_daily is created by the smartmoney schema (quant_hunter), not by
any pipeline migration, so on a fresh pipeline-only database the table
may not exist yet — guard and skip.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def apply(conn: Any) -> None:
    """Apply migration 021."""
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='futures_daily'",
    ).fetchone()
    if row is None:
        logger.info("  ➖ futures_daily does not exist, skipped")
        return
    conn.execute("DELETE FROM futures_daily")
    logger.info("  ✅ futures_daily cleared (trade_date semantics rebuild)")
