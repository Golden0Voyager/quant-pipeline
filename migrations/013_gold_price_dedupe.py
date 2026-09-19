"""Migration 013: Deduplicate gold_price and fix trade_date semantics.

update_gold_price used to stamp every run day onto the full ~10y history
returned by ak.spot_golden_benchmark_sge, so the table grew by ~2530
redundant rows per run (112900 rows / 45 run days at migration time).
The writer now derives trade_date from trading_time and only upserts the
trailing window. This migration repairs the accumulated damage:

1. keep a single row per trading_time (the latest fetch, MAX(id)) —
   dedupe MUST group by trading_time alone: grouping by the old
   trade_date first would keep one copy per run day and the following
   UPDATE would collide with UNIQUE(trade_date, trading_time).
2. trade_date := date part of trading_time (the price's own trading
   day, not the pipeline run day).

gold_price is created by the smartmoney schema (quant_hunter), not by
any pipeline migration, so on a fresh pipeline-only database the table
may not exist yet — guard and skip.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def apply(conn: Any) -> None:
    """Apply migration 013."""
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='gold_price'",
    ).fetchone()
    if row is None:
        logger.info("  ➖ gold_price does not exist, skipped")
        return

    conn.execute("""
        DELETE FROM gold_price
        WHERE id NOT IN (
            SELECT MAX(id) FROM gold_price
            GROUP BY trading_time
        )
    """)
    conn.execute("""
        UPDATE gold_price
        SET trade_date = substr(trading_time, 1, 10)
    """)
    logger.info("  ✅ gold_price deduplicated, trade_date repaired")
