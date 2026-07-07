#!/usr/bin/env python3
"""
Data health feedback loop for SmartMoney Hunter / quant_pipeline.

Exits non-zero when the shared DB lacks fields/tables required by the
6 core preset strategies.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

DB_PATH = Path("~/Code/quant_data/quant_core.db").expanduser()


def get_column_names(cur: sqlite3.Cursor, table: str) -> set[str]:
    cur.execute(f"PRAGMA table_info({table})")
    return {row[1] for row in cur.fetchall()}


def main() -> int:
    if not DB_PATH.exists():
        print(f"❌ DB not found: {DB_PATH}")
        return 1

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    errors: list[str] = []

    # 1. Latest daily_bars turnover_rate must be mostly non-null
    cur.execute("""
        SELECT COUNT(*) AS total,
               COUNT(turnover_rate) AS non_null
        FROM daily_bars
        WHERE trade_date = (SELECT MAX(trade_date) FROM daily_bars)
    """)
    total, non_null = cur.fetchone()
    ratio = non_null / total if total else 0.0
    print(f"daily_bars latest date turnover_rate: {non_null}/{total} ({ratio:.1%})")
    if ratio < 0.9:
        errors.append(
            f"latest daily_bars.turnover_rate mostly NULL ({ratio:.1%}); "
            "all preset liquidity guards will fail"
        )

    # 2. fundamentals must include operating_cashflow
    fundamentals_cols = get_column_names(cur, "fundamentals")
    if "operating_cashflow" not in fundamentals_cols:
        errors.append("fundamentals table missing operating_cashflow column")

    # 3. quarterly_financials schema must match what save_quarterly_financials inserts
    quarterly_cols = get_column_names(cur, "quarterly_financials")
    required_q_cols = {
        "report_period",
        "revenue",
        "net_profit",
        "operating_cashflow",
        "roe",
        "gross_margin",
        "net_margin",
        "revenue_growth",
        "profit_growth",
        "debt_ratio",
        "eps",
        "bps",
    }
    missing_q = required_q_cols - quarterly_cols
    if missing_q:
        errors.append(
            f"quarterly_financials missing columns required by pipeline: {sorted(missing_q)}"
        )

    # 4. Empty tables that strategies depend on
    for table in ("historical_valuation", "sector_industry", "sector_fund_flow"):
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        print(f"{table}: {count} rows")
        if count == 0:
            errors.append(f"{table} is empty")

    # 5. shareholder_count must be populated
    cur.execute("SELECT COUNT(*) FROM shareholder_count")
    sh_count = cur.fetchone()[0]
    print(f"shareholder_count: {sh_count} rows")
    if sh_count == 0:
        errors.append("shareholder_count is empty")

    conn.close()

    if errors:
        print("\n❌ Data health check FAILED:")
        for e in errors:
            print(f"  - {e}")
        return 1

    print("\n✅ Data health check passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
