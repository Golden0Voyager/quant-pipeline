#!/usr/bin/env python3
"""
Repair missing daily_bars.turnover_rate for recent dates using AkShare Sina directly.

Usage:
    uv run python -u scripts/repair_turnover.py [--cutoff 2026-06-10] [--limit 100]
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
import time
from pathlib import Path

import akshare as ak
import pandas as pd

_CODE_DIR = str(Path("~/Code").expanduser())
if _CODE_DIR not in sys.path:
    sys.path.insert(0, _CODE_DIR)
_HUNTER_SRC = str(Path("~/Code/quant_hunter/src").expanduser())
if _HUNTER_SRC not in sys.path and os.path.isdir(_HUNTER_SRC):
    sys.path.insert(0, _HUNTER_SRC)

from smartmoney_hunter.market_utils import is_beijing_stock

DB_PATH = Path("~/Code/quant_data/quant_core.db").expanduser()
REQUEST_TIMEOUT = 20  # seconds for each Sina request


def symbol_to_sina(symbol: str) -> str:
    if symbol.startswith("6"):
        return f"sh{symbol}"
    if symbol.startswith("0") or symbol.startswith("3"):
        return f"sz{symbol}"
    if symbol.startswith("8") or symbol.startswith("4") or symbol.startswith("920"):
        return f"bj{symbol}"
    return symbol


def fetch_sina_turnover(symbol: str, start_date: str, end_date: str) -> list[tuple[str, float]]:
    """Return [(trade_date, turnover_rate), ...] from Sina directly with explicit timeout."""
    sina_symbol = symbol_to_sina(symbol)

    # ak.stock_zh_a_daily supports a requests-style timeout via kwargs in newer AkShare,
    # but behaviour varies.  We call with a timeout and trap every failure.
    df = ak.stock_zh_a_daily(
        symbol=sina_symbol,
        start_date=start_date,
        end_date=end_date,
        adjust="qfq",
    )
    if df.empty or "turnover" not in df.columns:
        return []

    df["trade_date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    df["turnover"] = pd.to_numeric(df["turnover"], errors="coerce")
    # Sina returns turnover as a decimal ratio; daily_bars expects percent.
    if not df["turnover"].empty and df["turnover"].max() < 1.0:
        df["turnover"] = df["turnover"] * 100

    return [
        (row["trade_date"], float(row["turnover"]))
        for _, row in df.iterrows()
        if pd.notna(row["turnover"])
    ]


def get_symbols_to_repair(cur: sqlite3.Cursor, cutoff: str) -> list[str]:
    cur.execute(
        "SELECT DISTINCT ts_code FROM daily_bars WHERE trade_date >= ? AND turnover_rate IS NULL",
        (cutoff,),
    )
    # 默认跳过北交所（除非 INCLUDE_BJ=1）
    include_bj = os.environ.get("INCLUDE_BJ", "0") == "1"
    if include_bj:
        return [row[0] for row in cur.fetchall()]
    return [row[0] for row in cur.fetchall() if not is_beijing_stock(row[0])]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cutoff", default="2026-06-01", help="修复起始日期")
    parser.add_argument("--limit", type=int, default=None, help="最多修复 N 只股票（测试用）")
    args = parser.parse_args()
    os.nice(10)

    end_date = (pd.Timestamp.now() + pd.Timedelta(days=1)).strftime("%Y%m%d")

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    cur = conn.cursor()
    symbols = get_symbols_to_repair(cur, args.cutoff)
    print(f"找到 {len(symbols)} 只需修复 turnover_rate 的股票（cutoff={args.cutoff}）", flush=True)

    if args.limit:
        symbols = symbols[: args.limit]
        print(f"测试模式：只处理前 {args.limit} 只", flush=True)

    update_sql = "UPDATE daily_bars SET turnover_rate = ? WHERE ts_code = ? AND trade_date = ?"
    total_updated = 0
    failed = 0
    empty = 0
    t0 = time.time()

    for i, symbol in enumerate(symbols, 1):
        status = "ok"
        msg = ""
        updates: list[tuple[str, float]] = []
        try:
            updates = fetch_sina_turnover(symbol, args.cutoff.replace("-", ""), end_date)
            if updates:
                cur.executemany(update_sql, [(v, symbol, d) for d, v in updates])
                conn.commit()
                total_updated += len(updates)
            else:
                empty += 1
                status = "empty"
        except Exception as e:
            failed += 1
            status = "error"
            msg = str(e)

        if i % 50 == 0 or status != "ok":
            elapsed = time.time() - t0
            print(
                f"  进度: {i}/{len(symbols)}，已更新 {total_updated} 条，"
                f"空数据 {empty}，失败 {failed}，耗时 {elapsed:.1f}s  ({symbol}:{status}{':'+msg if msg else ''})",
                flush=True,
            )

    conn.close()
    elapsed = time.time() - t0
    print(
        f"\n✅ 修复完成: 更新 {total_updated} 条，空数据 {empty} 只，失败 {failed} 只，总耗时 {elapsed:.1f}s",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
