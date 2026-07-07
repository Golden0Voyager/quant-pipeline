#!/usr/bin/env python3
"""
回填 A 股历史日线数据：将默认 3 年历史数据补充至 6 年。
使用 ak.stock_zh_a_daily（新浪）直调，空数据不重试。
已确认无历史数据的股票会记录，避免每次重复尝试。
"""
from __future__ import annotations

import argparse
import logging
import random
import sqlite3
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import akshare as ak
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)-7s | %(message)s')
logger = logging.getLogger(__name__)

DB_PATH = Path("~/Code/quant_data/quant_core.db").expanduser()
SKIP_FILE = Path(__file__).parent / ".backfill_skip"


def fmt_date(d: datetime) -> str:
    return d.strftime("%Y-%m-%d")


def sina_symbol(code: str) -> str:
    if code.startswith(("6", "9")):
        return f"sh{code}"
    elif code.startswith(("0", "2", "3")):
        return f"sz{code}"
    else:
        return f"bj{code}"


def load_skip_set() -> set[str]:
    if SKIP_FILE.exists():
        return {line.strip() for line in SKIP_FILE.read_text().splitlines() if line.strip()}
    return set()


def save_skip_set(skip_set: set[str]):
    SKIP_FILE.write_text("\n".join(sorted(skip_set)) + "\n")


def fetch_lightweight(code: str, start: str, end: str, skip_set: set[str]) -> pd.DataFrame | None:
    if code in skip_set:
        return None
    try:
        sym = sina_symbol(code)
        df = ak.stock_zh_a_daily(symbol=sym, start_date=start, end_date=end, adjust="qfq")
        if df is not None and not df.empty:
            closes = df["close"]
            prev_close = closes.shift(1)
            df["pct_change"] = ((closes - prev_close) / prev_close * 100).round(2)
            df["amplitude"] = ((df["high"] - df["low"]) / prev_close * 100).round(2)
            df["data_source"] = "sina"
            return df
        skip_set.add(code)
    except Exception as e:
        logger.debug(f"  {code} 新浪失败 ({e})")
        skip_set.add(code)
    return None


def main():
    parser = argparse.ArgumentParser(description="回填历史日线至 6 年")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--batch-size", type=int, default=None)
    args = parser.parse_args()

    if not DB_PATH.exists():
        logger.error(f"数据库不存在: {DB_PATH}")
        sys.exit(1)

    target_start = datetime.now() - timedelta(days=2190)
    target_start_str = fmt_date(target_start)
    logger.info(f"🎯 数据回填目标起点: {target_start_str}")

    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()

    cursor.execute("""
        SELECT ts_code, MIN(trade_date) as first_date
        FROM daily_bars
        GROUP BY ts_code
    """)
    stock_min_dates = {r[0]: r[1] for r in cursor.fetchall()}

    cursor.execute("SELECT code FROM stock_list")
    all_stock_codes = {r[0] for r in cursor.fetchall()}

    skip_set = load_skip_set()
    if skip_set:
        logger.info(f"📋 已跳过 {len(skip_set)} 只确认无历史数据的股票")

    now_str = datetime.now().strftime("%Y-%m-%d")

    to_backfill = []
    for code in sorted(all_stock_codes):
        if code in skip_set:
            continue
        first_date = stock_min_dates.get(code)
        if not first_date:
            continue
        if first_date > target_start_str:
            end_dt = datetime.strptime(first_date, "%Y-%m-%d") - timedelta(days=1)
            end_str = fmt_date(end_dt)
            if target_start_str < end_str:
                to_backfill.append((code, target_start_str, end_str))

    if args.limit:
        to_backfill = to_backfill[:args.limit]
        logger.info(f"测试模式：只处理前 {args.limit} 只")
    elif args.batch_size:
        to_backfill = to_backfill[:args.batch_size]
        logger.info(f"批次模式：本次处理 {args.batch_size} 只，剩余 {max(0, len(to_backfill) - args.batch_size)} 只下次继续")

    total_stocks = len(to_backfill)
    logger.info(f"需回填: {total_stocks} 只 (已跳过 {len(skip_set)} 只)")

    if total_stocks == 0:
        logger.info("✅ 回填完成")
        return

    success = 0
    failed = 0

    for i, (symbol, start, end) in enumerate(to_backfill, 1):
        try:
            logger.info(f"[{i}/{total_stocks}] {symbol} ({start} ~ {end}) ...")
            df = fetch_lightweight(symbol, start, end, skip_set)
            if df is not None and not df.empty:
                df["trade_date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
                rows_to_insert = []
                for _, row in df.iterrows():
                    rows_to_insert.append((
                        symbol,
                        row.get("trade_date"),
                        row.get("open"),
                        row.get("close"),
                        row.get("high"),
                        row.get("low"),
                        row.get("volume"),
                        row.get("amount"),
                        row.get("turnover"),
                        row.get("pct_change"),
                        row.get("amplitude"),
                        row.get("data_source", "sina"),
                        now_str,
                    ))
                cursor.executemany("""
                    INSERT OR REPLACE INTO daily_bars (
                        ts_code, trade_date, open, close, high, low,
                        volume, amount, turnover_rate, pct_change, amplitude,
                        data_source, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, rows_to_insert)
                conn.commit()
                success += 1
                logger.info(f"  ✅ {symbol} 保存 {len(df)} 条")
            else:
                failed += 1
                logger.info(f"  ⏭ {symbol} 无数据（已记入跳过列表）")
            time.sleep(random.uniform(0.1, 0.3))
        except Exception as e:
            logger.error(f"  ❌ {symbol} 异常: {e}")
            failed += 1

    save_skip_set(skip_set)
    conn.close()
    logger.info(f"回填结束！成功: {success}, 跳过: {len(skip_set)} (本次新增需跳过)")

if __name__ == "__main__":
    main()
