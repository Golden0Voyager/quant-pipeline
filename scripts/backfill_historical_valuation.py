#!/usr/bin/env python3
"""
回填 historical_valuation 表：用东财 datacenter API 批量拉取历史估值数据。

优化版：
- 减少请求间隔 0.3s → 0.1s
- 支持 --resume 断点续传
- 支持 --start / --end 指定日期范围，默认从 2024 年起
- 自动跳过已有数据的日期
- 进度条 + 预估剩余时间

用法：
    uv run python scripts/backfill_historical_valuation.py                         # 从 2024-01-04 拉到今天
    uv run python scripts/backfill_historical_valuation.py --start 2024-01-04      # 同上，显式指定
    uv run python scripts/backfill_historical_valuation.py --days 60               # 最近 60 天（兼容旧用法）
    uv run python scripts/backfill_historical_valuation.py --dry-run               # 预览
"""
from __future__ import annotations

import argparse
import logging
import os
import sqlite3
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests

# 禁用代理（东财 API 走代理会报 ProxyError）
for var in ['http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY', 'all_proxy']:
    os.environ.pop(var, None)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_PATH = Path("~/Code/quant_data/quant_core.db").expanduser()


def _is_trade_day(d: datetime) -> bool:
    return d.weekday() < 5


def _get_trade_dates_since(start: datetime, end: datetime) -> list[str]:
    """返回 start~end 之间的所有交易日。"""
    dates = []
    d = start
    while d <= end:
        if _is_trade_day(d):
            dates.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)
    return dates


def _get_recent_trade_dates(n: int) -> list[str]:
    """返回最近 N 个交易日（从今天往前）。"""
    dates = []
    d = datetime.now()
    while len(dates) < n:
        if _is_trade_day(d):
            dates.append(d.strftime("%Y-%m-%d"))
        d -= timedelta(days=1)
    return list(reversed(dates))


def fetch_day_data(trade_date: str, session: requests.Session) -> list[dict]:
    """从东财 datacenter API 获取某天全市场估值数据。"""
    records = []
    page = 1
    page_size = 500

    while True:
        try:
            url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
            params = {
                "sortColumns": "TRADE_DATE,SECURITY_CODE",
                "sortTypes": "-1,1",
                "pageSize": str(page_size),
                "pageNumber": str(page),
                "reportName": "RPT_VALUEANALYSIS_DET",
                "columns": "SECURITY_CODE,SECURITY_NAME_ABBR,TRADE_DATE,CLOSE_PRICE,"
                           "TOTAL_MARKET_CAP,PE_TTM,PB_MRQ,PE_LAR,PEG_CAR,PS_TTM",
                "source": "WEB",
                "client": "WEB",
                "filter": f"(TRADE_DATE='{trade_date}')",
            }
            resp = session.get(url, params=params, timeout=15)
            data = resp.json()
            if data.get("success") and data.get("result") and data["result"].get("data"):
                chunk = data["result"]["data"]
                records.extend(chunk)
                total_count = data["result"].get("count", 0)
                if page * page_size >= total_count:
                    break
                page += 1
            else:
                break
        except Exception as e:
            logger.warning(f"  ⚠️ {trade_date} 第 {page} 页失败: {e}")
            break

    return records


def save_to_db(records: list[dict], trade_date: str, cur: sqlite3.Cursor) -> int:
    """保存一批估值数据到 historical_valuation 表。"""
    saved = 0
    for rec in records:
        try:
            code = str(rec.get("SECURITY_CODE", "")).strip()
            if not code:
                continue
            pe_ttm = rec.get("PE_TTM")
            pb = rec.get("PB_MRQ")
            ps_ttm = rec.get("PS_TTM")
            if pe_ttm is None and pb is None and ps_ttm is None:
                continue

            cur.execute(
                """INSERT OR IGNORE INTO historical_valuation
                   (ts_code, trade_date, pe_ttm, pb, ps_ttm, dividend_yield)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (code, trade_date, pe_ttm, pb, ps_ttm, None),
            )
            if cur.rowcount:
                saved += 1
        except Exception:
            continue
    return saved


def main() -> int:
    parser = argparse.ArgumentParser(description="回填历史估值快照（优化版）")
    parser.add_argument("--days", type=int, default=None,
                        help="回填最近 N 个交易日（默认从 2024-01-04 拉到今天）")
    parser.add_argument("--start", type=str, default=None,
                        help="起始日期 YYYY-MM-DD")
    parser.add_argument("--end", type=str, default=None,
                        help="结束日期 YYYY-MM-DD，默认今天")
    parser.add_argument("--sleep", type=float, default=0.1,
                        help="请求间隔秒数（默认 0.1）")
    parser.add_argument("--dry-run", action="store_true",
                        help="只打印要抓取的日期，不写入数据库")
    args = parser.parse_args()
    os.nice(10)

    # 确定日期范围
    if args.days:
        trade_dates = _get_recent_trade_dates(args.days)
        logger.info(f"📅 向后看 {args.days} 个交易日")
    elif args.start:
        start = datetime.strptime(args.start, "%Y-%m-%d")
        end = datetime.strptime(args.end, "%Y-%m-%d") if args.end else datetime.now()
        trade_dates = _get_trade_dates_since(start, end)
        logger.info(f"📅 区间: {args.start} ~ {end.strftime('%Y-%m-%d')}")
    else:
        # 默认从 2024-01-04（东财 API 最早可用数据）拉到今天
        start = datetime(2024, 1, 4)
        end = datetime.now()
        trade_dates = _get_trade_dates_since(start, end)
        logger.info(f"📅 默认区间: 2024-01-04 ~ {end.strftime('%Y-%m-%d')}")

    logger.info(f"📊 共 {len(trade_dates)} 个交易日需要回填")
    if args.dry_run:
        for td in trade_dates:
            print(f"  {td}")
        return 0

    # 连接数据库
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    cur = conn.cursor()

    # 确保表存在
    cur.execute("""
        CREATE TABLE IF NOT EXISTS historical_valuation (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_code TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            pe_ttm REAL,
            pb REAL,
            ps_ttm REAL,
            dividend_yield REAL,
            UNIQUE(ts_code, trade_date)
        )
    """)
    conn.commit()

    session = requests.Session()
    session.proxies = {"http": None, "https": None}
    session.trust_env = False

    total_saved = 0
    total_skipped = 0
    total_errors = 0
    t0 = time.time()

    for i, td in enumerate(trade_dates, 1):
        # 检查是否已有数据（断点续传）
        cur.execute("SELECT COUNT(*) FROM historical_valuation WHERE trade_date = ?", (td,))
        existing = cur.fetchone()[0]
        if existing > 0:
            logger.info(f"  [{i}/{len(trade_dates)}] {td} 已有 {existing} 条，跳过")
            total_skipped += 1
            continue

        records = fetch_day_data(td, session)
        if not records:
            logger.warning(f"  [{i}/{len(trade_dates)}] {td} 无数据（可能非交易日或 API 无记录）")
            total_errors += 1
            # 早期日期无数据是正常的（超出 API 覆盖范围），少记为空数据避免反复请求
            cur.execute(
                "INSERT OR IGNORE INTO historical_valuation (ts_code, trade_date, pe_ttm, pb) VALUES (?, ?, ?, ?)",
                ("__NO_DATA__", td, None, None),
            )
            conn.commit()
            continue

        saved = save_to_db(records, td, cur)
        conn.commit()
        total_saved += saved
        elapsed = time.time() - t0
        rate = i / elapsed if elapsed > 0 else 0
        remaining_s = (len(trade_dates) - i) / rate if rate > 0 else 0
        logger.info(
            f"  [{i}/{len(trade_dates)}] {td}: 保存 {saved} 条 "
            f"(总计: {total_saved} 条, 速率: {rate:.1f}日/秒, 剩余: {remaining_s/60:.1f}分)"
        )

        time.sleep(args.sleep)

    conn.close()
    elapsed = time.time() - t0
    logger.info(f"\n{'='*60}")
    logger.info("✅ 回填完成")
    logger.info(f"   新增: {total_saved} 条")
    logger.info(f"   跳过: {total_skipped} 个已有交易日")
    logger.info(f"   无数据: {total_errors} 个交易日")
    logger.info(f"   耗时: {elapsed/60:.1f} 分")
    logger.info(f"{'='*60}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
