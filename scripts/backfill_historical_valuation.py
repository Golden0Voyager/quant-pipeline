#!/usr/bin/env python3
"""
回填 historical_valuation 表：用东财 datacenter API 批量拉取过去 N 个交易日的历史估值数据。

相比逐日 cron 积累 60 天，这个脚本在 5-10 分钟内就能补全 60 天的历史估值快照，
之后 PE/PB 百分位就可以直接计算。

用法：
    uv run python scripts/backfill_historical_valuation.py               # 回填最近 60 个交易日
    uv run python scripts/backfill_historical_valuation.py --days 120    # 回填 120 天
    uv run python scripts/backfill_historical_valuation.py --days 10     # 测试：只补 10 天
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

DB_PATH = Path("~/Code/data/quant_data/quant_core.db").expanduser()

# A 股交易日历（简化：排除周末，不考虑法定节假日）
def _is_trade_day(d: datetime) -> bool:
    return d.weekday() < 5


def _get_trade_dates(n: int) -> list[str]:
    """返回最近 N 个交易日的日期字符串列表（从今天往前）。"""
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
                "columns": "SECURITY_CODE,SECURITY_NAME_ABBR,TRADE_DATE,CLOSE_PRICE,TOTAL_MARKET_CAP,PE_TTM,PB_MRQ,PE_LAR,PEG_CAR,PS_TTM",
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
                continue  # 没有有用数据就跳过

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
    parser = argparse.ArgumentParser(description="回填历史估值快照")
    parser.add_argument("--days", type=int, default=60, help="回填最近 N 个交易日（默认 60）")
    parser.add_argument("--start", type=str, default=None, help="起始日期 YYYY-MM-DD，替代 --days")
    parser.add_argument("--end", type=str, default=None, help="结束日期 YYYY-MM-DD，默认今天")
    parser.add_argument("--dry-run", action="store_true", help="只打印要抓取的日期，不写入数据库")
    args = parser.parse_args()

    # 确定日期范围
    if args.start:
        start = datetime.strptime(args.start, "%Y-%m-%d")
        end = datetime.strptime(args.end, "%Y-%m-%d") if args.end else datetime.now()
        trade_dates = []
        d = start
        while d <= end:
            if _is_trade_day(d):
                trade_dates.append(d.strftime("%Y-%m-%d"))
            d += timedelta(days=1)
    else:
        trade_dates = _get_trade_dates(args.days)

    logger.info(f"📅 共 {len(trade_dates)} 个交易日需要回填")
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
    t0 = time.time()

    for i, td in enumerate(trade_dates, 1):
        # 检查是否已有数据
        cur.execute("SELECT COUNT(*) FROM historical_valuation WHERE trade_date = ?", (td,))
        existing = cur.fetchone()[0]
        if existing > 0:
            logger.info(f"  [{i}/{len(trade_dates)}] {td} 已有 {existing} 条，跳过")
            total_skipped += 1
            continue

        records = fetch_day_data(td, session)
        if not records:
            logger.warning(f"  [{i}/{len(trade_dates)}] {td} 无数据")
            total_skipped += 1
            continue

        saved = save_to_db(records, td, cur)
        conn.commit()
        total_saved += saved
        elapsed = time.time() - t0
        rate = i / elapsed if elapsed > 0 else 0
        remaining = (len(trade_dates) - i) / rate if rate > 0 else 0
        logger.info(
            f"  [{i}/{len(trade_dates)}] {td}: 保存 {saved} 条 "
            f"(进度: {total_saved} 条, 剩余 {remaining/60:.1f} 分)"
        )

        # 请求间隔，避免被限流
        time.sleep(0.3)

    conn.close()
    elapsed = time.time() - t0
    logger.info(f"\n✅ 回填完成: 新增 {total_saved} 条, 跳过 {total_skipped} 个交易日, 耗时 {elapsed/60:.1f} 分")
    logger.info(f"📊 historical_valuation 表现共约 {total_saved} 条数据，PE/PB 百分位即可计算")

    return 0


if __name__ == "__main__":
    sys.exit(main())
