#!/usr/bin/env python3
"""
Repair missing daily_bars.turnover_rate for recent dates using AkShare Sina directly.

Usage:
    uv run python -u scripts/repair_turnover.py [--cutoff 2026-06-10] [--limit 100]
"""
from __future__ import annotations

import argparse
import os
import socket
import sqlite3
import sys
import time
from pathlib import Path

import akshare as ak
import pandas as pd

# 全局套接字超时兜底：本脚本独立运行不经过 core.config，个别请求路径
# （如 akshare 内部调用）漏传 timeout 时，SSL read 会无限期挂死
# （2026-08-01 事故：单只股票的静默连接卡住整个回补 27+ 分钟）
socket.setdefaulttimeout(20)

_CODE_DIR = str(Path("~/Code").expanduser())
if _CODE_DIR not in sys.path:
    sys.path.insert(0, _CODE_DIR)
_HUNTER_SRC = str(Path("~/Code/quant_hunter/src").expanduser())
if _HUNTER_SRC not in sys.path and os.path.isdir(_HUNTER_SRC):
    sys.path.insert(0, _HUNTER_SRC)

from smartmoney_hunter.market_utils import is_beijing_stock  # noqa: E402

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


def _normalize_turnover_to_percent(df: pd.DataFrame) -> pd.DataFrame:
    """将 turnover 统一为百分数口径（daily_bars.turnover_rate 的口径）。"""
    df["turnover"] = pd.to_numeric(df["turnover"], errors="coerce")
    # Sina 返回小数比率（如 0.005586），东财返回百分数（如 0.56）。
    # 以 1.0 为界做简单推断：如果全部 <1.0，则视为小数比率并乘 100。
    if not df["turnover"].empty and df["turnover"].max() < 1.0:
        df["turnover"] = df["turnover"] * 100
    return df


def fetch_sina_turnover(symbol: str, start_date: str, end_date: str) -> list[tuple[str, float]]:
    """Return [(trade_date, turnover_rate), ...] from Sina directly."""
    sina_symbol = symbol_to_sina(symbol)
    df = ak.stock_zh_a_daily(
        symbol=sina_symbol,
        start_date=start_date,
        end_date=end_date,
        adjust="qfq",
    )
    if df.empty or "turnover" not in df.columns:
        return []

    df["trade_date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    df = _normalize_turnover_to_percent(df)

    return [
        (row["trade_date"], float(row["turnover"]))
        for _, row in df.iterrows()
        if pd.notna(row["turnover"])
    ]


def fetch_eastmoney_turnover(symbol: str, start_date: str, end_date: str) -> list[tuple[str, float]]:
    """Return [(trade_date, turnover_rate), ...] from AkShare EastMoney interface.

    作为 Sina 的 fallback，用于科创板 CDR（如 689009 九号公司）等 Sina 未覆盖的标的。
    """
    # 临时清理代理变量：系统代理可能把东财请求导向本地不可用的代理服务器
    proxy_vars = ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"]
    saved_proxies = {k: os.environ.pop(k, None) for k in proxy_vars}
    saved_no_proxy = os.environ.get("NO_PROXY")
    os.environ["NO_PROXY"] = "push2his.eastmoney.com,push2.eastmoney.com,82.push2.eastmoney.com"
    try:
        df = ak.stock_zh_a_hist(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            adjust="qfq",
        )
    finally:
        for k, v in saved_proxies.items():
            if v is not None:
                os.environ[k] = v
            else:
                os.environ.pop(k, None)
        if saved_no_proxy is None:
            os.environ.pop("NO_PROXY", None)
        else:
            os.environ["NO_PROXY"] = saved_no_proxy

    if df.empty or "换手率" not in df.columns:
        return []

    df = df.rename(columns={"日期": "date", "换手率": "turnover"})
    df["trade_date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    df = _normalize_turnover_to_percent(df)

    return [
        (row["trade_date"], float(row["turnover"]))
        for _, row in df.iterrows()
        if pd.notna(row["turnover"])
    ]


def fetch_turnover(symbol: str, start_date: str, end_date: str) -> list[tuple[str, float]]:
    """先尝试 Sina，失败或为空则 fallback 到东方财富。"""
    try:
        updates = fetch_sina_turnover(symbol, start_date, end_date)
        if updates:
            return updates
    except Exception:
        pass

    # Sina 失败或返回空：常见于科创板 CDR，尝试东财接口
    return fetch_eastmoney_turnover(symbol, start_date, end_date)


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
            updates = fetch_turnover(symbol, args.cutoff.replace("-", ""), end_date)
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
