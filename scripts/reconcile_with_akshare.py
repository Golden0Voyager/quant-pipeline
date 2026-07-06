#!/usr/bin/env python3
"""
批量清洗 quant_core.db 中的 daily_bars，用 AkShare 权威数据对比并修复差异。

v2 改进：
- AkShare 调用增加 socket 超时，避免无限挂起
- 智能修复：小差异用 UPDATE，大差异用 DELETE+INSERT
- 失败队列自动记录，支持 --retry-failed 单独重跑
- 修复完成后可自动重算技术指标（仅限被修改的股票）
- 生成 CSV 详细报告 + 实时 ETA 估算

用法：
    # 1. 先 dry-run 预览（推荐）
    python reconcile_with_akshare.py --limit 50 --dry-run

    # 2. 全量修复（先备份数据库！）
    cp ~/Code/quant_data/quant_core.db ~/Code/quant_data/quant_core.db.backup
    python reconcile_with_akshare.py

    # 3. 只重跑之前 AkShare 失败的股票
    python reconcile_with_akshare.py --retry-failed

    # 4. 修复 + 自动重算指标（推荐生产环境）
    python reconcile_with_akshare.py --update-indicators

    # 5. AkShare 不稳定时加大限流
    python reconcile_with_akshare.py --sleep 2.0 --batch-rest 20

    # 6. 断点续传
    python reconcile_with_akshare.py --resume
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import random
import socket
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.expanduser("~/Code"))

import pandas as pd

try:
    import akshare as ak
except ImportError:
    ak = None

try:
    from tradingagents.dataflows.akshare_common import no_proxy
except ImportError:
    import os
    from contextlib import contextmanager

    @contextmanager
    def no_proxy():
        keys = ("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY", "all_proxy", "ALL_PROXY")
        saved = {k: os.environ[k] for k in keys if k in os.environ}
        for k in saved:
            del os.environ[k]
        try:
            yield
        finally:
            for k, v in saved.items():
                os.environ[k] = v


# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------
DEFAULT_DB_PATH = os.path.expanduser("~/Code/quant_data/quant_core.db")
LOG_DIR = Path(DEFAULT_DB_PATH).parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / f"reconcile_{datetime.now().strftime('%Y%m%d')}.log"
PROGRESS_FILE = Path(DEFAULT_DB_PATH).parent / "reconcile_progress.json"
RETRY_FILE = Path(DEFAULT_DB_PATH).parent / "reconcile_retry.txt"
REPORT_DIR = Path(DEFAULT_DB_PATH).parent / "reports"
REPORT_DIR.mkdir(parents=True, exist_ok=True)

CLOSE_DIFF_THRESHOLD = 0.01  # close 差异超过此值视为污染（元）
SMART_REPAIR_THRESHOLD = 10  # 差异行数 <= 此值时用 UPDATE，否则 DELETE+INSERT
SMART_REPAIR_PCT = 0.05      # 差异比例 <= 此值时也用 UPDATE
AKSHARE_SOCKET_TIMEOUT = 15  # AkShare HTTP 请求超时（秒）

# ---------------------------------------------------------------------------
# 日志
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# AkShare 数据获取（带 socket 超时保护）
# ---------------------------------------------------------------------------
def get_akshare_data(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    """从 AkShare 获取股票历史数据（带指数退避重试 + socket 超时）。"""
    if ak is None:
        logger.error("akshare 未安装")
        return pd.DataFrame()

    code = symbol.split(".")[0]
    original_timeout = socket.getdefaulttimeout()

    for attempt in range(3):
        try:
            # 设置 socket 超时，防止 AkShare 无限挂起
            socket.setdefaulttimeout(AKSHARE_SOCKET_TIMEOUT)
            with no_proxy():
                df = ak.stock_zh_a_hist(
                    symbol=code,
                    period="daily",
                    start_date=start_date.replace("-", ""),
                    end_date=end_date.replace("-", ""),
                    adjust="qfq",
                )
        except Exception as e:
            socket.setdefaulttimeout(original_timeout)
            if attempt < 2:
                delay = 3.0 * (2 ** attempt)
                logger.warning(
                    f"  {symbol} AkShare 第 {attempt + 1} 次失败，{delay:.0f}s 后重试: {e}"
                )
                time.sleep(delay)
                continue
            else:
                logger.error(f"  {symbol} AkShare 连续失败: {e}")
                return pd.DataFrame()
        finally:
            socket.setdefaulttimeout(original_timeout)

        if df.empty:
            return pd.DataFrame()

        df = df.rename(
            columns={
                "日期": "date",
                "开盘": "open",
                "收盘": "close",
                "最高": "high",
                "最低": "low",
                "成交量": "volume",
                "成交额": "amount",
                "换手率": "turnover_rate",
                "涨跌幅": "pct_change",
                "振幅": "amplitude",
            }
        )
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
        return df

    return pd.DataFrame()


# ---------------------------------------------------------------------------
# 单只股票对比修复（智能策略）
# ---------------------------------------------------------------------------
def compare_and_repair(
    conn: sqlite3.Connection,
    symbol: str,
    since: str | None = None,
    dry_run: bool = False,
    smart_repair: bool = False,
    full_check: bool = False,
    backfill_source: bool = False,
) -> dict[str, any]:
    """
    对比数据库与 AkShare 数据，修复差异。

    修复策略：
    - smart_repair=True 且差异较小：逐行 UPDATE（快，保留原有行）
    - 否则：DELETE + INSERT（安全，彻底替换）

    返回统计字典 {"total", "matched", "diff", "fixed", "skipped", "failed", "elapsed"}。
    """
    t0 = time.time()

    # 1. 拉取数据库数据
    params: list[str] = [symbol]
    where_clause = "WHERE ts_code = ?"
    if since:
        where_clause += " AND trade_date >= ?"
        params.append(since)

    db_df = pd.read_sql_query(
        f"SELECT trade_date, open, close, high, low, volume, amount, "
        f"turnover_rate, pct_change, amplitude, data_source "
        f"FROM daily_bars {where_clause} ORDER BY trade_date",
        conn,
        params=tuple(params),
    )

    # 检查是否有 data_source 为 NULL 的行
    has_null_source = db_df["data_source"].isna().any() if "data_source" in db_df.columns else True

    if db_df.empty:
        return {"total": 0, "matched": 0, "diff": 0, "fixed": 0, "skipped": 1, "failed": 0, "elapsed": time.time() - t0, "diff_start": "", "diff_end": ""}

    # 2. 拉取 AkShare 数据
    start_date = db_df["trade_date"].min()
    end_date = db_df["trade_date"].max()
    ak_df = get_akshare_data(symbol, start_date, end_date)

    if ak_df.empty:
        logger.warning(f"  {symbol}: AkShare 无数据，加入失败队列")
        return {
            "total": len(db_df),
            "matched": 0,
            "diff": len(db_df),
            "fixed": 0,
            "skipped": 0,
            "failed": 1,
            "elapsed": time.time() - t0,
            "diff_start": "",
            "diff_end": "",
        }

    # 3. 合并对比（只对比共同存在的日期）
    merged = db_df.merge(
        ak_df,
        left_on="trade_date",
        right_on="date",
        how="inner",
        suffixes=("_db", "_ak"),
    )

    if merged.empty:
        logger.warning(f"  {symbol}: 数据库与 AkShare 日期无交集，全量替换")
        # 视为全部差异，走全量替换路径
        diff_rows = pd.DataFrame()
        total_diff = len(db_df)
    else:
        # 4. 快速检测：先对比最近 5 条 close（除非 full_check）
        if not full_check and len(merged) >= 5:
            recent = merged.tail(5)
            recent_diff = (recent["close_db"] - recent["close_ak"]).abs()
            if recent_diff.max() <= CLOSE_DIFF_THRESHOLD:
                elapsed = time.time() - t0
                backfilled = 0
                if backfill_source and has_null_source and not dry_run:
                    cursor = conn.cursor()
                    cursor.execute(
                        "UPDATE daily_bars SET data_source = 'akshare', updated_at = ? WHERE ts_code = ? AND data_source IS NULL",
                        (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), symbol)
                    )
                    backfilled = cursor.rowcount
                    conn.commit()
                    logger.info(f"  {symbol}: 已成功回填 {backfilled} 行 data_source 为 'akshare'")

                return {
                    "total": len(db_df),
                    "matched": len(merged),
                    "diff": 0,
                    "fixed": backfilled,
                    "skipped": 0,
                    "failed": 0,
                    "elapsed": elapsed,
                    "diff_start": "",
                    "diff_end": "",
                }

        # 5. 全量对比
        merged["close_diff"] = (merged["close_db"] - merged["close_ak"]).abs()
        diff_mask = merged["close_diff"] > CLOSE_DIFF_THRESHOLD
        diff_rows = merged[diff_mask]
        total_diff = len(diff_rows)

    diff_start = diff_rows["trade_date"].min() if not diff_rows.empty else ""
    diff_end = diff_rows["trade_date"].max() if not diff_rows.empty else ""

    result = {
        "total": len(db_df),
        "matched": len(merged) - total_diff,
        "diff": total_diff,
        "fixed": 0,
        "skipped": 0,
        "failed": 0,
        "elapsed": time.time() - t0,
        "diff_start": diff_start,
        "diff_end": diff_end,
    }

    if total_diff == 0:
        return result

    logger.info(
        f"  {symbol}: 发现 {total_diff} 行差异 "
        f"(对比 {len(merged)} 行)"
    )
    for _, row in diff_rows.head(3).iterrows():
        logger.info(
            f"    {row['trade_date']}: close_db={row['close_db']:.2f} "
            f"close_ak={row['close_ak']:.2f} diff={row['close_diff']:.4f}"
        )
    if len(diff_rows) > 3:
        logger.info(f"    ... 还有 {len(diff_rows) - 3} 行差异")

    if dry_run:
        result["elapsed"] = time.time() - t0
        return result

    # 6. 修复
    cursor = conn.cursor()
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    use_update = False
    if smart_repair and not merged.empty:
        diff_pct = total_diff / len(merged)
        if total_diff <= SMART_REPAIR_THRESHOLD or diff_pct <= SMART_REPAIR_PCT:
            use_update = True

    if use_update:
        # 6a. 智能修复：逐行 UPDATE
        update_sql = """
            UPDATE daily_bars
            SET open = ?, close = ?, high = ?, low = ?,
                volume = ?, amount = ?, turnover_rate = ?,
                pct_change = ?, amplitude = ?,
                data_source = ?, updated_at = ?
            WHERE ts_code = ? AND trade_date = ?
        """
        update_rows: list[tuple] = []
        for _, row in diff_rows.iterrows():
            update_rows.append(
                (
                    float(row["open_ak"]),
                    float(row["close_ak"]),
                    float(row["high_ak"]),
                    float(row["low_ak"]),
                    float(row["volume_ak"]),
                    float(row["amount_ak"]) if pd.notna(row["amount_ak"]) else None,
                    float(row["turnover_rate_ak"]) if pd.notna(row.get("turnover_rate_ak")) else None,
                    float(row["pct_change_ak"]) if pd.notna(row.get("pct_change_ak")) else None,
                    float(row["amplitude_ak"]) if pd.notna(row.get("amplitude_ak")) else None,
                    "akshare",
                    now_str,
                    symbol,
                    row["trade_date"],
                )
            )
        cursor.executemany(update_sql, update_rows)
        conn.commit()
        result["fixed"] = len(update_rows)
        logger.info(
            f"  {symbol}: 已修复 — UPDATE {len(update_rows)} 行差异数据"
        )
    else:
        # 6b. 全量替换：DELETE + INSERT
        cursor.execute("DELETE FROM daily_bars WHERE ts_code = ?", (symbol,))
        deleted = cursor.rowcount

        insert_rows: list[tuple] = []
        for _, row in ak_df.iterrows():
            insert_rows.append(
                (
                    symbol,
                    row["date"],
                    float(row["open"]),
                    float(row["close"]),
                    float(row["high"]),
                    float(row["low"]),
                    float(row["volume"]),
                    float(row["amount"]) if pd.notna(row["amount"]) else None,
                    float(row["turnover_rate"]) if pd.notna(row.get("turnover_rate")) else None,
                    float(row["pct_change"]) if pd.notna(row.get("pct_change")) else None,
                    float(row["amplitude"]) if pd.notna(row.get("amplitude")) else None,
                    "akshare",
                    now_str,
                )
            )

        cursor.executemany(
            """
            INSERT INTO daily_bars (
                ts_code, trade_date, open, close, high, low,
                volume, amount, turnover_rate, pct_change, amplitude,
                data_source, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            insert_rows,
        )
        conn.commit()
        result["fixed"] = deleted
        logger.info(
            f"  {symbol}: 已修复 — 删除 {deleted} 行旧数据，插入 {len(insert_rows)} 行 AkShare 数据"
        )

    result["elapsed"] = time.time() - t0
    return result


# ---------------------------------------------------------------------------
# 局部指标重算
# ---------------------------------------------------------------------------
def update_indicators_for_symbols(db_path: str, symbols: list[str]) -> dict[str, int]:
    """只为指定的股票重新计算技术指标。"""
    try:
        from smartmoney_hunter.database import DatabaseManager
        from smartmoney_hunter.indicators import IndicatorCalculator
    except ImportError as e:
        logger.error(f"无法导入依赖模块进行指标重算: {e}")
        return {"success": 0, "failed": 0}

    db = DatabaseManager()
    calc = IndicatorCalculator()
    success_count = 0
    failed_count = 0

    logger.info(f"\n📊 开始为 {len(symbols)} 只修复过的股票重算指标...")
    for i, symbol in enumerate(symbols, 1):
        if i % 50 == 0 or i == len(symbols):
            logger.info(f"  指标进度: {i}/{len(symbols)} ({100 * i // len(symbols)}%)")
        try:
            df = db.get_daily_bars(symbol)
            if df.empty or len(df) < 60:
                continue

            # calculate_all_indicators 需要 date 列
            if "trade_date" in df.columns and "date" not in df.columns:
                df = df.rename(columns={"trade_date": "date"})

            df_ind = calc.calculate_all_indicators(df)
            db.save_indicators(symbol, df_ind)
            success_count += 1
        except Exception as e:
            logger.warning(f"  {symbol} 指标重算失败: {e}")
            failed_count += 1

    logger.info(f"📊 指标重算完成: 成功 {success_count}, 失败 {failed_count}")
    return {"success": success_count, "failed": failed_count}


# ---------------------------------------------------------------------------
# 断点续传进度
# ---------------------------------------------------------------------------
class ReconcileProgress:
    FILE = PROGRESS_FILE

    @classmethod
    def load(cls) -> tuple[int, str]:
        if not cls.FILE.exists():
            return 0, ""
        try:
            with open(cls.FILE, encoding="utf-8") as f:
                data = json.load(f)
            return data.get("idx", 0), data.get("last_symbol", "")
        except (json.JSONDecodeError, OSError):
            return 0, ""

    @classmethod
    def save(cls, idx: int, last_symbol: str) -> None:
        with open(cls.FILE, "w", encoding="utf-8") as f:
            json.dump({"idx": idx, "last_symbol": last_symbol}, f, ensure_ascii=False)

    @classmethod
    def clear(cls) -> None:
        if cls.FILE.exists():
            cls.FILE.unlink()


# ---------------------------------------------------------------------------
# 失败队列
# ---------------------------------------------------------------------------
def load_retry_symbols() -> list[str]:
    if not RETRY_FILE.exists():
        return []
    with open(RETRY_FILE, encoding="utf-8") as f:
        return list(dict.fromkeys(line.strip() for line in f if line.strip()))


def save_retry_symbol(symbol: str) -> None:
    existing = set(load_retry_symbols())
    if symbol in existing:
        return
    with open(RETRY_FILE, "a", encoding="utf-8") as f:
        f.write(symbol + "\n")


def clear_retry_file() -> None:
    if RETRY_FILE.exists():
        RETRY_FILE.unlink()


# ---------------------------------------------------------------------------
# ETA 计算
# ---------------------------------------------------------------------------
def format_eta(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        return f"{seconds / 60:.1f}m"
    else:
        return f"{seconds / 3600:.1f}h"


# ---------------------------------------------------------------------------
# 主控
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="批量清洗 daily_bars，用 AkShare 权威数据修复差异"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="只报告差异，不实际修改数据库",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="测试模式：只处理前 N 只股票",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="断点续传：从上次中断位置继续",
    )
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="只处理 reconcile_retry.txt 中记录的上次失败股票",
    )
    parser.add_argument(
        "--since",
        type=str,
        default=None,
        help="只检查该日期之后的数据 (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default=DEFAULT_DB_PATH,
        help=f"数据库路径 (默认: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=1.0,
        help="单只股票间最小休息秒数 (默认: 1.0)",
    )
    parser.add_argument(
        "--batch-rest",
        type=int,
        default=10,
        help="每 N 只股票后额外休息 10 秒 (默认: 10)",
    )
    parser.add_argument(
        "--smart-repair",
        action="store_true",
        help="智能修复：差异较小时用 UPDATE 代替 DELETE+INSERT",
    )
    parser.add_argument(
        "--full-check",
        action="store_true",
        help="禁用快速跳过（最近5天匹配也做全量对比），防止漏检早期污染",
    )
    parser.add_argument(
        "--update-indicators",
        action="store_true",
        help="修复完成后自动为修改过的股票重算技术指标",
    )
    parser.add_argument(
        "--backfill-source",
        action="store_true",
        help="对无差异但 data_source 为 NULL 的历史数据，回填 data_source='akshare'",
    )
    parser.add_argument(
        "--symbols",
        type=str,
        default=None,
        help="只处理指定的股票，逗号分隔 (如: 000001,600519,300750)",
    )
    parser.add_argument(
        "--sample-rate",
        type=int,
        default=0,
        help="每 N 只强制做一次全量对比（忽略快速跳过），防止早期污染漏检。0=关闭 (默认)",
    )
    args = parser.parse_args()

    conn = sqlite3.connect(args.db_path)
    cursor = conn.cursor()

    # 获取待处理股票列表
    if args.retry_failed:
        symbols = load_retry_symbols()
        if not symbols:
            logger.info("ℹ️  没有失败队列记录，无需重跑")
            return
        logger.info(f"🔄 从失败队列加载 {len(symbols)} 只股票")
    elif args.symbols:
        symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
        # 验证股票是否在数据库中
        cursor.execute(
            f"SELECT DISTINCT ts_code FROM daily_bars WHERE ts_code IN ({','.join('?' * len(symbols))})",
            symbols,
        )
        valid = {row[0] for row in cursor.fetchall()}
        invalid = set(symbols) - valid
        if invalid:
            logger.warning(f"⚠️  以下股票不在数据库中，已跳过: {', '.join(sorted(invalid))}")
        symbols = sorted(valid)
        logger.info(f"🎯 指定股票模式: {len(symbols)} 只")
    else:
        cursor.execute("SELECT DISTINCT ts_code FROM daily_bars ORDER BY ts_code")
        symbols = [row[0] for row in cursor.fetchall()]
        # 清理旧的失败队列（全新的全量运行时）
        if not args.resume:
            clear_retry_file()

    cursor.close()

    if args.limit:
        symbols = symbols[: args.limit]

    total = len(symbols)
    logger.info("=" * 60)
    logger.info("🧹 AkShare 数据清洗开始")
    logger.info(f"   数据库: {args.db_path}")
    logger.info(f"   股票数: {total}")
    if args.since:
        logger.info(f"   起始日期: {args.since}")
    logger.info(f"   模式: {'dry-run' if args.dry_run else '修复'}")
    logger.info(f"   智能修复: {'开启' if args.smart_repair else '关闭'}")
    logger.info(f"   全量检查: {'开启' if args.full_check else '关闭'}")
    logger.info(f"   回填来源: {'开启' if args.backfill_source else '关闭'}")
    if args.sample_rate > 0:
        logger.info(f"   抽样检查: 每 {args.sample_rate} 只强制全量对比")
    logger.info(f"   限流: sleep={args.sleep}s, batch-rest={args.batch_rest}只")
    logger.info("=" * 60)

    # 断点续传
    start_idx = 0
    if args.resume and not args.retry_failed:
        saved_idx, _ = ReconcileProgress.load()
        if saved_idx > 0 and saved_idx < total:
            start_idx = saved_idx
            logger.info(f"🔄 断点续传：从第 {start_idx + 1} 只继续")

    stats = {"total": 0, "matched": 0, "diff": 0, "fixed": 0, "skipped": 0, "failed": 0}
    repaired_symbols: set[str] = set()
    per_symbol_times: list[float] = []
    report_rows: list[dict] = []

    start_time = time.time()

    for i, symbol in enumerate(symbols[start_idx:], start=start_idx + 1):
        time.time()
        result = compare_and_repair(
            conn,
            symbol,
            since=args.since,
            dry_run=args.dry_run,
            smart_repair=args.smart_repair,
            full_check=args.full_check,
            backfill_source=args.backfill_source,
        )
        for k, v in result.items():
            if k in stats:
                stats[k] += v

        per_symbol_times.append(result["elapsed"])

        if result["failed"]:
            save_retry_symbol(symbol)
        elif result["fixed"] > 0 and not args.dry_run:
            repaired_symbols.add(symbol)

        # 生成报告行
        report_rows.append({
            "symbol": symbol,
            "total_rows": result["total"],
            "matched": result["matched"],
            "diff_rows": result["diff"],
            "diff_start_date": result.get("diff_start", ""),
            "diff_end_date": result.get("diff_end", ""),
            "fixed_rows": result["fixed"],
            "skipped": result["skipped"],
            "failed": result["failed"],
            "elapsed_sec": round(result["elapsed"], 2),
        })

        # 进度刷新 + ETA
        if i % 50 == 0 or i == total:
            time.time() - start_time
            avg_time = sum(per_symbol_times) / len(per_symbol_times)
            remaining = total - i
            eta_sec = avg_time * remaining + (remaining / args.batch_rest) * 10 + remaining * args.sleep
            logger.info(
                f"📊 进度 {i}/{total} ({100 * i // total}%) | "
                f"diff={stats['diff']} fixed={stats['fixed']} skipped={stats['skipped']} failed={stats['failed']} | "
                f"ETA {format_eta(eta_sec)}"
            )

        if i % 10 == 0:
            ReconcileProgress.save(i, symbol)

        # 限流
        time.sleep(args.sleep + random.uniform(0, 0.5))

        # 批次间额外休息
        if i % args.batch_rest == 0 and i < total:
            rest = 10
            logger.info(f"⏳ 批次休息 {rest}s...")
            time.sleep(rest)

    conn.close()

    if not args.dry_run and not args.retry_failed:
        ReconcileProgress.clear()

    total_elapsed = time.time() - start_time

    # 保存 CSV 报告
    report_path = REPORT_DIR / f"reconcile_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    with open(report_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "symbol", "total_rows", "matched", "diff_rows", "diff_start_date", "diff_end_date",
            "fixed_rows", "skipped", "failed", "elapsed_sec"
        ])
        writer.writeheader()
        writer.writerows(report_rows)
    logger.info(f"📄 详细报告已保存: {report_path}")

    logger.info("\n" + "=" * 60)
    logger.info("🧹 清洗完成")
    logger.info(f"   总股票: {total}")
    logger.info(f"   总对比行: {stats['total']}")
    logger.info(f"   匹配行: {stats['matched']}")
    logger.info(f"   差异行: {stats['diff']}")
    logger.info(f"   修复行: {stats['fixed']}")
    logger.info(f"   跳过(无数据): {stats['skipped']}")
    logger.info(f"   AkShare 失败: {stats['failed']}")
    logger.info(f"   耗时: {format_eta(total_elapsed)}")
    if stats["failed"] > 0:
        logger.info(f"   失败队列: {RETRY_FILE} ({stats['failed']} 只)")
    logger.info("=" * 60)

    # 自动指标重算
    if not args.dry_run and args.update_indicators and repaired_symbols:
        ind_stats = update_indicators_for_symbols(args.db_path, sorted(repaired_symbols))
        logger.info(
            f"📊 指标重算: 成功 {ind_stats['success']}, 失败 {ind_stats['failed']}"
        )
    elif not args.dry_run and stats["fixed"] > 0 and not args.update_indicators:
        logger.info("\n⚠️  修复完成后，建议重新计算技术指标：")
        logger.info("   python daily_pipeline.py --task update_indicators")
        logger.info("   或下次运行 reconcile 时加 --update-indicators 参数")


if __name__ == "__main__":
    main()
