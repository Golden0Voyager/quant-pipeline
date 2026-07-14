"""
日线数据更新任务
─────────────
从 daily_pipeline.py 提取出的 update_bars 和 _update_single_bar。
"""
# ruff: noqa: E402  -- sys.path setup must happen before package imports

from __future__ import annotations

import logging
import os
import random
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd  # noqa: F401  # DataFrame types used via db/loader returns

# ── 路径设置（与 daily_pipeline.py 一致，使 smartmoney_hunter 可导入）──
_CODE_DIR = os.path.expanduser("~/Code")
if _CODE_DIR not in sys.path:
    sys.path.insert(0, _CODE_DIR)
_HUNTER_SRC = os.path.expanduser("~/Code/quant_hunter/src")
if _HUNTER_SRC not in sys.path and os.path.isdir(_HUNTER_SRC):
    sys.path.insert(0, _HUNTER_SRC)

from smartmoney_hunter.market_utils import is_beijing_stock  # noqa: F401

from core.config import (
    BATCH_SIZE_VAL as BATCH_SIZE,
)
from core.config import (
    BATCH_SLEEP_VAL as BATCH_SLEEP,
)
from core.config import (
    DEFAULT_LOOKBACK_DAYS,
    SHARED_DATA_DIR,
)
from core.config import (
    MAX_RETRY_VAL as MAX_RETRY,
)
from core.config import (
    PARALLEL_WORKERS_VAL as PARALLEL_WORKERS,
)
from core.config import (
    PER_STOCK_MAX_SLEEP_VAL as PER_STOCK_MAX_SLEEP,
)
from core.config import (
    PER_STOCK_MIN_SLEEP_VAL as PER_STOCK_MIN_SLEEP,
)
from core.config import (
    PROGRESS_FLUSH_INTERVAL_VAL as PROGRESS_FLUSH_INTERVAL,
)
from core.config import (
    RETRY_DELAY_VAL as RETRY_DELAY,
)
from core.monitor import AkShareMonitor
from core.progress import ProgressTracker
from core.utils import should_skip_beijing
from interface import DatabaseInterface, DataLoaderInterface

try:
    import akshare as ak  # noqa: F401
except ImportError:
    ak = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)


def update_bars(
    db: DatabaseInterface,
    loader: DataLoaderInterface,
    limit: int = None,
    resume: bool = False,
    symbols: list[str] | None = None,
) -> dict:
    """分批增量更新指定或所有股票的日线数据，支持断点续传。"""
    logger.info("=" * 60)
    logger.info("📈 任务: 更新日线数据")
    logger.info("=" * 60)

    now = datetime.now()
    if now.hour == 15:
        logger.warning(
            f"当前时间 {now.hour}:{now.minute:02d}，处于收盘结算窗口（15:00~16:00），"
            "东财接口可能返回 RemoteDisconnected，建议等到 16:00 后再运行"
        )

    if symbols:
        stock_codes = [s for s in symbols if not should_skip_beijing(s)]
        bj_count = len(symbols) - len(stock_codes)
    else:
        stocks = db.get_stock_list()
        if stocks.empty:
            logger.error("❌ 股票列表为空")
            return {"success": 0, "failed": 0, "skipped": 0, "total": 0}
        stock_codes = [c for c in stocks["code"].tolist() if not should_skip_beijing(c)]
        bj_count = len(stocks) - len(stock_codes)
    if limit:
        stock_codes = stock_codes[:limit]
        logger.info(f"⚠️  测试模式：只更新前 {limit} 只")

    total = len(stock_codes)
    bj_count = len(stocks) - total
    if bj_count > 0:
        logger.info(f"📊 共 {total} 只股票待更新（已跳过 {bj_count} 只北交所）")
    else:
        logger.info(f"📊 共 {total} 只股票待更新（已包含北交所）")

    # ── 断点续传检测 ──
    progress = None
    start_idx = 0
    if resume:
        progress = ProgressTracker.load()
        if progress:
            if progress.get("date") == datetime.now().strftime("%Y-%m-%d"):
                last_symbol = progress.get("last_symbol", "")
                start_idx = ProgressTracker.find_resume_index(stock_codes, last_symbol)
                if start_idx > 0:
                    logger.info(
                        f"🔄 断点续传：上次处理到 {last_symbol} "
                        f"({start_idx}/{total})，继续处理..."
                    )
            else:
                logger.info(
                    f"ℹ️  进度文件是昨天的 ({progress.get('date')})，"
                    "今日从头开始"
                )
                ProgressTracker.clear()
        else:
            logger.info("ℹ️  未发现进度文件，从头开始")
    else:
        # 非续传模式：如果存在旧进度文件，先清理
        if ProgressTracker.FILE.exists():
            ProgressTracker.clear()

    processed_count = progress.get("processed", 0) if progress else 0
    success_count = 0
    failed_count = 0
    skipped_count = 0
    failed_symbols: list[str] = progress.get("failed_queue", []) if progress else []
    last_symbol = ""

    # 计算剩余需要处理的股票
    remaining_codes = stock_codes[start_idx:]
    remaining_total = len(remaining_codes)

    # 初始化 AkShare 稳定性监控
    monitor = AkShareMonitor()

    # ── 自选股全量拉取初始化 ──
    watchlist_symbols = set()
    backfilled_symbols = set()
    backfill_file = SHARED_DATA_DIR / "watchlist_backfilled.txt"
    try:
        watchlist_df = db.watchlist_get_all()
        if not watchlist_df.empty:
            watchlist_symbols = set(watchlist_df["ts_code"].tolist())
        if backfill_file.exists():
            with open(backfill_file, encoding="utf-8") as f:
                backfilled_symbols = {line.strip() for line in f if line.strip()}
    except Exception as e:
        logger.warning(f"⚠️ 初始化自选股拉取逻辑失败: {e}")

    for batch_idx in range(0, remaining_total, BATCH_SIZE):
        batch = remaining_codes[batch_idx : batch_idx + BATCH_SIZE]
        batch_num = batch_idx // BATCH_SIZE + 1
        total_batches = (remaining_total + BATCH_SIZE - 1) // BATCH_SIZE
        abs_start = start_idx + batch_idx
        abs_end = min(start_idx + batch_idx + BATCH_SIZE, total)

        logger.info(
            f"\n🔄 批次 {batch_num}/{total_batches} "
            f"({batch[0]} ~ {batch[-1]}, {abs_start+1}-{abs_end}/{total})"
        )

        if PARALLEL_WORKERS > 1 and len(batch) > 1:
            db_write_lock = threading.Lock()
            with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as executor:
                fut_to_symbol = {
                    executor.submit(
                        _update_single_bar, db, loader, symbol,
                        watchlist_symbols=watchlist_symbols,
                        backfilled_symbols=backfilled_symbols,
                        backfill_file=backfill_file,
                        db_lock=db_write_lock,
                    ): symbol
                    for symbol in batch
                }
                for future in as_completed(fut_to_symbol):
                    symbol = fut_to_symbol[future]
                    try:
                        result = future.result()
                    except Exception as e:
                        logger.error(f"❌ {symbol} 并行处理异常: {e}")
                        result = "failed"

                    # ── 以下结果处理逻辑与串行分支一致 ──
                    if result == "success":
                        success_count += 1
                    elif result == "skipped":
                        skipped_count += 1
                    else:
                        failed_count += 1
                        if symbol not in failed_symbols:
                            failed_symbols.append(symbol)
                    processed_count += 1

                    if result != "skipped":
                        monitor.record(result == "success", symbol)

                    last_symbol = symbol

                    # 每 N 只股票刷新一次进度文件
                    current_processed = processed_count
                    if current_processed % PROGRESS_FLUSH_INTERVAL == 0:
                        logger.info(
                            f"  📥 进度: {current_processed}/{total} "
                            f"(成功: {success_count}, 跳过: {skipped_count}, 失败: {failed_count})"
                        )
                        ProgressTracker.save(
                            task="update_bars",
                            last_symbol=last_symbol,
                            processed=current_processed,
                            total=total,
                            failed_queue=failed_symbols,
                        )

            # 并行批次结束后检查是否需要中止
            should_abort, abort_msg = monitor.should_abort()
            if should_abort:
                logger.warning(f"⛔ {abort_msg}")
                ProgressTracker.save(
                    task="update_bars",
                    last_symbol=last_symbol,
                    processed=processed_count,
                    total=total,
                    failed_queue=failed_symbols,
                )
                return {
                    "success": success_count,
                    "failed": failed_count,
                    "skipped": skipped_count,
                    "total": total,
                    "failed_symbols": failed_symbols,
                }
        else:
            for symbol in batch:
                result = _update_single_bar(
                    db,
                    loader,
                    symbol,
                    watchlist_symbols=watchlist_symbols,
                    backfilled_symbols=backfilled_symbols,
                    backfill_file=backfill_file,
                )
                if result == "success":
                    success_count += 1
                elif result == "skipped":
                    skipped_count += 1
                else:
                    failed_count += 1
                    if symbol not in failed_symbols:
                        failed_symbols.append(symbol)
                processed_count += 1

                # 记录 AkShare 稳定性（仅对真实执行过网络更新的股票进行记录，跳过的股票不影响统计）
                if result != "skipped":
                    monitor.record(result == "success", symbol)

                last_symbol = symbol

                # 如果触发了网络抓取（非 skipped），增加 0.1s 到 0.4s 的随机抖动延迟，平滑请求
                if result != "skipped":
                    time.sleep(random.uniform(0.1, 0.4))

                # 每 N 只股票刷新一次进度文件
                current_processed = processed_count
                if current_processed % PROGRESS_FLUSH_INTERVAL == 0:
                    logger.info(
                        f"  📥 进度: {current_processed}/{total} "
                        f"(成功: {success_count}, 跳过: {skipped_count}, 失败: {failed_count})"
                    )
                    ProgressTracker.save(
                        task="update_bars",
                        last_symbol=last_symbol,
                        processed=current_processed,
                        total=total,
                        failed_queue=failed_symbols,
                    )

                # 动态调整限流：成功率低时增加休息时间
                if result != "skipped":
                    multiplier = monitor.get_recommended_sleep_multiplier()
                    sleep_time = random.uniform(PER_STOCK_MIN_SLEEP, PER_STOCK_MAX_SLEEP) * multiplier
                    time.sleep(sleep_time)

                # 检查是否需要中止（AkShare 极度不稳定时）
                should_abort, abort_msg = monitor.should_abort()
                if should_abort:
                    logger.warning(f"⛔ {abort_msg}")
                    ProgressTracker.save(
                        task="update_bars",
                        last_symbol=last_symbol,
                        processed=current_processed,
                        total=total,
                        failed_queue=failed_symbols,
                    )
                    return {
                        "success": success_count,
                        "failed": failed_count,
                        "skipped": skipped_count,
                        "total": total,
                        "failed_symbols": failed_symbols,
                    }

        # 每批次结束也刷新进度
        current_processed = processed_count
        ProgressTracker.save(
            task="update_bars",
            last_symbol=last_symbol,
            processed=current_processed,
            total=total,
            failed_queue=failed_symbols,
        )

        # 批次结束时汇报监控状态
        monitor.log_status()

        if batch_idx + BATCH_SIZE < remaining_total:
            # 动态调整批次休息：成功率低时增加休息
            multiplier = monitor.get_recommended_sleep_multiplier()
            batch_sleep = BATCH_SLEEP * multiplier
            logger.info(f"⏳ 批次间休息 {batch_sleep:.1f}s... (倍率 {multiplier}x)")
            time.sleep(batch_sleep)

    # 处理完成：去重并保存失败队列
    unique_failed = list(dict.fromkeys(failed_symbols))  # 保持顺序去重
    if unique_failed:
        # 保留进度文件，记录失败队列供 retry_failed 任务使用
        ProgressTracker.save(
            task="retry",
            last_symbol=last_symbol,
            processed=processed_count,
            total=total,
            failed_queue=unique_failed,
        )
        logger.warning(f"⚠️  {len(unique_failed)} 只股票记录到失败队列，可通过 retry_failed 任务重试")
    else:
        # 无失败，清除进度文件
        ProgressTracker.clear()

    logger.info("\n" + "=" * 60)
    logger.info("📈 日线数据更新完成")
    logger.info(f"  ✅ 成功: {success_count} 只")
    logger.info(f"  ⏭️  跳过(已最新): {skipped_count} 只")
    logger.info(f"  ❌ 失败: {failed_count} 只")
    logger.info("=" * 60)

    return {
        "success": success_count,
        "failed": failed_count,
        "skipped": skipped_count,
        "total": total,
        "failed_symbols": failed_symbols,
    }


def _update_single_bar(
    db: DatabaseInterface,
    loader: DataLoaderInterface,
    symbol: str,
    watchlist_symbols: set[str] | None = None,
    backfilled_symbols: set[str] | None = None,
    backfill_file: Path | None = None,
    db_lock: threading.Lock | None = None,
) -> str:
    """更新单只股票的日线数据，带重试。

    数据质量规则：
    - 优先使用 AkShare 数据
    - 如果是自选股，且尚未进行全量拉取，则拉取全量历史数据
    - 如果新增/全部数据来自 yfinance，跳过保存（yfinance 仅作为运行时临时 fallback，
      不应写入 quant_core.db 这个黄金数据源）
    """
    # ── 自选股及全量拉取逻辑初始化 ──
    if watchlist_symbols is None:
        try:
            if db_lock:
                with db_lock:
                    watchlist_df = db.watchlist_get_all()
            else:
                watchlist_df = db.watchlist_get_all()
            watchlist_symbols = set(watchlist_df["ts_code"].tolist()) if not watchlist_df.empty else set()
        except Exception:
            watchlist_symbols = set()

    if backfill_file is None:
        backfill_file = SHARED_DATA_DIR / "watchlist_backfilled.txt"

    if backfilled_symbols is None:
        try:
            if backfill_file.exists():
                with open(backfill_file, encoding="utf-8") as f:
                    backfilled_symbols = {line.strip() for line in f if line.strip()}
            else:
                backfilled_symbols = set()
        except Exception:
            backfilled_symbols = set()

    is_watchlist = watchlist_symbols and symbol in watchlist_symbols
    is_backfilled = backfilled_symbols and symbol in backfilled_symbols

    for attempt in range(MAX_RETRY):
        try:
            # 1. 自选股且尚未全量拉取：执行从 19900101 开始的全量抓取
            if is_watchlist and not is_backfilled:
                logger.info(f"🚀 {symbol} 属于自选股且尚未进行全量拉取，准备下载 1990 年起的完整历史K线...")
                df_bars = loader.get_daily_bars(symbol, start_date="19900101")
                if not df_bars.empty:
                    # 检查是否全部是 yfinance，如果是则不保存以防污染
                    if 'data_source' in df_bars.columns:
                        src_values = df_bars['data_source'].dropna().unique()
                        if len(src_values) == 1 and src_values[0] == 'yfinance':
                            logger.warning(f"  ⚠️ {symbol} 自选股全量拉取全部来自 yfinance，跳过保存")
                            return "failed"

                    if db_lock:
                        with db_lock:
                            db.save_daily_bars(symbol, df_bars)
                    else:
                        db.save_daily_bars(symbol, df_bars)
                    logger.info(f"✅ {symbol} 自选股全量历史K线拉取并保存成功，共 {len(df_bars)} 条")

                    # 记录已完成全量回填
                    if db_lock:
                        with db_lock:
                            backfilled_symbols.add(symbol)
                            try:
                                with open(backfill_file, "a", encoding="utf-8") as f:
                                    f.write(f"{symbol}\n")
                            except Exception as fe:
                                logger.warning(f"⚠️ 无法更新自选股全量标记文件 {backfill_file}: {fe}")
                    else:
                        backfilled_symbols.add(symbol)
                        try:
                            with open(backfill_file, "a", encoding="utf-8") as f:
                                f.write(f"{symbol}\n")
                        except Exception as fe:
                            logger.warning(f"⚠️ 无法更新自选股全量标记文件 {backfill_file}: {fe}")

                    return "success"
                else:
                    logger.warning(f"⚠️ {symbol} 自选股全量拉取返回空数据")
                    return "failed"

            # 2. 正常增量/全量拉取路径
            if db_lock:
                with db_lock:
                    existing = db.get_daily_bars(symbol)
            else:
                existing = db.get_daily_bars(symbol)
            if not existing.empty:
                df_bars = loader.incremental_update(symbol, existing)
                # 优化点：如果行数没变，说明已经是最新，无需重复保存，直接返回 skipped
                if len(df_bars) == len(existing):
                    return "skipped"
            else:
                # 正常非自选股的全量拉取走 DEFAULT_LOOKBACK_DAYS 天配置
                df_bars = loader.get_daily_bars(symbol, start_date=(
                    datetime.now() - timedelta(days=DEFAULT_LOOKBACK_DAYS)
                ).strftime("%Y%m%d"))

            if df_bars.empty:
                return "skipped"

            # 数据质量把关：拒绝保存纯 yfinance 数据到数据库
            if 'data_source' in df_bars.columns:
                src_values = df_bars['data_source'].dropna().unique()
                if len(src_values) == 1 and src_values[0] == 'yfinance':
                    logger.warning(
                        f"  ⚠️  {symbol}: 数据全部来自 yfinance，跳过保存。"
                        f" quant_core.db 只接受 AkShare 数据。"
                    )
                    return "failed"

            if db_lock:
                with db_lock:
                    db.save_daily_bars(symbol, df_bars)
            else:
                db.save_daily_bars(symbol, df_bars)
            logger.debug(f"  ✅ {symbol}: {len(df_bars)} 条")
            return "success"

        except Exception as e:
            if attempt < MAX_RETRY - 1:
                logger.debug(
                    f"  ⚠️  {symbol} 第 {attempt + 1} 次失败，{RETRY_DELAY}s 后重试: {e}"
                )
                time.sleep(RETRY_DELAY)
            else:
                logger.warning(f"  ❌ {symbol}: {e}")
                return "failed"

    return "failed"

