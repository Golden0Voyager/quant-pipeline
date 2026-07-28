"""
日线数据更新任务
─────────────
从 daily_pipeline.py 提取出的 update_bars 和 _update_single_bar。
"""
# ruff: noqa: E402  -- sys.path setup must happen before package imports

from __future__ import annotations

import contextlib
import logging
import os
import random
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd  # noqa: F401  # DataFrame types used via db/loader returns

# ── 路径设置（与 daily_pipeline.py 一致，使 smartmoney_hunter 可导入）──
_CODE_DIR = os.path.expanduser("~/Code")
if _CODE_DIR not in sys.path:
    sys.path.insert(0, _CODE_DIR)
_HUNTER_SRC = os.path.expanduser("~/Code/quant_hunter/src")
if _HUNTER_SRC not in sys.path and os.path.isdir(_HUNTER_SRC):
    sys.path.insert(0, _HUNTER_SRC)

from smartmoney_hunter.market_utils import is_beijing_stock

from core.calendar import get_expected_latest_trading_day
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
from core.utils import is_real_db_path, should_skip_beijing
from interface import DatabaseInterface, DataLoaderInterface

try:
    import akshare as ak  # noqa: F401
except ImportError:
    ak = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)

# 熔断哨兵：连续失败触发熔断前，先拉取一只必然有数据的高流动性标的，
# 区分「网络彻底不可用」与「个别掉队股源端缺数」（如停牌股），避免误熔断。
CANARY_SYMBOL = "000001"
MAX_CANARY_PROBES = 3  # 每次运行最多哨兵验证次数，超出后直接信任熔断判定

# 停牌预检：落后股数量在该阈值内才逐只查雪球行情状态
#（正常交易日开盘前全市场都“落后”，此时预检无意义且请求量大，直接跳过）
_SUSPEND_PRECHECK_MAX = 50


def _detect_suspended_symbols(db: DatabaseInterface, expected_latest: str | None) -> set[str]:
    """运行前停牌预检：返回停牌股票的 6 位代码集合。

    只检查「日线落后于最新交易日」的少量股票：
    - 主源：雪球 batch/quote 的 status 字段（status==2 停牌；东财被封时仍可用）
    - 辅源：东财停复牌名单 stock_tfp_em（覆盖雪球不支持的北交所）
    任一源失败均静默降级，返回部分或空集合，不影响主流程。
    """
    if not _has_real_db_path(db) or not expected_latest:
        return set()

    # 1. 落后股集合（一次聚合查询，本地 SQL 无网络开销）
    import sqlite3
    try:
        with sqlite3.connect(str(db.db_path), timeout=5.0) as conn:
            rows = conn.execute(
                "SELECT ts_code, MAX(trade_date) FROM daily_bars GROUP BY ts_code"
            ).fetchall()
    except sqlite3.Error as e:
        logger.debug(f"停牌预检：落后股查询失败: {e}")
        return set()
    lagging = {
        str(c)[:6]
        for c, d in rows
        if not d or (_normalize_trade_date(d) or "") < expected_latest
    }
    if not lagging or len(lagging) > _SUSPEND_PRECHECK_MAX:
        return set()

    suspended: set[str] = set()

    # 2. 辅源：东财停复牌名单（一次请求，覆盖北交所）
    if ak is not None:
        try:
            tfp = ak.stock_tfp_em(date=datetime.now().strftime("%Y%m%d"))
            if tfp is not None and not tfp.empty and "代码" in tfp.columns:
                tfp_codes = {str(c).split(".")[0].zfill(6) for c in tfp["代码"].tolist()}
                suspended |= tfp_codes & lagging
        except Exception as e:
            logger.debug(f"停牌预检：东财停复牌名单获取失败: {e}")

    # 3. 主源：雪球行情状态确认（不支持北交所）
    remaining = [c for c in sorted(lagging - suspended) if not is_beijing_stock(c)]
    if remaining:
        try:
            from smartmoney_hunter import xueqiu as xq
            for quote in xq.get_batch_quotes(remaining):
                if quote.get("status") == 2 and quote.get("code"):
                    # 雪球 code 实测为裸 6 位码；取末 6 位防御带 SH/SZ 前缀的变体
                    suspended.add(str(quote["code"])[-6:])
        except Exception as e:
            logger.debug(f"停牌预检：雪球状态查询失败: {e}")
    return suspended


def _canary_probe(loader: DataLoaderInterface) -> bool:
    """熔断前哨兵验证：拉取哨兵股票近期日线，确认数据源是否真的不可用。

    Returns:
        True 表示哨兵拉取成功（网络正常，连续失败是个股问题）；
        False 表示哨兵也失败（网络确实不可用，应当熔断）。
    """
    try:
        start = (datetime.now() - timedelta(days=15)).strftime("%Y%m%d")
        df = loader.get_daily_bars(CANARY_SYMBOL, start_date=start)
        if df is None or df.empty:
            return False
        # 纯 yfinance fallback 数据不能证明 AkShare 可用
        if "data_source" in df.columns:
            src_values = df["data_source"].dropna().unique()
            if len(src_values) == 1 and src_values[0] == "yfinance":
                return False
        return True
    except Exception as e:
        logger.debug(f"哨兵请求失败: {e}")
        return False


def _normalize_trade_date(value: object) -> str | None:
    """Normalize common trade date forms to YYYY-MM-DD for lexical comparison."""
    if value is None:
        return None
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    if not isinstance(value, str):
        return None
    text = str(value).strip()
    if not text:
        return None
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return text[:10]
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) >= 8:
        return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"
    return text


def _has_real_db_path(db: DatabaseInterface) -> bool:
    return is_real_db_path(getattr(db, "db_path", None))


def _bars_result(
    *,
    success: int,
    failed: int,
    skipped: int,
    total: int,
    attempted: int,
    failed_symbols: list[str],
) -> dict[str, Any]:
    """构造兼容旧计数器的日线任务结果。"""
    status = "degraded" if failed else "success"
    result = {
        "status": status,
        "saved": success,
        "attempted": attempted,
        "success": success,
        "failed": failed,
        "skipped": skipped,
        "total": total,
        "failed_symbols": failed_symbols,
    }
    if failed:
        result["error"] = f"{failed} failures"
    return result


def _bars_no_data_result(*, reason: str) -> dict[str, Any]:
    """构造合法零工作量的日线任务结果。"""
    result = _bars_result(
        success=0,
        failed=0,
        skipped=0,
        total=0,
        attempted=0,
        failed_symbols=[],
    )
    result["status"] = "no_data"
    result["reason"] = reason
    return result


def _bars_failed_result(*, error: str) -> dict[str, Any]:
    """构造参数/契约错误的日线任务结果。"""
    result = _bars_result(
        success=0,
        failed=0,
        skipped=0,
        total=0,
        attempted=0,
        failed_symbols=[],
    )
    result["status"] = "failed"
    result["error_kind"] = "data_quality"
    result["error"] = error
    return result


def update_bars(
    db: DatabaseInterface,
    loader: DataLoaderInterface,
    limit: int = None,
    resume: bool = False,
    symbols: list[str] | None = None,
    force: bool = False,
) -> dict:
    """分批增量更新指定或所有股票的日线数据，支持断点续传。"""
    logger.info("=" * 60)
    logger.info("📈 任务: 更新日线数据")
    logger.info("=" * 60)

    if resume and (limit is not None or symbols is not None):
        return _bars_failed_result(
            error="resume cannot be combined with limit or symbols"
        )

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
            return _bars_no_data_result(reason="stock list is empty")
        stock_codes = [c for c in stocks["code"].tolist() if not should_skip_beijing(c)]
        bj_count = len(stocks) - len(stock_codes)
    if limit:
        stock_codes = stock_codes[:limit]
        logger.info(f"⚠️  测试模式：只更新前 {limit} 只")

    total = len(stock_codes)
    if bj_count > 0:
        logger.info(f"📊 共 {total} 只股票待更新（已跳过 {bj_count} 只北交所）")
    else:
        logger.info(f"📊 共 {total} 只股票待更新（已包含北交所）")

    if total == 0:
        return _bars_no_data_result(reason="no eligible symbols")

    # ── 智能探测：快速 SQL 检查是否全部已是最新 ──
    if not resume and not force and not limit and not symbols and _has_real_db_path(db):
        import sqlite3
        try:
            _conn = sqlite3.connect(str(db.db_path))
            _total_stocks = _conn.execute(
                "SELECT COUNT(*) FROM stock_list"
            ).fetchone()[0]
            _covered = _conn.execute(
                "SELECT COUNT(DISTINCT ts_code) FROM daily_bars"
            ).fetchone()[0]
            _latest_bar = _conn.execute(
                "SELECT MAX(trade_date) FROM daily_bars"
            ).fetchone()[0]
            _conn.close()

            if _covered >= _total_stocks and _latest_bar:
                _expected = get_expected_latest_trading_day()
                if _latest_bar >= _expected:
                    logger.info(
                        f"✅ 智能探测：全部 {_total_stocks} 只股票数据已是最新"
                        f"（截至 {_latest_bar}），跳过批次扫描"
                    )
                    result = _bars_result(
                        success=0,
                        failed=0,
                        skipped=_total_stocks,
                        total=_total_stocks,
                        attempted=0,
                        failed_symbols=[],
                    )
                    result["probe_skipped"] = True
                    return result
                logger.info(
                    f"💡 智能探测：数据截至 {_latest_bar}，最新交易日为 {_expected}，继续更新"
                )
            else:
                logger.info(
                    f"💡 智能探测：{_covered}/{_total_stocks} 只有数据"
                    f"（最新 {_latest_bar or 'N/A'}），继续更新"
                )
        except sqlite3.Error as e:
            logger.debug(f"智能探测跳过: {e}")
        finally:
            with contextlib.suppress(Exception):
                _conn.close()

    # ── 断点续传检测 ──
    progress = None
    start_idx = 0
    retry_mode = False
    retry_unresolved: list[str] = []
    if resume:
        progress = ProgressTracker.load()
        if progress:
            if progress.get("date") == datetime.now().strftime("%Y-%m-%d"):
                progress_task = progress.get("task")
                if progress_task == "retry":
                    stock_code_set = set(stock_codes)
                    stock_codes = [
                        code for code in dict.fromkeys(progress.get("failed_queue", []))
                        if code in stock_code_set
                    ]
                    retry_mode = True
                    retry_unresolved = stock_codes.copy()
                    total = len(stock_codes)
                    progress = None
                    logger.info("🔄 断点续传：仅重试失败队列 (%d 只)", total)
                    if total == 0:
                        ProgressTracker.clear()
                        return _bars_no_data_result(
                            reason="retry queue has no eligible symbols"
                        )
                elif progress_task in (None, "update_bars"):
                    last_symbol = progress.get("last_symbol", "")
                    start_idx = ProgressTracker.find_resume_index(stock_codes, last_symbol)
                    if start_idx > 0:
                        logger.info(
                            f"🔄 断点续传：上次处理到 {last_symbol} "
                            f"({start_idx}/{total})，继续处理..."
                        )
                else:
                    logger.info(
                        "ℹ️  忽略未知进度任务 %s 的扫描断点",
                        progress_task,
                    )
                    progress = None
            else:
                logger.info(
                    f"ℹ️  进度文件是昨天的 ({progress.get('date')})，"
                    "今日从头开始"
                )
                ProgressTracker.clear()
                progress = None
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

    def _mark_retry_resolved(symbol: str, result: str) -> None:
        if retry_mode and result in {"success", "skipped"}:
            with contextlib.suppress(ValueError):
                retry_unresolved.remove(symbol)

    def _save_checkpoint(last: str, processed: int) -> None:
        failed_queue = (
            retry_unresolved.copy()
            if retry_mode
            else list(dict.fromkeys(failed_symbols))
        )
        ProgressTracker.save(
            task="retry" if retry_mode else "update_bars",
            last_symbol=last,
            processed=processed,
            total=total,
            failed_queue=failed_queue,
        )

    # 初始化 AkShare 稳定性监控
    monitor = AkShareMonitor()
    expected_latest = get_expected_latest_trading_day()

    # ── 停牌预检：停牌股直接跳过抓取，不计失败、不触发重试 ──
    suspended_symbols: set[str] = set()
    try:
        suspended_symbols = _detect_suspended_symbols(db, _normalize_trade_date(expected_latest))
        if suspended_symbols:
            logger.info(
                f"⏸️ 停牌预检：{len(suspended_symbols)} 只停牌股本次跳过抓取: {sorted(suspended_symbols)}"
            )
    except Exception as e:
        logger.warning(f"⚠️ 停牌预检失败（不影响主流程）: {e}")

    # ── 熔断检查（带哨兵验证）──
    canary_probes_used = 0

    def _check_abort() -> tuple[bool, str]:
        """熔断检查：连续失败触发时先做哨兵验证，避免个股数据问题误熔断。"""
        nonlocal canary_probes_used
        should_abort, abort_msg = monitor.should_abort()
        if (
            should_abort
            and monitor.current_run_consecutive_failures >= 3
            and canary_probes_used < MAX_CANARY_PROBES
        ):
            canary_probes_used += 1
            if _canary_probe(loader):
                logger.warning(
                    f"⚠️ 连续 {monitor.current_run_consecutive_failures} 次失败触发熔断条件，"
                    f"但哨兵 {CANARY_SYMBOL} 拉取正常 → 判定为个股数据问题，继续运行"
                    f"（哨兵验证 {canary_probes_used}/{MAX_CANARY_PROBES}）"
                )
                # 只化解连续失败规则：不经 record() 写伪造成功，
                # 避免污染持久化监控历史与限流节奏；窗口成功率规则不受哨兵豁免
                monitor.current_run_consecutive_failures = 0
                should_abort, abort_msg = monitor.should_abort()
        return should_abort, abort_msg

    # ── 自选股全量拉取初始化 ──
    watchlist_symbols = set()
    backfilled_symbols = set()
    backfill_file = SHARED_DATA_DIR / "watchlist_backfilled.txt"
    try:
        watchlist_df = db.watchlist_get_all(status="tracking")
        if not watchlist_df.empty:
            watchlist_symbols = set(watchlist_df["ts_code"].tolist())
        if backfill_file.exists():
            with open(backfill_file, encoding="utf-8") as f:
                backfilled_symbols = {line.strip() for line in f if line.strip()}
    except Exception as e:
        logger.warning(f"⚠️ 初始化自选股拉取逻辑失败: {e}")

    for batch_idx in range(0, remaining_total, BATCH_SIZE):
        attempts_before_batch = monitor.current_run_attempts
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
                        expected_latest_date=expected_latest,
                        suspended_symbols=suspended_symbols,
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
                    _mark_retry_resolved(symbol, result)

                    if result != "skipped":
                        monitor.record(result == "success", symbol)

                    # 并行模式下不在循环内设 last_symbol（as_completed 顺序≠批次顺序）
                    # 批次结束统一设 batch[-1]

            # 并行批次结束后：使用批次原始顺序的最后一只股票作为断点
            last_symbol = batch[-1]
            _save_checkpoint(last_symbol, processed_count)
            logger.info(
                f"  📥 批次完成: {processed_count}/{total} "
                f"(成功: {success_count}, 跳过: {skipped_count}, 失败: {failed_count})"
            )

            # 并行批次结束后检查是否需要中止
            should_abort, abort_msg = _check_abort()
            if should_abort:
                logger.warning(f"⛔ {abort_msg}")
                monitor.flush()
                _save_checkpoint(last_symbol, processed_count)
                result = _bars_result(
                    success=success_count,
                    failed=failed_count,
                    skipped=skipped_count,
                    total=total,
                    attempted=success_count + failed_count + skipped_count,
                    failed_symbols=failed_symbols,
                )
                result["status"] = "aborted"
                result["error"] = abort_msg
                return result
        else:
            for symbol in batch:
                result = _update_single_bar(
                    db,
                    loader,
                    symbol,
                    watchlist_symbols=watchlist_symbols,
                    backfilled_symbols=backfilled_symbols,
                    backfill_file=backfill_file,
                    expected_latest_date=expected_latest,
                    suspended_symbols=suspended_symbols,
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
                _mark_retry_resolved(symbol, result)

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
                    _save_checkpoint(last_symbol, current_processed)

                # 动态调整限流：成功率低时增加休息时间
                if result != "skipped":
                    multiplier = monitor.get_recommended_sleep_multiplier()
                    sleep_time = random.uniform(PER_STOCK_MIN_SLEEP, PER_STOCK_MAX_SLEEP) * multiplier
                    time.sleep(sleep_time)

                # 检查是否需要中止（AkShare 极度不稳定时）
                should_abort, abort_msg = _check_abort()
                if should_abort:
                    logger.warning(f"⛔ {abort_msg}")
                    _save_checkpoint(last_symbol, current_processed)
                    result = _bars_result(
                        success=success_count,
                        failed=failed_count,
                        skipped=skipped_count,
                        total=total,
                        attempted=success_count + failed_count + skipped_count,
                        failed_symbols=failed_symbols,
                    )
                    result["status"] = "aborted"
                    result["error"] = abort_msg
                    return result

        # 每批次结束也刷新进度
        current_processed = processed_count
        _save_checkpoint(last_symbol, current_processed)

        # 批次结束时汇报监控状态
        monitor.log_status()

        if batch_idx + BATCH_SIZE < remaining_total:
            if monitor.current_run_attempts == attempts_before_batch:
                # 本批次全部跳过（零网络请求），无需限流休息
                logger.debug("  本批次无网络请求，跳过批次间休息")
            else:
                # 动态调整批次休息：成功率低时增加休息
                multiplier = monitor.get_recommended_sleep_multiplier()
                batch_sleep = BATCH_SLEEP * multiplier
                logger.info(f"⏳ 批次间休息 {batch_sleep:.1f}s... (倍率 {multiplier}x)")
                time.sleep(batch_sleep)

    # 处理完成：去重并保存失败队列
    unique_failed = (
        retry_unresolved.copy()
        if retry_mode
        else list(dict.fromkeys(failed_symbols))
    )
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
    logger.info(f"  ❌ 失败: {len(unique_failed)} 只")
    logger.info("=" * 60)

    monitor.flush()
    return _bars_result(
        success=success_count,
        failed=len(unique_failed),
        skipped=skipped_count,
        total=total,
        # attempted 只计本轮实际检查数；failed 含续传继承的未解决失败，
        # 两者允许不相等（见 bars-resume-result-contract 设计）
        attempted=success_count + failed_count + skipped_count,
        failed_symbols=unique_failed,
    )


def _update_single_bar(
    db: DatabaseInterface,
    loader: DataLoaderInterface,
    symbol: str,
    watchlist_symbols: set[str] | None = None,
    backfilled_symbols: set[str] | None = None,
    backfill_file: Path | None = None,
    db_lock: threading.Lock | None = None,
    expected_latest_date: str | None = None,
    suspended_symbols: set[str] | None = None,
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
                    watchlist_df = db.watchlist_get_all(status="tracking")
            else:
                watchlist_df = db.watchlist_get_all(status="tracking")
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
    expected_latest = _normalize_trade_date(expected_latest_date or get_expected_latest_trading_day())

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
            # 轻量查询：先检查 MAX(trade_date)，避免全表扫描（约 5500 次全表读 → 1 次聚合查询）
            latest_date = _normalize_trade_date(db.get_latest_bar_date(symbol))
            if latest_date and expected_latest and latest_date >= expected_latest:
                return "skipped"
            # 停牌预检命中：源端不会有新数据，直接跳过避免徒劳重试与失败计数
            #（例外：上方自选股首次全量回填分支不受此限制——停牌股的历史 K 线依然可拉）
            if suspended_symbols and symbol[:6] in suspended_symbols:
                logger.info(f"  ⏸️ {symbol} 停牌中，跳过抓取")
                return "skipped"
            # 非最新时才读取全量数据做增量更新
            if db_lock:
                with db_lock:
                    existing = db.get_daily_bars(symbol)
            else:
                existing = db.get_daily_bars(symbol)
            if not existing.empty:
                df_bars = loader.incremental_update(symbol, existing)
                # 优化点：如果行数没变，说明已经是最新，无需重复保存，直接返回 skipped
                if len(df_bars) == len(existing):
                    if latest_date and expected_latest and latest_date < expected_latest:
                        logger.warning(
                            f"  ❌ {symbol}: 增量更新未取得最新交易日数据 "
                            f"({latest_date} < {expected_latest})"
                        )
                        return "failed"
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
