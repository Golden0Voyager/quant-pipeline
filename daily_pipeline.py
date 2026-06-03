"""
SmartMoney 日常数据管道（解耦版 + 断点续传）
─────────────────────────────────────────────
职责：自动化每日数据更新、指标计算、质量监控
      支持断点续传：中断后重新运行自动从断点继续

架构：
  daily_pipeline ──▶ pipeline_interface (抽象接口)
                          │
                          └── pipeline_providers (适配层)
                                    │
                                    └── smartmoney_hunter (具体实现)

用法：
    python daily_pipeline.py --task update_bars
    python daily_pipeline.py --task update_bars --resume    # 从断点续传
    python daily_pipeline.py --task health_check
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import random
import sqlite3
import sys
import socket
import tempfile
import time
from datetime import datetime, timedelta

# 设置全局套接字超时，防止网络悬挂/DNS阻塞导致 API 请求无限期挂起
socket.setdefaulttimeout(15)
from pathlib import Path
from typing import List, Tuple, Optional, Dict, Any

# 将 ~/Code 加入 Python 路径（使 pipeline 能 import smartmoney_hunter）
_CODE_DIR = os.path.expanduser("~/Code")
if _CODE_DIR not in sys.path:
    sys.path.insert(0, _CODE_DIR)

from interface import (
    ProviderFactory,
    DatabaseInterface,
    DataLoaderInterface,
    IndicatorEngineInterface,
)

# 新数据维度直接调用 akshare（中台批量抓取）
try:
    import akshare as ak
except ImportError:
    ak = None  # type: ignore[assignment]

# ---------------------------------------------------------------------------
# 配置：数据库路径（环境变量优先）
# ---------------------------------------------------------------------------
DEFAULT_DB_PATH = os.path.expanduser("~/Code/data/quant_data/quant_core.db")
DB_PATH = os.getenv("QUANT_DB_PATH", DEFAULT_DB_PATH)
SHARED_DATA_DIR = Path(DB_PATH).parent

# ---------------------------------------------------------------------------
# 日志配置（统一存放到数据目录下）
# ---------------------------------------------------------------------------
LOG_DIR = SHARED_DATA_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / f"smartmoney_{datetime.now().strftime('%Y%m%d')}.log"

# 强制配置根 logger：清除已有 handlers，防止其他模块 basicConfig 干扰
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
for h in root_logger.handlers[:]:
    root_logger.removeHandler(h)

_file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
_file_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s"))
root_logger.addHandler(_file_handler)

if sys.stdout.isatty():
    _console_handler = logging.StreamHandler(sys.stdout)
    _console_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s"))
    root_logger.addHandler(_console_handler)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 配置常量
# ---------------------------------------------------------------------------
BATCH_SIZE = 100          # 每批处理的股票数（从200降到100，降低单批压力）
BATCH_SLEEP = 5.0         # 批次间休息（秒），AkShare 需防限流
PER_STOCK_MIN_SLEEP = 0.3  # 单只股票最小间隔（秒）
PER_STOCK_MAX_SLEEP = 0.8  # 单只股票最大间隔（秒）
MAX_RETRY = 3             # 单只股票失败重试次数
RETRY_DELAY = 5.0         # 重试间隔（秒）

# 极致稳定模式支持（通过环境变量 ULTRA_SAFE=1 触发）
if os.getenv("ULTRA_SAFE") == "1":
    BATCH_SIZE = 30             # 批次规模降为 30（显著分摊单次爆破压力）
    BATCH_SLEEP = 12.0          # 批次休息翻倍以上，进一步分摊流量压力
    PER_STOCK_MIN_SLEEP = 0.8   # 单只股票间隔下限增加，拉长频次
    PER_STOCK_MAX_SLEEP = 2.0   # 单只股票间隔上限增加，提升随机指纹隐蔽性
    MAX_RETRY = 2               # 在极致稳定模式下，将单只股票重试次数限制为 2 次，防多层重试叠加
    RETRY_DELAY = 3.0           # 重试等待时间缩短为 3s，提高降级流转速率

PROGRESS_FLUSH_INTERVAL = 10  # 每处理 N 只股票刷新一次进度文件

# ---------------------------------------------------------------------------
# AkShare 稳定性监控
# ---------------------------------------------------------------------------
class AkShareMonitor:
    """AkShare 稳定性监控器：根据最近请求成功率动态调整限流策略。"""

    FILE = SHARED_DATA_DIR / "akshare_monitor.json"
    WINDOW_SIZE = 30  # 滑动窗口大小

    def __init__(self):
        self.records = self._load()
        self.current_run_attempts = 0
        self.current_run_consecutive_failures = 0

    def _load(self) -> List[Dict]:
        if not self.FILE.exists():
            return []
        try:
            with open(self.FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return []

    def _save(self):
        try:
            tmp = self.FILE.with_suffix(".tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self.records[-self.WINDOW_SIZE * 2 :], f, ensure_ascii=False)
            tmp.replace(self.FILE)
        except Exception as e:
            logger.warning(f"⚠️ 无法写入 AkShare 监控文件: {e}")

    def record(self, success: bool, symbol: str):
        self.current_run_attempts += 1
        if success:
            self.current_run_consecutive_failures = 0
        else:
            self.current_run_consecutive_failures += 1

        self.records.append(
            {
                "timestamp": datetime.now().isoformat(),
                "success": success,
                "symbol": symbol,
            }
        )
        self._save()

    def get_success_rate(self, window: int = None) -> float:
        if not self.records:
            return 1.0
        window = window or self.WINDOW_SIZE
        recent = self.records[-window:]
        if not recent:
            return 1.0
        success_count = sum(1 for r in recent if r["success"])
        return success_count / len(recent)

    def get_recommended_sleep_multiplier(self) -> float:
        rate = self.get_success_rate()
        if rate >= 0.8:
            return 1.0
        elif rate >= 0.5:
            return 1.5
        elif rate >= 0.3:
            return 2.0
        else:
            return 3.0

    def should_abort(self) -> Tuple[bool, str]:
        # 1. 刚启动还没有进行过真实请求，决不中断
        if self.current_run_attempts == 0:
            return False, ""

        # 2. 连续失败快速中止（仅针对当前运行累计，避免历史数据导致启动即中止）
        if self.current_run_consecutive_failures >= 3:
            return (
                True,
                f"AkShare 在本次运行中连续 {self.current_run_consecutive_failures} 次请求失败，网络可能彻底不可用或受到强力限流阻断，已自动中止。",
            )

        # 3. 整体成功率低中止（需要当前运行至少尝试过 5 次，给网络恢复或新运行一个机会）
        if self.current_run_attempts >= 5:
            rate = self.get_success_rate(window=20)
            if rate < 0.2 and len(self.records) >= 20:
                return (
                    True,
                    f"AkShare 最近 20 次请求成功率仅 {rate*100:.0f}%，"
                    f"建议推迟到晚上 20:00+ 再跑",
                )
        return False, ""

    def log_status(self):
        rate = self.get_success_rate()
        multiplier = self.get_recommended_sleep_multiplier()
        if rate < 1.0:
            logger.info(
                f"📊 AkShare 最近 {min(len(self.records), self.WINDOW_SIZE)} 次成功率: "
                f"{rate*100:.0f}%，sleep 倍率: {multiplier}x"
            )


# ===========================================================================
# 断点续传：进度追踪器
# ===========================================================================

class ProgressTracker:
    """
    断点续传进度追踪器。

    使用原子写入（tempfile + rename）防止写一半断电导致进度文件损坏。
    记录内容：最后处理的 symbol、已处理数量、失败队列、启动时间。
    """

    FILE = SHARED_DATA_DIR / "progress.json"

    @classmethod
    def save(
        cls,
        task: str,
        last_symbol: str,
        processed: int,
        total: int,
        failed_queue: List[str],
    ) -> None:
        """原子写入进度文件。"""
        data = {
            "task": task,
            "date": datetime.now().strftime("%Y-%m-%d"),
            "start_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "last_symbol": last_symbol,
            "processed": processed,
            "total": total,
            "failed_queue": failed_queue,
        }
        # 原子写入：先写临时文件，再 rename
        tmp = cls.FILE.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        tmp.replace(cls.FILE)

    @classmethod
    def load(cls) -> Optional[Dict[str, Any]]:
        """读取进度文件。"""
        if not cls.FILE.exists():
            return None
        try:
            with open(cls.FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            logger.warning("⚠️  进度文件损坏，将从头开始")
            return None

    @classmethod
    def clear(cls) -> None:
        """清除进度文件（任务成功完成后调用）。"""
        if cls.FILE.exists():
            cls.FILE.unlink()
            logger.info("🗑️  进度文件已清除")
        # 同时清理 retry_queue.txt（如果存在且为空则删除）
        retry_file = SHARED_DATA_DIR / "retry_queue.txt"
        if retry_file.exists():
            with open(retry_file, "r", encoding="utf-8") as f:
                content = f.read().strip()
            if not content:
                retry_file.unlink()
                logger.info("🗑️  retry_queue.txt 已清理")

    @classmethod
    def find_resume_index(cls, stock_codes: List[str], last_symbol: str) -> int:
        """
        找到断点位置。返回应该从哪个索引开始处理。
        如果 last_symbol 不在列表中，返回 0（从头开始）。
        """
        try:
            idx = stock_codes.index(last_symbol)
            return idx + 1  # 从下一个开始
        except ValueError:
            logger.warning(
                f"⚠️  断点 symbol '{last_symbol}' 不在今日股票列表中，"
                "可能因新股上市/退市导致列表变化，将从头开始"
            )
            return 0


# ===========================================================================
# 辅助函数
# ===========================================================================

def _is_trading_day() -> bool:
    """判断今天是否为 A 股交易日（简化版，排除周末）。"""
    today = datetime.now()
    return today.weekday() < 5  # 周一到周五


def _should_update() -> bool:
    """判断是否需要更新（收盘后且是交易日）。"""
    now = datetime.now()
    if now.weekday() >= 5:
        logger.info("今天是周末，跳过更新")
        return False
    return True


def _sleep_with_progress(seconds: float, label: str = "等待"):
    """带进度显示的 sleep。"""
    for i in range(int(seconds)):
        print(f"\r  {label}: {i + 1}/{int(seconds)}s", end="", flush=True)
        time.sleep(1)
    print()


# ===========================================================================
# 任务 1: 更新日线数据（增量 + 断点续传）
# ===========================================================================

def update_bars(
    db: DatabaseInterface,
    loader: DataLoaderInterface,
    limit: int = None,
    resume: bool = False,
) -> dict:
    """分批增量更新所有股票的日线数据，支持断点续传。"""
    logger.info("=" * 60)
    logger.info("📈 任务: 更新日线数据")
    logger.info("=" * 60)

    stocks = db.get_stock_list()
    if stocks.empty:
        logger.error("❌ 股票列表为空")
        return {"success": 0, "failed": 0, "skipped": 0, "total": 0}

    stock_codes = stocks["code"].tolist()
    if limit:
        stock_codes = stock_codes[:limit]
        logger.info(f"⚠️  测试模式：只更新前 {limit} 只")

    total = len(stock_codes)
    logger.info(f"📊 共 {total} 只股票待更新")

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

    success_count = progress.get("processed", 0) if progress else 0
    failed_count = 0
    skipped_count = 0
    failed_symbols: List[str] = progress.get("failed_queue", []) if progress else []
    last_symbol = ""

    # ── 加载 retry_queue.txt 中之前失败的股票 ──
    retry_file = SHARED_DATA_DIR / "retry_queue.txt"
    if retry_file.exists():
        with open(retry_file, "r", encoding="utf-8") as f:
            retry_symbols = [line.strip() for line in f if line.strip()]
        if retry_symbols:
            # 去重合并到 failed_symbols
            new_retries = [s for s in retry_symbols if s not in failed_symbols]
            if new_retries:
                logger.info(f"🔄 从 retry_queue.txt 加载 {len(new_retries)} 只历史失败股票")
                failed_symbols.extend(new_retries)
        # 清空 retry_queue.txt，避免重复累积
        retry_file.unlink()

    # 计算剩余需要处理的股票
    remaining_codes = stock_codes[start_idx:]
    remaining_total = len(remaining_codes)

    # 初始化 AkShare 稳定性监控
    monitor = AkShareMonitor()

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

        for symbol in batch:
            result = _update_single_bar(db, loader, symbol)
            if result == "success":
                success_count += 1
            elif result == "skipped":
                skipped_count += 1
            else:
                failed_count += 1
                if symbol not in failed_symbols:
                    failed_symbols.append(symbol)

            # 记录 AkShare 稳定性（仅对真实执行过网络更新的股票进行记录，跳过的股票不影响统计）
            if result != "skipped":
                monitor.record(result == "success", symbol)

            last_symbol = symbol

            # 如果触发了网络抓取（非 skipped），增加 0.1s 到 0.4s 的随机抖动延迟，平滑并发请求，避免被封锁
            if result != "skipped":
                time.sleep(random.uniform(0.1, 0.4))

            # 每 N 只股票刷新一次进度文件
            current_processed = success_count + skipped_count + failed_count
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

            # 动态调整限流：成功率低时增加休息时间（只有真的抓取了新数据或失败时才休息，skipped不休息）
            if result != "skipped":
                multiplier = monitor.get_recommended_sleep_multiplier()
                sleep_time = random.uniform(PER_STOCK_MIN_SLEEP, PER_STOCK_MAX_SLEEP) * multiplier
                time.sleep(sleep_time)

            # 检查是否需要中止（AkShare 极度不稳定时）
            should_abort, abort_msg = monitor.should_abort()
            if should_abort:
                logger.warning(f"⛔ {abort_msg}")
                # 保存进度并退出
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
        current_processed = success_count + skipped_count + failed_count
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

    # 处理完成：去重并保存失败队列，清除进度文件
    unique_failed = list(dict.fromkeys(failed_symbols))  # 保持顺序去重
    if unique_failed:
        retry_file = SHARED_DATA_DIR / "retry_queue.txt"
        with open(retry_file, "w", encoding="utf-8") as f:
            for s in unique_failed:
                f.write(f"{s}\n")
        logger.warning(f"⚠️  {len(unique_failed)} 只股票写入 retry 队列: {retry_file}")
    else:
        # 如果没有失败，确保 retry_queue.txt 不存在
        retry_file = SHARED_DATA_DIR / "retry_queue.txt"
        if retry_file.exists():
            retry_file.unlink()

    # 成功完成，清除进度文件
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
    db: DatabaseInterface, loader: DataLoaderInterface, symbol: str
) -> str:
    """更新单只股票的日线数据，带重试。

    数据质量规则：
    - 优先使用 AkShare 数据
    - 如果新增/全部数据来自 yfinance，跳过保存（yfinance 仅作为运行时临时 fallback，
      不应写入 quant_core.db 这个黄金数据源）
    """
    for attempt in range(MAX_RETRY):
        try:
            existing = db.get_daily_bars(symbol)
            if not existing.empty:
                df_bars = loader.incremental_update(symbol, existing)
                # 优化点：如果行数没变，说明已经是最新，无需重复保存，直接返回 skipped
                if len(df_bars) == len(existing):
                    return "skipped"
            else:
                df_bars = loader.get_daily_bars(symbol)

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


# ===========================================================================
# 任务 2: 重新计算技术指标（修复 NULL）
# ===========================================================================

def update_indicators(
    db: DatabaseInterface, engine: IndicatorEngineInterface, symbols_to_update: List[str] = None
) -> dict:
    """为指定或所有需要更新的技术指标重新计算。"""
    logger.info("\n" + "=" * 60)
    logger.info("📊 任务: 计算技术指标")
    logger.info("=" * 60)

    conn = sqlite3.connect(str(db.db_path))
    cursor = conn.cursor()
    
    if symbols_to_update is not None:
        symbols = symbols_to_update
        logger.info(f"🎯 指定模式：计算 {len(symbols)} 只股票的指标")
    else:
        # 智能探测模式：只计算未计算过，或者有新行情数据的股票
        logger.info("🔍 智能探测需要更新指标的股票...")
        cursor.execute("""
            SELECT d.ts_code
            FROM (
                SELECT ts_code, MAX(trade_date) as max_bar_date
                FROM daily_bars
                GROUP BY ts_code
            ) d
            LEFT JOIN (
                SELECT ts_code, MAX(trade_date) as max_ind_date
                FROM indicators
                GROUP BY ts_code
            ) i ON d.ts_code = i.ts_code
            WHERE i.max_ind_date IS NULL OR d.max_bar_date > i.max_ind_date
            ORDER BY d.ts_code
        """)
        symbols = [row[0] for row in cursor.fetchall()]
        logger.info(f"💡 探测完成：共有 {len(symbols)} 只股票需要更新/计算指标")

    conn.close()

    total = len(symbols)
    if total == 0:
        logger.info("✅ 所有股票的指标均已是最新，无需计算")
        return {"success": 0, "failed": 0, "insufficient": 0, "total": 0}

    success_count = 0
    failed_count = 0
    insufficient_count = 0

    for i, symbol in enumerate(symbols, 1):
        if i % 100 == 0 or i == total:
            logger.info(f"  进度: {i}/{total} ({100 * i // total}%)")

        try:
            df = db.get_daily_bars(symbol)
            if df.empty or len(df) < 60:
                insufficient_count += 1
                continue

            if "trade_date" in df.columns and "date" not in df.columns:
                df = df.rename(columns={"trade_date": "date"})

            df_ind = engine.calculate_all_indicators(df)
            db.save_indicators(symbol, df_ind)
            success_count += 1

        except Exception as e:
            logger.warning(f"  ❌ {symbol} 指标计算失败: {e}")
            failed_count += 1

    logger.info("\n" + "=" * 60)
    logger.info("📊 技术指标计算完成")
    logger.info(f"  ✅ 成功: {success_count} 只")
    logger.info(f"  ⚠️  数据不足(<60天): {insufficient_count} 只")
    logger.info(f"  ❌ 失败: {failed_count} 只")
    logger.info("=" * 60)

    return {
        "success": success_count,
        "failed": failed_count,
        "insufficient": insufficient_count,
        "total": total,
    }


# ===========================================================================
# 任务 3: 批量获取全市场估值
# ===========================================================================

def update_fundamentals(
    db: DatabaseInterface, loader: DataLoaderInterface
) -> dict:
    """批量获取全市场估值数据并保存到 fundamentals 表。"""
    logger.info("\n" + "=" * 60)
    logger.info("📊 任务: 批量获取估值数据")
    logger.info("=" * 60)

    today = datetime.now().strftime("%Y-%m-%d")

    try:
        df = loader.get_market_valuation()
        if df.empty:
            logger.warning("⚠️  未获取到估值数据")
            return {"saved": 0, "total": 0}

        saved = 0
        for _, row in df.iterrows():
            try:
                code = str(row.get("code", "")).strip()
                if not code:
                    continue

                data = {
                    "trade_date": today,
                    "pe_ttm": row.get("pe_ttm"),
                    "pb": row.get("pb"),
                    "ps_ttm": row.get("ps_ttm"),
                    "dividend_yield": None,
                    "roe": None,
                    "roa": None,
                    "gross_margin": None,
                    "net_margin": None,
                    "debt_ratio": None,
                    "revenue_growth": None,
                    "profit_growth": None,
                    "eps_growth": None,
                    "peg": row.get("peg"),
                    "market_cap": row.get("market_cap"),
                }
                db.save_fundamentals(code, data)
                saved += 1
            except Exception as e:
                logger.debug(f"  保存估值失败: {e}")
                continue

        logger.info(f"✅ 估值数据保存完成: {saved}/{len(df)} 只")
        return {"saved": saved, "total": len(df)}

    except Exception as e:
        logger.error(f"❌ 估值数据获取失败: {e}")
        return {"saved": 0, "total": 0, "error": str(e)}


# ===========================================================================
# 任务 4: 批量获取全市场资金流向
# ===========================================================================

def update_fund_flow(db: DatabaseInterface, loader: DataLoaderInterface) -> dict:
    """批量获取全市场资金流向并保存。"""
    logger.info("\n" + "=" * 60)
    logger.info("💰 任务: 批量获取资金流向")
    logger.info("=" * 60)

    today = datetime.now().strftime("%Y-%m-%d")

    try:
        df = loader.get_market_fund_flow()
        if df.empty:
            logger.warning("⚠️  未获取到资金流向数据")
            return {"saved": 0, "total": 0}

        saved = 0
        for _, row in df.iterrows():
            try:
                code = str(row.get("code", "")).strip()
                if not code:
                    continue

                data = {
                    "date": today,
                    "main_net_inflow": row.get("main_net_inflow"),
                    "main_net_inflow_pct": row.get("main_net_inflow_pct"),
                    "super_large_net_inflow": row.get("super_large_net_inflow"),
                    "super_large_net_inflow_pct": row.get("super_large_net_inflow_pct"),
                    "large_net_inflow": row.get("large_net_inflow"),
                    "large_net_inflow_pct": row.get("large_net_inflow_pct"),
                    "simulated": False,
                }
                db.save_fund_flow(code, data)
                saved += 1
            except Exception as e:
                logger.debug(f"  保存资金流失败: {e}")
                continue

        logger.info(f"✅ 资金流向保存完成: {saved}/{len(df)} 只")
        return {"saved": saved, "total": len(df)}

    except Exception as e:
        logger.error(f"❌ 资金流向获取失败: {e}")
        return {"saved": 0, "total": 0, "error": str(e)}


# ===========================================================================
# 任务 5: 批量获取融资融券数据
# ===========================================================================

def update_margin_trading(db: DatabaseInterface) -> dict:
    """批量获取昨日全市场融资融券数据并保存。"""
    logger.info("\n" + "=" * 60)
    logger.info("📈 任务: 批量获取融资融券")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "total": 0, "error": "akshare not installed"}

    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    saved = 0
    total = 0

    for exchange, fetcher in [("sh", ak.stock_margin_detail_sse), ("sz", ak.stock_margin_detail_szse)]:
        try:
            df = fetcher(date=yesterday)
            if df is None or df.empty:
                logger.warning(f"⚠️  {exchange.upper()} 融资融券无数据")
                continue
            total += len(df)
            for _, row in df.iterrows():
                try:
                    code = str(row.get("标的证券代码" if exchange == "sh" else "证券代码", "")).strip()
                    if not code:
                        continue
                    data = {
                        "trade_date": yesterday,
                        "margin_balance": row.get("融资余额" if exchange == "sh" else "融资余额"),
                        "margin_buy": row.get("融资买入额" if exchange == "sh" else "融资买入额"),
                        "margin_repay": row.get("融资偿还额" if exchange == "sh" else None),
                        "short_balance": row.get("融券余量" if exchange == "sh" else "融券余量"),
                        "short_sell": row.get("融券卖出量" if exchange == "sh" else "融券卖出量"),
                        "short_repay": row.get("融券偿还量" if exchange == "sh" else None),
                        "total_balance": row.get("融资融券余额"),
                        "data_source": "akshare",
                    }
                    db.save_margin_trading(code, data)
                    saved += 1
                except Exception:
                    continue
        except Exception as e:
            logger.error(f"❌ {exchange.upper()} 融资融券获取失败: {e}")

    logger.info(f"✅ 融资融券保存完成: {saved}/{total}")
    return {"saved": saved, "total": total}


# ===========================================================================
# 任务 6: 批量获取龙虎榜数据
# ===========================================================================

def update_dragon_tiger(db: DatabaseInterface) -> dict:
    """批量获取昨日龙虎榜数据并保存。"""
    logger.info("\n" + "=" * 60)
    logger.info("🐉 任务: 批量获取龙虎榜")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "total": 0, "error": "akshare not installed"}

    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    try:
        df = ak.stock_lhb_detail_em(start_date=yesterday, end_date=yesterday)
        if df is None or df.empty:
            logger.warning("⚠️  龙虎榜无数据")
            return {"saved": 0, "total": 0}

        saved = 0
        for _, row in df.iterrows():
            try:
                code = str(row.get("代码", "")).strip()
                if not code:
                    continue
                data = {
                    "trade_date": yesterday,
                    "close_price": row.get("收盘价"),
                    "pct_change": row.get("涨跌幅"),
                    "net_buy_amount": row.get("龙虎榜净买额"),
                    "buy_amount": row.get("龙虎榜买入额"),
                    "sell_amount": row.get("龙虎榜卖出额"),
                    "turnover_rate": row.get("换手率"),
                    "market_cap": row.get("流通市值"),
                    "reason": row.get("上榜原因", ""),
                    "data_source": "akshare",
                }
                db.save_dragon_tiger(code, data)
                saved += 1
            except Exception:
                continue

        logger.info(f"✅ 龙虎榜保存完成: {saved}/{len(df)}")
        return {"saved": saved, "total": len(df)}
    except Exception as e:
        logger.error(f"❌ 龙虎榜获取失败: {e}")
        return {"saved": 0, "total": 0, "error": str(e)}


# ===========================================================================
# 任务 7: 批量获取大宗交易数据
# ===========================================================================

def update_block_trade(db: DatabaseInterface) -> dict:
    """批量获取昨日大宗交易数据并保存。"""
    logger.info("\n" + "=" * 60)
    logger.info("📦 任务: 批量获取大宗交易")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "total": 0, "error": "akshare not installed"}

    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    try:
        df = ak.stock_dzjy_mrmx(symbol="A股", start_date=yesterday, end_date=yesterday)
        if df is None or df.empty:
            logger.warning("⚠️  大宗交易无数据")
            return {"saved": 0, "total": 0}

        saved = 0
        for _, row in df.iterrows():
            try:
                code = str(row.get("证券代码", "")).strip()
                if not code:
                    continue
                data = {
                    "trade_date": yesterday,
                    "deal_price": row.get("成交价"),
                    "close_price": row.get("收盘价"),
                    "discount_rate": row.get("折溢率"),
                    "volume": row.get("成交量"),
                    "amount": row.get("成交额"),
                    "buyer_branch": row.get("买方营业部", ""),
                    "seller_branch": row.get("卖方营业部", ""),
                    "data_source": "akshare",
                }
                db.save_block_trade(code, data)
                saved += 1
            except Exception:
                continue

        logger.info(f"✅ 大宗交易保存完成: {saved}/{len(df)}")
        return {"saved": saved, "total": len(df)}
    except Exception as e:
        logger.error(f"❌ 大宗交易获取失败: {e}")
        return {"saved": 0, "total": 0, "error": str(e)}


# ===========================================================================
# 任务 8: 批量获取板块资金流向
# ===========================================================================

def update_sector_fund_flow(db: DatabaseInterface) -> dict:
    """批量获取板块资金流向并保存。"""
    logger.info("\n" + "=" * 60)
    logger.info("🏭 任务: 批量获取板块资金流向")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "total": 0, "error": "akshare not installed"}

    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    try:
        df = ak.stock_sector_fund_flow_hist(symbol="行业资金流")
        if df is None or df.empty:
            logger.warning("⚠️  板块资金流向无数据")
            return {"saved": 0, "total": 0}

        saved = 0
        for _, row in df.iterrows():
            try:
                sector = str(row.get("行业", "")).strip()
                if not sector:
                    continue
                data = {
                    "trade_date": yesterday,
                    "main_net_inflow": row.get("主力净流入-净额"),
                    "main_net_inflow_pct": row.get("主力净流入-净占比"),
                    "super_large_net_inflow": row.get("超大单净流入-净额"),
                    "large_net_inflow": row.get("大单净流入-净额"),
                    "medium_net_inflow": row.get("中单净流入-净额"),
                    "small_net_inflow": row.get("小单净流入-净额"),
                    "data_source": "akshare",
                }
                db.save_sector_fund_flow(sector, data)
                saved += 1
            except Exception:
                continue

        logger.info(f"✅ 板块资金流向保存完成: {saved}/{len(df)}")
        return {"saved": saved, "total": len(df)}
    except Exception as e:
        logger.error(f"❌ 板块资金流向获取失败: {e}")
        return {"saved": 0, "total": 0, "error": str(e)}


# ===========================================================================
# 任务 9: 批量获取股东户数（季度）
# ===========================================================================

def update_shareholder_count(db: DatabaseInterface) -> dict:
    """批量获取最新季度股东户数并保存。"""
    logger.info("\n" + "=" * 60)
    logger.info("👥 任务: 批量获取股东户数")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "total": 0, "error": "akshare not installed"}

    # 计算最近的报告期（0331, 0630, 0930, 1231）
    now = datetime.now()
    year = now.year
    month = now.month
    if month >= 11:
        period = f"{year}0930"
    elif month >= 8:
        period = f"{year}0630"
    elif month >= 5:
        period = f"{year}0331"
    else:
        period = f"{year - 1}0930"

    try:
        df = ak.stock_hold_num_cninfo(date=period)
        if df is None or df.empty:
            logger.warning(f"⚠️  股东户数无数据 ({period})")
            return {"saved": 0, "total": 0}

        saved = 0
        for _, row in df.iterrows():
            try:
                code = str(row.get("证券代码", "")).strip()
                if not code:
                    continue
                data = {
                    "report_date": period,
                    "holder_count": row.get("本期股东人数"),
                    "holder_count_change_pct": row.get("股东人数增幅"),
                    "avg_shares_per_holder": row.get("本期人均持股数量"),
                    "data_source": "akshare",
                }
                db.save_shareholder_count(code, data)
                saved += 1
            except Exception:
                continue

        logger.info(f"✅ 股东户数保存完成: {saved}/{len(df)} ({period})")
        return {"saved": saved, "total": len(df)}
    except Exception as e:
        logger.error(f"❌ 股东户数获取失败: {e}")
        return {"saved": 0, "total": 0, "error": str(e)}


# ===========================================================================
# 任务 10: 重试失败队列
# ===========================================================================

def retry_failed(
    db: DatabaseInterface, loader: DataLoaderInterface
) -> dict:
    """重试之前失败的股票。"""
    retry_file = SHARED_DATA_DIR / "retry_queue.txt"
    if not retry_file.exists():
        logger.info("ℹ️  retry 队列为空")
        return {"success": 0, "failed": 0, "total": 0}

    with open(retry_file, "r", encoding="utf-8") as f:
        symbols = list(set(line.strip() for line in f if line.strip()))

    if not symbols:
        logger.info("ℹ️  retry 队列为空")
        return {"success": 0, "failed": 0, "total": 0}

    logger.info("\n" + "=" * 60)
    logger.info(f"🔄 任务: 重试失败队列 ({len(symbols)} 只)")
    logger.info("=" * 60)

    success = 0
    still_failed = []

    for symbol in symbols:
        result = _update_single_bar(db, loader, symbol)
        if result == "success":
            success += 1
        else:
            still_failed.append(symbol)

    with open(retry_file, "w", encoding="utf-8") as f:
        for s in still_failed:
            f.write(f"{s}\n")

    logger.info(f"✅ 重试完成: {success}/{len(symbols)} 只成功")
    if still_failed:
        logger.warning(f"⚠️  仍有 {len(still_failed)} 只失败，保留在队列中")

    return {"success": success, "failed": len(still_failed), "total": len(symbols)}


# ===========================================================================
# 任务 6: 健康检查
# ===========================================================================

def health_check(db: DatabaseInterface) -> dict:
    """检查数据库健康状态并生成报告。"""
    logger.info("\n" + "=" * 60)
    logger.info("🏥 任务: 数据质量健康检查")
    logger.info("=" * 60)

    today = datetime.now().strftime("%Y-%m-%d")
    issues: List[str] = []
    report_lines = [f"\n📋 SmartMoney 数据健康报告 ({today})\n" + "=" * 50]

    conn = sqlite3.connect(str(db.db_path))
    cursor = conn.cursor()

    tables = [
        ("stock_list", "股票列表"),
        ("daily_bars", "日线数据"),
        ("indicators", "技术指标"),
        ("fund_flow", "资金流向"),
        ("fundamentals", "基本面数据"),
        ("chip_distribution", "筹码分布"),
        ("historical_valuation", "历史估值"),
        ("margin_trading", "融资融券"),
        ("dragon_tiger", "龙虎榜"),
        ("block_trade", "大宗交易"),
        ("sector_fund_flow", "板块资金流"),
        ("shareholder_count", "股东户数"),
    ]

    for table, label in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        report_lines.append(f"  {label:12s}: {count:>8,} 条")

    cursor.execute("SELECT COUNT(DISTINCT ts_code) FROM daily_bars")
    bars_coverage = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM stock_list")
    total_stocks = cursor.fetchone()[0]
    coverage_pct = 100 * bars_coverage / total_stocks if total_stocks else 0
    report_lines.append(f"\n  日线覆盖率: {bars_coverage}/{total_stocks} ({coverage_pct:.1f}%)")
    if coverage_pct < 80:
        issues.append(f"日线覆盖率过低: {coverage_pct:.1f}%")

    cursor.execute("SELECT MAX(trade_date) FROM daily_bars")
    latest_bar = cursor.fetchone()[0]
    cursor.execute("SELECT MAX(trade_date) FROM indicators")
    latest_ind = cursor.fetchone()[0]
    cursor.execute("SELECT MAX(trade_date) FROM fund_flow")
    latest_flow = cursor.fetchone()[0]
    cursor.execute("SELECT MAX(trade_date) FROM fundamentals")
    latest_fund = cursor.fetchone()[0]
    cursor.execute("SELECT MAX(trade_date) FROM margin_trading")
    latest_margin = cursor.fetchone()[0]
    cursor.execute("SELECT MAX(trade_date) FROM dragon_tiger")
    latest_lhb = cursor.fetchone()[0]
    cursor.execute("SELECT MAX(trade_date) FROM block_trade")
    latest_block = cursor.fetchone()[0]
    cursor.execute("SELECT MAX(trade_date) FROM sector_fund_flow")
    latest_sector = cursor.fetchone()[0]
    cursor.execute("SELECT MAX(report_date) FROM shareholder_count")
    latest_holder = cursor.fetchone()[0]

    report_lines.append(f"\n  最新日线日期: {latest_bar}")
    report_lines.append(f"  最新指标日期: {latest_ind}")
    report_lines.append(f"  最新资金流日期: {latest_flow}")
    report_lines.append(f"  最新估值日期: {latest_fund}")
    report_lines.append(f"  最新融资融券日期: {latest_margin}")
    report_lines.append(f"  最新龙虎榜日期: {latest_lhb}")
    report_lines.append(f"  最新大宗交易日期: {latest_block}")
    report_lines.append(f"  最新板块资金流日期: {latest_sector}")
    report_lines.append(f"  最新股东户数报告期: {latest_holder}")

    if latest_bar != today:
        issues.append(f"日线数据未更新到最新: {latest_bar} (今天是 {today})")

    cursor.execute(
        """
        SELECT COUNT(*) FROM indicators
        WHERE macd_hist IS NOT NULL AND rsi6 IS NOT NULL
        """
    )
    valid_ind = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM indicators")
    total_ind = cursor.fetchone()[0]
    if total_ind > 0:
        null_pct = 100 * (1 - valid_ind / total_ind)
        report_lines.append(f"\n  技术指标完整率: {valid_ind}/{total_ind} ({100-null_pct:.1f}%)")
        if null_pct > 20:
            issues.append(f"技术指标空值率过高: {null_pct:.1f}%")
    # ── 碎片空间检查 ──
    try:
        cursor.execute("PRAGMA page_count")
        page_count = cursor.fetchone()[0]
        cursor.execute("PRAGMA page_size")
        page_size = cursor.fetchone()[0]
        cursor.execute("PRAGMA freelist_count")
        freelist_count = cursor.fetchone()[0]

        total_size = page_count * page_size
        free_size = freelist_count * page_size
        free_pct = 100 * freelist_count / page_count if page_count else 0
        
        if free_size > 10 * 1024 * 1024 and free_pct > 20:
            issues.append(
                f"数据库存在较多碎片空间 (约 {free_size / (1024*1024):.2f} MB, "
                f"占比 {free_pct:.1f}%)，建议运行 `python validate_and_vacuum.py --vacuum` 进行压缩整理"
            )
    except Exception as e:
        logger.warning(f"⚠️  无法读取数据库 Page 状态: {e}")

    conn.close()

    db_size = Path(db.db_path).stat().st_size / (1024 * 1024)
    report_lines.append(f"\n  数据库大小: {db_size:.2f} MB")
    report_lines.append("=" * 50)

    report = "\n".join(report_lines)
    logger.info(report)

    if issues:
        logger.warning("\n⚠️  发现以下问题:")
        for issue in issues:
            logger.warning(f"  - {issue}")
    else:
        logger.info("\n✅ 所有检查通过，数据库健康")

    return {
        "issues": issues,
        "coverage_pct": coverage_pct,
        "latest_bar": latest_bar,
        "db_size_mb": db_size,
        "report": report,
    }


# ===========================================================================
# 主控流程
# ===========================================================================

def run_all(
    db: DatabaseInterface,
    loader: DataLoaderInterface,
    engine: IndicatorEngineInterface,
    resume: bool = False,
) -> dict:
    """运行完整数据管道。"""
    start_time = time.time()
    logger.info("\n🚀 SmartMoney 每日数据管道启动")
    logger.info(f"📂 数据库: {db.db_path}")
    logger.info(f"📅 今天: {datetime.now().strftime('%Y-%m-%d')}")

    if not _should_update():
        return {"status": "skipped", "reason": "非交易日"}

    results = {}
    results["bars"] = update_bars(db, loader, resume=resume)

    # 总是调用 update_indicators。由于优化了智能探测，即使 bars 更新了0只，
    # 也会在 <0.1 秒内判断出无须计算并跳过，同时能保证修复任何因中断而缺失指标的股票。
    results["indicators"] = update_indicators(db, engine)

    results["fundamentals"] = update_fundamentals(db, loader)
    results["fund_flow"] = update_fund_flow(db, loader)
    results["margin_trading"] = update_margin_trading(db)
    results["dragon_tiger"] = update_dragon_tiger(db)
    results["block_trade"] = update_block_trade(db)
    results["sector_fund_flow"] = update_sector_fund_flow(db)
    results["shareholder_count"] = update_shareholder_count(db)
    results["retry"] = retry_failed(db, loader)
    results["health"] = health_check(db)

    elapsed = time.time() - start_time
    logger.info("\n" + "=" * 60)
    logger.info("🏁 数据管道全部完成")
    logger.info(f"⏱️  总耗时: {elapsed:.1f}s ({elapsed/60:.1f}min)")
    logger.info("=" * 60)

    return results


# ===========================================================================
# CLI 入口
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(description="SmartMoney 日常数据管道（解耦版 + 断点续传）")
    parser.add_argument(
        "--task",
        choices=[
            "all",
            "update_bars",
            "update_indicators",
            "update_fundamentals",
            "update_fund_flow",
            "update_margin_trading",
            "update_dragon_tiger",
            "update_block_trade",
            "update_sector_fund_flow",
            "update_shareholder_count",
            "retry",
            "health_check",
        ],
        default="all",
        help="要执行的任务 (默认: all)",
    )
    parser.add_argument("--limit", type=int, default=None, help="测试模式：只处理前 N 只股票")
    parser.add_argument("--force", action="store_true", help="强制运行（忽略交易日检查）")
    parser.add_argument(
        "--resume",
        action="store_true",
        help="断点续传：从上次中断的位置继续",
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default=os.getenv("QUANT_DB_PATH", DEFAULT_DB_PATH),
        help="数据库路径（默认从环境变量 QUANT_DB_PATH 读取）",
    )

    args = parser.parse_args()

    # 初始化 provider
    ProviderFactory.configure(db_path=args.db_path, provider="smartmoney")
    db = ProviderFactory.get_db()
    loader = ProviderFactory.get_loader()
    engine = ProviderFactory.get_indicator_engine()

    if args.force:
        global _should_update
        _should_update = lambda: True

    if args.task == "all":
        run_all(db, loader, engine, resume=args.resume)
    elif args.task == "update_bars":
        update_bars(db, loader, limit=args.limit, resume=args.resume)
    elif args.task == "update_indicators":
        update_indicators(db, engine)
    elif args.task == "update_fundamentals":
        update_fundamentals(db, loader)
    elif args.task == "update_fund_flow":
        update_fund_flow(db, loader)
    elif args.task == "update_margin_trading":
        update_margin_trading(db)
    elif args.task == "update_dragon_tiger":
        update_dragon_tiger(db)
    elif args.task == "update_block_trade":
        update_block_trade(db)
    elif args.task == "update_sector_fund_flow":
        update_sector_fund_flow(db)
    elif args.task == "update_shareholder_count":
        update_shareholder_count(db)
    elif args.task == "retry":
        retry_failed(db, loader)
    elif args.task == "health_check":
        health_check(db)


if __name__ == "__main__":
    main()
