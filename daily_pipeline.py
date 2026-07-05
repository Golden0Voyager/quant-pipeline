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
import difflib
import json
import logging
import os
import random
import socket
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timedelta

# 设置全局套接字超时，防止网络悬挂/DNS阻塞导致 API 请求无限期挂起
socket.setdefaulttimeout(15)
from pathlib import Path
from typing import Any

import pandas as pd

# 将 ~/Code 加入 Python 路径（使 pipeline 能 import smartmoney_hunter）
_CODE_DIR = os.path.expanduser("~/Code")
if _CODE_DIR not in sys.path:
    sys.path.insert(0, _CODE_DIR)

from smartmoney_hunter.market_utils import is_beijing_stock

from interface import (
    DatabaseInterface,
    DataLoaderInterface,
    IndicatorEngineInterface,
    ProviderFactory,
)

# 新数据维度直接调用 akshare（中台批量抓取）
try:
    import akshare as ak
except ImportError:
    ak = None  # type: ignore[assignment]

# ── macOS 系统代理注入 ──
# 检测系统代理设置并注入环境变量，让 requests/urllib3 能通过 Clash 等代理访问 eastmoney
# 同时添加 NO_PROXY 绕过不需要走代理的域名
if not os.getenv("HTTP_PROXY") and not os.getenv("http_proxy"):
    try:
        _proxy_out = subprocess.run(
            ["scutil", "--proxy"], capture_output=True, text=True, timeout=5
        )
        if "HTTPEnable : 1" in _proxy_out.stdout and "HTTPProxy" in _proxy_out.stdout:
            _host = _port = None
            for _line in _proxy_out.stdout.split("\n"):
                _line = _line.strip()
                if _line.startswith("HTTPProxy"):
                    _host = _line.split(":")[1].strip()
                elif _line.startswith("HTTPPort"):
                    _port = _line.split(":")[1].strip()
            if _host and _port:
                _proxy_url = f"http://{_host}:{_port}"
                os.environ["HTTP_PROXY"] = _proxy_url
                os.environ["HTTPS_PROXY"] = _proxy_url
                os.environ["http_proxy"] = _proxy_url
                os.environ["https_proxy"] = _proxy_url
                # 绕过代理直接访问的域名
                os.environ["NO_PROXY"] = "localhost,127.0.0.1,datacenter-web.eastmoney.com,push2.eastmoney.com,push2his.eastmoney.com,*.eastmoney.com"
                os.environ["no_proxy"] = "localhost,127.0.0.1,datacenter-web.eastmoney.com,push2.eastmoney.com,push2his.eastmoney.com,*.eastmoney.com"
    except Exception:
        pass

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

    def _load(self) -> list[dict]:
        if not self.FILE.exists():
            return []
        try:
            with open(self.FILE, encoding="utf-8") as f:
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

    def should_abort(self) -> tuple[bool, str]:
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
        failed_queue: list[str],
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
    def load(cls) -> dict[str, Any] | None:
        """读取进度文件。"""
        if not cls.FILE.exists():
            return None
        try:
            with open(cls.FILE, encoding="utf-8") as f:
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
            with open(retry_file, encoding="utf-8") as f:
                content = f.read().strip()
            if not content:
                retry_file.unlink()
                logger.info("🗑️  retry_queue.txt 已清理")

    @classmethod
    def find_resume_index(cls, stock_codes: list[str], last_symbol: str) -> int:
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
# 任务 0: 更新全市场股票列表
# ===========================================================================

def _infer_market(code: str) -> str:
    """根据股票代码前缀精确推断板块市场标识。

    分类规则：
      688xxx → star  (科创板)
      6xxxxx → sh    (沪市主板)
      300xxx / 301xxx → gem  (创业板)
      002xxx / 003xxx → sme  (深市中小板)
      000xxx / 001xxx → sz   (深市主板)
      43xxxx / 83xxxx / 87xxxx / 82xxxx → bj (北交所)
      其余 → sz (兜底)
    """
    if code.startswith("688"):
        return "star"
    if code.startswith("6"):
        return "sh"
    if code.startswith(("300", "301")):
        return "gem"
    if code.startswith(("002", "003")):
        return "sme"
    if code.startswith(("000", "001")):
        return "sz"
    if code.startswith(("4", "8")):
        return "bj"
    return "sz"


def update_stock_list(db: DatabaseInterface) -> dict:
    """从 AkShare 拉取全量 A 股列表并写入 stock_list 表。"""
    logger.info("\n" + "=" * 60)
    logger.info("📋 任务: 更新全市场股票列表")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "error": "akshare not installed"}

    try:
        df_raw = ak.stock_info_a_code_name()
        if df_raw is None or df_raw.empty:
            logger.warning("⚠️  未获取到股票列表数据")
            return {"saved": 0, "error": "empty response"}

        df = df_raw[["code", "name"]].copy()
        df["code"] = df["code"].astype(str).str.strip()
        df["name"] = df["name"].astype(str).str.strip()
        df["market"] = df["code"].apply(_infer_market)
        df["industry"] = None  # 由 update_industry 任务填充

        db.save_stock_list(df)
        saved = len(df)
        sh_count = (df["market"] == "sh").sum()
        sz_count = (df["market"] == "sz").sum()
        bj_count = (df["market"] == "bj").sum()
        logger.info(
            f"✅ 股票列表更新完成: {saved} 只 "
            f"(沪市 {sh_count} / 深市 {sz_count} / 北交所 {bj_count})"
        )
        return {"saved": saved, "sh": int(sh_count), "sz": int(sz_count), "bj": int(bj_count)}
    except Exception as e:
        logger.error(f"❌ 股票列表更新失败: {e}")
        return {"saved": 0, "error": str(e)}


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

    stock_codes = [c for c in stocks["code"].tolist() if not is_beijing_stock(c)]
    if limit:
        stock_codes = stock_codes[:limit]
        logger.info(f"⚠️  测试模式：只更新前 {limit} 只")

    total = len(stock_codes)
    bj_count = len(stocks) - total
    logger.info(f"📊 共 {total} 只股票待更新（已跳过 {bj_count} 只北交所）")

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
    failed_symbols: list[str] = progress.get("failed_queue", []) if progress else []
    last_symbol = ""

    # ── 加载 retry_queue.txt 中之前失败的股票 ──
    retry_file = SHARED_DATA_DIR / "retry_queue.txt"
    if retry_file.exists():
        with open(retry_file, encoding="utf-8") as f:
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
    db: DatabaseInterface,
    loader: DataLoaderInterface,
    symbol: str,
    watchlist_symbols: set[str] | None = None,
    backfilled_symbols: set[str] | None = None,
    backfill_file: Path | None = None,
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

                    db.save_daily_bars(symbol, df_bars)
                    logger.info(f"✅ {symbol} 自选股全量历史K线拉取并保存成功，共 {len(df_bars)} 条")

                    # 记录已完成全量回填
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
            existing = db.get_daily_bars(symbol)
            if not existing.empty:
                df_bars = loader.incremental_update(symbol, existing)
                # 优化点：如果行数没变，说明已经是最新，无需重复保存，直接返回 skipped
                if len(df_bars) == len(existing):
                    return "skipped"
            else:
                # 正常非自选股的全量拉取走默认的 3 年配置
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
    db: DatabaseInterface, engine: IndicatorEngineInterface, symbols_to_update: list[str] = None
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
    # 尝试今天，如果没数据回退到最近交易日
    trade_dates = [today]
    for offset in range(1, 5):
        d = datetime.now() - timedelta(days=offset)
        if d.weekday() < 5:
            trade_dates.append(d.strftime("%Y-%m-%d"))

    import requests as _req

    session = _req.Session()
    session.proxies = {"http": None, "https": None}
    session.trust_env = False

    all_records = []
    for td in trade_dates:
        if all_records:
            break
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
                    "filter": f"(TRADE_DATE='{td}')",
                }
                resp = session.get(url, params=params, timeout=15)
                data = resp.json()
                if data.get("success") and data.get("result") and data["result"].get("data"):
                    records = data["result"]["data"]
                    all_records.extend(records)
                    total_count = data["result"].get("count", 0)
                    if page * page_size >= total_count:
                        break
                    page += 1
                else:
                    break
            except Exception as e:
                logger.warning(f"⚠️  获取估值数据失败 (日期={td}): {e}")
                break

    if not all_records:
        logger.warning("⚠️  未获取到估值数据")
        return {"saved": 0, "total": 0}

    saved = 0
    for rec in all_records:
        try:
            code = str(rec.get("SECURITY_CODE", "")).strip()
            if not code:
                continue
            trade_date = str(rec.get("TRADE_DATE", today))[:10]
            data = {
                "trade_date": trade_date,
                "pe_ttm": rec.get("PE_TTM"),
                "pb": rec.get("PB_MRQ"),
                "ps_ttm": rec.get("PS_TTM"),
                "dividend_yield": None,
                "roe": None,
                "roa": None,
                "gross_margin": None,
                "net_margin": None,
                "debt_ratio": None,
                "revenue_growth": None,
                "profit_growth": None,
                "eps_growth": None,
                "peg": rec.get("PEG_CAR"),
                "market_cap": rec.get("TOTAL_MARKET_CAP"),
            }
            db.save_fundamentals(code, data)
            saved += 1
        except Exception as e:
            logger.debug(f"  保存估值失败: {e}")
            continue

    date_used = str(all_records[0].get("TRADE_DATE", ""))[:10] if all_records else today
    logger.info(f"✅ 估值数据保存完成: {saved}/{len(all_records)} 只 (日期: {date_used})")
    return {"saved": saved, "total": len(all_records)}


# ===========================================================================
# 任务 3.5: 雪球 token 落地 — 批量补充实时行情指标
# ===========================================================================

def update_market_snapshot(db: DatabaseInterface) -> dict:
    """
    通过雪球 batch/quote API 批量获取全市场实时行情指标，
    补充 fundamentals 表的 dividend_yield 字段。

    Token 从环境变量 / smartmoney_hunter/.env 读取（XUEQIU_TOKEN / XUEQIU_USER_ID）。
    """
    logger.info("\n" + "=" * 60)
    logger.info("📊 任务: 雪球行情快照 (dividend_yield 补充)")
    logger.info("=" * 60)

    import sqlite3
    import time

    from smartmoney_hunter import xueqiu as xq

    today = datetime.now().strftime("%Y-%m-%d")

    # 1. 读取全量股票
    conn = sqlite3.connect(str(db.db_path))
    cursor = conn.cursor()
    cursor.execute("SELECT code, market FROM stock_list")
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        logger.warning("⚠️  股票列表为空")
        return {"saved": 0, "total": 0}

    rows = [(c, m) for c, m in rows if not is_beijing_stock(c)]

    if xq._get_token() is None:
        logger.warning("⚠️  XUEQIU_TOKEN 未设置，跳过雪球行情快照")
        return {"saved": 0, "total": 0, "skipped": True}

    total = len(rows)
    logger.info(f"📊 共 {total} 只股票，准备拉取雪球行情（已跳过北交所）")

    # 2. 分批调用（每批 50 只，串行 + 小延迟避免风控）
    batch = 50
    codes = [c for c, _ in rows]
    all_quotes: list[dict] = []

    for i in range(0, total, batch):
        chunk = codes[i : i + batch]
        try:
            quotes = xq.get_batch_quotes(chunk)
            all_quotes.extend(quotes)
        except Exception as e:
            logger.debug(f"  批次 {i//batch + 1} 失败: {e}")
        if (i // batch + 1) % 20 == 0 or i + batch >= total:
            logger.info(f"  批次进度: {min(i + batch, total)}/{total} (已获取 {len(all_quotes)} 只)")
        time.sleep(0.05)

    logger.info(f"📊 雪球行情获取完成: {len(all_quotes)} 只")

    # 3. 写回数据库 — 只补充 dividend_yield (不覆盖现有 pe_ttm/pb/market_cap)
    updated = 0
    if all_quotes:
        conn = sqlite3.connect(str(db.db_path))
        cursor = conn.cursor()
        for q in all_quotes:
            div_yield = q.get("dividend_yield")
            if div_yield is not None:
                cursor.execute(
                    """UPDATE fundamentals SET dividend_yield = ?
                       WHERE ts_code = ? AND trade_date = ?
                       AND (dividend_yield IS NULL OR dividend_yield = 0)""",
                    (div_yield, q["code"], today),
                )
                if cursor.rowcount:
                    updated += 1
        conn.commit()
        conn.close()
        logger.info(f"✅ dividend_yield 补充完成: {updated} 只")
    else:
        logger.warning("⚠️  雪球行情未获取到数据")

    return {"saved": len(all_quotes), "total": total, "updated": updated}


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

    target_date = _get_expected_latest_trading_day().replace("-", "")
    saved = 0
    total = 0

    for exchange, fetcher in [("sh", ak.stock_margin_detail_sse), ("sz", ak.stock_margin_detail_szse)]:
        try:
            df = fetcher(date=target_date)
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
                        "trade_date": target_date,
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

    target_date = _get_expected_latest_trading_day().replace("-", "")
    try:
        df = ak.stock_lhb_detail_em(start_date=target_date, end_date=target_date)
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
                    "trade_date": target_date,
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

    target_date = _get_expected_latest_trading_day().replace("-", "")
    try:
        df = ak.stock_dzjy_mrmx(symbol="A股", start_date=target_date, end_date=target_date)
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
                    "trade_date": target_date,
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

def _fetch_sector_fund_flow_primary(trade_date: str) -> pd.DataFrame | None:
    """Primary source: Eastmoney sector fund flow history."""
    df = ak.stock_sector_fund_flow_hist(symbol="行业资金流")
    if df is None or df.empty:
        return None
    records = []
    for _, row in df.iterrows():
        sector = str(row.get("行业", "")).strip()
        if not sector:
            continue
        records.append({
            "sector_name": sector,
            "trade_date": trade_date,
            "main_net_inflow": row.get("主力净流入-净额"),
            "main_net_inflow_pct": row.get("主力净流入-净占比"),
            "super_large_net_inflow": row.get("超大单净流入-净额"),
            "large_net_inflow": row.get("大单净流入-净额"),
            "medium_net_inflow": row.get("中单净流入-净额"),
            "small_net_inflow": row.get("小单净流入-净额"),
            "data_source": "akshare_eastmoney",
        })
    return pd.DataFrame(records)


def _fetch_sector_fund_flow_fallback(trade_date: str) -> pd.DataFrame | None:
    """Fallback source: Sina/Tonghuashun industry fund flow ranking.

    This endpoint is reachable when Eastmoney is blocked and provides
    net inflow (净额) and rank (序号) per industry.
    """
    df = ak.stock_fund_flow_industry()
    if df is None or df.empty:
        return None
    records = []
    for _, row in df.iterrows():
        sector = str(row.get("行业", "")).strip()
        if not sector:
            continue
        records.append({
            "sector_name": sector,
            "trade_date": trade_date,
            "main_net_inflow": row.get("净额"),
            "main_net_inflow_pct": row.get("行业-涨跌幅"),
            "super_large_net_inflow": None,
            "large_net_inflow": row.get("流入资金"),
            "medium_net_inflow": row.get("流出资金"),
            "small_net_inflow": None,
            "data_source": "akshare_fallback",
        })
    return pd.DataFrame(records)


def update_sector_fund_flow(db: DatabaseInterface) -> dict:
    """批量获取板块资金流向并保存（主源失败时自动降级到备用源）。"""
    logger.info("\n" + "=" * 60)
    logger.info("🏭 任务: 批量获取板块资金流向")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "total": 0, "error": "akshare not installed"}

    target_date = _get_expected_latest_trading_day()

    df: pd.DataFrame | None = None
    source = "primary"
    try:
        df = _fetch_sector_fund_flow_primary(target_date)
    except Exception as e:
        logger.warning(f"⚠️  东财板块资金流向失败: {e}，尝试备用源...")

    if df is None or df.empty:
        source = "fallback"
        try:
            df = _fetch_sector_fund_flow_fallback(target_date)
        except Exception as e:
            logger.error(f"❌ 备用板块资金流向也失败: {e}")
            return {"saved": 0, "total": 0, "error": str(e)}

    if df is None or df.empty:
        logger.warning("⚠️  板块资金流向无数据")
        return {"saved": 0, "total": 0}

    saved = 0
    for _, row in df.iterrows():
        try:
            db.save_sector_fund_flow(row["sector_name"], row.to_dict())
            saved += 1
        except Exception:
            continue

    logger.info(f"✅ 板块资金流向保存完成 ({source}): {saved}/{len(df)}")
    return {"saved": saved, "total": len(df), "source": source}


# ===========================================================================
# 任务 8.5: 保存历史估值快照
# ===========================================================================

def update_historical_valuation(db: DatabaseInterface) -> dict:
    """把最新 fundamentals 估值数据快照写入 historical_valuation，用于分位数计算。"""
    logger.info("\n" + "=" * 60)
    logger.info("📈 任务: 保存历史估值快照")
    logger.info("=" * 60)

    try:
        df = db.get_fundamentals_batch()
        if df.empty:
            logger.warning("⚠️  fundamentals 为空，跳过历史估值快照")
            return {"saved": 0, "total": 0}

        saved = 0
        for _, row in df.iterrows():
            try:
                symbol = row.get("ts_code")
                trade_date = row.get("trade_date")
                if not symbol or not trade_date:
                    continue
                data = {
                    "pe_ttm": row.get("pe_ttm"),
                    "pb": row.get("pb"),
                    "ps_ttm": row.get("ps_ttm"),
                    "dividend_yield": row.get("dividend_yield"),
                }
                db.save_historical_valuation(symbol, str(trade_date)[:10], data)
                saved += 1
            except Exception:
                continue

        logger.info(f"✅ 历史估值快照保存完成: {saved}/{len(df)}")
        return {"saved": saved, "total": len(df)}
    except Exception as e:
        logger.error(f"❌ 历史估值快照失败: {e}")
        return {"saved": 0, "total": 0, "error": str(e)}


# ===========================================================================
# 任务 8.6: 生成行业对比数据
# ===========================================================================

def update_sector_industry(db: DatabaseInterface) -> dict:
    """基于 fundamentals + stock_list 生成行业聚合数据，写入 sector_industry。"""
    logger.info("\n" + "=" * 60)
    logger.info("🏭 任务: 生成行业对比数据")
    logger.info("=" * 60)

    try:
        import pandas as pd

        stocks = db.get_stock_list()
        fundamentals = db.get_fundamentals_batch()
        if stocks.empty or fundamentals.empty:
            logger.warning("⚠️  股票列表或基本面为空，跳过行业对比")
            return {"saved": 0, "total": 0}

        df = fundamentals.merge(stocks[["code", "industry"]], left_on="ts_code", right_on="code", how="left")
        df["industry"] = df["industry"].fillna("未知行业")

        numeric_cols = ["pe_ttm", "pb", "ps_ttm", "roe", "revenue_growth", "profit_growth", "market_cap"]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        grouped = df.groupby("industry").agg(
            avg_pe=("pe_ttm", "mean"),
            avg_pb=("pb", "mean"),
            avg_ps=("ps_ttm", "mean"),
            avg_roe=("roe", "mean"),
            avg_revenue_growth=("revenue_growth", "mean"),
            avg_profit_growth=("profit_growth", "mean"),
            total_market_cap=("market_cap", "sum"),
        ).reset_index()

        # 尝试补充资金流入排名（如果 sector_fund_flow 表已有数据）
        try:
            conn = sqlite3.connect(db.db_path)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT sector_name, main_net_inflow
                FROM sector_fund_flow
                WHERE trade_date = (SELECT MAX(trade_date) FROM sector_fund_flow)
            """)
            rows = cursor.fetchall()
            conn.close()
            if rows:
                df_flow = pd.DataFrame(rows, columns=["sector_name", "main_net_inflow"])
                df_flow["main_net_inflow"] = pd.to_numeric(df_flow["main_net_inflow"], errors="coerce")
                df_flow = df_flow.dropna(subset=["main_net_inflow"])
                df_flow = df_flow.sort_values("main_net_inflow", ascending=False).reset_index(drop=True)
                df_flow["fund_inflow_rank"] = df_flow.index + 1

                # 行业名称在 stock_list / sector_industry 与 sector_fund_flow 之间常不一致。
                # 先精确匹配，再走少量 hard-coded 映射，最后用模糊匹配兜底。
                flow_names = df_flow["sector_name"].tolist()
                hard_coded_map = {
                    "酿酒行业": "白酒",
                    "家电行业": "白色家电",
                    "食品饮料": "食品加工制造",
                    "化工": "化学制品",
                    "化工行业": "化学制品",
                    "基础化工": "化学制品",
                    "化纤行业": "化学纤维",
                    "化肥行业": "农化制品",
                    "医疗行业": "医疗器械",
                    "医药制造": "化学制药",
                    "医药生物": "化学制药",
                    "水泥建材": "建筑材料",
                    "玻璃陶瓷": "建筑材料",
                    "装修建材": "建筑材料",
                    "旅游酒店": "旅游及酒店",
                    "商业百货": "零售",
                    "贸易行业": "贸易",
                    "电子信息": "电子",
                    "电子元件": "元件",
                    "电子零部件制造": "元件",
                    "输配电气": "电网设备",
                    "电源设备": "电网设备",
                    "高低压设备": "电网设备",
                    "安防设备": "计算机设备",
                    "计算机应用": "软件开发",
                    "计算机": "计算机设备",
                    "软件服务": "软件开发",
                    "汽车行业": "汽车整车",
                    "汽车服务": "汽车服务及其他",
                    "房地产服务": "房地产",
                    "房地产开发": "房地产",
                    "石油行业": "石油加工贸易",
                    "石油石化": "石油加工贸易",
                    "煤炭行业": "煤炭开采加工",
                    "煤炭采选": "煤炭开采加工",
                    "钢铁行业": "钢铁",
                    "环保工程": "环境治理",
                    "环保行业": "环境治理",
                    "环保工程及服务": "环境治理",
                    "港口水运": "港口航运",
                    "航空机场": "机场航运",
                    "航天航空": "军工装备",
                    "国防军工": "军工装备",
                    "交运物流": "物流",
                    "物流行业": "物流",
                    "通讯行业": "通信服务",
                    "通信配套服务": "通信服务",
                    "电信运营": "通信服务",
                    "造纸印刷": "造纸",
                    "服装家纺": "服装家纺",
                    "纺织服装": "纺织制造",
                    "纺织服饰": "纺织制造",
                    "橡胶": "橡胶制品",
                    "塑胶制品": "塑料制品",
                    "包装材料": "包装印刷",
                    "金属制品": "通用设备",
                    "机械行业": "通用设备",
                    "机械设备": "通用设备",
                    "农林牧渔": "养殖业",
                    "农牧饲渔": "养殖业",
                    "农业综合": "种植业与林业",
                    "种子生产": "种植业与林业",
                    "农产品加工": "农产品加工",
                    "农药": "农化制品",
                    "农药兽药": "农化制品",
                    "小金属": "小金属",
                    "有色金属": "工业金属",
                    "贵金属": "贵金属",
                    "能源金属": "能源金属",
                    "铁路公路": "公路铁路运输",
                    "航运": "港口航运",
                    "航运港口": "港口航运",
                    "船舶制造": "船舶制造",
                    "电力行业": "电力",
                    "电力设备": "电网设备",
                    "公用事业": "电力",
                    "燃气": "燃气",
                    "保险": "保险",
                    "券商信托": "证券",
                    "银行": "银行",
                    "多元金融": "多元金融",
                    "综合行业": "综合",
                    "塑料制品": "塑料制品",
                    "光学元件": "光学光电子",
                    "光学光电子": "光学光电子",
                    "半导体": "半导体",
                    "集成电路": "半导体",
                    "光伏设备": "光伏设备",
                    "风电设备": "风电设备",
                    "电池": "电池",
                    "储能设备": "电池",
                    "电机": "电机",
                    "电网设备": "电网设备",
                    "电子化学品": "电子化学品",
                    "非金属材料Ⅱ": "非金属材料",
                    "美容护理": "美容护理",
                    "生物制品": "生物制品",
                    "中药": "中药",
                    "化学制药": "化学制药",
                    "医疗服务": "医疗服务",
                    "医疗器械": "医疗器械",
                    "医药商业": "医药商业",
                    "传媒": "文化传媒",
                    "教育": "教育",
                    "厨卫电器": "厨卫电器",
                    "小家电": "小家电",
                    "黑色家电": "黑色家电",
                    "家居用品": "家居用品",
                    "家具": "家居用品",
                    "家用轻工": "家居用品",
                    "饲料": "养殖业",
                    "食品加工制造": "食品加工制造",
                    "饮料制造": "饮料制造",
                }

                rank_map: dict[str, int | None] = {}
                for industry in grouped["industry"]:
                    target_name = industry
                    # 1) hard-coded alias
                    if industry in hard_coded_map:
                        target_name = hard_coded_map[industry]
                    # 2) exact match after alias
                    if target_name in flow_names:
                        rank_map[industry] = int(
                            df_flow.loc[df_flow["sector_name"] == target_name, "fund_inflow_rank"].iloc[0]
                        )
                        continue
                    # 3) fuzzy fallback
                    matches = difflib.get_close_matches(industry, flow_names, n=1, cutoff=0.5)
                    if matches:
                        rank_map[industry] = int(
                            df_flow.loc[df_flow["sector_name"] == matches[0], "fund_inflow_rank"].iloc[0]
                        )
                    else:
                        rank_map[industry] = None

                grouped["fund_inflow_rank"] = grouped["industry"].map(rank_map)
            else:
                grouped["fund_inflow_rank"] = None
        except Exception:
            grouped["fund_inflow_rank"] = None

        today = datetime.now().strftime("%Y-%m-%d")
        saved = 0
        for _, row in grouped.iterrows():
            try:
                data = {
                    "industry_name": row["industry"],
                    "trade_date": today,
                    "avg_pe": row["avg_pe"],
                    "avg_pb": row["avg_pb"],
                    "avg_ps": row["avg_ps"],
                    "avg_roe": row["avg_roe"],
                    "avg_revenue_growth": row["avg_revenue_growth"],
                    "avg_profit_growth": row["avg_profit_growth"],
                    "total_market_cap": row["total_market_cap"],
                    "fund_inflow_rank": row.get("fund_inflow_rank"),
                    "data_source": "derived",
                }
                db.save_sector_industry(data)
                saved += 1
            except Exception:
                continue

        logger.info(f"✅ 行业对比数据保存完成: {saved}/{len(grouped)}")
        return {"saved": saved, "total": len(grouped)}
    except Exception as e:
        logger.error(f"❌ 行业对比数据生成失败: {e}")
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
# 任务 10: 批量获取季度财务数据
# ===========================================================================

def update_quarterly_financials(db: DatabaseInterface, loader: DataLoaderInterface) -> dict:
    """批量获取全市场季度财务数据并保存。"""
    logger.info("\n" + "=" * 60)
    logger.info("📋 任务: 批量获取季度财务数据")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "failed": 0, "total": 0, "error": "akshare not installed"}

    stocks = db.get_stock_list()
    if stocks.empty:
        logger.error("❌ 股票列表为空")
        return {"saved": 0, "failed": 0, "total": 0}

    stock_codes = [c for c in stocks["code"].tolist() if not is_beijing_stock(c)]
    total = len(stock_codes)
    saved = 0
    failed = 0

    import pandas as _pd

    def _extract_float(df, label):
        try:
            mask = df["指标"] == label
            if mask.any():
                val = df.loc[mask].iloc[:, 2]
                if _pd.notna(val.iloc[0]):
                    return float(val.iloc[0])
        except Exception:
            pass
        return None

    for i, code in enumerate(stock_codes, 1):
        try:
            df = ak.stock_financial_abstract(symbol=code)
            if df is not None and not df.empty and len(df.columns) > 2:
                data = {
                    "report_period": str(df.columns[2]),
                    "revenue": _extract_float(df, "营业总收入"),
                    "net_profit": _extract_float(df, "归母净利润"),
                    "operating_cashflow": _extract_float(df, "经营现金流量净额"),
                    "roe": _extract_float(df, "净资产收益率(ROE)"),
                    "gross_margin": _extract_float(df, "毛利率"),
                    "net_margin": _extract_float(df, "销售净利率"),
                    "revenue_growth": _extract_float(df, "营业总收入增长率"),
                    "profit_growth": _extract_float(df, "归属母公司净利润增长率"),
                    "debt_ratio": _extract_float(df, "资产负债率"),
                    "eps": _extract_float(df, "基本每股收益"),
                    "bps": _extract_float(df, "每股净资产"),
                }
                db.save_quarterly_financials(code, data)
                saved += 1
            if i % 200 == 0:
                logger.info(f"  进度: {i}/{total} (成功: {saved}, 失败: {failed})")
        except Exception as e:
            logger.debug(f"  股票 {code} 失败: {e}")
            failed += 1
            continue

    logger.info(f"✅ 季度财务数据保存完成: {saved}/{total} (失败: {failed})")
    return {"saved": saved, "failed": failed, "total": total}


# ===========================================================================
# 任务 11: 更新行业分类（通过东方财富 F10 API + 并发）
# ===========================================================================

def update_industry(db: DatabaseInterface) -> dict:
    """
    批量更新 stock_list.industry 列。

    策略 A: eastmoney F10 CompanySurvey API (SZ/SH 主板/创业板/科创板)
    https://emweb.securities.eastmoney.com/PC_HSF10/CompanySurvey/CompanySurveyAjax?code=...
    提取 申万行业 (jbzl.sshy)。
    策略 B (回退): Sina 财经个股资料页 (覆盖 BJ 及部分新上市股票)
    http://money.finance.sina.com.cn/corp/go.php/vCI_CorpOtherInfo/stockid/{code}.phtml
    使用 ThreadPoolExecutor 并发加速 (~0.1s/只, 5000 只 ≈ 1min 并发)。
    """
    logger.info("\n" + "=" * 60)
    logger.info("🏢 任务: 更新行业分类 (F10 API)")
    logger.info("=" * 60)

    import sqlite3
    from concurrent.futures import ThreadPoolExecutor, as_completed

    import requests as _req

    # 1. 读取需要更新的股票
    conn = sqlite3.connect(str(db.db_path))
    cursor = conn.cursor()
    cursor.execute(
        "SELECT code, market FROM stock_list WHERE industry IS NULL OR industry = '未分类'"
    )
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        logger.info("✅ 所有股票已有行业分类")
        return {"saved": 0, "total": 0}

    total = len(rows)
    logger.info(f"📊 共 {total} 只股票需要更新行业")

    # 2. 市场前缀映射
    exchange_map = {"sz": "SZ", "sh": "SH", "unknown": "BJ"}

    # 3. HTTP session（每个线程自建 session 以避免并发问题）
    def _fetch_industry(code: str, market: str) -> tuple[str, str | None]:
        # 策略 A: 尝试 eastmoney F10 API（覆盖 SZ/SH 主板/创业板/科创板）
        prefix = exchange_map.get(market, "SZ")
        api_code = f"{prefix}{code}"
        f10_url = f"https://emweb.securities.eastmoney.com/PC_HSF10/CompanySurvey/CompanySurveyAjax?code={api_code}"
        try:
            session = _req.Session()
            session.proxies = {"http": None, "https": None}
            session.trust_env = False
            resp = session.get(
                f10_url,
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=10,
            )
            if resp.status_code == 200:
                data = resp.json()
                jbzl = data.get("jbzl")
                if jbzl and isinstance(jbzl, dict):
                    industry = jbzl.get("sshy")
                    if industry and industry != "N/A":
                        return code, industry
        except Exception:
            pass

        # 策略 B: F10 失败时降级到 Sina 财经个股资料页 (覆盖 BJ 及新上市股票)
        try:
            sin_url = f"http://money.finance.sina.com.cn/corp/go.php/vCI_CorpOtherInfo/stockid/{code}.phtml"
            sin_session = _req.Session()
            sin_session.proxies = {"http": None, "https": None}
            sin_session.trust_env = False
            resp = sin_session.get(
                sin_url,
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=10,
            )
            resp.encoding = "gb2312"
            if resp.status_code == 200:
                import re
                m = re.search(
                    r'所属行业板块</td>\s*</tr>\s*<tr>.*?<td[^>]*>([^<]+)',
                    resp.text, re.DOTALL,
                )
                if m:
                    industry = m.group(1).strip()
                    if industry and "备注" not in industry:
                        return code, industry
        except Exception:
            pass

        return code, None

    # 4. 并发执行
    success_map: dict[str, str] = {}
    fail_list: list[str] = []
    processed = 0

    max_workers = 15
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        fut_map = {}
        for code, market in rows:
            fut = pool.submit(_fetch_industry, code, market)
            fut_map[fut] = code

        for fut in as_completed(fut_map):
            code = fut_map[fut]
            result = fut.result()
            if result and result[1]:
                success_map[code] = result[1]
            else:
                fail_list.append(code)

            processed += 1
            if processed % 500 == 0 or processed == total:
                logger.info(
                    f"  进度: {processed}/{total} "
                    f"(成功: {len(success_map)}, 失败: {len(fail_list)})"
                )

    logger.info(
        f"📊 接口请求完成: 成功 {len(success_map)}, 失败 {len(fail_list)}"
    )

    # 5. 批量写入数据库
    if success_map:
        conn = sqlite3.connect(str(db.db_path))
        cursor = conn.cursor()
        updated = 0
        batch = []
        for code, industry in success_map.items():
            batch.append((industry, code))
            if len(batch) >= 500:
                cursor.executemany(
                    "UPDATE stock_list SET industry = ?, updated_at = CURRENT_TIMESTAMP "
                    "WHERE code = ? AND (industry IS NULL OR industry = '未分类')",
                    batch,
                )
                updated += cursor.rowcount
                conn.commit()
                batch = []
        if batch:
            cursor.executemany(
                "UPDATE stock_list SET industry = ?, updated_at = CURRENT_TIMESTAMP "
                "WHERE code = ? AND (industry IS NULL OR industry = '未分类')",
                batch,
            )
            updated += cursor.rowcount
            conn.commit()
        conn.close()
        logger.info(f"✅ 行业分类更新完成: {updated} 只股票")
    else:
        updated = 0
        logger.warning("⚠️  未获取到任何行业数据")

    # 6. 更新 stock_list 表索引（如果不存在）
    try:
        conn = sqlite3.connect(str(db.db_path))
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_stock_list_industry ON stock_list(industry)"
        )
        conn.close()
    except Exception:
        pass

    return {
        "saved": updated,
        "total": total,
        "failed": len(fail_list),
        "coverage_pct": round(100 * updated / total, 1) if total else 0,
    }


# ===========================================================================
# 任务 12: 重试失败队列
# ===========================================================================

def retry_failed(
    db: DatabaseInterface, loader: DataLoaderInterface
) -> dict:
    """重试之前失败的股票。"""
    retry_file = SHARED_DATA_DIR / "retry_queue.txt"
    if not retry_file.exists():
        logger.info("ℹ️  retry 队列为空")
        return {"success": 0, "failed": 0, "total": 0}

    with open(retry_file, encoding="utf-8") as f:
        symbols = [line.strip() for line in f if line.strip()]

    symbols = [s for s in symbols if not is_beijing_stock(s)]

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


def _get_expected_latest_trading_day() -> str:
    """获取期望的最新交易日日期 (YYYY-MM-DD)。
    如果是周末，期望最新交易日为上周五；
    如果是周一至周五，且在 15:30 之前，期望最新交易日为前一个交易日；
    如果是周一至周五，且在 15:30 之后，期望最新交易日为今天。
    """
    now = datetime.now()
    target = now
    # 如果是交易日（周一至周五），在 15:30 之前，预期的数据最新是前一天
    if target.weekday() < 5 and (target.hour < 15 or (target.hour == 15 and target.minute < 30)):
        target -= timedelta(days=1)

    # 如果目标日期是周末，则向前回滚到周五
    while target.weekday() >= 5:
        target -= timedelta(days=1)

    return target.strftime("%Y-%m-%d")


def health_check(db: DatabaseInterface) -> dict:
    """检查数据库健康状态并生成报告。"""
    logger.info("\n" + "=" * 60)
    logger.info("🏥 任务: 数据质量健康检查")
    logger.info("=" * 60)

    today = datetime.now().strftime("%Y-%m-%d")
    issues: list[str] = []
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

    expected_latest = _get_expected_latest_trading_day()
    if latest_bar < expected_latest:
        issues.append(f"日线数据未更新到最新: {latest_bar} (期望最新: {expected_latest}, 今天是 {today})")

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

        page_count * page_size
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

    def _safe_task(name: str, fn, *args, **kwargs) -> dict:
        """安全执行单个任务，异常时记录日志不影响后续任务。"""
        try:
            logger.info(f"\n{'='*60}\n▶ 开始任务: {name}\n{'='*60}")
            return fn(*args, **kwargs)
        except Exception as e:
            logger.error(f"❌ 任务 {name} 异常终止: {e}", exc_info=True)
            return {"error": str(e), "status": "crashed"}

    results = {}
    results["stock_list"] = _safe_task("update_stock_list", update_stock_list, db)
    results["bars"] = _safe_task("update_bars", update_bars, db, loader, resume=resume)

    # 总是调用 update_indicators。由于优化了智能探测，即使 bars 更新了0只，
    # 也会在 <0.1 秒内判断出无须计算并跳过，同时能保证修复任何因中断而缺失指标的股票。
    results["indicators"] = _safe_task("update_indicators", update_indicators, db, engine)

    results["fundamentals"] = _safe_task("update_fundamentals", update_fundamentals, db, loader)
    results["market_snapshot"] = _safe_task("update_market_snapshot (雪球)", update_market_snapshot, db)
    results["fund_flow"] = _safe_task("update_fund_flow", update_fund_flow, db, loader)
    results["margin_trading"] = _safe_task("update_margin_trading", update_margin_trading, db)
    results["dragon_tiger"] = _safe_task("update_dragon_tiger", update_dragon_tiger, db)
    results["block_trade"] = _safe_task("update_block_trade", update_block_trade, db)
    results["sector_fund_flow"] = _safe_task("update_sector_fund_flow", update_sector_fund_flow, db)
    results["shareholder_count"] = _safe_task("update_shareholder_count", update_shareholder_count, db)
    results["quarterly_financials"] = _safe_task("update_quarterly_financials", update_quarterly_financials, db, loader)
    results["historical_valuation"] = _safe_task("update_historical_valuation", update_historical_valuation, db)
    results["sector_industry"] = _safe_task("update_sector_industry", update_sector_industry, db)
    results["industry"] = _safe_task("update_industry", update_industry, db)
    results["retry"] = _safe_task("retry_failed", retry_failed, db, loader)
    results["health"] = _safe_task("health_check", health_check, db)

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
            "update_stock_list",
            "update_bars",
            "update_indicators",
            "update_fundamentals",
            "update_market_snapshot",
            "update_fund_flow",
            "update_margin_trading",
            "update_dragon_tiger",
            "update_block_trade",
            "update_sector_fund_flow",
            "update_shareholder_count",
            "update_quarterly_financials",
            "update_historical_valuation",
            "update_sector_industry",
            "update_industry",
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
        def _should_update():
            return True

    if args.task == "all":
        run_all(db, loader, engine, resume=args.resume)
    elif args.task == "update_stock_list":
        update_stock_list(db)
    elif args.task == "update_bars":
        update_bars(db, loader, limit=args.limit, resume=args.resume)
    elif args.task == "update_indicators":
        update_indicators(db, engine)
    elif args.task == "update_fundamentals":
        update_fundamentals(db, loader)
    elif args.task == "update_market_snapshot":
        update_market_snapshot(db)
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
    elif args.task == "update_quarterly_financials":
        update_quarterly_financials(db, loader)
    elif args.task == "update_historical_valuation":
        update_historical_valuation(db)
    elif args.task == "update_sector_industry":
        update_sector_industry(db)
    elif args.task == "update_industry":
        update_industry(db)
    elif args.task == "retry":
        retry_failed(db, loader)
    elif args.task == "health_check":
        health_check(db)


if __name__ == "__main__":
    main()
