"""
SmartMoney 日常数据管道（解耦版 + 断点续传）
─────────────────────────────────────────────
职责：自动化每日数据更新、指标计算、质量监控
      支持断点续传：中断后重新运行自动从断点继续

架构：
  daily_pipeline ──▶ interface (抽象接口) ──▶ providers (适配层) ──▶ smartmoney_hunter (具体实现)
                    core/ (基础设施)          tasks/ (业务任务)

用法：
    python daily_pipeline.py --task update_bars
    python daily_pipeline.py --task update_bars --resume
    python daily_pipeline.py --task health_check
"""
from __future__ import annotations

import argparse
import datetime as _datetime_module  # noqa: F401 — re-export for test patches
import logging
import os
import sqlite3
import sys
import time
from datetime import datetime, timedelta  # noqa: F401 — timedelta exposed for test patches
from pathlib import Path
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

# 将 ~/Code 加入 Python 路径（使 pipeline 能 import smartmoney_hunter）
_CODE_DIR = os.path.expanduser("~/Code")
if _CODE_DIR not in sys.path:
    sys.path.insert(0, _CODE_DIR)
_HUNTER_SRC = os.path.expanduser("~/Code/quant_hunter/src")
if _HUNTER_SRC not in sys.path and os.path.isdir(_HUNTER_SRC):
    sys.path.insert(0, _HUNTER_SRC)

# ── Core module re-exports ──
from core.calendar import get_expected_latest_trading_day
from core.config import (
    BATCH_SIZE_VAL as BATCH_SIZE,  # noqa: F401
)
from core.config import (
    DB_PATH as DEFAULT_DB_PATH,
)
from core.config import (
    MAX_RETRY_VAL as MAX_RETRY,  # noqa: F401
)
from core.config import (
    PARALLEL_WORKERS_VAL as PARALLEL_WORKERS,
)
from core.config import (
    RETRY_DELAY_VAL as RETRY_DELAY,  # noqa: F401
)
from core.config import (
    SHARED_DATA_DIR,  # noqa: F401
    read_cross_source_config,
)
from core.lock import ProcessLock, TaskLock
from core.monitor import AkShareMonitor  # noqa: F401
from core.progress import ProgressTracker  # noqa: F401
from core.refresh import (
    CrossSourceCheckConfig,
    CrossSourceVerifier,
    RefreshAdapter,
    RefreshContext,
    RefreshOrchestrator,
)
from core.refresh_adapters import build_all_refresh_adapters
from core.refresh_audit import CrossSourceTolerance
from core.refresh_cross_source import XueqiuCrossSourceVerifier
from core.refresh_store import SQLiteRefreshStore
from core.runner import safe_task
from core.task_registry import Cadence, lookup_task, refreshable_trading_tasks
from core.task_result import TaskResult, normalize_task_result
from core.utils import (
    infer_market as _infer_market,  # noqa: F401
)
from core.utils import (
    is_trading_day as _is_trading_day,  # noqa: F401
)
from core.utils import (
    lower_process_priority,
    should_update,
)
from interface import (
    DatabaseInterface,
    DataLoaderInterface,
    IndicatorEngineInterface,
    ProviderFactory,
)

# ── Task module re-exports ──
from tasks.bars import _update_single_bar, update_bars  # noqa: F401
from tasks.china_macro import update_china_macro
from tasks.concept_board import update_concept_board, update_concept_member
from tasks.convertible_bond import (
    update_cb_index,
    update_cb_quotation,
    update_cb_redeem,
)
from tasks.core_chain import (
    update_chip_distribution,
    update_indicators,
    update_stock_list,
)

# ── New extended tasks ──
from tasks.corporate_actions import (
    update_earnings_forecast,
    update_restricted_share,
)
from tasks.finance_flow import (
    update_ah_premium,
    update_etf_daily,
    update_south_flow,
)
from tasks.financial_history import update_financial_history
from tasks.financials import (
    update_industry,
    update_quarterly_financials,
    update_shareholder_count,
)
from tasks.futures import update_futures
from tasks.index_chain import (
    update_chip_distribution_em,
    update_chip_distribution_em_fullmarket,
    update_index_daily,
)
from tasks.index_membership import update_index_membership
from tasks.institution_survey import update_institution_survey
from tasks.macro import (
    update_crude_oil,
    update_dividend_summary,
    update_global_index,
    update_gold_price,
    update_limit_up_down,
    update_north_flow,
    update_north_hold,
    update_us_treasury,
    update_usd,
)
from tasks.market_flow import (
    _fetch_sector_fund_flow,  # noqa: F401
    update_block_trade,
    update_dragon_tiger,
    update_fund_flow,
    update_margin_trading,
    update_sector_fund_flow,
)
from tasks.market_valuation import update_market_valuation
from tasks.money_market import update_money_market
from tasks.option_sentiment import update_option_sentiment
from tasks.sector_derivatives import update_sector_derivatives
from tasks.stock_pledge import update_stock_pledge
from tasks.stock_repurchase import update_stock_repurchase
from tasks.utility import health_check, retry_failed
from tasks.valuation_chain import (
    update_fundamentals,
    update_historical_valuation,
    update_market_snapshot,
    update_sector_industry,
)

# ── Third-party (re-export for test patches) ──
try:
    import akshare as ak
except ImportError:
    ak = None  # type: ignore[assignment]

from smartmoney_hunter.market_utils import is_beijing_stock  # noqa: F401

logger = logging.getLogger(__name__)

_SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")

# ── Backward compat aliases ──
_acquire_lock = ProcessLock.acquire
_release_lock = ProcessLock.release
_safe_task = safe_task
_lower_process_priority = lower_process_priority
_should_update = should_update

# ── Registry-backed task callable map ─────────────────────────────────
# Maps CLI task names to their callable functions, enabling the registry
# to drive task routing instead of the hardcoded if-elif chain.
_TASK_CALLABLES: dict[str, Any] = {
    "update_stock_list": update_stock_list,
    "update_bars": update_bars,
    "update_indicators": update_indicators,
    "update_fundamentals": update_fundamentals,
    "update_market_snapshot": update_market_snapshot,
    "update_fund_flow": update_fund_flow,
    "update_margin_trading": update_margin_trading,
    "update_dragon_tiger": update_dragon_tiger,
    "update_block_trade": update_block_trade,
    "update_sector_fund_flow": update_sector_fund_flow,
    "update_shareholder_count": update_shareholder_count,
    "update_quarterly_financials": update_quarterly_financials,
    "update_historical_valuation": update_historical_valuation,
    "update_sector_industry": update_sector_industry,
    "update_industry": update_industry,
    "update_north_flow": update_north_flow,
    "update_north_hold": update_north_hold,
    "update_south_flow": update_south_flow,
    "update_ah_premium": update_ah_premium,
    "update_etf_daily": update_etf_daily,
    "update_index_daily": update_index_daily,
    "update_cb_quotation": update_cb_quotation,
    "update_cb_redeem": update_cb_redeem,
    "update_cb_index": update_cb_index,
    "update_restricted_share": update_restricted_share,
    "update_earnings_forecast": update_earnings_forecast,
    "update_limit_up_down": update_limit_up_down,
    "update_dividend_summary": update_dividend_summary,
    "update_china_macro": update_china_macro,
    "update_money_market": update_money_market,
    "update_gold_price": update_gold_price,
    "update_crude_oil": update_crude_oil,
    "update_usd": update_usd,
    "update_global_index": update_global_index,
    "update_us_treasury": update_us_treasury,
    "update_futures": update_futures,
    "update_concept_board": update_concept_board,
    "update_concept_member": update_concept_member,
    "update_market_valuation": update_market_valuation,
    "update_sector_derivatives": update_sector_derivatives,
    "update_option_sentiment": update_option_sentiment,
    "update_stock_repurchase": update_stock_repurchase,
    "update_institution_survey": update_institution_survey,
    "update_stock_pledge": update_stock_pledge,
    "update_chip_distribution": update_chip_distribution,
    "update_chip_distribution_em": update_chip_distribution_em,
    "update_chip_distribution_em_fullmarket": update_chip_distribution_em_fullmarket,
    "update_financial_history": update_financial_history,
    "update_index_membership": update_index_membership,
    "retry": retry_failed,
    "health_check": health_check,
}


def _run_registry_task(
    task_name: str,
    db: DatabaseInterface,
    loader: DataLoaderInterface | None = None,
    engine: IndicatorEngineInterface | None = None,
    *,
    symbols: list[str] | None = None,
    limit: int | None = None,
    resume: bool = False,
    force: bool = False,
) -> Any:
    fn = _TASK_CALLABLES.get(task_name)
    if fn is None:
        raise ValueError(f"unknown task: {task_name}")

    if task_name == "update_indicators":
        if force or symbols:
            return _dispatch_indicators_force(fn, db, engine, symbols)
        return _safe_task(task_name, fn, db, engine)

    if task_name == "update_chip_distribution":
        if force or symbols:
            return _dispatch_chip_force(fn, db, symbols)
        return _safe_task(task_name, fn, db)

    if task_name in ("update_bars",):
        return _safe_task(
            task_name, fn, db, loader,
            limit=limit, resume=resume, symbols=symbols, force=force,
        )

    if task_name == "update_daily_core":
        # 编排器：内部各任务已各自经过 safe_task，不再包一层
        return fn(db, loader, engine, resume=resume, force=force)

    if task_name in ("update_fundamentals", "update_fund_flow", "update_quarterly_financials"):
        return _safe_task(task_name, fn, db, loader, symbols=symbols)

    if task_name in (
        "update_margin_trading", "update_dragon_tiger",
        "update_block_trade", "update_shareholder_count",
        "update_historical_valuation",
    ):
        return _safe_task(task_name, fn, db, symbols=symbols)

    if task_name == "retry":
        return _safe_task(task_name, fn, db, loader)

    # safe_task 负责创建 ingestion_runs 审计父行并注入 _task_run_id，
    # 使 PIT 表的 snapshot_run_id 外键在单任务模式下同样有父行可引用
    return _safe_task(task_name, fn, db)


def _dispatch_indicators_force(
    fn: Any, db: DatabaseInterface, engine: IndicatorEngineInterface,
    symbols: list[str] | None,
) -> Any:
    conn_kw = sqlite3.connect(str(db.db_path))
    if symbols:
        target_symbols = symbols
    else:
        target_symbols = [
            row[0] for row in
            conn_kw.execute("SELECT DISTINCT ts_code FROM daily_bars ORDER BY ts_code")
        ]
    conn_kw.close()
    logger.info(f"🔁 强制/指定股票模式：重算 {len(target_symbols)} 只股票的技术指标")
    return fn(db, engine, symbols_to_update=target_symbols)


def _dispatch_chip_force(
    fn: Any, db: DatabaseInterface, symbols: list[str] | None,
) -> Any:
    conn_kw = sqlite3.connect(str(db.db_path))
    if symbols:
        target_symbols = symbols
    else:
        target_symbols = [
            row[0] for row in
            conn_kw.execute("SELECT DISTINCT ts_code FROM daily_bars ORDER BY ts_code")
        ]
    conn_kw.close()
    logger.info(f"🔁 强制/指定股票模式：重算 {len(target_symbols)} 只股票的筹码分布")
    return fn(db, symbols_to_update=target_symbols)


# ===========================================================================
# 主流程编排
# ===========================================================================



def update_daily_core(
    db: DatabaseInterface,
    loader: DataLoaderInterface,
    engine: IndicatorEngineInterface,
    resume: bool = False,
    force: bool = False,
) -> dict:
    """Run all TRADING_DAY and DAILY cadence tasks (the daily core)."""
    logger.info("\n🚀 启动每日核心任务 (TRADING_DAY + DAILY)")
    return run_all(
        db,
        loader,
        engine,
        resume=resume,
        force=force,
        target_cadences={Cadence.TRADING_DAY, Cadence.DAILY},
    )


_TASK_CALLABLES["update_daily_core"] = update_daily_core


def run_all(
    db: DatabaseInterface,
    loader: DataLoaderInterface,
    engine: IndicatorEngineInterface,
    resume: bool = False,
    force: bool = False,
    target_cadences: set[Cadence] | None = None,
) -> dict:
    """运行完整数据管道。"""
    start_time = time.time()
    _lower_process_priority()

    def _run_task(name: str, fn, *args, **kwargs):
        if target_cadences is not None:
            spec = lookup_task(name)
            if spec is None:
                # 未注册任务不允许静默跳过：显式失败暴露注册遗漏
                logger.error("❌ 任务未注册于 TASK_REGISTRY，无法按 cadence 过滤: %s", name)
                return {"status": "failed", "reason": f"task not in registry: {name}"}
            # ON_DEMAND 为运维型任务（retry / health_check），始终随管道执行
            if spec.cadence is not Cadence.ON_DEMAND and spec.cadence not in target_cadences:
                logger.info("⏭️ 跳过任务: %s (cadence=%s)", name, spec.cadence)
                return {"status": "skipped", "reason": f"cadence not in {target_cadences}"}
        return _safe_task(name, fn, *args, **kwargs)

    logger.info("\n🚀 SmartMoney 每日数据管道启动")
    logger.info(f"📂 数据库: {db.db_path}")
    logger.info(f"⚙️  并行线程: {PARALLEL_WORKERS} (默认 1=串行)")
    logger.info(f"📅 今天: {datetime.now().strftime('%Y-%m-%d')}")

    if not _should_update():
        db.close()
        return {"status": "skipped", "reason": "非交易日"}

    results = {}
    results["stock_list"] = _run_task("update_stock_list", update_stock_list, db)
    results["bars"] = _run_task("update_bars", update_bars, db, loader, resume=resume, force=force)

    # 总是调用 update_indicators。由于优化了智能探测，即使 bars 更新了0只，
    # 也会在 <0.1 秒内判断出无须计算并跳过，同时能保证修复任何因中断而缺失指标的股票。
    results["indicators"] = _run_task("update_indicators", update_indicators, db, engine)

    results["fundamentals"] = _run_task("update_fundamentals", update_fundamentals, db, loader)
    results["market_snapshot"] = _run_task("update_market_snapshot", update_market_snapshot, db)
    results["fund_flow"] = _run_task("update_fund_flow", update_fund_flow, db, loader)
    results["margin_trading"] = _run_task("update_margin_trading", update_margin_trading, db)
    results["dragon_tiger"] = _run_task("update_dragon_tiger", update_dragon_tiger, db)
    results["block_trade"] = _run_task("update_block_trade", update_block_trade, db)
    results["sector_fund_flow"] = _run_task("update_sector_fund_flow", update_sector_fund_flow, db)
    results["shareholder_count"] = _run_task("update_shareholder_count", update_shareholder_count, db)
    results["quarterly_financials"] = _run_task("update_quarterly_financials", update_quarterly_financials, db, loader)
    results["historical_valuation"] = _run_task("update_historical_valuation", update_historical_valuation, db)
    results["sector_industry"] = _run_task("update_sector_industry", update_sector_industry, db)
    results["industry"] = _run_task("update_industry", update_industry, db)
    results["north_flow"] = _run_task("update_north_flow", update_north_flow, db)
    results["north_hold"] = _run_task("update_north_hold", update_north_hold, db)
    results["index_daily"] = _run_task("update_index_daily", update_index_daily, db)
    results["limit_up_down"] = _run_task("update_limit_up_down", update_limit_up_down, db)
    results["dividend_summary"] = _run_task("update_dividend_summary", update_dividend_summary, db)
    results["gold_price"] = _run_task("update_gold_price", update_gold_price, db)
    results["crude_oil"] = _run_task("update_crude_oil", update_crude_oil, db)
    results["fx_rate"] = _run_task("update_usd", update_usd, db)
    results["global_index"] = _run_task("update_global_index", update_global_index, db)
    results["us_treasury"] = _run_task("update_us_treasury", update_us_treasury, db)
    results["futures"] = _run_task("update_futures", update_futures, db)
    results["china_macro"] = _run_task("update_china_macro", update_china_macro, db)
    results["money_market"] = _run_task("update_money_market", update_money_market, db)
    results["market_valuation"] = _run_task("update_market_valuation", update_market_valuation, db)
    results["concept_board"] = _run_task("update_concept_board", update_concept_board, db)

    # ── Phase 2: 事件型强信号 ──
    results["option_sentiment"] = _run_task("update_option_sentiment", update_option_sentiment, db)
    results["stock_repurchase"] = _run_task("update_stock_repurchase", update_stock_repurchase, db)
    results["institution_survey"] = _run_task("update_institution_survey", update_institution_survey, db)
    results["stock_pledge"] = _run_task("update_stock_pledge", update_stock_pledge, db)

    # ── 新增衍生数据任务 ──
    results["south_flow"] = _run_task("update_south_flow", update_south_flow, db)
    results["ah_premium"] = _run_task("update_ah_premium", update_ah_premium, db)
    results["etf_daily"] = _run_task("update_etf_daily", update_etf_daily, db)
    results["cb_quotation"] = _run_task("update_cb_quotation", update_cb_quotation, db)
    results["cb_redeem"] = _run_task("update_cb_redeem", update_cb_redeem, db)
    results["cb_index"] = _run_task("update_cb_index", update_cb_index, db)
    results["restricted_share"] = _run_task("update_restricted_share", update_restricted_share, db)
    results["earnings_forecast"] = _run_task("update_earnings_forecast", update_earnings_forecast, db)
    results["sector_derivatives"] = _run_task("update_sector_derivatives", update_sector_derivatives, db)
    results["chip_distribution"] = _run_task(
        "update_chip_distribution", update_chip_distribution, db
    )
    results["chip_distribution_em"] = _run_task(
        "update_chip_distribution_em", update_chip_distribution_em, db
    )
    results["financial_history"] = _run_task(
        "update_financial_history", update_financial_history, db
    )
    results["index_membership"] = _run_task(
        "update_index_membership", update_index_membership, db
    )
    results["concept_member"] = _run_task(
        "update_concept_member", update_concept_member, db
    )

    results["retry"] = _run_task("retry", retry_failed, db, loader)
    results["health"] = _run_task("health_check", health_check, db)

    db.close()
    elapsed = time.time() - start_time
    logger.info("\n" + "=" * 60)
    logger.info("🏁 数据管道全部完成")
    logger.info(f"⏱️  总耗时: {elapsed:.1f}s ({elapsed/60:.1f}min)")
    logger.info("=" * 60)

    # 检查是否有任务失败，供 main() 决定退出码
    # 使用 TaskResult.exit_failure 语义：degraded / failed / aborted
    results["crashed"] = any(
        isinstance(v, dict)
        and v.get("status") in {"degraded", "failed", "aborted"}
        for v in results.values()
    )
    return results


# ===========================================================================
# 收盘刷新（--refresh-today）
# ===========================================================================

def _build_refresh_adapters(
    *,
    db: DatabaseInterface,
    loader: DataLoaderInterface,
    engine: IndicatorEngineInterface,
    store: SQLiteRefreshStore,
) -> dict[str, RefreshAdapter]:
    """任务名 → 刷新适配器映射；装配全部 29 个收盘刷新适配器。"""
    return build_all_refresh_adapters(db=db, loader=loader, engine=engine, store=store)


def _build_refresh_orchestrator(
    db_path: str,
    *,
    cross_source_verifier: CrossSourceVerifier | None = None,
) -> RefreshOrchestrator:
    """构建默认的收盘刷新编排器（非缓存 loader + SQLite 审计存储）。

    跨源抽样校验由 REFRESH_CROSS_SOURCE 门控（默认关闭）；开启时若未注入
    verifier，则装配默认的只读雪球 verifier（注入优先，供测试接缝）；
    report_only 默认真，首次启用即观察模式，只记录不降级。
    """
    # 调用时读取环境变量（非 import 期固化），便于测试翻转开关
    cross_cfg = read_cross_source_config()
    db = ProviderFactory.get_db()
    engine = ProviderFactory.get_indicator_engine()
    # 收盘刷新必须绕过本地缓存，确保拉到收盘后的最终数据
    loader = ProviderFactory.get_loader(use_cache=False)
    # 适配器与编排器共用同一 store 实例：前者发布数据，后者记录审计
    store = SQLiteRefreshStore(db_path)
    build_kwargs: dict[str, Any] = {}
    if cross_cfg.enabled:
        build_kwargs["cross_source"] = CrossSourceCheckConfig(
            task_name=cross_cfg.task_name,
            tolerance=CrossSourceTolerance(
                price=cross_cfg.price_tol, volume=cross_cfg.volume_tol
            ),
            sample_size=cross_cfg.sample_size,
            report_only=cross_cfg.report_only,
        )
        # 注入的 verifier（测试接缝）优先；否则装配默认的只读雪球 verifier
        build_kwargs["verifier"] = cross_source_verifier or XueqiuCrossSourceVerifier(
            db_path=db_path
        )
    return RefreshOrchestrator(
        specs=refreshable_trading_tasks(),
        adapters=_build_refresh_adapters(
            db=db, loader=loader, engine=engine, store=store
        ),
        store=store,
        **build_kwargs,
    )


def run_close_refresh(
    db_path: str,
    *,
    symbols: list[str] | None = None,
    force: bool = False,
    orchestrator: RefreshOrchestrator | None = None,
    cross_source_verifier: CrossSourceVerifier | None = None,
) -> TaskResult:
    """执行一次收盘后刷新，返回聚合 TaskResult。

    16:00 前的拦截由编排器自身的 pre-close 闸门完成，
    --force 仅映射为 allow_pre_close=True，不在 CLI 层重复门控逻辑。
    """
    if orchestrator is None:
        orchestrator = _build_refresh_orchestrator(
            db_path, cross_source_verifier=cross_source_verifier
        )
    # 单次读取上海时钟：闸门、started_at 与目标交易日共用同一时间基准
    now = datetime.now(_SHANGHAI_TZ)
    context = RefreshContext(
        target_date=get_expected_latest_trading_day(now=now),
        started_at=now,
        run_id=str(uuid4()),
        # 保留 None（全市场）与 ()（no-op）的区分：空列表绝不得升级为全市场
        symbols=None if symbols is None else tuple(symbols),
        allow_pre_close=force,
    )
    return orchestrator.run(context)


# ===========================================================================
# CLI 入口
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(description="SmartMoney 日常数据管道（解耦版 + 断点续传）")
    parser.add_argument(
        "--task",
        default=None,
        help="要执行的任务 (默认: all)",
    )
    parser.add_argument(
        "--refresh-today",
        action="store_true",
        help="收盘后刷新当日数据（与 --task / --resume 互斥）",
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
    parser.add_argument(
        "--symbols",
        type=str,
        default=None,
        help="逗号分隔的股票代码列表，或包含一行一个代码的文件路径。指定后只处理这些股票。",
    )

    args = parser.parse_args()

    # --refresh-today 是独立的顶层模式；未指定任何模式时仍走 legacy all
    if args.refresh_today and args.task is not None:
        parser.error("--task 不能与 --refresh-today 同时使用")
    if args.refresh_today and args.resume:
        parser.error("--resume 不能与 --refresh-today 同时使用")
    task = args.task if args.task is not None else "all"

    symbols_arg = args.symbols
    symbols: list[str] | None = None
    if symbols_arg is not None:
        # 空字符串不得进入文件分支：Path("") 会解析为当前目录
        if symbols_arg and Path(symbols_arg).exists():
            symbols = [
                line.strip()
                for line in Path(symbols_arg).read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
        else:
            symbols = [s.strip() for s in symbols_arg.split(",") if s.strip()]
        # 显式给出 --symbols 却解析为空：输入必然有误，禁止静默回退为全市场
        if not symbols:
            parser.error("--symbols 已指定但未解析出任何有效代码，请检查输入")

    if args.refresh_today:
        # 与 legacy all 共用同一把全局写锁，避免与常规管道并发写库
        _acquire_lock()
        try:
            ProviderFactory.configure(db_path=args.db_path, provider="smartmoney")
            result = run_close_refresh(args.db_path, symbols=symbols, force=args.force)
        except KeyboardInterrupt:
            # 刷新路径不同于 legacy：中断必须非零退出，供调度/TUI 感知 aborted
            logger.info("收到中断信号，正在退出...")
            sys.exit(1)
        if result.exit_failure:
            sys.exit(1)
        return

    task_lock_name: str | None = None
    db = None
    try:
        # 进程锁：all 任务使用全局锁；single task 使用按任务名锁，
        # 允许不同任务并行，避免 TUI 连续启动多个 single task 时互相冲突。
        if task in ("all", "update_daily_core"):
            _acquire_lock()
        elif task != "health_check":
            if not TaskLock.acquire(task):
                print(f"❌ 任务 {task} 已在运行，请勿重复启动")
                sys.exit(1)
            task_lock_name = task

        # 初始化 provider
        ProviderFactory.configure(db_path=args.db_path, provider="smartmoney")
        db = ProviderFactory.get_db()

        loader = ProviderFactory.get_loader()
        engine = ProviderFactory.get_indicator_engine()

        if args.force:
            global _should_update
            def _should_update():
                return True

        if task == "all":
            results = run_all(db, loader, engine, resume=args.resume, force=args.force)
            if results.get("crashed"):
                sys.exit(1)
        elif task == "update_daily_core":
            results = update_daily_core(db, loader, engine, resume=args.resume, force=args.force)
            if results.get("crashed"):
                sys.exit(1)
        elif task in _TASK_CALLABLES:
            raw = _run_registry_task(
                task, db, loader, engine,
                symbols=symbols, limit=args.limit,
                resume=args.resume, force=args.force,
            )
            if isinstance(raw, dict | TaskResult):
                result = normalize_task_result(task, raw)
                if result.exit_failure:
                    sys.exit(1)
        else:
            logger.error("未知任务: %s", task)
            sys.exit(1)
    except KeyboardInterrupt:
        logger.info("收到中断信号，正在退出...")
    finally:
        if db is not None:
            db.close()
        if task_lock_name:
            TaskLock.release(task_lock_name)


if __name__ == "__main__":
    main()
