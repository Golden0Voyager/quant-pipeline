"""
SmartMoney 日常数据管道（解耦版 + 断点续传）
─────────────────────────────────────────────
职责：自动化每日数据更新、指标计算、质量监控
      支持断点续传：中断后重新运行自动从断点继续

架构：
  daily_pipeline ──▶ interface (抽象接口) ──▶ providers (适配层) ──▶ smartmoney_hunter (具体实现)
                    core/ (基础设施)          tasks/ (业务任务)

用法：
    python daily_pipeline.py --task daily      # 每日层（TRADING_DAY+DAILY+ON_DEMAND）
    python daily_pipeline.py --task all        # daily 的兼容别名
    python daily_pipeline.py --task update_daily_core  # daily 的兼容别名
    python daily_pipeline.py --task weekly_backfill    # 每周层：WEEKLY 任务+补齐缺漏+retry+health（手动触发）
    python daily_pipeline.py --task monthly_repair     # 每月层：MONTHLY/QUARTERLY 任务+备份→对账→vacuum 修复链+health（手动触发）
    python daily_pipeline.py --task update_bars
    python daily_pipeline.py --task update_bars --resume
    python daily_pipeline.py --task health_check
    python daily_pipeline.py --backfill-days                    # 回补登记册里仍缺的整日空洞
    python daily_pipeline.py --backfill-days 2026-09-14,2026-09-16
    python daily_pipeline.py --backfill-table                     # 表格级：全部可回补表 × 登记册缺失日
    python daily_pipeline.py --backfill-table limit_up_down       # 单表 × 登记册缺失日
    python daily_pipeline.py --backfill-table limit_up_down 2026-09-14
"""
from __future__ import annotations

import argparse
import datetime as _datetime_module  # noqa: F401 — re-export for test patches
import logging
import os
import sqlite3
import subprocess
import sys
import time
from collections.abc import Callable
from datetime import datetime, timedelta  # noqa: F401 — timedelta exposed for test patches
from pathlib import Path
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

# 兄弟仓库（`smartmoney_hunter`）的路径由 `core._bootstrap` 统一注入。本文件后面有
# 模块级 `from smartmoney_hunter ...`，作为入口这里显式调一次，不依赖「core 先被导入」。
from core._bootstrap import ensure_sibling_paths

ensure_sibling_paths()

# ── Core module re-exports ──
from core.backfill import backfill_task_by_table, resolve_backfill_days, resolve_backfill_targets
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
    PARALLEL_WORKERS_VAL as PARALLEL_WORKERS,  # noqa: F401
)
from core.config import (
    RETRY_DELAY_VAL as RETRY_DELAY,  # noqa: F401
)
from core.config import (
    SHARED_DATA_DIR,  # noqa: F401
    read_cross_source_config,
)
from core.db_pragmas import truncate_wal
from core.freshness import compute_catch_up_tasks, get_latest_dates
from core.lock import ProcessLock, TaskLock, global_lock_held
from core.monitor import AkShareMonitor  # noqa: F401
from core.netcheck import is_online
from core.notifications import notify_all
from core.parallel_runner import ParallelTask, run_parallel_tasks
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
from core.refresh_store import (
    ResumedTaskOutcome,
    SQLiteRefreshStore,
    load_interrupted_refresh,
)
from core.run_state import mark_run_completed, mark_run_started
from core.runner import safe_task
from core.task_registry import TASK_REGISTRY, Cadence, lookup_task, refreshable_trading_tasks
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
from tasks.cftc_cot import update_cftc_cot
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
from tasks.eia_petroleum import update_eia_petroleum
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
from tasks.fund_holdings import update_fund_holdings
from tasks.futures import update_futures
from tasks.global_assets import update_global_assets
from tasks.hk_tech_index import update_hk_tech_index
from tasks.hot_rank import update_hot_rank
from tasks.index_chain import (
    update_chip_distribution_em,
    update_chip_distribution_em_fullmarket,
    update_index_daily,
)
from tasks.index_membership import update_index_membership
from tasks.institution_survey import update_institution_survey
from tasks.lithium_spot import update_lithium_spot
from tasks.macro import (
    update_dividend_summary,
    update_global_index,
    update_gold_price,
    update_limit_up_down,
    update_north_hold,
    update_us_treasury,
    update_usd,
)
from tasks.market_breadth import update_market_breadth
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
from tasks.placement import update_placement_announcements
from tasks.sector_derivatives import (
    fetch_sector_valuation_records,
    update_sector_derivatives,
)
from tasks.stock_comment import update_stock_comment
from tasks.stock_pledge import update_stock_pledge
from tasks.stock_repurchase import update_stock_repurchase
from tasks.top10_shareholders import update_top10_shareholders
from tasks.us_macro import update_us_macro
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
    ak = None

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
    "update_north_hold": update_north_hold,
    "update_fund_holdings": update_fund_holdings,
    "update_top10_shareholders": update_top10_shareholders,
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
    "update_stock_comment": update_stock_comment,
    "update_hot_rank": update_hot_rank,
    "update_dividend_summary": update_dividend_summary,
    "update_china_macro": update_china_macro,
    "update_money_market": update_money_market,
    "update_gold_price": update_gold_price,
    "update_usd": update_usd,
    "update_global_index": update_global_index,
    "update_us_treasury": update_us_treasury,
    "update_us_macro": update_us_macro,
    "update_hk_tech_index": update_hk_tech_index,
    "update_cftc_cot": update_cftc_cot,
    "update_eia_petroleum": update_eia_petroleum,
    "update_lithium_spot": update_lithium_spot,
    "update_futures": update_futures,
    "update_global_assets": update_global_assets,
    "update_concept_board": update_concept_board,
    "update_concept_member": update_concept_member,
    "update_market_valuation": update_market_valuation,
    "update_market_breadth": update_market_breadth,
    "update_sector_derivatives": update_sector_derivatives,
    "update_option_sentiment": update_option_sentiment,
    "update_stock_repurchase": update_stock_repurchase,
    "update_placement_announcements": update_placement_announcements,
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
    health_fast: bool = False,
) -> Any:
    fn = _TASK_CALLABLES.get(task_name)
    if fn is None:
        raise ValueError(f"unknown task: {task_name}")

    # 盘中门禁：交易日抓取任务在盘中/结算窗口写入的是实时快照，
    # 会被后续"当日已存在"类守卫冻结（2026-07-30 事故）。
    # 单任务/TUI/守护进程都经过此处，与 run_all 共用 _should_update 判定；
    # --force 保留逃生门（写入端仍有丢弃当日行的兜底）。
    # 本地衍生计算任务（从已入库数据推导，不抓数据源）不受门禁限制。
    derived_compute_tasks = {"update_indicators", "update_chip_distribution"}
    spec = lookup_task(task_name)
    if (
        spec is not None
        and spec.cadence is Cadence.TRADING_DAY
        and task_name not in derived_compute_tasks
        and not force
        and not _should_update()
    ):
        return TaskResult.no_data(
            task_name,
            reason="盘中/结算窗口不执行交易日抓取任务（上海时间 16:00 后自动放行，--force 跳过）",
        ).to_dict()

    if task_name == "update_indicators":
        if (force or symbols) and engine is not None:
            return _dispatch_indicators_force(fn, db, engine, symbols)
        return _safe_task(task_name, fn, db, engine)

    if task_name == "update_chip_distribution":
        if force or symbols:
            return _dispatch_chip_force(fn, db, symbols)
        return _safe_task(task_name, fn, db)

    if task_name == "update_chip_distribution_em":
        # --symbols 透传给筹码任务；此前无此分支，落入兜底 _safe_task 后
        # symbols 被丢弃，任务总是走自动探测（自选股+指数成分 ~1800 只）。
        # ts_code 在 quant_core.db 中为 6 位裸码，去掉交易所后缀防止错位写入。
        if symbols:
            bare = [s.split(".")[0] for s in symbols]
            return _safe_task(task_name, fn, db, symbols_to_update=bare)
        return _safe_task(task_name, fn, db)

    if task_name in ("update_bars",):
        return _safe_task(
            task_name, fn, db, loader,
            limit=limit, resume=resume, symbols=symbols, force=force,
        )

    if task_name == "health_check":
        # weekly/monthly 批处理末尾传 health_fast=True 走 page_count 估算；
        # 手动 --task health_check 不传该参数，保持精确 COUNT(*) 口径
        return _safe_task(task_name, fn, db, fast=health_fast)

    if task_name == "update_daily_core":
        # 编排器：内部各任务已各自经过 safe_task，不再包一层
        return fn(db, loader, engine, resume=resume, force=force)

    if task_name in ("update_fundamentals", "update_fund_flow", "update_quarterly_financials"):
        return _safe_task(task_name, fn, db, loader, symbols=symbols)

    if task_name in (
        "update_margin_trading", "update_dragon_tiger",
        "update_block_trade", "update_shareholder_count",
        "update_historical_valuation", "update_placement_announcements",
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


# ── 历史回补（--backfill-days 整日级 / --backfill-table 表格级）──────────────────────────
# 只有**源端支持历史日期**的表才在这里；可回补/部分可回补/不可回补的分类与依据见
# core/backfill.py 的模块 docstring，且由 tests/test_backfill.py 门禁保证该分类
# 覆盖全部探针表（不留「没说过能不能补」的表）。limit_up_down 属部分可回补：
# 东财涨跌停池只留最近约 16 个交易日，窗口外靠同花顺 HiThink 兜底，更早的日子
# 补不回来，任务对此返回 no_data 而非 failed（见 tasks/macro.py::update_limit_up_down）。


def _backfill_sector_valuation(db: DatabaseInterface, target_date: str) -> dict:
    """板块估值的回补入口（回补专用审计名 ``update_sector_valuation``）。

    日常写入方是复合任务 ``update_sector_derivatives``（同时写 sector_daily /
    index_futures_basis），这里只回补 ``sector_valuation`` 单表，因此复用收盘刷新
    那套按日期取数的 ``fetch_sector_valuation_records``，不去跑整个复合任务。
    """
    records = fetch_sector_valuation_records(target_date)
    if not records:
        return {
            "skipped": True,
            "reason": "no sector valuation rows for the day",
            "total": 0,
        }
    saved = db.save_sector_valuation_batch(records)
    return {"saved": saved, "total": len(records)}


_BACKFILL_TASK_CALLABLES: dict[str, Any] = {
    "update_index_daily": update_index_daily,
    "update_limit_up_down": update_limit_up_down,
    "update_block_trade": update_block_trade,
    "update_sector_valuation": _backfill_sector_valuation,
}


def run_backfill_days(db: DatabaseInterface, spec: str | None = None) -> dict:
    """把整日缺席的历史交易日重跑一遍（只跑可回补的任务）。

    只处理**仍然缺席**的日期（已补上的跳过，幂等）；每个日期逐任务执行，全部经
    ``safe_task`` 落 ``ingestion_runs`` 审计——回补同样要留痕。

    单个任务失败不打断其余：回补是手动的一次性操作，把能补的都补上比提前退出
    更有用；失败数由调用方转成非零退出码。
    """
    conn = sqlite3.connect(str(db.db_path))
    try:
        plan = resolve_backfill_days(conn.cursor(), spec)
    finally:
        conn.close()

    logger.info("\n" + "=" * 60)
    logger.info("🧩 整日缺席回补（日期来源: %s）", plan.source)
    logger.info("=" * 60)
    for day, reason in plan.skipped:
        logger.info("  跳过 %s：%s", day, reason)

    if not plan.days:
        logger.info("✅ 没有需要回补的交易日")
        return {
            "days": [],
            "skipped": [list(item) for item in plan.skipped],
            "failed": 0,
            "saved": 0,
        }

    failures: list[str] = []
    total_saved = 0
    for day in plan.days:
        for task_name, fn in _BACKFILL_TASK_CALLABLES.items():
            result = _safe_task(task_name, fn, db, target_date=day)
            status = result.get("status")
            saved = result.get("saved") or 0
            if isinstance(saved, int):
                total_saved += saved
            if status in ("failed", "degraded", "aborted"):
                failures.append(f"{day}/{task_name}: {result.get('error') or status}")
            logger.info("  %s %s → %s (saved=%s)", day, task_name, status, saved)

    logger.info(
        "🧩 回补完成：%s 个交易日，合计 saved=%s，失败 %s 项",
        len(plan.days),
        total_saved,
        len(failures),
    )
    return {
        "days": list(plan.days),
        "skipped": [list(item) for item in plan.skipped],
        "failed": len(failures),
        "failures": failures,
        "saved": total_saved,
    }


def run_backfill_table(db: DatabaseInterface, spec: str | None = None) -> dict:
    """按 (表, 日) 粒度回补历史单表空洞（``--backfill-table``）。

    与 ``run_backfill_days`` 的区别只在幂等门：这里看的是「该表当日是否有行」
    而非「整天是否缺席」，专门解完日回补把混合形态的日子补齐之后，单表残留
    空洞永远过不了整天门的死角（2026-09-27 实证：6 个登记缺口日补齐 3–4 张表后
    再跑 ``--backfill-days`` 全部被跳过）。

    逐 (表, 日) 对执行 ``safe_task`` 落审计；单个失败不打断其余，失败数由调用方
    转成非零退出码。
    """
    conn = sqlite3.connect(str(db.db_path))
    try:
        plan = resolve_backfill_targets(conn.cursor(), spec)
    finally:
        conn.close()

    tasks_by_table = backfill_task_by_table()
    logger.info("\n" + "=" * 60)
    logger.info("🧩 表格级回补（日期来源: %s）", plan.source)
    logger.info("=" * 60)
    for target, reason in plan.skipped:
        label = "/".join(target) if isinstance(target, tuple) else str(target)
        logger.info("  跳过 %s：%s", label, reason)

    if not plan.targets:
        logger.info("✅ 没有需要回补的 (表, 交易日) 对")
        return {
            "targets": [],
            "skipped": [
                ["/".join(item) if isinstance(item, tuple) else item, reason]
                for item, reason in plan.skipped
            ],
            "failed": 0,
            "saved": 0,
        }

    failures: list[str] = []
    total_saved = 0
    for table, day in plan.targets:
        task_name = tasks_by_table.get(table)
        if task_name is None:  # 防御：resolve 层不应产生未注册表
            failures.append(f"{day}/{table}: 未知的回补表")
            continue
        result = _safe_task(task_name, _BACKFILL_TASK_CALLABLES[task_name], db, target_date=day)
        status = result.get("status")
        saved = result.get("saved") or 0
        if isinstance(saved, int):
            total_saved += saved
        if status in ("failed", "degraded", "aborted"):
            failures.append(f"{day}/{table}: {result.get('error') or status}")
        logger.info("  %s %s (%s) → %s (saved=%s)", day, table, task_name, status, saved)

    logger.info(
        "🧩 表格级回补完成：%s 个 (表, 交易日) 对，合计 saved=%s，失败 %s 项",
        len(plan.targets),
        total_saved,
        len(failures),
    )
    return {
        "targets": [list(item) for item in plan.targets],
        "skipped": [
            ["/".join(item) if isinstance(item, tuple) else item, reason]
            for item, reason in plan.skipped
        ],
        "failed": len(failures),
        "failures": failures,
        "saved": total_saved,
    }


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
    sequential: bool = False,
    parallel_workers: int | None = None,
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
        sequential=sequential,
        parallel_workers=parallel_workers,
    )


_TASK_CALLABLES["update_daily_core"] = update_daily_core


def run_all(
    db: DatabaseInterface,
    loader: DataLoaderInterface,
    engine: IndicatorEngineInterface,
    resume: bool = False,
    force: bool = False,
    target_cadences: set[Cadence] | None = None,
    sequential: bool = False,
    parallel_workers: int | None = None,
) -> dict:
    """运行完整数据管道（DAG 5 阶段编排：stage4 并发 min(workers,12)，stage2/3/5 串行）。"""
    start_time = time.time()
    _lower_process_priority()

    workers = (
        parallel_workers
        if parallel_workers is not None
        else PARALLEL_WORKERS
    )
    if sequential or os.getenv("PARALLEL_PIPELINE", "1").lower() in (
        "0",
        "false",
        "no",
    ):
        workers = 1

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

    def _create_task(name: str, fn, *args, **kwargs) -> tuple[ParallelTask | None, dict | None]:
        if target_cadences is not None:
            spec = lookup_task(name)
            if spec is None:
                logger.error("❌ 任务未注册于 TASK_REGISTRY，无法按 cadence 过滤: %s", name)
                return None, {"status": "failed", "reason": f"task not in registry: {name}"}
            if spec.cadence is not Cadence.ON_DEMAND and spec.cadence not in target_cadences:
                logger.info("⏭️ 跳过任务: %s (cadence=%s)", name, spec.cadence)
                return None, {"status": "skipped", "reason": f"cadence not in {target_cadences}"}
        return ParallelTask(name=name, fn=fn, args=args, kwargs=kwargs), None

    logger.info("\n🚀 SmartMoney 每日数据管道启动")
    logger.info(f"📂 数据库: {db.db_path}")
    logger.info(f"⚙️  并行线程: {workers} ("
                f"{'串行模式' if workers <= 1 else 'DAG 模式（stage4 并发，stage2 暂串行）'})")
    logger.info(f"📅 今天: {datetime.now().strftime('%Y-%m-%d')}")

    if not _should_update():
        db.close()
        return {"status": "skipped", "reason": "非交易日"}

    # 运行完整性：本轮开始时检查上一轮是否留下未完成的标记。
    # 只靠日志里缺少「🏁 数据管道全部完成」发现中断是无效的（没人读日志），
    # 2026-09-17 的 31 任务部分运行就是这样溜过去的——39 个任务全 success、
    # 无任何告警，只留下至今仍在的数据空洞。详见 core/run_state.py。
    today_str = datetime.now().strftime("%Y-%m-%d")
    abandoned_run = mark_run_started(db, today_str)
    if abandoned_run:
        logger.error("⛔ 上一轮管道未完整结束: %s 开始后没有完成记录", abandoned_run)
        notify_all(
            "error",
            "上一轮管道未完整结束",
            f"{abandoned_run} 的管道开始后没有完成记录（可能被中断），该轮数据可能不完整",
        )

    results: dict[str, Any] = {}

    # ── 第一阶段：核心行情链（bars 收盘后走快照播种，必须最先完成）──
    results["stock_list"] = _run_task("update_stock_list", update_stock_list, db)
    results["bars"] = _run_task("update_bars", update_bars, db, loader, resume=resume, force=force)
    results["retry"] = _run_task("retry", retry_failed, db, loader)

    if workers <= 1:
        # 串行模式（完全按原有顺序执行）
        results["indicators"] = _run_task("update_indicators", update_indicators, db, engine)
        results["chip_distribution"] = _run_task("update_chip_distribution", update_chip_distribution, db)
        results["fundamentals"] = _run_task("update_fundamentals", update_fundamentals, db, loader)
        results["market_snapshot"] = _run_task("update_market_snapshot", update_market_snapshot, db)
        results["historical_valuation"] = _run_task("update_historical_valuation", update_historical_valuation, db)
        results["sector_fund_flow"] = _run_task("update_sector_fund_flow", update_sector_fund_flow, db)
        results["sector_industry"] = _run_task("update_sector_industry", update_sector_industry, db)
        results["fund_flow"] = _run_task("update_fund_flow", update_fund_flow, db, loader)
        results["margin_trading"] = _run_task("update_margin_trading", update_margin_trading, db)
        results["dragon_tiger"] = _run_task("update_dragon_tiger", update_dragon_tiger, db)
        results["block_trade"] = _run_task("update_block_trade", update_block_trade, db)
        results["limit_up_down"] = _run_task("update_limit_up_down", update_limit_up_down, db)
        results["stock_comment"] = _run_task("update_stock_comment", update_stock_comment, db)
        results["hot_rank"] = _run_task("update_hot_rank", update_hot_rank, db)
        results["index_daily"] = _run_task("update_index_daily", update_index_daily, db)
        results["market_valuation"] = _run_task("update_market_valuation", update_market_valuation, db)
        results["market_breadth"] = _run_task("update_market_breadth", update_market_breadth, db)
        results["concept_board"] = _run_task("update_concept_board", update_concept_board, db)
        results["south_flow"] = _run_task("update_south_flow", update_south_flow, db)
        results["ah_premium"] = _run_task("update_ah_premium", update_ah_premium, db)
        results["etf_daily"] = _run_task("update_etf_daily", update_etf_daily, db)
        results["cb_quotation"] = _run_task("update_cb_quotation", update_cb_quotation, db)
        results["cb_redeem"] = _run_task("update_cb_redeem", update_cb_redeem, db)
        results["cb_index"] = _run_task("update_cb_index", update_cb_index, db)
        results["sector_derivatives"] = _run_task("update_sector_derivatives", update_sector_derivatives, db)
        results["option_sentiment"] = _run_task("update_option_sentiment", update_option_sentiment, db)
        results["stock_repurchase"] = _run_task("update_stock_repurchase", update_stock_repurchase, db)
        results["placement_announcements"] = _run_task(
            "update_placement_announcements", update_placement_announcements, db
        )
        results["institution_survey"] = _run_task("update_institution_survey", update_institution_survey, db)
        results["stock_pledge"] = _run_task("update_stock_pledge", update_stock_pledge, db)
        results["restricted_share"] = _run_task("update_restricted_share", update_restricted_share, db)
        results["earnings_forecast"] = _run_task("update_earnings_forecast", update_earnings_forecast, db)
        results["dividend_summary"] = _run_task("update_dividend_summary", update_dividend_summary, db)
        results["gold_price"] = _run_task("update_gold_price", update_gold_price, db)
        results["fx_rate"] = _run_task("update_usd", update_usd, db)
        results["global_index"] = _run_task("update_global_index", update_global_index, db)
        results["us_treasury"] = _run_task("update_us_treasury", update_us_treasury, db)
        results["us_macro"] = _run_task("update_us_macro", update_us_macro, db)
        results["hk_tech_index"] = _run_task("update_hk_tech_index", update_hk_tech_index, db)
        results["cftc_cot"] = _run_task("update_cftc_cot", update_cftc_cot, db)
        results["eia_petroleum"] = _run_task("update_eia_petroleum", update_eia_petroleum, db)
        results["lithium_spot"] = _run_task("update_lithium_spot", update_lithium_spot, db)
        results["futures"] = _run_task("update_futures", update_futures, db)
        results["china_macro"] = _run_task("update_china_macro", update_china_macro, db)
        results["money_market"] = _run_task("update_money_market", update_money_market, db)
        results["global_assets"] = _run_task("update_global_assets", update_global_assets, db)
        results["financial_history"] = _run_task("update_financial_history", update_financial_history, db)
        results["shareholder_count"] = _run_task("update_shareholder_count", update_shareholder_count, db)
        results["quarterly_financials"] = _run_task("update_quarterly_financials", update_quarterly_financials, db, loader)
        results["industry"] = _run_task("update_industry", update_industry, db)
        results["north_hold"] = _run_task("update_north_hold", update_north_hold, db)
        results["index_membership"] = _run_task("update_index_membership", update_index_membership, db)
        results["concept_member"] = _run_task("update_concept_member", update_concept_member, db)
        results["chip_distribution_em"] = _run_task("update_chip_distribution_em", update_chip_distribution_em, db)
        results["health"] = _run_task("health_check", health_check, db, fast=True)
    else:
        # ── 第二阶段：并发分叉（本地计算 vs 全球宏观 vs 衍生快任务 重叠执行）──
        stage2_results: dict[str, Any] = {}

        stage2_raw_tasks: list[tuple[str, Callable[..., Any], tuple[Any, ...], dict[str, Any]]] = [
            ("update_indicators", update_indicators, (db, engine), {}),
            ("update_chip_distribution", update_chip_distribution, (db,), {}),
            ("update_global_assets", update_global_assets, (db,), {}),
            ("update_gold_price", update_gold_price, (db,), {}),
            ("update_usd", update_usd, (db,), {}),
            ("update_global_index", update_global_index, (db,), {}),
            ("update_us_treasury", update_us_treasury, (db,), {}),
            ("update_us_macro", update_us_macro, (db,), {}),
            ("update_hk_tech_index", update_hk_tech_index, (db,), {}),
            ("update_cftc_cot", update_cftc_cot, (db,), {}),
            ("update_eia_petroleum", update_eia_petroleum, (db,), {}),
            ("update_lithium_spot", update_lithium_spot, (db,), {}),
            ("update_futures", update_futures, (db,), {}),
            ("update_china_macro", update_china_macro, (db,), {}),
            ("update_money_market", update_money_market, (db,), {}),
            ("update_margin_trading", update_margin_trading, (db,), {}),
            ("update_dragon_tiger", update_dragon_tiger, (db,), {}),
            ("update_block_trade", update_block_trade, (db,), {}),
            ("update_limit_up_down", update_limit_up_down, (db,), {}),
            ("update_stock_comment", update_stock_comment, (db,), {}),
            ("update_hot_rank", update_hot_rank, (db,), {}),
            ("update_index_daily", update_index_daily, (db,), {}),
            ("update_market_valuation", update_market_valuation, (db,), {}),
            ("update_south_flow", update_south_flow, (db,), {}),
            ("update_ah_premium", update_ah_premium, (db,), {}),
            ("update_etf_daily", update_etf_daily, (db,), {}),
            ("update_cb_quotation", update_cb_quotation, (db,), {}),
            ("update_cb_redeem", update_cb_redeem, (db,), {}),
            ("update_cb_index", update_cb_index, (db,), {}),
            ("update_option_sentiment", update_option_sentiment, (db,), {}),
            ("update_stock_repurchase", update_stock_repurchase, (db,), {}),
            ("update_placement_announcements", update_placement_announcements, (db,), {}),
            ("update_institution_survey", update_institution_survey, (db,), {}),
            ("update_stock_pledge", update_stock_pledge, (db,), {}),
        ]

        stage2_ptasks: list[ParallelTask] = []
        for name, fn, args, kwargs in stage2_raw_tasks:
            ptask, skip_dict = _create_task(name, fn, *args, **kwargs)
            if skip_dict is not None:
                stage2_results[name] = skip_dict
            elif ptask is not None:
                stage2_ptasks.append(ptask)

        # stage2 保持串行（max_workers=1）。旧注释称阻碍是「不能把共享 db provider
        # 交给多个 SQLite 线程」——已被 stage4 反证：stage4 同样共享该 provider 并并发
        # 3 个任务，其写表两两不相交、有审计与测试钉住（tests/test_stage4_concurrency.py）。
        #
        # 写表不相交审计已于 2026-09-24 完成（tests/test_stage2_write_disjointness.py）：
        # 这 34 个任务落在 36 个 db 写方法上，方法各自只写一张互不相同的表，写表两两不
        # 相交成立；其中 11 个方法委托 smartmoney_hunter 写，该路径已由 stage4 的生产
        # 并发与并发压测覆盖。也就是说「写表相交」不再是保持串行的理由。
        #
        # 仍然串行的理由是收益上限而非正确性：耗时占比最高的两个任务（实测约占 75%）
        # 是纯本地计算且内部已用线程池并行 + 批量写（tasks/core_chain.py），任务级并发
        # 只对网络型小任务有收益。若要提速，应做「计算道 + 网络道」双车道，而不是把
        # 全体任务按 N 路均分；那是一次独立的行为变更。
        stage2_ran = run_parallel_tasks(stage2_ptasks, max_workers=1, runner_fn=_safe_task)
        stage2_results.update(stage2_ran)

        results["indicators"] = stage2_results.get("update_indicators", {})
        results["chip_distribution"] = stage2_results.get("update_chip_distribution", {})
        results["global_assets"] = stage2_results.get("update_global_assets", {})
        results["gold_price"] = stage2_results.get("update_gold_price", {})
        results["fx_rate"] = stage2_results.get("update_usd", {})
        results["global_index"] = stage2_results.get("update_global_index", {})
        results["us_treasury"] = stage2_results.get("update_us_treasury", {})
        results["us_macro"] = stage2_results.get("update_us_macro", {})
        results["hk_tech_index"] = stage2_results.get("update_hk_tech_index", {})
        results["cftc_cot"] = stage2_results.get("update_cftc_cot", {})
        results["eia_petroleum"] = stage2_results.get("update_eia_petroleum", {})
        results["lithium_spot"] = stage2_results.get("update_lithium_spot", {})
        results["futures"] = stage2_results.get("update_futures", {})
        results["china_macro"] = stage2_results.get("update_china_macro", {})
        results["money_market"] = stage2_results.get("update_money_market", {})
        results["margin_trading"] = stage2_results.get("update_margin_trading", {})
        results["dragon_tiger"] = stage2_results.get("update_dragon_tiger", {})
        results["block_trade"] = stage2_results.get("update_block_trade", {})
        results["limit_up_down"] = stage2_results.get("update_limit_up_down", {})
        results["stock_comment"] = stage2_results.get("update_stock_comment", {})
        results["hot_rank"] = stage2_results.get("update_hot_rank", {})
        results["index_daily"] = stage2_results.get("update_index_daily", {})
        results["market_valuation"] = stage2_results.get("update_market_valuation", {})
        results["south_flow"] = stage2_results.get("update_south_flow", {})
        results["ah_premium"] = stage2_results.get("update_ah_premium", {})
        results["etf_daily"] = stage2_results.get("update_etf_daily", {})
        results["cb_quotation"] = stage2_results.get("update_cb_quotation", {})
        results["cb_redeem"] = stage2_results.get("update_cb_redeem", {})
        results["cb_index"] = stage2_results.get("update_cb_index", {})
        results["option_sentiment"] = stage2_results.get("update_option_sentiment", {})
        results["stock_repurchase"] = stage2_results.get("update_stock_repurchase", {})
        results["placement_announcements"] = stage2_results.get("update_placement_announcements", {})
        results["institution_survey"] = stage2_results.get("update_institution_survey", {})
        results["stock_pledge"] = stage2_results.get("update_stock_pledge", {})

        # ── 第三阶段：估值链与行业板块（具有严格先后依赖，串行执行）──
        results["fundamentals"] = _run_task("update_fundamentals", update_fundamentals, db, loader)
        results["market_snapshot"] = _run_task("update_market_snapshot", update_market_snapshot, db)
        results["historical_valuation"] = _run_task("update_historical_valuation", update_historical_valuation, db)
        results["fund_flow"] = _run_task("update_fund_flow", update_fund_flow, db, loader)
        results["sector_fund_flow"] = _run_task("update_sector_fund_flow", update_sector_fund_flow, db)
        results["sector_industry"] = _run_task("update_sector_industry", update_sector_industry, db)
        results["sector_derivatives"] = _run_task("update_sector_derivatives", update_sector_derivatives, db)
        results["concept_board"] = _run_task("update_concept_board", update_concept_board, db)

        # ── 第四阶段：低频财务、事件与成分股（无依赖，并发执行）──
        stage4_results: dict[str, Any] = {}
        stage4_raw_tasks: list[tuple[str, Callable[..., Any], tuple[Any, ...], dict[str, Any]]] = [
            ("update_restricted_share", update_restricted_share, (db,), {}),
            ("update_earnings_forecast", update_earnings_forecast, (db,), {}),
            ("update_dividend_summary", update_dividend_summary, (db,), {}),
            ("update_market_breadth", update_market_breadth, (db,), {}),
            ("update_financial_history", update_financial_history, (db,), {}),
            ("update_shareholder_count", update_shareholder_count, (db,), {}),
            ("update_quarterly_financials", update_quarterly_financials, (db, loader), {}),
            ("update_industry", update_industry, (db,), {}),
            ("update_north_hold", update_north_hold, (db,), {}),
            ("update_fund_holdings", update_fund_holdings, (db,), {}),
            ("update_top10_shareholders", update_top10_shareholders, (db,), {}),
            ("update_index_membership", update_index_membership, (db,), {}),
            ("update_concept_member", update_concept_member, (db,), {}),
        ]
        stage4_ptasks: list[ParallelTask] = []
        for name, fn, args, kwargs in stage4_raw_tasks:
            ptask, skip_dict = _create_task(name, fn, *args, **kwargs)
            if skip_dict is not None:
                stage4_results[name] = skip_dict
            elif ptask is not None:
                stage4_ptasks.append(ptask)

        # stage4 任务写表已审计为两两不相交，写路径均在 _write_lock 或
        # 独立连接 + busy_timeout 保护下，可安全并发（bars 内部已有 3 线程先例）
        stage4_workers = max(1, min(workers, len(stage4_ptasks)))
        stage4_ran = run_parallel_tasks(stage4_ptasks, max_workers=stage4_workers, runner_fn=_safe_task)
        stage4_results.update(stage4_ran)

        results["restricted_share"] = stage4_results.get("update_restricted_share", {})
        results["earnings_forecast"] = stage4_results.get("update_earnings_forecast", {})
        results["dividend_summary"] = stage4_results.get("update_dividend_summary", {})
        results["market_breadth"] = stage4_results.get("update_market_breadth", {})
        results["financial_history"] = stage4_results.get("update_financial_history", {})
        results["shareholder_count"] = stage4_results.get("update_shareholder_count", {})
        results["quarterly_financials"] = stage4_results.get("update_quarterly_financials", {})
        results["industry"] = stage4_results.get("update_industry", {})
        results["north_hold"] = stage4_results.get("update_north_hold", {})
        results["fund_holdings"] = stage4_results.get("update_fund_holdings", {})
        results["top10_shareholders"] = stage4_results.get("update_top10_shareholders", {})
        results["index_membership"] = stage4_results.get("update_index_membership", {})
        results["concept_member"] = stage4_results.get("update_concept_member", {})

        # ── 第五阶段：长尾垫底与健康检查（串行执行）──
        results["chip_distribution_em"] = _run_task(
            "update_chip_distribution_em", update_chip_distribution_em, db
        )
        results["health"] = _run_task("health_check", health_check, db, fast=True)

    # 走到这里即整条任务链已跑完（两条分支在此汇合），落完成标记供下一轮校验。
    mark_run_completed(db, today_str)

    db.close()
    elapsed = time.time() - start_time
    logger.info("\n" + "=" * 60)
    logger.info("🏁 数据管道全部完成")
    logger.info(f"⏱️  总耗时: {elapsed:.1f}s ({elapsed/60:.1f}min)")
    logger.info("=" * 60)

    # 检查是否有任务失败，供 main() 决定退出码
    # 使用 TaskResult.exit_failure 语义：degraded / failed / aborted
    failed_tasks = sorted(
        k
        for k, v in results.items()
        if isinstance(v, dict)
        and v.get("status") in {"degraded", "failed", "aborted"}
    )
    retained_tasks = sorted(
        k
        for k, v in results.items()
        if isinstance(v, dict) and v.get("status") == "retained"
    )
    results["crashed"] = bool(failed_tasks)
    results["retained_tasks"] = retained_tasks
    # 无人值守告警：整轮结果必须主动外发，不能只靠日志
    if failed_tasks:
        notify_all(
            "error",
            "数据管道完成（含失败任务）",
            f"耗时 {elapsed / 60:.1f}min，失败任务: {', '.join(failed_tasks)}",
        )
    elif retained_tasks:
        notify_all(
            "warning",
            "数据管道完成（含保留旧数据任务）",
            f"耗时 {elapsed / 60:.1f}min，保留旧数据: {', '.join(retained_tasks)}",
        )
    else:
        notify_all("info", "数据管道全部完成", f"耗时 {elapsed / 60:.1f}min")
    return results


def weekly_backfill(
    db: DatabaseInterface,
    loader: DataLoaderInterface,
    engine: IndicatorEngineInterface,
    force: bool = False,
) -> dict:
    """每周数据补全层：WEEKLY 任务 → 补齐缺漏（stale 驱动）→ retry → health。"""
    start_time = time.time()
    _lower_process_priority()
    logger.info("\n🧩 每周数据补全层启动 (WEEKLY + 补齐缺漏 + retry)")
    results: dict[str, Any] = {}

    # 1. WEEKLY cadence 任务（registry 顺序即声明顺序）
    for spec in TASK_REGISTRY:
        if spec.cadence is Cadence.WEEKLY:
            results[spec.name] = _run_registry_task(
                spec.name, db, loader, engine, force=force
            )

    # 2. 补齐缺漏：与完整度面板同一套 stale 语义，不限 cadence；
    #    判定失败（DB 不可读等）只跳过补全，不影响主体
    try:
        latest_dates = get_latest_dates(str(db.db_path))
        expected = get_expected_latest_trading_day()
        for task_name in compute_catch_up_tasks(latest_dates, expected):
            if task_name not in results:
                results[task_name] = _run_registry_task(
                    task_name, db, loader, engine, force=force
                )
    except Exception as e:
        logger.warning("⚠️ 补齐缺漏判定失败，跳过补全步骤: %s", e)

    # 3. 失败股票重抓 + 4. 健康报告
    results["retry"] = _run_registry_task("retry", db, loader, engine)
    results["health"] = _run_registry_task("health_check", db, loader, engine, health_fast=True)

    db.close()
    elapsed = time.time() - start_time
    failed_tasks = sorted(
        k for k, v in results.items()
        if isinstance(v, dict)
        and v.get("status") in {"degraded", "failed", "aborted"}
    )
    retained_tasks = sorted(
        k for k, v in results.items()
        if isinstance(v, dict) and v.get("status") == "retained"
    )
    results["crashed"] = bool(failed_tasks)
    results["retained_tasks"] = retained_tasks
    if failed_tasks:
        notify_all("error", "每周补全完成（含失败任务）",
                   f"耗时 {elapsed / 60:.1f}min，失败任务: {', '.join(failed_tasks)}")
    elif retained_tasks:
        notify_all("warning", "每周补全完成（含保留旧数据任务）",
                   f"耗时 {elapsed / 60:.1f}min，保留旧数据: {', '.join(retained_tasks)}")
    else:
        notify_all("info", "每周补全全部完成", f"耗时 {elapsed / 60:.1f}min")
    return results


_REPAIR_CHAIN: tuple[str, ...] = (
    "backup_database.py",
    "reconcile_with_akshare.py",
    "validate_and_vacuum.py",
)

# 修复脚本超时（秒）：每月对账需逐只对比全市场日线、带限流休息，
# 实测 5549 只约 3-4h（2026-08-27 61 分钟仅完成 24% 即被旧 1h 超时杀死、
# 中途已修复的 11,668 行只能留待下次续跑），故单独放宽；其余脚本 1h 足够。
_REPAIR_TIMEOUTS: dict[str, int] = {
    "backup_database.py": 3600,
    "reconcile_with_akshare.py": 14400,  # 4h：全市场对账 + 新浪限流休息
    "validate_and_vacuum.py": 3600,
}
_REPAIR_TIMEOUT_DEFAULT = 3600


def _run_repair_script(script: str) -> dict:
    """以子进程运行 scripts/ 下的修复脚本，返回 safe_task 兼容结果。

    父管道已持有全局 ProcessLock，通过环境变量告知子脚本跳过重复加锁，
    否则 reconcile_with_akshare.py 等自带单实例保护的脚本必然加锁失败退出。
    """
    path = Path(__file__).parent / "scripts" / script
    env = {**os.environ, "QUANT_PIPELINE_LOCK_HELD": "1"}
    timeout = _REPAIR_TIMEOUTS.get(script, _REPAIR_TIMEOUT_DEFAULT)
    try:
        proc = subprocess.run(
            [sys.executable, str(path)],
            capture_output=True, text=True, timeout=timeout, env=env,
        )
    except (OSError, subprocess.TimeoutExpired) as e:
        logger.error(f"❌ 修复脚本 {script} 启动失败: {e}")
        return {"status": "failed", "error": f"{script}: {e}"}
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "(无输出)")[-500:]
        logger.error(f"❌ 修复脚本失败: {script} exited {proc.returncode}: {detail}")
        return {"status": "failed",
                "error": f"{script} exited {proc.returncode}: {detail}"}
    return {"status": "ok", "output_tail": proc.stdout[-500:]}


def monthly_repair(
    db: DatabaseInterface,
    loader: DataLoaderInterface,
    engine: IndicatorEngineInterface,
    force: bool = False,
) -> dict:
    """每月数据修复层：MONTHLY/QUARTERLY 任务 → 备份→对账→vacuum → health。"""
    start_time = time.time()
    _lower_process_priority()
    logger.info("\n🛠️ 每月数据修复层启动 (MONTHLY + QUARTERLY + 修复链)")
    results: dict[str, Any] = {}

    for spec in TASK_REGISTRY:
        if spec.cadence in (Cadence.MONTHLY, Cadence.QUARTERLY):
            results[spec.name] = _run_registry_task(
                spec.name, db, loader, engine, force=force
            )

    # 修复链：backup 失败则中止后续（不允许无备份修复），其余失败继续并汇总
    for script in _REPAIR_CHAIN:
        step = _run_repair_script(script)
        results[f"repair:{script}"] = step
        if script == "backup_database.py" and step.get("status") != "ok":
            logger.error("❌ 备份失败，中止修复链后续步骤")
            break

    results["health"] = _run_registry_task("health_check", db, loader, engine, health_fast=True)

    db.close()
    elapsed = time.time() - start_time
    failed_tasks = sorted(
        k for k, v in results.items()
        if isinstance(v, dict)
        and v.get("status") in {"degraded", "failed", "aborted"}
    )
    retained_tasks = sorted(
        k for k, v in results.items()
        if isinstance(v, dict) and v.get("status") == "retained"
    )
    results["crashed"] = bool(failed_tasks)
    results["retained_tasks"] = retained_tasks
    if failed_tasks:
        notify_all("error", "每月修复完成（含失败步骤）",
                   f"耗时 {elapsed / 60:.1f}min，失败: {', '.join(failed_tasks)}")
    elif retained_tasks:
        notify_all("warning", "每月修复完成（含保留旧数据步骤）",
                   f"耗时 {elapsed / 60:.1f}min，保留旧数据: {', '.join(retained_tasks)}")
    else:
        notify_all("info", "每月修复全部完成", f"耗时 {elapsed / 60:.1f}min")
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
        # 注入的 verifier（测试接缝）优先；按 is None 身份判断匹配 | None
        # 契约，假值但有效的注入 verifier 不得被默认雪球 verifier 替换
        build_kwargs["verifier"] = (
            XueqiuCrossSourceVerifier(db_path=db_path)
            if cross_source_verifier is None
            else cross_source_verifier
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
    resume: bool = False,
    orchestrator: RefreshOrchestrator | None = None,
    cross_source_verifier: CrossSourceVerifier | None = None,
) -> TaskResult:
    """执行一次收盘后刷新，返回聚合 TaskResult。

    16:00 前的拦截由编排器自身的 pre-close 闸门完成，
    --force 仅映射为 allow_pre_close=True，不在 CLI 层重复门控逻辑。

    ``resume`` 时先在同一目标交易日上寻找被中断的运行，把已完成任务的结果
    接力到本次运行（新 run_id，审计行重新落一遍），避免从 20-55 分钟的
    全市场任务重头再来。找不到可续跑的运行时按普通运行处理，只告警不报错。
    """
    if orchestrator is None:
        orchestrator = _build_refresh_orchestrator(
            db_path, cross_source_verifier=cross_source_verifier
        )
    # 单次读取上海时钟：闸门、started_at 与目标交易日共用同一时间基准
    now = datetime.now(_SHANGHAI_TZ)
    target_date = get_expected_latest_trading_day(now=now)
    resume_from: tuple[ResumedTaskOutcome, ...] = ()
    if resume:
        found = load_interrupted_refresh(db_path, target_date=target_date)
        if found is None:
            logger.warning(
                "⚠️ --resume 未找到 %s 的可续跑运行（需 status 为 running/aborted "
                "且已有可沿用的任务），本次将从头开始",
                target_date,
            )
        else:
            parent_run_id, resume_from = found
            logger.info(
                "🔁 续跑 %s 的中断运行 %s：可沿用 %d 个任务",
                target_date,
                parent_run_id,
                len(resume_from),
            )
    context = RefreshContext(
        target_date=target_date,
        started_at=now,
        run_id=str(uuid4()),
        # 保留 None（全市场）与 ()（no-op）的区分：空列表绝不得升级为全市场
        symbols=None if symbols is None else tuple(symbols),
        allow_pre_close=force,
    )
    return orchestrator.run(context, resume_from=resume_from)


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
        help="收盘后刷新当日数据（与 --task 互斥；可加 --resume）",
    )
    parser.add_argument(
        "--backfill-days",
        nargs="?",
        const="",
        default=None,
        metavar="YYYY-MM-DD[,YYYY-MM-DD...]",
        help=(
            "补写整日缺席的历史交易日（与 --task/--refresh-today 互斥）："
            "不带值 = 自动取 core/known_gaps.py 里仍缺的日期，带值 = 指定日期"
        ),
    )
    parser.add_argument(
        "--backfill-table",
        nargs="*",
        default=None,
        metavar="表名|YYYY-MM-DD",
        help=(
            "按 (表, 日) 粒度回补历史单表空洞（与 --task/--refresh-today/--backfill-days 互斥）："
            "条目可以是回补表名（如 limit_up_down）或日期；条目可混写、逗号分隔；"
            "不带值 = 全部可回补表 × 登记册缺失日。日期粒度幂等（该表当日已有行则跳过）"
        ),
    )
    parser.add_argument("--limit", type=int, default=None, help="测试模式：只处理前 N 只股票")
    parser.add_argument("--force", action="store_true", help="强制运行（忽略交易日检查）")
    parser.add_argument(
        "--resume",
        action="store_true",
        help="断点续传：从上次中断的位置继续（--refresh-today 时=TUI/CLI 中断后的续跑）",
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default=os.getenv("QUANT_DB_PATH", DEFAULT_DB_PATH),
        help="数据库路径（默认从环境变量 QUANT_DB_PATH 读取）",
    )
    parser.add_argument(
        "--sequential",
        action="store_true",
        help="强制串行执行所有任务，禁用并发调度",
    )
    parser.add_argument(
        "--parallel-workers",
        type=int,
        default=None,
        help="并发执行线程数 (默认: 3；仅作用于 stage4 与 bars 内部池，stage2/3/5 固定串行)",
    )
    parser.add_argument(
        "--symbols",
        type=str,
        default=None,
        help="逗号分隔的股票代码列表，或包含一行一个代码的文件路径。指定后只处理这些股票。",
    )

    args = parser.parse_args()

    # --refresh-today / --backfill-days 是独立的顶层模式；未指定任何模式时仍走 legacy all
    if args.refresh_today and args.task is not None:
        parser.error("--task 不能与 --refresh-today 同时使用")
    if args.backfill_days is not None and (args.refresh_today or args.task is not None):
        parser.error("--task/--refresh-today 不能与 --backfill-days 同时使用")
    if args.backfill_table is not None and (
        args.refresh_today or args.task is not None or args.backfill_days is not None
    ):
        parser.error("--backfill-table 不能与 --task/--refresh-today/--backfill-days 同时使用")
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
            result = run_close_refresh(
                args.db_path,
                symbols=symbols,
                force=args.force,
                resume=args.resume,
            )
        except KeyboardInterrupt:
            # 刷新路径不同于 legacy：中断必须非零退出，供调度/TUI 感知 aborted
            logger.info("收到中断信号，正在退出...")
            sys.exit(1)
        if result.exit_failure:
            sys.exit(1)
        return

    # ── 联网预检（全量管道专用）──
    # 定时/手动在无网络状态启动（如笔记本在包里被 launchd 唤醒）时，
    # 任务层的断网报错会把环境问题记成上百条 ERROR / 熔断 / 失败任务 / 告警。
    # all/daily/update_daily_core 启动前先做 TCP 联网探测：离线则本轮干净跳过
    # （仅一条 INFO、退出码 0、不初始化 Provider / 不加锁 / 不发告警），
    # 数据由下一次联网运行增量补齐（bars 增量拉取天然覆盖缺日）。
    # QUANT_ALLOW_OFFLINE=1 可强制照跑（与 --force 语义解耦的逃生门）。
    if (
        task in ("all", "daily", "update_daily_core")
        and args.backfill_days is None
        and args.backfill_table is None
        and os.getenv("QUANT_ALLOW_OFFLINE", "0").lower() not in ("1", "true", "yes")
        and not is_online()
    ):
        logger.info(
            "🌐 无网络连接，本轮全量更新离线跳过（不视为错误；联网后下次运行自动补齐）"
        )
        sys.exit(0)

    task_lock_name: str | None = None
    db = None
    try:
        # 进程锁：all 任务使用全局锁；single task 使用按任务名锁，
        # 允许不同任务并行，避免 TUI 连续启动多个 single task 时互相冲突。
        if task in ("all", "daily", "update_daily_core", "weekly_backfill", "monthly_repair"):
            _acquire_lock()
        elif task != "health_check":
            # 全局锁与 TaskLock 互不感知：全量管道（或收盘刷新）运行期间，
            # 单任务必须退出，否则双份抓取并发写同一批表
            if global_lock_held():
                print(f"❌ 全量管道正在运行，任务 {task} 退出以避免并发写同一批表")
                sys.exit(1)
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

        if args.backfill_days is not None:
            results = run_backfill_days(db, args.backfill_days)
            if results.get("failed"):
                sys.exit(1)
        elif args.backfill_table is not None:
            results = run_backfill_table(db, " ".join(args.backfill_table))
            if results.get("failed"):
                sys.exit(1)
        elif task in ("all", "daily", "update_daily_core"):
            kwargs: dict[str, Any] = {"resume": args.resume, "force": args.force}
            if args.sequential:
                kwargs["sequential"] = True
            if args.parallel_workers is not None:
                kwargs["parallel_workers"] = args.parallel_workers
            results = update_daily_core(
                db,
                loader,
                engine,
                **kwargs,
            )
            if results.get("crashed"):
                sys.exit(1)
        elif task == "weekly_backfill":
            results = weekly_backfill(db, loader, engine, force=args.force)
            if results.get("crashed"):
                sys.exit(1)
        elif task == "monthly_repair":
            results = monthly_repair(db, loader, engine, force=args.force)
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
        # 与 ProcessLock 的 SIGINT handler 口径一致：中断必须非零退出，
        # 否则调度器/TUI 队列会把"被取消"误判为"成功"
        sys.exit(130)
    finally:
        if db is not None:
            db.close()
        if task_lock_name:
            TaskLock.release(task_lock_name)
        # WAL 文件是只涨不缩的高水位线（实测：单个 310 MB 的事务把它撑到 310 MB，
        # 之后无论再写多少都不缩，生产库曾长期占着 2.39 GiB）。收尾显式 checkpoint 一次
        # 把空间还给磁盘——只要还有别的连接（TUI）开着，WAL 就不会被 SQLite 自己删除。
        # 失败（库被占用等）只记日志：回收磁盘是尽力而为，不能改变退出码。
        truncate_wal(args.db_path)


if __name__ == "__main__":
    main()
