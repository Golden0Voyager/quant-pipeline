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

# 将 ~/Code 加入 Python 路径（使 pipeline 能 import smartmoney_hunter）
_CODE_DIR = os.path.expanduser("~/Code")
if _CODE_DIR not in sys.path:
    sys.path.insert(0, _CODE_DIR)
_HUNTER_SRC = os.path.expanduser("~/Code/quant_hunter/src")
if _HUNTER_SRC not in sys.path and os.path.isdir(_HUNTER_SRC):
    sys.path.insert(0, _HUNTER_SRC)

# ── Core module re-exports ──
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
)
from core.lock import ProcessLock
from core.monitor import AkShareMonitor  # noqa: F401
from core.progress import ProgressTracker  # noqa: F401
from core.runner import safe_task
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
from tasks.macro import (
    update_crude_oil,
    update_dividend_summary,
    update_global_index,
    update_gold_price,
    update_limit_up_down,
    update_north_flow,
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
from tasks.sector_derivatives import update_sector_derivatives
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

# ── Backward compat aliases ──
_acquire_lock = ProcessLock.acquire
_release_lock = ProcessLock.release
_safe_task = safe_task
_lower_process_priority = lower_process_priority
_should_update = should_update


# ===========================================================================
# 主流程编排
# ===========================================================================

def run_all(
    db: DatabaseInterface,
    loader: DataLoaderInterface,
    engine: IndicatorEngineInterface,
    resume: bool = False,
    force: bool = False,
) -> dict:
    """运行完整数据管道。"""
    start_time = time.time()
    _lower_process_priority()
    logger.info("\n🚀 SmartMoney 每日数据管道启动")
    logger.info(f"📂 数据库: {db.db_path}")
    logger.info(f"⚙️  并行线程: {PARALLEL_WORKERS} (默认 1=串行)")
    logger.info(f"📅 今天: {datetime.now().strftime('%Y-%m-%d')}")

    if not _should_update():
        db.close()
        return {"status": "skipped", "reason": "非交易日"}

    results = {}
    results["stock_list"] = _safe_task("update_stock_list", update_stock_list, db)
    results["bars"] = _safe_task("update_bars", update_bars, db, loader, resume=resume, force=force)

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
    results["north_flow"] = _safe_task("update_north_flow", update_north_flow, db)
    results["index_daily"] = _safe_task("update_index_daily", update_index_daily, db)
    results["limit_up_down"] = _safe_task("update_limit_up_down", update_limit_up_down, db)
    results["dividend_summary"] = _safe_task("update_dividend_summary", update_dividend_summary, db)
    results["gold_price"] = _safe_task("update_gold_price", update_gold_price, db)
    results["crude_oil"] = _safe_task("update_crude_oil", update_crude_oil, db)
    results["fx_rate"] = _safe_task("update_usd", update_usd, db)
    results["global_index"] = _safe_task("update_global_index", update_global_index, db)
    results["us_treasury"] = _safe_task("update_us_treasury", update_us_treasury, db)
    results["futures"] = _safe_task("update_futures", update_futures, db)
    results["china_macro"] = _safe_task("update_china_macro", update_china_macro, db)

    # ── 新增衍生数据任务 ──
    results["south_flow"] = _safe_task("update_south_flow", update_south_flow, db)
    results["ah_premium"] = _safe_task("update_ah_premium", update_ah_premium, db)
    results["etf_daily"] = _safe_task("update_etf_daily", update_etf_daily, db)
    results["cb_quotation"] = _safe_task("update_cb_quotation", update_cb_quotation, db)
    results["cb_redeem"] = _safe_task("update_cb_redeem", update_cb_redeem, db)
    results["cb_index"] = _safe_task("update_cb_index", update_cb_index, db)
    results["restricted_share"] = _safe_task("update_restricted_share", update_restricted_share, db)
    results["earnings_forecast"] = _safe_task("update_earnings_forecast", update_earnings_forecast, db)
    results["sector_derivatives"] = _safe_task("update_sector_derivatives", update_sector_derivatives, db)
    results["chip_distribution"] = _safe_task(
        "update_chip_distribution", update_chip_distribution, db
    )
    results["chip_distribution_em"] = _safe_task(
        "update_chip_distribution_em", update_chip_distribution_em, db
    )

    results["retry"] = _safe_task("retry_failed", retry_failed, db, loader)
    results["health"] = _safe_task("health_check", health_check, db)

    db.close()
    elapsed = time.time() - start_time
    logger.info("\n" + "=" * 60)
    logger.info("🏁 数据管道全部完成")
    logger.info(f"⏱️  总耗时: {elapsed:.1f}s ({elapsed/60:.1f}min)")
    logger.info("=" * 60)

    # 检查是否有任务失败，供 main() 决定退出码
    results["crashed"] = any(
        isinstance(v, dict)
        and (
            v.get("status") in {"crashed", "completed_with_errors"}
            or bool(v.get("error"))
        )
        for v in results.values()
    )
    return results


# ===========================================================================
# CLI 入口
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(description="SmartMoney 日常数据管道（解耦版 + 断点续传）")
    parser.add_argument(
        "--task",
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
    parser.add_argument(
        "--symbols",
        type=str,
        default=None,
        help="逗号分隔的股票代码列表，或包含一行一个代码的文件路径。指定后只处理这些股票。",
    )

    args = parser.parse_args()

    symbols_arg = args.symbols
    symbols: list[str] | None = None
    if symbols_arg:
        if Path(symbols_arg).exists():
            symbols = [
                line.strip()
                for line in Path(symbols_arg).read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
        else:
            symbols = [s.strip() for s in symbols_arg.split(",") if s.strip()]

    lock_acquired = False
    db = None
    try:
        # 进程锁：防止多实例同时运行（health_check 除外）
        if args.task != "health_check":
            _acquire_lock()
            lock_acquired = True

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
            results = run_all(db, loader, engine, resume=args.resume, force=args.force)
            if results.get("crashed"):
                sys.exit(1)
        elif args.task == "update_stock_list":
            update_stock_list(db)
        elif args.task == "update_bars":
            update_bars(db, loader, limit=args.limit, resume=args.resume, symbols=symbols, force=args.force)
        elif args.task == "update_indicators":
            if args.force or symbols:
                conn_kw = sqlite3.connect(str(db.db_path))
                if symbols:
                    target_symbols = symbols
                else:
                    target_symbols = [row[0] for row in conn_kw.execute("SELECT DISTINCT ts_code FROM daily_bars ORDER BY ts_code").fetchall()]
                conn_kw.close()
                logger.info(f"🔁 强制/指定股票模式：重算 {len(target_symbols)} 只股票的技术指标")
                update_indicators(db, engine, symbols_to_update=target_symbols)
            else:
                update_indicators(db, engine)
        elif args.task == "update_chip_distribution":
            if args.force or symbols:
                conn_kw = sqlite3.connect(str(db.db_path))
                if symbols:
                    target_symbols = symbols
                else:
                    target_symbols = [row[0] for row in conn_kw.execute("SELECT DISTINCT ts_code FROM daily_bars ORDER BY ts_code").fetchall()]
                conn_kw.close()
                logger.info(f"🔁 强制/指定股票模式：重算 {len(target_symbols)} 只股票的筹码分布")
                update_chip_distribution(db, symbols_to_update=target_symbols)
            else:
                update_chip_distribution(db)
        elif args.task == "update_chip_distribution_em":
            update_chip_distribution_em(db)
        elif args.task == "update_chip_distribution_em_fullmarket":
            update_chip_distribution_em_fullmarket(db)
        elif args.task == "update_fundamentals":
            update_fundamentals(db, loader, symbols=symbols)
        elif args.task == "update_market_snapshot":
            update_market_snapshot(db)
        elif args.task == "update_fund_flow":
            update_fund_flow(db, loader, symbols=symbols)
        elif args.task == "update_margin_trading":
            update_margin_trading(db, symbols=symbols)
        elif args.task == "update_dragon_tiger":
            update_dragon_tiger(db, symbols=symbols)
        elif args.task == "update_block_trade":
            update_block_trade(db, symbols=symbols)
        elif args.task == "update_sector_fund_flow":
            update_sector_fund_flow(db)
        elif args.task == "update_shareholder_count":
            update_shareholder_count(db, symbols=symbols)
        elif args.task == "update_quarterly_financials":
            update_quarterly_financials(db, loader, symbols=symbols)
        elif args.task == "update_historical_valuation":
            update_historical_valuation(db, symbols=symbols)
        elif args.task == "update_sector_industry":
            update_sector_industry(db)
        elif args.task == "update_industry":
            update_industry(db)
        elif args.task == "update_north_flow":
            update_north_flow(db)
        elif args.task == "update_index_daily":
            update_index_daily(db)
        elif args.task == "update_limit_up_down":
            update_limit_up_down(db)
        elif args.task == "update_dividend_summary":
            update_dividend_summary(db)
        elif args.task == "update_gold_price":
            update_gold_price(db)
        elif args.task == "update_crude_oil":
            update_crude_oil(db)
        elif args.task == "update_usd":
            update_usd(db)
        elif args.task == "update_global_index":
            update_global_index(db)
        elif args.task == "update_us_treasury":
            update_us_treasury(db)
        elif args.task == "update_futures":
            update_futures(db)
        elif args.task == "update_south_flow":
            update_south_flow(db)
        elif args.task == "update_ah_premium":
            update_ah_premium(db)
        elif args.task == "update_etf_daily":
            update_etf_daily(db)
        elif args.task == "update_cb_quotation":
            update_cb_quotation(db)
        elif args.task == "update_cb_redeem":
            update_cb_redeem(db)
        elif args.task == "update_cb_index":
            update_cb_index(db)
        elif args.task == "update_restricted_share":
            update_restricted_share(db)
        elif args.task == "update_earnings_forecast":
            update_earnings_forecast(db)
        elif args.task == "update_sector_derivatives":
            update_sector_derivatives(db)
        elif args.task == "update_china_macro":
            update_china_macro(db)
        elif args.task == "retry":
            retry_failed(db, loader)
        elif args.task == "health_check":
            health_check(db)
    except KeyboardInterrupt:
        logger.info("收到中断信号，正在退出...")
    finally:
        if db is not None:
            db.close()
        if lock_acquired:
            _release_lock()


if __name__ == "__main__":
    main()
