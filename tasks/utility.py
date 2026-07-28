"""
实用工具任务
────────────
存放从 daily_pipeline.py 提取的 utility 类任务函数。
"""

from __future__ import annotations

import json  # noqa: F401
import logging
import os  # noqa: F401
import sqlite3
import time  # noqa: F401
from datetime import datetime
from pathlib import Path

import pandas as pd  # noqa: F401

from core.calendar import get_expected_latest_trading_day
from core.config import DB_PATH, SHARED_DATA_DIR  # noqa: F401
from core.monitor import AkShareMonitor  # noqa: F401
from core.utils import is_real_db_path, should_skip_beijing, should_update  # noqa: F401
from interface import DatabaseInterface, DataLoaderInterface

# akshare 作为可选依赖（部分任务函数可能不直接使用它）
try:
    import akshare as ak
except ImportError:
    ak = None  # type: ignore[assignment]

# 从 core/tasks 模块导入（避免与 daily_pipeline.py 的循环依赖）
from core.progress import ProgressTracker
from tasks.bars import _update_single_bar

logger = logging.getLogger(__name__)


# ===========================================================================
# 辅助函数
# ===========================================================================


# ===========================================================================
# 公共任务函数
# ===========================================================================


def retry_failed(
    db: DatabaseInterface, loader: DataLoaderInterface
) -> dict:
    """重试之前失败的股票。"""
    data = ProgressTracker.load()
    if data is not None and data.get("task") != "retry":
        logger.info("ℹ️  当前进度不是 retry checkpoint，保留原断点")
        return {
            "status": "no_data",
            "reason": "progress is not a retry checkpoint",
            "saved": 0,
            "attempted": 0,
            "success": 0,
            "failed": 0,
            "total": 0,
        }

    symbols: list[str] = data.get("failed_queue", []) if data else []
    symbols = list(dict.fromkeys(
        s for s in symbols if not should_skip_beijing(s)
    ))

    if not symbols:
        logger.info("ℹ️  retry 队列为空")
        ProgressTracker.clear()
        return {
            "status": "no_data",
            "reason": "retry queue empty",
            "saved": 0,
            "attempted": 0,
            "success": 0,
            "failed": 0,
            "total": 0,
        }

    logger.info("\n" + "=" * 60)
    logger.info(f"🔄 任务: 重试失败队列 ({len(symbols)} 只)")
    logger.info("=" * 60)

    success = 0
    still_failed: list[str] = []

    for symbol in symbols:
        result = _update_single_bar(db, loader, symbol)
        if result == "success":
            success += 1
        elif result != "skipped":
            still_failed.append(symbol)

    if still_failed:
        ProgressTracker.save(
            task="retry",
            last_symbol=symbols[-1],
            processed=len(symbols),
            total=len(symbols),
            failed_queue=still_failed,
        )
        logger.warning(f"⚠️  仍有 {len(still_failed)} 只失败，保留在重试队列中")
    else:
        ProgressTracker.clear()
        logger.info(f"✅ 重试完成: {success}/{len(symbols)} 只成功")

    result = {
        "status": "degraded" if still_failed else "success",
        "saved": success,
        "attempted": len(symbols),
        "success": success,
        "failed": len(still_failed),
        "total": len(symbols),
    }
    if still_failed:
        result["error"] = f"{len(still_failed)} failures"
    return result


def health_check(db: DatabaseInterface) -> dict:
    """检查数据库健康状态并生成报告。"""
    logger.info("\n" + "=" * 60)
    logger.info("🏥 任务: 数据质量健康检查")
    logger.info("=" * 60)

    today = datetime.now().strftime("%Y-%m-%d")
    issues: list[str] = []
    report_lines = [f"\n📋 SmartMoney 数据健康报告 ({today})\n" + "=" * 50]

    if not is_real_db_path(getattr(db, "db_path", None)):
        issues.append("无法打开数据库：db_path 不是有效路径")
        report_lines.append("  数据库连接失败")
        return {"issues": issues, "report": "\n".join(report_lines)}

    try:
        conn = sqlite3.connect(str(db.db_path))
    except sqlite3.OperationalError as e:
        issues.append(f"无法打开数据库 {db.db_path}: {e}")
        report_lines.append("  数据库连接失败")
        return {"issues": issues, "report": "\n".join(report_lines)}
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

    expected_latest = get_expected_latest_trading_day()
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

    # ── 字段级质量断言（2026-07 筹码全零/股息率全 NULL 事故后新增） ──
    # 这三个字段曾静默损坏且连续多日无人察觉，任何一项异常都必须显式告警。
    # 部分环境（测试 fixture / 旧库）可能缺表缺列，缺失时跳过对应断言。
    try:
        cursor.execute(
            """SELECT COUNT(*),
                      SUM(CASE WHEN turnover_rate IS NOT NULL AND turnover_rate > 0
                          THEN 1 ELSE 0 END)
               FROM daily_bars
               WHERE trade_date = (SELECT MAX(trade_date) FROM daily_bars)"""
        )
        tr_total, tr_valid = cursor.fetchone()
        if tr_total:
            tr_pct = 100 * (tr_valid or 0) / tr_total
            report_lines.append(f"  当日换手率非空率: {tr_valid}/{tr_total} ({tr_pct:.1f}%)")
            if tr_pct < 60:
                issues.append(f"daily_bars.turnover_rate 当日非空率过低: {tr_pct:.1f}% (< 60%)")

        cursor.execute(
            """SELECT COUNT(*),
                      SUM(CASE WHEN profit_ratio = 0 AND avg_cost = 0 THEN 1 ELSE 0 END)
               FROM chip_distribution_em
               WHERE trade_date = (SELECT MAX(trade_date) FROM chip_distribution_em)"""
        )
        chip_total, chip_zero = cursor.fetchone()
        if chip_total:
            zero_pct = 100 * (chip_zero or 0) / chip_total
            report_lines.append(f"  当日筹码全零率: {chip_zero}/{chip_total} ({zero_pct:.1f}%)")
            if zero_pct > 5:
                issues.append(f"chip_distribution_em 当日全零行占比过高: {zero_pct:.1f}% (> 5%)")

        cursor.execute(
            """SELECT COUNT(*),
                      SUM(CASE WHEN dividend_yield IS NOT NULL THEN 1 ELSE 0 END)
               FROM fundamentals
               WHERE trade_date = (SELECT MAX(trade_date) FROM fundamentals)"""
        )
        dy_total, dy_valid = cursor.fetchone()
        if dy_total:
            dy_pct = 100 * (dy_valid or 0) / dy_total
            report_lines.append(f"  当日股息率非空率: {dy_valid}/{dy_total} ({dy_pct:.1f}%)")
            if dy_pct < 40:
                issues.append(f"fundamentals.dividend_yield 当日非空率过低: {dy_pct:.1f}% (< 40%)")
    except sqlite3.OperationalError as exc:
        report_lines.append(f"  字段级质量断言跳过（表/列缺失）: {exc}")
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
                f"占比 {free_pct:.1f}%)，建议运行 `python scripts/validate_and_vacuum.py --vacuum` 进行压缩整理"
            )
    except Exception as e:
        logger.warning(f"⚠️  无法读取数据库 Page 状态: {e}")

    conn.close()

    db_size = Path(db.db_path).stat().st_size / (1024 * 1024)
    report_lines.append(f"\n  数据库大小: {db_size:.2f} MB")
    report_lines.append("=" * 50)
    report_lines.append("📋 详细契约审计: uv run python scripts/audit_data_contracts.py --db <path> --json")

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
