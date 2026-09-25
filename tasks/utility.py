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
from typing import Any

import pandas as pd  # noqa: F401

from core.calendar import get_expected_latest_trading_day
from core.config import DB_PATH, SHARED_DATA_DIR  # noqa: F401
from core.day_coverage import missing_trading_days, scan_window
from core.known_gaps import (
    AUDIT_ERA_START,
    describe_known_gap,
    describe_missing_day,
    is_known_gap,
    is_known_missing_day,
)
from core.monitor import AkShareMonitor  # noqa: F401
from core.retained_streak import retained_streaks
from core.run_state import describe_run_state, read_run_state
from core.utils import is_real_db_path, should_skip_beijing, should_update  # noqa: F401
from interface import DatabaseInterface, DataLoaderInterface

# akshare 作为可选依赖（部分任务函数可能不直接使用它）
try:
    import akshare as ak
except ImportError:
    ak = None

# 从 core/tasks 模块导入（避免与 daily_pipeline.py 的循环依赖）
from core.progress import ProgressTracker
from tasks.bars import _detect_suspended_symbols, _normalize_trade_date, _source_has_trading_day, _update_single_bar

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

    # 哨兵判定（每轮一次）：数据源是否已有预期交易日数据。
    # 有 → 个股缺数说明当日停牌/未交易（K 线不存在，重试无意义，移出队列）；
    # 无 → 源端问题（保留重试资格）。
    expected_latest = _normalize_trade_date(get_expected_latest_trading_day())
    # 用 if 而非 `bool(...) and ...`：mypy 无法穿过 bool() 收窄到 str，
    # 且这样保留原有的「空串视为无预期日」语义
    source_has_day = False
    if expected_latest:
        source_has_day = _source_has_trading_day(loader, expected_latest)

    # 停牌预检：与 update_bars 主流程同源的停牌判定（雪球 status + 东财停复牌名单）。
    # 停牌股源端无新数据，直接跳过（_update_single_bar 返回 "skipped"，不计失败），
    # 避免反复占用重试配额直到复牌（2026-08-04：7 只停牌股滞留重试队列）。
    suspended_symbols = _detect_suspended_symbols(db, expected_latest)
    if suspended_symbols:
        logger.info(f"⏸️ 停牌预检：{len(suspended_symbols)} 只停牌股本次跳过重试: {sorted(suspended_symbols)}")

    success = 0
    still_failed: list[str] = []
    skipped_no_data: list[str] = []

    for symbol in symbols:
        # 与返回的 result dict 区分命名：同名会让 mypy 把它并成一个变量
        outcome = _update_single_bar(db, loader, symbol, suspended_symbols=suspended_symbols)
        if outcome == "success":
            success += 1
        elif outcome == "failed" and source_has_day:
            skipped_no_data.append(symbol)
            logger.info(
                f"  ⏸️ {symbol} 源端无 {expected_latest} 新数据"
                "（当日停牌/未交易），移出重试队列"
            )
        elif outcome != "skipped":
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
        "skipped_no_data": len(skipped_no_data),
        "total": len(symbols),
    }
    if still_failed:
        result["error"] = f"{len(still_failed)} failures"
    return result


def health_check(db: DatabaseInterface, fast: bool = False) -> dict:
    """检查数据库健康状态并生成报告。

    fast=True 时跳过 12 张表的逐表 COUNT(*)（千万行大表全表扫描，耗时分钟级），
    改用 PRAGMA page_count 做库级行数估算（与 tui.get_all_table_counts 同口径：
    page_count * page_size / 500）。page_count 是库级指标，给不出表级估算，
    故 fast 报告省略逐表行数行；覆盖率/最新日期/字段级断言不受影响。
    手动 --task health_check 入口默认 fast=False，保持精确口径。
    """
    logger.info("\n" + "=" * 60)
    logger.info("🏥 任务: 数据质量健康检查")
    logger.info("=" * 60)

    today = datetime.now().strftime("%Y-%m-%d")
    issues: list[str] = []
    report_lines = [f"\n📋 SmartMoney 数据健康报告 ({today})\n" + "=" * 50]

    if not is_real_db_path(getattr(db, "db_path", None)):
        issues.append("无法打开数据库：db_path 不是有效路径")
        report_lines.append("  数据库连接失败")
        return {
            "status": "failed",
            "error": issues[0],
            "issues": issues,
            "report": "\n".join(report_lines),
        }

    try:
        conn = sqlite3.connect(str(db.db_path))
    except sqlite3.OperationalError as e:
        issues.append(f"无法打开数据库 {db.db_path}: {e}")
        report_lines.append("  数据库连接失败")
        return {
            "status": "failed",
            "error": issues[0],
            "issues": issues,
            "report": "\n".join(report_lines),
        }
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

    if fast:
        # 库级估算（与 tui.get_all_table_counts(fast=True) 同口径）：
        # page_count 是库级指标，无法拆出表级行数，逐表计数行在 fast 报告中
        # 省略而非虚构；每行约 500 bytes（含索引开销）为粗略经验值。
        cursor.execute("PRAGMA page_count")
        page_count = cursor.fetchone()[0]
        cursor.execute("PRAGMA page_size")
        page_size = cursor.fetchone()[0]
        estimated_total = max(1, page_count * page_size // 500)
        report_lines.append(
            f"  全库估算行数: ~{estimated_total:,} 条 (page_count 库级估算，逐表精确计数已跳过)"
        )
    else:
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

    # ── 历史日期空洞巡检（2026-09-24 新增） ──
    # 上面那条断言只看 MAX(trade_date)，所以旧日期的空洞一旦不再是最新日期就
    # 永久不可见；而 update_market_snapshot 的作用域就是 MAX(trade_date)，那些
    # 空洞也不会自愈。这里扫描审计期内的全部日期，区分「已声明空洞」
    # （core/known_gaps.py，已核实不可回补）与「新增空洞」，只对后者告警：
    # 否则每次巡检都会重报同一批旧噪音，人就会开始无视它（2026-08 连续 8 次
    # 告警无人处理正是如此），真正的新空洞也就淹没了。
    try:
        gap_rows = cursor.execute(
            "SELECT trade_date, COUNT(*) AS total,"
            " SUM(CASE WHEN dividend_yield IS NOT NULL THEN 1 ELSE 0 END) AS filled"
            " FROM fundamentals WHERE trade_date >= ?"
            " GROUP BY trade_date ORDER BY trade_date",
            (AUDIT_ERA_START,),
        ).fetchall()
        known_hits: list[tuple[str, float]] = []
        new_holes: list[tuple[str, float]] = []
        for gap_date, gap_total, gap_filled in gap_rows:
            if not gap_total:
                continue
            hole_pct = 100 * (gap_filled or 0) / gap_total
            if hole_pct >= 40:
                continue
            target = known_hits if is_known_gap("fundamentals", "dividend_yield", gap_date) else new_holes
            target.append((gap_date, hole_pct))

        report_lines.append(
            f"\n  股息率空洞巡检（{AUDIT_ERA_START} 起）: "
            f"已声明 {len(known_hits)} 个，新增 {len(new_holes)} 个"
        )
        for gap_date, hole_pct in known_hits:
            cause = describe_known_gap("fundamentals", "dividend_yield", gap_date)
            report_lines.append(f"    {gap_date} ({hole_pct:.1f}%) 已声明：{cause}")
        for gap_date, hole_pct in new_holes:
            issues.append(
                f"新增股息率空洞: {gap_date} 非空率 {hole_pct:.1f}% 且未登记在 core/known_gaps.py"
            )
    except sqlite3.OperationalError as exc:
        report_lines.append(f"  股息率空洞巡检跳过（表/列缺失）: {exc}")

    # ── 整日缺席巡检（2026-09-24 新增） ──
    # 比股息率空洞更粗的一种：整天什么都没写。它不是「某个字段没写」，而是当天
    # 管线只跑了一部分（或根本没启动）——两种形态的逐日证据见 core/known_gaps.py。
    # 上一条巡检扫的是「存在于 fundamentals 的日期」，所以**缺席的日子不会出现在
    # 它结果里**；这里才是那个观察点。同样只对**不在册**的告警。
    window = scan_window(AUDIT_ERA_START, get_expected_latest_trading_day())
    if window is None:
        report_lines.append("\n  整日缺席巡检跳过（窗口内交易日不足或日历不可用）")
    else:
        absent = missing_trading_days(cursor, start=window[0], end=window[1])
        declared_absent = [d for d in absent if is_known_missing_day(d)]
        new_absent = [d for d in absent if not is_known_missing_day(d)]
        report_lines.append(
            f"\n  整日缺席巡检（{window[0]} ~ {window[1]}）: "
            f"已声明 {len(declared_absent)} 天，新增 {len(new_absent)} 天"
        )
        for day in declared_absent:
            report_lines.append(f"    {day} 已声明：{describe_missing_day(day)}")
        for day in new_absent:
            issues.append(
                f"整日缺席: {day} 无任何写入且未登记在 core/known_gaps.py"
            )

    # ── 运行完整性（2026-09-24 新增） ──
    # 部分运行的危害见 core/run_state.py：任务全 success、无告警，只留下数据空洞。
    # run_all 会在下一轮开始时主动告警；这里是第二道防线——针对那些不经过
    # run_all 的调用（如 --task 单任务）期间遗留的 in-progress 标记。
    try:
        run_state = read_run_state(db)
        report_lines.append(f"\n  上一轮管道状态: {describe_run_state(db)}")
        if run_state is not None and run_state[0] == "in-progress" and run_state[1] != today:
            issues.append(
                f"上一轮管道未完整结束: {run_state[1]} 开始后没有完成记录，该轮数据可能不完整"
            )
    except Exception as exc:  # noqa: BLE001 - 巡检不应因标记读取失败而整体失败
        report_lines.append(f"  运行状态标记读取失败: {exc}")

    # ── 连续「保留旧数据」巡检（2026-09-25 新增） ──
    # retained 以 0 退出、只映射到 warning，而 NOTIFICATION_LEVEL 默认 error 恰好把它
    # 压掉：一个任务可以连续多日静默退化到「数据完全没更新」而无人知晓（实测
    # update_concept_board 连续 5 天、update_fund_flow 连续 3 天，见 core/retained_streak.py）。
    # 单次 retained 是正常的（源端抖动，core.runner 内建重试已吸收），只对**连续多日**
    # 告警。命中即进 issues → 本轮 degraded → daily_pipeline 收尾通知升为 error 级。
    try:
        streaks = retained_streaks(cursor)
        if streaks:
            report_lines.append(
                f"\n  连续保留旧数据巡检: {len(streaks)} 个任务仍在退化"
            )
            for streak in streaks:
                report_lines.append("    " + streak.describe())
                issues.append(streak.describe())
        else:
            report_lines.append("\n  连续保留旧数据巡检: 无")
    except sqlite3.OperationalError as exc:
        report_lines.append(f"  连续保留旧数据巡检跳过（表缺失）: {exc}")

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

    # 诊断型任务不产出数据行：显式声明状态，避免被结果归一化
    # 当作 "零行无解释" 误判为 failed（2026-07-30 假警报）
    result: dict[str, Any] = {
        "issues": issues,
        "coverage_pct": coverage_pct,
        "latest_bar": latest_bar,
        "db_size_mb": db_size,
        "report": report,
    }
    if issues:
        result["status"] = "degraded"
        result["error"] = "; ".join(issues)
    else:
        result["status"] = "success"
        result["saved"] = 0
    return result
