"""
表新鲜度判定与补全计算
──────────────────────
数据完整度面板与每周补全层共用的单一实现（2026-08-01 自 tui.py 收敛）。
判定语义与 tui.py 原实现完全一致，仅数据源从本地常量切换为
core.task_registry 派生视图。
"""
from __future__ import annotations

import sqlite3
import threading
from datetime import datetime
from pathlib import Path

from core.task_registry import (
    CATCH_UP_TASK_ORDER,
    panel_date_columns,
    table_date_columns,
    task_to_table,
)

_FRESHNESS_CACHE_LOCK = threading.Lock()
_LATEST_DATES_CACHE: dict[str, tuple[float, int, dict[str, str | None]]] = {}
_COVERAGE_CACHE: dict[str, tuple[float, int, str, tuple[int, int]]] = {}


def clear_freshness_cache() -> None:
    """清空新鲜度与覆盖率查询缓存。"""
    with _FRESHNESS_CACHE_LOCK:
        _LATEST_DATES_CACHE.clear()
        _COVERAGE_CACHE.clear()

# 按周度更新的表（数据源每周发布一次，不按交易日衡量新鲜度）
WEEKLY_TABLES: set[str] = {
    "stock_pledge",  # 中登公司每周五更新质押比例
    "index_member_history",  # 指数成分调整按周节奏发布（valid_from）
}

# 按月度更新的表（不按交易日衡量新鲜度）
MONTHLY_TABLES: set[str] = {
    "institutional_holdings",
    "macro_monthly",
    "stock_list",  # 股票清单按月全量刷新（updated_at）
    "concept_member",  # 概念成分按月刷新（updated_at）
}

# 随季报更新的表（使用 report_date/report_period，不按交易日衡量新鲜度）
QUARTERLY_TABLES: set[str] = {
    "shareholder_count",
    "quarterly_financials",
    "macro_quarterly",
    "north_hold",
    "earnings_forecast",
}

# T+1 更新的表（数据源当日尚未公布，取最近已发布日期，不按交易日衡量新鲜度）
DELAYED_PUBLISH_TABLES: set[str] = {
    "fx_rate",
    "us_treasury",
    "margin_trading",
    "dragon_tiger",
    "block_trade",
}


def normalize_date(value: object) -> str | None:
    """将日期/报告期归一化为 YYYY-MM-DD。

    部分表（margin_trading、dragon_tiger、block_trade、shareholder_count、
    quarterly_financials）的日期列以 YYYYMMDD 无横线格式存储；
    chip_distribution 等表以 YYYY-MM-DD HH:MM:SS 格式存储。需归一化后
    才能与期望日做新鲜度比较，否则会被 date_status 误判为滞后。
    """
    if value is None:
        return None
    s = str(value).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return s





def get_latest_dates(db_path: str) -> dict[str, str | None]:
    """查询每个表最新日期/报告期（已归一化为 YYYY-MM-DD）。"""
    p = Path(db_path)
    if not p.exists():
        return {}
    try:
        stat = p.stat()
        mtime, size = stat.st_mtime, stat.st_size
    except OSError:
        return {}

    abs_key = str(p.resolve())
    with _FRESHNESS_CACHE_LOCK:
        cached = _LATEST_DATES_CACHE.get(abs_key)
        if cached and cached[0] == mtime and cached[1] == size:
            return dict(cached[2])

    conn = None
    try:
        conn = sqlite3.connect(f"file:{abs_key}?mode=ro", uri=True, timeout=5.0)
        conn.execute("PRAGMA query_only = ON")
        cur = conn.cursor()
        result: dict[str, str | None] = {}
        for tbl, col in panel_date_columns().items():
            try:
                cur.execute(f"SELECT MAX({col}) FROM [{tbl}]")
                value = cur.fetchone()[0]
                result[tbl] = normalize_date(value)
            except Exception:
                result[tbl] = None

        with _FRESHNESS_CACHE_LOCK:
            _LATEST_DATES_CACHE[abs_key] = (mtime, size, dict(result))
        return result
    except Exception:
        return {}
    finally:
        if conn is not None:
            conn.close()


def get_daily_bars_coverage(db_path: str, expected_date: str) -> tuple[int, int]:
    """返回 (已更新到期望交易日的股票数, 有日线数据的股票总数)。

    用「各股最新交易日是否达到期望日」衡量覆盖率，比按行数对比更符合实际
    （新股历史不足、节假日等会导致行数天然少于理想值）。
    """
    p = Path(db_path)
    if not p.exists():
        return 0, 0
    try:
        stat = p.stat()
        mtime, size = stat.st_mtime, stat.st_size
    except OSError:
        return 0, 0

    abs_key = str(p.resolve())
    with _FRESHNESS_CACHE_LOCK:
        cached = _COVERAGE_CACHE.get(abs_key)
        if cached and cached[0] == mtime and cached[1] == size and cached[2] == expected_date:
            return cached[3]

    conn = None
    try:
        conn = sqlite3.connect(f"file:{abs_key}?mode=ro", uri=True, timeout=5.0)
        conn.execute("PRAGMA query_only = ON")
        cur = conn.cursor()
        # 容忍期望日前 2 个自然日（周末/节假日），视为已更新
        cur.execute(
            """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN max_date >= date(?, '-2 days') THEN 1 ELSE 0 END) AS up_to_date
            FROM (SELECT ts_code, MAX(trade_date) AS max_date FROM daily_bars GROUP BY ts_code)
            """,
            (expected_date,),
        )
        total, up_to_date = cur.fetchone()
        res = (int(up_to_date or 0), int(total or 0))
        with _FRESHNESS_CACHE_LOCK:
            _COVERAGE_CACHE[abs_key] = (mtime, size, expected_date, res)
        return res
    except Exception:
        return 0, 0
    finally:
        if conn is not None:
            conn.close()


def date_status(latest: str | None, expected: str) -> str:
    """返回日期新鲜度状态标签（纯文本，颜色由调用方根据 STATUS_STYLES 渲染）。"""
    if not latest:
        return "无数据"
    if latest == expected:
        return "最新"
    try:
        from datetime import datetime, timedelta

        latest_dt = datetime.strptime(latest, "%Y-%m-%d")
        expected_dt = datetime.strptime(expected, "%Y-%m-%d")
        # 数据日期 >= 期望日期 → 已更新到或超过预期（非交易日也有数据）
        if latest_dt >= expected_dt:
            return "最新"
        if latest_dt >= expected_dt - timedelta(days=2):
            return "略滞后"
    except Exception:
        pass
    return "滞后"


def status_for_table(
    table: str,
    latest: str | None,
    expected_date: str,
    updating_tables: list[str] | None,
) -> str:
    """返回指定表的新鲜度状态标签（纯文本）。"""
    if updating_tables and table in updating_tables:
        return "更新中"
    if table in WEEKLY_TABLES and latest:
        # 中登每周五发布；超过 10 个自然日未更新才判定为滞后，
        # 解析失败时保守地保留“按周更新”标记。
        try:
            latest_dt = datetime.strptime(latest, "%Y-%m-%d")
            expected_dt = datetime.strptime(expected_date, "%Y-%m-%d")
            if (expected_dt - latest_dt).days > 10:
                return date_status(latest, expected_date)
        except (ValueError, TypeError):
            pass
        return "按周更新"
    if table in MONTHLY_TABLES and latest:
        return "按月更新"
    if table in QUARTERLY_TABLES and latest:
        return "按季更新"
    if table in DELAYED_PUBLISH_TABLES and latest:
        return "T+1"
    return date_status(latest, expected_date)


def compute_catch_up_tasks(
    latest_dates: dict[str, str | None], expected_date: str
) -> list[str]:
    """根据完整度面板的新鲜度判定，计算需要补齐的任务清单（按执行顺序）。

    只补面板会标「略滞后/滞后」的表；T+1、周/月/季更、无数据（如北向
    资金停止披露）沿用面板既有语义，不视为缺失。
    """
    stale_tables = {
        tbl
        for tbl in table_date_columns()
        if status_for_table(
            tbl, latest_dates.get(tbl), expected_date, None
        )
        in ("略滞后", "滞后")
    }
    if not stale_tables:
        return []

    tasks: list[str] = []
    claimed: set[str] = set()
    for task in CATCH_UP_TASK_ORDER:
        tables = task_to_table().get(task, [])
        hit = [t for t in tables if t in stale_tables and t not in claimed]
        if hit:
            tasks.append(task)
            claimed.update(hit)
    return tasks
