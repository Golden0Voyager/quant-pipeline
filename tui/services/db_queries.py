"""数据库查询服务函数与表元数据常量。"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from core.task_registry import (
    TABLE_LABELS,
    panel_date_columns,
)

# 面板新鲜度监控的 {表: 日期列} 映射，由 core.task_registry 派生（注册表
# 全集 ∪ 遗留表 institutional_holdings）；面板渲染只遍历 TABLE_LABELS。
TABLE_DATE_COLUMNS: dict[str, str] = panel_date_columns()

# 无有意义日期列的表（不显示新鲜度标记，只显示行数）
NO_DATE_TABLES: set[str] = set()

# 健康度统计中视为“健康”的状态（含周/月/季周期性更新标记）
_HEALTHY_STATUSES: tuple[str, ...] = (
    "最新",
    "T+1",
    "按周更新",
    "按月更新",
    "按季更新",
)


def get_all_table_counts(db_path: str, fast: bool = False) -> dict[str, int]:
    """Query row counts for all key tables.

    fast=True: 使用 PRAGMA page_count 估算（瞬间完成，大表有误差）
    fast=False: 精确 COUNT(*)（大表可能耗时 10s+）
    """
    p = Path(db_path)
    if not p.exists():
        return {}
    conn = None
    try:
        conn = sqlite3.connect(db_path, timeout=5.0)
        cur = conn.cursor()
        # 表名列表由 TABLE_LABELS 单一来源派生，防止与面板显示漂移
        tables = list(TABLE_LABELS.keys())
        result: dict[str, int] = {}
        if fast:
            # 快速估算：用 page_count * page_size 推算行数（SQLite 内部统计）
            cur.execute("PRAGMA page_count")
            page_count = cur.fetchone()[0]
            cur.execute("PRAGMA page_size")
            page_size = cur.fetchone()[0]
            db_bytes = page_count * page_size
            # 粗略估算：每行约 500 bytes（含索引开销）
            estimated_total = max(1, db_bytes // 500)
            for tbl in tables:
                result[tbl] = 0  # 先返回 0，后台精确更新
            result["_estimated_total"] = estimated_total
            result["_db_bytes"] = db_bytes
        else:
            for tbl in tables:
                try:
                    # 方括号包裹防止保留字冲突（表名来自内部常量 TABLE_LABELS）
                    cur.execute(f"SELECT COUNT(*) FROM [{tbl}]")
                    result[tbl] = cur.fetchone()[0]
                except Exception:
                    result[tbl] = 0
        return result
    except Exception:
        return {}
    finally:
        if conn is not None:
            conn.close()


def get_active_stock_count(db_path: str) -> int:
    """查询活跃股票总数。"""
    p = Path(db_path)
    if not p.exists():
        return 0
    conn = None
    try:
        conn = sqlite3.connect(db_path, timeout=5.0)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM stock_list")
        count = cursor.fetchone()[0]
        return count
    except Exception:
        return 0
    finally:
        if conn is not None:
            conn.close()


def get_recent_failed_tasks(db_path: str, limit: int = 10) -> list[dict[str, str]]:
    """查询最近失败/降级/中止的任务审计记录（ingestion_runs，按完成时间倒序）。

    用于运行结束后的失败明细报告；表不存在或不可读时返回空列表。
    """
    p = Path(db_path)
    if not p.exists():
        return []
    try:
        conn = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
        try:
            rows = conn.execute(
                """
                SELECT task_name, status, finished_at, error_kind, error_message
                FROM ingestion_runs
                WHERE status IN ('failed', 'degraded', 'aborted')
                ORDER BY finished_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        finally:
            conn.close()
    except sqlite3.Error:
        return []
    return [
        {
            "task_name": r[0],
            "status": r[1],
            "finished_at": r[2],
            "error_kind": r[3] or "",
            "error_message": r[4] or "",
        }
        for r in rows
    ]
