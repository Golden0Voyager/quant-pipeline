"""收盘刷新审计与状态归类服务。"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

# 收盘刷新结构化任务状态 → 展示标签（设计文档要求的六态区分）
REFRESH_STATE_LABELS: dict[str, str] = {
    "pending": "未运行",
    "fetched_not_validated": "已抓取未通过校验",
    "committed": "已提交覆盖",
    "retained": "保留旧数据",
    "degraded": "部分降级",
    "blocked": "被依赖任务阻塞",
}


def classify_refresh_task_state(record: dict | None) -> str:
    """将 refresh_task_runs 审计记录归类为结构化展示状态。

    关键区分：已提交覆盖（新数据已落库）与保留旧数据（旧数据原样保留）。
    """
    if record is None or record.get("status") is None:
        return "pending"
    metadata = record.get("metadata") or {}
    if metadata.get("blocked_by"):
        return "blocked"
    status = record["status"]
    if status == "success":
        return "committed"
    if status == "degraded":
        return "degraded"
    # failed / aborted / no_data：旧数据保留；区分“抓到但未通过校验”
    if record.get("fetched", 0) > 0 and record.get("validated", 0) == 0:
        return "fetched_not_validated"
    return "retained"


def get_latest_refresh_run_id(db_path: str) -> str | None:
    """读取审计库中最近一次收盘刷新的 run_id（启动前边界快照）。

    审计表不存在或库不可读时返回 None（视为无历史运行），不抛异常。
    """
    p = Path(db_path)
    if not p.exists():
        return None
    conn = None
    try:
        conn = sqlite3.connect(db_path, timeout=5.0)
        row = conn.execute(
            "SELECT run_id FROM refresh_runs ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
        return row[0] if row is not None else None
    except Exception:
        return None
    finally:
        if conn is not None:
            conn.close()


def get_latest_refresh_task_states(
    db_path: str, boundary_run_id: str | None = None
) -> list[dict]:
    """读取最近一次收盘刷新的每任务审计记录（含结构化状态归类）。

    审计表不存在或库不可读时返回空列表，不抛异常。
    boundary_run_id 为启动前捕获的边界：若最新 run 仍是边界本身（子进程
    崩溃于写入自身 refresh_runs 行之前），返回空列表，避免把上一次运行
    的结果误报为本次。
    """
    p = Path(db_path)
    if not p.exists():
        return []
    conn = None
    try:
        conn = sqlite3.connect(db_path, timeout=5.0)
        cur = conn.cursor()
        row = cur.execute(
            "SELECT run_id FROM refresh_runs ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
        if row is None:
            return []
        if boundary_run_id is not None and row[0] == boundary_run_id:
            return []
        rows = cur.execute(
            """SELECT task_name, status, fetched, validated, replaced,
                      retained, failed, metadata_json
               FROM refresh_task_runs WHERE run_id = ?""",
            (row[0],),
        ).fetchall()
        records: list[dict] = []
        for task_name, status, fetched, validated, replaced, retained, failed, metadata_json in rows:
            try:
                metadata = json.loads(metadata_json or "{}")
            except (TypeError, json.JSONDecodeError):
                metadata = {}
            record = {
                "task_name": task_name,
                "status": status,
                "fetched": fetched,
                "validated": validated,
                "replaced": replaced,
                "retained": retained,
                "failed": failed,
                "metadata": metadata,
            }
            record["state"] = classify_refresh_task_state(record)
            records.append(record)
        return records
    except Exception:
        return []
    finally:
        if conn is not None:
            conn.close()


def format_refresh_summary(records: list[dict]) -> str:
    """汇总收盘刷新结果：区分已提交覆盖与保留旧数据，附失败态计数。"""
    if not records:
        return "收盘刷新: 无任务审计记录"
    counts: dict[str, int] = {}
    for record in records:
        state = record.get("state", "pending")
        counts[state] = counts.get(state, 0) + 1
    parts = [
        f"{REFRESH_STATE_LABELS[state]} {counts[state]}"
        for state in REFRESH_STATE_LABELS
        if state in counts
    ]
    return "收盘刷新: " + "，".join(parts)
