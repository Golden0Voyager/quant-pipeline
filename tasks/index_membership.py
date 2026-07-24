"""
指数成分与权重历史快照
─────────────────────
点时间 (Point-in-Time) 的指数成员与权重记录，
基于 interval 模型（valid_from / valid_to）管理历史变更。
"""

from __future__ import annotations

import logging
import uuid
from datetime import date
from typing import Any

try:
    import akshare as ak
except ImportError:
    ak = None

from interface import DatabaseInterface

logger = logging.getLogger(__name__)

# 5 大 A 股核心指数（中证指数公司代码）
INDEX_DEFS: list[dict[str, str]] = [
    {"code": "000016", "name": "上证50"},
    {"code": "000300", "name": "沪深300"},
    {"code": "000905", "name": "中证500"},
    {"code": "000852", "name": "中证1000"},
    {"code": "932000", "name": "中证2000"},
]

_FALLBACK_WEIGHT = 0.0


def _infer_market(code: str) -> str:
    if code.startswith(("6", "9")):
        return "sh"
    if code.startswith(("0", "2", "3")):
        return "sz"
    if code.startswith(("4", "8", "920")):
        return "bj"
    return "sh"


def _normalize_ts_code(raw: str) -> str:
    raw = raw.strip()
    if not raw:
        return raw
    if "." in raw:
        return raw
    market = _infer_market(raw)
    return f"{raw}.{market}"


def _fetch_index_constituents(
    index_code: str, index_name: str,
) -> list[dict[str, Any]]:
    if ak is None:
        logger.error("akshare 未安装，无法获取指数成分")
        return []

    try:
        df = ak.index_stock_cons_weight_csindex(symbol=index_code)
    except Exception as exc:
        logger.warning(
            "⚠️ 获取 %s(%s) 成分股失败: %s",
            index_name, index_code, exc,
        )
        return []

    if df is None or df.empty:
        logger.warning("⚠️ %s(%s) 成分股数据为空", index_name, index_code)
        return []

    records: list[dict[str, Any]] = []
    has_weight = "权重" in df.columns

    for _, row in df.iterrows():
        raw_code = str(row.get("成分券代码", "")).strip()
        if not raw_code:
            continue
        ts_code = _normalize_ts_code(raw_code)
        weight = float(row.get("权重", _FALLBACK_WEIGHT)) if has_weight else _FALLBACK_WEIGHT
        records.append({
            "index_code": index_code,
            "index_name": index_name,
            "ts_code": ts_code,
            "weight": weight,
            "source": "csindex",
        })

    logger.info(
        "📊 %s(%s): %d 只成分股%s",
        index_name, index_code, len(records),
        f" (权重区间 {min(r['weight'] for r in records):.3f}% ~ {max(r['weight'] for r in records):.3f}%)"
        if records and has_weight else "",
    )
    return records


def update_index_membership(
    db: DatabaseInterface,
    _task_run_id: str | None = None,
) -> dict:
    logger.info("\n" + "=" * 60)
    logger.info("📊 任务: 更新指数成分与权重历史快照")
    logger.info("=" * 60)

    run_id = _task_run_id or str(uuid.uuid4())
    valid_from = date.today().isoformat()

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "error": "akshare not installed"}

    all_records: list[dict[str, Any]] = []
    failures: list[str] = []

    for idx_def in INDEX_DEFS:
        code = idx_def["code"]
        name = idx_def["name"]
        members = _fetch_index_constituents(code, name)
        if not members:
            failures.append(f"{name}({code})")
        all_records.extend(members)

    total = len(all_records)
    if total == 0:
        logger.warning("⚠️ 所有指数成分股均获取失败: %s", ", ".join(failures))
        return {
            "saved": 0,
            "status": "degraded" if failures else "failed",
            "error": f"all indices failed: {', '.join(failures)}",
        }

    saved = db.save_index_member_history_batch(all_records, run_id, valid_from)
    total_unique = len({r["ts_code"] for r in all_records})

    logger.info(
        "✅ 指数成分快照: %d 条 PIT / %d 只去重股票 / %d/%d 个指数成功",
        saved, total_unique, len(INDEX_DEFS) - len(failures), len(INDEX_DEFS),
    )

    result: dict[str, Any] = {
        "saved": saved,
        "attempted": total,
    }

    if failures:
        result["status"] = "degraded"
        result["error"] = f"{len(failures)} indices failed: {', '.join(failures)}"
    else:
        result["status"] = "success"

    return result
