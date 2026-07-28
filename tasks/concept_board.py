"""
概念板块数据更新任务
────────────────────
东方财富概念板块日频行情 + 成分股映射。
数据源：东方财富 (push2.eastmoney.com)。
"""
from __future__ import annotations

import logging
import uuid
from datetime import date
from typing import Any

import pandas as pd

from core.data_contract import CONCEPT_BOARD_CONTRACT, validate_records
from core.source_client import get_default_client
from interface import DatabaseInterface

try:
    import akshare as ak
except ImportError:
    ak = None

logger = logging.getLogger(__name__)


def _to_float(val: Any) -> float | None:
    if val is None:
        return None
    try:
        v = float(val)
        return None if pd.isna(v) else v
    except (ValueError, TypeError):
        return None


def _to_int(val: Any) -> int | None:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


# ===========================================================================
# 东方财富概念板块实时行情（含涨跌幅/成交额/涨跌家数）
# ===========================================================================


def _fetch_em_spot() -> list[dict]:
    """直接从东方财富 push2 接口获取概念板块实时行情（自动分页）。

    Raises on HTTP/network errors so that ``SourceClient.call()`` can
    handle retry and circuit-breaker logic.
    """
    base_url = (
        "https://push2.eastmoney.com/api/qt/clist/get"
        "?pn={page}&pz=100&po=1&np=1"
        "&ut=bd1d9ddb04089700cf9c27f6f7426281"
        "&fltt=2&invt=2&fid=f3"
        "&fs=m:90+t:3"
        "&fields=f3,f4,f12,f14,f104,f105"
    )
    session = get_default_client().get_session("eastmoney")
    today = date.today().isoformat()
    records = []
    page = 1
    while True:
        resp = session.get(base_url.format(page=page), timeout=15)
        resp.raise_for_status()
        data = resp.json()
        items = data.get("data", {}).get("diff", [])
        if not items:
            break
        for item in items:
            code = str(item.get("f12", "")).strip()
            name = str(item.get("f14", "")).strip()
            if not code or not name:
                continue
            records.append({
                "trade_date": today,
                "concept_code": code,
                "concept_name": name,
                "pct_change": _to_float(item.get("f3")),
                "turnover": _to_float(item.get("f4")),
                "up_count": _to_int(item.get("f104")),
                "down_count": _to_int(item.get("f105")),
                "data_source": "em",
            })
        if len(items) < 100:
            break
        page += 1
    return records


# ===========================================================================
# 概念板块成分股映射（使用东方财富个股接口）
# ===========================================================================


def _fetch_concept_list_em() -> list[dict]:
    """Fetch concept board name list from East Money push2 API."""
    name_url = (
        "https://push2.eastmoney.com/api/qt/clist/get"
        "?pn=1&pz=500&po=1&np=1"
        "&ut=bd1d9ddb04089700cf9c27f6f7426281"
        "&fltt=2&invt=2&fid=f3"
        "&fs=m:90+t:3"
        "&fields=f12,f14"
    )
    session = get_default_client().get_session("eastmoney")
    resp = session.get(name_url, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    items = data.get("data", {}).get("diff", [])
    out = []
    for item in items:
        code = str(item.get("f12", "")).strip()
        name = str(item.get("f14", "")).strip()
        if code and name:
            out.append({"concept_code": code, "concept_name": name})
    return out


def _fetch_concept_members_em() -> list[dict]:
    """从东方财富获取所有概念板块的成分股映射。

    每板块通过 stock_board_concept_cons_em 获取（绕过 AkShare session 直调）。
    """
    if ak is None:
        return []

    try:
        resp = get_default_client().call("eastmoney", _fetch_concept_list_em)
        if not resp.success:
            logger.warning(f"⚠️ 东方财富概念板块列表获取失败: {resp.metadata.error}")
            return []
        items = resp.data
    except Exception as e:
        logger.warning(f"⚠️ 东方财富概念板块列表获取失败: {e}")
        items = []

    members = []
    for item in items:
        code = item.get("concept_code", "")
        name = item.get("concept_name", "")
        if not code or not name:
            continue
        try:
            # Use akshare but wrapped in try/except — this API might work
            # since it goes to a different eastmoney endpoint
            df = ak.stock_board_concept_cons_em(code)
            if df is None or df.empty:
                continue
            ts_col = "代码" if "代码" in df.columns else (df.columns[0] if len(df.columns) > 0 else None)
            if ts_col is None:
                continue
            for _, row in df.iterrows():
                ts_code = str(row.get(ts_col, "")).strip()
                if ts_code:
                    members.append({
                        "concept_code": code,
                        "concept_name": name,
                        "ts_code": ts_code,
                    })
        except Exception as e:
            logger.warning(f"⚠️ 概念 {name}({code}) 成分股获取失败: {e}")
    return members


# ===========================================================================
# 主更新函数
# ===========================================================================


def update_concept_board(db: DatabaseInterface) -> dict:
    """获取概念板块日频行情并保存。"""
    logger.info("\n" + "=" * 60)
    logger.info("🏷️ 任务: 更新概念板块行情")
    logger.info("=" * 60)

    results: dict[str, Any] = {}

    resp = get_default_client().call("eastmoney", _fetch_em_spot)
    if not resp.success:
        error_msg = str(resp.metadata.error) if resp.metadata.error else "network error"
        logger.warning(f"⚠️ 概念板块行情获取失败: {error_msg}")
        return {
            "status": "failed",
            "error_kind": "network",
            "error": error_msg,
            "board_saved": 0,
            "saved": 0,
        }

    spot = resp.data
    if not spot:
        logger.warning("⚠️ 概念板块行情无数据")
        return {"status": "no_data", "board_saved": 0, "saved": 0}

    try:
        # 补充 trade_date 字段（实时行情接口不返回日期）
        today_str = date.today().isoformat()
        for r in spot:
            r.setdefault("trade_date", today_str)
        validated_spot, violations = validate_records(spot, CONCEPT_BOARD_CONTRACT, logger)
        if violations and not validated_spot:
            logger.error(f"🚫 概念板块行情数据合约校验失败: {violations}")
            return {
                "status": "failed",
                "error_kind": "data_quality",
                "error": f"contract validation failed: {violations}",
                "board_saved": 0,
                "saved": 0,
            }

        if violations:
            logger.warning(f"⚠️ 概念板块行情合约校验过滤 {len(spot) - len(validated_spot)} 条")
        saved_board = db.save_concept_board_batch(validated_spot)
        logger.info(f"✅ 概念板块行情保存完成: {saved_board} 条")
    except Exception as e:
        logger.warning(f"⚠️ 概念板块行情保存失败: {e}")
        return {
            "status": "failed",
            "error_kind": "internal",
            "error": str(e),
            "board_saved": 0,
            "saved": 0,
        }

    results["board_saved"] = saved_board
    results["saved"] = saved_board
    results["status"] = "success" if saved_board > 0 else "no_data"

    return dict(results)


def update_concept_member(
    db: DatabaseInterface,
    _task_run_id: str | None = None,
) -> dict:
    """获取概念板块成分股映射并保存（较慢，建议按需运行而非每日）。

    同时写入 ``concept_member``（快照表）和 ``concept_member_history``（PIT 历史表）。

    Args:
        db: 数据库接口
        _task_run_id: 由 ``safe_task`` 注入的运行 ID。为 None 时自动生成。
    """
    logger.info("\n" + "=" * 60)
    logger.info("🏷️ 任务: 更新概念板块成分股映射 (含 PIT)")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "error": "akshare not installed"}

    results: dict[str, Any] = {}
    run_id = _task_run_id or str(uuid.uuid4())
    valid_from = date.today().isoformat()

    try:
        members = _fetch_concept_members_em()
        if members:
            # legacy snapshot table
            saved_member = db.save_concept_member_batch(members)
            # PIT history table
            pit_saved = db.save_concept_member_history_batch(members, run_id, valid_from)
            unique_codes = {m["concept_code"] for m in members}
            logger.info(
                f"✅ 概念板块成分股: 快照 {saved_member} 条 / "
                f"PIT {pit_saved} 条 / {len(unique_codes)} 个板块"
            )
        else:
            saved_member = 0
            pit_saved = 0
            logger.warning("⚠️ 概念板块成分股无数据")
    except Exception as e:
        saved_member = 0
        pit_saved = 0
        logger.warning(f"⚠️ 概念板块成分股获取失败: {e}")
        return {
            "status": "failed",
            "error_kind": "network",
            "error": str(e),
            "member_saved": 0,
            "pit_saved": 0,
            "saved": 0,
        }
    results["member_saved"] = saved_member
    results["pit_saved"] = pit_saved
    results["saved"] = saved_member + pit_saved
    results["status"] = "success" if (saved_member + pit_saved) > 0 else "no_data"

    return dict(results)
