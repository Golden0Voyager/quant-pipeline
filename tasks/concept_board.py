"""
概念板块数据更新任务
────────────────────
同花顺概念板块日频行情 + 成分股映射。
数据源：同花顺 (10jqka)，稳定，无需抗限流封装。
"""
from __future__ import annotations

import logging
from typing import Any

import pandas as pd

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


def _try_get_ak_df(func, **kwargs) -> pd.DataFrame | None:
    if ak is None:
        return None
    try:
        return func(**kwargs)
    except Exception as e:
        logger.warning(f"⚠️ {func.__name__} 获取失败: {e}")
        return None


# ===========================================================================
# 概念板块列表
# ===========================================================================


def _fetch_concept_list() -> list[dict]:
    """获取同花顺概念板块列表。"""
    df = _try_get_ak_df(ak.stock_board_concept_name_ths)
    if df is None or df.empty:
        return []
    col_map = {
        "板块名称": "concept_name",
        "板块代码": "concept_code",
    }
    df = df.rename(columns=col_map)
    records = []
    for _, row in df.iterrows():
        code = str(row.get("concept_code", "")).strip()
        name = str(row.get("concept_name", "")).strip()
        if code and name:
            records.append({
                "concept_code": code,
                "concept_name": name,
            })
    return records


# ===========================================================================
# 概念板块日频行情摘要
# ===========================================================================


def _fetch_concept_summary() -> list[dict]:
    """获取概念板块日频行情（涨跌幅/成交额/涨跌家数）。"""
    df = _try_get_ak_df(ak.stock_board_concept_summary_ths)
    if df is None or df.empty:
        return []
    col_map = {
        "日期": "trade_date",
        "板块名称": "concept_name",
        "涨跌幅": "pct_change",
        "成交额": "turnover",
        "上涨家数": "up_count",
        "下跌家数": "down_count",
    }
    df = df.rename(columns=col_map)
    keep = {"trade_date", "concept_name", "pct_change", "turnover", "up_count", "down_count"}
    available = [c for c in keep if c in df.columns]
    df = df[available]
    records = []
    for _, row in df.iterrows():
        date_val = str(row.get("trade_date", "")).strip()[:10]
        name = str(row.get("concept_name", "")).strip()
        if date_val and name:
            records.append({
                "trade_date": date_val,
                "concept_code": name,
                "concept_name": name,
                "pct_change": _to_float(row.get("pct_change")),
                "turnover": _to_float(row.get("turnover")),
                "up_count": _to_int(row.get("up_count")),
                "down_count": _to_int(row.get("down_count")),
            })
    return records


# ===========================================================================
# 概念板块成分股映射
# ===========================================================================


def _fetch_concept_members(concept_list: list[dict]) -> list[dict]:
    """获取所有概念板块的成分股映射。

    Args:
        concept_list: _fetch_concept_list() 返回的概念列表。
    """
    if ak is None:
        return []
    members = []
    for item in concept_list:
        code = item["concept_code"]
        name = item["concept_name"]
        try:
            df = ak.stock_board_concept_cons_ths(code)
            if df is None or df.empty:
                continue
            # 同花顺成分股返回列包含 "代码" 列
            col = "代码" if "代码" in df.columns else (df.columns[0] if len(df.columns) > 0 else None)
            if col is None:
                continue
            for _, row in df.iterrows():
                ts_code = str(row.get(col, "")).strip()
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
    """获取概念板块数据并保存。"""
    logger.info("\n" + "=" * 60)
    logger.info("🏷️ 任务: 更新概念板块数据")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "error": "akshare not installed"}

    results: dict[str, Any] = {}

    # --- 概念板块日频行情 ---
    try:
        summary = _fetch_concept_summary()
        if summary:
            # 为每条记录补充 data_source
            for r in summary:
                r.setdefault("data_source", "ths")
            saved_board = db.save_concept_board_batch(summary)
            logger.info(f"✅ 概念板块行情保存完成: {saved_board} 条")
        else:
            saved_board = 0
            logger.warning("⚠️ 概念板块行情无数据")
    except Exception as e:
        saved_board = 0
        logger.warning(f"⚠️ 概念板块行情获取失败: {e}")
    results["board_saved"] = saved_board

    # --- 概念板块列表 & 成分股映射 ---
    try:
        concept_list = _fetch_concept_list()
        results["concept_count"] = len(concept_list)
        if concept_list:
            members = _fetch_concept_members(concept_list)
            if members:
                saved_member = db.save_concept_member_batch(members)
                logger.info(f"✅ 概念板块成分股保存完成: {saved_member} 条 / {len(concept_list)} 个板块")
            else:
                saved_member = 0
                logger.warning("⚠️ 概念板块成分股无数据")
        else:
            saved_member = 0
    except Exception as e:
        saved_member = 0
        logger.warning(f"⚠️ 概念板块列表/成分股获取失败: {e}")
    results["member_saved"] = saved_member

    results["saved"] = saved_board + saved_member

    return dict(results)
