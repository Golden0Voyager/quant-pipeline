"""
概念板块数据更新任务
────────────────────
东方财富概念板块日频行情 + 成分股映射。
数据源：东方财富 (push2.eastmoney.com)。
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Any

import pandas as pd
import requests

from interface import DatabaseInterface

try:
    import akshare as ak
except ImportError:
    ak = None

logger = logging.getLogger(__name__)

_EM_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://data.eastmoney.com/",
}


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
# 东方财富概念板块实时行情（含涨跌幅/成交额/涨跌家数）
# ===========================================================================


def _fetch_em_spot() -> list[dict]:
    """直接从东方财富 push2 接口获取概念板块实时行情（自动分页）。

    返回 [{concept_code, concept_name, pct_change, turnover, up_count, down_count}, ...]
    """
    base_url = (
        "https://push2.eastmoney.com/api/qt/clist/get"
        "?pn={page}&pz=100&po=1&np=1"
        "&ut=bd1d9ddb04089700cf9c27f6f7426281"
        "&fltt=2&invt=2&fid=f3"
        "&fs=m:90+t:3"
        "&fields=f3,f4,f12,f14,f104,f105"
    )
    try:
        today = date.today().isoformat()
        records = []
        page = 1
        while True:
            resp = requests.get(base_url.format(page=page), headers=_EM_HEADERS, timeout=15)
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
    except Exception as e:
        logger.warning(f"⚠️ 东方财富概念板块行情获取失败: {e}")
        return []


# ===========================================================================
# 概念板块成分股映射（使用东方财富个股接口）
# ===========================================================================


def _fetch_concept_members_em() -> list[dict]:
    """从东方财富获取所有概念板块的成分股映射。

    每板块通过 stock_board_concept_cons_em 获取（绕过 AkShare session 直调）。
    """
    if ak is None:
        return []
    # Step 1: get concept list from eastmoney name API (bypass session via requests)
    name_url = (
        "https://push2.eastmoney.com/api/qt/clist/get"
        "?pn=1&pz=500&po=1&np=1"
        "&ut=bd1d9ddb04089700cf9c27f6f7426281"
        "&fltt=2&invt=2&fid=f3"
        "&fs=m:90+t:3"
        "&fields=f12,f14"
    )
    try:
        resp = requests.get(name_url, headers=_EM_HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        items = data.get("data", {}).get("diff", [])
    except Exception as e:
        logger.warning(f"⚠️ 东方财富概念板块列表获取失败: {e}")
        items = []

    members = []
    for item in items:
        code = str(item.get("f12", "")).strip()
        name = str(item.get("f14", "")).strip()
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

    # 不依赖 akshare — 直调 eastmoney push2 接口
    results: dict[str, Any] = {}

    try:
        spot = _fetch_em_spot()
        if spot:
            saved_board = db.save_concept_board_batch(spot)
            logger.info(f"✅ 概念板块行情保存完成: {saved_board} 条")
        else:
            saved_board = 0
            logger.warning("⚠️ 概念板块行情无数据")
    except Exception as e:
        saved_board = 0
        logger.warning(f"⚠️ 概念板块行情获取失败: {e}")
    results["board_saved"] = saved_board
    results["saved"] = saved_board

    return dict(results)


def update_concept_member(db: DatabaseInterface) -> dict:
    """获取概念板块成分股映射并保存（较慢，建议按需运行而非每日）。"""
    logger.info("\n" + "=" * 60)
    logger.info("🏷️ 任务: 更新概念板块成分股映射")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "error": "akshare not installed"}

    results: dict[str, Any] = {}

    try:
        members = _fetch_concept_members_em()
        if members:
            saved_member = db.save_concept_member_batch(members)
            unique_codes = {m["concept_code"] for m in members}
            logger.info(f"✅ 概念板块成分股保存完成: {saved_member} 条 / {len(unique_codes)} 个板块")
        else:
            saved_member = 0
            logger.warning("⚠️ 概念板块成分股无数据")
    except Exception as e:
        saved_member = 0
        logger.warning(f"⚠️ 概念板块成分股获取失败: {e}")
    results["member_saved"] = saved_member
    results["saved"] = saved_member

    return dict(results)
