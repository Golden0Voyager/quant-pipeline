"""
机构调研数据更新任务
────────────────────
来源: stock_jgdy_tj_em() (东方财富 机构调研统计)
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


def _to_int(val: Any) -> int | None:
    if val is None:
        return None
    try:
        v = int(float(val))
        return None if pd.isna(val) else v
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


_COLUMN_MAP = {
    "股票代码": "stock_code",
    "股票简称": "stock_name",
    "调研日期": "trade_date",
    "调研机构": "survey_org",
    "调研类型": "survey_type",
    "接待人数": "survey_count",
    "stock_code": "stock_code",
    "stock_name": "stock_name",
    "trade_date": "trade_date",
    "survey_org": "survey_org",
    "survey_type": "survey_type",
    "survey_count": "survey_count",
}


def update_institution_survey(db: DatabaseInterface) -> dict:
    """获取机构调研数据并保存。"""
    logger.info("\n" + "=" * 60)
    logger.info("🏢 任务: 更新机构调研数据")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "error": "akshare not installed"}

    df = _try_get_ak_df(ak.stock_jgdy_tj_em)
    if df is None or df.empty:
        logger.warning("⚠️ 机构调研数据为空")
        return {"saved": 0}

    raw_count = len(df)
    df = df.rename(columns=_COLUMN_MAP)
    keep = {"trade_date", "stock_code", "stock_name", "survey_org", "survey_type", "survey_count"}
    available = [c for c in keep if c in df.columns]
    df = df[available]

    if "trade_date" in df.columns:
        df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
        df = df[df["trade_date"].notna()]
        df["trade_date"] = df["trade_date"].dt.strftime("%Y-%m-%d")

    records = []
    for _, row in df.iterrows():
        records.append({
            "trade_date": row.get("trade_date"),
            "stock_code": str(row.get("stock_code") or "").strip(),
            "stock_name": str(row.get("stock_name") or "").strip(),
            "survey_org": str(row.get("survey_org", "")).strip(),
            "survey_type": str(row.get("survey_type", "")).strip(),
            "survey_count": _to_int(row.get("survey_count")),
        })

    if not records:
        logger.warning("⚠️ 机构调研记录为空")
        return {"saved": 0, "total": raw_count}

    saved = db.save_institution_survey_batch(records)
    logger.info(f"✅ 机构调研数据保存完成: {saved}/{raw_count} 条")
    return {"saved": saved, "total": raw_count}
