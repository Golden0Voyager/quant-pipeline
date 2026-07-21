"""
股票回购数据更新任务
────────────────────
来源: stock_repurchase_em() (东方财富)
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
    "公告日期": "trade_date",
    "回购金额": "repurchase_amount",
    "回购价格": "repurchase_price",
    "回购数量": "repurchase_quantity",
    "进度": "progress_status",
    "trade_date": "trade_date",
    "stock_code": "stock_code",
    "stock_name": "stock_name",
    "repurchase_amount": "repurchase_amount",
    "repurchase_price": "repurchase_price",
    "repurchase_quantity": "repurchase_quantity",
    "progress_status": "progress_status",
}


def update_stock_repurchase(db: DatabaseInterface) -> dict:
    """获取股票回购数据并保存。"""
    logger.info("\n" + "=" * 60)
    logger.info("🔄 任务: 更新股票回购数据")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "error": "akshare not installed"}

    df = _try_get_ak_df(ak.stock_repurchase_em)
    if df is None or df.empty:
        logger.warning("⚠️ 股票回购数据为空")
        return {"saved": 0, "total": 0}

    raw_count = len(df)
    df = df.rename(columns=_COLUMN_MAP)
    keep = {"trade_date", "stock_code", "stock_name",
            "repurchase_amount", "repurchase_price",
            "repurchase_quantity", "progress_status"}
    available = [c for c in keep if c in df.columns]
    df = df[available]

    # 统一日期格式
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
            "repurchase_amount": _to_float(row.get("repurchase_amount")),
            "repurchase_price": _to_float(row.get("repurchase_price")),
            "repurchase_quantity": _to_int(row.get("repurchase_quantity")),
            "progress_status": str(row.get("progress_status", "")).strip() or None,
        })

    if not records:
        logger.warning("⚠️ 股票回购记录为空")
        return {"saved": 0, "total": raw_count}

    saved = db.save_stock_repurchase_batch(records)
    logger.info(f"✅ 股票回购数据保存完成: {saved}/{raw_count} 条")
    return {"saved": saved, "total": raw_count}
