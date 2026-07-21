"""
股权质押数据更新任务
────────────────────
来源: stock_zyg_em() (东方财富 股权质押)
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
        if isinstance(val, str):
            val = val.strip().rstrip("%")
        v = float(val)
        return None if pd.isna(v) else v
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
    # akshare 新版 ``stock_zyg_em`` 已移除，改用 ``stock_gpzy_pledge_ratio_em``
    # 该接口返回股票级质押汇总（无单笔股东/机构明细），pledger/pledge_org 置空
    "股票代码": "stock_code",
    "股票简称": "stock_name",
    "交易日期": "trade_date",
    "质押日期": "trade_date",
    "质押股数": "pledge_amount",
    "质押数量": "pledge_amount",
    "质押比例": "pledge_ratio",
    "质押股东": "pledger",
    "质押机构": "pledge_org",
    "stock_code": "stock_code",
    "stock_name": "stock_name",
    "trade_date": "trade_date",
    "pledger": "pledger",
    "pledge_amount": "pledge_amount",
    "pledge_ratio": "pledge_ratio",
    "pledge_org": "pledge_org",
}


def update_stock_pledge(db: DatabaseInterface) -> dict:
    """获取股权质押数据并保存。"""
    logger.info("\n" + "=" * 60)
    logger.info("🔒 任务: 更新股权质押数据")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "error": "akshare not installed"}

    if not hasattr(ak, "stock_gpzy_pledge_ratio_em"):
        logger.warning("⚠️ akshare 不存在 stock_gpzy_pledge_ratio_em 接口，跳过股权质押更新")
        return {"saved": 0}

    df = _try_get_ak_df(ak.stock_gpzy_pledge_ratio_em)
    if df is None or df.empty:
        logger.warning("⚠️ 股权质押数据为空")
        return {"saved": 0}

    raw_count = len(df)
    df = df.rename(columns=_COLUMN_MAP)
    keep = {"trade_date", "stock_code", "stock_name", "pledger",
            "pledge_amount", "pledge_ratio", "pledge_org"}
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
            "pledger": str(row.get("pledger", "")).strip() or None,
            "pledge_amount": _to_float(row.get("pledge_amount")),
            "pledge_ratio": _to_float(row.get("pledge_ratio")),
            "pledge_org": str(row.get("pledge_org", "")).strip() or None,
        })

    if not records:
        logger.warning("⚠️ 股权质押记录为空")
        return {"saved": 0, "total": raw_count}

    saved = db.save_stock_pledge_batch(records)
    logger.info(f"✅ 股权质押数据保存完成: {saved}/{raw_count} 条")
    return {"saved": saved, "total": raw_count}
