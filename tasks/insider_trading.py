"""
董监高/大股东增减持数据更新任务
─────────────────────────────
来源: stock_cgxq_em() (东方财富)
"""

from __future__ import annotations

import logging
import time
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


_COLUMN_MAP = {
    "股票代码": "stock_code",
    "股票简称": "stock_name",
    "变动日期": "trade_date",
    "变动人姓名": "changer_name",
    "变动类型": "change_type",
    "变动数量": "change_quantity",
    "成交均价": "change_price",
    "变动后持股": "holdings_after_change",
    "stock_code": "stock_code",
    "stock_name": "stock_name",
    "trade_date": "trade_date",
    "changer_name": "changer_name",
    "change_type": "change_type",
    "change_quantity": "change_quantity",
    "change_price": "change_price",
    "holdings_after_change": "holdings_after_change",
}


def _fetch_insider_trading(market: str = "SH") -> list[dict]:
    """获取指定市场的增减持数据。

    原接口 ``ak.stock_cgxq_em`` 已在 akshare 新版中移除，且无返回等效
    日频全市场增减持数据的快速替代接口。当前任务保留占位，返回空列表，
    避免 AttributeError 导致整个任务异常终止。
    """
    if ak is None:
        return []
    if not hasattr(ak, "stock_cgxq_em"):
        logger.warning(
            "⚠️ akshare 已移除 stock_cgxq_em 接口，%s 增减持数据暂无法获取", market
        )
        return []
    try:
        df = ak.stock_cgxq_em(market=market)
    except Exception as e:
        logger.warning(f"⚠️ stock_cgxq_em({market}) 获取失败: {e}")
        return []
    if df is None or df.empty:
        return []

    df = df.rename(columns=_COLUMN_MAP)
    keep = {"trade_date", "stock_code", "stock_name", "changer_name",
            "change_type", "change_quantity", "change_price", "holdings_after_change"}
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
            "changer_name": str(row.get("changer_name", "")).strip(),
            "change_type": str(row.get("change_type", "")).strip(),
            "change_quantity": _to_int(row.get("change_quantity")),
            "change_price": _to_float(row.get("change_price")),
            "holdings_after_change": _to_float(row.get("holdings_after_change")),
        })
    return records


def update_insider_trading(db: DatabaseInterface) -> dict:
    """获取增减持数据并保存。"""
    logger.info("\n" + "=" * 60)
    logger.info("📋 任务: 更新董监高增减持数据")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "error": "akshare not installed"}

    all_records = []
    for market in ("SH", "SZ"):
        records = _fetch_insider_trading(market=market)
        all_records.extend(records)
        logger.info(f"  {market} 增减持: {len(records)} 条")
        time.sleep(1)  # 防限流

    if not all_records:
        logger.warning("⚠️ 增减持数据为空")
        return {"saved": 0}

    saved = db.save_insider_trading_batch(all_records)
    logger.info(f"✅ 增减持数据保存完成: {saved}/{len(all_records)} 条")
    return {"saved": saved, "total": len(all_records)}
