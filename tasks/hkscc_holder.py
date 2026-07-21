"""
沪深港通（北向）个股持仓数据更新任务
───────────────────────────────────
来源: stock_hsgt_individual_em() (东方财富)
写入现有 north_hold 表。
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


def _try_get_ak_df(func, **kwargs) -> pd.DataFrame | None:
    if ak is None:
        return None
    try:
        return func(**kwargs)
    except Exception as e:
        logger.warning(f"⚠️ {func.__name__} 获取失败: {e}")
        return None


_COLUMN_MAP = {
    # akshare 不同版本列名有差异：新版返回“持股日期”，旧版为“日期”
    "持股日期": "trade_date",
    "日期": "trade_date",
    "代码": "ts_code",
    "名称": "security_name",
    "收盘价": "close_price",
    "持股数量": "hold_shares",
    "持股市值": "hold_market_cap",
    "持股占流通股比": "hold_shares_ratio",
    "持股占总股本比": "total_shares_ratio",
    "trade_date": "trade_date",
    "ts_code": "ts_code",
    "security_name": "security_name",
    "close_price": "close_price",
    "hold_shares": "hold_shares",
    "hold_market_cap": "hold_market_cap",
    "hold_shares_ratio": "hold_shares_ratio",
    "total_shares_ratio": "total_shares_ratio",
}


def update_hkscc_holder(db: DatabaseInterface) -> dict:
    """获取北向个股持仓数据并保存到 north_hold 表。"""
    logger.info("\n" + "=" * 60)
    logger.info("🌐 任务: 更新北向资金个股持仓")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "error": "akshare not installed"}

    df = _try_get_ak_df(ak.stock_hsgt_individual_em)
    if df is None or df.empty:
        logger.warning("⚠️ 北向个股持仓数据为空")
        return {"saved": 0}

    raw_count = len(df)
    df = df.rename(columns=_COLUMN_MAP)

    if "trade_date" in df.columns:
        df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
        df = df[df["trade_date"].notna()]
        df["trade_date"] = df["trade_date"].dt.strftime("%Y-%m-%d")

    keep = {"trade_date", "ts_code", "security_name", "close_price",
            "hold_shares", "hold_market_cap", "hold_shares_ratio",
            "total_shares_ratio"}
    available = [c for c in keep if c in df.columns]
    df = df[available]

    records = []
    for _, row in df.iterrows():
        ts_code = str(row.get("ts_code") or "").strip()
        trade_date = row.get("trade_date")
        # 沪深交易所 2024-08-19 起停止每日个股北向披露；akshare 新版
        # stock_hsgt_individual_em 需指定 symbol 且不再返回个股代码列，
        # 没有有效代码/日期的记录不应写入 north_hold（NOT NULL 约束）。
        if not ts_code or not trade_date:
            continue
        records.append({
            "ts_code": ts_code,
            "security_name": str(row.get("security_name") or "").strip(),
            "trade_date": trade_date,
            "close_price": _to_float(row.get("close_price")),
            "hold_shares": _to_float(row.get("hold_shares")),
            "hold_market_cap": _to_float(row.get("hold_market_cap")),
            "hold_shares_ratio": _to_float(row.get("hold_shares_ratio")),
            "total_shares_ratio": _to_float(row.get("total_shares_ratio")),
            "data_source": "akshare",
        })

    if not records:
        logger.warning(
            "⚠️ 北向个股持仓记录为空。自 2024-08-19 起交易所不再披露每日个股北向数据，"
            "当前 akshare 接口也已变更，需指定 symbol 获取单只股票历史。"
        )
        return {"saved": 0, "total": raw_count}

    saved = db.save_north_hold_batch(records)
    logger.info(f"✅ 北向个股持仓数据保存完成: {saved}/{raw_count} 条")
    return {"saved": saved, "total": raw_count}
