"""
期权情绪数据更新任务
────────────────────
QVIX 波动率指数 + 50ETF 期权 PCR (Put/Call Ratio)。
来源: legulegu (qvix) + sse (50ETF 期权日频)。
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


# ===========================================================================
# QVIX 波动率指数
# ===========================================================================


def _fetch_qvix() -> list[dict]:
    """获取上证50ETF QVIX 波动率指数。"""
    df = _try_get_ak_df(ak.index_option_50etf_qvix)
    if df is None or df.empty:
        return []
    records = []
    for _, row in df.iterrows():
        date_val = row.get("date") or row.get("日期")
        if date_val is None:
            continue
        date_str = str(date_val).strip()[:10]
        if date_str:
            records.append({
                "trade_date": date_str,
                "qvix": _to_float(row.get("qvix") or row.get("QVIX")),
            })
    return records


# ===========================================================================
# 50ETF 期权日频（含 PCR）
# ===========================================================================


def _fetch_50etf_daily() -> list[dict]:
    """获取上证50ETF期权日频行情，含成交量、持仓量、PCR。

    原接口 ``ak.stock_option_sse_50etf_daily`` 已在 akshare 新版中移除，
    目前无等效返回 PCR / 认沽认购成交量的历史日频接口。保留函数占位，
    若未来 akshare 恢复可用接口可在此扩展。
    """
    if ak is None or not hasattr(ak, "stock_option_sse_50etf_daily"):
        logger.warning(
            "⚠️ akshare 已移除 stock_option_sse_50etf_daily 接口，"
            "50ETF 期权 PCR 数据暂无法获取"
        )
        return []
    df = _try_get_ak_df(ak.stock_option_sse_50etf_daily)
    if df is None or df.empty:
        return []
    records = []
    for _, row in df.iterrows():
        date_val = row.get("date") or row.get("日期")
        if date_val is None:
            continue
        date_str = str(date_val).strip()[:10]
        if not date_str:
            continue
        put_vol = _to_int(row.get("put_volume") or row.get("认沽成交量"))
        call_vol = _to_int(row.get("call_volume") or row.get("认购成交量"))
        put_oi_val = _to_int(row.get("put_oi") or row.get("认沽持仓量"))
        call_oi_val = _to_int(row.get("call_oi") or row.get("认购持仓量"))
        pcr = None
        if call_vol is not None and call_vol > 0 and put_vol is not None:
            pcr = round(put_vol / call_vol, 4)
        implied_vol = _to_float(row.get("implied_vol_avg") or row.get("隐含波动率均值"))
        records.append({
            "trade_date": date_str,
            "pcr": pcr,
            "put_volume": put_vol,
            "call_volume": call_vol,
            "put_oi": put_oi_val,
            "call_oi": call_oi_val,
            "implied_vol_avg": implied_vol,
        })
    return records


# ===========================================================================
# 主更新函数
# ===========================================================================


def update_option_sentiment(db: DatabaseInterface) -> dict:
    """获取期权情绪数据并保存。"""
    logger.info("\n" + "=" * 60)
    logger.info("📊 任务: 更新期权情绪数据")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "error": "akshare not installed"}

    results: dict[str, Any] = {}

    # QVIX
    qvix_records = _fetch_qvix()
    results["qvix"] = len(qvix_records)
    logger.info(f"  QVIX 波动率: {len(qvix_records)} 条")

    # 50ETF 期权日频
    daily_records = _fetch_50etf_daily()
    results["daily"] = len(daily_records)
    logger.info(f"  50ETF 期权日频: {len(daily_records)} 条")

    # 合并
    merge: dict[str, dict] = {}
    for r in qvix_records:
        d = r["trade_date"]
        merge[d] = {"trade_date": d}
        merge[d].update(r)
    for r in daily_records:
        d = r["trade_date"]
        if d not in merge:
            merge[d] = {"trade_date": d}
        merge[d].update(r)

    if merge:
        records = list(merge.values())
        saved = db.save_option_sentiment_batch(records)
        logger.info(f"✅ 期权情绪数据保存完成: {saved} 条 / {len(records)} 个交易日")
        results["saved"] = saved
    else:
        logger.warning("⚠️ 期权情绪数据无数据")
        results["saved"] = 0

    return dict(results)
