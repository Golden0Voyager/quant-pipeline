"""期权情绪数据更新任务——QVIX 波动率指数 + 50ETF 期权 PCR (Put/Call Ratio)。"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from core.calendar import get_expected_latest_trading_day
from core.data_contract import OPTION_SENTIMENT_CONTRACT, validate_records
from core.source_client import get_default_client
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


# ===========================================================================
# QVIX 波动率指数
# ===========================================================================


def _fetch_qvix() -> list[dict]:
    """获取上证50ETF QVIX 波动率指数。"""
    resp = get_default_client().call("eastmoney", lambda: ak.index_option_50etf_qvix())
    df = resp.data if resp.success else None
    if df is None or (hasattr(df, "empty") and df.empty):
        return []
    records = []
    for _, row in df.iterrows():
        date_val = row.get("date")
        if date_val is None or pd.isna(date_val):
            date_val = row.get("日期")
        if date_val is None or pd.isna(date_val):
            continue
        date_str = str(date_val).strip()[:10]
        if date_str:
            qvix = _to_float(row.get("close"))
            if qvix is None:
                qvix = _to_float(row.get("qvix"))
            if qvix is None:
                qvix = _to_float(row.get("QVIX"))
            records.append({
                "trade_date": date_str,
                "qvix": qvix,
            })
    return records


# ===========================================================================
# 50ETF 期权日频（含 PCR）
# ===========================================================================


def _fetch_50etf_daily() -> list[dict]:
    """获取上证50ETF期权日频行情，含成交量、持仓量、PCR。

    原接口 ``ak.stock_option_sse_50etf_daily`` 已在 akshare 新版中移除，
    改用 ``ak.option_daily_stats_sse`` 获取上交所期权每日统计，过滤 510050。
    """
    if ak is None or not hasattr(ak, "option_daily_stats_sse"):
        logger.warning("⚠️ akshare 缺少 option_daily_stats_sse 接口，50ETF 期权 PCR 数据暂无法获取")
        return []

    trade_date = get_expected_latest_trading_day()
    resp = get_default_client().call("eastmoney", lambda: ak.option_daily_stats_sse(date=trade_date.replace("-", "")))
    df = resp.data if resp.success else None
    if df is None or (hasattr(df, "empty") and df.empty):
        return []

    # 兼容新旧列名
    code_col = next((c for c in df.columns if "合约标的代码" in c), None)
    put_vol_col = next((c for c in df.columns if "认沽成交量" in c), None)
    call_vol_col = next((c for c in df.columns if "认购成交量" in c), None)
    put_oi_col = next((c for c in df.columns if "未平仓认沽合约数" in c), None)
    call_oi_col = next((c for c in df.columns if "未平仓认购合约数" in c), None)
    pcr_col = next((c for c in df.columns if "认沽/认购" in c), None)

    if code_col is None or put_vol_col is None or call_vol_col is None:
        logger.warning("⚠️ 上交所期权每日统计返回列名异常，跳过 50ETF PCR")
        return []

    row = df[df[code_col] == "510050"]
    if row.empty:
        return []

    row = row.iloc[0]
    put_vol = _to_int(row.get(put_vol_col))
    call_vol = _to_int(row.get(call_vol_col))
    put_oi_val = _to_int(row.get(put_oi_col)) if put_oi_col else None
    call_oi_val = _to_int(row.get(call_oi_col)) if call_oi_col else None
    pcr = _to_float(row.get(pcr_col)) if pcr_col else None
    if pcr is None and call_vol is not None and call_vol > 0 and put_vol is not None:
        pcr = round(put_vol / call_vol, 4)

    return [{
        "trade_date": trade_date,
        "pcr": pcr,
        "put_volume": put_vol,
        "call_volume": call_vol,
        "put_oi": put_oi_val,
        "call_oi": call_oi_val,
    }]


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
        validated_records, violations = validate_records(records, OPTION_SENTIMENT_CONTRACT, logger)
        if violations and not validated_records:
            logger.error(f"🚫 期权情绪数据合约校验失败: {violations}")
            results["saved"] = 0
            results["error"] = f"data contract violations: {violations}"
        else:
            if violations:
                logger.warning(f"⚠️ 期权情绪合约校验过滤 {len(records) - len(validated_records)} 条")
            saved = db.save_option_sentiment_batch(validated_records)
            logger.info(f"✅ 期权情绪数据保存完成: {saved} 条 / {len(records)} 个交易日")
            results["saved"] = saved
    else:
        logger.warning("⚠️ 期权情绪数据无数据")
        results["saved"] = 0

    return dict(results)
