"""
货币市场数据更新任务
────────────────────
SHIBOR、质押式回购利率（FR001/FR007/FR014）、
央行基准利率、央行资产负债表（月度）。
"""

from __future__ import annotations

import logging
from datetime import datetime
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
    """Safe call to an akshare function, return None on failure."""
    if ak is None:
        return None
    try:
        return func(**kwargs)
    except Exception as e:
        logger.warning(f"⚠️ {func.__name__} 获取失败: {e}")
        return None


# ===========================================================================
# SHIBOR
# ===========================================================================


def _fetch_shibor() -> list[dict]:
    """获取 SHIBOR 利率数据。"""
    df = _try_get_ak_df(ak.macro_china_shibor_all)
    if df is None or df.empty:
        return []
    col_map = {
        "日期": "date",
        "O/N-定价": "shibor_on",
        "1W-定价": "shibor_1w",
        "2W-定价": "shibor_2w",
        "1M-定价": "shibor_1m",
        "3M-定价": "shibor_3m",
        "6M-定价": "shibor_6m",
        "9M-定价": "shibor_9m",
        "1Y-定价": "shibor_1y",
    }
    df = df.rename(columns=col_map)
    keep = {"date", "shibor_on", "shibor_1w", "shibor_2w",
            "shibor_1m", "shibor_3m", "shibor_6m", "shibor_9m", "shibor_1y"}
    available = [c for c in keep if c in df.columns]
    df = df[available]
    records = []
    for _, row in df.iterrows():
        date_val = str(row.get("date", "")).strip()[:10]
        if date_val:
            records.append({
                "date": date_val,
                "shibor_on": _to_float(row.get("shibor_on")),
                "shibor_1w": _to_float(row.get("shibor_1w")),
                "shibor_2w": _to_float(row.get("shibor_2w")),
                "shibor_1m": _to_float(row.get("shibor_1m")),
                "shibor_3m": _to_float(row.get("shibor_3m")),
                "shibor_6m": _to_float(row.get("shibor_6m")),
                "shibor_9m": _to_float(row.get("shibor_9m")),
                "shibor_1y": _to_float(row.get("shibor_1y")),
            })
    return records


# ===========================================================================
# 质押式回购利率（FR001 / FR007 / FR014）
# ===========================================================================


def _fetch_repo_rates() -> list[dict]:
    """获取银行间质押式回购定盘利率（FR001/FR007/FR014）。"""
    df = _try_get_ak_df(ak.repo_rate_query)
    if df is None or df.empty:
        return []
    records = []
    for _, row in df.iterrows():
        date_val = row.get("date")
        if date_val is None:
            continue
        date_str = str(date_val).strip()[:10]
        if date_str:
            records.append({
                "date": date_str,
                "fr001": _to_float(row.get("FR001")),
                "fr007": _to_float(row.get("FR007")),
                "fr014": _to_float(row.get("FR014")),
            })
    return records


# ===========================================================================
# 央行基准利率
# ===========================================================================


def _fetch_pboc_policy_rate() -> list[dict]:
    """获取央行基准利率（存款/贷款基准利率调整）。"""
    df = _try_get_ak_df(ak.macro_bank_china_interest_rate)
    if df is None or df.empty:
        return []
    records = []
    for _, row in df.iterrows():
        date_val = row.get("日期")
        if date_val is None:
            continue
        date_str = str(date_val).strip()[:10]
        if date_str:
            records.append({
                "date": date_str,
                "pboc_policy_rate": _to_float(row.get("今值")),
            })
    return records


# ===========================================================================
# 央行资产负债表（月度）
# ===========================================================================


def _fetch_central_bank_balance() -> list[dict]:
    """获取央行（货币当局）资产负债表，按月发布。"""
    df = _try_get_ak_df(ak.macro_china_central_bank_balance)
    if df is None or df.empty:
        return []
    # 统计时间格式为 "2026.6"，转成 "2026-06-01"
    records = []
    for _, row in df.iterrows():
        t_str = str(row.get("统计时间", "")).strip()
        if not t_str:
            continue
        parts = t_str.split(".")
        if len(parts) == 2:
            year, month = parts[0], parts[1].zfill(2)
            date_str = f"{year}-{month}-01"
        else:
            continue
        records.append({
            "date": date_str,
            "total_assets": _to_float(row.get("总资产")),
            "reserve_money": _to_float(row.get("储备货币")),
            "currency_issue": _to_float(row.get("发行货币")),
            "claims_on_other_deposit": _to_float(row.get("对其他存款性公司债权")),
            "claims_on_gov": _to_float(row.get("对政府债权")),
            "gov_deposits": _to_float(row.get("政府存款")),
            "foreign_assets": _to_float(row.get("国外资产")),
            "fx_reserve": _to_float(row.get("外汇")),
        })
    return records


# ===========================================================================
# 主更新函数
# ===========================================================================


def update_money_market(db: DatabaseInterface) -> dict:
    """获取全部货币市场数据并保存。"""
    logger.info("\n" + "=" * 60)
    logger.info("💰 任务: 更新货币市场数据")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "error": "akshare not installed"}

    results: dict[str, Any] = {}
    data_date = datetime.now().strftime("%Y-%m-%d")

    # --- Daily money market: merge by date ---
    daily_merge: dict[str, dict] = {}
    daily_calls = [
        ("SHIBOR", _fetch_shibor),
        ("回购利率", _fetch_repo_rates),
        ("央行基准利率", _fetch_pboc_policy_rate),
    ]
    for name, fn in daily_calls:
        try:
            records = fn()
            for r in records:
                d = r.pop("date")
                if d not in daily_merge:
                    daily_merge[d] = {"date": d}
                daily_merge[d].update(r)
                daily_merge[d]["data_date"] = data_date
            results[name] = len(records)
        except Exception as e:
            logger.warning(f"⚠️ {name} 获取失败: {e}")
            results[name] = f"error: {e}"

    if daily_merge:
        daily_records = list(daily_merge.values())
        saved = db.save_money_market_batch(daily_records)
        logger.info(f"✅ 货币市场日度数据保存完成: {saved} 条 / {len(daily_records)} 个交易日")
    else:
        saved = 0
        logger.warning("⚠️ 货币市场日度数据无数据")
    results["daily_saved"] = saved

    # --- PBOC balance sheet (monthly) ---
    try:
        balance = _fetch_central_bank_balance()
        if balance:
            for r in balance:
                r["data_date"] = data_date
            saved_b = db.save_central_bank_balance_batch(balance)
            logger.info(f"✅ 央行资产负债表保存完成: {saved_b} 条")
        else:
            saved_b = 0
            logger.warning("⚠️ 央行资产负债表无数据")
    except Exception as e:
        saved_b = 0
        logger.warning(f"⚠️ 央行资产负债表获取失败: {e}")
        results["error_kind"] = "network"
    results["balance_saved"] = saved_b

    total_saved = results.get("daily_saved", 0) + results.get("balance_saved", 0)
    results["saved"] = total_saved
    if total_saved == 0:
        results["status"] = "no_data"
    else:
        results["status"] = "success"

    return dict(results)
