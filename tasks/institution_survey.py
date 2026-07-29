"""
机构调研数据更新任务
────────────────────
来源: stock_jgdy_tj_em() (东方财富 机构调研统计)
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

import pandas as pd

from core.calendar import get_expected_latest_trading_day
from core.data_contract import INSTITUTION_SURVEY_CONTRACT, validate_records
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


_COLUMN_MAP = {
    "代码": "stock_code",
    "名称": "stock_name",
    "接待日期": "trade_date",
    "接待方式": "survey_type",
    "接待机构数量": "survey_count",
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


def _to_text(val: Any) -> str:
    if val is None or pd.isna(val):
        return ""
    return str(val).strip()


def update_institution_survey(db: DatabaseInterface) -> dict:
    """获取机构调研数据并保存。"""
    logger.info("\n" + "=" * 60)
    logger.info("🏢 任务: 更新机构调研数据")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "error": "akshare not installed"}

    latest_date = get_expected_latest_trading_day()
    start_date = (datetime.strptime(latest_date, "%Y-%m-%d") - timedelta(days=30)).strftime("%Y%m%d")
    resp = get_default_client().call("eastmoney", lambda: ak.stock_jgdy_tj_em(date=start_date))
    df = resp.data if resp.success else None
    if df is None or (hasattr(df, "empty") and df.empty):
        logger.warning("⚠️ 机构调研数据为空")
        return {"saved": 0}

    raw_count = len(df)
    df = df.rename(columns=_COLUMN_MAP)
    keep = {"trade_date", "stock_code", "stock_name", "survey_org", "survey_type", "survey_count"}
    available = [c for c in keep if c in df.columns]
    df = df[available].drop_duplicates()

    if "trade_date" in df.columns:
        df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
        df = df[df["trade_date"].notna()]
        df["trade_date"] = df["trade_date"].dt.strftime("%Y-%m-%d")

    records = []
    for _, row in df.iterrows():
        trade_date = row.get("trade_date")
        stock_code = _to_text(row.get("stock_code"))
        if not trade_date or not stock_code:
            continue
        records.append({
            "trade_date": trade_date,
            "stock_code": stock_code,
            "stock_name": _to_text(row.get("stock_name")),
            "survey_org": _to_text(row.get("survey_org")) or None,
            "survey_type": _to_text(row.get("survey_type")) or None,
            "survey_count": _to_int(row.get("survey_count")),
        })

    if not records:
        logger.warning("⚠️ 机构调研记录为空")
        return {"saved": 0, "total": raw_count}

    validated_records, violations = validate_records(records, INSTITUTION_SURVEY_CONTRACT, logger)
    if violations and not validated_records:
        logger.error(f"🚫 机构调研数据合约校验失败: {violations}")
        return {"saved": 0, "total": raw_count, "error": f"data contract violations: {violations}"}
    if violations:
        logger.warning(f"⚠️ 机构调研合约校验过滤 {len(records) - len(validated_records)} 条")
    saved = db.save_institution_survey_batch(validated_records)
    logger.info(f"✅ 机构调研数据保存完成: {saved}/{raw_count} 条")
    return {"saved": saved, "total": raw_count}


# ===========================================================================
# 收盘刷新 helper（Task 9）：只抓取/归一化，不写库，源异常直接上抛
# ===========================================================================


def fetch_institution_survey_records(start_date: str) -> list[dict]:
    """收盘刷新专用：抓取 start_date 起的机构调研统计并归一化，附 64 位稳定源键。

    权威空返回 []；源异常直接上抛（保留旧数据的语义由适配器/编排器落实）。
    """
    from core.source_record_key import INSTITUTION_SURVEY_SOURCE_KEY_FIELDS, source_record_key

    df = ak.stock_jgdy_tj_em(date=start_date.replace("-", ""))
    if df is None or df.empty:
        return []

    df = df.rename(columns=_COLUMN_MAP)
    keep = {"trade_date", "stock_code", "stock_name", "survey_org", "survey_type", "survey_count"}
    available = [c for c in keep if c in df.columns]
    df = df[available].drop_duplicates()
    if "trade_date" in df.columns:
        df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
        df = df[df["trade_date"].notna()]
        df["trade_date"] = df["trade_date"].dt.strftime("%Y-%m-%d")

    records: list[dict] = []
    for _, row in df.iterrows():
        trade_date = row.get("trade_date")
        stock_code = _to_text(row.get("stock_code"))
        if not trade_date or not stock_code:
            continue
        record = {
            "trade_date": trade_date,
            "stock_code": stock_code,
            "stock_name": _to_text(row.get("stock_name")),
            "survey_org": _to_text(row.get("survey_org")) or None,
            "survey_type": _to_text(row.get("survey_type")) or None,
            "survey_count": _to_int(row.get("survey_count")),
        }
        record["source_record_key"] = source_record_key(record, INSTITUTION_SURVEY_SOURCE_KEY_FIELDS)
        records.append(record)
    return records
