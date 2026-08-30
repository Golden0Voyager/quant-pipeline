"""
股票回购数据更新任务
────────────────────
来源: stock_repurchase_em() (东方财富)
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from core.data_contract import STOCK_REPURCHASE_CONTRACT, validate_records
from core.source_client import get_default_client
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
    "最新公告日期": "trade_date",
    "已回购金额": "repurchase_amount",
    "已回购股份价格区间-下限": "repurchase_price_lower",
    "已回购股份价格区间-上限": "repurchase_price_upper",
    "已回购股份数量": "repurchase_quantity",
    "实施进度": "progress_status",
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
    "repurchase_price_lower": "repurchase_price_lower",
    "repurchase_price_upper": "repurchase_price_upper",
    "repurchase_quantity": "repurchase_quantity",
    "progress_status": "progress_status",
}


def _to_text(val: Any) -> str:
    if val is None or pd.isna(val):
        return ""
    return str(val).strip()


def update_stock_repurchase(db: DatabaseInterface) -> dict:
    """获取股票回购数据并保存。"""
    logger.info("\n" + "=" * 60)
    logger.info("🔄 任务: 更新股票回购数据")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "error": "akshare not installed"}

    resp = get_default_client().call("eastmoney", lambda: ak.stock_repurchase_em())
    if not resp.success:
        logger.error(f"❌ 股票回购数据获取失败: {resp.metadata.error}")
        return {"saved": 0, "error": resp.metadata.error or "fetch failed"}
    df = resp.data
    if df is None or (hasattr(df, "empty") and df.empty):
        logger.warning("⚠️ 股票回购数据为空")
        # 显式 skipped：零行属正常结果（如上流无新公告），避免被结果契约误判为 failed
        return {"skipped": True, "reason": "no stock repurchase data from upstream", "total": 0}

    raw_count = len(df)
    df = df.rename(columns=_COLUMN_MAP)
    keep = {"trade_date", "stock_code", "stock_name",
            "repurchase_amount", "repurchase_price", "repurchase_price_lower",
            "repurchase_price_upper",
            "repurchase_quantity", "progress_status"}
    available = [c for c in keep if c in df.columns]
    df = df[available].drop_duplicates()

    # 统一日期格式
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
        price_lower = _to_float(row.get("repurchase_price_lower"))
        price_upper = _to_float(row.get("repurchase_price_upper"))
        price = _to_float(row.get("repurchase_price"))
        if price is None:
            price = price_upper
        records.append({
            "trade_date": trade_date,
            "stock_code": stock_code,
            "stock_name": _to_text(row.get("stock_name")),
            "repurchase_amount": _to_float(row.get("repurchase_amount")),
            "repurchase_price": price,
            "repurchase_price_lower": price_lower,
            "repurchase_price_upper": price_upper,
            "repurchase_quantity": _to_int(row.get("repurchase_quantity")),
            "progress_status": _to_text(row.get("progress_status")) or None,
        })

    if not records:
        logger.warning("⚠️ 股票回购记录为空")
        # 显式 skipped：零行属正常结果，避免被结果契约误判为 failed
        return {"skipped": True, "reason": "no valid stock repurchase records", "total": raw_count}

    validated_records, violations = validate_records(records, STOCK_REPURCHASE_CONTRACT, logger)
    if violations and not validated_records:
        logger.error(f"🚫 股票回购数据合约校验失败: {violations}")
        return {"saved": 0, "total": raw_count, "error": f"data contract violations: {violations}"}
    if violations:
        logger.warning(f"⚠️ 股票回购合约校验过滤 {len(records) - len(validated_records)} 条")
    saved = db.save_stock_repurchase_batch(validated_records)
    logger.info(f"✅ 股票回购数据保存完成: {saved}/{raw_count} 条")
    if saved == 0:
        # 显式 skipped：全部记录已存在（幂等重跑），零新增属正常结果，
        # 避免被结果契约误判为 "zero rows without explanation" 失败
        return {"skipped": True, "reason": "all stock repurchase records already up to date",
                "saved": 0, "total": raw_count}
    return {"saved": saved, "total": raw_count}


# ===========================================================================
# 收盘刷新 helper（Task 9）：只抓取/归一化，不写库，源异常直接上抛
# ===========================================================================


def fetch_stock_repurchase_records() -> list[dict]:
    """收盘刷新专用：抓取全量回购快照并归一化，附 64 位稳定源键。

    权威空返回 []；源异常直接上抛（保留旧数据的语义由适配器/编排器落实）。
    """
    from core.source_record_key import STOCK_REPURCHASE_SOURCE_KEY_FIELDS, source_record_key

    df = ak.stock_repurchase_em()
    if df is None or df.empty:
        return []

    df = df.rename(columns=_COLUMN_MAP)
    keep = {"trade_date", "stock_code", "stock_name",
            "repurchase_amount", "repurchase_price", "repurchase_price_lower",
            "repurchase_price_upper", "repurchase_quantity", "progress_status"}
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
        price_lower = _to_float(row.get("repurchase_price_lower"))
        price_upper = _to_float(row.get("repurchase_price_upper"))
        price = _to_float(row.get("repurchase_price"))
        if price is None:
            price = price_upper
        record = {
            "trade_date": trade_date,
            "stock_code": stock_code,
            "stock_name": _to_text(row.get("stock_name")),
            "repurchase_amount": _to_float(row.get("repurchase_amount")),
            "repurchase_price": price,
            "repurchase_price_lower": price_lower,
            "repurchase_price_upper": price_upper,
            "repurchase_quantity": _to_int(row.get("repurchase_quantity")),
            "progress_status": _to_text(row.get("progress_status")) or None,
        }
        record["source_record_key"] = source_record_key(record, STOCK_REPURCHASE_SOURCE_KEY_FIELDS)
        records.append(record)
    return records
