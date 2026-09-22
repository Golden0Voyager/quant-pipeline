"""
股权质押数据更新任务
────────────────────
来源: stock_zyg_em() (东方财富 股权质押)
"""

from __future__ import annotations

import logging
import sqlite3
from typing import Any

import pandas as pd

from core.calendar import get_expected_latest_trading_day, get_recent_trading_days
from core.data_contract import STOCK_PLEDGE_CONTRACT, validate_records
from core.source_client import get_default_client
from core.source_record_key import stock_pledge_source_key
from core.utils import is_real_db_path
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


def _get_latest_stock_pledge_date(db: DatabaseInterface) -> str | None:
    """查询数据库中股权质押已有数据的最大日期，用于 API 无最新日期时回退。"""
    db_path = getattr(db, "db_path", None)
    if not is_real_db_path(db_path):
        return None
    try:
        with sqlite3.connect(str(db_path), timeout=5.0) as conn:
            cur = conn.cursor()
            cur.execute("SELECT MAX(trade_date) FROM stock_pledge")
            row = cur.fetchone()
            return row[0] if row and row[0] else None
    except Exception:
        return None


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
        # 显式 skipped：零行属预期（接口缺失），避免被结果契约误判为 failed
        return {"skipped": True, "reason": "akshare interface stock_gpzy_pledge_ratio_em unavailable"}

    target_date = get_expected_latest_trading_day()
    target_dates = get_recent_trading_days(target_date, 30)
    latest_db_date = _get_latest_stock_pledge_date(db)
    if latest_db_date and latest_db_date not in target_dates:
        target_dates.append(latest_db_date)

    client = get_default_client()
    df = None
    fetch_error: str | None = None
    for date in target_dates:
        resp = client.call("eastmoney", lambda d=date: ak.stock_gpzy_pledge_ratio_em(date=d.replace("-", "")))
        if not resp.success:
            fetch_error = resp.metadata.error or "fetch failed"
            continue
        df = resp.data
        if df is not None and hasattr(df, "empty") and not df.empty:
            logger.info(f"  股权质押使用日期 {date}")
            break

    if fetch_error and df is None:
        logger.error(f"❌ 股权质押数据获取失败: {fetch_error}")
        return {"saved": 0, "error": fetch_error, "error_kind": "network"}

    if df is None or df.empty:
        logger.warning("⚠️ 股权质押数据为空")
        # 显式 skipped：近 30 个交易日窗口内无质押数据属正常结果，避免被结果契约误判为 failed
        return {"skipped": True, "reason": "no stock pledge data in recent trading days"}

    raw_count = len(df)
    df = df.rename(columns=_COLUMN_MAP)
    keep = {"trade_date", "stock_code", "stock_name", "pledger",
            "pledge_amount", "pledge_ratio", "pledge_org"}
    available = [c for c in keep if c in df.columns]
    df = df[available].drop_duplicates()

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
        # 显式 skipped：零行属正常结果（字段过滤后无有效记录），避免被结果契约误判为 failed
        return {"skipped": True, "reason": "no valid stock pledge records", "total": raw_count}

    validated_records, violations = validate_records(records, STOCK_PLEDGE_CONTRACT, logger)
    if violations and not validated_records:
        logger.error(f"🚫 股权质押数据合约校验失败: {violations}")
        return {"saved": 0, "total": raw_count, "error": f"data contract violations: {violations}"}
    if violations:
        logger.warning(f"⚠️ 股权质押合约校验过滤 {len(records) - len(validated_records)} 条")
    # 附加稳定源键：null pledger 行重复插入时按键去重（迁移 011 唯一索引）
    for record in validated_records:
        record["source_record_key"] = stock_pledge_source_key(record)
    saved = db.save_stock_pledge_batch(validated_records)
    logger.info(f"✅ 股权质押数据保存完成: {saved}/{raw_count} 条")
    return {"saved": saved, "total": raw_count}


# ===========================================================================
# 收盘刷新 helper（Task 9）：只抓取/归一化，不写库，源异常直接上抛
# ===========================================================================


def fetch_stock_pledge_records(trade_date: str) -> list[dict]:
    """收盘刷新专用：抓取单日质押汇总并归一化，附 64 位稳定源键。

    空日返回 []（是否可接受由适配器按日探测把关）；源异常直接上抛。
    """
    df = ak.stock_gpzy_pledge_ratio_em(date=trade_date.replace("-", ""))
    if df is None or df.empty:
        return []

    df = df.rename(columns=_COLUMN_MAP)
    keep = {"trade_date", "stock_code", "stock_name", "pledger",
            "pledge_amount", "pledge_ratio", "pledge_org"}
    available = [c for c in keep if c in df.columns]
    df = df[available].drop_duplicates()
    if "trade_date" in df.columns:
        df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
        df = df[df["trade_date"].notna()]
        df["trade_date"] = df["trade_date"].dt.strftime("%Y-%m-%d")

    records: list[dict] = []
    for _, row in df.iterrows():
        record = {
            "trade_date": row.get("trade_date"),
            "stock_code": str(row.get("stock_code") or "").strip(),
            "stock_name": str(row.get("stock_name") or "").strip(),
            "pledger": str(row.get("pledger", "")).strip() or None,
            "pledge_amount": _to_float(row.get("pledge_amount")),
            "pledge_ratio": _to_float(row.get("pledge_ratio")),
            "pledge_org": str(row.get("pledge_org", "")).strip() or None,
        }
        record["source_record_key"] = stock_pledge_source_key(record)
        records.append(record)
    return records
