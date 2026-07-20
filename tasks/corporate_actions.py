"""
公司行动数据更新任务
────────────────
限售解禁、业绩预告。
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

import pandas as pd

from core.calendar import get_expected_latest_trading_day
from interface import DatabaseInterface

try:
    import akshare as ak
except ImportError:
    ak = None

logger = logging.getLogger(__name__)


def _to_float(value: object) -> float | None:
    """安全转换数值字段。"""
    if value is None:
        return None
    try:
        v = float(value)
        return None if pd.isna(v) else v
    except (TypeError, ValueError):
        return None


# ===========================================================================
# 限售解禁
# ===========================================================================


def _fetch_restricted_share() -> list[dict]:
    """获取限售解禁数据。

    stock_restricted_release_detail_em 按日期查询（YYYYMMDD 紧凑格式），
    单日查询常返回空，故向后回溯数天尝试多个日期以获取有效记录。
    """
    if ak is None:
        return []
    today = get_expected_latest_trading_day()
    # 尝试今天及回溯最多 7 天
    candidates = [today]
    base = datetime.strptime(today, "%Y-%m-%d")
    for i in range(1, 8):
        d = (base - timedelta(days=i)).strftime("%Y-%m-%d")
        candidates.append(d)

    seen: set[str] = set()
    records: list[dict] = []
    for date_str in candidates:
        date_compact = date_str.replace("-", "")
        try:
            df = ak.stock_restricted_release_detail_em(
                start_date=date_compact,
                end_date=date_compact,
            )
            if df is None or df.empty:
                continue
            for _, row in df.iterrows():
                ts_code = str(row.get("代码", "")).strip()
                # deduplicate by ts_code + release_date
                release_date = date_str
                key = f"{ts_code}_{release_date}"
                if key in seen:
                    continue
                seen.add(key)
                records.append(
                    {
                        "ts_code": ts_code,
                        "name": str(row.get("名称", "")).strip(),
                        "release_date": release_date,
                        "actual_release": _to_float(row.get("实际解禁数量")),
                        "total_shares": _to_float(row.get("总解禁量")),
                        "market_type": str(row.get("市场类型", "")).strip(),
                        "data_source": "akshare",
                    }
                )
        except Exception as e:
            logger.warning(f"⚠️ 限售解禁 {date_str} 获取失败: {e}")
    return records


def update_restricted_share(db: DatabaseInterface) -> dict:
    """获取限售解禁数据并保存。"""
    logger.info("\n" + "=" * 60)
    logger.info("🔒 任务: 更新限售解禁")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "error": "akshare not installed"}

    try:
        records = _fetch_restricted_share()
        if not records:
            logger.warning("⚠️ 限售解禁无数据")
            return {"saved": 0, "total": 0}
        saved = db.save_restricted_share_batch(records)
        logger.info(f"✅ 限售解禁保存完成: {saved} 条")
        return {"saved": saved, "total": len(records)}
    except Exception as e:
        logger.error(f"❌ 限售解禁更新失败: {e}")
        return {"saved": 0, "error": str(e)}


# ===========================================================================
# 业绩预告
# ===========================================================================


def _fetch_earnings_forecast() -> list[dict]:
    """获取全市场业绩预告数据。"""
    if ak is None:
        return []
    try:
        df = ak.stock_profit_forecast_em()
        if df is None or df.empty:
            return []
        records: list[dict] = []
        for _, row in df.iterrows():
            records.append(
                {
                    "ts_code": str(row.get("代码", "")).strip(),
                    "name": str(row.get("名称", "")).strip(),
                    "end_date": str(row.get("报告期", ""))[:10],
                    "forecast_type": str(row.get("预告类型", "")).strip(),
                    "net_profit_change": row.get("净利润变动幅度"),
                    "previous_profit": row.get("上年同期净利润"),
                    "data_source": "akshare",
                }
            )
        return records
    except Exception as e:
        logger.warning(f"⚠️ 业绩预告获取失败: {e}")
        return []


def update_earnings_forecast(db: DatabaseInterface) -> dict:
    """获取业绩预告并保存。"""
    logger.info("\n" + "=" * 60)
    logger.info("📋 任务: 更新业绩预告")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "error": "akshare not installed"}

    try:
        records = _fetch_earnings_forecast()
        if not records:
            logger.warning("⚠️ 业绩预告无数据")
            return {"saved": 0, "total": 0}
        saved = db.save_earnings_forecast_batch(records)
        logger.info(f"✅ 业绩预告保存完成: {saved} 条")
        return {"saved": saved, "total": len(records)}
    except Exception as e:
        logger.error(f"❌ 业绩预告更新失败: {e}")
        return {"saved": 0, "error": str(e)}


# ===========================================================================
# 合并更新
# ===========================================================================


def update_corporate_actions(db: DatabaseInterface) -> dict:
    """获取限售解禁、业绩预告数据并保存。"""
    logger.info("\n" + "=" * 60)
    logger.info("📋 任务: 更新限售解禁/业绩预告")
    logger.info("=" * 60)

    if ak is None:
        return {"error": "akshare not installed"}

    results: dict = {}

    try:
        records = _fetch_restricted_share()
        if records:
            saved = db.save_restricted_share_batch(records)
            results["restricted_share"] = saved
            logger.info(f"✅ 限售解禁: {saved} 条")
    except Exception as e:
        logger.warning(f"限售解禁失败: {e}")

    try:
        records = _fetch_earnings_forecast()
        if records:
            saved = db.save_earnings_forecast_batch(records)
            results["earnings_forecast"] = saved
            logger.info(f"✅ 业绩预告: {saved} 条")
    except Exception as e:
        logger.warning(f"业绩预告失败: {e}")

    return results
