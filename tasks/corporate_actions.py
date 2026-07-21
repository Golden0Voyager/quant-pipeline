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
from core.utils import warn_if_all_empty
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

    ``stock_restricted_release_detail_em`` 接受日期区间（YYYYMMDD 紧凑格式），
    单次查询近 30 天以覆盖最新解禁记录。列名以实际返回为准：
    ``股票代码``/``股票简称``/``解禁时间``/``限售股类型``/``解禁数量``/``实际解禁数量``。
    """
    if ak is None:
        return []
    today = get_expected_latest_trading_day()
    base = datetime.strptime(today, "%Y-%m-%d")
    start_compact = (base - timedelta(days=30)).strftime("%Y%m%d")
    end_compact = base.strftime("%Y%m%d")

    try:
        df = ak.stock_restricted_release_detail_em(
            start_date=start_compact,
            end_date=end_compact,
        )
    except Exception as e:
        logger.warning(f"⚠️ 限售解禁获取失败: {e}")
        return []
    if df is None or df.empty:
        return []

    seen: set[str] = set()
    records: list[dict] = []
    for _, row in df.iterrows():
        ts_code = str(row.get("股票代码", "")).strip()
        release_date = str(row.get("解禁时间", ""))[:10]
        key = f"{ts_code}_{release_date}"
        if key in seen:
            continue
        seen.add(key)
        records.append(
            {
                "ts_code": ts_code,
                "name": str(row.get("股票简称", "")).strip(),
                "release_date": release_date,
                "actual_release": _to_float(row.get("实际解禁数量")),
                "total_shares": _to_float(row.get("解禁数量")),
                "market_type": str(row.get("限售股类型", "")).strip(),
                "data_source": "akshare",
            }
        )
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
        warn_if_all_empty(records, ["ts_code", "total_shares"], "restricted_share")
        saved = db.save_restricted_share_batch(records)
        logger.info(f"✅ 限售解禁保存完成: {saved} 条")
        return {"saved": saved, "total": len(records)}
    except Exception as e:
        logger.error(f"❌ 限售解禁更新失败: {e}")
        return {"saved": 0, "error": str(e)}


# ===========================================================================
# 业绩预告
# ===========================================================================


def _recent_report_periods(count: int = 2) -> list[str]:
    """返回最近 count 个已结束的财报报告期（YYYYMMDD），最新在前。"""
    today = datetime.strptime(get_expected_latest_trading_day(), "%Y-%m-%d")
    quarter_ends = [(3, 31), (6, 30), (9, 30), (12, 31)]
    candidates = [
        datetime(y, m, d)
        for y in (today.year, today.year - 1)
        for m, d in quarter_ends
    ]
    past = sorted((c for c in candidates if c <= today), reverse=True)
    return [c.strftime("%Y%m%d") for c in past[:count]]


def _fetch_earnings_forecast() -> list[dict]:
    """获取全市场业绩预告数据。

    使用 ``ak.stock_yjyg_em(date=报告期)``（业绩预告，按报告期查询）。
    早期误用的 ``stock_profit_forecast_em`` 实为券商 EPS 预测，字段不匹配。
    实际列：``股票代码``/``股票简称``/``预告类型``/``业绩变动幅度``/``上年同期值``。
    """
    if ak is None:
        return []
    seen: set[str] = set()
    records: list[dict] = []
    for period in _recent_report_periods(2):
        end_date = f"{period[:4]}-{period[4:6]}-{period[6:]}"
        try:
            df = ak.stock_yjyg_em(date=period)
        except Exception as e:
            logger.warning(f"⚠️ 业绩预告 {period} 获取失败: {e}")
            continue
        if df is None or df.empty:
            continue
        for _, row in df.iterrows():
            ts_code = str(row.get("股票代码", "")).strip()
            key = f"{ts_code}_{end_date}"
            if key in seen:
                continue
            seen.add(key)
            records.append(
                {
                    "ts_code": ts_code,
                    "name": str(row.get("股票简称", "")).strip(),
                    "end_date": end_date,
                    "forecast_type": str(row.get("预告类型", "")).strip(),
                    "net_profit_change": _to_float(row.get("业绩变动幅度")),
                    "previous_profit": _to_float(row.get("上年同期值")),
                    "data_source": "akshare",
                }
            )
    return records


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
        warn_if_all_empty(records, ["forecast_type", "net_profit_change"], "earnings_forecast")
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
