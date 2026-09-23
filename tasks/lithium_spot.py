"""
碳酸锂现货价与期现基差更新任务
─────────────────────────────
数据源：生意社现货价格（ak.futures_spot_price_daily，vars_list=['LC']）。

现货价与主力合约基差（现货-期货）比单边价格更早反映供需拐点，
是锂矿持仓的核心高频信号。接口按日期区间返回，任务按库内最新
日期增量续拉（重叠窗口幂等），首次运行自动全量回填。
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from interface import DatabaseInterface

try:
    import akshare as ak
except ImportError:
    ak = None

logger = logging.getLogger(__name__)

_LITHIUM_VAR = "LC"
# 库内无历史时的全量回填起点（碳酸锂期货 2023-07 上市，现货数据更早）
_FULL_LOOKBACK_START = "2022-01-01"
# 增量重叠窗口：容忍生意社对近日的修订
_OVERLAP_DAYS = 10


def _num(val: Any) -> float | None:
    try:
        v = float(val)
        return None if v != v else v
    except (TypeError, ValueError):
        return None


def _fetch_lithium_spot(latest_date: str | None) -> list[dict]:
    """拉取碳酸锂现货+基差，按库内最新日期增量过滤。"""
    if ak is None:
        return []
    end = datetime.now().strftime("%Y%m%d")
    cutoff: str | None = None
    if latest_date is None:
        start = _FULL_LOOKBACK_START.replace("-", "")
    else:
        cutoff_dt = datetime.strptime(latest_date, "%Y-%m-%d") - timedelta(days=_OVERLAP_DAYS)
        start = cutoff_dt.strftime("%Y%m%d")
        cutoff = cutoff_dt.strftime("%Y-%m-%d")
    try:
        df = ak.futures_spot_price_daily(start_day=start, end_day=end, vars_list=[_LITHIUM_VAR])
    except Exception as e:
        logger.warning(f"⚠️ 碳酸锂现货获取失败: {e}")
        return []
    if df is None or df.empty:
        return []
    records: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        date_str = str(row.get("date", ""))[:10]
        if not date_str:
            continue
        # 接口实测返回 YYYYMMDD 纯数字格式，统一规整为 YYYY-MM-DD
        if len(date_str) == 8 and date_str.isdigit():
            date_str = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
        records.append(
            {
                "spot_date": date_str,
                "spot_price": _num(row.get("spot_price")),
                "near_contract": str(row.get("near_contract", "") or "").strip() or None,
                "near_contract_price": _num(row.get("near_contract_price")),
                "dom_contract": str(row.get("dominant_contract", "") or "").strip() or None,
                "dom_contract_price": _num(row.get("dominant_contract_price")),
                "dom_basis": _num(row.get("dom_basis")),
                "dom_basis_rate": _num(row.get("dom_basis_rate")),
                "data_source": "sunss(生意社)",
            }
        )
    # 显式增量过滤：不以接口窗口参数为唯一依赖（与 hk_tech_index 同模式）
    if cutoff is not None:
        records = [r for r in records if r["spot_date"] > cutoff]
    return records


def update_lithium_spot(db: DatabaseInterface) -> dict:
    """获取碳酸锂现货价与基差并保存。"""
    logger.info("\n" + "=" * 60)
    logger.info("🔋 任务: 更新碳酸锂现货价与基差")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "error": "akshare not installed"}

    try:
        latest = db.get_lithium_spot_latest_date()
        records = _fetch_lithium_spot(latest)
        if not records:
            logger.warning("⚠️ 碳酸锂现货无新数据")
            # fetch 内部吞异常，空 records 无法区分合法零行与全失败，保持 failed 语义
            return {"saved": 0, "total": 0}
        saved = db.save_lithium_spot_batch(records)
        logger.info(f"✅ 碳酸锂现货保存完成: {saved} 条")
        return {"saved": saved, "total": len(records)}
    except Exception as e:
        logger.error(f"❌ 碳酸锂现货更新失败: {e}")
        return {"saved": 0, "error": str(e), "error_kind": "network"}
