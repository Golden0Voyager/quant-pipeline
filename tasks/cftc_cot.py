"""
CFTC 持仓报告（COT）更新任务
─────────────────────────
数据源：akshare 的 CFTC 周度持仓封装：
- macro_usa_cftc_c_holding：商品期货非商业（投机）持仓
  （纽约原油、黄金、白银、天然气、大豆、豆粕、玉米、棉花、原糖等）
- macro_usa_cftc_nc_holding：外汇期货非商业持仓
  （美元、欧元、日元、英镑、澳元、加元、瑞郎、纽元、墨西哥比索）

接口返回宽表（每个品种三列：多头/空头/净仓位），此处转长表入库：
每行 = (周, 市场, 品种)。CFTC 每周五发布（滞后数日），故按库内最新
日期增量续拉（重叠窗口容忍发布方对近周的修订），多数自然日无新周
数据，空结果返回 skipped 而非 failed。
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

# market key -> (akshare 接口名, 中文名)
_MARKETS: dict[str, tuple[str, str]] = {
    "goods": ("macro_usa_cftc_c_holding", "商品非商业"),
    "fx": ("macro_usa_cftc_nc_holding", "外汇非商业"),
}
# 重叠窗口：CFTC 会对近若干周数据修订，回补时覆盖近 12 周
_OVERLAP_WEEKS = 12


def _melt_market(df: Any, market: str) -> list[dict]:
    """宽表转长表：'{品种}-多头仓位' 三列组 → 每品种一条记录。"""
    if df is None or df.empty:
        return []
    instruments: dict[str, dict[str, Any]] = {}
    suffix_map = (
        ("-多头仓位", "long_positions"),
        ("-空头仓位", "short_positions"),
        ("-净仓位", "net_positions"),
    )
    for col in df.columns:
        if col == "日期":
            continue
        for suffix, field in suffix_map:
            if col.endswith(suffix):
                instrument = col[: -len(suffix)]
                instruments.setdefault(instrument, {})[field] = col
                break

    records = []
    for _, row in df.iterrows():
        date_str = str(row.get("日期", ""))[:10]
        if not date_str:
            continue
        for instrument, cols in instruments.items():
            records.append(
                {
                    "trade_date": date_str,
                    "market": market,
                    "instrument": instrument,
                    "long_positions": _num(row.get(cols["long_positions"])),
                    "short_positions": _num(row.get(cols["short_positions"])),
                    "net_positions": _num(row.get(cols["net_positions"])),
                    "data_source": "cftc",
                }
            )
    return records


def _num(val: Any) -> float | None:
    try:
        v = float(val)
        return None if v != v else v  # NaN 检查
    except (TypeError, ValueError):
        return None


def _fetch_market_cot(market: str, latest_date: str | None) -> list[dict]:
    """拉取单个市场的 COT 全量历史并按库内最新日期增量过滤。"""
    if ak is None:
        return []
    func_name, market_cn = _MARKETS[market]
    try:
        df = getattr(ak, func_name)()
    except Exception as e:
        logger.warning(f"⚠️ CFTC {market_cn}持仓获取失败: {e}")
        return []
    records = _melt_market(df, market)
    if latest_date is None:
        return records
    cutoff = (
        datetime.strptime(latest_date, "%Y-%m-%d") - timedelta(weeks=_OVERLAP_WEEKS)
    ).strftime("%Y-%m-%d")
    return [r for r in records if r["trade_date"] > cutoff]


def update_cftc_cot(db: DatabaseInterface) -> dict:
    """获取 CFTC 周度持仓（商品+外汇非商业）并保存。"""
    logger.info("\n" + "=" * 60)
    logger.info("📊 任务: 更新 CFTC 持仓报告 (COT)")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "error": "akshare not installed"}

    try:
        total_new = 0
        total_saved = 0
        for market in _MARKETS:
            latest = db.get_cftc_cot_latest_date(market)
            records = _fetch_market_cot(market, latest)
            total_new += len(records)
            if records:
                total_saved += db.save_cftc_cot_batch(records)
        if total_new == 0:
            # CFTC 周更：多数自然日无新报告属预期，显式 skipped
            logger.info("ℹ️ CFTC 本周暂无新持仓报告")
            return {"skipped": True, "reason": "no new weekly COT report"}
        logger.info(f"✅ CFTC 持仓保存完成: {total_saved} 条")
        return {"saved": total_saved, "total": total_new}
    except Exception as e:
        logger.error(f"❌ CFTC 持仓更新失败: {e}")
        return {"saved": 0, "error": str(e)}
