"""
恒生科技指数日线更新任务
─────────────────────
数据源：新浪财经港股指数日线（ak.stock_hk_index_daily_sina）。

Yahoo 已下线 ^HSTECH（404），东财/新浪 A 股全球指数源对其覆盖率
不可靠，故单独走新浪港股指数接口。接口返回全量历史，任务按库内
最新日期增量续拉（INSERT OR REPLACE 幂等），首次运行自动全量回填。
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from interface import DatabaseInterface

try:
    import akshare as ak
except ImportError:
    ak = None

logger = logging.getLogger(__name__)

_HSTECH_SYMBOL = "HSTECH"
_INDEX_NAME = "恒生科技指数"
# 增量重叠窗口：覆盖节假日缺跑后的回补，同时容忍接口末日反复修订
_OVERLAP_DAYS = 7


def _fetch_hk_tech_records(latest_date: str | None) -> list[dict]:
    """拉取恒生科技指数日线，只保留库内最新日期之后的增量部分。"""
    if ak is None:
        return []
    try:
        df = ak.stock_hk_index_daily_sina(symbol=_HSTECH_SYMBOL)
    except Exception as e:
        logger.warning(f"⚠️ 恒生科技指数获取失败: {e}")
        return []
    if df is None or df.empty:
        return []

    records = []
    prev_close = None
    for _, row in df.iterrows():
        date_str = str(row.get("date", ""))[:10]
        if not date_str:
            continue
        close = float(row["close"])
        change_pct = (
            round((close - prev_close) / prev_close * 100, 4)
            if prev_close
            else None
        )
        prev_close = close
        records.append(
            {
                "trade_date": date_str,
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": close,
                "change_pct": change_pct,
                "volume": float(row["volume"]) if row.get("volume") is not None else None,
                "amount": float(row["amount"]) if row.get("amount") is not None else None,
                "data_source": "akshare_sina_hk",
            }
        )

    if latest_date is None:
        return records
    # 重叠窗口内重复拉取靠 UNIQUE(trade_date) + INSERT OR REPLACE 去重
    cutoff = (
        datetime.strptime(latest_date, "%Y-%m-%d") - timedelta(days=_OVERLAP_DAYS)
    ).strftime("%Y-%m-%d")
    return [r for r in records if r["trade_date"] > cutoff]


def update_hk_tech_index(db: DatabaseInterface) -> dict:
    """获取恒生科技指数日线并保存。"""
    logger.info("\n" + "=" * 60)
    logger.info("📈 任务: 更新恒生科技指数 (HSTECH)")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "error": "akshare not installed"}

    try:
        latest = db.get_hk_tech_latest_date()
        records = _fetch_hk_tech_records(latest)
        if not records:
            logger.warning("⚠️ 恒生科技指数无新数据")
            # fetch 内部吞异常，空 records 无法区分合法零行与全失败，保持 failed 语义
            return {"saved": 0, "total": 0}
        saved = db.save_hk_tech_index_batch(records)
        logger.info(f"✅ 恒生科技指数保存完成: {saved} 条")
        return {"saved": saved, "total": len(records)}
    except Exception as e:
        logger.error(f"❌ 恒生科技指数更新失败: {e}")
        return {"saved": 0, "error": str(e)}
