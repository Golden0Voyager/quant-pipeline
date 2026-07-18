"""
期货日线数据更新任务
────────────────
获取国内期货市场主力连续合约的日线 K 线数据。
数据源：新浪财经 (futures_main_sina)
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

import pandas as pd

from interface import DatabaseInterface

try:
    import akshare as ak
except ImportError:
    ak = None

logger = logging.getLogger(__name__)

# ===========================================================================
# 品种配置
# ===========================================================================

FUTURES_VARIETIES: list[dict] = [
    # ── 锂电池/新能源 ──
    {"sina_code": "LC0", "name": "碳酸锂"},
    {"sina_code": "SI0", "name": "工业硅"},
    {"sina_code": "PS0", "name": "多晶硅"},
    # ── 化工 ──
    {"sina_code": "MA0", "name": "甲醇"},
    {"sina_code": "SA0", "name": "纯碱"},
    {"sina_code": "V0", "name": "PVC"},
    {"sina_code": "PP0", "name": "聚丙烯(PP)"},
    {"sina_code": "L0", "name": "聚乙烯(LLDPE)"},
    # ── 有色金属 ──
    {"sina_code": "CU0", "name": "铜"},
    {"sina_code": "AL0", "name": "铝"},
    {"sina_code": "AO0", "name": "氧化铝"},
    {"sina_code": "SN0", "name": "锡"},
    # ── 农产品/畜牧 ──
    {"sina_code": "M0", "name": "豆粕"},
    {"sina_code": "LH0", "name": "生猪"},
]


# ===========================================================================
# 数据获取
# ===========================================================================


def _fetch_futures(variety: dict, trade_date: str) -> dict | None:
    """通过新浪财经获取单品种主力连续日线最新数据。"""
    if ak is None:
        return None
    start = (datetime.strptime(trade_date, "%Y-%m-%d") - timedelta(days=10)).strftime("%Y%m%d")
    end = trade_date.replace("-", "")
    try:
        df = ak.futures_main_sina(symbol=variety["sina_code"], start_date=start, end_date=end)
        if df is None or df.empty:
            return None
        latest = df.iloc[-1]
        close_val = latest.get("收盘价")
        # 涨跌幅：用前一日收盘价（Sina 数据无"昨收价"列）
        pre_close_val = df.iloc[-2].get("收盘价") if len(df) >= 2 else None
        change_pct = None
        if close_val is not None and pre_close_val is not None and float(pre_close_val) != 0:
            change_pct = (float(close_val) - float(pre_close_val)) / float(pre_close_val) * 100

        def _val(v):
            """pandas 标量安全取值：None/NaN → None，否则返回 Python 原生类型。"""
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return None
            return v

        return {
            "trade_date": str(latest.get("date", trade_date))[:10],
            "symbol": variety["sina_code"].replace("0", ""),
            "name": variety["name"],
            "open": _val(latest.get("开盘价")),
            "high": _val(latest.get("最高价")),
            "low": _val(latest.get("最低价")),
            "close": _val(close_val),
            "volume": int(_val(latest.get("成交量"))) if _val(latest.get("成交量")) is not None else None,
            "hold": int(_val(latest.get("持仓量"))) if _val(latest.get("持仓量")) is not None else None,
            "change_pct": round(change_pct, 4) if change_pct is not None else None,
            "data_source": "akshare_sina",
        }
    except Exception as e:
        logger.warning(f"⚠️ 期货 {variety['name']} 获取失败: {e}")
        return None


# ===========================================================================
# 主任务
# ===========================================================================


def update_futures(db: DatabaseInterface) -> dict:
    """获取全品种期货主力连续日线数据并保存。"""
    logger.info("\n" + "=" * 60)
    logger.info("📈 任务: 更新期货日线")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "error": "akshare not installed"}

    # 直接用当天日期作为查询窗口参考，不依赖 A 股交易日历。
    # 期货交易日与 A 股不完全一致（夜盘、节假日差异），
    # 且 Sina API 在非交易日自动返回最近交易日数据，
    # 存储的 trade_date 来自 API 响应的 date 字段，不受此值影响。
    trade_date = datetime.now().strftime("%Y-%m-%d")
    records: list[dict] = []

    for variety in FUTURES_VARIETIES:
        record = _fetch_futures(variety, trade_date)
        if record:
            records.append(record)
            chg = record.get('change_pct')
            chg_str = f"{chg:+.2f}%" if chg is not None else "N/A"
            logger.info(f"  ✅ {variety['name']}: {record['close']} ({chg_str})")
        else:
            logger.warning(f"  ⚠️ {variety['name']}: 无数据")

    if not records:
        logger.warning("⚠️ 期货日线全部无数据")
        return {"saved": 0, "total": 0}

    saved = db.save_futures_daily_batch(records)
    logger.info(f"✅ 期货日线保存完成: {saved}/{len(records)} 条")
    return {"saved": saved, "total": len(records)}


if __name__ == "__main__":
    from core.config import DB_PATH
    from providers import SmartMoneyDBProvider

    db = SmartMoneyDBProvider(db_path=DB_PATH)
    result = update_futures(db)
    print(result)
