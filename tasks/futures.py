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
    {"sina_code": "AG0", "name": "白银"},
    # ── 农产品/畜牧 ──
    {"sina_code": "M0", "name": "豆粕"},
    {"sina_code": "LH0", "name": "生猪"},
]

# 库内无历史时的全量回填起点（Sina 主力连续通常自品种上市起可用）
_FULL_LOOKBACK_START = "20190101"


# ===========================================================================
# 数据获取
# ===========================================================================


def _val(v):
    """pandas 标量安全取值：None/NaN → None，否则返回 Python 原生类型。"""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    return v


def _fetch_futures_history(variety: dict, start: str, end: str) -> list[dict]:
    """通过新浪财经获取单品种主力连续日线（窗口内全量行）。"""
    if ak is None:
        return []
    try:
        df = ak.futures_main_sina(symbol=variety["sina_code"], start_date=start, end_date=end)
    except Exception as e:
        logger.warning(f"⚠️ 期货 {variety['name']} 获取失败: {e}")
        return []
    if df is None or df.empty:
        return []
    # 涨跌幅：Sina 数据无"昨收价"列，用窗口内前一行收盘计算
    closes = df["收盘价"].astype(float).tolist()
    records = []
    for i, (_, row) in enumerate(df.iterrows()):
        # Sina 期货接口实测返回中文列名“日期”（旧代码误取 "date" 落入回退值，
        # 曾导致整表 trade_date 记成运行日——migration 021 重建修复）
        raw_date = row.get("日期") if "日期" in df.columns else row.get("date")
        if raw_date is None or (isinstance(raw_date, float) and pd.isna(raw_date)):
            continue
        date_str = str(raw_date)[:10]
        if not date_str:
            continue
        pre_close = closes[i - 1] if i >= 1 else None
        change_pct = (
            round((closes[i] - pre_close) / pre_close * 100, 4)
            if pre_close
            else None
        )
        volume = _val(row.get("成交量"))
        hold = _val(row.get("持仓量"))
        records.append(
            {
                "trade_date": date_str,
                "symbol": variety["sina_code"].replace("0", ""),
                "name": variety["name"],
                "open": _val(row.get("开盘价")),
                "high": _val(row.get("最高价")),
                "low": _val(row.get("最低价")),
                "close": _val(row.get("收盘价")),
                "volume": int(volume) if volume is not None else None,
                "hold": int(hold) if hold is not None else None,
                "change_pct": change_pct,
                "data_source": "akshare_sina",
            }
        )
    return records


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
    end = trade_date.replace("-", "")
    records: list[dict] = []

    for variety in FUTURES_VARIETIES:
        # 增量续拉：库内最新日前回退 10 天（UNIQUE 幂等去重）；无历史则全量回填
        symbol = variety["sina_code"].replace("0", "")
        try:
            latest = db.get_futures_latest_date(symbol)
            if not isinstance(latest, str):
                latest = None
        except Exception:
            latest = None
        if latest:
            start = (
                datetime.strptime(latest, "%Y-%m-%d") - timedelta(days=10)
            ).strftime("%Y%m%d")
        else:
            start = _FULL_LOOKBACK_START
        rows = _fetch_futures_history(variety, start, end)
        records.extend(rows)
        if rows:
            latest_row = rows[-1]
            chg = latest_row.get("change_pct")
            chg_str = f"{chg:+.2f}%" if chg is not None else "N/A"
            logger.info(f"  ✅ {variety['name']}: {latest_row['close']} ({chg_str}) +{len(rows)} 行")
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
