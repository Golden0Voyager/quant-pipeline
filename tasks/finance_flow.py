"""
资金流向与行情数据任务模块
─────────────────────────
南向资金、AH 股溢价、ETF 日线行情。
"""

from __future__ import annotations

import logging

import pandas as pd

from core.calendar import get_expected_latest_trading_day
from core.utils import warn_if_all_empty
from interface import DatabaseInterface

try:
    import akshare as ak
except ImportError:
    ak = None

logger = logging.getLogger(__name__)


# ===========================================================================
# 辅助函数
# ===========================================================================


def _to_float(val) -> float | None:
    if val is None:
        return None
    try:
        v = float(val)
        return None if pd.isna(v) else v
    except (ValueError, TypeError):
        return None


# ===========================================================================
# 南向资金（沪深港通 → 港股通）
# ===========================================================================


def _fetch_south_flow() -> list[dict]:
    if ak is None:
        return []
    try:
        df = ak.stock_hsgt_hist_em(symbol="南向资金")
        if df is None or df.empty:
            return []
        col_map = {
            "日期": "trade_date",
            "板块": "market",
            "当日成交净买额": "net_buy_amount",
            "买入成交额": "buy_amount",
            "卖出成交额": "sell_amount",
            "历史累计净买额": "cumulative_net_buy",
        }
        rename = {k: v for k, v in col_map.items() if k in df.columns}
        df = df.rename(columns=rename)
        records: list[dict] = []
        for _, row in df.iterrows():
            records.append(
                {
                    "trade_date": str(row.get("trade_date", ""))[:10],
                    "market": str(row.get("market", "") or "南向").strip(),
                    "net_buy_amount": _to_float(row.get("net_buy_amount")),
                    "buy_amount": _to_float(row.get("buy_amount")),
                    "sell_amount": _to_float(row.get("sell_amount")),
                    "cumulative_net_buy": _to_float(row.get("cumulative_net_buy")),
                    "data_source": "akshare",
                }
            )
        return records
    except Exception as e:
        logger.warning(f"⚠️ 南向资金获取失败: {e}")
        return []


def update_south_flow(db: DatabaseInterface) -> dict:
    logger.info("\n" + "=" * 60)
    logger.info("🏛️ 任务: 更新南向资金流向")
    logger.info("=" * 60)
    if ak is None:
        return {"saved": 0, "error": "akshare not installed"}
    try:
        records = _fetch_south_flow()
        if not records:
            logger.warning("⚠️ 南向资金无数据")
            return {"saved": 0, "total": 0}
        warn_if_all_empty(records, ["net_buy_amount", "buy_amount"], "south_flow")
        saved = db.save_south_flow_batch(records)
        logger.info(f"✅ 南向资金保存完成: {saved} 条")
        return {"saved": saved, "total": len(records)}
    except Exception as e:
        logger.error(f"❌ 南向资金更新失败: {e}")
        return {"saved": 0, "error": str(e)}


# ===========================================================================
# A/H 股溢价
# ===========================================================================


def _fetch_ah_premium() -> list[dict]:
    if ak is None:
        return []
    try:
        df = ak.stock_zh_ah_spot_em()
        if df is None or df.empty:
            return []
        trade_date = get_expected_latest_trading_day()
        col_map = {
            "A股代码": "ts_code",
            "H股代码": "h_code",
            "代码": "ts_code",
            "名称": "name",
            "最新价-HKD": "h_price",
            "最新价(HKD)": "h_price",
            "最新价-RMB": "a_price",
            "最新价": "a_price",
            "溢价": "premium",
            "溢价率": "premium",
        }
        rename = {k: v for k, v in col_map.items() if k in df.columns}
        df = df.rename(columns=rename)
        records: list[dict] = []
        for _, row in df.iterrows():
            records.append(
                {
                    "trade_date": trade_date,
                    "ts_code": str(row.get("ts_code", "")).strip(),
                    "h_code": str(row.get("h_code", "")).strip(),
                    "name": str(row.get("name", "")).strip(),
                    "h_price": _to_float(row.get("h_price")),
                    "a_price": _to_float(row.get("a_price")),
                    "premium": _to_float(row.get("premium")),
                    "data_source": "akshare",
                }
            )
        return records
    except Exception as e:
        logger.warning(f"⚠️ A/H 溢价获取失败: {e}")
        return []


def update_ah_premium(db: DatabaseInterface) -> dict:
    logger.info("\n" + "=" * 60)
    logger.info("🔗 任务: 更新 A/H 股溢价")
    logger.info("=" * 60)
    if ak is None:
        return {"saved": 0, "error": "akshare not installed"}
    try:
        records = _fetch_ah_premium()
        if not records:
            logger.warning("⚠️ A/H 溢价无数据")
            return {"saved": 0, "total": 0}
        saved = db.save_ah_premium_batch(records)
        logger.info(f"✅ A/H 溢价保存完成: {saved} 条")
        return {"saved": saved, "total": len(records)}
    except Exception as e:
        logger.error(f"❌ A/H 溢价更新失败: {e}")
        return {"saved": 0, "error": str(e)}


# ===========================================================================
# ETF 日线行情
# ===========================================================================

_ETF_CODES: list[tuple[str, str]] = [
    ("510050", "上证50ETF"),
    ("510300", "沪深300ETF"),
    ("510500", "中证500ETF"),
    ("510880", "红利ETF"),
    ("588000", "科创50ETF"),
    ("159915", "创业板ETF"),
    ("159845", "中证1000ETF"),
    ("513100", "纳指ETF"),
    ("513050", "中概互联ETF"),
    ("518880", "黄金ETF"),
    ("511880", "银华日利"),
    ("159949", "创业板50ETF"),
    ("512100", "中证1000ETF"),
    ("515050", "5GETF"),
    ("515790", "光伏ETF"),
    ("512010", "医药ETF"),
    ("159928", "消费ETF"),
    ("515700", "新能车ETF"),
    ("512880", "证券ETF"),
    ("510330", "沪深300ETF华夏"),
]


def _fetch_etf_daily(start_date: str, end_date: str) -> list[dict]:
    if ak is None:
        return []
    records: list[dict] = []
    for code, name in _ETF_CODES:
        try:
            df = ak.fund_etf_hist_em(
                symbol=code,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust="qfq",
            )
            if df is None or df.empty:
                logger.warning(f"⚠️ ETF {name}({code}) 无数据")
                continue
            col_map = {
                "日期": "trade_date",
                "开盘": "open",
                "最高": "high",
                "最低": "low",
                "收盘": "close",
                "成交量": "volume",
                "成交额": "amount",
            }
            rename = {k: v for k, v in col_map.items() if k in df.columns}
            df = df.rename(columns=rename)
            for _, row in df.iterrows():
                records.append(
                    {
                        "trade_date": str(row.get("trade_date", ""))[:10],
                        "ts_code": code,
                        "name": name,
                        "open": _to_float(row.get("open")),
                        "high": _to_float(row.get("high")),
                        "low": _to_float(row.get("low")),
                        "close": _to_float(row.get("close")),
                        "volume": _to_float(row.get("volume")),
                        "amount": _to_float(row.get("amount")),
                        "data_source": "akshare",
                    }
                )
            logger.info(f"  ✅ {name}({code}): {len(df)} 条")
        except Exception as e:
            logger.warning(f"⚠️ ETF {name}({code}) 获取失败: {e}")
    return records


def update_etf_daily(db: DatabaseInterface) -> dict:
    logger.info("\n" + "=" * 60)
    logger.info("📊 任务: 更新 ETF 日线行情")
    logger.info("=" * 60)
    if ak is None:
        return {"saved": 0, "error": "akshare not installed"}
    try:
        today = get_expected_latest_trading_day()
        end_date = today.replace("-", "")
        records = _fetch_etf_daily("20100101", end_date)
        if not records:
            logger.warning("⚠️ ETF 日线无数据")
            return {"saved": 0, "total": 0}
        warn_if_all_empty(records, ["close", "volume"], "etf_daily")
        saved = db.save_etf_daily_batch(records)
        logger.info(f"✅ ETF 日线保存完成: {saved} 条")
        return {"saved": saved, "total": len(records)}
    except Exception as e:
        logger.error(f"❌ ETF 日线更新失败: {e}")
        return {"saved": 0, "error": str(e)}


# ===========================================================================
# 统一入口
# ===========================================================================


def update_finance_flow(db: DatabaseInterface) -> dict:
    """获取资金流向 / AH 溢价 / ETF 日线数据。"""
    logger.info("\n" + "=" * 60)
    logger.info("🏛️ 任务: 更新资金流向与行情数据")
    logger.info("=" * 60)
    if ak is None:
        return {"saved": 0, "error": "akshare not installed"}

    results: dict[str, object] = {}

    sub_tasks = [
        ("south_flow", _fetch_south_flow, db.save_south_flow_batch, None),
        ("ah_premium", _fetch_ah_premium, db.save_ah_premium_batch, None),
        ("etf_daily", _fetch_etf_daily, db.save_etf_daily_batch, ("20100101", get_expected_latest_trading_day().replace("-", ""))),
    ]

    for name, fetch_fn, save_fn, extra_args in sub_tasks:
        try:
            records = fetch_fn(*extra_args) if extra_args else fetch_fn()
            saved = save_fn(records) if records else 0
            results[name] = {"saved": saved, "total": len(records)}
            logger.info(f"  ✅ {name}: {saved}/{len(records)} 条")
        except Exception as e:
            logger.warning(f"⚠️ {name} 更新失败: {e}")
            results[name] = {"saved": 0, "error": str(e)}

    total_saved = sum(
        (r.get("saved", 0) if isinstance(r, dict) else 0) for r in results.values()
    )
    logger.info(f"\n🏁 资金流向与行情数据更新完成，共保存 {total_saved} 条")
    return {"saved": total_saved, "details": results}
