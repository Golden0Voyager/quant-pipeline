"""
行业/板块/基差数据更新任务
─────────────────────────
行业板块涨跌幅、板块估值(PE/PB)、股指期货基差。
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from typing import Any

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
# 常量
# ===========================================================================

MAJOR_SECTORS: list[str] = [
    "半导体",
    "银行",
    "证券",
    "汽车整车",
    "白酒",
    "医疗器械",
    "光伏设备",
    "锂电池",
    "人工智能",
    "软件开发",
    "通信设备",
    "消费电子",
    "家用电器",
    "食品饮料",
    "房地产",
    "医药生物",
    "化工",
    "有色金属",
    "煤炭",
    "国防军工",
]

FUTURES_CONTRACTS: dict[str, tuple[str, str]] = {
    "IF0": ("sh000300", "沪深300"),
    "IC0": ("sh000905", "中证500"),
    "IH0": ("sh000016", "上证50"),
}


# ===========================================================================
# 辅助函数
# ===========================================================================


def _to_float(val: Any) -> float | None:
    """将值转换为 float，失败返回 None。"""
    if val is None:
        return None
    try:
        v = float(val)
        return None if pd.isna(v) else v
    except (ValueError, TypeError):
        return None


def _retry(fn, *, tries: int = 3, base_delay: float = 1.0, label: str = ""):
    """对易受网络波动影响的 AkShare 调用做指数退避重试，全部失败返回 None。"""
    last_exc: Exception | None = None
    for i in range(tries):
        try:
            return fn()
        except Exception as e:  # noqa: BLE001 — 网络类异常统一重试
            last_exc = e
            if i < tries - 1:
                time.sleep(base_delay * (2**i))
    logger.warning(f"⚠️ {label} 重试 {tries} 次仍失败: {last_exc}")
    return None


# ===========================================================================
# 1. 行业板块涨跌幅
# ===========================================================================


def _fetch_sector_daily() -> list[dict]:
    """获取主要行业板块日线涨跌幅数据。"""
    if ak is None:
        return []
    try:
        board_df = _retry(
            ak.stock_board_industry_name_em, label="行业板块列表"
        )
        if board_df is None or board_df.empty:
            return []
        sector_names = board_df["板块名称"].tolist() if "板块名称" in board_df.columns else []
        # 过滤出主要行业板块
        targets = [s for s in sector_names if s in MAJOR_SECTORS]
        if not targets:
            logger.warning("⚠️ 未找到匹配的行业板块")
            return []
        logger.info(f"📋 行业板块: {len(targets)} 个")

        records: list[dict] = []
        start_date = (datetime.now() - timedelta(days=730)).strftime("%Y%m%d")
        end_date = datetime.now().strftime("%Y%m%d")
        for sector in targets:
            try:
                df = _retry(
                    lambda s=sector, sd=start_date, ed=end_date: ak.stock_board_industry_hist_em(
                        symbol=s,
                        start_date=sd,
                        end_date=ed,
                    ),
                    label=f"行业板块 {sector} 历史",
                )
                if df is None or df.empty:
                    continue
                col_map = {
                    "日期": "trade_date",
                    "开盘": "open",
                    "收盘": "close",
                    "最高": "high",
                    "最低": "low",
                    "成交量": "volume",
                    "成交额": "amount",
                    "涨跌幅": "pct_change",
                }
                df = df.rename(columns=col_map)
                keep = {"trade_date", "open", "close", "high", "low", "volume", "amount", "pct_change"}
                available = [c for c in keep if c in df.columns]
                if not available:
                    continue
                for _, row in df.iterrows():
                    records.append(
                        {
                            "sector_name": sector,
                            "trade_date": str(row.get("trade_date", ""))[:10],
                            "open": _to_float(row.get("open")),
                            "close": _to_float(row.get("close")),
                            "high": _to_float(row.get("high")),
                            "low": _to_float(row.get("low")),
                            "volume": _to_float(row.get("volume")),
                            "amount": _to_float(row.get("amount")),
                            "pct_change": _to_float(row.get("pct_change")),
                            "data_source": "akshare",
                        }
                    )
            except Exception as e:
                logger.warning(f"⚠️ 行业板块 {sector} 历史数据获取失败: {e}")
        return records
    except Exception as e:
        logger.warning(f"⚠️ 行业板块列表获取失败: {e}")
        return []


# ===========================================================================
# 2. 板块估值
# ===========================================================================


def _fetch_sector_valuation() -> list[dict]:
    """获取行业板块估值数据（PE/PB）。"""
    if ak is None:
        return []
    try:
        # cninfo 估值接口需传有效交易日；用今天常返回空并触发内部 'records' 报错
        date_compact = get_expected_latest_trading_day().replace("-", "")
        df = _retry(
            lambda: ak.stock_industry_pe_ratio_cninfo(
                symbol="证监会行业分类", date=date_compact
            ),
            label="板块估值",
        )
        if df is None or df.empty:
            return []
        col_map = {
            "行业": "sector_name",
            "行业名称": "sector_name",
            "日期": "trade_date",
            "变动日期": "trade_date",
            "统计日期": "trade_date",
            "平均市盈率": "pe",
            "静态市盈率-加权平均": "pe",
            "静态市盈率": "pe",
            "市盈率": "pe",
            "PE": "pe",
            "平均市净率": "pb",
            "市净率": "pb",
            "PB": "pb",
            "总市值": "total_mv",
            "总市值(元)": "total_mv",
            "总市值-静态": "total_mv",
            "区间市值": "total_mv",
        }
        df = df.rename(columns=col_map)
        keep = {"sector_name", "trade_date", "pe", "pb", "total_mv"}
        available = [c for c in keep if c in df.columns]
        if not available:
            logger.warning("⚠️ 板块估值列名不匹配，可用列: %s", list(df.columns))
            return []
        df = df[available]
        records: list[dict] = []
        for _, row in df.iterrows():
            records.append(
                {
                    "sector_name": str(row.get("sector_name", "")).strip(),
                    "trade_date": str(row.get("trade_date", ""))[:10],
                    "pe": _to_float(row.get("pe")),
                    "pb": _to_float(row.get("pb")),
                    "total_mv": _to_float(row.get("total_mv")),
                    "data_source": "akshare",
                }
            )
        return records
    except Exception as e:
        logger.warning(f"⚠️ 板块估值获取失败: {e}")
        return []


# ===========================================================================
# 3. 股指期货基差
# ===========================================================================


def _fetch_index_futures_basis() -> list[dict]:
    """获取股指期货基差数据（IF0/IC0/IH0 vs 对应现货指数）。"""
    if ak is None:
        return []
    records: list[dict] = []
    for futures_code, (index_code, index_name) in FUTURES_CONTRACTS.items():
        # 期货日线（新浪接口易超时，做退避重试）
        futures_df = _retry(
            lambda fc=futures_code: ak.futures_zh_daily_sina(symbol=fc),
            label=f"{futures_code}({index_name}) 期货",
        )
        if futures_df is None or futures_df.empty:
            continue

        # 腾讯指数接口偶发 'qfqday' 内部错误，做退避重试
        index_df = _retry(
            lambda ic=index_code: ak.stock_zh_index_daily_tx(symbol=ic),
            label=f"{index_name}({index_code}) 指数",
        )
        if index_df is None or index_df.empty:
            continue

        # 重命名期货列
        futures_col_map = {
            "日期": "date",
            "开盘价": "open",
            "最高价": "high",
            "最低价": "low",
            "收盘价": "close",
            "成交量": "volume",
            "持仓量": "open_interest",
        }
        futures_df = futures_df.rename(columns=futures_col_map)
        if "date" not in futures_df.columns or "close" not in futures_df.columns:
            continue

        # 构建指数日期 -> 收盘价 字典
        index_col_map = {
            "date": "date",
            "close": "index_close",
        }
        index_df = index_df.rename(columns=index_col_map)
        if "date" not in index_df.columns or "index_close" not in index_df.columns:
            continue
        index_close_map: dict[str, float] = {}
        for _, row in index_df.iterrows():
            d = str(row.get("date", ""))[:10]
            c = _to_float(row.get("index_close"))
            if d and c is not None:
                index_close_map[d] = c

        # 逐日计算基差
        for _, row in futures_df.iterrows():
            d = str(row.get("date", ""))[:10]
            futures_price = _to_float(row.get("close"))
            if not d or futures_price is None:
                continue
            index_price = index_close_map.get(d)
            if index_price is None:
                continue
            basis = futures_price - index_price
            basis_pct = round((futures_price / index_price - 1) * 100, 4) if index_price else None
            records.append(
                {
                    "trade_date": d,
                    "futures_code": futures_code,
                    "futures_price": futures_price,
                    "index_price": index_price,
                    "basis": round(basis, 4),
                    "basis_pct": basis_pct,
                    "data_source": "akshare",
                }
            )
    return records


# ===========================================================================
# 主更新函数
# ===========================================================================


def update_sector_derivatives(db: DatabaseInterface) -> dict:
    """获取行业涨跌幅、板块估值、股指期货基差数据并保存。"""
    logger.info("\n" + "=" * 60)
    logger.info("📊 任务: 更新行业/板块/基差数据")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"error": "akshare not installed"}

    results: dict[str, Any] = {}

    # 1. 行业板块涨跌幅
    try:
        records = _fetch_sector_daily()
        if records:
            warn_if_all_empty(records, ["close", "pct_change"], "sector_daily")
            saved = db.save_sector_daily_batch(records)
            results["sector_daily"] = saved
            logger.info(f"✅ 行业涨跌幅保存完成: {saved} 条")
        else:
            results["sector_daily"] = 0
            logger.warning("⚠️ 行业涨跌幅无数据")
    except Exception as e:
        results["sector_daily"] = f"error: {e}"
        logger.warning(f"⚠️ 行业涨跌幅失败: {e}")

    # 2. 板块估值
    try:
        records = _fetch_sector_valuation()
        if records:
            warn_if_all_empty(records, ["pe", "total_mv"], "sector_valuation")
            saved = db.save_sector_valuation_batch(records)
            results["sector_valuation"] = saved
            logger.info(f"✅ 板块估值保存完成: {saved} 条")
        else:
            results["sector_valuation"] = 0
            logger.warning("⚠️ 板块估值无数据")
    except Exception as e:
        results["sector_valuation"] = f"error: {e}"
        logger.warning(f"⚠️ 板块估值失败: {e}")

    # 3. 股指期货基差
    try:
        records = _fetch_index_futures_basis()
        if records:
            warn_if_all_empty(records, ["basis", "futures_price"], "index_futures_basis")
            saved = db.save_index_futures_basis_batch(records)
            results["index_futures_basis"] = saved
            logger.info(f"✅ 基差数据保存完成: {saved} 条")
        else:
            results["index_futures_basis"] = 0
            logger.warning("⚠️ 基差数据无数据")
    except Exception as e:
        results["index_futures_basis"] = f"error: {e}"
        logger.warning(f"⚠️ 基差数据失败: {e}")

    return dict(results)
