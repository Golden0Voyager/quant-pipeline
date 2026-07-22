"""
沪深港通（北向）个股持仓数据更新任务
───────────────────────────────────
来源: stock_hsgt_individual_em() (东方财富)
写入现有 north_hold 表。
"""

from __future__ import annotations

import logging
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import pandas as pd

from core.config import (
    MAX_RETRY_VAL as MAX_RETRY,
)
from core.config import (
    RETRY_DELAY_VAL as RETRY_DELAY,
)
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
        v = float(val)
        return None if pd.isna(v) else v
    except (ValueError, TypeError):
        return None


def _try_get_ak_df(func, **kwargs) -> pd.DataFrame | None:
    if ak is None:
        return None
    try:
        return func(**kwargs)
    except Exception as e:
        logger.warning(f"⚠️ {func.__name__} 获取失败: {e}")
        return None


_COLUMN_MAP = {
    # akshare 不同版本列名有差异：新版返回“持股日期”，旧版为“日期”
    "持股日期": "trade_date",
    "日期": "trade_date",
    "代码": "ts_code",
    "名称": "security_name",
    "当日收盘价": "close_price",
    "收盘价": "close_price",
    "持股数量": "hold_shares",
    "持股市值": "hold_market_cap",
    "持股数量占A股百分比": "hold_shares_ratio",
    "持股占流通股比": "hold_shares_ratio",
    "持股占总股本比": "total_shares_ratio",
    "trade_date": "trade_date",
    "ts_code": "ts_code",
    "security_name": "security_name",
    "close_price": "close_price",
    "hold_shares": "hold_shares",
    "hold_market_cap": "hold_market_cap",
    "hold_shares_ratio": "hold_shares_ratio",
    "total_shares_ratio": "total_shares_ratio",
}


def _fetch_single_north_hold(symbol: str, name_map: dict[str, str] | None = None) -> dict | None:
    """获取单只股票的北向历史持仓并返回最新一条记录。"""
    if ak is None:
        return None
    for attempt in range(MAX_RETRY):
        try:
            df = ak.stock_hsgt_individual_em(symbol=symbol)
            if df is None or df.empty:
                return None
            df = df.rename(columns=_COLUMN_MAP)
            if "trade_date" not in df.columns:
                return None
            df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
            df = df[df["trade_date"].notna()]
            if df.empty:
                return None
            latest = df.iloc[-1]
            security_name = ""
            if name_map:
                security_name = name_map.get(symbol, "")
            if not security_name and "security_name" in df.columns:
                security_name = str(latest.get("security_name") or "").strip()
            return {
                "ts_code": symbol,
                "security_name": security_name,
                "trade_date": latest["trade_date"].strftime("%Y-%m-%d"),
                "close_price": _to_float(latest.get("close_price")),
                "hold_shares": _to_float(latest.get("hold_shares")),
                "hold_market_cap": _to_float(latest.get("hold_market_cap")),
                "hold_shares_ratio": _to_float(latest.get("hold_shares_ratio")),
                "total_shares_ratio": _to_float(latest.get("total_shares_ratio")),
                "data_source": "akshare",
            }
        except Exception as e:
            if attempt < MAX_RETRY - 1:
                sleep_time = RETRY_DELAY * (2 ** attempt) + random.uniform(0, 1)
                logger.warning(f"⚠️ 北向持仓 {symbol} 第 {attempt + 1} 次失败，{sleep_time:.1f}s 后重试: {e}")
                time.sleep(sleep_time)
            else:
                logger.warning(f"⚠️ 北向持仓 {symbol} 获取失败（已重试 {MAX_RETRY} 次）: {e}")
    return None


def update_hkscc_holder(db: DatabaseInterface) -> dict:
    """获取北向个股持仓数据并保存到 north_hold 表。

    2024-08-19 后交易所停止每日个股北向披露，akshare 的
    stock_hsgt_individual_em 改为按 symbol 返回单只股票历史持仓。
    本函数遍历 stock_list，取每只股票的最新一条北向持仓记录写入。
    """
    logger.info("\n" + "=" * 60)
    logger.info("🌐 任务: 更新北向资金个股持仓")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "error": "akshare not installed"}

    try:
        stock_list = db.get_stock_list()
    except Exception as e:
        logger.warning(f"⚠️ 获取股票列表失败: {e}")
        return {"saved": 0, "error": str(e)}

    if stock_list is None or stock_list.empty:
        logger.warning("⚠️ 股票列表为空，跳过北向个股持仓更新")
        return {"saved": 0}

    symbols = [str(c).strip() for c in stock_list["code"] if c]
    if not symbols:
        logger.warning("⚠️ 股票列表无有效代码")
        return {"saved": 0}

    name_map: dict[str, str] = {}
    if "name" in stock_list.columns:
        for _, row in stock_list.iterrows():
            code = str(row.get("code") or "").strip()
            name = str(row.get("name") or "").strip()
            if code and name:
                name_map[code] = name

    total = len(symbols)
    logger.info(f"📋 准备更新 {total} 只股票的北向持仓")

    records: list[dict] = []
    failed: list[str] = []
    workers = min(5, max(2, (len(symbols) // 200) + 1))

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_fetch_single_north_hold, s, name_map): s for s in symbols}
        for i, future in enumerate(as_completed(futures), 1):
            symbol = futures[future]
            try:
                record = future.result()
                if record:
                    records.append(record)
                else:
                    failed.append(symbol)
            except Exception as e:
                logger.warning(f"⚠️ 北向持仓 {symbol} 异常: {e}")
                failed.append(symbol)

            if i % 100 == 0 or i == total:
                logger.info(f"  进度: {i}/{total} ({100 * i // total}%), 成功 {len(records)}，失败 {len(failed)}")

    if not records:
        logger.warning(
            "⚠️ 北向个股持仓记录为空。自 2024-08-19 起交易所不再披露每日个股北向数据，"
            "当前 akshare 接口也已变更，需指定 symbol 获取单只股票历史。"
        )
        return {"saved": 0, "total": total, "failed": len(failed)}

    saved = db.save_north_hold_batch(records)
    logger.info(f"✅ 北向个股持仓数据保存完成: {saved}/{total} 条，失败 {len(failed)}")
    return {"saved": saved, "total": total, "failed": len(failed)}
