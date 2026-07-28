from __future__ import annotations

import hashlib  # noqa: F401
import json  # noqa: F401
import logging
import os
import sqlite3
import time  # noqa: F401
from collections import defaultdict  # noqa: F401
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime  # noqa: F401
from typing import Any

import numpy as np
import pandas as pd

from core.config import SHARED_DATA_DIR  # noqa: F401
from core.utils import (
    infer_market,
    is_real_db_path,
    should_skip_beijing,  # noqa: F401
)
from interface import (
    DatabaseInterface,
    DataLoaderInterface,  # noqa: F401
    IndicatorEngineInterface,
)

try:
    import akshare as ak
except ImportError:
    ak = None

logger = logging.getLogger(__name__)

CHIP_BINS = int(os.getenv("CHIP_BINS", "100"))
MIN_CHIP_DAYS = int(os.getenv("MIN_CHIP_DAYS", "60"))
CHIP_MAX_TURNOVER = 0.999


def update_stock_list(db: DatabaseInterface) -> dict:
    """从 AkShare 拉取全量 A 股列表并写入 stock_list 表。"""
    logger.info("\n" + "=" * 60)
    logger.info("📋 任务: 更新全市场股票列表")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "error": "akshare not installed"}

    try:
        df_raw = ak.stock_info_a_code_name()
        if df_raw is None or df_raw.empty:
            logger.warning("⚠️  未获取到股票列表数据")
            return {"saved": 0, "error": "empty response"}

        df = df_raw[["code", "name"]].copy()
        df["code"] = df["code"].astype(str).str.strip()
        df["name"] = df["name"].astype(str).str.strip()
        df["market"] = df["code"].apply(infer_market)

        # 从旧表合并已有 industry，避免 update_industry 全量重爬
        # 仅在 db_path 为真实文件路径时尝试合并；测试中的 mock db 直接跳过，
        # 避免把 MagicMock 的 repr 当成 SQLite 文件名在仓库根目录生成垃圾文件
        if is_real_db_path(getattr(db, "db_path", None)):
            try:
                old_conn = sqlite3.connect(str(db.db_path))
                old_df = pd.read_sql_query(
                    "SELECT code, industry FROM stock_list WHERE industry IS NOT NULL AND industry != '未分类'",
                    old_conn,
                )
                old_conn.close()
                if not old_df.empty:
                    old_df["code"] = old_df["code"].astype(str).str.strip()
                    old_industry = old_df.set_index("code")["industry"]
                    df["industry"] = df["code"].map(old_industry)
                    df["industry"] = df["industry"].where(df["industry"].notna(), None)
                else:
                    df["industry"] = None
            except Exception:
                df["industry"] = None  # 合并失败时退化为全量重爬
        else:
            df["industry"] = None

        db.save_stock_list(df)
        saved = len(df)
        sh_count = (df["market"] == "sh").sum()
        sz_count = (df["market"] == "sz").sum()
        bj_count = (df["market"] == "bj").sum()
        logger.info(
            f"✅ 股票列表更新完成: {saved} 只 "
            f"(沪市 {sh_count} / 深市 {sz_count} / 北交所 {bj_count})"
        )
        return {"saved": saved, "sh": int(sh_count), "sz": int(sz_count), "bj": int(bj_count)}
    except Exception as e:
        logger.error(f"❌ 股票列表更新失败: {e}")
        return {"saved": 0, "error": str(e)}


def update_indicators(
    db: DatabaseInterface, engine: IndicatorEngineInterface, symbols_to_update: list[str] | None = None
) -> dict:
    """为指定或所有需要更新的技术指标重新计算。"""
    logger.info("\n" + "=" * 60)
    logger.info("📊 任务: 计算技术指标")
    logger.info("=" * 60)

    conn = sqlite3.connect(str(db.db_path))
    cursor = conn.cursor()

    if symbols_to_update is not None:
        symbols = symbols_to_update
        logger.info(f"🎯 指定模式：计算 {len(symbols)} 只股票的指标")
    else:
        # 智能探测模式：只计算未计算过，或者有新行情数据的股票
        logger.info("🔍 智能探测需要更新指标的股票...")
        cursor.execute("""
            SELECT d.ts_code
            FROM (
                SELECT ts_code, MAX(trade_date) as max_bar_date
                FROM daily_bars
                GROUP BY ts_code
                HAVING COUNT(*) >= 60
            ) d
            LEFT JOIN (
                SELECT ts_code, MAX(trade_date) as max_ind_date
                FROM indicators
                GROUP BY ts_code
            ) i ON d.ts_code = i.ts_code
            WHERE i.max_ind_date IS NULL OR d.max_bar_date > i.max_ind_date
            ORDER BY d.ts_code
        """)
        symbols = [row[0] for row in cursor.fetchall()]
        logger.info(f"💡 探测完成：共有 {len(symbols)} 只股票需要更新/计算指标")

    conn.close()

    total = len(symbols)
    if total == 0:
        logger.info("✅ 所有股票的指标均已是最新，无需计算")
        return {
            "status": "success",
            "saved": 0,
            "success": 0,
            "failed": 0,
            "insufficient": 0,
            "total": 0,
        }

    success_count = 0
    failed_count = 0
    insufficient_count = 0

    def _process_one(symbol: str) -> tuple[str, list[dict]]:
        """计算单只股票指标并返回状态与待保存记录。"""
        try:
            df = db.get_daily_bars(symbol)
            if df.empty or len(df) < 60:
                return "insufficient", []

            if "trade_date" in df.columns and "date" not in df.columns:
                df = df.rename(columns={"trade_date": "date"})

            df_ind = engine.calculate_all_indicators(df)
            if df_ind.empty:
                return "insufficient", []

            # 统一日期列名
            date_col = "date" if "date" in df_ind.columns else "trade_date"
            records = []
            for _, row in df_ind.iterrows():
                record: dict[str, Any] = {"ts_code": symbol}
                trade_date = row.get(date_col)
                # SQLite executemany 不接受 pandas Timestamp，统一转字符串
                if hasattr(trade_date, "strftime"):
                    trade_date = trade_date.strftime("%Y-%m-%d")
                record["trade_date"] = trade_date
                for col in (
                    "close", "volume",
                    "ma5", "ma10", "ma20", "ma60", "ma120", "ma250",
                    "vol_ma5", "vol_ma50", "vol_ma60",
                    "boll_upper", "boll_mid", "boll_lower", "boll_bandwidth",
                    "cyc60", "chip_concentration",
                    "macd_dif", "macd_dea", "macd_hist",
                    "kdj_k", "kdj_d", "kdj_j",
                    "rsi6", "rsi12", "rsi24",
                    "cci",
                ):
                    record[col] = row.get(col)
                records.append(record)
            return "success", records
        except Exception as e:
            logger.warning(f"  ❌ {symbol} 指标计算失败: {e}")
            return "failed", []

    def _save_batch(records: list[dict]) -> int:
        """批量写入指标记录。"""
        if not records:
            return 0
        try:
            return db.save_indicators_batch(records)
        except Exception as e:
            logger.warning(f"  ⚠️ 批量保存指标失败: {e}")
            return 0

    workers = min(8, max(4, (os.cpu_count() or 2) + 2))
    batch_size = 100  # 每 100 只股票触发一次批量保存
    pending_records: list[dict] = []

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_process_one, symbol): symbol for symbol in symbols}
        for i, future in enumerate(as_completed(futures), 1):
            status, records = future.result()
            if status == "success":
                success_count += 1
                pending_records.extend(records)
            elif status == "insufficient":
                insufficient_count += 1
            else:
                failed_count += 1

            # 累积到批次大小或最后一批时统一保存，减少连接/提交开销
            if len(pending_records) >= batch_size or i == total:
                _save_batch(pending_records)
                pending_records = []

            if i % 100 == 0 or i == total:
                logger.info(f"  进度: {i}/{total} ({100 * i // total}%)")

    # 兜底：确保任何残留记录也被写入
    if pending_records:
        _save_batch(pending_records)

    logger.info("\n" + "=" * 60)
    logger.info("📊 技术指标计算完成")
    logger.info(f"  ✅ 成功: {success_count} 只")
    logger.info(f"  ⚠️  数据不足(<60天): {insufficient_count} 只")
    logger.info(f"  ❌ 失败: {failed_count} 只")
    logger.info("=" * 60)

    return {
        "status": "success" if failed_count == 0 else "degraded",
        "saved": success_count,
        "success": success_count,
        "failed": failed_count,
        "insufficient": insufficient_count,
        "total": total,
        "error": None if failed_count == 0 else f"{failed_count} symbols failed",
    }


# ===========================================================================
# 收盘刷新 helpers（Task 7）：由全量历史计算，只返回目标日一条记录，不写库
# ===========================================================================

_INDICATOR_VALUE_COLUMNS = (
    "close", "volume",
    "ma5", "ma10", "ma20", "ma60", "ma120", "ma250",
    "vol_ma5", "vol_ma50", "vol_ma60",
    "boll_upper", "boll_mid", "boll_lower", "boll_bandwidth",
    "cyc60", "chip_concentration",
    "macd_dif", "macd_dea", "macd_hist",
    "kdj_k", "kdj_d", "kdj_j",
    "rsi6", "rsi12", "rsi24",
    "cci",
)

_CHIP_VALUE_COLUMNS = (
    "profit_ratio", "avg_cost",
    "cost_90_low", "cost_90_high", "concentration_90",
    "cost_70_low", "cost_70_high", "concentration_70",
    "chip_concentration",
)


def _refresh_scalar_or_none(value: Any) -> float | None:
    """NaN / NaT / None → None，其余转 float（SQLite 不接受 NaN）。"""
    if value is None or (hasattr(value, "_is_nat") and value._is_nat) or value != value:
        return None
    return float(value)


def _refresh_date_str(value: Any) -> str:
    """pandas Timestamp / 字符串统一为 'YYYY-MM-DD'。"""
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d")
    return str(value)[:10]


def compute_indicator_record_for_refresh(
    db: DatabaseInterface,
    engine: IndicatorEngineInterface,
    symbol: str,
    target_date: str,
) -> tuple[dict | None, str | None]:
    """收盘刷新专用：由全量历史计算指标，仅返回目标日一条记录，不写库。

    Returns:
        (record, None) 成功；(None, reason) 未产出记录，reason 取值：
        "insufficient"（历史 < 60 天，新股不算失败）、
        "missing_target"（历史里没有目标日，如停牌）、
        "failed"（读库 / engine 计算异常）。
    """
    try:
        df = db.get_daily_bars(symbol)
        if df is None or df.empty or len(df) < 60:
            return None, "insufficient"

        if "trade_date" in df.columns and "date" not in df.columns:
            df = df.rename(columns={"trade_date": "date"})
        if target_date not in {_refresh_date_str(d) for d in df["date"]}:
            return None, "missing_target"

        df_ind = engine.calculate_all_indicators(df)
        if df_ind is None or df_ind.empty:
            return None, "failed"

        date_col = "date" if "date" in df_ind.columns else "trade_date"
        mask = df_ind[date_col].map(_refresh_date_str) == target_date
        matches = df_ind[mask]
        if matches.empty:
            return None, "missing_target"

        row = matches.iloc[-1]
        record: dict[str, Any] = {"ts_code": symbol, "trade_date": target_date}
        for col in _INDICATOR_VALUE_COLUMNS:
            record[col] = _refresh_scalar_or_none(row.get(col))
        return record, None
    except Exception as e:
        logger.warning(f"  {symbol} 收盘刷新指标计算失败: {e}")
        return None, "failed"


def compute_chip_record_for_refresh(
    db: DatabaseInterface,
    symbol: str,
    target_date: str,
    n_bins: int = CHIP_BINS,
) -> tuple[dict | None, str | None]:
    """收盘刷新专用：由全量历史计算筹码分布，仅返回目标日一条记录，不写库。

    Returns:
        (record, None) 成功；(None, reason) 未产出记录，reason 语义同
        compute_indicator_record_for_refresh。
    """
    try:
        df = db.get_daily_bars(symbol)
        if df is None or df.empty or len(df) < MIN_CHIP_DAYS:
            return None, "insufficient"
        if "date" in df.columns and "trade_date" not in df.columns:
            df = df.rename(columns={"date": "trade_date"})

        df_chip = _calculate_chip_distribution_for_symbol(df, n_bins=n_bins)
        if df_chip.empty:
            return None, "failed"

        mask = df_chip["trade_date"].map(_refresh_date_str) == target_date
        matches = df_chip[mask]
        if matches.empty:
            return None, "missing_target"

        row = matches.iloc[-1]
        record: dict[str, Any] = {"ts_code": symbol, "trade_date": target_date}
        for col in _CHIP_VALUE_COLUMNS:
            record[col] = _refresh_scalar_or_none(row.get(col))
        return record, None
    except Exception as e:
        logger.warning(f"  {symbol} 收盘刷新筹码计算失败: {e}")
        return None, "failed"


def _calculate_chip_distribution_for_symbol(
    df: pd.DataFrame, n_bins: int = 100
) -> pd.DataFrame:
    """基于换手率衰减模型计算单只股票的每日期望筹码分布统计量。

    输入：trade_date, open, high, low, close, turnover_rate
    输出：trade_date, profit_ratio, avg_cost, cost_90_low, cost_90_high,
          concentration_90, cost_70_low, cost_70_high, concentration_70,
          chip_concentration

    算法：每日筹码池 * (1 - turnover) 衰减，新筹码按典型价加入，提取分位数。

    Args:
        df: 日线 DataFrame（已按日期排序）。
        n_bins: 价格分档数，默认 100。

    Returns:
        筹码分布统计量的 DataFrame，数据不足 60 天则返回空。
    """
    df = df.sort_values("trade_date").reset_index(drop=True)
    if len(df) < MIN_CHIP_DAYS:
        return pd.DataFrame()

    close = df["close"].to_numpy(dtype=np.float64)
    open_ = df["open"].to_numpy(dtype=np.float64)
    high = df["high"].to_numpy(dtype=np.float64)
    low = df["low"].to_numpy(dtype=np.float64)

    typical_price = (open_ + high + low + close) / 4.0
    turnover = df["turnover_rate"].fillna(0).to_numpy(dtype=np.float64)
    if turnover.max() > 1:
        turnover = turnover / 100.0
    turnover = np.clip(turnover, 0.0, CHIP_MAX_TURNOVER)

    price_min = typical_price.min() * 0.95
    price_max = typical_price.max() * 1.05
    if price_max <= price_min:
        return pd.DataFrame()

    bin_edges = np.linspace(price_min, price_max, n_bins + 1)
    bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2.0

    weights = np.zeros(n_bins, dtype=np.float64)
    n_days = len(df)
    dates = df["trade_date"].values

    # 预分配结果数组
    arr_profit_ratio = np.full(n_days, np.nan, dtype=np.float64)
    arr_avg_cost = np.full(n_days, np.nan, dtype=np.float64)
    arr_cost_90_low = np.full(n_days, np.nan, dtype=np.float64)
    arr_cost_90_high = np.full(n_days, np.nan, dtype=np.float64)
    arr_cost_70_low = np.full(n_days, np.nan, dtype=np.float64)
    arr_cost_70_high = np.full(n_days, np.nan, dtype=np.float64)
    arr_concentration_90 = np.full(n_days, np.nan, dtype=np.float64)
    arr_concentration_70 = np.full(n_days, np.nan, dtype=np.float64)
    arr_chip_concentration = np.full(n_days, np.nan, dtype=np.float64)

    for i in range(n_days):
        t = turnover[i]
        price = typical_price[i]
        weights *= 1.0 - t
        bin_idx = int(np.searchsorted(bin_edges[1:], price))
        bin_idx = max(0, min(bin_idx, n_bins - 1))
        weights[bin_idx] += t

        total_w = weights.sum()
        if total_w > 0:
            avg_cost = float(np.average(bin_centers, weights=weights))
            cdf = np.cumsum(weights) / total_w

            # 批量 searchsorted（单次调用），保持与旧版一致的离散分位语义
            pct_indices = np.searchsorted(cdf, [0.05, 0.95, 0.15, 0.85])
            pct_indices = np.clip(pct_indices, 0, len(bin_centers) - 1)
            pct_vals = bin_centers[pct_indices]
            cost_90_low = float(pct_vals[0])
            cost_90_high = float(pct_vals[1])
            cost_70_low = float(pct_vals[2])
            cost_70_high = float(pct_vals[3])

            concentration_90 = (cost_90_high - cost_90_low) / avg_cost if avg_cost > 0 else 0.0
            concentration_70 = (cost_70_high - cost_70_low) / avg_cost if avg_cost > 0 else 0.0

            profit_ratio = float(np.sum(weights[bin_centers <= close[i]]) / total_w)
            chip_concentration = 1.0 - concentration_90

            arr_profit_ratio[i] = profit_ratio
            arr_avg_cost[i] = avg_cost
            arr_cost_90_low[i] = cost_90_low
            arr_cost_90_high[i] = cost_90_high
            arr_cost_70_low[i] = cost_70_low
            arr_cost_70_high[i] = cost_70_high
            arr_concentration_90[i] = concentration_90
            arr_concentration_70[i] = concentration_70
            arr_chip_concentration[i] = chip_concentration

    return pd.DataFrame({
        "trade_date": dates,
        "profit_ratio": arr_profit_ratio,
        "avg_cost": arr_avg_cost,
        "cost_90_low": arr_cost_90_low,
        "cost_90_high": arr_cost_90_high,
        "concentration_90": arr_concentration_90,
        "cost_70_low": arr_cost_70_low,
        "cost_70_high": arr_cost_70_high,
        "concentration_70": arr_concentration_70,
        "chip_concentration": arr_chip_concentration,
    })


def _process_chip_one(
    db: DatabaseInterface, symbol: str, n_bins: int, min_days: int
) -> str:
    """处理单只股票的筹码分布计算和保存。"""
    try:
        df = db.get_daily_bars(symbol)
        if df.empty or len(df) < min_days:
            return "insufficient"
        if "date" in df.columns and "trade_date" not in df.columns:
            df = df.rename(columns={"date": "trade_date"})

        df_chip = _calculate_chip_distribution_for_symbol(df, n_bins=n_bins)
        if df_chip.empty:
            return "failed"

        numeric_cols = [
            "profit_ratio", "avg_cost",
            "cost_90_low", "cost_90_high", "concentration_90",
            "cost_70_low", "cost_70_high", "concentration_70",
            "chip_concentration",
        ]
        records = []
        for _, row in df_chip.iterrows():
            trade_date = pd.to_datetime(row["trade_date"]).strftime("%Y-%m-%d")
            rec: dict[str, object] = {"ts_code": symbol, "trade_date": trade_date}
            for col in numeric_cols:
                val = row.get(col)
                # 抵御 pandas NaT / NaN / None → 存 None 避免 float() 崩溃
                if val is None or (hasattr(val, "_is_nat") and val._is_nat) or val != val:
                    rec[col] = None
                else:
                    rec[col] = float(val)
            records.append(rec)

        db.save_chip_distribution_batch(records)
        return "success"
    except Exception as e:
        logger.warning(f"  {symbol} 筹码分布计算失败: {e}")
        return "failed"


def update_chip_distribution(
    db: DatabaseInterface, symbols_to_update: list[str] | None = None
) -> dict:
    """计算并保存本地筹码分布数据。"""
    logger.info("\n" + "=" * 60)
    logger.info("任务: 本地筹码分布计算")
    logger.info("=" * 60)

    import sqlite3

    conn = sqlite3.connect(str(db.db_path))
    cursor = conn.cursor()

    if symbols_to_update is not None:
        symbols = symbols_to_update
        logger.info(f"指定模式: 计算 {len(symbols)} 只股票的筹码分布")
    else:
        logger.info("智能探测需要更新筹码分布的股票...")
        cursor.execute("""
            SELECT d.ts_code
            FROM (
                SELECT ts_code, MAX(trade_date) as max_bar_date
                FROM daily_bars
                GROUP BY ts_code
                HAVING COUNT(*) >= 60
            ) d
            LEFT JOIN (
                SELECT ts_code, MAX(trade_date) as max_chip_date
                FROM chip_distribution
                GROUP BY ts_code
            ) c ON d.ts_code = c.ts_code
            WHERE c.max_chip_date IS NULL OR d.max_bar_date > c.max_chip_date
            ORDER BY d.ts_code
        """)
        symbols = [row[0] for row in cursor.fetchall()]
        logger.info(f"探测完成: 共有 {len(symbols)} 只股票需要更新筹码分布")

    conn.close()

    total = len(symbols)
    if total == 0:
        logger.info("所有股票的筹码分布均已是最新，无需计算")
        return {
            "status": "success",
            "saved": 0,
            "success": 0,
            "failed": 0,
            "insufficient": 0,
            "total": 0,
        }

    n_bins = CHIP_BINS
    min_days = MIN_CHIP_DAYS
    success_count = 0
    failed_count = 0
    insufficient_count = 0

    workers = min(8, max(4, (os.cpu_count() or 2) + 2))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(_process_chip_one, db, symbol, n_bins, min_days): symbol
            for symbol in symbols
        }
        for i, future in enumerate(as_completed(futures), 1):
            status = future.result()
            if status == "success":
                success_count += 1
            elif status == "insufficient":
                insufficient_count += 1
            else:
                failed_count += 1

            if i % 100 == 0 or i == total:
                logger.info(f"  进度: {i}/{total} ({100 * i // total}%)")

    logger.info("\n" + "=" * 60)
    logger.info("本地筹码分布计算完成")
    logger.info(f"  成功: {success_count} 只")
    logger.info(f"  数据不足(<{min_days}天): {insufficient_count} 只")
    logger.info(f"  失败: {failed_count} 只")
    logger.info("=" * 60)

    return {
        "status": "success" if failed_count == 0 else "degraded",
        "saved": success_count,
        "success": success_count,
        "failed": failed_count,
        "insufficient": insufficient_count,
        "total": total,
        "error": None if failed_count == 0 else f"{failed_count} symbols failed",
    }
