"""
行业/板块/基差数据更新任务
─────────────────────────
行业板块涨跌幅、板块估值(PE/PB)、股指期货基差。
"""

from __future__ import annotations

import contextlib
import logging
import time
from collections.abc import Generator
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


@contextlib.contextmanager
def _suppress_akshare_tqdm() -> Generator[None, None, None]:
    """临时禁用 akshare 内部的 tqdm 进度条，避免污染 TUI 日志。"""
    try:
        from akshare.utils import tqdm as _tqdm_mod
    except Exception:  # noqa: BLE001
        yield
        return
    orig = _tqdm_mod.get_tqdm
    _tqdm_mod.get_tqdm = lambda enable=True: lambda iterable, *args, **kwargs: iterable
    try:
        yield
    finally:
        _tqdm_mod.get_tqdm = orig


def _records_from_hist_df(df: pd.DataFrame, sector: str) -> list[dict]:
    """把带标准列名的历史 DataFrame 转为 sector_daily 记录。"""
    keep = {"trade_date", "open", "close", "high", "low", "volume", "amount", "pct_change"}
    available = [c for c in keep if c in df.columns]
    if not available:
        return []
    records: list[dict] = []
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
    return records


def _fetch_sector_daily_em() -> tuple[list[dict], bool]:
    """通过东方财富获取主要行业板块日线。返回 (records, source_ok)。

    source_ok=False 表示列表接口失败，可尝试备用数据源。
    """
    board_df = _retry(
        ak.stock_board_industry_name_em, label="东方财富行业板块列表"
    )
    if board_df is None:
        return [], False
    if board_df.empty:
        return [], True
    sector_names = board_df["板块名称"].tolist() if "板块名称" in board_df.columns else []
    targets = [s for s in sector_names if s in MAJOR_SECTORS]
    if not targets:
        logger.warning("⚠️ 未找到匹配的主要行业板块")
        return [], True
    logger.info(f"📋 东方财富行业板块: {len(targets)} 个")

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
                label=f"东方财富行业板块 {sector} 历史",
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
            records.extend(_records_from_hist_df(df, sector))
        except Exception as e:
            logger.warning(f"⚠️ 东方财富行业板块 {sector} 历史数据获取失败: {e}")
    return records, True


def _fetch_sector_daily_ths() -> list[dict]:
    """通过同花顺获取全部行业板块日线，作为东方财富的备用数据源。"""
    if ak is None:
        return []
    if not hasattr(ak, "stock_board_industry_name_ths") or not hasattr(
        ak, "stock_board_industry_index_ths"
    ):
        return []
    try:
        with _suppress_akshare_tqdm():
            board_df = _retry(
                ak.stock_board_industry_name_ths, label="同花顺行业板块列表"
            )
        if board_df is None or board_df.empty:
            return []
        sector_names = board_df["name"].tolist() if "name" in board_df.columns else []
        if not sector_names:
            return []
        logger.info(f"📋 同花顺行业板块: {len(sector_names)} 个")

        records: list[dict] = []
        start_date = (datetime.now() - timedelta(days=730)).strftime("%Y%m%d")
        end_date = datetime.now().strftime("%Y%m%d")
        for sector in sector_names:
            try:
                with _suppress_akshare_tqdm():
                    df = _retry(
                        lambda s=sector, sd=start_date, ed=end_date: ak.stock_board_industry_index_ths(
                            symbol=s,
                            start_date=sd,
                            end_date=ed,
                        ),
                        label=f"同花顺行业板块 {sector} 历史",
                    )
                if df is None or df.empty:
                    continue
                col_map = {
                    "日期": "trade_date",
                    "开盘价": "open",
                    "收盘价": "close",
                    "最高价": "high",
                    "最低价": "low",
                    "成交量": "volume",
                    "成交额": "amount",
                }
                df = df.rename(columns=col_map)
                if "close" not in df.columns or "trade_date" not in df.columns:
                    continue
                df = df.sort_values("trade_date").reset_index(drop=True)
                df["pct_change"] = (df["close"].pct_change() * 100).round(4)
                records.extend(_records_from_hist_df(df, sector))
            except Exception as e:
                logger.warning(f"⚠️ 同花顺行业板块 {sector} 历史数据获取失败: {e}")
        return records
    except Exception as e:
        logger.warning(f"⚠️ 同花顺行业板块数据获取失败: {e}")
        return []


def _fetch_sector_daily() -> list[dict]:
    """获取主要行业板块日线涨跌幅数据，东方财富失败时自动回退到同花顺。"""
    if ak is None:
        return []
    records, source_ok = _fetch_sector_daily_em()
    if records:
        return records
    if source_ok:
        return records
    logger.info("🔄 东方财富行业板块列表不可用，尝试同花顺数据源...")
    return _fetch_sector_daily_ths()


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

    total_saved = sum(
        (v if isinstance(v, (int, float)) else 0) for v in results.values()
    )
    has_error = any(
        isinstance(v, str) and v.startswith("error:") for v in results.values()
    )
    if has_error:
        results["status"] = "degraded"
        results["error"] = "部分子任务失败，详见各子任务记录"
    elif total_saved > 0:
        results["status"] = "success"
    else:
        results["status"] = "no_data"
        results["reason"] = "all sub-tasks returned empty"

    results["saved"] = total_saved
    results["total"] = total_saved
    return results


# ===========================================================================
# 收盘刷新 helper（Task 9）：只抓取/归一化，不写库，源异常直接上抛
# ===========================================================================

_SECTOR_HIST_COL_MAP = {
    "日期": "trade_date",
    "开盘": "open",
    "收盘": "close",
    "最高": "high",
    "最低": "low",
    "成交量": "volume",
    "成交额": "amount",
    "涨跌幅": "pct_change",
}

_SECTOR_VALUATION_COL_MAP = {
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


def fetch_sector_daily_records(trade_date: str) -> list[dict]:
    """收盘刷新专用：目标日单日窗口抓取主要行业板块日线并归一化。

    不做退避重试/同花顺回退，源异常直接上抛；单板块空日跳过，
    三表是否可接受由复合适配器把关。
    """
    board_df = ak.stock_board_industry_name_em()
    if board_df is None or board_df.empty:
        return []
    sector_names = board_df["板块名称"].tolist() if "板块名称" in board_df.columns else []
    targets = [s for s in sector_names if s in MAJOR_SECTORS]
    compact = trade_date.replace("-", "")
    records: list[dict] = []
    for sector in targets:
        df = ak.stock_board_industry_hist_em(
            symbol=sector, start_date=compact, end_date=compact
        )
        if df is None or df.empty:
            continue
        df = df.rename(columns=_SECTOR_HIST_COL_MAP)
        records.extend(_records_from_hist_df(df, sector))
    return records


def fetch_sector_valuation_records(trade_date: str) -> list[dict]:
    """收盘刷新专用：直传目标日抓取板块估值（PE/PB）并归一化。

    权威空返回 []；源异常直接上抛。
    """
    df = ak.stock_industry_pe_ratio_cninfo(
        symbol="证监会行业分类", date=trade_date.replace("-", "")
    )
    if df is None or df.empty:
        return []
    df = df.rename(columns=_SECTOR_VALUATION_COL_MAP)
    keep = {"sector_name", "trade_date", "pe", "pb", "total_mv"}
    available = [c for c in keep if c in df.columns]
    if not available:
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


def fetch_index_futures_basis_records(trade_date: str) -> list[dict]:
    """收盘刷新专用：拆历史型期货/指数源，只发目标日基差行。

    合约空日跳过（是否可接受由复合适配器把关）；源异常直接上抛。
    """
    futures_col_map = {
        "日期": "date",
        "收盘价": "close",
    }
    records: list[dict] = []
    for futures_code, (index_code, _index_name) in FUTURES_CONTRACTS.items():
        futures_df = ak.futures_zh_daily_sina(symbol=futures_code)
        if futures_df is None or futures_df.empty:
            continue
        futures_df = futures_df.rename(columns=futures_col_map)
        if "date" not in futures_df.columns or "close" not in futures_df.columns:
            continue

        index_df = ak.stock_zh_index_daily_tx(symbol=index_code)
        if index_df is None or index_df.empty:
            continue
        index_df = index_df.rename(columns={"close": "index_close"})
        if "date" not in index_df.columns or "index_close" not in index_df.columns:
            continue
        index_close_map: dict[str, float] = {}
        for _, row in index_df.iterrows():
            d = str(row.get("date", ""))[:10]
            c = _to_float(row.get("index_close"))
            if d and c is not None:
                index_close_map[d] = c

        for _, row in futures_df.iterrows():
            d = str(row.get("date", ""))[:10]
            if d != trade_date:
                continue
            futures_price = _to_float(row.get("close"))
            index_price = index_close_map.get(d)
            if futures_price is None or index_price is None:
                continue
            basis = futures_price - index_price
            basis_pct = (
                round((futures_price / index_price - 1) * 100, 4) if index_price else None
            )
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
