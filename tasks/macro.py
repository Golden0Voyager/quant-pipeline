"""
宏观数据更新任务
────────────────
从 daily_pipeline.py 提取：北向资金、指数日线、涨停跌停、分红送转、
国际金价、国际原油、外汇汇率、全球指数、中美国债收益率。
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from core.calendar import get_expected_latest_trading_day
from interface import DatabaseInterface

try:
    import akshare as ak
except ImportError:
    ak = None

try:
    import requests
except ImportError:
    requests = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)


# ===========================================================================
# 辅助函数
# ===========================================================================


# ===========================================================================
# 北向资金
# ===========================================================================

# update_north_flow 已下线（2026-08）：港交所自 2024-08 起停止披露日度北向
# 资金净买入额，任务自建立起从未产出有效数据。north_flow 表保留在库中，
# 若未来恢复披露可从 git 历史找回本任务重新接入。


# ===========================================================================
# 北向资金个股持仓（季度快照）
# ===========================================================================

_EM_DATACENTER_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"
_EM_NORTH_HOLD_REPORT = "RPT_MUTUAL_HOLDSTOCKNORTH_STA"
_EM_NORTH_HOLD_COLUMNS = (
    "SECURITY_CODE,SECURITY_NAME,TRADE_DATE,CLOSE_PRICE,HOLD_SHARES,"
    "HOLD_MARKET_CAP,HOLD_SHARES_RATIO,FREE_SHARES_RATIO,TOTAL_SHARES_RATIO"
)
_EM_PAGE_SIZE = 500
_EM_MAX_PAGES = 20
_EM_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "https://data.eastmoney.com/hsgtcg/",
}


def _fetch_north_hold(timeout: float = 30.0) -> list[dict]:
    """获取北向资金个股持仓（东财数据中心，全市场最新季度快照）。

    2024-08-19 起沪深交易所停止每日个股北向披露，改为季度末快照。
    reportName=RPT_MUTUAL_HOLDSTOCKNORTH_STA 仅保留最新报告期，全市场约
    3900 只。每日运行幂等：同一季度重复 upsert，新季度披露后自动写入新一期。
    """
    if requests is None:
        logger.warning("⚠️ requests 未安装，无法获取北向持仓")
        return []
    records: list[dict] = []
    page = 1
    while page <= _EM_MAX_PAGES:
        params = {
            "reportName": _EM_NORTH_HOLD_REPORT,
            "columns": _EM_NORTH_HOLD_COLUMNS,
            "pageSize": str(_EM_PAGE_SIZE),
            "pageNumber": str(page),
            "sortColumns": "HOLD_MARKET_CAP",
            "sortTypes": "-1",
            "source": "WEB",
            "client": "WEB",
        }
        try:
            resp = requests.get(
                _EM_DATACENTER_URL, params=params,
                headers=_EM_HEADERS, timeout=timeout,
            )
            resp.raise_for_status()
            payload = resp.json()
        except (requests.RequestException, ValueError) as e:
            logger.warning(f"⚠️ 北向持仓第 {page} 页获取失败: {e}")
            break
        result = payload.get("result") or {}
        rows = result.get("data") or []
        if not rows:
            break
        for row in rows:
            code = str(row.get("SECURITY_CODE", "")).strip()
            if not code:
                continue
            records.append(
                {
                    "ts_code": code,
                    "security_name": str(row.get("SECURITY_NAME", "") or "").strip(),
                    "trade_date": str(row.get("TRADE_DATE", ""))[:10],
                    "close_price": row.get("CLOSE_PRICE"),
                    "hold_shares": row.get("HOLD_SHARES"),
                    "hold_market_cap": row.get("HOLD_MARKET_CAP"),
                    "hold_shares_ratio": row.get("HOLD_SHARES_RATIO"),
                    "free_shares_ratio": row.get("FREE_SHARES_RATIO"),
                    "total_shares_ratio": row.get("TOTAL_SHARES_RATIO"),
                    "data_source": "eastmoney",
                }
            )
        total_pages = result.get("pages") or 1
        if page >= total_pages:
            break
        page += 1
    return records


def update_north_hold(db: DatabaseInterface) -> dict:
    """获取北向资金个股持仓并保存（季度快照，每日幂等 upsert）。"""
    logger.info("\n" + "=" * 60)
    logger.info("🌐 任务: 更新北向资金个股持仓")
    logger.info("=" * 60)

    try:
        records = _fetch_north_hold()
        if not records:
            logger.warning("⚠️ 北向持仓无数据")
            return {"saved": 0, "total": 0}
        saved = db.save_north_hold_batch(records)
        latest = max((r["trade_date"] for r in records if r.get("trade_date")), default="")
        logger.info(f"✅ 北向持仓保存完成: {saved} 条 (报告期 {latest})")
        return {"saved": saved, "total": len(records)}
    except Exception as e:
        logger.error(f"❌ 北向持仓更新失败: {e}")
        return {"saved": 0, "error": str(e)}


# ===========================================================================
# 指数日线
# ===========================================================================


def _fetch_index_daily(trade_date: str) -> list[dict]:
    """获取主要指数日线行情（上证、深证、创业板、科创50）。"""
    if ak is None:
        return []
    records = []
    indices = {
        "sh000001": "上证指数",
        "sz399001": "深证成指",
        "sz399006": "创业板指",
        "sh000688": "科创50",
    }
    for index_code, index_name in indices.items():
        try:
            df = ak.stock_zh_index_daily_tx(symbol=index_code)
            if df is not None and not df.empty:
                latest = df.iloc[-1]
                records.append(
                    {
                        "index_code": index_code,
                        "index_name": index_name,
                        "trade_date": str(latest.get("date", trade_date))[:10],
                        "open": float(latest.get("open", 0)),
                        "high": float(latest.get("high", 0)),
                        "low": float(latest.get("low", 0)),
                        "close": float(latest.get("close", 0)),
                        "volume": float(latest.get("volume", 0)),
                        "data_source": "akshare",
                    }
                )
        except Exception as e:
            logger.warning(f"⚠️ 指数 {index_name}({index_code}) 获取失败: {e}")
    return records


def update_index_daily(db: DatabaseInterface) -> dict:
    """获取主要指数日线行情并保存。"""
    logger.info("\n" + "=" * 60)
    logger.info("📊 任务: 更新指数日线行情")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "error": "akshare not installed"}

    try:
        records = _fetch_index_daily(get_expected_latest_trading_day())
        if not records:
            logger.warning("⚠️ 指数日线无数据")
            return {"saved": 0, "total": 0}
        saved = db.save_index_daily_batch(records)
        logger.info(f"✅ 指数日线保存完成: {saved} 条")
        return {"saved": saved, "total": len(records)}
    except Exception as e:
        logger.error(f"❌ 指数日线更新失败: {e}")
        return {"saved": 0, "error": str(e)}


# ===========================================================================
# 涨停跌停统计
# ===========================================================================


def _fetch_limit_up_down(trade_date: str) -> list[dict]:
    """获取涨停跌停统计。"""
    if ak is None:
        return []
    date_compact = trade_date.replace("-", "")
    try:
        df = ak.stock_zt_pool_em(date=date_compact)
        if df is None or df.empty:
            return []
        records = []
        for _, row in df.iterrows():
            records.append(
                {
                    "trade_date": trade_date,
                    "ts_code": str(row.get("代码", "")).strip(),
                    "name": str(row.get("名称", "")).strip(),
                    "pct_change": row.get("涨跌幅"),
                    "close_price": row.get("最新价"),
                    "turnover_rate": row.get("换手率"),
                    "limit_type": "涨停",
                    "board_count": row.get("连板数"),
                    "industry": str(row.get("所属行业", "")).strip(),
                    "data_source": "akshare",
                }
            )
        return records
    except Exception as e:
        logger.warning(f"⚠️ 涨停数据获取失败: {e}")
        return []


def _fetch_limit_down(trade_date: str) -> list[dict]:
    """获取跌停统计。"""
    if ak is None:
        return []
    date_compact = trade_date.replace("-", "")
    try:
        df = ak.stock_zt_pool_dtgc_em(date=date_compact)
        if df is None or df.empty:
            return []
        records = []
        for _, row in df.iterrows():
            records.append(
                {
                    "trade_date": trade_date,
                    "ts_code": str(row.get("代码", "")).strip(),
                    "name": str(row.get("名称", "")).strip(),
                    "pct_change": row.get("涨跌幅"),
                    "close_price": row.get("最新价"),
                    "turnover_rate": row.get("换手率"),
                    "limit_type": "跌停",
                    "board_count": None,
                    "industry": str(row.get("所属行业", "")).strip(),
                    "data_source": "akshare",
                }
            )
        return records
    except Exception as e:
        logger.warning(f"⚠️ 跌停数据获取失败: {e}")
        return []


def update_limit_up_down(db: DatabaseInterface) -> dict:
    """获取涨停跌停统计并保存。"""
    logger.info("\n" + "=" * 60)
    logger.info("🚀 任务: 更新涨停跌停统计")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "error": "akshare not installed"}

    trade_date = get_expected_latest_trading_day()
    try:
        limit_up = _fetch_limit_up_down(trade_date)
        limit_down = _fetch_limit_down(trade_date)
        all_records = limit_up + limit_down
        if not all_records:
            logger.warning("⚠️ 涨停跌停无数据")
            return {"saved": 0, "total": 0}
        saved = db.save_limit_up_down_batch(all_records)
        logger.info(f"✅ 涨停跌停保存完成: {saved} 条 (涨停 {len(limit_up)}, 跌停 {len(limit_down)})")
        return {"saved": saved, "total": len(all_records)}
    except Exception as e:
        logger.error(f"❌ 涨停跌停更新失败: {e}")
        return {"saved": 0, "error": str(e)}


# ===========================================================================
# 分红送转
# ===========================================================================


def _fetch_dividend_summary() -> list[dict]:
    """获取全市场分红送转汇总。"""
    if ak is None:
        return []
    try:
        df = ak.stock_history_dividend()
        if df is None or df.empty:
            return []
        records = []
        for _, row in df.iterrows():
            records.append(
                {
                    "ts_code": str(row.get("代码", "")).strip(),
                    "name": str(row.get("名称", "")).strip(),
                    "list_date": str(row.get("上市日期", ""))[:10],
                    "cumulative_dividend": row.get("累计股息"),
                    "avg_annual_dividend": row.get("年均股息"),
                    "dividend_count": row.get("分红次数"),
                    "total_raise_amount": row.get("融资总额"),
                    "raise_count": row.get("融资次数"),
                    "data_source": "akshare",
                }
            )
        return records
    except Exception as e:
        logger.warning(f"⚠️ 分红送转数据获取失败: {e}")
        return []


def update_dividend_summary(db: DatabaseInterface) -> dict:
    """获取全市场分红送转汇总并保存。"""
    logger.info("\n" + "=" * 60)
    logger.info("💰 任务: 更新分红送转汇总")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "error": "akshare not installed"}

    try:
        records = _fetch_dividend_summary()
        if not records:
            logger.warning("⚠️ 分红送转无数据")
            return {"saved": 0, "total": 0}
        saved = db.save_dividend_summary_batch(records)
        logger.info(f"✅ 分红送转保存完成: {saved} 条")
        return {"saved": saved, "total": len(records)}
    except Exception as e:
        logger.error(f"❌ 分红送转更新失败: {e}")
        return {"saved": 0, "error": str(e)}


# ===========================================================================
# 国际金价
# ===========================================================================


def _fetch_gold_price(trade_date: str) -> list[dict]:
    """获取上海金交所基准金价（早盘价/晚盘价）。"""
    if ak is None:
        return []
    try:
        df = ak.spot_golden_benchmark_sge()
        if df is None or df.empty:
            return []
        records = []
        for _, row in df.iterrows():
            records.append(
                {
                    "trade_date": trade_date,
                    "trading_time": str(row.get("交易时间", "")).strip(),
                    "evening_price": row.get("晚盘价"),
                    "morning_price": row.get("早盘价"),
                    "data_source": "akshare",
                }
            )
        return records
    except Exception as e:
        logger.warning(f"⚠️ 国际金价获取失败: {e}")
        return []


def update_gold_price(db: DatabaseInterface) -> dict:
    """获取上海金交所基准金价并保存。"""
    logger.info("\n" + "=" * 60)
    logger.info("🥇 任务: 更新国际金价")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "error": "akshare not installed"}

    try:
        records = _fetch_gold_price(get_expected_latest_trading_day())
        if not records:
            logger.warning("⚠️ 国际金价无数据")
            return {"saved": 0, "total": 0}
        saved = db.save_gold_price_batch(records)
        logger.info(f"✅ 国际金价保存完成: {saved} 条")
        return {"saved": saved, "total": len(records)}
    except Exception as e:
        logger.error(f"❌ 国际金价更新失败: {e}")
        return {"saved": 0, "error": str(e)}


# ===========================================================================
# 国际原油
# ===========================================================================


def _fetch_crude_oil(trade_date: str) -> list[dict]:
    """获取国际原油实时行情（WTI=CL, Brent=OIL）。"""
    if ak is None:
        return []
    contracts = {"CL": "WTI原油", "OIL": "Brent原油"}
    records = []
    for contract, name in contracts.items():
        try:
            df = ak.futures_foreign_commodity_realtime(symbol=contract)
            if df is None or df.empty:
                continue
            for _, row in df.iterrows():
                records.append(
                    {
                        "trade_date": trade_date,
                        "contract": contract,
                        "name": name,
                        "latest_price": row.get("最新价"),
                        "cny_price": row.get("人民币报价"),
                        "change": row.get("涨跌额"),
                        "change_pct": row.get("涨跌幅"),
                        "open": row.get("开盘价"),
                        "high": row.get("最高价"),
                        "low": row.get("最低价"),
                        "pre_settle": row.get("昨日结算价"),
                        "data_source": "akshare",
                    }
                )
        except Exception as e:
            logger.warning(f"⚠️ 原油 {name}({contract}) 获取失败: {e}")
    return records


def update_crude_oil(db: DatabaseInterface) -> dict:
    """获取国际原油实时行情并保存。"""
    logger.info("\n" + "=" * 60)
    logger.info("🛢️ 任务: 更新国际原油")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "error": "akshare not installed"}

    try:
        records = _fetch_crude_oil(get_expected_latest_trading_day())
        if not records:
            logger.warning("⚠️ 国际原油无数据")
            return {"saved": 0, "total": 0}
        saved = db.save_crude_oil_batch(records)
        logger.info(f"✅ 国际原油保存完成: {saved} 条")
        return {"saved": saved, "total": len(records)}
    except Exception as e:
        logger.error(f"❌ 国际原油更新失败: {e}")
        return {"saved": 0, "error": str(e)}


# ===========================================================================
# 外汇汇率（美元兑人民币）
# ===========================================================================


def _fetch_usd(trade_date: str) -> list[dict]:
    """获取美元兑人民币外汇牌价（中国银行）。

    currency_boc_sina 的日期参数为紧凑格式 YYYYMMDD（无横线），
    且单日查询常返回空，故用一个向后回溯的小窗口取一段时间内的有效记录。
    INSERT OR REPLACE 写入保证不会产生重复。
    """
    if ak is None:
        return []
    start = (datetime.strptime(trade_date, "%Y-%m-%d") - timedelta(days=10)).strftime("%Y%m%d")
    end = trade_date.replace("-", "")
    try:
        df = ak.currency_boc_sina(symbol="美元", start_date=start, end_date=end)
        if df is None or df.empty:
            return []
        records: list[dict] = []
        for _, row in df.iterrows():
            records.append(
                {
                    "trade_date": str(row.get("日期", trade_date))[:10],
                    "currency": "美元",
                    "bank_buy_price": row.get("中行汇买价"),
                    "cash_buy_price": row.get("中行钞买价"),
                    "cash_sell_price": row.get("中行钞卖价/汇卖价"),
                    "central_parity_rate": row.get("央行中间价"),
                    "boc_convert_price": row.get("中行折算价"),
                    "data_source": "akshare",
                }
            )
        return records
    except Exception as e:
        logger.warning(f"⚠️ 美元汇率获取失败: {e}")
        return []


def update_usd(db: DatabaseInterface) -> dict:
    """获取美元兑人民币外汇牌价并保存。"""
    logger.info("\n" + "=" * 60)
    logger.info("💱 任务: 更新外汇汇率")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "error": "akshare not installed"}

    try:
        records = _fetch_usd(get_expected_latest_trading_day())
        if not records:
            logger.warning("⚠️ 外汇汇率无数据")
            return {"saved": 0, "total": 0}
        saved = db.save_usd_batch(records)
        logger.info(f"✅ 外汇汇率保存完成: {saved} 条")
        return {"saved": saved, "total": len(records)}
    except Exception as e:
        logger.error(f"❌ 外汇汇率更新失败: {e}")
        return {"saved": 0, "error": str(e)}


# ===========================================================================
# 全球指数
# ===========================================================================


# 新浪财经全球指数代码 -> 当前数据库使用的东方财富指数代码
# 用于东财实时接口失败时，用新浪历史行情 fallback 更新同一张表
_SINA_TO_EM_CODE: dict[str, str | None] = {
    "UKX": "FTSE",
    "DAX": "GDAXI",
    "INDEXCF": None,      # 俄罗斯 MICEX，数据库中暂无对应
    "CAC": "FCHI",
    "SWI20": None,        # 瑞士股票指数，数据库中暂无对应
    "FTSEMIB": "MIB",
    "AEX": "AEX",
    "IBEX": "IBEX",
    "SX5E": "SX5E",
    "GSPTSE": "TSX",
    "MXX": "MXX",
    "IBOV": "BVSP",
    "TWJQ": "TWII",
    "NKY": "N225",
    "KOSPI": "KS11",      # 东财另有 KOSPI200，新浪仅提供综合指数
    "JCI": "JKSE",
    "SENSEX": "SENSEX",
    "AS51": "AS51",
    "NZ250": "NZ50",
    "CASE": "CASE",
}


def _fetch_global_index(trade_date: str) -> list[dict]:
    """获取全球主要指数实时行情。"""
    if ak is None:
        return []
    try:
        df = ak.index_global_spot_em()
        if df is None or df.empty:
            return []
        records = []
        for _, row in df.iterrows():
            records.append(
                {
                    "trade_date": trade_date,
                    "index_code": str(row.get("代码", "")).strip(),
                    "index_name": str(row.get("名称", "")).strip(),
                    "latest_price": row.get("最新价"),
                    "change_amount": row.get("涨跌额"),
                    "change_pct": row.get("涨跌幅"),
                    "open": row.get("开盘价"),
                    "high": row.get("最高价"),
                    "low": row.get("最低价"),
                    "pre_close": row.get("昨收价"),
                    "amplitude": row.get("振幅"),
                    "quote_time": str(row.get("最新行情时间", "")).strip(),
                    "data_source": "akshare",
                }
            )
        return records
    except Exception as e:
        logger.warning(f"⚠️ 全球指数获取失败: {e}")
        return []


def _fetch_global_index_sina(trade_date: str) -> list[dict]:
    """当东财实时接口失败时，使用新浪财经全球指数历史行情作为 fallback。

    返回字段与 _fetch_global_index 保持一致，以便复用同一张表。
    """
    if ak is None:
        return []
    try:
        name_df = ak.index_global_name_table()
    except Exception as e:
        logger.warning(f"⚠️ 新浪全球指数列表获取失败: {e}")
        return []

    records: list[dict] = []
    for _, row in name_df.iterrows():
        sina_code = str(row.get("代码", "")).strip()
        name = str(row.get("指数名称", "")).strip()
        em_code = _SINA_TO_EM_CODE.get(sina_code)
        if em_code is None:
            continue
        try:
            df = ak.index_global_hist_sina(name)
            if df is None or df.empty:
                continue
            df = df.sort_values("date").reset_index(drop=True)
            latest = df.iloc[-1]
            prev = df.iloc[-2] if len(df) >= 2 else latest
            close = float(latest["close"])
            pre_close = float(prev["close"])
            change_amount = close - pre_close
            change_pct = (change_amount / pre_close * 100) if pre_close else 0.0
            high = float(latest["high"])
            low = float(latest["low"])
            amplitude = ((high - low) / pre_close * 100) if pre_close else 0.0
            records.append(
                {
                    "trade_date": str(latest["date"])[:10],
                    "index_code": em_code,
                    "index_name": name,
                    "latest_price": close,
                    "change_amount": round(change_amount, 4),
                    "change_pct": round(change_pct, 4),
                    "open": float(latest["open"]),
                    "high": high,
                    "low": low,
                    "pre_close": pre_close,
                    "amplitude": round(amplitude, 4),
                    "quote_time": "",
                    "data_source": "akshare_sina_fallback",
                }
            )
        except Exception as e:
            logger.warning(f"⚠️ 新浪全球指数 {name} 获取失败: {e}")
    return records


def update_global_index(db: DatabaseInterface) -> dict:
    """获取全球主要指数实时行情并保存；东财失败时自动切新浪 fallback。"""
    logger.info("\n" + "=" * 60)
    logger.info("🌍 任务: 更新全球指数")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "error": "akshare not installed"}

    try:
        records = _fetch_global_index(get_expected_latest_trading_day())
        if not records:
            logger.warning("⚠️ 东财全球指数无数据，尝试新浪 fallback...")
            records = _fetch_global_index_sina(get_expected_latest_trading_day())
        if not records:
            logger.warning("⚠️ 全球指数无数据")
            return {"saved": 0, "total": 0}
        saved = db.save_global_index_batch(records)
        logger.info(f"✅ 全球指数保存完成: {saved} 条")
        return {"saved": saved, "total": len(records)}
    except Exception as e:
        logger.error(f"❌ 全球指数更新失败: {e}")
        return {"saved": 0, "error": str(e)}


# ===========================================================================
# 中美国债收益率
# ===========================================================================


def _fetch_us_treasury(trade_date: str) -> list[dict]:
    """获取中美国债收益率曲线。

    bond_zh_us_rate 的 start_date 为紧凑格式 YYYYMMDD，且当日收益率通常尚未
    发布，故回溯一个月取一段时间内的有效记录。
    INSERT OR REPLACE 写入保证不会产生重复。
    """
    if ak is None:
        return []
    start = (datetime.strptime(trade_date, "%Y-%m-%d") - timedelta(days=30)).strftime("%Y%m%d")
    try:
        df = ak.bond_zh_us_rate(start_date=start)
        if df is None or df.empty:
            return []
        records: list[dict] = []
        for _, row in df.iterrows():
            records.append(
                {
                    "trade_date": str(row.get("日期", trade_date))[:10],
                    "us_2y": row.get("美国国债收益率2年"),
                    "us_5y": row.get("美国国债收益率5年"),
                    "us_10y": row.get("美国国债收益率10年"),
                    "us_30y": row.get("美国国债收益率30年"),
                    "cn_2y": row.get("中国国债收益率2年"),
                    "cn_5y": row.get("中国国债收益率5年"),
                    "cn_10y": row.get("中国国债收益率10年"),
                    "cn_30y": row.get("中国国债收益率30年"),
                    "spread_10y_2y": row.get("美国国债收益率10年-2年"),
                    "data_source": "akshare",
                }
            )
        return records
    except Exception as e:
        logger.warning(f"⚠️ 中美国债收益率获取失败: {e}")
        return []


def update_us_treasury(db: DatabaseInterface) -> dict:
    """获取中美国债收益率曲线并保存。"""
    logger.info("\n" + "=" * 60)
    logger.info("📈 任务: 更新中美国债收益率")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "error": "akshare not installed"}

    try:
        records = _fetch_us_treasury(get_expected_latest_trading_day())
        if not records:
            logger.warning("⚠️ 中美国债收益率无数据")
            return {"saved": 0, "total": 0}
        saved = db.save_us_treasury_batch(records)
        logger.info(f"✅ 中美国债收益率保存完成: {saved} 条")
        return {"saved": saved, "total": len(records)}
    except Exception as e:
        logger.error(f"❌ 中美国债收益率更新失败: {e}")
        return {"saved": 0, "error": str(e)}


# ===========================================================================
# 收盘刷新 helper（Task 9）：只抓取/归一化，不写库，源异常直接上抛
# ===========================================================================


def fetch_limit_pool_records(trade_date: str) -> list[dict]:
    """收盘刷新专用：抓取指定交易日涨停池 + 跌停池并合并归一化。

    两池均权威空返回 []（空池 ≠ 源失败）；源异常直接上抛，
    不得吞掉后当空池处理（由适配器/编排器落实保留旧数据）。
    """
    date_compact = trade_date.replace("-", "")
    records: list[dict] = []

    up_df = ak.stock_zt_pool_em(date=date_compact)
    if up_df is not None and not up_df.empty:
        for _, row in up_df.iterrows():
            records.append({
                "trade_date": trade_date,
                "ts_code": str(row.get("代码", "")).strip(),
                "name": str(row.get("名称", "")).strip(),
                "pct_change": row.get("涨跌幅"),
                "close_price": row.get("最新价"),
                "turnover_rate": row.get("换手率"),
                "limit_type": "涨停",
                "board_count": row.get("连板数"),
                "industry": str(row.get("所属行业", "")).strip(),
                "data_source": "akshare",
            })

    down_df = ak.stock_zt_pool_dtgc_em(date=date_compact)
    if down_df is not None and not down_df.empty:
        for _, row in down_df.iterrows():
            records.append({
                "trade_date": trade_date,
                "ts_code": str(row.get("代码", "")).strip(),
                "name": str(row.get("名称", "")).strip(),
                "pct_change": row.get("涨跌幅"),
                "close_price": row.get("最新价"),
                "turnover_rate": row.get("换手率"),
                "limit_type": "跌停",
                "board_count": None,
                "industry": str(row.get("所属行业", "")).strip(),
                "data_source": "akshare",
            })

    return records
