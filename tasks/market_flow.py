"""
市场资金流任务模块
───────────────────
资金流向、融资融券、龙虎榜、大宗交易、板块资金流向。
"""

from __future__ import annotations

import logging
import time  # noqa: F401
from datetime import datetime, timedelta

from interface import DatabaseInterface, DataLoaderInterface

try:
    import akshare as ak
except ImportError:
    ak = None  # type: ignore[assignment]

import pandas as pd

logger = logging.getLogger(__name__)


# ===========================================================================
# 任务 4: 批量获取资金流向
# ===========================================================================


def update_fund_flow(db: DatabaseInterface, loader: DataLoaderInterface) -> dict:
    """批量获取全市场资金流向并保存。"""
    logger.info("\n" + "=" * 60)
    logger.info("💰 任务: 批量获取资金流向")
    logger.info("=" * 60)

    today = datetime.now().strftime("%Y-%m-%d")

    try:
        df = loader.get_market_fund_flow()
        if df.empty:
            logger.warning("⚠️  未获取到资金流向数据")
            return {"saved": 0, "total": 0}

        batch_records = []
        for _, row in df.iterrows():
            try:
                code = str(row.get("code", "")).strip()
                if not code:
                    continue

                data = {
                    "symbol": code,
                    "date": today,
                    "main_net_inflow": row.get("main_net_inflow"),
                    "main_net_inflow_pct": row.get("main_net_inflow_pct"),
                    "super_large_net_inflow": row.get("super_large_net_inflow"),
                    "super_large_net_inflow_pct": row.get("super_large_net_inflow_pct"),
                    "large_net_inflow": row.get("large_net_inflow"),
                    "large_net_inflow_pct": row.get("large_net_inflow_pct"),
                    "simulated": False,
                }
                # Skip rows where ALL six numeric fields are NaN/None
                numeric_fields = [
                    "main_net_inflow", "main_net_inflow_pct",
                    "super_large_net_inflow", "super_large_net_inflow_pct",
                    "large_net_inflow", "large_net_inflow_pct",
                ]
                if all(pd.isna(data.get(f)) for f in numeric_fields):
                    logger.debug(f"  跳过全空资金流: {code}")
                    continue
                batch_records.append(data)
            except Exception as e:
                logger.debug(f"  保存资金流失败: {e}")
                continue

        saved = db.save_fund_flow_batch(batch_records) if batch_records else 0
        logger.info(f"✅ 资金流向保存完成: {saved}/{len(df)} 只")
        return {"saved": saved, "total": len(df)}

    except Exception as e:
        logger.error(f"❌ 资金流向获取失败: {e}")
        return {"saved": 0, "total": 0, "error": str(e)}


# ===========================================================================
# 任务 5: 批量获取融资融券数据
# ===========================================================================


def _get_expected_latest_trading_day() -> str:
    """获取期望的最新交易日日期 (YYYY-MM-DD)。
    如果是周末，期望最新交易日为上周五；
    如果是周一至周五，且在 15:30 之前，期望最新交易日为前一个交易日；
    如果是周一至周五，且在 15:30 之后，期望最新交易日为今天。
    """
    now = datetime.now()
    target = now
    # 如果是交易日（周一至周五），在 15:30 之前，预期的数据最新是前一天
    if target.weekday() < 5 and (target.hour < 15 or (target.hour == 15 and target.minute < 30)):
        target -= timedelta(days=1)

    # 如果目标日期是周末，则向前回滚到周五
    while target.weekday() >= 5:
        target -= timedelta(days=1)

    return target.strftime("%Y-%m-%d")


def _safe_fetch_margin_detail(fetcher, date: str, exchange: str) -> pd.DataFrame | None:
    """安全获取融资融券明细，处理 AkShare 空数据返回时的 pandas Length mismatch 异常。"""
    try:
        df = fetcher(date=date)
        if df is None or df.empty:
            return None
        return df
    except ValueError as e:
        if "Length mismatch" in str(e) or "Expected axis" in str(e):
            logger.warning(f"⚠️  {exchange.upper()} 融资融券 {date} 返回空数据")
            return None
        raise


def update_margin_trading(db: DatabaseInterface) -> dict:
    """批量获取昨日全市场融资融券数据并保存。"""
    logger.info("\n" + "=" * 60)
    logger.info("📈 任务: 批量获取融资融券")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "total": 0, "error": "akshare not installed"}

    target_date = _get_expected_latest_trading_day().replace("-", "")
    previous_date = (datetime.strptime(target_date, "%Y%m%d") - timedelta(days=1)).strftime("%Y%m%d")
    target_dates = [target_date, previous_date]
    batch_records = []
    total = 0

    for exchange, fetcher in [("sh", ak.stock_margin_detail_sse), ("sz", ak.stock_margin_detail_szse)]:
        df: pd.DataFrame | None = None
        actual_date = target_date
        for date in target_dates:
            try:
                df = _safe_fetch_margin_detail(fetcher, date, exchange)
                if df is not None:
                    actual_date = date
                    logger.info(f"  {exchange.upper()} 融资融券使用日期 {date}")
                    break
            except Exception as e:
                logger.warning(f"⚠️  {exchange.upper()} 融资融券 {date} 获取失败: {e}")
                df = None

        if df is None:
            logger.warning(f"⚠️  {exchange.upper()} 融资融券无数据")
            continue

        total += len(df)
        for _, row in df.iterrows():
            try:
                code = str(row.get("标的证券代码" if exchange == "sh" else "证券代码", "")).strip()
                if not code:
                    continue
                batch_records.append({
                    "ts_code": code,
                    "trade_date": actual_date,
                    "margin_balance": row.get("融资余额" if exchange == "sh" else "融资余额"),
                    "margin_buy": row.get("融资买入额" if exchange == "sh" else "融资买入额"),
                    "margin_repay": row.get("融资偿还额" if exchange == "sh" else None),
                    "short_balance": row.get("融券余量" if exchange == "sh" else "融券余量"),
                    "short_sell": row.get("融券卖出量" if exchange == "sh" else "融券卖出量"),
                    "short_repay": row.get("融券偿还量" if exchange == "sh" else None),
                    "total_balance": row.get("融资融券余额"),
                    "data_source": "akshare",
                })
            except Exception:
                continue

    saved = db.save_margin_trading_batch(batch_records) if batch_records else 0
    logger.info(f"✅ 融资融券保存完成: {saved}/{total}")
    return {"saved": saved, "total": total}


# ===========================================================================
# 任务 6: 批量获取龙虎榜数据
# ===========================================================================


def update_dragon_tiger(db: DatabaseInterface) -> dict:
    """批量获取昨日龙虎榜数据并保存。"""
    logger.info("\n" + "=" * 60)
    logger.info("🐉 任务: 批量获取龙虎榜")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "total": 0, "error": "akshare not installed"}

    target_date = _get_expected_latest_trading_day().replace("-", "")
    try:
        df = ak.stock_lhb_detail_em(start_date=target_date, end_date=target_date)
        if df is None or df.empty:
            logger.warning("⚠️  龙虎榜无数据")
            return {"saved": 0, "total": 0}

        batch_records = []
        for _, row in df.iterrows():
            try:
                code = str(row.get("代码", "")).strip()
                if not code:
                    continue
                batch_records.append({
                    "ts_code": code,
                    "trade_date": target_date,
                    "close_price": row.get("收盘价"),
                    "pct_change": row.get("涨跌幅"),
                    "net_buy_amount": row.get("龙虎榜净买额"),
                    "buy_amount": row.get("龙虎榜买入额"),
                    "sell_amount": row.get("龙虎榜卖出额"),
                    "turnover_rate": row.get("换手率"),
                    "market_cap": row.get("流通市值"),
                    "reason": row.get("上榜原因", ""),
                    "data_source": "akshare",
                })
            except Exception:
                continue

        saved = db.save_dragon_tiger_batch(batch_records) if batch_records else 0
        logger.info(f"✅ 龙虎榜保存完成: {saved}/{len(df)}")
        return {"saved": saved, "total": len(df)}
    except Exception as e:
        logger.error(f"❌ 龙虎榜获取失败: {e}")
        return {"saved": 0, "total": 0, "error": str(e)}


# ===========================================================================
# 任务 7: 批量获取大宗交易数据
# ===========================================================================


def update_block_trade(db: DatabaseInterface) -> dict:
    """批量获取昨日大宗交易数据并保存。"""
    logger.info("\n" + "=" * 60)
    logger.info("📦 任务: 批量获取大宗交易")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "total": 0, "error": "akshare not installed"}

    target_date = _get_expected_latest_trading_day().replace("-", "")
    try:
        df = ak.stock_dzjy_mrmx(symbol="A股", start_date=target_date, end_date=target_date)
        if df is None or df.empty:
            logger.warning("⚠️  大宗交易无数据")
            return {"saved": 0, "total": 0}

        batch_records = []
        for _, row in df.iterrows():
            try:
                code = str(row.get("证券代码", "")).strip()
                if not code:
                    continue
                batch_records.append({
                    "ts_code": code,
                    "trade_date": target_date,
                    "deal_price": row.get("成交价"),
                    "close_price": row.get("收盘价"),
                    "discount_rate": row.get("折溢率"),
                    "volume": row.get("成交量"),
                    "amount": row.get("成交额"),
                    "buyer_branch": row.get("买方营业部", ""),
                    "seller_branch": row.get("卖方营业部", ""),
                    "data_source": "akshare",
                })
            except Exception:
                continue

        saved = db.save_block_trade_batch(batch_records) if batch_records else 0
        logger.info(f"✅ 大宗交易保存完成: {saved}/{len(df)}")
        return {"saved": saved, "total": len(df)}
    except Exception as e:
        logger.error(f"❌ 大宗交易获取失败: {e}")
        return {"saved": 0, "total": 0, "error": str(e)}


# ===========================================================================
# 任务 8: 批量获取板块资金流向
# ===========================================================================


def _fetch_sector_fund_flow(trade_date: str) -> pd.DataFrame | None:
    """获取行业资金流向排名（同花顺源）。

    注意：同花顺仅提供"即时"快照，无法回填历史数据。
    此函数只用于每日积累，板块资金缺乏免费历史 API。
    """
    df = ak.stock_fund_flow_industry()
    if df is None or df.empty:
        return None
    records = []
    for _, row in df.iterrows():
        sector = str(row.get("行业", "")).strip()
        if not sector:
            continue
        records.append({
            "sector_name": sector,
            "trade_date": trade_date,
            "main_net_inflow": row.get("净额"),
            "main_net_inflow_pct": row.get("行业-涨跌幅"),
            "super_large_net_inflow": None,
            "large_net_inflow": row.get("流入资金"),
            "medium_net_inflow": row.get("流出资金"),
            "small_net_inflow": None,
            "data_source": "ths",
        })
    return pd.DataFrame(records)


def update_sector_fund_flow(db: DatabaseInterface) -> dict:
    """批量获取板块资金流向并保存（同花顺源，仅今日快照，每日积累）。"""
    logger.info("\n" + "=" * 60)
    logger.info("🏭 任务: 批量获取板块资金流向")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "total": 0, "error": "akshare not installed"}

    target_date = _get_expected_latest_trading_day()

    try:
        df = _fetch_sector_fund_flow(target_date)
    except Exception as e:
        logger.error(f"❌ 板块资金流向获取失败: {e}")
        return {"saved": 0, "total": 0, "error": str(e)}
    if df is None or df.empty:
        logger.warning("⚠️  板块资金流向无数据")
        return {"saved": 0, "total": 0}

    batch_records = []
    for _, row in df.iterrows():
        try:
            record = row.to_dict()
            record["sector_name"] = row["sector_name"]
            record["trade_date"] = target_date
            record["data_source"] = "ths"
            batch_records.append(record)
        except Exception:
            continue

    saved = db.save_sector_fund_flow_batch(batch_records) if batch_records else 0
    logger.info(f"✅ 板块资金流向保存完成: {saved}/{len(df)}")
    return {"saved": saved, "total": len(df), "source": "ths"}
