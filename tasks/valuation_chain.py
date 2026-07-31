"""
估值相关数据更新任务
────────────────────
包含：估值数据获取、实时行情快照、历史估值快照、行业对比数据。
"""

from __future__ import annotations

import json  # noqa: F401
import logging
import os
import time
from datetime import timedelta

import pandas as pd  # noqa: F401
from smartmoney_hunter.market_utils import is_beijing_stock

from core.calendar import get_expected_latest_trading_day
from core.config import SHARED_DATA_DIR  # noqa: F401
from core.market_time import (
    PHASE_POST_CLOSE,
    has_post_close_completion,
    market_phase,
    shanghai_now,
)
from core.source_client import get_default_client
from core.utils import infer_market, should_skip_beijing  # noqa: F401
from interface import DatabaseInterface, DataLoaderInterface

try:
    import akshare as ak  # noqa: F401
except ImportError:
    ak = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)

# 当 fundamentals 表某日记录数达到该阈值时，视为已完成并跳过
MIN_FUNDAMENTALS_STOCK_COUNT = 5000


# ===========================================================================
# 任务 3.4: 批量获取全市场估值数据（PE/PB/PS/PEG）
# ===========================================================================


def update_fundamentals(
    db: DatabaseInterface, loader: DataLoaderInterface, symbols: list[str] | None = None
) -> dict:
    """批量获取全市场（或指定股票）估值数据并保存到 fundamentals 表。"""
    logger.info("\n" + "=" * 60)
    logger.info("📊 任务: 批量获取估值数据")
    logger.info("=" * 60)

    sh_now = shanghai_now()
    today = sh_now.strftime("%Y-%m-%d")
    post_close = market_phase(sh_now) == PHASE_POST_CLOSE
    # 收盘定型后才请求今天：盘中东财返回的是实时估值快照，
    # 落库后会被"当日已存在"守卫冻结（2026-07-30 事故）
    trade_dates = [today] if post_close else []
    for offset in range(1, 5):
        d = sh_now - timedelta(days=offset)
        if d.weekday() < 5:
            trade_dates.append(d.strftime("%Y-%m-%d"))
    if not post_close:
        logger.info("  非收盘定型时段（上海 16:00 前），仅回补历史交易日估值")

    existing_count = db.count_fundamentals_for_date(today)
    if not isinstance(existing_count, int):
        logger.warning("count_fundamentals_for_date 返回非 int (%s)，视为 0", type(existing_count).__name__)
        existing_count = 0
    if existing_count >= MIN_FUNDAMENTALS_STOCK_COUNT and has_post_close_completion(
        str(db.db_path), "update_fundamentals", today
    ):
        # 行数达标 + 收盘后曾成功运行过，才视为当日完成；
        # 只有盘中写入的行数达标不算数，重抓覆盖为收盘终值
        logger.info(f"  跳过：today ({today}) 已有 {existing_count} 只估值数据（收盘后已完成）")
        return {
            "saved": 0,
            "total": 0,
            "skipped": True,
            "reason": f"today ({today}) already has {existing_count} fundamentals rows (post-close run recorded)",
            "data_date": today,
        }

    all_records = []
    session = get_default_client().get_session("eastmoney")
    for td in trade_dates:
        if all_records:
            break
        page = 1
        page_size = 500
        while True:
            try:
                url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
                params = {
                    "sortColumns": "TRADE_DATE,SECURITY_CODE",
                    "sortTypes": "-1,1",
                    "pageSize": str(page_size),
                    "pageNumber": str(page),
                    "reportName": "RPT_VALUEANALYSIS_DET",
                    "columns": "SECURITY_CODE,SECURITY_NAME_ABBR,TRADE_DATE,CLOSE_PRICE,TOTAL_MARKET_CAP,PE_TTM,PB_MRQ,PE_LAR,PEG_CAR,PS_TTM",
                    "source": "WEB",
                    "client": "WEB",
                    "filter": f"(TRADE_DATE='{td}')",
                }
                resp = session.get(url, params=params, timeout=15)
                data = resp.json()
                if data.get("success") and data.get("result") and data["result"].get("data"):
                    records = data["result"]["data"]
                    all_records.extend(records)
                    total_count = data["result"].get("count", 0)
                    if page * page_size >= total_count:
                        break
                    page += 1
                else:
                    break
            except Exception as e:
                logger.warning(f"⚠️  获取估值数据失败 (日期={td}): {e}")
                break

    if not all_records:
        logger.warning("⚠️  未获取到估值数据")
        return {"saved": 0, "total": 0}

    batch_records = []
    for rec in all_records:
        try:
            code = str(rec.get("SECURITY_CODE", "")).strip()
            if not code:
                continue
            trade_date = str(rec.get("TRADE_DATE", today))[:10]
            batch_records.append({
                "ts_code": code,
                "trade_date": trade_date,
                "pe_ttm": rec.get("PE_TTM"),
                "pb": rec.get("PB_MRQ"),
                "ps_ttm": rec.get("PS_TTM"),
                "dividend_yield": None,
                "roe": None,
                "roa": None,
                "gross_margin": None,
                "net_margin": None,
                "debt_ratio": None,
                "revenue_growth": None,
                "profit_growth": None,
                "eps_growth": None,
                "peg": rec.get("PEG_CAR"),
                "market_cap": rec.get("TOTAL_MARKET_CAP"),
            })
        except Exception as e:
            logger.debug(f"  保存估值失败: {e}")
            continue

    if symbols:
        symbol_set = set(symbols)
        before = len(batch_records)
        batch_records = [r for r in batch_records if r["ts_code"] in symbol_set]
        logger.info(f"  --symbols 过滤：{len(batch_records)}/{before} 只")

    try:
        saved = db.save_fundamentals_batch(batch_records) if batch_records else 0
    except Exception as e:
        logger.error(f"❌ 估值数据批量保存失败: {e}")
        saved = 0
    date_used = str(all_records[0].get("TRADE_DATE", ""))[:10] if all_records else today
    if saved > 0 and date_used:
        db.record_task_run("update_fundamentals", date_used)
    logger.info(f"✅ 估值数据保存完成: {saved}/{len(all_records)} 只 (日期: {date_used})")
    return {"saved": saved, "total": len(all_records)}


# ===========================================================================
# 收盘刷新 helper（Task 6）：只抓取/归一化，不写库
# ===========================================================================


def fetch_fundamentals_snapshot(
    target_date: str,
    *,
    session=None,
    page_size: int = 500,
) -> list[dict]:
    """收盘刷新专用：只抓目标日估值快照并归一化为写库记录形状。

    与 update_fundamentals 不同：无 5000 行完成阈值、无日期回退、不写库；
    源端异常直接上抛（保留旧数据的语义由适配器/编排器落实）。
    """
    if session is None:
        session = get_default_client().get_session("eastmoney")

    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    raw_records: list[dict] = []
    page = 1
    while True:
        params = {
            "sortColumns": "TRADE_DATE,SECURITY_CODE",
            "sortTypes": "-1,1",
            "pageSize": str(page_size),
            "pageNumber": str(page),
            "reportName": "RPT_VALUEANALYSIS_DET",
            "columns": "SECURITY_CODE,SECURITY_NAME_ABBR,TRADE_DATE,CLOSE_PRICE,TOTAL_MARKET_CAP,PE_TTM,PB_MRQ,PE_LAR,PEG_CAR,PS_TTM",
            "source": "WEB",
            "client": "WEB",
            "filter": f"(TRADE_DATE='{target_date}')",
        }
        resp = session.get(url, params=params, timeout=15)
        data = resp.json()
        if not (data.get("success") and data.get("result") and data["result"].get("data")):
            break
        raw_records.extend(data["result"]["data"])
        total_count = data["result"].get("count", 0)
        if page * page_size >= total_count:
            break
        page += 1

    records: list[dict] = []
    for rec in raw_records:
        code = str(rec.get("SECURITY_CODE", "")).strip()
        if not code:
            continue
        records.append({
            "ts_code": code,
            "trade_date": str(rec.get("TRADE_DATE", target_date))[:10],
            "pe_ttm": rec.get("PE_TTM"),
            "pb": rec.get("PB_MRQ"),
            "ps_ttm": rec.get("PS_TTM"),
            "dividend_yield": None,
            "roe": None,
            "roa": None,
            "gross_margin": None,
            "net_margin": None,
            "debt_ratio": None,
            "revenue_growth": None,
            "profit_growth": None,
            "eps_growth": None,
            "peg": rec.get("PEG_CAR"),
            "market_cap": rec.get("TOTAL_MARKET_CAP"),
        })
    return records


def fetch_market_snapshot_quotes(
    codes: list[str],
    *,
    batch_size: int = 50,
    sleep_seconds: float = 0.05,
) -> list[dict]:
    """收盘刷新专用：批量拉取雪球行情报价，不写库。

    无 Token 直接抛错（源端不可用 → 保留旧数据）；单批失败静默降级，
    覆盖率是否达标由适配器把关。北交所代码在此过滤（雪球不支持）。
    """
    from smartmoney_hunter import xueqiu as xq

    if xq._get_token() is None:
        raise RuntimeError("XUEQIU_TOKEN is not configured")

    eligible = [code for code in codes if not is_beijing_stock(code)]
    quotes: list[dict] = []
    for i in range(0, len(eligible), batch_size):
        chunk = eligible[i : i + batch_size]
        try:
            quotes.extend(xq.get_batch_quotes(chunk))
        except Exception as e:
            logger.debug(f"  收盘刷新批次 {i // batch_size + 1} 失败: {e}")
        time.sleep(sleep_seconds)
    return quotes


# ===========================================================================
# 任务 3.5: 雪球 token 落地 — 批量补充实时行情指标
# ===========================================================================


def update_market_snapshot(db: DatabaseInterface) -> dict:
    """
    通过雪球 batch/quote API 批量获取全市场实时行情指标，
    补充 fundamentals 表的 dividend_yield 字段。

    Token 从环境变量 / smartmoney_hunter/.env 读取（XUEQIU_TOKEN / XUEQIU_USER_ID）。
    """
    logger.info("\n" + "=" * 60)
    logger.info("📊 任务: 雪球行情快照 (dividend_yield 补充)")
    logger.info("=" * 60)

    import sqlite3

    from smartmoney_hunter import xueqiu as xq

    sh_now = shanghai_now()
    today = sh_now.strftime("%Y-%m-%d")

    # 1. 查找 fundamentals 表中最新的交易日，确保在正确的日期上更新股息率
    conn = sqlite3.connect(str(db.db_path))
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(trade_date) FROM fundamentals")
    row = cursor.fetchone()
    conn.close()
    target_date = row[0] if (row and row[0]) else today

    # 盘中防线：target 为今日且未收盘定型时，雪球实时价算出的股息率
    # 是盘中快照，落库后会被下方守卫冻结——直接不补充（2026-07-30 事故）
    if target_date == today and market_phase(sh_now) != PHASE_POST_CLOSE:
        logger.info("  非收盘定型时段（上海 16:00 前），跳过今日 dividend_yield 补充")
        return {
            "status": "no_data",
            "saved": 0,
            "reason": "intraday: dividend_yield backfill deferred until post-close",
        }

    # 增量检测：以数据实态为准 —— target_date 当天 dividend_yield 实际非空行数
    # 达到阈值才跳过。仅凭 task_runs 判断会在数据事后被覆盖为 NULL 时永久漏补
    # （2026-07-24 全表 NULL 事故根因之一）。
    conn = sqlite3.connect(str(db.db_path))
    try:
        total_rows, filled = conn.execute(
            "SELECT COUNT(*),"
            " SUM(CASE WHEN dividend_yield IS NOT NULL THEN 1 ELSE 0 END)"
            " FROM fundamentals WHERE trade_date = ?",
            (target_date,),
        ).fetchone()
        filled = filled or 0
        # 非空率 >= 40% 视为已补充（雪球对全市场的覆盖率约 60-70%）
        min_filled = max(1, int((total_rows or 0) * 0.4))
    except sqlite3.OperationalError:
        # 旧库/测试 fixture 可能缺 dividend_yield 列，退回旧行为（视为已补充）
        filled = min_filled = 0
    finally:
        conn.close()
    last_run = db.get_last_task_run("update_market_snapshot")
    # target 为今日时，还要求收盘后曾成功运行过——盘中补充的实时股息率不算完成
    post_close_done = target_date != today or has_post_close_completion(
        str(db.db_path), "update_market_snapshot", target_date
    )
    if last_run == target_date and filled >= min_filled and post_close_done:
        logger.info(
            f"  跳过：target_date={target_date} 的 dividend_yield 已补充过 ({filled} 行非空)"
        )
        return {"status": "success", "saved": 0, "total": 0, "updated": 0, "skipped": True}
    if last_run == target_date and filled >= min_filled:
        logger.info(
            f"  target_date={target_date} 行数达标但无收盘后完成记录，重新补充为收盘终值"
        )
    elif last_run == target_date:
        logger.warning(
            f"  ⚠️ target_date={target_date} 曾标记完成，但 dividend_yield 非空仅 {filled} 行"
            f" (< {min_filled})，重新补充"
        )

    # 1. 读取全量股票
    conn = sqlite3.connect(str(db.db_path))
    cursor = conn.cursor()
    cursor.execute("SELECT code, market FROM stock_list")
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        logger.warning("⚠️  股票列表为空")
        return {"saved": 0, "total": 0}

    rows = [(c, m) for c, m in rows if not should_skip_beijing(c)]

    # 雪球不支持北交所，即使 INCLUDE_BJ=1 也要过滤掉
    xq_rows = [(c, m) for c, m in rows if not is_beijing_stock(c)]
    if len(xq_rows) < len(rows):
        logger.info(f"  过滤北交所: {len(rows)} → {len(xq_rows)} (雪球不支持北交所)")

    if xq._get_token() is None:
        logger.warning("⚠️  XUEQIU_TOKEN 未设置，跳过雪球行情快照")
        return {
            "saved": 0,
            "total": 0,
            "skipped": True,
            "reason": "XUEQIU_TOKEN not configured",
        }

    total = len(xq_rows)
    include_bj = os.getenv("INCLUDE_BJ", "0").lower() in ("1", "true", "yes")
    if include_bj:
        logger.info(f"📊 共 {total} 只股票（排除北交所），准备拉取雪球行情")
    else:
        logger.info(f"📊 共 {total} 只股票，准备拉取雪球行情")

    # 2. 分批调用（每批 50 只，串行 + 小延迟避免风控）
    batch = 50
    codes = [c for c, _ in xq_rows]
    all_quotes: list[dict] = []

    for i in range(0, total, batch):
        chunk = codes[i : i + batch]
        try:
            quotes = xq.get_batch_quotes(chunk)
            all_quotes.extend(quotes)
        except Exception as e:
            logger.debug(f"  批次 {i//batch + 1} 失败: {e}")
        if (i // batch + 1) % 20 == 0 or i + batch >= total:
            logger.info(f"  批次进度: {min(i + batch, total)}/{total} (已获取 {len(all_quotes)} 只)")
        time.sleep(0.05)

    logger.info(f"📊 雪球行情获取完成: {len(all_quotes)} 只")

    # 3. 写回数据库 — 只补充 dividend_yield (不覆盖现有 pe_ttm/pb/market_cap)
    updated = 0
    if all_quotes:
        conn = sqlite3.connect(str(db.db_path))
        cursor = conn.cursor()
        for q in all_quotes:
            div_yield = q.get("dividend_yield")
            if div_yield is not None:
                cursor.execute(
                    """UPDATE fundamentals SET dividend_yield = ?
                       WHERE ts_code = ? AND trade_date = ?
                       AND (dividend_yield IS NULL OR dividend_yield = 0)""",
                    (div_yield, q["code"], target_date),
                )
                if cursor.rowcount:
                    updated += 1
        conn.commit()
        conn.close()
        logger.info(f"✅ dividend_yield 补充完成: {updated} 只")
        db.record_task_run("update_market_snapshot", target_date)
    else:
        logger.warning("⚠️  雪球行情未获取到数据")

    return {"status": "success", "saved": len(all_quotes), "total": total, "updated": updated}


# ===========================================================================
# 任务 8.5: 历史估值快照
# ===========================================================================


def update_historical_valuation(db: DatabaseInterface, symbols: list[str] | None = None) -> dict:
    """把最新 fundamentals 估值数据快照写入 historical_valuation，用于分位数计算。"""
    if symbols:
        logger.info(f"  --symbols 过滤：{len(symbols)} 只")
    logger.info("\n" + "=" * 60)
    logger.info("📈 任务: 保存历史估值快照")
    logger.info("=" * 60)

    try:
        df = db.get_fundamentals_batch()
        if df.empty:
            logger.warning("⚠️  fundamentals 为空，跳过历史估值快照")
            return {"saved": 0, "total": 0}

        if symbols:
            symbol_set = set(symbols)
            before = len(df)
            df = df[df["ts_code"].isin(symbol_set)]
            logger.info(f"  --symbols 过滤：{len(df)}/{before} 只")

        before_dedup = len(df)
        df = df.drop_duplicates(subset=["ts_code", "trade_date"], keep="last")
        if len(df) < before_dedup:
            logger.info(f"  去重：移除 {before_dedup - len(df)} 条重复估值快照")

        records = []
        for _, row in df.iterrows():
            try:
                symbol = row.get("ts_code")
                trade_date = row.get("trade_date")
                if not symbol or not trade_date:
                    continue
                records.append({
                    "ts_code": symbol,
                    "trade_date": str(trade_date)[:10],
                    "pe_ttm": row.get("pe_ttm"),
                    "pb": row.get("pb"),
                    "ps_ttm": row.get("ps_ttm"),
                    "dividend_yield": row.get("dividend_yield"),
                })
            except Exception:
                continue

        batch_saver = None
        if "save_historical_valuation_batch" in dir(db):
            batch_saver = getattr(db, "save_historical_valuation_batch", None)

        if callable(batch_saver):
            saved = batch_saver(records) if records else 0
        else:
            saved = 0
            for record in records:
                data = {
                    "pe_ttm": record.get("pe_ttm"),
                    "pb": record.get("pb"),
                    "ps_ttm": record.get("ps_ttm"),
                    "dividend_yield": record.get("dividend_yield"),
                }
                db.save_historical_valuation(record["ts_code"], record["trade_date"], data)
                saved += 1

        logger.info(f"✅ 历史估值快照保存完成: {saved}/{len(df)}")
        return {"saved": saved, "total": len(df)}
    except Exception as e:
        logger.error(f"❌ 历史估值快照失败: {e}")
        return {"saved": 0, "total": 0, "error": str(e)}


# ===========================================================================
# 任务 8.6: 生成行业对比数据
# ===========================================================================


def update_sector_industry(db: DatabaseInterface) -> dict:
    """基于 fundamentals + stock_list 生成行业聚合数据，写入 sector_industry。"""
    logger.info("\n" + "=" * 60)
    logger.info("🏭 任务: 生成行业对比数据")
    logger.info("=" * 60)

    try:
        import difflib
        import sqlite3

        import pandas as pd

        stocks = db.get_stock_list()
        fundamentals = db.get_fundamentals_batch()
        if stocks.empty or fundamentals.empty:
            logger.warning("⚠️  股票列表或基本面为空，跳过行业对比")
            return {"saved": 0, "total": 0}

        df = fundamentals.merge(stocks[["code", "industry"]], left_on="ts_code", right_on="code", how="left")
        df["industry"] = df["industry"].fillna("未知行业")

        numeric_cols = ["pe_ttm", "pb", "ps_ttm", "roe", "revenue_growth", "profit_growth", "market_cap"]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        grouped = df.groupby("industry").agg(
            avg_pe=("pe_ttm", "mean"),
            avg_pb=("pb", "mean"),
            avg_ps=("ps_ttm", "mean"),
            avg_roe=("roe", "mean"),
            avg_revenue_growth=("revenue_growth", "mean"),
            avg_profit_growth=("profit_growth", "mean"),
            total_market_cap=("market_cap", "sum"),
        ).reset_index()

        # 尝试补充资金流入排名（如果 sector_fund_flow 表已有数据）
        try:
            conn = sqlite3.connect(db.db_path)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT sector_name, main_net_inflow
                FROM sector_fund_flow
                WHERE trade_date = (SELECT MAX(trade_date) FROM sector_fund_flow)
            """)
            rows = cursor.fetchall()
            conn.close()
            if rows:
                df_flow = pd.DataFrame(rows, columns=["sector_name", "main_net_inflow"])
                df_flow["main_net_inflow"] = pd.to_numeric(df_flow["main_net_inflow"], errors="coerce")
                df_flow = df_flow.dropna(subset=["main_net_inflow"])
                df_flow = df_flow.sort_values("main_net_inflow", ascending=False).reset_index(drop=True)
                df_flow["fund_inflow_rank"] = df_flow.index + 1

                # 行业名称在 stock_list / sector_industry 与 sector_fund_flow 之间常不一致。
                # 先精确匹配，再走少量 hard-coded 映射，最后用模糊匹配兜底。
                flow_names = df_flow["sector_name"].tolist()
                hard_coded_map = {
                    "酿酒行业": "白酒",
                    "家电行业": "白色家电",
                    "食品饮料": "食品加工制造",
                    "化工": "化学制品",
                    "化工行业": "化学制品",
                    "基础化工": "化学制品",
                    "化纤行业": "化学纤维",
                    "化肥行业": "农化制品",
                    "医疗行业": "医疗器械",
                    "医药制造": "化学制药",
                    "医药生物": "化学制药",
                    "水泥建材": "建筑材料",
                    "玻璃陶瓷": "建筑材料",
                    "装修建材": "建筑材料",
                    "旅游酒店": "旅游及酒店",
                    "商业百货": "零售",
                    "贸易行业": "贸易",
                    "电子信息": "电子",
                    "电子元件": "元件",
                    "电子零部件制造": "元件",
                    "输配电气": "电网设备",
                    "电源设备": "电网设备",
                    "高低压设备": "电网设备",
                    "安防设备": "计算机设备",
                    "计算机应用": "软件开发",
                    "计算机": "计算机设备",
                    "软件服务": "软件开发",
                    "汽车行业": "汽车整车",
                    "汽车服务": "汽车服务及其他",
                    "房地产服务": "房地产",
                    "房地产开发": "房地产",
                    "石油行业": "石油加工贸易",
                    "石油石化": "石油加工贸易",
                    "煤炭行业": "煤炭开采加工",
                    "煤炭采选": "煤炭开采加工",
                    "钢铁行业": "钢铁",
                    "环保工程": "环境治理",
                    "环保行业": "环境治理",
                    "环保工程及服务": "环境治理",
                    "港口水运": "港口航运",
                    "航空机场": "机场航运",
                    "航天航空": "军工装备",
                    "国防军工": "军工装备",
                    "交运物流": "物流",
                    "物流行业": "物流",
                    "通讯行业": "通信服务",
                    "通信配套服务": "通信服务",
                    "电信运营": "通信服务",
                    "造纸印刷": "造纸",
                    "服装家纺": "服装家纺",
                    "纺织服装": "纺织制造",
                    "纺织服饰": "纺织制造",
                    "橡胶": "橡胶制品",
                    "塑胶制品": "塑料制品",
                    "包装材料": "包装印刷",
                    "金属制品": "通用设备",
                    "机械行业": "通用设备",
                    "机械设备": "通用设备",
                    "农林牧渔": "养殖业",
                    "农牧饲渔": "养殖业",
                    "农业综合": "种植业与林业",
                    "种子生产": "种植业与林业",
                    "农产品加工": "农产品加工",
                    "农药": "农化制品",
                    "农药兽药": "农化制品",
                    "小金属": "小金属",
                    "有色金属": "工业金属",
                    "贵金属": "贵金属",
                    "能源金属": "能源金属",
                    "铁路公路": "公路铁路运输",
                    "航运": "港口航运",
                    "航运港口": "港口航运",
                    "船舶制造": "船舶制造",
                    "电力行业": "电力",
                    "电力设备": "电网设备",
                    "公用事业": "电力",
                    "燃气": "燃气",
                    "保险": "保险",
                    "券商信托": "证券",
                    "银行": "银行",
                    "多元金融": "多元金融",
                    "综合行业": "综合",
                    "塑料制品": "塑料制品",
                    "光学元件": "光学光电子",
                    "光学光电子": "光学光电子",
                    "半导体": "半导体",
                    "集成电路": "半导体",
                    "光伏设备": "光伏设备",
                    "风电设备": "风电设备",
                    "电池": "电池",
                    "储能设备": "电池",
                    "电机": "电机",
                    "电网设备": "电网设备",
                    "电子化学品": "电子化学品",
                    "非金属材料Ⅱ": "非金属材料",
                    "美容护理": "美容护理",
                    "生物制品": "生物制品",
                    "中药": "中药",
                    "化学制药": "化学制药",
                    "医疗服务": "医疗服务",
                    "医疗器械": "医疗器械",
                    "医药商业": "医药商业",
                    "传媒": "文化传媒",
                    "教育": "教育",
                    "厨卫电器": "厨卫电器",
                    "小家电": "小家电",
                    "黑色家电": "黑色家电",
                    "家居用品": "家居用品",
                    "家具": "家居用品",
                    "家用轻工": "家居用品",
                    "饲料": "养殖业",
                    "食品加工制造": "食品加工制造",
                    "饮料制造": "饮料制造",
                }

                rank_map: dict[str, int | None] = {}
                for industry in grouped["industry"]:
                    target_name = industry
                    # 1) hard-coded alias
                    if industry in hard_coded_map:
                        target_name = hard_coded_map[industry]
                    # 2) exact match after alias
                    if target_name in flow_names:
                        rank_map[industry] = int(
                            df_flow.loc[df_flow["sector_name"] == target_name, "fund_inflow_rank"].iloc[0]
                        )
                        continue
                    # 3) fuzzy fallback
                    matches = difflib.get_close_matches(industry, flow_names, n=1, cutoff=0.5)
                    if matches:
                        rank_map[industry] = int(
                            df_flow.loc[df_flow["sector_name"] == matches[0], "fund_inflow_rank"].iloc[0]
                        )
                    else:
                        rank_map[industry] = None

                grouped["fund_inflow_rank"] = grouped["industry"].map(rank_map)
            else:
                grouped["fund_inflow_rank"] = None
        except Exception:
            grouped["fund_inflow_rank"] = None

        today = get_expected_latest_trading_day()
        saved = 0
        for _, row in grouped.iterrows():
            try:
                data = {
                    "industry_name": row["industry"],
                    "trade_date": today,
                    "avg_pe": row["avg_pe"],
                    "avg_pb": row["avg_pb"],
                    "avg_ps": row["avg_ps"],
                    "avg_roe": row["avg_roe"],
                    "avg_revenue_growth": row["avg_revenue_growth"],
                    "avg_profit_growth": row["avg_profit_growth"],
                    "total_market_cap": row["total_market_cap"],
                    "fund_inflow_rank": row.get("fund_inflow_rank"),
                    "data_source": "derived",
                }
                db.save_sector_industry(data)
                saved += 1
            except Exception:
                continue

        logger.info(f"✅ 行业对比数据保存完成: {saved}/{len(grouped)}")
        return {"saved": saved, "total": len(grouped)}
    except Exception as e:
        logger.error(f"❌ 行业对比数据生成失败: {e}")
        return {"saved": 0, "total": 0, "error": str(e)}


# ===========================================================================
# 收盘刷新 helpers（Task 7）：派生任务只读目标日分区，不写库
# ===========================================================================


def fetch_historical_valuation_rows_for_refresh(
    db_path: str, target_date: str
) -> list[dict]:
    """收盘刷新专用：读取 fundamentals 目标日分区为历史估值快照行。

    每行恰好 6 键（ts_code/trade_date/pe_ttm/pb/ps_ttm/dividend_yield），
    按 ts_code 去重；分区为空返回 []（由适配器决定保留旧快照）。
    """
    import sqlite3  # 局部导入：保持本文件其余部分零改动（append-only）

    conn = sqlite3.connect(db_path)
    try:
        fetched = conn.execute(
            "SELECT ts_code, pe_ttm, pb, ps_ttm, dividend_yield"
            " FROM fundamentals WHERE trade_date = ?",
            (target_date,),
        ).fetchall()
    finally:
        conn.close()

    rows: list[dict] = []
    seen: set[str] = set()
    for code, pe_ttm, pb, ps_ttm, dividend_yield in fetched:
        code = str(code).strip()
        if not code or code in seen:
            continue
        seen.add(code)
        rows.append({
            "ts_code": code,
            "trade_date": target_date,
            "pe_ttm": pe_ttm,
            "pb": pb,
            "ps_ttm": ps_ttm,
            "dividend_yield": dividend_yield,
        })
    return rows


def compute_sector_industry_rows_for_refresh(
    db_path: str, target_date: str
) -> list[dict]:
    """收盘刷新专用：由目标日 fundamentals + stock_list 聚合行业对比行。

    只聚合目标日分区；无行业归属的股票计入“未知行业”。刷新模式
    不做 sector_fund_flow 模糊映射：fund_inflow_rank 置空，
    data_source 固定为 "derived"；分区为空返回 []。
    """
    import sqlite3  # 局部导入：保持本文件其余部分零改动（append-only）

    conn = sqlite3.connect(db_path)
    try:
        fund = pd.read_sql_query(
            "SELECT ts_code, pe_ttm, pb, ps_ttm, roe, revenue_growth,"
            " profit_growth, market_cap FROM fundamentals WHERE trade_date = ?",
            conn,
            params=(target_date,),
        )
        stock = pd.read_sql_query("SELECT code, industry FROM stock_list", conn)
    finally:
        conn.close()

    if fund.empty:
        return []

    merged = fund.merge(stock, left_on="ts_code", right_on="code", how="left")
    merged["industry"] = merged["industry"].fillna("未知行业")
    merged.loc[
        merged["industry"].astype(str).str.strip() == "", "industry"
    ] = "未知行业"

    grouped = merged.groupby("industry").agg(
        avg_pe=("pe_ttm", "mean"),
        avg_pb=("pb", "mean"),
        avg_ps=("ps_ttm", "mean"),
        avg_roe=("roe", "mean"),
        avg_revenue_growth=("revenue_growth", "mean"),
        avg_profit_growth=("profit_growth", "mean"),
        total_market_cap=("market_cap", "sum"),
    ).reset_index()

    def _scalar_or_none(value: object) -> float | None:
        """NaN / None → None（SQLite 不接受 NaN）。"""
        return None if value is None or value != value else float(value)

    rows: list[dict] = []
    for _, row in grouped.iterrows():
        rows.append({
            "industry_name": str(row["industry"]),
            "trade_date": target_date,
            "avg_pe": _scalar_or_none(row["avg_pe"]),
            "avg_pb": _scalar_or_none(row["avg_pb"]),
            "avg_ps": _scalar_or_none(row["avg_ps"]),
            "avg_roe": _scalar_or_none(row["avg_roe"]),
            "avg_revenue_growth": _scalar_or_none(row["avg_revenue_growth"]),
            "avg_profit_growth": _scalar_or_none(row["avg_profit_growth"]),
            "total_market_cap": _scalar_or_none(row["total_market_cap"]),
            "fund_inflow_rank": None,
            "data_source": "derived",
        })
    return rows
