"""
市场资金流任务模块
───────────────────
资金流向、融资融券、龙虎榜、大宗交易、板块资金流向。
"""

from __future__ import annotations

import logging
import time  # noqa: F401
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from core.calendar import get_expected_latest_trading_day, get_recent_trading_days
from core.freshness import check_task_freshness
from core.source_record_key import block_trade_source_key, dragon_tiger_source_key
from interface import DatabaseInterface, DataLoaderInterface

try:
    import akshare as ak
except ImportError:
    ak = None

import pandas as pd

logger = logging.getLogger(__name__)


# ===========================================================================
# 任务 4: 批量获取资金流向
# ===========================================================================

# 覆盖度水位。生产库实测校准（2026-10-13 只读复核）：
#   健康运行 2026-07-07 ~ 09-18 稳定在 93.8% ~ 94.1%（缺口来自新股/停牌，
#   是数据源的结构性水位，不会更高）；2026-09-24 起塌到 66.9% ~ 67.0%
#   （东财榜单端点间歇失败 → 同花顺兜底只覆盖约 5200 只）。
# 取 90%：低于健康水位 4pp 不误报，高于塌陷水位 23pp 不漏报。
_FUND_FLOW_COVERAGE_FLOOR = 0.90


def _bare_code(value: object) -> str:
    """归一为裸 6 位代码。

    数据源的 code 列在不同源上形态不一致：东财/同花顺给裸 6 位，
    而任务测试与部分适配器给 '000001.SZ'。不归一会让交集恒为空，
    覆盖度守卫就会天天误报塌陷。
    """
    text = str(value).strip().split(".")[0]
    return text.zfill(6) if text.isdigit() else text


def _fund_flow_universe(db: DatabaseInterface) -> set[str]:
    """当天「按定义应该有资金流」的代码集：daily_bars 的代码集。

    它是本地唯一逐票齐全的表。取不到时返回空集表示「无法判定」，调用方据此
    跳过守卫 —— 守卫自己不能成为新的单点故障。
    """
    try:
        return {_bare_code(c) for c in db.get_distinct_codes("daily_bars", "ts_code")}
    except Exception as e:  # noqa: BLE001 — 守卫失败不得拖垮主任务
        logger.debug(f"  覆盖度守卫跳过：universe 查询失败 ({e})")
        return set()


def _fund_flow_coverage(universe: set[str], df: pd.DataFrame) -> tuple[int, int]:
    """返回 (covered, missing)。universe 为空时返回 (0, 0)。"""
    if not universe:
        return (0, 0)
    have = {_bare_code(c) for c in df["code"]} if "code" in df.columns else set()
    covered = len(have & universe)
    return (covered, len(universe) - covered)


def _rebuild_market_day(universe: set[str], trade_date: str) -> pd.DataFrame | None:
    """聚合源覆盖不足时，用东财逐票日 K 把整天重建一遍。

    **整日替换而不是合并**：合并会让同一天里一半是榜单口径、一半是兜底口径，
    而兜底口径（无分解、精度只到万）正是这次要消灭的东西。宁可全用同源数据。

    返回 None 表示重建不出可用数据 —— 调用方据此不写任何东西，而不是退回
    写口径不一致的兜底数据。
    """
    from core.fund_flow_source import fetch_market_day_records

    try:
        records, missing = fetch_market_day_records(universe, trade_date)
    except Exception as e:  # noqa: BLE001 — 重建失败不得让主任务崩掉
        logger.warning(f"⚠️ 东财逐票重建失败: {e}")
        return None
    if not records:
        logger.warning(f"⚠️ 东财逐票重建未取到任何 {trade_date} 数据（{len(missing)} 只全部失败）")
        return None
    df = pd.DataFrame(records)
    logger.info(f"✅ 东财逐票重建 {trade_date}: {len(df)} 只（未取到 {len(missing)} 只）")
    return df


def update_fund_flow(db: DatabaseInterface, loader: DataLoaderInterface, symbols: list[str] | None = None) -> dict:
    """批量获取全市场（或指定股票）资金流向并保存。"""
    logger.info("\n" + "=" * 60)
    logger.info("💰 任务: 批量获取资金流向")
    logger.info("=" * 60)

    today = get_expected_latest_trading_day()

    try:
        df = loader.get_market_fund_flow()
        if df.empty:
            logger.warning("⚠️  未获取到资金流向数据")
            verdict = check_task_freshness(df, date_field=None, expected=today)
            if verdict.is_stale:
                logger.warning(f"⚠️  资金流向源端空数据：{verdict.reason}")
                return {
                    "status": "retained",
                    "reason": verdict.reason,
                    "error_kind": "network",
                    "retained_old_data": True,
                    "total": 0,
                }
            return {"skipped": True, "reason": "no market fund flow data", "total": 0}

        # 覆盖度在写库前判定：它描述的是「源端给全了没有」，与写入无关。
        # --symbols 是子集运行，对全市场覆盖率没有意义，跳过（否则每次
        # 子集运行都会误报塌陷）。
        universe: set[str] = set()
        if not symbols:
            universe = _fund_flow_universe(db)
            covered, missing = _fund_flow_coverage(universe, df)
            if universe and covered / len(universe) < _FUND_FLOW_COVERAGE_FLOOR:
                logger.warning(
                    f"⚠️ 聚合源覆盖不足（{covered}/{len(universe)} = "
                    f"{covered / len(universe):.1%}），改用东财逐票重建 {today}"
                )
                rebuilt = _rebuild_market_day(universe, today)
                if rebuilt is not None:
                    df = rebuilt
                    covered, missing = _fund_flow_coverage(universe, df)
                else:
                    logger.error(
                        "❌ 东财逐票重建失败：不写入任何资金流数据"
                        "（保留旧数据，不写入口径不一致的兜底数据）"
                    )
                    return {
                        "status": "retained",
                        "reason": (
                            f"聚合源覆盖 {covered}/{len(universe)} 且东财逐票重建失败"
                        ),
                        "error_kind": "network",
                        "retained_old_data": True,
                        "total": 0,
                    }

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

        if symbols:
            symbol_set = set(symbols)
            before = len(batch_records)
            batch_records = [r for r in batch_records if r["symbol"] in symbol_set]
            logger.info(f"  --symbols 过滤：{len(batch_records)}/{before} 只")

        saved = db.save_fund_flow_batch(batch_records) if batch_records else 0
        logger.info(f"✅ 资金流向保存完成: {saved}/{len(df)} 只")

        result: dict = {"saved": saved, "total": len(df)}
        # 逐票重建之后覆盖度通常回到 ~100%，守卫自然不再触发；只有重建也
        # 没能补齐时才降级上报 —— 否则「数据没更新」会以 success 的面目进
        # 审计，连续多日也无人发现（与 P2-12 的 market_snapshot 同型）。
        # degraded 而非 failed：failed 在 to_dict 里丢 saved，审计会把一次
        # 几千行的写入误读成「一行都没写」。
        if universe and covered / len(universe) < _FUND_FLOW_COVERAGE_FLOOR:
            pct = covered / len(universe)
            result.update({
                "status": "degraded",
                "error_kind": "data_quality",
                "error": (
                    f"资金流覆盖度 {pct:.1%} 低于水位 {_FUND_FLOW_COVERAGE_FLOOR:.0%}："
                    f"{len(universe)} 只应有，仅 {covered} 只拿到，缺 {missing} 只"
                    "（东财逐票重建后仍不足，多为停牌/退市或源端无数据）"
                ),
                "metadata": {
                    "universe_size": len(universe),
                    "covered": covered,
                    "missing": missing,
                    "coverage_pct": round(pct, 4),
                },
            })
            logger.error(
                f"❌ 资金流覆盖度仍不足: {covered}/{len(universe)} ({pct:.1%}) "
                f"< {_FUND_FLOW_COVERAGE_FLOOR:.0%}，缺 {missing} 只"
            )
        return result

    except Exception as e:
        logger.error(f"❌ 资金流向获取失败: {e}")
        return {"saved": 0, "total": 0, "error": str(e), "error_kind": "network"}


# ===========================================================================
# 收盘刷新 helper（Task 6）：只抓取/归一化，不写库
# ===========================================================================

_FUND_FLOW_NUMERIC_FIELDS = (
    "main_net_inflow",
    "main_net_inflow_pct",
    "super_large_net_inflow",
    "super_large_net_inflow_pct",
    "large_net_inflow",
    "large_net_inflow_pct",
)


def fetch_fund_flow_records(loader: DataLoaderInterface, trade_date: str) -> list[dict]:
    """收盘刷新专用：抓取全市场资金流并归一化为 legacy 记录形状，不写库。

    跳过空 code 与六个数值字段全空的行；部分 NaN 转为 None；
    loader 异常直接上抛（保留旧数据的语义由适配器/编排器落实）。
    """
    df = loader.get_market_fund_flow()
    if df is None or df.empty:
        return []

    records: list[dict] = []
    for _, row in df.iterrows():
        code = str(row.get("code", "")).strip()
        if not code:
            continue
        values = {
            field: (None if pd.isna(row.get(field)) else float(row.get(field)))
            for field in _FUND_FLOW_NUMERIC_FIELDS
        }
        if all(value is None for value in values.values()):
            continue
        records.append({"symbol": code, "date": trade_date, **values, "simulated": False})
    return records


# ===========================================================================
# 任务 5: 批量获取融资融券数据
# ===========================================================================


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


def update_margin_trading(db: DatabaseInterface, symbols: list[str] | None = None) -> dict:
    """批量获取昨日全市场（或指定股票）融资融券数据并保存。"""
    if symbols:
        logger.info(f"  --symbols 过滤：{len(symbols)} 只")
    logger.info("\n" + "=" * 60)
    logger.info("📈 任务: 批量获取融资融券")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "total": 0, "error": "akshare not installed"}

    target_date = get_expected_latest_trading_day()
    target_dates = [date.replace("-", "") for date in get_recent_trading_days(target_date, 3)]
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
                    "trade_date": datetime.strptime(actual_date, "%Y%m%d").strftime("%Y-%m-%d"),
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

    if symbols:
        symbol_set = set(symbols)
        before = len(batch_records)
        batch_records = [r for r in batch_records if r["ts_code"] in symbol_set]
        logger.info(f"  --symbols 过滤：{len(batch_records)}/{before} 只")

    saved = db.save_margin_trading_batch(batch_records) if batch_records else 0
    logger.info(f"✅ 融资融券保存完成: {saved}/{total}")
    return {"saved": saved, "total": total}


# ===========================================================================
# 任务 6: 批量获取龙虎榜数据
# ===========================================================================


def update_dragon_tiger(db: DatabaseInterface, symbols: list[str] | None = None) -> dict:
    """批量获取昨日龙虎榜数据并保存。"""
    if symbols:
        logger.info(f"  --symbols 过滤：{len(symbols)} 只")
    logger.info("\n" + "=" * 60)
    logger.info("🐉 任务: 批量获取龙虎榜")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "total": 0, "error": "akshare not installed"}

    target_date = get_expected_latest_trading_day()
    try:
        # 超时护栏：akshare 底层 requests 无可靠超时，2026-09-15 源端挂起
        # 16.8 分钟才断连。超时的 future 无法强杀，shutdown(wait=False) 放弃它
        # （泄漏一个线程，对批处理进程无害），配合 runner 网络重试快速恢复
        compact = target_date.replace("-", "")
        pool = ThreadPoolExecutor(max_workers=1)
        try:
            df = pool.submit(
                ak.stock_lhb_detail_em, start_date=compact, end_date=compact
            ).result(timeout=180)
        finally:
            pool.shutdown(wait=False)
        if df is None or df.empty:
            logger.warning("⚠️  龙虎榜无数据")
            # 显式 skipped：当日无龙虎榜个股属正常结果，避免被结果契约误判为 failed
            return {"skipped": True, "reason": "no dragon-tiger list for the day", "total": 0}

        batch_records = []
        for _, row in df.iterrows():
            try:
                code = str(row.get("代码", "")).strip()
                if not code:
                    continue
                record = {
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
                }
                # 稳定事件键：同股同日不同上榜原因的合法多事件互异
                record["source_record_key"] = dragon_tiger_source_key(record)
                batch_records.append(record)
            except Exception:
                continue

        if symbols:
            symbol_set = set(symbols)
            before = len(batch_records)
            batch_records = [r for r in batch_records if r["ts_code"] in symbol_set]
            logger.info(f"  --symbols 过滤：{len(batch_records)}/{before} 只")

        saved = db.save_dragon_tiger_batch(batch_records) if batch_records else 0
        logger.info(f"✅ 龙虎榜保存完成: {saved}/{len(df)}")
        return {"saved": saved, "total": len(df)}
    except Exception as e:
        logger.error(f"❌ 龙虎榜获取失败: {e}")
        # error_kind=network：本任务的失败模式即源端网络问题（含上方超时护栏
        # 抛出的 TimeoutError），标记后 runner 会自动重试一次
        return {"saved": 0, "total": 0, "error": str(e), "error_kind": "network"}


# ===========================================================================
# 任务 7: 批量获取大宗交易数据
# ===========================================================================


def update_block_trade(
    db: DatabaseInterface,
    symbols: list[str] | None = None,
    target_date: str | None = None,
) -> dict:
    """批量获取昨日大宗交易数据并保存。

    ``target_date`` 给定时改写该历史交易日（整日缺席回补入口）；缺省仍取
    ``get_expected_latest_trading_day()``。源端 ``stock_dzjy_mrmx`` 本就按
    ``start_date``/``end_date`` 取数，因此回补无需额外适配。
    """
    if symbols:
        logger.info(f"  --symbols 过滤：{len(symbols)} 只")
    logger.info("\n" + "=" * 60)
    logger.info("📦 任务: 批量获取大宗交易")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "total": 0, "error": "akshare not installed"}

    target_date = target_date or get_expected_latest_trading_day()
    try:
        df = ak.stock_dzjy_mrmx(symbol="A股", start_date=target_date.replace("-", ""), end_date=target_date.replace("-", ""))
        if df is None or df.empty:
            logger.warning("⚠️  大宗交易无数据")
            # 显式 skipped：当日无大宗交易属正常结果，避免被结果契约误判为 failed
            return {"skipped": True, "reason": "no block trade for the day", "total": 0}

        batch_records = []
        for _, row in df.iterrows():
            try:
                code = str(row.get("证券代码", "")).strip()
                if not code:
                    continue
                record = {
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
                }
                # 稳定事件键：同股同日不同价/量的合法多笔交易互异
                record["source_record_key"] = block_trade_source_key(record)
                batch_records.append(record)
            except Exception:
                continue

        if symbols:
            symbol_set = set(symbols)
            before = len(batch_records)
            batch_records = [r for r in batch_records if r["ts_code"] in symbol_set]
            logger.info(f"  --symbols 过滤：{len(batch_records)}/{before} 只")

        saved = db.save_block_trade_batch(batch_records) if batch_records else 0
        logger.info(f"✅ 大宗交易保存完成: {saved}/{len(df)}")
        return {"saved": saved, "total": len(df)}
    except Exception as e:
        logger.error(f"❌ 大宗交易获取失败: {e}")
        return {"saved": 0, "total": 0, "error": str(e), "error_kind": "network"}


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

    target_date = get_expected_latest_trading_day()

    try:
        df = _fetch_sector_fund_flow(target_date)
    except Exception as e:
        logger.error(f"❌ 板块资金流向获取失败: {e}")
        return {"saved": 0, "total": 0, "error": str(e), "error_kind": "network"}
    if df is None or df.empty:
        logger.warning("⚠️  板块资金流向无数据")
        # 显式 skipped：非交易日/上游无数据属正常结果，避免被结果契约误判为 failed
        return {"skipped": True, "reason": "no sector fund flow data", "total": 0}

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


# ===========================================================================
# 收盘刷新 helper（Task 9）：只抓取/归一化，不写库，源异常直接上抛
# ===========================================================================


def _refresh_float(value) -> float | None:
    """NaN/None → None，其余转 float（SQLite 不接受 NaN）。"""
    if value is None or pd.isna(value):
        return None
    return float(value)


def fetch_margin_trading_records(trade_date: str) -> list[dict]:
    """收盘刷新专用：抓取指定交易日沪深两市融资融券明细并归一化。

    Length mismatch（AkShare 空日返回）视为该市当日无数据；
    其他源异常直接上抛，由适配器/编排器落实保留旧数据。
    """
    compact = trade_date.replace("-", "")
    records: list[dict] = []
    for exchange, fetcher in [("sh", ak.stock_margin_detail_sse), ("sz", ak.stock_margin_detail_szse)]:
        df = _safe_fetch_margin_detail(fetcher, compact, exchange)
        if df is None:
            continue
        code_col = "标的证券代码" if exchange == "sh" else "证券代码"
        for _, row in df.iterrows():
            code = str(row.get(code_col, "")).strip()
            if not code:
                continue
            records.append({
                "ts_code": code,
                "trade_date": trade_date,
                "margin_balance": _refresh_float(row.get("融资余额")),
                "margin_buy": _refresh_float(row.get("融资买入额")),
                "margin_repay": _refresh_float(row.get("融资偿还额")) if exchange == "sh" else None,
                "short_balance": _refresh_float(row.get("融券余量")),
                "short_sell": _refresh_float(row.get("融券卖出量")),
                "short_repay": _refresh_float(row.get("融券偿还量")) if exchange == "sh" else None,
                "total_balance": _refresh_float(row.get("融资融券余额")),
                "data_source": "akshare",
            })
    return records


def fetch_dragon_tiger_records(trade_date: str) -> list[dict]:
    """收盘刷新专用：抓取指定交易日龙虎榜并附稳定事件键。

    权威空榜返回 []（空榜 ≠ 源失败）；源异常直接上抛。
    """
    compact = trade_date.replace("-", "")
    df = ak.stock_lhb_detail_em(start_date=compact, end_date=compact)
    if df is None or df.empty:
        return []
    records: list[dict] = []
    for _, row in df.iterrows():
        code = str(row.get("代码", "")).strip()
        if not code:
            continue
        record = {
            "ts_code": code,
            "trade_date": trade_date,
            "close_price": _refresh_float(row.get("收盘价")),
            "pct_change": _refresh_float(row.get("涨跌幅")),
            "net_buy_amount": _refresh_float(row.get("龙虎榜净买额")),
            "buy_amount": _refresh_float(row.get("龙虎榜买入额")),
            "sell_amount": _refresh_float(row.get("龙虎榜卖出额")),
            "turnover_rate": _refresh_float(row.get("换手率")),
            "market_cap": _refresh_float(row.get("流通市值")),
            "reason": str(row.get("上榜原因", "") or ""),
            "data_source": "akshare",
        }
        # 稳定事件键：同股同日不同上榜原因的合法多事件互异
        record["source_record_key"] = dragon_tiger_source_key(record)
        records.append(record)
    return records


def fetch_block_trade_records(trade_date: str) -> list[dict]:
    """收盘刷新专用：抓取指定交易日大宗交易并附稳定事件键。

    权威空榜返回 []；源异常直接上抛。
    """
    compact = trade_date.replace("-", "")
    df = ak.stock_dzjy_mrmx(symbol="A股", start_date=compact, end_date=compact)
    if df is None or df.empty:
        return []
    records: list[dict] = []
    for _, row in df.iterrows():
        code = str(row.get("证券代码", "")).strip()
        if not code:
            continue
        record = {
            "ts_code": code,
            "trade_date": trade_date,
            "deal_price": _refresh_float(row.get("成交价")),
            "close_price": _refresh_float(row.get("收盘价")),
            "discount_rate": _refresh_float(row.get("折溢率")),
            "volume": _refresh_float(row.get("成交量")),
            "amount": _refresh_float(row.get("成交额")),
            "buyer_branch": str(row.get("买方营业部", "") or ""),
            "seller_branch": str(row.get("卖方营业部", "") or ""),
            "data_source": "akshare",
        }
        # 稳定事件键：同股同日不同价/量的合法多笔交易互异
        record["source_record_key"] = block_trade_source_key(record)
        records.append(record)
    return records


def fetch_sector_fund_flow_records(trade_date: str) -> list[dict]:
    """收盘刷新专用：抓取同花顺行业资金流即时快照并归一化。

    同花顺仅提供即时快照，trade_date 由调用方指定；源异常直接上抛。
    """
    df = ak.stock_fund_flow_industry()
    if df is None or df.empty:
        return []
    records: list[dict] = []
    for _, row in df.iterrows():
        sector = str(row.get("行业", "")).strip()
        if not sector:
            continue
        records.append({
            "sector_name": sector,
            "trade_date": trade_date,
            "main_net_inflow": _refresh_float(row.get("净额")),
            "main_net_inflow_pct": _refresh_float(row.get("行业-涨跌幅")),
            "super_large_net_inflow": None,
            "large_net_inflow": _refresh_float(row.get("流入资金")),
            "medium_net_inflow": _refresh_float(row.get("流出资金")),
            "small_net_inflow": None,
            "data_source": "ths",
        })
    return records
