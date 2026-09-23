"""
资金流向与行情数据任务模块
─────────────────────────
南向资金、AH 股溢价、ETF 日线行情。

fallback 策略（东财被限流/封禁时）：
- ETF 日线：东财 → 雪球 kline
- A/H 溢价：东财现成溢价表 → 腾讯 H 股现价 + 中行汇率 + 库内 A 股收盘价合成
"""

from __future__ import annotations

import logging
import sqlite3
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Any

from core.calendar import get_expected_latest_trading_day
from core.utils import is_real_db_path, warn_if_all_empty
from core.utils import to_float as _to_float
from interface import DatabaseInterface

try:
    import akshare as ak
except ImportError:
    ak = None

logger = logging.getLogger(__name__)


# ===========================================================================
# 辅助函数
# ===========================================================================


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
            return {"status": "no_data", "saved": 0, "total": 0, "reason": "empty response"}
        warn_if_all_empty(records, ["net_buy_amount", "buy_amount"], "south_flow")
        saved = db.save_south_flow_batch(records)
        logger.info(f"✅ 南向资金保存完成: {saved} 条")
        return {"status": "success", "saved": saved, "total": len(records)}
    except Exception as e:
        logger.error(f"❌ 南向资金更新失败: {e}")
        return {"saved": 0, "error": str(e), "error_kind": "network"}


# ===========================================================================
# A/H 股溢价
# ===========================================================================


def _fetch_ah_premium() -> list[dict]:
    if ak is None:
        return []
    try:
        from core.source_client import get_default_client

        resp = get_default_client().call("eastmoney", lambda: ak.stock_zh_ah_spot_em())
        df = resp.data if resp.success else None
        if df is None or getattr(df, "empty", True):
            err = resp.metadata.error if resp and not resp.success else "empty result"
            logger.warning(f"⚠️ A/H 溢价获取失败: {err}")
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


def _fetch_ah_premium_fallback(db_path: str | None) -> list[dict]:
    """A/H 溢价 fallback：腾讯 H 股现价 + 中行汇率 + 库内 A 股收盘价合成。

    东财 stock_zh_ah_spot_em 被封时使用：
    - H 股价（HKD）：ak.stock_zh_ah_spot（腾讯源）
    - A/H 代码映射：库内 ah_premium 历史记录
    - A 股价（CNY）：库内 daily_bars 最新收盘价
    - 汇率：ak.currency_boc_sina 中行折算价（100 港币兑人民币）
    - 溢价率 = (A价 / (H价 × 汇率) - 1) × 100
    """
    if ak is None or not is_real_db_path(db_path):
        return []
    try:
        h_spot = ak.stock_zh_ah_spot()
        if h_spot is None or h_spot.empty:
            return []
    except Exception as e:
        logger.warning(f"⚠️ A/H fallback：腾讯 H 股行情获取失败: {e}")
        return []

    # 汇率：中行折算价是 100 港币兑人民币
    try:
        end = datetime.now().strftime("%Y%m%d")
        start = (datetime.now() - timedelta(days=10)).strftime("%Y%m%d")
        fx = ak.currency_boc_sina(symbol="港币", start_date=start, end_date=end)
        rate_col = next((c for c in fx.columns if "折" in str(c)), None)
        hkd_cny = float(fx[rate_col].dropna().iloc[-1]) / 100.0
    except Exception as e:
        logger.warning(f"⚠️ A/H fallback：港币汇率获取失败: {e}")
        return []

    try:
        with sqlite3.connect(str(db_path), timeout=5.0) as conn:
            # A/H 代码映射：取库内最近一个有数据日期的全量对照表
            mapping = conn.execute(
                "SELECT ts_code, h_code, name FROM ah_premium"
                " WHERE trade_date = (SELECT MAX(trade_date) FROM ah_premium)"
                " AND ts_code != '' AND h_code != ''"
            ).fetchall()
            if not mapping:
                logger.warning("⚠️ A/H fallback：库内无历史 A/H 代码映射，无法合成")
                return []
            # A 股最新收盘价
            latest_bar_date = conn.execute(
                "SELECT MAX(trade_date) FROM daily_bars"
            ).fetchone()[0]
            a_close_rows = conn.execute(
                "SELECT ts_code, close FROM daily_bars WHERE trade_date = ?",
                (latest_bar_date,),
            ).fetchall()
    except Exception as e:
        logger.warning(f"⚠️ A/H fallback：读取库内映射/收盘价失败: {e}")
        return []

    # daily_bars 的 ts_code 带市场后缀（如 000002.SZ），按 6 位码归一
    a_close = {str(c)[:6]: v for c, v in a_close_rows if v is not None}
    h_price_map: dict[str, float] = {}
    for _, row in h_spot.iterrows():
        code = str(row.get("代码", "")).strip().zfill(5)
        price = _to_float(row.get("最新价"))
        if code and price:
            h_price_map[code] = price

    trade_date = latest_bar_date or get_expected_latest_trading_day()
    records: list[dict] = []
    for ts_code, h_code, name in mapping:
        h_price = h_price_map.get(str(h_code).zfill(5))
        a_price = a_close.get(str(ts_code)[:6])
        if not h_price or not a_price:
            continue
        h_in_cny = h_price * hkd_cny
        premium = round((a_price / h_in_cny - 1) * 100, 2) if h_in_cny else None
        records.append(
            {
                "trade_date": trade_date,
                "ts_code": ts_code,
                "h_code": h_code,
                "name": name,
                "h_price": h_price,
                "a_price": a_price,
                "premium": premium,
                "data_source": "tencent_synth",
            }
        )
    if records:
        logger.info(
            f"  🔄 A/H fallback 合成成功: {len(records)} 对"
            f"（H价=腾讯, A价=库内 {trade_date}, 汇率={hkd_cny:.4f}）"
        )
    return records


def update_ah_premium(db: DatabaseInterface) -> dict:
    logger.info("\n" + "=" * 60)
    logger.info("🔗 任务: 更新 A/H 股溢价")
    logger.info("=" * 60)
    if ak is None:
        return {"saved": 0, "error": "akshare not installed"}
    try:
        records = _fetch_ah_premium()
        if not records:
            logger.warning("⚠️ 东财 A/H 溢价无数据，尝试 fallback 合成")
            records = _fetch_ah_premium_fallback(getattr(db, "db_path", None))
        if not records:
            logger.warning("⚠️ A/H 溢价无数据（含 fallback）")
            return {"status": "no_data", "saved": 0, "total": 0, "reason": "empty response"}
        saved = db.save_ah_premium_batch(records)
        logger.info(f"✅ A/H 溢价保存完成: {saved} 条")
        return {"status": "success", "saved": saved, "total": len(records)}
    except Exception as e:
        logger.error(f"❌ A/H 溢价更新失败: {e}")
        return {"saved": 0, "error": str(e), "error_kind": "network"}


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


def _get_etf_update_range(db: DatabaseInterface) -> tuple[str, str, str | None]:
    """返回 ETF 增量更新需要的 (start_date, end_date, latest_date)。

    - 若数据库已有数据：从最新日期前推 5 天开始，补齐可能缺失的近期数据
    - 若数据库为空：拉取最近 30 天
    日期格式均为 YYYYMMDD。
    """
    today = get_expected_latest_trading_day()
    end_date = today.replace("-", "")
    db_path = getattr(db, "db_path", None)
    latest_date: str | None = None
    if is_real_db_path(db_path):
        try:
            with sqlite3.connect(str(db_path), timeout=5.0) as conn:
                cur = conn.cursor()
                cur.execute("SELECT MAX(trade_date) FROM etf_daily")
                row = cur.fetchone()
                if row and row[0]:
                    latest_date = str(row[0])[:10]
        except Exception:
            pass
    if latest_date:
        start_dt = datetime.strptime(latest_date, "%Y-%m-%d") - timedelta(days=5)
        start_date = start_dt.strftime("%Y%m%d")
    else:
        start_dt = datetime.strptime(end_date, "%Y%m%d") - timedelta(days=30)
        start_date = start_dt.strftime("%Y%m%d")
    return start_date, end_date, latest_date


def _fetch_single_etf_xueqiu(code: str, name: str, start_date: str, end_date: str) -> list[dict]:
    """ETF 日线 fallback：雪球 kline（东财被封时使用）。

    口径对齐：雪球成交量单位是股，东财是手，需 ÷100；成交额均为元。
    """
    try:
        from smartmoney_hunter import xueqiu as xq
    except ImportError:
        return []
    try:
        df = xq.get_daily_bars(code, start_date=start_date, end_date=end_date, adjust="qfq")
    except Exception as e:
        logger.warning(f"⚠️ ETF {name}({code}) 雪球 fallback 失败: {e}")
        return []
    if df is None or df.empty:
        return []
    records: list[dict] = []
    for _, row in df.iterrows():
        volume = _to_float(row.get("volume"))
        records.append(
            {
                "trade_date": str(row.get("date", ""))[:10],
                "ts_code": code,
                "name": name,
                "open": _to_float(row.get("open")),
                "high": _to_float(row.get("high")),
                "low": _to_float(row.get("low")),
                "close": _to_float(row.get("close")),
                "volume": volume / 100.0 if volume is not None else None,  # 股 → 手
                "amount": _to_float(row.get("amount")),
                "data_source": "xueqiu",
            }
        )
    logger.info(f"  🔄 {name}({code}): {len(df)} 条 (雪球 fallback)")
    return records


def _fetch_single_etf(code: str, name: str, start_date: str, end_date: str) -> list[dict]:
    """获取单只 ETF 日线，通过 SourceClient.call() 获得重试+熔断保护；东财失败时 fallback 雪球。"""
    if ak is None:
        return []
    from core.source_client import get_default_client

    resp = get_default_client().call(
        "eastmoney",
        lambda: ak.fund_etf_hist_em(
            symbol=code,
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust="qfq",
        ),
    )
    df = resp.data if resp.success else None
    if df is None or (hasattr(df, "empty") and df.empty):
        logger.warning(f"⚠️ ETF {name}({code}) 东财获取失败（{resp.metadata.error}），尝试雪球 fallback")
        return _fetch_single_etf_xueqiu(code, name, start_date, end_date)
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
    records: list[dict] = []
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
    return records


def _fetch_etf_daily(
    start_date: str,
    end_date: str,
    db_path: str | None = None,
) -> list[dict]:
    if ak is None:
        return []
    records: list[dict] = []
    max_workers = min(5, max(2, (len(_ETF_CODES) // 4) + 1))

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 按 _ETF_CODES 顺序提交，确保结果顺序与列表一致
        futures = [
            executor.submit(_fetch_single_etf, code, name, start_date, end_date)
            for code, name in _ETF_CODES
        ]
        for future in futures:
            try:
                etf_records = future.result(timeout=25)
                records.extend(etf_records)
            except Exception as e:
                logger.warning(f"⚠️ ETF 获取异常: {e}")
    return records


def update_etf_daily(db: DatabaseInterface) -> dict:
    logger.info("\n" + "=" * 60)
    logger.info("📊 任务: 更新 ETF 日线行情")
    logger.info("=" * 60)
    if ak is None:
        return {"saved": 0, "error": "akshare not installed"}

    now = datetime.now()
    if now.hour == 15:
        logger.warning(
            f"当前时间 {now.hour}:{now.minute:02d}，处于收盘结算窗口（15:00~16:00），"
            "东财接口可能返回 RemoteDisconnected，建议等到 16:00 后再运行"
        )

    try:
        start_date, end_date, latest_date = _get_etf_update_range(db)
        if latest_date:
            logger.info(f"📅 增量更新：从 {start_date} 到 {end_date}（数据库最新: {latest_date}）")
        else:
            logger.info(f"📅 首次/空表更新：从 {start_date} 到 {end_date}（最近 30 天）")

        records = _fetch_etf_daily(start_date, end_date)
        if not records:
            logger.warning("⚠️ ETF 日线无数据（所有标的均获取失败，很可能是东财接口网络问题）")
            return {"saved": 0, "total": 0, "status": "degraded", "error_kind": "network", "error": "all ETFs failed — network issue (RemoteDisconnected)"}
        warn_if_all_empty(records, ["close", "volume"], "etf_daily")
        saved = db.save_etf_daily_batch(records)
        logger.info(f"✅ ETF 日线保存完成: {saved} 条")
        return {"saved": saved, "total": len(records)}
    except Exception as e:
        logger.error(f"❌ ETF 日线更新失败: {e}")
        return {"saved": 0, "error": str(e), "error_kind": "network"}


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

    etf_start, etf_end, _ = _get_etf_update_range(db)

    def _fetch_ah_with_fallback() -> list[dict]:
        return _fetch_ah_premium() or _fetch_ah_premium_fallback(getattr(db, "db_path", None))

    # 显式标注：三元组里 fetch/save 是不同函数，不标注 mypy 会把联合类型
    # 退化成「unknown」，直接调用报错（参数按元组解包后无法还原签名）
    sub_tasks: list[
        tuple[str, Callable[..., Any], Callable[..., Any], tuple[str, str] | None]
    ] = [
        ("south_flow", _fetch_south_flow, db.save_south_flow_batch, None),
        ("ah_premium", _fetch_ah_with_fallback, db.save_ah_premium_batch, None),
        ("etf_daily", _fetch_etf_daily, db.save_etf_daily_batch, (etf_start, etf_end)),
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


# ===========================================================================
# 收盘刷新 helper（Task 9）：只抓取/归一化，不写库，源异常直接上抛
# ===========================================================================


def fetch_south_flow_records() -> list[dict]:
    """收盘刷新专用：抓取南向资金全历史并归一化（历史型源，适配器挑选回看分区）。

    权威空返回 []；源异常直接上抛，由适配器/编排器落实保留旧数据。
    """
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


def fetch_ah_premium_records(trade_date: str) -> list[dict]:
    """收盘刷新专用：抓取 A/H 溢价即时快照并盖调用方指定交易日。

    权威空返回 []；源异常直接上抛（不走 fallback 合成，刷新语义要求显式失败）。
    """
    df = ak.stock_zh_ah_spot_em()
    if df is None or df.empty:
        return []
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


def fetch_etf_daily_records(code: str, name: str, trade_date: str) -> list[dict]:
    """收盘刷新专用：抓取单只 ETF 目标交易日日线并归一化。

    只请求目标日单日窗口（绝不重写历史）；目标日无数据返回 []；
    源异常直接上抛（不走雪球 fallback，刷新语义要求显式失败）。
    """
    compact = trade_date.replace("-", "")
    df = ak.fund_etf_hist_em(
        symbol=code,
        period="daily",
        start_date=compact,
        end_date=compact,
        adjust="qfq",
    )
    if df is None or df.empty:
        return []
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
    records: list[dict] = []
    for _, row in df.iterrows():
        row_date = str(row.get("trade_date", ""))[:10]
        if row_date != trade_date:
            continue
        records.append(
            {
                "trade_date": row_date,
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
    return records
