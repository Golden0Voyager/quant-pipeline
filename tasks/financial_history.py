"""
季度财务数据报告期发现、批量抓取与 PIT 历史写入
───────────────────────────────────────────

替换旧有的逐股票 stock_financial_abstract 模式，改为按报告期
批量获取并维护 quarterly_financials_history PIT 表。
"""

from __future__ import annotations

import logging
import time
from datetime import date, datetime
from typing import Any

import pandas as pd

from interface import DatabaseInterface

try:
    import akshare as ak
except ImportError:
    ak = None

logger = logging.getLogger(__name__)


# ── 报告期工具 ──────────────────────────────────────────────────────────


def _closed_report_periods(as_of_date: date, lookback_quarters: int = 8) -> list[str]:
    """Return closed report-period labels (e.g. ``20260630``) back from *as_of_date*.

    Quarter-end months: 03, 06, 09, 12.  An open (current) quarter whose end
    date is after *as_of_date* is excluded.
    """
    periods: list[str] = []
    y, m = as_of_date.year, as_of_date.month
    for i in range(lookback_quarters):
        q_offset = -i  # 0 = current quarter, -1 = previous, etc.
        q = ((m - 1) // 3) + q_offset
        qy = y + q // 4
        qm = (q % 4 + 1) * 3  # 3, 6, 9, 12
        qd = 31 if qm in (3, 12) else 30
        quarter_end = date(qy, qm, qd)
        if quarter_end <= as_of_date:
            periods.append(f"{qy}{qm:02d}{qd:02d}")
    return periods


def _minimum_market_coverage(period: str, as_of_date: date) -> float:
    """返回指定报告期的最低覆盖率阈值。

    披露窗口内（报告期后 120 天）为 60%，之后为 90%。
    """
    y, m = int(period[:4]), int(period[4:6])
    qe = 31 if m in (3, 12) else 30
    if m == 6:
        qe = 30
    elif m == 9:
        qe = 30
    period_end = date(y, m, qe)
    days_since = (as_of_date - period_end).days
    return 0.6 if days_since <= 120 else 0.9


# ── 周期发现 ────────────────────────────────────────────────────────────


def discover_missing_financial_periods(
    db: DatabaseInterface,
    as_of_date: date | None = None,
) -> list[str]:
    """返回需要抓取的报告期列表。

    将已完结的报告期与数据库覆盖度对比，低于阈值则返回。
    """
    if as_of_date is None:
        as_of_date = date.today()
    expected = _closed_report_periods(as_of_date, lookback_quarters=8)
    coverage = db.get_financial_period_coverage(expected)
    total_stocks = _estimate_active_stocks(db)
    missing: list[str] = []
    for period in expected:
        cov_count = coverage.get(period, 0)
        threshold = _minimum_market_coverage(period, as_of_date)
        needed = max(1, int(total_stocks * threshold))
        if cov_count < needed:
            missing.append(period)
    return missing


def _estimate_active_stocks(db: DatabaseInterface) -> int:
    """估算活跃股票数量（最近有日线数据的股票）。"""
    try:
        stocks = db.get_stock_list()
        return len(stocks) if stocks is not None and not stocks.empty else 5500
    except Exception:
        return 5500


# ── 按报告期批量抓取 ──────────────────────────────────────────────────


def _fetch_yjbb(period: str) -> pd.DataFrame:
    """获取业绩报表（stock_yjbb_em）。"""
    df = ak.stock_yjbb_em(date=period)
    return df if df is not None and not df.empty else pd.DataFrame()


def _fetch_lrb(period: str) -> pd.DataFrame:
    """获取利润表（stock_lrb_em）。"""
    df = ak.stock_lrb_em(date=period)
    return df if df is not None and not df.empty else pd.DataFrame()


def _fetch_zcfz(period: str) -> pd.DataFrame:
    """获取资产负债表（stock_zcfz_em）。"""
    df = ak.stock_zcfz_em(date=period)
    return df if df is not None and not df.empty else pd.DataFrame()


def _fetch_xjll(period: str) -> pd.DataFrame:
    """获取现金流量表（stock_xjll_em）。"""
    df = ak.stock_xjll_em(date=period)
    return df if df is not None and not df.empty else pd.DataFrame()


def _fetch_disclosure_dates(period: str) -> pd.DataFrame:
    """获取披露日期表（stock_report_disclosure）。"""
    df = ak.stock_report_disclosure(market="沪深京", period=period)
    return df if df is not None and not df.empty else pd.DataFrame()


# ── 合并 ──────────────────────────────────────────────────────────────


def _normalize_code(raw: Any) -> str:
    """将各种格式的股票代码统一为 6 位数字。"""
    s = str(raw).strip()
    # 去掉 .SH / .SZ / .BJ 后缀
    for suffix in (".SH", ".SZ", ".BJ", ".sh", ".sz", ".bj"):
        if s.endswith(suffix):
            s = s[: -len(suffix)]
            break
    return s.zfill(6)


def _merge_financial_period(period: str) -> list[dict]:
    """获取一个报告期的全部财务数据，合并后返回记录列表。"""
    if ak is None:
        return []

    yjbb = _fetch_yjbb(period)
    lrb = _fetch_lrb(period)
    zcfz = _fetch_zcfz(period)
    xjll = _fetch_xjll(period)
    disc = _fetch_disclosure_dates(period)

    if yjbb.empty:
        logger.warning(f"⚠️ {period} 业绩报表为空，跳过")
        return []

    # 归一化股票代码并建立披露日期字典
    disc_map: dict[str, str] = {}
    if not disc.empty:
        for _, row in disc.iterrows():
            code = _normalize_code(row.get("股票代码", ""))
            pub_date = str(row.get("公告日期", "") or "").strip()[:10]
            if code and pub_date:
                disc_map[code] = pub_date

    # 从 yjbb 开始，依次 merge
    merged = yjbb.copy()
    for df, suffix in [(lrb, "_lrb"), (zcfz, "_zcfz"), (xjll, "_xjll")]:
        if df.empty:
            continue
        df = df.copy()
        df.columns = [f"{c}{suffix}" if c not in ("股票代码",) else c for c in df.columns]
        merged = merged.merge(df, on="股票代码", how="left")

    records: list[dict] = []
    for _, row in merged.iterrows():
        code = _normalize_code(row.get("股票代码", ""))
        if not code:
            continue
        pub_date = disc_map.get(code, "")
        if not pub_date:
            continue

        record: dict[str, Any] = {
            "ts_code": code,
            "report_period": period,
            "publish_date": pub_date,
            "data_source": "akshare",
        }

        # 映射常用指标
        for col, key in [
            ("营业收入-营业收入", "revenue"),
            ("净利润-净利润", "net_profit"),
            ("净利润-扣除非经常性损益后的净利润", "deduct_profit"),
            ("经营活动产生的现金流量净额", "operating_cashflow"),
            ("研发费用", "rd_expense"),
        ]:
            val = row.get(col) or row.get(col.replace("_lrb", "").replace("_zcfz", "").replace("_xjll", ""))
            if val is not None and val != "":
                try:
                    record[key] = float(val)
                except (ValueError, TypeError):
                    pass

        # 从 yjbb 获取 ROE/毛利率等
        for col, key in [
            ("净资产收益率", "roe"),
            ("毛利率", "gross_margin"),
            ("净利率", "net_margin"),
            ("资产负债率", "debt_ratio"),
            ("营业收入同比增长率", "revenue_growth"),
            ("净利润同比增长率", "profit_growth"),
        ]:
            val = row.get(col)
            if val is not None and val != "":
                try:
                    record[key] = float(val)
                except (ValueError, TypeError):
                    pass

        records.append(record)

    return records


# ── 主入口 ─────────────────────────────────────────────────────────────


def update_financial_history(
    db: DatabaseInterface,
    periods: list[str] | None = None,
) -> dict[str, Any]:
    """按报告期批量获取财务数据并写入历史表。

    Args:
        db: 数据库接口
        periods: 指定报告期列表，为 None 时自动发现缺失期次

    Returns:
        {"saved": int, "error": str | None}
    """
    logger.info("\n" + "=" * 60)
    logger.info("📊 任务: 按报告期更新财务历史")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "error": "akshare not installed"}

    if periods is None:
        periods = discover_missing_financial_periods(db)

    if not periods:
        logger.info("✅ 所有报告期数据已覆盖，无需更新")
        return {"saved": 0}

    logger.info(f"📋 待更新报告期: {periods}")
    total_saved = 0
    last_error: str | None = None

    for period in periods:
        logger.info(f"  🔄 处理报告期 {period}...")
        try:
            records = _merge_financial_period(period)
            if not records:
                logger.warning(f"  ⚠️ {period} 无有效记录")
                continue
            result = db.save_financial_history_batch(records)
            saved = result.get("history_saved", 0)
            total_saved += saved
            logger.info(f"  ✅ {period}: 写入 {saved} 条历史记录")
            time.sleep(2)
        except Exception as e:
            last_error = f"{period}: {e}"
            logger.warning(f"  ⚠️ {period} 处理失败: {e}")

    logger.info(f"✅ 财务历史更新完成: 共写入 {total_saved} 条")
    result: dict[str, Any] = {"saved": total_saved}
    if last_error:
        result["error"] = last_error
    return result


def get_financials_as_of(
    db: DatabaseInterface,
    symbol: str,
    as_of_date: str,
) -> dict[str, Any] | None:
    """返回给定截止日前已知的最新财务数据。

    直接透传到 provider 的 PIT 查询，避免未来信息偏差。
    """
    return db.get_financials_as_of(symbol, as_of_date)
