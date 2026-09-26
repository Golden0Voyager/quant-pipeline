"""
季度财务数据报告期发现、批量抓取与 PIT 历史写入
───────────────────────────────────────────

替换旧有的逐股票 stock_financial_abstract 模式，改为按报告期
批量获取并维护 quarterly_financials_history PIT 表。
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import logging
import os
import time
from collections.abc import Callable, Iterable
from datetime import date, datetime
from functools import partial
from typing import Any

import pandas as pd

from core.config import SHARED_DATA_DIR
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
    if m == 6 or m == 9:
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


def _cninfo_period(period: str) -> str:
    """将数字期次 ``"20260630"`` 转为巨潮资讯披露接口需要的中文标签。

    akshare ``stock_report_disclosure`` 内部按 ``{year}一季/半年报/三季/年报``
    查表，传入数字期次会触发 ``KeyError``。
    """
    year, mmdd = period[:4], period[4:]
    return {
        "0331": f"{year}一季",
        "0630": f"{year}半年报",
        "0930": f"{year}三季",
        "1231": f"{year}年报",
    }.get(mmdd, f"{year}年报")


def _fetch_disclosure_dates(period: str) -> pd.DataFrame:
    """获取披露日期表（stock_report_disclosure）。

    akshare 的 ``stock_report_disclosure`` 需要中文期次标签（如 ``"2026半年报"``）
    而非数字格式 ``"20260630"``，故先经 :func:`_cninfo_period` 转换。
    """
    df = ak.stock_report_disclosure(market="沪深京", period=_cninfo_period(period))
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


def _retry(fn: Callable[[], Any], *, tries: int = 3, base_delay: float = 1.0, label: str = "") -> Any:
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


def _merge_financial_period(period: str) -> list[dict]:
    """获取一个报告期的全部财务数据，合并后返回记录列表。"""
    if ak is None:
        return []

    yjbb = _fetch_yjbb(period)
    lrb = _fetch_lrb(period)
    zcfz = _fetch_zcfz(period)
    xjll = _fetch_xjll(period)
    # 披露日接口可能临时失败或期次过老无数据，失败不影响主流程（用业绩报表兜底）
    try:
        disc = _fetch_disclosure_dates(period)
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"⚠️ {period} 披露日期获取失败，改用业绩报表公告日: {exc}")
        disc = pd.DataFrame()

    if yjbb.empty:
        logger.warning(f"⚠️ {period} 业绩报表为空，跳过")
        return []

    # 披露日期字典：优先巨潮资讯「实际披露日」，缺省回退业绩报表「最新公告日期」
    disc_map: dict[str, str] = {}
    if not disc.empty:
        for _, row in disc.iterrows():
            code = _normalize_code(row.get("股票代码", ""))
            pub_date = str(row.get("公告日期", "") or "").strip()[:10]
            if code and pub_date:
                disc_map[code] = pub_date

    # 业绩报表自带「最新公告日期」，作为披露日缺失时的兜底（覆盖非最近四期）
    yjbb_pub_map: dict[str, str] = {}
    if not yjbb.empty:
        for _, row in yjbb.iterrows():
            code = _normalize_code(row.get("股票代码", ""))
            pub_date = str(row.get("最新公告日期", "") or "").strip()[:10]
            if code and pub_date:
                yjbb_pub_map.setdefault(code, pub_date)

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
        pub_date = disc_map.get(code) or yjbb_pub_map.get(code, "")
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
                with contextlib.suppress(ValueError, TypeError):
                    record[key] = float(val)

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
                with contextlib.suppress(ValueError, TypeError):
                    record[key] = float(val)

        records.append(record)

    return records


# ── 失败报告期重试队列 ────────────────────────────────────────────────

# 报告期抓取一旦失败，若仅依赖 discover_missing_financial_periods（它只看库内
# 覆盖率是否低于阈值）会漏掉「覆盖率已达标、但实际不完整」的报告期：失败前部分
# 数据已落库，覆盖率可能已经过线，于是以后永远不会被重新抓取。本队列把失败期次
# 显式记下，下次自动发现时并回待抓取列表，抓取成功后自动移除。
#
# 文件语义与 core.progress.ProgressTracker 一致：flock 互斥 + 原子替换，避免多
# 进程并发或写一半崩溃损坏队列。路径在**调用时**读取模块属性，便于测试替换
# （P2-15 教训：导入期固化的常量会让 monkeypatch 失效）。
_FAILED_PERIODS_FILE: Any = SHARED_DATA_DIR / "financial_period_retry.json"


@contextlib.contextmanager
def _queue_lock(exclusive: bool = True):
    """用文件锁保护失败报告期队列的读写（锁文件锚点，不删除）。"""
    lock_path = _FAILED_PERIODS_FILE.with_suffix(".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR, 0o666)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def load_failed_periods() -> list[str]:
    """读取待重试的报告期列表；文件缺失或损坏时返回空列表（不抛异常）。"""
    with _queue_lock(exclusive=False):
        if not _FAILED_PERIODS_FILE.exists():
            return []
        try:
            with open(_FAILED_PERIODS_FILE, encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError):
            logger.warning("⚠️ 失败报告期队列文件损坏，按空队列处理")
            return []
    periods = data.get("periods") if isinstance(data, dict) else None
    if not isinstance(periods, list):
        return []
    return [str(p).strip() for p in periods if str(p).strip()]


def _write_failed_periods(periods: Iterable[str]) -> None:
    """原子写入队列；为空时删除文件（不留空壳）。"""
    unique = sorted({str(p).strip() for p in periods if str(p).strip()})
    with _queue_lock(exclusive=True):
        if not unique:
            if _FAILED_PERIODS_FILE.exists():
                _FAILED_PERIODS_FILE.unlink()
            return
        payload = {
            "periods": unique,
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        tmp = _FAILED_PERIODS_FILE.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        tmp.replace(_FAILED_PERIODS_FILE)


def record_failed_periods(periods: Iterable[str]) -> list[str]:
    """把报告期并入失败队列（去重、排序），返回合并后的完整队列。"""
    merged = sorted(set(load_failed_periods()) | {str(p).strip() for p in periods})
    _write_failed_periods(merged)
    return merged


def clear_failed_periods(periods: Iterable[str]) -> list[str]:
    """把已成功抓取的报告期从失败队列移除，返回剩余队列。"""
    remove = {str(p).strip() for p in periods}
    remaining = [p for p in load_failed_periods() if p not in remove]
    _write_failed_periods(remaining)
    return remaining


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
        # 并回上次失败的报告期：覆盖率发现会漏掉「已达标但不完整」的期次，
        # 队列里的期次即使覆盖率过线也必须重试。
        queued = [p for p in load_failed_periods() if p not in periods]
        if queued:
            logger.info(f"🔁 并回上次失败的报告期: {queued}")
            periods = periods + queued

    if not periods:
        logger.info("✅ 所有报告期数据已覆盖，无需更新")
        # 显式 skipped：零行属正常结果，避免被结果契约误判为
        # "zero rows without explanation" 失败（2026-08-25 全量运行实录，
        # 与 update_industry 同类问题）
        return {"skipped": True, "reason": "all financial periods covered",
                "saved": 0}

    logger.info(f"📋 待更新报告期: {periods}")
    total_saved = 0
    last_error: str | None = None

    failed_periods: list[str] = []
    for period in periods:
        logger.info(f"  🔄 处理报告期 {period}...")
        try:
            # partial 才能给出 Callable[[], ...]，带默认参数的 lambda 不行
            records = _retry(
                partial(_merge_financial_period, period),
                label=f"financial_history:{period}",
            )
            if records is None:
                logger.warning(f"  ⚠️ {period} 重试耗尽")
                failed_periods.append(period)
                last_error = f"{period}: retry exhausted"
                continue
            if not records:
                logger.warning(f"  ⚠️ {period} 无有效记录")
                continue
            save_result = db.save_financial_history_batch(records)
            saved = save_result.get("history_saved", 0)
            total_saved += saved
            logger.info(f"  ✅ {period}: 写入 {saved} 条历史记录")
            time.sleep(2)
        except Exception as e:
            last_error = f"{period}: {e}"
            failed_periods.append(period)
            logger.warning(f"  ⚠️ {period} 处理失败: {e}")

    # 持久化重试队列：成功的期次清出，失败的期次记入，供下次自动重试。
    # 手动指定 periods 的定向修复同样生效（成功则清队列、失败则入队列）。
    resolved_periods = [p for p in periods if p not in failed_periods]
    if resolved_periods:
        clear_failed_periods(resolved_periods)
    if failed_periods:
        record_failed_periods(failed_periods)

    logger.info(f"✅ 财务历史更新完成: 共写入 {total_saved} 条")
    if total_saved == 0 and failed_periods:
        return {
            "status": "retained",
            "reason": f"all {len(failed_periods)} periods failed; kept old data",
            "error": last_error,
            "retained_old_data": True,
            "saved": 0,
            "metadata": {"failed_periods": failed_periods},
        }

    result: dict[str, Any] = {"saved": total_saved}
    if last_error:
        result["error"] = last_error
    if failed_periods:
        result["metadata"] = {"failed_periods": failed_periods}
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
