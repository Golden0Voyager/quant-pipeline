from __future__ import annotations

import logging
import os  # noqa: F401
import random
import time

import pandas as pd

from core.calendar import get_expected_latest_trading_day
from core.config import SHARED_DATA_DIR  # noqa: F401
from core.lock import skip_if_task_locked
from core.stock_cyq_em import InsufficientDataError, stock_cyq_em
from core.utils import infer_market  # noqa: F401
from interface import DatabaseInterface

try:
    import akshare as ak
except ImportError:
    ak = None

logger = logging.getLogger(__name__)


def _select_index_row(df: pd.DataFrame, trade_date: str):
    """在腾讯全历史序列里挑出**目标日**那一行；取不到时返回 ``None``。

    行为分三种：

    * 目标日在序列里 → 取该行（回补历史日期靠这条）；
    * 目标日**晚于**序列最新（当日行情源端尚未发布）→ 仍取最后一行，
      保留旧的「源端滞后自愈」行为（正常日跑不该因为没发布就整体空手而归）；
    * 目标日早于序列最新但当天确实无行（停牌/非交易日）→ ``None``，
      绝不拿别的日期张冠李戴。
    """
    if "date" not in df.columns:
        return df.iloc[-1]
    dates = df["date"].astype(str).str.slice(0, 10)
    matched = df[dates == trade_date]
    if not matched.empty:
        return matched.iloc[-1]
    if trade_date >= str(dates.max()):
        return df.iloc[-1]
    return None


def _fetch_index_daily(trade_date: str) -> list[dict]:
    """获取主要指数日线行情（上证、深证、创业板、科创50）。"""
    if ak is None:
        return []
    records = []
    last_error: str | None = None
    indices = {
        "sh000001": "上证指数",
        "sz399001": "深证成指",
        "sz399006": "创业板指",
        "sh000688": "科创50",
        "sh000300": "沪深300",
    }
    for index_code, index_name in indices.items():
        try:
            df = ak.stock_zh_index_daily_tx(symbol=index_code)
            if df is not None and not df.empty:
                latest = _select_index_row(df, trade_date)
                if latest is None:
                    logger.warning(f"⚠️ 指数 {index_name}({index_code}) 无 {trade_date} 数据，跳过")
                    continue
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
            last_error = f"{index_name}({index_code}): {e}"
            logger.warning(f"⚠️ 指数 {index_name}({index_code}) 获取失败: {e}")
    if not records and last_error:
        raise RuntimeError(f"all index fetches failed: {last_error}")
    return records


def update_index_daily(db: DatabaseInterface, target_date: str | None = None) -> dict:
    """获取主要指数日线行情并保存。

    ``target_date`` 给定时改写该历史交易日（整日缺席回补入口）；缺省仍取
    ``get_expected_latest_trading_day()``。
    """
    logger.info("\n" + "=" * 60)
    logger.info("📊 任务: 更新指数日线行情")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "error": "akshare not installed"}

    try:
        records = _fetch_index_daily(target_date or get_expected_latest_trading_day())
        if not records:
            logger.warning("⚠️ 指数日线无数据")
            # 显式 skipped：上游正常返回但无数据（非交易日等），避免被结果契约误判为 failed
            return {"skipped": True, "reason": "no index daily data from upstream", "total": 0}
        saved = db.save_index_daily_batch(records)
        logger.info(f"✅ 指数日线保存完成: {saved} 条")
        return {"saved": saved, "total": len(records)}
    except Exception as e:
        logger.error(f"❌ 指数日线更新失败: {e}")
        return {"saved": 0, "error": str(e), "error_kind": "network"}


def _fetch_cyq_em(symbol: str) -> pd.DataFrame | None:
    """调用 core.stock_cyq_em 获取单只股票的东方财富筹码分布。

    使用 ``curl_cffi`` 绕过 ``push2his.eastmoney.com`` 的 TLS 指纹检测。
    """
    try:
        # 部分环境存在系统代理，临时清除后重试一次（部分代理会干扰 EM 连接）
        proxy_vars = ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"]
        saved = {k: os.environ.pop(k, None) for k in proxy_vars}

        ts_code = symbol.replace(".SZ", "").replace(".SH", "").replace(".BJ", "")
        try:
            df = stock_cyq_em(symbol=ts_code, adjust="")
        except Exception:
            # 如果失败，试试带原生代理的请求
            for k, v in saved.items():
                if v is not None:
                    os.environ[k] = v
            df = stock_cyq_em(symbol=ts_code, adjust="")
        finally:
            for k, v in saved.items():
                if v is not None:
                    os.environ[k] = v

        if df is None or df.empty:
            return None
        df.columns = [
            "trade_date",
            "profit_ratio",
            "avg_cost",
            "cost_90_low",
            "cost_90_high",
            "concentration_90",
            "cost_70_low",
            "cost_70_high",
            "concentration_70",
        ]
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.strftime("%Y-%m-%d")
        return df
    except InsufficientDataError:
        # 本地数据前置条件不满足（换手率缺失等）：交由调用方按"跳过"处理，
        # 不得与网络失败混为一谈（避免触发冷却/熔断）
        raise
    except Exception as e:
        logger.warning(f"  {symbol} 东方财富筹码获取失败: {e}")
        return None


_CSI_INDICES: list[tuple[str, str]] = [
    ("000300", "sina"),    # 沪深 300（新浪）
    ("000852", "csindex"), # 中证 1000（中证指数官网）
    ("000905", "sina"),    # 中证 500（新浪）
]


def _fetch_index_constituents(index_code: str, source: str) -> set[str]:
    """返回指数成分股的 ts_code 集合，忽略北交所。"""
    import akshare as ak

    codes: set[str] = set()
    try:
        if source == "sina":
            df = ak.index_stock_cons_sina(index_code)
            col = "code"
        elif source == "csindex":
            df = ak.index_stock_cons_csindex(index_code)
            col = "成分券代码"
        else:
            return codes

        for _, row in df.iterrows():
            code = str(row.get(col, ""))
            if code.startswith(("6", "0", "3")):
                codes.add(code)
    except Exception:
        pass
    return codes


# quant_agents 消费端自选股清单目录（纯文本 "代码  # 名称"，每行一只）。
# pipeline 的 watchlist 表来自自身选股扫描，覆盖不到消费端手工维护的自选股
# （如 000975 山金国际非指数成分且不在扫描结果里），故筹码任务额外读取这些
# txt，确保消费端 batch 用到的每只股票都能拿到当日筹码。
_AGENTS_WATCHLIST_DIR = os.getenv(
    "QUANT_AGENTS_WATCHLIST_DIR",
    os.path.expanduser("~/Code/quant_agents/watchlists"),
)


def _read_agents_watchlist_symbols() -> set[str]:
    """Best-effort 读取 quant_agents watchlists/*.txt 的 6 位股票代码。

    每行格式为 ``600519  # 贵州茅台``；忽略空行、注释行与非法代码。
    目录不存在或读取失败时返回空集合（不影响筹码任务主流程）。
    """
    import re
    from pathlib import Path

    symbols: set[str] = set()
    wl_dir = Path(_AGENTS_WATCHLIST_DIR)
    if not wl_dir.is_dir():
        return symbols
    for txt in wl_dir.glob("*.txt"):
        try:
            for line in txt.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                m = re.match(r"(\d{6})", line)
                if m:
                    symbols.add(m.group(1))
        except Exception:
            continue
    return symbols


def _get_chip_em_target_symbols(db: DatabaseInterface) -> list[str]:
    """获取需要线上抓取筹码分布的目标股票清单。

    优先顺序：
    1. 自选股（watchlist）
    2. 指数成分股（CSI300 / CSI500 / CSI1000，实时拉取）
    3. 兜底：本地已有筹码分布且有日线数据的活跃股票
    """
    import sqlite3

    conn = sqlite3.connect(str(db.db_path))
    symbols: set[str] = set()

    try:
        rows = conn.execute(
            "SELECT DISTINCT ts_code FROM watchlist WHERE status = 'tracking' ORDER BY ts_code"
        ).fetchall()
        symbols.update(r[0] for r in rows)
    except Exception:
        pass

    # 1b. quant_agents 消费端手工自选股（txt），覆盖非指数成分的自选标的
    agents_syms = _read_agents_watchlist_symbols()
    if agents_syms:
        symbols |= agents_syms
        logger.info(f"  合并 quant_agents 自选股 {len(agents_syms)} 只")

    for index_code, source in _CSI_INDICES:
        symbols |= _fetch_index_constituents(index_code, source)

    if len(symbols) < 200:
        try:
            rows = conn.execute(
                """
                SELECT ts_code FROM (
                    SELECT ts_code, COUNT(*) as cnt
                    FROM daily_bars
                    GROUP BY ts_code
                    HAVING cnt > 60
                )
                WHERE ts_code IN (
                    SELECT DISTINCT ts_code FROM chip_distribution
                )
                ORDER BY RANDOM()
                LIMIT ?
            """,
                (500 - len(symbols),),
            ).fetchall()
            symbols.update(r[0] for r in rows)
        except Exception:
            pass

    conn.close()
    return sorted(symbols)


@skip_if_task_locked("update_chip_distribution_em")
def update_chip_distribution_em(
    db: DatabaseInterface,
    symbols_to_update: list[str] | None = None,
    max_consecutive_failures: int = 9,
) -> dict:
    """从东方财富线上获取筹码分布数据，写入 chip_distribution_em 表。

    限量为：自选股 + 指数成分股，避免全市场 5500 次 HTTP 调用。

    Args:
        db: 数据库接口
        symbols_to_update: 指定目标股票清单，None 则自动探测
        max_consecutive_failures: 连续失败硬熔断阈值，默认 9 次

    Returns:
        统计字典
    """
    logger.info("\n" + "=" * 60)
    logger.info("任务: 线上获取东方财富筹码分布")
    logger.info("=" * 60)

    if symbols_to_update is not None:
        symbols = symbols_to_update
        logger.info(f"指定模式: 获取 {len(symbols)} 只股票")
    else:
        symbols = _get_chip_em_target_symbols(db)
        logger.info(f"自动探测: 自选股+指数成分股共 {len(symbols)} 只")

    total = len(symbols)
    if total == 0:
        logger.info("没有需要获取的股票")
        return {
            "success": 0,
            "failed": 0,
            "skipped": 0,
            "total": 0,
            "processed": 0,
            "aborted": False,
            "status": "no_data",
            "reason": "no eligible symbols",
        }

    success_count = 0
    failed_count = 0
    skipped_count = 0
    consecutive_failures = 0
    total_retry_delay = 0.0

    def _abort_payload(processed: int) -> dict:
        return {
            "success": success_count,
            "failed": failed_count,
            "skipped": skipped_count,
            "total": total,
            "processed": processed,
            "aborted": True,
            "abort_reason": "consecutive_failures",
            "status": "aborted",
        }

    for i, symbol in enumerate(symbols, 1):
        # ── 熔断：连续失败超过阈值，冷却一段时间 ──
        if consecutive_failures >= 5 and consecutive_failures % 5 == 0:
            cool_sec = min(120, 15 * (consecutive_failures // 5))
            logger.warning(
                f"  🔥 连续 {consecutive_failures} 次失败，冷却 {cool_sec}s (已耗时 {total_retry_delay:.0f}s)"
            )
            time.sleep(cool_sec)

        _t0 = time.time()
        try:
            df = _fetch_cyq_em(symbol)
        except InsufficientDataError as e:
            # 数据前置条件不满足（如北交所换手率历史尚未积累）：按跳过处理，
            # 不计连续失败、不冷却——本地判定仅 ~30ms，直接下一只
            logger.info(f"  ⏭️ {symbol} 数据不足跳过: {e}")
            skipped_count += 1
            continue
        # ── 节流仅针对线上源：本地 DB numpy 计算 (~30ms) 无需限速；
        #    耗时超过 0.5s 说明走了线上兜底（EM/雪球/新浪），限速防爆发请求 ──
        if time.time() - _t0 > 0.5:
            time.sleep(random.uniform(1.0, 2.0))
        if df is None:
            failed_count += 1
            consecutive_failures += 1
            if consecutive_failures >= max_consecutive_failures:
                logger.error(
                    "  🛑 连续 %d 次失败，触发硬熔断，跳过剩余 %d 只",
                    consecutive_failures,
                    total - i,
                )
                return _abort_payload(i)
            continue

        records = []
        rejected_rows = 0
        for _, row in df.iterrows():
            profit_ratio = float(row["profit_ratio"])
            avg_cost = float(row["avg_cost"])
            # 入库校验：NaN 或全零行 (获利比例=0 且 平均成本=0) 属于计算失败的
            # 伪数据，拒绝入库（真实市场中平均成本必然 > 0）
            if pd.isna(profit_ratio) or pd.isna(avg_cost):
                rejected_rows += 1
                continue
            if profit_ratio == 0.0 and avg_cost == 0.0:
                rejected_rows += 1
                continue
            records.append(
                {
                    "ts_code": symbol,
                    "trade_date": str(row["trade_date"]),
                    "profit_ratio": profit_ratio,
                    "avg_cost": avg_cost,
                    "cost_90_low": float(row["cost_90_low"]),
                    "cost_90_high": float(row["cost_90_high"]),
                    "concentration_90": float(row["concentration_90"]),
                    "cost_70_low": float(row["cost_70_low"]),
                    "cost_70_high": float(row["cost_70_high"]),
                    "concentration_70": float(row["concentration_70"]),
                }
            )

        if not records:
            logger.warning(
                f"  {symbol} 筹码数据全部无效 (拒绝 {rejected_rows} 行全零/NaN)，不入库"
            )
            failed_count += 1
            consecutive_failures += 1
            if consecutive_failures >= max_consecutive_failures:
                logger.error(
                    "  🛑 连续 %d 次数据全废，触发硬熔断，跳过剩余 %d 只",
                    consecutive_failures,
                    total - i,
                )
                return _abort_payload(i)
            continue

        if rejected_rows:
            logger.debug(f"  {symbol} 拒绝 {rejected_rows} 行无效筹码数据")

        try:
            db.save_chip_distribution_em_batch(records)
            success_count += 1
            # 只有真正入库才重置熔断计数器：抓取成功但数据全废 / 写库失败
            # 同样意味着这一只没有产出，清零会让熔断永远不触发
            #（2026-07 全零事故中即因此走完全市场也没能提前中止）
            consecutive_failures = 0
        except Exception as e:
            logger.warning(f"  {symbol} 保存失败: {e}")
            failed_count += 1
            consecutive_failures += 1
            if consecutive_failures >= max_consecutive_failures:
                logger.error(
                    "  🛑 连续 %d 次保存失败，触发硬熔断，跳过剩余 %d 只",
                    consecutive_failures,
                    total - i,
                )
                return _abort_payload(i)

        if i % 50 == 0 or i == total:
            logger.info(f"  进度: {i}/{total}  |  成功 {success_count}  失败 {failed_count}  跳过 {skipped_count}")

    logger.info("\n" + "=" * 60)
    logger.info("线上筹码分布获取完成")
    logger.info(f"  成功: {success_count} 只")
    logger.info(f"  失败: {failed_count} 只")
    logger.info(f"  跳过: {skipped_count} 只")
    logger.info("=" * 60)

    return {
        "success": success_count,
        "failed": failed_count,
        "skipped": skipped_count,
        "total": total,
        "processed": total,
        "aborted": False,
        # 结果契约：归一化器只认 status/saved/failed/skipped 键，
        # 不认 success——显式声明，避免成功运行被误报为 failed
        # （2026-08-03：5 只北交所全部成功却报 zero rows）
        "saved": success_count,
        "status": (
            "success"
            if success_count
            else ("degraded" if failed_count else "no_data")
        ),
    }


@skip_if_task_locked("update_chip_distribution_em_fullmarket")
def update_chip_distribution_em_fullmarket(db: DatabaseInterface) -> dict:
    """全市场模式：对 daily_bars 中所有股票跑 chip_distribution_em。

    仅在 TUI 下拉菜单中手动触发，不会自动执行。
    """
    import sqlite3

    conn = sqlite3.connect(str(db.db_path))
    all_symbols = [
        r[0]
        for r in conn.execute(
            "SELECT DISTINCT ts_code FROM daily_bars ORDER BY ts_code"
        ).fetchall()
    ]
    conn.close()

    logger.info("\n" + "=" * 60)
    logger.info("全市场筹码分布拉取（手动触发）")
    logger.info(f"共 {len(all_symbols)} 只股票")
    logger.info("=" * 60)

    import sqlite3

    _conn = sqlite3.connect(str(db.db_path))
    existing = {
        r[0]
        for r in _conn.execute("SELECT DISTINCT ts_code FROM chip_distribution_em").fetchall()
    }
    _conn.close()
    remaining = [s for s in all_symbols if s not in existing]
    logger.info(f"已有 {len(existing)} 只，还需拉取 {len(remaining)} 只")

    return update_chip_distribution_em(db, symbols_to_update=remaining)


# ===========================================================================
# 收盘刷新 helper（Task 7）：只抽目标日一行，不写库
# ===========================================================================

_CHIP_EM_REFRESH_COLUMNS = (
    "profit_ratio", "avg_cost",
    "cost_90_low", "cost_90_high", "concentration_90",
    "cost_70_low", "cost_70_high", "concentration_70",
)


def fetch_chip_em_record_for_refresh(
    symbol: str,
    target_date: str,
    *,
    fetch=None,
) -> tuple[dict | None, str | None]:
    """收盘刷新专用：抓取 EM 筹码全历史，仅抽目标日一行为写库记录。

    刷新模式下绝不调用随机目标选择器（_get_chip_em_target_symbols）；
    不写库、不重写历史。EM 源没有 chip_concentration 列，记录中不携带。

    Args:
        symbol: 股票代码。
        target_date: 目标交易日 'YYYY-MM-DD'。
        fetch: 可注入的抓取函数 symbol -> DataFrame | None，
            默认 _fetch_cyq_em。

    Returns:
        (record, None) 成功；(None, reason) 未产出记录，reason 取值：
        "fetch_failed"（源端失败，计入连续失败熔断）、
        "missing_target"（历史里没有目标日）、
        "invalid"（目标日行 NaN 或双零，视为无效数据）。
    """
    fetcher = fetch if fetch is not None else _fetch_cyq_em
    df = fetcher(symbol)
    if df is None or df.empty:
        return None, "fetch_failed"

    matches = df[df["trade_date"].astype(str).str[:10] == target_date]
    if matches.empty:
        return None, "missing_target"

    row = matches.iloc[-1]
    record: dict[str, object] = {"ts_code": symbol, "trade_date": target_date}
    for col in _CHIP_EM_REFRESH_COLUMNS:
        value = row.get(col)
        # NaN → None（SQLite 不接受 NaN）
        record[col] = None if value is None or value != value else float(value)

    profit_ratio = record["profit_ratio"]
    avg_cost = record["avg_cost"]
    if profit_ratio is None or avg_cost is None:
        return None, "invalid"
    if profit_ratio == 0.0 and avg_cost == 0.0:
        return None, "invalid"
    return record, None


# ===========================================================================
# 收盘刷新 helper（Task 9）：只抓取/归一化，不写库，源异常直接上抛
# ===========================================================================

_INDEX_DAILY_REFRESH_INDICES = {
    "sh000001": "上证指数",
    "sz399001": "深证成指",
    "sz399006": "创业板指",
    "sh000688": "科创50",
    "sh000300": "沪深300",
}


def fetch_index_daily_records() -> list[dict]:
    """收盘刷新专用：抓取四大指数全历史日线并归一化（历史型源）。

    与 legacy ``_fetch_index_daily`` 不同：返回全部历史行而非仅最后
    一行，由适配器挑选回看窗口内可接受的目标分区；指数源报错直接
    上抛，单指数返回空则跳过不产行，由适配器按静态指数全集
    （_INDEX_DAILY_REFRESH_INDICES）判定分区不完整并拒绝替换。
    """
    records: list[dict] = []
    for index_code, index_name in _INDEX_DAILY_REFRESH_INDICES.items():
        df = ak.stock_zh_index_daily_tx(symbol=index_code)
        if df is None or df.empty:
            continue
        for _, row in df.iterrows():
            trade_date = str(row.get("date", ""))[:10]
            if not trade_date:
                continue
            records.append(
                {
                    "index_code": index_code,
                    "index_name": index_name,
                    "trade_date": trade_date,
                    "open": float(row.get("open", 0)),
                    "high": float(row.get("high", 0)),
                    "low": float(row.get("low", 0)),
                    "close": float(row.get("close", 0)),
                    "volume": float(row.get("volume", 0)),
                    "data_source": "akshare",
                }
            )
    return records
