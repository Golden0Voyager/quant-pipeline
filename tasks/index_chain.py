from __future__ import annotations

import logging
import os  # noqa: F401
import random
import time

import pandas as pd

from core.calendar import get_expected_latest_trading_day
from core.config import SHARED_DATA_DIR  # noqa: F401
from core.lock import skip_if_task_locked
from core.stock_cyq_em import stock_cyq_em
from core.utils import infer_market  # noqa: F401
from interface import DatabaseInterface

try:
    import akshare as ak
except ImportError:
    ak = None

logger = logging.getLogger(__name__)


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
    except Exception as e:
        logger.warning(f"  {symbol} 东方财富筹码获取失败: {e}")
        return None


def _get_chip_em_target_symbols(db: DatabaseInterface) -> list[str]:
    """获取需要线上抓取筹码分布的目标股票清单。

    优先顺序：
    1. 自选股（watchlist）
    2. 指数成分股（通过 AkShare 实时拉取沪深 300 / 中证 500 成分股）
    3. 兜底：本地已有筹码分布且有日线数据的活跃股票
    """
    import sqlite3

    import akshare as ak

    conn = sqlite3.connect(str(db.db_path))
    symbols: set[str] = set()

    try:
        rows = conn.execute(
            "SELECT DISTINCT ts_code FROM watchlist WHERE status = 'tracking' ORDER BY ts_code"
        ).fetchall()
        symbols.update(r[0] for r in rows)
    except Exception:
        pass

    # 实时拉取指数成分股（去掉 sh/sz 前缀后以 ts_code 格式存入）
    for index_code in ("000300", "000905"):
        try:
            df = ak.index_stock_cons_sina(index_code)
            # code 列是纯数字，需补齐前缀
            for _, row in df.iterrows():
                code = str(row.get("code", ""))
                if code.startswith("6"):
                    symbols.add(code)
                elif code.startswith(("0", "3")):
                    symbols.add(code)
                elif code.startswith(("8", "4", "920")):
                    symbols.add(code)
        except Exception:
            pass

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
        }

    success_count = 0
    failed_count = 0
    skipped_count = 0
    consecutive_failures = 0
    total_retry_delay = 0.0

    for i, symbol in enumerate(symbols, 1):
        # ── 熔断：连续失败超过阈值，冷却一段时间 ──
        if consecutive_failures >= 5 and consecutive_failures % 5 == 0:
            cool_sec = min(120, 15 * (consecutive_failures // 5))
            logger.warning(
                f"  🔥 连续 {consecutive_failures} 次失败，冷却 {cool_sec}s (已耗时 {total_retry_delay:.0f}s)"
            )
            time.sleep(cool_sec)

        # ── 股票间至少间隔 1-2s，避免爆发式请求 ──
        if i > 1:
            time.sleep(random.uniform(1.0, 2.0))

        df = _fetch_cyq_em(symbol)
        if df is None:
            failed_count += 1
            consecutive_failures += 1
            if consecutive_failures >= max_consecutive_failures:
                logger.error(
                    "  🛑 连续 %d 次失败，触发硬熔断，跳过剩余 %d 只",
                    consecutive_failures,
                    total - i,
                )
                return {
                    "success": success_count,
                    "failed": failed_count,
                    "skipped": skipped_count,
                    "total": total,
                    "processed": i,
                    "aborted": True,
                    "abort_reason": "consecutive_failures",
                }
            continue

        # 成功一次就重置熔断计数器
        consecutive_failures = 0

        records = []
        for _, row in df.iterrows():
            records.append(
                {
                    "ts_code": symbol,
                    "trade_date": str(row["trade_date"]),
                    "profit_ratio": float(row["profit_ratio"]),
                    "avg_cost": float(row["avg_cost"]),
                    "cost_90_low": float(row["cost_90_low"]),
                    "cost_90_high": float(row["cost_90_high"]),
                    "concentration_90": float(row["concentration_90"]),
                    "cost_70_low": float(row["cost_70_low"]),
                    "cost_70_high": float(row["cost_70_high"]),
                    "concentration_70": float(row["concentration_70"]),
                }
            )

        try:
            db.save_chip_distribution_em_batch(records)
            success_count += 1
        except Exception as e:
            logger.warning(f"  {symbol} 保存失败: {e}")
            failed_count += 1

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
    }
