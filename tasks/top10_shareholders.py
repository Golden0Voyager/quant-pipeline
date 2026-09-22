"""
十大股东数据更新任务
────────────────────
来源: 新浪财经 stock_main_stock_holder()
写入 top10_shareholders 表。季频任务。

API 返回单只股票全部历史十大股东记录，需按"截至日期"取最新报告期。
"""

from __future__ import annotations

import logging
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

from interface import DatabaseInterface

try:
    import akshare as ak
except ImportError:
    ak = None

logger = logging.getLogger(__name__)

_MAX_WORKERS = 20
_RATE_LIMIT_MIN = 0.05
_RATE_LIMIT_MAX = 0.15


def _sina_to_ts_code(code: str) -> str:
    """将 Sina 裸 6 位码转换为 ts_code（如 000001 → 000001.SZ）。"""
    code = str(code).strip()
    if len(code) != 6:
        return code
    prefix = code[0]
    if prefix in ("6", "9"):
        return f"{code}.SH"
    return f"{code}.SZ"


def _fetch_single_top10(symbol: str) -> list[dict] | None:
    """获取单只股票最新一期的十大股东，失败返回 None。"""
    try:
        df = ak.stock_main_stock_holder(stock=symbol)
    except Exception as e:
        logger.warning(f"⚠️ 十大股东 {symbol} 获取失败: {e}")
        return None
    if df is None or df.empty:
        return []
    # 取最新报告期
    date_col = "截至日期"
    if date_col not in df.columns:
        return []
    latest_date = df[date_col].max()
    latest = df[df[date_col] == latest_date].copy()
    if latest.empty:
        return []
    records: list[dict] = []
    for _, row in latest.iterrows():
        rank_raw = row.get("编号")
        try:
            rank = int(float(rank_raw)) if pd.notna(rank_raw) else 0
        except (ValueError, TypeError):
            rank = 0
        holder_name = str(row.get("股东名称") or "").strip()
        if not holder_name:
            continue
        shares_raw = row.get("持股数量")
        try:
            shares = float(shares_raw) if pd.notna(shares_raw) else None
        except (ValueError, TypeError):
            shares = None
        ratio_raw = row.get("持股比例")
        try:
            ratio = float(ratio_raw) if pd.notna(ratio_raw) else None
        except (ValueError, TypeError):
            ratio = None
        nature = str(row.get("股本性质") or "").strip()
        announcement = str(row.get("公告日期") or "").strip()[:10]
        records.append(
            {
                "ts_code": _sina_to_ts_code(symbol),
                "report_date": str(latest_date)[:10],
                "holder_rank": rank,
                "holder_name": holder_name,
                "shares_held": shares,
                "share_ratio": ratio,
                "share_nature": nature,
                "announcement_date": announcement or None,
                "data_source": "sina",
            }
        )
    return records


def update_top10_shareholders(db: DatabaseInterface) -> dict:
    """获取全市场十大股东并保存（季频，每只股票取最新一期）。"""
    logger.info("\n" + "=" * 60)
    logger.info("👥 任务: 更新十大股东")
    logger.info("=" * 60)

    try:
        stock_list = db.get_stock_list()
    except Exception as e:
        logger.error(f"❌ 获取股票列表失败: {e}")
        return {"saved": 0, "error": str(e), "error_kind": "network"}

    if stock_list is None or stock_list.empty:
        logger.warning("⚠️ 股票列表为空，跳过十大股东更新")
        return {"skipped": True, "reason": "stock list empty"}

    symbols = [
        str(c).strip()
        for c in stock_list["code"]
        if c and str(c).strip()
    ]
    total = len(symbols)
    logger.info(f"📋 准备更新 {total} 只股票的十大股东")

    all_records: list[dict] = []
    failed: list[str] = []

    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as executor:
        futures = {executor.submit(_fetch_single_top10, s): s for s in symbols}
        completed = 0
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                records = future.result(timeout=30)
            except Exception as e:
                logger.warning(f"⚠️ 十大股东 {symbol} 线程异常: {e}")
                failed.append(symbol)
                completed += 1
                if completed % 500 == 0:
                    logger.info(
                        f"  进度: {completed}/{total}, "
                        f"成功 {len(all_records)} 条, 失败 {len(failed)} 只"
                    )
                continue
            if records is None:
                failed.append(symbol)
            else:
                all_records.extend(records)
            completed += 1
            if completed % 500 == 0:
                logger.info(
                    f"  进度: {completed}/{total}, "
                    f"成功 {len(all_records)} 条, 失败 {len(failed)} 只"
                )
            # 速率控制：每只请求间隔 50-150ms 随机
            if completed < total:
                time.sleep(random.uniform(_RATE_LIMIT_MIN, _RATE_LIMIT_MAX))

    if not all_records:
        logger.warning("⚠️ 十大股东无数据")
        return {"saved": 0, "total": 0, "failed": len(failed)}

    saved = db.save_top10_shareholders_batch(all_records)
    dates = {r["report_date"] for r in all_records if r.get("report_date")}
    latest = max(dates) if dates else ""
    logger.info(
        f"✅ 十大股东保存完成: {saved} 条 "
        f"(最新报告期 {latest}, 失败 {len(failed)} 只)"
    )
    return {"saved": saved, "total": len(all_records), "failed": len(failed)}
