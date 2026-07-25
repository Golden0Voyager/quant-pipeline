"""
财务数据更新任务
────────────────
从 daily_pipeline.py 提取：股东户数、季度财务数据、行业分类。
"""

from __future__ import annotations

import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError, as_completed, wait
from datetime import datetime

from core.lock import skip_if_task_locked
from core.source_client import get_default_client
from core.utils import should_skip_beijing
from interface import DatabaseInterface, DataLoaderInterface

try:
    import akshare as ak
except ImportError:
    ak = None

logger = logging.getLogger(__name__)


def update_shareholder_count(db: DatabaseInterface, symbols: list[str] | None = None) -> dict:
    """批量获取最新季度股东户数并保存。"""
    if symbols:
        logger.info(f"  --symbols 过滤：{len(symbols)} 只")
    logger.info("\n" + "=" * 60)
    logger.info("👥 任务: 批量获取股东户数")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "total": 0, "error": "akshare not installed"}

    # 计算最近的报告期（0331, 0630, 0930, 1231）
    now = datetime.now()
    year = now.year
    month = now.month
    if month >= 11:
        period = f"{year}0930"
    elif month >= 8:
        period = f"{year}0630"
    elif month >= 5:
        period = f"{year}0331"
    else:
        period = f"{year - 1}0930"

    try:
        df = ak.stock_hold_num_cninfo(date=period)
        if df is None or df.empty:
            logger.warning(f"⚠️  股东户数无数据 ({period})")
            return {"saved": 0, "total": 0}

        batch_records = []
        for _, row in df.iterrows():
            try:
                code = str(row.get("证券代码", "")).strip()
                if not code:
                    continue
                batch_records.append(
                    {
                        "ts_code": code,
                        "report_date": period,
                        "holder_count": row.get("本期股东人数"),
                        "holder_count_change_pct": row.get("股东人数增幅"),
                        "avg_shares_per_holder": row.get("本期人均持股数量"),
                        "data_source": "akshare",
                    }
                )
            except Exception:
                continue

        if symbols:
            symbol_set = set(symbols)
            before = len(batch_records)
            batch_records = [r for r in batch_records if r["ts_code"] in symbol_set]
            logger.info(f"  --symbols 过滤：{len(batch_records)}/{before} 只")

        saved = db.save_shareholder_count_batch(batch_records) if batch_records else 0
        logger.info(f"✅ 股东户数保存完成: {saved}/{len(df)} ({period})")
        return {"saved": saved, "total": len(df)}
    except Exception as e:
        logger.error(f"❌ 股东户数获取失败: {e}")
        return {"saved": 0, "total": 0, "error": str(e)}


def update_quarterly_financials(db: DatabaseInterface, loader: DataLoaderInterface, symbols: list[str] | None = None) -> dict:
    """批量获取全市场（或指定股票）季度财务数据并保存。"""
    if symbols:
        logger.info(f"  --symbols 过滤：{len(symbols)} 只")
    logger.info("\n" + "=" * 60)
    logger.info("📋 任务: 批量获取季度财务数据")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "failed": 0, "total": 0, "error": "akshare not installed"}

    if symbols:
        stock_codes = symbols[:]
    else:
        stocks = db.get_stock_list()
        if stocks.empty:
            logger.error("❌ 股票列表为空")
            return {"saved": 0, "failed": 0, "total": 0}

        stock_codes = [c for c in stocks["code"].tolist() if not should_skip_beijing(c)]
    total = len(stock_codes)
    saved = 0
    failed = 0
    batch_chunk = 500

    import pandas as _pd

    def _extract_float(df, label):
        try:
            mask = df["指标"] == label
            if mask.any():
                val = df.loc[mask].iloc[:, 2]
                if _pd.notna(val.iloc[0]):
                    return float(val.iloc[0])
        except Exception:
            pass
        return None

    def _fetch_one(code: str) -> tuple[dict | None, bool]:
        try:
            time.sleep(0.03)  # 控制请求频率，降低被限流风险
            df = ak.stock_financial_abstract(symbol=code)
            if df is not None and not df.empty and len(df.columns) > 2:
                record = {
                    "ts_code": code,
                    "report_period": str(df.columns[2]),
                    "revenue": _extract_float(df, "营业总收入"),
                    "net_profit": _extract_float(df, "归母净利润"),
                    "operating_cashflow": _extract_float(df, "经营现金流量净额"),
                    "roe": _extract_float(df, "净资产收益率(ROE)"),
                    "gross_margin": _extract_float(df, "毛利率"),
                    "net_margin": _extract_float(df, "销售净利率"),
                    "revenue_growth": _extract_float(df, "营业总收入增长率"),
                    "profit_growth": _extract_float(df, "归属母公司净利润增长率"),
                    "debt_ratio": _extract_float(df, "资产负债率"),
                    "eps": _extract_float(df, "基本每股收益"),
                    "bps": _extract_float(df, "每股净资产"),
                }
                return record, False
        except Exception as e:
            logger.debug(f"  股票 {code} 失败: {e}")
            return None, True
        return None, False

    batch_buffer: list[dict] = []
    workers = min(8, max(4, (os.cpu_count() or 2) + 2))
    submit_batch_size = 500  # 分批提交，避免 futures 无限堆积
    with ThreadPoolExecutor(max_workers=workers) as executor:
        processed = 0
        for batch_start in range(0, len(stock_codes), submit_batch_size):
            batch_codes = stock_codes[batch_start:batch_start + submit_batch_size]
            fut_map = {executor.submit(_fetch_one, code): code for code in batch_codes}
            for future in as_completed(fut_map):
                try:
                    record, is_failed = future.result(timeout=30)
                except TimeoutError:
                    logger.warning(f"  ⏰ 股票 {fut_map[future]} 超时，跳过")
                    failed += 1
                    continue
                except Exception:
                    failed += 1
                    continue
                if record:
                    batch_buffer.append(record)
                    saved += 1
                if is_failed:
                    failed += 1

                if len(batch_buffer) >= batch_chunk:
                    db.save_quarterly_financials_batch(batch_buffer)
                    batch_buffer.clear()

            processed += len(batch_codes)
            logger.info(f"  进度: {processed}/{total} (成功: {saved}, 失败: {failed})")

    if batch_buffer:
        db.save_quarterly_financials_batch(batch_buffer)

    logger.info(f"✅ 季度财务数据保存完成: {saved}/{total} (失败: {failed})")
    return {"saved": saved, "failed": failed, "total": total}


@skip_if_task_locked("update_industry")
def update_industry(db: DatabaseInterface) -> dict:
    """
    批量更新 stock_list.industry 列。

    策略 A: eastmoney F10 CompanySurvey API (SZ/SH 主板/创业板/科创板)
    提取 申万行业 (jbzl.sshy)。
    策略 B (回退): AkShare stock_individual_info_em 接口。
    策略 C (回退): Sina 财经个股资料页 (覆盖 BJ 及部分新上市股票)
    使用 ThreadPoolExecutor 并发加速，但控制并发数以避免被限流。
    """
    logger.info("\n" + "=" * 60)
    logger.info("🏢 任务: 更新行业分类 (F10 API)")
    logger.info("=" * 60)

    import sqlite3

    # 1. 读取需要更新的股票
    conn = sqlite3.connect(str(db.db_path))
    cursor = conn.cursor()
    cursor.execute("SELECT code, market FROM stock_list WHERE industry IS NULL OR industry = '未分类'")
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        logger.info("✅ 所有股票已有行业分类")
        return {"saved": 0, "total": 0}

    total = len(rows)
    logger.info(f"📊 共 {total} 只股票需要更新行业")

    # 2. 市场前缀映射
    exchange_map = {
        "sz": "SZ",
        "gem": "SZ",
        "sh": "SH",
        "star": "SH",
        "unknown": "BJ",
        "bj": "BJ",
    }

    # 3. 网络请求策略（依次降级）
    # F10 限流探测标志：连续限流时整轮跳过 F10 API，避免浪费时间
    _f10_blocked = threading.Event()

    def _fetch_industry(code: str, market: str) -> tuple[str, str | None]:
        prefix = exchange_map.get(market, "SZ")
        api_code = f"{prefix}{code}"

        if not _f10_blocked.is_set():
            f10_url = f"https://emweb.securities.eastmoney.com/PC_HSF10/CompanySurvey/CompanySurveyAjax?code={api_code}"
            f10_rate_limited = False  # 跟踪是否遇到 HTTP 限流
            for attempt in range(3):
                session = None
                try:
                    session = get_default_client().get_session("eastmoney")
                    resp = session.get(
                        f10_url,
                        headers={"User-Agent": "Mozilla/5.0"},
                        timeout=10,
                    )
                    if resp.status_code == 200:
                        data = resp.json()
                        jbzl = data.get("jbzl")
                        if isinstance(jbzl, dict):
                            industry = jbzl.get("sshy")
                            if industry and industry != "N/A":
                                return code, industry
                    if resp.status_code in (403, 429, 503):
                        f10_rate_limited = True
                        time.sleep(2**attempt)
                except Exception:
                    if attempt < 2:
                        time.sleep(2**attempt)
                finally:
                    if session is not None:
                        session.close()
            # 仅当遇到 HTTP 限流(403/429/503)时才全局熔断
            # 普通解析错误、单股票 404、连接超时不阻断后续股票
            if f10_rate_limited:
                _f10_blocked.set()
        else:
            logger.debug(f"  F10 API 已被限流，{code} 跳过直接走备用源")

        if ak is not None:
            try:
                df = ak.stock_individual_info_em(symbol=code)
                if df is not None and not df.empty:
                    industry_row = df[df.iloc[:, 0] == "行业"]
                    if not industry_row.empty:
                        industry = str(industry_row.iloc[0, 1]).strip()
                        if industry and industry != "nan":
                            return code, industry
            except Exception:
                pass

        try:
            sin_url = f"http://money.finance.sina.com.cn/corp/go.php/vCI_CorpOtherInfo/stockid/{code}.phtml"
            session = get_default_client().get_session("sina")
            resp = session.get(
                sin_url,
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=10,
            )
            resp.encoding = "gb2312"
            if resp.status_code == 200:
                import re

                m = re.search(
                    r"所属行业板块[^<]*</td>(?:\s*</tr>\s*<tr>)?\s*<td[^>]*>\s*([^<]+?)\s*</td>",
                    resp.text,
                    re.DOTALL,
                )
                if m:
                    industry = m.group(1).strip()
                    if industry and "备注" not in industry:
                        return code, industry
        except Exception:
            pass
        finally:
            session.close()

        return code, None

    # 4. 分批并发执行（防止单线程挂死导致整体卡住）
    success_map: dict[str, str] = {}
    fail_list: list[str] = []
    processed = 0
    batch_size = 50  # 减小批次，避免限流时损失过多
    batch_timeout = 600  # 每批最多等 10 分钟（给重试留足时间）
    batch_cooldown = 30  # 批间冷却 30s，降低被限流概率

    max_workers = 4
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        for batch_start in range(0, len(rows), batch_size):
            batch = rows[batch_start : batch_start + batch_size]
            fut_map = {}
            for code, market in batch:
                fut = pool.submit(_fetch_industry, code, market)
                fut_map[fut] = code

            # 等待本批完成或超时
            done_set, pending_set = wait(fut_map, timeout=batch_timeout)

            # 处理已完成的任务
            for fut in done_set:
                code = fut_map[fut]
                try:
                    result = fut.result(timeout=5)
                    if result and result[1]:
                        success_map[code] = result[1]
                    else:
                        fail_list.append(code)
                except Exception:
                    fail_list.append(code)

            # 超时未完成的视为失败，尝试取消
            for fut in pending_set:
                code = fut_map[fut]
                fut.cancel()
                fail_list.append(code)

            processed += len(batch)
            if processed % 500 == 0 or processed == total:
                logger.info(f"  进度: {processed}/{total} (成功: {len(success_map)}, 失败: {len(fail_list)})")

            # 批间冷却，降低限流概率
            if batch_start + batch_size < len(rows):
                time.sleep(batch_cooldown)

    logger.info(f"📊 接口请求完成: 成功 {len(success_map)}, 失败 {len(fail_list)}")

    # 5. 批量写入数据库
    if success_map:
        conn = sqlite3.connect(str(db.db_path))
        cursor = conn.cursor()
        updated = 0
        batch = []
        for code, industry in success_map.items():
            batch.append((industry, code))
            if len(batch) >= 500:
                cursor.executemany(
                    "UPDATE stock_list SET industry = ?, updated_at = CURRENT_TIMESTAMP "
                    "WHERE code = ? AND (industry IS NULL OR industry = '未分类')",
                    batch,
                )
                updated += cursor.rowcount
                conn.commit()
                batch = []
        if batch:
            cursor.executemany(
                "UPDATE stock_list SET industry = ?, updated_at = CURRENT_TIMESTAMP "
                "WHERE code = ? AND (industry IS NULL OR industry = '未分类')",
                batch,
            )
            updated += cursor.rowcount
            conn.commit()
        conn.close()
        logger.info(f"✅ 行业分类更新完成: {updated} 只股票")
    else:
        updated = 0
        logger.warning("⚠️  未获取到任何行业数据")

    # 6. 更新 stock_list 表索引（如果不存在）
    try:
        conn = sqlite3.connect(str(db.db_path))
        conn.execute("CREATE INDEX IF NOT EXISTS idx_stock_list_industry ON stock_list(industry)")
        conn.close()
    except Exception as e:
        logger.warning(f"⚠️ 创建索引失败（可能已存在）: {e}")

    return {
        "saved": updated,
        "total": total,
        "failed": len(fail_list),
        "coverage_pct": round(100 * updated / total, 1) if total else 0,
    }
