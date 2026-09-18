"""
定增公告数据更新任务
────────────────────
来源: stock_qbzf_em() (东方财富，全量增发列表，无日期参数)
筛选: 仅保留 发行方式 == 定向增发
模板: tasks/stock_repurchase.py（全量快照 + 稳定源键）+
      tasks/market_flow.py update_dragon_tiger（超时护栏 / --symbols /
      结果契约 / error_kind=network）
"""

from __future__ import annotations

import logging
import re
from concurrent.futures import ThreadPoolExecutor

import pandas as pd

from core.source_client import get_default_client
from interface import DatabaseInterface

try:
    import akshare as ak
except ImportError:
    ak = None

logger = logging.getLogger(__name__)

# 实测 akshare stock_qbzf_em() 列名（2026-09-18 探测，全表 5887 行 / 约 96s）
_COLUMN_MAP = {
    "股票代码": "ts_code",
    "股票简称": "name",
    "发行方式": "issue_method",
    "发行日期": "issue_date",
    "ts_code": "ts_code",
    "name": "name",
    "issue_method": "issue_method",
    "issue_date": "issue_date",
}

_TARGET_METHOD = "定向增发"
_CODE_PATTERN = re.compile(r"(\d{6})")
_FETCH_TIMEOUT = 180.0


def _to_symbol(code: str) -> str:
    """6 位裸码 → 交易所前缀代码（项目市场前缀规则）。"""
    if code.startswith(("6", "9")):
        return f"sh{code}"
    if code.startswith(("4", "8")) or code.startswith("920"):
        return f"bj{code}"
    return f"sz{code}"


def _records_from_df(df: pd.DataFrame) -> list[dict]:
    """归一化全量增发 DataFrame → 定增公告记录（含稳定源键）。

    供日常任务与收盘刷新共用；上游抓取在调用方完成，本函数不触网。
    """
    df = df.rename(columns=_COLUMN_MAP)
    keep = {"ts_code", "name", "issue_method", "issue_date"}
    available = [c for c in keep if c in df.columns]
    df = df[available].copy()
    if "issue_method" in df.columns:
        df = df[df["issue_method"].astype(str).str.strip() == _TARGET_METHOD]
    if df.empty:
        return []
    if "issue_date" in df.columns:
        df["issue_date"] = pd.to_datetime(df["issue_date"], errors="coerce")
        df = df[df["issue_date"].notna()]
        df["issue_date"] = df["issue_date"].dt.strftime("%Y-%m-%d")

    from core.source_record_key import PLACEMENT_SOURCE_KEY_FIELDS, source_record_key

    records: list[dict] = []
    for _, row in df.iterrows():
        raw_code = str(row.get("ts_code", "")).strip()
        match = _CODE_PATTERN.search(raw_code)
        if not match:
            continue
        code = match.group(1)
        record = {
            "ts_code": code,
            "symbol": _to_symbol(code),
            "name": str(row.get("name", "")).strip() or None,
            "issue_method": _TARGET_METHOD,
            "issue_date": row.get("issue_date"),
            "data_source": "akshare",
        }
        record["source_record_key"] = source_record_key(record, PLACEMENT_SOURCE_KEY_FIELDS)
        records.append(record)
    return records


def update_placement_announcements(
    db: DatabaseInterface, symbols: list[str] | None = None
) -> dict:
    """获取定增公告数据并保存。老公告靠 source_record_key 去重，天然增量续跑。"""
    if symbols:
        logger.info(f"  --symbols 过滤：{len(symbols)} 只")
    logger.info("\n" + "=" * 60)
    logger.info("📋 任务: 更新定增公告数据")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "error": "akshare not installed"}

    try:
        # 超时护栏：实测 stock_qbzf_em 全量约 96s（12 页分页），源端挂起时
        # 由超时切断；超时的 future 无法强杀，shutdown(wait=False) 放弃它
        pool = ThreadPoolExecutor(max_workers=1)
        try:
            resp = pool.submit(
                lambda: get_default_client().call("eastmoney", ak.stock_qbzf_em)
            ).result(timeout=_FETCH_TIMEOUT)
        finally:
            pool.shutdown(wait=False)
        if not resp.success:
            logger.error(f"❌ 定增公告获取失败: {resp.metadata.error}")
            return {"saved": 0, "total": 0,
                    "error": resp.metadata.error or "fetch failed",
                    "error_kind": "network"}
        df = resp.data
        if df is None or (hasattr(df, "empty") and df.empty):
            logger.warning("⚠️ 定增公告数据为空")
            # 显式 skipped：零行属正常结果（上游无新公告），避免被结果契约误判为 failed
            return {"skipped": True, "reason": "no placement data from upstream", "total": 0}

        raw_count = len(df)
        records = _records_from_df(df)
        if not records:
            logger.warning("⚠️ 定增公告记录为空")
            # 显式 skipped：上游有数据但无定向增发/无有效日期属正常结果
            return {"skipped": True, "reason": "no valid placement records", "total": raw_count}

        if symbols:
            symbol_set = {s.split(".")[0] for s in symbols}
            before = len(records)
            records = [r for r in records if r["ts_code"] in symbol_set]
            logger.info(f"  --symbols 过滤：{len(records)}/{before} 只")

        saved = db.save_placement_batch(records) if records else 0
        logger.info(f"✅ 定增公告保存完成: {saved}/{raw_count}")
        return {"saved": saved, "total": raw_count}
    except Exception as e:
        logger.error(f"❌ 定增公告获取失败: {e}")
        # error_kind=network：本任务的失败模式即源端网络问题（含上方超时护栏
        # 抛出的 TimeoutError），标记后 runner 会自动重试一次
        return {"saved": 0, "total": 0, "error": str(e), "error_kind": "network"}


# ===========================================================================
# 收盘刷新 helper（Task 9）：只抓取/归一化，不写库，源异常直接上抛
# ===========================================================================


def fetch_placement_announcements_records() -> list[dict]:
    """收盘刷新专用：抓取全量增发快照并归一化，附 64 位稳定源键。

    权威空返回 []；源异常直接上抛（保留旧数据的语义由适配器/编排器落实）。
    """
    df = ak.stock_qbzf_em()
    if df is None or df.empty:
        return []
    return _records_from_df(df)
