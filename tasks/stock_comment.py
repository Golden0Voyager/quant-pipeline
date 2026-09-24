"""
千股千评数据更新任务
────────────────────
东方财富数据中心-特色数据-千股千评全市场快照（综合得分/排名/关注指数等）。

数据源：datacenter-web.eastmoney.com（report RPT_DMSK_TS_STOCKNEW），
与 akshare ``stock_comment_em()`` 同一接口，但用 SourceClient 的
curl_cffi 浏览器掩护会话直连，规避 akshare 硬编码子域名被 WAF 封锁的问题
（与 tasks/concept_board.py 同一策略），整表分页抓取、每天一次。

消费方：TradingAgents 的 eastmoney_sentiment（本地 quant_core.db 优先）。
"""
from __future__ import annotations

import logging
from typing import Any

from core.data_contract import STOCK_COMMENT_CONTRACT, validate_records
from core.source_client import get_default_client
from core.utils import to_float as _to_float
from interface import DatabaseInterface

logger = logging.getLogger(__name__)

_API_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"
_PAGE_SIZE = 500
_REPORT = "RPT_DMSK_TS_STOCKNEW"


def _fetch_stock_comment() -> list[dict]:
    """抓取千股千评全市场快照（分页，直到短页或到达总页数）。

    Raises on HTTP/network/parse errors so that ``SourceClient.call()`` can
    handle retry and circuit-breaker logic.
    """
    session = get_default_client().get_session("eastmoney")
    params = {
        "sortColumns": "SECURITY_CODE",
        "sortTypes": "1",
        "pageSize": str(_PAGE_SIZE),
        "pageNumber": "1",
        "reportName": _REPORT,
        "quoteColumns": (
            "f2~01~SECURITY_CODE~CLOSE_PRICE,"
            "f8~01~SECURITY_CODE~TURNOVERRATE,"
            "f3~01~SECURITY_CODE~CHANGE_RATE,"
            "f9~01~SECURITY_CODE~PE_DYNAMIC"
        ),
        "columns": "ALL",
        "filter": "",
        "token": "894050c76af8597a853f5b408b759f5d",
    }

    records: list[dict] = []
    page = 1
    total_pages: int | None = None
    while True:
        params["pageNumber"] = str(page)
        resp = session.get(_API_URL, params=params, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, dict):
            raise RuntimeError(f"千股千评返回畸形响应: {type(data).__name__}")
        result = data.get("result") or {}
        pages = result.get("pages")
        if pages is None:
            raise RuntimeError(f"千股千评响应缺 result.pages: {str(data)[:200]}")
        total_pages = int(pages)
        items = result.get("data") or []
        for item in items:
            code = str(item.get("SECURITY_CODE", "")).strip()
            trade_date = str(item.get("TRADE_DATE", ""))[:10]
            if not code or not trade_date or trade_date in ("None", ""):
                continue
            records.append({
                "trade_date": trade_date,
                "code": code,
                "name": str(item.get("SECURITY_NAME_ABBR", "")).strip() or None,
                "close_price": _to_float(item.get("CLOSE_PRICE")),
                "change_pct": _to_float(item.get("CHANGE_RATE")),
                "turnover": _to_float(item.get("TURNOVERRATE")),
                "pe_dynamic": _to_float(item.get("PE_DYNAMIC")),
                "prime_cost": _to_float(item.get("PRIME_COST")),
                "org_participation": _to_float(item.get("ORG_PARTICIPATE")),
                "composite_score": _to_float(item.get("TOTALSCORE")),
                "rank_up": _to_float(item.get("RANK_UP")),
                "rank": _to_float(item.get("RANK")),
                "focus_index": _to_float(item.get("FOCUS")),
                "data_source": "eastmoney",
            })
        if page >= total_pages or len(items) < _PAGE_SIZE:
            break
        page += 1

    return records


def fetch_stock_comment_records(trade_date: str) -> list[dict]:
    """收盘刷新专用：抓取千股千评整表快照并覆写 trade_date 为目标日。

    ``_fetch_stock_comment`` 本身即 raise 版（HTTP 错误直接上抛）；快照的
    自然日戳统一覆写为调用方指定的目标交易日。权威空返回 []。
    """
    records = _fetch_stock_comment()
    for record in records:
        record["trade_date"] = trade_date
    return records


def update_stock_comment(db: DatabaseInterface) -> dict[str, Any]:
    """获取千股千评全市场快照并保存。"""
    logger.info("\n" + "=" * 60)
    logger.info("💬 任务: 更新千股千评快照")
    logger.info("=" * 60)

    resp = get_default_client().call("eastmoney", _fetch_stock_comment)
    if not resp.success:
        error_msg = str(resp.metadata.error) if resp.metadata.error else "network error"
        logger.warning(f"⚠️ 千股千评获取失败: {error_msg}")
        return {
            "status": "retained",
            "error_kind": "network",
            "reason": f"eastmoney stock_comment unavailable: {error_msg}",
            "error": error_msg,
            "retained_old_data": True,
            "saved": 0,
        }

    records = resp.data
    if not records:
        logger.warning("⚠️ 千股千评无数据")
        return {"status": "no_data", "saved": 0}

    validated, violations = validate_records(records, STOCK_COMMENT_CONTRACT, logger)
    if violations and not validated:
        logger.error(f"🚫 千股千评数据合约校验失败: {violations}")
        return {
            "status": "failed",
            "error_kind": "data_quality",
            "error": f"contract validation failed: {violations}",
            "saved": 0,
        }
    if violations:
        logger.warning(f"⚠️ 千股千评合约校验过滤 {len(records) - len(validated)} 条")

    try:
        saved = db.save_stock_comment_batch(validated)
        dates = {r["trade_date"] for r in validated}
        logger.info(f"✅ 千股千评保存完成: {saved} 条 / 交易日 {sorted(dates)}")
    except Exception as e:
        logger.warning(f"⚠️ 千股千评保存失败: {e}")
        return {
            "status": "failed",
            "error_kind": "internal",
            "error": str(e),
            "saved": 0,
        }

    return {
        "saved": saved,
        "status": "success" if saved > 0 else "no_data",
    }
