"""
东财人气榜数据更新任务
────────────────────
东方财富个股人气榜 Top-100 快照（sc/rk/rc/hisRc）。

数据源：emappdata.eastmoney.com/stockrank/getAllCurrentList（与 akshare
``stock_hot_rank_em()`` 第一步同一接口），用 SourceClient 的 curl_cffi
浏览器掩护会话直连。该端点自 2026-08 起间歇性被 WAF 拦截（返回非 JSON
触发 JSONDecodeError）， TradingAgents 每票运行时各拉一次的重试命中率
很差；改为 pipeline 每天整表抓一次，失败时保留上一交易日快照。

消费方：TradingAgents 的 eastmoney_sentiment（本地 quant_core.db 优先）。
"""
from __future__ import annotations

import logging
from typing import Any

from core.data_contract import STOCK_HOT_RANK_CONTRACT, validate_records
from core.market_time import shanghai_today
from core.source_client import get_default_client
from core.utils import to_float as _to_float
from interface import DatabaseInterface

logger = logging.getLogger(__name__)

_RANK_API_URL = "https://emappdata.eastmoney.com/stockrank/getAllCurrentList"


def _to_bare_code(sc: str) -> str:
    """``SZ002119`` / ``SH600825`` → ``002119`` / ``600825``."""
    sc = str(sc).strip()
    return sc[2:] if sc[:2] in ("SZ", "SH", "BJ") else sc


def _fetch_hot_rank() -> list[dict]:
    """抓取人气榜 Top-100 并补充最新价/涨跌幅（最佳努力）。

    Raises on HTTP/network/parse errors so that ``SourceClient.call()`` can
    handle retry and circuit-breaker logic. Quote enrichment failures only
    drop the price columns — the rank itself is the payload.
    """
    session = get_default_client().get_session("eastmoney")

    payload = {
        "appId": "appId01",
        "globalId": "786e4c21-70dc-435a-93bb-38",
        "marketType": "",
        "pageNo": 1,
        "pageSize": 100,
    }
    resp = session.post(_RANK_API_URL, json=payload, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, dict):
        raise RuntimeError(f"人气榜返回畸形响应: {type(data).__name__}")
    items = data.get("data") or []
    if not items:
        return []

    today = shanghai_today()
    records: list[dict] = []
    for item in items:
        sc = str(item.get("sc", "")).strip()
        code = _to_bare_code(sc)
        if not code:
            continue
        records.append({
            "trade_date": today,
            "code": code,
            "name": None,
            "rank": item.get("rk"),
            "rank_change": _to_float(item.get("rc")),
            "prev_rank": _to_float(item.get("hisRc")),
            "close_price": None,
            "change_pct": None,
            "data_source": "eastmoney",
        })

    # --- best-effort quote enrichment (push2 ulist, mirrors akshare) ---
    # push2.eastmoney.com 在本机被 WAF 间歇性 TLS 重置（与 concept_board
    # 同一现象），双域名 failover 后再放弃行情列——排名数据不受影响。
    try:
        mark = [
            ("0" if r["code"].startswith(("0", "3")) else "1")
            + "." + r["code"]
            for r in records
        ]
        params = {
            "ut": "f057cbcbce2a86e2866ab8877db1d059",
            "fltt": "2",
            "invt": "2",
            "fields": "f14,f3,f12,f2",
            "secids": ",".join(mark),
        }
        diffs: list[Any] = []
        for host in ("push2.eastmoney.com", "push2delay.eastmoney.com"):
            try:
                qresp = session.get(
                    f"https://{host}/api/qt/ulist.np/get",
                    params=params,
                    timeout=15,
                )
                qresp.raise_for_status()
                qdata = qresp.json()
                diffs = ((qdata.get("data") or {}).get("diff")) or []
            except Exception as exc:  # noqa: BLE001 — try failover host
                logger.debug(f"人气榜行情补充 {host} 失败: {exc}")
                continue
            if diffs:
                break
        by_code = {}
        for d in diffs:
            if not isinstance(d, dict):
                continue
            # push2 字段: f14 名称, f3 涨跌幅, f12 代码, f2 最新价
            code = str(d.get("f12", "")).zfill(6)
            if code and code != "000000":
                by_code[code] = (d.get("f14"), d.get("f3"), d.get("f2"))
        for r in records:
            quote = by_code.get(r["code"])
            if quote:
                r["name"] = str(quote[0]) or None
                r["change_pct"] = _to_float(quote[1])
                r["close_price"] = _to_float(quote[2])
    except Exception as exc:  # noqa: BLE001 — 行情补充是锦上添花
        logger.debug(f"人气榜行情补充失败（保留排名数据）: {exc}")

    return records


def fetch_hot_rank_records(trade_date: str) -> list[dict]:
    """收盘刷新专用：抓取人气榜 Top-100 快照并覆写 trade_date 为目标日。

    ``_fetch_hot_rank`` 本身即 raise 版（HTTP 错误直接上抛）；快照的
    自然日戳统一覆写为调用方指定的目标交易日。权威空返回 []。
    """
    records = _fetch_hot_rank()
    for record in records:
        record["trade_date"] = trade_date
    return records


def update_hot_rank(db: DatabaseInterface) -> dict[str, Any]:
    """获取东财人气榜 Top-100 快照并保存。"""
    logger.info("\n" + "=" * 60)
    logger.info("🔥 任务: 更新东财人气榜快照")
    logger.info("=" * 60)

    resp = get_default_client().call("eastmoney", _fetch_hot_rank)
    if not resp.success:
        error_msg = str(resp.metadata.error) if resp.metadata.error else "network error"
        logger.warning(f"⚠️ 人气榜获取失败: {error_msg}")
        return {
            "status": "retained",
            "error_kind": "network",
            "reason": f"eastmoney hot rank unavailable: {error_msg}",
            "error": error_msg,
            "retained_old_data": True,
            "saved": 0,
        }

    records = resp.data
    if not records:
        logger.warning("⚠️ 人气榜无数据")
        return {"status": "no_data", "saved": 0}

    validated, violations = validate_records(records, STOCK_HOT_RANK_CONTRACT, logger)
    if violations and not validated:
        logger.error(f"🚫 人气榜数据合约校验失败: {violations}")
        return {
            "status": "failed",
            "error_kind": "data_quality",
            "error": f"contract validation failed: {violations}",
            "saved": 0,
        }
    if violations:
        logger.warning(f"⚠️ 人气榜合约校验过滤 {len(records) - len(validated)} 条")

    try:
        saved = db.save_stock_hot_rank_batch(validated)
        top = min(validated, key=lambda r: r["rank"] or 10**9)
        logger.info(
            f"✅ 人气榜保存完成: {saved} 条 / "
            f"{validated[0]['trade_date']} / 榜首 {top['name'] or top['code']}"
        )
    except Exception as e:
        logger.warning(f"⚠️ 人气榜保存失败: {e}")
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
