"""
基金持股数据更新任务
────────────────────
从东财主力数据中心获取全市场个股的基金/QFII/社保/券商/保险/信托持股明细。
季频任务，存 fund_holdings 表。
"""

from __future__ import annotations

import logging

from interface import DatabaseInterface

try:
    import requests
except ImportError:
    requests = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)

# ── 东财数据中心配置 ──────────────────────────────────────────────────────────
_EM_DATACENTER_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"
_EM_FUND_HOLD_REPORT = "RPT_FUND_HOLD_STOCK"
_EM_FUND_HOLD_COLUMNS = (
    "SECURITY_CODE,SECURITY_NAME_ABBR,REPORT_DATE,ORG_TYPE,"
    "HOULD_NUM,TOTAL_SHARES,HOLD_VALUE,FREESHARES_RATIO,"
    "HOLDCHA,HOLDCHA_NUM,HOLDCHA_RATIO"
)
_EM_PAGE_SIZE = 500
_EM_MAX_PAGES = 20
_EM_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "https://data.eastmoney.com/zlsj/",
}

# 机构类型映射：API 代码 → 标准缩写
_ORG_TYPE_MAP: dict[str, str] = {
    "01": "fund",
    "02": "qfii",
    "03": "social",
    "04": "broker",
    "05": "insurance",
    "06": "trust",
}


def _sina_to_ts_code(code: str) -> str:
    """将东财 6 位代码转换为 ts_code 格式（如 300750 → 300750.SZ）。"""
    code = str(code).strip()
    if not code or len(code) != 6:
        return code
    prefix = code[0]
    if prefix in ("6", "9"):
        return f"{code}.SH"
    return f"{code}.SZ"


def _fetch_fund_holdings_page(page: int, timeout: float = 30.0) -> tuple[list[dict], int]:
    """抓取东财基金持股单页数据，返回 (records, total_pages)。"""
    if requests is None:
        return [], 0
    params = {
        "reportName": _EM_FUND_HOLD_REPORT,
        "columns": _EM_FUND_HOLD_COLUMNS,
        "pageSize": str(_EM_PAGE_SIZE),
        "pageNumber": str(page),
        "sortColumns": "HOULD_NUM",
        "sortTypes": "-1",
        "source": "WEB",
        "client": "WEB",
    }
    try:
        resp = requests.get(
            _EM_DATACENTER_URL, params=params,
            headers=_EM_HEADERS, timeout=timeout,
        )
        resp.raise_for_status()
        payload = resp.json()
    except (requests.RequestException, ValueError) as e:
        logger.warning(f"⚠️ 基金持股第 {page} 页获取失败: {e}")
        return [], 0
    result = payload.get("result") or {}
    rows = result.get("data") or []
    total_pages = result.get("pages") or 1
    records: list[dict] = []
    for row in rows:
        code = str(row.get("SECURITY_CODE", "")).strip()
        if not code:
            continue
        org_type_raw = str(row.get("ORG_TYPE", "")).strip()
        institution_type = _ORG_TYPE_MAP.get(org_type_raw, org_type_raw)
        report_date_raw = str(row.get("REPORT_DATE", ""))
        report_date = report_date_raw[:10] if report_date_raw else ""
        records.append(
            {
                "ts_code": _sina_to_ts_code(code),
                "stock_name": str(row.get("SECURITY_NAME_ABBR", "") or "").strip(),
                "report_date": report_date,
                "institution_type": institution_type,
                "fund_count": row.get("HOULD_NUM"),
                "total_shares": row.get("TOTAL_SHARES"),
                "hold_value": row.get("HOLD_VALUE"),
                "hold_ratio": row.get("FREESHARES_RATIO"),
                "update_kind": str(row.get("HOLDCHA", "") or "").strip() or None,
                "update_shares": row.get("HOLDCHA_NUM"),
                "update_ratio": row.get("HOLDCHA_RATIO"),
                "data_source": "eastmoney",
            }
        )
    return records, total_pages


def _fetch_all_fund_holdings(timeout: float = 30.0) -> list[dict]:
    """获取全市场基金持股明细（分页拉取）。"""
    all_records: list[dict] = []
    page = 1
    while page <= _EM_MAX_PAGES:
        records, total_pages = _fetch_fund_holdings_page(page, timeout)
        all_records.extend(records)
        if page >= total_pages or not records:
            break
        page += 1
    return all_records


def update_fund_holdings(db: DatabaseInterface) -> dict:
    """获取基金持股数据并保存（季频，每日幂等 upsert）。"""
    logger.info("\n" + "=" * 60)
    logger.info("📊 任务: 更新基金持股明细")
    logger.info("=" * 60)

    try:
        records = _fetch_all_fund_holdings()
        if not records:
            logger.warning("⚠️ 基金持股无数据")
            return {"saved": 0, "total": 0}
        saved = db.save_fund_holdings_batch(records)
        dates = {r["report_date"] for r in records if r.get("report_date")}
        latest = max(dates) if dates else ""
        types = {r["institution_type"] for r in records}
        logger.info(
            f"✅ 基金持股保存完成: {saved} 条 "
            f"(报告期 {latest}, 机构类型: {', '.join(sorted(types))})"
        )
        return {"saved": saved, "total": len(records)}
    except Exception as e:
        logger.error(f"❌ 基金持股更新失败: {e}")
        return {"saved": 0, "error": str(e), "error_kind": "network"}
