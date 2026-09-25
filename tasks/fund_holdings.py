"""
基金持股数据更新任务
────────────────────
从东财数据中心获取全市场个股的基金/QFII/社保/券商/保险/信托持股明细。
季频任务，存 fund_holdings 表。

接口说明
--------
使用东财「数据中心-主力数据-持仓」列表接口
``http://data.eastmoney.com/dataapi/zlsj/list``（与 akshare
``stock_report_fund_hold`` 同源），按机构类型 ``type`` 1..6 与报告期 ``date``
分页拉取。

历史坑：旧实现调用 ``datacenter-web.eastmoney.com/api/data/v1/get`` +
``reportName=RPT_FUND_HOLD_STOCK``，该报表在服务端不存在（返回 200 +
``{"message":"报表配置不存在"}``，``result`` 为 None），被静默吞成 0 行，
结果契约判 ``failed: zero rows without explanation``，且 fund_holdings 表从未
成功写入。详见 tests/test_fund_holdings.py。
"""

from __future__ import annotations

import logging
from datetime import date
from types import ModuleType

from interface import DatabaseInterface

# 显式声明使 `except ImportError: requests = None` 类型可收窄（原为 ignore[assignment]）
requests: ModuleType | None

try:
    import requests
except ImportError:
    requests = None

logger = logging.getLogger(__name__)

# ── 东财数据中心（主力数据-持仓）配置 ─────────────────────────────────────────
_EM_ZLSJ_URL = "http://data.eastmoney.com/dataapi/zlsj/list"
# 机构类型：1 基金 / 2 QFII / 3 社保 / 4 券商 / 5 保险 / 6 信托
_EM_ORG_TYPE_IDS = ("1", "2", "3", "4", "5", "6")
_EM_PAGE_SIZE = 500
_EM_MAX_PAGES = 50
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

# 季度末报告期（月, 日）
_QUARTER_ENDS = ((3, 31), (6, 30), (9, 30), (12, 31))


class FundHoldingsFetchError(RuntimeError):
    """基金持股源端网络/解析失败（区别于"确实无数据"）。"""


def _sina_to_ts_code(code: str) -> str:
    """将东财 6 位代码转换为 ts_code 格式（如 300750 → 300750.SZ）。

    市场前缀规则：``4``/``8``/``920`` → bj，``6``/``9`` → sh，其余 → sz。
    ``920`` 需先于 ``9`` 判断（北交所新代码段）。
    """
    code = str(code).strip()
    if not code or len(code) != 6:
        return code
    if code.startswith("920") or code[0] in ("4", "8"):
        return f"{code}.BJ"
    if code[0] in ("6", "9"):
        return f"{code}.SH"
    return f"{code}.SZ"


def _recent_report_dates(today: date | None = None, count: int = 4) -> list[str]:
    """最近 ``count`` 个已结束的季度末报告期（YYYY-MM-DD，从新到旧）。"""
    today = today or date.today()
    ends = [
        date(year, month, day)
        for year in (today.year, today.year - 1)
        for month, day in _QUARTER_ENDS
    ]
    past = sorted((e for e in ends if e <= today), reverse=True)
    return [e.isoformat() for e in past[:count]]


def _fetch_fund_holdings_page(
    report_date: str,
    org_type: str,
    page: int,
    timeout: float = 30.0,
) -> tuple[list[dict], int]:
    """抓取单个 (报告期, 机构类型) 的单页数据，返回 (records, total_pages)。

    网络/解析异常抛 ``FundHoldingsFetchError``；该 (日期, 类型) 无数据时返回
    ``([], 0)``（东财对无数据日期返回顶层空列表 ``[]``）。
    """
    if requests is None:
        raise FundHoldingsFetchError("requests 未安装")

    params = {
        "date": report_date,
        "type": org_type,
        "zjc": "0",
        "sortField": "HOULD_NUM",
        "sortDirec": "1",
        "pageNum": str(page),
        "pageSize": str(_EM_PAGE_SIZE),
        "p": str(page),
        "pageNo": str(page),
    }
    try:
        resp = requests.get(
            _EM_ZLSJ_URL, params=params,
            headers=_EM_HEADERS, timeout=timeout,
        )
        resp.raise_for_status()
        payload = resp.json()
    except (requests.RequestException, ValueError) as e:
        raise FundHoldingsFetchError(
            f"基金持股 {report_date}/type={org_type} 第 {page} 页获取失败: {e}"
        ) from e

    if isinstance(payload, list):
        # 东财对无数据的日期/类型返回顶层空列表，属正常"无数据"
        return [], 0
    if not isinstance(payload, dict):
        raise FundHoldingsFetchError(
            f"基金持股返回非预期载荷类型: {type(payload).__name__}"
        )

    rows = payload.get("data")
    rows = rows if isinstance(rows, list) else []
    total_pages = payload.get("pages") or 1

    records: list[dict] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        code = str(row.get("SECURITY_CODE", "")).strip()
        if not code:
            continue
        org_type_raw = str(row.get("ORG_TYPE", "")).strip()
        institution_type = _ORG_TYPE_MAP.get(org_type_raw, org_type_raw)
        report_date_raw = str(row.get("REPORT_DATE", ""))
        row_report_date = report_date_raw[:10] if report_date_raw else ""
        records.append(
            {
                "ts_code": _sina_to_ts_code(code),
                "stock_name": str(row.get("SECURITY_NAME_ABBR", "") or "").strip(),
                "report_date": row_report_date,
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


def _fetch_report_date(report_date: str, timeout: float = 30.0) -> list[dict]:
    """拉取某报告期全部 6 类机构的明细（分页）。"""
    records: list[dict] = []
    for org_type in _EM_ORG_TYPE_IDS:
        page = 1
        while page <= _EM_MAX_PAGES:
            page_records, total_pages = _fetch_fund_holdings_page(
                report_date, org_type, page, timeout
            )
            records.extend(page_records)
            if page >= total_pages or not page_records:
                break
            page += 1
    return records


def _fetch_all_fund_holdings(
    today: date | None = None, timeout: float = 30.0
) -> list[dict]:
    """获取最近一个可用报告期的全市场基金持股明细。

    从最近已结束的季度末往前回退，直到某个报告期有数据；全部为空时返回 []。
    源端网络异常直接上抛 ``FundHoldingsFetchError``（回退不会修复网络故障）。
    """
    for report_date in _recent_report_dates(today=today):
        records = _fetch_report_date(report_date, timeout)
        if records:
            return records
    return []


def update_fund_holdings(db: DatabaseInterface) -> dict:
    """获取基金持股数据并保存（季频，每日幂等 upsert）。

    结果语义：
    - 源端网络失败 → ``retained``（保留旧数据），避免误判 failed；
    - 所有候选报告期无数据 → ``skipped`` → no_data；
    - 有数据 → 写入并返回 saved/total。
    """
    logger.info("\n" + "=" * 60)
    logger.info("📊 任务: 更新基金持股明细")
    logger.info("=" * 60)

    try:
        records = _fetch_all_fund_holdings()
    except FundHoldingsFetchError as e:
        logger.warning(f"⚠️ 基金持股源端失败: {e}")
        return {
            "status": "retained",
            "reason": f"fund holdings source failed: {e}",
            "error_kind": "network",
            "retained_old_data": True,
            "total": 0,
        }

    if not records:
        logger.warning("⚠️ 基金持股无数据（最近报告期均为空）")
        return {
            "skipped": True,
            "reason": "no fund holdings data for recent report periods",
            "total": 0,
        }

    try:
        saved = db.save_fund_holdings_batch(records)
    except Exception as e:
        logger.error(f"❌ 基金持股写入失败: {e}")
        return {"saved": 0, "error": str(e), "error_kind": "database"}

    dates = {r["report_date"] for r in records if r.get("report_date")}
    latest = max(dates) if dates else ""
    types = {r["institution_type"] for r in records}
    logger.info(
        f"✅ 基金持股保存完成: {saved} 条 "
        f"(报告期 {latest}, 机构类型: {', '.join(sorted(types))})"
    )
    return {"saved": saved, "total": len(records)}
