"""
市场宽度指标更新任务
────────────────────
新高/新低家数（20/60/120 日）、破净股统计与赚钱效应快照。

数据源：乐咕乐咕 (legulegu.com)。三个来源按日期合并进同一张
``market_breadth`` 表；任一来源缺失只留 NULL，不影响其他来源落库。

- ``stock_a_high_low_statistics``  全历史（~500 交易日）
- ``stock_a_below_net_asset_statistics`` 全历史（上游当前返回缺 ``marketId``，
  属乐咕端变更；尽力而为，失败只告警不阻断）
- ``stock_market_activity_legu``   仅当日快照（统计日期为准）
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from core.source_client import get_default_client
from core.utils import to_float as _to_float
from interface import DatabaseInterface

try:
    import akshare as ak
except ImportError:
    ak = None

logger = logging.getLogger(__name__)

# 乐咕创新高/新低接口的指数口径：all=全 A。
_HIGH_LOW_SYMBOL = "all"
# 破净统计口径：全部 A 股。
_BELOW_NET_ASSET_SYMBOL = "全部A股"

# 赚钱效应快照的中文 item → 列名。
_ACTIVITY_ITEM_MAP = {
    "上涨": "up_count",
    "下跌": "down_count",
    "平盘": "flat_count",
    "涨停": "limit_up",
    "跌停": "limit_down",
    "真实涨停": "real_limit_up",
    "真实跌停": "real_limit_down",
    "st st*涨停": "st_limit_up",
    "st st*跌停": "st_limit_down",
    "停牌": "suspended",
    "活跃度": "activity_ratio",
    "统计日期": "_stat_time",
}


def _to_int(value: Any) -> int | None:
    """把家数/计数类值转换为 int；None/NaN/空串/非数 → None。"""
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        value = text
    f = _to_float(value)
    return None if f is None else int(f)


# ===========================================================================
# 乐咕来源：创新高/新低（全历史）
# ===========================================================================


def _fetch_high_low() -> list[dict]:
    """获取全 A 创新高/新低家数历史（20/60/120 日）。"""
    resp = get_default_client().call(
        "legu", lambda: ak.stock_a_high_low_statistics(symbol=_HIGH_LOW_SYMBOL)
    )
    df = resp.data if resp.success else None
    if df is None or getattr(df, "empty", True):
        return []
    records: list[dict] = []
    for _, row in df.iterrows():
        date_val = str(row.get("date", "")).strip()[:10]
        if not date_val:
            continue
        records.append({
            "date": date_val,
            "close": _to_float(row.get("close")),
            "high20": _to_int(row.get("high20")),
            "low20": _to_int(row.get("low20")),
            "high60": _to_int(row.get("high60")),
            "low60": _to_int(row.get("low60")),
            "high120": _to_int(row.get("high120")),
            "low120": _to_int(row.get("low120")),
        })
    return records


# ===========================================================================
# 乐咕来源：破净股统计（全历史）
# ===========================================================================


def _fetch_below_net_asset() -> list[dict]:
    """获取全部 A 股破净家数与占比历史。"""
    resp = get_default_client().call(
        "legu",
        lambda: ak.stock_a_below_net_asset_statistics(symbol=_BELOW_NET_ASSET_SYMBOL),
    )
    df = resp.data if resp.success else None
    if df is None or getattr(df, "empty", True):
        return []
    records: list[dict] = []
    for _, row in df.iterrows():
        date_val = str(row.get("date", "")).strip()[:10]
        if not date_val:
            continue
        records.append({
            "date": date_val,
            "below_net_asset": _to_int(row.get("below_net_asset")),
            "total_company": _to_int(row.get("total_company")),
            "below_net_asset_ratio": _to_float(row.get("below_net_asset_ratio")),
        })
    return records


# ===========================================================================
# 乐咕来源：赚钱效应（当日快照）
# ===========================================================================


def _fetch_activity() -> dict | None:
    """获取当日赚钱效应快照（上涨/下跌/涨跌停家数/活跃度）。

    该接口只给当前时点的 item/value 表；以「统计日期」为 date，无日期则丢弃。
    """
    resp = get_default_client().call("legu", lambda: ak.stock_market_activity_legu())
    df = resp.data if resp.success else None
    if df is None or getattr(df, "empty", True):
        return None

    record: dict[str, Any] = {}
    stat_time = ""
    for _, row in df.iterrows():
        key = _ACTIVITY_ITEM_MAP.get(str(row.get("item", "")).strip())
        if key is None:
            continue
        raw = row.get("value")
        if key == "_stat_time":
            stat_time = str(raw).strip()
        elif key == "activity_ratio":
            # 活跃度形如 "20.76%"，默认 to_float 不剥离百分号，这里显式去掉。
            record[key] = _to_float(str(raw).replace("%", "").strip())
        else:
            record[key] = _to_int(raw)

    date_val = stat_time[:10]
    if not date_val:
        return None
    record["date"] = date_val
    return record


# ===========================================================================
# 主更新函数
# ===========================================================================


def update_market_breadth(db: DatabaseInterface) -> dict:
    """抓取市场宽度三源并按日期合并落库。"""
    logger.info("\n" + "=" * 60)
    logger.info("📊 任务: 更新市场宽度（新高新低/破净/赚钱效应）")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "error": "akshare not installed"}

    data_date = datetime.now().strftime("%Y-%m-%d")
    merged: dict[str, dict] = {}
    errors: list[str] = []

    # 历史型来源：任一失败仍保留另一来源的数据。
    for name, fetch in (("创新高低", _fetch_high_low), ("破净统计", _fetch_below_net_asset)):
        try:
            fetched = fetch()
        except Exception as e:  # noqa: BLE001 — 上游/契约变更都只告警不阻断
            errors.append(f"{name}: {e}")
            logger.warning(f"⚠️ {name} 获取失败: {e}")
            continue
        for record in fetched:
            date_val = record.pop("date")
            merged.setdefault(date_val, {"date": date_val}).update(record)
        logger.info(f"  ✅ {name}: {len(fetched)} 个交易日")

    # 快照型来源：只有当天一行，覆盖同日历史来源的记录。
    try:
        activity = _fetch_activity()
    except Exception as e:  # noqa: BLE001
        errors.append(f"赚钱效应: {e}")
        logger.warning(f"⚠️ 赚钱效应 获取失败: {e}")
        activity = None
    if activity:
        date_val = activity.pop("date")
        merged.setdefault(date_val, {"date": date_val}).update(activity)
        logger.info(f"  ✅ 赚钱效应: {date_val}")

    if not merged:
        if errors:
            logger.error(f"🚫 市场宽度全部来源失败: {'; '.join(errors)}")
            return {
                "status": "retained",
                "retained_old_data": True,
                "reason": "all market breadth sources failed; kept old data",
                "error_kind": "network",
                "error": "; ".join(errors),
                "saved": 0,
            }
        return {"skipped": True, "reason": "all market breadth sources empty", "saved": 0}

    records: list[dict] = []
    for date_val in sorted(merged):
        record = merged[date_val]
        record["data_source"] = "legu"
        record["data_date"] = data_date
        records.append(record)

    saved = db.save_market_breadth_batch(records)
    logger.info(f"✅ 市场宽度保存完成: {saved} 条 / {len(records)} 个交易日")

    result: dict[str, Any] = {"saved": saved}
    if errors:
        # 部分来源失败但仍有数据落库：**不能**用顶层 "error" 键——
        # normalize_task_result 见到 error 会直接把整任务判成 failed，把这个
        # 已知上游漂移的 partial 结果天天报成失败。放进 metadata，随审计落库；
        # 日志里已逐来源告警。
        result["metadata"] = {"source_errors": errors}
    return result
