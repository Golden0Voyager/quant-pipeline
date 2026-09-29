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
- 乐咕快照不可用时回退**东财涨跌停池**（``stock_zt_pool_em`` /
  ``stock_zt_pool_dtgc_em``，按 ``get_expected_latest_trading_day()`` 取交易日），
  只补涨跌停家数，``data_source`` 标记 ``eastmoney``；高新低/破净无东财等价物，
  乐咕挂掉期间继续缺失（预期行为）。

各来源的失败/空数据一律 WARNING 告警（不阻断主流程）；部分来源失败只进
``metadata.source_errors``，**不进**顶层 ``error`` 键——``normalize_task_result``
见到顶层 error 会把整任务判 failed。
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from core.calendar import get_expected_latest_trading_day
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


def _resp_error(resp: Any) -> str:
    """取 ``SourceResponse`` 的错误文本（响应对象缺该字段时给占位文案）。

    测试里 ``resp`` 常是 ``SimpleNamespace``（无 metadata），因此逐层 getattr，
    不假设属性一定存在。
    """
    meta = getattr(resp, "metadata", None)
    err = getattr(meta, "error", None)
    return str(err) if err else "未知错误"


# ===========================================================================
# 乐咕来源：创新高/新低（全历史）
# ===========================================================================


def _fetch_high_low() -> list[dict]:
    """获取全 A 创新高/新低家数历史（20/60/120 日）。"""
    resp = get_default_client().call(
        "legu", lambda: ak.stock_a_high_low_statistics(symbol=_HIGH_LOW_SYMBOL)
    )
    if not resp.success:
        logger.warning(f"⚠️ 创新高低（乐咕）获取失败: {_resp_error(resp)}")
        return []
    df = resp.data
    if df is None or getattr(df, "empty", True):
        logger.warning("⚠️ 创新高低（乐咕）返回空数据")
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
    if not resp.success:
        logger.warning(f"⚠️ 破净统计（乐咕）获取失败: {_resp_error(resp)}")
        return []
    df = resp.data
    if df is None or getattr(df, "empty", True):
        logger.warning("⚠️ 破净统计（乐咕）返回空数据")
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
    if not resp.success:
        logger.warning(f"⚠️ 赚钱效应（乐咕）获取失败: {_resp_error(resp)}")
        return None
    df = resp.data
    if df is None or getattr(df, "empty", True):
        logger.warning("⚠️ 赚钱效应（乐咕）返回空数据")
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
        logger.warning("⚠️ 赚钱效应（乐咕）缺少「统计日期」，丢弃该快照")
        return None
    record["date"] = date_val
    return record


# ===========================================================================
# 东财备用源：赚钱效应快照（乐咕不可用时的回退）
# ===========================================================================


def _pool_counts(df: Any) -> tuple[int, int, int]:
    """涨跌停池 DataFrame → ``(总数, 非ST, ST)``；空/None → (0, 0, 0)。"""
    if df is None or getattr(df, "empty", True):
        return 0, 0, 0
    total = len(df)
    # ST 前缀（ST / *ST）出现在「名称」列开头；口径与乐咕「真实涨停」一致：
    # 真实 = 非 ST。
    real = sum(
        1
        for _, row in df.iterrows()
        if "ST" not in str(row.get("名称", "")).strip().upper()
    )
    return total, real, total - real


def _fetch_activity_em(trade_date: str) -> dict | None:
    """东财备用源：按交易日取涨停/跌停池快照（乐咕赚钱效应不可用时回退）。

    只补涨跌停家数（含 ST 拆分）与 ``date``；涨跌/平盘家数需要全市场 spot
    快照（akshare 逐页 ~50+ 次请求，且当前环境对 push2 端点不稳定），因此
    **留空不写**——宁缺毋假，别把慢/不稳接口拖进每日主流程。

    两池皆空（窗口外/非交易日/源端无数据）返回 None 并告警；单池失败只告警
    不阻断（另一池仍可用）。``trade_date`` 由调用方按交易日历推导。
    """
    if ak is None:
        return None
    date_compact = trade_date.replace("-", "")
    client = get_default_client()

    resp_up = client.call("eastmoney", lambda: ak.stock_zt_pool_em(date=date_compact))
    if not resp_up.success:
        logger.warning(f"⚠️ 东财涨停池获取失败({trade_date}): {_resp_error(resp_up)}")
    resp_down = client.call(
        "eastmoney", lambda: ak.stock_zt_pool_dtgc_em(date=date_compact)
    )
    if not resp_down.success:
        logger.warning(f"⚠️ 东财跌停池获取失败({trade_date}): {_resp_error(resp_down)}")

    up_df = resp_up.data if resp_up.success else None
    down_df = resp_down.data if resp_down.success else None
    up_total, up_real, up_st = _pool_counts(up_df)
    down_total, down_real, down_st = _pool_counts(down_df)
    if up_total == 0 and down_total == 0:
        logger.warning(f"⚠️ 东财备用源无数据（涨停/跌停池皆空）: {trade_date}")
        return None

    # 只写**确实拿到行**的那一侧：抓取失败/空池的字段保持缺省（落库 NULL），
    # 不拿 0 冒充「当日 0 家涨停」——0 与「没抓到」语义完全不同。
    record: dict[str, Any] = {"date": trade_date}
    if up_total:
        record.update(limit_up=up_total, real_limit_up=up_real, st_limit_up=up_st)
    if down_total:
        record.update(
            limit_down=down_total, real_limit_down=down_real, st_limit_down=down_st
        )
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
    row_sources: dict[str, set[str]] = {}  # 每行由哪些来源贡献（data_source 口径）
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
            row_sources.setdefault(date_val, set()).add("legu")
        if fetched:
            logger.info(f"  ✅ {name}: {len(fetched)} 个交易日")
        else:
            # 空结果 = 上游挂了或没数据：曾经打 ✅ 让「0 个交易日」看着像成功。
            logger.warning(f"  ⚠️ {name}: 0 个交易日（来源空数据或抓取失败）")

    # 快照型来源：乐咕优先，返回 None（失败/空/缺统计日期）时回退东财涨跌停池。
    try:
        activity = _fetch_activity()
    except Exception as e:  # noqa: BLE001
        errors.append(f"赚钱效应: {e}")
        logger.warning(f"⚠️ 赚钱效应 获取失败: {e}")
        activity = None
    activity_source = "legu"
    if activity is None:
        trade_date = get_expected_latest_trading_day()
        try:
            activity = _fetch_activity_em(trade_date)
        except Exception as e:  # noqa: BLE001 — 东财限流/窗口错误同样只告警
            errors.append(f"赚钱效应(东财备用): {e}")
            logger.warning(f"⚠️ 赚钱效应东财备用源 获取失败: {e}")
            activity = None
        if activity is not None:
            activity_source = "eastmoney"
            logger.info(f"  🔁 赚钱效应改用东财备用源: {trade_date}")
    if activity:
        date_val = activity.pop("date")
        merged.setdefault(date_val, {"date": date_val}).update(activity)
        row_sources.setdefault(date_val, set()).add(activity_source)
        logger.info(f"  ✅ 赚钱效应: {date_val} ({activity_source})")

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
        logger.warning("🚫 市场宽度全部来源无数据（或抓取失败），保留旧数据，本轮无新行可写")
        return {"skipped": True, "reason": "all market breadth sources empty", "saved": 0}

    records: list[dict] = []
    for date_val in sorted(merged):
        record = merged[date_val]
        # 同一行可能混有乐咕历史列 + 东财快照列 → 逐行记实际贡献来源
        record["data_source"] = "+".join(sorted(row_sources.get(date_val) or {"legu"}))
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
