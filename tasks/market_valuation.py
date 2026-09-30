"""
大盘估值指标更新任务
────────────────────
全市场 PE/PB 中位数、股债利差 (FED spread)。
数据源：乐咕乐咕 (legulegu.com)，稳定，无需抗限流封装。
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from core.data_contract import MARKET_VALUATION_CONTRACT, validate_records
from core.source_client import get_default_client
from core.utils import to_float as _to_float
from interface import DatabaseInterface

try:
    import akshare as ak
except ImportError:
    ak = None

logger = logging.getLogger(__name__)


# ===========================================================================
# 大盘 PE(TTM + LYR) 中位数
# ===========================================================================


def _fetch_pe(failures: list[str] | None = None) -> list[dict]:
    """获取全市场 PE(TTM/LYR) 中位数及历史分位。"""
    resp = get_default_client().call("legu", lambda: ak.stock_a_ttm_lyr())
    if not resp.success:
        # 失败原因（如上游 504 断服）必须落日志：旧实现静默吞掉，
        # 任务只剩「zero rows without explanation」，现场无法分诊
        logger.warning(f"⚠️ 全市场PE 源请求失败: {resp.metadata.error}")
        if failures is not None:
            failures.append(f"全市场PE: {resp.metadata.error}")
        return []
    df = resp.data
    if df is None or (hasattr(df, "empty") and df.empty):
        return []
    col_map = {
        "date": "date",
        "middlePETTM": "pe_median",
        "quantileInAllHistoryMiddlePeTtm": "pe_quantile",
        "middlePELYR": "pe_lyr_median",
    }
    df = df.rename(columns=col_map)
    keep = {"date", "pe_median", "pe_quantile", "pe_lyr_median"}
    available = [c for c in keep if c in df.columns]
    df = df[available]
    records = []
    for _, row in df.iterrows():
        date_val = str(row.get("date", "")).strip()[:10]
        if date_val:
            records.append({
                "date": date_val,
                "pe_median": _to_float(row.get("pe_median")),
                "pe_quantile": _to_float(row.get("pe_quantile")),
                "pe_lyr_median": _to_float(row.get("pe_lyr_median")),
            })
    return records


# ===========================================================================
# 大盘 PB 中位数
# ===========================================================================


def _fetch_pb(failures: list[str] | None = None) -> list[dict]:
    """获取全市场 PB 中位数及历史分位。"""
    resp = get_default_client().call("legu", lambda: ak.stock_a_all_pb())
    if not resp.success:
        # 失败原因（如上游 504 断服）必须落日志：旧实现静默吞掉，
        # 任务只剩「zero rows without explanation」，现场无法分诊
        logger.warning(f"⚠️ 全市场PB 源请求失败: {resp.metadata.error}")
        if failures is not None:
            failures.append(f"全市场PB: {resp.metadata.error}")
        return []
    df = resp.data
    if df is None or (hasattr(df, "empty") and df.empty):
        return []
    col_map = {
        "date": "date",
        "middlePB": "pb_median",
        "quantileInAllHistoryMiddlePB": "pb_quantile",
    }
    df = df.rename(columns=col_map)
    keep = {"date", "pb_median", "pb_quantile"}
    available = [c for c in keep if c in df.columns]
    df = df[available]
    records = []
    for _, row in df.iterrows():
        date_val = str(row.get("date", "")).strip()[:10]
        if date_val:
            records.append({
                "date": date_val,
                "pb_median": _to_float(row.get("pb_median")),
                "pb_quantile": _to_float(row.get("pb_quantile")),
            })
    return records


# ===========================================================================
# 股债利差 (FED spread)
# ===========================================================================


def _fetch_ebs(failures: list[str] | None = None) -> list[dict]:
    """获取股债利差（沪深300 vs 10年国债）。"""
    resp = get_default_client().call("legu", lambda: ak.stock_ebs_lg())
    if not resp.success:
        # 失败原因（如上游 504 断服）必须落日志：旧实现静默吞掉，
        # 任务只剩「zero rows without explanation」，现场无法分诊
        logger.warning(f"⚠️ 股债利差 源请求失败: {resp.metadata.error}")
        if failures is not None:
            failures.append(f"股债利差: {resp.metadata.error}")
        return []
    df = resp.data
    if df is None or (hasattr(df, "empty") and df.empty):
        return []
    col_map = {
        "日期": "date",
        "沪深300指数": "csi300_close",
        "股债利差": "equity_bond_spread",
        "股债利差均线": "ebs_ma",
    }
    df = df.rename(columns=col_map)
    keep = {"date", "csi300_close", "equity_bond_spread", "ebs_ma"}
    available = [c for c in keep if c in df.columns]
    df = df[available]
    records = []
    for _, row in df.iterrows():
        date_val = str(row.get("date", "")).strip()[:10]
        if date_val:
            records.append({
                "date": date_val,
                "equity_bond_spread": _to_float(row.get("equity_bond_spread")),
                "ebs_ma": _to_float(row.get("ebs_ma")),
                "csi300_close": _to_float(row.get("csi300_close")),
            })
    return records


# ===========================================================================
# 主更新函数
# ===========================================================================


def update_market_valuation(db: DatabaseInterface) -> dict:
    """获取大盘估值指标并保存。"""
    logger.info("\n" + "=" * 60)
    logger.info("📊 任务: 更新大盘估值指标")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "error": "akshare not installed"}

    results: dict[str, Any] = {}
    data_date = datetime.now().strftime("%Y-%m-%d")

    # --- Merge by date (PE / PB / 股债利差 三者按日期合并) ---
    daily_merge: dict[str, dict] = {}
    # 「源不可用」与「源可用但当天没数据」必须分开：前者是网络故障，该按
    # retained 保留旧数据并让 safe_task 的网络重试生效；后者是真正需要人看一眼
    # 的异常，混为一谈就变成 P2-18 记过的 retained 静默退化。
    transport_failures: list[str] = []
    daily_calls = [
        ("全市场PE", _fetch_pe),
        ("全市场PB", _fetch_pb),
        ("股债利差", _fetch_ebs),
    ]
    for name, fn in daily_calls:
        try:
            records = fn(transport_failures)
            for r in records:
                d = r.pop("date")
                if d not in daily_merge:
                    daily_merge[d] = {"date": d}
                daily_merge[d].update(r)
                daily_merge[d]["data_date"] = data_date
            results[name] = len(records)
        except Exception as e:
            logger.warning(f"⚠️ {name} 获取失败: {e}")
            results[name] = f"error: {e}"
            transport_failures.append(f"{name}: {e}")

    if daily_merge:
        daily_records = list(daily_merge.values())
        validated_records, violations = validate_records(daily_records, MARKET_VALUATION_CONTRACT, logger)
        if violations and not validated_records:
            logger.error(f"🚫 大盘估值数据合约校验失败: {violations}")
            results["saved"] = 0
            results["error"] = f"data contract violations: {violations}"
            return dict(results)
        if validated_records and len(validated_records) < len(daily_records):
            logger.warning(f"⚠️ 大盘估值数据合约校验过滤 {len(daily_records) - len(validated_records)} 条问题记录")
        saved = db.save_market_valuation_batch(validated_records or daily_records)
        logger.info(f"✅ 大盘估值保存完成: {saved} 条 / {len(daily_records)} 个交易日")
    else:
        saved = 0
        # 附分源明细：三源各自是「0 条」还是「error: …」，上游断服时
        # 日志里直接可见原因，不用再翻代码猜（2026-09-28 乐咕 504 教训）
        logger.warning(f"⚠️ 大盘估值无数据（分源明细: {results}）")
        if transport_failures:
            # 源确实不可用（不是「源正常但今天没数据」）：保留库里已有的
            # market_valuation 行并按网络故障上报。error_kind=network 同时让
            # core/runner.py 的 safe_task 在 30s 后重试一次——旧实现缺 status，
            # normalize_task_result 落成 FAILED/DATA_QUALITY，重试永不触发。
            results["status"] = "retained"
            results["error_kind"] = "network"
            results["retained_old_data"] = True
            results["reason"] = "legu unavailable; kept previous market valuation rows"
            results["error"] = "; ".join(transport_failures)
    results["saved"] = saved

    return dict(results)


# ===========================================================================
# 收盘刷新 helper（Task 9）：只抓取/归一化，不写库，源异常直接上抛
# ===========================================================================


def fetch_market_valuation_records(data_date: str) -> list[dict]:
    """收盘刷新专用：抓取 PE/PB/股债利差三源全历史并按日期合并。

    历史型源，由适配器挑选回看窗口内可接受的目标分区；三源直连
    akshare 不走 SourceClient（刷新语义要求显式失败），任一源异常
    直接上抛。每条记录附 data_source/data_date（date 为策略必填键）。
    """
    daily_merge: dict[str, dict] = {}

    pe_df = ak.stock_a_ttm_lyr()
    if pe_df is not None and not pe_df.empty:
        pe_df = pe_df.rename(columns={
            "middlePETTM": "pe_median",
            "quantileInAllHistoryMiddlePeTtm": "pe_quantile",
            "middlePELYR": "pe_lyr_median",
        })
        for _, row in pe_df.iterrows():
            date_val = str(row.get("date", "")).strip()[:10]
            if not date_val:
                continue
            daily_merge.setdefault(date_val, {"date": date_val}).update({
                "pe_median": _to_float(row.get("pe_median")),
                "pe_quantile": _to_float(row.get("pe_quantile")),
                "pe_lyr_median": _to_float(row.get("pe_lyr_median")),
            })

    pb_df = ak.stock_a_all_pb()
    if pb_df is not None and not pb_df.empty:
        pb_df = pb_df.rename(columns={
            "middlePB": "pb_median",
            "quantileInAllHistoryMiddlePB": "pb_quantile",
        })
        for _, row in pb_df.iterrows():
            date_val = str(row.get("date", "")).strip()[:10]
            if not date_val:
                continue
            daily_merge.setdefault(date_val, {"date": date_val}).update({
                "pb_median": _to_float(row.get("pb_median")),
                "pb_quantile": _to_float(row.get("pb_quantile")),
            })

    ebs_df = ak.stock_ebs_lg()
    if ebs_df is not None and not ebs_df.empty:
        ebs_df = ebs_df.rename(columns={
            "日期": "date",
            "沪深300指数": "csi300_close",
            "股债利差": "equity_bond_spread",
            "股债利差均线": "ebs_ma",
        })
        for _, row in ebs_df.iterrows():
            date_val = str(row.get("date", "")).strip()[:10]
            if not date_val:
                continue
            daily_merge.setdefault(date_val, {"date": date_val}).update({
                "equity_bond_spread": _to_float(row.get("equity_bond_spread")),
                "ebs_ma": _to_float(row.get("ebs_ma")),
                "csi300_close": _to_float(row.get("csi300_close")),
            })

    records: list[dict] = []
    for date_val in sorted(daily_merge):
        record = daily_merge[date_val]
        record["data_source"] = "legu"
        record["data_date"] = data_date
        records.append(record)
    return records
