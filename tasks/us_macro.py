"""
美国日度宏观利率更新任务
─────────────────────
从 FRED（圣路易斯联储）抓取加息周期核心定价变量：
EFFR（有效联邦基金利率）、DGS2/DGS3MO/DGS10（美债 2Y/3M/10Y 收益率）、
T10YIE/T5YIE（10Y/5Y 盈亏平衡通胀率）、ICSA（初请失业金，周度）、
HY/IG 信用利差（BAMLH0A0HYM2/BAMLC0A0CM）与金融压力指数（STLFSI4，
周度），并派生 10Y-3M 利差与 10Y 实际利率。

FRED 日度利率均为 T+1 发布（当日收益率尚未公布），故回溯一个窗口取
已发布记录；fredgraph.csv 无 API key 即可访问，缺失值以空串/`.` 表示。
INSERT OR REPLACE 写入保证幂等，不产生重复。
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from core.calendar import get_expected_latest_trading_day
from interface import DatabaseInterface

try:
    import requests
except ImportError:
    requests = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)

_FRED_GRAPH_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"

# FRED 系列 ID -> 记录字段名
_FRED_SERIES: dict[str, str] = {
    "EFFR": "effr",        # 有效联邦基金利率
    "DGS2": "dgs2",        # 美债 2 年收益率（政策利率预期锚）
    "DGS3MO": "dgs3mo",    # 美债 3 个月收益率
    "DGS10": "dgs10",      # 美债 10 年收益率
    "T10YIE": "t10yie",    # 10 年盈亏平衡通胀率
    "ICSA": "icsa",        # 初请失业金人数（周度发布，日度表中稀疏填充）
    "BAMLH0A0HYM2": "hy_oas",   # 高收益债 OAS（信用压力，加息周期核心仪表）
    "BAMLC0A0CM": "ig_oas",     # 投资级债 OAS
    "STLFSI4": "stlfi",         # 圣路易斯联储金融压力指数（周度，周一观察日）
    "T5YIE": "t5yie",           # 5Y 盈亏平衡通胀率
}

# 回溯窗口：覆盖 T+1 发布延迟 + 周末/节假日 + FRED 偶发修订
_LOOKBACK_DAYS = 45
_TIMEOUT = 30.0
# 注意：fredgraph.csv 对浏览器 UA 会挂起不响应（实测 60s+ 无数据），
# 使用 requests 默认 UA 即可正常访问。


def _fetch_fred_series(series_id: str, start: str, end: str) -> dict[str, float | None]:
    """抓取单个 FRED 日度序列，返回 {date: value}（缺失值为 None）。"""
    if requests is None:
        return {}
    try:
        resp = requests.get(
            _FRED_GRAPH_URL,
            params={"id": series_id, "cosd": start, "coed": end},
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.warning(f"⚠️ FRED {series_id} 获取失败: {e}")
        return {}
    records: dict[str, float | None] = {}
    lines = resp.text.strip().splitlines()
    if len(lines) < 2:
        return records
    for line in lines[1:]:
        parts = line.split(",")
        if len(parts) != 2:
            continue
        date_str, raw = parts[0].strip(), parts[1].strip()
        if not date_str:
            continue
        if raw in ("", "."):
            records[date_str] = None
            continue
        try:
            records[date_str] = float(raw)
        except ValueError:
            records[date_str] = None
    return records


def _week_to_friday(series: dict[str, float | None]) -> dict[str, float | None]:
    """周度序列（如 ICSA）的 FRED 观察日为周六，映射到当周周五。

    日度表只有业务日行，周六的观察日永远匹配不上；周度数据按惯例
    归属其所在交易周，取该周周五落库。
    """
    remapped: dict[str, float | None] = {}
    for date_str, value in series.items():
        friday = (
            datetime.strptime(date_str, "%Y-%m-%d") - timedelta(days=1)
        ).strftime("%Y-%m-%d")
        remapped[friday] = value
    return remapped


def _fetch_us_macro(trade_date: str) -> list[dict]:
    """抓取并合并 FRED 六个序列，按日期对齐并派生利差/实际利率。"""
    start = (datetime.strptime(trade_date, "%Y-%m-%d") - timedelta(days=_LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    merged: dict[str, dict[str, float | None]] = {}
    for series_id, field in _FRED_SERIES.items():
        series = _fetch_fred_series(series_id, start, trade_date)
        if field == "icsa":
            series = _week_to_friday(series)
        for date_str, value in series.items():
            merged.setdefault(date_str, dict.fromkeys(_FRED_SERIES.values()))[field] = value

    records = []
    for date_str in sorted(merged):
        row = merged[date_str]
        # 各序列全空的日期（如长假连休）无入库价值
        if all(v is None for v in row.values()):
            continue
        dgs10, dgs3mo, t10yie = row["dgs10"], row["dgs3mo"], row["t10yie"]
        row["spread_10y_3m"] = round(dgs10 - dgs3mo, 4) if dgs10 is not None and dgs3mo is not None else None
        row["real_rate_10y"] = round(dgs10 - t10yie, 4) if dgs10 is not None and t10yie is not None else None
        row["trade_date"] = date_str
        row["data_source"] = "fred"
        records.append(row)
    return records


def update_us_macro(db: DatabaseInterface) -> dict:
    """获取美国日度宏观利率（FRED）并保存。"""
    logger.info("\n" + "=" * 60)
    logger.info("🏦 任务: 更新美国宏观利率 (FRED)")
    logger.info("=" * 60)

    if requests is None:
        logger.error("❌ requests 未安装")
        return {"saved": 0, "error": "requests not installed"}

    try:
        records = _fetch_us_macro(get_expected_latest_trading_day())
        if not records:
            logger.warning("⚠️ 美国宏观利率无数据")
            return {"saved": 0, "total": 0}
        saved = db.save_us_macro_batch(records)
        logger.info(f"✅ 美国宏观利率保存完成: {saved} 条")
        return {"saved": saved, "total": len(records)}
    except Exception as e:
        logger.error(f"❌ 美国宏观利率更新失败: {e}")
        return {"saved": 0, "error": str(e), "error_kind": "network"}
