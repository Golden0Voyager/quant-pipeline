"""
全市场日线快照（快路径数据源）
────────────────────────────
东财 spot 接口一次分页请求返回全市场 ~5500 只当日行情，
用于收盘后把"每只只缺今天一根 K 线"的常规日更从逐股
~3700 次请求压缩为 1 次快照 + 少量逐股兜底。

设计约束（与 update_bars 的防线衔接）：
- 仅收盘定型后调用（调用方负责 market_phase 判定）；
- 前复权以最新价为锚 → 当日原始价 ≡ 当日前复权价，
  追加"今天"这一行与存量 qfq 序列一致；
- 「昨收」字段用于调用方检测当日除权股（历史锚点变动 → 逐股重拉）。
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from core.market_time import shanghai_today

try:
    import akshare as ak
except ImportError:
    ak = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)

SNAPSHOT_SOURCE = "eastmoney_spot"

# spot_em 中文列 → daily_bars 列
_COLUMN_MAP = {
    "今开": "open",
    "最新价": "close",
    "最高": "high",
    "最低": "low",
    "成交量": "volume",
    "成交额": "amount",
    "换手率": "turnover_rate",
    "涨跌幅": "pct_change",
    "振幅": "amplitude",
}
_REQUIRED_COLUMNS = ("代码", "最新价", "今开", "昨收")


def _to_float(val: Any) -> float | None:
    if val is None:
        return None
    try:
        f = float(val)
    except (TypeError, ValueError):
        return None
    return None if pd.isna(f) else f


def fetch_market_snapshot() -> pd.DataFrame:
    """抓取东财全市场当日快照。

    异常直接上抛，由调用方决定回退逐股路径；
    返回的 DataFrame 缺少必需列同样视为源端异常。
    """
    if ak is None:
        raise RuntimeError("akshare not installed")
    df = ak.stock_zh_a_spot_em()
    if df is None or df.empty:
        raise RuntimeError("spot snapshot returned no rows")
    missing = [c for c in _REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise RuntimeError(f"spot snapshot missing columns: {missing}")
    return df


def snapshot_to_bar_records(
    df: pd.DataFrame,
    trade_date: str | None = None,
) -> tuple[dict[str, dict[str, Any]], set[str]]:
    """把快照归一化为 daily_bars 行。

    Returns:
        (records, suspended)
        records: 6 位裸码 → 单行记录（含 prev_close 供除权检测，写库前需剔除）
        suspended: 无有效价格/零成交的代码集合（停牌或当日未交易）
    """
    trade_date = trade_date or shanghai_today()
    records: dict[str, dict[str, Any]] = {}
    suspended: set[str] = set()

    for _, row in df.iterrows():
        code = str(row.get("代码", "")).strip()
        if not code or not code.isdigit():
            continue
        close = _to_float(row.get("最新价"))
        open_ = _to_float(row.get("今开"))
        volume = _to_float(row.get("成交量"))
        if close is None or open_ is None or not volume:
            # 停牌/当日无成交：spot 中价格为 NaN 或成交量为 0
            suspended.add(code)
            continue
        record: dict[str, Any] = {
            "trade_date": trade_date,
            "data_source": SNAPSHOT_SOURCE,
            "prev_close": _to_float(row.get("昨收")),
        }
        for src_col, dst_col in _COLUMN_MAP.items():
            record[dst_col] = _to_float(row.get(src_col))
        records[code] = record
    return records, suspended


def bar_record_to_frame(record: dict[str, Any]) -> pd.DataFrame:
    """单条快照记录 → 可直接交给 save_daily_bars 的单行 DataFrame。"""
    row = {k: v for k, v in record.items() if k != "prev_close"}
    return pd.DataFrame([row])
