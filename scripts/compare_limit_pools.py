#!/usr/bin/env python3
"""
东财（akshare）与同花顺（HiThink）涨跌停池的离线差异对比。

背景
────
2026-09-27 起 ``limit_up_down`` 的历史回补会用同花顺兜底（东财池只保留约 16 个
交易日的滚动窗口）。同日两家池子的股票集合并不相等（2026-09-15 跌停 33 vs 27、
2026-09-03 涨停 46 vs 60）——本脚本量化这个差异，并区分两种成因：

* **口径差异**：一家的池子含破板/未封住等、另一家只收贴板价 → 集合差异落在
  涨跌幅明显偏离板价的股票上；
* **数据分歧**：同一只票双方都给了且都贴板，但涨跌幅对不上 → 真实分歧。

只读分析：不写库、不改任务语义；结果打印到 stdout，便于归档到 docs/。

用法：
    uv run python scripts/compare_limit_pools.py                       # 最近 5 个交易日
    uv run python scripts/compare_limit_pools.py 2026-09-15 2026-09-03 # 指定日期
    uv run python scripts/compare_limit_pools.py --days 10             # 最近 10 个交易日
"""
from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any, TypedDict

# 仓库根目录必须就地注入：本脚本以 `python scripts/compare_limit_pools.py`
# 直接执行，此时 `sys.path[0]` 是 scripts/，还 import 不到 core。注入之后，兄弟仓库的
# 路径交给 `core._bootstrap` 统一处理。
_REPO_ROOT = str(Path(__file__).resolve().parent.parent)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import core.config  # noqa: F401,E402 — 先于一切仓库模块导入，确保 .env 已加载
from core.calendar import trading_days_between  # noqa: E402
from core.source_hithink import HithinkClient  # noqa: E402
from tasks.macro import _fetch_limit_down, _fetch_limit_up_down  # noqa: E402

# 涨跌幅与板价的允许偏差（百分点）。两家源都给相对昨收的涨跌幅，板价为
# ±10%/±20%（主板/创业板）与 ±5%/±30%（ST/北交所）；偏差在容差内视为「贴板」。
_BOARD_PRICE_TOLERANCE_PCT = 0.3

_PCT_BUCKETS = (
    "both_board",
    "field_mismatch",
    "em_only_board",
    "ht_only_board",
    "both_off_board",
    "unknown",
)


class PoolDiff(TypedDict):
    """单个池（涨停/跌停）的集合差异摘要。"""

    pool: str
    em_count: int
    ht_count: int
    shared: int
    only_em: list[str]
    only_ht: list[str]
    jaccard: float
    pct_gap_buckets: dict[str, list[str]]


# ── 纯函数：集合差异与分桶（单测钉住，不发网络）─────────────────────


def board_price_for(pct: float) -> float:
    """该涨跌幅对应的板价幅度（±5%/±10%/±20%/±30% 中最接近的那个）。"""
    board = min((5.0, 10.0, 20.0, 30.0), key=lambda b: abs(abs(pct) - b))
    return board if pct >= 0 else -board


def _is_at_board(pct: float) -> bool:
    return abs(pct - board_price_for(pct)) <= _BOARD_PRICE_TOLERANCE_PCT


def classify_pct_gap(em_pct: float | None, ht_pct: float | None) -> str:
    """按同码股票在两家的涨跌幅分桶（口径差异 vs 数据分歧）。

    * ``"both_board"``：两家都贴板且数值一致；
    * ``"field_mismatch"``：两家都贴板但涨跌幅互异（数据分歧）；
    * ``"em_only_board"``：只有东财贴板（同花顺疑似含破板/未封住，口径差异）；
    * ``"ht_only_board"``：只有同花顺贴板；
    * ``"both_off_board"``：两家都不贴板（口径疑点，需逐例核查）；
    * ``"unknown"``：任一侧缺涨跌幅。
    """
    if em_pct is None or ht_pct is None:
        return "unknown"
    em_board, ht_board = _is_at_board(em_pct), _is_at_board(ht_pct)
    if em_board and ht_board:
        return "field_mismatch" if abs(em_pct - ht_pct) > _BOARD_PRICE_TOLERANCE_PCT else "both_board"
    if em_board:
        return "em_only_board"
    if ht_board:
        return "ht_only_board"
    return "both_off_board"


def _codes(rows: list[dict]) -> set[str]:
    """行集合 → 6 位代码集合（跳过空码）。"""
    return {str(row.get("ts_code") or "").strip() for row in rows} - {""}


def _by_code(rows: list[dict]) -> dict[str, dict]:
    """行集合 → {代码: 行}，后写覆盖前写（同码重复行取最后一条）。"""
    return {str(row["ts_code"]).strip(): row for row in rows if row.get("ts_code")}


def _as_float(value: Any) -> float | None:
    try:
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None


def describe_set_diff(em_rows: list[dict], ht_rows: list[dict], label: str) -> PoolDiff:
    """单个池（涨停/跌停）的集合差异摘要：独有股票 + 共有股票的涨跌幅分桶。"""
    em_by, ht_by = _by_code(em_rows), _by_code(ht_rows)
    only_em = sorted(set(em_by) - set(ht_by))
    only_ht = sorted(set(ht_by) - set(em_by))
    shared = sorted(set(em_by) & set(ht_by))

    pct_gap_buckets: dict[str, list[str]] = {}
    for code in shared:
        bucket = classify_pct_gap(
            _as_float(em_by[code].get("pct_change")),
            _as_float(ht_by[code].get("pct_change")),
        )
        pct_gap_buckets.setdefault(bucket, []).append(code)

    union = set(em_by) | set(ht_by)
    return {
        "pool": label,
        "em_count": len(em_by),
        "ht_count": len(ht_by),
        "shared": len(shared),
        "only_em": only_em,
        "only_ht": only_ht,
        "jaccard": round(len(shared) / len(union), 3) if union else 1.0,
        "pct_gap_buckets": pct_gap_buckets,
    }


# ── 取数（网络侧）────────────────────────────────────────────────────


def fetch_pools(
    trade_date: str,
) -> tuple[dict[str, list[dict]], dict[str, list[dict]], list[str]]:
    """单日取两家的池，返回 ``(em, ht, warnings)``。

    东财侧带窗口限制（约 16 个交易日）：超窗日涨停池静默为空、跌停池报错，
    此时差异分析无从谈起，以 warning 说明后跳过该日。
    """
    warnings: list[str] = []
    em_up, up_err = _fetch_limit_up_down(trade_date)
    em_down, down_err = _fetch_limit_down(trade_date)
    if up_err:
        warnings.append(f"东财涨停池: {up_err}")
    if down_err:
        warnings.append(f"东财跌停池: {down_err}")

    client = HithinkClient()
    if not client.available:
        raise SystemExit("HITHINK_FINANCE_API_KEY 未配置，无法取同花顺池（.env 由 core.config 加载）")
    ht_up, ht_down = client.fetch_limit_pools(trade_date)

    em = {"涨停": em_up, "跌停": em_down}
    ht = {"涨停": ht_up, "跌停": ht_down}
    if not any(em.values()) and not any(ht.values()):
        warnings.append("两家池子都为空（可能非交易日或超保留期），跳过")
    return em, ht, warnings


def print_report(day: str, em: dict[str, list[dict]], ht: dict[str, list[dict]]) -> None:
    print(f"\n{'=' * 60}\n📅 {day}\n{'=' * 60}")
    for pool in ("涨停", "跌停"):
        summary = describe_set_diff(em[pool], ht[pool], pool)
        print(f"\n── {pool}池 ──")
        print(
            f"  东财 {summary['em_count']} | 同花顺 {summary['ht_count']} | "
            f"共有 {summary['shared']} | Jaccard {summary['jaccard']}"
        )
        if summary["only_em"]:
            print(f"  仅东财 ({len(summary['only_em'])}): {', '.join(summary['only_em'])}")
        if summary["only_ht"]:
            print(f"  仅同花顺 ({len(summary['only_ht'])}): {', '.join(summary['only_ht'])}")
        buckets = summary["pct_gap_buckets"]
        for bucket in _PCT_BUCKETS:
            if buckets.get(bucket):
                print(f"  {bucket}: {len(buckets[bucket])} 只 → {', '.join(buckets[bucket])}")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "dates",
        nargs="*",
        metavar="YYYY-MM-DD",
        help="要对比的交易日；不给则取最近 --days 个交易日",
    )
    parser.add_argument("--days", type=int, default=5, help="不给日期时取最近 N 个交易日（默认 5）")
    args = parser.parse_args(argv)

    if args.dates:
        days = sorted(set(args.dates))
    else:
        all_days = trading_days_between("2026-01-01", "2026-12-31")
        if not all_days:
            print("交易日历为空（本地缓存不可用？），请显式给日期", file=sys.stderr)
            return 1
        days = all_days[-args.days :]

    exit_code = 0
    for day in days:
        em, ht, warnings = fetch_pools(day)
        for warning in warnings:
            print(f"⚠️  {day}: {warning}", file=sys.stderr)
            if "两家池子都为空" in warning:
                exit_code = 1
        if any(em.values()) or any(ht.values()):
            print_report(day, em, ht)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
