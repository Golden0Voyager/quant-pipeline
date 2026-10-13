#!/usr/bin/env python3
"""
修复 fund_flow 里被「伪零值」污染的行。

问题（2026-10-13 生产库只读核实）：
    同花顺降级兜底（providers._ths_fund_flow_fallback）只提供「净额」，
    docstring 也写明「其余字段留空」；但它产出的 DataFrame 里根本没有那些列，
    `row.get()` 返回 NaN，而写入层 `database._coerce_float(NaN, default=0.0)`
    把「无数据」静默写成了 **0.0**。

    后果是 5 个派生字段（超大单/大单及其占比、主力占比）变成**假真值**：
        - 36,470 行受影响（09-24 ~ 10-12，7 个交易日）
        - 5,216 只股票的「最新一行」被污染 —— backtest.py 取 MAX(trade_date)
          那一行读超大单净流入，读到的是 0 元
    09-18 之前 0 行受影响：那段是东财榜单端点写的，恒等式成立。

为什么必须连主力净额一起修：
    同花顺的净额对不上东财的任何一天（30 只样本 0 命中），不是错位而是不同口径；
    且它以「万」为粒度四舍五入（`5805.91万`、`-1.22亿`）、是盘中实时快照、
    自身不带日期（日期是我们贴的）。留着它等于留一个无法追溯、无法复现的数字。
    东财侧则自洽：榜单端点与逐票端点对同一 (票, 日) 40/40 完全一致。

修复策略（两步一体，优先级不可颠倒）：
    1. push2his 能取到该 (票, 日) → 用东财的六个字段**全量覆盖**
    2. 取不到（停牌/退市/超窗）→ 只把 5 个派生字段置 **NULL**，保留原主力净额
    3. 完全取不到该票 → 同样只置 NULL

    为什么不可只用第 2 步：主力净额本身是假的，只清分解等于留一颗雷。
    为什么不可只用第 1 步：push2his 覆盖不到的票仍留着 0.0 假值。
    NULL 与 0.0 在资金流分析里含义相反，而聚合/排序/回测都不会跳过 0.0。

只 UPDATE 已存在的行，从不 INSERT —— 缺失 (票, 日) 由
scripts/backfill_fund_flow_gaps.py 负责，两者职责不重叠。

用法：
    uv run python scripts/repair_fund_flow_decomposition.py --dry-run   # 先看报告
    uv run python scripts/repair_fund_flow_decomposition.py             # 执行
    uv run python scripts/repair_fund_flow_decomposition.py --limit 50  # 先试 50 只
"""
from __future__ import annotations

import argparse
import logging
import os
import sqlite3
import sys
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# 禁用代理环境变量（东财走环境代理会被源站拒连，scripts/repair_turnover.py 同款）。
for _var in (
    "http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "all_proxy",
):
    os.environ.pop(_var, None)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# 仓库根目录必须就地注入：本脚本以 `python scripts/repair_fund_flow_decomposition.py`
# 直接执行，此时 `sys.path[0]` 是 scripts/，还 import 不到 core。统一写法与门禁见
# core/_bootstrap.py 与 tests/test_sys_path_bootstrap.py。
_REPO_ROOT = str(Path(__file__).resolve().parent.parent)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from core.db_pragmas import apply_write_pragmas  # noqa: E402
from core.fund_flow_source import (  # noqa: E402
    DERIVED_COLUMNS,
    fetch_day_klines,
    is_weekend,
    make_session,
    parse_kline,
)

DEFAULT_DB_PATH = Path("~/Code/quant_data/quant_core.db").expanduser()

# 污染指纹：同花顺行的 5 个派生字段全是 0.0，而主力净额非 0。
# 东财行恒等式成立（主力 = 大单 + 超大单），不可能同时出现 large=0 且 super_large=0
# 而 main<>0，所以这个指纹不会误伤真实数据。
_POISONED_WHERE = """
    large_net_inflow = 0
    AND super_large_net_inflow = 0
    AND main_net_inflow <> 0
"""

_SELECT_POISONED = f"""
    SELECT ts_code, trade_date, main_net_inflow
    FROM fund_flow
    WHERE {_POISONED_WHERE}
      AND trade_date >= ?
      AND strftime('%w', trade_date) NOT IN ('0', '6')
    ORDER BY ts_code, trade_date
"""

# 两条 UPDATE 都带上污染指纹作为 WHERE 条件。这有两个作用：
#   1. 幂等 —— 修完的行不再满足指纹，重跑匹配 0 行；
#   2. total_changes 才诚实 —— SQLite 把「值没变的 UPDATE」也算作一次变更，
#      不加指纹的话重复执行会一直报「改了 N 行」。
_UPDATE_ALL = f"""
    UPDATE fund_flow SET
        main_net_inflow = ?, main_net_inflow_pct = ?,
        super_large_net_inflow = ?, super_large_net_inflow_pct = ?,
        large_net_inflow = ?, large_net_inflow_pct = ?
    WHERE ts_code = ? AND trade_date = ? AND {_POISONED_WHERE}
"""

_UPDATE_DERIVED_NULL = f"""
    UPDATE fund_flow SET
        {", ".join(f"{col} = NULL" for col in DERIVED_COLUMNS)}
    WHERE ts_code = ? AND trade_date = ? AND {_POISONED_WHERE}
"""


def connect(db_path: Path) -> sqlite3.Connection:
    """写连接。WAL + 体积上限的唯一出处是 core.db_pragmas，勿内联。"""
    conn = sqlite3.connect(str(db_path), timeout=30.0)
    apply_write_pragmas(conn, busy_timeout_ms=30000)
    return conn


def find_poisoned(db_path: Path, since: str, limit: int = 0) -> list[tuple[str, str, float]]:
    """返回 [(code, day, main_net_inflow)]，即待修复行。只读连接。"""
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=30.0)
    try:
        sql = _SELECT_POISONED
        params: list[object] = [since]
        if limit:
            sql += " LIMIT ?"
            params.append(limit)
        return [(str(c), str(d), float(m)) for c, d, m in conn.execute(sql, params)]
    finally:
        conn.close()


def build_plan(
    session: object,
    poisoned: list[tuple[str, str, float]],
    workers: int = 8,
) -> tuple[list[tuple], list[tuple[str, str]]]:
    """把待修复行分成「东财可覆盖」与「只能置 NULL」两组。

    返回 (overwrites, nulls)。overwrites 的每项是
    (code, day, 六个数值)。nulls 是 (code, day)。
    """
    by_code: dict[str, set[str]] = {}
    for code, day, _ in poisoned:
        by_code.setdefault(code, set()).add(day)

    fetched: dict[str, dict[str, dict]] = {}
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = {
            pool.submit(fetch_day_klines, session, code): code for code in by_code
        }
        for fut in as_completed(futures):
            code = futures[fut]
            parsed: dict[str, dict] = {}
            for raw in fut.result():
                rec = parse_kline(raw)
                if rec is None:
                    continue
                day = str(rec["date"])
                if day in by_code[code] and not is_weekend(day):
                    parsed[day] = rec
            fetched[code] = parsed

    overwrites: list[tuple] = []
    nulls: list[tuple[str, str]] = []
    for code, day, main_before in poisoned:
        rec = fetched.get(code, {}).get(day)
        if rec is None:
            nulls.append((code, day))
            continue
        # 元组顺序必须与 _UPDATE_ALL 的占位符顺序一致：六个值在前、(票, 日) 在后。
        # 顺序写反不会报错，只会让 WHERE 匹配不到任何行、静默零变更。
        overwrites.append((
            rec["main_net_inflow"],
            rec["main_net_inflow_pct"],
            rec["super_large_net_inflow"],
            rec["super_large_net_inflow_pct"],
            rec["large_net_inflow"],
            rec["large_net_inflow_pct"],
            code,
            day,
            main_before,
        ))
    return overwrites, nulls


def apply_plan(
    conn: sqlite3.Connection,
    overwrites: list[tuple],
    nulls: list[tuple[str, str]],
) -> int:
    """执行修复，返回实际变更行数（``total_changes`` 差值，不是提交条数）。

    两条 UPDATE 的 WHERE 都带污染指纹，所以重复执行会匹配 0 行并返回 0。
    """
    before = conn.total_changes
    conn.executemany(_UPDATE_ALL, [row[:8] for row in overwrites])
    conn.executemany(_UPDATE_DERIVED_NULL, nulls)
    conn.commit()
    return conn.total_changes - before


def summarize(rows: Iterable[tuple[str, str, float]]) -> str:
    days = sorted({d for _, d, _ in rows})
    if not days:
        return "无"
    return f"{days[0]} ~ {days[-1]}（{len(days)} 个交易日）"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH, help="数据库路径")
    parser.add_argument(
        "--since",
        default="2026-09-18",
        help="只修该日期（含）之后的污染行（默认：东财榜单端点开始失败的日期）",
    )
    parser.add_argument("--limit", type=int, default=0, help="只处理前 N 行（0 = 全部）")
    parser.add_argument("--workers", type=int, default=8, help="并发线程数（默认 8）")
    parser.add_argument("--dry-run", action="store_true", help="只打印报告，不写库")
    args = parser.parse_args()

    if not args.db.exists():
        logger.error(f"❌ 数据库不存在: {args.db}")
        return 1

    poisoned = find_poisoned(args.db, args.since, args.limit)
    if not poisoned:
        logger.info(f"✅ {args.since} 起没有伪零值污染行，无需修复")
        return 0

    logger.info(f"🔍 待修复: {len(poisoned)} 行 / {len({c for c, _, _ in poisoned})} 只")
    logger.info(f"   区间: {summarize(poisoned)}")
    if args.limit:
        logger.info(f"   --limit {args.limit}")

    overwrites, nulls = build_plan(make_session(), poisoned, args.workers)

    # 报告主力净额将被改动的幅度 —— 这是本脚本唯一不可逆的部分
    # 元组末位是原主力净额（main_before），用来报告「同花顺值 → 东财值」的改动幅度
    deltas = [
        abs(row[0] - row[8]) for row in overwrites
        if row[0] is not None and row[8] is not None
    ]
    if deltas:
        ordered = sorted(deltas)
        logger.info(
            f"📊 主力净额将被改动: 中位 {ordered[len(ordered) // 2] / 1e8:.2f} 亿 / "
            f"最大 {ordered[-1] / 1e8:.2f} 亿（同花顺值 → 东财值）"
        )
    logger.info(
        f"   东财可全量覆盖: {len(overwrites)} 行；仅置 NULL（源端取不到）: {len(nulls)} 行"
    )

    if args.dry_run:
        logger.info("🧪 dry-run：未写库")
        for row in sorted(overwrites, key=lambda r: (r[6], r[7]))[:5]:
            logger.info(
                f"    覆盖 {row[6]} {row[7]}: 主力 {row[8]:,.0f} → {row[0]:,.0f}"
            )
        for code, day in sorted(nulls)[:5]:
            logger.info(f"    置NULL {code} {day}: 东财取不到该日，保留原主力净额")
        if len(overwrites) + len(nulls) > 10:
            logger.info(f"    … 其余 {len(overwrites) + len(nulls) - 10} 行省略")
        return 0

    conn = connect(args.db)
    try:
        changed = apply_plan(conn, overwrites, nulls)
    finally:
        conn.close()
    logger.info(f"✅ 修复完成: 实际变更 {changed} 行")
    return 0


if __name__ == "__main__":
    sys.exit(main())
