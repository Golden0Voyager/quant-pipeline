"""整日缺席检测
──────────────
“某天跑了、但某列没写”由 ``core/known_gaps.py`` + 股息率巡检覆盖；这里管更粗的一种：
**整天什么都没写**。生产库审计期（``2026-07-24`` 起）里有 6 个这样的交易日，
成因是「部分运行」与「完全未启动」两种（逐日证据见 ``core/known_gaps.py``）。

为什么「最新日期」那套机制发现不了它
────────────────────────────────────
``core/freshness.status_for_table`` 与 ``health_check`` 的字段级断言都只看
``MAX(trade_date)``，所以某天一旦不再是最新日期，它就永久不可见；而写入方的作用域
恰好就是 ``get_expected_latest_trading_day()``，因此那天也不会被回访。两者叠加的
结果：**缺席本身没有任何观察点**——只能靠「数据里那个日期确实缺了」来反推。

探针表
──────
``PROBE_TABLES`` 是「每个交易日都该有行」的表。实测（生产库）：它们在审计期内每个
完整交易日都有行（对照日 09-15：``index_daily`` 4 行 … ``fundamentals`` 5562 行），
而在那 6 个缺席日上**一行都没有**。

判定取「**全部**探针表皆空」而非「任一为空」：后者会把「某一个任务失败」也算成整日
缺席，让这条巡检的含义变模糊——那是部分运行的形态，由 ``core/run_state.py`` 与股息率
巡检各自负责。相应地，``tests/test_day_coverage.py`` 有一条规则门禁：声明的探针表必须
都是 ``TRADING_DAY`` 任务写入的表（防止探针与注册表分叉）。

一次巡检的查询量：每张探针表一条 ``SELECT DISTINCT <日期列> ... BETWEEN ? AND ?``
覆盖整个窗口，共 10 条——不是「每天每表查一次」。
"""

from __future__ import annotations

import sqlite3

from core.calendar import trading_days_between

# 「每个交易日都该有行」的探针表。表名/列名是模块常量（非外部输入），
# 内插进 SQL 与 tasks/utility.py 的既有写法一致。
PROBE_TABLES: tuple[tuple[str, str], ...] = (
    ("fundamentals", "trade_date"),
    ("historical_valuation", "trade_date"),
    ("fund_flow", "trade_date"),
    ("ah_premium", "trade_date"),
    ("block_trade", "trade_date"),
    ("index_daily", "trade_date"),
    ("limit_up_down", "trade_date"),
    ("sector_industry", "trade_date"),
    ("sector_valuation", "trade_date"),
    ("sector_fund_flow", "trade_date"),
)


def date_column(table: str, probe_tables: tuple[tuple[str, str], ...] = PROBE_TABLES) -> str | None:
    """返回探针表的日期列名；非探针表返回 ``None``。"""
    for probe_table, column in probe_tables:
        if probe_table == table:
            return column
    return None


def scan_window(start: str, expected_latest: str) -> tuple[str, str] | None:
    """返回巡检窗口 ``(start, end)``——**不含**最新交易日本身。

    最新那天由新鲜度巡检负责（``status_for_table`` / ``health_check`` 的字段级断言
    本来就是为它写的）。若把它也算进来，管线当天尚未跑完时就会天天报假空洞。

    窗口内不足两个交易日时返回 ``None``（无从判断，调用方应跳过并说明）。
    """
    days = trading_days_between(start, expected_latest)
    if len(days) < 2:
        return None
    return days[0], days[-2]


def _dates_with_rows(
    cursor: sqlite3.Cursor, table: str, column: str, start: str, end: str
) -> set[str] | None:
    """该表在窗口内有行的日期集合；表/列不存在时返回 ``None``。"""
    try:
        rows = cursor.execute(
            f"SELECT DISTINCT {column} FROM {table} WHERE {column} >= ? AND {column} <= ?",
            (start, end),
        ).fetchall()
    except sqlite3.OperationalError:
        return None
    return {str(row[0]) for row in rows if row[0] is not None}


def missing_trading_days(
    cursor: sqlite3.Cursor,
    *,
    start: str,
    end: str,
    probe_tables: tuple[tuple[str, str], ...] = PROBE_TABLES,
) -> list[str]:
    """返回 ``[start, end]`` 内**整日缺席**的交易日，按旧到新排列。

    仅在**至少一张**探针表可用时才判定：测试库或未迁移的库上探针表可能不存在，
    那时应跳过而不是把所有工作日都报成空洞（假空洞比漏报更糟——它会让真空洞被无视）。
    """
    days = trading_days_between(start, end)
    if not days:
        return []

    covered: set[str] = set()
    usable = 0
    for table, column in probe_tables:
        dates = _dates_with_rows(cursor, table, column, start, end)
        if dates is None:
            continue
        usable += 1
        covered |= dates

    if not usable:
        return []
    return [day for day in days if day not in covered]
