"""整日缺席回补入口
──────────────────
``core/day_coverage.py`` 会发现「整天什么都没写」的交易日，``core/known_gaps.py``
把它们登记为已接受的空洞。本模块是那个缺口的**补写入口**：把指定的历史交易日
交给能按日期取数的任务重跑一遍。

关键事实：只有一部分表补得回来
──────────────────────────────
回补能力取决于**源端是否支持历史日期**，与写入方签名无关（2026-09-26 逐一核对源端）：

* **可回补**（源端带日期参数，或返回全历史可按日期选行）：

  =================== ==========================================
  表                   源
  =================== ==========================================
  ``index_daily``       ``stock_zh_index_daily_tx``（全历史，按日期选行）
  ``block_trade``       ``stock_dzjy_mrmx``（``start_date``/``end_date``）
  ``sector_valuation``  ``stock_industry_pe_ratio_cninfo``（``date=``）
  =================== ==========================================

* **部分可回补**（主源只保留最近一段窗口，窗口外靠第二源兜底，更早的日子仍补不回来）：

  =================== ==========================================
  表                   源与限制
  =================== ==========================================
  ``limit_up_down``     东财涨跌停池（``stock_zt_pool_em`` / ``stock_zt_pool_dtgc_em``）
                        只保留最近约 **16 个交易日**的滚动窗口（2026-09-27 实测；
                        跌停池超窗报错、涨停池超窗静默返回空表）。任务在两池皆空时
                        改用**同花顺 HiThink 兜底**（``special-data/limit-up-pool``
                        等，保留约近几个月），因此 2026-09-02 这类东财窗口外的
                        历史日也能补上；但更早的日子仍拿不到，且同花顺不提供
                        ``industry`` 与涨停池换手率。
  =================== ==========================================

* **补不回来**（源端只给实时值，或表本身由缺失的上游派生）：

  ========================== ==================================================
  表                          原因
  ========================== ==================================================
  ``fundamentals``            雪球行情只给实时值
  ``historical_valuation``    是 ``fundamentals`` 的副本，同样只给实时值
  ``ah_premium``              ``stock_zh_ah_spot_em`` 只给实时值
  ``fund_flow``               ``loader.get_market_fund_flow()`` 只给实时值
  ``sector_fund_flow``        同花顺 ``stock_fund_flow_industry`` 只有即时快照
  ``sector_industry``         由 ``fundamentals`` + ``stock_list`` 派生，上游本身就缺
  ========================== ==================================================

``tests/test_backfill.py`` 有一条门禁：上面三组必须正好覆盖 ``day_coverage.PROBE_TABLES``
——探针表增删时这张分类表必须同步，不允许留下「没说过能不能补」的表。

第二种入口：表格级回补（``--backfill-table``，2026-09-27 新增）
─────────────────────────────────────────────────────
``--backfill-days`` 的幂等门是**整天级**的：某天只要任何一张探针表有行，就不再被
当成回补目标。这在混合形态下会卡死：部分运行遗留的那几天，整日回补把缺口补齐之后，
任何**单表残留空洞**（例如当日 ``limit_up_down`` 仍为空）就永远过不了那道门——
该日不再「整天缺席」，而任务作用域又只认 ``get_expected_latest_trading_day()``。
``--backfill-table`` 把幂等门下沉到 **(表, 日)** 粒度解决这个死角：某天某表没有行
就可以对那一对目标重跑，互不影响其他表。其余语义与日期级入口一致：显式日期与
登记册日期共用一套筛选，日期来源自动判定（给了日期 = explicit，没给 = declared），
非法日期与已填过的目标进 ``skipped`` 而不是报错。

（``sector_daily`` 不在探针表里：它的源返回全历史、写入方顺手覆盖了历史日期，
实测 6 个缺席日都有 90 行，从未真正缺过。``index_futures_basis`` 同理不在探针集内。）
"""

from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime

from core.day_coverage import date_column, missing_trading_days
from core.known_gaps import declared_missing_days

# 可回补的「探针表 → 负责任务」映射。任务名不是 daily 路径上的任务名时（如
# ``sector_valuation`` 日常由复合任务 ``update_sector_derivatives`` 写入），
# 用回补专用的审计名，见 daily_pipeline._BACKFILL_TASK_CALLABLES。
BACKFILLABLE_TABLES: tuple[tuple[str, str], ...] = (
    ("index_daily", "update_index_daily"),
    ("block_trade", "update_block_trade"),
    ("sector_valuation", "update_sector_valuation"),
)

# **部分可回补**：源端只保留最近一段窗口的数据，窗口外永远补不回来。这些表仍留在
# 回补注册表里（窗口内的日子要尝试补），但文档/巡检不得再把它们说成「一定补得上」。
# 三元组 = (表名, 责任任务, 限制说明)。
PARTIAL_BACKFILLABLE_TABLES: tuple[tuple[str, str, str], ...] = (
    (
        "limit_up_down",
        "update_limit_up_down",
        "东财涨跌停池只保留最近约 16 个交易日的滚动窗口（2026-09-27 实测："
        "跌停池超窗报错、涨停池超窗静默返回空表）。任务在两池皆空时改用同花顺 "
        "HiThink 兜底（保留约近几个月，能补 2026-09-02 等窗口外历史日），"
        "但更早的日子仍拿不到；同花顺也不提供 industry 与涨停池换手率",
    ),
)

# 已核实**不可回补**的探针表及原因（供巡检报告与文档引用；不要在代码里假装它们可补）。
NOT_BACKFILLABLE_TABLES: tuple[tuple[str, str], ...] = (
    ("fundamentals", "雪球行情只给实时值"),
    ("historical_valuation", "fundamentals 的副本，同样只给实时值"),
    ("ah_premium", "stock_zh_ah_spot_em 只给实时值"),
    ("fund_flow", "get_market_fund_flow() 只给实时值"),
    ("sector_fund_flow", "同花顺 stock_fund_flow_industry 只有即时快照"),
    ("sector_industry", "由 fundamentals + stock_list 派生，上游本身缺失"),
)


@dataclass(frozen=True)
class BackfillPlan:
    """一次回补要处理的日期，以及被跳过的日期及原因。

    Attributes
    ----------
    days:
        真正要回补的交易日，按旧到新排列。
    skipped:
        ``(日期, 跳过原因)``，按旧到新排列。显式指定但已经补上/格式不对的日期会落这里。
    source:
        ``"explicit"``（命令行给了日期）或 ``"declared"``（取登记册里的缺失日）。
    """

    days: tuple[str, ...] = ()
    skipped: tuple[tuple[str, str], ...] = field(default_factory=tuple)
    source: str = "declared"


@dataclass(frozen=True)
class BackfillTablePlan:
    """表格级回补计划：要补的 (表, 日期) 对，以及被跳过的项及原因。

    Attributes
    ----------
    targets:
        ``(表名, 日期)`` 对，旧日到新日、逐日内按注册表顺序排列。
    skipped:
        ``(目标, 原因)``。目标可以是日期串（解析失败/非交易日）或
        ``(表名, 日期)`` 对（该表当日已有行）。
    source:
        ``"explicit"``（命令行给了日期）或 ``"declared"``（取登记册里的缺失日）。
    """

    targets: tuple[tuple[str, str], ...] = ()
    skipped: tuple[tuple[str | tuple[str, str], str], ...] = field(default_factory=tuple)
    source: str = "declared"


def parse_days(spec: str) -> tuple[list[str], list[tuple[str, str]]]:
    """解析逗号/空白分隔的日期串，返回 ``(合法日期, 非法项及原因)``。

    去重并保序（旧到新）。日期必须严格是 ``YYYY-MM-DD``。
    """
    raw = [part for part in spec.replace(",", " ").split() if part]
    days: list[str] = []
    rejected: list[tuple[str, str]] = []
    seen: set[str] = set()
    for item in raw:
        try:
            parsed = datetime.strptime(item, "%Y-%m-%d").date().isoformat()
        except ValueError:
            rejected.append((item, "不是合法的 YYYY-MM-DD 日期"))
            continue
        if parsed in seen:
            continue
        seen.add(parsed)
        days.append(parsed)
    days.sort()
    return days, rejected


def is_still_absent(cursor: sqlite3.Cursor, day: str) -> bool:
    """该交易日在生产库里是否**仍然**整日缺席（全部探针表皆空）。

    非交易日、或探针表不可用时返回 ``False``——「无法判定」不得当成「缺」，
    否则会去回补一个根本不存在的交易日。
    """
    return day in missing_trading_days(cursor, start=day, end=day)


def resolve_backfill_days(cursor: sqlite3.Cursor, spec: str | None) -> BackfillPlan:
    """把命令行输入解析成回补计划。

    ``spec`` 为空/空白（如只写了 ``--backfill-days``）→ 取 ``core/known_gaps.py``
    的已登记缺失日；否则按 ``spec`` 里显式给出的日期。两种来源都只会保留
    **仍然缺席**的日期：已经补上的直接跳过（幂等，避免无谓的网络请求）。
    """
    explicit = bool(spec and spec.strip())
    if explicit:
        requested, rejected = parse_days(spec or "")
        source = "explicit"
    else:
        requested, rejected = sorted(declared_missing_days()), []
        source = "declared"

    days: list[str] = []
    skipped: list[tuple[str, str]] = list(rejected)
    for day in requested:
        if is_still_absent(cursor, day):
            days.append(day)
        else:
            skipped.append((day, "该日已有数据（或不是交易日），无需回补"))
    days.sort()
    skipped.sort()
    return BackfillPlan(days=tuple(days), skipped=tuple(skipped), source=source)


# 「形似日期」的判定：合法 YYYY-MM-DD 之外，也把斜杠式/紧凑式当成日期意图，
# 用于 source 分类（是「给了日期」还是「没给」），不用于日期解析本身。
_DATE_HINT_RE = re.compile(r"\d{4}[-/]\d{1,2}[-/]\d{1,2}|\d{8}")


# 回补注册表里 (表名 → 责任任务) 的全集，含部分可回补表。任务名由
# daily_pipeline._BACKFILL_TASK_CALLABLES 兑现（tests/test_backfill.py 门禁）。
def backfill_task_by_table() -> dict[str, str]:
    """表名 → 责任任务的全集（可回补 + 部分可回补）。"""
    return dict(BACKFILLABLE_TABLES) | {
        table: task for table, task, _ in PARTIAL_BACKFILLABLE_TABLES
    }


def table_has_rows(cursor: sqlite3.Cursor, table: str, day: str) -> bool | None:
    """该表在 ``day`` 是否已有行；表不存在时返回 ``None``（无法判定）。

    「无法判定」不得当成「已有行」：那会把可回补的表悄悄跳过。
    """
    column = date_column(table)
    if column is None:
        raise ValueError(f"未知回补表：{table}")
    try:
        row = cursor.execute(
            f"SELECT 1 FROM {table} WHERE {column} = ? LIMIT 1",
            (day,),
        ).fetchone()
    except sqlite3.OperationalError:
        return None
    return row is not None


def resolve_backfill_targets(
    cursor: sqlite3.Cursor,
    spec: str | None,
) -> BackfillTablePlan:
    """把 ``--backfill-table`` 的命令行输入解析成 (表, 日) 回补计划。

    ``spec`` 里的条目既可以是表名（回补注册表里的表），也可以是 ``YYYY-MM-DD``
    日期；其余形式的条目原样进 ``skipped``（不猜测意图）。两种条目可任意混写。

    * 只给表名 → 那些表 × 登记册缺失日；
    * 只给日期 → 全部可回补表 × 那些日期；
    * 什么都没给（裸旗标）→ 全部可回补表 × 登记册缺失日。

    日期来源自动判定：给了日期 = explicit，没给 = declared。幂等门在
    **(表, 日)** 粒度：该表当日已有行就跳过（表不存在时视为仍然缺失，照样尝试）。
    """
    tasks_by_table = backfill_task_by_table()
    all_tables = tuple(tasks_by_table)

    raw = [part for part in (spec or "").replace(",", " ").split() if part]
    tables: list[str] = []
    days: list[str] = []
    skipped: list[tuple[str | tuple[str, str], str]] = []
    date_like = False
    for item in raw:
        if item in tasks_by_table:
            if item not in tables:
                tables.append(item)
            continue
        try:
            day = datetime.strptime(item, "%Y-%m-%d").date().isoformat()
        except ValueError:
            skipped.append((item, "既不是回补表名，也不是合法的 YYYY-MM-DD 日期"))
            # 形似日期的非法项（如 ``2026/09/14``）也算「用户给了日期」：
            # 否则会静默落到 declared 分支，突然回补全表 × 登记册日。
            if _DATE_HINT_RE.fullmatch(item):
                date_like = True
            continue
        date_like = True
        if day not in days:
            days.append(day)
    days.sort()

    if date_like:
        source = "explicit"
    else:
        days = sorted(declared_missing_days())
        source = "declared"
    if not tables:
        tables = list(all_tables)

    targets: list[tuple[str, str]] = []
    for day in days:
        for table in tables:
            has_rows = table_has_rows(cursor, table, day)
            if has_rows:
                skipped.append(((table, day), "该表当日已有数据，无需回补"))
            else:
                targets.append((table, day))
    return BackfillTablePlan(
        targets=tuple(targets),
        skipped=tuple(skipped),
        source=source,
    )
