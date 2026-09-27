"""已声明、已接受、且经核实不可回补的数据空洞登记册。

为什么需要它
────────────
某些日期上的某个字段永久性地没有被写入。以 ``fundamentals.dividend_yield``
为例，2026-08 至 2026-09 有 6 个交易日整列为 NULL。这些空洞**回补不了**，
三条路都断掉（2026-09-24 逐一核实）：

1. 雪球行情接口（唯一写入方 ``update_market_snapshot``）只返回**实时**值，
   历史日期的股息率拿不到；
2. ``historical_valuation.dividend_yield`` 是 ``fundamentals`` 的副本，
   同样为空（2,824,046 行里只有 2026-06 之后有值）；
3. ``dividend_summary`` 只有无日期的汇总值（``avg_annual_dividend``），
   不是逐日快照。

不登记的后果有两层，第二层才是真正致命的：

* 表面：每次巡检都重报同一批旧空洞，噪音无法归零 —— 2026-08-08 至
  08-25 的 ``dividend_yield 非空率过低: 0.0%`` 连续告警 8 次无人处理，
  正是这个机制在起作用；
* 实质：**新增**空洞会淹没在这批噪音里而无人分辨。

所以这里是「显式声明」而非「静默豁免」：每条都写明日期、影响范围、成因。
``tasks/utility.py::health_check`` 只对**不在册**的空洞告警。

第二种形态：整日缺席（2026-09-24 新增）
──────────────────────────────────────
上一种空洞是「某天跑了，但某个字段没写」；还有一种更粗的：**整天什么都没写**。
以审计期（2026-07-24 起）为例，有 6 个交易日对**全部**探针表都没有任何行：
``2026-08-03``/``08-19``/``08-21``/``09-02``/``09-14``/``09-16``。逐日查证后
它们其实是**两种不同的形态**（这是本节的要点，不要合并叙述）：

* **部分运行**（08-03 / 08-19 / 08-21 / 09-02）：当天天管线**启动过**，但只跑了
  一小部分任务就结束——日志与 ``ingestion_runs`` 都在。例如 08-21 只有 1 条记录
  （``health_check`` degraded），08-03 只有 2 条，08-19/09-02 各 12–13 条且**不含**
  任何按交易日写入探针表的任务。这正是 ``core/run_state.py`` 那一类问题的历史版本
  ——但 ``run_state`` 是 2026-09-24 才加的，这些历史日无标记可查。
* **完全未启动**（09-14 / 09-16）：既无日志文件、也无任何任务记录。

实测受影响的表：每个交易日都应有行的 10 张表在这 6 天全空
（``fundamentals``/``historical_valuation``/``fund_flow``/``ah_premium``/``block_trade``/
``index_daily``/``limit_up_down``/``sector_industry``/``sector_valuation``/
``sector_fund_flow``，对照日 09-15 分别有 4–5562 行）。

为何不会自愈：这些任务的作用域固定在 ``get_expected_latest_trading_day()``
（写入方签名原本都不接受日期参数），因此那天一旦过去就再也不会被回访
——与上一种空洞同一机理。仓库里已有的自愈先例是 ``update_margin_trading``：它回看
最近 3 个交易日、逐日尝试直到成功（实测后果：``margin_trading`` 在 09-14 有 4105 行，
而 09-15/09-16 反为空——回看只在当日源端尚未发布时才会落到前一日）。把这个模式
推广到其余任务，是**独立工作项**（整日缺席的回补入口已于 2026-09-26 落地：
``daily_pipeline.py --backfill-days``，日期筛选与分类见 ``core/backfill.py``）。

回补判定（2026-09-26 按**源端**重新逐一复核，2026-09-27 按实测回补结果修正）：

* **不可回补**（源端只给实时值）：``fundamentals``/``historical_valuation``（雪球接口）、
  ``ah_premium``（``stock_zh_ah_spot_em``）、``fund_flow``（``get_market_fund_flow()``）。
  另有两张不是「抓不到」而是**派生**：``sector_fund_flow``（同花顺
  ``stock_fund_flow_industry`` 只有即时快照）、``sector_industry``（由 ``fundamentals``
  + ``stock_list`` 派生，上游本身就缺）。
* **可回补（3 张）**：``index_daily``（``stock_zh_index_daily_tx`` 返回全历史，按日期选行）、
  ``block_trade``（``stock_dzjy_mrmx`` 的 ``start_date``/``end_date``）、
  ``sector_valuation``（``stock_industry_pe_ratio_cninfo`` 的 ``date=``）。
* **部分可回补（1 张）**：``limit_up_down``——东财涨跌停池只保留最近约 16 个交易日的
  滚动窗口（2026-09-27 实测；跌停池超窗报错、涨停池超窗静默返回空表）。任务在两池皆空时
  改用同花顺 HiThink 兜底（保留约近几个月），因此 2026-09-02 这类东财窗口外的历史日也能
  补上（同花顺不提供 industry 与涨停池换手率）。这三张 + 这张都已接上 ``--backfill-days``
  入口（更早的日子仍补不回来）。
* 此前这里写的「``sector_*`` 走 ``stock_board_industry_hist_em``、``limit_up_down`` 走
  ``stock_lhb_detail_em``」是错的：前者是 ``sector_daily`` 的源（而该表源端返回全历史、
  实测从未真正缺过），后者是龙虎榜的源。分类以 ``core/backfill.py`` +
  ``tests/test_backfill.py`` 的门禁为准，不要在这里重述细节。

因此这里也是「已声明」而不是「装作无事」：漏跑的那几天，数据确实少了，
而且其中一部分可能永远补不回来。
"""

from __future__ import annotations

from dataclasses import dataclass

# ``ingestion_runs`` 开始逐轮记录审计的日期。早于此的自建库回填时代（bulk
# backfill 只写 pe_ttm/pb/ps_ttm/market_cap 四列）没有逐任务审计轨迹，无法
# 归因到具体哪一轮跑漏了什么，故不纳入巡检范围。
AUDIT_ERA_START = "2026-07-24"


@dataclass(frozen=True)
class KnownGap:
    """一条已声明、已接受的数据空洞。

    Attributes
    ----------
    table:
        受影响的表名。
    column:
        受影响且整列为空的列名。
    date:
        受影响的交易日（``YYYY-MM-DD``）。
    cause:
        已核实的成因，含为何不可回补。
    """

    table: str
    column: str
    date: str
    cause: str


# 每条都可在生产库 + ingestion_runs 里复核：
#   SELECT COUNT(*), COUNT(dividend_yield) FROM fundamentals WHERE trade_date = '<date>';
#   以及该日期在 ingestion_runs 里 update_market_snapshot 的记录。
KNOWN_GAPS: tuple[KnownGap, ...] = (
    KnownGap(
        "fundamentals",
        "dividend_yield",
        "2026-08-10",
        "当日仅 26 个任务运行（非完整管道），update_market_snapshot 未执行",
    ),
    KnownGap(
        "fundamentals",
        "dividend_yield",
        "2026-08-12",
        "抓到 5203 条报价但每条 dividend_yield 均为 None，实际写入 0 行；"
        "旧版仍返回 success，空写被掩盖",
    ),
    KnownGap(
        "fundamentals",
        "dividend_yield",
        "2026-08-20",
        "当日 38 个任务运行（非完整管道），update_market_snapshot 未执行",
    ),
    KnownGap(
        "fundamentals",
        "dividend_yield",
        "2026-08-25",
        "任务执行但雪球返回 0 条报价（saved_rows=0），旧版仍返回 success，空写被掩盖",
    ),
    KnownGap(
        "fundamentals",
        "dividend_yield",
        "2026-09-03",
        "当日 48 个任务运行但 update_market_snapshot 未执行",
    ),
    KnownGap(
        "fundamentals",
        "dividend_yield",
        "2026-09-17",
        "部分运行中断：31 个任务（含 update_fundamentals，无 update_market_snapshot），"
        "且当日 health_check 亦未运行，故无人发现",
    ),
)


@dataclass(frozen=True)
class MissingDay:
    """一个**整日缺席**的交易日：当天对所有按交易日应有的表都没有写入任何行。

    Attributes
    ----------
    date:
        缺席的交易日（``YYYY-MM-DD``）。
    cause:
        已核实的成因。整日缺席的成因几乎都是「当天天管线未被触发」——本管线靠
        手动触发（见模块 docstring），所以成因要写明具体证据，不能只写「没跑」。
    """

    date: str
    cause: str


# 每条都可在生产库 + 日志目录里复核：
#   SELECT task_name, status FROM ingestion_runs WHERE started_at LIKE '<date>%';
#   ls ~/Code/quant_data/logs/smartmoney_<date 去横线>.log
#   SELECT COUNT(*) FROM fundamentals WHERE trade_date = '<date>';  -- 应为 0，而对照日非 0
KNOWN_MISSING_DAYS: tuple[MissingDay, ...] = (
    MissingDay(
        "2026-08-03",
        "部分运行后被终止：仅 2 条任务记录（update_chip_distribution_em failed、"
        "health_check degraded），日志末行为 `daily_pipeline.py --task all --force exited with code 143`",
    ),
    MissingDay(
        "2026-08-19",
        "部分运行：13 条任务记录（update_bars/update_futures/retry/health_check 等），"
        "不含任何按交易日写入探针表的任务",
    ),
    MissingDay(
        "2026-08-21",
        "只跑了 health_check（1 条记录，degraded），无任何数据任务",
    ),
    MissingDay(
        "2026-09-02",
        "部分运行：12 条任务记录（update_bars degraded 等），不含 fundamentals/"
        "historical_valuation/fund_flow 等写入方",
    ),
    MissingDay("2026-09-14", "既无日志文件也无任务记录 ⇒ 当天天管线未被启动"),
    MissingDay("2026-09-16", "既无日志文件也无任务记录 ⇒ 当天天管线未被启动"),
)


def declared_missing_days() -> frozenset[str]:
    """全部已声明的整日缺席日期。"""
    return frozenset(day.date for day in KNOWN_MISSING_DAYS)


def is_known_missing_day(date: str) -> bool:
    """该交易日的整日缺席是否已声明。"""
    return date in declared_missing_days()


def describe_missing_day(date: str) -> str | None:
    """返回该整日缺席的已核实成因；未登记时返回 ``None``。"""
    for day in KNOWN_MISSING_DAYS:
        if day.date == date:
            return day.cause
    return None


def known_gap_dates(table: str, column: str) -> frozenset[str]:
    """返回 *table*.*column* 上全部已声明的空洞日期。"""
    return frozenset(g.date for g in KNOWN_GAPS if g.table == table and g.column == column)


def is_known_gap(table: str, column: str, date: str) -> bool:
    """*table*.*column* 在 *date* 上的空洞是否已被声明。"""
    return date in known_gap_dates(table, column)


def describe_known_gap(table: str, column: str, date: str) -> str | None:
    """返回该空洞的已核实成因；未登记时返回 ``None``。"""
    for gap in KNOWN_GAPS:
        if gap.table == table and gap.column == column and gap.date == date:
            return gap.cause
    return None
