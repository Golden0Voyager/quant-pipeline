"""
概念板块缺口回补
────────────────
``concept_board`` 由 ``update_concept_board`` 经 ``push2`` **实时快照**写入，而快照
接口只给「此刻」、不留历史：某天抓取失败 → 该天永久不可恢复。库内形态即此 —
2026-09-29 实测已有 31 个交易日、跨度内应有 50 个，日期跳跃（09-28, 09-20,
09-18, 09-17…）标的是「那天拍成了」。

本模块用东财**历史**接口 ``stock_board_concept_hist_em`` 补回这些日子，与
``tasks/sector_derivatives.py`` 已验证的行业板块模式对称（该任务让 ``sector_daily``
的日期保持连续、从未真正缺过）。

两条硬约束
──────────
1. **严格不写 ``expected`` 当天。** 快照已写该日且带 ``up_count``/``down_count``，
   而历史 K 线没有这两列；``providers.save_concept_board_batch`` 用
   ``INSERT OR REPLACE``，回补一旦覆盖到 ``expected`` 就会用 NULL 抹掉真值。
2. **只补缺口，不做全量重写。** 单个概念失败即跳过并计数，不中断其余——
   回补是尽力而为，一次失败不该让 504 个概念白跑。
"""

from __future__ import annotations

import logging
import math
import sqlite3
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pandas as pd

from core.calendar import (
    get_expected_latest_trading_day,
    get_recent_trading_days,
)
from core.known_gaps import declared_missing_days
from core.source_client import get_default_client
from interface import DatabaseInterface
from tasks.concept_board import _fetch_concept_list_em

try:
    import akshare as ak
except ImportError:
    ak = None

logger = logging.getLogger(__name__)

# 窗口默认值的含义：504 个概念 × **可用**缺口天数 = 请求数。有效窗口比 lookback_days
# 少一天——get_recent_trading_days 给的是「含 expected 在内」的 N 个交易日
# （core/calendar.py 用 `d <= end_date`），而 expected 永不入窗（上面硬约束 1），
# 于是默认 10 对应 9 个可用交易日 = 4536 次请求、约 22 分钟。这是成本上界，
# 超出窗口的缺口不会被自动发现——回补是手动任务，这是它的代价。
DEFAULT_LOOKBACK_DAYS = 10


# 板块覆盖下界：某天抓到的**不同板块**数低于 ``len(boards) *`` 本值，就判定那天只
# 补了一半，不写入、不记为已回补、留在缺口集里等下一轮重试。
#
# 为什么钉在**写入时**、而不是让 ``find_missing_days`` 学会「每天应该有多少板块」：
# 后者要改 Task 1 的纯函数和它的整套用例。本机生产库只读核验（2026-09-29）显示
# 31 个交易日、15613 行，最少的一天 495 个板块、最多的 504 个，``data_source``
# 全是 'em'，``up_count`` 为 NULL 的只有 3 行——现存数据里**没有一天是残缺的**，
# 没有需要自愈的历史空洞，所以写入前这道闸就够。
#
# 为什么是 0.8：两种错判的代价完全不对称。误判（真有某天因故只有 <80% 的板块）
# = 那天不写、留在缺口集、下轮自动重试，代价是一轮请求。漏判 = 那天只落零星几行、
# 被记成已回补、而 ``find_missing_days`` 只看「那天有没有行」，于是**永久**出局——
# 正是 2026-08-12 P2-12 与 ``core/known_gaps.py`` 存在的理由。实测最差的一天覆盖率
# 是 495/504 ≈ 98.2%，0.8 在它下方 18 个百分点，正常响应碰不到；而熔断那种形态
# 是 5/504 ≈ 1%，任何合理下界都拦得住。宁可误判重试，不可漏判丢失。
MIN_BOARD_COVERAGE = 0.8


def find_missing_days(
    have: Iterable[str],
    *,
    expected: str,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    declared: frozenset[str] | None = None,
) -> list[str]:
    """返回需要回补的交易日（升序）。**严格不含 expected 当天。**

    窗口 = 最近 ``lookback_days`` 个交易日 ∩ ``< expected``；再减去库内已有的
    与 ``declared``（已登记的整日缺席，源端不可回补）。

    Args:
        have: 库内 ``concept_board`` 已有的 ``trade_date`` 集合。
        expected: ``get_expected_latest_trading_day()``。
        lookback_days: 窗口大小，单位是**交易日**；``get_recent_trading_days`` 返回的
            N 天**含** ``expected``，而它永不入窗，故实际可用窗口是 N-1 天。
        declared: 已登记的整日缺席日期；None 时取 ``declared_missing_days()``。
    """
    skip = declared_missing_days() if declared is None else declared
    present = set(have)
    recent = get_recent_trading_days(expected, lookback_days)
    return sorted(
        day
        for day in recent
        if day < expected and day not in present and day not in skip
    )


# 历史行与快照行靠这个值区分：快照有涨跌家数，历史没有。
HISTORY_SOURCE = "em_hist"

# 列名按同族接口 stock_board_industry_hist_em 推定（tasks/sector_derivatives.py
# 已在生产验证）。待验证项见 spec：东财线路自 2026-09-29 起分钟级抖动，该接口的
# 真实列名尚未实测，因此这里用「能识别多少写多少」而不是硬断言。
_HIST_COL_MAP = {
    "日期": "trade_date",
    "开盘": "open",
    "收盘": "close",
    "最高": "high",
    "最低": "low",
    "成交量": "volume",
    "成交额": "amount",
    "涨跌幅": "pct_change",
    # 换手率：源给、``concept_board.turnover REAL`` 也存（providers.py:373），
    # 之前漏映射 → 回补行的 turnover 恒 NULL，白丢一个可用字段。
    "换手率": "turnover",
}
# 记录键集固定，缺列写 None。行与行之间键不一致会让下游按 key 取值踩 KeyError。
# ⚠️ 这张表与上面那张字典、以及下面 records.append 的键集是**三处必须同步**的：
#    tests/test_concept_board_backfill.py 的键集断言直接引用本表，改一处就红。
# open/close/high/low/volume/amount 映射着但 ``save_concept_board_batch`` 的 INSERT
# 不写它们——保留是为了与 tasks/sector_derivatives.py 的先例一致（sector_daily 真存），
# 将来 concept_board 加列时它们已经就位。
_HIST_VALUE_COLS = (
    "open", "close", "high", "low", "volume", "amount", "pct_change", "turnover",
)


def _records_from_hist_df(
    df: pd.DataFrame, *, concept_code: str, concept_name: str
) -> list[dict]:
    """东财历史 K 线 → concept_board 记录。``up_count``/``down_count`` 恒为 None。

    ``df is None`` 这道守卫静态上不可达（``SourceClient.call`` 只在 operation 返回时
    才给 ``success=True``，而该 operation 返回 DataFrame 或抛异常），但它是**第三手**
    不可信载荷前唯一的闸。真走到时，代价只是白白遍历一次空表——本函数的所有调用方
    （``fetch_day_records`` 的 per-board try）都已把异常边界兜住，所以这里**不必**再
    自己吞异常：职责分层是「循环按板块兜住」，不是每层都吞一遍。返回类型写非 Optional
    是因为那才是**约定**，不是对未类型化上游的断言。
    """
    if df is None or df.empty:
        return []
    df = df.rename(columns=_HIST_COL_MAP)
    if "trade_date" not in df.columns:
        logger.warning("⚠️ 概念板块历史缺少日期列，跳过该板块")
        return []
    # 一列值都没认出来 → 这个板块**一条都不产出**（同 tasks/sector_derivatives.py:110）。
    # 「宽松映射」只该容忍**多**出来的列，不该容忍**全**缺失：照旧放行的话，源端把
    # 「涨跌幅」改名后 504 个板块每个仍返回 1 条记录，而 pct_change/turnover/… 全是
    # None——``save_concept_board_batch`` 照收、任务报 success，而 ``find_missing_days``
    # 下次看到那天「已经有了」就再也不会回来。那正是 2026-08-12 P2-12 的空洞形状。
    available = [c for c in _HIST_VALUE_COLS if c in df.columns]
    if not available:
        logger.warning(
            f"⚠️ 概念板块历史值列全部漂移，实到列 {list(df.columns)}，跳过该板块"
        )
        return []
    # 告警方向是「**已映射的列消失了**」，不是「来了没映射的列」：东财正常就多给
    # 振幅/涨跌额（该接口实测 11 列，见本机 akshare 源码），按反向判据则每次健康
    # 响应都告警——一次 504×9 的回补刷 4536 条一模一样的噪音，真漂移反而被淹没。
    missing = [c for c in _HIST_VALUE_COLS if c not in df.columns]
    if missing:
        logger.warning(f"⚠️ 概念板块历史缺少已映射列 {missing}，这几列写 None")
    records: list[dict] = []
    for _, row in df.iterrows():
        raw_day = row.get("trade_date")
        # pd.isna 守卫：str(None) 是 'None'、str(nan) 是 'nan'，两个都 truthy，
        # `if not day` 抓不住，而 concept_board.trade_date 是 DATE NOT NULL 也不管用
        # ——SQLite 不做类型检查，'None' 会真的落库并污染 find_missing_days 的 have。
        if raw_day is None or pd.isna(raw_day):
            continue
        day = str(raw_day).strip()[:10]
        if not day:
            continue
        # 键集固定：未识别的列写 None 而不是省略，否则同一批记录里有的行有
        # pct_change、有的行没有，下游按 key 取值会踩 KeyError。
        records.append({
            "trade_date": day,
            "concept_code": concept_code,
            "concept_name": concept_name,
            "open": row.get("open"),
            "close": row.get("close"),
            "high": row.get("high"),
            "low": row.get("low"),
            "volume": row.get("volume"),
            "amount": row.get("amount"),
            "pct_change": row.get("pct_change"),
            "turnover": row.get("turnover"),
            "up_count": None,
            "down_count": None,
            "data_source": HISTORY_SOURCE,
        })
    return records


def _fetch_board_list() -> list[tuple[str, str]] | None:
    """概念板块列表，返回 ``(code, name)``。

    **委托**给 ``tasks.concept_board._fetch_concept_list_em``，不自己再实现一遍：
    那个函数已经解决了本机反复踩到的两件事——①东财 WAF 会间歇性掐掉
    ``push2.eastmoney.com``（``tasks/concept_board.py:225`` 记着这条），它按短页翻完
    ``push2`` → ``push2delay``；②它用 ``fs=m:90+t:3``，与实时快照同一个板块全集。
    本模块若自己调 ``stock_board_concept_name_em``，拿到的是单主机 + 另一套筛选，
    回补的板块范围会和快照写入的那批对不上。
    跨模块 import 一个下划线名是本仓库既有做法（``tasks/utility.py:44`` 从
    ``tasks.bars`` import ``_detect_suspended_symbols`` 等）：下划线只表示「模块私有」，
    不表示「模块禁入」；两者同属 ``tasks/``、依赖方向单一、无环。

    Returns:
        ``None`` = 源端不可用（网络/熔断/协议错）；``[]`` = 源端正常但确实没有板块。
        两者必须能分开：前者是**故障**（该日回补整体作废、值得重试），后者是**事实**
        （无事可做）。旧实现两者都返回 ``[]``，调用方分不出「东财挂了」和「没板块」。
    """
    resp = get_default_client().call("eastmoney", _fetch_concept_list_em)
    if not resp.success:
        logger.warning(f"⚠️ 概念板块列表获取失败: {resp.metadata.error}")
        return None
    items: list[dict] = resp.data
    if not items:
        return []
    return [(item["concept_code"], item["concept_name"]) for item in items]


def fetch_day_records(
    day: str, *, boards: list[tuple[str, str]] | None = None
) -> list[dict]:
    """取某个交易日的全部概念板块记录。单个板块失败即跳过，不中断其余。

    第二道日期闸：只留 ``trade_date == day`` 的行。``find_missing_days`` 的
    ``day < expected`` 只约束**请求哪些天**，管不到**回来的是哪几天**——源端若无视
    ``start_date``/``end_date`` 把整段历史（含 expected 当天）吐回来，Task 3 就会
    照写，而 ``INSERT OR REPLACE`` 会用历史行的 NULL ``up_count``/``down_count``
    盖掉快照那天的真值：模块 docstring 硬约束 1 要防的正是这个。
    """
    if ak is None:
        logger.error("❌ akshare 未安装")
        return []
    if boards is None:
        boards = _fetch_board_list()
    # ``not boards`` 一次覆盖 ``None``（源端不可用）与 ``[]``（源端正常但确实没有
    # 板块）——本函数对两者**一律**返回空，没有第三种答案可给。「源挂了」还是
    # 「没板块」的分界归主任务：它自己调 ``_fetch_board_list()``、在 ``None`` 时
    # 判 ``retained`` + ``error_kind=network``，之后总是显式传 ``boards=``（Task 3
    # 的 Interfaces 约定）。这里再判一次只会多打一行无人断言的告警。
    if not boards:
        return []
    compact = day.replace("-", "")
    records: list[dict] = []
    for code, name in boards:
        resp = get_default_client().call(
            "eastmoney",
            lambda c=code: ak.stock_board_concept_hist_em(
                symbol=c, period="daily",
                start_date=compact, end_date=compact, adjust="",
            ),
        )
        if not resp.success:
            logger.warning(f"⚠️ 概念 {name}({code}) 历史获取失败: {resp.metadata.error}")
            continue
        # 映射这一步也必须按板块兜住，而不只是「请求失败」才兜。``SourceClient.call``
        # 对任何非 HTTP 返回都判 ``success=True``（core/source_client.py:267），于是既
        # 不是 DataFrame 也不抛异常的载荷（东财偶尔吐 JSON 错误信封）会在这里触发
        # ``df.empty`` 的 AttributeError——而它抛在 ``client.call`` 的 try **之外**，
        # 一路冒出本函数，带走已累积的记录**和**剩余所有待补的日子，直接违反硬约束 2。
        # 四个类型正是对一个未类型化载荷做 df.empty / rename / 列成员判断 / iterrows
        # 会抛的（收窄而非 Exception，同 P2-11 的纪律：不吞 MemoryError 与中断）。
        try:
            board_records = _records_from_hist_df(
                resp.data, concept_code=code, concept_name=name
            )
        except (AttributeError, KeyError, TypeError, ValueError) as exc:
            logger.warning(
                f"⚠️ 概念 {name}({code}) 历史载荷无法映射"
                f"（{type(exc).__name__}: {exc}），跳过该板块"
            )
            continue
        kept = [r for r in board_records if r["trade_date"] == day]
        if len(kept) != len(board_records):
            logger.warning(
                f"⚠️ 概念 {name}({code}) 历史混入 {len(board_records) - len(kept)} "
                f"条非 {day} 的行（源端可能无视了 start/end），已丢弃"
            )
        records.extend(kept)
    return records


# ===========================================================================
# 主任务
# ===========================================================================


# 取不到库内已有日期时的**真实代价**：不是「白跑一遍」，而是数据损坏。``set()``
# 会让 ``find_missing_days`` 把窗口内**已经有行**的日子也列进待补，而回补行不带
# ``up_count``/``down_count``（历史接口没这两列），``INSERT OR REPLACE`` 落在
# ``UNIQUE(trade_date, concept_code)`` 上就是把快照写的真值换成 NULL。
_STORED_DATES_GAP = (
    "缺口计算会把窗口内**已有数据**的日子也当成缺失，回补将用 NULL 覆盖"
    "它们由快照写入的涨跌家数并翻转 data_source（数据损坏，不是白跑一遍）"
)


def _stored_dates(db: DatabaseInterface) -> set[str]:
    """库内 ``concept_board`` 已有的 trade_date。取不到时返回空集合。"""
    path = getattr(db, "db_path", None)
    if not path:
        return set()
    try:
        # ``as_uri()`` 而不是 f-string 拼 ``file:`` URI：后者遇到路径里的 ``?`` 或
        # ``#`` 会被 sqlite3 当成 query/fragment 而抛错（生产路径两者都不含，属潜伏
        # 问题），落进 except 就会变成上面那段数据损坏。
        conn = sqlite3.connect(
            f"{Path(str(path)).resolve().as_uri()}?mode=ro", uri=True, timeout=5.0
        )
    except (sqlite3.Error, ValueError, OSError) as exc:
        logger.warning(f"⚠️ 读取 concept_board 已有日期失败（{exc}）→ {_STORED_DATES_GAP}")
        return set()
    try:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT trade_date FROM concept_board")
        return {str(r[0])[:10] for r in cur.fetchall() if r[0]}
    except sqlite3.Error as exc:
        logger.warning(f"⚠️ 读取 concept_board 已有日期失败（{exc}）→ {_STORED_DATES_GAP}")
        return set()
    finally:
        conn.close()


def _target_days(target_date: str, *, expected: str, stored: set[str]) -> list[str]:
    """``target_date`` 能不能补？命中任一否决条件就返回空列表。

    1. ``>= expected``：模块硬约束 1——``expected`` 归快照任务所有，而历史行没有
       涨跌家数，写上去就是用 NULL 覆盖真值。
    2. **已在库内**：那天已经有行。``INSERT OR REPLACE`` 配
       ``UNIQUE(trade_date, concept_code)`` 会把快照写的 ``up_count``/``down_count``
       换成历史行的 NULL，``data_source`` 从 'em' 翻成 'em_hist'——**以「补全」的名义
       毁掉真值**。窗口路径靠 ``find_missing_days`` 的 present 过滤天然避开这一条，
       而 ``target_date`` 是**绕过**它的唯一入口。

    旧实现只在 ``target_date`` 分支比 ``expected``，读出来的 ``stored`` 被直接丢掉，
    于是「补一个已经在库里的日子」是可达的——而 Task 4 的 CLI 就会把它暴露出去。
    """
    if target_date >= expected:
        logger.warning(
            f"⚠️ 指定日期 {target_date} 不早于 expected({expected})，拒绝回补"
            "（该日归快照任务所有，历史行会把涨跌家数覆盖成 NULL）"
        )
        return []
    if target_date in stored:
        logger.warning(
            f"⚠️ 指定日期 {target_date} 库内已有数据，拒绝回补"
            "（INSERT OR REPLACE 会用历史行的 NULL 抹掉快照写的涨跌家数）"
        )
        return []
    return [target_date]


def update_concept_board_backfill(
    db: DatabaseInterface,
    *,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    target_date: str | None = None,
    _task_run_id: str | None = None,
) -> dict[str, Any]:
    """回补 ``concept_board`` 已丢失的交易日。手动任务，不进日常管道。

    状态语义（按「有没有补上任何一天」分，不按「失败几天」分）：

    * 无缺口 → ``success``, saved=0，零源端请求
    * 板块列表源端不可用（``None``）→ ``retained`` + ``error_kind=network``，
      让 ``core/runner.py`` 的 ``safe_task`` 在 30s 后重试
    * 板块列表**合法为空**（``[]``）→ ``no_data``：源正常、无事可做，重试无意义
    * 全部补上 → ``success``
    * 补上至少一天 → ``degraded``，未补足的日子列入 ``metadata["failed_days"]``
    * **一天都没补上** → ``retained`` + ``error_kind=network``（同第一条：源整体不可用）

    三个日子清单放在 ``result["metadata"]`` 而非顶层：``safe_task`` 的契约是
    ``metadata``，那才是真正写进 ``ingestion_runs.metadata_json`` 的东西。放顶层则
    任何裁定都留不下——``normalize_task_result`` 的 degraded 路径只取 ``error``
    （拿到的是 ``reason`` 那个串，failed_days 的名字全丢）、retained 只取 ``error``，
    success 与 no_data 干脆没有 metadata 形参。

    Args:
        db: 数据库接口。
        lookback_days: 窗口大小，单位**交易日**。
        target_date: 指定只补这一天；None 时按窗口算缺口。``>= expected`` 或库内
            已有的日期会被拒绝（见 ``_target_days``），不写入。
        _task_run_id: 由 ``safe_task`` 注入。
    """
    logger.info("\n" + "=" * 60)
    logger.info("🩹 任务: 概念板块缺口回补 (窗口 %d 个交易日)", lookback_days)
    logger.info("=" * 60)

    expected = get_expected_latest_trading_day()
    stored = _stored_dates(db)
    if target_date is not None:
        days = _target_days(target_date, expected=expected, stored=stored)
    else:
        days = find_missing_days(stored, expected=expected, lookback_days=lookback_days)

    # ``meta`` **就是** ``result["metadata"]``（同一个对象），不是副本：只留一份
    # 真相，免得顶层与 metadata 两处各记一遍日后必然分叉。
    meta: dict[str, Any] = {
        "requested_days": len(days),
        "backfilled_days": [],
        "failed_days": [],
    }
    result: dict[str, Any] = {"saved": 0, "metadata": meta}
    if not days:
        logger.info("✅ 概念板块无缺口，无需回补")
        result["status"] = "success"
        return result

    logger.info(f"📋 待补 {len(days)} 个交易日: {', '.join(days)}")
    boards = _fetch_board_list()
    if boards is None:
        # 源端不可用 = **故障**，值得 safe_task 30s 后重试。
        result["status"] = "retained"
        result["error_kind"] = "network"
        result["retained_old_data"] = True
        result["error"] = "概念板块列表不可用（源端故障），未回补任何一天"
        return result
    if not boards:
        # 源端正常但确实没有板块 = **事实**，不是故障。报 retained + error_kind=network
        # 会让 safe_task 白等一轮 30s 重试，而重试多少次结果都一样。
        logger.warning("⚠️ 概念板块列表为空，源正常但无可回补板块")
        result["status"] = "no_data"
        result["reason"] = "概念板块列表为空，无可回补板块"
        return result

    # 下界由**本次**的板块数推出，不钉死任何数字。
    min_boards = max(1, math.ceil(MIN_BOARD_COVERAGE * len(boards)))
    for day in days:
        records = fetch_day_records(day, boards=boards)
        # 上界闸门：源端若返回 expected 当天或更晚的行，丢弃——快照已写 expected 且带
        # 涨跌家数，INSERT OR REPLACE 会用 NULL 覆盖它。``isinstance(..., str)`` 一并
        # 挡掉缺 ``trade_date`` 键的记录：该列是 NOT NULL，落个 None 会真的进库并
        # 污染 ``find_missing_days`` 的 have；直接用下标则 KeyError 冒出整个任务。
        records = [
            r for r in records
            if isinstance(r.get("trade_date"), str) and r["trade_date"] < expected
        ]
        if not records:
            meta["failed_days"].append(day)
            logger.warning(f"⚠️ 概念板块 {day} 回补 0 行，保留旧数据")
            continue
        # 覆盖闸门：数**不同板块**而不是记录条数（同一板块的重复行不该把覆盖率灌水）。
        # 低于下界说明这天的源是残的——多半是熔断在循环中途打开了
        # （core/source_client.py 每主机 5 次失败即冷却 120s），剩下几百个板块一个
        # 请求都不发。**不写入**：写进去 ``find_missing_days`` 就认为那天齐了（它只看
        # 有没有行），而 ``target_date`` 又会以「库内已有」拒掉重跑——那天从此永久残缺
        # 且无人能修。留在缺口集里，下一轮自动重试才是真正的自愈。
        covered = len({r["concept_code"] for r in records})
        if covered < min_boards:
            meta["failed_days"].append(day)
            logger.warning(
                f"⚠️ 概念板块 {day} 只覆盖 {covered}/{len(boards)} 个板块"
                f"（下界 {min_boards}），判定为残缺：不写入、保留缺口待下轮重试"
            )
            continue
        saved = db.save_concept_board_batch(records)
        if saved == 0:
            # 「抓到」不等于「写入」（AGENTS.md 硬规则 4）。``save_concept_board_batch``
            # 吞掉一切异常并返回 0（providers.py:1980），而库被占/被锁正是
            # core/runner.py:113 记着的真实故障。把 0 当成功会让 ingestion_runs 记下
            # saved_rows=0 而表里一行都没有，且那天仍被记成已回补。
            meta["failed_days"].append(day)
            logger.warning(
                f"⚠️ 概念板块 {day} 抓到 {len(records)} 条但写入 0 行，判为写入失败"
                "（数据库可能被占），保留缺口待下轮重试"
            )
            continue
        # 累加：``saved`` 是这一轮写进去的**总**行数。旧的 `= saved` 让两天各 504 条
        # 的一轮在 ingestion_runs 里记成 504，少报 9 倍。
        result["saved"] += saved
        meta["backfilled_days"].append(day)
        logger.info(f"✅ 概念板块 {day} 回补完成: {saved} 条")

    if not meta["backfilled_days"]:
        # 什么都没落库 = 整体不可用。重试对「熔断/限速」和「库被占」两种成因都对：
        # 后者 30s 后锁通常也松了。retained（而非 failed）保住了旧数据且不以非 0
        # 退出，连续多日 retained 由 core/retained_streak.py 负责报。
        result["status"] = "retained"
        result["error_kind"] = "network"
        result["retained_old_data"] = True
        result["error"] = f"源不可用，{len(days)} 个交易日全部未补上"
        logger.warning(f"⚠️ 概念板块回补未补上任何一天，保留旧数据: {days}")
    elif meta["failed_days"]:
        result["status"] = "degraded"
        result["reason"] = f"partially backfilled; failed days: {', '.join(meta['failed_days'])}"
        result["retained_old_data"] = True
    else:
        result["status"] = "success"
    return result
