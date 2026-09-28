"""同一任务连续「保留旧数据」检测
────────────────────────────────
``retained``（保留旧数据）是本仓库最危险的一种状态：它让任务**以 0 退出**、只映射到
``warning``，而 ``NOTIFICATION_LEVEL`` 默认 ``error`` 恰好把它压掉——于是「这个任务
其实已经死了」在整轮里没有任何观察点，管线照样报「全部完成」。单次 ``retained`` 是
正常现象（源端抖动，``core.runner`` 内建的网络重试已经吸收过一次）；**连续多日**
``retained`` 才说明数据真的停在那儿了。

实测（生产库 ``~/Code/quant_data/quant_core.db``，2026-09-25 只读复核）：

* ``update_concept_board`` 连续 **5** 个运行日 retained（09-21 ~ 09-25，报
  ``HTTPError: HTTP Error 502: Bad Gateway``）：``concept_board`` 表停在 09-20，
  而同日 ``daily_bars`` 已到 09-24。这 5 天的运行**没有一次告警**。
* ``update_fund_flow`` 连续 **3** 个运行日 retained（09-21 ~ 09-23，``fund_flow``
  停在 09-18）后于 09-24 恢复。它正好落在阈值上，是这条巡检要覆盖的窗口；
  恢复之后不应继续报（见 ``tests/test_retained_streak.py`` 的「恢复即清零」用例）。

为什么是「连续 N 个**运行日**」而不是「连续 N 次运行」
────────────────────────────────────────────────────
一天之内同一任务可能被触发多次（重跑、TUI 单任务入口、分阶段重复执行），把行数当
「天数」会让一次手动重跑把计数直接灌到阈值；而取「当天最后一次**有结论**的运行」才
对应运维真正关心的问题——**这一天结束时，这个任务的数据到底更新了没有。** 所以这里
按运行日聚合、每天只留最后一次裁定，再数「最近的连续多少个运行日裁定都是 retained」。

两个刻意的取舍：

* **没有运行的日子跳过，不打断计数。** 本管线靠手动触发（无 launchd/cron，见
  ``AGENTS.md``），「某天没跑」是另一类问题（``core.day_coverage`` / ``core.run_state``），
  不该在这里被当成「恢复」。跨过空档继续计数是刻意的：09-21 retained、
  09-23 retained、09-25 retained 就是 3 个运行日的退化。
* **没有「已声明」豁免名单。** ``core/known_gaps.py`` 那种登记册针对的是**已核实不可
  回补**的历史空洞；连续 retained 是**正在发生、且可修复**的退化（源端挂了、契约漂移）。
  把它声明掉等于让它继续静默，与本模块的目的相反。所以这里是纯告警。

调用方
──────
``tasks/utility.py::health_check`` 把命中项追加进 ``issues``，于是本轮状态变
``degraded`` → ``daily_pipeline`` 收尾通知升级为 **error 级**（不再被
``NOTIFICATION_LEVEL=error`` 抑制），退出码也变非 0。
"""

from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from core.market_time import SHANGHAI

# 阈值取 3：单次 retained 正常（网络重试已吸收一次瞬断），2 次仍可能是连续两次源端
# 抖动；连续 3 个运行日说明这个任务已连续三天没产出任何数据。
RETAINED_STREAK_THRESHOLD = 3

# 连续计数只看最近这么多个自然日的审计记录：ingestion_runs 只增不减，全表扫描既慢又让
# 窗口外的历史残留（如已恢复的 old streak 尾巴）参与聚合。默认 30 显著大于阈值 3。
# 注意：环境变量必须在**调用时**读取（P2-15 的教训），不能做成导入期模块常量。
DEFAULT_WINDOW_DAYS = 30
_WINDOW_ENV_VAR = "QUANT_RETAINED_STREAK_WINDOW_DAYS"


@dataclass(frozen=True)
class RetainedStreak:
    """一个仍在持续中的「连续 retained」退化。

    Attributes
    ----------
    task_name:
        退化的任务名。
    days:
        连续运行日数（>= ``RETAINED_STREAK_THRESHOLD``）。
    first_day:
        这串连续运行日里最早的一天（``YYYY-MM-DD``，上海运行日）。
    last_day:
        最近一天。
    error_kind:
        最近一次 retained 的 ``error_kind``（可能是 ``None``）。
    """

    task_name: str
    days: int
    first_day: str
    last_day: str
    error_kind: str | None = None

    def describe(self) -> str:
        """一行可读描述，直接进 ``issues``（因此也是外发通知的内容来源）。"""
        detail = f"，最近 error_kind={self.error_kind}" if self.error_kind else ""
        return (
            f"连续保留旧数据: {self.task_name} 连续 {self.days} 个运行日 retained"
            f"（{self.first_day} ~ {self.last_day}{detail}）——"
            "数据已多日未更新，而 retained 只记 warning、被 NOTIFICATION_LEVEL=error 抑制"
        )


def _run_day(finished_at: object) -> str | None:
    """把 ``ingestion_runs.finished_at``（ISO UTC）折算成它所属的**上海运行日**。

    审计时间戳按 UTC 落库（``core.runner`` 用 ``datetime.now(UTC)``），而「哪一天跑的」
    必须按上海市场日算——否则 UTC 16:00 之后（= 上海次日 0 点后）的运行会被算到前一天，
    把连续计数割断。naive 时间戳按 UTC 解释（与 ``core.market_time.has_post_close_completion``
    同一约定）；解析失败返回 ``None``，由调用方跳过该行而不是猜。
    """
    if not isinstance(finished_at, str) or not finished_at:
        return None
    try:
        stamp = datetime.fromisoformat(finished_at)
    except ValueError:
        return None
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=UTC)
    return stamp.astimezone(SHANGHAI).strftime("%Y-%m-%d")


def _resolve_window_days(explicit: int | None) -> int:
    """窗口天数：显式参数优先，否则调用时读环境变量（非法值 fail-closed）。"""
    if explicit is not None:
        days = explicit
    else:
        raw = os.environ.get(_WINDOW_ENV_VAR)
        if raw is None:
            days = DEFAULT_WINDOW_DAYS
        else:
            try:
                days = int(raw)
            except ValueError:
                raise ValueError(
                    f"{_WINDOW_ENV_VAR} 必须是正整数，得到 {raw!r}"
                ) from None
    if days < 1:
        source = "window_days" if explicit is not None else _WINDOW_ENV_VAR
        raise ValueError(f"{source} 必须 >= 1，得到 {days}")
    return days


def _window_cutoff(window_days: int) -> str:
    """窗口下界（UTC ISO 字符串，秒精度）。

    ``finished_at`` 按 UTC 落库（``core.runner`` 用 ``datetime.now(UTC).isoformat``），
    同格式字符串按字典序即按时间序，SQL 里直接做字符串比较。
    """
    cutoff = datetime.now(UTC) - timedelta(days=window_days)
    return cutoff.strftime("%Y-%m-%dT%H:%M:%S+00:00")


def _latest_verdict_per_day(
    cursor: sqlite3.Cursor,
    window_days: int,
) -> dict[str, dict[str, tuple[str, str | None]]]:
    """``{task_name: {运行日: (status, error_kind)}}``，每天只留最后一次裁定。

    ``status='running'`` 的父行是 ``safe_task`` 执行前写的占位（正常结束时会被同
    ``run_id`` 覆盖）。残留的 running 行**不带结论**，若拿它当「当天最后状态」，一个
    崩溃日就会被误判成「非 retained」而打断计数。这里显式排除它，让每天的裁定来自
    最后一次**有结论**的运行。

    只取 ``finished_at`` 落在窗口内的行：``ingestion_runs`` 只增不减，全表扫描既慢
    又让窗口外早已无关的历史记录参与连续计数。
    """
    rows = cursor.execute(
        "SELECT task_name, status, finished_at, error_kind FROM ingestion_runs "
        "WHERE status != 'running' AND finished_at >= ? ORDER BY finished_at",
        (_window_cutoff(window_days),),
    ).fetchall()

    per_task: dict[str, dict[str, tuple[str, str | None]]] = {}
    for task_name, status, finished_at, error_kind in rows:
        if not task_name:
            continue
        day = _run_day(finished_at)
        if day is None:
            continue
        # 按 finished_at 升序遍历，后写覆盖先写 ⇒ 留下的即当天最后一次裁定。
        per_task.setdefault(str(task_name), {})[day] = (
            str(status),
            str(error_kind) if error_kind else None,
        )
    return per_task


def retained_streaks(
    cursor: sqlite3.Cursor,
    *,
    threshold: int = RETAINED_STREAK_THRESHOLD,
    window_days: int | None = None,
) -> list[RetainedStreak]:
    """返回**仍在持续中**的连续 retained 退化，按天数降序、任务名升序。

    只报当前仍在持续的：某个任务历史上连续 3 天 retained、之后恢复了，就不该继续告警
    ——否则一旦恢复，旧噪音仍会天天重报，真正的新退化会被淹没（2026-08 连续 8 次无人
    处理的告警正是这个机理）。因此从最近的运行日往回数，遇到第一个非 retained 的裁定
    即停止，不足 *threshold* 的一律不报。

    连续计数只看最近 *window_days* 个自然日的审计记录（默认
    ``DEFAULT_WINDOW_DAYS``，可用环境变量 ``QUANT_RETAINED_STREAK_WINDOW_DAYS``
    覆盖，**调用时读取**；显式传参优先于环境变量）。窗口只需显著大于 *threshold*：
    本检测只关心「最近的连续段」，更老的历史既不影响仍在持续的判定，也不该让
    ``ingestion_runs`` 的全表扫描越来越慢。

    ``ingestion_runs`` 表不存在时由 ``sqlite3`` 抛 ``OperationalError``，由调用方决定
    是跳过还是失败（``health_check`` 记一行「跳过」并在报告里说明）。
    """
    if threshold < 1:
        raise ValueError("threshold 必须 >= 1")
    days = _resolve_window_days(window_days)

    streaks: list[RetainedStreak] = []
    for task_name, by_day in _latest_verdict_per_day(cursor, days).items():
        # 日期字符串按字典序即按时间序；倒序 = 从最新的一天往回数。
        kept: list[tuple[str, str | None]] = []
        for day, (status, error_kind) in sorted(by_day.items(), reverse=True):
            if status != "retained":
                break
            kept.append((day, error_kind))
        if len(kept) < threshold:
            continue
        streaks.append(
            RetainedStreak(
                task_name=task_name,
                days=len(kept),
                first_day=kept[-1][0],
                last_day=kept[0][0],
                error_kind=kept[0][1],
            )
        )
    return sorted(streaks, key=lambda s: (-s.days, s.task_name))
