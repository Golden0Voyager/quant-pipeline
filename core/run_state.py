"""管道运行状态标记：把「这一轮没跑完」变成可编程信号。

背景
────
2026-09-17 的每日管道只跑了 31 个任务就结束了（含 ``update_fundamentals``
但没有 ``update_market_snapshot``），而且这 31 个任务**全部 success**，因此
没有任何告警会响。当日唯一线索是日志里缺少 ``🏁 数据管道全部完成`` 那一行
——纯日志信号，没人会去读。它留下的数据空洞至今仍在（见
``core.known_gaps``），并且因为 ``update_market_snapshot`` 以
``MAX(trade_date)`` 为作用域，那个日期**再也不会被回访**。

做法
────
在 ``task_runs``（单列 ``task_name`` 主键的「最后运行日期」表）里存一个合成
任务名::

    开始 → "in-progress:<date>"      正常结束 → "complete:<date>"

下一轮开始时若发现遗留的 ``in-progress`` 且日期不是今天，即证明上一轮被中断，
由调用方告警。``task_runs`` 只有 ``get_last_task_run`` 一个读取入口，写入合成
任务名不会影响既有语义。

为什么是「下一轮检测」而不是「同轮自检」
────────────────────────────────────────
中途被 kill 的进程没有机会自己报告任何东西；而同一轮内自检会与本模块的写入
时序冲突（巡检发生在任务链中段，此时标记必然是 in-progress）。把检测放在
下一轮开始，既避开了时序问题，又让发现时机落在一个「人还能补救」的时刻。
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - 仅用于类型标注
    from interface import DatabaseInterface

# 合成任务名：不会与任何真实任务重名（真实任务名均为 update_* / retry / health_check）。
RUN_STATE_TASK = "__pipeline_run_state__"

IN_PROGRESS_PREFIX = "in-progress:"
COMPLETE_PREFIX = "complete:"

_VALID_STATES = (IN_PROGRESS_PREFIX, COMPLETE_PREFIX)


def read_run_state(db: DatabaseInterface) -> tuple[str, str] | None:
    """读取运行状态标记。

    Returns
    -------
    tuple[str, str] | None
        ``("in-progress" | "complete", date)``；无标记或格式不可识别时返回
        ``None``（后者同时覆盖了旧库与测试夹具里没有该行的情形）。
    """
    raw = db.get_last_task_run(RUN_STATE_TASK)
    if not raw or not isinstance(raw, str):
        return None
    for prefix in _VALID_STATES:
        if raw.startswith(prefix):
            state = prefix.rstrip(":")
            date = raw[len(prefix) :]
            if date:
                return state, date
            return None
    return None


def mark_run_started(db: DatabaseInterface, date: str) -> str | None:
    """记录本轮开始，并返回上一轮遗留的未完成日期（无遗留时为 ``None``）。

    调用方应在返回值非 ``None`` 时告警：那意味着被中断的那一轮数据可能不完整。
    """
    previous = read_run_state(db)
    abandoned: str | None = None
    if previous is not None and previous[0] == "in-progress" and previous[1] != date:
        abandoned = previous[1]
    db.record_task_run(RUN_STATE_TASK, f"{IN_PROGRESS_PREFIX}{date}")
    return abandoned


def mark_run_completed(db: DatabaseInterface, date: str) -> None:
    """记录本轮正常结束。"""
    db.record_task_run(RUN_STATE_TASK, f"{COMPLETE_PREFIX}{date}")


def describe_run_state(db: DatabaseInterface) -> str:
    """给 health_check 用的一行人类可读描述。"""
    state = read_run_state(db)
    if state is None:
        return "无运行状态标记（首次运行或旧库）"
    kind, date = state
    label = "进行中（可能被中断）" if kind == "in-progress" else "已完整结束"
    return f"{label}：{date}"
