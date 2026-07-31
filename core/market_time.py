"""
统一上海市场时钟
────────────────
全管道对"今天/盘中/收盘"的判断必须使用同一个上海时区时钟，
禁止直接用本机 naive `datetime.now()` 做市场时间决策
（本机时区 ≠ +0800 时，盘中门禁与 expected 翻转会整体错位；
2026-07-30 事故：盘中写入的当日行被收盘后运行按"已存在"跳过）。

收盘线与 ``core.refresh._CLOSE_TIME`` 保持一致（16:00）。
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import UTC, datetime, time
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

SHANGHAI = ZoneInfo("Asia/Shanghai")

# 市场时段边界（上海时间）
_SESSION_START = time(9, 15)  # 集合竞价开始
_SESSION_END = time(15, 0)  # 收盘
_CLOSE_SETTLED = time(16, 0)  # 结算窗口结束，数据源定型（与 core.refresh 一致）

PHASE_PRE_OPEN = "pre_open"
PHASE_SESSION = "session"
PHASE_SETTLEMENT = "settlement"
PHASE_POST_CLOSE = "post_close"


def shanghai_now() -> datetime:
    """当前上海时间（timezone-aware）。"""
    return datetime.now(tz=SHANGHAI)


def shanghai_today() -> str:
    """上海时区的今天（YYYY-MM-DD）。"""
    return shanghai_now().strftime("%Y-%m-%d")


def _to_shanghai(now: datetime | None) -> datetime:
    if now is None:
        return shanghai_now()
    if now.tzinfo is None:
        # naive 输入按上海时间解释（测试注入的惯例）
        return now.replace(tzinfo=SHANGHAI)
    return now.astimezone(SHANGHAI)


def market_phase(now: datetime | None = None) -> str:
    """返回当前市场时段：pre_open / session / settlement / post_close。

    仅按时刻划分，不判断交易日——非交易日由调用方结合
    ``core.calendar.is_trading_day`` 判断。
    """
    t = _to_shanghai(now).time()
    if t < _SESSION_START:
        return PHASE_PRE_OPEN
    if t < _SESSION_END:
        return PHASE_SESSION
    if t < _CLOSE_SETTLED:
        return PHASE_SETTLEMENT
    return PHASE_POST_CLOSE


def is_fetch_window(now: datetime | None = None) -> bool:
    """交易日抓取任务是否允许执行（盘前或收盘定型后）。"""
    return market_phase(now) in (PHASE_PRE_OPEN, PHASE_POST_CLOSE)


def has_post_close_completion(
    db_path: str, task_name: str, target_date: str
) -> bool:
    """判断 *task_name* 是否在 *target_date* 收盘定型（16:00 上海）后成功运行过。

    数据依据是 ``ingestion_runs`` 审计表（safe_task 每次运行写入，
    ``finished_at`` 为 ISO UTC）。用途：把"今天的行已存在"这类跳过守卫
    收紧为"今天的行存在，且写它的运行发生在收盘之后"——盘中写入的
    实时快照不算完成，收盘后重跑会覆盖它（2026-07-30 事故防线）。

    任何查询/解析异常一律 fail-open 返回 False：宁可重抓，不可误跳过。
    """
    try:
        close_local = datetime.combine(
            datetime.strptime(target_date, "%Y-%m-%d").date(),
            _CLOSE_SETTLED,
            tzinfo=SHANGHAI,
        )
    except ValueError:
        return False
    close_utc = close_local.astimezone(UTC)

    try:
        conn = sqlite3.connect(db_path, timeout=5.0)
        try:
            rows = conn.execute(
                """
                SELECT finished_at FROM ingestion_runs
                WHERE task_name = ? AND status IN ('success', 'no_data')
                ORDER BY finished_at DESC LIMIT 50
                """,
                (task_name,),
            ).fetchall()
        finally:
            conn.close()
    except sqlite3.Error as exc:
        logger.debug("has_post_close_completion 查询失败（按未完成处理）: %s", exc)
        return False

    for (finished_at,) in rows:
        if not finished_at:
            continue
        try:
            finished = datetime.fromisoformat(str(finished_at))
        except ValueError:
            continue
        if finished.tzinfo is None:
            finished = finished.replace(tzinfo=UTC)
        if finished >= close_utc:
            return True
    return False
