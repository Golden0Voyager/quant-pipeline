"""
统一重试工具
────────────
为 AkShare / 东财等易受网络波动影响的抓取函数提供指数退避重试装饰器。
替代各任务文件中散落的 for-attempt 手动循环。

用法：
    @retry_on_network(label="fetch_symbol", max_attempts=3, base_delay=6.0)
    def _fetch_symbol(symbol: str) -> pd.DataFrame | None:
        ...

装饰器仅包装网络抓取阶段；调用方的校验、空数据判断、日志仍由调用方控制。

有意不使用本装饰器的任务（设计决策，勿"顺手统一"）：
────────────────────────────────────────────────────
- bars.py `update_bars` (≈L1023)：整周期重试 —— try 块含 DB 读写、
  标记文件写入与 skipped/failed/success 早返回。装饰器只应包装网络
  抓取阶段，把 DB 写纳入重试会放大失败面；抽取抓取子函数的重构在
  5500 只/日的关键路径上回归风险过高。
- financials.py `_fetch_industry` (≈L262)：限流熔断 —— 需区分
  403/429/503（置 f10_rate_limited → 全局 _f10_blocked 熔断）与
  普通 404/解析错（不熔断）。装饰器对所有异常一视同仁、不向调用方
  暴露失败类别，无法表达该语义。
- global_assets.py `_fetch_with_retry` (≈L63)：空结果重试 —— 不抛
  异常，df 为空才重试（2 次、隔 10s）。装饰器只重试异常，空返回直接
  透传，语义相反。
（如确需迁移，应先为装饰器扩展 on_empty / 失败分类回调等能力，
 再逐个任务迁移并配套回归测试，不要一次性统一。）
"""

from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable
from functools import wraps
from typing import Any, TypeVar

from core.config import MAX_RETRY_VAL, RETRY_DELAY_VAL

logger = logging.getLogger(__name__)
F = TypeVar("F", bound=Callable[..., Any])


def retry_on_network(
    *,
    max_attempts: int = MAX_RETRY_VAL,
    base_delay: float = RETRY_DELAY_VAL,
    label: str = "",
) -> Callable[[F], F]:
    """返回装饰器：对抓取函数做指数退避重试，全部失败返回 None。

    - 任意异常均视为可重试（网络类为主）
    - 第 i 次失败后等待 base_delay * 2^i + jitter 秒
    - 成功返回真实值；全部失败返回 None 并打 warning 日志
    """

    def decorator(fn: F) -> F:
        @wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any | None:
            last_exc: Exception | None = None
            for attempt in range(max_attempts):
                try:
                    return fn(*args, **kwargs)
                except Exception as exc:  # noqa: BLE001
                    last_exc = exc
                    if attempt < max_attempts - 1:
                        sleep_time = base_delay * (2**attempt) + random.uniform(0, 1)
                        if label:
                            logger.warning(
                                f"⚠️ {label} 第 {attempt + 1} 次失败，"
                                f"{sleep_time:.1f}s 后重试: {exc}"
                            )
                        else:
                            logger.warning(
                                f"⚠️ {fn.__name__} 第 {attempt + 1} 次失败，"
                                f"{sleep_time:.1f}s 后重试: {exc}"
                            )
                        time.sleep(sleep_time)
            if label:
                logger.warning(f"⚠️ {label} 重试 {max_attempts} 次仍失败: {last_exc}")
            else:
                logger.warning(f"⚠️ {fn.__name__} 重试 {max_attempts} 次仍失败: {last_exc}")
            return None

        return wrapper  # type: ignore[return-value]

    return decorator
