"""Read-only Xueqiu cross-source verifier for close-refresh sampling checks.

实现 core/refresh.py 的 CrossSourceVerifier 协议：primary_quotes 只读本库
daily_bars 目标日分区，reference_quotes 只读雪球 qfq 日线；两侧均绝不写入
任何数据（备用源永不作为 A 股收盘数据的权威来源）。
"""

from __future__ import annotations

import sqlite3
import time
from collections.abc import Callable, Mapping, Sequence
from typing import Any

import pandas as pd

# 口径对齐（3 处，均以本库 daily_bars 为基准）：
# 1. 复权：daily_bars 为前复权（qfq），雪球请求 adjust="qfq"（映射为
#    type=before），close 口径一致。
# 2. 成交量单位：daily_bars.volume 来自 AkShare 东财 stock_zh_a_hist 的
#    成交量，单位为手（1 手 = 100 股）；雪球 kline 的 volume 单位为股。
#    因此参考侧必须除以 100 归一为手。该换算是基于两侧文档/实测的假设，
#    report-only 模式（先观察不降级）存在的目的之一正是用真实数据验证
#    并校准这一假设。
# 3. 日期：雪球时间戳已在 xq 层按 Asia/Shanghai 归一为日期，这里再按
#    目标日精确取行，绝不拿邻近交易日充数。
_SHARES_PER_LOT = 100.0

# 每两次雪球调用之间的默认限速：样本约 30 只，总耗时约 10s，避免风控。
_DEFAULT_THROTTLE_SECONDS = 0.3


class XueqiuCrossSourceVerifier:
    """以雪球为参考源的只读跨源校验器（不支持北交所）。"""

    source_name = "xueqiu"
    # 板块名必须取自 core/refresh_audit.py 的 CROSS_SOURCE_BOARDS；
    # 雪球不提供北交所行情，beijing 显式排除（采样框架据此剔除并上报）。
    supported_boards = frozenset({"shanghai", "shenzhen", "chinext", "star"})

    def __init__(
        self,
        db_path: str,
        *,
        xq_module: Any | None = None,
        throttle_seconds: float = _DEFAULT_THROTTLE_SECONDS,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if throttle_seconds < 0:
            raise ValueError("throttle_seconds must be nonnegative")
        self._db_path = db_path
        self._xq = xq_module
        self._throttle_seconds = throttle_seconds
        self._sleep = sleep

    def primary_quotes(
        self,
        symbols: Sequence[str],
        target_date: str,
    ) -> Mapping[str, Mapping[str, Any]]:
        """读取本次刷新已发布的目标日 close/volume；无行的股票不发键。"""
        if not symbols:
            return {}
        placeholders = ",".join("?" for _ in symbols)
        # 与 core/refresh_adapters._target_partition_symbols 相同的只读查询模式
        conn = sqlite3.connect(self._db_path)
        try:
            fetched = conn.execute(
                "SELECT ts_code, close, volume FROM daily_bars"
                f" WHERE trade_date = ? AND ts_code IN ({placeholders})",
                (target_date, *symbols),
            ).fetchall()
        finally:
            conn.close()
        return {
            str(ts_code): {"close": close, "volume": volume}
            for ts_code, close, volume in fetched
        }

    def reference_quotes(
        self,
        symbols: Sequence[str],
        target_date: str,
    ) -> Mapping[str, Mapping[str, Any]]:
        """逐只读取雪球 qfq 日线的目标日 close/volume（volume 归一为手）。

        空 DataFrame（停牌/无数据）或缺目标日行的股票直接跳过不发键；
        缺键在下游比对中如何计入由 cross_source_mismatches 统一裁决。
        """
        xq = self._xq_module()
        target = pd.to_datetime(target_date).normalize()
        quotes: dict[str, dict[str, Any]] = {}
        for index, symbol in enumerate(symbols):
            if index and self._throttle_seconds > 0:
                self._sleep(self._throttle_seconds)
            # 本库代码形如 000001.SZ，雪球层接受裸 6 位码
            bare = symbol.split(".", 1)[0]
            df = xq.get_daily_bars(
                bare,
                start_date=target_date,
                end_date=target_date,
                adjust="qfq",
            )
            if df is None or df.empty:
                continue
            rows = df[pd.to_datetime(df["date"]).dt.normalize() == target]
            if rows.empty:
                continue
            row = rows.iloc[-1]
            quotes[symbol] = {
                "close": float(row["close"]),
                # 雪球 volume 单位为股，本库为手：/100 归一（见模块头注释）
                "volume": float(row["volume"]) / _SHARES_PER_LOT,
            }
        return quotes

    def _xq_module(self) -> Any:
        # 延迟导入真实雪球模块：测试注入 fake，绝不触发网络会话预热
        if self._xq is None:
            from smartmoney_hunter import xueqiu as xq

            self._xq = xq
        return self._xq
