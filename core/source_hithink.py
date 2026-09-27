"""同花顺 HiThink Financial-API 数据源（公测期）。

定位：A 股日线降级源。quant_hunter DataLoader 链（东财 → 新浪 → 腾讯 → 雪球）
全部失败后，由 ``providers.SmartMoneyLoaderProvider`` 调用本模块兜底，
写库 ``data_source='hithink'``。

契约要点（2026-08-26 实测，详见
``docs/2026-08-26-hithink-integration-feasibility-plan.md``）：

- HTTP 恒 200，业务错误看信封 ``code``：0=成功，4001=QPS 超限，2003=权限不足
- ``prices/historical`` 的 ``start``/``end`` 为**毫秒时间戳**；``thscode`` 形如 ``600519.SH``
- 北交所仅 920 前缀受支持（430/830 等老代码报 ``code=1002 Unknown thscode``）
- 复权口径：``adjust=forward`` 与 akshare qfq 最新段一致（10 票 × 10 日实测）；
  除权事件窗口内历史段可能差 ~1%（实测 601899，2026-08），
  因此本源只用于兜底增量数据，不做历史覆写
"""

from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any

import pandas as pd
import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://fuyao.aicubes.cn"
ENV_API_KEY = "HITHINK_FINANCE_API_KEY"

_CODE_OK = 0
_CODE_RATE_LIMIT = 4001
_CODE_FORBIDDEN = 2003

_REQUEST_TIMEOUT = 15

# 涨跌停池分页大小：单日涨停可过百（实测 2026-06-01 有 164 只），而服务端
# 默认 size=50，故显式放大并按 pagination.total 循环取全。
_LIMIT_POOL_PAGE_SIZE = 200


class HithinkError(RuntimeError):
    """hithink 业务错误（信封 code != 0 且非限流），不可重试。"""


def to_thscode(symbol: str) -> str | None:
    """6 位代码 → thscode（``600519`` → ``600519.SH``）。

    北交所仅 920 前缀实测受支持；43/83/87 等老 BJ 代码及带市场前缀的
    指数代码（``sh000300``）返回 None，调用方应跳过。
    """
    code = symbol.strip()
    if not (len(code) == 6 and code.isdigit()):
        return None
    if code.startswith("920"):
        return f"{code}.BJ"  # 须先于 9→SH 判断（920 是北交所新号段）
    if code.startswith(("6", "9")):
        return f"{code}.SH"
    if code.startswith(("0", "2", "3")):
        return f"{code}.SZ"
    return None


def _to_ms(date_str: str | None, default: str) -> int:
    """``YYYYMMDD``/``YYYY-MM-DD`` → 毫秒时间戳（本地时区零点）。"""
    value = (date_str or default).replace("-", "")
    return int(time.mktime(time.strptime(value, "%Y%m%d")) * 1000)


class HithinkClient:
    """同花顺 Financial-API 客户端（公测期）。

    认证：``X-api-key`` 请求头，Key 从环境变量 ``HITHINK_FINANCE_API_KEY``
    读取（``.env`` 由 ``core.config`` 在 import 时加载），禁止硬编码。
    """

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str = BASE_URL,
        session: requests.Session | None = None,
    ) -> None:
        self._api_key = api_key if api_key is not None else os.getenv(ENV_API_KEY, "")
        self._base_url = base_url.rstrip("/")
        self._session = session or requests.Session()
        # 国内端点，绕过系统代理（同 akshare_common 的 no_proxy 处理）
        self._session.trust_env = False
        # 2003（权限收缩）后本次进程内停用，避免 5500 票逐只空转
        self._disabled = False
        self._lock = threading.Lock()

    @property
    def available(self) -> bool:
        """Key 已配置且未因权限错误停用。"""
        return bool(self._api_key) and not self._disabled

    def _get(self, path: str, **params: Any) -> dict:
        if not self._api_key:
            raise HithinkError(f"{ENV_API_KEY} 未配置，hithink 源不可用")
        if self._disabled:
            raise HithinkError("hithink 已因权限错误(code=2003)停用")

        resp = self._session.get(
            f"{self._base_url}{path}",
            params=params,
            headers={"X-api-key": self._api_key},
            timeout=_REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        envelope = resp.json()
        code = envelope.get("code")
        if code == _CODE_OK:
            return envelope.get("data") or {}

        message = envelope.get("message", "")
        if code == _CODE_RATE_LIMIT:
            # 转成 SourceClient 可识别的可重试错误，走既有指数退避
            raise RuntimeError("HTTP 429")
        if code == _CODE_FORBIDDEN:
            with self._lock:
                self._disabled = True
            logger.error("🔴 hithink 权限不足(code=2003)，本次运行停用该源: %s", message)
        raise HithinkError(f"hithink code={code}: {message}")

    def fetch_daily_bars(
        self,
        symbol: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        """获取前复权日线，返回与 DataLoader 一致的列规范。

        列：date, open, high, low, close, volume, amount, pct_change,
        amplitude, data_source='hithink'。
        hithink 不提供换手率（turnover_rate 留空，由后续修复任务补齐）。

        Args:
            symbol: 6 位股票代码（如 ``600519``）
            start_date / end_date: ``YYYYMMDD`` 或 ``YYYY-MM-DD``，None 表示不限制
        """
        thscode = to_thscode(symbol)
        if thscode is None:
            logger.debug("hithink 跳过不支持的代码: %s", symbol)
            return pd.DataFrame()

        data = self._get(
            "/api/a-share/prices/historical",
            thscode=thscode,
            interval="1d",
            adjust="forward",
            start=_to_ms(start_date, "19900101"),
            end=_to_ms(end_date, time.strftime("%Y%m%d")),
        )
        items = data.get("item") or []
        if not items:
            return pd.DataFrame()

        df = pd.DataFrame(items)
        # date_ms 为 Asia/Shanghai 零点毫秒戳：先按 UTC 解析再转上海时区，
        # 避免运行机器时区不同导致日期偏移一天
        df["date"] = (
            pd.to_datetime(df["date_ms"], unit="ms", utc=True)
            .dt.tz_convert("Asia/Shanghai")
            .dt.tz_localize(None)
        )
        df = df.rename(columns={
            "open_price": "open",
            "high_price": "high",
            "low_price": "low",
            "close_price": "close",
            "turnover": "amount",  # hithink 的 turnover 字段是成交额（实测对齐 akshare 成交额）
        })
        prev_close = df["close"].shift(1)
        df["pct_change"] = (df["close"] / prev_close - 1) * 100
        df["amplitude"] = (df["high"] - df["low"]) / prev_close * 100
        df["data_source"] = "hithink"

        keep = ["date", "open", "high", "low", "close", "volume",
                "amount", "pct_change", "amplitude", "data_source"]
        return df[keep].sort_values("date").reset_index(drop=True)

    # ── 涨跌停池（special-data） ───────────────────────────────────────

    def _fetch_limit_pool(
        self, endpoint: str, trade_date: str, limit_type: str,
    ) -> list[dict]:
        """取单个池（涨停/跌停）某交易日的全部记录，按 pagination 分页拉全。

        参数名是 **``date_ms``（毫秒时间戳）**——传 ``trade_date`` 会被服务端
        静默忽略并返回 0 行（2026-09-27 实测），是最容易踩的坑。
        """
        date_ms = _to_ms(trade_date, trade_date)
        rows: list[dict] = []
        page = 1
        while True:
            data = self._get(
                f"/api/a-share/special-data/{endpoint}",
                date_ms=date_ms,
                page=page,
                size=_LIMIT_POOL_PAGE_SIZE,
            )
            items = data.get("item") or []
            rows.extend(items)
            total = (data.get("pagination") or {}).get("total") or 0
            # 取满、空页或服务端未给总数时停；总数缺失时靠空页兜底
            if not items or len(rows) >= total:
                break
            page += 1
        return [self._limit_row(item, trade_date, limit_type) for item in rows]

    @staticmethod
    def _limit_row(item: dict, trade_date: str, limit_type: str) -> dict:
        """池记录 → ``limit_up_down`` 行规范（与 ``tasks/macro.py`` 一致）。

        同花顺不提供 ``industry``，涨停池也无换手率，均置 None（东财口径才有）。
        """
        return {
            "trade_date": trade_date,
            "ts_code": str(item.get("ticker") or "").strip(),
            "name": str(item.get("name") or "").strip(),
            "pct_change": item.get("price_change_ratio_pct"),
            "close_price": item.get("last_price"),
            "turnover_rate": item.get("turnover_ratio_pct"),
            "limit_type": limit_type,
            "board_count": item.get("continue_day_cnt"),
            "industry": None,
            "data_source": "hithink",
        }

    def fetch_limit_pools(self, trade_date: str) -> tuple[list[dict], list[dict]]:
        """按交易日取涨停/跌停池，返回 ``(limit_up_rows, limit_down_rows)``。

        行字段对齐 ``limit_up_down`` 表，``data_source='hithink'``。实测保留约
        近几个月（2026-09-27 时点：2026-06-01 有数、2026-01-02 为空），足以覆盖
        东财涨跌停池约 16 个交易日的滚动窗口之外的历史日。
        """
        return (
            self._fetch_limit_pool("limit-up-pool", trade_date, "涨停"),
            self._fetch_limit_pool("limit-down-pool", trade_date, "跌停"),
        )


_DEFAULT_HITHINK_CLIENT: HithinkClient | None = None
_client_lock = threading.Lock()


def get_hithink_client() -> HithinkClient:
    """返回进程级共享的 HithinkClient 单例（保留 2003 停用状态）。"""
    global _DEFAULT_HITHINK_CLIENT
    with _client_lock:
        if _DEFAULT_HITHINK_CLIENT is None:
            _DEFAULT_HITHINK_CLIENT = HithinkClient()
    return _DEFAULT_HITHINK_CLIENT


def reset_hithink_client() -> None:
    """重置单例（测试用）。"""
    global _DEFAULT_HITHINK_CLIENT
    with _client_lock:
        _DEFAULT_HITHINK_CLIENT = None
