"""
东财 push2his 逐票资金流客户端
──────────────────────────────
`https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get` 的唯一实现。

为什么单独成模块：逐票字段序是**整段字段映射的错位不会报错**的那类代码 ——
索引整体挪一格，小单净额就写进了主力列，而一切照常「成功」。回补与修复两个
脚本都必须用它，复制两份等于给这类静默错误开第二个入口。

实测校准（2026-10-13，secid=0.920002 万达轴承）：
    主力净额 = 大单 + 超大单        821331 = -674722 + 1496053
    主力占比 = 大单占比 + 超大单占比   2.05 = -1.68 + 3.73
两条恒等式同时成立才说明下标整体没错位；`tests/test_fund_flow_source.py` 把它钉住。

为什么可信：东财的榜单端点与本逐票端点对同一 (票, 日) **40/40 完全一致**，
两者内部恒等式也都成立 —— 同一数据源、同一口径、自洽。同城的同花顺「即时」
则与本端点 0/25 一致、且匹配不上任何一个交易日，故不在此列。
"""

from __future__ import annotations

import logging
from types import ModuleType
from typing import Any

from core.utils import infer_market

requests: ModuleType | None

try:
    import requests
except ImportError:
    requests = None

logger = logging.getLogger(__name__)

FFLOW_URL = "https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get"
FFLOW_PARAMS: dict[str, str] = {
    "klt": "101",
    "lmt": "30",
    "fields1": "f1,f2,f3,f7",
    "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63",
    "ut": "b2884a393a59ad64002292a3e90d46a5",
}

# 逐票字段序（实测 920002 校验：主力净额 = 大单 + 超大单，主力占比 = 两者占比之和）
#   0 f51 日期          1 f52 主力净额   2 f53 小单净额  3 f54 中单净额
#   4 f55 大单净额      5 f56 超大单净额 6 f57 主力占比  7 f58 小单占比
#   8 f59 中单占比      9 f60 大单占比  10 f61 超大单占比
NUMERIC_COLUMNS = (
    "main_net_inflow",
    "main_net_inflow_pct",
    "super_large_net_inflow",
    "super_large_net_inflow_pct",
    "large_net_inflow",
    "large_net_inflow_pct",
)
# 下标 → 字段名。下标 2/3/7/8（小单/中单）本地表无对应列，丢弃。
PART_INDEX: dict[str, int] = {
    "main_net_inflow": 1,
    "main_net_inflow_pct": 6,
    "super_large_net_inflow": 5,
    "super_large_net_inflow_pct": 10,
    "large_net_inflow": 4,
    "large_net_inflow_pct": 9,
}

# fund_flow 里由「主力净额」派生、本地表无对应列的小单/中单字段
DERIVED_COLUMNS = (
    "main_net_inflow_pct",
    "super_large_net_inflow",
    "super_large_net_inflow_pct",
    "large_net_inflow",
    "large_net_inflow_pct",
)


def secid_for(code: str) -> str:
    """东财 secid 前缀。沪市与科创板是 1.，其余（深/创/中小/北交所）是 0.。

    权威判定是 core.utils.infer_market：**920 必须先于 90 判北交所**——
    写成 `code.startswith(("6", "9")) → 1.` 会把 350 只 920 段北交所全判成
    沪市，实测 secid=1.920002 返回 `rc:100, data:null`，而 0.920002 正常。
    """
    return ("1." if infer_market(code) in ("sh", "star") else "0.") + code


def make_session() -> Any:
    """独立会话。``trust_env=False`` 是必需的：东财在环境代理下会拒连。"""
    if requests is None:
        raise RuntimeError("requests 未安装")
    session = requests.Session()
    session.trust_env = False
    return session


def parse_kline(raw: str) -> dict[str, Any] | None:
    """解析单根日 K → {date, 六个数值字段}；不可解析返回 None。

    接口只回名称不回代码，故不含代码，由调用方按请求的票填入。
    六个数值字段全为 None 视为无数据（源端给 '-' 之类）。
    """
    parts = raw.split(",")
    if len(parts) < 11 or not parts[0]:
        return None
    record: dict[str, Any] = {"date": parts[0]}
    for field, idx in PART_INDEX.items():
        try:
            record[field] = float(parts[idx])
        except (ValueError, IndexError):
            record[field] = None
    if all(record[f] is None for f in NUMERIC_COLUMNS):
        return None
    return record


def _same_code(requested: str, returned: str) -> bool:
    """比较请求代码与源站回显代码（去前导零后按字符串比）。"""
    return requested.strip().lstrip("0") == returned.strip().lstrip("0")


def fetch_day_klines(session: Any, code: str, lmt: int = 30) -> list[str]:
    """取该票最近 ``lmt`` 根日 K 的原始行；网络/解析失败返回空表（不抛）。

    单票失败不得中断整轮：批量作业里一只停牌票会让全表回补失败。

    **必须校验源站回显的 code**：非 6 位代码（可转债等）会让 secid 变成
    **前缀匹配**，源站返回的是另一只证券。实测 `secid=0.1318` 返回
    `code=131800, name=Ｒ-003` —— 拿不到校验就会把 131800 的资金流
    写到 1318 名下，且不报任何错。回显缺失同样拒绝：宁可响亮地取不到，
    也不要静默写入错证券的数据。
    """
    if requests is None:
        return []
    params = dict(FFLOW_PARAMS)
    params["lmt"] = str(lmt)
    try:
        resp = session.get(
            FFLOW_URL,
            params={"secid": secid_for(code), **params},
            timeout=(10, 20),
        )
        resp.raise_for_status()
        data = resp.json().get("data") or {}
        returned = str(data.get("code") or "")
        if not returned or not _same_code(code, returned):
            logger.debug(
                f"  {code}: 源站回显 code={returned or '(缺失)'} 与请求不符，丢弃"
            )
            return []
        klines = data.get("klines") or []
        return [k for k in klines if isinstance(k, str)]
    except Exception as e:  # noqa: BLE001
        logger.debug(f"  {code} 抓取失败: {e}")
        return []


def is_weekend(day: str) -> bool:
    """该日期是否周末。fund_flow 有 trg_fund_flow_reject_weekend 拒写周末。"""
    from datetime import date

    return date.fromisoformat(day).weekday() >= 5
