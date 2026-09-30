# 概念板块缺口回补 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 新增手动任务 `update_concept_board_backfill`，用东财历史接口补齐 `concept_board` 已丢失的交易日，日常管道耗时零增长。

**Architecture:** 快照接口（`push2`）不可回补是病根。新任务走 `stock_board_concept_hist_em` 逐概念取历史，按「库内缺失 ∩ 最近 N 个交易日 ∩ `< expected`」计算缺口，逐日循环写入。**严格不写 `expected` 当天**——快照已写该日，且历史接口没有涨跌家数，`INSERT OR REPLACE` 会把快照的真值抹成 NULL。任务手动触发，不进 `run_all` 任何 stage。

**Tech Stack:** Python 3.12、akshare 1.18.64、SQLite（`providers.SmartMoneyDBProvider`）、pytest。

设计依据：`docs/superpowers/specs/2026-09-30-concept-board-backfill-design.md`（commit e9e0865）

## Global Constraints

- 包管理器只用 `uv`；运行脚本用 `uv run python <script>`；不新增依赖
- 数据库默认 `~/Code/quant_data/quant_core.db`；测试用 `QUANT_DB_PATH` 隔离
- **一文件一提交**，严禁把多文件打进同一个 commit
- commit message 中英双语，**英文块在前、中文在后**，conventional commits 格式
- 每项修复必须附**会因旧实现变红**的测试；commit message 写明「还原旧实现会让 X 测试全红」
- 四道门禁：`ruff check`、`uv run mypy`、`uv run pytest --cov`（含 `tests/`）、`coverage report`（`fail_under = 85`）
- 测试必须 hermetic：**不打真实网络**，fetch 层一律显式 patch
- 回补任务**不得**出现在 `daily_pipeline.run_all` 的任何 stage 列表、不得进 `CATCH_UP_TASK_ORDER`
- 写入日期**严格 `< expected`**（`expected = get_expected_latest_trading_day()`）
- `--lookback` 单位是**交易日**，不是自然日；默认 10

---

### Task 1: 缺口计算（纯函数，无网络）

**Files:**
- Create: `tasks/concept_board_backfill.py`
- Test: `tests/test_concept_board_backfill.py`

**Interfaces:**
- Consumes: `core.calendar.get_recent_trading_days(end_date: str, count: int) -> list[str]`（返回新到旧排列）；`core.known_gaps.declared_missing_days() -> frozenset[str]`
- Produces:
  ```python
  def find_missing_days(
      have: Iterable[str],
      *,
      expected: str,
      lookback_days: int = 10,
      declared: frozenset[str] | None = None,
  ) -> list[str]:
      """返回需要回补的交易日，升序。严格不含 expected 当天。"""
  ```

- [ ] **Step 1: 写失败测试**

创建 `tests/test_concept_board_backfill.py`：

```python
"""概念板块缺口回补：缺口计算的边界。

实测背景（2026-09-29）：`concept_board` 只由 `push2` 实时快照写入，接口不留
历史，线路一断该天就永久丢失。库内 31 个交易日 / 应有 50 个，日期跳跃即为
「那天没拍成」的痕迹。本模块负责算出该补哪些天。
"""

from __future__ import annotations

from tasks.concept_board_backfill import find_missing_days

# 真实日历：2026-09-29 是周二，09-28 周一，**09-25 是中秋节休市、不在日历里**
# （已对生产库只读核实：get_recent_trading_days("2026-09-29", 6) 不含它）。
# 夹具里混入 09-25 会让回补任务去补一个根本不存在的交易日——504 次白跑的请求。
_TRADING_DAYS = [
    "2026-09-29", "2026-09-28", "2026-09-24", "2026-09-23", "2026-09-22",
    "2026-09-21", "2026-09-18", "2026-09-17", "2026-09-16", "2026-09-15",
    "2026-09-14",
]

# lookback=10 / expected=09-29 时，去掉 expected 之后的窗口内容（9 天）
_ALL_IN_WINDOW = [
    "2026-09-28", "2026-09-24", "2026-09-23", "2026-09-22", "2026-09-21",
    "2026-09-18", "2026-09-17", "2026-09-16", "2026-09-15",
]

# 每个用例都**显式**传 declared，不走生产的 declared_missing_days()。
# 默认 None 会读 core/known_gaps.py 的真实内容，而 2026-09-16 就在本窗口内——
# 哪天那条登记被清掉（2026-09-27 的部分恢复已经把 4 张表补齐了），
# 这些断言就会因为一个跟它们无关的理由变红。生产登记册由 tests/test_known_gaps.py
# 单独钉住，不该顺带决定这里的期望值。
_NO_DECLARED: frozenset[str] = frozenset()


def _calendar(monkeypatch):
    """把交易日历钉死，使边界不依赖宿主机缓存。"""
    import tasks.concept_board_backfill as mod

    monkeypatch.setattr(
        mod, "get_recent_trading_days",
        lambda end, count: [d for d in _TRADING_DAYS if d <= end][:count],
    )


def test_no_gap_returns_empty(monkeypatch):
    _calendar(monkeypatch)
    assert find_missing_days(
        _ALL_IN_WINDOW, expected="2026-09-29", lookback_days=10, declared=_NO_DECLARED
    ) == []


def test_missing_days_are_returned_in_ascending_order(monkeypatch):
    _calendar(monkeypatch)
    # 库内实测形态：只有零星几天。窗口 9 天里已有 3 天 → 缺 6 天。
    have = ["2026-09-21", "2026-09-18", "2026-09-17"]
    got = find_missing_days(have, expected="2026-09-29", lookback_days=10, declared=_NO_DECLARED)
    assert got == [
        "2026-09-15", "2026-09-16", "2026-09-22", "2026-09-23", "2026-09-24", "2026-09-28",
    ]
    assert got == sorted(got)


def test_expected_day_is_never_included(monkeypatch):
    """回补绝不写 expected 当天——那道闸防的是 NULL 抹掉快照的涨跌家数。"""
    _calendar(monkeypatch)
    got = find_missing_days(
        [], expected="2026-09-29", lookback_days=10, declared=_NO_DECLARED
    )
    assert got, "窗口内应有缺口"
    assert "2026-09-29" not in got


def test_declared_missing_days_are_skipped(monkeypatch):
    """已登记的整日缺席是不可回补的，反复尝试只是浪费 504 次请求。"""
    _calendar(monkeypatch)
    got = find_missing_days(
        ["2026-09-24"], expected="2026-09-29", lookback_days=10,
        declared=frozenset({"2026-09-28", "2026-09-24"}),
    )
    assert "2026-09-28" not in got and "2026-09-24" not in got
    assert got == [
        "2026-09-15", "2026-09-16", "2026-09-17", "2026-09-18",
        "2026-09-21", "2026-09-22", "2026-09-23",
    ]


def test_lookback_window_is_truncated(monkeypatch):
    """窗口是成本上界：只看最近 lookback_days 个交易日。"""
    _calendar(monkeypatch)
    got = find_missing_days([], expected="2026-09-29", lookback_days=2, declared=_NO_DECLARED)
    assert got == ["2026-09-28"]


def test_holiday_is_never_treated_as_a_gap(monkeypatch):
    """09-25 是中秋节休市，钉住的日历里没有它，就绝不能被要求回补。"""
    _calendar(monkeypatch)
    assert find_missing_days(
        _ALL_IN_WINDOW, expected="2026-09-29", lookback_days=10, declared=_NO_DECLARED
    ) == []
    assert "2026-09-25" not in find_missing_days(
        [], expected="2026-09-29", lookback_days=10, declared=_NO_DECLARED)
    assert "2026-09-25" not in find_missing_days(
        [], expected="2026-09-29", lookback_days=11, declared=_NO_DECLARED)
```

- [ ] **Step 2: 跑测试确认失败**

```bash
uv run pytest tests/test_concept_board_backfill.py -q
```
Expected: FAIL — `ModuleNotFoundError: No module named 'tasks.concept_board_backfill'`

- [ ] **Step 3: 写最小实现**

创建 `tasks/concept_board_backfill.py`：

```python
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
from collections.abc import Iterable

from core.calendar import get_expected_latest_trading_day, get_recent_trading_days
from core.known_gaps import declared_missing_days

logger = logging.getLogger(__name__)

# 窗口默认值的含义：504 个概念 × 缺口天数 = 请求数，10 天即 5040 次、约 25 分钟。
# 这是成本上界，超出窗口的缺口不会被自动发现——回补是手动任务，这是它的代价。
DEFAULT_LOOKBACK_DAYS = 10


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
        lookback_days: 窗口大小，单位是**交易日**。
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


def resolve_expected() -> str:
    """独立成函数便于测试打桩。"""
    return get_expected_latest_trading_day()
```

- [ ] **Step 4: 跑测试确认通过**

```bash
uv run pytest tests/test_concept_board_backfill.py -q
```
Expected: `6 passed`

- [ ] **Step 5: 提交（两个文件分开）**

```bash
git add tests/test_concept_board_backfill.py
git commit -F - <<'MSG'
test(concept_board): cover the backfill gap calculation

`concept_board` is written from a spot-snapshot interface, which has no
history, so a day that failed to fetch is unrecoverable. The table shows
it: 31 trading days present where 50 are expected, and the dates jump
around because they mark the runs that happened to succeed.

Pins six behaviours of the gap calculation, including the two that protect
data rather than code: expected is never a candidate (the backfill's NULL
counts would overwrite the snapshot's real ones), and declared missing
days are skipped (they are unrecoverable, and retrying costs 504 requests
each).

Red-proof: against a `tasks/concept_board_backfill.py` that includes
expected in its window, test_expected_day_is_never_included goes red; one
that ignores `declared` reddens test_declared_missing_days_are_skipped.

为缺口计算补测试：

- 六个用例钉住窗口、升序、登记日跳过、窗口截断、节假日不算缺口
- 两个用例守数据而不只是守代码：expected 永不入窗（防 NULL 覆盖快照家数）、
  已登记的整日缺席跳过（防对不可回补日反复发 504 次请求）
- 红证：把 expected 放进窗口 → test_expected_day_is_never_included 变红；
  忽略 declared → test_declared_missing_days_are_skipped 变红
MSG
```

```bash
git add tasks/concept_board_backfill.py
git commit -F - <<'MSG'
feat(concept_board): compute backfill gaps with an exclusive expected bound

find_missing_days() returns the trading days to repair: the most recent
`lookback_days` sessions, minus what the table already has, minus the
declared-missing days, and always strictly below `expected`.

The exclusive bound is the whole point. The snapshot task already wrote
`expected` and it carries up_count/down_count; the history interface has
neither, and save_concept_board_batch is INSERT OR REPLACE, so a backfill
reaching `expected` would overwrite real values with NULL. The default
window of 10 sessions caps the cost at 5040 requests (~25 min) for 504
boards; beyond the window gaps are not discovered automatically, which is
the price of keeping this manual.

新增缺口计算，expected 取严格小于：

- find_missing_days 返回待补交易日：最近 lookback_days 个交易日，减去库内
  已有、减去已登记的整日缺席，且**严格小于 expected**
- 上界的排他性正是重点：快照任务已写过 expected 且带涨跌家数，历史接口两者
  都没有，而 save_concept_board_batch 是 INSERT OR REPLACE —— 回补一旦碰到
  expected 就会用 NULL 覆盖真值
- 默认窗口 10 个交易日把成本封在 5040 次请求（~25 分钟）；超出窗口的缺口
  不自动发现，这是保持手动触发的代价
MSG
```

---

### Task 2: 历史抓取与字段映射

**Files:**
- Modify: `tasks/concept_board_backfill.py`（追加）
- Test: `tests/test_concept_board_backfill.py`（追加）

**Interfaces:**
- Consumes: `get_default_client()`（`core/source_client.py`）的 `call("eastmoney", op)`；akshare 的 `stock_board_concept_name_em()` 与 `stock_board_concept_hist_em(symbol, period, start_date, end_date, adjust)`
- Produces:
  ```python
  HISTORY_SOURCE = "em_hist"

  def _records_from_hist_df(df, *, concept_code: str, concept_name: str) -> list[dict]:
      """东财历史 K 线 → concept_board 记录。up/down_count 恒为 None。"""

  def fetch_day_records(day: str, *, boards: list[tuple[str, str]] | None = None) -> list[dict]:
      """取某个交易日的全部概念板块记录。单个板块失败即跳过。"""
  ```
  `boards` 是 `(concept_code, concept_name)` 列表；None 时内部调
  `stock_board_concept_name_em()` 取得。

- [ ] **Step 1: 写失败测试**

在 `tests/test_concept_board_backfill.py` 顶部补上本批用例需要的 import：

```python
import logging

import pandas as pd
import pytest
```

然后追加：

```python
def test_history_records_carry_em_hist_source_and_null_counts():
    """历史 K 线没有涨跌家数，必须显式写 None 并标记来源，供下游区分。"""
    import pandas as pd

    from tasks.concept_board_backfill import HISTORY_SOURCE, _records_from_hist_df

    df = pd.DataFrame({
        "日期": ["2026-09-28", "2026-09-28"],
        "开盘": [100.0, 100.0],
        "收盘": [102.0, 102.0],
        "最高": [103.0, 103.0],
        "最低": [99.0, 99.0],
        "成交量": [1000.0, 1000.0],
        "成交额": [1.0e8, 1.0e8],
        "涨跌幅": [2.0, 2.0],
    })
    got = _records_from_hist_df(df, concept_code="BK0425", concept_name="算力")
    assert len(got) == 2
    row = got[0]
    assert row["trade_date"] == "2026-09-28"
    assert row["concept_code"] == "BK0425"
    assert row["concept_name"] == "算力"
    assert row["close"] == 102.0
    assert row["pct_change"] == 2.0
    assert row["data_source"] == HISTORY_SOURCE
    assert row["up_count"] is None
    assert row["down_count"] is None


def test_history_records_drop_unknown_columns(caplog):
    """源端列名漂移时只写能识别的列并告警，不抛异常——否则整轮回补全废。"""
    import pandas as pd

    from tasks.concept_board_backfill import _records_from_hist_df

    df = pd.DataFrame({"日期": ["2026-09-28"], "收盘": [102.0], "某个新字段": [1.0]})
    with caplog.at_level(logging.WARNING):
        got = _records_from_hist_df(df, concept_code="BK0425", concept_name="算力")
    assert len(got) == 1
    assert got[0]["close"] == 102.0
    assert got[0]["pct_change"] is None
    assert "列名" in caplog.text


def test_fetch_day_skips_failed_board_and_keeps_the_rest(monkeypatch):
    """单个板块失败不中断其余——一次失败不该让 504 个概念白跑。"""
    import pandas as pd

    import tasks.concept_board_backfill as mod

    boards = [("BK0001", "甲"), ("BK0002", "乙"), ("BK0003", "丙")]

    def fake_hist(symbol, **_):
        if symbol == "乙":
            raise ConnectionError("Connection closed abruptly")
        return pd.DataFrame({"日期": ["2026-09-28"], "收盘": [100.0]})

    monkeypatch.setattr(mod.ak, "stock_board_concept_hist_em", fake_hist)
    got = mod.fetch_day_records("2026-09-28", boards=boards)
    assert {r["concept_name"] for r in got} == {"甲", "丙"}


def test_fetch_day_returns_empty_when_all_boards_fail(monkeypatch):
    import tasks.concept_board_backfill as mod

    def boom(*_a, **_k):
        raise ConnectionError("down")

    monkeypatch.setattr(mod.ak, "stock_board_concept_hist_em", boom)
    assert mod.fetch_day_records("2026-09-28", boards=[("BK0001", "甲")]) == []
```

- [ ] **Step 2: 跑测试确认失败**

```bash
uv run pytest tests/test_concept_board_backfill.py -q
```
Expected: FAIL — `ImportError: cannot import name 'HISTORY_SOURCE'`

- [ ] **Step 3: 写实现**

追加到 `tasks/concept_board_backfill.py`：

```python
import pandas as pd

from core.source_client import get_default_client

try:
    import akshare as ak
except ImportError:
    ak = None
```

**限速机制：与 spec 的一处有意偏离。** spec 写「复用
`tasks/sector_derivatives.py:71` 的 `_retry`」，这里改用
`get_default_client().call("eastmoney", ...)`。`core/source_client.py` 的模块
docstring 自述职责就是取代各处的 ad-hoc 重试循环，eastmoney policy 自带
`max_attempts=3`、指数退避、`min_interval_seconds=0.8` 限速与熔断。两套重试叠
在一起会让单次失败退避到几分钟，正是「跑一小时只补上几百行」要避免的结果。
**因此不引入 `_retry`。**

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
}
# 记录键集固定，缺列写 None。行与行之间键不一致会让下游按 key 取值踩 KeyError。
_HIST_VALUE_COLS = ("open", "close", "high", "low", "volume", "amount", "pct_change")


def _records_from_hist_df(
    df: "pd.DataFrame", *, concept_code: str, concept_name: str
) -> list[dict]:
    """东财历史 K 线 → concept_board 记录。``up_count``/``down_count`` 恒为 None。"""
    if df is None or df.empty:
        return []
    df = df.rename(columns=_HIST_COL_MAP)
    known = [c for c in _HIST_VALUE_COLS if c in df.columns]
    unknown = [c for c in df.columns if c not in _HIST_COL_MAP.values()]
    if unknown:
        logger.warning(
            f"⚠️ 概念板块历史列名漂移，未识别 {sorted(unknown)}，只写 {known}"
        )
    if "trade_date" not in df.columns:
        logger.warning("⚠️ 概念板块历史缺少日期列，跳过该板块")
        return []
    records: list[dict] = []
    for _, row in df.iterrows():
        day = str(row.get("trade_date", "")).strip()[:10]
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
            "up_count": None,
            "down_count": None,
            "data_source": HISTORY_SOURCE,
        })
    return records


def _fetch_board_list() -> list[tuple[str, str]]:
    """概念板块列表，返回 ``(code, name)``。取不到时返回空列表。"""
    if ak is None:
        return []
    resp = get_default_client().call("eastmoney", ak.stock_board_concept_name_em)
    if not resp.success or resp.data is None:
        logger.warning(f"⚠️ 概念板块列表获取失败: {resp.metadata.error}")
        return []
    df = resp.data
    if "板块代码" not in df.columns or "板块名称" not in df.columns:
        logger.warning(f"⚠️ 概念板块列表列名不符，可用列: {list(df.columns)}")
        return []
    out: list[tuple[str, str]] = []
    for _, row in df.iterrows():
        code = str(row.get("板块代码", "")).strip()
        name = str(row.get("板块名称", "")).strip()
        if code and name:
            out.append((code, name))
    return out


def fetch_day_records(
    day: str, *, boards: list[tuple[str, str]] | None = None
) -> list[dict]:
    """取某个交易日的全部概念板块记录。单个板块失败即跳过，不中断其余。"""
    if ak is None:
        logger.error("❌ akshare 未安装")
        return []
    if boards is None:
        boards = _fetch_board_list()
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
        records.extend(_records_from_hist_df(resp.data, concept_code=code, concept_name=name))
    return records
```

- [ ] **Step 4: 跑测试确认通过**

```bash
uv run pytest tests/test_concept_board_backfill.py -q
```
Expected: `10 passed`

- [ ] **Step 5: 提交（两个文件分开）**

```bash
git add tests/test_concept_board_backfill.py
git commit -F - <<'MSG'
test(concept_board): cover the history fetch and its column mapping

The column names are inferred from stock_board_industry_hist_em — the
same API family, already running in production for industry boards — and
eastmoney has been flapping since 2026-09-29, so the real names are not
yet observed. These cases therefore pin the *degradation* behaviour that
makes an unverified mapping safe: unknown columns are dropped with a
warning instead of raising, which would otherwise waste all 504 boards in
one run.

Also pins that a single failing board is skipped rather than aborting the
day, and that up_count/down_count are explicitly None with data_source
em_hist, so a backfilled row is distinguishable from a snapshot row.

Red-proof: dropping the unknown-column branch reddens
test_history_records_drop_unknown_columns; making one board failure abort
reddens test_fetch_day_skips_failed_board_and_keeps_the_rest.

为历史抓取与字段映射补测试：

- 列名未实测，因此钉住的是**降级行为**：遇到不认识的列只写能识别的并告警，
  不抛异常——否则一次列名漂移就让 504 个概念全部白跑
- 单个板块失败只跳过，不中断当天其余板块
- 断言 up_count/down_count 显式为 None 且 data_source='em_hist'，使回补行
  可与快照行区分
- 红证：去掉未知列降级分支 → test_history_records_drop_unknown_columns 变红；
  让单个板块失败中断 → test_fetch_day_skips_failed_board_and_keeps_the_rest 变红
MSG
```

```bash
git add tasks/concept_board_backfill.py
git commit -F - <<'MSG'
feat(concept_board): fetch history per board with a lenient column mapping

fetch_day_records() walks the board list and pulls one day from
stock_board_concept_hist_em per board. A board that fails is logged and
skipped; the rest of the day still lands.

_records_from_hist_df() maps columns through _HIST_COL_MAP and keeps
whatever it recognises. The mapping is inferred, not verified — eastmoney
has been unreachable at minute granularity since 2026-09-29 — so an
unrecognised column produces a warning and a partial record rather than an
exception. That choice matters: this loop is 504 requests, and an
exception on board 1 would waste the other 503.

up_count and down_count are set to None explicitly and every row is
tagged data_source='em_hist', so a backfilled row is never mistaken for a
snapshot row.

逐板块抓取历史，列名宽松降级：

- fetch_day_records 遍历板块列表，逐个取某一天；单个失败记日志并跳过，
  当天其余板块照常落库
- _records_from_hist_df 按 _HIST_COL_MAP 映射并只保留能识别的列。映射是推定
  而非实测——东财自 2026-09-29 起分钟级抖动——所以不认识的列产生告警与部分
  记录，而不是抛异常。这个选择有分量：这是 504 次请求的循环，第 1 个板块就
  抛异常会把另外 503 个全浪费掉
- up_count/down_count 显式写 None，每行标 data_source='em_hist'，回补行不会被
  误认成快照行
MSG
```

---

### Task 3: 主任务函数与状态语义

**Files:**
- Modify: `tasks/concept_board_backfill.py`（追加）
- Test: `tests/test_concept_board_backfill.py`（追加）

**Interfaces:**
- Consumes: `find_missing_days`、`fetch_day_records`、`db.save_concept_board_batch(records) -> int`（`interface.DatabaseInterface`）
- Produces:
  ```python
  def update_concept_board_backfill(
      db: DatabaseInterface,
      *,
      lookback_days: int = DEFAULT_LOOKBACK_DAYS,
      target_date: str | None = None,
      _task_run_id: str | None = None,
  ) -> dict[str, Any]:
  ```
  返回含 `status` / `saved` / `requested_days` / `backfilled_days` / `failed_days`。

- [ ] **Step 1: 写失败测试**

追加到 `tests/test_concept_board_backfill.py`：

```python
def _db():
    from unittest.mock import MagicMock
    db = MagicMock()
    db.save_concept_board_batch = MagicMock(return_value=504)
    return db


def test_no_gap_makes_no_network_call_and_no_write(monkeypatch):
    """无缺口必须零请求零写入——这是日常手动跑时的常态路径。"""
    import tasks.concept_board_backfill as mod

    monkeypatch.setattr(mod, "get_recent_trading_days",
                        lambda end, count: ["2026-09-29", "2026-09-28"])
    monkeypatch.setattr(mod, "get_expected_latest_trading_day", lambda: "2026-09-29")
    monkeypatch.setattr(
        mod, "_stored_dates", lambda db: {"2026-09-28"}
    )
    def boom(*_a, **_k):
        raise AssertionError("不应发起任何网络请求")
    monkeypatch.setattr(mod, "fetch_day_records", boom)

    db = _db()
    result = mod.update_concept_board_backfill(db)
    assert result["status"] == "success"
    assert result["saved"] == 0
    assert result["requested_days"] == 0
    assert not db.save_concept_board_batch.called


def test_all_days_backfilled_is_success(monkeypatch):
    import tasks.concept_board_backfill as mod

    monkeypatch.setattr(mod, "get_recent_trading_days",
                        lambda end, count: ["2026-09-29", "2026-09-28"])
    monkeypatch.setattr(mod, "get_expected_latest_trading_day", lambda: "2026-09-29")
    monkeypatch.setattr(mod, "_stored_dates", lambda db: set())
    monkeypatch.setattr(
        mod, "fetch_day_records",
        lambda day, boards=None: [{"trade_date": day, "concept_code": "BK1"}],
    )
    result = mod.update_concept_board_backfill(_db())
    assert result["status"] == "success"
    assert result["saved"] == 504
    assert result["backfilled_days"] == ["2026-09-28"]


def test_partial_backfill_is_degraded(monkeypatch):
    import tasks.concept_board_backfill as mod

    monkeypatch.setattr(mod, "get_recent_trading_days",
                        lambda end, count: ["2026-09-29", "2026-09-28", "2026-09-25"])
    monkeypatch.setattr(mod, "get_expected_latest_trading_day", lambda: "2026-09-29")
    monkeypatch.setattr(mod, "_stored_dates", lambda db: set())
    monkeypatch.setattr(
        mod, "fetch_day_records",
        lambda day, boards=None: ([{"trade_date": day, "concept_code": "BK1"}]
                                  if day == "2026-09-28" else []),
    )
    result = mod.update_concept_board_backfill(_db())
    assert result["status"] == "degraded"
    assert result["backfilled_days"] == ["2026-09-28"]
    assert result["failed_days"] == ["2026-09-25"]


def test_nothing_backfilled_when_source_is_down_is_retained_network(monkeypatch):
    """一天都没补上 = 源整体不可用，标 network 让 safe_task 的 30s 重试生效。"""
    import tasks.concept_board_backfill as mod

    monkeypatch.setattr(mod, "get_recent_trading_days",
                        lambda end, count: ["2026-09-29", "2026-09-28"])
    monkeypatch.setattr(mod, "get_expected_latest_trading_day", lambda: "2026-09-29")
    monkeypatch.setattr(mod, "_stored_dates", lambda db: set())
    monkeypatch.setattr(mod, "fetch_day_records", lambda day, boards=None: [])
    result = mod.update_concept_board_backfill(_db())
    assert result["status"] == "retained"
    assert result["error_kind"] == "network"
    assert result["retained_old_data"] is True


def test_backfill_never_writes_expected_or_later(monkeypatch):
    """写入上界闸门：源端若返回 expected 当天（或更晚）的行，必须被丢弃。"""
    import tasks.concept_board_backfill as mod

    monkeypatch.setattr(mod, "get_recent_trading_days",
                        lambda end, count: ["2026-09-29", "2026-09-28"])
    monkeypatch.setattr(mod, "get_expected_latest_trading_day", lambda: "2026-09-29")
    monkeypatch.setattr(mod, "_stored_dates", lambda db: set())
    # 源端好心（或错乱）地把 09-29 也塞了回来
    monkeypatch.setattr(
        mod, "fetch_day_records",
        lambda day, boards=None: [
            {"trade_date": day, "concept_code": "BK1"},
            {"trade_date": "2026-09-29", "concept_code": "BK2"},
        ],
    )
    db = _db()
    mod.update_concept_board_backfill(db)
    written = db.save_concept_board_batch.call_args[0][0]
    assert {r["trade_date"] for r in written} == {"2026-09-28"}
```

- [ ] **Step 2: 跑测试确认失败**

```bash
uv run pytest tests/test_concept_board_backfill.py -q
```
Expected: FAIL — `AttributeError: module 'tasks.concept_board_backfill' has no attribute 'update_concept_board_backfill'`

- [ ] **Step 3: 写实现**

追加到 `tasks/concept_board_backfill.py`：

```python
import sqlite3
from pathlib import Path
from typing import Any

from interface import DatabaseInterface


def _stored_dates(db: DatabaseInterface) -> set[str]:
    """库内 ``concept_board`` 已有的 trade_date。取不到时返回空集合。"""
    path = getattr(db, "db_path", None)
    if not path:
        return set()
    try:
        conn = sqlite3.connect(f"file:{Path(str(path)).resolve()}?mode=ro", uri=True, timeout=5.0)
    except sqlite3.Error as exc:
        logger.warning(f"⚠️ 读取 concept_board 已有日期失败: {exc}")
        return set()
    try:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT trade_date FROM concept_board")
        return {str(r[0])[:10] for r in cur.fetchall() if r[0]}
    except sqlite3.Error as exc:
        logger.warning(f"⚠️ 读取 concept_board 已有日期失败: {exc}")
        return set()
    finally:
        conn.close()


def update_concept_board_backfill(
    db: DatabaseInterface,
    *,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    target_date: str | None = None,
    _task_run_id: str | None = None,
) -> dict[str, Any]:
    """回补 ``concept_board`` 已丢失的交易日。手动任务，不进日常管道。

    状态语义（按「有没有补上任何一天」分，不按「失败几天」分）：

    * 无缺口 → ``success``, saved=0，零网络请求
    * 全部补上 → ``success``
    * 补上至少一天 → ``degraded``，未补足的日子列入 ``failed_days``
    * **一天都没补上** → ``retained`` + ``error_kind=network``，
      让 ``core/runner.py`` 的 ``safe_task`` 在 30s 后重试

    Args:
        db: 数据库接口。
        lookback_days: 窗口大小，单位**交易日**。
        target_date: 指定只补这一天；None 时按窗口算缺口。
        _task_run_id: 由 ``safe_task`` 注入。
    """
    logger.info("\n" + "=" * 60)
    logger.info("🩹 任务: 概念板块缺口回补 (窗口 %d 个交易日)", lookback_days)
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"status": "failed", "error": "akshare not installed", "saved": 0}

    expected = resolve_expected()
    stored = _stored_dates(db)
    if target_date is not None:
        days = [] if target_date >= expected else [target_date]
    else:
        days = find_missing_days(stored, expected=expected, lookback_days=lookback_days)

    result: dict[str, Any] = {
        "saved": 0,
        "requested_days": len(days),
        "backfilled_days": [],
        "failed_days": [],
    }
    if not days:
        logger.info("✅ 概念板块无缺口，无需回补")
        result["status"] = "success"
        return result

    logger.info(f"📋 待补 {len(days)} 个交易日: {', '.join(days)}")
    boards = _fetch_board_list()
    if not boards:
        result["status"] = "retained"
        result["error_kind"] = "network"
        result["retained_old_data"] = True
        result["error"] = "概念板块列表不可用，未回补任何一天"
        return result

    for day in days:
        records = fetch_day_records(day, boards=boards)
        # 上界闸门：源端若返回 expected 当天或更晚的行，丢弃。
        # 快照已写 expected 且带涨跌家数，INSERT OR REPLACE 会用 NULL 覆盖它。
        records = [r for r in records if r["trade_date"] < expected]
        if not records:
            result["failed_days"].append(day)
            logger.warning(f"⚠️ 概念板块 {day} 回补 0 行，保留旧数据")
            continue
        saved = db.save_concept_board_batch(records)
        result["saved"] = saved
        result["backfilled_days"].append(day)
        logger.info(f"✅ 概念板块 {day} 回补完成: {saved} 条")

    if not result["backfilled_days"]:
        result["status"] = "retained"
        result["error_kind"] = "network"
        result["retained_old_data"] = True
        result["error"] = f"源不可用，{len(days)} 个交易日全部未补上"
        logger.warning(f"⚠️ 概念板块回补未补上任何一天，保留旧数据: {days}")
    elif result["failed_days"]:
        result["status"] = "degraded"
        result["reason"] = f"partially backfilled; failed days: {', '.join(result['failed_days'])}"
        result["retained_old_data"] = True
    else:
        result["status"] = "success"
    return result
```

- [ ] **Step 4: 跑测试确认通过**

```bash
uv run pytest tests/test_concept_board_backfill.py -q
```
Expected: `15 passed`

- [ ] **Step 5: 提交（两个文件分开）**

```bash
git add tests/test_concept_board_backfill.py
git commit -F - <<'MSG'
test(concept_board): pin the status semantics and the write ceiling

The ceiling case is the important one: a source that helpfully returns
rows for `expected` must have them discarded, because the snapshot already
wrote that day with up_count/down_count and INSERT OR REPLACE would
overwrite real values with NULL. That is the one bug in this feature that
corrupts data rather than merely failing.

The status cases pin the split that was ambiguous in the spec: degraded
means progress was made, retained + error_kind=network means nothing
landed and the source is down — and error_kind is what makes
core/runner.py's 30s retry fire. A run that backfilled nothing must not
report success; that is the silent-degradation shape P2-18 documents.

Red-proof: dropping the write-ceiling filter reddens
test_backfill_never_writes_expected_or_later; returning success when
nothing landed reddens
test_nothing_backfilled_when_source_is_down_is_retained_network.

钉住状态语义与写入上界：

- 上界用例最要紧：源端若把 expected 当天的行也返回，必须丢弃——快照已写过
  那天且带涨跌家数，INSERT OR REPLACE 会用 NULL 覆盖真值。这是本功能里唯一
  会**损坏数据**而非仅仅失败的缺陷
- 状态用例钉住 spec 里原本含糊的分界：degraded = 有进展；
  retained + error_kind=network = 一天都没补上、源挂了——而 error_kind 正是
  core/runner.py 的 30s 重试能触发的原因
- 什么都没补上时不得报 success，否则就是 P2-18 记述的静默退化形态
- 红证：去掉上界过滤 → test_backfill_never_writes_expected_or_later 变红；
  没补上任何一天时返回 success →
  test_nothing_backfilled_when_source_is_down_is_retained_network 变红
MSG
```

```bash
git add tasks/concept_board_backfill.py
git commit -F - <<'MSG'
feat(concept_board): the backfill task itself

update_concept_board_backfill() computes the gaps, then walks them one day
at a time, writing each day's records as it lands so a run that dies
halfway keeps what it already repaired.

Status splits on whether any day landed, not on how many failed:
degraded for partial progress with failed_days listed, retained +
error_kind=network when nothing landed at all. The error_kind is what
makes safe_task retry after 30s; without it a dead source would be
indistinguishable from a healthy one that had nothing to do.

Every batch passes through a `trade_date < expected` filter before the
write. That filter is the feature's data-safety guarantee — the snapshot
task owns `expected` and carries the up/down counts the history interface
does not have.

新增回补主任务：

- update_concept_board_backfill 先算缺口，再逐日抓取并当天落库，中途中断也
  保住已修好的部分
- 状态按「有没有补上任何一天」分：部分进展 = degraded 并列出 failed_days；
  一天都没补上 = retained + error_kind=network。error_kind 正是让 safe_task
  在 30s 后重试的开关，没有它「源挂了」与「没活干」无法区分
- 每批写入前都过 `trade_date < expected` 过滤。这是本功能的数据安全保证——
  expected 归快照任务所有，而涨跌家数是历史接口给不出的
MSG
```

---

### Task 4: 注册与接线（手动，不进日常管道）

**Files:**
- Modify: `core/task_registry.py:1086` 之后（新增 `TaskSpec`）
- Modify: `daily_pipeline.py:119`（import）、`daily_pipeline.py:267` 附近（`_TASK_CALLABLES`）
- Modify: `daily_pipeline.py` argparse（新增 `--lookback`）
- Modify: `tui/widgets/single_task.py:64` 之后
- Test: `tests/test_concept_board_backfill_wiring.py`（新建）

**Interfaces:**
- Consumes: `tasks.concept_board_backfill.update_concept_board_backfill`
- Produces: CLI `--task update_concept_board_backfill [--lookback N]`

- [ ] **Step 1: 写失败测试**

创建 `tests/test_concept_board_backfill_wiring.py`：

```python
"""回补任务的接线门禁：它必须是手动任务，一不小心接进日常管道就是 15 分钟。

`core/runner.py` 的 cadence 过滤**不跳过** `ON_DEMAND` 任务，所以只靠注册表
的 cadence 字段并不能保证它不进 `run_all`——必须由门禁钉住。
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from core.task_registry import CATCH_UP_TASK_ORDER, lookup_task

TASK = "update_concept_board_backfill"
DAILY_PIPELINE = Path(__file__).resolve().parents[1] / "daily_pipeline.py"


def test_task_is_registered_as_on_demand():
    spec = lookup_task(TASK)
    assert spec is not None
    assert spec.cadence.value == "on_demand"
    assert spec.tables == ("concept_board",)
    assert spec.display_label


def test_task_is_absent_from_catch_up_order():
    """`compute_catch_up_tasks` 的 claimed 集合只允许一张表被一个任务认领。

    进了这个列表且排在 update_concept_board 之后，「补齐缺失」按钮会永远选到
    快照任务 → 只补今天 → 面板仍滞后 → 死循环。
    """
    assert TASK not in CATCH_UP_TASK_ORDER


def _stage_task_names() -> set[str]:
    """静态扫出 run_all 里所有 stage*_raw_tasks / _ptasks 声明的任务名。"""
    tree = ast.parse(DAILY_PIPELINE.read_text(encoding="utf-8"))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id.startswith("stage") \
                        and target.id.endswith(("_raw_tasks", "_ptasks")):
                    for elt in ast.walk(node.value):
                        if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
                            names.add(elt.value)
    return names


def test_task_is_not_wired_into_any_daily_stage():
    assert TASK not in _stage_task_names(), (
        f"{TASK} 出现在 run_all 的 stage 列表里：core/runner.py 的 cadence 过滤"
        "不跳过 ON_DEMAND 任务，一旦接入就会每晚执行 5040 次请求"
    )


def test_task_is_dispatchable_via_task_callables():
    from daily_pipeline import _TASK_CALLABLES
    assert TASK in _TASK_CALLABLES


def test_lookback_flag_is_registered():
    """`--lookback` 必须真的出现在 CLI 上（parser 局部于 main()，用 --help 探针）。"""
    import subprocess
    import sys
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    out = subprocess.run(
        [sys.executable, str(root / "daily_pipeline.py"), "--help"],
        capture_output=True, text=True, cwd=root, timeout=120,
    ).stdout
    assert "--lookback" in out
    assert "交易日" in out, "帮助文本必须写明单位是交易日，否则 --lookback 10 会被读成 10 天"


def test_run_registry_task_forwards_lookback(monkeypatch):
    """--lookback 的值必须真的透传到任务，否则改窗口只会是个摆设。"""
    from daily_pipeline import _run_registry_task, _TASK_CALLABLES
    from unittest.mock import MagicMock

    seen = {}
    fn = MagicMock(return_value={"status": "success", "saved": 0})
    def _capture(db, **kwargs):
        seen.update(kwargs)
        return {"status": "success", "saved": 0}
    fn.side_effect = _capture
    monkeypatch.setitem(_TASK_CALLABLES, TASK, fn)
    _run_registry_task(TASK, MagicMock(), None, None, lookback_days=30)
    assert seen.get("lookback_days") == 30

- [ ] **Step 2: 跑测试确认失败**

```bash
uv run pytest tests/test_concept_board_backfill_wiring.py -q
```
Expected: FAIL — `test_task_is_registered_as_on_demand` 报 `spec is None`

- [ ] **Step 3: 注册 TaskSpec**

在 `core/task_registry.py` 的 `update_concept_board` `TaskSpec`（约 1086-1101 行）之后插入：

```python
    TaskSpec(
        name="update_concept_board_backfill",
        callable=None,
        tables=("concept_board",),
        # ON_DEMAND 表示「这是运维型任务」。注意 core/runner.py 的 cadence 过滤
        # **不跳过** ON_DEMAND，所以本任务**不得**接入 run_all 的任何 stage——
        # 接线门禁见 tests/test_concept_board_backfill_wiring.py。
        cadence=Cadence.ON_DEMAND,
        date_columns={"concept_board": "trade_date"},
        empty_policy=EmptyPolicy.ALLOW,
        primary_source="akshare",
        display_label="概念板块缺口回补",
    ),
```

- [ ] **Step 4: 接到 CLI**

`daily_pipeline.py` 第 119 行附近，把

```python
from tasks.concept_board import update_concept_board, update_concept_member
```

改为

```python
from tasks.concept_board import update_concept_board, update_concept_member
from tasks.concept_board_backfill import update_concept_board_backfill
```

在 `_TASK_CALLABLES`（约 267 行）里，紧跟 `"update_concept_board": update_concept_board,` 之后加一行：

```python
    "update_concept_board_backfill": update_concept_board_backfill,
```

在单任务分发函数 `_run_registry_task`（`daily_pipeline.py:287`）里，
`force` 形参之后加 `lookback_days: int = 10`，并在约 366 行的兜底分支之前插入：

```python
    if task_name == "update_concept_board_backfill":
        return _safe_task(task_name, fn, db, lookback_days=lookback_days)
```

在 `main()` 的 `argparse.ArgumentParser`（`daily_pipeline.py:1219`）里加：

```python
    parser.add_argument(
        "--lookback", type=int, default=10,
        help="回补窗口，单位是交易日（仅 update_concept_board_backfill 使用）",
    )
```

并把 `args.lookback` 传给 `_run_registry_task(...)` 的调用处。

**注意盘中门禁**：`daily_pipeline.py:308-317` 的门禁只拦
`spec.cadence is Cadence.TRADING_DAY`。本任务是 `ON_DEMAND`，**不会被拦**——
这是正确的：快照任务盘中跑会把实时值冻结成日线（2026-07-30 事故），而回补
只写 `< expected` 的历史日，盘中跑没有这个风险。不要为此把 cadence 改成
`TRADING_DAY`。

- [ ] **Step 5: 接到 TUI**

`tui/widgets/single_task.py` 第 64 行 `"update_concept_board": "概念板块 (Concept Board)",` 之后加：

```python
    "update_concept_board_backfill": "概念板块缺口回补 (Concept Board Backfill)",
```

- [ ] **Step 6: 跑测试确认通过**

```bash
uv run pytest tests/test_concept_board_backfill_wiring.py -q
```
Expected: `5 passed`（`test_lookback_flag_is_registered` 依实现可能需按实际 parser 调整）

- [ ] **Step 7: 跑全量门禁**

```bash
uv run ruff check
uv run mypy
uv run pytest --cov -q
uv run coverage report
```
Expected: ruff 0；mypy `Success`；pytest 全绿；`Required test coverage of 85.0% reached`

- [ ] **Step 8: 提交（每个文件一个 commit）**

```bash
git add core/task_registry.py
git commit -F - <<'MSG'
feat(concept_board): register the backfill task as on-demand

Declares the task alongside update_concept_board with the same table —
the chip_distribution_em daily/fullmarket pair is the existing precedent
for one table having two owners.

The cadence comment is load-bearing: core/runner.py's filter does not skip
ON_DEMAND tasks, so cadence alone does not keep this out of the nightly
run. That is why the wiring gate in the next commit exists.

注册回补任务为 ON_DEMAND：

- 与 update_concept_board 声明同一张表，沿用 chip_distribution_em
  日常版/全市场版这一「一表两 owner」先例
- cadence 处的注释是要紧的：core/runner.py 的过滤**不跳过** ON_DEMAND，
  所以光靠 cadence 并不能保证它不进夜跑——这正是下一提交那道门禁的由来
MSG
```

```bash
git add daily_pipeline.py
git commit -F - <<'MSG'
feat(concept_board): dispatch the backfill through --task and --lookback

Registers the callable and adds a --lookback flag whose unit is trading
days, so `--lookback 60` reaches the 23 days of historical debt in one
run. Deliberately absent: any change to CATCH_UP_TASK_ORDER or to the
run_all stage lists. Wiring it into the nightly pipeline would add 5040
requests to every run, and adding it to the catch-up order would route
the button to the snapshot task forever.

接上 --task 与 --lookback 入口：

- 注册 callable，新增 --lookback 开关，单位是交易日，`--lookback 60` 可一次
  覆盖 23 天历史欠账
- 刻意不动 CATCH_UP_TASK_ORDER 与 run_all 的任何 stage 列表：接进夜跑会让每轮
  多 5040 次请求；接进补齐清单会让按钮永远选到快照任务
MSG
```

```bash
git add tui/widgets/single_task.py
git commit -F - <<'MSG'
feat(tui): list the concept board backfill as a clickable task

So the manual repair is reachable from the TUI without memorising the CLI
invocation. Kept next to update_concept_board in the single-task map.

TUI 单任务清单加入「概念板块缺口回补」：

- 手动修复不必记命令行
- 紧邻 update_concept_board，保持单任务清单的分组顺序
MSG
```

```bash
git add tests/test_concept_board_backfill_wiring.py
git commit -F - <<'MSG'
test(concept_board): gate the backfill against being wired into the run

Cadence alone does not keep this task out of the nightly pipeline:
core/runner.py's filter skips only non-ON_DEMAND tasks, so adding the task
to a stage list would execute 5040 requests every night while still
reading as ON_DEMAND in the registry. The gate parses daily_pipeline.py's
stage assignments with ast and asserts the name is absent from all of them.

The catch-up-order case is the subtler one. compute_catch_up_tasks gives
each table to exactly one task via its claimed set, so a backfill entry
ordered after update_concept_board would make the catch-up button repair
only today, leave the panel stale, and reappear tomorrow — a loop.

Red-proof: adding the name to any stage list reddens
test_task_is_not_wired_into_any_daily_stage; adding it to
CATCH_UP_TASK_ORDER reddens test_task_is_absent_from_catch_up_order.

门禁：回补任务不得被接进日常管道

- cadence 本身挡不住：core/runner.py 只跳过非 ON_DEMAND 的任务，所以一旦
  被加进任何 stage 列表，它会每晚跑 5040 次请求，而注册表里仍显示 ON_DEMAND。
  门禁用 ast 静态解析 daily_pipeline.py 的 stage 赋值并断言名字不在其中
- 补齐清单那条更隐蔽：compute_catch_up_tasks 用 claimed 集合把一张表只交给
  一个任务，所以排在 update_concept_board 之后的回补条目会让按钮只补今天、
  面板仍滞后、次日再进清单，形成死循环
- 红证：把名字加进任何 stage 列表 → test_task_is_not_wired_into_any_daily_stage
  变红；加进 CATCH_UP_TASK_ORDER → test_task_is_absent_from_catch_up_order 变红
MSG
```

---

## 执行后验证（实网，需东财可达）

实现完成后，线路恢复时跑一次真实回补并核对：

```bash
# 小窗口先试，确认字段映射与耗时
uv run python daily_pipeline.py --task update_concept_board_backfill --force --lookback 3

# 核对：data_source 分布、日期连续性、up/down_count 是否为 NULL
uv run python -c "
import sqlite3
c=sqlite3.connect('file:/Users/hainingyu/Code/quant_data/quant_core.db?mode=ro',uri=True)
for r in c.execute('SELECT trade_date, data_source, COUNT(*), SUM(up_count IS NULL) FROM concept_board GROUP BY 1,2 ORDER BY 1 DESC LIMIT 10'): print(r)
"
```

期望：`data_source='em_hist'` 的行 `SUM(up_count IS NULL) == COUNT(*)`，日期
连续覆盖 09-24 ~ 09-28。**若列名映射与真实接口不符**，核对后修正
`_HIST_COL_MAP` 并在 spec 的「待验证清单」里划掉第 1 条。
