# 概念板块缺口回补设计

日期：2026-09-30
状态：待评审

## 问题

`concept_board` 是唯一一个用**实时快照接口**写入的日频板块任务。快照接口只给
「此刻」，不留历史，因此：

- 某天抓取失败 → 该天数据**永久不可恢复**
- 库内日期分布是跳跃的：已有 31 个交易日，实际跨度内应有 50 个（缺 23 天），
  缺失日集中在 2026-07-22 ~ 2026-09-29
- 每天必须重抓，线路一抖就多一个永久空洞

对照 `sector_daily`（走 `stock_board_industry_hist_em` 历史接口）：日期连续、
`2026-09-29` 已有 90 行，从未真正缺过。

**根因不是「缺备用源」，是「用了不可回补的接口」。** 本设计换接口，不换源。

## 为什么不是同花顺兜底

调研（2026-09-30 实测）否决了「东财 → 同花顺」兜底路线，四个硬障碍：

| 障碍 | 实测证据 |
|---|---|
| code 空间不兼容 | 同花顺 `308614`/`309121`（6 位数字）vs 东财 `BK0425`。`concept_code` 是 `unique_by=("trade_date", "concept_code")` 的组成部分，混写会污染主键语义并破坏与 `concept_member` 的关联 |
| 概念集合不对齐 | 同花顺 375 个 vs 东财 504 个；`白酒` 不在同花顺列表内。兜底写入的不是同一批板块 |
| 拿不到涨跌家数 | `stock_board_concept_index_ths` 只给 OHLC + 量额。`stock_board_concept_info_ths` **不是**成分股接口（返回 `项目`/`值` 两列的行情快照），无法据此算 `up_count`/`down_count` |
| 成本不可接受 | 单概念 0.2~3s × 375 个 = 1~19 分钟；东财路径一次分页拿全部 504 个只要 ~1s |

附带陷阱：`stock_board_concept_index_ths` 不传 `start_date`/`end_date` 会返回
错误区间（实测默认参数 → `2025-02-28`，显式区间 → `2026-09-29`）。

## 方案

新增手动任务 `update_concept_board_backfill`，走东财历史接口
`ak.stock_board_concept_hist_em(symbol, period, start_date, end_date, adjust)`
（与 `tasks/sector_derivatives.py` 已验证的行业板块模式对称）。

**手动触发，不接进日常流程。** 夜跑耗时 0 请求、0 秒。

### 任务边界

| | `update_concept_board`（不动） | `update_concept_board_backfill`（新增） |
|---|---|---|
| 语义 | 今日快照 | 历史缺口修复 |
| 数据源 | `push2` 实时快照 | `stock_board_concept_hist_em` |
| 请求数 | 1（分页拿全 504 个） | 0，或 504 × 缺口天数 |
| 耗时 | ~1s | 0，或 ~15min（10 天窗口） |
| 写入日期 | 仅 `expected` | **严格 `< expected`** |
| `up_count`/`down_count` | 有值 | `NULL` |
| `data_source` | `"em"` | `"em_hist"` |

**写入日期上界是硬约束。** 两者写同一批 `(trade_date, concept_code)`，而
`providers.py:1973` 用 `INSERT OR REPLACE` —— 回补若覆盖到 `expected`，会用
`NULL` 抹掉快照刚写入的涨跌家数。回补只写 `< expected`，今日完全交给快照。

### 缺口判定

```
窗口 = 最近 lookback_days 个**交易日**（默认 10）∩ [< expected]
缺口 = 窗口内不在库中的交易日
跳过 = 已在 core/known_gaps.py 登记的日子
无缺口 → 立即 success, saved=0，零网络请求
```

`--lookback` 的单位是**交易日**（经 `core/calendar.get_recent_trading_days`
折算），不是自然日。`--lookback 60` 指最近 60 个交易日。

**刻意不写入 `expected`**，所以窗口是 `< expected` 而非 `<= expected`。

跳过登记日：那些是已核实不可回补的历史空洞，反复尝试只是浪费 504 次请求。
与 `core/backfill.py` 的处理一致（它同样先过 `known_gaps.declared_missing_days`）。

窗口默认 10 个交易日是**成本上界**：504 × 10 = 5040 次请求、~25 分钟。
超出窗口的缺口不会被自动发现，这是手动触发的直接代价。

### 入口

```bash
# 补最近 10 天内的所有缺口
uv run python daily_pipeline.py --task update_concept_board_backfill --force

# 放宽窗口（一次性还清 23 天历史欠账用这个）
uv run python daily_pipeline.py --task update_concept_board_backfill --force --lookback 60

# TUI 任务列表里点「概念板块缺口回补」
```

**不接 `--backfill-table`。** `core/backfill.py` 有门禁
（`tests/test_backfill.py`）要求 `BACKFILLABLE_TABLES` /
`PARTIAL_BACKFILLABLE_TABLES` / `NOT_BACKFILLABLE_TABLES` 三组**正好覆盖**
`day_coverage.PROBE_TABLES`，而 `concept_board` 不是探针表，加进去会破门禁。
`--lookback` 已经能覆盖「补某一天」的需求（旧债全在 60 个交易日窗口内），
不值得为此放松一条已审计的不变量。

### 接线点

| 位置 | 改动 | 理由 |
|---|---|---|
| `core/task_registry.py` | 新增 `TaskSpec`：`tables=("concept_board",)`、`cadence=ON_DEMAND`、`display_label="概念板块缺口回补"`、`date_columns={"concept_board": "trade_date"}` | 沿用 `chip_distribution_em` 的「一表两 owner」先例（日常版 + ON_DEMAND 全市场版） |
| `CATCH_UP_TASK_ORDER` | **不动** | 见下 |
| `daily_pipeline._TASK_CALLABLES` | 注册 | 供 `--task` 单任务入口 |
| `daily_pipeline` stage2/3/4 | **不动** | 手动任务不进日常编排 |
| `tui/widgets/single_task.py` | 加显示名 | TUI 可点 |

`cadence=ON_DEMAND` 在此**只表示「这是运维型任务」**：`daily_pipeline.py:636` 的
cadence 过滤只跳过非 `ON_DEMAND` 的任务，所以若日后有人把它加进 `run_all` 的
stage 列表，它会被无条件执行。**这正是要防的**——所以加注释写明「不得接入
`run_all`」，并由一条门禁测试断言它不在任何 stage 列表里。

**`CATCH_UP_TASK_ORDER` 刻意不加。** `compute_catch_up_tasks` 用 `claimed` 集合
保证「一张表只被一个任务认领」（`core/task_registry.py:404-406` 注释写明
「确保选到日常任务而非 ON_DEMAND 任务」）。若回补任务进这个列表且排在
`update_concept_board` 之后，「补齐缺失」按钮永远选到快照任务 → 只补今天 →
面板仍显示滞后 → 明天再进清单，**死循环**。不加入该列表后，按钮行为与现在完全
一致（只补今天），历史缺口靠手动跑回补。

### 限速与容错

- 复用 `tasks/sector_derivatives.py:71` 的 `_retry`（指数退避 3 次）
- `SourceClient` 的 eastmoney policy 自带 `min_interval_seconds=0.8` 限速
- 单个概念失败 → 跳过并计数，**不中断**其余（回补是尽力而为）
- 某一天全部概念都失败 → 跳过该天，计入 `failed_days`

### 状态语义

```python
无缺口                 → success,  saved=0
全部补上               → success,  saved=N
部分补上               → degraded，列出未补的日子
回补源整体不可用       → retained + error_kind=network   ← safe_task 会在 30s 后重试
```

`retained + error_kind=network` 的写法与 2026-09-30 已合入的
`tasks/market_valuation.py` 一致；`core/runner.py:139-142` 只在
`error_kind == NETWORK` 时触发网络重试。

**`degraded` 与 `retained` 的分界**：按「成功补上几天」判定，不按「失败几天」。

- 至少补上 1 天 → `degraded`（有进展，未补足的日子列入 `failed_days`）
- **一天都没补上**（且窗口非空）→ `retained` + `error_kind=network`

后者意味着源整体不可用，交给 `safe_task` 重试；前者是部分进展，重试意义不大，
但仍需可见（不能静默成 `success`——那正是 P2-18 记述的 retained 静默退化的
反面：数据没补齐却报成功）。

### 字段映射

`stock_board_concept_hist_em` 的列名按 `stock_board_industry_hist_em` 的既有
映射推定（同一 API 家族，`tasks/sector_derivatives.py:164-174` 已在生产验证）：

```
日期 → trade_date    开盘 → open      收盘 → close
最高 → high          最低 → low       成交量 → volume
成交额 → amount      涨跌幅 → pct_change
```

**待验证项**：东财线路自 2026-09-29 起在分钟级抖动，该接口的真实列名尚未实测。
映射以 `available = [c for c in keep if c in df.columns]` 的方式声明，列名不符
时降级为「只写能识别的列」并 WARNING，不抛异常。线路恢复后需实测确认并回填本文档。

`up_count`/`down_count` 写 `None`：`CONCEPT_BOARD_CONTRACT`
（`core/data_contract.py:332`）中两列均可空；`ConceptBoardRefreshAdapter`
（`core/refresh_adapters.py:2044`）只把列名列进 `_CONCEPT_BOARD_COLUMNS` 做分区
替换，不做任何计算。已核实 TUI / `scripts/` / 兄弟仓库 `quant_hunter` 均无消费者。

## 测试策略

全部 hermetic，不打网络（仓库既有约定：既有用例显式 patch fetcher）。

| 用例 | 钉住的行为 |
|---|---|
| 缺口判定 × 4 | 无缺口 / 全缺 / 含已登记日 / 窗口截断 |
| **写入上界** | 回补结果里**不含** `trade_date >= expected` 的行 —— 防止覆盖快照的那道闸 |
| 字段映射 | mock akshare 返回，断言 `data_source == "em_hist"`、`up_count is None` |
| 状态映射 × 4 | `success` / `degraded` / `retained+network` / 无缺口零请求 |
| 列名降级 | 源返回未知列名时只写可识别列且 WARNING，不抛 |
| 幂等 | 已补上的日子被跳过（不重复写） |

**红证**：还原「写入到 `expected`」会让写入上界用例红；还原「窗口含
`expected`」会让缺口判定用例红；去掉 `error_kind=network` 会让状态映射用例红。

## 明确不做

- **不改** `update_concept_board` 的现有行为
- **不改** `core/backfill.py` 的三组分类与 `tests/test_backfill.py` 门禁
- **不修** 23 天历史欠账本身 —— 本设计只提供还债的工具（`--lookback 60`），
  是否执行、何时执行由运维决定。11592 次请求约 1 小时，必须手动跑
- **不引入** 同花顺概念源（见「为什么不是同花顺兜底」）
- 概念板块 2026-09-29 的空洞：快照接口不留历史，已不可恢复

## 待验证清单

1. `stock_board_concept_hist_em` 的真实列名与 `pct_change` 是否直给
   （推测与 `stock_board_industry_hist_em` 同构，需实测）
2. 504 个概念全量循环的实测耗时与 WAF 耐受度
3. `providers.py:1975` 的 `data_source` 默认值仍是 `"ths"`（历史遗留），
   本设计不改它——回补行显式传 `"em_hist"`，不受默认值影响
