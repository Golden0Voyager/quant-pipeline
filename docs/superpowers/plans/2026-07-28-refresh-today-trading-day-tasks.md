# Refresh Today Trading-Day Tasks Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 新增安全的 `--refresh-today` 收盘刷新模式，在不改变 `--force` 语义的前提下重新抓取、校验并覆盖全部 29 个交易日任务。

**Architecture:** 以任务注册表中的显式 `RefreshPolicy` 驱动独立 `RefreshOrchestrator`。任务适配器负责抓取和规范化，`SQLiteRefreshStore` 负责 staging、校验后事务替换；抓取或校验失败时不触碰正式表旧数据。

**Tech Stack:** Python 3.12、SQLite、pandas、pytest、Textual、Ruff、mypy、uv

## Global Constraints

- 所有 shell 命令使用 `rtk`；Python 与测试使用 `rtk uv run python ...`。
- 不改变普通 `--task`、`--task all`、`--resume` 或 `--force` 的现有语义。
- `--refresh-today` 默认仅在北京时间 16:00 后运行；提前运行必须同时提供 `--force`。
- AkShare 保持首选；纯 yfinance 数据禁止写入 `quant_core.db`。
- 获取或校验失败时保留旧数据；不得先删除正式表再请求网络。
- 目标日期整体替换必须通过 staging 和同一 SQLite 事务完成。
- Git 一文件一提交，提交信息英文在前、中文在后。
- 不修改 `/Users/hainingyu/Code/quant_hunter`。
- `update_chip_distribution_em_fullmarket` 是 `ON_DEMAND`，不纳入收盘刷新。

---

## File Map

- `core/task_registry.py`：刷新策略、日期策略及 29 项声明。
- `core/refresh.py`：刷新上下文、依赖计划、编排和结果聚合。
- `core/refresh_store.py`：SQLite staging、事务替换与审计。
- `core/refresh_adapters.py`：适配器协议、注册和通用远端任务包装。
- `core/refresh_audit.py`：任务级与全局收盘质量审计。
- `migrations/010_refresh_runs.py`：刷新运行记录。
- `migrations/011_refresh_event_keys.py`：事件型稳定键与去重迁移。
- `daily_pipeline.py`：CLI 顶层模式、锁和退出码。
- `providers.py`：创建无缓存 loader，不扩展外部 DatabaseManager。
- `tasks/*.py`：仅增加任务特有的收盘抓取/规范化函数。
- `tui.py`：收盘刷新确认、启动和状态展示。
- `tests/test_refresh*.py`：纯编排、SQLite 安全和端到端验收。
- 现有各任务测试文件：任务适配器的 RED/GREEN 回归。

## Required Policy Matrix

| Task | Refresh kind | Date strategy |
|---|---|---|
| `update_bars` | `REMOTE_DATE_SNAPSHOT` | `EXACT_TARGET` |
| `update_indicators` | `DERIVED_RECOMPUTE` | `EXACT_TARGET` |
| `update_fundamentals` | `REMOTE_DATE_SNAPSHOT` | `EXACT_TARGET` |
| `update_market_snapshot` | `REMOTE_DATE_SNAPSHOT` | `EXACT_TARGET` |
| `update_fund_flow` | `REMOTE_DATE_SNAPSHOT` | `EXACT_TARGET` |
| `update_margin_trading` | `REMOTE_DATE_SNAPSHOT` | `LATEST_AVAILABLE_WITHIN_LOOKBACK` |
| `update_dragon_tiger` | `REMOTE_DATE_SNAPSHOT` | `EXACT_TARGET` |
| `update_block_trade` | `REMOTE_DATE_SNAPSHOT` | `EXACT_TARGET` |
| `update_sector_fund_flow` | `REMOTE_RUN_SNAPSHOT` | `RUN_SNAPSHOT` |
| `update_historical_valuation` | `DERIVED_RECOMPUTE` | `EXACT_TARGET` |
| `update_sector_industry` | `DERIVED_RECOMPUTE` | `EXACT_TARGET` |
| `update_north_flow` | `REMOTE_DATE_SNAPSHOT` | `EXACT_TARGET` |
| `update_south_flow` | `REMOTE_DATE_SNAPSHOT` | `LATEST_AVAILABLE_WITHIN_LOOKBACK` |
| `update_ah_premium` | `REMOTE_RUN_SNAPSHOT` | `RUN_SNAPSHOT` |
| `update_etf_daily` | `REMOTE_DATE_SNAPSHOT` | `EXACT_TARGET` |
| `update_index_daily` | `REMOTE_DATE_SNAPSHOT` | `LATEST_AVAILABLE_WITHIN_LOOKBACK` |
| `update_cb_quotation` | `REMOTE_RUN_SNAPSHOT` | `RUN_SNAPSHOT` |
| `update_cb_redeem` | `REMOTE_RUN_SNAPSHOT` | `RUN_SNAPSHOT` |
| `update_cb_index` | `REMOTE_DATE_SNAPSHOT` | `LATEST_AVAILABLE_WITHIN_LOOKBACK` |
| `update_limit_up_down` | `REMOTE_DATE_SNAPSHOT` | `EXACT_TARGET` |
| `update_concept_board` | `REMOTE_RUN_SNAPSHOT` | `RUN_SNAPSHOT` |
| `update_market_valuation` | `REMOTE_DATE_SNAPSHOT` | `LATEST_AVAILABLE_WITHIN_LOOKBACK` |
| `update_sector_derivatives` | `COMPOSITE_ATOMIC` | `EXACT_TARGET` |
| `update_option_sentiment` | `REMOTE_DATE_SNAPSHOT` | `EXACT_TARGET` |
| `update_stock_repurchase` | `REMOTE_KEYED_UPSERT` | `RUN_SNAPSHOT` |
| `update_institution_survey` | `REMOTE_KEYED_UPSERT` | `LATEST_AVAILABLE_WITHIN_LOOKBACK` |
| `update_stock_pledge` | `REMOTE_KEYED_UPSERT` | `LATEST_AVAILABLE_WITHIN_LOOKBACK` |
| `update_chip_distribution` | `DERIVED_RECOMPUTE` | `EXACT_TARGET` |
| `update_chip_distribution_em` | `DERIVED_RECOMPUTE` | `EXACT_TARGET` |

### Task 1: Declare Complete Refresh Policies

**Files:**

- Modify: `core/task_registry.py`
- Modify: `tests/test_task_registry.py`

**Interfaces:**

- Produces: `RefreshKind`, `DateStrategy`, `RefreshPolicy`
- Produces: `refreshable_trading_tasks() -> tuple[TaskSpec, ...]`
- Produces: `TaskSpec.refresh_policy: RefreshPolicy | None`

- [ ] **Step 1: Add failing registry contract tests**

Add assertions equivalent to:

```python
def test_all_trading_day_tasks_have_refresh_policy():
    specs = tuple(s for s in TASK_REGISTRY if s.cadence is Cadence.TRADING_DAY)
    assert len(specs) == 29
    assert all(s.refresh_policy is not None for s in specs)
    assert {s.name for s in refreshable_trading_tasks()} == {s.name for s in specs}


def test_non_trading_day_tasks_are_not_close_refreshable():
    assert all(
        s.refresh_policy is None
        for s in TASK_REGISTRY
        if s.cadence is not Cadence.TRADING_DAY
    )


def test_sector_derivatives_declares_all_written_tables():
    spec = lookup_task("update_sector_derivatives")
    assert spec.tables == (
        "sector_daily",
        "sector_valuation",
        "index_futures_basis",
    )
```

Also verify every dependency names an existing task, policy tables are declared by
the spec, and `update_market_snapshot` depends on `update_fundamentals`.

- [ ] **Step 2: Verify RED**

Run:

```bash
rtk uv run python -m pytest -q tests/test_task_registry.py
```

Expected: FAIL because refresh metadata does not exist and sector derivatives
omits `index_futures_basis`.

- [ ] **Step 3: Add policy value types**

Implement:

```python
class RefreshKind(StrEnum):
    REMOTE_DATE_SNAPSHOT = "remote_date_snapshot"
    REMOTE_KEYED_UPSERT = "remote_keyed_upsert"
    DERIVED_RECOMPUTE = "derived_recompute"
    COMPOSITE_ATOMIC = "composite_atomic"
    REMOTE_RUN_SNAPSHOT = "remote_run_snapshot"


class DateStrategy(StrEnum):
    EXACT_TARGET = "exact_target"
    LATEST_AVAILABLE_WITHIN_LOOKBACK = "latest_available_within_lookback"
    RUN_SNAPSHOT = "run_snapshot"


@dataclass(frozen=True)
class RefreshPolicy:
    kind: RefreshKind
    date_strategy: DateStrategy
    natural_keys: Mapping[str, tuple[str, ...]]
    required_fields: Mapping[str, tuple[str, ...]]
    dependencies: tuple[str, ...] = ()
    supports_symbols: bool = False
    minimum_coverage: float | None = None
    cache_namespace: str | None = None
    lookback_days: int = 0
```

Add an explicit policy to every trading-day `TaskSpec`; do not infer policies
from table names. Use the design specification's five strategy classes and set:

- `update_margin_trading`, `update_south_flow`, `update_index_daily`,
  `update_market_valuation`, `update_cb_index` to
  `LATEST_AVAILABLE_WITHIN_LOOKBACK`;
- `update_sector_fund_flow`, `update_ah_premium`, `update_cb_quotation`,
  `update_cb_redeem`, `update_concept_board` to `RUN_SNAPSHOT`;
- indicators, historical valuation, sector industry, and both chip tasks to
  `DERIVED_RECOMPUTE`;
- sector derivatives to `COMPOSITE_ATOMIC`;
- event tasks to `REMOTE_KEYED_UPSERT`;
- all remaining dated snapshots to `REMOTE_DATE_SNAPSHOT`.

- [ ] **Step 4: Verify GREEN and lint**

```bash
rtk uv run python -m pytest -q tests/test_task_registry.py
rtk uv run ruff check core/task_registry.py tests/test_task_registry.py
```

- [ ] **Step 5: Commit each file separately**

Commit `tests/test_task_registry.py`, then `core/task_registry.py`, using bilingual
messages.

### Task 2: Add Atomic SQLite Refresh Store

**Files:**

- Create: `core/refresh_store.py`
- Create: `tests/test_refresh_store.py`

**Interfaces:**

- Produces: `DateSnapshotReplacement`, `KeyedUpsertReplacement`,
  `RunSnapshotReplacement`, `CompositeReplacement`, `ReplaceResult`
- Produces: `SQLiteRefreshStore.replace_date_snapshot(...)`
- Produces: `SQLiteRefreshStore.upsert_keyed_snapshot(...)`
- Produces: `SQLiteRefreshStore.replace_run_snapshot(...)`
- Produces: `SQLiteRefreshStore.replace_composite(...)`

- [ ] **Step 1: Write failing safety tests**

Use temporary SQLite tables to cover:

```python
def test_date_snapshot_replaces_complete_partition(store):
    result = store.replace_date_snapshot(request_with_two_valid_rows)
    assert result.replaced == 2
    assert select_target_rows() == expected_close_rows
    assert select_historical_rows() == original_history


def test_validation_failure_preserves_old_partition(store):
    with pytest.raises(RefreshValidationError):
        store.replace_date_snapshot(request_with_duplicate_keys)
    assert select_target_rows() == original_intraday_rows


def test_composite_failure_rolls_back_every_table(store):
    with pytest.raises(sqlite3.Error):
        store.replace_composite(request_with_second_table_failure)
    assert select_sector_daily() == original_sector_rows
    assert select_sector_valuation() == original_valuation_rows
```

Also cover keyed UPSERT without date deletion, run-snapshot replacement, SQL
identifier rejection, required-field validation, minimum coverage, and
transaction rollback.

- [ ] **Step 2: Verify RED**

```bash
rtk uv run python -m pytest -q tests/test_refresh_store.py
```

Expected: import failure because `core.refresh_store` does not exist.

- [ ] **Step 3: Implement the narrow store**

Use frozen request dataclasses containing table, fixed columns, validated rows,
natural keys, date predicate and policy thresholds. Validate identifiers with:

```python
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
```

For date replacement:

1. open a fresh connection with `PRAGMA foreign_keys=ON`;
2. create a unique TEMP staging table from the formal table schema;
3. insert and validate staging rows;
4. begin one immediate transaction;
5. delete only the accepted partition;
6. insert staging rows into the formal table;
7. commit or roll back.

Never accept a raw SQL predicate from adapters. Requests carry a date column and
date value, and the store quotes validated identifiers itself.

- [ ] **Step 4: Verify GREEN**

```bash
rtk uv run python -m pytest -q tests/test_refresh_store.py
rtk uv run ruff check core/refresh_store.py tests/test_refresh_store.py
```

- [ ] **Step 5: Commit one file at a time**

Commit the test file, then the implementation file.

### Task 3: Persist Refresh Runs

**Files:**

- Create: `migrations/010_refresh_runs.py`
- Modify: `tests/test_migrations.py`
- Modify: `core/refresh_store.py`
- Modify: `tests/test_refresh_store.py`

**Interfaces:**

- Produces tables `refresh_runs` and `refresh_task_runs`
- Produces store methods `start_run`, `record_task_result`, `finish_run`

- [ ] **Step 1: Add migration RED tests**

Assert fresh creation, upgrade, idempotence, indexes, and rollback. Required
columns:

```text
refresh_runs:
run_id, target_date, started_at, finished_at, status, symbols_json

refresh_task_runs:
run_id, task_name, policy_kind, requested_date, as_of_date, status,
fetched, validated, replaced, retained, failed, metadata_json
```

Primary key for task rows is `(run_id, task_name)`.

- [ ] **Step 2: Verify RED**

```bash
rtk uv run python -m pytest -q tests/test_migrations.py -k refresh
```

- [ ] **Step 3: Implement migration and store persistence**

Migration `apply(conn)` must use `CREATE TABLE IF NOT EXISTS`, create indexes on
`target_date` and task status, and preserve existing ingestion tables.

Store methods must use parameter binding and JSON serialization with stable
keys. A failed write must not change business-table transactions.

- [ ] **Step 4: Verify GREEN**

```bash
rtk uv run python -m pytest -q tests/test_migrations.py tests/test_refresh_store.py
rtk uv run ruff check migrations/010_refresh_runs.py core/refresh_store.py
```

- [ ] **Step 5: Commit changed files individually**

### Task 4: Build the Refresh Orchestrator and Audit

**Files:**

- Create: `core/refresh.py`
- Create: `core/refresh_audit.py`
- Create: `tests/test_refresh.py`
- Create: `tests/test_refresh_audit.py`

**Interfaces:**

- Produces: `RefreshContext`
- Produces: `RefreshAdapter` protocol and `RefreshAdapterResult`
- Produces: `RefreshOrchestrator.run(context) -> TaskResult`
- Produces: `RefreshAudit.validate_task(...)`

- [ ] **Step 1: Write orchestration RED tests**

Cover Shanghai time gates, deterministic target date, topological order,
shared-table serialization, failed dependency blocking, independent-task
continuation, symbols filtering, one retry, and final degraded exit semantics.

Use:

```python
@dataclass(frozen=True)
class RefreshContext:
    target_date: str
    started_at: datetime
    run_id: str
    symbols: tuple[str, ...] | None = None
    bypass_cache: Literal[True] = True
    refresh_mode: Literal["close_refresh"] = "close_refresh"
```

Pre-close behavior must use `ZoneInfo("Asia/Shanghai")`.

- [ ] **Step 2: Write audit RED tests**

Cover exact-target, lookback as-of, run snapshot, duplicate natural keys,
required fields, OHLC invariants, nonnegative volume/amount, coverage thresholds,
and dead-source metadata.

- [ ] **Step 3: Verify RED**

```bash
rtk uv run python -m pytest -q tests/test_refresh.py tests/test_refresh_audit.py
```

- [ ] **Step 4: Implement orchestration and audit**

The orchestrator receives adapter and store mappings by dependency injection.
It must not import task modules dynamically or execute network code in tests.
Adapters return:

```python
@dataclass(frozen=True)
class RefreshAdapterResult:
    task_name: str
    as_of_date: str | None
    fetched: int
    validated: int
    replaced: int
    retained: int
    failed_symbols: tuple[str, ...]
    changed_symbols: tuple[str, ...]
    metadata: Mapping[str, Any]
```

Blocked tasks use `TaskResult.failed(..., metadata={"blocked_by": (...)})`
without adding a new global `TaskStatus`.

- [ ] **Step 5: Verify and commit each file**

```bash
rtk uv run python -m pytest -q tests/test_refresh.py tests/test_refresh_audit.py
rtk uv run ruff check core/refresh.py core/refresh_audit.py tests/test_refresh.py tests/test_refresh_audit.py
```

### Task 5: Add CLI Mode and Uncached Provider Construction

**Files:**

- Modify: `daily_pipeline.py`
- Modify: `providers.py`
- Modify: `tests/test_daily_pipeline.py`
- Modify: `tests/test_providers.py`

**Interfaces:**

- Produces CLI `--refresh-today`
- Produces `ProviderFactory.get_loader(use_cache=False)`
- Produces `run_close_refresh(...) -> TaskResult`

- [ ] **Step 1: Add parser and dispatch RED tests**

Test:

- no flags still runs legacy `all`;
- explicit `--task` conflicts with `--refresh-today`;
- `--resume` conflicts with refresh;
- post-16:00 refresh dispatches the orchestrator;
- pre-16:00 fails unless `--force`;
- refresh acquires the global pipeline write lock;
- degraded/failed/aborted results exit nonzero.

- [ ] **Step 2: Add provider RED tests**

Assert refresh construction passes `use_cache=False` to the local loader adapter
without changing the normal cached singleton.

- [ ] **Step 3: Implement minimal entry point**

Change `--task` default to `None`, then resolve no explicit top-level mode to
`all`. Do not call `run_all()` from refresh mode. Construct `RefreshContext`
with the Shanghai clock and `get_expected_latest_trading_day()`.

- [ ] **Step 4: Verify**

```bash
rtk uv run python -m pytest -q tests/test_daily_pipeline.py tests/test_providers.py
rtk uv run ruff check daily_pipeline.py providers.py
```

- [ ] **Step 5: Commit each file separately**

### Task 6: Implement Core Remote Adapters

**Files:**

- Create: `core/refresh_adapters.py`
- Modify: `tasks/bars.py`
- Modify: `tasks/valuation_chain.py`
- Modify: `tasks/market_flow.py`
- Create: `tests/test_refresh_adapters.py`
- Modify: `tests/test_bars_integration.py`
- Modify: `tests/test_valuation_chain_integration.py`
- Modify: `tests/test_market_flow.py`

**Interfaces:**

- Produces adapters for bars, fundamentals, market snapshot and fund flow
- Produces bars metadata `changed_symbols`, `failed_symbols`

- [ ] **Step 1: Add bars refresh RED tests**

Verify refresh bypasses both latest-date skip paths and cache, fetches only the
target date, rejects yfinance/invalid OHLC, stages valid rows, preserves old rows
for failed symbols, and returns changed symbols.

- [ ] **Step 2: Add shared fundamentals RED tests**

Verify fundamentals ignores the 5000-row threshold while preserving existing
`dividend_yield`; market snapshot overwrites a non-null intraday
`dividend_yield` but never changes PE/PB/PEG; insufficient quote coverage
preserves all prior values.

- [ ] **Step 3: Add fund-flow RED tests**

Resolve the `symbol/date` versus `ts_code/trade_date` boundary in the adapter.
Verify full-empty numeric rows are rejected and low coverage preserves the old
partition.

- [ ] **Step 4: Implement core remote adapters**

Keep legacy task functions unchanged. Add task-specific fetch/normalization
helpers that return rows without committing. Register adapters explicitly in
`core/refresh_adapters.py`; do not infer function names.

- [ ] **Step 5: Verify**

```bash
rtk uv run python -m pytest -q \
  tests/test_refresh_adapters.py \
  tests/test_bars_integration.py \
  tests/test_valuation_chain_integration.py \
  tests/test_market_flow.py
rtk uv run ruff check core/refresh_adapters.py tasks/bars.py tasks/valuation_chain.py tasks/market_flow.py
```

- [ ] **Step 6: Commit each file separately**

### Task 7: Implement Derived Adapters

**Files:**

- Modify: `tasks/core_chain.py`
- Modify: `tasks/index_chain.py`
- Modify: `tasks/valuation_chain.py`
- Modify: `core/refresh_adapters.py`
- Modify: `tests/test_core_chain_integration.py`
- Modify: `tests/test_daily_pipeline.py`
- Modify: `tests/test_new_tasks.py`

**Interfaces:**

- Produces adapters for indicators, historical valuation, sector industry,
  local chips and EM chips

- [ ] **Step 1: Add target-date-only RED tests**

For every derived task, calculate from required history but submit only
`context.target_date`. Verify it receives only upstream
`changed_symbols`, never rewrites history, and marks symbols blocked when bars
failed.

- [ ] **Step 2: Add EM abort propagation test**

Verify nine consecutive failures return `aborted`, include all unprocessed
symbols in the failure queue, and do not erase old chip rows.

- [ ] **Step 3: Implement derived adapters**

Use explicit target symbol sets; never call EM's random/default target selector
in refresh mode. Preserve the existing hard circuit breaker and the user's
uncommitted `tasks/index_chain.py` change when later integrating branches.

- [ ] **Step 4: Verify and commit each file**

```bash
rtk uv run python -m pytest -q \
  tests/test_core_chain_integration.py \
  tests/test_new_tasks.py \
  tests/test_daily_pipeline.py
```

### Task 8: Repair Event Keys Before Extended Refresh

**Files:**

- Create: `migrations/011_refresh_event_keys.py`
- Modify: `tests/test_migrations.py`
- Modify: `tasks/market_flow.py`
- Modify: `tasks/stock_pledge.py`
- Modify: `tests/test_market_flow.py`
- Modify: `tests/test_phase1_tasks.py`

**Interfaces:**

- Produces stable source keys for dragon tiger, block trades and stock pledge

- [ ] **Step 1: Add collision RED tests**

Create two legal same-stock same-day block trades and two separate dragon-tiger
reasons; assert both survive. Insert pledge rows with null pledger twice; assert
the stable source key prevents duplicates.

- [ ] **Step 2: Implement deterministic keys and migration**

Use source IDs when available; otherwise hash normalized source fields. Backfill
keys, quarantine irreconcilable duplicates, create unique indexes, and keep the
migration idempotent.

- [ ] **Step 3: Verify**

```bash
rtk uv run python -m pytest -q \
  tests/test_migrations.py \
  tests/test_market_flow.py \
  tests/test_phase1_tasks.py
```

- [ ] **Step 4: Commit each file separately**

### Task 9: Implement Remaining Trading-Day Adapters

**Files:**

- Modify: `core/refresh_adapters.py`
- Modify: `tasks/market_flow.py`
- Modify: `tasks/macro.py`
- Modify: `tasks/finance_flow.py`
- Modify: `tasks/index_chain.py`
- Modify: `tasks/market_valuation.py`
- Modify: `tasks/sector_derivatives.py`
- Modify: `tasks/option_sentiment.py`
- Modify: `tasks/convertible_bond.py`
- Modify: `tasks/concept_board.py`
- Modify: `tasks/stock_repurchase.py`
- Modify: `tasks/institution_survey.py`
- Modify: `tasks/stock_pledge.py`
- Modify: `tests/test_market_flow.py`
- Modify: `tests/test_daily_pipeline.py`
- Modify: `tests/test_new_tasks.py`
- Modify: `tests/test_phase1_tasks.py`
- Modify: `tests/test_column_fixes.py`

**Interfaces:**

- Produces adapters for all remaining policies so registry coverage and runtime
  adapter coverage both equal 29

- [ ] **Step 1: Add runtime adapter coverage test**

```python
def test_every_refresh_policy_has_runtime_adapter():
    assert set(REFRESH_ADAPTERS) == {
        spec.name for spec in refreshable_trading_tasks()
    }
```

- [ ] **Step 2: Add strategy-specific tests**

Cover:

- margin/South/index/market valuation/CB index accepted lookback dates;
- empty dragon-tiger and limit pools distinguished from source failure;
- sector fund flow, AH, CB quotation/redeem and concept board as run snapshots;
- north flow returns `degraded` with `dead_source` and preserves old data;
- sector derivatives replaces all three tables atomically;
- event adapters perform keyed UPSERT without target-date deletion;
- historical-returning sources submit only the accepted target/as-of partition.

- [ ] **Step 3: Implement adapters by strategy group**

Add no generic fallback that calls a legacy task and treats `saved > 0` as
refresh success. Each adapter must return normalized rows and an explicit
`as_of_date` or `run_id`.

- [ ] **Step 4: Run related tests and commit each file**

```bash
rtk uv run python -m pytest -q \
  tests/test_market_flow.py \
  tests/test_new_tasks.py \
  tests/test_phase1_tasks.py \
  tests/test_column_fixes.py
rtk uv run ruff check core tasks tests
```

### Task 10: Add Cross-Source Sampling and Final Audit

**Files:**

- Modify: `core/refresh_audit.py`
- Modify: `core/refresh.py`
- Modify: `tests/test_refresh_audit.py`
- Modify: `tests/test_refresh.py`

**Interfaces:**

- Produces deterministic stratified sample selection
- Produces one targeted retry for mismatched symbols

- [ ] **Step 1: Add sampling RED tests**

Use a fixed seed derived from target date and test approximately 30 supported
symbols across Shanghai, Shenzhen, ChiNext, STAR and Beijing. Verify unsupported
backup-source markets are excluded explicitly.

- [ ] **Step 2: Add tolerance and retry RED tests**

Price and volume use separate configured tolerances. Only mismatched symbols
retry once; retry failure produces degraded status and does not trigger a
full-market delete or rerun.

- [ ] **Step 3: Implement and verify**

```bash
rtk uv run python -m pytest -q tests/test_refresh.py tests/test_refresh_audit.py
```

- [ ] **Step 4: Commit each file**

### Task 11: Add TUI Close Refresh

**Files:**

- Modify: `tui.py`
- Modify: `tests/test_tui.py`

**Interfaces:**

- Produces action `action_refresh_today`
- Launches exactly `daily_pipeline.py --refresh-today`

- [ ] **Step 1: Add TUI RED tests**

Verify confirmation displays Shanghai target date, 29 tasks and optional symbol
scope; acceptance launches the exact command; cancellation launches nothing;
structured task states distinguish retained old data from committed replacement.

- [ ] **Step 2: Implement TUI action**

Add a distinct binding/button and confirmation screen. Do not reuse ordinary
full-update delayed scheduling because the CLI owns the 16:00 safety gate.

- [ ] **Step 3: Verify and commit**

```bash
rtk uv run python -m pytest -q tests/test_tui.py
rtk uv run ruff check tui.py tests/test_tui.py
```

### Task 12: End-to-End Acceptance and Documentation

**Files:**

- Create: `tests/test_refresh_integration.py`
- Modify: `README.md`
- Modify: `docs/AGENTS.md`

**Interfaces:**

- Produces production runbook and end-to-end temporary-database proof

- [ ] **Step 1: Add end-to-end acceptance tests**

Preload a temporary database with intraday target-date data and historical rows.
Run fake adapters through the real orchestrator/store, then assert:

- close rows replace intraday rows;
- historical dates are byte-for-byte unchanged;
- source or validation failure preserves old data;
- derived tasks receive only changed symbols;
- composite tasks roll back together;
- all 29 tasks produce persisted results;
- final degraded status is nonzero.

- [ ] **Step 2: Document operator workflow**

Document:

```bash
rtk uv run python daily_pipeline.py --refresh-today
rtk uv run python daily_pipeline.py --refresh-today --symbols 000001.SZ,600000.SH
```

Explain the 16:00 Shanghai gate, `--force` override, retained-old-data states,
run records, failure queue and rollback behavior.

- [ ] **Step 3: Run complete gates**

```bash
rtk uv run ruff check .
rtk uv run mypy .
rtk uv run python -m pytest -q
rtk git diff --check
```

If mypy still fails only because the repository maps files under both
`quant_pipeline.*` and top-level module names, record that existing nonblocking
CI debt separately; do not report mypy as passing.

- [ ] **Step 4: Check side effects**

Confirm no repo-root `MagicMock*`, `.DS_Store`, production DB writes, live locks,
or new network fixtures exist.

- [ ] **Step 5: Commit each documentation/test file separately and request final review**
