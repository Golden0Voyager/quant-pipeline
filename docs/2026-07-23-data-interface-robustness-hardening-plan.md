# Data Interface Robustness Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 修复当前七类数据正确性与接口鲁棒性问题，使每次抓取都能明确区分成功、合法无数据、降级、失败和中止，并保证写入数据具备可验证的字段完整性、时间点一致性和可回滚迁移路径。

**Architecture:** 将目前散落在 `tasks/*.py` 中的传输、字段判断、任务状态和健康检查拆成四个边界清晰的模块：数据源客户端、数据契约、任务结果、任务注册表。SQLite 使用有版本号的迁移作为唯一结构演进入口；抓取先经过契约验证，再在事务中写正式表，同时记录 `ingestion_runs` 审计信息。季度财务、概念成员和指数成分改为 point-in-time 模型，避免季度停更和回测穿越。

**Tech Stack:** Python 3.12、AkShare、pandas、requests/curl_cffi、SQLite、pytest、Ruff、Textual。

## Global Constraints

- 所有命令必须使用 `rtk` 前缀。
- Python 命令统一通过 `rtk uv run python ...` 执行，不使用系统 Python 或 pip。
- 默认生产库为 `/Users/hainingyu/Code/quant_data/quant_core.db`。
- 生产迁移前必须使用 SQLite Backup API 生成一致性备份并执行 `PRAGMA quick_check`。
- 任何数据源异常、字段漂移或保存异常都不得仅返回 `{"saved": 0}`。
- 数据契约不满足时不得写入正式业务表；原始响应摘要和拒绝原因写入审计表。
- 不恢复 yfinance 写入回退；备用源必须在任务注册表中显式声明。
- 新增行为先写失败测试，再写实现；每个任务完成后独立提交。
- 不在单次生产发布中同时执行结构迁移、全量历史回填和全部日常任务。
- 现有 `/Users/hainingyu/Code/quant_data/backups/quant_core_20260723_092630.db` 仅作为历史备份；正式实施时必须再创建当次备份。

---

## 1. Scope And Priority

本方案将上一轮七个问题规范为以下优先级：

| ID | 优先级 | 问题 | 完成标准 |
|---|---|---|---|
| R1 | P0 | 季度财务按股票存在性跳过，无法持续更新 | 新报告期自动增量；历史版本按 `publish_date` 可查询 |
| R2 | P0 | `saved=0`、接口移除、字段为空可能被调度器视为成功 | 所有任务返回统一状态；进程退出码与状态一致 |
| R3 | P0 | 生产库存在空壳、重复数据，DDL 与迁移分散 | 可重复迁移完成清理、约束重建和回滚验证 |
| R4 | P1 | 东财等接口的重试、限速、熔断和回退分散 | 同一主机共享策略；错误分类和降级源可观测 |
| R5 | P1 | 字段漂移只告警，仍可能写入半空记录 | 必需字段、完整率、值域和唯一性校验阻断坏写入 |
| R6 | P1 | 健康检查只覆盖早期表且以行数为主 | 所有注册表均有新鲜度、覆盖率、重复率和完整率检查 |
| R7 | P2 | 概念成员和指数成分只保存当前状态，导致回测穿越 | 按生效区间保存历史，支持任意交易日的 as-of 查询 |

### Out Of Scope

- 不在本轮引入分钟线、逐笔成交或新闻全文。
- 不更换 SQLite。
- 不重写全部现有任务；先迁移七类问题涉及的任务，再逐步纳入其他任务。
- 不依赖付费数据源；如未来引入 Tushare/JQData，复用本方案的适配器和契约接口。

## 2. Target Data Flow

```text
TaskSpec
  |
  v
SourceClient.fetch()
  |-- timeout / retry / rate limit / circuit breaker
  |-- primary source -> declared fallback source
  v
RawFrame + FetchMetadata
  |
  v
DataContract.validate()
  |-- required columns
  |-- normalization
  |-- completeness / value range / uniqueness
  |-- rejected rows + schema fingerprint
  v
ValidatedBatch
  |
  +--> ingestion_runs / ingestion_rejections
  |
  v
Database writer transaction
  |
  v
TaskResult(status, counts, dates, source, errors)
  |
  +--> daily_pipeline exit code
  +--> TUI
  +--> health_check
```

## 3. File Map

### New Files

- `core/task_result.py`: 统一任务状态、错误分类和序列化。
- `core/source_client.py`: 主机级会话、限速、重试、熔断和回退。
- `core/data_contract.py`: DataFrame/记录级契约与验证结果。
- `core/task_registry.py`: 任务、表、频率、空数据策略和数据源声明。
- `core/migrations.py`: 迁移发现、版本记录、事务执行和校验。
- `migrations/001_ingestion_audit.sql`: 审计表。
- `migrations/002_phase2_data_cleanup.py`: 空壳清理和 Phase 2 表重建。
- `migrations/003_point_in_time_tables.sql`: 财务版本、概念成员、指数成分历史表。
- `tasks/financial_history.py`: 报告期发现、批量财务抓取、披露日期合并和历史写入。
- `tasks/index_membership.py`: 指数成分与权重历史快照。
- `scripts/backup_database.py`: 使用 SQLite Backup API 创建、验证备份。
- `scripts/migrate_database.py`: 显式执行迁移并输出版本、校验和与验证结果。
- `scripts/audit_data_contracts.py`: 只读全库契约审计，支持 JSON 报告和退出码。
- `docs/runbooks/data-pipeline-production-migration.md`: 生产迁移、观察和回滚操作手册。
- `tests/test_backup_database.py`
- `tests/test_task_result.py`
- `tests/test_source_client.py`
- `tests/test_data_contract.py`
- `tests/test_migrations.py`
- `tests/test_financial_history.py`
- `tests/test_task_registry.py`
- `tests/test_data_audit.py`
- `tests/test_membership_history.py`
- `tests/test_pipeline_resilience.py`
- `tests/fixtures/contracts/`: 当前 AkShare 返回列的脱敏小样本。

### Modified Files

- `core/runner.py`: 只接受 `TaskResult` 或兼容转换后的统一字典。
- `core/utils.py`: 移除仅告警式质量检查的核心职责，保留通用工具。
- `daily_pipeline.py`: 由任务注册表驱动执行和退出码。
- `interface.py`: 增加审计、迁移和 point-in-time 写入接口。
- `providers.py`: 调用迁移器；写入失败抛出类型化异常；删除 Phase 2 临时迁移逻辑。
- `tasks/market_valuation.py`
- `tasks/concept_board.py`
- `tasks/insider_trading.py`
- `tasks/institution_survey.py`
- `tasks/stock_pledge.py`
- `tasks/stock_repurchase.py`
- `tasks/option_sentiment.py`
- `tasks/financials.py`
- `tasks/index_chain.py`
- `tasks/utility.py`
- `tui.py`
- `tests/conftest.py`
- `tests/test_core_infrastructure.py`
- `tests/test_daily_pipeline.py`
- `tests/test_phase1_tasks.py`
- `tests/test_new_tasks.py`
- `/Users/hainingyu/Code/quant_hunter/src/smartmoney_hunter/database.py`: 去除与 pipeline-owned 表冲突的演进逻辑，保留兼容创建。
- `/Users/hainingyu/Code/quant_agents/tradingagents/dataflows/smartmoney_vendor.py`: 增加 `as_of_date` 查询和数据状态暴露。

---

### Task 1: Establish Production Baseline And Backup Gate

**Addresses:** R3

**Files:**
- Create: `scripts/backup_database.py`
- Create: `tests/test_backup_database.py`
- Modify: `.gitignore`

**Interfaces:**
- Produces: `backup_database(source: Path, destination_dir: Path) -> BackupReport`
- Produces: CLI `rtk uv run python scripts/backup_database.py --db /Users/hainingyu/Code/quant_data/quant_core.db --output-dir /Users/hainingyu/Code/quant_data/backups`

- [ ] **Step 1: Write failing backup tests**

Tests must create a WAL-mode temporary database, keep a writer connection open, call `backup_database`, then assert:

```python
assert report.source_rows == report.backup_rows
assert report.quick_check == "ok"
assert report.sha256
assert report.backup_path.name.startswith("quant_core_")
```

Also test a missing source path and a destination already containing the generated filename.

- [ ] **Step 2: Run the tests and confirm failure**

```bash
rtk uv run python -m pytest tests/test_backup_database.py -q
```

Expected: collection fails because `scripts.backup_database` does not exist.

- [ ] **Step 3: Implement the backup utility**

Use `sqlite3.Connection.backup`, never `shutil.copy`, because the production database uses WAL:

```python
@dataclass(frozen=True)
class BackupReport:
    backup_path: Path
    source_rows: int
    backup_rows: int
    quick_check: str
    sha256: str


def backup_database(source: Path, destination_dir: Path) -> BackupReport:
    source = source.expanduser().resolve()
    destination_dir.mkdir(parents=True, exist_ok=True)
    target = destination_dir / f"{source.stem}_{datetime.now():%Y%m%d_%H%M%S}.db"
    with sqlite3.connect(f"file:{source}?mode=ro", uri=True) as src:
        with sqlite3.connect(target) as dst:
            src.backup(dst)
            check = dst.execute("PRAGMA quick_check").fetchone()[0]
    if check != "ok":
        target.unlink(missing_ok=True)
        raise RuntimeError(f"backup quick_check failed: {check}")
    return build_report(source, target, check)
```

`build_report` must compare the sum of row counts for all non-internal tables and calculate SHA-256 in 8 MiB chunks.

- [ ] **Step 4: Add baseline audit commands to the release checklist**

The implementation run must archive these outputs under `/Users/hainingyu/Code/quant_data/audits/<timestamp>/`:

```bash
rtk sqlite3 'file:/Users/hainingyu/Code/quant_data/quant_core.db?immutable=1' 'PRAGMA quick_check;'
rtk sqlite3 -json 'file:/Users/hainingyu/Code/quant_data/quant_core.db?immutable=1' \
  "SELECT name, sql FROM sqlite_master WHERE type IN ('table','index') ORDER BY name;"
rtk uv run python scripts/backup_database.py \
  --db /Users/hainingyu/Code/quant_data/quant_core.db \
  --output-dir /Users/hainingyu/Code/quant_data/backups
```

- [ ] **Step 5: Verify and commit**

```bash
rtk uv run python -m pytest tests/test_backup_database.py -q
rtk uv run ruff check scripts/backup_database.py tests/test_backup_database.py
rtk git add scripts/backup_database.py tests/test_backup_database.py .gitignore
rtk git commit -m "feat(data): add consistent sqlite backup gate"
```

Expected: tests and Ruff pass; no database file is tracked.

---

### Task 2: Introduce Typed Task Results

**Addresses:** R2

**Files:**
- Create: `core/task_result.py`
- Create: `tests/test_task_result.py`
- Modify: `core/runner.py`
- Modify: `tests/test_core_infrastructure.py`

**Interfaces:**
- Produces: `TaskStatus`, `ErrorKind`, `TaskResult`
- Produces: `normalize_task_result(name: str, value: TaskResult | dict) -> TaskResult`
- Consumed by: all later tasks, `daily_pipeline.py`, TUI and ingestion audit

- [ ] **Step 1: Write status contract tests**

Cover these cases:

```python
assert TaskResult.success("x", saved=10).status is TaskStatus.SUCCESS
assert TaskResult.no_data("x", reason="market holiday").status is TaskStatus.NO_DATA
assert TaskResult.failed("x", ErrorKind.SCHEMA_DRIFT, "missing date").exit_failure
assert normalize_task_result("x", {"saved": 0}).status is TaskStatus.FAILED
assert normalize_task_result("x", {"saved": 0, "status": "no_data"}).status is TaskStatus.NO_DATA
assert normalize_task_result("x", {"failed": 2}).status is TaskStatus.DEGRADED
```

The compatibility rule is intentionally strict: an unqualified zero-row legacy result is failure, not success.

- [ ] **Step 2: Run the focused tests and confirm failure**

```bash
rtk uv run python -m pytest tests/test_task_result.py tests/test_core_infrastructure.py -q
```

- [ ] **Step 3: Implement the result types**

```python
class TaskStatus(StrEnum):
    SUCCESS = "success"
    NO_DATA = "no_data"
    DEGRADED = "degraded"
    FAILED = "failed"
    ABORTED = "aborted"


class ErrorKind(StrEnum):
    NETWORK = "network"
    RATE_LIMIT = "rate_limit"
    SOURCE_REMOVED = "source_removed"
    SCHEMA_DRIFT = "schema_drift"
    DATA_QUALITY = "data_quality"
    DATABASE = "database"
    INTERNAL = "internal"


@dataclass
class TaskResult:
    task_name: str
    status: TaskStatus
    attempted: int = 0
    fetched: int = 0
    accepted: int = 0
    rejected: int = 0
    saved: int = 0
    source: str | None = None
    data_date: str | None = None
    error_kind: ErrorKind | None = None
    error: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def exit_failure(self) -> bool:
        return self.status in {
            TaskStatus.DEGRADED,
            TaskStatus.FAILED,
            TaskStatus.ABORTED,
        }

    @classmethod
    def success(
        cls,
        task_name: str,
        *,
        saved: int,
        attempted: int = 0,
        fetched: int = 0,
        accepted: int = 0,
        rejected: int = 0,
        source: str | None = None,
        data_date: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> "TaskResult":
        return cls(
            task_name=task_name,
            status=TaskStatus.SUCCESS,
            attempted=attempted,
            fetched=fetched,
            accepted=accepted,
            rejected=rejected,
            saved=saved,
            source=source,
            data_date=data_date,
            metadata=metadata or {},
        )

    @classmethod
    def no_data(
        cls,
        task_name: str,
        *,
        reason: str,
        attempted: int = 0,
        source: str | None = None,
        data_date: str | None = None,
    ) -> "TaskResult":
        return cls(
            task_name=task_name,
            status=TaskStatus.NO_DATA,
            attempted=attempted,
            source=source,
            data_date=data_date,
            metadata={"reason": reason},
        )

    @classmethod
    def degraded(
        cls,
        task_name: str,
        error_kind: ErrorKind,
        error: str,
        *,
        saved: int = 0,
        fetched: int = 0,
        accepted: int = 0,
        rejected: int = 0,
        source: str | None = None,
    ) -> "TaskResult":
        return cls(
            task_name=task_name,
            status=TaskStatus.DEGRADED,
            fetched=fetched,
            accepted=accepted,
            rejected=rejected,
            saved=saved,
            source=source,
            error_kind=error_kind,
            error=error,
        )

    @classmethod
    def failed(
        cls,
        task_name: str,
        error_kind: ErrorKind,
        error: str,
        *,
        fetched: int = 0,
        rejected: int = 0,
        source: str | None = None,
    ) -> "TaskResult":
        return cls(
            task_name=task_name,
            status=TaskStatus.FAILED,
            fetched=fetched,
            rejected=rejected,
            source=source,
            error_kind=error_kind,
            error=error,
        )
```

Factories must require a reason for `NO_DATA` and an `ErrorKind` for
`DEGRADED` or `FAILED`. Add further optional count fields only when an actual
call site needs them; do not accept arbitrary unknown keywords.

- [ ] **Step 4: Convert `safe_task`**

`safe_task` must:

1. Normalize legacy dictionaries.
2. Preserve elapsed time in `metadata["elapsed_seconds"]`.
3. Convert raised exceptions to `FAILED/INTERNAL`.
4. Log success only for `SUCCESS` and explicit `NO_DATA`.
5. Return `TaskResult.to_dict()` temporarily so existing CLI consumers continue working.

- [ ] **Step 5: Add regression tests for removed and empty sources**

Add tests proving:

- `{"saved": 0}` becomes `failed`.
- `{"saved": 0, "status": "no_data", "reason": "holiday"}` does not fail the process.
- `SOURCE_REMOVED` causes non-zero pipeline exit.
- partial success with rejected rows becomes `degraded`.

- [ ] **Step 6: Verify and commit**

```bash
rtk uv run python -m pytest tests/test_task_result.py tests/test_core_infrastructure.py tests/test_daily_pipeline.py -q
rtk uv run ruff check core/task_result.py core/runner.py tests/test_task_result.py
rtk git add core/task_result.py core/runner.py tests/test_task_result.py tests/test_core_infrastructure.py tests/test_daily_pipeline.py
rtk git commit -m "refactor(tasks): enforce typed task outcomes"
```

---

### Task 3: Add Blocking Data Contracts

**Addresses:** R5

**Files:**
- Create: `core/data_contract.py`
- Create: `tests/test_data_contract.py`
- Create: `tests/fixtures/contracts/*.json`
- Modify: `core/utils.py`

**Interfaces:**
- Produces: `FieldRule`, `DataContract`, `ValidationResult`
- Produces: `validate_frame(frame: pd.DataFrame, contract: DataContract) -> ValidationResult`
- Consumed by: source-specific tasks and audit script

- [ ] **Step 1: Write contract tests**

Tests must cover:

- required source column absent;
- aliases resolving to a canonical column;
- invalid date parsing;
- minimum non-null ratio;
- numeric bounds such as `0 <= pledge_ratio <= 100`;
- uniqueness violations;
- all-key-fields-empty records;
- schema fingerprint changing when source columns change.

- [ ] **Step 2: Define the contract model**

```python
@dataclass(frozen=True)
class FieldRule:
    name: str
    aliases: tuple[str, ...] = ()
    required: bool = False
    nullable: bool = True
    min_non_null_ratio: float = 0.0
    minimum: float | None = None
    maximum: float | None = None


@dataclass(frozen=True)
class DataContract:
    name: str
    fields: tuple[FieldRule, ...]
    unique_by: tuple[str, ...]
    required_any_of: tuple[tuple[str, ...], ...] = ()
    min_rows: int = 1
    max_rejected_ratio: float = 0.02


@dataclass
class ValidationResult:
    accepted: pd.DataFrame
    rejected: pd.DataFrame
    violations: list[str]
    source_columns: tuple[str, ...]
    schema_fingerprint: str

    @property
    def can_write(self) -> bool:
        return not self.violations
```

Each `required_any_of` group passes when at least one named canonical field is
present and meets its non-null rule. The fingerprint must be SHA-256 over
sorted `column_name:dtype` entries.

- [ ] **Step 3: Define exact contracts for the seven affected datasets**

At minimum:

| Contract | Required canonical fields | Unique key | Blocking threshold |
|---|---|---|---|
| `market_valuation` | `date` and one of PE/PB/EBS | `date` | latest expected date missing; all metrics null |
| `concept_board` | `trade_date`, `concept_code`, `concept_name` | date+code | fewer than 300 boards |
| `stock_repurchase` | `trade_date`, `stock_code`, `progress_status` | date+code | rejected ratio above 2% |
| `institution_survey` | `trade_date`, `stock_code` | date+code | rejected ratio above 2% |
| `stock_pledge` | `trade_date`, `stock_code`, `pledge_ratio` | date+code | ratio outside 0..100 |
| `option_sentiment` | `trade_date` and QVIX or PCR | `trade_date` | both metrics null |
| `quarterly_financials` | `ts_code`, `report_period`, `publish_date` | code+period+publish date | report period invalid |

Do not use `df.columns[0]` as a fallback for identifiers. Unknown layouts must produce `SCHEMA_DRIFT`.

- [ ] **Step 4: Replace `warn_if_all_empty` at the first seven call sites**

The write sequence becomes:

```python
validation = validate_frame(raw_df, MARKET_VALUATION_CONTRACT)
if not validation.can_write:
    return TaskResult.failed(
        task_name,
        ErrorKind.DATA_QUALITY,
        "; ".join(validation.violations),
        fetched=len(raw_df),
        rejected=len(validation.rejected),
    )
saved = db.save_market_valuation_batch(validation.accepted.to_dict("records"))
```

- [ ] **Step 5: Verify and commit**

```bash
rtk uv run python -m pytest tests/test_data_contract.py tests/test_phase1_tasks.py tests/test_new_tasks.py -q
rtk uv run ruff check core/data_contract.py tasks tests/test_data_contract.py
rtk git add core/data_contract.py core/utils.py tasks tests/test_data_contract.py tests/fixtures/contracts
rtk git commit -m "feat(data): block writes that violate source contracts"
```

---

### Task 4: Centralize Source Transport And Fallback

**Addresses:** R4

**Files:**
- Create: `core/source_client.py`
- Create: `tests/test_source_client.py`
- Modify: `core/stock_cyq_em.py`
- Modify: `tasks/concept_board.py`
- Modify: `tasks/institution_survey.py`
- Modify: `tasks/stock_pledge.py`
- Modify: `tasks/stock_repurchase.py`
- Modify: `tasks/market_valuation.py`
- Modify: `tasks/option_sentiment.py`

**Interfaces:**
- Produces: `SourcePolicy`, `FetchMetadata`, `SourceResponse`, `SourceClient`
- Produces: `SourceClient.call(source, operation, callback) -> SourceResponse`
- Consumes: `ErrorKind` from Task 2

- [ ] **Step 1: Write deterministic transport tests**

Use a fake clock and scripted responses. Cover:

- retry on timeout, 429, 502 and 503;
- no retry on 400, 401, schema failure or source removal;
- exponential backoff with bounded jitter;
- per-host minimum interval;
- circuit opens after five consecutive retryable failures;
- half-open probe after cooldown;
- fallback only after primary exhaustion;
- metadata records attempts, latency, source and fallback reason.

- [ ] **Step 2: Implement source policies**

```python
@dataclass(frozen=True)
class SourcePolicy:
    source: str
    host: str
    timeout_seconds: float
    max_attempts: int
    base_delay_seconds: float
    max_delay_seconds: float
    min_interval_seconds: float
    circuit_failures: int = 5
    circuit_cooldown_seconds: float = 120.0


POLICIES = {
    "eastmoney": SourcePolicy("eastmoney", "eastmoney.com", 15, 3, 1, 30, 0.8),
    "legu": SourcePolicy("legu", "legulegu.com", 15, 3, 1, 20, 0.5),
    "ths": SourcePolicy("ths", "10jqka.com.cn", 15, 3, 1, 20, 0.8),
    "sina": SourcePolicy("sina", "sina.com.cn", 15, 2, 1, 10, 0.5),
}
```

Rate limit and circuit state must be shared by host within one process, protected by a lock.

- [ ] **Step 3: Standardize HTTP behavior**

- Eastmoney direct calls use one `curl_cffi.requests.Session` with browser impersonation.
- AkShare callbacks remain supported, but execute inside the same policy accounting.
- Parse HTTP status before JSON.
- Treat an empty JSON `data` object differently from transport failure.
- Close sessions in `SourceClient.close()` and at pipeline shutdown.

- [ ] **Step 4: Migrate concept and event tasks**

For concept boards:

1. Primary source: THS board list/history where the required historical fields are available.
2. Fallback: Eastmoney spot endpoint.
3. A fallback result sets task status to `DEGRADED`, not `SUCCESS`.

For repurchase, survey and pledge:

1. Primary source remains the current AkShare operation.
2. Each call passes through Eastmoney host policy.
3. Empty results after all recent candidate dates are `FAILED/NETWORK` or `FAILED/RATE_LIMIT` when attempts failed, and `NO_DATA` only when successful responses explicitly contain zero records.

- [ ] **Step 5: Make removed sources explicit**

`insider_trading` must immediately return:

```python
TaskResult.failed(
    "update_insider_trading",
    ErrorKind.SOURCE_REMOVED,
    "akshare stock_cgxq_em is unavailable; no equivalent source is configured",
)
```

Do not sleep twice before reporting a known missing attribute. Re-enable the task only when a tested replacement adapter is configured.

- [ ] **Step 6: Verify and commit**

```bash
rtk uv run python -m pytest tests/test_source_client.py tests/test_phase1_tasks.py tests/test_new_tasks.py tests/test_stock_cyq_em.py -q
rtk uv run ruff check core/source_client.py core/stock_cyq_em.py tasks
rtk git add core/source_client.py core/stock_cyq_em.py tasks tests/test_source_client.py
rtk git commit -m "refactor(sources): centralize retry rate limit and fallback"
```

---

### Task 5: Replace Ad Hoc DDL With Versioned Migrations

**Addresses:** R3

**Files:**
- Create: `core/migrations.py`
- Create: `migrations/001_ingestion_audit.sql`
- Create: `migrations/002_phase2_data_cleanup.py`
- Create: `migrations/003_point_in_time_tables.sql`
- Create: `scripts/migrate_database.py`
- Create: `tests/test_migrations.py`
- Modify: `providers.py`
- Modify: `interface.py`
- Modify: `/Users/hainingyu/Code/quant_hunter/src/smartmoney_hunter/database.py`

**Interfaces:**
- Produces: `Migration`, `MigrationReport`, `run_migrations(db_path: Path) -> MigrationReport`
- Produces tables: `schema_migrations`, `ingestion_runs`, `ingestion_rejections`

- [ ] **Step 1: Write migration tests against four starting schemas**

Fixtures:

1. empty database;
2. current intended schema;
3. production-like legacy schema with missing repurchase price-range columns;
4. polluted schema containing blank survey/repurchase rows and duplicate pledge summaries.

Each fixture must pass two consecutive migration runs with the second run reporting zero applied migrations.

- [ ] **Step 2: Implement migration runner**

Every migration executes under:

```sql
BEGIN IMMEDIATE;
PRAGMA foreign_keys = ON;
```

After the migration:

```sql
PRAGMA foreign_key_check;
PRAGMA quick_check;
```

Only then insert `(version, name, checksum, applied_at)` into `schema_migrations` and commit. A checksum mismatch for an applied migration is a hard failure.

- [ ] **Step 3: Create ingestion audit schema**

```sql
CREATE TABLE ingestion_runs (
    run_id TEXT PRIMARY KEY,
    task_name TEXT NOT NULL,
    source TEXT,
    status TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT NOT NULL,
    requested_date TEXT,
    data_date TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    fetched_rows INTEGER NOT NULL DEFAULT 0,
    accepted_rows INTEGER NOT NULL DEFAULT 0,
    rejected_rows INTEGER NOT NULL DEFAULT 0,
    saved_rows INTEGER NOT NULL DEFAULT 0,
    schema_fingerprint TEXT,
    error_kind TEXT,
    error_message TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE ingestion_rejections (
    run_id TEXT NOT NULL REFERENCES ingestion_runs(run_id),
    row_number INTEGER NOT NULL,
    reason TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    PRIMARY KEY (run_id, row_number)
);
```

Keep rejected payloads bounded to the first 100 rows per run.

- [ ] **Step 4: Rebuild polluted Phase 2 tables**

`002_phase2_data_cleanup.py` must:

1. Create `_new` tables with `NOT NULL` business keys.
2. Copy only valid rows.
3. Deduplicate repurchase and survey rows with
   `ROW_NUMBER() OVER (PARTITION BY trade_date, stock_code ORDER BY id DESC) = 1`;
   apply the same `(trade_date, stock_code)` partition to pledge summary rows.
4. Compare source-valid count with destination count.
5. Rename old tables to `_backup_<timestamp>`.
6. Rename new tables to production names and recreate indexes.
7. Drop migration backup tables only after the post-deployment validation task.

For `stock_pledge`, the current API is a stock-level ratio summary, so the business key is `(trade_date, stock_code)`, not the nullable `pledger`. Preserve `pledger` and `pledge_org` as optional attributes.

For `institution_survey`, use `(trade_date, stock_code)` because the task consumes the statistics endpoint. A future organization-detail table must use a separate table and key.

- [ ] **Step 5: Make migrations the schema authority**

- `SmartMoneyDBProvider.__init__` calls `run_migrations` before opening the shared writer.
- Remove `_migrate_phase2_tables`.
- Reduce `_ensure_tables` to compatibility checks or remove it after all migrations cover pipeline-owned tables.
- `quant_hunter` may create core legacy tables for standalone use, but must not independently alter pipeline-owned Phase 2 or point-in-time tables.
- Provider batch writes must raise `DatabaseWriteError`; they must not catch an exception and return zero.

- [ ] **Step 6: Add an explicit migration CLI**

`scripts/migrate_database.py` must require `--db`, reject a missing file unless
`--create` is explicitly supplied, print every migration version/checksum, and
return non-zero when pre-check, migration or post-check fails:

```python
def main() -> int:
    args = parse_args()
    db_path = Path(args.db).expanduser().resolve()
    if not db_path.exists() and not args.create:
        raise SystemExit(f"database does not exist: {db_path}")
    report = run_migrations(db_path)
    print(json.dumps(asdict(report), ensure_ascii=False, indent=2, default=str))
    return 0 if report.quick_check == "ok" else 2
```

The CLI must not create a backup implicitly; the runbook requires the backup
gate to be run and verified separately so operators cannot mistake migration
execution for backup completion.

- [ ] **Step 7: Verify and commit in both repositories**

```bash
rtk uv run python -m pytest tests/test_migrations.py tests/test_providers.py tests/test_providers_extended2.py -q
rtk uv run ruff check core/migrations.py scripts/migrate_database.py providers.py migrations tests/test_migrations.py
rtk git add core/migrations.py migrations scripts/migrate_database.py providers.py interface.py tests/test_migrations.py
rtk git commit -m "feat(db): add versioned pipeline migrations"
```

In `quant_hunter`, run its own unit suite and commit the compatibility adjustment separately.

---

### Task 6: Fix Quarterly Financial Incremental And Point-In-Time Storage

**Addresses:** R1

**Files:**
- Create: `tasks/financial_history.py`
- Create: `tests/test_financial_history.py`
- Modify: `tasks/financials.py`
- Modify: `interface.py`
- Modify: `providers.py`
- Modify: `daily_pipeline.py`
- Modify: `/Users/hainingyu/Code/quant_agents/tradingagents/dataflows/smartmoney_vendor.py`

**Interfaces:**
- Produces: `discover_missing_financial_periods(db, as_of_date) -> list[str]`
- Produces: `update_financial_history(db, periods: list[str] | None = None) -> TaskResult`
- Produces: `get_financials_as_of(symbol: str, as_of_date: str) -> dict | None`

- [ ] **Step 1: Write failing report-period tests**

Cover:

- a stock with `20260331` is not considered complete for `20260630`;
- rerunning an existing `(symbol, report_period, publish_date)` is idempotent;
- a restatement with a later `publish_date` creates a second history version;
- as-of query before publication returns no future report;
- as-of query after restatement returns the latest version known on that date.

- [ ] **Step 2: Replace code-level existence filtering**

Delete this behavior:

```python
existing_codes = db.get_distinct_codes("quarterly_financials")
stock_codes = [code for code in stock_codes if code not in existing_codes]
```

Period discovery must compare the expected closed report periods against distinct `report_period` values:

```python
def discover_missing_financial_periods(db, as_of_date: date) -> list[str]:
    expected = closed_report_periods(as_of_date, lookback_quarters=8)
    coverage = db.get_financial_period_coverage(expected)
    return [
        period for period in expected
        if coverage.get(period, 0) < minimum_market_coverage(period)
    ]
```

Coverage thresholds are 90% of active stocks for periods older than 120 days and 60% during the active disclosure window.

- [ ] **Step 3: Implement bulk report-period acquisition**

Use the installed AkShare operations by report period:

- `stock_yjbb_em(date=period)` for performance summary;
- `stock_lrb_em(date=period)` for income statement;
- `stock_zcfz_em(date=period)` for balance sheet;
- `stock_xjll_em(date=period)` for cash flow;
- `stock_report_disclosure(market="沪深京", period=<label>)` for disclosure schedule/date.

Merge on normalized six-digit stock code. The financial contract must require `report_period` and a real `publish_date`; records without an actual publication date remain rejected and are not made visible to as-of consumers.

- [ ] **Step 4: Maintain latest and history projections atomically**

Within one transaction:

1. Insert into `quarterly_financials_history` with key `(ts_code, report_period, publish_date)`.
2. Update `quarterly_financials` only when the incoming report has the latest `publish_date` for that code and period.
3. Record accepted, rejected and saved counts in `ingestion_runs`.

The history table must include `source_payload_hash` so identical source rows do not create versions.

- [ ] **Step 5: Add as-of consumption**

The vendor query must use:

```sql
SELECT *
FROM quarterly_financials_history
WHERE ts_code = ?
  AND publish_date <= ?
ORDER BY report_period DESC, publish_date DESC
LIMIT 1
```

Do not fall back to `quarterly_financials` when `as_of_date` is supplied, because that reintroduces look-ahead bias.

- [ ] **Step 6: Verify and commit**

```bash
rtk uv run python -m pytest tests/test_financial_history.py tests/test_daily_pipeline.py tests/test_phase1_tasks.py -q
rtk uv run ruff check tasks/financial_history.py tasks/financials.py tests/test_financial_history.py
rtk git add tasks/financial_history.py tasks/financials.py providers.py interface.py daily_pipeline.py tests/test_financial_history.py
rtk git commit -m "fix(financials): update by report period and publication date"
```

Run and commit the `quant_agents` as-of vendor tests separately.

---

### Task 7: Build A Single Task Registry

**Addresses:** R2, R6

**Files:**
- Create: `core/task_registry.py`
- Create: `tests/test_task_registry.py`
- Modify: `daily_pipeline.py`
- Modify: `tui.py`

**Interfaces:**
- Produces: `TaskSpec`, `Cadence`, `EmptyPolicy`, `TASK_REGISTRY`
- Consumed by: pipeline orchestration, CLI, TUI and health checks

- [ ] **Step 1: Write registry completeness tests**

Assert:

- every CLI task is registered;
- every registered table exists in migrations;
- every table with a date column declares that column;
- every task declares cadence and empty policy;
- every TUI task button resolves to a registered task;
- duplicate task names and duplicate table ownership fail collection.

- [ ] **Step 2: Implement registry model**

```python
class Cadence(StrEnum):
    TRADING_DAY = "trading_day"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    ON_DEMAND = "on_demand"


class EmptyPolicy(StrEnum):
    ALLOW = "allow"
    ALLOW_ON_NON_TRADING_DAY = "allow_on_non_trading_day"
    FAIL = "fail"


@dataclass(frozen=True)
class TaskSpec:
    name: str
    callable: Callable[..., TaskResult]
    tables: tuple[str, ...]
    cadence: Cadence
    date_columns: Mapping[str, str]
    empty_policy: EmptyPolicy
    primary_source: str
    fallback_sources: tuple[str, ...] = ()
```

- [ ] **Step 3: Generate orchestration and TUI metadata**

- `daily_pipeline.run_all` iterates ordered `TaskSpec` entries.
- CLI choices are registry keys.
- `TABLE_DATE_COLUMNS`, monthly/quarterly sets and task-to-table mappings are derived views.
- TUI can add display labels in one registry-adjacent mapping, but it cannot redefine task identity or table ownership.

- [ ] **Step 4: Verify and commit**

```bash
rtk uv run python -m pytest tests/test_task_registry.py tests/test_daily_pipeline.py tests/test_tui.py -q
rtk uv run ruff check core/task_registry.py daily_pipeline.py tui.py
rtk git add core/task_registry.py daily_pipeline.py tui.py tests/test_task_registry.py
rtk git commit -m "refactor(pipeline): drive tasks and freshness from one registry"
```

---

### Task 8: Expand Health Checks Into Contract Audits

**Addresses:** R6

**Files:**
- Create: `scripts/audit_data_contracts.py`
- Create: `tests/test_data_audit.py`
- Modify: `tasks/utility.py`
- Modify: `tui.py`

**Interfaces:**
- Produces: `TableHealth`, `HealthReport`, `audit_database(db_path, registry) -> HealthReport`
- Produces exit codes: `0=healthy`, `1=degraded`, `2=critical`

- [ ] **Step 1: Write production-shaped audit tests**

Fixtures must reproduce:

- 66,877 empty institution survey rows;
- repurchase rows with missing dates and all metrics null;
- pledge rows duplicated because a nullable key bypassed uniqueness;
- a fresh table with 99% null core metrics;
- quarterly data whose maximum report period is current but market coverage is below threshold;
- an intentionally empty on-demand table.

- [ ] **Step 2: Implement table health dimensions**

For every registered table report:

```python
@dataclass
class TableHealth:
    table: str
    row_count: int
    latest_date: str | None
    expected_date: str | None
    coverage_ratio: float | None
    core_field_completeness: float | None
    duplicate_count: int
    invalid_value_count: int
    status: Literal["healthy", "degraded", "critical", "not_due"]
    issues: list[str]
```

Use cadence-aware dates. A quarterly table is judged by expected report period and coverage, not by whether its date equals the latest trading day.

- [ ] **Step 3: Join ingestion status with table health**

Flag critical when:

- the last due ingestion is failed or aborted;
- schema fingerprint changed without an approved fixture update;
- required key completeness is below 100%;
- logical duplicates exist;
- data is stale beyond the task-specific grace period.

Flag degraded when fallback source was used or rejected-row ratio is non-zero but below the blocking threshold.

- [ ] **Step 4: Expose concise TUI state**

The TUI displays status, latest data date, coverage and last ingestion result. Row count remains informational and cannot make a table healthy by itself.

- [ ] **Step 5: Verify and commit**

```bash
rtk uv run python -m pytest tests/test_data_audit.py tests/test_tui.py tests/test_daily_pipeline.py -q
rtk uv run python scripts/audit_data_contracts.py --db /tmp/contract-audit-test.db --json
rtk uv run ruff check scripts/audit_data_contracts.py tasks/utility.py tui.py
rtk git add scripts/audit_data_contracts.py tasks/utility.py tui.py tests/test_data_audit.py
rtk git commit -m "feat(health): audit freshness coverage and data integrity"
```

---

### Task 9: Store Point-In-Time Concept And Index Membership

**Addresses:** R7

**Files:**
- Create: `tasks/index_membership.py`
- Create: `tests/test_membership_history.py`
- Modify: `tasks/concept_board.py`
- Modify: `tasks/index_chain.py`
- Modify: `interface.py`
- Modify: `providers.py`
- Modify: `/Users/hainingyu/Code/quant_agents/tradingagents/dataflows/smartmoney_vendor.py`

**Interfaces:**
- Produces tables: `concept_member_history`, `index_member_history`
- Produces: `update_concept_member_history(db, effective_date=None) -> TaskResult`
- Produces: `update_index_membership(db, effective_date=None) -> TaskResult`
- Produces: `get_stock_memberships_as_of(symbol, as_of_date) -> dict`

- [ ] **Step 1: Write interval-model tests**

Cover:

- first snapshot creates open intervals;
- unchanged next snapshot creates no rows;
- removed member closes `valid_to` on the day before the new snapshot;
- new member opens at `valid_from`;
- re-entry creates a new interval;
- partial source failure cannot close missing members;
- as-of queries return only memberships valid on the requested date.

- [ ] **Step 2: Add history schemas**

```sql
CREATE TABLE concept_member_history (
    concept_code TEXT NOT NULL,
    concept_name TEXT NOT NULL,
    ts_code TEXT NOT NULL,
    valid_from TEXT NOT NULL,
    valid_to TEXT,
    source TEXT NOT NULL,
    snapshot_run_id TEXT NOT NULL REFERENCES ingestion_runs(run_id),
    PRIMARY KEY (concept_code, ts_code, valid_from)
);

CREATE TABLE index_member_history (
    index_code TEXT NOT NULL,
    index_name TEXT NOT NULL,
    ts_code TEXT NOT NULL,
    weight REAL,
    valid_from TEXT NOT NULL,
    valid_to TEXT,
    source TEXT NOT NULL,
    snapshot_run_id TEXT NOT NULL REFERENCES ingestion_runs(run_id),
    PRIMARY KEY (index_code, ts_code, valid_from)
);
```

Add partial unique indexes enforcing at most one open interval per membership.

- [ ] **Step 3: Require complete snapshots before interval closure**

A snapshot may modify intervals only if:

- every configured board/index request completed;
- contract validation passed;
- returned universe size remains within 20% of the previous complete snapshot;
- no circuit is open for the source.

Otherwise record a degraded ingestion and leave existing intervals unchanged.

- [ ] **Step 4: Persist index weights**

Use:

- `index_stock_cons_weight_csindex` when weights are available;
- existing Sina/CSIndex constituent APIs as membership fallback;
- explicit `weight=NULL` when fallback provides membership only.

Track CSI 300, CSI 500 and CSI 1000 first. Additional indices require only registry configuration and contract fixtures.

- [ ] **Step 5: Change internal target selection and agent queries**

- `index_chain._get_chip_em_target_symbols` reads the latest valid local membership instead of making live constituent calls during an unrelated chip task.
- `quant_agents` accepts `as_of_date` and queries `valid_from <= date AND (valid_to IS NULL OR valid_to >= date)`.
- Keep current `concept_member` as a materialized latest-state compatibility table until all consumers migrate.

- [ ] **Step 6: Verify and commit**

```bash
rtk uv run python -m pytest tests/test_membership_history.py tests/test_new_tasks.py tests/test_daily_pipeline.py -q
rtk uv run ruff check tasks/index_membership.py tasks/concept_board.py tasks/index_chain.py
rtk git add tasks/index_membership.py tasks/concept_board.py tasks/index_chain.py providers.py interface.py tests/test_membership_history.py
rtk git commit -m "feat(membership): store point-in-time concept and index history"
```

Run and commit the `quant_agents` vendor changes separately.

---

### Task 10: End-To-End Failure Injection And Production Rollout

**Addresses:** R1-R7

**Files:**
- Create: `tests/test_pipeline_resilience.py`
- Create: `docs/runbooks/data-pipeline-production-migration.md`
- Modify: `docs/2026-07-20-new-data-sources-assessment.md`
- Modify: `docs/roadmap.md`

**Interfaces:**
- Consumes all prior tasks.
- Produces the production migration and rollback runbook.

- [ ] **Step 1: Add end-to-end failure injection**

The suite must exercise a temporary database through real registry, contracts, migrations and providers while mocking only network boundaries:

1. Primary timeout, fallback success -> degraded, data written with fallback source.
2. Removed source -> failed, no rows written, non-zero pipeline result.
3. Schema drift -> failed, rejected sample recorded, no business rows written.
4. Database constraint failure -> failed/database, transaction rolled back.
5. Repeated migration -> no changes.
6. Quarterly restatement -> correct as-of results before and after publication.
7. Partial membership snapshot -> no interval closures.

- [ ] **Step 2: Run repository-wide static and test gates**

```bash
rtk uv run ruff check .
rtk uv run python -m pytest -q
rtk uv run python -m pytest -W error::RuntimeWarning -W error::pytest.PytestUnraisableExceptionWarning -q
rtk git diff --check
rtk git status --short
```

Expected:

- all commands exit zero;
- no `<MagicMock ...>` files exist in the repository root;
- only intentional source and documentation changes are present.

- [ ] **Step 3: Run a disposable copy migration**

```bash
rtk uv run python scripts/backup_database.py \
  --db /Users/hainingyu/Code/quant_data/quant_core.db \
  --output-dir /Users/hainingyu/Code/quant_data/backups
rtk cp /Users/hainingyu/Code/quant_data/quant_core.db /tmp/quant_core_migration_test.db
rtk uv run python scripts/migrate_database.py --db /tmp/quant_core_migration_test.db
rtk uv run python scripts/audit_data_contracts.py --db /tmp/quant_core_migration_test.db --json
```

Expected:

- `PRAGMA quick_check` is `ok`;
- invalid survey and repurchase rows are removed;
- pledge logical duplicates are zero;
- migration can run a second time with zero applied changes.

- [ ] **Step 4: Write the production rollout sequence**

The runbook must use this order:

1. Stop daemon and verify no pipeline process owns the database.
2. Create a fresh SQLite API backup.
3. Record baseline audit.
4. Run migrations only.
5. Run post-migration audit.
6. Run `market_valuation`, `option_sentiment`, `stock_repurchase`, `institution_survey` and `stock_pledge` individually.
7. Run financial history for one report period.
8. Run concept/index membership snapshots.
9. Start daemon.
10. Observe two scheduled cycles before dropping migration backup tables.

- [ ] **Step 5: Define rollback**

Rollback is triggered by any of:

- `quick_check` failure;
- missing valid-row count after migration;
- critical contract audit;
- unexpected schema checksum;
- more than 5% drop in a complete dataset without a source-confirmed reason.

Rollback procedure:

1. Stop daemon.
2. Preserve failed database and logs under a timestamped incident directory.
3. Restore the pre-migration backup to a new file.
4. Validate restored file.
5. Atomically replace the production path while no process is connected.
6. Revert only the release commits that depend on the new schema.
7. Restart read-only health checks before enabling writes.

- [ ] **Step 6: Update assessment status**

The July 20 assessment must distinguish:

- table created;
- transport verified;
- contract verified;
- production data healthy;
- historical coverage complete.

Do not label `insider_trading` complete while its source is removed. Record actual table counts and the date of the audit.

- [ ] **Step 7: Final cross-repository verification and release commit**

Run each repository's Ruff and unit suite. Then:

```bash
rtk git diff --check
rtk git status --short
rtk git log --oneline -12
```

Create a release commit only after the production migration runbook has been reviewed:

```bash
rtk git add tests/test_pipeline_resilience.py docs
rtk git commit -m "docs(data): add hardened ingestion rollout runbook"
```

---

## 4. Verification Matrix

| Layer | Verification | Blocking condition |
|---|---|---|
| Unit | TaskResult, contracts, source policies, migration functions | any failure |
| Integration | task -> contract -> provider -> SQLite | wrong status, partial commit, missing audit |
| Schema | migration twice, quick check, FK check, index inspection | non-idempotent or invalid DB |
| Data | key completeness, metric completeness, uniqueness, bounds | critical contract violation |
| Temporal | publication-date and membership as-of queries | future data visible |
| Resilience | timeout, 429, source removal, fallback, circuit | silent success or uncontrolled retry |
| UI/CLI | registry-derived menu, freshness and exit code | mapping divergence |
| Production | backup, dry-run copy, canary tasks, two-cycle observation | any rollback trigger |

## 5. Acceptance Criteria

- All pipeline tasks return a normalized status and error category.
- An unqualified zero-row result cannot be logged as success.
- Source removal, schema drift and database write failures produce non-zero CLI exit status.
- No official table contains rows with null/blank business keys.
- `stock_pledge` has zero duplicates by `(trade_date, stock_code)`.
- `institution_survey` and `stock_repurchase` contain no historical empty-shell rows.
- New financial report periods are fetched without deleting or overwriting earlier publication versions.
- `quarterly_financials_history` supports correct as-of-date lookup.
- Concept and index memberships support interval-based historical queries.
- Every registered task/table appears automatically in pipeline execution, TUI and health audit.
- Health status uses freshness, coverage, completeness and uniqueness rather than row count alone.
- Source retries are bounded, host-aware and observable.
- Full Ruff, pytest, warning-as-error and `git diff --check` gates pass.
- Production backup and rollback procedure are tested before migration.

## 6. Recommended Execution Order

Implement in four reviewable releases:

1. **Release A, correctness gate:** Tasks 1-3. No schema change except audit-ready code.
2. **Release B, storage and registry:** Tasks 5 and 7. Migrate and clean production after dry-run, then make registry metadata authoritative.
3. **Release C, acquisition and observability:** Tasks 4, 6 and 8. Convert tasks, fix financial updates and enable registry-driven health audits.
4. **Release D, temporal correctness:** Tasks 9-10. Backfill membership history and complete rollout.

Each release must pass its own tests and may be reverted without reverting later unrelated work. Do not begin Release D until Release B has completed two healthy scheduled cycles.
