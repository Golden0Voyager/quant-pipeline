# Source Record Key Integrity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Preserve every legitimate stock repurchase and institution survey source record while keeping daily reruns idempotent.

**Architecture:** Add one canonical source-record hash helper and use it from Provider writes plus migration 009. Rebuild the two legacy tables to replace `UNIQUE(trade_date, stock_code)` with `source_record_key TEXT NOT NULL UNIQUE`, and align task data contracts with the exact same business-key fields.

**Tech Stack:** Python 3, SQLite, `core.migrations.MigrationEngine`, `providers.SmartMoneyDBProvider`, pytest, Ruff, coverage, RTK-prefixed commands.

## Global Constraints

- Use `rtk` before shell commands.
- Use `uv` only for Python execution and dependency tooling.
- Do not edit published migrations 001-008.
- Do not run the production full data pipeline without explicit user approval.
- Do not modify `/Users/hainingyu/Code/quant_data/quant_core.db` during development verification.
- Keep commits one file per commit with an English subject and Chinese body.
- Follow TDD: write each behavior test first, run it to verify RED, then implement.

---

## File Structure

- Create `core/source_record_key.py`: canonical field definitions, value normalization, and `source_record_key(record, fields)` SHA-256 helper.
- Modify `core/data_contract.py`: set repurchase and survey `unique_by` to the same complete business-field tuples used by the hash.
- Modify `providers.py`: add `source_record_key` columns to fallback DDL and write both affected batches with `ON CONFLICT(source_record_key) DO UPDATE`.
- Create `migrations/009_source_record_keys.py`: transactional table rebuild for `stock_repurchase` and `institution_survey`.
- Modify `tests/test_data_contract.py`: contract-level duplicate acceptance/rejection coverage.
- Modify `tests/test_providers_extended2.py`: real SQLite Provider write coverage for distinct records and rerun idempotency.
- Modify `tests/test_migrations.py`: migration 009 preserve, schema, and rollback coverage.
- Modify `tests/test_phase1_tasks.py`: task-to-contract behavior for distinct same-date records.
- Modify `docs/runbooks/data-pipeline-production-migration.md`: document v9 and production operator expectations.

## Task 1: RED Tests for Contracts and Provider Storage

**Files:**
- Modify: `tests/test_data_contract.py`
- Modify: `tests/test_providers_extended2.py`
- Test: `tests/test_data_contract.py`, `tests/test_providers_extended2.py`

**Interfaces:**
- Consumes: existing `validate_records(records, contract, logger)` and `SmartMoneyDBProvider.save_*_batch`.
- Produces: executable RED proof that current contracts/storage still collapse or reject valid source records.

- [ ] **Step 1: Add contract tests**

Add tests that assert:

```python
def test_stock_repurchase_contract_rejects_exact_duplicate_but_accepts_distinct_plan(logger):
    base = {
        "trade_date": "2026-07-21",
        "stock_code": "000001",
        "stock_name": "Ping An Bank",
        "repurchase_amount": 100.0,
        "repurchase_price": 12.0,
        "repurchase_price_lower": 11.0,
        "repurchase_price_upper": 13.0,
        "repurchase_quantity": 10,
        "progress_status": "planned",
    }
    distinct = {**base, "repurchase_amount": 120.0}

    valid, violations = validate_records([base, distinct], STOCK_REPURCHASE_CONTRACT, logger)

    assert len(valid) == 2
    assert not violations
```

```python
def test_institution_survey_contract_includes_nullable_org_and_count(logger):
    base = {
        "trade_date": "2026-07-21",
        "stock_code": "000001",
        "stock_name": "Ping An Bank",
        "survey_org": None,
        "survey_type": "call",
        "survey_count": 3,
    }
    distinct = {**base, "survey_count": 4}

    valid, violations = validate_records([base, distinct], INSTITUTION_SURVEY_CONTRACT, logger)

    assert len(valid) == 2
    assert not violations
```

- [ ] **Step 2: Add Provider storage tests**

Add real SQLite tests that create a temporary `SmartMoneyDBProvider`, call `save_stock_repurchase_batch` and `save_institution_survey_batch`, then assert:

```python
assert rows == [
    ("2026-07-21", "000001", 100.0, "planned"),
    ("2026-07-21", "000001", 120.0, "planned"),
]
```

and for survey:

```python
assert rows == [
    ("2026-07-21", "000001", None, "call", 3),
    ("2026-07-21", "000001", None, "call", 4),
]
```

Add a repeat-write assertion:

```python
assert provider.save_stock_repurchase_batch([record]) >= 1
assert provider.save_stock_repurchase_batch([record]) >= 0
assert conn.execute("SELECT COUNT(*) FROM stock_repurchase").fetchone()[0] == 1
```

- [ ] **Step 3: Run RED tests**

Run:

```bash
rtk uv run python -m pytest tests/test_data_contract.py tests/test_providers_extended2.py -q
```

Expected now: FAIL because Provider still uses legacy `UNIQUE(trade_date, stock_code)` conflict behavior and the contracts do not use the full business-field tuples.

- [ ] **Step 4: Commit only test file changes**

```bash
rtk git add tests/test_data_contract.py
rtk git commit -m "test: cover source record contract keys

中文: 增加回购和机构调研业务键合约测试。"
rtk git add tests/test_providers_extended2.py
rtk git commit -m "test: cover source record provider storage

中文: 增加回购和机构调研来源记录存储测试。"
```

## Task 2: Canonical Source Record Key Helper

**Files:**
- Create: `core/source_record_key.py`
- Test: `tests/test_providers_extended2.py`

**Interfaces:**
- Consumes: dictionaries produced by tasks and read by Provider/migration code.
- Produces:
  - `STOCK_REPURCHASE_SOURCE_KEY_FIELDS: tuple[str, ...]`
  - `INSTITUTION_SURVEY_SOURCE_KEY_FIELDS: tuple[str, ...]`
  - `source_record_key(record: Mapping[str, Any], fields: Sequence[str]) -> str`

- [ ] **Step 1: Add helper-focused tests**

Add assertions that:

```python
key1 = source_record_key({"trade_date": "2026-07-21", "stock_code": "000001", "stock_name": " Ping An Bank "}, ("trade_date", "stock_code", "stock_name"))
key2 = source_record_key({"trade_date": "2026-07-21", "stock_code": "000001", "stock_name": "Ping An Bank"}, ("trade_date", "stock_code", "stock_name"))
assert key1 == key2
assert len(key1) == 64
```

and:

```python
assert source_record_key({"survey_org": None}, ("survey_org",)) == source_record_key({}, ("survey_org",))
```

- [ ] **Step 2: Run RED helper tests**

Run:

```bash
rtk uv run python -m pytest tests/test_providers_extended2.py -q
```

Expected now: FAIL with import error because `core.source_record_key` does not exist.

- [ ] **Step 3: Implement helper**

Create `core/source_record_key.py` with deterministic JSON serialization:

```python
from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from decimal import Decimal
from typing import Any

STOCK_REPURCHASE_SOURCE_KEY_FIELDS = (
    "trade_date",
    "stock_code",
    "stock_name",
    "repurchase_amount",
    "repurchase_price",
    "repurchase_price_lower",
    "repurchase_price_upper",
    "repurchase_quantity",
    "progress_status",
)

INSTITUTION_SURVEY_SOURCE_KEY_FIELDS = (
    "trade_date",
    "stock_code",
    "stock_name",
    "survey_org",
    "survey_type",
    "survey_count",
)

def source_record_key(record: Mapping[str, Any], fields: Sequence[str]) -> str:
    payload = [_normalize(record.get(field)) for field in fields]
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()

def _normalize(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        return text if text else None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, Decimal):
        return str(value)
    if hasattr(value, "item"):
        try:
            return _normalize(value.item())
        except Exception:
            return str(value).strip() or None
    return value
```

- [ ] **Step 4: Run GREEN helper tests**

Run:

```bash
rtk uv run python -m pytest tests/test_providers_extended2.py -q
```

Expected: helper tests pass; storage tests may still fail until Task 4.

- [ ] **Step 5: Commit helper**

```bash
rtk git add core/source_record_key.py
rtk git commit -m "feat: add source record key helper

中文: 增加来源记录稳定哈希键工具。"
```

## Task 3: Align Data Contracts

**Files:**
- Modify: `core/data_contract.py`
- Test: `tests/test_data_contract.py`

**Interfaces:**
- Consumes: field tuples from `core.source_record_key`.
- Produces: contract duplicate detection that mirrors Provider idempotency keys.

- [ ] **Step 1: Import field tuples in `core/data_contract.py`**

Use:

```python
from core.source_record_key import (
    INSTITUTION_SURVEY_SOURCE_KEY_FIELDS,
    STOCK_REPURCHASE_SOURCE_KEY_FIELDS,
)
```

- [ ] **Step 2: Replace contract `unique_by` values**

Set:

```python
unique_by=STOCK_REPURCHASE_SOURCE_KEY_FIELDS,
```

and:

```python
unique_by=INSTITUTION_SURVEY_SOURCE_KEY_FIELDS,
```

- [ ] **Step 3: Run contract tests**

Run:

```bash
rtk uv run python -m pytest tests/test_data_contract.py -q
```

Expected: PASS.

- [ ] **Step 4: Commit contract file**

```bash
rtk git add core/data_contract.py
rtk git commit -m "fix: align source record data contracts

中文: 让数据合约使用完整来源记录业务键。"
```

## Task 4: Provider Schema and Write Semantics

**Files:**
- Modify: `providers.py`
- Test: `tests/test_providers_extended2.py`

**Interfaces:**
- Consumes: `source_record_key()` and both field tuples.
- Produces: Provider schemas and batch writes keyed by `source_record_key`.

- [ ] **Step 1: Add imports**

In `providers.py`, import:

```python
from core.source_record_key import (
    INSTITUTION_SURVEY_SOURCE_KEY_FIELDS,
    STOCK_REPURCHASE_SOURCE_KEY_FIELDS,
    source_record_key,
)
```

- [ ] **Step 2: Update fallback DDL**

In both `_ensure_tables()` and `_old_migrate_phase2_tables()`, add:

```sql
source_record_key TEXT NOT NULL UNIQUE,
```

to `stock_repurchase` and `institution_survey`, and remove legacy `UNIQUE(trade_date, stock_code)` from those two table definitions.

- [ ] **Step 3: Update `save_stock_repurchase_batch`**

Build tuples including the key:

```python
(
    source_record_key(r, STOCK_REPURCHASE_SOURCE_KEY_FIELDS),
    r.get("trade_date"),
    r.get("stock_code"),
    r.get("stock_name"),
    r.get("repurchase_amount"),
    r.get("repurchase_price"),
    r.get("repurchase_price_lower"),
    r.get("repurchase_price_upper"),
    r.get("repurchase_quantity"),
    r.get("progress_status"),
)
```

Use SQL:

```sql
INSERT INTO stock_repurchase
    (source_record_key, trade_date, stock_code, stock_name, repurchase_amount,
     repurchase_price, repurchase_price_lower, repurchase_price_upper,
     repurchase_quantity, progress_status)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(source_record_key) DO UPDATE SET
    trade_date = excluded.trade_date,
    stock_code = excluded.stock_code,
    stock_name = excluded.stock_name,
    repurchase_amount = excluded.repurchase_amount,
    repurchase_price = excluded.repurchase_price,
    repurchase_price_lower = excluded.repurchase_price_lower,
    repurchase_price_upper = excluded.repurchase_price_upper,
    repurchase_quantity = excluded.repurchase_quantity,
    progress_status = excluded.progress_status
```

- [ ] **Step 4: Update `save_institution_survey_batch`**

Use `source_record_key(r, INSTITUTION_SURVEY_SOURCE_KEY_FIELDS)` and `ON CONFLICT(source_record_key) DO UPDATE SET` for all non-key columns.

- [ ] **Step 5: Run Provider tests**

Run:

```bash
rtk uv run python -m pytest tests/test_providers_extended2.py -q
```

Expected: PASS.

- [ ] **Step 6: Commit provider file**

```bash
rtk git add providers.py
rtk git commit -m "fix: key source record provider writes

中文: 使用来源记录哈希键保存回购和机构调研数据。"
```

## Task 5: Migration 009

**Files:**
- Create: `migrations/009_source_record_keys.py`
- Modify: `tests/test_migrations.py`
- Test: `tests/test_migrations.py`

**Interfaces:**
- Consumes: existing SQLite tables with or without target columns.
- Produces: version 009 migration that rebuilds both tables transactionally and preserves existing rows.

- [ ] **Step 1: Add migration tests**

Add tests that create old tables with legacy unique constraints, insert one repurchase row and one survey row, run real migrations to v9, then assert:

```python
assert "source_record_key" in stock_repurchase_columns
assert "source_record_key" in institution_survey_columns
assert conn.execute("SELECT COUNT(*) FROM stock_repurchase").fetchone()[0] == 1
assert conn.execute("SELECT COUNT(*) FROM institution_survey").fetchone()[0] == 1
```

Assert the old date-code unique index is gone by inserting a second same-date same-code row with a different computed key.

- [ ] **Step 2: Add rollback test**

Create a temporary migrations directory containing `009_source_record_keys.py` plus a monkeypatched or injected failure after the first replacement table is created. Run `MigrationEngine.apply_pending(target_version=9)` and assert:

```python
assert conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='stock_repurchase__v9'").fetchone() is None
assert conn.execute("SELECT COUNT(*) FROM schema_migrations WHERE version = 9 AND success = 1").fetchone()[0] == 0
```

- [ ] **Step 3: Run RED migration tests**

Run:

```bash
rtk uv run python -m pytest tests/test_migrations.py -q
```

Expected now: FAIL because migration 009 does not exist.

- [ ] **Step 4: Implement migration 009**

Create `migrations/009_source_record_keys.py` with:

```python
def apply(conn):
    _rebuild_stock_repurchase(conn)
    _rebuild_institution_survey(conn)
```

Use helper functions that:

1. skip when table does not exist;
2. skip when `source_record_key` already exists;
3. create `<table>__v9` with the new schema;
4. select old rows ordered by `id`;
5. compute keys with `source_record_key(record, FIELDS)`;
6. insert into replacement preserving `id`;
7. drop old table and rename replacement.

- [ ] **Step 5: Run GREEN migration tests**

Run:

```bash
rtk uv run python -m pytest tests/test_migrations.py -q
```

Expected: PASS.

- [ ] **Step 6: Commit migration and migration tests separately**

```bash
rtk git add tests/test_migrations.py
rtk git commit -m "test: cover source record key migration

中文: 增加来源记录键迁移回归测试。"
rtk git add migrations/009_source_record_keys.py
rtk git commit -m "feat: add source record key migration

中文: 新增回购和机构调研来源记录键迁移。"
```

## Task 6: Task Integration Coverage

**Files:**
- Modify: `tests/test_phase1_tasks.py`
- Test: `tests/test_phase1_tasks.py`

**Interfaces:**
- Consumes: task `drop_duplicates()`, contract validation, and Provider-facing record lists.
- Produces: evidence that tasks pass distinct same-date records through to the database layer.

- [ ] **Step 1: Add task tests**

Add repurchase and survey tests using patched AkShare DataFrames with two rows sharing `trade_date` and `stock_code` but differing in one business field. Assert:

```python
records = db.save_stock_repurchase_batch.call_args.args[0]
assert len(records) == 2
```

and:

```python
records = db.save_institution_survey_batch.call_args.args[0]
assert len(records) == 2
```

- [ ] **Step 2: Run RED/GREEN task tests**

Run:

```bash
rtk uv run python -m pytest tests/test_phase1_tasks.py -q
```

Expected after contract/provider changes: PASS. If this fails, adjust only task normalization or the tests until exact duplicates are still removed and distinct business records remain.

- [ ] **Step 3: Commit task test file**

```bash
rtk git add tests/test_phase1_tasks.py
rtk git commit -m "test: cover source record task passthrough

中文: 验证任务保留同日同股的不同来源记录。"
```

## Task 7: Runbook and Final Verification

**Files:**
- Modify: `docs/runbooks/data-pipeline-production-migration.md`
- Test: full project verification commands

**Interfaces:**
- Consumes: implemented v9 behavior.
- Produces: operator instructions and final evidence for pipe-tui readiness.

- [ ] **Step 1: Update runbook**

Change `v1-v8` to `v1-v9`, add row:

```markdown
| 009 | `009_source_record_keys.py` | Python | 为 `stock_repurchase` 和 `institution_survey` 增加来源记录哈希键，移除旧 date+code 唯一约束 |
```

Add a paragraph explaining that first startup after deploy may apply v8 and v9, and operators should back up `~/Code/quant_data/quant_core.db` first.

- [ ] **Step 2: Commit runbook**

```bash
rtk git add docs/runbooks/data-pipeline-production-migration.md
rtk git commit -m "docs: document source record key migration

中文: 记录来源记录键生产迁移说明。"
```

- [ ] **Step 3: Run focused verification**

Run:

```bash
rtk uv run ruff check .
rtk uv run python -m pytest tests/test_data_contract.py tests/test_providers_extended2.py tests/test_migrations.py tests/test_phase1_tasks.py -q
```

Expected: Ruff passes and all focused tests pass.

- [ ] **Step 4: Run full verification**

Run:

```bash
rtk uv run python -m pytest -q
rtk uv run coverage run -m pytest -q
rtk uv run coverage report -m
```

Expected: full pytest passes; coverage total remains at or above 85%.

- [ ] **Step 5: Run nonblocking mypy**

Run:

```bash
rtk uv run mypy .
```

Expected: may still fail with the existing duplicate-module configuration issue; report it as a known nonblocking project configuration risk if unchanged.

- [ ] **Step 6: Sanity checks before handoff**

Run:

```bash
rtk git status --short
rtk git diff --check HEAD
rtk shasum -a 256 migrations/006_reconcile_ingestion_audit.py
```

Expected: clean worktree after commits, no whitespace errors, migration 006 checksum still `a783c28347a05f415f4f6b4dd15f068cde964194657cea3c1573523085af65e0`.

## Self-Review

- Spec coverage: This plan covers canonical hash generation, Provider writes, migration 009, contract alignment, task pass-through, runbook update, and final verification.
- Placeholder scan: No unresolved implementation placeholders remain.
- Type consistency: The helper exposes field tuple constants and a `source_record_key(record, fields)` function used by contracts, Provider, and migration.
