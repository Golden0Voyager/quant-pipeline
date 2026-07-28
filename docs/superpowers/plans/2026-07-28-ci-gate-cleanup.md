# CI Gate Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 清除完整 pytest 与 Ruff 的四个独立阻塞，同时保持生产业务行为不变。

**Architecture:** 通过动态预期日期和测试边界锁隔离消除环境耦合；两处 Ruff 问题采用纯机械重写。每个文件独立修改、验证和提交。

**Tech Stack:** Python、pytest、unittest.mock、Ruff、uv

## Global Constraints

- 所有 shell 命令使用 `rtk` 前缀；测试使用 `rtk uv run python -m pytest`。
- 不终止或干扰正在运行的生产管道。
- 不改变数据源策略、审计规则、估值阈值或筹码计算行为。
- Git 一文件一提交；提交信息英文在前、中文在后。

---

## File Map

- `tests/test_data_audit.py`：使健康表测试使用动态预期日期。
- `tests/test_reconcile.py`：隔离 CLI 测试与真实进程锁。
- `tasks/valuation_chain.py`：修复局部变量命名 lint。
- `tests/test_stock_cyq_em.py`：合并嵌套上下文管理器。

### Task 1: Make Data Audit Test Date-Independent

**Files:**

- Modify: `tests/test_data_audit.py`

**Interfaces:**

- Consumes: `_expected_date_for(spec) -> str`
- Produces: 一个不随运行日期漂移的健康表测试。

- [ ] **Step 1: Confirm the existing failure**

Run:

```bash
rtk uv run python -m pytest -q tests/test_data_audit.py::TestCheckTable::test_populated_table_healthy
```

Expected: FAIL，固定日期已超过健康窗口。

- [ ] **Step 2: Use the expected date**

将测试改为先构造 `spec = _spec("ok_tbl")`，再使用：

```python
expected_date = _expected_date_for(spec)
_build_db(str(db_path), stale_tables={"ok_tbl": expected_date})
```

最终严格断言：

```python
assert h.status == "healthy"
```

- [ ] **Step 3: Verify and commit**

Run:

```bash
rtk uv run python -m pytest -q tests/test_data_audit.py
```

Commit only `tests/test_data_audit.py` with a bilingual message.

### Task 2: Isolate Reconcile CLI Tests from Process Locks

**Files:**

- Modify: `tests/test_reconcile.py`

**Interfaces:**

- Consumes: `ProcessLock.acquire() -> bool`, `ProcessLock.release() -> None`
- Produces: CLI tests that exercise `main()` without touching the real lock.

- [ ] **Step 1: Confirm the existing failure**

Run:

```bash
rtk uv run python -m pytest -q tests/test_reconcile.py::TestMain::test_retry_failed_empty
```

Expected: FAIL while the production lock is held.

- [ ] **Step 2: Patch the lock boundary**

For each `TestMain` test that invokes `rwa.main()`, add:

```python
patch("scripts.reconcile_with_akshare.ProcessLock.acquire", return_value=True)
patch("scripts.reconcile_with_akshare.ProcessLock.release")
```

Do not patch task routing or argument parsing beyond existing mocks.

- [ ] **Step 3: Verify and commit**

Run:

```bash
rtk uv run python -m pytest -q tests/test_reconcile.py::TestMain
```

Commit only `tests/test_reconcile.py` with a bilingual message.

### Task 3: Fix Valuation Chain Local Naming

**Files:**

- Modify: `tasks/valuation_chain.py`

**Interfaces:**

- Produces: identical threshold calculation using the Ruff-compliant local name `min_filled`.

- [ ] **Step 1: Confirm lint failure**

Run:

```bash
rtk uv run ruff check tasks/valuation_chain.py
```

Expected: FAIL with N806.

- [ ] **Step 2: Rename without behavior change**

Rename every function-local `_MIN_FILLED` assignment and reference to
`min_filled`, including the warning message interpolation.

- [ ] **Step 3: Verify and commit**

Run:

```bash
rtk uv run ruff check tasks/valuation_chain.py
```

Commit only `tasks/valuation_chain.py` with a bilingual message.

### Task 4: Flatten Stock CYQ Test Context Managers

**Files:**

- Modify: `tests/test_stock_cyq_em.py`

**Interfaces:**

- Produces: identical exception assertion expressed through one multi-context `with`.

- [ ] **Step 1: Confirm lint failure**

Run:

```bash
rtk uv run ruff check tests/test_stock_cyq_em.py
```

Expected: FAIL with SIM117.

- [ ] **Step 2: Combine contexts**

Use one parenthesized `with` containing the three `patch(...)` contexts and:

```python
pytest.raises(ValueError, match="换手率")
```

- [ ] **Step 3: Verify and commit**

Run:

```bash
rtk uv run python -m pytest -q tests/test_stock_cyq_em.py
rtk uv run ruff check tests/test_stock_cyq_em.py
```

Commit only `tests/test_stock_cyq_em.py` with a bilingual message.

### Task 5: Full Verification

**Files:**

- No modifications expected.

**Interfaces:**

- Produces: fresh full-suite evidence for merge readiness.

- [ ] **Step 1: Run full Ruff**

```bash
rtk uv run ruff check .
```

- [ ] **Step 2: Run full pytest**

```bash
rtk uv run python -m pytest -q
```

- [ ] **Step 3: Check repository state**

```bash
rtk git diff --check
rtk git status --short --branch
```
