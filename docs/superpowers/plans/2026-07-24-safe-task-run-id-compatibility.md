# `safe_task` Run ID Compatibility Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore `pipe-tui` full-pipeline execution while preserving shared run IDs for callbacks that explicitly support them.

**Architecture:** Keep run-ID ownership inside `core.runner.safe_task`, but move callback injection behind a private signature-capability check. The check follows wrapped signatures, accepts explicit keyword-capable `_task_run_id` parameters and `**kwargs`, and fails closed when a callable cannot be inspected.

**Tech Stack:** Python 3, `inspect.signature`, pytest, Ruff

## Global Constraints

- Use `uv` only for Python dependency and command execution.
- Do not modify the existing uncommitted changes in `providers.py` or `migrations/006_reconcile_ingestion_audit.py`.
- Do not change task order, data-source behavior, retry policy, database migrations, or TUI behavior.
- Follow TDD: observe the new regression tests fail before changing `core/runner.py`.
- Follow the repository's one-file-per-commit rule with English commit text first and Chinese text second.

---

### Task 1: Add callback-signature regression coverage

**Files:**
- Modify: `tests/test_core_infrastructure.py`

**Interfaces:**
- Consumes: `core.runner.safe_task(name: str, fn: Callable, *args, **kwargs) -> dict[str, Any]`
- Produces: regression coverage for fixed signatures, explicit run-ID support, arbitrary keyword support, wrapped functions, caller-provided IDs, and uninspectable callables

- [ ] **Step 1: Add required test imports**

Add:

```python
from functools import wraps
from uuid import UUID
```

- [ ] **Step 2: Add focused tests to `TestSafeTask`**

Add:

```python
def test_fixed_signature_callback_does_not_receive_internal_run_id(self):
    """A callback with a fixed signature must remain callable."""

    def fixed() -> dict[str, int]:
        return {"saved": 1}

    result = safe_task("fixed", fixed)

    assert result["status"] == "success"
    assert result["saved"] == 1

def test_explicit_run_id_parameter_receives_generated_uuid(self):
    """A callback that declares _task_run_id receives the scheduler UUID."""
    captured: dict[str, str | None] = {}

    def supported(_task_run_id: str | None = None) -> dict[str, int]:
        captured["run_id"] = _task_run_id
        return {"saved": 1}

    result = safe_task("supported", supported)

    assert result["status"] == "success"
    assert UUID(captured["run_id"] or "").version == 4

def test_var_keyword_callback_receives_generated_run_id(self):
    """A generic **kwargs callback keeps the existing injection behavior."""
    captured: dict[str, object] = {}

    def generic(**kwargs: object) -> dict[str, int]:
        captured.update(kwargs)
        return {"saved": 1}

    result = safe_task("generic", generic)

    assert result["status"] == "success"
    assert UUID(str(captured["_task_run_id"])).version == 4

def test_wrapped_fixed_signature_callback_does_not_receive_run_id(self):
    """Signature inspection follows functools.wraps to the original task."""

    def fixed() -> dict[str, int]:
        return {"saved": 1}

    @wraps(fixed)
    def wrapped(*args: object, **kwargs: object) -> dict[str, int]:
        return fixed(*args, **kwargs)

    result = safe_task("wrapped", wrapped)

    assert result["status"] == "success"
    assert result["saved"] == 1

def test_caller_provided_run_id_is_preserved(self):
    """safe_task must not overwrite a supported caller-provided run ID."""
    captured: dict[str, str | None] = {}

    def supported(_task_run_id: str | None = None) -> dict[str, int]:
        captured["run_id"] = _task_run_id
        return {"saved": 1}

    result = safe_task("provided", supported, _task_run_id="provided-run-id")

    assert result["status"] == "success"
    assert captured["run_id"] == "provided-run-id"

def test_uninspectable_callback_runs_without_internal_run_id(self):
    """Optional metadata injection must not block an uninspectable callback."""

    class UninspectableCallable:
        __signature__ = "invalid"

        def __call__(self) -> dict[str, int]:
            return {"saved": 1}

    result = safe_task("uninspectable", UninspectableCallable())

    assert result["status"] == "success"
    assert result["saved"] == 1
```

- [ ] **Step 3: Run the new fixed-signature test and verify RED**

Run:

```bash
rtk uv run python -m pytest tests/test_core_infrastructure.py::TestSafeTask::test_fixed_signature_callback_does_not_receive_internal_run_id -q
```

Expected: FAIL because `safe_task` passes `_task_run_id` to `fixed()`, producing `status == "failed"`.

- [ ] **Step 4: Run all new signature tests and capture the baseline**

Run:

```bash
rtk uv run python -m pytest tests/test_core_infrastructure.py::TestSafeTask -q
```

Expected: the fixed-signature, wrapped-signature, and uninspectable-callable tests fail for the same unexpected-keyword behavior; existing tests remain unchanged.

---

### Task 2: Implement fail-closed conditional run-ID injection

**Files:**
- Modify: `core/runner.py`

**Interfaces:**
- Consumes: any Python callable passed to `safe_task`
- Produces: `_accepts_task_run_id(fn: Callable[..., Any]) -> bool`
- Preserves: `safe_task(name: str, fn: Callable, *args: Any, **kwargs: Any) -> dict[str, Any]`

- [ ] **Step 1: Import the standard signature inspection module**

Add alongside the standard-library imports:

```python
import inspect
```

- [ ] **Step 2: Add the private capability check before `safe_task`**

Add:

```python
def _accepts_task_run_id(fn: Callable[..., Any]) -> bool:
    """Return whether *fn* safely accepts ``_task_run_id`` as a keyword."""
    try:
        parameters = inspect.signature(fn).parameters
    except (TypeError, ValueError):
        return False

    run_id_parameter = parameters.get("_task_run_id")
    if (
        run_id_parameter is not None
        and run_id_parameter.kind is not inspect.Parameter.POSITIONAL_ONLY
    ):
        return True
    return any(
        parameter.kind is inspect.Parameter.VAR_KEYWORD
        for parameter in parameters.values()
    )
```

- [ ] **Step 3: Make injection conditional**

Replace:

```python
kwargs.setdefault("_task_run_id", run_id)
```

with:

```python
if _accepts_task_run_id(fn):
    kwargs.setdefault("_task_run_id", run_id)
```

Update the adjacent comment to state that fixed-signature legacy tasks are intentionally called without the internal keyword.

- [ ] **Step 4: Run the focused tests and verify GREEN**

Run:

```bash
rtk uv run python -m pytest tests/test_core_infrastructure.py::TestSafeTask -q
```

Expected: all `TestSafeTask` tests pass.

- [ ] **Step 5: Run the existing resilience regression**

Run:

```bash
rtk uv run python -m pytest tests/test_pipeline_resilience.py::TestSafeTaskResilience -q
```

Expected: all tests pass, including the existing `**kwargs` run-ID injection assertion.

- [ ] **Step 6: Verify the original symptom with a minimal fixed-signature callback**

Run:

```bash
rtk uv run python -c 'from core.runner import safe_task; result = safe_task("fixed", lambda: {"saved": 1}); assert result["status"] == "success", result; print(result)'
```

Expected: a normalized result containing `"status": "success"` and no unexpected-keyword traceback.

- [ ] **Step 7: Commit each changed file separately**

First commit `core/runner.py`:

```bash
rtk git add core/runner.py
rtk git commit -m "fix: conditionally inject safe_task run IDs" -m "修复：按回调签名有条件注入 safe_task 运行 ID"
```

Then commit `tests/test_core_infrastructure.py`:

```bash
rtk git add tests/test_core_infrastructure.py
rtk git commit -m "test: cover safe_task callback compatibility" -m "测试：覆盖 safe_task 回调参数兼容性"
```

---

### Task 3: Verify pipeline integration and repository health

**Files:**
- Verify only; no planned file modifications

**Interfaces:**
- Consumes: the final working tree from Tasks 1–2
- Produces: fresh evidence that the original bug is fixed without regressions or test side effects

- [ ] **Step 1: Run orchestration-focused regressions**

Run:

```bash
rtk uv run python -m pytest tests/test_core_infrastructure.py tests/test_daily_pipeline.py tests/test_pipeline_resilience.py -q
```

Expected: all selected tests pass.

- [ ] **Step 2: Run the full test suite**

Run:

```bash
rtk uv run python -m pytest -q
```

Expected: exit code 0 with no failed tests.

- [ ] **Step 3: Run Ruff**

Run:

```bash
rtk uv run ruff check .
```

Expected: exit code 0 and no lint errors.

- [ ] **Step 4: Check whitespace and existing user changes**

Run:

```bash
rtk git diff --check
rtk git status --short
```

Expected: no whitespace errors; `providers.py` and `migrations/006_reconcile_ingestion_audit.py` remain present and uncommitted exactly as before.

- [ ] **Step 5: Check for test-generated repository-root artifacts**

Run:

```bash
rtk find . -maxdepth 1 -type f -name '<MagicMock*' -print
```

Expected: no output.

- [ ] **Step 6: Report the safe launch command**

After all verification succeeds, report:

```bash
pipe-tui
```

Do not automatically start a production full-data run as part of the test suite.

## Verification-discovered extension addendum

Task 3 verification found root-level MagicMock SQLite artifacts. Diagnosis
isolated the two finance-flow tests and `_get_etf_update_range`; a mocked
`db_path` was being treated as a SQLite filename. The plan was extended with
the shared `is_real_db_path` guard and a no-artifact regression test.

Final review also required effective caller-provided run-ID alignment: when a
compatible callback receives a non-empty caller-supplied `_task_run_id`, the
same ID must be written to the ingestion audit record. It additionally
required positive `_get_etf_update_range` coverage for both `str` and `Path`
database paths.
