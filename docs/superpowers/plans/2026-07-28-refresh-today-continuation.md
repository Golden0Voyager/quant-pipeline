# Refresh Today Continuation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Resume the safely checkpointed `--refresh-today` feature, independently review Tasks 3–4, then complete CLI integration, all 29 trading-day adapters, data-quality sampling, TUI support, and end-to-end acceptance.

**Architecture:** Keep `--refresh-today` separate from legacy `--force`. Fetch and validate outside formal tables, publish through `SQLiteRefreshStore`, persist run/task audits, and execute adapters through `RefreshOrchestrator`; failures retain old data and produce a nonzero degraded result.

**Tech Stack:** Python 3.12, SQLite, AkShare, pytest, Ruff, mypy, Textual, `uv`, `rtk`.

## Global Constraints

- Work only in `/Users/hainingyu/Code/quant_pipeline/.worktrees/refresh-today`.
- Branch: `feat/refresh-today`; do not develop on `main`.
- Use `rtk` for shell commands and `uv` for Python tooling.
- One file per commit; commit messages must contain an English subject and a Chinese body.
- Do not access or mutate `~/Code/quant_data/quant_core.db` during tests.
- Do not merge, push, or run `/git-feature done` until all remaining gates pass.
- Preserve the unrelated dirty `tasks/index_chain.py` change in the parent checkout.

---

## Checkpoint

- Checkpoint commit: `dc0ef1e` (`feat: add close refresh audit rules`).
- Task 1 policy registry: independently reviewed, Spec PASS and Quality PASS.
- Task 2 atomic SQLite store: independently reviewed, Spec PASS and Quality PASS.
- Task 3 refresh-run persistence: implementation and all initial review fixes committed; final independent re-review is still required.
- Task 4 orchestrator/audit: implementation committed and locally verified; independent review is still required.
- Last combined gate:

```text
145 passed in 0.77s
All checks passed!
git diff --check: clean
```

- Canonical full design and implementation detail:
  - `docs/superpowers/specs/2026-07-28-refresh-today-trading-day-tasks-design.md`
  - `docs/superpowers/plans/2026-07-28-refresh-today-trading-day-tasks.md`
- Task reports and prior review evidence are under `.superpowers/sdd/`.

### Resume verification

- [ ] **Step 1: Confirm the isolated checkout**

```bash
cd /Users/hainingyu/Code/quant_pipeline/.worktrees/refresh-today
rtk git status --short
rtk git branch --show-current
rtk git log -5 --oneline
```

Expected: branch `feat/refresh-today`; no tracked source changes before new work.

- [ ] **Step 2: Re-run the checkpoint gate**

```bash
rtk uv run python -m pytest -q \
  tests/test_migrations.py \
  tests/test_refresh_store.py \
  tests/test_refresh.py \
  tests/test_refresh_audit.py
rtk uv run ruff check \
  core/refresh_store.py \
  migrations/010_refresh_runs.py \
  core/refresh.py \
  core/refresh_audit.py \
  tests/test_migrations.py \
  tests/test_refresh_store.py \
  tests/test_refresh.py \
  tests/test_refresh_audit.py
rtk git diff --check
```

Expected: 145 tests pass, Ruff passes, and diff check is clean.

---

### Task A: Independently Re-review Tasks 3 and 4

**Files:**

- Review: `migrations/010_refresh_runs.py`
- Review: `core/refresh_store.py`
- Review: `core/refresh.py`
- Review: `core/refresh_audit.py`
- Review: corresponding four test files

**Interfaces:**

- Consumes: `SQLiteRefreshStore.start_run`, `record_task_result`, `finish_run`
- Produces: accepted Task 3 and Task 4 review verdicts before integration work

- [ ] **Step 1: Generate bounded review packages**

```bash
rtk /Users/hainingyu/.agents/skills/subagent-driven-development/scripts/review-package \
  78fde1f 234ed24 .superpowers/sdd/task-3-final-review-package.md
rtk /Users/hainingyu/.agents/skills/subagent-driven-development/scripts/review-package \
  234ed24 dc0ef1e .superpowers/sdd/task-4-review-package.md
```

- [ ] **Step 2: Review Task 3**

Check lifecycle transitions, status/count constraints, arbitrary `Mapping`
metadata, non-finite JSON rejection, migration rollback, and isolation between
audit failures and committed business replacements.

Expected: Spec PASS and Quality PASS with no Critical/Important findings.

- [ ] **Step 3: Review Task 4**

Check Shanghai time gates, deterministic target dates, dependency ordering,
shared-table serialization, retry count, retained-old-data behavior, run/task
audit lifecycle, symbol filtering, and final degraded/nonzero semantics.

Expected: Spec PASS and Quality PASS with no Critical/Important findings.

- [ ] **Step 4: Fix and re-review any Critical/Important finding**

Use test-first fixes, run only the affected focused suite, commit each file
separately, regenerate a bounded review package, and require PASS before Task B.

---

### Task B: Add CLI Mode and Uncached Provider Construction

Execute **Task 5** in the canonical full plan.

**Files:**

- Modify: `daily_pipeline.py`
- Modify: `providers.py`
- Modify: `tests/test_daily_pipeline.py`
- Modify: `tests/test_providers.py`

**Required acceptance:**

- `--refresh-today` is a distinct top-level mode.
- No flags retain legacy `all`.
- `--task` and `--resume` conflict with refresh mode.
- Before 16:00 Asia/Shanghai, refresh fails unless `--force`.
- Refresh uses the global write lock and an uncached loader.
- Degraded, failed, or aborted refresh exits nonzero.

Run the exact Task 5 pytest/Ruff commands in the canonical plan and review
before continuing.

---

### Task C: Implement Core Remote and Derived Adapters

Execute **Tasks 6 and 7** in the canonical full plan.

**Required sequencing:**

1. Bars, fundamentals, Xueqiu field-owned enrichment, and fund flow.
2. Indicators, historical valuation, sector industry, local chip distribution,
   and Eastmoney chip distribution.

**Safety requirements:**

- Bypass completion shortcuts and caches only in close-refresh mode.
- Never use yfinance fallback for writes.
- Xueqiu may update only `dividend_yield`.
- Derived tasks consume only successfully refreshed upstream rows/symbols.
- Reconcile the parent checkout's unrelated `tasks/index_chain.py` change
  deliberately; do not overwrite or silently omit it.

Run each task's focused tests and independent review before the next task.

---

### Task D: Repair Event Keys and Implement Remaining Adapters

Execute **Tasks 8 and 9** in the canonical full plan.

**Required ordering:**

1. Apply migration 011 and deterministic `source_record_key` generation.
2. Only then enable refresh adapters for dragon tiger, block trades, and stock
   pledge.
3. Implement the remaining index, ETF, margin, convertible-bond, cross-market,
   board, valuation, option, event, and composite sector adapters.

**Safety requirements:**

- Preserve multiple legal same-stock/same-day events.
- Treat Northbound flow as degraded `dead_source` and retain old data.
- Distinguish legitimate empty pools from source failure.
- Use `allow_empty=True` only after an adapter proves an authoritative empty
  result.
- Publish sector derivatives as one composite transaction.

---

### Task E: Add Cross-source Sampling and TUI Support

Execute **Tasks 10 and 11** in the canonical full plan.

**Required acceptance:**

- Sampling is deterministic and persisted in task metadata.
- Sampling failure degrades the task without destroying accepted old data.
- `pipe-tui` exposes `--refresh-today`, target date, task state, refreshed and
  retained counts, and failure summaries.
- TUI cancellation/termination preserves the run audit as aborted or degraded.

---

### Task F: End-to-end Acceptance, Documentation, and Branch Completion

Execute **Task 12** in the canonical full plan.

- [ ] **Step 1: Run temporary-database end-to-end acceptance**

Prove close rows replace intraday rows, historical partitions remain unchanged,
validation/source failures retain old data, composite writes roll back, all 29
tasks persist outcomes, and degraded completion exits nonzero.

- [ ] **Step 2: Run full repository gates**

```bash
rtk uv run ruff check .
rtk uv run mypy .
rtk uv run python -m pytest -q
rtk git diff --check
rtk git status --short
```

Expected: all configured gates pass; no stray repository-root
`<MagicMock ...>` files; only intentional tracked changes remain.

- [ ] **Step 3: Update operator documentation**

Document the 16:00 Shanghai gate, explicit `--force` override, supported
`--symbols` tasks, retained-old-data states, retry workflow, and examples for
CLI and `pipe-tui`.

- [ ] **Step 4: Independently review the complete feature**

Review the entire range from baseline `00ba3d3` to final HEAD, with special
attention to destructive SQL scope, cache bypass, shared-table ownership,
event-key migrations, and nonzero exit behavior.

- [ ] **Step 5: Finish the feature branch**

Only after the full review and CI gates pass, invoke `git-feature done` for PR,
CI, merge, and cleanup. Do not rewrite or discard the parent checkout's
unrelated `tasks/index_chain.py` work.

