# Source Record Key Storage Integrity Design

## Context

`stock_repurchase` and `institution_survey` can legitimately contain more than
one record for the same stock and date. The task contracts now accept those
records, but the SQLite tables still enforce `UNIQUE(trade_date, stock_code)`.
The current `INSERT OR REPLACE` / `ON CONFLICT` writes therefore overwrite
valid records while reporting success.

The repair must preserve all distinct source records, remain idempotent when
the same batch is fetched again, and upgrade existing databases without editing
an already-published migration.

## Decision

Add a deterministic `source_record_key` to both tables and make it the only
record-level unique key.

The Provider owns key generation so every caller and task uses the same
canonicalization. It serializes an ordered list of normalized field values as
compact UTF-8 JSON and stores its SHA-256 digest.

Key fields:

- `stock_repurchase`: `trade_date`, `stock_code`, `stock_name`,
  `repurchase_amount`, `repurchase_price`, `repurchase_price_lower`,
  `repurchase_price_upper`, `repurchase_quantity`, `progress_status`.
- `institution_survey`: `trade_date`, `stock_code`, `stock_name`,
  `survey_org`, `survey_type`, `survey_count`.

Normalization rules are deterministic:

- missing values and pandas-style nulls become JSON `null`;
- strings are trimmed;
- dates use the normalized task output;
- numeric values retain their Provider input value;
- JSON uses a fixed field order, compact separators, and UTF-8 characters.

The data contracts use the same complete business-field sets for in-batch
duplicate detection. Exact source duplicates are removed before validation;
distinct plans or surveys remain accepted.

## Migration 009

Create `009_source_record_keys.py`; do not modify migrations 001–008.

For each target table:

1. Detect whether the table exists.
2. Create a replacement table with the current columns plus
   `source_record_key TEXT NOT NULL UNIQUE`.
3. Read existing rows and compute keys with migration-local canonicalization
   equivalent to the Provider algorithm.
4. Insert all existing rows into the replacement table.
5. Swap the replacement table into place and recreate required indexes.

The migration runs inside the MigrationEngine `BEGIN IMMEDIATE` boundary.
Any collision or injected failure rolls back the entire table rebuild and the
version-009 success record. Re-running after a completed migration is skipped
by the migration engine.

## Provider Writes

Provider fallback table creation uses the new schema. Batch writes compute a
key for every valid record and use:

```sql
INSERT ... ON CONFLICT(source_record_key) DO UPDATE SET ...
```

This preserves row identity, keeps repeated identical batches idempotent, and
allows multiple same-stock/same-date records when their business payloads
differ.

## Compatibility

- Existing databases at version 008 upgrade through migration 009.
- Fresh databases apply all migrations and end with the same schema.
- Existing rows are retained and receive deterministic keys.
- `survey_org = NULL` is safe because the hash includes every business field;
  uniqueness does not depend on SQLite's special handling of `NULL`.
- No production database is modified during tests or development verification.

## Test Strategy

Strict RED–GREEN coverage:

1. Provider real-SQLite test retains two distinct same-day repurchase records.
2. Provider real-SQLite test retains two distinct same-day survey records,
   including a `survey_org=None` case.
3. Repeating an identical batch leaves one row and updates it in place.
4. Task-to-contract-to-Provider integration persists distinct records.
5. Migration 009 preserves old rows and removes the legacy unique constraint.
6. Failure injection during migration 009 restores the original tables and
   records version 009 as failed.
7. Fresh migration suite, Provider suite, task suites, full pytest, Ruff, and
   coverage remain green.

## Alternatives Rejected

- Expanding uniqueness only to `progress_status` or `survey_org` still
  collides when those values repeat or are null.
- Removing uniqueness entirely preserves records but duplicates every daily
  rerun.
- Restoring the old task contracts avoids storage changes by rejecting or
  collapsing valid source data, which does not meet the data-integrity goal.
