"""DataFrame-level data contracts that block writes when violated.

Each ``DataContract`` declares required columns, aliases, non-null ratios,
numeric bounds, uniqueness constraints and rejection thresholds. The
``validate_frame`` function compares a raw DataFrame against the contract
and returns a ``ValidationResult`` with accepted / rejected rows and a
structured violation list.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field

import pandas as pd


@dataclass(frozen=True)
class FieldRule:
    """Constraint for a single field / column.

    Attributes
    ----------
    name:
        Canonical column name.
    aliases:
        Alternative source column names that are mapped to *name* at
        validation time.
    required:
        When True the column (or one of its aliases) must exist in the
        source frame.
    nullable:
        Whether null values are allowed in this column.
    min_non_null_ratio:
        Minimum fraction of non-null values required (0.0 = no
        requirement, 1.0 = no nulls allowed).
    minimum, maximum:
        Numeric bounds. Values outside this range are rejected.
    """

    name: str
    aliases: tuple[str, ...] = ()
    required: bool = False
    nullable: bool = True
    min_non_null_ratio: float = 0.0
    minimum: float | None = None
    maximum: float | None = None


@dataclass(frozen=True)
class DataContract:
    """Contract that a DataFrame must satisfy to be written.

    Attributes
    ----------
    name:
        Human-readable contract name (e.g. ``"market_valuation"``).
    fields:
        Field-level rules.
    unique_by:
        Column name(s) that must form a unique key (duplicates are
        rejected).
    required_any_of:
        Groups of canonical field names where at least one field in
        each group must be present and have non-null values.
    min_rows:
        Minimum number of rows the frame must contain.
    max_rejected_ratio:
        Maximum fraction of rows that may be rejected (0.02 = 2%).
    """

    name: str
    fields: tuple[FieldRule, ...]
    unique_by: tuple[str, ...]
    required_any_of: tuple[tuple[str, ...], ...] = ()
    min_rows: int = 1
    max_rejected_ratio: float = 0.02


@dataclass
class ValidationResult:
    """Outcome of validating a DataFrame against a ``DataContract``."""

    accepted: pd.DataFrame
    rejected: pd.DataFrame
    violations: list[str] = field(default_factory=list)
    source_columns: tuple[str, ...] = ()
    schema_fingerprint: str = ""

    @property
    def can_write(self) -> bool:
        """True when there are no blocking violations."""
        return len(self.violations) == 0


# ═══════════════════════════════════════════════════════════════════════
# Validation
# ═══════════════════════════════════════════════════════════════════════


def _resolve_field(
    field: FieldRule, frame: pd.DataFrame
) -> tuple[str, bool]:
    """Resolve *field* to an actual column in *frame*.

    Returns ``(resolved_name, found)`` where *found* is True when the
    column (or one of its aliases) exists.
    """
    if field.name in frame.columns:
        return field.name, True
    for alias in field.aliases:
        if alias in frame.columns:
            return alias, True
    return field.name, False


def _schema_fingerprint(frame: pd.DataFrame) -> str:
    """SHA-256 over sorted ``column_name:dtype`` entries."""
    entries = sorted(f"{col}:{frame[col].dtype}" for col in frame.columns)
    return hashlib.sha256(
        ("|".join(entries)).encode("utf-8")
    ).hexdigest()


def validate_frame(
    frame: pd.DataFrame,
    contract: DataContract,
) -> ValidationResult:
    """Validate *frame* against *contract*.

    Returns a ``ValidationResult`` with accepted / rejected rows,
    violation messages, source columns and schema fingerprint.
    """
    violations: list[str] = []
    source_columns = tuple(frame.columns.tolist())
    fingerprint = _schema_fingerprint(frame)

    if len(frame) < contract.min_rows:
        violations.append(
            f"frame has {len(frame)} rows, minimum is {contract.min_rows}"
        )

    # ── 1. Column existence (required + required_any_of) ──────
    resolved_map: dict[str, str] = {}  # canonical → actual column name

    for rule in contract.fields:
        actual, found = _resolve_field(rule, frame)
        if rule.required and not found:
            aliases = list(rule.aliases)
            violations.append(
                f"required column '{rule.name}' not found"
                + (f" (aliases: {aliases})" if aliases else "")
            )
        if found:
            resolved_map[rule.name] = actual

    for group in contract.required_any_of:
        present = [c for c in group if c in resolved_map]
        if not present:
            violations.append(
                f"required_any_of group {list(group)}: none present"
            )

    # If required columns are missing there is no point validating
    # row-level rules — abort early.
    if violations:
        empty = frame.iloc[:0].copy() if len(frame) > 0 else frame.copy()
        return ValidationResult(
            accepted=empty,
            rejected=frame.copy() if len(frame) > 0 else frame.copy(),
            violations=violations,
            source_columns=source_columns,
            schema_fingerprint=fingerprint,
        )

    # ── 2. Row-level validation ───────────────────────────────
    mask_keep = pd.Series(True, index=frame.index)

    for rule in contract.fields:
        actual_col = resolved_map.get(rule.name)
        if actual_col is None:
            continue
        col = frame[actual_col]

        # Non-null ratio
        if rule.min_non_null_ratio > 0:
            non_null_frac = col.notna().mean()
            if non_null_frac < rule.min_non_null_ratio:
                violations.append(
                    f"'{rule.name}' non-null ratio {non_null_frac:.3f} "
                    f"< minimum {rule.min_non_null_ratio}"
                )

        # Numeric bounds
        if (rule.minimum is not None or rule.maximum is not None) and pd.api.types.is_numeric_dtype(col):
            if rule.minimum is not None:
                below = col.notna() & (col < rule.minimum)
                if below.any():
                    n_below = below.sum()
                    violations.append(
                        f"'{rule.name}' has {n_below} value(s) "
                        f"below minimum {rule.minimum}"
                    )
                    mask_keep = mask_keep & ~below
            if rule.maximum is not None:
                above = col.notna() & (col > rule.maximum)
                if above.any():
                    n_above = above.sum()
                    violations.append(
                        f"'{rule.name}' has {n_above} value(s) "
                        f"above maximum {rule.maximum}"
                    )
                    mask_keep = mask_keep & ~above

    # Date validity — check each date-typed or date-named column
    for rule in contract.fields:
        actual_col = resolved_map.get(rule.name)
        if actual_col is None:
            continue
        if "date" in rule.name.lower() or "time" in rule.name.lower():
            col = frame[actual_col]
            try:
                parsed = pd.to_datetime(col, errors="coerce")
                invalid = col.notna() & parsed.isna()
                if invalid.any():
                    n_invalid = invalid.sum()
                    violations.append(
                        f"'{rule.name}' has {n_invalid} invalid date "
                        f"value(s)"
                    )
                    mask_keep = mask_keep & ~invalid
            except Exception:
                pass

    # Uniqueness
    if contract.unique_by:
        # Map canonical unique keys to actual column names
        actual_keys = []
        for key in contract.unique_by:
            if key in resolved_map:
                actual_keys.append(resolved_map[key])
        if actual_keys:
            dup_mask = frame.duplicated(subset=actual_keys, keep=False)
            if dup_mask.any():
                n_dup = dup_mask.sum()
                violations.append(
                    f"found {n_dup} duplicate rows by "
                    f"unique key {actual_keys}"
                )
                # Keep first occurrence, mark rest as duplicates
                first_occurrence = ~frame.duplicated(
                    subset=actual_keys, keep="first"
                )
                mask_keep = mask_keep & first_occurrence

    # All-key-fields-empty check: rows where ALL unique-by columns
    # are null are rejected (non-blocking if valid rows remain)
    if contract.unique_by:
        key_cols = [
            resolved_map[k]
            for k in contract.unique_by
            if k in resolved_map
        ]
        if key_cols:
            all_null = frame[key_cols].isna().all(axis=1)
            if all_null.any():
                mask_keep = mask_keep & ~all_null

    # All-metrics-null check for contracts with required_any_of-like
    # semantics: if every numeric/metric field is null for ALL rows
    non_key_fields = [
        resolved_map[f.name]
        for f in contract.fields
        if f.name in resolved_map
        and f.name not in contract.unique_by
        and not f.required
    ]
    if non_key_fields and len(frame) > 0:
        all_metrics_null = frame[non_key_fields].isna().all(axis=1)
        if all_metrics_null.all():
            violations.append(
                "all metric columns are entirely null"
            )

    # Rejected ratio threshold
    rejected = frame[~mask_keep].copy() if mask_keep is not None else frame.copy()
    accepted = frame[mask_keep].copy() if mask_keep is not None else frame.iloc[:0].copy()
    total = len(frame)
    if total > 0 and len(rejected) / total > contract.max_rejected_ratio:
        violations.append(
            f"rejected ratio {len(rejected)}/{total} exceeds "
            f"maximum {contract.max_rejected_ratio}"
        )

    return ValidationResult(
        accepted=accepted,
        rejected=rejected,
        violations=violations,
        source_columns=source_columns,
        schema_fingerprint=fingerprint,
    )
