"""Canonical source-record keys for idempotent ingestion writes."""

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
    """Return a stable SHA-256 key for ordered business fields."""
    payload = [_normalize(record.get(field)) for field in fields]
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode(
        "utf-8"
    )
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
            text = str(value).strip()
            return text if text else None
    return value
