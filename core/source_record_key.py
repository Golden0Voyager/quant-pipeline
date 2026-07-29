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

# 事件表（Task 8）：同股同日合法多事件必须互异 → 全业务字段入键；
# 质押为自然键 UPSERT → 度量字段排除在键外，重新抓取时走更新。
DRAGON_TIGER_SOURCE_KEY_FIELDS = (
    "trade_date",
    "ts_code",
    "close_price",
    "pct_change",
    "net_buy_amount",
    "buy_amount",
    "sell_amount",
    "turnover_rate",
    "market_cap",
    "reason",
)

BLOCK_TRADE_SOURCE_KEY_FIELDS = (
    "trade_date",
    "ts_code",
    "deal_price",
    "close_price",
    "discount_rate",
    "volume",
    "amount",
    "buyer_branch",
    "seller_branch",
)

STOCK_PLEDGE_SOURCE_KEY_FIELDS = (
    "trade_date",
    "stock_code",
    "pledger",
)

_DRAGON_TIGER_NUMERIC_FIELDS = frozenset({
    "close_price",
    "pct_change",
    "net_buy_amount",
    "buy_amount",
    "sell_amount",
    "turnover_rate",
    "market_cap",
})

_BLOCK_TRADE_NUMERIC_FIELDS = frozenset({
    "deal_price",
    "close_price",
    "discount_rate",
    "volume",
    "amount",
})


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


# ── 事件表键构建器（Task 8） ────────────────────────────────────────


def _coerce_float(value: Any) -> Any:
    """数值字段统一为 float，避免 int/REAL 往返导致哈希漂移。"""
    normalized = _normalize(value)
    if normalized is None or isinstance(normalized, bool):
        return normalized
    if isinstance(normalized, (int, float)):
        return float(normalized)
    try:
        return float(str(normalized).strip())
    except (TypeError, ValueError):
        return normalized


def _event_key(
    record: Mapping[str, Any],
    fields: Sequence[str],
    numeric_fields: frozenset[str],
) -> str:
    payload = {
        field: (
            _coerce_float(record.get(field))
            if field in numeric_fields
            else record.get(field)
        )
        for field in fields
    }
    return source_record_key(payload, fields)


def dragon_tiger_source_key(record: Mapping[str, Any]) -> str:
    """Stable key for one dragon-tiger listing event."""
    return _event_key(
        record, DRAGON_TIGER_SOURCE_KEY_FIELDS, _DRAGON_TIGER_NUMERIC_FIELDS
    )


def block_trade_source_key(record: Mapping[str, Any]) -> str:
    """Stable key for one block-trade deal."""
    return _event_key(
        record, BLOCK_TRADE_SOURCE_KEY_FIELDS, _BLOCK_TRADE_NUMERIC_FIELDS
    )


def stock_pledge_source_key(record: Mapping[str, Any]) -> str:
    """Stable natural key for one pledge row (null-pledger tolerant)."""
    return _event_key(record, STOCK_PLEDGE_SOURCE_KEY_FIELDS, frozenset())
