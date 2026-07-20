"""Coverage for the small pure helpers in ``core.utils``."""

from __future__ import annotations

from core.utils import warn_if_all_empty


def test_warn_if_all_empty_skips_on_empty_records():
    # No records -> trivially valid, no warning.
    assert warn_if_all_empty([], key_cols=["ts_code"], task_name="cb") is True


def test_warn_if_all_empty_valid_when_any_key_has_value():
    records = [
        {"ts_code": "", "price": None},
        {"ts_code": "000001", "price": 120.0},
    ]
    assert (
        warn_if_all_empty(records, key_cols=["ts_code", "price"], task_name="cb") is True
    )


def test_warn_if_all_empty_flags_all_empty(caplog):
    records = [
        {"ts_code": None, "price": ""},
        {"ts_code": "", "price": None},
    ]
    assert (
        warn_if_all_empty(records, key_cols=["ts_code", "price"], task_name="cb")
        is False
    )
    assert any("数据质量告警" in rec.message for rec in caplog.records)
