"""Coverage for the small pure helpers in ``core.utils``."""

from __future__ import annotations

import importlib

import pandas as pd
import pytest

from core.utils import to_float, warn_if_all_empty


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


def test_to_float_converts_scalars():
    assert to_float(42.0) == 42.0
    assert to_float(42) == 42.0
    assert to_float("12.5") == 12.5


def test_to_float_maps_none_nan_and_invalid_to_none():
    assert to_float(None) is None
    assert to_float(float("nan")) is None
    assert to_float(pd.NA) is None
    assert to_float("abc") is None


def test_to_float_default_rejects_percent_suffix():
    """默认不剥离 ``%``——这是 12 个模块的历史行为，不要为「顺手支持」改掉。

    改为支持会让原本落库为 NULL 的字段突然变成数字，属于静默的数据语义变更。
    """
    assert to_float("12.5%") is None


def test_to_float_strip_percent_is_opt_in():
    """``strip_percent=True`` 仅给 ``tasks/stock_pledge.py`` 使用。"""
    assert to_float("12.5%", strip_percent=True) == 12.5
    assert to_float(" 12.5% ", strip_percent=True) == 12.5
    assert to_float(None, strip_percent=True) is None
    assert to_float("abc%", strip_percent=True) is None


_SHARED_TO_FLOAT_MODULES = (
    "tasks.bars_snapshot",
    "tasks.china_macro",
    "tasks.concept_board",
    "tasks.convertible_bond",
    "tasks.corporate_actions",
    "tasks.finance_flow",
    "tasks.hkscc_holder",
    "tasks.market_valuation",
    "tasks.money_market",
    "tasks.option_sentiment",
    "tasks.sector_derivatives",
    "tasks.stock_repurchase",
)


@pytest.mark.parametrize("module_name", _SHARED_TO_FLOAT_MODULES)
def test_task_modules_share_the_single_to_float(module_name: str):
    """P2-10 回归：这些模块不再各自复制 ``_to_float``，而是共用同一个实现。

    重新复制一份会在这里变红；``tasks/stock_pledge.py`` 是有意保留的适配器
    （唯一语义分叉：剥离 ``%``），单独由 ``test_phase1_tasks`` 钉住。
    """
    module = importlib.import_module(module_name)
    assert module._to_float is to_float


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
