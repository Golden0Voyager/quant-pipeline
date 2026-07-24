"""Tests for financial history — report-period discovery, merge, PIT storage."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from tasks.financial_history import (
    _closed_report_periods,
    _minimum_market_coverage,
    _normalize_code,
    discover_missing_financial_periods,
    update_financial_history,
)


class TestNormalizeCode:
    def test_plain_code(self) -> None:
        assert _normalize_code("000001") == "000001"

    def test_sh_suffix(self) -> None:
        assert _normalize_code("600519.SH") == "600519"

    def test_sz_suffix(self) -> None:
        assert _normalize_code("000001.SZ") == "000001"

    def test_bj_suffix(self) -> None:
        assert _normalize_code("830799.BJ") == "830799"

    def test_lowercase_suffix(self) -> None:
        assert _normalize_code("000001.sz") == "000001"

    def test_short_code(self) -> None:
        assert _normalize_code("1") == "000001"


class TestClosedReportPeriods:
    def test_returns_expected_format(self) -> None:
        periods = _closed_report_periods(date(2026, 6, 30), lookback_quarters=4)
        assert len(periods) == 4
        for p in periods:
            assert len(p) == 8
            assert p.isdigit()

    def test_omits_open_quarter(self) -> None:
        """2026-07-15 (Q3 in progress) should not include 20260930."""
        periods = _closed_report_periods(date(2026, 7, 15), lookback_quarters=4)
        assert "20260930" not in periods
        assert "20260630" in periods


class TestMinimumMarketCoverage:
    def test_within_disclosure_window(self) -> None:
        cov = _minimum_market_coverage("20260630", date(2026, 9, 30))
        assert cov == 0.6

    def test_after_disclosure_window(self) -> None:
        cov = _minimum_market_coverage("20260331", date(2026, 9, 30))
        assert cov == 0.9


class TestDiscoverMissingPeriods:
    def test_all_covered(self) -> None:
        db = MagicMock()
        # Cover all 8 lookback quarters
        db.get_financial_period_coverage.return_value = {
            "20260630": 5000,
            "20260331": 5000,
            "20251231": 5000,
            "20250930": 5000,
            "20250630": 5000,
            "20250331": 5000,
            "20241231": 5000,
            "20240930": 5000,
        }
        db.get_stock_list.return_value.type = "DataFrame"
        missing = discover_missing_financial_periods(db, as_of_date=date(2026, 7, 15))
        assert missing == []

    def test_some_missing(self) -> None:
        db = MagicMock()
        db.get_financial_period_coverage.return_value = {
            "20260630": 100,
            "20260331": 5000,
            "20251231": 5000,
        }
        db.get_stock_list.return_value.type = "DataFrame"
        db.get_stock_list.return_value.empty = False
        db.get_stock_list.return_value.__len__ = lambda self: 5500
        with patch.object(db.get_stock_list.return_value, "empty", False):
            missing = discover_missing_financial_periods(db, as_of_date=date(2026, 7, 15))
        assert "20260630" in missing


class TestUpdateFinancialHistory:
    def test_ak_none(self) -> None:
        db = MagicMock()
        with patch("tasks.financial_history.ak", None):
            result = update_financial_history(db, periods=["20260630"])
        assert result.get("error") == "akshare not installed"

    def test_explicit_periods_empty(self) -> None:
        db = MagicMock()
        with patch("tasks.financial_history.ak", object()):
            result = update_financial_history(db, periods=[])
        assert result["saved"] == 0
        assert result.get("error") is None
