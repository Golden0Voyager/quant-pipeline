"""Tests for financial history — report-period discovery, merge, PIT storage."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

from tasks.financial_history import (
    _closed_report_periods,
    _cninfo_period,
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


class TestCninfoPeriod:
    """`_cninfo_period` 把数字期次转成巨潮资讯接口需要的中文标签。

    回归保护：早期直接把 ``"20260630"`` 传给 ``stock_report_disclosure``
    会触发 ``KeyError('20260630')``，导致整个财务历史更新失败。
    """

    def test_q1(self) -> None:
        assert _cninfo_period("20260331") == "2026一季"

    def test_interim(self) -> None:
        assert _cninfo_period("20260630") == "2026半年报"

    def test_q3(self) -> None:
        assert _cninfo_period("20260930") == "2026三季"

    def test_annual(self) -> None:
        assert _cninfo_period("20251231") == "2025年报"

    def test_unknown_mmdd_falls_back_to_annual(self) -> None:
        assert _cninfo_period("20269999") == "2026年报"


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

    def test_no_periods_skipped_not_failed(self) -> None:
        """合法零行必须带 skipped 标记，避免被契约误判为 failed。

        回归保护：2026-08-25 全量运行中"所有报告期数据已覆盖，无需更新"
        被结果契约误报为 zero rows without explanation。
        """
        from core.task_result import normalize_task_result

        db = MagicMock()
        with patch("tasks.financial_history.ak", object()):
            result = update_financial_history(db, periods=[])
        assert result["skipped"] is True
        normalized = normalize_task_result("update_financial_history", result)
        assert normalized.status.value == "no_data"

    def test_period_retry_then_success(self) -> None:
        db = MagicMock()
        db.save_financial_history_batch.return_value = {"history_saved": 1}
        merge = MagicMock(side_effect=[Exception("boom"), Exception("boom"), [{"x": 1}]])
        with (
            patch("tasks.financial_history.ak", object()),
            patch("tasks.financial_history._merge_financial_period", merge),
            patch("tasks.financial_history.time.sleep"),
        ):
            result = update_financial_history(db, periods=["20260630"])

        assert result["saved"] == 1
        assert merge.call_count == 3

    def test_all_periods_failed_is_retained(self) -> None:
        db = MagicMock()
        merge = MagicMock(side_effect=Exception("boom"))
        with (
            patch("tasks.financial_history.ak", object()),
            patch("tasks.financial_history._merge_financial_period", merge),
            patch("tasks.financial_history.time.sleep"),
        ):
            result = update_financial_history(db, periods=["20260630", "20260331"])

        assert result["status"] == "retained"
        assert result["retained_old_data"] is True
        assert result["metadata"]["failed_periods"] == ["20260630", "20260331"]
        db.save_financial_history_batch.assert_not_called()

    def test_partial_failure_keeps_current_contract(self) -> None:
        db = MagicMock()
        db.save_financial_history_batch.return_value = {"history_saved": 1}
        merge = MagicMock(side_effect=[Exception("boom"), [{"x": 1}]])
        with (
            patch("tasks.financial_history.ak", object()),
            patch("tasks.financial_history._merge_financial_period", merge),
            patch("tasks.financial_history.time.sleep"),
        ):
            result = update_financial_history(db, periods=["20260630", "20260331"])

        assert result["saved"] > 0
        assert "error" in result

    def test_failed_period_is_persisted_for_next_run(self) -> None:
        """失败的报告期要落进重试队列，供下次自动重试。"""
        from tasks import financial_history as fh

        db = MagicMock()
        merge = MagicMock(side_effect=Exception("boom"))
        with (
            patch("tasks.financial_history.ak", object()),
            patch("tasks.financial_history._merge_financial_period", merge),
            patch("tasks.financial_history.time.sleep"),
        ):
            update_financial_history(db, periods=["20260630"])

        assert fh.load_failed_periods() == ["20260630"]

    def test_success_clears_persisted_period(self) -> None:
        """定向修复成功后，该报告期必须从队列移除，且不动其他期次。"""
        from tasks import financial_history as fh

        db = MagicMock()
        db.save_financial_history_batch.return_value = {"history_saved": 1}
        fh.record_failed_periods(["20260630", "20260331"])
        merge = MagicMock(return_value=[{"x": 1}])
        with (
            patch("tasks.financial_history.ak", object()),
            patch("tasks.financial_history._merge_financial_period", merge),
            patch("tasks.financial_history.time.sleep"),
        ):
            update_financial_history(db, periods=["20260630"])

        assert fh.load_failed_periods() == ["20260331"]

    def test_queued_period_retried_when_discovery_finds_nothing(self) -> None:
        """覆盖率已达标的报告期：发现逻辑不返回，但队列里的失败期次仍要重试。"""
        from tasks import financial_history as fh

        db = MagicMock()
        db.save_financial_history_batch.return_value = {"history_saved": 1}
        fh.record_failed_periods(["20251231"])
        merge = MagicMock(return_value=[{"x": 1}])
        with (
            patch("tasks.financial_history.ak", object()),
            patch("tasks.financial_history._merge_financial_period", merge),
            patch(
                "tasks.financial_history.discover_missing_financial_periods",
                return_value=[],
            ),
            patch("tasks.financial_history.time.sleep"),
        ):
            update_financial_history(db)  # periods=None → 自动发现路径

        merge.assert_called_once_with("20251231")
        assert fh.load_failed_periods() == []


class TestFailedPeriodQueue:
    """失败报告期队列的读写语义（仿 bars 的 failed_symbols 队列）。"""

    def test_missing_file_returns_empty(self) -> None:
        from tasks import financial_history as fh

        assert fh.load_failed_periods() == []

    def test_record_merges_dedupes_and_sorts(self) -> None:
        from tasks import financial_history as fh

        fh.record_failed_periods(["20260630", "20260331"])
        fh.record_failed_periods(["20260630", "20251231"])
        assert fh.load_failed_periods() == ["20251231", "20260331", "20260630"]

    def test_clear_removes_only_named_periods(self) -> None:
        from tasks import financial_history as fh

        fh.record_failed_periods(["20260630", "20260331"])
        assert fh.clear_failed_periods(["20260630"]) == ["20260331"]
        assert fh.load_failed_periods() == ["20260331"]

    def test_clearing_last_period_removes_the_file(self) -> None:
        from tasks import financial_history as fh

        fh.record_failed_periods(["20260630"])
        fh.clear_failed_periods(["20260630"])
        assert not fh._FAILED_PERIODS_FILE.exists()

    def test_corrupt_file_is_tolerated(self) -> None:
        """文件损坏不能抛异常，只能当作空队列。"""
        from tasks import financial_history as fh

        fh._FAILED_PERIODS_FILE.parent.mkdir(parents=True, exist_ok=True)
        fh._FAILED_PERIODS_FILE.write_text("{not json", encoding="utf-8")
        assert fh.load_failed_periods() == []
