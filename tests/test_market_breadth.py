"""Tests for the market breadth task (legu 新高新低 / 破净 / 赚钱效应)."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd

import tasks.market_breadth as market_breadth
from core.task_result import TaskStatus, normalize_task_result


def _client_returning(df):
    client = MagicMock()
    client.call.return_value = SimpleNamespace(success=True, data=df)
    return client


class TestToInt:
    def test_float_and_int(self) -> None:
        assert market_breadth._to_int(653.0) == 653
        assert market_breadth._to_int(12) == 12

    def test_none_and_blank(self) -> None:
        assert market_breadth._to_int(None) is None
        assert market_breadth._to_int("") is None
        assert market_breadth._to_int("  ") is None

    def test_non_numeric(self) -> None:
        assert market_breadth._to_int("abc") is None


class TestFetchHighLow:
    def test_maps_columns_and_truncates_date(self) -> None:
        df = pd.DataFrame([
            {
                "date": "2026-09-24",
                "close": 3888.37,
                "high20": 265,
                "low20": 938,
                "high60": 148,
                "low60": 242,
                "high120": 51,
                "low120": 162,
            }
        ])
        with patch(
            "tasks.market_breadth.get_default_client",
            return_value=_client_returning(df),
        ):
            records = market_breadth._fetch_high_low()

        assert records == [
            {
                "date": "2026-09-24",
                "close": 3888.37,
                "high20": 265,
                "low20": 938,
                "high60": 148,
                "low60": 242,
                "high120": 51,
                "low120": 162,
            }
        ]

    def test_empty_on_failed_response(self) -> None:
        client = MagicMock()
        client.call.return_value = SimpleNamespace(success=False, data=None)
        with patch("tasks.market_breadth.get_default_client", return_value=client):
            assert market_breadth._fetch_high_low() == []


class TestFetchActivity:
    def test_parses_counts_percent_and_stat_date(self) -> None:
        df = pd.DataFrame([
            {"item": "上涨", "value": 1084.0},
            {"item": "涨停", "value": 52.0},
            {"item": "st st*涨停", "value": 0.0},
            {"item": "下跌", "value": 4001.0},
            {"item": "活跃度", "value": "20.76%"},
            {"item": "统计日期", "value": "2026-09-24 15:00:00"},
        ])
        with patch(
            "tasks.market_breadth.get_default_client",
            return_value=_client_returning(df),
        ):
            record = market_breadth._fetch_activity()

        assert record == {
            "up_count": 1084,
            "limit_up": 52,
            "st_limit_up": 0,
            "down_count": 4001,
            "activity_ratio": 20.76,
            "date": "2026-09-24",
        }

    def test_missing_stat_date_returns_none(self) -> None:
        df = pd.DataFrame([{"item": "上涨", "value": 1084.0}])
        with patch(
            "tasks.market_breadth.get_default_client",
            return_value=_client_returning(df),
        ):
            assert market_breadth._fetch_activity() is None


class TestUpdateMarketBreadth:
    def test_ak_none(self) -> None:
        with patch("tasks.market_breadth.ak", None):
            result = market_breadth.update_market_breadth(MagicMock())
        assert result.get("error") == "akshare not installed"

    def test_merges_sources_by_date_and_saves(self) -> None:
        db = MagicMock()
        db.save_market_breadth_batch.return_value = 2
        high_low = [
            {"date": "2026-09-23", "close": 3936.52, "high20": 624, "low20": 522},
            {"date": "2026-09-24", "close": 3888.37, "high20": 265, "low20": 938},
        ]
        below = [{"date": "2026-09-24", "below_net_asset": 300, "total_company": 5400}]
        activity = {"date": "2026-09-24", "up_count": 1084, "down_count": 4001}
        with (
            patch("tasks.market_breadth.ak", object()),
            patch("tasks.market_breadth._fetch_high_low", return_value=high_low),
            patch("tasks.market_breadth._fetch_below_net_asset", return_value=below),
            patch("tasks.market_breadth._fetch_activity", return_value=activity),
        ):
            result = market_breadth.update_market_breadth(db)

        assert result["saved"] == 2
        records = db.save_market_breadth_batch.call_args.args[0]
        assert [r["date"] for r in records] == ["2026-09-23", "2026-09-24"]
        # 三个来源在同一天合并进一行
        assert records[1]["high20"] == 265
        assert records[1]["below_net_asset"] == 300
        assert records[1]["up_count"] == 1084
        assert records[0]["data_source"] == "legu"

    def test_all_sources_empty_is_skipped(self) -> None:
        with (
            patch("tasks.market_breadth.ak", object()),
            patch("tasks.market_breadth._fetch_high_low", return_value=[]),
            patch("tasks.market_breadth._fetch_below_net_asset", return_value=[]),
            patch("tasks.market_breadth._fetch_activity", return_value=None),
        ):
            result = market_breadth.update_market_breadth(MagicMock())

        assert result["skipped"] is True
        assert normalize_task_result(
            "update_market_breadth", result
        ).status is TaskStatus.NO_DATA

    def test_all_sources_failed_is_retained(self) -> None:
        with (
            patch("tasks.market_breadth.ak", object()),
            patch(
                "tasks.market_breadth._fetch_high_low",
                side_effect=ConnectionError("down"),
            ),
            patch(
                "tasks.market_breadth._fetch_below_net_asset",
                side_effect=ConnectionError("down"),
            ),
            patch("tasks.market_breadth._fetch_activity", return_value=None),
        ):
            result = market_breadth.update_market_breadth(MagicMock())

        assert result["status"] == "retained"
        assert result["retained_old_data"] is True
        assert normalize_task_result(
            "update_market_breadth", result
        ).status is TaskStatus.RETAINED

    def test_partial_source_failure_does_not_fail_the_task(self) -> None:
        """已知上游漂移（如破净接口）不能把 partial 结果天天报成 failed。

        ``normalize_task_result`` 见到顶层 ``error`` 键会直接判 FAILED；因此
        partial 失败必须放进 metadata。
        """
        db = MagicMock()
        db.save_market_breadth_batch.return_value = 1
        with (
            patch("tasks.market_breadth.ak", object()),
            patch(
                "tasks.market_breadth._fetch_high_low",
                return_value=[{"date": "2026-09-24", "close": 3888.37}],
            ),
            patch(
                "tasks.market_breadth._fetch_below_net_asset",
                side_effect=KeyError("marketId"),
            ),
            patch("tasks.market_breadth._fetch_activity", return_value=None),
        ):
            result = market_breadth.update_market_breadth(db)

        assert result["saved"] == 1
        assert "error" not in result
        assert result["metadata"]["source_errors"] == ["破净统计: 'marketId'"]
        assert normalize_task_result(
            "update_market_breadth", result
        ).status is TaskStatus.SUCCESS
