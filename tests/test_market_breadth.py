"""Tests for the market breadth task (legu 新高新低 / 破净 / 赚钱效应)."""

from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd

import tasks.market_breadth as market_breadth
from core.task_result import TaskStatus, normalize_task_result


def _client_returning(df):
    client = MagicMock()
    client.call.return_value = SimpleNamespace(success=True, data=df)
    return client


def _client_responding(*responses):
    """client.call 依次返回给定的 SourceResponse 形状对象。"""
    client = MagicMock()
    client.call.side_effect = list(responses)
    return client


def _resp(success, data=None, error=None):
    return SimpleNamespace(
        success=success, data=data, metadata=SimpleNamespace(error=error)
    )


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


class TestFailureVisibility:
    """上游挂了不能静默：失败/空数据必须留下 WARNING（旧实现一条日志都没有）。"""

    def test_high_low_failure_logs_source_and_error(self, caplog) -> None:
        client = MagicMock()
        client.call.return_value = _resp(False, None, "HTTP 504 Gateway Time-out")
        with (
            patch("tasks.market_breadth.get_default_client", return_value=client),
            caplog.at_level(logging.WARNING, logger="tasks.market_breadth"),
        ):
            assert market_breadth._fetch_high_low() == []
        assert any("创新高低" in m and "504" in m for m in caplog.messages)

    def test_below_net_asset_failure_logs_source_and_error(self, caplog) -> None:
        client = MagicMock()
        client.call.return_value = _resp(False, None, "JSONDecodeError")
        with (
            patch("tasks.market_breadth.get_default_client", return_value=client),
            caplog.at_level(logging.WARNING, logger="tasks.market_breadth"),
        ):
            assert market_breadth._fetch_below_net_asset() == []
        assert any("破净统计" in m and "JSONDecodeError" in m for m in caplog.messages)

    def test_activity_empty_dataframe_logs_warning(self, caplog) -> None:
        with (
            patch(
                "tasks.market_breadth.get_default_client",
                return_value=_client_returning(pd.DataFrame()),
            ),
            caplog.at_level(logging.WARNING, logger="tasks.market_breadth"),
        ):
            assert market_breadth._fetch_activity() is None
        assert any("赚钱效应" in m and "空数据" in m for m in caplog.messages)


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


class TestFetchActivityEm:
    """东财备用源：涨跌停池计数、ST 拆分、单池失败不拖垮另一侧。"""

    def test_maps_pools_with_st_split(self) -> None:
        up_df = pd.DataFrame([
            {"代码": "600001", "名称": "涨停一"},
            {"代码": "600002", "名称": "ST某某"},
            {"代码": "600003", "名称": "*ST新亿"},
        ])
        down_df = pd.DataFrame([{"代码": "600004", "名称": "跌停股"}])
        client = _client_responding(_resp(True, up_df), _resp(True, down_df))
        with patch("tasks.market_breadth.get_default_client", return_value=client):
            record = market_breadth._fetch_activity_em("2026-09-29")

        assert record == {
            "date": "2026-09-29",
            "limit_up": 3,
            "real_limit_up": 1,
            "st_limit_up": 2,
            "limit_down": 1,
            "real_limit_down": 1,
            "st_limit_down": 0,
        }

    def test_both_pools_empty_returns_none_and_warns(self, caplog) -> None:
        empty = pd.DataFrame(columns=["代码", "名称"])
        client = _client_responding(_resp(True, empty), _resp(True, empty))
        with (
            patch("tasks.market_breadth.get_default_client", return_value=client),
            caplog.at_level(logging.WARNING, logger="tasks.market_breadth"),
        ):
            assert market_breadth._fetch_activity_em("2026-09-27") is None
        assert any("东财备用源无数据" in m for m in caplog.messages)

    def test_failed_pool_keeps_the_other_and_warns(self, caplog) -> None:
        down_df = pd.DataFrame([{"代码": "600004", "名称": "跌停股"}])
        client = _client_responding(
            _resp(False, None, "HTTP 502"), _resp(True, down_df)
        )
        with (
            patch("tasks.market_breadth.get_default_client", return_value=client),
            caplog.at_level(logging.WARNING, logger="tasks.market_breadth"),
        ):
            record = market_breadth._fetch_activity_em("2026-09-29")

        # 抓取失败的一侧不许写 0（0 = 当日真没有，不是没抓到）
        assert record == {
            "date": "2026-09-29",
            "limit_down": 1,
            "real_limit_down": 1,
            "st_limit_down": 0,
        }
        assert any("东财涨停池获取失败" in m and "502" in m for m in caplog.messages)

    def test_ak_missing_returns_none(self) -> None:
        with patch("tasks.market_breadth.ak", None):
            assert market_breadth._fetch_activity_em("2026-09-29") is None


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
            patch("tasks.market_breadth._fetch_activity_em", return_value=None),
        ):
            result = market_breadth.update_market_breadth(MagicMock())

        assert result["skipped"] is True
        assert normalize_task_result(
            "update_market_breadth", result
        ).status is TaskStatus.NO_DATA

    def test_all_sources_empty_logs_warning(self, caplog) -> None:
        """空手而归不再是「静默成功」：0 个交易日与 skipped 分支都要 WARNING。"""
        with (
            patch("tasks.market_breadth.ak", object()),
            patch("tasks.market_breadth._fetch_high_low", return_value=[]),
            patch("tasks.market_breadth._fetch_below_net_asset", return_value=[]),
            patch("tasks.market_breadth._fetch_activity", return_value=None),
            patch("tasks.market_breadth._fetch_activity_em", return_value=None),
            caplog.at_level(logging.WARNING, logger="tasks.market_breadth"),
        ):
            result = market_breadth.update_market_breadth(MagicMock())

        assert result["skipped"] is True
        assert "error" not in result
        assert any("0 个交易日" in m for m in caplog.messages)
        assert any("保留旧数据" in m for m in caplog.messages)

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
            patch("tasks.market_breadth._fetch_activity_em", return_value=None),
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
            patch("tasks.market_breadth._fetch_activity_em", return_value=None),
        ):
            result = market_breadth.update_market_breadth(db)

        assert result["saved"] == 1
        assert "error" not in result
        assert result["metadata"]["source_errors"] == ["破净统计: 'marketId'"]
        assert normalize_task_result(
            "update_market_breadth", result
        ).status is TaskStatus.SUCCESS


class TestEastmoneyFallback:
    """乐咕赚钱效应不可用时回退东财涨跌停池；乐咕可用时东财绝不被调用。"""

    def _em_record(self, date="2026-09-29"):
        return {
            "date": date,
            "limit_up": 46,
            "real_limit_up": 40,
            "st_limit_up": 6,
            "limit_down": 7,
            "real_limit_down": 7,
            "st_limit_down": 0,
        }

    def test_legu_down_falls_back_to_eastmoney(self) -> None:
        db = MagicMock()
        db.save_market_breadth_batch.return_value = 1
        with (
            patch("tasks.market_breadth.ak", object()),
            patch("tasks.market_breadth._fetch_high_low", return_value=[]),
            patch("tasks.market_breadth._fetch_below_net_asset", return_value=[]),
            patch("tasks.market_breadth._fetch_activity", return_value=None),
            patch(
                "tasks.market_breadth._fetch_activity_em",
                return_value=self._em_record(),
            ),
        ):
            result = market_breadth.update_market_breadth(db)

        records = db.save_market_breadth_batch.call_args.args[0]
        assert len(records) == 1
        record = records[0]
        assert record["date"] == "2026-09-29"
        assert record["data_source"] == "eastmoney"
        assert record["limit_up"] == 46
        assert record["real_limit_up"] == 40
        assert record["st_limit_up"] == 6
        assert record["limit_down"] == 7
        assert "up_count" not in record  # 东财路径不编造涨跌家数
        assert result["saved"] == 1
        assert "error" not in result
        assert normalize_task_result(
            "update_market_breadth", result
        ).status is TaskStatus.SUCCESS

    def test_legu_ok_means_eastmoney_never_called(self) -> None:
        db = MagicMock()
        db.save_market_breadth_batch.return_value = 1
        em = MagicMock()
        with (
            patch("tasks.market_breadth.ak", object()),
            patch("tasks.market_breadth._fetch_high_low", return_value=[]),
            patch("tasks.market_breadth._fetch_below_net_asset", return_value=[]),
            patch(
                "tasks.market_breadth._fetch_activity",
                return_value={"date": "2026-09-29", "up_count": 1084},
            ),
            patch("tasks.market_breadth._fetch_activity_em", em),
        ):
            market_breadth.update_market_breadth(db)

        assert em.call_count == 0
        records = db.save_market_breadth_batch.call_args.args[0]
        assert records[0]["data_source"] == "legu"

    def test_mixed_row_records_both_sources(self) -> None:
        """乐咕历史列 + 东财快照列同居一行时，两个来源都要留痕。"""
        db = MagicMock()
        db.save_market_breadth_batch.return_value = 1
        with (
            patch("tasks.market_breadth.ak", object()),
            patch(
                "tasks.market_breadth._fetch_high_low",
                return_value=[{"date": "2026-09-29", "high20": 265}],
            ),
            patch("tasks.market_breadth._fetch_below_net_asset", return_value=[]),
            patch("tasks.market_breadth._fetch_activity", return_value=None),
            patch(
                "tasks.market_breadth._fetch_activity_em",
                return_value=self._em_record(),
            ),
        ):
            market_breadth.update_market_breadth(db)

        records = db.save_market_breadth_batch.call_args.args[0]
        assert records[0]["data_source"] == "eastmoney+legu"
        assert records[0]["high20"] == 265
        assert records[0]["limit_up"] == 46

    def test_em_failure_goes_to_source_errors_not_top_level_error(self) -> None:
        """东财限流等失败只进 metadata：顶层 error 会让整任务判 failed。"""
        db = MagicMock()
        db.save_market_breadth_batch.return_value = 1
        with (
            patch("tasks.market_breadth.ak", object()),
            patch(
                "tasks.market_breadth._fetch_high_low",
                return_value=[{"date": "2026-09-24", "close": 3888.37}],
            ),
            patch("tasks.market_breadth._fetch_below_net_asset", return_value=[]),
            patch("tasks.market_breadth._fetch_activity", return_value=None),
            patch(
                "tasks.market_breadth._fetch_activity_em",
                side_effect=ConnectionError("rate limited"),
            ),
        ):
            result = market_breadth.update_market_breadth(db)

        assert result["saved"] == 1
        assert "error" not in result
        assert result["metadata"]["source_errors"] == [
            "赚钱效应(东财备用): rate limited"
        ]
        assert normalize_task_result(
            "update_market_breadth", result
        ).status is TaskStatus.SUCCESS

