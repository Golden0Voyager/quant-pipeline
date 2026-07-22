"""Coverage tests for Phase 1 optional task modules.

Each module has a uniform pattern:
  - ``_to_float`` / ``_to_int`` / ``_try_get_ak_df`` helpers
  - ``_COLUMN_MAP`` dict for column renaming
  - ``update_*`` entry point → fetches → renames → batch-saves

Covers happy path, ak=None, empty DataFrame, exception, and edge cases.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd

import tasks.institution_survey as institution_survey
import tasks.option_sentiment as option_sentiment
import tasks.stock_pledge as stock_pledge
import tasks.stock_repurchase as stock_repurchase

# ===========================================================================
# 通用 mock 工厂
# ===========================================================================


def _ak_with(df: pd.DataFrame) -> MagicMock:
    ak = MagicMock()
    # 新版 akshare 已移除 stock_zyg_em，改用 stock_gpzy_pledge_ratio_em
    ak.stock_gpzy_pledge_ratio_em.return_value = df
    ak.stock_repurchase_em.return_value = df
    ak.stock_jgdy_tj_em.return_value = df
    # option_sentiment
    ak.index_option_50etf_qvix.return_value = df
    ak.stock_option_sse_50etf_daily.return_value = df
    return ak


def _pledge_df() -> pd.DataFrame:
    # 新版接口 stock_gpzy_pledge_ratio_em 返回股票级质押汇总
    return pd.DataFrame({
        "股票代码": ["000001"],
        "股票简称": ["平安银行"],
        "交易日期": ["2024-01-01"],
        "质押股数": [1000000.0],
        "质押比例": [0.05],
    })


def _repurchase_df() -> pd.DataFrame:
    return pd.DataFrame({
        "股票代码": ["000001"],
        "股票简称": ["平安银行"],
        "公告日期": ["2024-01-01"],
        "回购金额": [5e8],
        "回购价格": [12.5],
        "回购数量": [40000000],
        "进度": ["已完成"],
    })


def _survey_df() -> pd.DataFrame:
    return pd.DataFrame({
        "股票代码": ["000001"],
        "股票简称": ["平安银行"],
        "调研日期": ["2024-01-01"],
        "调研机构": ["华夏基金"],
        "调研类型": ["实地调研"],
        "接待人数": [5],
    })


def _qvix_df() -> pd.DataFrame:
    return pd.DataFrame({
        "date": ["2024-01-01"],
        "qvix": [18.5],
    })


def _option_daily_df() -> pd.DataFrame:
    return pd.DataFrame({
        "date": ["2024-01-01"],
        "put_volume": [50000],
        "call_volume": [100000],
        "put_oi": [200000],
        "call_oi": [300000],
        "implied_vol_avg": [0.20],
    })


# ===========================================================================
# 1. stock_pledge
# ===========================================================================


class TestStockPledge:
    def test_happy_path(self):
        db = MagicMock()
        db.save_stock_pledge_batch.return_value = 1
        with patch.object(stock_pledge, "ak", _ak_with(_pledge_df())):
            result = stock_pledge.update_stock_pledge(db)
        assert result["saved"] == 1
        assert db.save_stock_pledge_batch.called

    def test_ak_none(self):
        db = MagicMock()
        with patch.object(stock_pledge, "ak", None):
            result = stock_pledge.update_stock_pledge(db)
        assert result["saved"] == 0
        assert "error" in result

    def test_empty_df(self):
        db = MagicMock()
        with patch.object(stock_pledge, "ak", _ak_with(pd.DataFrame())):
            result = stock_pledge.update_stock_pledge(db)
        assert result["saved"] == 0

    def test_exception(self):
        ak = MagicMock()
        # Set __name__ so _try_get_ak_df can log func.__name__
        ak.stock_zyg_em = MagicMock(
            side_effect=RuntimeError("network err"), __name__="stock_zyg_em"
        )
        db = MagicMock()
        with patch.object(stock_pledge, "ak", ak):
            result = stock_pledge.update_stock_pledge(db)
        assert result["saved"] == 0

    def test_to_float_edge_cases(self):
        assert stock_pledge._to_float(None) is None
        assert stock_pledge._to_float(float("nan")) is None
        assert stock_pledge._to_float("abc") is None
        assert stock_pledge._to_float("12.5%") == 12.5
        assert stock_pledge._to_float(42.0) == 42.0

    def test_try_get_ak_df_ak_none(self):
        with patch.object(stock_pledge, "ak", None):
            assert stock_pledge._try_get_ak_df(lambda: None) is None

    def test_try_get_ak_df_exception(self):
        func = MagicMock(side_effect=RuntimeError("fail"), __name__="test_func")
        with patch.object(stock_pledge, "ak", MagicMock()):
            assert stock_pledge._try_get_ak_df(func) is None
        assert func.call_count == 1


# ===========================================================================
# 2. stock_repurchase
# ===========================================================================


class TestStockRepurchase:
    def test_happy_path(self):
        db = MagicMock()
        db.save_stock_repurchase_batch.return_value = 1
        with patch.object(stock_repurchase, "ak", _ak_with(_repurchase_df())):
            result = stock_repurchase.update_stock_repurchase(db)
        assert result["saved"] == 1
        assert db.save_stock_repurchase_batch.called

    def test_ak_none(self):
        db = MagicMock()
        with patch.object(stock_repurchase, "ak", None):
            result = stock_repurchase.update_stock_repurchase(db)
        assert result["saved"] == 0
        assert "error" in result

    def test_empty_df(self):
        db = MagicMock()
        with patch.object(stock_repurchase, "ak", _ak_with(pd.DataFrame())):
            result = stock_repurchase.update_stock_repurchase(db)
        assert result["saved"] == 0

    def test_exception(self):
        ak = MagicMock()
        ak.stock_repurchase_em = MagicMock(
            side_effect=RuntimeError("network err"), __name__="stock_repurchase_em"
        )
        db = MagicMock()
        with patch.object(stock_repurchase, "ak", ak):
            result = stock_repurchase.update_stock_repurchase(db)
        assert result["saved"] == 0

    def test_to_float_edge_cases(self):
        assert stock_repurchase._to_float(None) is None
        assert stock_repurchase._to_float(float("nan")) is None
        assert stock_repurchase._to_float("abc") is None
        assert stock_repurchase._to_float(42.0) == 42.0

    def test_to_int_edge_cases(self):
        assert stock_repurchase._to_int(None) is None
        assert stock_repurchase._to_int(float("nan")) is None
        assert stock_repurchase._to_int("abc") is None
        assert stock_repurchase._to_int(42.7) == 42
        assert stock_repurchase._to_int("100") == 100

    def test_try_get_ak_df_ak_none(self):
        with patch.object(stock_repurchase, "ak", None):
            assert stock_repurchase._try_get_ak_df(lambda: None) is None


# ===========================================================================
# 3. institution_survey
# ===========================================================================


class TestInstitutionSurvey:
    def test_happy_path(self):
        db = MagicMock()
        db.save_institution_survey_batch.return_value = 1
        with patch.object(institution_survey, "ak", _ak_with(_survey_df())):
            result = institution_survey.update_institution_survey(db)
        assert result["saved"] == 1
        assert db.save_institution_survey_batch.called

    def test_ak_none(self):
        db = MagicMock()
        with patch.object(institution_survey, "ak", None):
            result = institution_survey.update_institution_survey(db)
        assert result["saved"] == 0
        assert "error" in result

    def test_empty_df(self):
        db = MagicMock()
        with patch.object(institution_survey, "ak", _ak_with(pd.DataFrame())):
            result = institution_survey.update_institution_survey(db)
        assert result["saved"] == 0

    def test_exception(self):
        ak = MagicMock()
        ak.stock_jgdy_tj_em = MagicMock(
            side_effect=RuntimeError("network err"), __name__="stock_jgdy_tj_em"
        )
        db = MagicMock()
        with patch.object(institution_survey, "ak", ak):
            result = institution_survey.update_institution_survey(db)
        assert result["saved"] == 0

    def test_to_int_edge_cases(self):
        assert institution_survey._to_int(None) is None
        assert institution_survey._to_int(float("nan")) is None
        assert institution_survey._to_int("abc") is None
        assert institution_survey._to_int(42.7) == 42

    def test_try_get_ak_df_ak_none(self):
        with patch.object(institution_survey, "ak", None):
            assert institution_survey._try_get_ak_df(lambda: None) is None


# ===========================================================================
# 6. option_sentiment
# ===========================================================================


class TestOptionSentiment:
    def test_happy_path(self):
        db = MagicMock()
        db.save_option_sentiment_batch.return_value = 2
        ak = MagicMock()
        ak.index_option_50etf_qvix.return_value = _qvix_df()
        ak.stock_option_sse_50etf_daily.return_value = _option_daily_df()
        with patch.object(option_sentiment, "ak", ak):
            result = option_sentiment.update_option_sentiment(db)
        assert result["saved"] == 2
        assert db.save_option_sentiment_batch.called

    def test_ak_none(self):
        db = MagicMock()
        with patch.object(option_sentiment, "ak", None):
            result = option_sentiment.update_option_sentiment(db)
        assert result["saved"] == 0
        assert "error" in result

    def test_both_empty(self):
        ak = MagicMock()
        ak.index_option_50etf_qvix.return_value = pd.DataFrame()
        ak.stock_option_sse_50etf_daily.return_value = pd.DataFrame()
        db = MagicMock()
        with patch.object(option_sentiment, "ak", ak):
            result = option_sentiment.update_option_sentiment(db)
        assert result["saved"] == 0
        assert result["qvix"] == 0
        assert result["daily"] == 0

    def test_fetch_qvix_empty(self):
        ak = MagicMock()
        ak.index_option_50etf_qvix.return_value = pd.DataFrame()
        with patch.object(option_sentiment, "ak", ak):
            records = option_sentiment._fetch_qvix()
        assert records == []

    def test_fetch_qvix_with_alt_column(self):
        """_fetch_qvix: 使用 '日期' 列作为 date_val 回退。"""
        ak = MagicMock()
        ak.index_option_50etf_qvix.return_value = pd.DataFrame({
            "日期": ["2024-01-01"],
            "QVIX": [20.0],
        })
        with patch.object(option_sentiment, "ak", ak):
            records = option_sentiment._fetch_qvix()
        assert len(records) == 1
        assert records[0]["trade_date"] == "2024-01-01"
        assert records[0]["qvix"] == 20.0

    def test_fetch_qvix_date_none(self):
        """_fetch_qvix: date_val 为 None 时跳过。"""
        ak = MagicMock()
        ak.index_option_50etf_qvix.return_value = pd.DataFrame({"date": [None]})
        with patch.object(option_sentiment, "ak", ak):
            records = option_sentiment._fetch_qvix()
        assert records == []

    def _mock_50etf_daily(self, data: dict) -> MagicMock:
        """Helper: build akshare mock with option_daily_stats_sse returning data for 510050."""
        ak = MagicMock()
        df = pd.DataFrame(data)
        ak.option_daily_stats_sse.return_value = df
        return ak

    def test_fetch_50etf_empty(self):
        ak = self._mock_50etf_daily({})
        with (
            patch.object(option_sentiment, "ak", ak),
            patch.object(option_sentiment, "get_expected_latest_trading_day", return_value="2024-01-01"),
        ):
            records = option_sentiment._fetch_50etf_daily()
        assert records == []

    def test_fetch_50etf_alt_columns(self):
        """_fetch_50etf_daily: 中文列名正常读取。"""
        ak = self._mock_50etf_daily({
            "合约标的代码": ["510050"],
            "认沽成交量": [30000],
            "认购成交量": [60000],
            "未平仓认沽合约数": [100000],
            "未平仓认购合约数": [200000],
        })
        with (
            patch.object(option_sentiment, "ak", ak),
            patch.object(option_sentiment, "get_expected_latest_trading_day", return_value="2024-01-01"),
        ):
            records = option_sentiment._fetch_50etf_daily()
        assert len(records) == 1
        assert records[0]["pcr"] == 0.5  # 30000 / 60000
        assert records[0]["put_volume"] == 30000
        assert records[0]["call_volume"] == 60000

    def test_fetch_50etf_call_vol_zero(self):
        """_fetch_50etf_daily: call_vol 为 0 时 PCR 为 None。"""
        ak = self._mock_50etf_daily({
            "合约标的代码": ["510050"],
            "认沽成交量": [50000],
            "认购成交量": [0],
        })
        with (
            patch.object(option_sentiment, "ak", ak),
            patch.object(option_sentiment, "get_expected_latest_trading_day", return_value="2024-01-01"),
        ):
            records = option_sentiment._fetch_50etf_daily()
        assert records[0]["pcr"] is None

    def test_fetch_50etf_no_510050(self):
        """_fetch_50etf_daily: 数据中无 510050 → 返回空。"""
        ak = self._mock_50etf_daily({
            "合约标的代码": ["510050"],
            "认沽成交量": [None],
        })
        with (
            patch.object(option_sentiment, "ak", ak),
            patch.object(option_sentiment, "get_expected_latest_trading_day", return_value="2024-01-01"),
        ):
            records = option_sentiment._fetch_50etf_daily()
        assert len(records) == 0

    def test_to_float_edge_cases(self):
        assert option_sentiment._to_float(None) is None
        assert option_sentiment._to_float(float("nan")) is None
        assert option_sentiment._to_float("abc") is None
        assert option_sentiment._to_float(42.0) == 42.0

    def test_to_int_edge_cases(self):
        assert option_sentiment._to_int(None) is None
        assert option_sentiment._to_int(float("nan")) is None
        assert option_sentiment._to_int("abc") is None
        assert option_sentiment._to_int(42.7) == 42

    def test_try_get_ak_df_ak_none(self):
        with patch.object(option_sentiment, "ak", None):
            assert option_sentiment._try_get_ak_df(lambda: None) is None

    def test_try_get_ak_df_exception(self):
        func = MagicMock(side_effect=RuntimeError("fail"), __name__="test_func")
        with patch.object(option_sentiment, "ak", MagicMock()):
            assert option_sentiment._try_get_ak_df(func) is None
