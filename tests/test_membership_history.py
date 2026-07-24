"""Tests for index membership PIT history update."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd

import tasks.index_membership as index_membership
from interface import DatabaseInterface


class TestFetchIndexConstituents:
    def test_happy_path(self):
        mock_df = pd.DataFrame({
            "成分券代码": ["000001", "000002", "600000"],
            "成分券名称": ["平安银行", "万科A", "浦发银行"],
            "权重": [0.5, 0.3, 0.2],
        })
        with patch.object(index_membership, "ak") as mock_ak:
            mock_ak.index_stock_cons_weight_csindex.return_value = mock_df
            records = index_membership._fetch_index_constituents("000300", "沪深300")

        assert len(records) == 3
        assert records[0]["index_code"] == "000300"
        assert records[0]["ts_code"] == "000001.sz"
        assert records[0]["weight"] == 0.5
        assert records[1]["ts_code"] == "000002.sz"
        assert records[2]["ts_code"] == "600000.sh"

    def test_api_failure(self):
        with patch.object(index_membership, "ak") as mock_ak:
            mock_ak.index_stock_cons_weight_csindex.side_effect = ConnectionError("timeout")
            records = index_membership._fetch_index_constituents("000300", "沪深300")
        assert records == []

    def test_empty_dataframe(self):
        with patch.object(index_membership, "ak") as mock_ak:
            mock_ak.index_stock_cons_weight_csindex.return_value = pd.DataFrame()
            records = index_membership._fetch_index_constituents("000300", "沪深300")
        assert records == []

    def test_ak_none(self):
        with patch.object(index_membership, "ak", None):
            records = index_membership._fetch_index_constituents("000300", "沪深300")
        assert records == []

    def test_missing_weight_column(self):
        mock_df = pd.DataFrame({
            "成分券代码": ["000001"],
            "成分券名称": ["平安银行"],
        })
        with patch.object(index_membership, "ak") as mock_ak:
            mock_ak.index_stock_cons_weight_csindex.return_value = mock_df
            records = index_membership._fetch_index_constituents("000300", "沪深300")
        assert len(records) == 1
        assert records[0]["weight"] == 0.0


class TestUpdateIndexMembership:
    def test_success_all_indices(self):
        mock_df = pd.DataFrame({
            "成分券代码": ["000001", "600000"],
            "成分券名称": ["平安银行", "浦发银行"],
            "权重": [0.6, 0.4],
        })
        db = MagicMock(spec=DatabaseInterface)
        db.save_index_member_history_batch.return_value = 2

        with patch.object(index_membership, "ak") as mock_ak:
            mock_ak.index_stock_cons_weight_csindex.return_value = mock_df
            result = index_membership.update_index_membership(db)

        assert result["saved"] == 2
        assert result["status"] == "success"
        assert db.save_index_member_history_batch.called

    def test_some_indices_fail(self):
        db = MagicMock(spec=DatabaseInterface)
        db.save_index_member_history_batch.return_value = 3

        good_df = pd.DataFrame({
            "成分券代码": ["000001"],
            "成分券名称": ["平安银行"],
            "权重": [1.0],
        })

        with patch.object(index_membership, "ak") as mock_ak:
            mock_ak.index_stock_cons_weight_csindex.side_effect = [
                good_df,
                ConnectionError("x"),
                good_df,
                ConnectionError("x"),
                good_df,
            ]
            result = index_membership.update_index_membership(db)

        assert result["status"] == "degraded"
        assert result["saved"] == 3

    def test_all_indices_fail(self):
        db = MagicMock(spec=DatabaseInterface)

        with patch.object(index_membership, "ak") as mock_ak:
            mock_ak.index_stock_cons_weight_csindex.side_effect = ConnectionError("network")
            result = index_membership.update_index_membership(db)

        assert result["saved"] == 0
        assert result["status"] == "degraded"
        assert db.save_index_member_history_batch.called is False

    def test_ak_none(self):
        db = MagicMock(spec=DatabaseInterface)
        with patch.object(index_membership, "ak", None):
            result = index_membership.update_index_membership(db)
        assert result["saved"] == 0
        assert "error" in result


class TestNormalizeTsCode:
    def test_shenzhen(self):
        assert index_membership._normalize_ts_code("000001") == "000001.sz"

    def test_shanghai(self):
        assert index_membership._normalize_ts_code("600000") == "600000.sh"

    def test_beijing(self):
        assert index_membership._normalize_ts_code("430017") == "430017.bj"

    def test_already_normalized(self):
        assert index_membership._normalize_ts_code("000001.sz") == "000001.sz"

    def test_empty(self):
        assert index_membership._normalize_ts_code("") == ""
