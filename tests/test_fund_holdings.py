"""基金持股明细任务（update_fund_holdings）回归测试。

背景：旧实现调用东财 datacenter-web 的 ``reportName=RPT_FUND_HOLD_STOCK``，
该报表在服务端并不存在（返回 200 + ``{"message":"报表配置不存在"}``，``result``
为 None），被静默吞成 0 行，导致结果契约判 ``failed: zero rows without
explanation``，且 ``fund_holdings`` 表从未成功写入。

本文件钉住修复后的行为：正确的 ``dataapi/zlsj/list`` 接口、按机构类型循环、
报告期回退、以及「网络失败 → retained / 确实无数据 → no_data」的语义区分。
"""
from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest
import requests as real_requests

import tasks.fund_holdings as fh
from core.task_result import normalize_task_result

MODULE = "tasks.fund_holdings"


def _mk_response(payload: object) -> MagicMock:
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = payload
    resp.raise_for_status.return_value = None
    return resp


def _zlsj_row(code: str = "300750", org_type: str = "01") -> dict:
    return {
        "SECURITY_CODE": code,
        "SECURITY_NAME_ABBR": "宁德时代",
        "REPORT_DATE": "2026-06-30 00:00:00",
        "ORG_TYPE": org_type,
        "HOULD_NUM": 100,
        "TOTAL_SHARES": 12345,
        "HOLD_VALUE": 999.0,
        "FREESHARES_RATIO": 1.5,
        "HOLDCHA": "增仓",
        "HOLDCHA_NUM": 10,
        "HOLDCHA_RATIO": 0.5,
    }


# ===========================================================================
# _fetch_fund_holdings_page：接口地址 / 参数 / 解析
# ===========================================================================


@patch(f"{MODULE}.requests")
def test_fetch_page_uses_zlsj_endpoint_and_params(mock_requests: MagicMock) -> None:
    """必须打到 dataapi/zlsj/list 并透传 date/type/zjc，而非已废弃的 reportName。"""
    mock_requests.get.return_value = _mk_response({"data": [_zlsj_row()], "pages": 1})

    records, pages = fh._fetch_fund_holdings_page("2026-06-30", "1", 1)

    url = mock_requests.get.call_args[0][0]
    assert "dataapi/zlsj/list" in url
    params = mock_requests.get.call_args[1]["params"]
    assert params["date"] == "2026-06-30"
    assert params["type"] == "1"
    assert params["zjc"] == "0"
    assert pages == 1
    assert len(records) == 1
    row = records[0]
    assert row["ts_code"] == "300750.SZ"
    assert row["report_date"] == "2026-06-30"
    assert row["institution_type"] == "fund"
    assert row["fund_count"] == 100
    assert row["data_source"] == "eastmoney"


@patch(f"{MODULE}.requests")
def test_fetch_page_empty_list_payload_is_no_data(mock_requests: MagicMock) -> None:
    """东财对无数据的日期返回顶层 ``[]``（非 dict），须视作空页而非异常。"""
    mock_requests.get.return_value = _mk_response([])

    records, pages = fh._fetch_fund_holdings_page("2026-09-30", "1", 1)

    assert records == []
    assert pages == 0


@patch(f"{MODULE}.requests")
def test_fetch_page_network_error_raises(mock_requests: MagicMock) -> None:
    """网络异常须上抛 FundHoldingsFetchError，不得吞成空数据。"""
    mock_requests.RequestException = real_requests.RequestException
    mock_requests.get.side_effect = real_requests.RequestException("502 Bad Gateway")

    with pytest.raises(fh.FundHoldingsFetchError):
        fh._fetch_fund_holdings_page("2026-06-30", "1", 1)


@patch(f"{MODULE}.requests")
def test_fetch_page_requests_missing_raises(mock_requests: MagicMock) -> None:
    """requests 未安装时同样按网络错误处理，避免静默零行。"""
    with patch(f"{MODULE}.requests", None), pytest.raises(fh.FundHoldingsFetchError):
        fh._fetch_fund_holdings_page("2026-06-30", "1", 1)


# ===========================================================================
# 报告期回退
# ===========================================================================


def test_recent_report_dates_newest_first() -> None:
    dates = fh._recent_report_dates(today=date(2026, 9, 23), count=4)
    assert dates == ["2026-06-30", "2026-03-31", "2025-12-31", "2025-09-30"]


def test_recent_report_dates_early_january() -> None:
    dates = fh._recent_report_dates(today=date(2026, 1, 5), count=2)
    assert dates == ["2025-12-31", "2025-09-30"]


@patch(f"{MODULE}.requests")
def test_fetch_all_walks_back_until_data(mock_requests: MagicMock) -> None:
    """最近报告期为空时回退到上一期，直到取到数据。"""
    requested_dates: list[str] = []

    def side_effect(url: str, params: dict | None = None, **kwargs: object) -> MagicMock:
        assert params is not None
        requested_dates.append(params["date"])
        if params["date"] == "2026-03-31" and params["type"] == "1":
            row = _zlsj_row()
            row["REPORT_DATE"] = "2026-03-31 00:00:00"
            return _mk_response({"data": [row], "pages": 1})
        return _mk_response([])

    mock_requests.get.side_effect = side_effect

    records = fh._fetch_all_fund_holdings(today=date(2026, 9, 23))

    # 先试最近报告期 2026-06-30（空），再回退到 2026-03-31（有数据）
    assert "2026-06-30" in requested_dates
    assert "2026-03-31" in requested_dates
    assert requested_dates.index("2026-06-30") < requested_dates.index("2026-03-31")
    assert len(records) == 1
    assert records[0]["report_date"] == "2026-03-31"


# ===========================================================================
# _sina_to_ts_code：市场前缀（含北交所）
# ===========================================================================


@pytest.mark.parametrize(
    ("code", "expected"),
    [
        ("600000", "600000.SH"),
        ("900901", "900901.SH"),
        ("000001", "000001.SZ"),
        ("002594", "002594.SZ"),
        ("300750", "300750.SZ"),
        ("430047", "430047.BJ"),
        ("830799", "830799.BJ"),
        ("920002", "920002.BJ"),
    ],
)
def test_sina_to_ts_code(code: str, expected: str) -> None:
    assert fh._sina_to_ts_code(code) == expected


# ===========================================================================
# update_fund_holdings：结果契约
# ===========================================================================


def test_update_retained_on_network_error() -> None:
    """源端网络失败 → retained（保留旧数据），不得再落 zero-rows 失败分支。"""
    db = MagicMock()
    with patch(
        f"{MODULE}._fetch_all_fund_holdings",
        side_effect=fh.FundHoldingsFetchError("502 Bad Gateway"),
    ):
        result = fh.update_fund_holdings(db)

    assert result["status"] == "retained"
    assert result["retained_old_data"] is True
    assert result["error_kind"] == "network"
    assert normalize_task_result("update_fund_holdings", result).status.value == "retained"
    db.save_fund_holdings_batch.assert_not_called()


def test_update_no_data_is_skipped() -> None:
    """所有候选报告期皆空 → skipped/no_data，而非 failed。"""
    db = MagicMock()
    with patch(f"{MODULE}._fetch_all_fund_holdings", return_value=[]):
        result = fh.update_fund_holdings(db)

    assert result["skipped"] is True
    assert normalize_task_result("update_fund_holdings", result).status.value == "no_data"
    db.save_fund_holdings_batch.assert_not_called()


def test_update_success_saves() -> None:
    db = MagicMock()
    db.save_fund_holdings_batch.return_value = 2
    records = [
        {"ts_code": "300750.SZ", "report_date": "2026-06-30", "institution_type": "fund"},
        {"ts_code": "600000.SH", "report_date": "2026-06-30", "institution_type": "qfii"},
    ]
    with patch(f"{MODULE}._fetch_all_fund_holdings", return_value=records):
        result = fh.update_fund_holdings(db)

    assert result["saved"] == 2
    assert result["total"] == 2
    assert normalize_task_result("update_fund_holdings", result).status.value == "success"
    db.save_fund_holdings_batch.assert_called_once_with(records)
