"""tasks/eia_petroleum.py 单元测试：EIA API 解析、key 缺失早退与任务契约。"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import requests as real_requests

from tasks.eia_petroleum import _fetch_series, update_eia_petroleum

MODULE = "tasks.eia_petroleum"
API_KEY = "test-key-0000"


def _mk_response(payload: dict) -> MagicMock:
    resp = MagicMock()
    resp.json.return_value = payload
    resp.raise_for_status.return_value = None
    return resp


def _eia_payload() -> dict:
    """模拟 /v2/seriesid 响应（两个周数据点）。"""
    return {
        "response": {
            "total": 2294,
            "frequency": "weekly",
            "data": [
                {
                    "period": "2026-09-11",
                    "series": "WCESTUS1",
                    "series-description": "U.S. Ending Stocks excluding SPR of Crude Oil",
                    "value": 423429,
                    "units": "MBBL",
                },
                {
                    "period": "2026-09-04",
                    "series": "WCESTUS1",
                    "series-description": "U.S. Ending Stocks excluding SPR of Crude Oil",
                    "value": 424069,
                    "units": "MBBL",
                },
            ],
        }
    }


# ===========================================================================
# _fetch_series
# ===========================================================================


@patch(f"{MODULE}.requests")
def test_fetch_series_parses_weekly_rows(mock_requests: MagicMock):
    mock_requests.get.return_value = _mk_response(_eia_payload())
    records = _fetch_series("PET.WCESTUS1.W", API_KEY)
    assert len(records) == 2
    row = records[0]
    assert row["week_date"] == "2026-09-11"
    assert row["series_id"] == "PET.WCESTUS1.W"
    assert row["series_name"] == "全美商业原油库存(除SPR)"
    assert row["value"] == 423429.0
    assert row["units"] == "MBBL"
    assert row["data_source"] == "eia"
    # URL 与参数透传
    url = mock_requests.get.call_args[0][0]
    assert url.endswith("/PET.WCESTUS1.W")
    assert mock_requests.get.call_args[1]["params"]["api_key"] == API_KEY


@patch(f"{MODULE}.requests")
def test_fetch_series_request_failure(mock_requests: MagicMock):
    mock_requests.RequestException = real_requests.RequestException
    mock_requests.get.side_effect = real_requests.RequestException("timeout")
    assert _fetch_series("PET.WCESTUS1.W", API_KEY) == []


# ===========================================================================
# update_eia_petroleum
# ===========================================================================


def test_update_eia_petroleum_key_missing():
    db = MagicMock()
    with patch(f"{MODULE}.requests"), patch.dict("os.environ", {}, clear=True):
        r = update_eia_petroleum(db)
    assert r["saved"] == 0
    assert "error" in r
    db.save_eia_petroleum_batch.assert_not_called()


@patch(f"{MODULE}.requests")
def test_update_eia_petroleum_success(mock_requests: MagicMock):
    db = MagicMock()
    db.save_eia_petroleum_batch.side_effect = lambda records: len(records)
    mock_requests.get.return_value = _mk_response(_eia_payload())
    env = {"EIA_API_KEY": API_KEY}
    with patch.dict("os.environ", env, clear=True):
        r = update_eia_petroleum(db)
    assert r == {"saved": 10, "total": 10}
    assert db.save_eia_petroleum_batch.call_count == 1
    assert mock_requests.get.call_count == 5  # 五个序列各一次


@patch(f"{MODULE}.requests")
def test_update_eia_petroleum_all_failed(mock_requests: MagicMock):
    db = MagicMock()
    mock_requests.RequestException = real_requests.RequestException
    mock_requests.get.side_effect = real_requests.RequestException("403")
    env = {"EIA_API_KEY": API_KEY}
    with patch.dict("os.environ", env, clear=True):
        r = update_eia_petroleum(db)
    assert r == {"saved": 0, "total": 0}
    db.save_eia_petroleum_batch.assert_not_called()


@patch(f"{MODULE}.requests")
def test_update_eia_petroleum_exception(mock_requests: MagicMock):
    db = MagicMock()
    db.save_eia_petroleum_batch.side_effect = RuntimeError("boom")
    mock_requests.get.return_value = _mk_response(_eia_payload())
    env = {"EIA_API_KEY": API_KEY}
    with patch.dict("os.environ", env, clear=True):
        r = update_eia_petroleum(db)
    assert r["saved"] == 0
    assert "error" in r
