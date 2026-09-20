"""tasks/us_macro.py 单元测试：FRED 抓取、序列合并与派生指标计算。"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import requests as real_requests

from tasks.us_macro import _fetch_fred_series, _fetch_us_macro, update_us_macro

MODULE = "tasks.us_macro"


def _patch_requests(mock_requests: MagicMock) -> None:
    """补回被 mock 吞掉的真实异常类（except 子句引用 requests.RequestException）。"""
    mock_requests.RequestException = real_requests.RequestException


def _mk_response(text: str) -> MagicMock:
    """构造一个类 requests.Response 的 mock。"""
    resp = MagicMock()
    resp.text = text
    resp.raise_for_status.return_value = None
    return resp


# ===========================================================================
# _fetch_fred_series
# ===========================================================================


@patch(f"{MODULE}.requests")
def test_fetch_fred_series_parses_csv(mock_requests: MagicMock):
    mock_requests.get.return_value = _mk_response(
        "observation_date,EFFR\n"
        "2026-09-14,3.63\n"
        "2026-09-15,3.63\n"
        "2026-09-16,.\n"       # FRED 缺失值表示法
        "2026-09-17,\n"        # 空串缺失值
        "2026-09-18,3.88\n"
    )
    series = _fetch_fred_series("EFFR", "2026-09-01", "2026-09-18")
    assert series == {
        "2026-09-14": 3.63,
        "2026-09-15": 3.63,
        "2026-09-16": None,
        "2026-09-17": None,
        "2026-09-18": 3.88,
    }
    # 窗口参数透传（cosd/coed 为 fredgraph.csv 的单序列窗口参数）
    params = mock_requests.get.call_args[1]["params"]
    assert params == {"id": "EFFR", "cosd": "2026-09-01", "coed": "2026-09-18"}


@patch(f"{MODULE}.requests")
def test_fetch_fred_series_request_failure(mock_requests: MagicMock):
    _patch_requests(mock_requests)
    mock_requests.get.side_effect = real_requests.RequestException("timeout")
    assert _fetch_fred_series("EFFR", "2026-09-01", "2026-09-18") == {}


# ===========================================================================
# _fetch_us_macro（合并 + 派生）
# ===========================================================================


def _fred_data() -> dict[str, dict[str, float | None]]:
    """六个 FRED 序列的 stub 数据（2026-09-16 加息 25bp 场景）。"""
    return {
        "EFFR": {"2026-09-14": 3.63, "2026-09-15": 3.63, "2026-09-16": None},
        "DGS2": {"2026-09-14": 3.85, "2026-09-15": 3.87, "2026-09-16": 3.90},
        "DGS3MO": {"2026-09-14": 3.70, "2026-09-15": 3.71, "2026-09-16": 3.72},
        "DGS10": {"2026-09-14": 4.20, "2026-09-15": 4.22, "2026-09-16": 4.25},
        "T10YIE": {"2026-09-14": 2.35, "2026-09-15": 2.36, "2026-09-16": 2.40},
        # ICSA 为周度序列，FRED 观察日为周六（如 2026-09-12 → 映射到周五 09-11）
        "ICSA": {"2026-09-12": 205000},
    }


def test_fetch_us_macro_merges_and_derives():
    def fake_series(series_id, start, end):
        return _fred_data().get(series_id, {})

    with patch(f"{MODULE}._fetch_fred_series", side_effect=fake_series):
        records = _fetch_us_macro("2026-09-16")
    # 09-11（周五）为 ICSA 映射落库行，仅 icsa 有值
    assert len(records) == 4
    by_date = {r["trade_date"]: r for r in records}
    assert by_date["2026-09-11"]["icsa"] == 205000
    assert by_date["2026-09-11"]["effr"] is None
    row = by_date["2026-09-15"]
    assert row["effr"] == 3.63
    assert row["dgs2"] == 3.87
    assert row["icsa"] is None
    assert row["spread_10y_3m"] == round(4.22 - 3.71, 4)
    assert row["real_rate_10y"] == round(4.22 - 2.36, 4)
    assert row["data_source"] == "fred"
    # EFFR 当日缺失但其余序列有值：保留行，派生值仍计算
    row = by_date["2026-09-16"]
    assert row["effr"] is None
    assert row["dgs2"] == 3.90
    # ICSA 周度发布：非发布日稀疏为 None
    assert row["icsa"] is None
    assert row["spread_10y_3m"] == round(4.25 - 3.72, 4)


def test_fetch_us_macro_skips_all_empty_dates():
    def fake_series(series_id, start, end):
        # 同一日期各序列均为缺失（长假连休）
        return {"2026-09-07": None} if series_id == "EFFR" else {}

    with patch(f"{MODULE}._fetch_fred_series", side_effect=fake_series):
        assert _fetch_us_macro("2026-09-16") == []


# ===========================================================================
# update_us_macro
# ===========================================================================


def test_update_us_macro_success():
    db = MagicMock()
    db.save_us_macro_batch.return_value = 3
    record = {
        "trade_date": "2026-09-15", "effr": 3.63, "dgs3mo": 3.71, "dgs10": 4.22,
        "t10yie": 2.36, "spread_10y_3m": 0.51, "real_rate_10y": 1.86,
        "data_source": "fred",
    }
    with patch(f"{MODULE}._fetch_us_macro", return_value=[record]):
        r = update_us_macro(db)
    assert r == {"saved": 3, "total": 1}
    db.save_us_macro_batch.assert_called_once_with([record])


def test_update_us_macro_empty():
    db = MagicMock()
    with patch(f"{MODULE}._fetch_us_macro", return_value=[]):
        r = update_us_macro(db)
    assert r == {"saved": 0, "total": 0}
    db.save_us_macro_batch.assert_not_called()


def test_update_us_macro_requests_none():
    db = MagicMock()
    with patch(f"{MODULE}.requests", None):
        r = update_us_macro(db)
    assert r["saved"] == 0
    assert "error" in r


def test_update_us_macro_exception():
    db = MagicMock()
    with patch(f"{MODULE}._fetch_us_macro", side_effect=RuntimeError("boom")):
        r = update_us_macro(db)
    assert r["saved"] == 0
    assert "error" in r
