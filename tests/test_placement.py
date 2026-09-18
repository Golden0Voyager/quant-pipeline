"""tasks/placement.py 单元测试：全量快照归一化、定增筛选、源键与结果契约。

全部使用 synthetic 假数据，不触网（任务书约束：仅一次 live 列名探测 +
调通后一次端到端验证）。
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd

from tasks.placement import (
    _records_from_df,
    fetch_placement_announcements_records,
    update_placement_announcements,
)

MODULE = "tasks.placement"


def _qbzf_df(rows: list[dict]) -> pd.DataFrame:
    """构造 stock_qbzf_em 风格的原始 DataFrame（含无关列与非目标行）。"""
    return pd.DataFrame(rows)


def _success_resp(df: pd.DataFrame):
    return SimpleNamespace(success=True, data=df, metadata=SimpleNamespace(error=None))


# ===========================================================================
# _records_from_df：筛选 / 清洗 / 源键
# ===========================================================================


def test_records_from_df_filters_private_placement_only():
    df = _qbzf_df([
        {"股票代码": "600519", "股票简称": "贵州茅台", "发行方式": "定向增发",
         "发行日期": "2026-09-03", "发行价格": 43.34},
        {"股票代码": "000001", "股票简称": "平安银行", "发行方式": "公开增发",
         "发行日期": "2026-09-02", "发行价格": 12.0},
        {"股票代码": "688347", "股票简称": "华虹宏力", "发行方式": "定向增发",
         "发行日期": "2026-09-03", "发行价格": 43.34},
    ])
    records = _records_from_df(df)
    assert len(records) == 2
    assert all(r["issue_method"] == "定向增发" for r in records)
    assert {r["ts_code"] for r in records} == {"600519", "688347"}
    # 交易所前缀规则
    by_code = {r["ts_code"]: r for r in records}
    assert by_code["600519"]["symbol"] == "sh600519"
    assert by_code["688347"]["symbol"] == "sh688347"
    assert by_code["600519"]["issue_date"] == "2026-09-03"
    assert by_code["600519"]["data_source"] == "akshare"
    # 稳定源键：同码同日同方式 → 同键；不同码 → 不同键
    assert by_code["600519"]["source_record_key"] == records[0]["source_record_key"]
    assert len({r["source_record_key"] for r in records}) == 2


def test_records_from_df_drops_invalid_date_and_code():
    df = _qbzf_df([
        {"股票代码": "600519", "股票简称": "茅台", "发行方式": "定向增发",
         "发行日期": "2026-09-03"},
        {"股票代码": "600519", "股票简称": "茅台", "发行方式": "定向增发",
         "发行日期": "无效日期"},
        {"股票代码": "SH.600519", "股票简称": "茅台", "发行方式": "定向增发",
         "发行日期": "2026-09-01"},
        {"股票代码": "无码", "股票简称": "茅台", "发行方式": "定向增发",
         "发行日期": "2026-09-01"},
    ])
    records = _records_from_df(df)
    # 非法日期丢弃；"SH.600519" 提取 6 位数字保留；"无码" 丢弃
    assert len(records) == 2
    assert {r["issue_date"] for r in records} == {"2026-09-03", "2026-09-01"}


def test_records_from_df_empty_and_no_private_placement():
    assert _records_from_df(_qbzf_df([])) == []
    df = _qbzf_df([
        {"股票代码": "000001", "股票简称": "平安", "发行方式": "公开增发",
         "发行日期": "2026-09-02"},
    ])
    assert _records_from_df(df) == []


# ===========================================================================
# update_placement_announcements
# ===========================================================================


def test_update_success():
    db = MagicMock()
    db.save_placement_batch.return_value = 2
    df = _qbzf_df([
        {"股票代码": "600519", "股票简称": "贵州茅台", "发行方式": "定向增发",
         "发行日期": "2026-09-03"},
        {"股票代码": "000001", "股票简称": "平安银行", "发行方式": "公开增发",
         "发行日期": "2026-09-02"},
    ])
    with patch(f"{MODULE}.get_default_client") as client, \
         patch(f"{MODULE}.ak"):
        client.return_value.call.return_value = _success_resp(df)
        r = update_placement_announcements(db)
    assert r == {"saved": 2, "total": 2}
    saved_records = db.save_placement_batch.call_args[0][0]
    assert len(saved_records) == 1
    assert saved_records[0]["ts_code"] == "600519"
    assert saved_records[0]["source_record_key"]


def test_update_ak_not_installed():
    db = MagicMock()
    with patch(f"{MODULE}.ak", None):
        r = update_placement_announcements(db)
    assert r["saved"] == 0
    assert "error" in r


def test_update_empty_upstream_returns_skipped():
    db = MagicMock()
    with patch(f"{MODULE}.get_default_client") as client, \
         patch(f"{MODULE}.ak"):
        client.return_value.call.return_value = _success_resp(pd.DataFrame())
        r = update_placement_announcements(db)
    assert r.get("skipped") is True
    db.save_placement_batch.assert_not_called()


def test_update_no_valid_records_returns_skipped():
    db = MagicMock()
    df = _qbzf_df([
        {"股票代码": "000001", "股票简称": "平安", "发行方式": "公开增发",
         "发行日期": "2026-09-02"},
    ])
    with patch(f"{MODULE}.get_default_client") as client, \
         patch(f"{MODULE}.ak"):
        client.return_value.call.return_value = _success_resp(df)
        r = update_placement_announcements(db)
    assert r.get("skipped") is True
    db.save_placement_batch.assert_not_called()


def test_update_fetch_failure_marks_network_kind():
    db = MagicMock()
    with patch(f"{MODULE}.get_default_client") as client, \
         patch(f"{MODULE}.ak"):
        client.return_value.call.return_value = SimpleNamespace(
            success=False, data=None, metadata=SimpleNamespace(error="eastmoney 503"))
        r = update_placement_announcements(db)
    assert r["saved"] == 0
    assert r["error_kind"] == "network"


def test_update_exception_marks_network_kind():
    db = MagicMock()
    with patch(f"{MODULE}.get_default_client") as client, \
         patch(f"{MODULE}.ak"):
        client.return_value.call.side_effect = RuntimeError("boom")
        r = update_placement_announcements(db)
    assert r["saved"] == 0
    assert r["error_kind"] == "network"


def test_update_symbols_filter():
    db = MagicMock()
    db.save_placement_batch.return_value = 1
    df = _qbzf_df([
        {"股票代码": "600519", "股票简称": "茅台", "发行方式": "定向增发",
         "发行日期": "2026-09-03"},
        {"股票代码": "000001", "股票简称": "平安", "发行方式": "定向增发",
         "发行日期": "2026-09-01"},
    ])
    # --symbols 带交易所后缀的写法也要能匹配 6 位裸码
    with patch(f"{MODULE}.get_default_client") as client, \
         patch(f"{MODULE}.ak"):
        client.return_value.call.return_value = _success_resp(df)
        r = update_placement_announcements(db, symbols=["600519.SH"])
    assert r == {"saved": 1, "total": 2}
    saved_records = db.save_placement_batch.call_args[0][0]
    assert [x["ts_code"] for x in saved_records] == ["600519"]


# ===========================================================================
# fetch_placement_announcements_records（收盘刷新 helper）
# ===========================================================================


def test_fetch_records_success_and_keys():
    df = _qbzf_df([
        {"股票代码": "600519", "股票简称": "贵州茅台", "发行方式": "定向增发",
         "发行日期": "2026-09-03"},
    ])
    with patch(f"{MODULE}.ak") as mock_ak:
        mock_ak.stock_qbzf_em.return_value = df
        records = fetch_placement_announcements_records()
    assert len(records) == 1
    assert records[0]["ts_code"] == "600519"
    assert records[0]["source_record_key"]


def test_fetch_records_empty_returns_list():
    with patch(f"{MODULE}.ak") as mock_ak:
        mock_ak.stock_qbzf_em.return_value = pd.DataFrame()
        assert fetch_placement_announcements_records() == []


def test_fetch_records_source_error_propagates():
    with patch(f"{MODULE}.ak") as mock_ak:
        mock_ak.stock_qbzf_em.side_effect = RuntimeError("upstream down")
        try:
            fetch_placement_announcements_records()
        except RuntimeError as e:
            assert str(e) == "upstream down"
        else:
            raise AssertionError("源异常必须上抛，不得吞掉")
