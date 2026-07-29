"""tasks.market_flow 覆盖率测试：聚焦 update_fund_flow 主路径与边界。

db / loader / akshare 全部 mock，不触发真实网络或数据库写。
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

import tasks.market_flow as mf


def _flow_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "code": "600000",
                "main_net_inflow": 100.0,
                "main_net_inflow_pct": 1.0,
                "super_large_net_inflow": 50.0,
                "super_large_net_inflow_pct": 0.5,
                "large_net_inflow": 30.0,
                "large_net_inflow_pct": 0.3,
            },
            {
                "code": "",  # 空 code，跳过
                "main_net_inflow": 1.0,
                "main_net_inflow_pct": 0.1,
                "super_large_net_inflow": 0.5,
                "super_large_net_inflow_pct": 0.05,
                "large_net_inflow": 0.3,
                "large_net_inflow_pct": 0.03,
            },
            {
                "code": "000001",
                # 六个数值字段全为空 → 整行跳过
                "main_net_inflow": float("nan"),
                "main_net_inflow_pct": float("nan"),
                "super_large_net_inflow": float("nan"),
                "super_large_net_inflow_pct": float("nan"),
                "large_net_inflow": float("nan"),
                "large_net_inflow_pct": float("nan"),
            },
        ]
    )


def test_update_fund_flow_happy_path():
    db = MagicMock()
    db.save_fund_flow_batch.return_value = 1
    loader = MagicMock()
    loader.get_market_fund_flow.return_value = _flow_df()
    with patch.object(mf, "get_expected_latest_trading_day", return_value="2026-07-20"):
        res = mf.update_fund_flow(db, loader)
    # 仅 600000 有效；空 code 与全空行被跳过 → 1 条
    assert res["saved"] == 1
    assert res["total"] == 3
    db.save_fund_flow_batch.assert_called_once()
    saved_rows = db.save_fund_flow_batch.call_args.args[0]
    assert len(saved_rows) == 1
    assert saved_rows[0]["symbol"] == "600000"


def test_update_fund_flow_empty():
    db = MagicMock()
    loader = MagicMock()
    loader.get_market_fund_flow.return_value = pd.DataFrame()
    with patch.object(mf, "get_expected_latest_trading_day", return_value="2026-07-20"):
        res = mf.update_fund_flow(db, loader)
    assert res == {"saved": 0, "total": 0}
    db.save_fund_flow_batch.assert_not_called()


def test_update_fund_flow_symbols_filter():
    db = MagicMock()
    db.save_fund_flow_batch.return_value = 1
    loader = MagicMock()
    loader.get_market_fund_flow.return_value = _flow_df()
    with patch.object(mf, "get_expected_latest_trading_day", return_value="2026-07-20"):
        res = mf.update_fund_flow(db, loader, symbols=["600000"])
    assert res["saved"] == 1
    saved_rows = db.save_fund_flow_batch.call_args.args[0]
    assert all(r["symbol"] in {"600000"} for r in saved_rows)


def test_update_fund_flow_loader_raises():
    db = MagicMock()
    loader = MagicMock()
    loader.get_market_fund_flow.side_effect = RuntimeError("boom")
    with patch.object(mf, "get_expected_latest_trading_day", return_value="2026-07-20"):
        res = mf.update_fund_flow(db, loader)
    assert res["saved"] == 0
    assert res["total"] == 0
    assert "error" in res


def test_safe_fetch_margin_detail_length_mismatch():
    fetcher = MagicMock()
    fetcher.side_effect = ValueError("Length mismatch: Expected axis")
    assert mf._safe_fetch_margin_detail(fetcher, "20260720", "sh") is None


def test_safe_fetch_margin_detail_other_value_error_raises():
    fetcher = MagicMock()
    fetcher.side_effect = ValueError("something else")
    try:
        mf._safe_fetch_margin_detail(fetcher, "20260720", "sh")
    except ValueError:
        pass
    else:
        raise AssertionError("should have raised")


def _margin_df(exchange: str) -> pd.DataFrame:
    code_col = "标的证券代码" if exchange == "sh" else "证券代码"
    return pd.DataFrame(
        [
            {
                code_col: "600000",
                "融资余额": 100.0,
                "融资买入额": 10.0,
                "融资偿还额": 5.0,
                "融券余量": 2.0,
                "融券卖出量": 1.0,
                "融券偿还量": 0.5,
                "融资融券余额": 105.0,
            }
        ]
    )


def test_update_margin_trading_happy_path():
    db = MagicMock()
    db.save_margin_trading_batch.return_value = 2
    fake_ak = MagicMock()
    fake_ak.stock_margin_detail_sse.return_value = _margin_df("sh")
    fake_ak.stock_margin_detail_szse.return_value = _margin_df("sz")
    with patch.object(mf, "ak", fake_ak), patch.object(
        mf, "get_expected_latest_trading_day", return_value="2026-07-20"
    ):
        res = mf.update_margin_trading(db)
    assert res["saved"] == 2
    assert res["total"] == 2
    db.save_margin_trading_batch.assert_called_once()


def test_update_margin_trading_ak_none():
    db = MagicMock()
    with patch.object(mf, "ak", None), patch.object(
        mf, "get_expected_latest_trading_day", return_value="2026-07-20"
    ):
        res = mf.update_margin_trading(db)
    assert res["error"] == "akshare not installed"
    db.save_margin_trading_batch.assert_not_called()


def test_update_dragon_tiger_happy_path():
    db = MagicMock()
    db.save_dragon_tiger_batch.return_value = 1
    fake_ak = MagicMock()
    fake_ak.stock_lhb_detail_em.return_value = pd.DataFrame(
        [{"代码": "600000", "收盘价": 10.0, "涨跌幅": 1.0, "龙虎榜净买额": 5.0,
          "龙虎榜买入额": 8.0, "龙虎榜卖出额": 3.0, "换手率": 2.0, "流通市值": 100.0,
          "上榜原因": "涨幅偏离"}]
    )
    with patch.object(mf, "ak", fake_ak), patch.object(
        mf, "get_expected_latest_trading_day", return_value="2026-07-20"
    ):
        res = mf.update_dragon_tiger(db)
    assert res["saved"] == 1
    assert res["total"] == 1
    db.save_dragon_tiger_batch.assert_called_once()


def test_update_dragon_tiger_ak_none():
    db = MagicMock()
    with patch.object(mf, "ak", None), patch.object(
        mf, "get_expected_latest_trading_day", return_value="2026-07-20"
    ):
        res = mf.update_dragon_tiger(db)
    assert res["error"] == "akshare not installed"


def test_update_block_trade_happy_path():
    db = MagicMock()
    db.save_block_trade_batch.return_value = 1
    fake_ak = MagicMock()
    fake_ak.stock_dzjy_mrmx.return_value = pd.DataFrame(
        [{"证券代码": "600000", "成交价": 10.0, "收盘价": 9.5, "折溢率": -5.0,
          "成交量": 100, "成交额": 1000, "买方营业部": "A", "卖方营业部": "B"}]
    )
    with patch.object(mf, "ak", fake_ak), patch.object(
        mf, "get_expected_latest_trading_day", return_value="2026-07-20"
    ):
        res = mf.update_block_trade(db)
    assert res["saved"] == 1
    assert res["total"] == 1
    db.save_block_trade_batch.assert_called_once()


def test_update_block_trade_ak_none():
    db = MagicMock()
    with patch.object(mf, "ak", None), patch.object(
        mf, "get_expected_latest_trading_day", return_value="2026-07-20"
    ):
        res = mf.update_block_trade(db)
    assert res["error"] == "akshare not installed"


def test_update_sector_fund_flow_happy_path():
    db = MagicMock()
    db.save_sector_fund_flow_batch.return_value = 1
    fake_ak = MagicMock()
    fake_ak.stock_fund_flow_industry.return_value = pd.DataFrame(
        [{"行业": "银行", "净额": 10.0, "行业-涨跌幅": 1.0, "流入资金": 5.0, "流出资金": 3.0}]
    )
    with patch.object(mf, "ak", fake_ak), patch.object(
        mf, "get_expected_latest_trading_day", return_value="2026-07-20"
    ):
        res = mf.update_sector_fund_flow(db)
    assert res["saved"] == 1
    assert res["total"] == 1
    db.save_sector_fund_flow_batch.assert_called_once()


def test_update_sector_fund_flow_ak_none():
    db = MagicMock()
    with patch.object(mf, "ak", None), patch.object(
        mf, "get_expected_latest_trading_day", return_value="2026-07-20"
    ):
        res = mf.update_sector_fund_flow(db)
    assert res["error"] == "akshare not installed"


def test_update_margin_trading_empty_data():
    """融资融券两市均无数据时返回 saved=0。"""
    db = MagicMock()
    fake_ak = MagicMock()
    fake_ak.stock_margin_detail_sse.return_value = pd.DataFrame()
    fake_ak.stock_margin_detail_szse.return_value = pd.DataFrame()
    with patch.object(mf, "ak", fake_ak), patch.object(
        mf, "get_expected_latest_trading_day", return_value="2026-07-20"
    ):
        res = mf.update_margin_trading(db)
    assert res["saved"] == 0
    assert res["total"] == 0
    db.save_margin_trading_batch.assert_not_called()


def test_update_margin_trading_exception():
    """融资融券 fetcher 异常时跳过该交易所。"""
    db = MagicMock()
    db.save_margin_trading_batch.return_value = 1
    fake_ak = MagicMock()
    fake_ak.stock_margin_detail_sse.side_effect = RuntimeError("sse down")
    fake_ak.stock_margin_detail_szse.return_value = _margin_df("sz")
    with patch.object(mf, "ak", fake_ak), patch.object(
        mf, "get_expected_latest_trading_day", return_value="2026-07-20"
    ):
        res = mf.update_margin_trading(db)
    assert res["saved"] == 1
    assert res["total"] == 1


def test_update_margin_trading_fallback_date():
    """当日无数据时回退到前一天。"""
    db = MagicMock()
    db.save_margin_trading_batch.return_value = 1
    fake_ak = MagicMock()
    # 当天返回空，前一天返回有效数据
    fake_ak.stock_margin_detail_sse.side_effect = [pd.DataFrame(), _margin_df("sh")]
    fake_ak.stock_margin_detail_szse.side_effect = [pd.DataFrame(), pd.DataFrame()]
    with patch.object(mf, "ak", fake_ak), patch.object(
        mf, "get_expected_latest_trading_day", return_value="2026-07-20"
    ):
        res = mf.update_margin_trading(db)
    assert res["saved"] == 1


def test_update_margin_trading_monday_falls_back_to_friday():
    db = MagicMock()
    db.save_margin_trading_batch.return_value = 1
    fake_ak = MagicMock()

    def fetch(*, date):
        return _margin_df("sh") if date == "20260717" else pd.DataFrame()

    fake_ak.stock_margin_detail_sse.side_effect = fetch
    fake_ak.stock_margin_detail_szse.return_value = pd.DataFrame()
    with (
        patch.object(mf, "ak", fake_ak),
        patch.object(mf, "get_expected_latest_trading_day", return_value="2026-07-20"),
        patch.object(
            mf,
            "get_recent_trading_days",
            return_value=["2026-07-20", "2026-07-17", "2026-07-16"],
            create=True,
        ),
    ):
        result = mf.update_margin_trading(db)

    assert result["saved"] == 1
    assert any(call.kwargs.get("date") == "20260717" for call in fake_ak.stock_margin_detail_sse.call_args_list)


def test_update_dragon_tiger_empty():
    """龙虎榜空数据时返回 saved=0。"""
    db = MagicMock()
    fake_ak = MagicMock()
    fake_ak.stock_lhb_detail_em.return_value = pd.DataFrame()
    with patch.object(mf, "ak", fake_ak), patch.object(
        mf, "get_expected_latest_trading_day", return_value="2026-07-20"
    ):
        res = mf.update_dragon_tiger(db)
    assert res["saved"] == 0
    db.save_dragon_tiger_batch.assert_not_called()


def test_update_dragon_tiger_exception():
    """龙虎榜 akshare 异常时返回 error。"""
    db = MagicMock()
    fake_ak = MagicMock()
    fake_ak.stock_lhb_detail_em.side_effect = RuntimeError("lhb down")
    with patch.object(mf, "ak", fake_ak), patch.object(
        mf, "get_expected_latest_trading_day", return_value="2026-07-20"
    ):
        res = mf.update_dragon_tiger(db)
    assert res["saved"] == 0
    assert "error" in res


def test_update_block_trade_empty():
    """大宗交易空数据时返回 saved=0。"""
    db = MagicMock()
    fake_ak = MagicMock()
    fake_ak.stock_dzjy_mrmx.return_value = pd.DataFrame()
    with patch.object(mf, "ak", fake_ak), patch.object(
        mf, "get_expected_latest_trading_day", return_value="2026-07-20"
    ):
        res = mf.update_block_trade(db)
    assert res["saved"] == 0
    db.save_block_trade_batch.assert_not_called()


def test_update_block_trade_exception():
    """大宗交易 akshare 异常时返回 error。"""
    db = MagicMock()
    fake_ak = MagicMock()
    fake_ak.stock_dzjy_mrmx.side_effect = RuntimeError("dzjy down")
    with patch.object(mf, "ak", fake_ak), patch.object(
        mf, "get_expected_latest_trading_day", return_value="2026-07-20"
    ):
        res = mf.update_block_trade(db)
    assert res["saved"] == 0
    assert "error" in res


def test_update_sector_fund_flow_empty():
    """板块资金流向空数据时返回 saved=0。"""
    db = MagicMock()
    fake_ak = MagicMock()
    fake_ak.stock_fund_flow_industry.return_value = pd.DataFrame()
    with patch.object(mf, "ak", fake_ak), patch.object(
        mf, "get_expected_latest_trading_day", return_value="2026-07-20"
    ):
        res = mf.update_sector_fund_flow(db)
    assert res["saved"] == 0
    db.save_sector_fund_flow_batch.assert_not_called()


def test_update_sector_fund_flow_exception():
    """板块资金流向 akshare 异常时返回 error。"""
    db = MagicMock()
    fake_ak = MagicMock()
    fake_ak.stock_fund_flow_industry.side_effect = RuntimeError("industry flow down")
    with patch.object(mf, "ak", fake_ak), patch.object(
        mf, "get_expected_latest_trading_day", return_value="2026-07-20"
    ):
        res = mf.update_sector_fund_flow(db)
    assert res["saved"] == 0
    assert "error" in res


# ===========================================================================
# 收盘刷新 helper（Task 6）：fetch_fund_flow_records 不落库
# ===========================================================================

_REFRESH_TARGET = "2026-07-27"


class _RecordingFlowLoader:
    """记录调用次数并返回预设 DataFrame 的手写 fake。"""

    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.calls = 0

    def get_market_fund_flow(self) -> pd.DataFrame:
        self.calls += 1
        return self.df


def test_fetch_fund_flow_records_legacy_shape():
    """返回 legacy 形状（symbol/date 键），跳过空 code 与全空数值行。"""
    loader = _RecordingFlowLoader(_flow_df())

    records = mf.fetch_fund_flow_records(loader, _REFRESH_TARGET)

    assert loader.calls == 1
    assert len(records) == 1
    rec = records[0]
    assert rec["symbol"] == "600000"
    assert rec["date"] == _REFRESH_TARGET
    assert rec["main_net_inflow"] == 100.0
    assert rec["simulated"] is False


def test_fetch_fund_flow_records_converts_nan_to_none():
    """部分 NaN 数值字段转为 None，行仍保留。"""
    df = pd.DataFrame(
        [
            {
                "code": "000002",
                "main_net_inflow": 5.0,
                "main_net_inflow_pct": float("nan"),
                "super_large_net_inflow": float("nan"),
                "super_large_net_inflow_pct": float("nan"),
                "large_net_inflow": float("nan"),
                "large_net_inflow_pct": float("nan"),
            }
        ]
    )
    records = mf.fetch_fund_flow_records(_RecordingFlowLoader(df), _REFRESH_TARGET)

    assert len(records) == 1
    rec = records[0]
    assert rec["main_net_inflow"] == 5.0
    assert rec["main_net_inflow_pct"] is None
    assert rec["large_net_inflow"] is None


def test_fetch_fund_flow_records_empty_source():
    """源端空 DataFrame → 返回空列表（失败语义由适配器把关）。"""
    records = mf.fetch_fund_flow_records(
        _RecordingFlowLoader(pd.DataFrame()), _REFRESH_TARGET
    )
    assert records == []


def test_fetch_fund_flow_records_propagates_loader_error():
    """loader 异常直接上抛，不吃掉（保留旧数据由编排器处理）。"""

    class _BrokenLoader:
        def get_market_fund_flow(self):
            raise ConnectionError("eastmoney down")

    with pytest.raises(ConnectionError):
        mf.fetch_fund_flow_records(_BrokenLoader(), _REFRESH_TARGET)


# ===========================================================================
# 事件键（Task 8）：同股同日合法多事件必须携带互异稳定键并在真库中存活
# ===========================================================================


def test_update_dragon_tiger_attaches_distinct_source_keys():
    """同股同日两条不同上榜原因 → 两条记录携带互异的 source_record_key。"""
    db = MagicMock()
    db.save_dragon_tiger_batch.return_value = 2
    fake_ak = MagicMock()
    fake_ak.stock_lhb_detail_em.return_value = pd.DataFrame(
        [
            {"代码": "600000", "收盘价": 10.0, "涨跌幅": 1.0, "龙虎榜净买额": 5.0,
             "龙虎榜买入额": 8.0, "龙虎榜卖出额": 3.0, "换手率": 2.0, "流通市值": 100.0,
             "上榜原因": "涨幅偏离"},
            {"代码": "600000", "收盘价": 10.0, "涨跌幅": 1.0, "龙虎榜净买额": 5.0,
             "龙虎榜买入额": 8.0, "龙虎榜卖出额": 3.0, "换手率": 2.0, "流通市值": 100.0,
             "上榜原因": "换手率达20%"},
        ]
    )
    with patch.object(mf, "ak", fake_ak), patch.object(
        mf, "get_expected_latest_trading_day", return_value="2026-07-20"
    ):
        res = mf.update_dragon_tiger(db)

    assert res["saved"] == 2
    saved_rows = db.save_dragon_tiger_batch.call_args.args[0]
    keys = [r["source_record_key"] for r in saved_rows]
    assert len(keys) == 2
    assert keys[0] != keys[1]
    assert all(isinstance(k, str) and len(k) == 64 for k in keys)


def test_update_block_trade_attaches_distinct_source_keys():
    """同股同日两笔不同价格/成交量的大宗交易 → 键互异。"""
    db = MagicMock()
    db.save_block_trade_batch.return_value = 2
    fake_ak = MagicMock()
    fake_ak.stock_dzjy_mrmx.return_value = pd.DataFrame(
        [
            {"证券代码": "600000", "成交价": 10.0, "收盘价": 9.5, "折溢率": -5.0,
             "成交量": 100, "成交额": 1000, "买方营业部": "A", "卖方营业部": "B"},
            {"证券代码": "600000", "成交价": 9.8, "收盘价": 9.5, "折溢率": -3.0,
             "成交量": 200, "成交额": 1960, "买方营业部": "A", "卖方营业部": "B"},
        ]
    )
    with patch.object(mf, "ak", fake_ak), patch.object(
        mf, "get_expected_latest_trading_day", return_value="2026-07-20"
    ):
        res = mf.update_block_trade(db)

    assert res["saved"] == 2
    saved_rows = db.save_block_trade_batch.call_args.args[0]
    keys = [r["source_record_key"] for r in saved_rows]
    assert len(keys) == 2
    assert keys[0] != keys[1]
    assert all(isinstance(k, str) and len(k) == 64 for k in keys)


def _make_event_provider(tmp_path):
    """真临时 SQLite 库上的 SmartMoneyDBProvider（conftest 会 mock DatabaseManager，
    需显式重绑 db_path 后重跑建表与迁移，同 test_providers_extended2 fixture）。"""
    from providers import SmartMoneyDBProvider

    db_path = tmp_path / "event_keys_test.db"
    provider = SmartMoneyDBProvider(db_path=str(db_path))
    provider._db.db_path = str(db_path)
    provider._ensure_wal_mode()
    provider._ensure_tables()
    provider._run_versioned_migrations()
    return provider


def _dragon_records() -> list[dict]:
    base = {
        "ts_code": "600000",
        "trade_date": "2026-07-21",
        "close_price": 10.0,
        "pct_change": 1.0,
        "net_buy_amount": 5.0,
        "buy_amount": 8.0,
        "sell_amount": 3.0,
        "turnover_rate": 2.0,
        "market_cap": 100.0,
        "data_source": "akshare",
    }
    return [
        {**base, "reason": "涨幅偏离"},
        {**base, "reason": "换手率达20%"},
    ]


def _block_records() -> list[dict]:
    base = {
        "ts_code": "600000",
        "trade_date": "2026-07-21",
        "close_price": 9.5,
        "buyer_branch": "A",
        "seller_branch": "B",
        "data_source": "akshare",
    }
    return [
        {**base, "deal_price": 10.0, "discount_rate": -5.0, "volume": 100.0, "amount": 1000.0},
        {**base, "deal_price": 9.8, "discount_rate": -3.0, "volume": 200.0, "amount": 1960.0},
    ]


def test_provider_dragon_tiger_batch_keeps_two_reasons_and_dedupes_on_rerun(tmp_path):
    """真库：同股同日两条不同原因均存活；重跑同批不膨胀。"""
    import sqlite3

    provider = _make_event_provider(tmp_path)
    try:
        assert provider.save_dragon_tiger_batch(_dragon_records()) >= 2
        provider.save_dragon_tiger_batch(_dragon_records())
        with sqlite3.connect(provider.db_path) as conn:
            rows = conn.execute(
                """SELECT reason FROM dragon_tiger
                   WHERE ts_code = '600000' AND trade_date = '2026-07-21'
                   ORDER BY reason"""
            ).fetchall()
    finally:
        provider.close()
    assert rows == [("换手率达20%",), ("涨幅偏离",)]


def test_provider_block_trade_batch_keeps_two_deals_and_dedupes_on_rerun(tmp_path):
    """真库：同股同日两笔不同价/量的大宗交易均存活；重跑不膨胀。"""
    import sqlite3

    provider = _make_event_provider(tmp_path)
    try:
        assert provider.save_block_trade_batch(_block_records()) >= 2
        provider.save_block_trade_batch(_block_records())
        with sqlite3.connect(provider.db_path) as conn:
            rows = conn.execute(
                """SELECT deal_price, volume FROM block_trade
                   WHERE ts_code = '600000' AND trade_date = '2026-07-21'
                   ORDER BY deal_price"""
            ).fetchall()
    finally:
        provider.close()
    assert rows == [(9.8, 200.0), (10.0, 100.0)]


# ===========================================================================
# 收盘刷新 helper（Task 9）：margin/龙虎榜/大宗/板块资金流 只抓取归一化，不写库
# ===========================================================================


def _margin_refresh_ak() -> MagicMock:
    fake_ak = MagicMock()
    fake_ak.stock_margin_detail_sse.return_value = pd.DataFrame(
        [{"标的证券代码": "600000", "融资余额": 100.0, "融资买入额": 10.0,
          "融资偿还额": 5.0, "融券余量": 1.0, "融券卖出量": 2.0,
          "融券偿还量": 3.0, "融资融券余额": 110.0}]
    )
    fake_ak.stock_margin_detail_szse.return_value = pd.DataFrame(
        [{"证券代码": "000001", "融资余额": 200.0, "融资买入额": 20.0,
          "融券余量": 4.0, "融券卖出量": 5.0, "融资融券余额": 220.0}]
    )
    return fake_ak


def test_fetch_margin_trading_records_normalizes_both_exchanges():
    """沪深两市列名互异 → 统一 legacy 形状；trade_date 用入参 ISO 日期。"""
    with patch.object(mf, "ak", _margin_refresh_ak()):
        records = mf.fetch_margin_trading_records(_REFRESH_TARGET)

    assert len(records) == 2
    by_code = {r["ts_code"]: r for r in records}
    assert by_code["600000"]["trade_date"] == _REFRESH_TARGET
    assert by_code["600000"]["margin_repay"] == 5.0
    assert by_code["000001"]["margin_repay"] is None  # 深市无该列
    assert by_code["000001"]["total_balance"] == 220.0
    assert all(r["data_source"] == "akshare" for r in records)


def test_fetch_margin_trading_records_length_mismatch_is_empty():
    """AkShare 空日返回 Length mismatch → 视为该市当日空，不是失败。"""
    fake_ak = MagicMock()
    fake_ak.stock_margin_detail_sse.side_effect = ValueError(
        "Length mismatch: Expected axis has 0 elements"
    )
    fake_ak.stock_margin_detail_szse.return_value = pd.DataFrame()
    with patch.object(mf, "ak", fake_ak):
        assert mf.fetch_margin_trading_records(_REFRESH_TARGET) == []


def test_fetch_margin_trading_records_propagates_source_error():
    """源异常直接上抛（保留旧数据由编排器处理）。"""
    fake_ak = MagicMock()
    fake_ak.stock_margin_detail_sse.side_effect = ConnectionError("sse down")
    with patch.object(mf, "ak", fake_ak), pytest.raises(ConnectionError):
        mf.fetch_margin_trading_records(_REFRESH_TARGET)


def test_fetch_dragon_tiger_records_shape_and_distinct_keys():
    """同股同日两条不同上榜原因 → 两条记录携带互异 64 位稳定键。"""
    fake_ak = MagicMock()
    fake_ak.stock_lhb_detail_em.return_value = pd.DataFrame(
        [
            {"代码": "600000", "收盘价": 10.0, "涨跌幅": 1.0, "龙虎榜净买额": 5.0,
             "龙虎榜买入额": 8.0, "龙虎榜卖出额": 3.0, "换手率": 2.0,
             "流通市值": 100.0, "上榜原因": "涨幅偏离"},
            {"代码": "600000", "收盘价": 10.0, "涨跌幅": 1.0, "龙虎榜净买额": 5.0,
             "龙虎榜买入额": 8.0, "龙虎榜卖出额": 3.0, "换手率": 2.0,
             "流通市值": 100.0, "上榜原因": "换手率达20%"},
        ]
    )
    with patch.object(mf, "ak", fake_ak):
        records = mf.fetch_dragon_tiger_records(_REFRESH_TARGET)

    fake_ak.stock_lhb_detail_em.assert_called_once_with(
        start_date="20260727", end_date="20260727"
    )
    assert len(records) == 2
    assert all(r["trade_date"] == _REFRESH_TARGET for r in records)
    keys = [r["source_record_key"] for r in records]
    assert keys[0] != keys[1]
    assert all(isinstance(k, str) and len(k) == 64 for k in keys)


def test_fetch_dragon_tiger_records_authoritative_empty():
    """源端权威空榜 → 返回空列表而非异常（空榜 ≠ 源失败）。"""
    fake_ak = MagicMock()
    fake_ak.stock_lhb_detail_em.return_value = pd.DataFrame()
    with patch.object(mf, "ak", fake_ak):
        assert mf.fetch_dragon_tiger_records(_REFRESH_TARGET) == []


def test_fetch_dragon_tiger_records_propagates_source_error():
    fake_ak = MagicMock()
    fake_ak.stock_lhb_detail_em.side_effect = ConnectionError("em down")
    with patch.object(mf, "ak", fake_ak), pytest.raises(ConnectionError):
        mf.fetch_dragon_tiger_records(_REFRESH_TARGET)


def test_fetch_block_trade_records_shape_and_distinct_keys():
    """同股同日两笔不同价/量 → 键互异；空榜返回 []；异常上抛。"""
    fake_ak = MagicMock()
    fake_ak.stock_dzjy_mrmx.return_value = pd.DataFrame(
        [
            {"证券代码": "600000", "成交价": 10.0, "收盘价": 9.5, "折溢率": -5.0,
             "成交量": 100, "成交额": 1000, "买方营业部": "A", "卖方营业部": "B"},
            {"证券代码": "600000", "成交价": 9.8, "收盘价": 9.5, "折溢率": -3.0,
             "成交量": 200, "成交额": 1960, "买方营业部": "A", "卖方营业部": "B"},
        ]
    )
    with patch.object(mf, "ak", fake_ak):
        records = mf.fetch_block_trade_records(_REFRESH_TARGET)

    assert len(records) == 2
    keys = [r["source_record_key"] for r in records]
    assert keys[0] != keys[1]
    assert all(isinstance(k, str) and len(k) == 64 for k in keys)

    fake_ak.stock_dzjy_mrmx.return_value = pd.DataFrame()
    with patch.object(mf, "ak", fake_ak):
        assert mf.fetch_block_trade_records(_REFRESH_TARGET) == []

    fake_ak.stock_dzjy_mrmx.side_effect = ConnectionError("em down")
    with patch.object(mf, "ak", fake_ak), pytest.raises(ConnectionError):
        mf.fetch_block_trade_records(_REFRESH_TARGET)


def test_fetch_sector_fund_flow_records_shape():
    """同花顺即时快照归一化为 legacy 记录形状；NaN → None。"""
    fake_ak = MagicMock()
    fake_ak.stock_fund_flow_industry.return_value = pd.DataFrame(
        [
            {"行业": "半导体", "净额": 1.5, "行业-涨跌幅": 2.0,
             "流入资金": 3.0, "流出资金": float("nan")},
            {"行业": "", "净额": 9.9},
        ]
    )
    with patch.object(mf, "ak", fake_ak):
        records = mf.fetch_sector_fund_flow_records(_REFRESH_TARGET)

    assert len(records) == 1
    rec = records[0]
    assert rec["sector_name"] == "半导体"
    assert rec["trade_date"] == _REFRESH_TARGET
    assert rec["main_net_inflow"] == 1.5
    assert rec["medium_net_inflow"] is None  # NaN → None
    assert rec["data_source"] == "ths"


def test_fetch_sector_fund_flow_records_propagates_source_error():
    fake_ak = MagicMock()
    fake_ak.stock_fund_flow_industry.side_effect = ConnectionError("ths down")
    with patch.object(mf, "ak", fake_ak), pytest.raises(ConnectionError):
        mf.fetch_sector_fund_flow_records(_REFRESH_TARGET)
