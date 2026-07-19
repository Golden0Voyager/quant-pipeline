"""tasks.market_flow 覆盖率测试：聚焦 update_fund_flow 主路径与边界。

db / loader / akshare 全部 mock，不触发真实网络或数据库写。
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd

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
