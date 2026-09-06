"""Edge-path tests for tasks/bars.py.

Fills coverage gaps in:
- _normalize_trade_date pure function (all branches)
- _update_single_bar boundary paths (watchlist, db_lock, increment paths)
"""
from __future__ import annotations

import threading
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

from tasks.bars import _is_suspended_realtime, _normalize_trade_date, _update_single_bar

# ===========================================================================
# _normalize_trade_date — 纯函数全分支覆盖
# ===========================================================================


class TestNormalizeTradeDate:
    """_normalize_trade_date 纯函数测试 — 覆盖全部 7 个分支。"""

    def test_none(self):
        """None → None."""
        assert _normalize_trade_date(None) is None

    def test_date_object(self):
        """date 对象 → YYYY-MM-DD 字符串。"""
        d = date(2026, 7, 19)
        assert _normalize_trade_date(d) == "2026-07-19"

    def test_non_string_non_date(self):
        """既不是字符串也不是 date → None。"""
        assert _normalize_trade_date(20260719) is None
        assert _normalize_trade_date(3.14) is None
        assert _normalize_trade_date([2026, 7, 19]) is None

    def test_empty_string(self):
        """空字符串 → None。"""
        assert _normalize_trade_date("") is None
        assert _normalize_trade_date("   ") is None

    def test_already_normalized(self):
        """YYYY-MM-DD 格式 → 取前 10 位。"""
        assert _normalize_trade_date("2026-07-19") == "2026-07-19"
        # 带多余字符
        assert _normalize_trade_date("2026-07-19 15:30:00") == "2026-07-19"

    def test_digits_compact(self):
        """8+ 位纯数字 → 格式化为 YYYY-MM-DD。"""
        assert _normalize_trade_date("20260719") == "2026-07-19"
        assert _normalize_trade_date("2026-07-19")  # already covered above

    def test_fewer_than_8_digits(self):
        """不足 8 位数字 → 原样返回。"""
        assert _normalize_trade_date("abc") == "abc"
        assert _normalize_trade_date("123") == "123"

    def test_mixed_chars_with_digits(self):
        """混合字符中包含 8+ 位数字 → 提取并格式化。"""
        assert _normalize_trade_date("20260719abc") == "2026-07-19"
        # 注意：提取全部数字后可能有 8 位以上
        assert _normalize_trade_date("date:20260719") == "2026-07-19"


# ===========================================================================
# _update_single_bar — 边缘路径
# ===========================================================================


def _bars_df(dates: list[str]) -> pd.DataFrame:
    return pd.DataFrame({
        "trade_date": dates,
        "open": [10.0] * len(dates),
        "close": [10.5] * len(dates),
        "data_source": ["akshare"] * len(dates),
    })


def _yfinance_df(dates: list[str]) -> pd.DataFrame:
    return pd.DataFrame({
        "trade_date": dates,
        "open": [10.0] * len(dates),
        "close": [10.5] * len(dates),
        "data_source": ["yfinance"] * len(dates),
    })


class TestUpdateSingleBarEdge:
    """补充 test_daily_pipeline.py 中 TestUpdateSingleBar 未覆盖的边界路径。"""

    def test_skipped_when_latest_is_current(self):
        """latest_date >= expected_latest → 跳过不抓取。"""
        db = MagicMock()
        loader = MagicMock()
        db.get_latest_bar_date.return_value = "2026-07-19"
        with patch("tasks.bars.get_expected_latest_trading_day",
                   return_value="2026-07-19"):
            result = _update_single_bar(
                db, loader, "000001.SZ",
            )
        assert result == "skipped"
        loader.incremental_update.assert_not_called()

    def test_yfinance_only_non_watchlist_full_load(self):
        """非自选股全量加载全部是 yfinance → 返回 failed。"""
        db = MagicMock()
        loader = MagicMock()
        db.get_latest_bar_date.return_value = "2026-07-16"
        db.get_daily_bars.return_value = pd.DataFrame()  # 无现有数据
        loader.get_daily_bars.return_value = _yfinance_df(
            ["2026-07-17", "2026-07-18", "2026-07-19"]
        )
        with patch("tasks.bars.get_expected_latest_trading_day",
                   return_value="2026-07-19"), \
             patch("tasks.bars.time.sleep"):
            result = _update_single_bar(
                db, loader, "000001.SZ",
            )
        assert result == "failed"
        db.save_daily_bars.assert_not_called()

    def test_watchlist_backfill_with_db_lock(self):
        """自选股全量回填 + db_lock。"""
        db = MagicMock()
        loader = MagicMock()
        loader.get_daily_bars.return_value = _bars_df(
            ["2026-01-01", "2026-01-02"]
        )
        lock = threading.Lock()
        with patch("tasks.bars.get_expected_latest_trading_day",
                   return_value="2026-07-19"), \
             patch("tasks.bars.time.sleep"):
            result = _update_single_bar(
                db, loader, "000001.SZ",
                watchlist_symbols={"000001.SZ"},
                backfilled_symbols=set(),
                backfill_file=Path("/tmp/test_backfill.txt"),
                db_lock=lock,
            )
        assert result == "success"
        db.save_daily_bars.assert_called_once()

    def test_incremental_update_unchanged_stale(self):
        """增量更新未取得新数据且最新日期低于预期 → failed。"""
        db = MagicMock()
        loader = MagicMock()
        db.get_latest_bar_date.return_value = "2026-07-16"
        existing = _bars_df(["2026-07-16"])
        db.get_daily_bars.return_value = existing
        loader.incremental_update.return_value = existing  # 行数不变
        with patch("tasks.bars.get_expected_latest_trading_day",
                   return_value="2026-07-19"), \
             patch("tasks.bars._is_suspended_realtime", return_value=False), \
             patch("tasks.bars.time.sleep"):
            result = _update_single_bar(
                db, loader, "000001.SZ",
            )
        assert result == "failed"
        db.save_daily_bars.assert_not_called()

    def test_incremental_stale_but_suspended_skipped(self):
        """增量无新数据但实时确认为停牌（雪球 status=2）→ skipped，不计失败。

        场景：最新交易日当天才开始停牌的股票（如 2026-09-04 起停牌的 *ST康佳A），
        只落后 1 天够不到停牌预检阈值，全源无数据属正常，不应误计失败。
        """
        db = MagicMock()
        loader = MagicMock()
        db.get_latest_bar_date.return_value = "2026-07-16"
        existing = _bars_df(["2026-07-16"])
        db.get_daily_bars.return_value = existing
        loader.incremental_update.return_value = existing  # 行数不变
        suspended: set[str] = set()
        with patch("tasks.bars.get_expected_latest_trading_day",
                   return_value="2026-07-19"), \
             patch("tasks.bars._is_suspended_realtime", return_value=True), \
             patch("tasks.bars.time.sleep"):
            result = _update_single_bar(
                db, loader, "000016.SZ", suspended_symbols=suspended,
            )
        assert result == "skipped"
        assert suspended == {"000016"}  # 并入停牌集合，同轮后续路径直接跳过
        db.save_daily_bars.assert_not_called()

    def test_watchlist_backfill_yfinance_all(self):
        """自选股全量拉取全部是 yfinance → 返回 failed 不保存。"""
        db = MagicMock()
        loader = MagicMock()
        loader.get_daily_bars.return_value = _yfinance_df(
            ["2026-01-01", "2026-01-02"]
        )
        with patch("tasks.bars.get_expected_latest_trading_day",
                   return_value="2026-07-19"), \
             patch("tasks.bars.time.sleep"):
            result = _update_single_bar(
                db, loader, "000001.SZ",
                watchlist_symbols={"000001.SZ"},
                backfilled_symbols=set(),
                backfill_file=Path("/tmp/test_backfill_yf.txt"),
            )
        assert result == "failed"
        db.save_daily_bars.assert_not_called()

    def test_full_load_empty_result(self):
        """全量加载返回空 DataFrame → skipped。"""
        db = MagicMock()
        loader = MagicMock()
        db.get_latest_bar_date.return_value = None  # 全新股票
        db.get_daily_bars.return_value = pd.DataFrame()
        loader.get_daily_bars.return_value = pd.DataFrame()
        with patch("tasks.bars.get_expected_latest_trading_day",
                   return_value="2026-07-19"), \
             patch("tasks.bars.time.sleep"):
            result = _update_single_bar(
                db, loader, "000001.SZ",
            )
        assert result == "skipped"
        db.save_daily_bars.assert_not_called()


# ===========================================================================
# _is_suspended_realtime — 全源落空后的实时停牌确认
# ===========================================================================


def _stub_xueqiu_modules(mock_xq):
    """构造可注入 sys.modules 的 smartmoney_hunter stub。"""
    import types

    pkg = types.ModuleType("smartmoney_hunter")
    pkg.xueqiu = mock_xq
    return {"smartmoney_hunter": pkg, "smartmoney_hunter.xueqiu": mock_xq}


class TestIsSuspendedRealtime:
    """雪球 status==2 → True；其余（正常/异常/北交所）→ False。"""

    def test_status_2_returns_true(self):
        import sys

        mock_xq = MagicMock()
        mock_xq.get_batch_quotes.return_value = [{"code": "000016", "status": 2}]
        with patch.dict(sys.modules, _stub_xueqiu_modules(mock_xq)):
            assert _is_suspended_realtime("000016.SZ") is True
        mock_xq.get_batch_quotes.assert_called_once_with(["000016"])

    def test_status_1_returns_false(self):
        import sys

        mock_xq = MagicMock()
        mock_xq.get_batch_quotes.return_value = [{"code": "000001", "status": 1}]
        with patch.dict(sys.modules, _stub_xueqiu_modules(mock_xq)):
            assert _is_suspended_realtime("000001.SZ") is False

    def test_xueqiu_error_returns_false(self):
        """雪球查询异常 → 静默降级 False（保持原失败语义）。"""
        import sys

        mock_xq = MagicMock()
        mock_xq.get_batch_quotes.side_effect = RuntimeError("network down")
        with patch.dict(sys.modules, _stub_xueqiu_modules(mock_xq)):
            assert _is_suspended_realtime("000016.SZ") is False

    def test_beijing_skipped_without_query(self):
        """北交所雪球不支持 → 直接 False，不发请求。

        注：conftest 将 is_beijing_stock 全局打桩为 False，此处按真实行为补桩。
        """
        import sys

        mock_xq = MagicMock()
        with patch("tasks.bars.is_beijing_stock", return_value=True), \
             patch.dict(sys.modules, _stub_xueqiu_modules(mock_xq)):
            assert _is_suspended_realtime("920685.BJ") is False
        mock_xq.get_batch_quotes.assert_not_called()
