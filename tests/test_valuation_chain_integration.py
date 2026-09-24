"""集成测试：tasks/valuation_chain.py 的中层函数。

使用真实 SQLite 数据库或精细 Mock 覆盖：
- update_fundamentals（symbols 过滤、回退日期）
- update_market_snapshot（无 Token 提前返回、有 Token 全流程）
- update_historical_valuation（symbols 过滤、去重、batch save）
- update_sector_industry（sector_fund_flow 排行映射）
"""
from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from tasks.valuation_chain import (
    fetch_fundamentals_snapshot,
    fetch_market_snapshot_quotes,
    update_fundamentals,
    update_historical_valuation,
    update_market_snapshot,
    update_sector_industry,
)

# ===========================================================================
# Helpers
# ===========================================================================

def _mock_db_with_path(tmp_path: Path, filename: str = "test.db") -> MagicMock:
    db = MagicMock()
    db.db_path = str(tmp_path / filename)
    return db


def _create_fundamentals_db(db_path: str) -> None:
    """创建含 stock_list + fundamentals 表的真实数据库。

    stock_list 包含 market 列（供 update_market_snapshot 查询使用）。
    """
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE stock_list (code TEXT, name TEXT, industry TEXT, market TEXT)")
    conn.execute(
        "CREATE TABLE fundamentals "
        "(ts_code TEXT, trade_date TEXT, pe_ttm REAL, pb REAL, "
        " ps_ttm REAL, dividend_yield REAL, total_market_cap REAL)"
    )
    conn.execute(
        "INSERT INTO stock_list (code, name, industry, market) VALUES ('000001', '股票A', '银行', 'sz')"
    )
    conn.execute(
        "INSERT INTO stock_list (code, name, industry, market) VALUES ('600000', '股票B', '银行', 'sh')"
    )
    conn.execute(
        "INSERT INTO fundamentals (ts_code, trade_date, pe_ttm, pb, dividend_yield, total_market_cap) "
        "VALUES ('000001', '2026-07-17', 10.0, 1.0, 2.5, 1e9)"
    )
    conn.execute(
        "INSERT INTO fundamentals (ts_code, trade_date, pe_ttm, pb, dividend_yield, total_market_cap) "
        "VALUES ('600000', '2026-07-17', 12.0, 1.2, 3.1, 2e9)"
    )
    conn.commit()
    conn.close()


def _create_sector_flow_db(db_path: str) -> None:
    """创建含 sector_fund_flow 表的数据库。"""
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE sector_fund_flow "
        "(sector_name TEXT, trade_date TEXT, main_net_inflow REAL)"
    )
    conn.execute(
        "INSERT INTO sector_fund_flow (sector_name, trade_date, main_net_inflow) "
        "VALUES ('银行', '2026-07-17', 1e9)"
    )
    conn.execute(
        "INSERT INTO sector_fund_flow (sector_name, trade_date, main_net_inflow) "
        "VALUES ('白酒', '2026-07-17', 2e9)"
    )
    conn.commit()
    conn.close()


# ===========================================================================
# update_fundamentals
# ===========================================================================

class TestUpdateFundamentals:
    """update_fundamentals 中层覆盖。"""

    def test_with_symbols_filter(self):
        """--symbols 过滤路径。"""
        db = MagicMock()
        db.save_fundamentals_batch.return_value = 1
        db.count_fundamentals_for_date.return_value = 0
        loader = MagicMock()

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "success": True,
            "result": {
                "data": [
                    {"SECURITY_CODE": "000001", "TRADE_DATE": "2026-06-30",
                     "PE_TTM": 10.0, "PB_MRQ": 1.5, "PS_TTM": 2.0,
                     "PEG_CAR": 1.2, "TOTAL_MARKET_CAP": 1e9},
                    {"SECURITY_CODE": "600000", "TRADE_DATE": "2026-06-30",
                     "PE_TTM": 8.0, "PB_MRQ": 0.8, "PS_TTM": 1.0,
                     "PEG_CAR": None, "TOTAL_MARKET_CAP": 5e9},
                ],
                "count": 2,
            },
        }

        with (
            patch("tasks.valuation_chain.logger"),
            patch("tasks.valuation_chain.get_default_client") as mock_gc,
        ):
            mock_gc.return_value.get_session.return_value.get.return_value = mock_resp
            r = update_fundamentals(db, loader, symbols=["000001"])

        assert r["saved"] == 1  # 只有 000001 被过滤出来

    def test_count_non_int_handled(self):
        """count_fundamentals_for_date 返回非 int 时视为 0。"""
        db = MagicMock()
        db.count_fundamentals_for_date.return_value = "not_an_int"
        loader = MagicMock()

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "success": True,
            "result": {"data": [], "count": 0},
        }

        with (
            patch("tasks.valuation_chain.logger"),
            patch("tasks.valuation_chain.get_default_client") as mock_gc,
        ):
            mock_gc.return_value.get_session.return_value.get.return_value = mock_resp
            r = update_fundamentals(db, loader)

        assert r["saved"] == 0

    def test_skip_requires_post_close_completion(self):
        """行数达标但无收盘后完成记录 → 不跳过，重抓覆盖盘中快照。"""
        db = MagicMock()
        db.db_path = "/tmp/fake.db"
        db.count_fundamentals_for_date.return_value = 6000
        loader = MagicMock()

        mock_resp = MagicMock()
        mock_resp.json.return_value = {"success": True, "result": {"data": [], "count": 0}}

        with (
            patch("tasks.valuation_chain.logger"),
            patch("tasks.valuation_chain.has_post_close_completion", return_value=False),
            patch("tasks.valuation_chain.get_default_client") as mock_gc,
        ):
            mock_gc.return_value.get_session.return_value.get.return_value = mock_resp
            r = update_fundamentals(db, loader)

        # 未跳过：走了抓取路径（源端空 → saved 0，但没有 skipped 标记）
        assert "skipped" not in r

    def test_skip_with_post_close_completion(self):
        """行数达标 + 收盘后完成记录 → 跳过。"""
        db = MagicMock()
        db.db_path = "/tmp/fake.db"
        db.count_fundamentals_for_date.return_value = 6000
        loader = MagicMock()

        with (
            patch("tasks.valuation_chain.logger"),
            patch("tasks.valuation_chain.has_post_close_completion", return_value=True),
        ):
            r = update_fundamentals(db, loader)

        assert r["skipped"] is True

    def test_intraday_does_not_request_today(self):
        """盘中运行不请求 TRADE_DATE=今天（东财返回实时估值快照）。"""
        from zoneinfo import ZoneInfo

        sh_now = datetime(2026, 7, 17, 10, 30, tzinfo=ZoneInfo("Asia/Shanghai"))
        db = MagicMock()
        db.db_path = "/tmp/fake.db"
        db.count_fundamentals_for_date.return_value = 0
        loader = MagicMock()

        session = MagicMock()
        session.get.return_value.json.return_value = {
            "success": True,
            "result": {"data": [], "count": 0},
        }

        with (
            patch("tasks.valuation_chain.logger"),
            patch("tasks.valuation_chain.shanghai_now", return_value=sh_now),
            patch("tasks.valuation_chain.get_default_client") as mock_gc,
        ):
            mock_gc.return_value.get_session.return_value = session
            update_fundamentals(db, loader)

        requested_filters = [
            call.kwargs["params"]["filter"] for call in session.get.call_args_list
        ]
        assert requested_filters, "盘中仍应回补历史交易日"
        assert all("2026-07-17" not in f for f in requested_filters)

    def test_post_close_requests_today_first(self):
        """收盘定型后第一优先请求今天。"""
        from zoneinfo import ZoneInfo

        sh_now = datetime(2026, 7, 17, 16, 30, tzinfo=ZoneInfo("Asia/Shanghai"))
        db = MagicMock()
        db.db_path = "/tmp/fake.db"
        db.count_fundamentals_for_date.return_value = 0
        loader = MagicMock()

        session = MagicMock()
        session.get.return_value.json.return_value = {
            "success": True,
            "result": {"data": [], "count": 0},
        }

        with (
            patch("tasks.valuation_chain.logger"),
            patch("tasks.valuation_chain.shanghai_now", return_value=sh_now),
            patch("tasks.valuation_chain.get_default_client") as mock_gc,
        ):
            mock_gc.return_value.get_session.return_value = session
            update_fundamentals(db, loader)

        first_filter = session.get.call_args_list[0].kwargs["params"]["filter"]
        assert "2026-07-17" in first_filter


# ===========================================================================
# update_fundamentals — 雪球兜底（东财不可达时）
# ===========================================================================

def _em_empty_client() -> MagicMock:
    """构造东财 datacenter 全日期返回空数据的 get_default_client mock。"""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"success": True, "result": {"data": [], "count": 0}}
    mock_gc = MagicMock()
    mock_gc.return_value.get_session.return_value.get.return_value = mock_resp
    return mock_gc


class TestUpdateFundamentalsXueqiuFallback:
    """EM 零记录时的雪球兜底路径。"""

    def _db(self) -> MagicMock:
        db = MagicMock()
        db.count_fundamentals_for_date.return_value = 0
        return db

    def test_fallback_saves_quote_fields(self):
        """EM 空 + 雪球有报价 → 落库 pe_ttm/pb/market_cap/dividend_yield，其余 None。"""
        db = self._db()
        db.save_fundamentals_batch.return_value = 2
        db.get_stock_list.return_value = pd.DataFrame({"code": ["000001", "600000"]})
        loader = MagicMock()

        received_codes: list[list[str]] = []

        def fake_fetch(codes, **kwargs):
            received_codes.append(list(codes))
            # 实测口径（2026-07-30）：market_cap 单位元、dividend_yield 百分比原值
            return [
                {"code": "000001", "pe_ttm": 5.232, "pb": 0.486,
                 "market_cap": 225302710279.0, "dividend_yield": 5.134,
                 "current": 11.5},
                {"code": "600000", "pe_ttm": 8.0, "pb": 0.8,
                 "market_cap": 5e9, "dividend_yield": None},
            ]

        with (
            patch("tasks.valuation_chain.logger"),
            patch("tasks.valuation_chain.get_default_client", new=_em_empty_client()),
            patch("tasks.valuation_chain.fetch_market_snapshot_quotes", side_effect=fake_fetch),
            patch("tasks.valuation_chain.get_expected_latest_trading_day",
                  return_value="2026-07-30"),
        ):
            r = update_fundamentals(db, loader)

        assert r["saved"] == 2
        assert r["total"] == 2
        assert r["source"] == "xueqiu"
        assert received_codes == [["000001", "600000"]]

        records = db.save_fundamentals_batch.call_args[0][0]
        assert len(records) == 2
        first = records[0]
        assert first["ts_code"] == "000001"
        # 雪球报价为实时数据，统一 stamp 为期望最新交易日（周六运行不能落周六）
        assert first["trade_date"] == "2026-07-30"
        assert first["pe_ttm"] == 5.232
        assert first["pb"] == 0.486
        assert first["market_cap"] == 225302710279.0
        # 与 update_market_snapshot 口径一致：百分比原值直接落库
        assert first["dividend_yield"] == 5.134
        # 雪球不提供的东财独有字段 → None
        assert first["ps_ttm"] is None
        assert first["peg"] is None
        assert first["roe"] is None
        assert set(first.keys()) == {
            "ts_code", "trade_date", "pe_ttm", "pb", "ps_ttm", "dividend_yield",
            "roe", "roa", "gross_margin", "net_margin", "debt_ratio",
            "revenue_growth", "profit_growth", "eps_growth", "peg", "market_cap",
        }
        db.record_task_run.assert_called_once_with("update_fundamentals", "2026-07-30")

    def test_fallback_failure_returns_zero_without_raise(self):
        """EM 空 + 雪球无 Token（RuntimeError）→ saved 0，不上抛。"""
        db = self._db()
        db.get_stock_list.return_value = pd.DataFrame({"code": ["000001"]})
        loader = MagicMock()

        with (
            patch("tasks.valuation_chain.logger"),
            patch("tasks.valuation_chain.get_default_client", new=_em_empty_client()),
            patch("tasks.valuation_chain.fetch_market_snapshot_quotes",
                  side_effect=RuntimeError("XUEQIU_TOKEN is not configured")),
        ):
            r = update_fundamentals(db, loader)

        assert r == {"saved": 0, "total": 0}
        db.save_fundamentals_batch.assert_not_called()
        db.record_task_run.assert_not_called()

    def test_fallback_empty_quotes_returns_zero(self):
        """EM 空 + 雪球返回空列表 → saved 0，无 source 键。"""
        db = self._db()
        db.get_stock_list.return_value = pd.DataFrame({"code": ["000001"]})
        loader = MagicMock()

        with (
            patch("tasks.valuation_chain.logger"),
            patch("tasks.valuation_chain.get_default_client", new=_em_empty_client()),
            patch("tasks.valuation_chain.fetch_market_snapshot_quotes", return_value=[]),
        ):
            r = update_fundamentals(db, loader)

        assert r == {"saved": 0, "total": 0}
        db.save_fundamentals_batch.assert_not_called()

    def test_fallback_symbols_restricts_universe(self):
        """symbols 给定时兜底 universe 即 symbols，不读 stock_list。"""
        db = self._db()
        db.save_fundamentals_batch.return_value = 1
        loader = MagicMock()

        received_codes: list[list[str]] = []

        def fake_fetch(codes, **kwargs):
            received_codes.append(list(codes))
            return [{"code": "000001", "pe_ttm": 5.0, "pb": 0.5,
                     "market_cap": 1e9, "dividend_yield": 2.0}]

        with (
            patch("tasks.valuation_chain.logger"),
            patch("tasks.valuation_chain.get_default_client", new=_em_empty_client()),
            patch("tasks.valuation_chain.fetch_market_snapshot_quotes", side_effect=fake_fetch),
            patch("tasks.valuation_chain.get_expected_latest_trading_day",
                  return_value="2026-07-30"),
        ):
            r = update_fundamentals(db, loader, symbols=["000001"])

        assert received_codes == [["000001"]]
        db.get_stock_list.assert_not_called()
        assert r["saved"] == 1
        assert r["source"] == "xueqiu"

    def test_fallback_empty_stock_list_returns_zero(self):
        """stock_list 为空 → 兜底放弃，saved 0，不调雪球。"""
        db = self._db()
        db.get_stock_list.return_value = pd.DataFrame()
        loader = MagicMock()

        with (
            patch("tasks.valuation_chain.logger"),
            patch("tasks.valuation_chain.get_default_client", new=_em_empty_client()),
            patch("tasks.valuation_chain.fetch_market_snapshot_quotes") as mock_fetch,
        ):
            r = update_fundamentals(db, loader)

        assert r == {"saved": 0, "total": 0}
        mock_fetch.assert_not_called()

    def test_em_success_path_never_calls_xueqiu(self):
        """回归：EM 有数据时行为不变，不触发雪球兜底。"""
        db = self._db()
        db.save_fundamentals_batch.return_value = 1
        loader = MagicMock()

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "success": True,
            "result": {
                "data": [
                    {"SECURITY_CODE": "000001", "TRADE_DATE": "2026-07-30",
                     "PE_TTM": 10.0, "PB_MRQ": 1.5, "PS_TTM": 2.0,
                     "PEG_CAR": 1.2, "TOTAL_MARKET_CAP": 1e9},
                ],
                "count": 1,
            },
        }
        mock_gc = MagicMock()
        mock_gc.return_value.get_session.return_value.get.return_value = mock_resp

        with (
            patch("tasks.valuation_chain.logger"),
            patch("tasks.valuation_chain.get_default_client", new=mock_gc),
            patch("tasks.valuation_chain.fetch_market_snapshot_quotes") as mock_fetch,
        ):
            r = update_fundamentals(db, loader)

        assert r["saved"] == 1
        assert "source" not in r
        mock_fetch.assert_not_called()

    def test_fallback_filters_beijing_via_helper(self):
        """universe 含北交所代码 → 真实 helper 过滤，雪球请求不含 bj 代码。"""
        db = self._db()
        db.save_fundamentals_batch.return_value = 1
        db.get_stock_list.return_value = pd.DataFrame({"code": ["600000", "830001"]})
        loader = MagicMock()

        seen_chunks: list[list[str]] = []

        def fake_batch(chunk):
            seen_chunks.append(list(chunk))
            return [{"code": code, "pe_ttm": 8.0, "pb": 0.8,
                     "market_cap": 5e9, "dividend_yield": 3.0} for code in chunk]

        with (
            patch("tasks.valuation_chain.logger"),
            patch("tasks.valuation_chain.get_default_client", new=_em_empty_client()),
            patch("smartmoney_hunter.xueqiu._get_token", return_value="tok"),
            patch("smartmoney_hunter.xueqiu.get_batch_quotes", side_effect=fake_batch),
            patch("tasks.valuation_chain.is_beijing_stock", lambda c: c.startswith(("4", "8"))),
            patch("tasks.valuation_chain.time.sleep"),
            patch("tasks.valuation_chain.get_expected_latest_trading_day",
                  return_value="2026-07-30"),
        ):
            r = update_fundamentals(db, loader)

        assert seen_chunks == [["600000"]]
        records = db.save_fundamentals_batch.call_args[0][0]
        assert [rec["ts_code"] for rec in records] == ["600000"]
        assert r["source"] == "xueqiu"


# ===========================================================================
# update_market_snapshot
# ===========================================================================

class TestUpdateMarketSnapshot:
    """update_market_snapshot 中层覆盖。"""

    def test_no_token_returns_early(self, tmp_path: Path):
        """XUEQIU_TOKEN 未设置 → 提前返回。

        update_market_snapshot 在检查 token 前会执行 SQLite 查询（MAX trade_date 等），
        因此必须提供真实数据库路径。
        """
        db_path = str(tmp_path / "test.db")
        _create_fundamentals_db(db_path)
        db = MagicMock()
        db.db_path = db_path
        db.get_last_task_run.return_value = None

        with patch("smartmoney_hunter.xueqiu._get_token", return_value=None), \
             patch("tasks.valuation_chain.logger"):
            r = update_market_snapshot(db)

        assert r["skipped"] is True
        assert r["saved"] == 0

    def test_with_real_db_and_no_token(self, tmp_path: Path):
        """真实 DB + 无 Token → 读取 stock_list 后提前返回。"""
        db_path = str(tmp_path / "test.db")
        _create_fundamentals_db(db_path)
        db = MagicMock()
        db.db_path = db_path
        db.get_last_task_run.return_value = None

        with patch("smartmoney_hunter.xueqiu._get_token", return_value=None), \
             patch("tasks.valuation_chain.logger"):
            r = update_market_snapshot(db)

        assert r["skipped"] is True

    def test_already_run_today(self, tmp_path: Path):
        """今天已跑过 → 跳过。

        target_date 来自 fundamentals 表的 MAX(trade_date)，= '2026-07-17'。
        get_last_task_run 返回值需匹配 target_date 才能触发跳过。
        """
        db_path = str(tmp_path / "test.db")
        _create_fundamentals_db(db_path)
        db = MagicMock()
        db.db_path = db_path
        # 必须匹配 fundamentals 表中的 MAX(trade_date)
        db.get_last_task_run.return_value = "2026-07-17"

        with patch("tasks.valuation_chain.logger"):
            r = update_market_snapshot(db)

        assert r["skipped"] is True

    def test_intraday_today_target_deferred(self, tmp_path: Path):
        """target 为今日且未收盘定型 → no_data，不写盘中股息率。"""
        from zoneinfo import ZoneInfo

        db_path = str(tmp_path / "test.db")
        _create_fundamentals_db(db_path)
        db = MagicMock()
        db.db_path = db_path

        sh_now = datetime(2026, 7, 17, 10, 30, tzinfo=ZoneInfo("Asia/Shanghai"))
        with patch("tasks.valuation_chain.logger"), \
             patch("tasks.valuation_chain.shanghai_now", return_value=sh_now):
            r = update_market_snapshot(db)

        assert r["status"] == "no_data"
        db.get_last_task_run.assert_not_called()

    def test_today_target_skip_requires_post_close_completion(self, tmp_path: Path):
        """target 为今日、行数达标但无收盘后完成记录 → 不跳过，重新补充。"""
        from zoneinfo import ZoneInfo

        db_path = str(tmp_path / "test.db")
        _create_fundamentals_db(db_path)
        db = MagicMock()
        db.db_path = db_path
        db.get_last_task_run.return_value = "2026-07-17"

        sh_now = datetime(2026, 7, 17, 17, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
        with patch("tasks.valuation_chain.logger"), \
             patch("tasks.valuation_chain.shanghai_now", return_value=sh_now), \
             patch("tasks.valuation_chain.has_post_close_completion", return_value=False), \
             patch("smartmoney_hunter.xueqiu._get_token", return_value=None):
            r = update_market_snapshot(db)

        # 未走守卫跳过（守卫跳过的返回带 updated 键），落到 token 缺失的早退
        assert "updated" not in r
        assert r.get("reason") == "XUEQIU_TOKEN not configured"

    def test_today_target_skip_with_post_close_completion(self, tmp_path: Path):
        """target 为今日、行数达标且有收盘后完成记录 → 跳过。"""
        from zoneinfo import ZoneInfo

        db_path = str(tmp_path / "test.db")
        _create_fundamentals_db(db_path)
        db = MagicMock()
        db.db_path = db_path
        db.get_last_task_run.return_value = "2026-07-17"

        sh_now = datetime(2026, 7, 17, 17, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
        with patch("tasks.valuation_chain.logger"), \
             patch("tasks.valuation_chain.shanghai_now", return_value=sh_now), \
             patch("tasks.valuation_chain.has_post_close_completion", return_value=True):
            r = update_market_snapshot(db)

        assert r["skipped"] is True
        assert r["updated"] == 0


# ===========================================================================
# update_historical_valuation
# ===========================================================================

class TestUpdateHistoricalValuation:
    """update_historical_valuation 中层覆盖。"""

    def test_with_symbols_filter(self):
        """--symbols 过滤路径。"""
        db = MagicMock()
        db.get_fundamentals_batch.return_value = pd.DataFrame({
            "ts_code": ["000001", "000002", "600000"],
            "trade_date": ["2026-07-17", "2026-07-17", "2026-07-17"],
            "pe_ttm": [10.0, 12.0, 8.0],
            "pb": [1.0, 1.5, 0.8],
            "ps_ttm": [2.0, 2.5, 1.0],
            "dividend_yield": [0.03, 0.02, 0.04],
        })
        db.save_historical_valuation.return_value = None

        with patch("tasks.valuation_chain.logger"):
            r = update_historical_valuation(db, symbols=["000001", "600000"])

        assert r["saved"] == 2
        assert db.save_historical_valuation.call_count == 2

    def test_deduplicate_keeps_last(self):
        """相同(symbol, date) 去重，保留最后一条。"""
        db = MagicMock()
        db.get_fundamentals_batch.return_value = pd.DataFrame({
            "ts_code": ["000001", "000001", "000002"],
            "trade_date": ["2026-07-17", "2026-07-17", "2026-07-17"],
            "pe_ttm": [10.0, 10.1, 12.0],
            "pb": [1.0, 1.1, 1.5],
            "ps_ttm": [2.0, 2.1, 2.5],
            "dividend_yield": [0.03, 0.031, 0.02],
        })
        db.save_historical_valuation.return_value = None

        with patch("tasks.valuation_chain.logger"):
            r = update_historical_valuation(db)

        assert r["saved"] == 2
        assert db.save_historical_valuation.call_count == 2

    def test_empty_fundamentals(self):
        """fundamentals 为空 → 提前返回（合法零行，skipped）。"""
        db = MagicMock()
        db.get_fundamentals_batch.return_value = pd.DataFrame()

        with patch("tasks.valuation_chain.logger"):
            r = update_historical_valuation(db)

        assert r.get("skipped") is True
        assert r["total"] == 0

    def test_uses_batch_save_when_available(self):
        """DB 有 save_historical_valuation_batch 时使用批量保存。"""
        class _BatchDB:
            def __init__(self) -> None:
                self.save_historical_valuation = MagicMock()
                self.save_historical_valuation_batch = MagicMock(return_value=3)

            def get_fundamentals_batch(self) -> pd.DataFrame:
                return pd.DataFrame({
                    "ts_code": ["000001", "000002", "000003"],
                    "trade_date": ["2026-07-17", "2026-07-17", "2026-07-17"],
                    "pe_ttm": [10.0, 12.0, 8.0],
                    "pb": [1.0, 1.5, 0.8],
                    "ps_ttm": [2.0, 2.5, 1.0],
                    "dividend_yield": [0.03, 0.02, 0.04],
                })

        db = _BatchDB()

        with patch("tasks.valuation_chain.logger"):
            r = update_historical_valuation(db)

        assert r["saved"] == 3
        db.save_historical_valuation_batch.assert_called_once()
        db.save_historical_valuation.assert_not_called()

    def test_exception_returns_error(self):
        """函数内部异常 → 返回 error 字典。"""
        db = MagicMock()
        db.get_fundamentals_batch.side_effect = ValueError("unexpected")

        with patch("tasks.valuation_chain.logger"):
            r = update_historical_valuation(db)

        assert r["saved"] == 0
        assert "error" in r


# ===========================================================================
# update_sector_industry
# ===========================================================================

class TestUpdateSectorIndustry:
    """update_sector_industry 中层覆盖。"""

    def test_no_fundamentals_returns_early(self):
        """fundamentals 为空 → 提前返回（合法零行，skipped）。"""
        db = MagicMock()
        db.get_stock_list.return_value = pd.DataFrame({
            "code": ["000001"],
            "industry": ["银行"],
        })
        db.get_fundamentals_batch.return_value = pd.DataFrame()

        with patch("tasks.valuation_chain.logger"):
            r = update_sector_industry(db)

        assert r.get("skipped") is True

    def test_no_stock_list_returns_early(self):
        """stock_list 为空 → 提前返回（合法零行，skipped）。"""
        db = MagicMock()
        db.get_stock_list.return_value = pd.DataFrame()

        with patch("tasks.valuation_chain.logger"):
            r = update_sector_industry(db)

        assert r.get("skipped") is True

    def test_with_sector_fund_flow_mapping(self, tmp_path: Path):
        """sector_fund_flow 精确匹配 + 排行映射。"""
        db_path = str(tmp_path / "test.db")
        _create_fundamentals_db(db_path)
        _create_sector_flow_db(db_path)

        db = MagicMock()
        db.db_path = db_path
        db.get_stock_list.return_value = pd.DataFrame({
            "code": ["000001", "600000"],
            "industry": ["银行", "银行"],
        })
        db.get_fundamentals_batch.return_value = pd.DataFrame({
            "ts_code": ["000001", "600000"],
            "pe_ttm": [10.0, 12.0],
            "pb": [1.0, 1.2],
            "ps_ttm": [2.0, 2.4],
            "roe": [0.12, 0.10],
            "revenue_growth": [0.20, 0.15],
            "profit_growth": [0.18, 0.12],
            "market_cap": [1e9, 2e9],
        })
        db.save_sector_industry.return_value = None

        with patch("tasks.valuation_chain.logger"), \
             patch("tasks.valuation_chain.get_expected_latest_trading_day",
                   return_value="2026-07-17"):
            r = update_sector_industry(db)

        assert r["saved"] == 1
        db.save_sector_industry.assert_called_once()

    def test_with_hardcoded_alias_mapping(self, tmp_path: Path):
        """行业名称 hard-coded alias 映射（如 酿酒行业 → 白酒）。"""
        db_path = str(tmp_path / "test.db")
        _create_sector_flow_db(db_path)

        db = MagicMock()
        db.db_path = db_path
        db.get_stock_list.return_value = pd.DataFrame({
            "code": ["000001"],
            "industry": ["酿酒行业"],  # hard_coded_map: → "白酒"
        })
        db.get_fundamentals_batch.return_value = pd.DataFrame({
            "ts_code": ["000001"],
            "pe_ttm": [10.0],
            "pb": [1.0],
            "ps_ttm": [2.0],
            "roe": [0.12],
            "revenue_growth": [0.20],
            "profit_growth": [0.18],
            "market_cap": [1e9],
        })
        db.save_sector_industry.return_value = None

        with patch("tasks.valuation_chain.logger"), \
             patch("tasks.valuation_chain.get_expected_latest_trading_day",
                   return_value="2026-07-17"):
            r = update_sector_industry(db)

        assert r["saved"] == 1

    def test_sector_flow_no_data(self, tmp_path: Path):
        """sector_fund_flow 表为空 → fund_inflow_rank 设为 None。"""
        db_path = str(tmp_path / "test.db")
        _create_fundamentals_db(db_path)

        db = MagicMock()
        db.db_path = db_path
        db.get_stock_list.return_value = pd.DataFrame({
            "code": ["000001"],
            "industry": ["银行"],
        })
        db.get_fundamentals_batch.return_value = pd.DataFrame({
            "ts_code": ["000001"],
            "pe_ttm": [10.0],
            "pb": [1.0],
            "ps_ttm": [2.0],
            "roe": [0.12],
            "revenue_growth": [0.20],
            "profit_growth": [0.18],
            "market_cap": [1e9],
        })
        db.save_sector_industry.return_value = None

        with patch("tasks.valuation_chain.logger"), \
             patch("tasks.valuation_chain.get_expected_latest_trading_day",
                   return_value="2026-07-17"):
            # 创建不带 sector_fund_flow 表的 DB
            r = update_sector_industry(db)

        assert r["saved"] == 1

    def test_sector_flow_db_error_graceful(self, tmp_path: Path):
        """sector_fund_flow 查询异常 → fund_inflow_rank 设为 None。"""
        db = MagicMock()
        db.db_path = "/nonexistent/path/test.db"  # 让首次连接就失败
        db.get_stock_list.return_value = pd.DataFrame({
            "code": ["000001"],
            "industry": ["银行"],
        })
        db.get_fundamentals_batch.return_value = pd.DataFrame({
            "ts_code": ["000001"],
            "pe_ttm": [10.0],
            "pb": [1.0],
            "ps_ttm": [2.0],
            "roe": [0.12],
            "revenue_growth": [0.20],
            "profit_growth": [0.18],
            "market_cap": [1e9],
        })
        db.save_sector_industry.return_value = None

        with patch("tasks.valuation_chain.logger"), \
             patch("tasks.valuation_chain.get_expected_latest_trading_day",
                   return_value="2026-07-17"):
            r = update_sector_industry(db)

        # sector_fund_flow 查询失败不应影响主流程
        assert r["saved"] == 1

    def test_fuzzy_industry_matching(self, tmp_path: Path):
        """行业名称通过 difflib 模糊匹配 sector_fund_flow。"""
        db_path = str(tmp_path / "test.db")
        _create_fundamentals_db(db_path)
        _create_sector_flow_db(db_path)

        db = MagicMock()
        db.db_path = db_path
        # "白酒行业" 不在 hard_coded_map 中，但应模糊匹配到 "白酒"（相似度 >= 0.5）
        db.get_stock_list.return_value = pd.DataFrame({
            "code": ["000001"],
            "industry": ["白酒行业"],
        })
        db.get_fundamentals_batch.return_value = pd.DataFrame({
            "ts_code": ["000001"],
            "pe_ttm": [10.0],
            "pb": [1.0],
            "ps_ttm": [2.0],
            "roe": [0.12],
            "revenue_growth": [0.20],
            "profit_growth": [0.18],
            "market_cap": [1e9],
        })
        db.save_sector_industry.return_value = None

        with patch("tasks.valuation_chain.logger"), \
             patch("tasks.valuation_chain.get_expected_latest_trading_day",
                   return_value="2026-07-17"):
            r = update_sector_industry(db)

        assert r["saved"] == 1

    def test_outer_exception_returns_error(self):
        """update_sector_industry 外层异常 → 返回 error 字典。"""
        db = MagicMock()
        db.get_stock_list.side_effect = ValueError("unexpected crash")

        with patch("tasks.valuation_chain.logger"):
            r = update_sector_industry(db)

        assert r["saved"] == 0
        assert "error" in r


# ===========================================================================
# update_market_snapshot — 有 Token 全流程
# ===========================================================================

class TestUpdateMarketSnapshotWithToken:
    """update_market_snapshot 有有效 Token 时的完整路径。"""

    def test_with_valid_token_updates_dividend_yield(self, tmp_path: Path):
        """有效 Token → 拉取雪球数据 → 写入 dividend_yield。"""
        db_path = str(tmp_path / "test.db")
        _create_fundamentals_db(db_path)
        # 确保 dividend_yield 为 NULL 可被更新
        conn = sqlite3.connect(db_path)
        conn.execute(
            "UPDATE fundamentals SET dividend_yield = NULL"
        )
        conn.commit()
        conn.close()

        db = MagicMock()
        db.db_path = db_path
        db.get_last_task_run.return_value = None
        db.record_task_run.return_value = None

        mock_quotes = [
            {"code": "000001", "dividend_yield": 0.035},
        ]

        with patch("smartmoney_hunter.xueqiu._get_token", return_value="fake_token"), \
             patch("smartmoney_hunter.xueqiu.get_batch_quotes", return_value=mock_quotes), \
             patch("tasks.valuation_chain.logger"), \
             patch("tasks.valuation_chain.time.sleep"):
            r = update_market_snapshot(db)

        assert r["updated"] == 1
        assert r["saved"] == 1
        assert r["total"] == 2
        db.record_task_run.assert_called_once_with("update_market_snapshot", "2026-07-17")

    def test_with_valid_token_empty_quotes(self, tmp_path: Path):
        """有效 Token 但 API 返回空 → failed(network)，不得报 success。

        旧版在此返回 success（saved=0），使「一条都没抓到」在 ingestion_runs
        里与「补充完成」不可区分。
        """
        db_path = str(tmp_path / "test.db")
        _create_fundamentals_db(db_path)

        db = MagicMock()
        db.db_path = db_path
        db.get_last_task_run.return_value = None
        db.record_task_run.return_value = None

        with patch("smartmoney_hunter.xueqiu._get_token", return_value="fake_token"), \
             patch("smartmoney_hunter.xueqiu.get_batch_quotes", return_value=[]), \
             patch("tasks.valuation_chain.logger"), \
             patch("tasks.valuation_chain.time.sleep"):
            r = update_market_snapshot(db)

        assert r["status"] == "failed"
        assert r["error_kind"] == "network"
        assert r["updated"] == 0
        assert r["total"] == 2
        db.record_task_run.assert_not_called()

    def test_quotes_without_dividend_yield_is_failed_not_success(self, tmp_path: Path):
        """回归（2026-08-12 静默空洞）：抓到报价但一条都写不进 → 必须 failed。

        当日审计行是 saved_rows=5203 + status=success，而 fundamentals 整列为
        NULL；因为作用域是 MAX(trade_date)，该日期此后永不被回访。
        """
        db_path = str(tmp_path / "test.db")
        _create_fundamentals_db(db_path)
        conn = sqlite3.connect(db_path)
        conn.execute("UPDATE fundamentals SET dividend_yield = NULL")
        conn.commit()
        conn.close()

        db = MagicMock()
        db.db_path = db_path
        db.get_last_task_run.return_value = None
        db.record_task_run.return_value = None

        quotes = [
            {"code": "000001", "dividend_yield": None},
            {"code": "600000", "dividend_yield": None},
        ]
        with patch("smartmoney_hunter.xueqiu._get_token", return_value="fake_token"), \
             patch("smartmoney_hunter.xueqiu.get_batch_quotes", return_value=quotes), \
             patch("tasks.valuation_chain.logger"), \
             patch("tasks.valuation_chain.time.sleep"):
            r = update_market_snapshot(db)

        assert r["status"] == "failed"
        assert r["error_kind"] == "data_quality"
        assert r["saved"] == 0
        assert r["updated"] == 0
        # 失败信息必须能区分「源没给字段」，否则下次仍无从定位
        assert "无 dividend_yield 字段" in r["error"]
        # 未达标不得落「已完成」标记，否则下一轮的跳过判定会误以为该日已补充
        db.record_task_run.assert_not_called()

    def test_saved_counts_written_rows_not_fetched_rows(self, tmp_path: Path):
        """saved 是真实写入行数：抓到 2 条、只有 1 条可写 → saved == 1。

        旧版 saved = len(all_quotes)，故此处会是 2，审计表无法区分
        「抓到多少」与「写了多少」。
        """
        db_path = str(tmp_path / "test.db")
        _create_fundamentals_db(db_path)
        conn = sqlite3.connect(db_path)
        conn.execute("UPDATE fundamentals SET dividend_yield = NULL")
        conn.commit()
        conn.close()

        db = MagicMock()
        db.db_path = db_path
        db.get_last_task_run.return_value = None
        db.record_task_run.return_value = None

        quotes = [
            {"code": "000001", "dividend_yield": 3.5},
            {"code": "600000", "dividend_yield": None},
        ]
        with patch("smartmoney_hunter.xueqiu._get_token", return_value="fake_token"), \
             patch("smartmoney_hunter.xueqiu.get_batch_quotes", return_value=quotes), \
             patch("tasks.valuation_chain.logger"), \
             patch("tasks.valuation_chain.time.sleep"):
            r = update_market_snapshot(db)

        assert r["status"] == "success"
        assert r["saved"] == 1
        assert r["updated"] == 1
        assert r["total"] == 2
        db.record_task_run.assert_called_once_with("update_market_snapshot", "2026-07-17")


# ===========================================================================
# 收盘刷新 helpers（Task 6）：不落库的抓取 / 归一化
# ===========================================================================

_REFRESH_TARGET = "2026-07-27"


class _FakeResponse:
    """手写 requests 响应 fake，只提供 json()。"""

    def __init__(self, payload: dict):
        self._payload = payload

    def json(self) -> dict:
        return self._payload


class _RecordingSession:
    """记录请求参数并按页返回预设 payload 的手写 session fake。"""

    def __init__(self, pages: list[dict]):
        self.pages = pages
        self.calls: list[dict] = []

    def get(self, url, params=None, timeout=None):
        self.calls.append(dict(params))
        page = int(params["pageNumber"]) - 1
        return _FakeResponse(self.pages[page])


def _valuation_page(records: list[dict], count: int) -> dict:
    return {"success": True, "result": {"data": records, "count": count}}


class TestFetchFundamentalsSnapshot:
    """fetch_fundamentals_snapshot 只抓目标日，无 5000 行阈值，不写库。"""

    def test_fetches_target_date_across_pages(self):
        """只请求目标日 filter，分页拼接，归一化为写库记录形状。"""
        session = _RecordingSession([
            _valuation_page([
                {"SECURITY_CODE": "000001", "TRADE_DATE": f"{_REFRESH_TARGET} 00:00:00",
                 "PE_TTM": 10.0, "PB_MRQ": 1.5, "PS_TTM": 2.0,
                 "PEG_CAR": 1.2, "TOTAL_MARKET_CAP": 1e9},
                {"SECURITY_CODE": "600000", "TRADE_DATE": f"{_REFRESH_TARGET} 00:00:00",
                 "PE_TTM": 8.0, "PB_MRQ": 0.8, "PS_TTM": 1.0,
                 "PEG_CAR": None, "TOTAL_MARKET_CAP": 5e9},
            ], count=3),
            _valuation_page([
                {"SECURITY_CODE": "600519", "TRADE_DATE": f"{_REFRESH_TARGET} 00:00:00",
                 "PE_TTM": 30.0, "PB_MRQ": 9.0, "PS_TTM": 12.0,
                 "PEG_CAR": 2.0, "TOTAL_MARKET_CAP": 2e12},
            ], count=3),
        ])

        records = fetch_fundamentals_snapshot(
            _REFRESH_TARGET, session=session, page_size=2
        )

        assert len(records) == 3
        assert len(session.calls) == 2
        assert all(
            call["filter"] == f"(TRADE_DATE='{_REFRESH_TARGET}')" for call in session.calls
        )
        first = records[0]
        assert first["ts_code"] == "000001"
        assert first["trade_date"] == _REFRESH_TARGET
        assert first["pe_ttm"] == 10.0
        assert first["pb"] == 1.5
        assert first["peg"] == 1.2
        assert first["dividend_yield"] is None

    def test_returns_empty_when_source_has_no_target_data(self):
        session = _RecordingSession([{"success": True, "result": None}])
        records = fetch_fundamentals_snapshot(
            _REFRESH_TARGET, session=session, page_size=2
        )
        assert records == []

    def test_propagates_source_errors(self):
        """源端异常直接上抛，交给适配器/编排器处理（保留旧数据）。"""

        class _BrokenSession:
            def get(self, url, params=None, timeout=None):
                raise ConnectionError("eastmoney down")

        with pytest.raises(ConnectionError):
            fetch_fundamentals_snapshot(_REFRESH_TARGET, session=_BrokenSession())


class TestFetchMarketSnapshotQuotes:
    """fetch_market_snapshot_quotes 只拉行情不写库，无 token 即报错。"""

    def test_requires_token(self):
        with patch("smartmoney_hunter.xueqiu._get_token", return_value=None), \
             pytest.raises(RuntimeError):
            fetch_market_snapshot_quotes(["600000"])

    def test_filters_beijing_and_batches(self):
        """北交所代码不进雪球请求，其余按批拉取并拼接。"""
        seen_chunks: list[list[str]] = []

        def fake_batch(chunk):
            seen_chunks.append(list(chunk))
            return [{"code": code, "dividend_yield": 2.0} for code in chunk]

        with patch("smartmoney_hunter.xueqiu._get_token", return_value="tok"), \
             patch("smartmoney_hunter.xueqiu.get_batch_quotes", side_effect=fake_batch), \
             patch("tasks.valuation_chain.is_beijing_stock", lambda c: c.startswith(("4", "8"))), \
             patch("tasks.valuation_chain.time.sleep"):
            quotes = fetch_market_snapshot_quotes(
                ["600000", "830001", "000001"], batch_size=1
            )

        assert seen_chunks == [["600000"], ["000001"]]
        assert [q["code"] for q in quotes] == ["600000", "000001"]

    def test_single_batch_failure_is_partial(self):
        """单批失败静默降级，其余批次继续（覆盖率由适配器把关）。"""
        calls = {"n": 0}

        def flaky_batch(chunk):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("risk control")
            return [{"code": code, "dividend_yield": 1.0} for code in chunk]

        with patch("smartmoney_hunter.xueqiu._get_token", return_value="tok"), \
             patch("smartmoney_hunter.xueqiu.get_batch_quotes", side_effect=flaky_batch), \
             patch("tasks.valuation_chain.time.sleep"), \
             patch("tasks.valuation_chain.logger"):
            quotes = fetch_market_snapshot_quotes(["600000", "000001"], batch_size=1)

        assert [q["code"] for q in quotes] == ["000001"]
