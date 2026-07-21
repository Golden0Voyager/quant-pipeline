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

from tasks.valuation_chain import (
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
        "INSERT INTO fundamentals (ts_code, trade_date, pe_ttm, pb, total_market_cap) "
        "VALUES ('000001', '2026-07-17', 10.0, 1.0, 1e9)"
    )
    conn.execute(
        "INSERT INTO fundamentals (ts_code, trade_date, pe_ttm, pb, total_market_cap) "
        "VALUES ('600000', '2026-07-17', 12.0, 1.2, 2e9)"
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
            patch("requests.Session") as mock_session_cls,
            patch("tasks.valuation_chain.datetime") as mock_dt,
        ):
            mock_session_cls.return_value.get.return_value = mock_resp
            mock_dt.now.return_value = datetime(2026, 6, 30, 9, 0, 0)
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw) if a else mock_dt.now()
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
            patch("requests.Session") as mock_session_cls,
            patch("tasks.valuation_chain.datetime") as mock_dt,
        ):
            mock_session_cls.return_value.get.return_value = mock_resp
            mock_dt.now.return_value = datetime(2026, 6, 30, 9, 0, 0)
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw) if a else mock_dt.now()
            r = update_fundamentals(db, loader)

        assert r["saved"] == 0


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
        """fundamentals 为空 → 提前返回。"""
        db = MagicMock()
        db.get_fundamentals_batch.return_value = pd.DataFrame()

        with patch("tasks.valuation_chain.logger"):
            r = update_historical_valuation(db)

        assert r["saved"] == 0
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
        """fundamentals 为空 → 提前返回。"""
        db = MagicMock()
        db.get_stock_list.return_value = pd.DataFrame({
            "code": ["000001"],
            "industry": ["银行"],
        })
        db.get_fundamentals_batch.return_value = pd.DataFrame()

        with patch("tasks.valuation_chain.logger"):
            r = update_sector_industry(db)

        assert r["saved"] == 0

    def test_no_stock_list_returns_early(self):
        """stock_list 为空 → 提前返回。"""
        db = MagicMock()
        db.get_stock_list.return_value = pd.DataFrame()

        with patch("tasks.valuation_chain.logger"):
            r = update_sector_industry(db)

        assert r["saved"] == 0

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
        """有效 Token 但 API 返回空 → 记录 0。"""
        db_path = str(tmp_path / "test.db")
        _create_fundamentals_db(db_path)

        db = MagicMock()
        db.db_path = db_path
        db.get_last_task_run.return_value = None

        with patch("smartmoney_hunter.xueqiu._get_token", return_value="fake_token"), \
             patch("smartmoney_hunter.xueqiu.get_batch_quotes", return_value=[]), \
             patch("tasks.valuation_chain.logger"), \
             patch("tasks.valuation_chain.time.sleep"):
            r = update_market_snapshot(db)

        assert r["updated"] == 0
        assert r["total"] == 2
