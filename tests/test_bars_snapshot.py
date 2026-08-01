"""全市场快照模块与播种逻辑测试。"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from tasks.bars import _seed_from_snapshot, update_bars
from tasks.bars_snapshot import (
    SNAPSHOT_SOURCE,
    bar_record_to_frame,
    fetch_market_snapshot,
    snapshot_to_bar_records,
)


def _spot_df(rows: list[dict]) -> pd.DataFrame:
    base = {
        "代码": "000001", "名称": "平安银行", "最新价": 10.5, "今开": 10.0,
        "最高": 10.6, "最低": 9.9, "昨收": 10.2, "成交量": 100000.0,
        "成交额": 1.03e8, "换手率": 0.52, "涨跌幅": 2.94, "振幅": 6.86,
    }
    return pd.DataFrame([{**base, **row} for row in rows])


class TestSnapshotToBarRecords:
    def test_normalizes_columns(self):
        records, suspended = snapshot_to_bar_records(
            _spot_df([{}]), trade_date="2026-07-31"
        )
        assert suspended == set()
        rec = records["000001"]
        assert rec["trade_date"] == "2026-07-31"
        assert rec["open"] == 10.0
        assert rec["close"] == 10.5
        assert rec["high"] == 10.6
        assert rec["low"] == 9.9
        assert rec["volume"] == 100000.0
        assert rec["amount"] == 1.03e8
        assert rec["turnover_rate"] == 0.52
        assert rec["pct_change"] == 2.94
        assert rec["amplitude"] == 6.86
        assert rec["prev_close"] == 10.2
        assert rec["data_source"] == SNAPSHOT_SOURCE

    def test_suspended_stock_filtered(self):
        df = _spot_df([
            {},
            {"代码": "000002", "最新价": None, "今开": None, "成交量": None},
            {"代码": "000003", "成交量": 0.0},
        ])
        records, suspended = snapshot_to_bar_records(df, trade_date="2026-07-31")
        assert set(records) == {"000001"}
        assert suspended == {"000002", "000003"}

    def test_non_numeric_code_ignored(self):
        df = _spot_df([{}, {"代码": "BK0001"}])
        records, suspended = snapshot_to_bar_records(df, trade_date="2026-07-31")
        assert set(records) == {"000001"}
        assert suspended == set()

    def test_bar_record_to_frame_strips_prev_close(self):
        records, _ = snapshot_to_bar_records(_spot_df([{}]), trade_date="2026-07-31")
        frame = bar_record_to_frame(records["000001"])
        assert len(frame) == 1
        assert "prev_close" not in frame.columns
        assert frame.iloc[0]["close"] == 10.5


class TestFetchMarketSnapshot:
    def test_empty_result_raises(self):
        with patch("tasks.bars_snapshot.ak") as mock_ak:
            mock_ak.stock_zh_a_spot_em.return_value = pd.DataFrame()
            with pytest.raises(RuntimeError):
                fetch_market_snapshot()

    def test_missing_columns_raises(self):
        with patch("tasks.bars_snapshot.ak") as mock_ak:
            mock_ak.stock_zh_a_spot_em.return_value = pd.DataFrame({"代码": ["000001"]})
            with pytest.raises(RuntimeError):
                fetch_market_snapshot()


class TestSeedFromSnapshot:
    TARGET = "2026-07-31"
    PREV = "2026-07-30"

    def _make_db(self, tmp_path: Path, rows: list[tuple[str, str, float]]) -> MagicMock:
        db_path = str(tmp_path / "bars.db")
        conn = sqlite3.connect(db_path)
        conn.execute(
            "CREATE TABLE daily_bars (ts_code TEXT, trade_date TEXT, close REAL)"
        )
        conn.executemany("INSERT INTO daily_bars VALUES (?, ?, ?)", rows)
        conn.commit()
        conn.close()
        db = MagicMock()
        db.db_path = db_path
        return db

    def _run(self, db, codes, spot_rows):
        with patch("tasks.bars_snapshot.ak") as mock_ak, \
             patch("tasks.bars.get_recent_trading_days",
                   return_value=[self.TARGET, self.PREV]), \
             patch("tasks.bars.logger"):
            mock_ak.stock_zh_a_spot_em.return_value = _spot_df(spot_rows)
            return _seed_from_snapshot(db, codes, self.TARGET)

    def test_seeds_up_to_date_stock(self, tmp_path: Path):
        """库中最新=昨日 且 昨收匹配 → 播种今日行并移出逐股清单。"""
        db = self._make_db(tmp_path, [("000001", self.PREV, 10.2)])
        remaining, seeded, suspended = self._run(db, ["000001"], [{}])
        assert seeded == 1
        assert remaining == []
        assert suspended == set()
        saved_df = db.save_daily_bars.call_args.args[1]
        assert saved_df.iloc[0]["trade_date"] == self.TARGET
        assert saved_df.iloc[0]["data_source"] == SNAPSHOT_SOURCE

    def test_ex_dividend_routes_to_per_stock(self, tmp_path: Path):
        """快照昨收与库中昨收偏差 >0.2%（当日除权）→ 不播种，留给逐股重拉。"""
        db = self._make_db(tmp_path, [("000001", self.PREV, 11.5)])
        remaining, seeded, _ = self._run(db, ["000001"], [{"昨收": 10.2}])
        assert seeded == 0
        assert remaining == ["000001"]
        db.save_daily_bars.assert_not_called()

    def test_behind_stock_routes_to_per_stock(self, tmp_path: Path):
        """库中最新早于昨日（落后多天）→ 留给逐股回补。"""
        db = self._make_db(tmp_path, [("000001", "2026-07-25", 10.2)])
        remaining, seeded, _ = self._run(db, ["000001"], [{}])
        assert seeded == 0
        assert remaining == ["000001"]

    def test_snapshot_suspended_reported(self, tmp_path: Path):
        db = self._make_db(tmp_path, [("000001", self.PREV, 10.2)])
        remaining, seeded, suspended = self._run(
            db, ["000001"], [{"最新价": None, "今开": None, "成交量": None}]
        )
        assert seeded == 0
        assert suspended == {"000001"}

    def test_symbol_suffix_normalized(self, tmp_path: Path):
        """清单里带交易所后缀的代码也能与快照裸码匹配。"""
        db = self._make_db(tmp_path, [("000001.SZ", self.PREV, 10.2)])
        remaining, seeded, _ = self._run(db, ["000001.SZ"], [{}])
        assert seeded == 1
        assert remaining == []
        assert db.save_daily_bars.call_args.args[0] == "000001.SZ"


class TestUpdateBarsSeedingGate:
    """update_bars 阶段0 的启用条件与失败回退。"""

    TARGET = "2026-07-31"

    def _run_update_bars(self, tmp_path: Path, *, phase: str, symbols=None):
        db = MagicMock()
        db.db_path = str(tmp_path / "bars.db")
        sqlite3.connect(db.db_path).close()
        db.get_stock_list.return_value = pd.DataFrame({"code": ["000001"]})
        db.get_latest_bar_date.return_value = self.TARGET  # 逐股循环全部秒跳过
        db.watchlist_get_all.return_value = pd.DataFrame()
        loader = MagicMock()

        with patch("tasks.bars.ProgressTracker") as tracker, \
             patch("tasks.bars.market_phase", return_value=phase), \
             patch("tasks.bars.shanghai_today", return_value=self.TARGET), \
             patch("tasks.bars.get_expected_latest_trading_day",
                   return_value=self.TARGET), \
             patch("tasks.bars._detect_suspended_symbols", return_value=set()), \
             patch("tasks.bars._seed_from_snapshot",
                   return_value=([], 1, set())) as seed, \
             patch("tasks.bars.time.sleep"), \
             patch("tasks.bars.logger"):
            tracker.FILE.exists.return_value = False
            result = update_bars(db, loader, symbols=symbols, force=True)
        return seed, result

    def test_post_close_enables_seeding(self, tmp_path: Path):
        seed, result = self._run_update_bars(tmp_path, phase="post_close")
        seed.assert_called_once()
        assert result["success"] == 1

    def test_session_phase_disables_seeding(self, tmp_path: Path):
        seed, _ = self._run_update_bars(tmp_path, phase="session")
        seed.assert_not_called()

    def test_symbols_mode_disables_seeding(self, tmp_path: Path):
        seed, _ = self._run_update_bars(
            tmp_path, phase="post_close", symbols=["000001"]
        )
        seed.assert_not_called()

    def test_seeding_failure_falls_back_to_per_stock(self, tmp_path: Path):
        """快照异常 → 完整回退逐股路径，任务不失败。"""
        db = MagicMock()
        db.db_path = str(tmp_path / "bars.db")
        sqlite3.connect(db.db_path).close()
        db.get_stock_list.return_value = pd.DataFrame({"code": ["000001"]})
        db.get_latest_bar_date.return_value = self.TARGET
        db.watchlist_get_all.return_value = pd.DataFrame()
        loader = MagicMock()

        with patch("tasks.bars.ProgressTracker") as tracker, \
             patch("tasks.bars.market_phase", return_value="post_close"), \
             patch("tasks.bars.shanghai_today", return_value=self.TARGET), \
             patch("tasks.bars.get_expected_latest_trading_day",
                   return_value=self.TARGET), \
             patch("tasks.bars._detect_suspended_symbols", return_value=set()), \
             patch("tasks.bars._seed_from_snapshot",
                   side_effect=RuntimeError("spot down")), \
             patch("tasks.bars.time.sleep"), \
             patch("tasks.bars.logger"):
            tracker.FILE.exists.return_value = False
            result = update_bars(db, loader, force=True)

        # 回退后逐股循环正常走完（该股已最新 → skipped）
        assert result["skipped"] == 1
        assert result["status"] != "failed"
