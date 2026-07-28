"""Integration tests for tasks/core_chain.py.

Covers:
- update_stock_list with real DB path (industry merge via is_real_db_path guard)
- _calculate_chip_distribution_for_symbol pure function
- _process_chip_one edge paths
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from tasks.core_chain import (
    _calculate_chip_distribution_for_symbol,
    _process_chip_one,
    compute_chip_record_for_refresh,
    compute_indicator_record_for_refresh,
    update_chip_distribution,
    update_stock_list,
)

# ===========================================================================
# Helpers
# ===========================================================================


def _make_stock_ak_df() -> pd.DataFrame:
    return pd.DataFrame({
        "code": ["000001", "600000", "688001", "300001"],
        "name": ["平安银行", "浦发银行", "科创板测试", "创业板测试"],
    })


# ===========================================================================
# update_stock_list — 集成测试（真实数据库 / is_real_db_path 守卫路径）
# ===========================================================================


class TestUpdateStockListIntegration:
    """测试 update_stock_list 中 is_real_db_path 控制的行业合并分支。

    test_daily_pipeline.py 的 TestUpdateStockList 已覆盖 Mock DB 路径
    (is_real_db_path=False → industry=None)。本文件补全真实 DB 路径。
    """

    def test_merge_industry_from_existing_db(self, tmp_path: Path):
        """真实 DB 路径 + DB 中有 industry → 合并旧 industry。"""
        db_path = tmp_path / "quant_core.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute(
            "CREATE TABLE stock_list (code TEXT, industry TEXT)"
        )
        conn.execute(
            "INSERT INTO stock_list VALUES ('000001', '银行')"
        )
        conn.execute(
            "INSERT INTO stock_list VALUES ('600000', '银行')"
        )
        conn.commit()
        conn.close()

        db = MagicMock()
        db.db_path = str(db_path)
        db.save_stock_list.return_value = None

        with patch("tasks.core_chain.ak") as mock_ak:
            mock_ak.stock_info_a_code_name.return_value = _make_stock_ak_df()
            with patch("tasks.core_chain.logger"):
                r = update_stock_list(db)

        assert r["saved"] == 4
        # 验证 save_stock_list 收到带 industry 列的 DataFrame
        calls = db.save_stock_list.call_args_list
        assert len(calls) == 1
        df_saved: pd.DataFrame = calls[0][0][0]
        assert "industry" in df_saved.columns
        # 000001 应合并到旧 industry
        row_000001 = df_saved[df_saved["code"] == "000001"].iloc[0]
        assert row_000001["industry"] == "银行"

    def test_merge_industry_empty_old_db(self, tmp_path: Path):
        """真实 DB 路径但旧 stock_list 无 industry → industry 列为 None。"""
        db_path = tmp_path / "quant_core.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute(
            "CREATE TABLE stock_list (code TEXT, industry TEXT)"
        )
        conn.execute(
            "INSERT INTO stock_list VALUES ('000001', NULL)"
        )
        conn.commit()
        conn.close()

        db = MagicMock()
        db.db_path = str(db_path)
        db.save_stock_list.return_value = None

        with patch("tasks.core_chain.ak") as mock_ak:
            mock_ak.stock_info_a_code_name.return_value = _make_stock_ak_df()
            with patch("tasks.core_chain.logger"):
                r = update_stock_list(db)

        assert r["saved"] == 4
        df_saved: pd.DataFrame = db.save_stock_list.call_args[0][0]
        # 旧 DB 中 industry 为 NULL，合并后应为 None
        assert df_saved["industry"].isna().all()

    def test_merge_industry_db_exception_fallback(self, tmp_path: Path):
        """真实 DB 路径但 SQL 查询报错 → fallback 为 industry=None。"""
        # 创建损坏的 DB（有表但无法查询）
        db_path = tmp_path / "bad.db"
        db_path.write_bytes(b"not a valid sqlite file")

        db = MagicMock()
        db.db_path = str(db_path)
        db.save_stock_list.return_value = None

        with patch("tasks.core_chain.ak") as mock_ak:
            mock_ak.stock_info_a_code_name.return_value = _make_stock_ak_df()
            with patch("tasks.core_chain.logger"):
                r = update_stock_list(db)

        assert r["saved"] == 4
        df_saved: pd.DataFrame = db.save_stock_list.call_args[0][0]
        # 异常导致 fallback：industry 全部为 None
        assert df_saved["industry"].isna().all()


# ===========================================================================
# _calculate_chip_distribution_for_symbol — 纯函数测试
# ===========================================================================


def _chip_df(n_days: int, start_date: str = "2024-01-01",
             turnover: float = 0.02) -> pd.DataFrame:
    """构造筹码分布测试用日线数据。"""
    dates = pd.date_range(start=start_date, periods=n_days, freq="B")
    np.random.seed(42)
    closes = np.linspace(10.0, 12.0, n_days) + np.random.randn(n_days) * 0.5
    opens = closes + np.random.randn(n_days) * 0.3
    highs = np.maximum(opens, closes) + np.abs(np.random.randn(n_days)) * 0.3
    lows = np.minimum(opens, closes) - np.abs(np.random.randn(n_days)) * 0.3
    return pd.DataFrame({
        "trade_date": [d.strftime("%Y-%m-%d") for d in dates],
        "open": opens,
        "high": highs,
        "low": lows,
        "close": closes,
        "turnover_rate": [turnover] * n_days,
    })


class TestChipDistribution:
    """_calculate_chip_distribution_for_symbol 纯函数测试。"""

    def test_insufficient_days(self):
        """少于 60 天 → 空 DataFrame。"""
        df = _chip_df(30)
        result = _calculate_chip_distribution_for_symbol(df)
        assert result.empty

    def test_normal_calculation(self):
        """正常 120 天 → 返回所有预期列。"""
        df = _chip_df(120)
        result = _calculate_chip_distribution_for_symbol(df)
        assert not result.empty
        assert len(result) == 120
        expected_cols = {
            "trade_date", "profit_ratio", "avg_cost",
            "cost_90_low", "cost_90_high", "concentration_90",
            "cost_70_low", "cost_70_high", "concentration_70",
            "chip_concentration",
        }
        assert set(result.columns) == expected_cols
        # profit_ratio ≥ 0（可能出现 1.0 或 NaN 但不应为负）
        pr = result["profit_ratio"].dropna()
        assert (pr >= 0).all()
        assert (pr <= 1.0 + 1e-9).all()
        # avg_cost 应在价格区间内
        assert result["avg_cost"].between(9, 13).all()
        # 集中度 >= 0
        assert (result["concentration_90"] >= 0).all()
        assert (result["concentration_70"] >= 0).all()
        # chip_concentration = 1 - concentration_90
        assert result["chip_concentration"].equals(
            1.0 - result["concentration_90"]
        )

    def test_high_turnover_normalized(self):
        """换手率 > 1 (如 50%) 应归一化为 /100。"""
        df = _chip_df(120, turnover=50.0)  # 50% 但 AkShare 用百分数
        result = _calculate_chip_distribution_for_symbol(df)
        assert not result.empty
        # 不应是 NaN 或全零
        assert result["profit_ratio"].notna().any()

    def test_zero_turnover(self):
        """零换手率 → 权重全零 → 所有输出为 NaN。"""
        df = _chip_df(120, turnover=0.0)
        result = _calculate_chip_distribution_for_symbol(df)
        assert not result.empty
        # 换手率为零时，筹码衰减因子 (1-t) = 1，新筹码不加，weights 保持 0
        # 所以 total_w = 0，所有 profile 值为 NaN
        assert result["profit_ratio"].isna().all()

    def test_price_max_equals_min(self):
        """价格区间为 0（全部为 0）→ 空 DataFrame。"""
        df = _chip_df(120)
        # 把价格全部设为 0，padding 后仍为 0 → price_max(0) <= price_min(0)
        df["close"] = 0.0
        df["open"] = 0.0
        df["high"] = 0.0
        df["low"] = 0.0
        result = _calculate_chip_distribution_for_symbol(df)
        assert result.empty

    def test_stable_price_distribution(self):
        """价格波动很小时，应有有效结果（profit_ratio 不为 NaN）。"""
        df = _chip_df(120)
        df["close"] = 10.0
        df["open"] = 10.0
        df["high"] = 10.05
        df["low"] = 9.95
        result = _calculate_chip_distribution_for_symbol(df)
        assert not result.empty
        # 价格稳定时，profit_ratio 应全部有值且非负
        assert result["profit_ratio"].notna().any()
        assert (result["profit_ratio"].dropna() >= 0).all()


# ===========================================================================
# _process_chip_one — 单只股票筹码分布处理路径
# ===========================================================================


class TestProcessChipOne:
    """_process_chip_one 的边缘路径测试。"""

    def test_insufficient_data(self):
        """数据不足 60 天 → 'insufficient'。"""
        db = MagicMock()
        db.get_daily_bars.return_value = _chip_df(30)
        status = _process_chip_one(db, "000001", n_bins=100, min_days=60)
        assert status == "insufficient"

    def test_empty_chip_result(self):
        """筹码分布计算为空 → 'failed'。"""
        db = MagicMock()
        # 价格全部为 0 → price_max(0) <= price_min(0) → 空结果
        df = _chip_df(120)
        df["close"] = 0.0
        df["open"] = 0.0
        df["high"] = 0.0
        df["low"] = 0.0
        db.get_daily_bars.return_value = df
        status = _process_chip_one(db, "000001", n_bins=100, min_days=60)
        assert status == "failed"

    def test_exception_during_processing(self):
        """处理过程中抛异常 → 'failed'。"""
        db = MagicMock()
        db.get_daily_bars.side_effect = RuntimeError("DB error")
        status = _process_chip_one(db, "000001", n_bins=100, min_days=60)
        assert status == "failed"

    def test_normal_success(self):
        """正常 120 天数据 → 'success'，且 save_chip_distribution_batch 被调用。"""
        db = MagicMock()
        db.get_daily_bars.return_value = _chip_df(120)
        db.save_chip_distribution_batch.return_value = 1
        status = _process_chip_one(db, "000001", n_bins=100, min_days=60)
        assert status == "success"
        db.save_chip_distribution_batch.assert_called_once()
        # 验证保存的记录数 = 天数
        records = db.save_chip_distribution_batch.call_args[0][0]
        assert len(records) == 120
        # 验证每条记录含必要字段
        for rec in records[:3]:
            assert "ts_code" in rec
            assert "trade_date" in rec
            assert "avg_cost" in rec

    def test_date_column_renamed(self):
        """列名为 'date' 而非 'trade_date' → 应自动重命名。"""
        db = MagicMock()
        df = _chip_df(120).rename(columns={"trade_date": "date"})
        db.get_daily_bars.return_value = df
        db.save_chip_distribution_batch.return_value = 1
        status = _process_chip_one(db, "000001", n_bins=100, min_days=60)
        assert status == "success"


# ===========================================================================
# update_chip_distribution — 入口调度路径
# ===========================================================================


class TestUpdateChipDistribution:
    """update_chip_distribution 的调度逻辑测试。"""

    def test_empty_symbols(self, tmp_path: Path):
        """没有需要更新的股票 → total=0。"""
        db_path = tmp_path / "empty.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute(
            "CREATE TABLE daily_bars (ts_code TEXT, trade_date TEXT)"
        )
        conn.execute(
            "INSERT INTO daily_bars VALUES ('000001', '2024-01-01')"
        )
        conn.commit()
        conn.close()

        db = MagicMock()
        db.db_path = str(db_path)
        with patch("tasks.core_chain.logger"):
            r = update_chip_distribution(db, symbols_to_update=[])
        assert r["total"] == 0

    def test_explicit_symbols(self, tmp_path: Path):
        """指定 symbols_to_update → 只处理指定股票。"""
        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute(
            "CREATE TABLE daily_bars (ts_code TEXT, trade_date TEXT)"
        )
        conn.execute(
            "INSERT INTO daily_bars VALUES ('000001', '2024-01-01')"
        )
        conn.commit()
        conn.close()

        db = MagicMock()
        db.db_path = str(db_path)
        db.get_daily_bars.return_value = _chip_df(120)
        db.save_chip_distribution_batch.return_value = 1
        with patch("tasks.core_chain.logger"):
            r = update_chip_distribution(
                db, symbols_to_update=["000001"]
            )
        assert r["total"] == 1
        assert r["success"] == 1


# ===========================================================================
# 收盘刷新 helper（Task 7）—— 只返回目标日一条记录，绝不写库
# ===========================================================================


def _fake_indicator_engine() -> MagicMock:
    """回显 date/close 并附确定性 ma5 的手写 engine fake。"""
    engine = MagicMock()

    def _calc(df: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame({
            "date": df["date"],
            "close": df["close"],
            "ma5": df["close"].rolling(5).mean(),
        })

    engine.calculate_all_indicators.side_effect = _calc
    return engine


class TestComputeIndicatorRecordForRefresh:
    """compute_indicator_record_for_refresh：由全量历史计算，只提交目标日。"""

    def test_returns_only_target_date_record(self):
        """120 天历史 → 单条目标日记录，值来自全历史窗口计算。"""
        df = _chip_df(120)
        target = df["trade_date"].iloc[-1]
        db = MagicMock()
        db.get_daily_bars.return_value = df

        record, reason = compute_indicator_record_for_refresh(
            db, _fake_indicator_engine(), "000001.SZ", target
        )

        assert reason is None
        assert record["ts_code"] == "000001.SZ"
        assert record["trade_date"] == target
        assert record["close"] == pytest.approx(df["close"].iloc[-1])
        # ma5 由完整历史窗口滚动得出（目标日前 5 天均值）
        assert record["ma5"] == pytest.approx(df["close"].iloc[-5:].mean())
        # engine 未输出的指标列统一为 None，而非缺键
        assert record["macd_hist"] is None
        # 不写库：helper 不得调用任何 save
        db.save_indicators_batch.assert_not_called()

    def test_insufficient_history(self):
        """< 60 天 → (None, 'insufficient')。"""
        db = MagicMock()
        db.get_daily_bars.return_value = _chip_df(30)
        record, reason = compute_indicator_record_for_refresh(
            db, _fake_indicator_engine(), "000001.SZ", "2024-06-14"
        )
        assert record is None
        assert reason == "insufficient"

    def test_missing_target_date(self):
        """历史里没有目标日（如停牌）→ (None, 'missing_target')。"""
        db = MagicMock()
        db.get_daily_bars.return_value = _chip_df(120)
        record, reason = compute_indicator_record_for_refresh(
            db, _fake_indicator_engine(), "000001.SZ", "2030-01-01"
        )
        assert record is None
        assert reason == "missing_target"

    def test_engine_failure(self):
        """engine 抛异常 → (None, 'failed')。"""
        db = MagicMock()
        df = _chip_df(120)
        db.get_daily_bars.return_value = df
        engine = MagicMock()
        engine.calculate_all_indicators.side_effect = ValueError("calc error")
        with patch("tasks.core_chain.logger"):
            record, reason = compute_indicator_record_for_refresh(
                db, engine, "000001.SZ", df["trade_date"].iloc[-1]
            )
        assert record is None
        assert reason == "failed"

    def test_nan_values_coerced_to_none(self):
        """指标值为 NaN → 记录里存 None（SQLite 不接受 NaN）。"""
        df = _chip_df(120)
        target = df["trade_date"].iloc[-1]
        db = MagicMock()
        db.get_daily_bars.return_value = df
        engine = MagicMock()
        engine.calculate_all_indicators.side_effect = lambda d: pd.DataFrame({
            "date": d["date"],
            "close": d["close"],
            "ma5": [float("nan")] * len(d),
        })

        record, reason = compute_indicator_record_for_refresh(
            db, engine, "000001.SZ", target
        )

        assert reason is None
        assert record["ma5"] is None


class TestComputeChipRecordForRefresh:
    """compute_chip_record_for_refresh：由全量历史计算，只提交目标日。"""

    def test_returns_only_target_date_record(self):
        """120 天历史 → 单条目标日记录，与全历史计算的末行一致。"""
        df = _chip_df(120)
        target = df["trade_date"].iloc[-1]
        db = MagicMock()
        db.get_daily_bars.return_value = df

        record, reason = compute_chip_record_for_refresh(db, "000001.SZ", target)

        assert reason is None
        assert record["ts_code"] == "000001.SZ"
        assert record["trade_date"] == target
        full = _calculate_chip_distribution_for_symbol(df)
        assert record["avg_cost"] == pytest.approx(full["avg_cost"].iloc[-1])
        assert record["profit_ratio"] == pytest.approx(full["profit_ratio"].iloc[-1])
        assert record["chip_concentration"] == pytest.approx(
            full["chip_concentration"].iloc[-1]
        )
        # 不写库
        db.save_chip_distribution_batch.assert_not_called()

    def test_insufficient_history(self):
        """< 60 天 → (None, 'insufficient')。"""
        db = MagicMock()
        db.get_daily_bars.return_value = _chip_df(30)
        record, reason = compute_chip_record_for_refresh(db, "000001.SZ", "2024-06-14")
        assert record is None
        assert reason == "insufficient"

    def test_missing_target_date(self):
        """历史里没有目标日 → (None, 'missing_target')。"""
        db = MagicMock()
        db.get_daily_bars.return_value = _chip_df(120)
        record, reason = compute_chip_record_for_refresh(db, "000001.SZ", "2030-01-01")
        assert record is None
        assert reason == "missing_target"

    def test_db_failure(self):
        """读取日线抛异常 → (None, 'failed')。"""
        db = MagicMock()
        db.get_daily_bars.side_effect = RuntimeError("DB error")
        with patch("tasks.core_chain.logger"):
            record, reason = compute_chip_record_for_refresh(
                db, "000001.SZ", "2024-06-14"
            )
        assert record is None
        assert reason == "failed"
