"""核心远端刷新适配器测试（Task 6）。

真实临时 SQLite + 手写 fake，覆盖：
- BarsRefreshAdapter：绕过完成快捷路径、只抓目标日、失败保留旧行、changed_symbols
- FundamentalsRefreshAdapter：无 5000 行阈值、保留已有 dividend_yield、低覆盖率保留旧分区
- MarketSnapshotRefreshAdapter：只改 dividend_yield（可覆盖非空盘中值）、绝不动 PE/PB/PEG
- FundFlowRefreshAdapter：symbol/date → ts_code/trade_date 边界、全空数值行剔除、低覆盖率保留
- build_core_refresh_adapters：显式注册表
"""
from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from core.refresh import RefreshContext
from core.refresh_adapters import (
    BarsRefreshAdapter,
    EastmoneyChipRefreshAdapter,
    FundamentalsRefreshAdapter,
    FundFlowRefreshAdapter,
    HistoricalValuationRefreshAdapter,
    IndicatorsRefreshAdapter,
    LocalChipRefreshAdapter,
    MarketSnapshotRefreshAdapter,
    SectorIndustryRefreshAdapter,
    build_core_refresh_adapters,
    build_derived_refresh_adapters,
)
from core.refresh_store import RefreshValidationError, SQLiteRefreshStore

TARGET = "2026-07-27"

# ===========================================================================
# Helpers
# ===========================================================================


def _create_refresh_db(db_path: str) -> None:
    """创建收盘刷新涉及的正式表（带 UNIQUE 约束，供 upsert 使用）。"""
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE stock_list (code TEXT, name TEXT, market TEXT)")
    conn.execute(
        """CREATE TABLE daily_bars (
            ts_code TEXT NOT NULL, trade_date TEXT NOT NULL,
            open REAL, high REAL, low REAL, close REAL,
            volume REAL, amount REAL, data_source TEXT,
            UNIQUE(ts_code, trade_date))"""
    )
    conn.execute(
        """CREATE TABLE fundamentals (
            ts_code TEXT NOT NULL, trade_date TEXT NOT NULL,
            pe_ttm REAL, pb REAL, ps_ttm REAL, dividend_yield REAL,
            roe REAL, roa REAL, gross_margin REAL, net_margin REAL,
            debt_ratio REAL, revenue_growth REAL, profit_growth REAL,
            eps_growth REAL, peg REAL, market_cap REAL,
            UNIQUE(ts_code, trade_date))"""
    )
    conn.execute(
        """CREATE TABLE fund_flow (
            ts_code TEXT NOT NULL, trade_date TEXT NOT NULL,
            main_net_inflow REAL, main_net_inflow_pct REAL,
            super_large_net_inflow REAL, super_large_net_inflow_pct REAL,
            large_net_inflow REAL, large_net_inflow_pct REAL,
            is_simulated INTEGER DEFAULT 0,
            UNIQUE(ts_code, trade_date))"""
    )
    conn.commit()
    conn.close()


def _query(db_path: str, sql: str, params: tuple = ()) -> list[tuple]:
    conn = sqlite3.connect(db_path)
    try:
        return conn.execute(sql, params).fetchall()
    finally:
        conn.close()


def _execute(db_path: str, sql: str, params: tuple = ()) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute(sql, params)
    conn.commit()
    conn.close()


def _context(symbols: tuple[str, ...] | None = None) -> RefreshContext:
    return RefreshContext(
        target_date=TARGET,
        started_at=datetime(2026, 7, 27, 16, 30, tzinfo=ZoneInfo("Asia/Shanghai")),
        run_id="run-test",
        symbols=symbols,
    )


class FakeDb:
    """只提供 db_path 与 get_stock_list 的手写 db fake。"""

    def __init__(self, db_path: str, codes: tuple[str, ...] = ()):
        self.db_path = db_path
        self._codes = list(codes)

    def get_stock_list(self) -> pd.DataFrame:
        return pd.DataFrame({"code": self._codes})


class FakeBarsLoader:
    """按股票返回预设 DataFrame（或抛预设异常）的手写 loader fake。"""

    def __init__(self, frames: dict[str, pd.DataFrame | Exception]):
        self.frames = frames
        self.calls: list[tuple[str, str | None, str | None]] = []

    def get_daily_bars(self, symbol, start_date=None, end_date=None):
        self.calls.append((symbol, start_date, end_date))
        item = self.frames[symbol]
        if isinstance(item, Exception):
            raise item
        return item


def _bar_frame(close: float = 10.5, **overrides) -> pd.DataFrame:
    row = {
        "trade_date": TARGET,
        "open": 10.0,
        "high": 11.0,
        "low": 9.5,
        "close": close,
        "volume": 1000.0,
        "amount": 10500.0,
        "data_source": "akshare",
    }
    row.update(overrides)
    return pd.DataFrame([row])


def _seed_bar(db_path: str, code: str, close: float = 9.0) -> None:
    _execute(
        db_path,
        "INSERT INTO daily_bars (ts_code, trade_date, open, high, low, close,"
        " volume, amount, data_source)"
        " VALUES (?, ?, 9.0, 9.5, 8.5, ?, 500.0, 4500.0, 'akshare')",
        (code, TARGET, close),
    )


def _seed_fundamental(
    db_path: str,
    code: str,
    pe: float = 10.0,
    dividend_yield: float | None = None,
    peg: float | None = 1.5,
) -> None:
    _execute(
        db_path,
        "INSERT INTO fundamentals (ts_code, trade_date, pe_ttm, pb, ps_ttm,"
        " dividend_yield, peg, market_cap)"
        " VALUES (?, ?, ?, 1.0, 2.0, ?, ?, 1e9)",
        (code, TARGET, pe, dividend_yield, peg),
    )


def _fundamental_record(code: str, pe: float = 20.0) -> dict:
    return {
        "ts_code": code,
        "trade_date": TARGET,
        "pe_ttm": pe,
        "pb": 1.8,
        "ps_ttm": 2.2,
        "dividend_yield": None,
        "roe": None,
        "roa": None,
        "gross_margin": None,
        "net_margin": None,
        "debt_ratio": None,
        "revenue_growth": None,
        "profit_growth": None,
        "eps_growth": None,
        "peg": 1.1,
        "market_cap": 2e9,
    }


def _seed_fund_flow(db_path: str, code: str, main: float = 1.0) -> None:
    _execute(
        db_path,
        "INSERT INTO fund_flow (ts_code, trade_date, main_net_inflow,"
        " main_net_inflow_pct, super_large_net_inflow, super_large_net_inflow_pct,"
        " large_net_inflow, large_net_inflow_pct, is_simulated)"
        " VALUES (?, ?, ?, 0.1, 0.5, 0.05, 0.3, 0.03, 0)",
        (code, TARGET, main),
    )


def _fund_flow_record(code: str, main: float | None = 100.0) -> dict:
    return {
        "symbol": code,
        "date": TARGET,
        "main_net_inflow": main,
        "main_net_inflow_pct": 1.0 if main is not None else None,
        "super_large_net_inflow": 50.0 if main is not None else None,
        "super_large_net_inflow_pct": 0.5 if main is not None else None,
        "large_net_inflow": 30.0 if main is not None else None,
        "large_net_inflow_pct": 0.3 if main is not None else None,
        "simulated": False,
    }


@pytest.fixture
def db_path(tmp_path: Path) -> str:
    path = str(tmp_path / "refresh.db")
    _create_refresh_db(path)
    return path


@pytest.fixture
def store(db_path: str) -> SQLiteRefreshStore:
    return SQLiteRefreshStore(db_path)


# ===========================================================================
# BarsRefreshAdapter
# ===========================================================================


class TestBarsRefreshAdapter:
    def test_refreshes_target_date_despite_fresh_rows(self, db_path, store):
        """已有目标日行（legacy 会 latest-date 跳过）仍强制重抓并替换。"""
        _seed_bar(db_path, "000001")
        _seed_bar(db_path, "000002")
        loader = FakeBarsLoader({
            "000001": _bar_frame(close=10.5),
            "000002": _bar_frame(close=11.0, high=11.5),
        })
        adapter = BarsRefreshAdapter(store=store, db=FakeDb(db_path), loader=loader)

        result = adapter.refresh(_context(symbols=("000001", "000002")))

        assert loader.calls == [
            ("000001", "20260727", "20260727"),
            ("000002", "20260727", "20260727"),
        ]
        assert result.task_name == "update_bars"
        assert result.as_of_date == TARGET
        assert (result.fetched, result.validated, result.replaced) == (2, 2, 2)
        assert result.failed_symbols == ()
        assert result.changed_symbols == ("000001", "000002")
        assert result.metadata["expected_count"] == 2
        rows = dict(_query(
            db_path, "SELECT ts_code, close FROM daily_bars WHERE trade_date = ?", (TARGET,)
        ))
        assert rows == {"000001": 10.5, "000002": 11.0}

    def test_failed_symbol_retains_old_row(self, db_path, store):
        """源端异常的股票保留盘中旧行，其余正常替换。"""
        _seed_bar(db_path, "000001")
        _seed_bar(db_path, "000002", close=8.8)
        loader = FakeBarsLoader({
            "000001": _bar_frame(close=10.5),
            "000002": ConnectionError("akshare down"),
        })
        adapter = BarsRefreshAdapter(store=store, db=FakeDb(db_path), loader=loader)

        result = adapter.refresh(_context(symbols=("000001", "000002")))

        assert result.failed_symbols == ("000002",)
        assert result.changed_symbols == ("000001",)
        assert (result.validated, result.replaced, result.retained) == (1, 1, 1)
        rows = dict(_query(
            db_path, "SELECT ts_code, close FROM daily_bars WHERE trade_date = ?", (TARGET,)
        ))
        assert rows == {"000001": 10.5, "000002": 8.8}

    def test_rejects_yfinance_rows(self, db_path, store):
        """yfinance 来源整行拒绝：计失败、保留旧行。"""
        _seed_bar(db_path, "000001", close=8.8)
        loader = FakeBarsLoader({"000001": _bar_frame(data_source="yfinance")})
        adapter = BarsRefreshAdapter(store=store, db=FakeDb(db_path), loader=loader)

        result = adapter.refresh(_context(symbols=("000001",)))

        assert result.failed_symbols == ("000001",)
        assert result.replaced == 0
        rows = _query(
            db_path, "SELECT close FROM daily_bars WHERE ts_code = '000001'"
        )
        assert rows == [(8.8,)]

    def test_rejects_invalid_ohlc_rows(self, db_path, store):
        """OHLC 不变量违规整行拒绝，保留旧行。"""
        _seed_bar(db_path, "000001", close=8.8)
        loader = FakeBarsLoader({"000001": _bar_frame(high=10.2, close=10.5)})
        adapter = BarsRefreshAdapter(store=store, db=FakeDb(db_path), loader=loader)

        result = adapter.refresh(_context(symbols=("000001",)))

        assert result.failed_symbols == ("000001",)
        assert _query(
            db_path, "SELECT close FROM daily_bars WHERE ts_code = '000001'"
        ) == [(8.8,)]

    def test_empty_symbols_tuple_is_noop(self, db_path, store):
        """symbols=() 表示不刷新任何股票，绝不落库、绝不请求。"""
        loader = FakeBarsLoader({})
        adapter = BarsRefreshAdapter(store=store, db=FakeDb(db_path), loader=loader)

        result = adapter.refresh(_context(symbols=()))

        assert loader.calls == []
        assert (result.fetched, result.validated, result.replaced) == (0, 0, 0)
        assert result.as_of_date == TARGET
        assert result.changed_symbols == ()

    def test_full_market_uses_stock_list_and_skips_beijing(self, db_path, store):
        """symbols=None → 全市场（stock_list），北交所按全局配置跳过。

        conftest 将 smartmoney_hunter.is_beijing_stock 打桩为恒 False，
        故在适配器接缝处 patch 真实的北交所判断行为。
        """
        loader = FakeBarsLoader({"000001": _bar_frame()})
        db = FakeDb(db_path, codes=("000001", "830001"))
        adapter = BarsRefreshAdapter(store=store, db=db, loader=loader)

        with patch(
            "core.refresh_adapters.should_skip_beijing",
            lambda code: code.startswith(("4", "8")),
        ):
            result = adapter.refresh(_context(symbols=None))

        assert [call[0] for call in loader.calls] == ["000001"]
        assert result.metadata["expected_count"] == 1


# ===========================================================================
# FundamentalsRefreshAdapter
# ===========================================================================


class TestFundamentalsRefreshAdapter:
    def test_replaces_partition_preserving_dividend_yield(self, db_path, store):
        """无 5000 行阈值；分区整体替换但保留已有 dividend_yield。"""
        _seed_fundamental(db_path, "000001", pe=9.0, dividend_yield=2.5)
        _seed_fundamental(db_path, "600000", pe=11.0, dividend_yield=None)
        fetched_dates: list[str] = []

        def fake_fetch(target_date: str) -> list[dict]:
            fetched_dates.append(target_date)
            return [_fundamental_record("000001", pe=20.0), _fundamental_record("600000", pe=21.0)]

        adapter = FundamentalsRefreshAdapter(
            store=store, db_path=db_path, fetch_snapshot=fake_fetch
        )

        result = adapter.refresh(_context())

        assert fetched_dates == [TARGET]
        assert result.task_name == "update_fundamentals"
        assert result.as_of_date == TARGET
        assert (result.fetched, result.validated, result.replaced) == (2, 2, 2)
        assert result.changed_symbols == ("000001", "600000")
        rows = {
            code: (pe, dy)
            for code, pe, dy in _query(
                db_path,
                "SELECT ts_code, pe_ttm, dividend_yield FROM fundamentals"
                " WHERE trade_date = ?",
                (TARGET,),
            )
        }
        assert rows["000001"] == (20.0, 2.5)  # dividend_yield 保留
        assert rows["600000"] == (21.0, None)

    def test_drops_rows_outside_target_date(self, db_path, store):
        """非目标日的行不发布（EXACT_TARGET）。"""
        _seed_fundamental(db_path, "000001", pe=9.0)
        stale = _fundamental_record("600000")
        stale["trade_date"] = "2026-07-24"

        adapter = FundamentalsRefreshAdapter(
            store=store,
            db_path=db_path,
            fetch_snapshot=lambda target_date: [_fundamental_record("000001"), stale],
        )

        result = adapter.refresh(_context())

        assert (result.fetched, result.validated) == (2, 1)
        codes = _query(
            db_path, "SELECT ts_code FROM fundamentals WHERE trade_date = ?", (TARGET,)
        )
        assert codes == [("000001",)]

    def test_low_coverage_preserves_partition(self, db_path, store):
        """新快照覆盖率不足 → 拒绝发布，旧分区原样保留。"""
        for i in range(3):
            _seed_fundamental(db_path, f"00000{i + 1}", pe=9.0 + i)

        adapter = FundamentalsRefreshAdapter(
            store=store,
            db_path=db_path,
            fetch_snapshot=lambda target_date: [_fundamental_record("000001")],
        )

        with pytest.raises(RefreshValidationError):
            adapter.refresh(_context())

        rows = _query(
            db_path,
            "SELECT ts_code, pe_ttm FROM fundamentals WHERE trade_date = ? ORDER BY ts_code",
            (TARGET,),
        )
        assert rows == [("000001", 9.0), ("000002", 10.0), ("000003", 11.0)]

    def test_symbols_scope_upserts_only_requested(self, db_path, store):
        """symbols 范围内 upsert，范围外旧行不删不改。"""
        _seed_fundamental(db_path, "000001", pe=9.0, dividend_yield=2.5)
        _seed_fundamental(db_path, "600000", pe=11.0)

        adapter = FundamentalsRefreshAdapter(
            store=store,
            db_path=db_path,
            fetch_snapshot=lambda target_date: [
                _fundamental_record("000001", pe=20.0),
                _fundamental_record("600000", pe=21.0),
            ],
        )

        result = adapter.refresh(_context(symbols=("000001",)))

        assert result.changed_symbols == ("000001",)
        rows = {
            code: (pe, dy)
            for code, pe, dy in _query(
                db_path,
                "SELECT ts_code, pe_ttm, dividend_yield FROM fundamentals"
                " WHERE trade_date = ?",
                (TARGET,),
            )
        }
        assert rows["000001"] == (20.0, 2.5)
        assert rows["600000"] == (11.0, None)  # 范围外未动

    def test_empty_symbols_tuple_is_noop(self, db_path, store):
        calls: list[str] = []
        adapter = FundamentalsRefreshAdapter(
            store=store,
            db_path=db_path,
            fetch_snapshot=lambda target_date: calls.append(target_date) or [],
        )

        result = adapter.refresh(_context(symbols=()))

        assert calls == []
        assert (result.fetched, result.replaced) == (0, 0)
        assert result.as_of_date == TARGET


# ===========================================================================
# MarketSnapshotRefreshAdapter
# ===========================================================================


class TestMarketSnapshotRefreshAdapter:
    def test_overwrites_dividend_yield_never_pe_pb_peg(self, db_path, store):
        """收盘刷新可覆盖非空盘中 dividend_yield，但绝不改 PE/PB/PEG。"""
        _seed_fundamental(db_path, "000001", pe=10.0, dividend_yield=1.1, peg=1.5)
        _seed_fundamental(db_path, "600000", pe=8.0, dividend_yield=None, peg=0.9)

        adapter = MarketSnapshotRefreshAdapter(
            store=store,
            db_path=db_path,
            fetch_quotes=lambda codes: [
                {"code": "000001", "dividend_yield": 2.5},
                {"code": "600000", "dividend_yield": 3.3},
            ],
        )

        result = adapter.refresh(_context())

        assert result.task_name == "update_market_snapshot"
        assert result.as_of_date == TARGET
        assert result.replaced == 2
        assert result.retained == 0
        rows = {
            code: (pe, pb, peg, dy)
            for code, pe, pb, peg, dy in _query(
                db_path,
                "SELECT ts_code, pe_ttm, pb, peg, dividend_yield FROM fundamentals"
                " WHERE trade_date = ?",
                (TARGET,),
            )
        }
        assert rows["000001"] == (10.0, 1.0, 1.5, 2.5)  # 只有 dy 变
        assert rows["600000"] == (8.0, 1.0, 0.9, 3.3)

    def test_low_coverage_preserves_all_values(self, db_path, store):
        """报价覆盖率 < 0.4 → 拒绝更新，全部旧值保留。"""
        for code in ("000001", "000002", "000003"):
            _seed_fundamental(db_path, code, dividend_yield=1.1)

        adapter = MarketSnapshotRefreshAdapter(
            store=store,
            db_path=db_path,
            fetch_quotes=lambda codes: [{"code": "000001", "dividend_yield": 2.5}],
        )

        with pytest.raises(RefreshValidationError):
            adapter.refresh(_context())

        rows = _query(
            db_path,
            "SELECT DISTINCT dividend_yield FROM fundamentals WHERE trade_date = ?",
            (TARGET,),
        )
        assert rows == [(1.1,)]

    def test_ignores_quotes_outside_partition_and_null_yield(self, db_path, store):
        """分区外代码与 dividend_yield 为空的报价不参与更新。"""
        _seed_fundamental(db_path, "000001", dividend_yield=1.1)
        _seed_fundamental(db_path, "600000", dividend_yield=1.2)

        adapter = MarketSnapshotRefreshAdapter(
            store=store,
            db_path=db_path,
            fetch_quotes=lambda codes: [
                {"code": "000001", "dividend_yield": 2.5},
                {"code": "600000", "dividend_yield": None},
                {"code": "999999", "dividend_yield": 9.9},  # 分区外
            ],
        )

        result = adapter.refresh(_context())

        assert result.replaced == 1
        assert result.retained == 1
        rows = dict(_query(
            db_path,
            "SELECT ts_code, dividend_yield FROM fundamentals WHERE trade_date = ?",
            (TARGET,),
        ))
        assert rows == {"000001": 2.5, "600000": 1.2}
        # 分区外代码不产生新行
        assert len(rows) == 2

    def test_missing_partition_raises(self, db_path, store):
        """目标日无 fundamentals 分区 → 无法补充，直接失败保留。"""
        adapter = MarketSnapshotRefreshAdapter(
            store=store, db_path=db_path, fetch_quotes=lambda codes: []
        )

        with pytest.raises(RefreshValidationError):
            adapter.refresh(_context())


# ===========================================================================
# FundFlowRefreshAdapter
# ===========================================================================


class TestFundFlowRefreshAdapter:
    def test_maps_legacy_keys_to_table_columns(self, db_path, store):
        """symbol/date（legacy 形状）映射为 ts_code/trade_date 后整分区替换。"""
        _seed_fund_flow(db_path, "600000", main=1.0)
        seen: list[str] = []

        def fake_fetch(loader, trade_date: str) -> list[dict]:
            seen.append(trade_date)
            return [_fund_flow_record("600000", main=100.0)]

        adapter = FundFlowRefreshAdapter(
            store=store, loader=object(), fetch_records=fake_fetch
        )

        result = adapter.refresh(_context())

        assert seen == [TARGET]
        assert result.task_name == "update_fund_flow"
        assert result.as_of_date == TARGET
        assert result.changed_symbols == ("600000",)
        rows = _query(
            db_path,
            "SELECT ts_code, main_net_inflow, is_simulated FROM fund_flow"
            " WHERE trade_date = ?",
            (TARGET,),
        )
        assert rows == [("600000", 100.0, 0)]

    def test_rejects_all_empty_numeric_rows(self, db_path, store):
        """六个数值字段全空的记录不发布。"""
        _seed_fund_flow(db_path, "600000")

        adapter = FundFlowRefreshAdapter(
            store=store,
            loader=object(),
            fetch_records=lambda loader, trade_date: [
                _fund_flow_record("600000", main=100.0),
                _fund_flow_record("000001", main=None),
            ],
        )

        result = adapter.refresh(_context())

        assert (result.fetched, result.validated) == (2, 1)
        codes = _query(
            db_path, "SELECT ts_code FROM fund_flow WHERE trade_date = ?", (TARGET,)
        )
        assert codes == [("600000",)]

    def test_low_coverage_preserves_partition(self, db_path, store):
        """新快照行数远低于旧分区 → 拒绝发布，旧分区保留。"""
        for i in range(10):
            _seed_fund_flow(db_path, f"6000{i:02d}")

        adapter = FundFlowRefreshAdapter(
            store=store,
            loader=object(),
            fetch_records=lambda loader, trade_date: [_fund_flow_record("600000")],
        )

        with pytest.raises(RefreshValidationError):
            adapter.refresh(_context())

        count = _query(
            db_path, "SELECT COUNT(*) FROM fund_flow WHERE trade_date = ?", (TARGET,)
        )
        assert count == [(10,)]

    def test_symbols_scope_upserts_only_requested(self, db_path, store):
        """symbols 范围内 upsert，范围外旧行不删不改。"""
        _seed_fund_flow(db_path, "600000", main=1.0)
        _seed_fund_flow(db_path, "000001", main=2.0)

        adapter = FundFlowRefreshAdapter(
            store=store,
            loader=object(),
            fetch_records=lambda loader, trade_date: [
                _fund_flow_record("600000", main=100.0),
                _fund_flow_record("000001", main=200.0),
            ],
        )

        result = adapter.refresh(_context(symbols=("600000",)))

        assert result.changed_symbols == ("600000",)
        rows = dict(_query(
            db_path,
            "SELECT ts_code, main_net_inflow FROM fund_flow WHERE trade_date = ?",
            (TARGET,),
        ))
        assert rows == {"600000": 100.0, "000001": 2.0}

    def test_empty_symbols_tuple_is_noop(self, db_path, store):
        calls: list[str] = []
        adapter = FundFlowRefreshAdapter(
            store=store,
            loader=object(),
            fetch_records=lambda loader, trade_date: calls.append(trade_date) or [],
        )

        result = adapter.refresh(_context(symbols=()))

        assert calls == []
        assert (result.fetched, result.replaced) == (0, 0)
        assert result.as_of_date == TARGET


# ===========================================================================
# build_core_refresh_adapters
# ===========================================================================


def test_build_core_refresh_adapters_registry(db_path, store):
    """显式注册四个核心远端任务，键为注册表任务名。"""
    db = FakeDb(db_path)
    loader = FakeBarsLoader({})

    adapters = build_core_refresh_adapters(db=db, loader=loader, store=store)

    assert set(adapters) == {
        "update_bars",
        "update_fundamentals",
        "update_market_snapshot",
        "update_fund_flow",
    }
    for adapter in adapters.values():
        assert callable(adapter.refresh)
    assert adapters["update_bars"].loader is loader
    assert adapters["update_fund_flow"].loader is loader


# ===========================================================================
# 派生适配器（Task 7）公共 fixture / fake
# ===========================================================================


def _create_derived_tables(db_path: str) -> None:
    """补建派生任务正式表（带 UNIQUE 约束，供 upsert 使用）。"""
    conn = sqlite3.connect(db_path)
    conn.execute(
        """CREATE TABLE indicators (
            ts_code TEXT NOT NULL, trade_date TEXT NOT NULL,
            close REAL, volume REAL,
            ma5 REAL, ma10 REAL, ma20 REAL, ma60 REAL, ma120 REAL, ma250 REAL,
            vol_ma5 REAL, vol_ma50 REAL, vol_ma60 REAL,
            boll_upper REAL, boll_mid REAL, boll_lower REAL, boll_bandwidth REAL,
            cyc60 REAL, chip_concentration REAL,
            macd_dif REAL, macd_dea REAL, macd_hist REAL,
            kdj_k REAL, kdj_d REAL, kdj_j REAL,
            rsi6 REAL, rsi12 REAL, rsi24 REAL, cci REAL,
            UNIQUE(ts_code, trade_date))"""
    )
    for table in ("chip_distribution", "chip_distribution_em"):
        conn.execute(
            f"""CREATE TABLE {table} (
                ts_code TEXT NOT NULL, trade_date TEXT NOT NULL,
                profit_ratio REAL, avg_cost REAL,
                cost_90_low REAL, cost_90_high REAL, concentration_90 REAL,
                cost_70_low REAL, cost_70_high REAL, concentration_70 REAL,
                chip_concentration REAL,
                UNIQUE(ts_code, trade_date))"""
        )
    conn.execute(
        """CREATE TABLE historical_valuation (
            ts_code TEXT NOT NULL, trade_date TEXT NOT NULL,
            pe_ttm REAL, pb REAL, ps_ttm REAL, dividend_yield REAL,
            UNIQUE(ts_code, trade_date))"""
    )
    conn.execute(
        """CREATE TABLE sector_industry (
            industry_name TEXT NOT NULL, trade_date TEXT NOT NULL,
            avg_pe REAL, avg_pb REAL, avg_ps REAL, avg_roe REAL,
            avg_revenue_growth REAL, avg_profit_growth REAL,
            total_market_cap REAL, fund_inflow_rank INTEGER, data_source TEXT,
            UNIQUE(industry_name, trade_date))"""
    )
    conn.execute("ALTER TABLE stock_list ADD COLUMN industry TEXT")
    conn.commit()
    conn.close()


@pytest.fixture
def derived_db_path(db_path: str) -> str:
    _create_derived_tables(db_path)
    return db_path


def _history_frame(n_days: int, end: str = TARGET) -> pd.DataFrame:
    """构造以 end 为最后一个交易日的确定性日线历史。"""
    dates = pd.date_range(end=end, periods=n_days, freq="B")
    closes = [10.0 + 0.01 * i for i in range(n_days)]
    return pd.DataFrame({
        "trade_date": [d.strftime("%Y-%m-%d") for d in dates],
        "open": [c - 0.05 for c in closes],
        "high": [c + 0.1 for c in closes],
        "low": [c - 0.1 for c in closes],
        "close": closes,
        "volume": [1000.0] * n_days,
        "turnover_rate": [0.02] * n_days,
    })


class FakeHistoryDb:
    """db_path + 按股票返回预设日线历史的手写 db fake。"""

    def __init__(self, db_path: str, frames: dict[str, pd.DataFrame | Exception]):
        self.db_path = db_path
        self.frames = frames
        self.calls: list[str] = []

    def get_daily_bars(self, symbol: str) -> pd.DataFrame:
        self.calls.append(symbol)
        item = self.frames[symbol]
        if isinstance(item, Exception):
            raise item
        return item.copy()


class FakeIndicatorEngine:
    """回显 date/close 并附确定性 ma5 的手写 engine fake。"""

    def calculate_all_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame({
            "date": df["date"],
            "close": df["close"],
            "ma5": df["close"].rolling(5).mean(),
        })


def _seed_indicator(db_path: str, code: str, trade_date: str, close: float = 1.0) -> None:
    _execute(
        db_path,
        "INSERT INTO indicators (ts_code, trade_date, close, ma5) VALUES (?, ?, ?, 9.9)",
        (code, trade_date, close),
    )


def _seed_chip(db_path: str, table: str, code: str, trade_date: str,
               avg_cost: float = 8.8) -> None:
    _execute(
        db_path,
        f"INSERT INTO {table} (ts_code, trade_date, profit_ratio, avg_cost)"
        " VALUES (?, ?, 0.5, ?)",
        (code, trade_date, avg_cost),
    )


def _chip_em_record(code: str, trade_date: str = TARGET) -> dict:
    return {
        "ts_code": code,
        "trade_date": trade_date,
        "profit_ratio": 0.6,
        "avg_cost": 10.0,
        "cost_90_low": 9.0,
        "cost_90_high": 11.0,
        "concentration_90": 0.2,
        "cost_70_low": 9.5,
        "cost_70_high": 10.5,
        "concentration_70": 0.1,
    }


class FakeChipEmFetch:
    """按股票返回预设 (record, reason) 的手写 EM 抓取 fake。"""

    def __init__(self, outcomes: dict[str, tuple[dict | None, str | None]]):
        self.outcomes = outcomes
        self.calls: list[tuple[str, str]] = []

    def __call__(self, symbol: str, target_date: str) -> tuple[dict | None, str | None]:
        self.calls.append((symbol, target_date))
        return self.outcomes[symbol]


# ===========================================================================
# IndicatorsRefreshAdapter
# ===========================================================================


class TestIndicatorsRefreshAdapter:
    def test_publishes_only_target_date_row(self, derived_db_path, store):
        """由全历史计算但只提交目标日；历史指标行绝不重写。"""
        _seed_indicator(derived_db_path, "000001", "2026-07-24", close=7.7)
        db = FakeHistoryDb(derived_db_path, {"000001": _history_frame(120)})
        adapter = IndicatorsRefreshAdapter(
            store=store, db=db, engine=FakeIndicatorEngine()
        )

        result = adapter.refresh(_context(symbols=("000001",)))

        assert result.task_name == "update_indicators"
        assert result.as_of_date == TARGET
        assert result.changed_symbols == ("000001",)
        assert result.failed_symbols == ()
        rows = _query(
            derived_db_path,
            "SELECT trade_date, close FROM indicators WHERE ts_code = '000001'"
            " ORDER BY trade_date",
        )
        # 历史行原样保留，新增仅目标日一行
        assert rows[0] == ("2026-07-24", 7.7)
        assert len(rows) == 2
        assert rows[1][0] == TARGET

    def test_failed_symbol_keeps_old_row_and_enters_queue(self, derived_db_path, store):
        """计算失败 → 目标日旧行保留，股票进入失败队列。"""
        _seed_indicator(derived_db_path, "000002", TARGET, close=6.6)
        db = FakeHistoryDb(derived_db_path, {
            "000001": _history_frame(120),
            "000002": RuntimeError("db error"),
        })
        adapter = IndicatorsRefreshAdapter(
            store=store, db=db, engine=FakeIndicatorEngine()
        )

        result = adapter.refresh(_context(symbols=("000001", "000002")))

        assert result.failed_symbols == ("000002",)
        assert result.changed_symbols == ("000001",)
        rows = _query(
            derived_db_path,
            "SELECT close FROM indicators WHERE ts_code = '000002' AND trade_date = ?",
            (TARGET,),
        )
        assert rows == [(6.6,)]

    def test_insufficient_history_is_not_failure(self, derived_db_path, store):
        """历史不足 60 天（新股）不算失败，不发布任何行。"""
        db = FakeHistoryDb(derived_db_path, {"000001": _history_frame(30)})
        adapter = IndicatorsRefreshAdapter(
            store=store, db=db, engine=FakeIndicatorEngine()
        )

        result = adapter.refresh(_context(symbols=("000001",)))

        assert result.failed_symbols == ()
        assert result.changed_symbols == ()
        assert result.metadata.get("insufficient") == 1
        assert _query(derived_db_path, "SELECT COUNT(*) FROM indicators") == [(0,)]

    def test_processes_exactly_context_symbols(self, derived_db_path, store):
        """只处理编排器收窄后的 scope，不自行扩大范围。"""
        db = FakeHistoryDb(derived_db_path, {
            "000001": _history_frame(120),
            "600000": _history_frame(120),
        })
        adapter = IndicatorsRefreshAdapter(
            store=store, db=db, engine=FakeIndicatorEngine()
        )

        adapter.refresh(_context(symbols=("000001",)))

        assert db.calls == ["000001"]

    def test_none_symbols_uses_target_bar_partition(self, derived_db_path, store):
        """symbols=None → 以目标日 daily_bars 分区为全市场范围。"""
        _seed_bar(derived_db_path, "000001")
        _seed_bar(derived_db_path, "600000")
        db = FakeHistoryDb(derived_db_path, {
            "000001": _history_frame(120),
            "600000": _history_frame(120),
        })
        adapter = IndicatorsRefreshAdapter(
            store=store, db=db, engine=FakeIndicatorEngine()
        )

        result = adapter.refresh(_context())

        assert sorted(db.calls) == ["000001", "600000"]
        assert sorted(result.changed_symbols) == ["000001", "600000"]

    def test_empty_symbols_tuple_is_noop(self, derived_db_path, store):
        db = FakeHistoryDb(derived_db_path, {})
        adapter = IndicatorsRefreshAdapter(
            store=store, db=db, engine=FakeIndicatorEngine()
        )

        result = adapter.refresh(_context(symbols=()))

        assert db.calls == []
        assert (result.fetched, result.replaced) == (0, 0)


# ===========================================================================
# LocalChipRefreshAdapter
# ===========================================================================


class TestLocalChipRefreshAdapter:
    def test_publishes_only_target_date_row(self, derived_db_path, store):
        """由全历史计算但只提交目标日；历史筹码行绝不重写。"""
        _seed_chip(derived_db_path, "chip_distribution", "000001", "2026-07-24")
        db = FakeHistoryDb(derived_db_path, {"000001": _history_frame(120)})
        adapter = LocalChipRefreshAdapter(store=store, db=db)

        result = adapter.refresh(_context(symbols=("000001",)))

        assert result.task_name == "update_chip_distribution"
        assert result.changed_symbols == ("000001",)
        rows = _query(
            derived_db_path,
            "SELECT trade_date, avg_cost FROM chip_distribution"
            " WHERE ts_code = '000001' ORDER BY trade_date",
        )
        assert rows[0] == ("2026-07-24", 8.8)
        assert len(rows) == 2
        assert rows[1][0] == TARGET
        assert rows[1][1] != 8.8

    def test_failed_symbol_keeps_old_rows(self, derived_db_path, store):
        """单股失败不删其他股票数据，失败股旧行保留。"""
        _seed_chip(derived_db_path, "chip_distribution", "000002", TARGET, avg_cost=7.7)
        db = FakeHistoryDb(derived_db_path, {
            "000001": _history_frame(120),
            "000002": RuntimeError("db error"),
        })
        adapter = LocalChipRefreshAdapter(store=store, db=db)

        result = adapter.refresh(_context(symbols=("000001", "000002")))

        assert result.failed_symbols == ("000002",)
        rows = _query(
            derived_db_path,
            "SELECT avg_cost FROM chip_distribution"
            " WHERE ts_code = '000002' AND trade_date = ?",
            (TARGET,),
        )
        assert rows == [(7.7,)]

    def test_empty_symbols_tuple_is_noop(self, derived_db_path, store):
        db = FakeHistoryDb(derived_db_path, {})
        adapter = LocalChipRefreshAdapter(store=store, db=db)

        result = adapter.refresh(_context(symbols=()))

        assert db.calls == []
        assert (result.fetched, result.replaced) == (0, 0)


# ===========================================================================
# HistoricalValuationRefreshAdapter
# ===========================================================================


class TestHistoricalValuationRefreshAdapter:
    def test_snapshots_target_partition_only(self, derived_db_path, store):
        """只把 fundamentals 目标日分区快照过去；历史估值行保留。"""
        _seed_fundamental(derived_db_path, "000001", pe=10.0, dividend_yield=1.5)
        _seed_fundamental(derived_db_path, "600000", pe=20.0)
        _execute(
            derived_db_path,
            "INSERT INTO historical_valuation (ts_code, trade_date, pe_ttm)"
            " VALUES ('000001', '2026-07-24', 99.0)",
        )
        adapter = HistoricalValuationRefreshAdapter(store=store, db_path=derived_db_path)

        result = adapter.refresh(_context())

        assert result.task_name == "update_historical_valuation"
        assert sorted(result.changed_symbols) == ["000001", "600000"]
        rows = _query(
            derived_db_path,
            "SELECT ts_code, trade_date, pe_ttm FROM historical_valuation"
            " ORDER BY trade_date, ts_code",
        )
        assert rows == [
            ("000001", "2026-07-24", 99.0),
            ("000001", TARGET, 10.0),
            ("600000", TARGET, 20.0),
        ]

    def test_symbols_scope_filters_partition(self, derived_db_path, store):
        _seed_fundamental(derived_db_path, "000001", pe=10.0)
        _seed_fundamental(derived_db_path, "600000", pe=20.0)
        adapter = HistoricalValuationRefreshAdapter(store=store, db_path=derived_db_path)

        result = adapter.refresh(_context(symbols=("600000",)))

        assert result.changed_symbols == ("600000",)
        codes = _query(derived_db_path, "SELECT ts_code FROM historical_valuation")
        assert codes == [("600000",)]

    def test_empty_partition_raises_and_retains(self, derived_db_path, store):
        """fundamentals 无目标日分区 → 报错，旧快照保留。"""
        _execute(
            derived_db_path,
            "INSERT INTO historical_valuation (ts_code, trade_date, pe_ttm)"
            " VALUES ('000001', '2026-07-24', 99.0)",
        )
        adapter = HistoricalValuationRefreshAdapter(store=store, db_path=derived_db_path)

        with pytest.raises(RefreshValidationError):
            adapter.refresh(_context())

        assert _query(
            derived_db_path, "SELECT COUNT(*) FROM historical_valuation"
        ) == [(1,)]

    def test_empty_symbols_tuple_is_noop(self, derived_db_path, store):
        adapter = HistoricalValuationRefreshAdapter(store=store, db_path=derived_db_path)
        result = adapter.refresh(_context(symbols=()))
        assert (result.fetched, result.replaced) == (0, 0)


# ===========================================================================
# SectorIndustryRefreshAdapter
# ===========================================================================


class TestSectorIndustryRefreshAdapter:
    def _seed_industry(self, db_path: str, code: str, industry: str) -> None:
        _execute(
            db_path,
            "INSERT INTO stock_list (code, name, market, industry) VALUES (?, '', 'sz', ?)",
            (code, industry),
        )

    def test_replaces_only_target_partition(self, derived_db_path, store):
        """目标日分区整体替换（盘中残留清理），历史分区保留。"""
        _seed_fundamental(derived_db_path, "000001", pe=10.0)
        _seed_fundamental(derived_db_path, "600000", pe=20.0)
        self._seed_industry(derived_db_path, "000001", "银行")
        self._seed_industry(derived_db_path, "600000", "银行")
        # 盘中残留（应被替换）+ 历史分区（应保留）
        _execute(
            derived_db_path,
            "INSERT INTO sector_industry (industry_name, trade_date, avg_pe)"
            " VALUES ('旧行业', ?, 1.0)",
            (TARGET,),
        )
        _execute(
            derived_db_path,
            "INSERT INTO sector_industry (industry_name, trade_date, avg_pe)"
            " VALUES ('银行', '2026-07-24', 5.0)",
        )
        adapter = SectorIndustryRefreshAdapter(store=store, db_path=derived_db_path)

        result = adapter.refresh(_context())

        assert result.task_name == "update_sector_industry"
        assert result.as_of_date == TARGET
        rows = _query(
            derived_db_path,
            "SELECT industry_name, trade_date, avg_pe, fund_inflow_rank, data_source"
            " FROM sector_industry ORDER BY trade_date, industry_name",
        )
        assert rows == [
            ("银行", "2026-07-24", 5.0, None, None),
            ("银行", TARGET, 15.0, None, "derived"),
        ]

    def test_empty_partition_raises_and_retains(self, derived_db_path, store):
        _execute(
            derived_db_path,
            "INSERT INTO sector_industry (industry_name, trade_date, avg_pe)"
            " VALUES ('银行', ?, 5.0)",
            (TARGET,),
        )
        adapter = SectorIndustryRefreshAdapter(store=store, db_path=derived_db_path)

        with pytest.raises(RefreshValidationError):
            adapter.refresh(_context())

        assert _query(
            derived_db_path, "SELECT COUNT(*) FROM sector_industry"
        ) == [(1,)]

    def test_empty_symbols_tuple_is_noop(self, derived_db_path, store):
        adapter = SectorIndustryRefreshAdapter(store=store, db_path=derived_db_path)
        result = adapter.refresh(_context(symbols=()))
        assert (result.fetched, result.replaced) == (0, 0)


# ===========================================================================
# EastmoneyChipRefreshAdapter
# ===========================================================================


class TestEastmoneyChipRefreshAdapter:
    def test_none_symbols_raises_instead_of_random_selector(self, derived_db_path, store):
        """刷新模式必须显式 scope；symbols=None 拒绝而非随机选股。"""
        fetch = FakeChipEmFetch({})
        adapter = EastmoneyChipRefreshAdapter(
            store=store, fetch_record=fetch, throttle=lambda: None
        )

        with pytest.raises(RefreshValidationError):
            adapter.refresh(_context())

        assert fetch.calls == []

    def test_publishes_only_target_rows_and_keeps_history(self, derived_db_path, store):
        """成功股票只 upsert 目标日一行；历史行与未涉及股票保留。"""
        _seed_chip(derived_db_path, "chip_distribution_em", "000001", "2026-07-24")
        fetch = FakeChipEmFetch({
            "000001": (_chip_em_record("000001"), None),
            "600000": (None, "missing_target"),
        })
        adapter = EastmoneyChipRefreshAdapter(
            store=store, fetch_record=fetch, throttle=lambda: None
        )

        result = adapter.refresh(_context(symbols=("000001", "600000")))

        assert result.task_name == "update_chip_distribution_em"
        assert result.changed_symbols == ("000001",)
        assert result.failed_symbols == ("600000",)
        assert result.metadata.get("aborted") is False
        rows = _query(
            derived_db_path,
            "SELECT trade_date, avg_cost FROM chip_distribution_em"
            " WHERE ts_code = '000001' ORDER BY trade_date",
        )
        assert rows == [("2026-07-24", 8.8), (TARGET, 10.0)]

    def test_aborts_after_nine_consecutive_failures(self, derived_db_path, store):
        """九连败 → aborted，未处理股票全部进入失败队列，旧筹码不删。"""
        symbols = tuple(f"0000{i:02d}" for i in range(1, 13))
        _seed_chip(derived_db_path, "chip_distribution_em", "000001", "2026-07-24")
        fetch = FakeChipEmFetch(dict.fromkeys(symbols, (None, "fetch_failed")))
        adapter = EastmoneyChipRefreshAdapter(
            store=store, fetch_record=fetch, throttle=lambda: None
        )

        result = adapter.refresh(_context(symbols=symbols))

        assert len(fetch.calls) == 9
        assert result.metadata.get("aborted") is True
        assert result.metadata.get("abort_reason") == "consecutive_failures"
        # 已失败 9 只 + 未处理 3 只全部进入失败队列
        assert result.failed_symbols == symbols
        assert result.changed_symbols == ()
        # 旧筹码行不得被删除
        assert _query(
            derived_db_path, "SELECT COUNT(*) FROM chip_distribution_em"
        ) == [(1,)]

    def test_success_resets_consecutive_counter(self, derived_db_path, store):
        """成功一次即重置计数器：8 败 + 1 成 + 8 败不触发熔断。"""
        fails_a = tuple(f"1000{i:02d}" for i in range(8))
        fails_b = tuple(f"2000{i:02d}" for i in range(8))
        symbols = fails_a + ("000001",) + fails_b
        outcomes: dict[str, tuple[dict | None, str | None]] = dict.fromkeys(
            fails_a + fails_b, (None, "fetch_failed")
        )
        outcomes["000001"] = (_chip_em_record("000001"), None)
        fetch = FakeChipEmFetch(outcomes)
        adapter = EastmoneyChipRefreshAdapter(
            store=store, fetch_record=fetch, throttle=lambda: None
        )

        result = adapter.refresh(_context(symbols=symbols))

        assert len(fetch.calls) == 17
        assert result.metadata.get("aborted") is False
        assert result.changed_symbols == ("000001",)

    def test_non_fetch_failures_do_not_trip_breaker(self, derived_db_path, store):
        """missing_target/invalid 不计入连续失败熔断计数。"""
        symbols = tuple(f"3000{i:02d}" for i in range(10))
        fetch = FakeChipEmFetch(dict.fromkeys(symbols, (None, "missing_target")))
        adapter = EastmoneyChipRefreshAdapter(
            store=store, fetch_record=fetch, throttle=lambda: None
        )

        result = adapter.refresh(_context(symbols=symbols))

        assert len(fetch.calls) == 10
        assert result.metadata.get("aborted") is False
        assert result.failed_symbols == symbols

    def test_empty_symbols_tuple_is_noop(self, derived_db_path, store):
        fetch = FakeChipEmFetch({})
        adapter = EastmoneyChipRefreshAdapter(
            store=store, fetch_record=fetch, throttle=lambda: None
        )

        result = adapter.refresh(_context(symbols=()))

        assert fetch.calls == []
        assert (result.fetched, result.replaced) == (0, 0)


# ===========================================================================
# build_derived_refresh_adapters
# ===========================================================================


def test_build_derived_refresh_adapters_registry(derived_db_path, store):
    """显式注册五个派生任务，键为注册表任务名。"""
    db = FakeHistoryDb(derived_db_path, {})
    engine = FakeIndicatorEngine()

    adapters = build_derived_refresh_adapters(db=db, engine=engine, store=store)

    assert set(adapters) == {
        "update_indicators",
        "update_chip_distribution",
        "update_chip_distribution_em",
        "update_historical_valuation",
        "update_sector_industry",
    }
    for adapter in adapters.values():
        assert callable(adapter.refresh)
    assert adapters["update_indicators"].engine is engine
    assert adapters["update_indicators"].db is db
    assert adapters["update_chip_distribution"].db is db
