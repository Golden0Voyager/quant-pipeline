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
    REFRESH_ADAPTERS,
    AhPremiumRefreshAdapter,
    BarsRefreshAdapter,
    BlockTradeRefreshAdapter,
    CbIndexRefreshAdapter,
    CbQuotationRefreshAdapter,
    CbRedeemRefreshAdapter,
    ConceptBoardRefreshAdapter,
    DragonTigerRefreshAdapter,
    EastmoneyChipRefreshAdapter,
    EtfDailyRefreshAdapter,
    FundamentalsRefreshAdapter,
    FundFlowRefreshAdapter,
    HistoricalValuationRefreshAdapter,
    IndexDailyRefreshAdapter,
    IndicatorsRefreshAdapter,
    InstitutionSurveyRefreshAdapter,
    LimitUpDownRefreshAdapter,
    LocalChipRefreshAdapter,
    MarginTradingRefreshAdapter,
    MarketSnapshotRefreshAdapter,
    MarketValuationRefreshAdapter,
    NorthFlowRefreshAdapter,
    OptionSentimentRefreshAdapter,
    SectorDerivativesRefreshAdapter,
    SectorFundFlowRefreshAdapter,
    SectorIndustryRefreshAdapter,
    SouthFlowRefreshAdapter,
    StockPledgeRefreshAdapter,
    StockRepurchaseRefreshAdapter,
    build_all_refresh_adapters,
    build_core_refresh_adapters,
    build_derived_refresh_adapters,
)
from core.refresh_audit import RefreshAudit
from core.refresh_store import RefreshValidationError, SQLiteRefreshStore
from core.task_registry import refreshable_trading_tasks

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


# ===========================================================================
# 运行时适配器覆盖（Task 9 Step 1，逐字）
# ===========================================================================


def test_every_refresh_policy_has_runtime_adapter():
    assert set(REFRESH_ADAPTERS) == {
        spec.name for spec in refreshable_trading_tasks()
    }


def test_build_all_refresh_adapters_covers_every_refresh_policy(derived_db_path, store):
    """build_all_refresh_adapters 组装全部 29 个适配器，键与注册表逐一对应。"""
    db = FakeHistoryDb(derived_db_path, {})
    loader = FakeBarsLoader({})
    engine = FakeIndicatorEngine()

    adapters = build_all_refresh_adapters(
        db=db, loader=loader, engine=engine, store=store
    )

    expected = {spec.name for spec in refreshable_trading_tasks()}
    assert set(adapters) == set(REFRESH_ADAPTERS) == expected
    # 每个值满足 RefreshAdapter 协议：task_name 与键一致且 refresh 可调用
    for name, adapter in adapters.items():
        assert adapter.task_name == name
        assert callable(adapter.refresh)
    # 复合工厂正确注入依赖到需要它们的适配器
    assert adapters["update_bars"].loader is loader
    assert adapters["update_indicators"].engine is engine
    assert adapters["update_north_flow"].db_path == str(db.db_path)


# ===========================================================================
# Task 9 剩余适配器：公共 fixture 与 fake
# ===========================================================================


def _create_market_tables(db_path: str) -> None:
    """补建 Task 9 剩余适配器涉及的正式表（带 UNIQUE 约束）。"""
    conn = sqlite3.connect(db_path)
    conn.execute(
        """CREATE TABLE margin_trading (
            ts_code TEXT NOT NULL, trade_date TEXT NOT NULL,
            margin_balance REAL, margin_buy REAL, margin_repay REAL,
            short_balance REAL, short_sell REAL, short_repay REAL,
            total_balance REAL, data_source TEXT,
            UNIQUE(trade_date, ts_code))"""
    )
    conn.execute(
        """CREATE TABLE south_flow (
            trade_date TEXT NOT NULL, market TEXT,
            net_buy_amount REAL, buy_amount REAL, sell_amount REAL,
            cumulative_net_buy REAL, data_source TEXT,
            UNIQUE(trade_date, market))"""
    )
    conn.execute(
        """CREATE TABLE north_flow (
            trade_date TEXT NOT NULL, market TEXT,
            net_buy_amount REAL, data_source TEXT,
            UNIQUE(trade_date, market))"""
    )
    conn.execute(
        """CREATE TABLE index_daily (
            index_code TEXT NOT NULL, index_name TEXT, trade_date TEXT NOT NULL,
            open REAL, high REAL, low REAL, close REAL, volume REAL,
            data_source TEXT,
            UNIQUE(trade_date, index_code))"""
    )
    conn.execute(
        """CREATE TABLE market_valuation (
            date TEXT PRIMARY KEY, pe_median REAL, pe_quantile REAL,
            pe_lyr_median REAL, pb_median REAL, pb_quantile REAL,
            equity_bond_spread REAL, ebs_ma REAL, csi300_close REAL,
            data_source TEXT, data_date TEXT)"""
    )
    conn.execute(
        """CREATE TABLE cb_index (
            trade_date TEXT NOT NULL, index_code TEXT, index_name TEXT,
            open REAL, close REAL, high REAL, low REAL, volume REAL,
            data_source TEXT,
            UNIQUE(trade_date, index_code))"""
    )
    conn.execute(
        """CREATE TABLE institution_survey (
            trade_date TEXT NOT NULL, stock_code TEXT NOT NULL, stock_name TEXT,
            survey_org TEXT, survey_type TEXT, survey_count INTEGER,
            source_record_key TEXT NOT NULL UNIQUE)"""
    )
    conn.execute(
        """CREATE TABLE stock_pledge (
            trade_date TEXT NOT NULL, stock_code TEXT NOT NULL, stock_name TEXT,
            pledger TEXT, pledge_amount REAL, pledge_ratio REAL, pledge_org TEXT,
            source_record_key TEXT NOT NULL UNIQUE)"""
    )
    conn.execute(
        """CREATE TABLE etf_daily (
            ts_code TEXT NOT NULL, name TEXT, trade_date TEXT NOT NULL,
            open REAL, high REAL, low REAL, close REAL, volume REAL, amount REAL,
            data_source TEXT,
            UNIQUE(ts_code, trade_date))"""
    )
    conn.execute(
        """CREATE TABLE limit_up_down (
            trade_date TEXT NOT NULL, ts_code TEXT NOT NULL, name TEXT,
            pct_change REAL, close_price REAL, turnover_rate REAL,
            limit_type TEXT, board_count INTEGER, industry TEXT, data_source TEXT,
            UNIQUE(trade_date, ts_code))"""
    )
    conn.execute(
        """CREATE TABLE option_sentiment (
            trade_date TEXT PRIMARY KEY, qvix REAL, pcr REAL,
            put_volume INTEGER, call_volume INTEGER,
            put_oi INTEGER, call_oi INTEGER, implied_vol_avg REAL)"""
    )
    conn.execute(
        """CREATE TABLE dragon_tiger (
            source_record_key TEXT NOT NULL UNIQUE, ts_code TEXT NOT NULL,
            trade_date TEXT NOT NULL, close_price REAL, pct_change REAL,
            net_buy_amount REAL, buy_amount REAL, sell_amount REAL,
            turnover_rate REAL, market_cap REAL, reason TEXT, data_source TEXT)"""
    )
    conn.execute(
        """CREATE TABLE block_trade (
            source_record_key TEXT NOT NULL UNIQUE, ts_code TEXT NOT NULL,
            trade_date TEXT NOT NULL, deal_price REAL, close_price REAL,
            discount_rate REAL, volume REAL, amount REAL,
            buyer_branch TEXT, seller_branch TEXT, data_source TEXT)"""
    )
    conn.execute(
        """CREATE TABLE sector_fund_flow (
            trade_date TEXT NOT NULL, sector_name TEXT NOT NULL,
            main_net_inflow REAL, main_net_inflow_pct REAL,
            super_large_net_inflow REAL, large_net_inflow REAL,
            medium_net_inflow REAL, small_net_inflow REAL, data_source TEXT,
            UNIQUE(trade_date, sector_name))"""
    )
    conn.execute(
        """CREATE TABLE ah_premium (
            trade_date TEXT NOT NULL, ts_code TEXT, h_code TEXT, name TEXT,
            a_price REAL, h_price REAL, premium REAL, data_source TEXT,
            UNIQUE(trade_date, ts_code))"""
    )
    conn.execute(
        """CREATE TABLE cb_quotation (
            ts_code TEXT, bond_name TEXT, price REAL, premium REAL,
            double_low REAL, expire_date TEXT, data_source TEXT, updated_at TEXT,
            UNIQUE(ts_code))"""
    )
    conn.execute(
        """CREATE TABLE cb_redeem (
            ts_code TEXT, bond_name TEXT, redeem_flag TEXT, redeem_price REAL,
            redeem_date TEXT, data_source TEXT, updated_at TEXT,
            UNIQUE(ts_code))"""
    )
    conn.execute(
        """CREATE TABLE concept_board (
            trade_date TEXT NOT NULL, concept_code TEXT NOT NULL, concept_name TEXT,
            pct_change REAL, turnover REAL, up_count INTEGER, down_count INTEGER,
            data_source TEXT,
            UNIQUE(trade_date, concept_code))"""
    )
    conn.execute(
        """CREATE TABLE stock_repurchase (
            trade_date TEXT NOT NULL, stock_code TEXT NOT NULL, stock_name TEXT,
            repurchase_amount REAL, repurchase_price REAL,
            repurchase_price_lower REAL, repurchase_price_upper REAL,
            repurchase_quantity INTEGER, progress_status TEXT,
            source_record_key TEXT NOT NULL UNIQUE)"""
    )
    conn.execute(
        """CREATE TABLE sector_daily (
            sector_name TEXT, trade_date TEXT NOT NULL,
            open REAL, close REAL, high REAL, low REAL, volume REAL, amount REAL,
            pct_change REAL, data_source TEXT,
            UNIQUE(sector_name, trade_date))"""
    )
    conn.execute(
        """CREATE TABLE sector_valuation (
            sector_name TEXT, trade_date TEXT NOT NULL,
            pe REAL, pb REAL, total_mv REAL, data_source TEXT,
            UNIQUE(sector_name, trade_date))"""
    )
    conn.execute(
        """CREATE TABLE index_futures_basis (
            trade_date TEXT NOT NULL, futures_code TEXT,
            futures_price REAL, index_price REAL, basis REAL, basis_pct REAL,
            data_source TEXT,
            UNIQUE(trade_date, futures_code))"""
    )
    conn.commit()
    conn.close()


@pytest.fixture
def market_db_path(db_path: str) -> str:
    _create_market_tables(db_path)
    return db_path


class FakeFetcher:
    """记录调用参数并按预设返回记录（或抛预设异常）的手写 fetch fake。"""

    def __init__(self, result: list | dict | Exception | None = None):
        self.result = result
        self.calls: list[tuple] = []

    def __call__(self, *args):
        self.calls.append(args)
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


class FakeDatedFetcher:
    """按日期返回预设记录的手写 fetch fake（逐日探测型源）。"""

    def __init__(self, by_date: dict[str, list | Exception]):
        self.by_date = by_date
        self.calls: list[str] = []

    def __call__(self, trade_date: str) -> list[dict]:
        self.calls.append(trade_date)
        item = self.by_date.get(trade_date, [])
        if isinstance(item, Exception):
            raise item
        return item


def _margin_record(code: str, trade_date: str, balance: float = 1e8) -> dict:
    return {
        "ts_code": code,
        "trade_date": trade_date,
        "margin_balance": balance,
        "margin_buy": 2e7,
        "margin_repay": 1e7,
        "short_balance": 5e5,
        "short_sell": 2e5,
        "short_repay": 1e5,
        "total_balance": balance + 5e5,
        "data_source": "akshare",
    }


def _south_record(trade_date: str, market: str = "南向", net: float = 30.0) -> dict:
    return {
        "trade_date": trade_date,
        "market": market,
        "net_buy_amount": net,
        "buy_amount": 100.0,
        "sell_amount": 70.0,
        "cumulative_net_buy": 2e4,
        "data_source": "akshare",
    }


def _index_record(code: str, trade_date: str, close: float = 3000.0) -> dict:
    return {
        "index_code": code,
        "index_name": f"指数{code}",
        "trade_date": trade_date,
        "open": close - 10.0,
        "high": close + 20.0,
        "low": close - 20.0,
        "close": close,
        "volume": 1e9,
        "data_source": "akshare",
    }


def _cb_index_record(trade_date: str, price: float = 2100.0) -> dict:
    return {
        "trade_date": trade_date,
        "index_code": "JSL_EW",
        "index_name": "集思录可转债等权指数",
        "open": None,
        "close": price,
        "high": None,
        "low": None,
        "volume": 8e8,
        "data_source": "akshare",
    }


# ===========================================================================
# 组1：回看接受型（LATEST_AVAILABLE_WITHIN_LOOKBACK）
# ===========================================================================


class TestMarginTradingRefreshAdapter:
    def test_accepts_prior_day_partition_when_target_empty(self, market_db_path, store):
        """目标日空 → 逐日回退到最近有数据的回看日，as_of 为接受日。"""
        fetch = FakeDatedFetcher({
            "2026-07-27": [],
            "2026-07-26": [],
            "2026-07-25": [],
            "2026-07-24": [_margin_record("000001", "2026-07-24")],
        })
        adapter = MarginTradingRefreshAdapter(store=store, fetch_records=fetch)

        result = adapter.refresh(_context())

        assert result.as_of_date == "2026-07-24"
        assert fetch.calls == ["2026-07-27", "2026-07-26", "2026-07-25", "2026-07-24"]
        rows = _query(
            market_db_path,
            "SELECT ts_code, trade_date FROM margin_trading",
        )
        assert rows == [("000001", "2026-07-24")]

    def test_all_candidates_empty_raises_and_keeps_old_rows(self, market_db_path, store):
        """回看窗口内全部空 → 上抛，旧数据保留。"""
        _execute(
            market_db_path,
            "INSERT INTO margin_trading (ts_code, trade_date, margin_balance, data_source)"
            " VALUES ('000001', '2026-07-24', 9e7, 'akshare')",
        )
        adapter = MarginTradingRefreshAdapter(store=store, fetch_records=FakeDatedFetcher({}))

        with pytest.raises(RefreshValidationError):
            adapter.refresh(_context())

        assert _query(market_db_path, "SELECT COUNT(*) FROM margin_trading") == [(1,)]

    def test_thin_lookback_partition_below_floor_keeps_old_rows(self, market_db_path, store):
        """回看日瘦分区（<0.8×旧行数）不得 DELETE 既有完整分区。"""
        for i in range(5):
            _execute(
                market_db_path,
                "INSERT INTO margin_trading (ts_code, trade_date, margin_balance, data_source)"
                " VALUES (?, '2026-07-24', 9e7, 'akshare')",
                (f"00000{i}",),
            )
        fetch = FakeDatedFetcher({
            "2026-07-27": [],
            "2026-07-26": [],
            "2026-07-25": [],
            "2026-07-24": [
                _margin_record(f"00000{i}", "2026-07-24") for i in range(3)
            ],
        })
        adapter = MarginTradingRefreshAdapter(store=store, fetch_records=fetch)

        with pytest.raises(RefreshValidationError, match="coverage"):
            adapter.refresh(_context())

        rows = _query(
            market_db_path,
            "SELECT ts_code, margin_balance FROM margin_trading ORDER BY ts_code",
        )
        assert rows == [(f"00000{i}", 9e7) for i in range(5)]

    def test_lookback_partition_at_floor_replaces_old_rows(self, market_db_path, store):
        """达到 0.8 覆盖率下限的回看分区仍正常整体替换。"""
        for i in range(5):
            _execute(
                market_db_path,
                "INSERT INTO margin_trading (ts_code, trade_date, margin_balance, data_source)"
                " VALUES (?, '2026-07-24', 9e7, 'akshare')",
                (f"00000{i}",),
            )
        fetch = FakeDatedFetcher({
            "2026-07-27": [],
            "2026-07-26": [],
            "2026-07-25": [],
            "2026-07-24": [
                _margin_record(f"00000{i}", "2026-07-24") for i in range(4)
            ],
        })
        adapter = MarginTradingRefreshAdapter(store=store, fetch_records=fetch)

        result = adapter.refresh(_context())

        assert result.replaced == 4
        rows = _query(
            market_db_path,
            "SELECT ts_code, margin_balance FROM margin_trading ORDER BY ts_code",
        )
        assert rows == [(f"00000{i}", 1e8) for i in range(4)]

    def test_symbol_scope_upserts_only_requested(self, market_db_path, store):
        """symbols 显式范围只 upsert 请求股票，其余旧行保留。"""
        _execute(
            market_db_path,
            "INSERT INTO margin_trading (ts_code, trade_date, margin_balance, data_source)"
            " VALUES ('000002', '2026-07-27', 8e7, 'akshare')",
        )
        fetch = FakeDatedFetcher({
            TARGET: [
                _margin_record("000001", TARGET, balance=1.1e8),
                _margin_record("000002", TARGET, balance=2.2e8),
            ],
        })
        adapter = MarginTradingRefreshAdapter(store=store, fetch_records=fetch)

        result = adapter.refresh(_context(symbols=("000001",)))

        assert result.as_of_date == TARGET
        assert result.replaced == 1
        rows = _query(
            market_db_path,
            "SELECT ts_code, margin_balance FROM margin_trading ORDER BY ts_code",
        )
        assert rows == [("000001", 1.1e8), ("000002", 8e7)]

    def test_empty_symbol_scope_is_noop(self, market_db_path, store):
        fetch = FakeDatedFetcher({TARGET: [_margin_record("000001", TARGET)]})
        adapter = MarginTradingRefreshAdapter(store=store, fetch_records=fetch)

        result = adapter.refresh(_context(symbols=()))

        assert fetch.calls == []
        assert (result.fetched, result.replaced) == (0, 0)


class TestSouthFlowRefreshAdapter:
    def test_submits_only_accepted_partition_never_rewrites_history(
        self, market_db_path, store
    ):
        """历史型源只提交回看窗口内最新分区，历史行绝不重写。"""
        _execute(
            market_db_path,
            "INSERT INTO south_flow (trade_date, market, net_buy_amount, data_source)"
            " VALUES ('2026-07-20', '南向', 11.0, 'akshare')",
        )
        fetch = FakeFetcher([
            _south_record("2026-07-20", net=99.0),
            _south_record("2026-07-24", net=42.0),
        ])
        adapter = SouthFlowRefreshAdapter(store=store, fetch_records=fetch)

        result = adapter.refresh(_context())

        assert result.as_of_date == "2026-07-24"
        assert result.replaced == 1
        rows = _query(
            market_db_path,
            "SELECT trade_date, net_buy_amount FROM south_flow ORDER BY trade_date",
        )
        assert rows == [("2026-07-20", 11.0), ("2026-07-24", 42.0)]

    def test_no_partition_within_lookback_raises(self, market_db_path, store):
        adapter = SouthFlowRefreshAdapter(
            store=store, fetch_records=FakeFetcher([_south_record("2026-07-20")])
        )

        with pytest.raises(RefreshValidationError):
            adapter.refresh(_context())

    def test_thin_partition_below_floor_keeps_old_rows(self, market_db_path, store):
        """接受日瘦分区（<0.8×旧行数）不得删除既有市场行。"""
        for market in ("沪市港股通", "深市港股通"):
            _execute(
                market_db_path,
                "INSERT INTO south_flow (trade_date, market, net_buy_amount, data_source)"
                " VALUES ('2026-07-24', ?, 11.0, 'akshare')",
                (market,),
            )
        fetch = FakeFetcher([
            _south_record("2026-07-24", market="沪市港股通", net=99.0),
        ])
        adapter = SouthFlowRefreshAdapter(store=store, fetch_records=fetch)

        with pytest.raises(RefreshValidationError, match="coverage"):
            adapter.refresh(_context())

        rows = _query(
            market_db_path,
            "SELECT market, net_buy_amount FROM south_flow ORDER BY market",
        )
        assert rows == [("沪市港股通", 11.0), ("深市港股通", 11.0)]

    def test_partition_at_floor_replaces_old_rows(self, market_db_path, store):
        """达到 0.8 覆盖率下限的接受日分区仍正常整体替换。"""
        for market in ("沪市港股通", "深市港股通"):
            _execute(
                market_db_path,
                "INSERT INTO south_flow (trade_date, market, net_buy_amount, data_source)"
                " VALUES ('2026-07-24', ?, 11.0, 'akshare')",
                (market,),
            )
        fetch = FakeFetcher([
            _south_record("2026-07-24", market="沪市港股通", net=99.0),
            _south_record("2026-07-24", market="深市港股通", net=88.0),
        ])
        adapter = SouthFlowRefreshAdapter(store=store, fetch_records=fetch)

        result = adapter.refresh(_context())

        assert result.replaced == 2
        rows = _query(
            market_db_path,
            "SELECT market, net_buy_amount FROM south_flow ORDER BY market",
        )
        assert rows == [("沪市港股通", 99.0), ("深市港股通", 88.0)]


_INDEX_UNIVERSE = ("sh000001", "sz399001", "sz399006", "sh000688")


class TestIndexDailyRefreshAdapter:
    def test_accepts_latest_complete_partition(self, market_db_path, store):
        """目标日缺指数 → 回退到最近包含静态全集四指数的完整分区。"""
        fetch = FakeFetcher([
            *(_index_record(code, "2026-07-24") for code in _INDEX_UNIVERSE),
            _index_record("sh000001", TARGET, close=3310.0),
        ])
        adapter = IndexDailyRefreshAdapter(store=store, fetch_records=fetch)

        result = adapter.refresh(_context())

        assert result.as_of_date == "2026-07-24"
        assert result.replaced == 4
        rows = _query(
            market_db_path,
            "SELECT index_code, trade_date FROM index_daily ORDER BY index_code",
        )
        assert rows == [(code, "2026-07-24") for code in sorted(_INDEX_UNIVERSE)]

    def test_missing_index_never_shrinks_existing_partition(self, market_db_path, store):
        """完整性按静态指数全集判定：源只回 3/4 指数 → 上抛，既有 4 行分区不缩。"""
        for code in _INDEX_UNIVERSE:
            _execute(
                market_db_path,
                "INSERT INTO index_daily (index_code, index_name, trade_date,"
                " close, data_source) VALUES (?, ?, '2026-07-24', 3000.0, 'akshare')",
                (code, f"指数{code}"),
            )
        fetch = FakeFetcher([
            _index_record(code, "2026-07-24") for code in _INDEX_UNIVERSE[:3]
        ])
        adapter = IndexDailyRefreshAdapter(store=store, fetch_records=fetch)

        with pytest.raises(RefreshValidationError):
            adapter.refresh(_context())

        assert _query(
            market_db_path,
            "SELECT COUNT(*) FROM index_daily WHERE trade_date = '2026-07-24'",
        ) == [(4,)]

    def test_no_complete_partition_within_lookback_raises(self, market_db_path, store):
        fetch = FakeFetcher([
            _index_record("sh000001", "2026-07-10"),
            _index_record("sz399001", "2026-07-10"),
            _index_record("sh000001", TARGET),
        ])
        adapter = IndexDailyRefreshAdapter(store=store, fetch_records=fetch)

        with pytest.raises(RefreshValidationError):
            adapter.refresh(_context())

    def test_complete_partition_below_coverage_floor_keeps_old_rows(
        self, market_db_path, store
    ):
        """静态全集完整但 <0.8×旧行数（旧分区含额外指数）→ 保留旧分区。"""
        for code in (*_INDEX_UNIVERSE, "sh000016", "sz399905"):
            _execute(
                market_db_path,
                "INSERT INTO index_daily (index_code, index_name, trade_date,"
                " close, data_source) VALUES (?, ?, '2026-07-24', 3000.0, 'akshare')",
                (code, f"指数{code}"),
            )
        fetch = FakeFetcher([
            _index_record(code, "2026-07-24") for code in _INDEX_UNIVERSE
        ])
        adapter = IndexDailyRefreshAdapter(store=store, fetch_records=fetch)

        with pytest.raises(RefreshValidationError, match="coverage"):
            adapter.refresh(_context())

        assert _query(
            market_db_path,
            "SELECT COUNT(*) FROM index_daily WHERE trade_date = '2026-07-24'",
        ) == [(6,)]

    def test_complete_partition_at_coverage_floor_replaces_old_rows(
        self, market_db_path, store
    ):
        """达到 0.8 覆盖率下限（4/5）的完整分区仍正常整体替换。"""
        for code in (*_INDEX_UNIVERSE, "sh000016"):
            _execute(
                market_db_path,
                "INSERT INTO index_daily (index_code, index_name, trade_date,"
                " close, data_source) VALUES (?, ?, '2026-07-24', 3000.0, 'akshare')",
                (code, f"指数{code}"),
            )
        fetch = FakeFetcher([
            _index_record(code, "2026-07-24") for code in _INDEX_UNIVERSE
        ])
        adapter = IndexDailyRefreshAdapter(store=store, fetch_records=fetch)

        result = adapter.refresh(_context())

        assert result.replaced == 4
        rows = _query(
            market_db_path,
            "SELECT index_code FROM index_daily WHERE trade_date = '2026-07-24'"
            " ORDER BY index_code",
        )
        assert rows == [(code,) for code in sorted(_INDEX_UNIVERSE)]


class TestMarketValuationRefreshAdapter:
    def test_publishes_single_accepted_row_from_history(self, market_db_path, store):
        """全历史合并源只发布回看窗口内最新一行，历史行不落库。"""
        fetch = FakeFetcher([
            {"date": "2026-06-30", "pe_median": 30.0, "data_source": "legu", "data_date": TARGET},
            {"date": "2026-07-24", "pe_median": 31.5, "pb_median": 3.1,
             "data_source": "legu", "data_date": TARGET},
        ])
        adapter = MarketValuationRefreshAdapter(store=store, fetch_records=fetch)

        result = adapter.refresh(_context())

        assert fetch.calls == [(TARGET,)]
        assert result.as_of_date == "2026-07-24"
        rows = _query(
            market_db_path,
            "SELECT date, pe_median, pb_median, pe_lyr_median FROM market_valuation",
        )
        assert rows == [("2026-07-24", 31.5, 3.1, None)]

    def test_stale_history_only_raises(self, market_db_path, store):
        adapter = MarketValuationRefreshAdapter(
            store=store,
            fetch_records=FakeFetcher([
                {"date": "2026-07-01", "pe_median": 30.0, "data_source": "legu", "data_date": TARGET},
            ]),
        )

        with pytest.raises(RefreshValidationError):
            adapter.refresh(_context())


class TestCbIndexRefreshAdapter:
    def test_accepts_lookback_partition(self, market_db_path, store):
        fetch = FakeFetcher([
            _cb_index_record("2026-07-20", price=2050.0),
            _cb_index_record("2026-07-25", price=2101.5),
        ])
        adapter = CbIndexRefreshAdapter(store=store, fetch_records=fetch)

        result = adapter.refresh(_context())

        assert result.as_of_date == "2026-07-25"
        rows = _query(
            market_db_path,
            "SELECT trade_date, index_code, close FROM cb_index",
        )
        assert rows == [("2026-07-25", "JSL_EW", 2101.5)]

    def test_thin_partition_below_floor_keeps_old_rows(self, market_db_path, store):
        """接受日瘦分区（<0.8×旧行数）不得删除既有指数行。"""
        for code in ("JSL_EW", "JSL_PRICE"):
            _execute(
                market_db_path,
                "INSERT INTO cb_index (trade_date, index_code, close, data_source)"
                " VALUES ('2026-07-25', ?, 2000.0, 'akshare')",
                (code,),
            )
        fetch = FakeFetcher([_cb_index_record("2026-07-25", price=2101.5)])
        adapter = CbIndexRefreshAdapter(store=store, fetch_records=fetch)

        with pytest.raises(RefreshValidationError, match="coverage"):
            adapter.refresh(_context())

        rows = _query(
            market_db_path,
            "SELECT index_code, close FROM cb_index ORDER BY index_code",
        )
        assert rows == [("JSL_EW", 2000.0), ("JSL_PRICE", 2000.0)]

    def test_partition_at_floor_replaces_old_rows(self, market_db_path, store):
        """满足覆盖率下限（1/1）的接受日分区仍正常整体替换。"""
        _execute(
            market_db_path,
            "INSERT INTO cb_index (trade_date, index_code, close, data_source)"
            " VALUES ('2026-07-25', 'JSL_EW', 2000.0, 'akshare')",
        )
        fetch = FakeFetcher([_cb_index_record("2026-07-25", price=2101.5)])
        adapter = CbIndexRefreshAdapter(store=store, fetch_records=fetch)

        result = adapter.refresh(_context())

        assert result.replaced == 1
        rows = _query(
            market_db_path,
            "SELECT index_code, close FROM cb_index",
        )
        assert rows == [("JSL_EW", 2101.5)]


# ===========================================================================
# 组2：键控 30 日回看（institution_survey / stock_pledge）
# ===========================================================================


def _survey_record(code: str, trade_date: str, count: int = 10) -> dict:
    return {
        "trade_date": trade_date,
        "stock_code": code,
        "stock_name": f"股票{code}",
        "survey_org": "某基金",
        "survey_type": "特定对象调研",
        "survey_count": count,
        "source_record_key": f"survey-{code}-{trade_date}",
    }


def _pledge_record(code: str, trade_date: str, ratio: float = 12.5) -> dict:
    return {
        "trade_date": trade_date,
        "stock_code": code,
        "stock_name": f"股票{code}",
        "pledger": None,
        "pledge_amount": 1e6,
        "pledge_ratio": ratio,
        "pledge_org": None,
        "source_record_key": f"pledge-{code}-{trade_date}",
    }


class TestInstitutionSurveyRefreshAdapter:
    def test_upserts_window_records_without_deleting_history(self, market_db_path, store):
        """30 日窗口记录键控 upsert，窗口外旧行保留；as_of 取窗口内最新调研日。"""
        _execute(
            market_db_path,
            "INSERT INTO institution_survey (trade_date, stock_code, stock_name, source_record_key)"
            " VALUES ('2026-06-01', '000009', '旧股', 'survey-000009-2026-06-01')",
        )
        fetch = FakeFetcher([
            _survey_record("000001", "2026-07-24"),
            _survey_record("000002", "2026-07-10"),
        ])
        adapter = InstitutionSurveyRefreshAdapter(store=store, fetch_records=fetch)

        result = adapter.refresh(_context())

        assert fetch.calls == [("2026-06-27",)]
        assert result.as_of_date == "2026-07-24"
        assert result.replaced == 2
        assert _query(
            market_db_path, "SELECT COUNT(*) FROM institution_survey"
        ) == [(3,)]

    def test_records_outside_window_dropped_and_empty_raises(self, market_db_path, store):
        """未来日 / 超出回看窗口的记录剔除；剔除后空 → 上抛，旧数据保留。"""
        fetch = FakeFetcher([
            _survey_record("000001", "2026-07-29"),
            _survey_record("000002", "2026-05-01"),
        ])
        adapter = InstitutionSurveyRefreshAdapter(store=store, fetch_records=fetch)

        with pytest.raises(RefreshValidationError):
            adapter.refresh(_context())

        assert _query(
            market_db_path, "SELECT COUNT(*) FROM institution_survey"
        ) == [(0,)]


class TestStockPledgeRefreshAdapter:
    def test_probes_back_until_nonempty_day(self, market_db_path, store):
        """目标日起逐日探测，接受首个非空日；既有键外旧行保留。"""
        _execute(
            market_db_path,
            "INSERT INTO stock_pledge (trade_date, stock_code, stock_name, source_record_key)"
            " VALUES ('2026-07-10', '000009', '旧股', 'pledge-000009-2026-07-10')",
        )
        fetch = FakeDatedFetcher({
            "2026-07-24": [_pledge_record("000001", "2026-07-24")],
        })
        adapter = StockPledgeRefreshAdapter(store=store, fetch_records=fetch)

        result = adapter.refresh(_context())

        assert result.as_of_date == "2026-07-24"
        assert fetch.calls == ["2026-07-27", "2026-07-26", "2026-07-25", "2026-07-24"]
        rows = _query(
            market_db_path,
            "SELECT trade_date, stock_code FROM stock_pledge ORDER BY trade_date",
        )
        assert rows == [("2026-07-10", "000009"), ("2026-07-24", "000001")]

    def test_all_days_empty_raises_and_keeps_old_rows(self, market_db_path, store):
        _execute(
            market_db_path,
            "INSERT INTO stock_pledge (trade_date, stock_code, stock_name, source_record_key)"
            " VALUES ('2026-07-10', '000009', '旧股', 'pledge-000009-2026-07-10')",
        )
        fetch = FakeDatedFetcher({})
        adapter = StockPledgeRefreshAdapter(store=store, fetch_records=fetch)

        with pytest.raises(RefreshValidationError):
            adapter.refresh(_context())

        assert len(fetch.calls) == 31
        assert _query(market_db_path, "SELECT COUNT(*) FROM stock_pledge") == [(1,)]


# ===========================================================================
# 组3：目标日精确型（etf_daily / limit_up_down / option_sentiment）
# ===========================================================================


class FakeEtfFetcher:
    """按 ETF 代码返回预设记录（或抛预设异常）的手写 fetch fake。"""

    def __init__(self, frames: dict[str, list | Exception]):
        self.frames = frames
        self.calls: list[tuple[str, str, str]] = []

    def __call__(self, code: str, name: str, trade_date: str) -> list[dict]:
        self.calls.append((code, name, trade_date))
        item = self.frames.get(code, [])
        if isinstance(item, Exception):
            raise item
        return item


def _etf_record(code: str, name: str, close: float = 3.5) -> dict:
    return {
        "trade_date": TARGET,
        "ts_code": code,
        "name": name,
        "open": close - 0.1,
        "high": close + 0.1,
        "low": close - 0.2,
        "close": close,
        "volume": 1e6,
        "amount": 3.5e6,
        "data_source": "akshare",
    }


def _limit_record(code: str, limit_type: str = "涨停", trade_date: str = TARGET) -> dict:
    return {
        "trade_date": trade_date,
        "ts_code": code,
        "name": f"股票{code}",
        "pct_change": 10.0 if limit_type == "涨停" else -10.0,
        "close_price": 11.0,
        "turnover_rate": 5.0,
        "limit_type": limit_type,
        "board_count": 2 if limit_type == "涨停" else None,
        "industry": "某行业",
        "data_source": "akshare",
    }


_ETF_UNIVERSE = (
    ("510050", "上证50ETF"),
    ("510300", "沪深300ETF"),
    ("510500", "中证500ETF"),
    ("510880", "红利ETF"),
    ("588000", "科创50ETF"),
)


class TestEtfDailyRefreshAdapter:
    def test_partial_failure_keeps_old_row_when_coverage_met(self, market_db_path, store):
        """单只失败不拖垮整体：达覆盖率则发布其余，失败 ETF 旧行保留。"""
        _execute(
            market_db_path,
            "INSERT INTO etf_daily (ts_code, name, trade_date, close, data_source)"
            " VALUES ('588000', '科创50ETF', ?, 1.0, 'akshare')",
            (TARGET,),
        )
        frames: dict[str, list | Exception] = {
            code: [_etf_record(code, name)] for code, name in _ETF_UNIVERSE[:4]
        }
        frames["588000"] = RuntimeError("source down")
        fetch = FakeEtfFetcher(frames)
        adapter = EtfDailyRefreshAdapter(
            store=store, fetch_records=fetch, codes=_ETF_UNIVERSE
        )

        result = adapter.refresh(_context())

        assert result.as_of_date == TARGET
        assert result.replaced == 4
        assert result.failed_symbols == ("588000",)
        rows = _query(
            market_db_path,
            "SELECT ts_code, close FROM etf_daily WHERE ts_code = '588000'",
        )
        assert rows == [("588000", 1.0)]

    def test_low_coverage_raises_and_keeps_old_rows(self, market_db_path, store):
        _execute(
            market_db_path,
            "INSERT INTO etf_daily (ts_code, name, trade_date, close, data_source)"
            " VALUES ('510050', '上证50ETF', ?, 2.0, 'akshare')",
            (TARGET,),
        )
        frames: dict[str, list | Exception] = {
            code: RuntimeError("source down") for code, _ in _ETF_UNIVERSE[1:]
        }
        frames["510050"] = [_etf_record("510050", "上证50ETF")]
        adapter = EtfDailyRefreshAdapter(
            store=store, fetch_records=FakeEtfFetcher(frames), codes=_ETF_UNIVERSE
        )

        with pytest.raises(RefreshValidationError):
            adapter.refresh(_context())

        assert _query(
            market_db_path, "SELECT ts_code, close FROM etf_daily"
        ) == [("510050", 2.0)]

    def test_symbol_scope_fetches_only_requested(self, market_db_path, store):
        fetch = FakeEtfFetcher({"510300": [_etf_record("510300", "沪深300ETF")]})
        adapter = EtfDailyRefreshAdapter(
            store=store, fetch_records=fetch, codes=_ETF_UNIVERSE
        )

        result = adapter.refresh(_context(symbols=("510300",)))

        assert fetch.calls == [("510300", "沪深300ETF", TARGET)]
        assert result.replaced == 1


class TestLimitUpDownRefreshAdapter:
    def test_replaces_target_partition(self, market_db_path, store):
        """目标日分区整体替换，盘中残留行清理，历史分区不动。"""
        _execute(
            market_db_path,
            "INSERT INTO limit_up_down (trade_date, ts_code, name, limit_type, data_source)"
            " VALUES (?, '000003', '残留股', '涨停', 'akshare')",
            (TARGET,),
        )
        _execute(
            market_db_path,
            "INSERT INTO limit_up_down (trade_date, ts_code, name, limit_type, data_source)"
            " VALUES ('2026-07-24', '000004', '历史股', '跌停', 'akshare')",
        )
        fetch = FakeFetcher([
            _limit_record("000001", "涨停"),
            _limit_record("000002", "跌停"),
        ])
        adapter = LimitUpDownRefreshAdapter(store=store, fetch_records=fetch)

        result = adapter.refresh(_context())

        assert fetch.calls == [(TARGET,)]
        assert result.as_of_date == TARGET
        assert result.replaced == 2
        rows = _query(
            market_db_path,
            "SELECT trade_date, ts_code FROM limit_up_down ORDER BY trade_date, ts_code",
        )
        assert rows == [
            ("2026-07-24", "000004"),
            (TARGET, "000001"),
            (TARGET, "000002"),
        ]

    def test_authoritative_empty_pool_clears_partition(self, market_db_path, store):
        """权威空池 ≠ 源失败：发布空分区清目标日残留，历史不动。"""
        _execute(
            market_db_path,
            "INSERT INTO limit_up_down (trade_date, ts_code, name, limit_type, data_source)"
            " VALUES (?, '000003', '残留股', '涨停', 'akshare')",
            (TARGET,),
        )
        _execute(
            market_db_path,
            "INSERT INTO limit_up_down (trade_date, ts_code, name, limit_type, data_source)"
            " VALUES ('2026-07-24', '000004', '历史股', '跌停', 'akshare')",
        )
        adapter = LimitUpDownRefreshAdapter(store=store, fetch_records=FakeFetcher([]))

        result = adapter.refresh(_context())

        assert (result.fetched, result.replaced) == (0, 0)
        assert result.as_of_date == TARGET
        rows = _query(market_db_path, "SELECT trade_date, ts_code FROM limit_up_down")
        assert rows == [("2026-07-24", "000004")]

    def test_source_error_propagates_and_keeps_target_rows(self, market_db_path, store):
        _execute(
            market_db_path,
            "INSERT INTO limit_up_down (trade_date, ts_code, name, limit_type, data_source)"
            " VALUES (?, '000001', '旧股', '涨停', 'akshare')",
            (TARGET,),
        )
        adapter = LimitUpDownRefreshAdapter(
            store=store, fetch_records=FakeFetcher(RuntimeError("source down"))
        )

        with pytest.raises(RuntimeError):
            adapter.refresh(_context())

        assert _query(market_db_path, "SELECT COUNT(*) FROM limit_up_down") == [(1,)]


class TestOptionSentimentRefreshAdapter:
    def test_publishes_single_target_row(self, market_db_path, store):
        fetch = FakeFetcher({
            "trade_date": TARGET,
            "qvix": 18.5,
            "pcr": 0.95,
            "put_volume": 100,
            "call_volume": 120,
            "put_oi": 90,
            "call_oi": 110,
        })
        adapter = OptionSentimentRefreshAdapter(store=store, fetch_record=fetch)

        result = adapter.refresh(_context())

        assert fetch.calls == [(TARGET,)]
        assert result.as_of_date == TARGET
        assert result.replaced == 1
        rows = _query(
            market_db_path,
            "SELECT trade_date, qvix, pcr, implied_vol_avg FROM option_sentiment",
        )
        assert rows == [(TARGET, 18.5, 0.95, None)]

    def test_missing_target_row_raises_and_keeps_old(self, market_db_path, store):
        _execute(
            market_db_path,
            "INSERT INTO option_sentiment (trade_date, qvix) VALUES ('2026-07-24', 17.0)",
        )
        adapter = OptionSentimentRefreshAdapter(store=store, fetch_record=FakeFetcher(None))

        with pytest.raises(RefreshValidationError):
            adapter.refresh(_context())

        assert _query(market_db_path, "SELECT COUNT(*) FROM option_sentiment") == [(1,)]


# ===========================================================================
# 组4：事件型（dragon_tiger / block_trade）—— 键控 UPSERT，绝无删除
# ===========================================================================


def _dragon_record(code: str, reason: str = "日涨幅偏离值", net: float = 5e7) -> dict:
    return {
        "ts_code": code,
        "trade_date": TARGET,
        "close_price": 11.0,
        "pct_change": 10.0,
        "net_buy_amount": net,
        "buy_amount": 8e7,
        "sell_amount": 3e7,
        "turnover_rate": 6.0,
        "market_cap": 5e9,
        "reason": reason,
        "data_source": "akshare",
        "source_record_key": f"dt-{code}-{TARGET}-{reason}",
    }


def _block_record(code: str, price: float = 9.8, volume: float = 1e5) -> dict:
    return {
        "ts_code": code,
        "trade_date": TARGET,
        "deal_price": price,
        "close_price": 10.0,
        "discount_rate": -2.0,
        "volume": volume,
        "amount": price * volume,
        "buyer_branch": "买方营业部",
        "seller_branch": "卖方营业部",
        "data_source": "akshare",
        "source_record_key": f"bt-{code}-{TARGET}-{price}-{volume}",
    }


class TestDragonTigerRefreshAdapter:
    def test_upserts_events_and_same_day_rows_survive(self, market_db_path, store):
        """键控 UPSERT 无目标日删除：同日已有不同键的合法事件必须存活。"""
        _execute(
            market_db_path,
            "INSERT INTO dragon_tiger (source_record_key, ts_code, trade_date, reason, data_source)"
            " VALUES ('dt-000001-old-reason', '000001', ?, '旧原因', 'akshare')",
            (TARGET,),
        )
        fetch = FakeFetcher([
            _dragon_record("000001", reason="日涨幅偏离值"),
            _dragon_record("000001", reason="连续三日涨幅偏离"),
        ])
        adapter = DragonTigerRefreshAdapter(store=store, fetch_records=fetch)

        result = adapter.refresh(_context())

        assert fetch.calls == [(TARGET,)]
        assert result.as_of_date == TARGET
        assert result.replaced == 2
        assert _query(market_db_path, "SELECT COUNT(*) FROM dragon_tiger") == [(3,)]

    def test_authoritative_empty_board_is_success_without_deletion(
        self, market_db_path, store
    ):
        """权威空榜 ≠ 源失败：成功返回且绝不动既有事件行。"""
        _execute(
            market_db_path,
            "INSERT INTO dragon_tiger (source_record_key, ts_code, trade_date, reason, data_source)"
            " VALUES ('dt-000001-old-reason', '000001', ?, '旧原因', 'akshare')",
            (TARGET,),
        )
        adapter = DragonTigerRefreshAdapter(store=store, fetch_records=FakeFetcher([]))

        result = adapter.refresh(_context())

        assert (result.fetched, result.replaced) == (0, 0)
        assert result.as_of_date == TARGET
        assert result.failed_symbols == ()
        assert _query(market_db_path, "SELECT COUNT(*) FROM dragon_tiger") == [(1,)]

    def test_symbol_scope_filters_events(self, market_db_path, store):
        fetch = FakeFetcher([
            _dragon_record("000001"),
            _dragon_record("000002"),
        ])
        adapter = DragonTigerRefreshAdapter(store=store, fetch_records=fetch)

        result = adapter.refresh(_context(symbols=("000002",)))

        assert result.replaced == 1
        assert _query(
            market_db_path, "SELECT ts_code FROM dragon_tiger"
        ) == [("000002",)]


class TestBlockTradeRefreshAdapter:
    def test_same_day_duplicate_trades_survive(self, market_db_path, store):
        """同股同日不同价/量的合法多笔交易均入库，已有行保留。"""
        _execute(
            market_db_path,
            "INSERT INTO block_trade (source_record_key, ts_code, trade_date, deal_price, data_source)"
            " VALUES ('bt-000001-old', '000001', ?, 9.5, 'akshare')",
            (TARGET,),
        )
        fetch = FakeFetcher([
            _block_record("000001", price=9.8, volume=1e5),
            _block_record("000001", price=9.9, volume=2e5),
        ])
        adapter = BlockTradeRefreshAdapter(store=store, fetch_records=fetch)

        result = adapter.refresh(_context())

        assert result.replaced == 2
        assert _query(market_db_path, "SELECT COUNT(*) FROM block_trade") == [(3,)]

    def test_authoritative_empty_is_success_without_deletion(self, market_db_path, store):
        _execute(
            market_db_path,
            "INSERT INTO block_trade (source_record_key, ts_code, trade_date, deal_price, data_source)"
            " VALUES ('bt-000001-old', '000001', ?, 9.5, 'akshare')",
            (TARGET,),
        )
        adapter = BlockTradeRefreshAdapter(store=store, fetch_records=FakeFetcher([]))

        result = adapter.refresh(_context())

        assert (result.fetched, result.replaced) == (0, 0)
        assert result.as_of_date == TARGET
        assert _query(market_db_path, "SELECT COUNT(*) FROM block_trade") == [(1,)]


# ===========================================================================
# 组5：运行快照型（sector_fund_flow / ah_premium / concept_board /
#        cb_quotation / cb_redeem / stock_repurchase）
# ===========================================================================

_STARTED_AT_ISO = "2026-07-27T16:30:00+08:00"


def _sector_flow_record(sector: str, main: float = 1e8) -> dict:
    return {
        "sector_name": sector,
        "trade_date": TARGET,
        "main_net_inflow": main,
        "main_net_inflow_pct": 1.2,
        "super_large_net_inflow": None,
        "large_net_inflow": 5e7,
        "medium_net_inflow": 3e7,
        "small_net_inflow": None,
        "data_source": "ths",
    }


def _ah_record(code: str, h_code: str = "00001") -> dict:
    return {
        "trade_date": TARGET,
        "ts_code": code,
        "h_code": h_code,
        "name": f"股票{code}",
        "a_price": 10.0,
        "h_price": 8.0,
        "premium": 25.0,
        "data_source": "akshare",
    }


def _concept_record(code: str, name: str) -> dict:
    return {
        "trade_date": TARGET,
        "concept_code": code,
        "concept_name": name,
        "pct_change": 2.5,
        "turnover": 1.0,
        "up_count": 30,
        "down_count": 5,
        "data_source": "em",
    }


def _cb_quotation_record(code: str, price: float = 120.5) -> dict:
    return {
        "ts_code": code,
        "bond_name": f"转债{code}",
        "price": price,
        "premium": 15.0,
        "double_low": 135.5,
        "expire_date": "2030-01-01",
        "data_source": "akshare",
        "updated_at": _STARTED_AT_ISO,
    }


def _cb_redeem_record(code: str, flag: str = "已公告强赎") -> dict:
    return {
        "ts_code": code,
        "bond_name": f"转债{code}",
        "redeem_flag": flag,
        "redeem_price": 100.3,
        "redeem_date": "2026-08-10",
        "data_source": "akshare",
        "updated_at": _STARTED_AT_ISO,
    }


def _seed_cb_quotation(db_path: str, code: str) -> None:
    _execute(
        db_path,
        "INSERT INTO cb_quotation (ts_code, bond_name, price, data_source, updated_at)"
        " VALUES (?, ?, 99.0, 'akshare', '2026-07-24T16:30:00+08:00')",
        (code, f"转债{code}"),
    )


def _seed_cb_redeem(db_path: str, code: str) -> None:
    _execute(
        db_path,
        "INSERT INTO cb_redeem (ts_code, bond_name, redeem_flag, data_source, updated_at)"
        " VALUES (?, ?, '已解除', 'akshare', '2026-07-24T16:30:00+08:00')",
        (code, f"转债{code}"),
    )


def _repurchase_record(code: str, trade_date: str = "2026-07-25") -> dict:
    return {
        "trade_date": trade_date,
        "stock_code": code,
        "stock_name": f"股票{code}",
        "repurchase_amount": 1e8,
        "repurchase_price": 12.0,
        "repurchase_price_lower": 10.0,
        "repurchase_price_upper": 12.0,
        "repurchase_quantity": 1000000,
        "progress_status": "实施中",
        "source_record_key": f"rep-{code}-{trade_date}",
    }


class TestSectorFundFlowRefreshAdapter:
    def test_replaces_target_partition_with_run_metadata(self, market_db_path, store):
        """含历史的运行快照表：只替目标日分区，历史保留，metadata 携 run_id。"""
        _execute(
            market_db_path,
            "INSERT INTO sector_fund_flow (trade_date, sector_name, main_net_inflow, data_source)"
            " VALUES (?, '盘中残留', 0.0, 'ths')",
            (TARGET,),
        )
        _execute(
            market_db_path,
            "INSERT INTO sector_fund_flow (trade_date, sector_name, main_net_inflow, data_source)"
            " VALUES ('2026-07-24', '银行', 1.0, 'ths')",
        )
        fetch = FakeFetcher([
            _sector_flow_record("银行"),
            _sector_flow_record("证券", main=2e8),
        ])
        adapter = SectorFundFlowRefreshAdapter(store=store, fetch_records=fetch)

        result = adapter.refresh(_context())

        assert fetch.calls == [(TARGET,)]
        assert result.as_of_date == TARGET
        assert result.replaced == 2
        assert result.metadata["run_id"] == "run-test"
        rows = _query(
            market_db_path,
            "SELECT trade_date, sector_name FROM sector_fund_flow ORDER BY trade_date, sector_name",
        )
        assert rows == [
            ("2026-07-24", "银行"),
            (TARGET, "证券"),
            (TARGET, "银行"),
        ]

    def test_empty_snapshot_raises_and_keeps_old(self, market_db_path, store):
        _execute(
            market_db_path,
            "INSERT INTO sector_fund_flow (trade_date, sector_name, main_net_inflow, data_source)"
            " VALUES (?, '银行', 1.0, 'ths')",
            (TARGET,),
        )
        adapter = SectorFundFlowRefreshAdapter(store=store, fetch_records=FakeFetcher([]))

        with pytest.raises(RefreshValidationError):
            adapter.refresh(_context())

        assert _query(market_db_path, "SELECT COUNT(*) FROM sector_fund_flow") == [(1,)]


class TestAhPremiumRefreshAdapter:
    def test_replaces_target_partition_with_run_metadata(self, market_db_path, store):
        _execute(
            market_db_path,
            "INSERT INTO ah_premium (trade_date, ts_code, name, premium, data_source)"
            " VALUES ('2026-07-24', '600000', '历史行', 20.0, 'akshare')",
        )
        fetch = FakeFetcher([_ah_record("600000"), _ah_record("600036", "03968")])
        adapter = AhPremiumRefreshAdapter(store=store, fetch_records=fetch)

        result = adapter.refresh(_context())

        assert result.as_of_date == TARGET
        assert result.replaced == 2
        assert result.metadata["run_id"] == "run-test"
        assert _query(market_db_path, "SELECT COUNT(*) FROM ah_premium") == [(3,)]

    def test_empty_snapshot_raises(self, market_db_path, store):
        adapter = AhPremiumRefreshAdapter(store=store, fetch_records=FakeFetcher([]))

        with pytest.raises(RefreshValidationError):
            adapter.refresh(_context())


class TestConceptBoardRefreshAdapter:
    def test_replaces_target_partition_with_run_metadata(self, market_db_path, store):
        _execute(
            market_db_path,
            "INSERT INTO concept_board (trade_date, concept_code, concept_name, data_source)"
            " VALUES (?, 'BK9999', '盘中残留', 'em')",
            (TARGET,),
        )
        fetch = FakeFetcher([
            _concept_record("BK0001", "人工智能"),
            _concept_record("BK0002", "固态电池"),
        ])
        adapter = ConceptBoardRefreshAdapter(store=store, fetch_records=fetch)

        result = adapter.refresh(_context())

        assert fetch.calls == [(TARGET,)]
        assert result.replaced == 2
        assert result.metadata["run_id"] == "run-test"
        rows = _query(
            market_db_path,
            "SELECT concept_code FROM concept_board ORDER BY concept_code",
        )
        assert rows == [("BK0001",), ("BK0002",)]


class TestCbQuotationRefreshAdapter:
    def test_replaces_entire_snapshot(self, market_db_path, store):
        """表即当前快照：整表替换，不在新快照的旧转债行移除。"""
        _execute(
            market_db_path,
            "INSERT INTO cb_quotation (ts_code, bond_name, price, data_source, updated_at)"
            " VALUES ('113001', '退市转债', 99.0, 'akshare', '2026-07-24T16:30:00+08:00')",
        )
        fetch = FakeFetcher([
            _cb_quotation_record("113002"),
            _cb_quotation_record("113003", price=115.0),
        ])
        adapter = CbQuotationRefreshAdapter(store=store, fetch_records=fetch)

        result = adapter.refresh(_context())

        assert fetch.calls == [(_STARTED_AT_ISO,)]
        assert result.replaced == 2
        assert result.metadata["run_id"] == "run-test"
        rows = _query(market_db_path, "SELECT ts_code FROM cb_quotation ORDER BY ts_code")
        assert rows == [("113002",), ("113003",)]

    def test_empty_snapshot_raises_and_keeps_old(self, market_db_path, store):
        _execute(
            market_db_path,
            "INSERT INTO cb_quotation (ts_code, bond_name, price, data_source, updated_at)"
            " VALUES ('113001', '转债', 99.0, 'akshare', '2026-07-24T16:30:00+08:00')",
        )
        adapter = CbQuotationRefreshAdapter(store=store, fetch_records=FakeFetcher([]))

        with pytest.raises(RefreshValidationError):
            adapter.refresh(_context())

        assert _query(market_db_path, "SELECT COUNT(*) FROM cb_quotation") == [(1,)]

    def test_truncated_snapshot_raises_and_keeps_old(self, market_db_path, store):
        """残缺快照（覆盖率 < 0.8）→ 上抛，旧快照整表保留。"""
        for suffix in range(5):
            _seed_cb_quotation(market_db_path, f"11300{suffix}")
        fetch = FakeFetcher([
            _cb_quotation_record("113001"),
            _cb_quotation_record("113002"),
        ])
        adapter = CbQuotationRefreshAdapter(store=store, fetch_records=fetch)

        with pytest.raises(RefreshValidationError):
            adapter.refresh(_context())

        assert _query(market_db_path, "SELECT COUNT(*) FROM cb_quotation") == [(5,)]

    def test_snapshot_at_coverage_floor_replaces(self, market_db_path, store):
        """覆盖率恰好 0.8（4/5）→ 正常整表替换。"""
        for suffix in range(5):
            _seed_cb_quotation(market_db_path, f"11300{suffix}")
        fetch = FakeFetcher([
            _cb_quotation_record(f"11310{suffix}") for suffix in range(4)
        ])
        adapter = CbQuotationRefreshAdapter(store=store, fetch_records=fetch)

        result = adapter.refresh(_context())

        assert result.replaced == 4
        assert _query(market_db_path, "SELECT COUNT(*) FROM cb_quotation") == [(4,)]


class TestCbRedeemRefreshAdapter:
    def test_replaces_entire_snapshot(self, market_db_path, store):
        _execute(
            market_db_path,
            "INSERT INTO cb_redeem (ts_code, bond_name, redeem_flag, data_source, updated_at)"
            " VALUES ('113001', '旧转债', '已解除', 'akshare', '2026-07-24T16:30:00+08:00')",
        )
        fetch = FakeFetcher([_cb_redeem_record("113002")])
        adapter = CbRedeemRefreshAdapter(store=store, fetch_records=fetch)

        result = adapter.refresh(_context())

        assert fetch.calls == [(_STARTED_AT_ISO,)]
        assert result.replaced == 1
        assert result.metadata["run_id"] == "run-test"
        assert _query(market_db_path, "SELECT ts_code FROM cb_redeem") == [("113002",)]

    def test_truncated_snapshot_raises_and_keeps_old(self, market_db_path, store):
        """残缺快照（覆盖率 < 0.8）→ 上抛，旧快照整表保留。"""
        for suffix in range(5):
            _seed_cb_redeem(market_db_path, f"11300{suffix}")
        adapter = CbRedeemRefreshAdapter(
            store=store, fetch_records=FakeFetcher([_cb_redeem_record("113001")])
        )

        with pytest.raises(RefreshValidationError):
            adapter.refresh(_context())

        assert _query(market_db_path, "SELECT COUNT(*) FROM cb_redeem") == [(5,)]

    def test_snapshot_at_coverage_floor_replaces(self, market_db_path, store):
        """覆盖率恰好 0.8（4/5）→ 正常整表替换。"""
        for suffix in range(5):
            _seed_cb_redeem(market_db_path, f"11300{suffix}")
        fetch = FakeFetcher([
            _cb_redeem_record(f"11310{suffix}") for suffix in range(4)
        ])
        adapter = CbRedeemRefreshAdapter(store=store, fetch_records=fetch)

        result = adapter.refresh(_context())

        assert result.replaced == 4
        assert _query(market_db_path, "SELECT COUNT(*) FROM cb_redeem") == [(4,)]


class TestStockRepurchaseRefreshAdapter:
    def test_upserts_by_key_and_keeps_history(self, market_db_path, store):
        """键控 upsert + run_id metadata：快照外旧回购事件保留。"""
        _execute(
            market_db_path,
            "INSERT INTO stock_repurchase (trade_date, stock_code, stock_name, source_record_key)"
            " VALUES ('2026-06-01', '000009', '旧股', 'rep-000009-2026-06-01')",
        )
        fetch = FakeFetcher([
            _repurchase_record("000001"),
            _repurchase_record("000002", trade_date="2026-07-24"),
        ])
        adapter = StockRepurchaseRefreshAdapter(store=store, fetch_records=fetch)

        result = adapter.refresh(_context())

        assert fetch.calls == [()]
        assert result.replaced == 2
        assert result.metadata["run_id"] == "run-test"
        assert _query(market_db_path, "SELECT COUNT(*) FROM stock_repurchase") == [(3,)]

    def test_empty_snapshot_raises(self, market_db_path, store):
        adapter = StockRepurchaseRefreshAdapter(store=store, fetch_records=FakeFetcher([]))

        with pytest.raises(RefreshValidationError):
            adapter.refresh(_context())


# ===========================================================================
# 组6：死源（north_flow）
# ===========================================================================


class TestNorthFlowRefreshAdapter:
    def test_reports_dead_source_and_preserves_rows(self, market_db_path, store):
        """死源：不抓取、零替换，保留旧行并携 dead_source 元数据。"""
        _execute(
            market_db_path,
            "INSERT INTO north_flow (trade_date, market, net_buy_amount, data_source)"
            " VALUES ('2026-07-24', '北向', 10.0, 'akshare')",
        )
        _execute(
            market_db_path,
            "INSERT INTO north_flow (trade_date, market, net_buy_amount, data_source)"
            " VALUES ('2026-07-25', '北向', -5.0, 'akshare')",
        )
        adapter = NorthFlowRefreshAdapter(store=store, db_path=market_db_path)

        result = adapter.refresh(_context())

        assert result.as_of_date is None
        assert (result.fetched, result.validated, result.replaced) == (0, 0, 0)
        assert result.retained == 2
        assert result.failed_symbols == ()
        assert result.metadata["source_status"] == "dead_source"
        assert str(result.metadata["reason"]).strip()
        assert _query(market_db_path, "SELECT COUNT(*) FROM north_flow") == [(2,)]

    def test_empty_table_reports_zero_retained(self, market_db_path, store):
        """旧表为空时如实报 retained=0，不伪造保留量。"""
        adapter = NorthFlowRefreshAdapter(store=store, db_path=market_db_path)

        result = adapter.refresh(_context())

        assert result.retained == 0
        assert result.metadata["source_status"] == "dead_source"

    def test_empty_table_attests_baseline_and_passes_audit(self, market_db_path, store):
        """空基线携 baseline_empty=True 声明，并通过编排器的审计路径。"""
        adapter = NorthFlowRefreshAdapter(store=store, db_path=market_db_path)

        result = adapter.refresh(_context())

        assert result.retained == 0
        assert result.metadata["baseline_empty"] is True
        spec = next(
            spec
            for spec in refreshable_trading_tasks()
            if spec.name == "update_north_flow"
        )
        report = RefreshAudit().validate_task(spec, _context(), result)
        assert report.dead_source is True
        assert report.degraded is True


# ===========================================================================
# 组7：复合行业衍生（sector_daily / sector_valuation / index_futures_basis）
# ===========================================================================


def _sector_daily_record(sector: str, trade_date: str = TARGET) -> dict:
    return {
        "sector_name": sector,
        "trade_date": trade_date,
        "open": 100.0,
        "close": 101.5,
        "high": 102.0,
        "low": 99.0,
        "volume": 1e8,
        "amount": 1e9,
        "pct_change": 1.5,
        "data_source": "akshare",
    }


def _sector_valuation_record(sector: str, trade_date: str = TARGET) -> dict:
    return {
        "sector_name": sector,
        "trade_date": trade_date,
        "pe": 20.0,
        "pb": 2.0,
        "total_mv": 1e12,
        "data_source": "akshare",
    }


def _basis_record(code: str = "IF0", trade_date: str = TARGET) -> dict:
    return {
        "trade_date": trade_date,
        "futures_code": code,
        "futures_price": 3900.0,
        "index_price": 3880.0,
        "basis": 20.0,
        "basis_pct": 0.5155,
        "data_source": "akshare",
    }


class TestSectorDerivativesRefreshAdapter:
    def test_replaces_three_target_partitions_in_one_transaction(self, market_db_path, store):
        """三表目标日分区一次复合事务替换；非目标日记录被过滤。"""
        _execute(
            market_db_path,
            "INSERT INTO sector_daily (sector_name, trade_date, close, data_source)"
            " VALUES ('盘中残留', ?, 1.0, 'akshare')",
            (TARGET,),
        )
        _execute(
            market_db_path,
            "INSERT INTO sector_daily (sector_name, trade_date, close, data_source)"
            " VALUES ('银行', '2026-07-24', 99.0, 'akshare')",
        )
        _execute(
            market_db_path,
            "INSERT INTO sector_valuation (sector_name, trade_date, pe, data_source)"
            " VALUES ('盘中残留', ?, 1.0, 'akshare')",
            (TARGET,),
        )
        _execute(
            market_db_path,
            "INSERT INTO index_futures_basis (trade_date, futures_code, basis, data_source)"
            " VALUES (?, 'IC0', 1.0, 'akshare')",
            (TARGET,),
        )
        fetch_daily = FakeFetcher([
            _sector_daily_record("电子"),
            _sector_daily_record("银行"),
        ])
        fetch_valuation = FakeFetcher([
            _sector_valuation_record("银行"),
            _sector_valuation_record("旧日分区", trade_date="2026-07-24"),
        ])
        fetch_basis = FakeFetcher([_basis_record("IF0")])
        adapter = SectorDerivativesRefreshAdapter(
            store=store,
            fetch_daily=fetch_daily,
            fetch_valuation=fetch_valuation,
            fetch_basis=fetch_basis,
        )

        result = adapter.refresh(_context())

        assert fetch_daily.calls == [(TARGET,)]
        assert fetch_valuation.calls == [(TARGET,)]
        assert fetch_basis.calls == [(TARGET,)]
        assert result.as_of_date == TARGET
        assert result.replaced == 4
        rows = _query(
            market_db_path,
            "SELECT sector_name, trade_date FROM sector_daily ORDER BY trade_date, sector_name",
        )
        assert rows == [
            ("银行", "2026-07-24"),
            ("电子", TARGET),
            ("银行", TARGET),
        ]
        assert _query(
            market_db_path,
            "SELECT sector_name FROM sector_valuation WHERE trade_date = ?",
            (TARGET,),
        ) == [("银行",)]
        assert _query(
            market_db_path,
            "SELECT futures_code FROM index_futures_basis WHERE trade_date = ?",
            (TARGET,),
        ) == [("IF0",)]

    def test_any_empty_component_raises_and_keeps_all_tables(self, market_db_path, store):
        """任一分量目标日无数据 → 上抛，三表全部保留旧行（all-or-nothing）。"""
        _execute(
            market_db_path,
            "INSERT INTO sector_daily (sector_name, trade_date, close, data_source)"
            " VALUES ('银行', ?, 99.0, 'akshare')",
            (TARGET,),
        )
        _execute(
            market_db_path,
            "INSERT INTO sector_valuation (sector_name, trade_date, pe, data_source)"
            " VALUES ('银行', ?, 20.0, 'akshare')",
            (TARGET,),
        )
        _execute(
            market_db_path,
            "INSERT INTO index_futures_basis (trade_date, futures_code, basis, data_source)"
            " VALUES (?, 'IC0', 1.0, 'akshare')",
            (TARGET,),
        )
        adapter = SectorDerivativesRefreshAdapter(
            store=store,
            fetch_daily=FakeFetcher([_sector_daily_record("电子")]),
            fetch_valuation=FakeFetcher([_sector_valuation_record("银行")]),
            fetch_basis=FakeFetcher([]),
        )

        with pytest.raises(RefreshValidationError):
            adapter.refresh(_context())

        assert _query(market_db_path, "SELECT COUNT(*) FROM sector_daily") == [(1,)]
        assert _query(market_db_path, "SELECT COUNT(*) FROM sector_valuation") == [(1,)]
        assert _query(market_db_path, "SELECT COUNT(*) FROM index_futures_basis") == [(1,)]
