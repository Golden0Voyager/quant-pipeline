"""Behavior tests for the read-only Xueqiu cross-source verifier."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from typing import Any

import pandas as pd
import pytest

from core.refresh_audit import CROSS_SOURCE_BOARDS
from core.refresh_cross_source import XueqiuCrossSourceVerifier

_TARGET = "2026-07-27"


@dataclass
class FakeXueqiu:
    """可注入的 xq 模块替身：返回受控 DataFrame，绝不联网。"""

    frames: dict[str, pd.DataFrame]
    calls: list[dict[str, Any]] = field(default_factory=list)

    def get_daily_bars(
        self,
        symbol: str,
        start_date: str | None = None,
        end_date: str | None = None,
        adjust: str = "qfq",
    ) -> pd.DataFrame:
        self.calls.append(
            {
                "symbol": symbol,
                "start_date": start_date,
                "end_date": end_date,
                "adjust": adjust,
            }
        )
        return self.frames.get(symbol, pd.DataFrame())


def _bar_frame(dates: list[str], closes: list[float], volumes: list[float]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": pd.to_datetime(dates),
            "open": closes,
            "high": closes,
            "low": closes,
            "close": closes,
            "volume": volumes,
            "amount": [v * 10 for v in volumes],
        }
    )


@pytest.fixture()
def bars_db(tmp_path) -> str:
    """临时 daily_bars 库：两只目标日股票 + 一只旧分区股票。"""
    db_path = tmp_path / "bars.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE daily_bars ("
        " ts_code TEXT NOT NULL, trade_date TEXT NOT NULL,"
        " close REAL, volume REAL,"
        " UNIQUE(ts_code, trade_date))"
    )
    conn.executemany(
        "INSERT INTO daily_bars VALUES (?, ?, ?, ?)",
        [
            ("000001.SZ", _TARGET, 12.34, 250000.0),
            ("600000.SH", _TARGET, 8.5, 180000.0),
            ("300001.SZ", "2026-07-24", 6.6, 90000.0),
        ],
    )
    conn.commit()
    conn.close()
    return str(db_path)


def _verifier(
    bars_db: str,
    fake: FakeXueqiu | None = None,
    **kwargs: Any,
) -> XueqiuCrossSourceVerifier:
    return XueqiuCrossSourceVerifier(
        db_path=bars_db,
        xq_module=fake or FakeXueqiu(frames={}),
        throttle_seconds=kwargs.pop("throttle_seconds", 0.0),
        **kwargs,
    )


def test_source_name_and_supported_boards_exclude_beijing(bars_db) -> None:
    """雪球不覆盖北交所：必须显式排除 beijing，且板块名与采样框架一致。"""
    verifier = _verifier(bars_db)
    assert verifier.source_name == "xueqiu"
    assert verifier.supported_boards == frozenset(
        {"shanghai", "shenzhen", "chinext", "star"}
    )
    assert "beijing" not in verifier.supported_boards
    assert verifier.supported_boards <= set(CROSS_SOURCE_BOARDS)


def test_primary_quotes_reads_our_close_and_volume_for_target_date(bars_db) -> None:
    """primary_quotes 只读目标日分区；无行的股票不出现在结果中。"""
    verifier = _verifier(bars_db)
    quotes = verifier.primary_quotes(
        ("000001.SZ", "600000.SH", "300001.SZ", "688001.SH"), _TARGET
    )
    assert quotes == {
        "000001.SZ": {"close": 12.34, "volume": 250000.0},
        "600000.SH": {"close": 8.5, "volume": 180000.0},
    }


def test_primary_quotes_empty_symbols_returns_empty(bars_db) -> None:
    assert _verifier(bars_db).primary_quotes((), _TARGET) == {}


def test_reference_quotes_maps_symbols_and_normalizes_volume_to_lots(bars_db) -> None:
    """雪球按裸 6 位码请求 qfq 日线；成交量 50000 股必须归一为 500 手。"""
    fake = FakeXueqiu(
        frames={
            "000001": _bar_frame([_TARGET], [12.30], [50000.0]),
            "600000": _bar_frame([_TARGET], [8.52], [18000000.0]),
        }
    )
    verifier = _verifier(bars_db, fake)

    quotes = verifier.reference_quotes(("000001.SZ", "600000.SH"), _TARGET)

    assert quotes == {
        "000001.SZ": {"close": 12.30, "volume": 500.0},
        "600000.SH": {"close": 8.52, "volume": 180000.0},
    }
    assert [call["symbol"] for call in fake.calls] == ["000001", "600000"]
    for call in fake.calls:
        assert call["adjust"] == "qfq"
        assert call["start_date"] == _TARGET
        assert call["end_date"] == _TARGET


def test_reference_quotes_picks_row_matching_target_date(bars_db) -> None:
    """多行返回时只取归一化日期等于目标日的那一行。"""
    fake = FakeXueqiu(
        frames={
            "000001": _bar_frame(
                ["2026-07-24", _TARGET], [11.11, 12.30], [30000.0, 50000.0]
            ),
        }
    )
    quotes = _verifier(bars_db, fake).reference_quotes(("000001.SZ",), _TARGET)
    assert quotes == {"000001.SZ": {"close": 12.30, "volume": 500.0}}


def test_reference_quotes_skips_suspended_or_missing_symbols(bars_db) -> None:
    """空 DataFrame（停牌/无数据）或缺目标日行：跳过不发键，而非报出错误值。"""
    fake = FakeXueqiu(
        frames={
            "000001": _bar_frame([_TARGET], [12.30], [50000.0]),
            "300001": pd.DataFrame(),
            "688001": _bar_frame(["2026-07-24"], [99.9], [1000.0]),
        }
    )
    quotes = _verifier(bars_db, fake).reference_quotes(
        ("000001.SZ", "300001.SZ", "688001.SH"), _TARGET
    )
    assert set(quotes) == {"000001.SZ"}


def test_reference_quotes_throttles_between_calls(bars_db) -> None:
    """每两次雪球调用之间限速一次；throttle=0 时完全不 sleep。"""
    frames = {
        code: _bar_frame([_TARGET], [10.0], [10000.0])
        for code in ("000001", "600000", "688001")
    }
    symbols = ("000001.SZ", "600000.SH", "688001.SH")

    sleeps: list[float] = []
    verifier = XueqiuCrossSourceVerifier(
        db_path=bars_db,
        xq_module=FakeXueqiu(frames=dict(frames)),
        throttle_seconds=0.01,
        sleep=sleeps.append,
    )
    verifier.reference_quotes(symbols, _TARGET)
    assert sleeps == [0.01, 0.01]

    no_sleeps: list[float] = []
    quiet = XueqiuCrossSourceVerifier(
        db_path=bars_db,
        xq_module=FakeXueqiu(frames=dict(frames)),
        throttle_seconds=0.0,
        sleep=no_sleeps.append,
    )
    quiet.reference_quotes(symbols, _TARGET)
    assert no_sleeps == []


def test_negative_throttle_is_rejected(bars_db) -> None:
    with pytest.raises(ValueError, match="throttle"):
        XueqiuCrossSourceVerifier(
            db_path=bars_db,
            xq_module=FakeXueqiu(frames={}),
            throttle_seconds=-0.1,
        )
