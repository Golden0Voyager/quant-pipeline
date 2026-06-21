"""Tests for validate_and_vacuum.py."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from validate_and_vacuum import do_vacuum, fmt_num, run_checks


@pytest.fixture
def full_schema_daily_bars(daily_bars_schema: str) -> str:
    return daily_bars_schema


@pytest.fixture
def daily_bars_schema() -> str:
    return """
        CREATE TABLE daily_bars (
            ts_code TEXT,
            trade_date TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            amount REAL,
            turnover_rate REAL,
            pct_change REAL,
            amplitude REAL
        )
    """


def test_fmt_num():
    assert fmt_num(0) == "0"
    assert fmt_num(1000) == "1,000"
    assert fmt_num(1234567) == "1,234,567"
    assert fmt_num(-500) == "-500"


def test_run_checks_empty_db(tmp_path: Path, daily_bars_schema: str):
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE stock_list (ts_code TEXT)")
    conn.execute(daily_bars_schema)
    conn.close()

    results = run_checks(db_path)

    assert "passed" in results
    assert "failed" in results
    assert "warnings" in results
    assert "details" in results
    assert isinstance(results["details"], list)


def test_run_checks_full_data(tmp_path: Path, daily_bars_schema: str):
    db_path = tmp_path / "full.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(daily_bars_schema)
    conn.execute("CREATE TABLE stock_list (ts_code TEXT)")
    conn.execute("INSERT INTO stock_list (ts_code) VALUES ('000001.SZ')")
    conn.execute("INSERT INTO stock_list (ts_code) VALUES ('600000.SH')")
    conn.execute("""
        INSERT INTO daily_bars (ts_code, trade_date, open, high, low, close,
                                volume, amount, turnover_rate, pct_change, amplitude)
        VALUES ('000001.SZ', '2024-01-02', 10.0, 11.0, 9.5, 10.5,
                1000000, 10500000, 0.5, 2.0, 1.5)
    """)
    conn.execute("""
        INSERT INTO daily_bars (ts_code, trade_date, open, high, low, close,
                                volume, amount, turnover_rate, pct_change, amplitude)
        VALUES ('000001.SZ', '2024-01-03', 10.5, 11.5, 10.0, 11.0,
                1200000, 13200000, 0.6, 3.0, 2.0)
    """)
    conn.execute("""
        INSERT INTO daily_bars (ts_code, trade_date, open, high, low, close,
                                volume, amount, turnover_rate, pct_change, amplitude)
        VALUES ('600000.SH', '2024-01-02', 8.0, 8.5, 7.8, 8.2,
                800000, 6560000, 0.3, 1.5, 1.0)
    """)
    conn.commit()
    conn.close()

    results = run_checks(db_path)
    assert results["failed"] == 0
    assert results["passed"] >= 5


def test_do_vacuum(tmp_path: Path):
    db_path = tmp_path / "vacuum_test.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)")
    for i in range(100):
        conn.execute("INSERT INTO test (val) VALUES (?)", (f"data_{i}",))
    conn.commit()
    conn.execute("DELETE FROM test WHERE id <= 50")
    conn.commit()
    conn.close()

    do_vacuum(db_path)


def test_main_with_nonexistent_db():
    from validate_and_vacuum import main

    with patch.object(sys, "argv", ["validate_and_vacuum.py", "--db", "/nonexistent/test.db"]):
        with pytest.raises(SystemExit) as exc:
            main()
        assert exc.value.code == 1


def test_main_with_vacuum_and_failure(tmp_path: Path, daily_bars_schema: str):
    from validate_and_vacuum import main

    db_path = tmp_path / "broken.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE stock_list (ts_code TEXT)")
    conn.execute(daily_bars_schema)
    conn.close()

    with patch.object(sys, "argv", [
        "validate_and_vacuum.py", "--db", str(db_path), "--vacuum"
    ]):
        with pytest.raises(SystemExit) as exc:
            main()
        assert exc.value.code == 1


def test_main_with_vacuum_success(tmp_path: Path, daily_bars_schema: str):
    from validate_and_vacuum import main

    db_path = tmp_path / "clean.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE stock_list (ts_code TEXT)")
    conn.execute("INSERT INTO stock_list (ts_code) VALUES ('000001.SZ')")
    conn.execute(daily_bars_schema)
    conn.execute("""
        INSERT INTO daily_bars (ts_code, trade_date, open, high, low, close,
                                volume, amount, turnover_rate, pct_change, amplitude)
        VALUES ('000001.SZ', '2024-01-02', 10.0, 11.0, 9.5, 10.5,
                1000000, 10500000, 0.5, 2.0, 1.5)
    """)
    conn.commit()
    conn.close()

    with patch.object(sys, "argv", [
        "validate_and_vacuum.py", "--db", str(db_path), "--vacuum"
    ]):
        main()
