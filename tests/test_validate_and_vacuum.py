"""scripts/validate_and_vacuum.py 单元测试。"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from scripts import validate_and_vacuum as vv


def _make_minimal_db(path: Path) -> None:
    """建一份能通过表存在性检查的极简库。"""
    conn = sqlite3.connect(str(path))
    conn.execute(
        "CREATE TABLE daily_bars ("
        "  ts_code TEXT, trade_date TEXT, open REAL, high REAL, low REAL, close REAL,"
        "  volume REAL, amount REAL, turnover_rate REAL, pct_change REAL, amplitude REAL)"
    )
    conn.execute("CREATE TABLE stock_list (code TEXT)")
    conn.execute(
        "INSERT INTO daily_bars VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("000001", "2024-01-02", 10.0, 11.0, 9.0, 10.5, 100000, 1e6, 1.5, 1.0, 5.0),
    )
    conn.execute("INSERT INTO stock_list VALUES ('000001')")
    conn.commit()
    conn.close()


def test_run_checks_on_minimal_db(tmp_path: Path):
    """冒烟：run_checks 在合法小库上完整跑通并归还结果结构。"""
    db = tmp_path / "mini.db"
    _make_minimal_db(db)
    results = vv.run_checks(db)
    assert results["passed"] > 0
    assert results["details"]


def test_do_vacuum_zero_byte_file_no_crash(tmp_path: Path, capsys):
    """red-proof: 零字节 SQLite 文件（size_before=0）不得 ZeroDivisionError。

    还原 scripts/validate_and_vacuum.py 的除零守卫会让本用例变红：
    旧实现 `saved/size_before*100` 直接抛 ZeroDivisionError。
    """
    db = tmp_path / "empty.db"
    db.write_bytes(b"")
    vv.do_vacuum(db)
    assert "0.0%" in capsys.readouterr().out
