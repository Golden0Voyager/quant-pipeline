"""Tests for the WAL-safe SQLite backup gate."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime
from pathlib import Path

import pytest

import scripts.backup_database as backup_module
from scripts.backup_database import backup_database


class FrozenDateTime(datetime):
    """Return a stable timestamp for collision tests."""

    @classmethod
    def now(cls, tz=None):
        return cls(2026, 7, 23, 12, 34, 56, tzinfo=tz)


def _create_database(path: Path) -> None:
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE records (value TEXT)")
        connection.execute("INSERT INTO records VALUES ('ready')")


def test_backup_database_copies_live_wal_and_reports_integrity(tmp_path: Path):
    source = tmp_path / "quant_core.db"
    destination_dir = tmp_path / "backups"
    writer = sqlite3.connect(source)

    try:
        journal_mode = writer.execute("PRAGMA journal_mode=WAL").fetchone()[0]
        writer.execute(
            "CREATE TABLE prices (id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT)"
        )
        writer.execute("CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT)")
        writer.executemany(
            "INSERT INTO prices (symbol) VALUES (?)",
            [("000001.SZ",), ("600000.SH",)],
        )
        writer.execute("INSERT INTO metadata VALUES ('trade_date', '2026-07-23')")
        writer.commit()

        assert journal_mode == "wal"
        assert Path(f"{source}-wal").exists()

        report = backup_database(source, destination_dir)
    finally:
        writer.close()

    assert report.source_rows == report.backup_rows
    assert report.quick_check == "ok"
    assert report.sha256
    assert report.backup_path.name.startswith("quant_core_")
    assert report.source_rows == 3
    assert report.backup_path.parent == destination_dir
    assert report.sha256 == hashlib.sha256(report.backup_path.read_bytes()).hexdigest()

    with sqlite3.connect(report.backup_path) as backup:
        assert backup.execute("SELECT symbol FROM prices ORDER BY id").fetchall() == [
            ("000001.SZ",),
            ("600000.SH",),
        ]


def test_backup_database_rejects_missing_source(tmp_path: Path):
    source = tmp_path / "missing.db"

    with pytest.raises(FileNotFoundError, match="source database does not exist"):
        backup_database(source, tmp_path / "backups")


def test_backup_database_refuses_existing_timestamp_target(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    source = tmp_path / "quant_core.db"
    destination_dir = tmp_path / "backups"
    target = destination_dir / "quant_core_20260723_123456.db"
    _create_database(source)
    destination_dir.mkdir()
    target.write_bytes(b"existing backup")
    monkeypatch.setattr(backup_module, "datetime", FrozenDateTime)

    with pytest.raises(FileExistsError, match="backup target already exists"):
        backup_database(source, destination_dir)

    assert target.read_bytes() == b"existing backup"


def test_main_prints_machine_readable_report(
    tmp_path: Path, capsys: pytest.CaptureFixture
):
    source = tmp_path / "quant_core.db"
    destination_dir = tmp_path / "backups"
    _create_database(source)

    exit_code = backup_module.main(
        ["--db", str(source), "--output-dir", str(destination_dir)]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["backup_path"].startswith(str(destination_dir))
    assert payload["source_rows"] == payload["backup_rows"] == 1
    assert payload["quick_check"] == "ok"
    assert payload["sha256"]


def test_main_without_args_uses_env_defaults(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
):
    """monthly_repair 以「无参数」方式调用时，脚本不应因 argparse 必填项报错。

    用环境变量把默认值指向临时库/目录，避免触碰生产库（~/Code/quant_data/quant_core.db）。
    """
    source = tmp_path / "quant_core.db"
    _create_database(source)
    destination_dir = tmp_path / "backups"
    monkeypatch.setenv("QUANT_DB_PATH", str(source))
    monkeypatch.setenv("QUANT_BACKUP_DIR", str(destination_dir))

    exit_code = backup_module.main([])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["source_rows"] == payload["backup_rows"] == 1
    assert payload["quick_check"] == "ok"
    assert payload["sha256"]
