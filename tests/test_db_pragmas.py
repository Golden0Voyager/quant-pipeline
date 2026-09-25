"""``core/db_pragmas`` 的门禁与行为测试

背景（实测，2026-09-25）：生产库 ``quant_core.db-wal`` 曾长期占着 **2.39 GiB**，而同一时刻
它的活跃日志只有 22,350 帧（87 MB）—— 文件里 96% 是活跃日志之外的死区。成因是 WAL 文件为
只涨不缩的高水位线：单个大事务把它撑到事务大小，之后即使 checkpoint 早已完成、帧被复用，
文件也不会缩小。

本文件把这条性质与修复钉死：

* **行为**：``journal_size_limit`` 会在「超大 WAL 代被 reset 后的第一次写入」把文件缩回上限；
  ``truncate_wal`` 立即截到 0，且在**别的连接开着**时同样生效（TUI 常驻就是这种形态）。
* **门禁**：``PRAGMA journal_mode`` 全仓只准出现在 ``core/db_pragmas.py`` —— 上限不是持久设置
  （新连接读回 ``-1``），所以「每个写连接都要设」，散在 7 处迟早漏一处。

红证（还原旧实现各让哪些用例变红）：

* 去掉 ``PRAGMA journal_size_limit`` → ``test_oversized_transaction_leaves_a_wal_bounded_by_the_limit``
* 去掉 ``daily_pipeline`` 收尾的 ``truncate_wal`` → ``test_entry_point_reclaims_the_wal_at_exit``
* 把任一站点改回内联 ``PRAGMA journal_mode=WAL`` → ``test_wal_is_enabled_in_exactly_one_place``
  与对应的 ``test_every_write_connection_site_calls_the_single_helper``
* 写连接不设上限（如 ``providers._get_write_conn`` 退回旧写法）→
  ``test_provider_shared_write_connection_declares_the_limit``（旧实现读回 ``-1``）
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from core.db_pragmas import (
    DEFAULT_WAL_SIZE_LIMIT_BYTES,
    WAL_SIZE_LIMIT_ENV,
    apply_write_pragmas,
    truncate_wal,
    wal_bytes,
    wal_size_limit_bytes,
)

MIB = 1024 * 1024
_ROW = 4096
# 3000 行 × 4 KiB ≈ 12 MiB 的单个事务：够大到撑出高水位，又小到测试只需一两秒。
_GIANT_ROWS = 3000
_LIMIT = 4 * MIB

_ROOT = Path(__file__).resolve().parent.parent

# 生产面（与 mypy 的 files 列表同源，但排除 tests/：测试里自建 WAL 库是合法的）。
_PRODUCTION_SOURCES = ("core", "tasks", "tui", "scripts", "daily_pipeline.py", "providers.py", "interface.py")
_WAL_ALLOWED = "core/db_pragmas.py"
# 改动前自己开 WAL 的站点：全部必须改为调用唯一出处。
_WAL_WRITER_FILES = (
    "providers.py",
    "core/migrations.py",
    "scripts/repair_turnover.py",
    "scripts/backfill_historical_valuation.py",
    "scripts/reconcile_with_akshare.py",
)


def _make_db(path: Path, *, limit: int = _LIMIT) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    apply_write_pragmas(conn, wal_size_limit=limit)
    conn.execute("PRAGMA wal_autocheckpoint=1000")
    conn.execute("CREATE TABLE big(x BLOB)")
    conn.commit()
    return conn


def _bulk_write(conn: sqlite3.Connection, rows: int, blob: bytes) -> None:
    conn.executemany("INSERT INTO big VALUES (?)", [(blob,)] * rows)
    conn.commit()


def _production_python_files() -> list[Path]:
    found: list[Path] = []
    for entry in _PRODUCTION_SOURCES:
        target = _ROOT / entry
        if target.is_file():
            found.append(target)
        elif target.is_dir():
            found.extend(sorted(target.rglob("*.py")))
    return found


# ── 上限的解析 ──────────────────────────────────────────────────────────


def test_limit_defaults_to_64_mib(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(WAL_SIZE_LIMIT_ENV, raising=False)
    assert DEFAULT_WAL_SIZE_LIMIT_BYTES == 64 * MIB
    assert wal_size_limit_bytes() == DEFAULT_WAL_SIZE_LIMIT_BYTES


def test_limit_honours_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(WAL_SIZE_LIMIT_ENV, "32")
    assert wal_size_limit_bytes() == 32 * MIB


@pytest.mark.parametrize("raw", ["", "abc", "16.5", "-1"])
def test_invalid_limit_falls_back_to_the_default(monkeypatch: pytest.MonkeyPatch, raw: str) -> None:
    """非法值回落默认并告警，而不是抛异常或静默变成「无上限」。"""
    monkeypatch.setenv(WAL_SIZE_LIMIT_ENV, raw)
    assert wal_size_limit_bytes() == DEFAULT_WAL_SIZE_LIMIT_BYTES


# ── apply_write_pragmas ────────────────────────────────────────────────


def test_apply_write_pragmas_enables_wal_and_declares_the_limit(tmp_path: Path) -> None:
    conn = sqlite3.connect(tmp_path / "a.db")
    try:
        assert apply_write_pragmas(conn, wal_size_limit=8 * MIB) == "wal"
        assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert conn.execute("PRAGMA journal_size_limit").fetchone()[0] == 8 * MIB
    finally:
        conn.close()


def test_apply_write_pragmas_passes_through_optional_pragmas(tmp_path: Path) -> None:
    conn = sqlite3.connect(tmp_path / "b.db")
    try:
        apply_write_pragmas(conn, busy_timeout_ms=1234, foreign_keys=True, wal_size_limit=MIB)
        assert conn.execute("PRAGMA busy_timeout").fetchone()[0] == 1234
        assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1
    finally:
        conn.close()


def test_apply_write_pragmas_leaves_unrequested_pragmas_alone(tmp_path: Path) -> None:
    """``synchronous=None`` 表示「不动」：脚本接入本函数不得悄悄改变原有 durability 行为。"""
    conn = sqlite3.connect(tmp_path / "c.db")
    try:
        apply_write_pragmas(conn, synchronous=None, wal_size_limit=MIB)
        assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 0
        assert conn.execute("PRAGMA journal_size_limit").fetchone()[0] == MIB
    finally:
        conn.close()


def test_limit_is_per_connection_and_not_persisted(tmp_path: Path) -> None:
    """这就是「必须每个写连接都设」的原因：模式持久，上限不持久。"""
    db = tmp_path / "d.db"
    conn = _make_db(db)
    conn.close()

    fresh = sqlite3.connect(db)
    try:
        assert fresh.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert fresh.execute("PRAGMA journal_size_limit").fetchone()[0] == -1
    finally:
        fresh.close()


# ── 上限的实际效果 ──────────────────────────────────────────────────────


def test_oversized_transaction_leaves_a_wal_bounded_by_the_limit(tmp_path: Path) -> None:
    """单个大事务把 WAL 撑到事务大小；上限在**下一次写入**把它缩回去。"""
    db = tmp_path / "e.db"
    conn = _make_db(db)

    _bulk_write(conn, _GIANT_ROWS, b"z" * _ROW)
    peak = wal_bytes(db)
    assert peak > 2 * _LIMIT, f"事务没撑出高水位，测试前提不成立：{peak} 字节"

    # 接下来的正常小写入让 WAL reset，上限在此生效
    _bulk_write(conn, 1, b"x")
    assert wal_bytes(db) <= _LIMIT
    conn.close()


def test_without_a_limit_the_oversized_wal_stays_forever(tmp_path: Path) -> None:
    """对照组：证明上一条断言来自上限本身，而不是 SQLite 自己会缩（生产库就是这种形态）。"""
    db = tmp_path / "f.db"
    conn = _make_db(db, limit=-1)

    _bulk_write(conn, _GIANT_ROWS, b"z" * _ROW)
    peak = wal_bytes(db)
    _bulk_write(conn, 1, b"x")
    assert wal_bytes(db) == peak > 2 * _LIMIT
    conn.close()


# ── truncate_wal ───────────────────────────────────────────────────────


def test_truncate_wal_reclaims_the_space(tmp_path: Path) -> None:
    db = tmp_path / "g.db"
    conn = _make_db(db)
    _bulk_write(conn, 2000, b"z" * _ROW)
    before = wal_bytes(db)
    assert before > 0

    result = truncate_wal(db)
    assert result["ok"] is True
    assert result["busy"] is False
    assert result["before_bytes"] == before
    assert result["after_bytes"] == 0
    assert result["reclaimed_bytes"] == before
    assert wal_bytes(db) == 0
    conn.close()


def test_truncate_wal_works_while_another_connection_is_open(tmp_path: Path) -> None:
    """TUI 常驻就是这种形态：有别的连接开着时，上限生效时点不确定，回收必须仍然有效。"""
    db = tmp_path / "h.db"
    writer = _make_db(db)
    reader = sqlite3.connect(db)
    try:
        reader.execute("PRAGMA journal_mode=WAL")
        reader.execute("SELECT count(*) FROM sqlite_master").fetchone()
        _bulk_write(writer, 2000, b"z" * _ROW)

        result = truncate_wal(db)
        assert result["ok"] is True, result
        assert wal_bytes(db) == 0
    finally:
        reader.close()
        writer.close()


def test_truncate_wal_reports_conflict_instead_of_raising(tmp_path: Path) -> None:
    """库被写事务占住时必须返回结果而不是抛异常 —— 调用点在收尾，不该改变退出码。"""
    db = tmp_path / "i.db"
    holder = _make_db(db)
    _bulk_write(holder, 2000, b"z" * _ROW)

    blocker = sqlite3.connect(db, timeout=0)
    blocker.execute("BEGIN IMMEDIATE")
    blocker.execute("INSERT INTO big VALUES (?)", (b"y",))
    try:
        result = truncate_wal(db, timeout=0.2)
        assert result["ok"] is False
        assert result["error"]
        assert result["after_bytes"] == result["before_bytes"]
    finally:
        blocker.rollback()
        blocker.close()

    assert truncate_wal(db)["ok"] is True
    holder.close()


def test_truncate_wal_is_a_noop_without_a_wal_file(tmp_path: Path) -> None:
    result = truncate_wal(tmp_path / "empty.db")
    assert result["ok"] is True
    assert result["before_bytes"] == result["after_bytes"] == 0


def test_truncate_wal_never_raises_on_an_unusable_path(tmp_path: Path) -> None:
    result = truncate_wal(tmp_path / "no-such-dir" / "x.db")
    assert result["ok"] is False
    assert result["error"]


# ── 门禁：唯一出处与接线 ────────────────────────────────────────────────


def test_wal_is_enabled_in_exactly_one_place() -> None:
    """``PRAGMA journal_mode`` 全仓只准出现在 ``core/db_pragmas.py``。

    这不是洁癖：上限不持久，散在 7 处时「漏一处」就等于**没有上限**，而漏掉的那一处
    照样正常工作（WAL 生效、读写正常），只是悄悄把磁盘吃光——正是 2.39 GiB 那一次的形态。
    """
    offenders = []
    for path in _production_python_files():
        rel = path.relative_to(_ROOT).as_posix()
        if rel == _WAL_ALLOWED:
            continue
        if "pragma journal_mode" in path.read_text(encoding="utf-8").lower():
            offenders.append(rel)
    assert offenders == [], f"这些文件绕过 core.db_pragmas 自开 WAL：{offenders}"


@pytest.mark.parametrize("rel", _WAL_WRITER_FILES)
def test_every_write_connection_site_calls_the_single_helper(rel: str) -> None:
    """逐个钉住接线：删掉某处的 ``apply_write_pragmas`` 调用也必须变红。"""
    text = (_ROOT / rel).read_text(encoding="utf-8")
    assert "apply_write_pragmas(" in text


def test_entry_point_reclaims_the_wal_at_exit() -> None:
    """``daily_pipeline`` 收尾必须真的回收：上限要等到「下一次写入」才生效，而批量回填的
    典型形态就是「大事务之后本轮就结束了」—— 那一次写入可能永远不会来。"""
    text = (_ROOT / "daily_pipeline.py").read_text(encoding="utf-8")
    assert "truncate_wal(" in text


def test_provider_shared_write_connection_declares_the_limit(tmp_path: Path) -> None:
    """真实写路径（每个 batch 写入用到的共享写连接）必须带上上限。"""
    from providers import SmartMoneyDBProvider

    db_path = tmp_path / "quant_core_test.db"
    instance = SmartMoneyDBProvider(db_path=str(db_path))
    # conftest 会 mock 掉 DatabaseManager，这里把 fixture 的库路径绑回自己的临时文件
    instance._db.db_path = str(db_path)

    conn = instance._get_write_conn()
    assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    assert conn.execute("PRAGMA journal_size_limit").fetchone()[0] == DEFAULT_WAL_SIZE_LIMIT_BYTES
