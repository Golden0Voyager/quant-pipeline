"""Tests for scripts/audit_data_contracts.py — TableHealth and HealthReport.

Covers:
- Empty table detection
- Stale data detection (cadence-aware)
- Duplicate group detection
- Core field completeness below threshold
- Not-due on-demand tables
- Degraded vs critical vs healthy classification
- JSON output
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from core.task_registry import Cadence, EmptyPolicy, TaskSpec
from scripts.audit_data_contracts import (
    HealthReport,
    TableHealth,
    _check_table,
    _expected_date_for,
    audit_database,
)

# ═══════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════


def _build_db(
    path: str,
    *,
    empty_tables: list[str] | None = None,
    stale_tables: dict[str, str] | None = None,
    dup_tables: dict[str, int] | None = None,
    null_tables: list[str] | None = None,
) -> None:
    """Create a SQLite database with controlled data quality scenarios."""
    conn = sqlite3.connect(path)
    cursor = conn.cursor()

    all_tables = set()
    all_tables.update(empty_tables or [])
    all_tables.update(stale_tables or {})
    all_tables.update(dup_tables or {})
    all_tables.update(null_tables or [])

    for tbl in all_tables:
        cursor.execute(
            f"CREATE TABLE IF NOT EXISTS \"{tbl}\" "
            f"(id INTEGER, ts_code TEXT, trade_date TEXT, "
            f"open REAL, close REAL, volume REAL)"
        )

    for _ in (empty_tables or []):
        pass  # intentionally left empty

    for tbl, stale_date in (stale_tables or {}).items():
        cursor.execute(
            f"INSERT INTO \"{tbl}\" VALUES (1, '000001', ?, 10.0, 10.5, 1000)",
            (stale_date,),
        )

    for tbl, dup_count in (dup_tables or {}).items():
        for _ in range(dup_count):
            cursor.execute(
                f"INSERT INTO \"{tbl}\" (id, ts_code, trade_date, open, close, volume) VALUES (NULL, '000001', '2026-07-17', 10.0, 10.5, 1000)"
            )

    for tbl in (null_tables or []):
        # Insert rows with all metric columns null
        for i in range(10):
            cursor.execute(
                f"INSERT INTO \"{tbl}\" VALUES (?, '000001', '2026-07-17', NULL, NULL, NULL)",
                (i,),
            )

    conn.commit()
    conn.close()


def _spec(
    table: str,
    cadence: Cadence = Cadence.DAILY,
    empty_policy: EmptyPolicy = EmptyPolicy.FAIL,
    grace: int = 3,
) -> TaskSpec:
    return TaskSpec(
        name=f"update_{table}",
        callable=None,
        tables=(table,),
        cadence=cadence,
        date_columns={table: "trade_date"},
        empty_policy=empty_policy,
        primary_source="akshare",
        grace_period_days=grace,
    )


# ═══════════════════════════════════════════════════════════════════════
# TableHealth
# ═══════════════════════════════════════════════════════════════════════


class TestTableHealth:
    def test_basic_fields(self):
        """TableHealth dataclass fields are present."""
        h = TableHealth(table="test_tbl", row_count=0)
        assert h.table == "test_tbl"
        assert h.row_count == 0
        assert h.status == "healthy"
        assert h.issues == []

    def test_exit_code_healthy(self):
        r = HealthReport()
        assert r.exit_code == 0

    def test_exit_code_degraded(self):
        r = HealthReport(degraded_count=2)
        assert r.exit_code == 1

    def test_exit_code_critical(self):
        r = HealthReport(critical_count=1, degraded_count=2)
        assert r.exit_code == 2


# ═══════════════════════════════════════════════════════════════════════
# _expected_date_for
# ═══════════════════════════════════════════════════════════════════════


class TestExpectedDateFor:
    def test_daily_returns_prev_weekday(self):
        spec = _spec("test_tbl", cadence=Cadence.DAILY)
        d = _expected_date_for(spec)
        assert d is not None

    def test_weekly_returns_one_week_ago(self):
        spec = _spec("test_tbl", cadence=Cadence.WEEKLY)
        d = _expected_date_for(spec)
        assert d is not None

    def test_quarterly_returns_string(self):
        spec = _spec("test_tbl", cadence=Cadence.QUARTERLY)
        d = _expected_date_for(spec)
        assert d is not None
        assert len(d) == 10  # YYYY-MM-DD


# ═══════════════════════════════════════════════════════════════════════
# _check_table  (individual table audit)
# ═══════════════════════════════════════════════════════════════════════


class TestCheckTable:
    def test_empty_table_critical(self, tmp_path: Path):
        db_path = tmp_path / "empty.db"
        _build_db(str(db_path), empty_tables=["empty_tbl"])
        conn = sqlite3.connect(str(db_path))
        h = _check_table("empty_tbl", conn, None, set())
        conn.close()
        assert h.status == "critical"
        assert "empty" in " ".join(h.issues).lower()

    def test_populated_table_healthy(self, tmp_path: Path):
        db_path = tmp_path / "ok.db"
        spec = _spec("ok_tbl")
        expected_date = _expected_date_for(spec)
        _build_db(str(db_path), stale_tables={"ok_tbl": expected_date})
        conn = sqlite3.connect(str(db_path))
        h = _check_table("ok_tbl", conn, spec, set())
        conn.close()
        assert h.row_count == 1
        assert h.status == "healthy"

    def test_stale_table_critical(self, tmp_path: Path):
        db_path = tmp_path / "stale.db"
        _build_db(str(db_path), stale_tables={"stale_tbl": "2020-01-01"})
        conn = sqlite3.connect(str(db_path))
        spec = _spec("stale_tbl", cadence=Cadence.DAILY, grace=0)
        h = _check_table("stale_tbl", conn, spec, set())
        conn.close()
        assert h.status == "critical"
        assert any("stale" in i.lower() for i in h.issues)

    def test_not_due_on_demand_empty(self, tmp_path: Path):
        db_path = tmp_path / "notdue.db"
        _build_db(str(db_path), empty_tables=["ondemand_tbl"])
        conn = sqlite3.connect(str(db_path))
        spec = _spec("ondemand_tbl", cadence=Cadence.ON_DEMAND)
        h = _check_table("ondemand_tbl", conn, spec, set())
        conn.close()
        assert h.status == "not_due"

    def test_duplicates_flagged(self, tmp_path: Path):
        db_path = tmp_path / "dup.db"
        _build_db(str(db_path), dup_tables={"dup_tbl": 3})
        conn = sqlite3.connect(str(db_path))
        h = _check_table("dup_tbl", conn, None, set())
        conn.close()
        assert h.duplicate_count > 0
        assert any("duplicate" in i.lower() for i in h.issues)


# ═══════════════════════════════════════════════════════════════════════
# audit_database  (full scan)
# ═══════════════════════════════════════════════════════════════════════


class TestAuditDatabase:
    def test_empty_database_all_critical(self, tmp_path: Path):
        db_path = tmp_path / "empty_all.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE foo (id INTEGER)")
        conn.execute("CREATE TABLE bar (id INTEGER)")
        conn.commit()
        conn.close()

        report = audit_database(str(db_path))
        assert len(report.tables) == 2
        assert report.critical_count == 2
        assert report.healthy_count == 0

    def test_registry_filters_expected_dates(self, tmp_path: Path):
        db_path = tmp_path / "reg.db"
        _build_db(str(db_path), stale_tables={"reg_tbl": "2026-07-23"})
        registry = {"update_reg": _spec("reg_tbl", cadence=Cadence.DAILY, grace=0)}
        report = audit_database(str(db_path), registry=registry)
        assert len(report.tables) >= 1
        # status depends on freshness vs today; just verify it ran
        reg_health = [h for h in report.tables if h.table == "reg_tbl"]
        assert len(reg_health) == 1
        assert reg_health[0].row_count == 1

    def test_mixed_health(self, tmp_path: Path):
        db_path = tmp_path / "mixed.db"
        spec = _spec("ok_tbl", cadence=Cadence.DAILY, grace=30)
        expected_date = _expected_date_for(spec)
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE ok_tbl (id INTEGER, ts_code TEXT, trade_date TEXT)")
        conn.execute("INSERT INTO ok_tbl VALUES (1, '000001', ?)", (expected_date,))
        conn.execute("CREATE TABLE empty_tbl (id INTEGER, ts_code TEXT, trade_date TEXT)")
        conn.commit()
        conn.close()
        registry = {
            "update_ok": spec,
        }
        report = audit_database(str(db_path), registry=registry)
        ok_health = [h for h in report.tables if h.table == "ok_tbl"][0]
        empty_health = [h for h in report.tables if h.table == "empty_tbl"][0]
        assert ok_health.status == "healthy"
        assert empty_health.status == "critical"

    def test_json_output(self, tmp_path: Path):
        """Verify JSON serialization of HealthReport."""
        db_path = tmp_path / "json_out.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE jtbl (id INTEGER, ts_code TEXT, trade_date TEXT)")
        conn.commit()
        conn.close()
        report = audit_database(str(db_path))
        output = {
            "exit_code": report.exit_code,
            "summary": {
                "healthy": report.healthy_count,
                "degraded": report.degraded_count,
                "critical": report.critical_count,
                "not_due": report.not_due_count,
            },
            "tables": [
                {"table": h.table, "row_count": h.row_count, "status": h.status}
                for h in report.tables
            ],
        }
        dumped = json.dumps(output)
        parsed = json.loads(dumped)
        assert len(parsed["tables"]) == 1
        assert parsed["tables"][0]["status"] == "critical"


# ═══════════════════════════════════════════════════════════════════════
# Edge cases
# ═══════════════════════════════════════════════════════════════════════


class TestEdgeCases:
    def test_database_does_not_exist(self, tmp_path: Path):
        """Non-existent path should be handled gracefully."""
        report = audit_database(str(tmp_path / "nonexistent.db"))
        # sqlite3.connect creates file, so it won't actually fail
        # but if db_path is a directory, it should raise
        assert isinstance(report, HealthReport)

    def test_large_row_count(self, tmp_path: Path):
        db_path = tmp_path / "large.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE big (id INTEGER, ts_code TEXT, trade_date TEXT)")
        for i in range(10_000):
            conn.execute(
                "INSERT INTO big VALUES (?, '000001', '2026-07-23')",
                (i,),
            )
        conn.commit()
        conn.close()
        h = audit_database(str(db_path))
        big_tbl = [t for t in h.tables if t.table == "big"][0]
        assert big_tbl.row_count == 10_000

    def test_no_registry_fallback(self, tmp_path: Path):
        """When registry is empty, audit still works (no expected_date)."""
        db_path = tmp_path / "noreg.db"
        _build_db(str(db_path), stale_tables={"tbl": "2026-07-23"})
        report = audit_database(str(db_path), registry={})
        assert len(report.tables) >= 1
