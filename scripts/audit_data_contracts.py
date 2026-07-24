"""
只读全库契约审计：检查每张表的数据健康度并输出 JSON 报告。
Exit codes: 0=healthy, 1=degraded, 2=critical
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
from dataclasses import asdict, dataclass, field
from datetime import date, timedelta
from typing import Literal

import pandas as pd

from core.task_registry import TASK_REGISTRY, Cadence, TaskSpec

logger = logging.getLogger(__name__)


@dataclass
class TableHealth:
    table: str
    row_count: int
    latest_date: str | None = None
    expected_date: str | None = None
    coverage_ratio: float | None = None
    core_field_completeness: float | None = None
    duplicate_count: int = 0
    invalid_value_count: int = 0
    status: Literal["healthy", "degraded", "critical", "not_due"] = "healthy"
    issues: list[str] = field(default_factory=list)


@dataclass
class HealthReport:
    tables: list[TableHealth] = field(default_factory=list)
    healthy_count: int = 0
    degraded_count: int = 0
    critical_count: int = 0
    not_due_count: int = 0

    @property
    def exit_code(self) -> int:
        if self.critical_count > 0:
            return 2
        if self.degraded_count > 0:
            return 1
        return 0


def _get_date_column(table: str, cursor) -> str | None:
    """Heuristic: find the first date-typed column in the table."""
    cursor.execute(f'PRAGMA table_info("{table}")')
    cols = cursor.fetchall()
    date_cols = ["trade_date", "date", "report_period", "end_date", "publish_date", "report_date"]
    for col in cols:
        name = col[1]
        if name in date_cols:
            return name
    return None


def _get_latest_date(table: str, date_col: str, cursor) -> str | None:
    try:
        cursor.execute(f'SELECT MAX("{date_col}") FROM "{table}"')
        row = cursor.fetchone()
        return str(row[0])[:10] if row and row[0] else None
    except Exception:
        return None


def _expected_date_for(spec: TaskSpec) -> str | None:
    """Return the latest date we expect data for, given cadence."""
    now = date.today()
    if spec.cadence == "daily":
        prev = now - timedelta(days=1)
        while prev.weekday() >= 5:
            prev -= timedelta(days=1)
        return prev.isoformat()
    if spec.cadence == "weekly":
        return (now - timedelta(weeks=1)).isoformat()
    if spec.cadence == "quarterly":
        m = now.month
        q = (m - 1) // 3
        qm = (q * 3 + 3)
        qy = now.year
        if qm == 3:
            qd = 31
        elif qm == 6 or qm == 9:
            qd = 30
        else:
            qd = 31
        qd_str = date(qy, qm, qd)
        return qd_str.isoformat()
    return None


def _check_table(
    table: str,
    conn: sqlite3.Connection,
    spec: TaskSpec | None,
    registry_tables: set[str],
) -> TableHealth:
    """Audit one table and return its health."""
    cursor = conn.cursor()
    issues: list[str] = []

    try:
        cursor.execute(f'SELECT COUNT(*) FROM "{table}"')
        row_count = cursor.fetchone()[0]
    except Exception as e:
        return TableHealth(table=table, row_count=0, status="critical",
                           issues=[f"cannot query: {e}"])

    date_col = _get_date_column(table, cursor)
    latest_date = _get_latest_date(table, date_col, cursor) if date_col else None
    expected_date = _expected_date_for(spec) if spec else None

    dup_count = 0
    completeness: float | None = None

    # Duplicate check via pragma
    try:
        cursor.execute(f'PRAGMA table_info("{table}")')
        col_infos = cursor.fetchall()
        col_names = [c[1] for c in col_infos]
        if col_names:
            name_str = ", ".join(f'"{c}"' for c in col_names)
            cursor.execute(f"SELECT {name_str}, COUNT(*) as _cnt FROM \"{table}\" GROUP BY {name_str} HAVING _cnt > 1")
            dup_count = len(cursor.fetchall())
    except Exception:
        pass

    # Core field completeness: check known numeric/metric columns
    try:
        df = pd.read_sql_query(f"SELECT * FROM \"{table}\" LIMIT 100", conn)
        if len(df) > 0:
            metric_cols = [c for c in df.columns if c not in (
                "id", "ts_code", "stock_code", "code", "trade_date", "date",
                "report_period", "data_source", "updated_at",
            ) and df[c].dtype in ("float64", "int64", "float", "int")]
            if metric_cols:
                completeness = float(df[metric_cols].notna().mean().mean())
    except Exception:
        pass

    # ---- Status determination ----
    status: Literal["healthy", "degraded", "critical", "not_due"] = "healthy"

    if spec and spec.cadence == Cadence.ON_DEMAND and row_count == 0:
        status = "not_due"

    if spec and expected_date and latest_date and latest_date < expected_date:
        days_stale = (date.fromisoformat(expected_date) - date.fromisoformat(latest_date)).days
        if days_stale > spec.grace_period_days:
            issues.append(f"stale: latest={latest_date}, expected={expected_date}, grace={spec.grace_period_days}d")
            status = "critical"
        elif days_stale > 0:
            issues.append(f"slightly stale: latest={latest_date}, expected={expected_date}")
            status = "degraded"

    if row_count == 0 and (not spec or spec.cadence != Cadence.ON_DEMAND):
        issues.append("empty table")
        status = "critical"

    if dup_count > 0:
        issues.append(f"{dup_count} duplicate groups found")
        if status == "healthy":
            status = "degraded"

    if completeness is not None and completeness < 0.5:
        issues.append(f"core field completeness {completeness:.1%} < 50%")
        if status == "healthy":
            status = "degraded"

    return TableHealth(
        table=table,
        row_count=row_count,
        latest_date=latest_date,
        expected_date=expected_date,
        coverage_ratio=None,
        core_field_completeness=round(completeness, 4) if completeness is not None else None,
        duplicate_count=dup_count,
        invalid_value_count=0,
        status=status,
        issues=issues,
    )


def audit_database(
    db_path: str,
    registry: dict[str, TaskSpec] | None = None,
) -> HealthReport:
    """Run a full audit of all tables in the database."""
    if registry is None:
        registry = {}
    report = HealthReport()
    conn = sqlite3.connect(db_path)

    try:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        all_tables = [row[0] for row in cursor.fetchall()]

        registry_tables = set()
        for spec in registry.values():
            registry_tables.update(spec.tables)

        for table in all_tables:
            spec = None
            for ts in registry.values():
                if table in ts.tables:
                    spec = ts
                    break
            health = _check_table(table, conn, spec, registry_tables)
            report.tables.append(health)
            if health.status == "healthy":
                report.healthy_count += 1
            elif health.status == "degraded":
                report.degraded_count += 1
            elif health.status == "critical":
                report.critical_count += 1
            elif health.status == "not_due":
                report.not_due_count += 1
    finally:
        conn.close()

    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit database data contracts")
    parser.add_argument("--db", default=None, help="Path to database (default: ~/Code/quant_data/quant_core.db)")
    parser.add_argument("--json", action="store_true", help="Output JSON report")
    args = parser.parse_args()

    db_path = args.db or "~/Code/quant_data/quant_core.db"

    try:
        registry = {spec.name: spec for spec in TASK_REGISTRY}
    except Exception:
        logger.exception("Failed to build registry map; running audit without registry context")
        registry = {}

    report = audit_database(db_path, registry=registry)

    if args.json:
        output = {
            "exit_code": report.exit_code,
            "summary": {
                "healthy": report.healthy_count,
                "degraded": report.degraded_count,
                "critical": report.critical_count,
                "not_due": report.not_due_count,
            },
            "tables": [asdict(h) for h in report.tables],
        }
        print(json.dumps(output, indent=2, ensure_ascii=False))
    else:
        print(f"Audit complete: {report.healthy_count} healthy, "
              f"{report.degraded_count} degraded, "
              f"{report.critical_count} critical, "
              f"{report.not_due_count} not_due")

    return report.exit_code


if __name__ == "__main__":
    sys.exit(main())
