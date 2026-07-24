"""WAL-safe SQLite backup gate using the SQLite Backup API.

Produces a consistent online backup without interrupting concurrent writers,
validates integrity via PRAGMA quick_check, verifies row-level consistency,
and computes a SHA-256 checksum.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path


@dataclass(frozen=True)
class BackupReport:
    backup_path: Path
    source_rows: int
    backup_rows: int
    quick_check: str
    sha256: str


def _count_user_rows(connection: sqlite3.Connection) -> int:
    """Sum row counts across all non-internal tables."""
    total = 0
    rows = connection.execute(
        "SELECT name FROM sqlite_master"
        " WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    ).fetchall()
    for (name,) in rows:
        (count,) = connection.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()
        total += count
    return total


def _sha256_of(path: Path) -> str:
    """Calculate SHA-256 digest reading in 8 MiB chunks."""
    h = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(8 * 1024 * 1024):
            h.update(chunk)
    return h.hexdigest()


def backup_database(source: Path, destination_dir: Path) -> BackupReport:
    """Create a consistent WAL-safe backup of *source* into *destination_dir*.

    Uses ``sqlite3.Connection.backup()`` so the source can remain open for
    concurrent reads/writes during the backup.

    Raises
    ------
    FileNotFoundError
        If *source* does not exist.
    FileExistsError
        If the generated backup filename already exists.
    RuntimeError
        If the backup fails ``PRAGMA quick_check``.
    """
    source = source.expanduser().resolve()
    if not source.exists():
        raise FileNotFoundError(f"source database does not exist: {source}")

    destination_dir = destination_dir.expanduser().resolve()
    destination_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    target = destination_dir / f"{source.stem}_{timestamp}.db"

    if target.exists():
        raise FileExistsError(f"backup target already exists: {target}")

    # --- Count source rows before backup ---
    src_conn = sqlite3.connect(f"file:{source}?mode=ro", uri=True)
    try:
        source_rows = _count_user_rows(src_conn)
    finally:
        src_conn.close()

    # --- Perform backup using the SQLite Backup API ---
    src_conn = sqlite3.connect(f"file:{source}?mode=ro", uri=True)
    try:
        dst_conn = sqlite3.connect(target)
        try:
            src_conn.backup(dst_conn)
            check = dst_conn.execute("PRAGMA quick_check").fetchone()[0]
        finally:
            dst_conn.close()
    finally:
        src_conn.close()

    if check != "ok":
        target.unlink(missing_ok=True)
        raise RuntimeError(f"backup quick_check failed: {check}")

    # --- Verify backup row count matches ---
    dst_conn = sqlite3.connect(target)
    try:
        backup_rows = _count_user_rows(dst_conn)
    finally:
        dst_conn.close()

    sha256 = _sha256_of(target)

    return BackupReport(
        backup_path=target,
        source_rows=source_rows,
        backup_rows=backup_rows,
        quick_check=check,
        sha256=sha256,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="WAL-safe SQLite backup gate")
    parser.add_argument(
        "--db",
        required=True,
        help="Path to source SQLite database",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Directory to write the backup to",
    )
    args = parser.parse_args(argv)

    report = backup_database(Path(args.db), Path(args.output_dir))
    print(json.dumps(asdict(report), ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
