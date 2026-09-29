#!/usr/bin/env python3
"""
CLI entry point for running schema migrations.
════════════════════════════════════════════

Usage:
    uv run python scripts/migrate_database.py [--dry-run] [--target N] [--db PATH]

Examples:
    uv run python scripts/migrate_database.py            # apply all pending
    uv run python scripts/migrate_database.py --dry-run   # preview only
    uv run python scripts/migrate_database.py --target 2  # apply only up to v002
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# 仓库根必须就地注入：以 `python scripts/migrate_database.py` 直接执行时 `sys.path[0]`
# 是 scripts/，此时 import 不到 core。这一句无法抽成函数——语言层面的先后顺序。
# 统一写法与门禁见 core/_bootstrap.py 与 tests/test_sys_path_bootstrap.py。
_REPO_ROOT = str(Path(__file__).resolve().parent.parent)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from core.migrations import MigrationEngine  # noqa: E402

logger = logging.getLogger(__name__)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="SQLite schema migration tool",
    )
    parser.add_argument(
        "--db",
        default="",
        help="Path to the SQLite database (default: $QUANT_DB_PATH or ~/Code/quant_data/quant_core.db)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show pending migrations without applying them",
    )
    parser.add_argument(
        "--target",
        type=int,
        default=None,
        help="Apply only migrations up to this version number",
    )
    parser.add_argument(
        "--migrations-dir",
        default="",
        help="Path to the migrations directory (default: <project>/migrations/)",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable debug logging",
    )
    return parser.parse_args()


def _resolve_db_path(db_arg: str) -> str:
    if db_arg:
        return db_arg
    env_db = __import__("os").environ.get("QUANT_DB_PATH")
    if env_db:
        return env_db
    default = Path.home() / "Code" / "quant_data" / "quant_core.db"
    return str(default)


def main() -> int:
    args = _parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    db_path = _resolve_db_path(args.db)
    migrations_dir = args.migrations_dir or str(
        Path(__file__).resolve().parent.parent / "migrations"
    )

    if not Path(db_path).exists():
        logger.error("database not found: %s", db_path)
        return 1

    engine = MigrationEngine(db_path=db_path, migrations_dir=migrations_dir)
    pending = engine.plan()

    if not pending:
        logger.info("✅ No pending migrations — schema is up to date.")
        return 0

    logger.info("Pending migrations (%d):", len(pending))
    for p in pending:
        logger.info("  %03d  %s  (sha256: %s…)", p["version"], p["description"], p["checksum"][:12])

    if args.dry_run:
        logger.info("Dry-run mode — nothing applied.")
        return 0

    # Ask for confirmation when applying to the real production DB
    # 精确匹配文件名：旧子串匹配会把 backup_quant_core.db / quant_core.db2
    # 当成生产库（误弹确认），反之边缘命名会漏掉生产确认
    is_prod = Path(db_path).name == "quant_core.db"
    if is_prod:
        answer = input(f"\n⚠️  This will modify: {db_path}\nContinue? [y/N] ").strip().lower()
        if answer not in ("y", "yes"):
            logger.info("Aborted by user.")
            return 1

    # 复用上方 plan() 的 engine：run_migrations 内部就是「再建一个 engine +
    # apply_pending」，双引擎两次连接且 preview 与执行可能不一致
    results = engine.apply_pending(target_version=args.target)

    errors = [r for r in results if r.get("error")]
    if errors:
        for e in errors:
            logger.error("❌ %03d failed: %s", e["version"], e["error"])
        return 1

    logger.info("✅ All migrations applied successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
