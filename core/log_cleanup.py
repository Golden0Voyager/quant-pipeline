from __future__ import annotations

import argparse
import sys
import time
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path


@dataclass
class CleanupResult:
    """日志清理结果。"""

    deleted_count: int
    freed_bytes: int
    errors: list[str]


def cleanup_logs(
    logs_dir: str | Path,
    keep_days: int,
    exclude: Iterable[str | Path] | None = None,
) -> CleanupResult:
    """清理 logs 目录下的日志文件（``*.log``）。

    Args:
        logs_dir: 日志目录路径。
        keep_days: 保留天数。
            - ``0``：删除全部（除 ``exclude`` 中的文件）。
            - ``>0``：保留最近 ``keep_days`` 天，删除更早的文件。
        exclude: 需排除的文件路径集合（如当前正被 TUI tail 的活跃日志），
            这些文件不会被删除。

    Returns:
        CleanupResult：删除文件数、释放字节数、错误信息列表。
    """
    logs_path = Path(logs_dir)
    if not logs_path.is_dir():
        return CleanupResult(0, 0, [f"日志目录不存在: {logs_path}"])

    exclude_set = {str(p) for p in (exclude or [])}
    cutoff = time.time() - keep_days * 86400 if keep_days > 0 else 0.0

    deleted_count = 0
    freed_bytes = 0
    errors: list[str] = []

    for log_file in sorted(logs_path.glob("*.log")):
        if not log_file.is_file():
            continue
        if str(log_file) in exclude_set:
            continue
        try:
            mtime = log_file.stat().st_mtime
            if keep_days > 0 and mtime >= cutoff:
                continue  # 在保留期内，跳过
            size = log_file.stat().st_size
            log_file.unlink()
            deleted_count += 1
            freed_bytes += size
        except OSError as exc:
            errors.append(f"删除失败 {log_file.name}: {exc}")

    return CleanupResult(deleted_count, freed_bytes, errors)


def _format_size(n: int) -> str:
    """Human-readable 字节数。"""
    mb = n / (1024 * 1024)
    if mb >= 1024:
        return f"{mb / 1024:.2f} GB"
    if mb >= 1:
        return f"{mb:.2f} MB"
    return f"{n / 1024:.1f} KB"


def main() -> None:
    parser = argparse.ArgumentParser(description="清理 quant_data/logs 下的日志文件")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--all", action="store_true", help="删除所有日志（保留排除项）")
    group.add_argument(
        "--keep-days", type=int, metavar="N", help="保留最近 N 天，删除更早的日志"
    )
    parser.add_argument(
        "--logs-dir",
        default=str(Path.home() / "Code/quant_data/logs"),
        help="日志目录（默认 ~/Code/quant_data/logs）",
    )
    parser.add_argument("--dry-run", action="store_true", help="只统计，不实际删除")
    args = parser.parse_args()

    keep_days = 0 if args.all else args.keep_days

    if args.dry_run:
        logs_path = Path(args.logs_dir)
        cutoff = time.time() - keep_days * 86400 if keep_days > 0 else 0.0
        candidates = [
            f
            for f in logs_path.glob("*.log")
            if f.is_file() and (keep_days == 0 or f.stat().st_mtime < cutoff)
        ]
        total = sum(f.stat().st_size for f in candidates)
        print(f"[dry-run] 将删除 {len(candidates)} 个文件，释放 {_format_size(total)}")
        for f in sorted(candidates, key=lambda x: x.name):
            print(f"  {f.name}  ({_format_size(f.stat().st_size)})")
        return

    result = cleanup_logs(args.logs_dir, keep_days)
    if result.deleted_count == 0:
        print("没有需要清理的日志文件")
    else:
        print(
            f"已删除 {result.deleted_count} 个日志文件，释放 {_format_size(result.freed_bytes)}"
        )
    for err in result.errors:
        print(f"警告: {err}", file=sys.stderr)


if __name__ == "__main__":
    main()
