"""自选股文件同步服务。"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import NamedTuple

from tui.config import WATCHLIST_DIR

logger = logging.getLogger(__name__)


class WatchlistSyncResult(NamedTuple):
    added: int
    reactivated: int
    deactivated: int
    files: int
    success: bool
    error: str | None


def _code_to_ts_code(code: str) -> str | None:
    """将纯数字股票代码转为 ts_code 格式（加交易所后缀）。"""
    code = code.strip()
    if not code.isdigit():
        return None
    # 920xxx 北交所（必须在 6/9 之前检查，否则被 "9" 误匹配为 SH）
    if code.startswith("920"):
        return f"{code}.BJ"
    if code.startswith(("6", "9")):
        return f"{code}.SH"
    if code.startswith(("0", "2", "3")):
        return f"{code}.SZ"
    if code.startswith(("4", "8")):
        return f"{code}.BJ"
    return None


def sync_watchlists_from_files(db_path: str) -> WatchlistSyncResult:
    """从文本文件同步自选股到数据库，支持新增/恢复/停用。"""
    import sys

    tui_mod = sys.modules.get("tui")
    watch_dir_val = (
        getattr(tui_mod, "WATCHLIST_DIR", WATCHLIST_DIR)
        if tui_mod
        else WATCHLIST_DIR
    )
    watch_dir = Path(watch_dir_val)
    if not watch_dir.is_dir():
        error = f"自选股目录不存在: {watch_dir}"
        logger.warning(error)
        return WatchlistSyncResult(0, 0, 0, 0, False, error)

    conn = None
    txt_files: list[Path] = []
    try:
        txt_files = sorted(watch_dir.glob("*.txt"))
        desired_codes: set[str] = set()
        for fpath in txt_files:
            text = fpath.read_text(encoding="utf-8")
            for line in text.splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                code = line.split("#")[0].split()[0].strip()
                ts_code = _code_to_ts_code(code)
                if ts_code:
                    desired_codes.add(ts_code)

        conn = sqlite3.connect(db_path, timeout=5.0)
        cur = conn.cursor()

        existing: dict[str, str] = {
            row[0]: row[1]
            for row in cur.execute(
                "SELECT ts_code, status FROM watchlist "
                "WHERE source_scan = 'watchlist_sync'"
            )
        }

        today = datetime.now().strftime("%Y-%m-%d")
        updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        added = reactivated = deactivated = 0

        for ts_code in sorted(desired_codes):
            if ts_code not in existing:
                cur.execute(
                    "INSERT OR IGNORE INTO watchlist "
                    "(ts_code, added_date, source_scan, status) "
                    "VALUES (?, ?, 'watchlist_sync', 'tracking')",
                    (ts_code, today),
                )
                added += max(cur.rowcount, 0)
            elif existing[ts_code] != "tracking":
                cur.execute(
                    "UPDATE watchlist SET status='tracking', updated_at=? "
                    "WHERE ts_code=? AND source_scan='watchlist_sync'",
                    (updated_at, ts_code),
                )
                reactivated += max(cur.rowcount, 0)

        removed_codes = set(existing) - desired_codes
        if removed_codes:
            placeholders = ",".join("?" for _ in removed_codes)
            cur.execute(
                f"UPDATE watchlist SET status='inactive', updated_at=? "
                f"WHERE source_scan='watchlist_sync' "
                f"AND status!='inactive' AND ts_code IN ({placeholders})",
                (updated_at, *sorted(removed_codes)),
            )
            deactivated = max(cur.rowcount, 0)

        conn.commit()
        logger.info(
            "自选股同步完成: 新增 %s, 恢复 %s, 停用 %s, 来源 %s 个文件",
            added,
            reactivated,
            deactivated,
            len(txt_files),
        )
        return WatchlistSyncResult(
            added, reactivated, deactivated, len(txt_files), True, None
        )
    except Exception as exc:
        if conn is not None:
            conn.rollback()
        error = str(exc)
        logger.warning(f"自选股同步失败: {error}")
        return WatchlistSyncResult(0, 0, 0, len(txt_files), False, error)
    finally:
        if conn is not None:
            conn.close()
