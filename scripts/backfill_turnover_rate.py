"""回填 daily_bars.turnover_rate — 修复重复列名 bug 造成的历史换手率全 NULL。

背景: save_daily_bars 的重复列名 bug 导致 daily_bars.turnover_rate 长期被写成
NULL，筹码分布 (chip_distribution_em) 因此自 2026-07-21 起全零。本脚本从新浪
K 线接口 (含换手率) 批量回填，仅更新 NULL/0 的行，不动其他字段。

用法:
    uv run python scripts/backfill_turnover_rate.py            # 筹码目标清单
    uv run python scripts/backfill_turnover_rate.py --all      # 全市场
    uv run python scripts/backfill_turnover_rate.py 300454 000975   # 指定股票
"""
from __future__ import annotations

import os
import random
import sqlite3
import sys
import time
from pathlib import Path

os.environ.setdefault("NO_PROXY", "*")
for _k in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
    os.environ.pop(_k, None)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.stock_cyq_em import _fetch_kline_sina  # noqa: E402

DB_PATH = Path.home() / "Code" / "quant_data" / "quant_core.db"
PROGRESS_FILE = Path(__file__).resolve().parent / ".backfill_turnover_progress.txt"


def _target_symbols(conn: sqlite3.Connection, mode: str) -> list[str]:
    if mode == "all":
        rows = conn.execute("SELECT DISTINCT ts_code FROM daily_bars ORDER BY ts_code").fetchall()
        return [r[0] for r in rows]
    # 默认: 与筹码任务相同的目标 —— chip_distribution_em 中已有记录的股票
    rows = conn.execute(
        "SELECT DISTINCT ts_code FROM chip_distribution_em ORDER BY ts_code"
    ).fetchall()
    return [r[0] for r in rows]


def backfill_symbol(conn: sqlite3.Connection, symbol: str) -> tuple[int, str]:
    """返回 (更新行数, 状态)。仅更新 turnover_rate IS NULL/0 的行。"""
    # 目标清单可能混有带交易所后缀的代码 (如 000975.SZ)，统一归一为 bare 格式
    symbol = symbol.split(".")[0]
    kline = _fetch_kline_sina(symbol)
    if not kline:
        return 0, "fetch_failed"
    rows = [
        (r["turnover_rate"], symbol, str(r["date"])[:10])
        for r in kline
        if (r.get("turnover_rate") or 0) > 0
    ]
    if not rows:
        return 0, "no_turnover"
    cur = conn.executemany(
        """UPDATE daily_bars SET turnover_rate = ?
           WHERE ts_code = ? AND trade_date = ?
           AND (turnover_rate IS NULL OR turnover_rate = 0)""",
        rows,
    )
    conn.commit()
    return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0, "ok"


def main() -> None:
    args = [a for a in sys.argv[1:] if not a.startswith("-")]
    mode = "all" if "--all" in sys.argv else "default"

    conn = sqlite3.connect(str(DB_PATH), timeout=60)
    conn.execute("PRAGMA busy_timeout = 60000")

    symbols = args if args else _target_symbols(conn, mode)

    done: set[str] = set()
    if PROGRESS_FILE.exists():
        done = {line.strip() for line in PROGRESS_FILE.read_text(encoding="utf-8").splitlines() if line.strip()}
    todo = [s for s in symbols if s not in done]
    print(f"目标 {len(symbols)} 只，已完成 {len(symbols) - len(todo)}，待处理 {len(todo)}", flush=True)

    updated_total = 0
    failed = 0
    for i, sym in enumerate(todo, 1):
        try:
            n, status = backfill_symbol(conn, sym)
            updated_total += n
            if status == "ok":
                with open(PROGRESS_FILE, "a", encoding="utf-8") as f:
                    f.write(f"{sym}\n")
            else:
                failed += 1
                print(f"  ⚠️ {sym}: {status}", flush=True)
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"  ❌ {sym}: {type(e).__name__}: {e}", flush=True)
        if i % 50 == 0 or i == len(todo):
            print(f"进度 {i}/{len(todo)} | 累计更新 {updated_total} 行 | 失败 {failed}", flush=True)
        time.sleep(random.uniform(0.3, 0.6))

    conn.close()
    print(f"完成: 更新 {updated_total} 行, 失败 {failed} 只", flush=True)


if __name__ == "__main__":
    main()
