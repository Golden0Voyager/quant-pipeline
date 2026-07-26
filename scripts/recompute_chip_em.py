"""重算 chip_distribution_em — 基于回填后的换手率，本地 numpy 路径批量重建。

2026-07-21 起的全零垃圾行已隔离删除，本脚本用修复后的 daily_bars.turnover_rate
重新计算最近 90 天筹码分布并写回。直接走 DB→numpy 主路径（EM API 已封禁）。
"""
from __future__ import annotations

import sqlite3
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.stock_cyq_em import _calculate_chip_numpy, _fetch_kline_db  # noqa: E402

DB_PATH = Path.home() / "Code" / "quant_data" / "quant_core.db"


def recompute_symbol(conn: sqlite3.Connection, symbol: str) -> tuple[int, str]:
    kline = _fetch_kline_db(symbol, str(DB_PATH))
    if not kline or len(kline) < 120:
        return 0, "insufficient_kline"
    tr_valid = sum(1 for r in kline[-120:] if (r.get("turnover_rate") or 0) > 0)
    if tr_valid < 60:
        return 0, "turnover_missing"

    df = _calculate_chip_numpy(kline[-240:])
    df = df.dropna(subset=["获利比例", "平均成本"])
    # 全零行防线（与采集任务一致）
    df = df[~((df["获利比例"] == 0) & (df["平均成本"] == 0))]
    df = df.iloc[-90:]
    if df.empty:
        return 0, "no_valid_rows"

    rows = [
        (
            symbol,
            str(r["日期"])[:10],
            float(r["获利比例"]),
            float(r["平均成本"]),
            float(r["90成本-低"]),
            float(r["90成本-高"]),
            float(r["90集中度"]),
            float(r["70成本-低"]),
            float(r["70成本-高"]),
            float(r["70集中度"]),
        )
        for _, r in df.iterrows()
    ]
    conn.executemany(
        """INSERT OR REPLACE INTO chip_distribution_em (
               ts_code, trade_date, profit_ratio, avg_cost,
               cost_90_low, cost_90_high, concentration_90,
               cost_70_low, cost_70_high, concentration_70
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        rows,
    )
    conn.commit()
    return len(rows), "ok"


def main() -> None:
    conn = sqlite3.connect(str(DB_PATH), timeout=60)
    conn.execute("PRAGMA busy_timeout = 60000")

    args = [a for a in sys.argv[1:] if not a.startswith("-")]
    if args:
        symbols = args
    else:
        # 目标: 历史上有筹码记录的股票（含被清理的全零股票，从 quarantine 恢复目标）
        rows = conn.execute(
            """SELECT DISTINCT ts_code FROM (
                   SELECT ts_code FROM chip_distribution_em
                   UNION SELECT ts_code FROM chip_distribution_em_quarantine
               ) ORDER BY ts_code"""
        ).fetchall()
        symbols = [r[0].split(".")[0] for r in rows]
        symbols = sorted(set(symbols))

    print(f"目标 {len(symbols)} 只", flush=True)
    t0 = time.time()
    ok = failed = total_rows = 0
    fail_reasons: dict[str, int] = {}
    for i, sym in enumerate(symbols, 1):
        try:
            n, status = recompute_symbol(conn, sym)
            if status == "ok":
                ok += 1
                total_rows += n
            else:
                failed += 1
                fail_reasons[status] = fail_reasons.get(status, 0) + 1
        except Exception as e:  # noqa: BLE001
            failed += 1
            fail_reasons[type(e).__name__] = fail_reasons.get(type(e).__name__, 0) + 1
        if i % 200 == 0 or i == len(symbols):
            print(f"进度 {i}/{len(symbols)} | 成功 {ok} | 失败 {failed} | 写入 {total_rows} 行 | {time.time()-t0:.0f}s", flush=True)

    conn.close()
    print(f"完成: 成功 {ok}, 失败 {failed}, 写入 {total_rows} 行", flush=True)
    if fail_reasons:
        print(f"失败原因分布: {fail_reasons}", flush=True)


if __name__ == "__main__":
    main()
