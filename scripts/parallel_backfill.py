#!/usr/bin/env python3
"""
并行全量回填脚本 —— 多进程加速数据抓取
================================================
用法:
    python parallel_backfill.py --workers 4

逻辑:
    1. 读取主库 stock_list，排除 daily_bars 已存在的股票
    2. 将剩余股票均分给 N 个 worker
    3. 每个 worker 有自己独立的临时数据库（避免 SQLite 锁竞争）
    4. 全部完成后合并回主库

注意:
    - 当前已有进程（如 PID 40453）不用停
    - 新 worker 会自动跳过已处理的股票
    - 环境变量 FORCE_AKSHARE=1, DISABLE_YFINANCE_FALLBACK=1 自动生效
"""
from __future__ import annotations

import argparse
import contextlib
import os
import shutil
import sqlite3
import subprocess
import sys
import time
from multiprocessing import Process
from pathlib import Path

# ---------------------------------------------------------------------------
# 路径配置
# ---------------------------------------------------------------------------
HOME = Path.home()
MASTER_DB = HOME / "Code/quant_data/quant_core.db"
# 仓库根从 __file__ 推导：此前这里硬编码 ~/Code/quant_pipeline，于是在另一个 checkout 里
# 运行会把 worker 的 cwd、脚本路径与进度文件全部指向**另一个**仓库副本（改动看着生效、其实没生效）。
_REPO_ROOT = Path(__file__).resolve().parent.parent
# worker 解释器用当前解释器（`uv run python scripts/parallel_backfill.py` 下即本项目 venv）。
# 此前写死兄弟仓库的 ~/Code/quant_hunter/.venv/bin/python3——那是另一个项目的 venv，
# 本仓库的依赖不保证装在那里。
PYTHON = Path(sys.executable)


def copy_schema(src_db: Path, dst_db: Path):
    """把主库的 schema（不含数据）复制到临时库"""
    if dst_db.exists():
        dst_db.unlink()
    shutil.copy(src_db, dst_db)
    # 清空数据表（保留 schema）
    conn = sqlite3.connect(dst_db)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [r[0] for r in cursor.fetchall()]
    for t in tables:
        if t not in ("sqlite_sequence",):
            with contextlib.suppress(sqlite3.OperationalError):
                cursor.execute(f"DELETE FROM {t}")  # VIEW 等跳过
    conn.commit()
    conn.close()


def prepare_worker_db(worker_id: int, stocks: list[str]) -> Path:
    """为 worker 创建临时数据库，只包含分配到的股票"""
    worker_db = MASTER_DB.parent / f"quant_core_worker{worker_id}.db"
    copy_schema(MASTER_DB, worker_db)

    conn = sqlite3.connect(worker_db)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM stock_list")
    cursor.executemany(
        "INSERT INTO stock_list (code) VALUES (?)",
        [(s,) for s in stocks]
    )
    conn.commit()
    conn.close()
    return worker_db


def run_worker(worker_id: int, stocks: list[str], total_workers: int):
    """启动一个 worker 进程"""
    worker_db = prepare_worker_db(worker_id, stocks)
    progress_file = _REPO_ROOT / f"progress_worker{worker_id}.json"
    if progress_file.exists():
        progress_file.unlink()

    env = os.environ.copy()
    env["QUANT_DB_PATH"] = str(worker_db)
    env["DISABLE_YFINANCE_FALLBACK"] = "1"

    cmd = [
        str(PYTHON),
        str(_REPO_ROOT / "daily_pipeline.py"),
        "--task", "update_bars",
        "--force",
    ]

    log_file = MASTER_DB.parent / "logs" / f"backfill_worker{worker_id}.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)

    print(f"[Worker {worker_id}] 启动，处理 {len(stocks)} 只股票 -> {worker_db}")
    with open(log_file, "w") as f:
        proc = subprocess.Popen(
            cmd,
            cwd=str(_REPO_ROOT),
            env=env,
            stdout=f,
            stderr=subprocess.STDOUT,
        )
        proc.wait()

    if proc.returncode == 0:
        print(f"[Worker {worker_id}] ✅ 完成")
    else:
        print(f"[Worker {worker_id}] ⚠️ 退出码 {proc.returncode}")


def merge_worker_dbs(worker_ids: list[int]):
    """把所有 worker 的 daily_bars 合并回主库"""
    print("\n📦 开始合并数据到主库...")
    master_conn = sqlite3.connect(str(MASTER_DB))
    master_cursor = master_conn.cursor()

    total_rows = 0
    for wid in worker_ids:
        worker_db = MASTER_DB.parent / f"quant_core_worker{wid}.db"
        if not worker_db.exists():
            continue

        worker_conn = sqlite3.connect(str(worker_db))
        worker_cursor = worker_conn.cursor()
        # 查询时加入 data_source, updated_at
        worker_cursor.execute(
            "SELECT ts_code, trade_date, open, high, low, close, volume, amount, turnover_rate, pct_change, amplitude, data_source, updated_at FROM daily_bars"
        )
        rows = worker_cursor.fetchall()
        worker_conn.close()

        if rows:
            master_cursor.executemany(
                """INSERT OR REPLACE INTO daily_bars
                (ts_code, trade_date, open, high, low, close, volume, amount, turnover_rate, pct_change, amplitude, data_source, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                rows,
            )
            total_rows += len(rows)
            print(f"  Worker {wid}: {len(rows)} 行")

    master_conn.commit()
    master_conn.close()
    print(f"\n✅ 合并完成，共 {total_rows} 行写入主库")


def cleanup(worker_ids: list[int]):
    """清理临时文件"""
    print("\n🧹 清理临时文件...")
    for wid in worker_ids:
        for p in [
            MASTER_DB.parent / f"quant_core_worker{wid}.db",
            _REPO_ROOT / f"progress_worker{wid}.json",
        ]:
            if p.exists():
                p.unlink()
                print(f"  已删除 {p.name}")


def get_remaining_stocks() -> list[str]:
    """获取尚未写入 daily_bars 的股票"""
    conn = sqlite3.connect(str(MASTER_DB))
    cursor = conn.cursor()
    cursor.execute("SELECT code FROM stock_list")
    all_stocks = [r[0] for r in cursor.fetchall()]
    cursor.execute("SELECT DISTINCT ts_code FROM daily_bars")
    done = {r[0] for r in cursor.fetchall()}
    conn.close()
    remaining = [s for s in all_stocks if s not in done]
    print(f"总股票: {len(all_stocks)} | 已完成: {len(done)} | 剩余: {len(remaining)}")
    return remaining


def main():
    parser = argparse.ArgumentParser(description="并行全量数据回填")
    default_workers = min(os.cpu_count() or 2, 3)
    parser.add_argument("--workers", type=int, default=default_workers, help=f"并行进程数（默认 {default_workers}，最大建议 3）")
    parser.add_argument("--merge-only", action="store_true", help="仅合并已有 worker 库，不启动新任务")
    parser.add_argument("--no-cleanup", action="store_true", help="合并后不删除临时库")
    args = parser.parse_args()
    os.nice(10)

    if not MASTER_DB.exists():
        print(f"❌ 主库不存在: {MASTER_DB}")
        sys.exit(1)

    # 仅合并模式
    if args.merge_only:
        existing = [i for i in range(args.workers) if (MASTER_DB.parent / f"quant_core_worker{i}.db").exists()]
        if not existing:
            print("没有可合并的 worker 数据库")
            return
        merge_worker_dbs(existing)
        if not args.no_cleanup:
            cleanup(existing)
        return

    # 获取剩余股票
    remaining = get_remaining_stocks()
    if not remaining:
        print("✅ 所有股票已处理完毕")
        return

    # 分组
    n = args.workers
    chunk_size = (len(remaining) + n - 1) // n
    groups = [remaining[i : i + chunk_size] for i in range(0, len(remaining), chunk_size)]

    print(f"\n🚀 启动 {len(groups)} 个 worker 并行处理...")
    print(f"   预计总时间: ~{len(remaining) * 1.2 // 60 // n} 小时（按 {n} 进程估算）\n")

    start_time = time.time()

    # 启动进程
    processes = []
    for i, stocks in enumerate(groups):
        p = Process(target=run_worker, args=(i, stocks, len(groups)))
        p.start()
        processes.append(p)
        time.sleep(2)  # 错开启动，避免同时burst

    # 等待全部完成
    for p in processes:
        p.join()

    elapsed = time.time() - start_time
    print(f"\n⏱️  并行阶段耗时: {elapsed / 3600:.1f} 小时")

    # 合并
    worker_ids = list(range(len(groups)))
    merge_worker_dbs(worker_ids)

    if not args.no_cleanup:
        cleanup(worker_ids)

    # 合并完成后，自动为所有新回填的股票计算技术指标
    # 由于我们在 daily_pipeline.py 中实现了智能增量检测，这个任务会极快，只计算没有指标的股票！
    print("\n📊 自动为新回填股票计算技术指标...")
    try:
        cmd_ind = [
            str(PYTHON),
            str(_REPO_ROOT / "daily_pipeline.py"),
            "--task", "update_indicators",
            "--force",
        ]
        subprocess.run(cmd_ind, cwd=str(_REPO_ROOT), check=True)
        print("✅ 技术指标计算完成！")
    except Exception as e:
        print(f"⚠️ 技术指标计算失败: {e}")

    # 最终统计
    remaining_after = get_remaining_stocks()
    if not remaining_after:
        print("\n🎉 全量回填全部完成！")
    else:
        print(f"\n⚠️ 还有 {len(remaining_after)} 只股票未成功，建议再跑一次")


if __name__ == "__main__":
    main()
