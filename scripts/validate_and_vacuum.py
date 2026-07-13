#!/usr/bin/env python3
"""
数据校验 + VACUUM 压缩脚本
============================
在并行回填合并完成后运行，确保数据质量并优化存储。

用法:
    python validate_and_vacuum.py [--vacuum]
    --vacuum: 执行 VACUUM（需要额外磁盘空间）
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

MASTER_DB = Path.home() / "Code/quant_data/quant_core.db"


def fmt_num(n: int) -> str:
    return f"{n:,}"


def run_checks(db_path: Path) -> dict:
    """运行全部数据校验，返回结果字典"""
    results = {"passed": 0, "failed": 0, "warnings": 0, "details": []}

    def log(status: str, msg: str):
        icon = {"PASS": "✅", "FAIL": "❌", "WARN": "⚠️"}.get(status, "ℹ️")
        results["details"].append(f"{icon} [{status}] {msg}")
        if status == "PASS":
            results["passed"] += 1
        elif status == "FAIL":
            results["failed"] += 1
        else:
            results["warnings"] += 1

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    print(f"\n📊 数据库: {db_path}")
    print(f"   大小: {db_path.stat().st_size / (1024*1024):.1f} MB")
    print(f"   时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("\n" + "=" * 60)

    # ------------------------------------------------------------------
    # 1. 物理完整性检查
    # ------------------------------------------------------------------
    cursor.execute("PRAGMA integrity_check")
    integrity = cursor.fetchone()[0]
    if integrity == "ok":
        log("PASS", "SQLite 物理完整性: OK")
    else:
        log("FAIL", f"SQLite 物理完整性异常: {integrity}")

    # ------------------------------------------------------------------
    # 2. 表存在性
    # ------------------------------------------------------------------
    required_tables = [
        "daily_bars", "indicators", "stock_list", "fundamentals",
        "fund_flow", "historical_valuation", "scan_results", "chip_distribution",
        "chip_distribution_em"
    ]
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    existing = {r[0] for r in cursor.fetchall()}
    for t in required_tables:
        if t in existing:
            log("PASS", f"表存在: {t}")
        else:
            log("WARN", f"表缺失: {t}")

    # ------------------------------------------------------------------
    # 3. 覆盖率: daily_bars / stock_list
    # ------------------------------------------------------------------
    cursor.execute("SELECT COUNT(*) FROM stock_list")
    total_stocks = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(DISTINCT ts_code) FROM daily_bars")
    covered_stocks = cursor.fetchone()[0]
    coverage_pct = covered_stocks / total_stocks * 100 if total_stocks else 0

    if coverage_pct >= 99:
        log("PASS", f"覆盖率: {covered_stocks}/{total_stocks} ({coverage_pct:.1f}%)")
    elif coverage_pct >= 95:
        log("WARN", f"覆盖率偏低: {covered_stocks}/{total_stocks} ({coverage_pct:.1f}%)")
    else:
        log("FAIL", f"覆盖率严重不足: {covered_stocks}/{total_stocks} ({coverage_pct:.1f}%)")

    # ------------------------------------------------------------------
    # 4. 重复行检查 (ts_code, trade_date)
    # ------------------------------------------------------------------
    cursor.execute("""
        SELECT COUNT(*) - COUNT(DISTINCT ts_code || '_' || trade_date)
        FROM daily_bars
    """)
    dup_count = cursor.fetchone()[0]
    if dup_count == 0:
        log("PASS", "重复行检查: 0 条重复")
    else:
        log("FAIL", f"重复行检查: {fmt_num(dup_count)} 条重复")

    # ------------------------------------------------------------------
    # 5. 日期范围
    # ------------------------------------------------------------------
    cursor.execute("""
        SELECT MIN(trade_date) as min_dt, MAX(trade_date) as max_dt,
               COUNT(DISTINCT trade_date) as uniq_dates
        FROM daily_bars
    """)
    row = cursor.fetchone()
    min_dt, max_dt, uniq_dates = row
    log("PASS", f"日期范围: {min_dt} ~ {max_dt} ({fmt_num(uniq_dates)} 个交易日)")

    # ------------------------------------------------------------------
    # 6. 每只股票的数据量分布
    # ------------------------------------------------------------------
    cursor.execute("""
        SELECT AVG(cnt) as avg_cnt, MIN(cnt) as min_cnt, MAX(cnt) as max_cnt
        FROM (SELECT COUNT(*) as cnt FROM daily_bars GROUP BY ts_code)
    """)
    row = cursor.fetchone()
    avg_cnt, min_cnt, max_cnt = row
    if avg_cnt is not None:
        log("PASS", f"每只股票平均 {avg_cnt:.0f} 条, 最少 {min_cnt}, 最多 {max_cnt}")
    else:
        log("WARN", "daily_bars 为空表，无数据量统计")

    # 检查数据量异常少的股票（可能刚上市或长期停牌）
    cursor.execute("""
        SELECT ts_code, COUNT(*) as cnt FROM daily_bars
        GROUP BY ts_code HAVING cnt < 100
    """)
    low_data = cursor.fetchall()
    if len(low_data) <= 50:
        log("PASS", f"数据量<100天的股票: {len(low_data)} 只（正常: 新股/停牌）")
    else:
        log("WARN", f"数据量<100天的股票: {len(low_data)} 只，建议排查")

    # ------------------------------------------------------------------
    # 7. NULL 率统计
    # ------------------------------------------------------------------
    null_cols = ["open", "high", "low", "close", "volume", "amount", "turnover_rate", "pct_change", "amplitude"]
    for col in null_cols:
        cursor.execute(f"SELECT COUNT(*) FROM daily_bars WHERE {col} IS NULL")
        null_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM daily_bars")
        total_rows = cursor.fetchone()[0]
        null_pct = null_count / total_rows * 100 if total_rows else 0

        if col == "turnover_rate":
            # turnover_rate 从 yfinance 获取为 NULL 是预期行为
            if null_pct > 90:
                log("WARN", f"{col}: {null_pct:.1f}% 为 NULL (yfinance 不提供，预期)")
            else:
                log("PASS", f"{col}: {null_pct:.1f}% 为 NULL")
        elif null_pct > 1:
            log("FAIL", f"{col}: {null_pct:.1f}% 为 NULL (异常)")
        elif null_pct > 0:
            log("WARN", f"{col}: {null_pct:.1f}% 为 NULL")
        else:
            log("PASS", f"{col}: 0% 为 NULL")

    # ------------------------------------------------------------------
    # 8. 价格合理性检查
    # ------------------------------------------------------------------
    cursor.execute("""
        SELECT COUNT(*) FROM daily_bars
        WHERE close <= 0 OR close > 10000 OR volume < 0
    """)
    bad_prices = cursor.fetchone()[0]
    if bad_prices == 0:
        log("PASS", "价格合理性: 无异常")
    else:
        log("FAIL", f"价格合理性: {fmt_num(bad_prices)} 条异常价格")

    conn.close()
    return results


def do_vacuum(db_path: Path):
    """执行 VACUUM"""
    size_before = db_path.stat().st_size
    print("\n🔧 开始 VACUUM...")
    print(f"   前: {size_before / (1024*1024):.1f} MB")

    conn = sqlite3.connect(str(db_path))
    conn.execute("VACUUM")
    conn.close()

    size_after = db_path.stat().st_size
    saved = size_before - size_after
    print(f"   后: {size_after / (1024*1024):.1f} MB")
    print(f"   节省: {saved / (1024*1024):.1f} MB ({saved/size_before*100:.1f}%)")
    print("✅ VACUUM 完成")


def main():
    parser = argparse.ArgumentParser(description="数据校验 + VACUUM")
    parser.add_argument("--vacuum", action="store_true", help="执行 VACUUM 压缩")
    parser.add_argument("--db", type=str, default=str(MASTER_DB), help="数据库路径")
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"❌ 数据库不存在: {db_path}")
        sys.exit(1)

    # 运行校验
    results = run_checks(db_path)

    # 打印汇总
    print("\n" + "=" * 60)
    print("📋 校验汇总")
    print(f"   ✅ 通过: {results['passed']}")
    print(f"   ⚠️  警告: {results['warnings']}")
    print(f"   ❌ 失败: {results['failed']}")

    if results["failed"] > 0:
        print("\n❌ 存在失败项，建议修复后再使用")
    elif results["warnings"] > 0:
        print("\n⚠️ 存在警告项，数据可用但需注意")
    else:
        print("\n🎉 全部通过")

    # 打印详情
    print("\n📖 详细结果:")
    for d in results["details"]:
        print(f"   {d}")

    # VACUUM
    if args.vacuum:
        if results["failed"] > 0:
            print("\n⛔ 校验存在失败项，跳过 VACUUM（先修复数据）")
            sys.exit(1)
        do_vacuum(db_path)

    # 最终建议
    print("\n💡 后续建议:")
    print("   1. 若 turnover_rate 为 NULL，等有 VPN/回国后用 AkShare 补全")
    print("   2. 每天 15:35 的增量更新会自动维护数据新鲜度")
    print("   3. 建议每月运行一次本脚本做健康检查")


if __name__ == "__main__":
    main()
