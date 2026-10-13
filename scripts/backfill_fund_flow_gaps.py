#!/usr/bin/env python3
"""
回填 fund_flow 历史缺口：东财 push2his 逐票日 K 补 daily_bars 有、fund_flow 缺的 (票, 日)。

背景（2026-10-13 生产库只读核实）：
    东财 push2 榜单端点间歇失败 → update_fund_flow 走同花顺兜底，兜底只覆盖
    约 5200 只且无超大单/大单分解。自 2026-09-24 起覆盖率从健康水位
    93.8%~94.1% 塌到 66.9%，1582 只彻底掉出 fund_flow，累计 29523 个
    (票, 日) 对缺失。榜单端点只给「当日快照」，补历史必须走逐票日 K。

与覆盖度守卫（tasks/market_flow.py::_FUND_FLOW_COVERAGE_FLOOR）配套：
    守卫负责让缺口可见，本脚本负责把已积欠的洞填上。

只写「本地确实缺」的 (票, 日)，因此可重复执行（幂等）；周末行在入库前过滤，
避免撞上 fund_flow 的 trg_fund_flow_reject_weekend 触发器——那会让
save_fund_flow_batch 的单事务 executemany 整批回滚。

用法：
    uv run python scripts/backfill_fund_flow_gaps.py --dry-run          # 预览
    uv run python scripts/backfill_fund_flow_gaps.py                    # 写库
    uv run python scripts/backfill_fund_flow_gaps.py --since 2026-09-18 # 自定起始日
    uv run python scripts/backfill_fund_flow_gaps.py --limit 50         # 先试 50 只
"""
from __future__ import annotations

import argparse
import logging
import os
import sqlite3
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path

import requests

# 禁用代理环境变量（东财走环境代理会被源站拒连，scripts/repair_turnover.py 同款）。
# 与下面的 trust_env=False 是两道互补的保险：前者对全进程生效，后者只对本次
# Session 生效（量化脚本内部另建的连接不受影响）。
for _var in (
    "http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "all_proxy",
):
    os.environ.pop(_var, None)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# 仓库根目录必须就地注入：本脚本以 `python scripts/backfill_fund_flow_gaps.py`
# 直接执行，此时 `sys.path[0]` 是 scripts/，还 import 不到 core。统一写法与
# 门禁见 core/_bootstrap.py 与 tests/test_sys_path_bootstrap.py。
_REPO_ROOT = str(Path(__file__).resolve().parent.parent)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from core.utils import infer_market  # noqa: E402
from providers import SmartMoneyDBProvider  # noqa: E402

DEFAULT_DB_PATH = Path("~/Code/quant_data/quant_core.db").expanduser()

_FFLOW_URL = "https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get"
_FFLOW_PARAMS = {
    "klt": "101",
    "lmt": "30",
    "fields1": "f1,f2,f3,f7",
    "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63",
    "ut": "b2884a393a59ad64002292a3e90d46a5",
}
# 逐票字段序（实测 920002 校验：主力净额 = 大单 + 超大单，主力占比 = 两者占比之和）
#   0 f51 日期          1 f52 主力净额   2 f53 小单净额  3 f54 中单净额
#   4 f55 大单净额      5 f56 超大单净额 6 f57 主力占比  7 f58 小单占比
#   8 f59 中单占比      9 f60 大单占比  10 f61 超大单占比
_NUMERIC_COLUMNS = (
    "main_net_inflow",
    "main_net_inflow_pct",
    "super_large_net_inflow",
    "super_large_net_inflow_pct",
    "large_net_inflow",
    "large_net_inflow_pct",
)
# parts 索引 → 字段名。索引 2/3/7/8（小单/中单）本地表无对应列，丢弃。
_PART_INDEX = {
    "main_net_inflow": 1,
    "main_net_inflow_pct": 6,
    "super_large_net_inflow": 5,
    "super_large_net_inflow_pct": 10,
    "large_net_inflow": 4,
    "large_net_inflow_pct": 9,
}


def secid_for(code: str) -> str:
    """东财 secid 前缀。沪市与科创板是 1.，其余（深/创/中小/北交所）是 0.。

    权威判定是 core.utils.infer_market：**920 必须先于 90 判北交所**——
    handoff 初稿写的 `code.startswith(("6", "9")) → 1.` 会把 350 只 920 段
    北交所全判成沪市，实测 secid=1.920002 返回 `rc:100, data:null`，
    而 0.920002 正常返回数据。
    """
    return ("1." if infer_market(code) in ("sh", "star") else "0.") + code


def parse_kline(raw: str) -> dict[str, object] | None:
    """解析 push2his 单根日 K → save_fund_flow_batch 的记录；不可解析返回 None。

    接口只回名称不回代码，所以 ``symbol`` 留空由调用方按请求的票填入。
    """
    parts = raw.split(",")
    if len(parts) < 11 or not parts[0]:
        return None
    record: dict[str, object] = {"symbol": "", "date": parts[0]}
    for field, idx in _PART_INDEX.items():
        try:
            record[field] = float(parts[idx])
        except (ValueError, IndexError):
            record[field] = None
    if all(record[f] is None for f in _NUMERIC_COLUMNS):
        return None
    return record


def fetch_code_history(session: requests.Session, code: str) -> list[str]:
    """返回该票最近 lmt 根日 K 的原始行；网络/解析失败返回空表（不抛）。"""
    try:
        resp = session.get(
            _FFLOW_URL,
            params={"secid": secid_for(code), **_FFLOW_PARAMS},
            timeout=(10, 20),
        )
        resp.raise_for_status()
        klines = (resp.json().get("data") or {}).get("klines") or []
        return [k for k in klines if isinstance(k, str)]
    except Exception as e:  # noqa: BLE001 — 单票失败不得中断整轮回补
        logger.debug(f"  {code} 抓取失败: {e}")
        return []


def _is_weekend(day: str) -> bool:
    return date.fromisoformat(day).weekday() >= 5


def find_gaps(db_path: Path, since: str) -> dict[str, set[str]]:
    """返回 {code: {本地缺失的交易日}}，universe 取 daily_bars。"""
    conn = sqlite3.connect(str(db_path), timeout=30.0)
    try:
        rows = conn.execute(
            """
            SELECT d.ts_code, d.trade_date
            FROM daily_bars d
            WHERE d.trade_date >= ?
              AND NOT EXISTS (
                  SELECT 1 FROM fund_flow f
                  WHERE f.ts_code = d.ts_code AND f.trade_date = d.trade_date
              )
              AND strftime('%w', d.trade_date) NOT IN ('0', '6')
            """,
            (since,),
        ).fetchall()
    finally:
        conn.close()
    gaps: dict[str, set[str]] = {}
    for code, day in rows:
        gaps.setdefault(str(code).strip(), set()).add(str(day))
    return gaps


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH, help="数据库路径")
    parser.add_argument(
        "--since",
        default="2026-09-18",
        help="只补该日期（含）之后的缺口（默认：东财榜单端点开始失败的日期）",
    )
    parser.add_argument("--limit", type=int, default=0, help="只处理前 N 只（0 = 全部）")
    parser.add_argument("--workers", type=int, default=8, help="并发线程数（默认 8）")
    parser.add_argument("--dry-run", action="store_true", help="只打印统计，不写库")
    args = parser.parse_args()

    if not args.db.exists():
        logger.error(f"❌ 数据库不存在: {args.db}")
        return 1

    gaps = find_gaps(args.db, args.since)
    if not gaps:
        logger.info(f"✅ {args.since} 起没有 fund_flow 缺口，无需回补")
        return 0

    codes = sorted(gaps)
    total_days = sum(len(v) for v in gaps.values())
    logger.info(
        f"🔍 发现缺口: {len(codes)} 只 / {total_days} 个 (票, 日) 对"
        f"（{args.since} 起）"
    )
    if args.limit:
        codes = codes[: args.limit]
        logger.info(f"  --limit {args.limit} → 本轮只处理 {len(codes)} 只")

    session = requests.Session()
    session.trust_env = False  # 东财在环境代理下会拒连（实测 ProxyError）

    records: list[dict[str, object]] = []
    failed: list[str] = []
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futures = {
            pool.submit(fetch_code_history, session, code): code for code in codes
        }
        for fut in as_completed(futures):
            code = futures[fut]
            want = gaps[code]
            got = fut.result()
            if not got:
                failed.append(code)
                continue
            hit = 0
            for raw in got:
                rec = parse_kline(raw)
                if rec is None:
                    continue
                day = str(rec["date"])
                if day not in want or _is_weekend(day):
                    continue
                rec["symbol"] = code
                records.append(rec)
                hit += 1
            if not hit:
                failed.append(code)

    if failed:
        logger.warning(f"⚠️ {len(failed)} 只未取到可用日 K（停牌/退市/源端无数据）")

    if not records:
        logger.warning("⚠️ 没有可回补的记录")
        return 0

    if args.dry_run:
        logger.info(
            f"🧪 dry-run：将写入 {len(records)} 行 / "
            f"{len({r['symbol'] for r in records})} 只（未写库）"
        )
        for rec in sorted(records, key=lambda r: (str(r["symbol"]), str(r["date"])))[:10]:
            logger.info(f"    {rec['symbol']} {rec['date']} 主力={rec['main_net_inflow']}")
        if len(records) > 10:
            logger.info(f"    … 其余 {len(records) - 10} 行省略")
        return 0

    db = SmartMoneyDBProvider(db_path=str(args.db))
    saved = 0
    # 分批提交：单批过大时一次事务要持锁很久，会撞上并发跑的管道
    for start in range(0, len(records), 500):
        saved += db.save_fund_flow_batch(records[start : start + 500])
    logger.info(f"✅ 回填完成: 写入 {saved} 行")
    return 0


if __name__ == "__main__":
    sys.exit(main())
