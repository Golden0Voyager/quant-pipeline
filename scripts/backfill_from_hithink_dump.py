#!/usr/bin/env python3
"""从同花顺 hithink market-dumps Parquet 回补历史日线。

用途：替代 scripts/backfill_to_6years.py 的逐股 akshare 慢速回补——
一次下载全市场 10 年日 K Parquet（不复权原始价）+ 复权因子事件流 Parquet，
本地按前复权公式算 qfq 后批量灌库，把 6 年级回补从小时级降到分钟级。

前复权公式（对每个除权事件：除权日 E，E 前一交易日原始收盘价 C_prev）：
    factor = (C_prev - D + P*R) / (C_prev * (1 + B + R))
    前复权价(date) = 原始价(date) × Π factor（对所有 ex_date > date 的事件累乘）
volume 不复权（与 akshare qfq 语义一致）。

安全约束：
- 默认只回补「历史不足 6 年且不在 scripts/.backfill_skip 中」的股票，不做全市场覆写
- --dry-run 只打印统计不写库
- 写库前抽样股票与库内已有 close 做 sanity check，偏差 >1% 仅 WARNING 不中止
  （hithink 官方前复权与 akshare 在除权窗口有已知口径差，实测 601899 ~1.3%）
"""
from __future__ import annotations

import argparse
import logging
import os
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

# 路径必须在导入本仓库模块之前设置：脚本直跑时 sys.path 只含 scripts/，
# import core 需要仓库根目录
_REPO_ROOT = str(Path(__file__).resolve().parent.parent)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402
import requests  # noqa: E402

import core.config  # noqa: E402,F401  # import 即加载 .env（不覆盖已有环境变量）
from core.source_hithink import HithinkClient  # noqa: E402

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)-7s | %(message)s')
logger = logging.getLogger(__name__)

DB_PATH = Path("~/Code/quant_data/quant_core.db").expanduser()
DUMP_ROOT = Path("~/Code/quant_data/hithink_dump").expanduser()
SKIP_FILE = Path(__file__).parent / ".backfill_skip"

BACKFILL_DAYS = 2190  # 6 年，与 backfill_to_6years.py 一致
DAILY_K_ENDPOINT = "/api/dump/market-dumps/daily-k/download-url"
FACTORS_ENDPOINT = "/api/dump/market-dumps/adjustment-factors/download-url"
COMMIT_BATCH = 200  # 每 200 只股票提交一次事务

INSERT_SQL = """
    INSERT OR REPLACE INTO daily_bars (
        ts_code, trade_date, open, close, high, low,
        volume, amount, turnover_rate, pct_change, amplitude,
        data_source, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

_EMPTY_EVENTS = pd.DataFrame(
    columns=["ex_date", "dividend_per_share", "per_share_bonus", "allotment_ratio", "allotment_price"]
)


def fmt_date(d: datetime) -> str:
    return d.strftime("%Y-%m-%d")


def load_skip_set() -> set[str]:
    if SKIP_FILE.exists():
        return {line.strip() for line in SKIP_FILE.read_text().splitlines() if line.strip()}
    return set()


def thscode_to_code(thscode: str) -> str | None:
    """``600519.SH`` → ``600519``；北交所仅保留 920 前缀，43/83/87 开头返回 None。"""
    code, _, suffix = thscode.partition(".")
    if not (len(code) == 6 and code.isdigit()):
        return None
    if suffix == "BJ" and not code.startswith("920"):
        return None
    return code


def get_download_url(client: HithinkClient, endpoint: str) -> str:
    """取 presigned URL（300 秒有效，取到后须立即下载）。"""
    data = client._get(endpoint)
    url = data.get("presigned_url")
    if not url:
        raise RuntimeError(f"{endpoint} 响应缺少 presigned_url: {list(data.keys())}")
    return str(url)


def download_file(url: str, dest: Path) -> None:
    """流式下载到本地文件（日 K dump 约几百 MB，避免一次性读入内存）。"""
    logger.info(f"⬇️  下载 {dest.name} ...")
    session = requests.Session()
    session.trust_env = False  # 国内端点，绕过系统代理
    with session.get(url, stream=True, timeout=600) as resp:
        resp.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1 << 20):
                f.write(chunk)
    size_mb = dest.stat().st_size / 1024 / 1024
    logger.info(f"  ✅ {dest.name} 下载完成 ({size_mb:.1f} MB)")


def ms_to_date(ms: pd.Series) -> pd.Series:
    """毫秒戳 → 日期字符串。date_ms 为 Asia/Shanghai 零点：先按 UTC 解析再转上海时区。"""
    return (
        pd.to_datetime(ms, unit="ms", utc=True)
        .dt.tz_convert("Asia/Shanghai")
        .dt.strftime("%Y-%m-%d")
    )


def compute_qfq(bars: pd.DataFrame, events: pd.DataFrame) -> pd.DataFrame:
    """按复权因子事件流计算前复权价。

    Args:
        bars: 单只股票的原始日 K，列含 date(str)/open/high/low/close/volume/amount
        events: 该股票的除权事件，列含 ex_date(str)/dividend_per_share(D)/
                per_share_bonus(B)/allotment_ratio(R)/allotment_price(P)

    Returns:
        按 date 升序的 DataFrame，open/high/low/close 已前复权（round 2，对齐
        akshare qfq 精度），volume/amount 保持原始值，新增 pct_change/amplitude。
    """
    bars = bars.sort_values("date").reset_index(drop=True)
    raw_close = bars["close"].to_numpy()
    dates = bars["date"].to_numpy()

    # 逐事件计算 factor：C_prev 取原始价序列中 E 之前最近一个交易日的 close
    multiplier = np.ones(len(bars))
    for ev in events.itertuples():
        mask = dates < ev.ex_date
        if not mask.any():
            continue  # E 之前无交易日（事件早于 dump 起点），无法取 C_prev，跳过
        c_prev = raw_close[mask][-1]
        denom = c_prev * (1 + ev.per_share_bonus + ev.allotment_ratio)
        if denom <= 0:
            continue
        factor = (c_prev - ev.dividend_per_share + ev.allotment_price * ev.allotment_ratio) / denom
        if factor <= 0:
            continue
        multiplier[mask] *= factor

    out = bars.copy()
    for col in ("open", "high", "low", "close"):
        out[col] = (out[col] * multiplier).round(2)
    prev_close = out["close"].shift(1)
    out["pct_change"] = ((out["close"] / prev_close - 1) * 100).round(2)
    out["amplitude"] = ((out["high"] - out["low"]) / prev_close * 100).round(2)
    return out


def load_target_codes(conn: sqlite3.Connection, target_start_str: str) -> list[str]:
    """目标股票：历史不足 6 年且不在跳过列表中（语义同 backfill_to_6years.py）。"""
    cursor = conn.cursor()
    cursor.execute("SELECT ts_code, MIN(trade_date) FROM daily_bars GROUP BY ts_code")
    stock_min_dates = {r[0]: r[1] for r in cursor.fetchall()}
    cursor.execute("SELECT code FROM stock_list")
    all_codes = {r[0] for r in cursor.fetchall()}

    skip_set = load_skip_set()
    if skip_set:
        logger.info(f"📋 跳过列表 {len(skip_set)} 只（确认无历史数据）")

    targets = []
    for code in sorted(all_codes):
        if code in skip_set:
            continue
        first_date = stock_min_dates.get(code)
        if not first_date:
            continue  # 库内无数据的不在本次回补范围（保持与 6 年脚本一致）
        if first_date > target_start_str:
            targets.append(code)
    return targets


def write_bars(conn: sqlite3.Connection, code: str, qfq: pd.DataFrame, now_str: str) -> int:
    """单只股票批量写库，返回写入行数（调用方控制事务提交节奏）。"""
    rows = [
        (
            code,
            r.date,
            r.open,
            r.close,
            r.high,
            r.low,
            r.volume,
            r.amount,
            None,  # hithink dump 无换手率，留 NULL 由后续修复任务补齐
            r.pct_change if pd.notna(r.pct_change) else None,
            r.amplitude if pd.notna(r.amplitude) else None,
            "hithink",
            now_str,
        )
        for r in qfq.itertuples()
    ]
    conn.executemany(INSERT_SQL, rows)
    return len(rows)


def sanity_check(
    conn: sqlite3.Connection,
    qfq_by_code: dict[str, pd.DataFrame],
    sample_size: int = 5,
) -> None:
    """抽样比对脚本 qfq 与库内已有 close，相对差 >1% 打印 WARNING（不中止）。"""
    cursor = conn.cursor()
    checked = 0
    for code, qfq in qfq_by_code.items():
        if checked >= sample_size:
            break
        latest_date = str(qfq["date"].iloc[-1])
        row = cursor.execute(
            "SELECT trade_date, close FROM daily_bars WHERE ts_code = ? AND trade_date <= ? "
            "ORDER BY trade_date DESC LIMIT 1",
            (code, latest_date),
        ).fetchone()
        if not row or not row[1]:
            continue
        db_date, db_close = row
        match = qfq.loc[qfq["date"] == db_date, "close"]
        if match.empty:
            continue
        qfq_close = float(match.iloc[0])
        rel_diff = abs(qfq_close - db_close) / db_close
        checked += 1
        if rel_diff > 0.01:
            logger.warning(
                f"  ⚠️  sanity check {code} @{db_date}: qfq={qfq_close} vs 库内={db_close} "
                f"相对差 {rel_diff:.2%}（除权窗口已知口径差，仅告警不中止）"
            )
        else:
            logger.info(f"  ✓ sanity check {code} @{db_date}: qfq={qfq_close} vs 库内={db_close} (差 {rel_diff:.3%})")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="从 hithink market-dumps Parquet 回补历史日线（前复权）")
    parser.add_argument("--symbols", type=str, default=None, help="逗号分隔的 6 位代码，覆盖默认目标集合")
    parser.add_argument("--dry-run", action="store_true", help="只打印统计，不写库")
    parser.add_argument("--keep-files", action="store_true", help="保留本次下载的 Parquet 文件（默认跑完删除）")
    parser.add_argument("--limit", type=int, default=None, help="测试模式：只处理前 N 只")
    parser.add_argument("--db-path", type=str, default=str(DB_PATH), help=argparse.SUPPRESS)
    parser.add_argument("--dump-root", type=str, default=str(DUMP_ROOT), help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    os.nice(10)

    db_path = Path(args.db_path).expanduser()
    if not db_path.exists():
        logger.error(f"数据库不存在: {db_path}")
        return 1

    conn = sqlite3.connect(str(db_path))

    target_start_str = fmt_date(datetime.now() - timedelta(days=BACKFILL_DAYS))
    if args.symbols:
        targets = [c.strip() for c in args.symbols.split(",") if c.strip()]
        logger.info(f"🎯 指定股票模式: {len(targets)} 只")
    else:
        targets = load_target_codes(conn, target_start_str)
        logger.info(f"🎯 回补目标: {len(targets)} 只（历史起点晚于 {target_start_str}）")
    if args.limit:
        targets = targets[:args.limit]
        logger.info(f"测试模式：只处理前 {args.limit} 只")
    if not targets:
        logger.info("✅ 无需回补")
        conn.close()
        return 0

    # 下载（当天目录内已有文件则复用，便于调试重跑）
    dump_dir = Path(args.dump_root).expanduser() / datetime.now().strftime("%Y%m%d")
    dump_dir.mkdir(parents=True, exist_ok=True)
    daily_path = dump_dir / "daily_k.parquet"
    factors_path = dump_dir / "factors.parquet"
    downloaded: list[Path] = []

    client = HithinkClient()
    for endpoint, path in ((DAILY_K_ENDPOINT, daily_path), (FACTORS_ENDPOINT, factors_path)):
        if path.exists():
            logger.info(f"♻️  复用已下载文件: {path.name}")
            continue
        url = get_download_url(client, endpoint)
        download_file(url, path)
        downloaded.append(path)

    logger.info("📖 读取 Parquet ...")
    daily = pq.read_table(daily_path).to_pandas()
    factors = pq.read_table(factors_path).to_pandas()
    logger.info(f"  日 K {len(daily)} 行 / {daily['thscode'].nunique()} 只，因子事件 {len(factors)} 行")

    daily["date"] = ms_to_date(daily["date_ms"])
    factors["ex_date"] = ms_to_date(factors["ex_date_ms"])

    # 代码转换并与目标集合取交集（thscode → 裸 6 位）
    daily["code"] = daily["thscode"].map(thscode_to_code)
    target_set = set(targets)
    daily = daily[daily["code"].isin(target_set)]
    logger.info(f"  命中目标 {daily['code'].nunique()}/{len(targets)} 只")
    if daily.empty:
        logger.info("✅ dump 中无目标股票数据")
        conn.close()
        return 0

    # 按股票分组算前复权
    # dict(groupby) 会误判 pandas 的 keys 协议（'str' object is not callable），只能用推导式
    events_by_code = {  # noqa: C416
        code: g for code, g in factors.assign(code=factors["thscode"].map(thscode_to_code)).groupby("code")
    }
    qfq_by_code: dict[str, pd.DataFrame] = {}
    for code, g in daily.groupby("code"):
        bars = g[["date", "open_price", "high_price", "low_price", "close_price", "volume", "turnover"]].rename(
            columns={
                "open_price": "open",
                "high_price": "high",
                "low_price": "low",
                "close_price": "close",
                "turnover": "amount",  # hithink turnover 字段是成交额
            }
        )
        events = events_by_code.get(code, _EMPTY_EVENTS)
        qfq_by_code[str(code)] = compute_qfq(bars, events)

    # 写库前抽样 sanity check（只读库，dry-run 也执行）
    sanity_check(conn, qfq_by_code)

    total_rows = sum(len(df) for df in qfq_by_code.values())
    if args.dry_run:
        logger.info(f"🧪 dry-run：将写入 {len(qfq_by_code)} 只 / {total_rows} 行（未写库）")
        for code in list(qfq_by_code)[:5]:
            df = qfq_by_code[code]
            logger.info(f"  {code}: {df['date'].iloc[0]} ~ {df['date'].iloc[-1]}, {len(df)} 行")
        conn.close()
        return 0

    now_str = datetime.now().strftime("%Y-%m-%d")
    written_stocks = 0
    written_rows = 0
    for i, (code, qfq) in enumerate(qfq_by_code.items(), 1):
        try:
            written_rows += write_bars(conn, code, qfq, now_str)
            written_stocks += 1
            if i % COMMIT_BATCH == 0:
                conn.commit()
                logger.info(f"  💾 已提交 {i}/{len(qfq_by_code)} 只")
        except Exception as e:
            conn.rollback()
            logger.error(f"  ❌ {code} 写库异常: {e}")
    conn.commit()
    conn.close()
    logger.info(f"✅ 回补完成：{written_stocks} 只 / {written_rows} 行")

    # 默认清理本次下载的大文件（复用的旧文件不动）
    if not args.keep_files:
        for path in downloaded:
            path.unlink(missing_ok=True)
            logger.info(f"🗑  已清理 {path.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
