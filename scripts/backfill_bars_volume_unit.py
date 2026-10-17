"""把 daily_bars 里以「手」为单位的 volume 换算成「股」。

为什么需要
----------
``daily_bars.volume`` 历史上混过两种单位：东财 spot 快照路径与腾讯
fallback 给的是**手**（1 手 = 100 股），逐股路径给的是**股**。写进同一列
后，同一只票在来源切换处会出现 100 倍断层（实测 000002 在 2026-10-13：
sina 105,863,456 → eastmoney_spot 994,937，而真实成交量三天稳定）。
生产库有 5,558 / 5,567 只股票的历史同时含两种单位，``indicators.vol_ma60``
因此建立在带断层的序列上。

判定依据：**不按 data_source 判定**
--------------------------------
``data_source`` 不足以标识单位，有两处实证：

1. 同一个标签在同一个月内单位都不同。``akshare`` 在 2026-05 及以前约 71.7%
   的行是手，2026-07 起几乎全是股——上游 ``stock_zh_a_hist`` 在 2026-06-04
   改过返回值单位（000002 当天从 901,298 跳到 181,654,444，201 倍）。
2. 比值 ≈100 有两种完全相反的含义，见 ``_classify`` 的注释。

所以逐行用「均价恒等式」判定：``amount / volume`` 必须等于均价（≈ close）。
手单位的行该比值是 close 的 ~100 倍，股单位的是 ~1 倍。

安全阀
------
默认 dry-run，必须显式 ``--apply`` 才写库。写前自动备份受影响行。
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from pathlib import Path

_REPO_ROOT = str(Path(__file__).resolve().parent.parent)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from core.config import DEFAULT_DB_PATH  # noqa: E402

# core.config 在导入时把 root logger 的 handler 换成 FileHandler（core/config.py:156），
# 输出会全部落进当天的运行日志而不是终端。脚本的盘点结果必须让运维看得见，
# 故在这里用 force=True 把 handler 抢回 stdout。
logging.basicConfig(level=logging.INFO, format="%(message)s", force=True)
logger = logging.getLogger(__name__)

# 均价恒等式的容差窗口。真实数据 amount/volume/close ≈ 1（考虑 VWAP 与收盘价
# 的差异、复权因子），手单位则 ≈ 100。留出宽容区间避免误伤复权价差得多的老数据。
_SHARE_MAX = 10.0
_LOT_MIN = 50.0

# 比值落在这条窄带里无法判定，单独报出来交人看，不自动改。
_AMBIGUOUS_LO = 10.0
_AMBIGUOUS_HI = 50.0


def _classify(ratio: float) -> str:
    """按 amount/volume/close 判定单位。

    ``ratio`` ≈ 1   → volume 是股，无需处理
    ``ratio`` ≈ 100 → **两种含义相反的情况**，需要靠 extra 区分：

      a) volume 是手 + amount 是真实成交额（VWAP ≈ 100×close）
      b) volume 是股 + amount 是合成的 close×volume×100

    腾讯 fallback 属于 (b)——它没有成交额，用 close×volume×100 造了一个，
    比值**恰好** 100.00。对 (b) 乘 100 会把成交量放大 100 倍。

    区分办法：真实成交额对应真实 VWAP，与收盘价有自然偏离，比值在 100 上下
    **散布**；合成的比值是精确的 100.00。故用 ``exactly_100`` 判别。
    """
    if ratio < _SHARE_MAX:
        return "shares"
    if ratio >= _LOT_MIN:
        return "lots_or_synthetic"
    return "ambiguous"


def survey(conn: sqlite3.Connection) -> dict[str, int]:
    """统计各类别的行数（只读）。"""
    q = f"""
        SELECT COUNT(*) FROM daily_bars
        WHERE close > 0 AND volume > 0 AND amount > 0
          AND amount / volume / close < {_SHARE_MAX}
    """
    shares = conn.execute(q).fetchone()[0]
    lot_like = conn.execute(
        f"""SELECT COUNT(*) FROM daily_bars
            WHERE close > 0 AND volume > 0 AND amount > 0
              AND amount/volume/close >= {_LOT_MIN}"""
    ).fetchone()[0]
    ambiguous = conn.execute(
        f"""SELECT COUNT(*) FROM daily_bars
            WHERE close > 0 AND volume > 0 AND amount > 0
              AND amount/volume/close >= {_SHARE_MAX}
              AND amount/volume/close < {_LOT_MIN}"""
    ).fetchone()[0]
    synthetic = conn.execute(
        f"""SELECT COUNT(*) FROM daily_bars
            WHERE close > 0 AND volume > 0 AND amount > 0
              AND ROUND(amount/volume/close, 2) = 100.00
              AND amount/volume/close >= {_LOT_MIN}"""
    ).fetchone()[0]
    return {
        "shares": shares,
        "lots_real_amount": lot_like - synthetic,
        "synthetic_amount": synthetic,
        "ambiguous": ambiguous,
        "total": conn.execute("SELECT COUNT(*) FROM daily_bars").fetchone()[0],
    }


def convert(conn: sqlite3.Connection, apply: bool) -> int:
    """把「手 + 真实成交额」的行乘 100。

    用 ``UPDATE ... WHERE`` 逐行判定而不是按 data_source 整批处理，见模块
    文档。合成 amount 的行**不动**（腾讯那 6 行，以及少量其它来源的合成行）——
    它们的 volume 已经是股。
    """
    sql = f"""
        UPDATE daily_bars
           SET volume = volume * 100
         WHERE id IN (
               SELECT id FROM daily_bars
                WHERE close > 0 AND volume > 0 AND amount > 0
                  AND amount/volume/close >= {_LOT_MIN}
                  AND ROUND(amount/volume/close, 2) <> 100.00
             )
    """
    if not apply:
        cur = conn.execute(
            f"""SELECT COUNT(*) FROM daily_bars
                WHERE close > 0 AND volume > 0 AND amount > 0
                  AND amount/volume/close >= {_LOT_MIN}
                  AND ROUND(amount/volume/close, 2) <> 100.00"""
        )
        return cur.fetchone()[0]
    return conn.execute(sql).rowcount


def backup(conn: sqlite3.Connection, db_path: Path) -> Path:
    """把受影响行整表备份，便于回滚。"""
    stamp = conn.execute("SELECT datetime('now')").fetchone()[0].replace(":", "").replace(" ", "_")
    path = db_path.with_name(f"daily_bars_volume_backup_{stamp}.sqlite")
    conn.execute("DROP TABLE IF EXISTS volume_unit_backup")
    conn.execute(
        f"""CREATE TABLE volume_unit_backup AS SELECT id, ts_code, trade_date, volume, data_source
            FROM daily_bars
            WHERE close > 0 AND volume > 0 AND amount > 0
              AND amount/volume/close >= {_LOT_MIN}
              AND ROUND(amount/volume/close, 2) <> 100.00"""
    )
    conn.commit()
    return path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH, help="数据库路径")
    parser.add_argument("--apply", action="store_true", help="真正写库（默认只演练）")
    args = parser.parse_args()

    if not args.db.exists():
        logger.error(f"数据库不存在: {args.db}")
        return 1

    conn = sqlite3.connect(args.db, isolation_level=None)
    conn.execute("PRAGMA busy_timeout=30000")
    try:
        before = survey(conn)
        logger.info("=" * 62)
        logger.info("daily_bars.volume 单位盘点")
        logger.info("=" * 62)
        logger.info(f"  总行数              {before['total']:>10,}")
        logger.info(f"  已是股              {before['shares']:>10,}")
        logger.info(f"  手 + 真实成交额     {before['lots_real_amount']:>10,}   ← 将 ×100")
        logger.info(f"  合成成交额(不动)    {before['synthetic_amount']:>10,}")
        logger.info(f"  无法判定(不动)      {before['ambiguous']:>10,}")

        affected = convert(conn, apply=False)
        logger.info("")
        logger.info(f"将受影响行数: {affected:,}")

        if not args.apply:
            logger.info("")
            logger.info("演练模式，未写库。确认无误后加 --apply 执行。")
            return 0

        if affected == 0:
            logger.info("无需处理。")
            return 0

        bpath = backup(conn, args.db)
        logger.info(f"已备份受影响行 → {bpath}")
        conn.execute("BEGIN IMMEDIATE")
        try:
            n = convert(conn, apply=True)
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        logger.info(f"已换算 {n:,} 行（手 → 股）")

        after = survey(conn)
        logger.info("")
        logger.info("换算后：")
        logger.info(f"  已是股              {after['shares']:>10,}")
        logger.info(f"  手 + 真实成交额     {after['lots_real_amount']:>10,}   ← 应为 0")
        leftover = after["lots_real_amount"]
        if leftover:
            logger.error(f"仍有 {leftover:,} 行未归一，请复查")
            return 1
        logger.info("✅ 单位已全库一致")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
