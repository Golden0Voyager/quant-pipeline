"""东方财富筹码分布 — 五源级联（EM → numpy → 雪球 → 新浪 → JS 降级）。

替代 AkShare 的 ``ak.stock_cyq_em``，解决 ``push2his.eastmoney.com``
TLS 指纹阻断及 IP 封禁后无数据可用的问题。

级联策略
--------
1. **EM API**（``curl_cffi`` + ``impersonate='chrome110'`` 绕 JA3，已封禁但保留兜底）
2. **本地 DB → numpy 计算** — 读 ``daily_bars``，纯向量化，零网络（**主路径**）
3. **雪球 API**（``smartmoney_hunter.xueqiu``，需 ``XUEQIU_TOKEN``）
4. **新浪 API**（``akshare.stock_zh_a_daily``，返回换手率）
5. **本地 DB → JS 计算** — numpy 因数据不足未能输出时降级

第 2 路使用 numpy 实现与 JS 完全一致的 120 根滑窗三角形分配算法，
速度快 100x 以上（~30ms/股 vs ~7s/股），且不依赖外部 API。

用法
----
.. code-block:: python

    from core.stock_cyq_em import stock_cyq_em
    df = stock_cyq_em("000001")
"""

from __future__ import annotations

import http.client
import logging
import random
import sqlite3
import threading
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from curl_cffi import exceptions as curl_exc
from curl_cffi.requests import get as curl_get
from py_mini_racer import MiniRacer

logger = logging.getLogger(__name__)


class InsufficientDataError(ValueError):
    """本地数据前置条件不满足（如换手率历史缺失），无法计算筹码。

    与网络/源端失败区分开：调用方应按"跳过"处理，
    不应计入连续失败触发冷却与熔断（2026-08-02：约 330 只
    北交所因换手率缺失被当作失败，触发连环冷却爬行半小时）。
    """


# ── 重试配置 ──────────────────────────────────────────────
_RETRY_TIMES = 3
_RETRY_BASE_SLEEP = 2.0
_RETRY_MAX_SLEEP = 15.0
_RETRYABLE_EXCEPTIONS = (
    curl_exc.CurlError,
    curl_exc.ConnectionError,
    curl_exc.Timeout,
    curl_exc.ProxyError,
    http.client.RemoteDisconnected,
    ConnectionError,
    TimeoutError,
)

# 默认 DB 路径
_DEFAULT_DB_PATH = Path.home() / "Code" / "quant_data" / "quant_core.db"

# 最近 K 线笔数（JS 算法内部 range）
_RANGE = 120


# ═══════════════════════════════════════════════════════════
# Numpy 筹码分布计算（JS 算法的纯向量化移植，快 100x）
# ═══════════════════════════════════════════════════════════


def _calculate_chip_numpy(kline_data: list[dict]) -> pd.DataFrame:
    """纯 numpy 实现的 CYQCalculator，精确匹配 JS 算法。

    逻辑完全一致：120 根 K 线滑窗、三角形筹码分配、150 档价格区间。
    返回值 DataFrame 列名与 ``stock_cyq_em`` 中文输出一致。

    Parameters
    ----------
    kline_data : list[dict]
        每笔含 date/open/close/high/low/volume/turnover_rate（百分数）。

    Returns
    -------
    pd.DataFrame
        列: 日期 / 获利比例 / 平均成本 / 90成本-低 / 90成本-高
            / 90集中度 / 70成本-低 / 70成本-高 / 70集中度
    """
    n = len(kline_data)
    _factor = 150
    _range = 120

    opens = np.array([r["open"] for r in kline_data], dtype=np.float64)
    closes = np.array([r["close"] for r in kline_data], dtype=np.float64)
    highs = np.array([r["high"] for r in kline_data], dtype=np.float64)
    lows = np.array([r["low"] for r in kline_data], dtype=np.float64)
    tr_arr = np.array(
        [r.get("turnover_rate", 0.0) or 0.0 for r in kline_data], dtype=np.float64
    )
    dates = [r["date"] for r in kline_data]

    # 预分配输出列
    out: dict[str, np.ndarray] = {
        "获利比例": np.full(n, np.nan, dtype=np.float64),
        "平均成本": np.full(n, np.nan, dtype=np.float64),
        "90成本-低": np.full(n, np.nan, dtype=np.float64),
        "90成本-高": np.full(n, np.nan, dtype=np.float64),
        "90集中度": np.full(n, np.nan, dtype=np.float64),
        "70成本-低": np.full(n, np.nan, dtype=np.float64),
        "70成本-高": np.full(n, np.nan, dtype=np.float64),
        "70集中度": np.full(n, np.nan, dtype=np.float64),
    }

    for idx in range(n):
        start = max(0, idx - _range + 1)

        # 滑窗价格范围（同 JS：maxprice / minprice 从 window 中取）
        maxprice = float(highs[start : idx + 1].max())
        minprice = float(lows[start : idx + 1].min())

        if maxprice - minprice < 1e-8:
            continue  # 全部同价，跳过（JS 会抛异常）

        accuracy = max(0.01, (maxprice - minprice) / (_factor - 1))
        bin_prices = minprice + accuracy * np.arange(_factor, dtype=np.float64)

        distribution = np.zeros(_factor, dtype=np.float64)

        # 逐根 K 线处理（由远及近，同 JS 逻辑）
        for j in range(idx - start + 1):
            p = start + j
            tr = min(1.0, (tr_arr[p] or 0.0) / 100.0)

            h_bin = int(np.floor((highs[p] - minprice) / accuracy))
            lo_bin = int(np.ceil((lows[p] - minprice) / accuracy))
            h_bin = min(h_bin, _factor - 1)
            lo_bin = max(lo_bin, 0)

            if h_bin < lo_bin:
                continue

            # 衰减既有筹码
            distribution *= 1.0 - tr

            if highs[p] == lows[p]:
                # 同价 K 线：筹码集中到一个 bin
                avg_bin = int(
                    np.floor(
                        (
                            (opens[p] + closes[p] + highs[p] + lows[p]) / 4.0
                            - minprice
                        )
                        / accuracy
                    )
                )
                avg_bin = max(0, min(avg_bin, _factor - 1))
                distribution[avg_bin] += float(_factor - 1) * tr / 2.0
            else:
                avg_val = (opens[p] + closes[p] + highs[p] + lows[p]) / 4.0
                gp0 = 2.0 / (highs[p] - lows[p])
                bprices = bin_prices[lo_bin : h_bin + 1]

                # 三角形筹码分配（左侧：低价 → 均价，右侧：均价 → 高价）
                left = bprices <= avg_val
                if left.any():
                    if abs(avg_val - lows[p]) < 1e-8:
                        w = np.full(left.sum(), gp0 * tr, dtype=np.float64)
                    else:
                        w = (bprices[left] - lows[p]) / (avg_val - lows[p]) * gp0 * tr
                    distribution[lo_bin : h_bin + 1][left] += w

                right = bprices > avg_val
                if right.any():
                    if abs(highs[p] - avg_val) < 1e-8:
                        w = np.full(right.sum(), gp0 * tr, dtype=np.float64)
                    else:
                        w = (highs[p] - bprices[right]) / (highs[p] - avg_val) * gp0 * tr
                    distribution[lo_bin : h_bin + 1][right] += w

        # ── 从最终筹码分布提取统计量（同 JS gcc / cpc 逻辑）──
        total = distribution.sum()
        if total < 1e-12:
            continue

        cp = float(closes[idx])

        # 获利比例
        profit_ratio = float(np.sum(distribution[bin_prices <= cp]) / total)

        # CDF
        cdf = np.cumsum(distribution) / total

        def _gcc(portion: float, _cdf=cdf, _bins=bin_prices) -> float:
            i = int(np.searchsorted(_cdf, portion))
            i = min(i, _factor - 1)
            return float(_bins[i])

        avg_cost = _gcc(0.5)

        # 90% / 70% 成本区间及集中度
        def _cpc(pct: float) -> tuple[float, float, float]:
            p_low = (1.0 - pct) / 2.0
            p_high = (1.0 + pct) / 2.0
            pr_low = _gcc(p_low)
            pr_high = _gcc(p_high)
            con = 0.0 if abs(pr_low + pr_high) < 1e-12 else (pr_high - pr_low) / (pr_low + pr_high)
            return pr_low, pr_high, con

        c90_l, c90_h, cn90_v = _cpc(0.9)
        c70_l, c70_h, cn70_v = _cpc(0.7)

        out["获利比例"][idx] = profit_ratio
        out["平均成本"][idx] = avg_cost
        out["90成本-低"][idx] = c90_l
        out["90成本-高"][idx] = c90_h
        out["90集中度"][idx] = cn90_v
        out["70成本-低"][idx] = c70_l
        out["70成本-高"][idx] = c70_h
        out["70集中度"][idx] = cn70_v

    result_df = pd.DataFrame(out)
    result_df.insert(0, "日期", dates)
    result_df["日期"] = pd.to_datetime(result_df["日期"], errors="coerce").dt.date
    return result_df


# ═══════════════════════════════════════════════════════════
# JS 筹码分布算法（py_mini_racer 执行，保留为 fallback）
# ═══════════════════════════════════════════════════════════


def _js_code() -> str:
    """返回 CYQCalculator JS 函数源码（单次 eval，供多次调用）。"""
    return r"""
function CYQCalculator(index, klinedata) {
    var range = 120;
    var maxprice = 0, minprice = 0, factor = 150;
    var start = range ? Math.max(0, index - range + 1) : 0;
    var kdata = klinedata.slice(start, Math.max(1, index + 1));
    if (kdata.length === 0) throw 'invalid index';
    for (var i = 0; i < kdata.length; i++) {
        var e = kdata[i];
        maxprice = !maxprice ? e.high : Math.max(maxprice, e.high);
        minprice = !minprice ? e.low : Math.min(minprice, e.low);
    }
    var accuracy = Math.max(0.01, (maxprice - minprice) / (factor - 1));
    var yrange = [];
    for (var i = 0; i < factor; i++) yrange.push((minprice + accuracy * i).toFixed(2) / 1);
    var xdata = createNumberArray(factor);
    for (var i = 0; i < kdata.length; i++) {
        var e = kdata[i];
        var avg = (e.open + e.close + e.high + e.low) / 4;
        var tr = Math.min(1, (e.hsl || e.turnover_rate || 0) / 100 || 0);
        var H = Math.floor((e.high - minprice) / accuracy);
        var L = Math.ceil((e.low - minprice) / accuracy);
        var gp = [e.high == e.low ? factor - 1 : 2 / (e.high - e.low),
                   Math.floor((avg - minprice) / accuracy)];
        for (var n = 0; n < xdata.length; n++) xdata[n] *= (1 - tr);
        if (e.high == e.low) {
            xdata[gp[1]] += gp[0] * tr / 2;
        } else {
            for (var j = L; j <= H; j++) {
                var cp = minprice + accuracy * j;
                if (cp <= avg) {
                    xdata[j] += Math.abs(avg - e.low) < 1e-8
                        ? gp[0] * tr
                        : (cp - e.low) / (avg - e.low) * gp[0] * tr;
                } else {
                    xdata[j] += Math.abs(e.high - avg) < 1e-8
                        ? gp[0] * tr
                        : (e.high - cp) / (e.high - avg) * gp[0] * tr;
                }
            }
        }
    }
    var cp = kdata[kdata.length - 1].close;
    var total = 0;
    for (var i = 0; i < factor; i++) { var x = xdata[i].toPrecision(12) / 1; total += x; }
    function gcc(chip) {
        var s = 0;
        for (var i = 0; i < factor; i++) {
            var x = xdata[i].toPrecision(12) / 1;
            if (s + x > chip) return minprice + i * accuracy;
            s += x;
        }
        return 0;
    }
    var bp = total == 0 ? 0 : (function() {
        var b = 0;
        for (var i = 0; i < factor; i++) {
            if (cp >= minprice + i * accuracy) b += xdata[i].toPrecision(12) / 1;
        }
        return b / total;
    })();
    var ac = gcc(total * 0.5).toFixed(2);
    function cpc(pct) {
        var ps = [(1 - pct) / 2, (1 + pct) / 2];
        var pr = [gcc(total * ps[0]), gcc(total * ps[1])];
        return {
            pr: [pr[0].toFixed(2), pr[1].toFixed(2)],
            con: pr[0] + pr[1] === 0 ? 0 : (pr[1] - pr[0]) / (pr[0] + pr[1])
        };
    }
    var p90 = cpc(0.9), p70 = cpc(0.7);
    return {bp: bp, ac: ac,
            c90l: p90.pr[0], c90h: p90.pr[1], cn90: p90.con,
            c70l: p70.pr[0], c70h: p70.pr[1], cn70: p70.con};
}
function createNumberArray(count) { var a = []; for (var i = 0; i < count; i++) a.push(0); return a; }
"""


_CYQ_JS: MiniRacer | None = None
_JS_INITIALIZED = False
_JS_LOCK = threading.Lock()


def _get_js_runtime() -> MiniRacer | None:
    """延迟初始化 MiniRacer JS 运行时，避免 fork 前初始化导致 V8 崩溃。"""
    global _CYQ_JS, _JS_INITIALIZED
    with _JS_LOCK:
        if _JS_INITIALIZED:
            return _CYQ_JS
        _JS_INITIALIZED = True
        try:
            runtime = MiniRacer()
            runtime.eval(_js_code())
            _CYQ_JS = runtime
        except Exception as exc:
            logger.warning("MiniRacer/JS 不可用，JS 计算路径将降级: %s", exc)
    return _CYQ_JS


def _calc_one_day(kline: list[dict], index: int) -> dict:
    """对单日 K 线切片执行 CYQCalculator。"""
    js_env = _get_js_runtime()
    if js_env is None:
        raise RuntimeError("MiniRacer 未初始化，JS 计算路径不可用")
    return js_env.call("CYQCalculator", index, kline)


# ═══════════════════════════════════════════════════════════
# K 线获取：三源级联
# ═══════════════════════════════════════════════════════════

# ── 源 1：EM API（curl_cffi） ────────────────────────────


def _fetch_kline_em(symbol: str, adjust: str = "") -> list[dict] | None:
    """通过 curl_cffi 从东方财富线上获取 K 线数据。"""
    adjust_dict = {"qfq": "1", "hfq": "2", "": "0"}
    market_code = 1 if symbol.startswith("6") else 0
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    params: dict = {
        "secid": f"{market_code}.{symbol}",
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": "101",
        "fqt": adjust_dict[adjust],
        "end": datetime.now().date().strftime("%Y%m%d"),
        "lmt": "210",
    }
    for attempt in range(_RETRY_TIMES):
        try:
            r = curl_get(
                url,
                params=params,
                impersonate="chrome110",
                timeout=10,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36"
                    ),
                    "Referer": "https://quote.eastmoney.com/",
                },
            )
            data_json = r.json()
            if "data" not in data_json or "klines" not in data_json["data"]:
                return None
            raw = data_json["data"]["klines"]
            break
        except _RETRYABLE_EXCEPTIONS:
            if attempt == _RETRY_TIMES - 1:
                logger.debug("EM API 全部重试失败")
                return None
            sleep_sec = min(
                _RETRY_BASE_SLEEP * (2**attempt) * random.uniform(0.75, 1.25),
                _RETRY_MAX_SLEEP,
            )
            time.sleep(sleep_sec)
    else:
        return None

    return _parse_kline_raw_em(raw)


def _parse_kline_raw_em(raw: list[str]) -> list[dict]:
    """解析 EM K 线 CSV 行为 dict 列表。"""
    cols = [
        "date",
        "open",
        "close",
        "high",
        "low",
        "volume",
        "volume_money",
        "zf",
        "zdf",
        "zde",
        "hsl",
    ]
    records = []
    for item in raw:
        parts = item.split(",")
        rec: dict = {}
        for i, col in enumerate(cols):
            try:
                rec[col] = float(parts[i]) if i > 0 else parts[i]
            except (ValueError, IndexError):
                rec[col] = 0.0 if i > 0 else ""
        # 统一字段名：hsl → turnover_rate
        rec["turnover_rate"] = rec.pop("hsl", 0.0)
        records.append(rec)
    return records


# ── 源 2：雪球 API（smartmoney_hunter 封装） ─────────────


def _fetch_kline_xueqiu(symbol: str) -> list[dict] | None:
    """通过雪球 API 获取 K 线数据（含换手率，百分数）。

    Requires ``XUEQIU_TOKEN`` 环境变量。
    """
    try:
        from smartmoney_hunter import xueqiu as xq  # noqa: PLC0415
    except ImportError:
        return None

    try:
        df = xq.get_daily_bars(symbol)
    except Exception as e:
        logger.debug("雪球 API 请求失败 %s: %s", symbol, e)
        return None

    if df is None or df.empty:
        return None

    df = df.sort_values("date").reset_index(drop=True)
    records = []
    for _, row in df.iterrows():
        tr = float(row["turnover"]) if pd.notna(row.get("turnover")) else 0.0
        records.append(
            {
                "date": str(row["date"])[:10],
                "open": float(row["open"]),
                "close": float(row["close"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "volume": float(row["volume"]),
                "turnover_rate": tr,
            }
        )
    return records


# ── 源 3：新浪 API（akshare） ────────────────────────────


def _fetch_kline_sina(symbol: str) -> list[dict] | None:
    """通过 akshare 新浪接口获取 K 线数据（含换手率）。"""
    import akshare as ak  # noqa: PLC0415

    # 转换 code 到新浪格式
    if symbol.startswith("6"):
        sina_symbol = f"sh{symbol}"
    elif symbol.startswith("0") or symbol.startswith("3"):
        sina_symbol = f"sz{symbol}"
    elif symbol.startswith("8") or symbol.startswith("4") or symbol.startswith("920"):
        sina_symbol = f"bj{symbol}"
    else:
        sina_symbol = symbol

    try:
        df = ak.stock_zh_a_daily(
            symbol=sina_symbol,
            adjust="",
        )
    except Exception as e:
        logger.debug(f"新浪 API 请求失败: {e}")
        return None

    if df.empty or "turnover" not in df.columns:
        return None

    df = df.sort_values("date").reset_index(drop=True)

    # 新浪换手率为小数比率（如 0.005586），统一转为百分数
    df["turnover"] = pd.to_numeric(df["turnover"], errors="coerce")
    if not df["turnover"].empty and df["turnover"].max() < 1.0:
        df["turnover"] = df["turnover"] * 100

    records = []
    for _, row in df.iterrows():
        records.append(
            {
                "date": str(row["date"])[:10],
                "open": float(row["open"]),
                "close": float(row["close"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "volume": float(row["volume"]),
                "turnover_rate": float(row["turnover"]) if pd.notna(row["turnover"]) else 0.0,
            }
        )
    return records


# ── 源 3：本地 DB ─────────────────────────────────────────


def _fetch_kline_db(symbol: str, db_path: str) -> list[dict] | None:
    """从本地 quant_core.db 的 daily_bars 表读取 K 线数据。"""
    ts_code = symbol.replace(".SH", "").replace(".SZ", "").replace(".BJ", "")
    try:
        conn = sqlite3.connect(db_path)
        rows = conn.execute(
            """SELECT trade_date, open, close, high, low, volume, turnover_rate
               FROM daily_bars
               WHERE ts_code = ?
               ORDER BY trade_date ASC""",
            (ts_code,),
        ).fetchall()
        conn.close()
    except Exception:
        return None

    if not rows:
        return None

    records = []
    for r in rows:
        tr = float(r[6]) if r[6] is not None else 0.0
        records.append(
            {
                "date": str(r[0])[:10],
                "open": float(r[1]),
                "close": float(r[2]),
                "high": float(r[3]),
                "low": float(r[4]),
                "volume": float(r[5]),
                "turnover_rate": tr,
            }
        )
    return records


# ═══════════════════════════════════════════════════════════
# 公开 API
# ═══════════════════════════════════════════════════════════


def stock_cyq_em(
    symbol: str,
    adjust: str = "",
    use_local_db: bool = True,
    db_path: str | Path | None = None,
) -> pd.DataFrame:
    """获取个股筹码分布。

    级联策略（自动回退，越快越优先）：
      1. **EM API** — ``curl_cffi`` + ``impersonate='chrome110'``（已封禁，保留仅作兜底）
      2. **本地 DB → numpy 计算** — 读 ``daily_bars``，纯向量化，零网络开销
      3. **雪球 API** — ``smartmoney_hunter.xueqiu``（需 ``XUEQIU_TOKEN``）
      4. **新浪 API** — ``akshare.stock_zh_a_daily``（含换手率）
      5. **本地 DB → JS 计算** — 当 numpy 路径因数据不足未能输出时降级

    Parameters
    ----------
    symbol : str
        股票代码（如 ``"000001"``），可不带交易所后缀。
    adjust : str
        复权类型（仅 EM API 有效）：``""`` 不复权, ``"qfq"`` 前复权。
    use_local_db : bool
        是否启用本地 DB 回退（默认启用）。
    db_path : str | Path | None
        quant_core.db 路径，默认 ``~/Code/quant_data/quant_core.db``。

    Returns
    -------
    pd.DataFrame
        筹码分布数据（最近 90 天）。

    Raises
    ------
    ConnectionError
        所有数据源均失败。
    """
    ts_code = symbol.replace(".SH", "").replace(".SZ", "").replace(".BJ", "")

    # ── 级联获取 K 线数据 ──────────────────────────────────
    kline_data: list[dict] | None = None
    src: str | None = None  # 数据源名称

    # 1) 本地 DB — numpy 主路径（零网络，~30ms/股）。
    #    EM API 自 2026-07 被封禁后，线上优先会让每支股票空耗 ~11s 重试，
    #    1808 支跑不完就被调度器杀掉（筹码滞后 6 天事故），故本地优先。
    if use_local_db:
        db = db_path or _DEFAULT_DB_PATH
        kline_data = _fetch_kline_db(ts_code, str(db))
        if kline_data is not None and len(kline_data) >= 120:
            src = "DB_numpy"
        else:
            # K 线不足 120 根无法计算筹码，继续尝试线上源
            kline_data = None

    # 2) EM API（已封禁，保留兜底）
    if kline_data is None:
        kline_data = _fetch_kline_em(ts_code, adjust=adjust)
        if kline_data is not None:
            src = "EM"

    # 3) 雪球 API
    if kline_data is None:
        kline_data = _fetch_kline_xueqiu(ts_code)
        if kline_data is not None:
            src = "Xueqiu"

    # 4) 新浪 API
    if kline_data is None:
        kline_data = _fetch_kline_sina(ts_code)
        if kline_data is not None:
            src = "Sina"

    # （原第 5 路 "DB→JS 降级" 已移除：DB 数据在第 1 路已尝试过，
    #   不足 120 根时 JS 路径同样无法产出有效筹码，只会输出全零伪数据）

    if kline_data is None:
        raise ConnectionError(f"{symbol}: EM/雪球/新浪/DB 均无法获取 K 线数据")

    # ── 统一 K 线格式（取最近 240 条） ──
    _max_kline = 240
    kline_clean = [
        {
            "date": r["date"],
            "open": r["open"],
            "close": r["close"],
            "high": r["high"],
            "low": r["low"],
            "volume": r["volume"],
            "turnover_rate": r.get("turnover_rate", 0.0),
        }
        for r in kline_data[-_max_kline:]
    ]

    logger.debug("%s → %s (%d 条)", symbol, src, len(kline_clean))

    # ── 换手率有效性防线：换手率全缺失时任何算法都只会产出全零伪数据 ──
    # (2026-07-21 起 chip_distribution_em 全零事故根因：EM 封禁后降级到
    #  daily_bars，其 turnover_rate 全 NULL → JS 路径静默输出全 0 行)
    _recent = kline_clean[-_RANGE:]
    _tr_valid = sum(1 for r in _recent if (r.get("turnover_rate") or 0) > 0)
    if _tr_valid < max(1, len(_recent) // 2):
        raise InsufficientDataError(
            f"{symbol}: 换手率有效数据仅 {_tr_valid}/{len(_recent)} 条 (源: {src})，"
            "拒绝计算筹码分布以免产出全零伪数据"
        )

    # ── 计算筹码分布 ────────────────────────────────────────
    result_df: pd.DataFrame | None = None

    # numpy 路径（仅 DB 数据）
    if src == "DB_numpy":
        result_df = _calculate_chip_numpy(kline_clean)
        valid_cnt = result_df["获利比例"].notna().sum()
        if valid_cnt < 10:
            # numpy 输出无效说明这批 K 线数据本身不可用（换手率缺失/数据不足），
            # 同批数据交给 JS 只会静默输出全零行，直接报错而非降级
            raise InsufficientDataError(
                f"{symbol}: numpy 筹码计算无有效输出 (valid={valid_cnt})，"
                "K 线数据不足或换手率缺失"
            )

    # JS 路径（EM / 雪球 / 新浪 / DB_JS 降级）
    if src in ("EM", "Xueqiu", "Sina", "DB_JS"):
        n = len(kline_clean)
        date_list: list[str] = []
        profit_list: list[float] = []
        avg_cost_list: list[str] = []
        c90l: list[str] = []
        c90h: list[str] = []
        cn90: list[float] = []
        c70l: list[str] = []
        c70h: list[str] = []
        cn70: list[float] = []

        for i in range(n):
            result = _calc_one_day(kline_clean, i)
            date_list.append(kline_clean[i]["date"])
            profit_list.append(result["bp"])
            avg_cost_list.append(result["ac"])
            c90l.append(result["c90l"])
            c90h.append(result["c90h"])
            cn90.append(result["cn90"])
            c70l.append(result["c70l"])
            c70h.append(result["c70h"])
            cn70.append(result["cn70"])

        result_df = pd.DataFrame(
            {
                "日期": date_list,
                "获利比例": profit_list,
                "平均成本": avg_cost_list,
                "90成本-低": c90l,
                "90成本-高": c90h,
                "90集中度": cn90,
                "70成本-低": c70l,
                "70成本-高": c70h,
                "70集中度": cn70,
            }
        )

    # ── 统一后处理 ──
    assert result_df is not None, "result_df 未能计算"

    for col in result_df.columns[1:]:
        result_df[col] = pd.to_numeric(result_df[col], errors="coerce")

    # 全零行（JS 在筹码 total==0 时输出 bp=0/ac=0）视为无效数据，置 NaN。
    # 真实市场中获利比例可为 0，但平均成本必然 > 0，组合判断不会误伤。
    _zero_mask = (result_df["获利比例"] == 0) & (result_df["平均成本"] == 0)
    if _zero_mask.any():
        logger.debug("%s: %d 行全零筹码输出置为 NaN", symbol, int(_zero_mask.sum()))
        result_df.loc[_zero_mask, result_df.columns[1:]] = np.nan

    result_df["日期"] = pd.to_datetime(result_df["日期"], errors="coerce").dt.date
    result_df = result_df.iloc[-90:, :].copy()
    result_df.reset_index(inplace=True, drop=True)
    return result_df
