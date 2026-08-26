"""hithink market-dumps Parquet 回补脚本的测试（全离线：合成 parquet + 临时 sqlite）。"""

from __future__ import annotations

import shutil
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import scripts.backfill_from_hithink_dump as bf


def _date_ms(date_str: str) -> int:
    """日期字符串 → Asia/Shanghai 零点的毫秒戳（与 dump 的 date_ms 语义一致）。"""
    dt = pd.Timestamp(date_str, tz="Asia/Shanghai")
    return int(dt.timestamp() * 1000)


def _write_daily_parquet(path: Path, rows: list[dict]) -> None:
    pq.write_table(pa.Table.from_pylist(rows), path)


def _write_factors_parquet(path: Path, rows: list[dict]) -> None:
    # 空表也要带 schema（from_pylist([]) 会丢掉列）
    schema = pa.schema([
        ("thscode", pa.string()),
        ("ticker", pa.string()),
        ("ex_date_ms", pa.int64()),
        ("dividend_per_share", pa.float64()),
        ("per_share_bonus", pa.float64()),
        ("allotment_ratio", pa.float64()),
        ("allotment_price", pa.float64()),
        ("currency", pa.string()),
    ])
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)


def _make_db(path: Path, stocks: list[str], existing: dict[str, list[tuple[str, float]]]) -> None:
    """造一个最小可用的测试库：stock_list + daily_bars（列与生产库一致）。"""
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE stock_list (code TEXT, name TEXT)")
        conn.executemany("INSERT INTO stock_list (code) VALUES (?)", [(s,) for s in stocks])
        conn.execute(
            """
            CREATE TABLE daily_bars (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_code TEXT NOT NULL,
                trade_date DATE NOT NULL,
                open REAL, close REAL, high REAL, low REAL,
                volume REAL, amount REAL, turnover_rate REAL,
                pct_change REAL, amplitude REAL,
                data_source TEXT, updated_at DATETIME,
                UNIQUE(ts_code, trade_date)
            )
            """
        )
        for code, bars in existing.items():
            conn.executemany(
                "INSERT INTO daily_bars (ts_code, trade_date, close, data_source) VALUES (?, ?, ?, 'akshare')",
                [(code, d, c) for d, c in bars],
            )


def _daily_rows(thscode: str, bars: list[tuple[str, float, float]]) -> list[dict]:
    """bars: (date, close, volume)；open/high/low 围绕 close 造，turnover 任意。"""
    return [
        {
            "thscode": thscode,
            "currency": "CNY",
            "interval": "1d",
            "adjusted": "none",
            "date_ms": _date_ms(d),
            "open_price": c - 0.5,
            "high_price": c + 1.0,
            "low_price": c - 1.0,
            "close_price": c,
            "volume": v,
            "turnover": c * v,
        }
        for d, c, v in bars
    ]


def _factor_row(thscode: str, ex_date: str, dividend: float = 0.0, bonus: float = 0.0,
                ratio: float = 0.0, price: float = 0.0) -> dict:
    return {
        "thscode": thscode,
        "ticker": thscode[:6],
        "ex_date_ms": _date_ms(ex_date),
        "dividend_per_share": dividend,
        "per_share_bonus": bonus,
        "allotment_ratio": ratio,
        "allotment_price": price,
        "currency": "CNY",
    }


# ---------------------------------------------------------------- thscode 转换


def test_thscode_to_code():
    assert bf.thscode_to_code("600519.SH") == "600519"
    assert bf.thscode_to_code("000001.SZ") == "000001"
    assert bf.thscode_to_code("920001.BJ") == "920001"
    # 43/83/87 等老 BJ 代码 hithink 不支持，应跳过
    assert bf.thscode_to_code("430047.BJ") is None
    assert bf.thscode_to_code("830799.BJ") is None
    assert bf.thscode_to_code("not-a-code") is None


# ---------------------------------------------------------------- qfq 计算


def test_compute_qfq_dividend_hand_calculation():
    """单次现金分红：手算 factor 比对。

    除权日 E=2024-06-03，前一交易日(05-31)原始收盘 C_prev=10.0，D=1.0，B=R=0：
        factor = (10 - 1) / 10 = 0.9
    E 之前的所有价格 ×0.9，E 当天及之后不变；volume 不动。
    """
    bars = pd.DataFrame({
        "date": ["2024-05-29", "2024-05-30", "2024-05-31", "2024-06-03", "2024-06-04"],
        "open": [9.8, 10.0, 10.2, 9.2, 9.3],
        "high": [10.1, 10.3, 10.4, 9.4, 9.5],
        "low": [9.7, 9.9, 10.0, 9.0, 9.1],
        "close": [10.0, 10.1, 10.0, 9.1, 9.2],
        "volume": [1000.0, 1100.0, 1200.0, 2000.0, 1500.0],
        "amount": [10000.0, 11110.0, 12000.0, 18200.0, 13800.0],
    })
    events = pd.DataFrame({
        "ex_date": ["2024-06-03"],
        "dividend_per_share": [1.0],
        "per_share_bonus": [0.0],
        "allotment_ratio": [0.0],
        "allotment_price": [0.0],
    })

    qfq = bf.compute_qfq(bars, events)

    # E 之前 3 天 ×0.9，E 及之后保持原始价
    assert list(qfq["close"]) == pytest.approx([9.0, 9.09, 9.0, 9.1, 9.2], abs=1e-9)
    assert list(qfq["open"]) == pytest.approx([8.82, 9.0, 9.18, 9.2, 9.3], abs=1e-9)
    # volume/amount 不复权
    assert list(qfq["volume"]) == [1000.0, 1100.0, 1200.0, 2000.0, 1500.0]
    assert list(qfq["amount"]) == [10000.0, 11110.0, 12000.0, 18200.0, 13800.0]
    # 除权日 pct_change 应连续（9.1/9.0-1），而非用原始价的 (9.1/10.0-1)
    row_e = qfq[qfq["date"] == "2024-06-03"].iloc[0]
    assert row_e["pct_change"] == pytest.approx(round((9.1 / 9.0 - 1) * 100, 2), abs=1e-9)


def test_compute_qfq_bonus_and_allotment():
    """送股+配股复合事件：factor = (C_prev - D + P*R) / (C_prev*(1+B+R))。"""
    bars = pd.DataFrame({
        "date": ["2024-05-30", "2024-05-31", "2024-06-03"],
        "open": [20.0, 20.0, 12.0],
        "high": [20.5, 20.5, 12.5],
        "low": [19.5, 19.5, 11.5],
        "close": [20.0, 20.0, 12.0],
        "volume": [1000.0, 1000.0, 3000.0],
        "amount": [20000.0, 20000.0, 36000.0],
    })
    events = pd.DataFrame({
        "ex_date": ["2024-06-03"],
        "dividend_per_share": [0.5],
        "per_share_bonus": [0.5],
        "allotment_ratio": [0.2],
        "allotment_price": [8.0],
    })
    # factor = (20 - 0.5 + 8*0.2) / (20 * 1.7) = 21.1 / 34
    factor = 21.1 / 34.0
    qfq = bf.compute_qfq(bars, events)
    assert qfq["close"].iloc[0] == pytest.approx(round(20.0 * factor, 2), abs=1e-9)
    assert qfq["close"].iloc[2] == pytest.approx(12.0, abs=1e-9)


def test_compute_qfq_no_events_passthrough():
    bars = pd.DataFrame({
        "date": ["2024-05-30", "2024-05-31"],
        "open": [10.0, 10.5],
        "high": [10.2, 10.8],
        "low": [9.9, 10.4],
        "close": [10.0, 10.5],
        "volume": [1000.0, 1000.0],
        "amount": [10000.0, 10500.0],
    })
    qfq = bf.compute_qfq(bars, bf._EMPTY_EVENTS)
    assert list(qfq["close"]) == [10.0, 10.5]
    assert pd.isna(qfq["pct_change"].iloc[0])
    assert qfq["pct_change"].iloc[1] == pytest.approx(5.0, abs=1e-6)


def test_compute_qfq_event_before_first_bar_skipped():
    """除权日早于 dump 起点时无 C_prev，事件跳过且不留 NaN。"""
    bars = pd.DataFrame({
        "date": ["2024-06-03", "2024-06-04"],
        "open": [9.0, 9.1],
        "high": [9.2, 9.3],
        "low": [8.9, 9.0],
        "close": [9.0, 9.1],
        "volume": [1000.0, 1000.0],
        "amount": [9000.0, 9100.0],
    })
    events = pd.DataFrame({
        "ex_date": ["2024-06-01"],
        "dividend_per_share": [1.0],
        "per_share_bonus": [0.0],
        "allotment_ratio": [0.0],
        "allotment_price": [0.0],
    })
    qfq = bf.compute_qfq(bars, events)
    assert list(qfq["close"]) == [9.0, 9.1]


# ---------------------------------------------------------------- 写库路径


@pytest.fixture()
def pipeline_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """造合成 dump + 测试库，并把下载/mock 掉，返回 (db_path, dump_dir)。

    股票 600519（5 根日 K，中间一次分红），因子 1 条。
    库内已有一行 close=9.0（与脚本 qfq 口径一致，sanity check 应通过）。
    """
    dump_dir = tmp_path / "dump" / datetime.now().strftime("%Y%m%d")
    dump_dir.mkdir(parents=True)
    daily_rows = _daily_rows("600519.SH", [
        ("2024-05-29", 10.0, 1000.0),
        ("2024-05-30", 10.1, 1100.0),
        ("2024-05-31", 10.0, 1200.0),
        ("2024-06-03", 9.1, 2000.0),
        ("2024-06-04", 9.2, 1500.0),
    ])
    _write_daily_parquet(dump_dir / "daily_k.parquet", daily_rows)
    _write_factors_parquet(dump_dir / "factors.parquet", [_factor_row("600519.SH", "2024-06-03", dividend=1.0)])

    db_path = tmp_path / "test.db"
    _make_db(db_path, ["600519"], {"600519": [("2024-06-04", 9.2)]})

    # 双保险：文件已预置，正常路径不会触网；若逻辑改动导致下载则直接失败
    monkeypatch.setattr(bf, "get_download_url", lambda *_a: pytest.fail("不应触网获取 presigned URL"))
    monkeypatch.setattr(bf, "download_file", lambda *_a: pytest.fail("不应触网下载"))
    return db_path, dump_dir


def test_main_writes_qfq_bars(pipeline_env):
    db_path, dump_dir = pipeline_env
    rc = bf.main([
        "--symbols", "600519",
        "--db-path", str(db_path),
        "--dump-root", str(dump_dir.parent),
        "--keep-files",
    ])
    assert rc == 0

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT trade_date, open, close, volume, amount, turnover_rate, data_source "
            "FROM daily_bars WHERE ts_code = '600519' ORDER BY trade_date"
        ).fetchall()
    assert len(rows) == 5
    # 前 3 天 ×0.9，后 2 天原始价（INSERT OR REPLACE 覆盖库内旧行）
    closes = [r[2] for r in rows]
    assert closes == pytest.approx([9.0, 9.09, 9.0, 9.1, 9.2], abs=1e-9)
    assert rows[0][1] == pytest.approx(8.55, abs=1e-9)  # open (10.0-0.5) × 0.9
    assert all(r[5] is None for r in rows)  # turnover_rate 写 NULL
    assert all(r[6] == "hithink" for r in rows)
    assert rows[0][3] == 1000.0  # volume 不复权


def test_main_insert_or_replace_idempotent(pipeline_env):
    db_path, dump_dir = pipeline_env
    argv = ["--symbols", "600519", "--db-path", str(db_path),
            "--dump-root", str(dump_dir.parent), "--keep-files"]
    assert bf.main(argv) == 0
    assert bf.main(argv) == 0  # 重跑不重复插行
    with sqlite3.connect(db_path) as conn:
        count = conn.execute("SELECT COUNT(*) FROM daily_bars WHERE ts_code = '600519'").fetchone()[0]
    assert count == 5


def test_main_dry_run_writes_nothing(pipeline_env):
    db_path, dump_dir = pipeline_env
    rc = bf.main([
        "--symbols", "600519", "--dry-run",
        "--db-path", str(db_path), "--dump-root", str(dump_dir.parent),
    ])
    assert rc == 0
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute("SELECT trade_date, data_source FROM daily_bars").fetchall()
    # 只剩 fixture 预置的那一行，且未被覆盖
    assert rows == [("2024-06-04", "akshare")]


def test_main_downloads_when_files_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """dump 目录无文件时应走 presigned URL → 下载路径（此处 mock 下载为复制预置文件）。"""
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    _write_daily_parquet(src_dir / "daily_k.parquet", _daily_rows("000001.SZ", [("2024-06-04", 11.25, 1000.0)]))
    _write_factors_parquet(src_dir / "factors.parquet", [])

    db_path = tmp_path / "test.db"
    _make_db(db_path, ["000001"], {})

    monkeypatch.setattr(bf, "get_download_url", lambda _client, endpoint: f"mock://{endpoint}")
    monkeypatch.setattr(bf, "HithinkClient", lambda: object())

    def fake_download(url: str, dest: Path) -> None:
        name = "daily_k.parquet" if "daily-k" in url else "factors.parquet"
        shutil.copy(src_dir / name, dest)

    monkeypatch.setattr(bf, "download_file", fake_download)

    rc = bf.main(["--symbols", "000001", "--db-path", str(db_path), "--dump-root", str(tmp_path / "dump")])
    assert rc == 0
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT close, data_source FROM daily_bars WHERE ts_code = '000001'"
        ).fetchone()
    assert row == (11.25, "hithink")
    # 未传 --keep-files：本次下载的文件跑完应被清理
    assert not list((tmp_path / "dump").rglob("*.parquet"))


# ---------------------------------------------------------------- 目标股票选择


def test_load_target_codes_semantics(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """默认目标 = 库内已有数据但历史不足 6 年、且不在跳过列表中的股票。"""
    recent = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    old = (datetime.now() - timedelta(days=3000)).strftime("%Y-%m-%d")
    db_path = tmp_path / "test.db"
    _make_db(db_path, ["600519", "000001", "000002", "000003"], {
        "600519": [(recent, 9.2)],   # 历史不足 6 年 → 目标
        "000001": [(old, 10.0)],     # 历史超 6 年 → 排除
        "000002": [(recent, 5.0)],   # 在跳过列表 → 排除
        # 000003 库内无数据 → 排除（与 6 年脚本一致）
    })
    monkeypatch.setattr(bf, "SKIP_FILE", tmp_path / ".backfill_skip")
    bf.SKIP_FILE.write_text("000002\n")

    target_start = (datetime.now() - timedelta(days=bf.BACKFILL_DAYS)).strftime("%Y-%m-%d")
    with sqlite3.connect(db_path) as conn:
        targets = bf.load_target_codes(conn, target_start)
    assert targets == ["600519"]


def test_sanity_check_warns_on_large_drift(tmp_path: Path, caplog: pytest.LogCaptureFixture):
    db_path = tmp_path / "test.db"
    _make_db(db_path, ["600519"], {"600519": [("2024-06-04", 100.0)]})  # 库内严重偏离
    qfq = pd.DataFrame({"date": ["2024-06-03", "2024-06-04"], "close": [9.0, 9.2]})
    with sqlite3.connect(db_path) as conn, caplog.at_level("WARNING", logger=bf.logger.name):
        bf.sanity_check(conn, {"600519": qfq})
    assert any("sanity check" in r.message and "600519" in r.message for r in caplog.records)
