"""daily_bars.volume 单位一致性门禁（quant_pipeline 侧）。

``tasks/bars_snapshot.py`` 把东财 spot 的「成交量」写进 ``daily_bars``，
而该字段以**手**返回；同一列的另一个写入方（quant_hunter 的 sina 路径）
写的是**股**。两者混在一列，来源切换处出现 100 倍断层（生产库实测：
同��只票 10-12 sina=105,863,456 → 10-13 eastmoney_spot=994,937，
而真实成交量三天稳定在 ~10 万手，5,558/5,567 只股票中招）。

口径与 quant_hunter 侧对齐为「股」，见该仓库的同���门禁。
"""

from __future__ import annotations

import pandas as pd

from tasks import bars_snapshot

LOT_SIZE = 100


def _spot_df(volume: float) -> pd.DataFrame:
    return pd.DataFrame({
        "代码": ["000001"],
        "最新价": [10.5],
        "今开": [10.0],
        "最高": [10.6],
        "最低": [9.9],
        "成交量": [volume],
        "成交额": [volume * 10.5 * LOT_SIZE],
        "换手率": [1.0],
        "涨跌幅": [1.0],
        "振幅": [2.0],
        "昨收": [10.4],
    })


def test_snapshot_volume_is_normalized_to_shares():
    """东财 spot 的手 → 股，与 sina 路径同口径。"""
    records, _ = bars_snapshot.snapshot_to_bar_records(_spot_df(12345.0), "2026-07-31")

    assert records["000001"]["volume"] == 12345.0 * LOT_SIZE


def test_amount_and_volume_stay_consistent():
    """归一后 amount/volume 必须≈close（这是「单位对不对」的无假设判据）。"""
    records, _ = bars_snapshot.snapshot_to_bar_records(_spot_df(12345.0), "2026-07-31")

    rec = records["000001"]
    assert abs(rec["amount"] / rec["volume"] - rec["close"]) < 0.01


def test_zero_volume_is_not_dropped_into_nan():
    """停牌/无成交：volume=0 是真实取值，不该被换算成 NaN 或丢弃。"""
    records, suspended = bars_snapshot.snapshot_to_bar_records(_spot_df(0.0), "2026-07-31")

    assert "000001" in suspended or records.get("000001", {}).get("volume") == 0.0
