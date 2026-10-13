"""
core/fund_flow_source.py 的红证测试。

这里钉的是**整段字段映射**：索引整体错位一格不会抛异常，只会把小单净额写进
主力列，然后一路「成功」入库。所以除了逐字段断言，还用两条恒等式兜底
（主力 = 大单 + 超大单，占比同理）——它们只在映射完全正确时成立。
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from core import fund_flow_source as ffs
from core.utils import infer_market

# 真实响应（2026-10-13 实测 push2his secid=0.920002 万达轴承）
REAL_KLINE = (
    "2026-10-12,821331.0,-195436.0,-3387360.0,-674722.0,1496053.0,"
    "2.05,-0.49,-8.45,-1.68,3.73,48.97,-2.06"
)


class TestSecid:
    """secid 市场前缀 —— 920 段错判会让整段北交所永远取不到数据。"""

    @pytest.mark.parametrize(
        ("code", "expected"),
        [
            ("600000", "1.600000"),   # 沪主板
            ("688712", "1.688712"),   # 科创板
            ("900901", "1.900901"),   # 沪 B
            ("000001", "0.000001"),   # 深主板
            ("002241", "0.002241"),   # 中小板
            ("300750", "0.300750"),   # 创业板
            ("920002", "0.920002"),   # 北交所新号段：920 必须先于 9 判
            ("830799", "0.830799"),   # 北交所旧号段
            ("430047", "0.430047"),   # 北交所旧号段
        ],
    )
    def test_market_prefix(self, code, expected):
        assert ffs.secid_for(code) == expected

    def test_920_is_not_shanghai(self):
        """红证：写成 startswith(("6","9")) 会让本用例变红（secid=1.920002 实测 rc:100）。"""
        assert ffs.secid_for("920002").startswith("0.")

    def test_agrees_with_infer_market(self):
        """必须与 core.utils.infer_market 同源，不得各写一份市场规则。"""
        for code in ("600000", "688712", "900901", "000001", "002241", "300750", "920002"):
            expected = "1." if infer_market(code) in ("sh", "star") else "0."
            assert ffs.secid_for(code) == expected + code


class TestParseKline:
    def test_field_mapping(self):
        rec = ffs.parse_kline(REAL_KLINE)
        assert rec is not None
        assert rec["date"] == "2026-10-12"
        assert rec["main_net_inflow"] == 821331.0
        assert rec["super_large_net_inflow"] == 1496053.0
        assert rec["large_net_inflow"] == -674722.0
        assert rec["main_net_inflow_pct"] == 2.05
        assert rec["super_large_net_inflow_pct"] == 3.73
        assert rec["large_net_inflow_pct"] == -1.68

    def test_main_equals_large_plus_super_large(self):
        rec = ffs.parse_kline(REAL_KLINE)
        assert rec is not None
        assert rec["main_net_inflow"] == pytest.approx(
            rec["large_net_inflow"] + rec["super_large_net_inflow"]
        )

    def test_main_pct_equals_the_two_pcts(self):
        rec = ffs.parse_kline(REAL_KLINE)
        assert rec is not None
        assert rec["main_net_inflow_pct"] == pytest.approx(
            rec["large_net_inflow_pct"] + rec["super_large_net_inflow_pct"]
        )

    def test_no_small_medium_columns(self):
        """小单/中单在本地 fund_flow 无对应列，不得凭空造出列名。"""
        rec = ffs.parse_kline(REAL_KLINE)
        assert rec is not None
        assert set(rec) == {"date", *ffs.NUMERIC_COLUMNS}

    def test_short_row_returns_none(self):
        assert ffs.parse_kline("2026-10-12,1.0,2.0") is None

    def test_empty_date_returns_none(self):
        assert ffs.parse_kline(",1,2,3,4,5,6,7,8,9,10") is None

    def test_all_dashes_returns_none(self):
        assert ffs.parse_kline(",".join(["2026-10-12"] + ["-"] * 12)) is None

    def test_partial_dashes_keep_the_rest(self):
        """只有部分字段坏掉时应保留其余字段，而不是整行丢弃。"""
        parts = REAL_KLINE.split(",")
        parts[4] = "-"
        rec = ffs.parse_kline(",".join(parts))
        assert rec is not None
        assert rec["large_net_inflow"] is None
        assert rec["main_net_inflow"] == 821331.0


class TestFetchDayKlines:
    def test_network_error_returns_empty(self):
        session = MagicMock()
        session.get.side_effect = RuntimeError("boom")
        assert ffs.fetch_day_klines(session, "000001") == []

    def test_http_error_returns_empty(self):
        session = MagicMock()
        session.get.return_value.raise_for_status.side_effect = RuntimeError("500")
        assert ffs.fetch_day_klines(session, "000001") == []

    def test_null_data_returns_empty(self):
        """实测 secid 错时源站回 rc:100 + data:null —— 不抛也不返回行。"""
        session = MagicMock()
        session.get.return_value.json.return_value = {"rc": 100, "data": None}
        assert ffs.fetch_day_klines(session, "000001") == []

    def test_returns_raw_klines(self):
        session = MagicMock()
        session.get.return_value.json.return_value = {
            "data": {"code": "920002", "klines": [REAL_KLINE]}
        }
        assert ffs.fetch_day_klines(session, "920002") == [REAL_KLINE]

    def test_request_carries_secid_and_lmt(self):
        session = MagicMock()
        session.get.return_value.json.return_value = {"data": {"klines": []}}
        ffs.fetch_day_klines(session, "920002", lmt=60)
        params = session.get.call_args.kwargs["params"]
        assert params["secid"] == "0.920002"
        assert params["lmt"] == "60"

    def test_mismatched_echoed_code_is_rejected(self):
        """红证：去掉回显校验后本用例变红。

        非 6 位代码会让 secid 变成前缀匹配 —— 实测 secid=0.1318 返回
        code=131800 / name=Ｒ-003，即另一只证券的资金流。不校验就会把
        131800 的数据写到 1318 名下，全程不报错。
        """
        session = MagicMock()
        session.get.return_value.json.return_value = {
            "data": {"code": "131800", "name": "Ｒ-003", "klines": [REAL_KLINE]}
        }
        assert ffs.fetch_day_klines(session, "1318") == []

    def test_missing_echoed_code_is_rejected(self):
        """回显缺失时拒绝：宁可响亮地取不到，也不要静默写入错证券。"""
        session = MagicMock()
        session.get.return_value.json.return_value = {"data": {"klines": [REAL_KLINE]}}
        assert ffs.fetch_day_klines(session, "000001") == []

    def test_matching_echoed_code_is_accepted(self):
        session = MagicMock()
        session.get.return_value.json.return_value = {
            "data": {"code": "000001", "klines": [REAL_KLINE]}
        }
        assert ffs.fetch_day_klines(session, "000001") == [REAL_KLINE]

    def test_echo_match_ignores_leading_zeros(self):
        session = MagicMock()
        session.get.return_value.json.return_value = {
            "data": {"code": "000001", "klines": [REAL_KLINE]}
        }
        assert ffs.fetch_day_klines(session, "1") == [REAL_KLINE]

    def test_non_string_klines_are_dropped(self):
        session = MagicMock()
        session.get.return_value.json.return_value = {
            "data": {"code": "000001", "klines": [REAL_KLINE, None, 7]}
        }
        assert ffs.fetch_day_klines(session, "000001") == [REAL_KLINE]


class TestMakeSession:
    def test_trust_env_disabled(self):
        """东财在环境代理下会拒连（实测 ProxyError），trust_env 必须关。"""
        session = ffs.make_session()
        assert session.trust_env is False


class TestIsWeekend:
    def test_weekend_detection(self):
        assert ffs.is_weekend("2026-10-11") is True   # 周日
        assert ffs.is_weekend("2026-10-10") is True   # 周六
        assert ffs.is_weekend("2026-10-09") is False  # 周五


class TestColumnGroups:
    def test_derived_columns_are_exactly_the_five_fabricatable_ones(self):
        """这五列正是被 0.0 填充过的「派生字段」，修复脚本按它选取。"""
        assert set(ffs.DERIVED_COLUMNS) == {
            "main_net_inflow_pct",
            "super_large_net_inflow",
            "super_large_net_inflow_pct",
            "large_net_inflow",
            "large_net_inflow_pct",
        }
        assert set(ffs.DERIVED_COLUMNS) < set(ffs.NUMERIC_COLUMNS)
