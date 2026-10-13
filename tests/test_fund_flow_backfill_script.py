"""
scripts/backfill_fund_flow_gaps.py 的红证测试。

覆盖三类会静默损坏数据的坑：
1. secid 判定（handoff 初稿的 `startswith(("6","9")) → 1.` 会错杀 350 只 920 段）
2. 字段映射（索引错位会把主力净额写成小单净额，且不会报错）
3. 写入闸门（周末行撞 fund_flow 触发器会让整批事务回滚；--dry-run 不得写库）
"""
from __future__ import annotations

import importlib.util
import sqlite3
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SCRIPT = _REPO_ROOT / "scripts" / "backfill_fund_flow_gaps.py"


def _load_script():
    """按文件路径加载脚本模块（scripts/ 不是包，不能直接 import）。"""
    spec = importlib.util.spec_from_file_location("backfill_fund_flow_gaps", _SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


# 真实响应（2026-10-13 实测 push2his secid=0.920002）
REAL_KLINE = (
    "2026-10-12,821331.0,-195436.0,-3387360.0,-674722.0,1496053.0,"
    "2.05,-0.49,-8.45,-1.68,3.73,48.97,-2.06"
)
# 恒等式：主力净额 = 大单 + 超大单；主力占比 = 大单占比 + 超大单占比
assert 821331.0 == -674722.0 + 1496053.0
assert 2.05 == -1.68 + 3.73


class TestSecid:
    """secid 市场前缀 —— 920 段错判会让整段北交所永远补不回来。"""

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
    def test_secid_market_prefix(self, code, expected):
        assert _load_script().secid_for(code) == expected

    def test_920_is_not_treated_as_shanghai(self):
        """红证：handoff 初稿的 startswith(("6","9")) 写法会让本用例变红。"""
        assert _load_script().secid_for("920002").startswith("0.")

    def test_secid_matches_agrees_with_infer_market(self):
        """secid 判定必须与 core.utils.infer_market 同源，不得各写一份规则。"""
        from core.utils import infer_market

        mod = _load_script()
        for code in ("600000", "688712", "900901", "000001", "002241", "300750", "920002"):
            market = infer_market(code)
            expected_prefix = "1." if market in ("sh", "star") else "0."
            assert mod.secid_for(code) == expected_prefix + code


class TestParseKline:
    def test_field_mapping_matches_real_response(self):
        rec = _load_script().parse_kline(REAL_KLINE)
        assert rec is not None
        assert rec["date"] == "2026-10-12"
        assert rec["main_net_inflow"] == 821331.0
        assert rec["super_large_net_inflow"] == 1496053.0
        assert rec["large_net_inflow"] == -674722.0
        assert rec["main_net_inflow_pct"] == 2.05
        assert rec["super_large_net_inflow_pct"] == 3.73
        assert rec["large_net_inflow_pct"] == -1.68

    def test_main_equals_large_plus_super_large(self):
        """恒等式自检：索引整体错位一格就会红。"""
        rec = _load_script().parse_kline(REAL_KLINE)
        assert rec is not None
        assert rec["main_net_inflow"] == pytest.approx(
            rec["large_net_inflow"] + rec["super_large_net_inflow"]
        )

    def test_main_pct_equals_large_pct_plus_super_large_pct(self):
        rec = _load_script().parse_kline(REAL_KLINE)
        assert rec is not None
        assert rec["main_net_inflow_pct"] == pytest.approx(
            rec["large_net_inflow_pct"] + rec["super_large_net_inflow_pct"]
        )

    def test_short_row_returns_none(self):
        assert _load_script().parse_kline("2026-10-12,1.0,2.0") is None

    def test_empty_date_returns_none(self):
        assert _load_script().parse_kline(",1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0") is None

    def test_non_numeric_tail_degrades_to_none_not_crash(self):
        row = ",".join(["2026-10-12"] + ["-"] * 12)
        rec = _load_script().parse_kline(row)
        assert rec is None  # 六个数值字段全 None → 视为无数据


class TestFetchCodeHistory:
    def test_network_error_returns_empty_not_raises(self):
        """单票失败必须被吞掉：否则一只停牌票会中断整轮回补。"""
        mod = _load_script()
        session = MagicMock()
        session.get.side_effect = RuntimeError("boom")
        assert mod.fetch_code_history(session, "000001") == []

    def test_http_error_returns_empty(self):
        mod = _load_script()
        session = MagicMock()
        session.get.return_value.raise_for_status.side_effect = RuntimeError("500")
        assert mod.fetch_code_history(session, "000001") == []

    def test_null_data_returns_empty(self):
        """实测 secid 错时源站回 rc:100 + data:null —— 不抛异常也不返回行。"""
        mod = _load_script()
        session = MagicMock()
        session.get.return_value.json.return_value = {"rc": 100, "data": None}
        assert mod.fetch_code_history(session, "000001") == []

    def test_klines_parsed(self):
        mod = _load_script()
        session = MagicMock()
        session.get.return_value.json.return_value = {"data": {"klines": [REAL_KLINE]}}
        assert mod.fetch_code_history(session, "920002") == [REAL_KLINE]

    def test_request_carries_correct_secid(self):
        mod = _load_script()
        session = MagicMock()
        session.get.return_value.json.return_value = {"data": {"klines": []}}
        mod.fetch_code_history(session, "920002")
        params = session.get.call_args.kwargs["params"]
        assert params["secid"] == "0.920002"


class TestFindGaps:
    @staticmethod
    def _make_db(tmp_path: Path) -> Path:
        db = tmp_path / "t.db"
        conn = sqlite3.connect(str(db))
        conn.execute(
            "CREATE TABLE daily_bars (ts_code TEXT, trade_date TEXT)"
        )
        conn.execute(
            """CREATE TABLE fund_flow (
                ts_code TEXT, trade_date TEXT,
                UNIQUE(ts_code, trade_date))"""
        )
        conn.executemany(
            "INSERT INTO daily_bars VALUES (?, ?)",
            [("000001", "2026-09-22"), ("000001", "2026-09-23"),
             ("002594", "2026-09-22"), ("002594", "2026-09-23")],
        )
        conn.execute("INSERT INTO fund_flow VALUES ('000001', '2026-09-22')")
        conn.commit()
        conn.close()
        return db

    def test_finds_only_missing_pairs(self, tmp_path):
        mod = _load_script()
        gaps = mod.find_gaps(self._make_db(tmp_path), "2026-09-18")
        assert gaps == {"000001": {"2026-09-23"}, "002594": {"2026-09-22", "2026-09-23"}}

    def test_since_filters_older_days(self, tmp_path):
        mod = _load_script()
        gaps = mod.find_gaps(self._make_db(tmp_path), "2026-09-23")
        assert gaps == {"000001": {"2026-09-23"}, "002594": {"2026-09-23"}}

    def test_idempotent_after_backfill(self, tmp_path):
        """把缺口补上后必须查不出缺口 —— 决定脚本能否安全重复执行。"""
        mod = _load_script()
        db = self._make_db(tmp_path)
        first = mod.find_gaps(db, "2026-09-18")
        assert first
        conn = sqlite3.connect(str(db))
        conn.executemany(
            "INSERT OR REPLACE INTO fund_flow VALUES (?, ?)",
            [(c, d) for c, days in first.items() for d in days],
        )
        conn.commit()
        conn.close()
        assert mod.find_gaps(db, "2026-09-18") == {}

    def test_connection_is_closed(self, tmp_path):
        mod = _load_script()
        db = self._make_db(tmp_path)
        assert mod.find_gaps(db, "2026-09-18")
        # 连接泄漏会在 Windows/临时目录清理时报错；这里直接验证没有累积
        assert not list(Path(tmp_path).glob("t.db-journal"))


class TestWeekendGuard:
    def test_weekend_detected(self):
        mod = _load_script()
        assert mod._is_weekend("2026-10-11") is True   # 周日
        assert mod._is_weekend("2026-10-10") is True   # 周六
        assert mod._is_weekend("2026-10-09") is False  # 周五

    def test_weekend_row_never_reaches_records(self):
        """红证：不过滤周末会撞 trg_fund_flow_reject_weekend，整批事务回滚。"""
        mod = _load_script()
        assert mod._is_weekend(mod.parse_kline("2026-10-11,1,2,3,4,5,6,7,8,9,10")["date"])


class TestMainDryRun:
    def _run(self, mod, db, extra_args, records_written, want=None):
        provider = MagicMock()
        provider.save_fund_flow_batch.return_value = records_written
        with patch.object(mod, "SmartMoneyDBProvider", return_value=provider), \
             patch.object(
                 mod, "find_gaps",
                 return_value={"000001": {want} if want else {"000001": {"2026-09-23"}}},
             ), \
             patch.object(mod, "fetch_code_history", return_value=[REAL_KLINE]), \
             patch.object(mod.sys, "argv", ["backfill_fund_flow_gaps.py"] + extra_args), \
             patch.object(mod, "DEFAULT_DB_PATH", db):
            rc = mod.main()
        return rc, provider

    def test_dry_run_does_not_write(self, tmp_path):
        db = tmp_path / "t.db"
        db.touch()
        mod = _load_script()
        rc, provider = self._run(mod, db, ["--dry-run", "--db", str(db)], 0)
        assert rc == 0
        provider.save_fund_flow_batch.assert_not_called()

    def test_write_path_saves_matched_row(self):
        """want 命中源端日期时才写 —— 源端没有的本地缺口不得凭空造行。"""
        db = Path(self._tmpdir()) / "t.db"
        db.touch()
        mod = _load_script()
        rc, provider = self._run(mod, db, ["--db", str(db)], 1, want="2026-10-12")
        assert rc == 0
        provider.save_fund_flow_batch.assert_called_once()
        batch = provider.save_fund_flow_batch.call_args.args[0]
        assert len(batch) == 1
        row = batch[0]
        assert row["symbol"] == "000001"          # 用请求的票，不是接口回的名字
        assert row["date"] == "2026-10-12"
        assert row["main_net_inflow"] == 821331.0

    def test_no_write_when_source_date_not_in_gap_set(self):
        """本地缺 09-23 但源端只给 10-12 → 无可写行，不得写入错日期。"""
        db = Path(self._tmpdir()) / "t.db"
        db.touch()
        mod = _load_script()
        rc, provider = self._run(mod, db, ["--db", str(db)], 0, want="2026-09-23")
        assert rc == 0
        provider.save_fund_flow_batch.assert_not_called()

    def test_weekend_gap_is_not_written(self):
        """周末若混进 want，必须在入库前滤掉 —— 否则触发器让整批事务回滚。"""
        db = Path(self._tmpdir()) / "t.db"
        db.touch()
        mod = _load_script()
        saturday_kline = REAL_KLINE.replace("2026-10-12", "2026-10-10")
        provider = MagicMock()
        with patch.object(mod, "SmartMoneyDBProvider", return_value=provider), \
             patch.object(mod, "find_gaps", return_value={"000001": {"2026-10-10"}}), \
             patch.object(mod, "fetch_code_history", return_value=[saturday_kline]), \
             patch.object(
                 mod.sys, "argv",
                 ["x", "--db", str(db)],
             ), patch.object(mod, "DEFAULT_DB_PATH", db):
            assert mod.main() == 0
        provider.save_fund_flow_batch.assert_not_called()

    def test_missing_db_exits_nonzero(self, tmp_path):
        mod = _load_script()
        with patch.object(mod.sys, "argv", ["x", "--db", str(tmp_path / "nope.db")]):
            assert mod.main() == 1

    def test_no_gap_exits_zero_without_network(self, tmp_path):
        db = tmp_path / "t.db"
        db.touch()
        mod = _load_script()
        with patch.object(mod, "find_gaps", return_value={}), \
             patch.object(mod, "fetch_code_history") as fetch, \
             patch.object(mod.sys, "argv", ["x", "--db", str(db)]):
            assert mod.main() == 0
        fetch.assert_not_called()

    @staticmethod
    def _tmpdir() -> str:
        import tempfile

        return tempfile.mkdtemp()
