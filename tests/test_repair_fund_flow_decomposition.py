"""
scripts/repair_fund_flow_decomposition.py 的红证测试。

钉三类会静默损坏数据的坑：
1. 污染指纹的**精度**（漏检真数据 / 误伤真实零值）
2. 两步修复的**优先级**：能覆盖就全量覆盖，取不到才置 NULL —— 只做后者会留下
   假的主力净额，只做前者会让取不到的票留着 0.0 假值
3. NULL 与 0.0 的区别：置 NULL 必须真的写 NULL，走 save_fund_flow_batch 会被
   `_coerce_float(NaN, default=0.0)` 重新变回 0.0 —— 所以本脚本必须直连 UPDATE
"""
from __future__ import annotations

import importlib.util
import sqlite3
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SCRIPT = _REPO_ROOT / "scripts" / "repair_fund_flow_decomposition.py"

REAL_KLINE = (
    "2026-10-12,821331.0,-195436.0,-3387360.0,-674722.0,1496053.0,"
    "2.05,-0.49,-8.45,-1.68,3.73,48.97,-2.06"
)


def _load_script():
    spec = importlib.util.spec_from_file_location("repair_fund_flow_decomposition", _SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def mod():
    return _load_script()


def _make_db(tmp_path: Path) -> Path:
    """构造一个含「污染行 / 真实行 / 真实零值行」的库。"""
    db = tmp_path / "t.db"
    conn = sqlite3.connect(str(db))
    conn.execute(
        """CREATE TABLE fund_flow (
            ts_code TEXT, trade_date TEXT, main_net_inflow REAL,
            main_net_inflow_pct REAL, super_large_net_inflow REAL,
            super_large_net_inflow_pct REAL, large_net_inflow REAL,
            large_net_inflow_pct REAL, is_simulated BOOLEAN DEFAULT 0,
            UNIQUE(ts_code, trade_date))"""
    )
    rows = [
        # 污染行（同花顺）：派生字段全 0，主力非 0
        ("000001", "2026-09-24", 27538600.0, 0.0, 0.0, 0.0, 0.0, 0.0),
        ("000002", "2026-09-24", -5937000.0, 0.0, 0.0, 0.0, 0.0, 0.0),
        # 真实东财行：恒等式成立，分解非零
        ("600000", "2026-09-24", 100.0, 1.0, 40.0, 0.4, 60.0, 0.6),
        # 真实「全零」行：主力也是 0（合法的无异动日），不是污染
        ("000003", "2026-09-24", 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
        # 周末行不该被选中
        ("000004", "2026-09-26", 500.0, 0.0, 0.0, 0.0, 0.0, 0.0),
        # since 之前的行不该被选中
        ("000005", "2026-09-01", 500.0, 0.0, 0.0, 0.0, 0.0, 0.0),
    ]
    conn.executemany(
        "INSERT INTO fund_flow (ts_code, trade_date, main_net_inflow, main_net_inflow_pct,"
        " super_large_net_inflow, super_large_net_inflow_pct, large_net_inflow,"
        " large_net_inflow_pct) VALUES (?,?,?,?,?,?,?,?)",
        rows,
    )
    conn.commit()
    conn.close()
    return db


class TestFindPoisoned:
    def test_selects_only_signature_rows(self, mod, tmp_path):
        rows = mod.find_poisoned(_make_db(tmp_path), "2026-09-18")
        assert [(c, d) for c, d, _ in rows] == [("000001", "2026-09-24"), ("000002", "2026-09-24")]

    def test_real_em_row_with_nonzero_decomposition_is_untouched(self, mod, tmp_path):
        """东财行的分解非零，指纹不匹配 —— 不得被当成污染行改掉。"""
        rows = mod.find_poisoned(_make_db(tmp_path), "2026-09-18")
        assert "600000" not in {c for c, _, _ in rows}

    def test_genuine_all_zero_day_is_not_flagged(self, mod, tmp_path):
        """主力也是 0 的合法行不得被误判：它是真的「无异动」，不是缺失。"""
        rows = mod.find_poisoned(_make_db(tmp_path), "2026-09-18")
        assert "000003" not in {c for c, _, _ in rows}

    def test_weekend_rows_excluded(self, mod, tmp_path):
        """周末行本不该存在（触发器拒写），但选择器仍须排除，不依赖该假设。"""
        rows = mod.find_poisoned(_make_db(tmp_path), "2026-09-18")
        assert "000004" not in {c for c, _, _ in rows}

    def test_since_bounds_the_scan(self, mod, tmp_path):
        rows = mod.find_poisoned(_make_db(tmp_path), "2026-09-02")
        assert "000005" not in {c for c, _, _ in rows}

    def test_limit_caps_rows(self, mod, tmp_path):
        assert len(mod.find_poisoned(_make_db(tmp_path), "2026-09-18", limit=1)) == 1

    def test_uses_read_only_connection(self, mod, tmp_path):
        """只读连接：修复脚本的「发现」阶段不得有任何写入副作用。"""
        db = _make_db(tmp_path)
        before = db.stat().st_mtime_ns
        mod.find_poisoned(db, "2026-09-18")
        assert db.stat().st_mtime_ns == before


class TestBuildPlan:
    def test_reachable_row_is_fully_overwritten(self, mod):
        """东财取得到 → 六个字段全量覆盖，主力净额也换成东财值。"""
        session = MagicMock()
        session.get.return_value.json.return_value = {
            "data": {"code": "000001", "klines": [REAL_KLINE]}
        }
        overwrites, nulls = mod.build_plan(session, [("000001", "2026-10-12", 999.0)])
        assert nulls == []
        assert len(overwrites) == 1
        main, main_pct, sl, sl_pct, lg, lg_pct, code, day, main_before = overwrites[0]
        assert (code, day) == ("000001", "2026-10-12")
        assert main == 821331.0          # 东财值，不是同花顺的 999.0
        assert main_before == 999.0      # 原值留痕，报告改动幅度用
        assert (lg, sl) == (-674722.0, 1496053.0)

    def test_unreachable_row_is_nulled_keeping_main(self, mod):
        """东财取不到 → 只置 NULL，**保留**原主力净额（不删数据）。"""
        session = MagicMock()
        session.get.return_value.json.return_value = {"data": {"klines": []}}
        overwrites, nulls = mod.build_plan(session, [("000001", "2026-10-12", 999.0)])
        assert overwrites == []
        assert nulls == [("000001", "2026-10-12")]

    def test_day_not_in_source_window_is_nulled(self, mod):
        """源端只给 lmt 根日 K；超出窗口的本地日必须走 NULL 而不是丢弃。"""
        session = MagicMock()
        session.get.return_value.json.return_value = {
            "data": {"code": "000001", "klines": [REAL_KLINE]}
        }
        overwrites, nulls = mod.build_plan(
            session, [("000001", "2026-09-01", 5.0), ("000001", "2026-10-12", 6.0)]
        )
        assert len(overwrites) == 1
        assert nulls == [("000001", "2026-09-01")]

    def test_network_failure_nulls_rather_than_crashes(self, mod):
        session = MagicMock()
        session.get.side_effect = RuntimeError("boom")
        overwrites, nulls = mod.build_plan(session, [("000001", "2026-10-12", 1.0)])
        assert overwrites == []
        assert len(nulls) == 1

    def test_writes_nulls_for_all_five_derived_columns(self, mod):
        """红证：只置 NULL 不含 main_net_inflow_pct 时，用例会因漏列而红。"""
        sql = " ".join(mod._UPDATE_DERIVED_NULL.split())
        for col in ("main_net_inflow_pct", "super_large_net_inflow",
                    "super_large_net_inflow_pct", "large_net_inflow",
                    "large_net_inflow_pct"):
            assert f"{col} = NULL" in sql
        assert "main_net_inflow = NULL" not in sql


class TestApplyPlan:
    def test_null_path_really_writes_null(self, mod, tmp_path):
        """红证：走 save_fund_flow_batch 会被 _coerce_float(NaN→0.0) 变回 0。

        所以「置 NULL」只能靠直连 UPDATE，本用例验证它真的落成 NULL。
        """
        db = _make_db(tmp_path)
        conn = sqlite3.connect(str(db))
        changed = mod.apply_plan(conn, [], [("000001", "2026-09-24")])
        assert changed == 1
        row = conn.execute(
            "SELECT main_net_inflow, main_net_inflow_pct, super_large_net_inflow,"
            " super_large_net_inflow_pct, large_net_inflow, large_net_inflow_pct"
            " FROM fund_flow WHERE ts_code='000001'"
        ).fetchone()
        assert row[0] == 27538600.0, "主力净额必须保留"
        assert row[1:] == (None, None, None, None, None), "五个派生字段必须是 NULL，不是 0"

    def test_overwrite_path_writes_all_six(self, mod, tmp_path):
        db = _make_db(tmp_path)
        conn = sqlite3.connect(str(db))
        overwrites = [(111.0, 1.1, 22.0, 0.2, 89.0, 0.9, "000001", "2026-09-24", 27538600.0)]
        assert mod.apply_plan(conn, overwrites, []) == 1
        row = conn.execute(
            "SELECT main_net_inflow, main_net_inflow_pct, super_large_net_inflow,"
            " super_large_net_inflow_pct, large_net_inflow, large_net_inflow_pct"
            " FROM fund_flow WHERE ts_code='000001'"
        ).fetchone()
        assert row == (111.0, 1.1, 22.0, 0.2, 89.0, 0.9)

    def test_repaired_row_satisfies_the_identity(self, mod, tmp_path):
        """修完之后该行必须满足 主力 = 大单 + 超大单 —— 污染指纹应当消失。"""
        db = _make_db(tmp_path)
        conn = sqlite3.connect(str(db))
        mod.apply_plan(conn, [(111.0, 1.1, 22.0, 0.2, 89.0, 0.9, "000001", "2026-09-24", 1.0)], [])
        assert mod.find_poisoned(db, "2026-09-18") == [("000002", "2026-09-24", -5937000.0)]

    def test_changed_count_reflects_actual_mutations(self, mod, tmp_path):
        """total_changes 差值而非提交条数：重复执行应报 0。"""
        db = _make_db(tmp_path)
        conn = sqlite3.connect(str(db))
        nulls = [("000001", "2026-09-24")]
        assert mod.apply_plan(conn, [], nulls) == 1
        assert mod.apply_plan(conn, [], nulls) == 0


class TestPlanToApply:
    """build_plan → apply_plan 的**接缝**测试。

    单独测两个函数会漏掉「元组顺序与 SQL 占位符顺序不一致」这类接缝缺陷：
    2026-10-13 开发时真出现过 —— build_plan 产出 (code, day, 六个值)，
    而 UPDATE 的占位符是 (六个值, code, day)。两边各自都对，合起来 UPDATE
    匹配 0 行、静默零变更，而两个单元测试照样全绿。
    """

    @staticmethod
    def _session_with(*klines: str, echo: str = "000001"):
        session = MagicMock()
        session.get.return_value.json.return_value = {
            "data": {"code": echo, "klines": list(klines)}
        }
        return session

    def test_end_to_end_overwrite_reaches_the_row(self, mod, tmp_path):
        db = _make_db(tmp_path)
        # K 线日期必须与库里那一行相同：日期对不上时 UPDATE 匹配 0 行（正确行为）
        session = self._session_with(REAL_KLINE.replace("2026-10-12", "2026-09-24"))
        overwrites, nulls = mod.build_plan(session, [("000001", "2026-09-24", 999.0)])
        conn = sqlite3.connect(str(db))
        changed = mod.apply_plan(conn, overwrites, nulls)
        conn.close()
        assert changed == 1, "UPDATE 必须真的命中行；顺序错位时会静默返回 0"
        check = sqlite3.connect(str(db))
        row = check.execute(
            "SELECT main_net_inflow, super_large_net_inflow, large_net_inflow"
            " FROM fund_flow WHERE ts_code='000001' AND trade_date='2026-09-24'"
        ).fetchone()
        check.close()
        assert row == (821331.0, 1496053.0, -674722.0)

    def test_end_to_end_null_reaches_the_row(self, mod, tmp_path):
        db = _make_db(tmp_path)
        session = self._session_with()  # 源端无数据
        overwrites, nulls = mod.build_plan(session, [("000001", "2026-09-24", 27538600.0)])
        conn = sqlite3.connect(str(db))
        changed = mod.apply_plan(conn, overwrites, nulls)
        conn.close()
        assert changed == 1
        check = sqlite3.connect(str(db))
        row = check.execute(
            "SELECT main_net_inflow, super_large_net_inflow, large_net_inflow_pct"
            " FROM fund_flow WHERE ts_code='000001' AND trade_date='2026-09-24'"
        ).fetchone()
        check.close()
        assert row == (27538600.0, None, None)

    def test_second_pass_is_a_no_op(self, mod, tmp_path):
        """指纹进了 WHERE，所以重跑不再命中 —— 这是幂等性的来源。"""
        db = _make_db(tmp_path)
        session = self._session_with(REAL_KLINE.replace("2026-10-12", "2026-09-24"))
        overwrites, nulls = mod.build_plan(session, [("000001", "2026-09-24", 999.0)])
        conn = sqlite3.connect(str(db))
        first = mod.apply_plan(conn, overwrites, nulls)
        second = mod.apply_plan(conn, overwrites, nulls)
        conn.close()
        assert (first, second) == (1, 0)


class TestMain:
    def _run(self, mod, db, args, poisoned, plan=((), ())):
        with patch.object(mod, "find_poisoned", return_value=poisoned), \
             patch.object(mod, "build_plan", return_value=plan), \
             patch.object(mod, "make_session"), \
             patch.object(mod.sys, "argv", ["x", "--db", str(db)] + args), \
             patch.object(mod, "connect") as connect:
            rc = mod.main()
        return rc, connect

    def test_dry_run_does_not_write(self, mod, tmp_path):
        db = _make_db(tmp_path)
        rc, connect = self._run(
            mod, db, ["--dry-run"],
            [("000001", "2026-09-24", 1.0)],
            plan=([(2.0, 0, 0, 0, 0, 0, "000001", "2026-09-24", 1.0)], [("000002", "2026-09-24")]),
        )
        assert rc == 0
        connect.assert_not_called()
        # 库里也必须真的没变
        conn = sqlite3.connect(str(db))
        row = conn.execute(
            "SELECT super_large_net_inflow FROM fund_flow WHERE ts_code='000001'"
        ).fetchone()
        conn.close()
        assert row == (0.0,)

    def test_write_path_connects_and_applies(self, mod, tmp_path):
        db = _make_db(tmp_path)
        rc, connect = self._run(
            mod, db, [],
            [("000001", "2026-09-24", 1.0)],
            plan=([(2.0, 0, 0, 0, 0, 0, "000001", "2026-09-24", 1.0)], []),
        )
        assert rc == 0
        connect.assert_called_once()

    def test_no_poisoned_rows_exits_zero_without_network(self, mod, tmp_path):
        db = _make_db(tmp_path)
        with patch.object(mod, "find_poisoned", return_value=[]), \
             patch.object(mod, "make_session") as mk, \
             patch.object(mod.sys, "argv", ["x", "--db", str(db)]):
            assert mod.main() == 0
        mk.assert_not_called()

    def test_missing_db_exits_nonzero(self, mod, tmp_path):
        with patch.object(mod.sys, "argv", ["x", "--db", str(tmp_path / "nope.db")]):
            assert mod.main() == 1


class TestNoProviderImportAtModuleLevel:
    def test_providers_not_imported(self, mod):
        assert not hasattr(mod, "SmartMoneyDBProvider")

    def test_writes_via_direct_connection_not_save_batch(self, mod):
        """必须直连 UPDATE：save_fund_flow_batch 的 _coerce_float 会把 NULL 变回 0.0。"""
        assert "save_fund_flow_batch" not in _SCRIPT.read_text(encoding="utf-8")
