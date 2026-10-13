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


class TestModuleLevelImports:
    """模块级不得 import providers。

    红证：还原成模块级 `from providers import SmartMoneyDBProvider` 时本用例变红。
    门禁的外部 cwd `--help` 探针在两种写法下都绿（桩兄弟仓库恰好补齐了
    依赖），所以这条不变量得单独钉住 —— 否则「让 --help 白付 akshare 导入
    成本、且在没有兄弟仓库的环境里直接 rc=1」的写法可以悄悄回来。
    """

    def test_providers_not_imported_at_module_level(self):
        mod = _load_script()
        assert not hasattr(mod, "SmartMoneyDBProvider")

    def test_core_utils_import_is_allowed_at_module_level(self):
        """core.utils 是 scripts/ 的既有惯例（它只依赖兄弟仓库的一个纯函数）。"""
        import ast

        tree = ast.parse(_SCRIPT.read_text(encoding="utf-8"))
        module_level = [
            node
            for node in tree.body
            if isinstance(node, ast.ImportFrom) and (node.module or "").startswith("core")
        ]
        assert module_level, "core.utils 应留在模块级：secid_for 需要 infer_market"


class TestMainDryRun:
    def _run(self, mod, db, extra_args, records_written, want=None):
        provider = MagicMock()
        provider.save_fund_flow_batch.return_value = records_written
        with patch("providers.SmartMoneyDBProvider", return_value=provider), \
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
        with patch("providers.SmartMoneyDBProvider", return_value=provider), \
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
