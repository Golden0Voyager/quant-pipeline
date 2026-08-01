"""集成测试：tasks/bars.py 的 update_bars 调度函数。

使用真实 SQLite 数据库绕过 is_real_db_path 守卫，覆盖：
- 智能探测 SQL 路径（最新跳过 / 需要更新）
- 断点续传 resume 路径
- 序列模式 + AkShareMonitor 中止
- watchlist_init 失败降级
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from core.task_result import TaskStatus, normalize_task_result
from tasks.bars import fetch_bars_for_refresh, normalize_bar_row_for_refresh, update_bars

# ===========================================================================
# Helpers
# ===========================================================================

def _create_real_db(db_path: str) -> None:
    """创建有 stock_list 和 daily_bars 表的真实数据库。"""
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE stock_list (code TEXT, name TEXT)")
    conn.execute("CREATE TABLE daily_bars (ts_code TEXT, trade_date TEXT, open REAL, close REAL)")
    for i in range(5):
        code = f"{i:06d}.SZ"
        conn.execute("INSERT INTO stock_list (code, name) VALUES (?, ?)", (code, f"stock_{i}"))
        conn.execute(
            "INSERT INTO daily_bars (ts_code, trade_date, open, close) VALUES (?, '2026-07-17', 10.0, 10.5)",
            (code,),
        )
    conn.commit()
    conn.close()


def _bars_df(dates: list[str]) -> pd.DataFrame:
    return pd.DataFrame({
        "trade_date": dates,
        "open": [10.0] * len(dates),
        "close": [10.5] * len(dates),
        "data_source": ["akshare"] * len(dates),
    })


# ===========================================================================
# Fixtures
# ===========================================================================

@pytest.fixture
def real_db(tmp_path: Path):
    """创建真实 DB 并返回 Mock(db_path=路径)。"""
    db_path = str(tmp_path / "quant_core.db")
    _create_real_db(db_path)
    db = MagicMock()
    db.db_path = db_path
    return db


# ===========================================================================
# 智能探测（Smart Probe）
# ===========================================================================

class TestBarsSmartProbe:
    """update_bars 智能探测 SQL 路径（需要真实 DB）。

    注意：不能使用 real_db fixture，因为 MagicMock 的 .empty 属性
    在未设置 return_value 时默认为真，导致 stock_list 检查提前返回。
    需在测试中显式设置 db.get_stock_list.return_value。
    """

    @staticmethod
    def _setup_db(tmp_path: Path) -> MagicMock:
        """创建真实 DB + 配置完整的 Mock db。"""
        db_path = str(tmp_path / "quant_core.db")
        _create_real_db(db_path)
        db = MagicMock()
        db.db_path = db_path
        db.get_stock_list.return_value = pd.DataFrame({
            "code": [f"{i:06d}.SZ" for i in range(5)],
        })
        db.get_daily_bars.return_value = _bars_df(["2026-07-17"])
        db.watchlist_get_all.return_value = pd.DataFrame()
        return db

    def test_probe_all_uptodate_skips(self, tmp_path: Path):
        """全部已最新 → 智能探测跳过批量扫描。"""
        db = self._setup_db(tmp_path)
        loader = MagicMock()

        with patch("tasks.bars.get_expected_latest_trading_day", return_value="2026-07-17"), \
             patch("tasks.bars.logger"):
            r = update_bars(db, loader)

        assert r.get("probe_skipped"), f"期望 probe_skipped=True，实际得到: {r}"
        assert r["total"] == 5
        assert r["skipped"] == 5
        loader.incremental_update.assert_not_called()

    def test_probe_stale_continues(self, tmp_path: Path):
        """数据截止 2026-07-17，最新交易日 2026-07-20 → 继续更新。"""
        db = self._setup_db(tmp_path)
        loader = MagicMock()
        loader.incremental_update.return_value = _bars_df(["2026-07-17", "2026-07-18"])

        with patch("tasks.bars.get_expected_latest_trading_day", return_value="2026-07-20"), \
             patch("tasks.bars._detect_suspended_symbols", return_value=set()), \
             patch("tasks.bars.time.sleep"), \
             patch("tasks.bars.logger"):
            r = update_bars(db, loader)

        assert r["success"] > 0
        db.save_daily_bars.assert_called()

    def test_probe_partial_coverage(self, tmp_path: Path):
        """只有部分股票有数据 → 智能探测提示继续更新。"""
        db = self._setup_db(tmp_path)
        loader = MagicMock()
        loader.incremental_update.return_value = _bars_df(["2026-07-17", "2026-07-18"])

        # 删除一条 daily_bars 记录，使 covered < total
        conn = sqlite3.connect(db.db_path)
        conn.execute("DELETE FROM daily_bars WHERE ts_code = '000004.SZ'")
        conn.commit()
        conn.close()

        with patch("tasks.bars.get_expected_latest_trading_day", return_value="2026-07-20"), \
             patch("tasks.bars._detect_suspended_symbols", return_value=set()), \
             patch("tasks.bars.time.sleep"), \
             patch("tasks.bars.logger"):
            r = update_bars(db, loader)

        assert r["success"] > 0
        assert "probe_skipped" not in r

    def test_probe_db_error_fallback(self, tmp_path: Path):
        """DB 查询异常 → 智能探测降级，不中断流程。"""
        db_path = str(tmp_path / "quant_core.db")
        _create_real_db(db_path)
        db = MagicMock()
        # 用一个不存在的父目录路径使 sqlite3.connect 失败
        db.db_path = "/nonexistent_parent_dir/quant_core.db"
        db.get_stock_list.return_value = pd.DataFrame({
            "code": [f"{i:06d}.SZ" for i in range(5)],
        })
        db.get_daily_bars.return_value = _bars_df(["2026-07-17"])
        db.watchlist_get_all.return_value = pd.DataFrame()
        loader = MagicMock()
        loader.incremental_update.return_value = _bars_df(["2026-07-17", "2026-07-18"])

        with patch("tasks.bars.get_expected_latest_trading_day", return_value="2026-07-20"), \
             patch("tasks.bars.time.sleep"), \
             patch("tasks.bars.logger"):
            r = update_bars(db, loader)

        # 智能探测异常降级后应继续正常流程
        assert r["total"] == 5


# ===========================================================================
# 断点续传（Resume）
# ===========================================================================

class TestBarsResume:
    """update_bars resume 路径。"""

    def test_retry_resume_only_processes_failed_queue(self, tmp_path: Path):
        """retry 进度仅重试其失败队列，而非按扫描断点续传。"""
        db = MagicMock()
        loader = MagicMock()
        db.get_stock_list.return_value = pd.DataFrame({
            "code": ["000001.SZ", "000002.SZ", "000003.SZ"],
        })
        db.watchlist_get_all.return_value = pd.DataFrame()

        progress_file = tmp_path / "progress.json"
        progress_file.write_text(json.dumps({
            "task": "retry",
            "date": datetime.now().strftime("%Y-%m-%d"),
            "last_symbol": "000003.SZ",
            "processed": 3,
            "total": 3,
            "failed_queue": ["000002.SZ"],
        }))

        with patch("tasks.bars.ProgressTracker.FILE", progress_file), \
             patch("tasks.bars.PARALLEL_WORKERS", 1), \
             patch("tasks.bars._update_single_bar", return_value="success") as update_one, \
             patch("tasks.bars.time.sleep"), \
             patch("tasks.bars.logger"):
            result = update_bars(db, loader, resume=True)

        update_one.assert_called_once()
        assert update_one.call_args.args[2] == "000002.SZ"
        assert result["status"] == "success"
        assert result["saved"] == 1
        assert result["attempted"] == 1
        assert result["failed"] == 0

    @pytest.mark.parametrize(
        "scope_kwargs",
        [{"limit": 1}, {"symbols": ["000001.SZ"]}],
        ids=["limit", "symbols"],
    )
    def test_resume_rejects_scope_restrictions_without_changing_progress(
        self,
        tmp_path: Path,
        scope_kwargs: dict[str, object],
    ):
        """resume 与范围限制组合必须显式失败并保留原 checkpoint。"""
        db = MagicMock()
        db.get_stock_list.return_value = pd.DataFrame({
            "code": ["000001.SZ", "000002.SZ"],
        })
        progress_file = tmp_path / "progress.json"
        original = {
            "task": "retry",
            "date": datetime.now().strftime("%Y-%m-%d"),
            "last_symbol": "000001.SZ",
            "processed": 1,
            "total": 2,
            "failed_queue": ["000001.SZ", "000002.SZ"],
        }
        progress_file.write_text(json.dumps(original))

        with patch("tasks.bars.ProgressTracker.FILE", progress_file), \
             patch("tasks.bars.ProgressTracker.clear") as mock_clear, \
             patch("tasks.bars.ProgressTracker.save") as mock_save:
            result = update_bars(
                db,
                MagicMock(),
                resume=True,
                **scope_kwargs,
            )

        assert result["status"] == "failed"
        assert result["error_kind"] == "data_quality"
        assert json.loads(progress_file.read_text()) == original
        mock_save.assert_not_called()
        mock_clear.assert_not_called()

    def test_retry_abort_checkpoint_keeps_unattempted_suffix(self, tmp_path: Path):
        """retry 中途熔断时保存失败项和所有尚未尝试项。"""
        db = MagicMock()
        loader = MagicMock()
        codes = ["000001.SZ", "000002.SZ", "000003.SZ"]
        db.get_stock_list.return_value = pd.DataFrame({"code": codes})
        db.watchlist_get_all.return_value = pd.DataFrame()

        progress_file = tmp_path / "progress.json"
        progress_file.write_text(json.dumps({
            "task": "retry",
            "date": datetime.now().strftime("%Y-%m-%d"),
            "last_symbol": "000003.SZ",
            "processed": 0,
            "total": 3,
            "failed_queue": codes,
        }))

        with patch("tasks.bars.ProgressTracker.FILE", progress_file), \
             patch("tasks.bars.ProgressTracker.LOCK_FILE", tmp_path / "progress.lock"), \
             patch("tasks.bars.AkShareMonitor.FILE", tmp_path / "monitor.json"), \
             patch("tasks.bars.AkShareMonitor.should_abort", return_value=(True, "stop")), \
             patch("tasks.bars.PARALLEL_WORKERS", 1), \
             patch("tasks.bars._update_single_bar", return_value="failed") as update_one, \
             patch("tasks.bars.time.sleep"), \
             patch("tasks.bars.logger"):
            result = update_bars(db, loader, resume=True)

        assert result["status"] == "aborted"
        update_one.assert_called_once()
        saved = json.loads(progress_file.read_text())
        assert saved["task"] == "retry"
        assert saved["processed"] == 1
        assert saved["failed_queue"] == codes

    def test_retry_batch_checkpoint_keeps_unattempted_suffix(self, tmp_path: Path):
        """retry 首批完成时 checkpoint 仍包含后续批次。"""
        db = MagicMock()
        loader = MagicMock()
        codes = ["000001.SZ", "000002.SZ", "000003.SZ"]
        db.get_stock_list.return_value = pd.DataFrame({"code": codes})
        db.watchlist_get_all.return_value = pd.DataFrame()
        progress_file = tmp_path / "progress.json"
        progress_file.write_text(json.dumps({
            "task": "retry",
            "date": datetime.now().strftime("%Y-%m-%d"),
            "failed_queue": codes,
        }))
        save_calls: list[dict[str, object]] = []

        with patch("tasks.bars.ProgressTracker.FILE", progress_file), \
             patch("tasks.bars.ProgressTracker.LOCK_FILE", tmp_path / "progress.lock"), \
             patch("tasks.bars.ProgressTracker.save", side_effect=lambda **kw: save_calls.append(kw)), \
             patch("tasks.bars.BATCH_SIZE", 2), \
             patch("tasks.bars.PARALLEL_WORKERS", 1), \
             patch("tasks.bars.PROGRESS_FLUSH_INTERVAL", 99), \
             patch("tasks.bars._update_single_bar", return_value="success"), \
             patch("tasks.bars.time.sleep"), \
             patch("tasks.bars.logger"):
            update_bars(db, loader, resume=True)

        first_batch = next(
            call for call in save_calls if call["last_symbol"] == "000002.SZ"
        )
        assert first_batch["task"] == "retry"
        assert first_batch["processed"] == 2
        assert first_batch["failed_queue"] == ["000003.SZ"]

    def test_retry_resume_with_no_eligible_failures_is_no_data(self, tmp_path: Path):
        """retry 队列过滤为空是合法零工作量。"""
        db = MagicMock()
        loader = MagicMock()
        db.get_stock_list.return_value = pd.DataFrame({"code": ["000001.SZ"]})

        progress_file = tmp_path / "progress.json"
        progress_file.write_text(json.dumps({
            "task": "retry",
            "date": datetime.now().strftime("%Y-%m-%d"),
            "last_symbol": "000001.SZ",
            "processed": 9,
            "total": 9,
            "failed_queue": ["999999.SZ"],
        }))

        with patch("tasks.bars.ProgressTracker.FILE", progress_file), \
             patch("tasks.bars._update_single_bar") as update_one, \
             patch("tasks.bars.logger"):
            result = update_bars(db, loader, resume=True)

        update_one.assert_not_called()
        assert result["status"] == "no_data"
        assert result["attempted"] == 0
        assert result["reason"]

    @pytest.mark.parametrize("progress_task", [None, "update_bars"])
    def test_scan_resume_reports_unresolved_failures(
        self,
        tmp_path: Path,
        progress_task: str | None,
    ):
        """扫描续传继承的失败队列仍属于本轮最终未解决失败。"""
        db = MagicMock()
        loader = MagicMock()
        db.get_stock_list.return_value = pd.DataFrame({
            "code": ["000001.SZ", "000002.SZ", "000003.SZ"],
        })
        db.watchlist_get_all.return_value = pd.DataFrame()

        progress = {
            "date": datetime.now().strftime("%Y-%m-%d"),
            "last_symbol": "000001.SZ",
            "processed": 1,
            "total": 3,
            "failed_queue": ["000001.SZ"],
        }
        if progress_task is not None:
            progress["task"] = progress_task
        progress_file = tmp_path / "progress.json"
        progress_file.write_text(json.dumps(progress))

        with patch("tasks.bars.ProgressTracker.FILE", progress_file), \
             patch("tasks.bars.PARALLEL_WORKERS", 1), \
             patch("tasks.bars._update_single_bar", return_value="success") as update_one, \
             patch("tasks.bars.time.sleep"), \
             patch("tasks.bars.logger"):
            result = update_bars(db, loader, resume=True)

        assert [call.args[2] for call in update_one.call_args_list] == [
            "000002.SZ",
            "000003.SZ",
        ]
        assert result["status"] == "degraded"
        assert result["saved"] == 2
        assert result["attempted"] == 2
        assert result["failed"] == 1
        assert result["failed_symbols"] == ["000001.SZ"]
        normalised = normalize_task_result("update_bars", result)
        assert normalised.status is TaskStatus.DEGRADED
        assert normalised.attempted == result["attempted"]

    def test_unknown_progress_does_not_contaminate_current_run(self, tmp_path: Path):
        """未知任务的 processed/failed_queue 不得进入本轮结果。"""
        db = MagicMock()
        loader = MagicMock()
        db.get_stock_list.return_value = pd.DataFrame({
            "code": ["000001.SZ", "000002.SZ"],
        })
        db.watchlist_get_all.return_value = pd.DataFrame()

        progress_file = tmp_path / "progress.json"
        progress_file.write_text(json.dumps({
            "task": "other_task",
            "date": datetime.now().strftime("%Y-%m-%d"),
            "last_symbol": "000002.SZ",
            "processed": 41,
            "total": 41,
            "failed_queue": ["999999.SZ"],
        }))

        with patch("tasks.bars.ProgressTracker.FILE", progress_file), \
             patch("tasks.bars.PARALLEL_WORKERS", 1), \
             patch("tasks.bars._update_single_bar", return_value="success"), \
             patch("tasks.bars.time.sleep"), \
             patch("tasks.bars.logger"):
            result = update_bars(db, loader, resume=True)

        assert result["status"] == "success"
        assert result["attempted"] == 2
        assert result["failed"] == 0
        assert result["failed_symbols"] == []

    def test_expired_progress_does_not_contaminate_current_run(self, tmp_path: Path):
        """过期进度清理后不得继承 processed/failed_queue。"""
        db = MagicMock()
        loader = MagicMock()
        db.get_stock_list.return_value = pd.DataFrame({"code": ["000001.SZ"]})
        db.watchlist_get_all.return_value = pd.DataFrame()

        progress_file = tmp_path / "progress.json"
        progress_file.write_text(json.dumps({
            "task": "update_bars",
            "date": (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
            "last_symbol": "000001.SZ",
            "processed": 17,
            "total": 17,
            "failed_queue": ["999999.SZ"],
        }))

        with patch("tasks.bars.ProgressTracker.FILE", progress_file), \
             patch("tasks.bars.PARALLEL_WORKERS", 1), \
             patch("tasks.bars._update_single_bar", return_value="success"), \
             patch("tasks.bars.time.sleep"), \
             patch("tasks.bars.logger"):
            result = update_bars(db, loader, resume=True)

        assert result["status"] == "success"
        assert result["attempted"] == 1
        assert result["failed"] == 0
        assert result["failed_symbols"] == []

    def test_resume_from_checkpoint(self, tmp_path: Path):
        """从断点继续，跳过已处理的股票。"""
        db = MagicMock()
        loader = MagicMock()
        db.get_stock_list.return_value = pd.DataFrame({
            "code": [f"{i:06d}.SZ" for i in range(5)],
        })
        db.get_daily_bars.return_value = _bars_df(["2026-07-17"])
        loader.incremental_update.return_value = _bars_df(["2026-07-17", "2026-07-18"])
        db.watchlist_get_all.return_value = pd.DataFrame()

        progress_file = tmp_path / "progress.json"
        progress_file.write_text(json.dumps({
            "last_symbol": "000002.SZ",
            "date": datetime.now().strftime("%Y-%m-%d"),
            "failed_queue": ["000000.SZ"],
        }))

        with patch("tasks.bars.ProgressTracker.FILE", progress_file), \
             patch("tasks.bars.time.sleep"), \
             patch("tasks.bars.logger"):
            r = update_bars(db, loader, resume=True)

        assert r["total"] == 5
        # 至少有 2 只股票被处理（跳过前 3 只中最少已处理 1 只）
        assert r["success"] + r["failed"] + r["skipped"] >= 2

    def test_resume_with_old_date_clears(self, tmp_path: Path):
        """进度文件是昨天的 → 清除并从头开始。"""
        db = MagicMock()
        loader = MagicMock()
        db.get_stock_list.return_value = pd.DataFrame({
            "code": [f"{i:06d}.SZ" for i in range(3)],
        })
        db.get_daily_bars.return_value = _bars_df(["2026-07-17"])
        loader.incremental_update.return_value = _bars_df(["2026-07-17", "2026-07-18"])
        db.watchlist_get_all.return_value = pd.DataFrame()

        progress_file = tmp_path / "progress.json"
        progress_file.write_text(json.dumps({
            "last_symbol": "000002.SZ",
            "date": "2026-07-01",  # 旧的日期
        }))

        with patch("tasks.bars.ProgressTracker.FILE", progress_file), \
             patch("tasks.bars.time.sleep"), \
             patch("tasks.bars.logger"):
            r = update_bars(db, loader, resume=True)

        assert r["total"] == 3
        assert not progress_file.exists()

    def test_resume_no_progress_file(self, tmp_path: Path):
        """无进度文件 → 从头开始。"""
        db = MagicMock()
        loader = MagicMock()
        db.get_stock_list.return_value = pd.DataFrame({
            "code": [f"{i:06d}.SZ" for i in range(3)],
        })
        db.get_daily_bars.return_value = _bars_df(["2026-07-17"])
        loader.incremental_update.return_value = _bars_df(["2026-07-17", "2026-07-18"])
        db.watchlist_get_all.return_value = pd.DataFrame()

        progress_file = tmp_path / "progress.json"

        with patch("tasks.bars.ProgressTracker.FILE", progress_file), \
             patch("tasks.bars.time.sleep"), \
             patch("tasks.bars.logger"):
            r = update_bars(db, loader, resume=True)

        assert r["total"] == 3
        assert r["success"] >= 3

    def test_non_resume_clears_old_progress(self, tmp_path: Path):
        """非续传模式清理旧进度文件。"""
        db = MagicMock()
        loader = MagicMock()
        db.get_stock_list.return_value = pd.DataFrame({
            "code": [f"{i:06d}.SZ" for i in range(3)],
        })
        db.get_daily_bars.return_value = _bars_df(["2026-07-17"])
        loader.incremental_update.return_value = _bars_df(["2026-07-17", "2026-07-18"])
        db.watchlist_get_all.return_value = pd.DataFrame()

        progress_file = tmp_path / "progress.json"
        progress_file.write_text(json.dumps({"last_symbol": "000002.SZ"}))

        with patch("tasks.bars.ProgressTracker.FILE", progress_file), \
             patch("tasks.bars.time.sleep"), \
             patch("tasks.bars.logger"):
            r = update_bars(db, loader, resume=False)

        assert r["total"] == 3
        # 非续传模式应清理旧进度文件
        assert not progress_file.exists()


# ===========================================================================
# 序列模式 + watchlist
# ===========================================================================

class TestBarsSerialMode:
    """序列模式的日志 / 进度刷新 / 休眠 / 中止路径。"""

    def test_watchlist_init_failure(self):
        """watchlist 初始化失败不中断流程。"""
        db = MagicMock()
        loader = MagicMock()
        db.get_stock_list.return_value = pd.DataFrame({
            "code": ["000001.SZ", "000002.SZ"],
        })
        db.get_daily_bars.return_value = _bars_df(["2026-07-17"])
        loader.incremental_update.return_value = _bars_df(["2026-07-17", "2026-07-18"])
        db.watchlist_get_all.side_effect = Exception("DB error")

        with patch("tasks.bars.get_expected_latest_trading_day", return_value="2026-07-20"), \
             patch("tasks.bars.time.sleep"), \
             patch("tasks.bars.logger"):
            r = update_bars(db, loader)

        assert r["success"] == 2

    def test_serial_batch_uses_single_thread(self):
        """PARALLEL_WORKERS=1 时使用串行分支。"""
        db = MagicMock()
        loader = MagicMock()
        db.get_stock_list.return_value = pd.DataFrame({
            "code": ["000001.SZ", "000002.SZ"],
        })
        db.get_daily_bars.return_value = _bars_df(["2026-07-17"])
        loader.incremental_update.return_value = _bars_df(["2026-07-17", "2026-07-18"])
        db.watchlist_get_all.return_value = pd.DataFrame()

        with patch("tasks.bars.PARALLEL_WORKERS", 1), \
             patch("tasks.bars.get_expected_latest_trading_day", return_value="2026-07-20"), \
             patch("tasks.bars.time.sleep"), \
             patch("tasks.bars.logger"):
            r = update_bars(db, loader)

        assert r["success"] == 2

    def test_full_load_without_existing_data(self):
        """无现有数据的全量加载路径。"""
        db = MagicMock()
        loader = MagicMock()
        db.get_stock_list.return_value = pd.DataFrame({
            "code": ["000001.SZ"],
        })
        db.get_daily_bars.return_value = pd.DataFrame()  # 无现有数据
        loader.get_daily_bars.return_value = _bars_df(["2026-07-17", "2026-07-18"])
        db.watchlist_get_all.return_value = pd.DataFrame()

        with patch("tasks.bars.get_expected_latest_trading_day", return_value="2026-07-20"), \
             patch("tasks.bars.time.sleep"), \
             patch("tasks.bars.BATCH_SIZE", 30), \
             patch("tasks.bars.logger"):
            r = update_bars(db, loader)

        assert r["success"] == 1
        db.save_daily_bars.assert_called_once()

    def test_serial_mode_no_skipped_doesnt_sleep_via_monitor(self):
        """serial 模式: 非 skipped 时调用 monitor.record 并 sleep。"""
        db = MagicMock()
        loader = MagicMock()
        db.get_stock_list.return_value = pd.DataFrame({
            "code": ["000001.SZ", "000002.SZ"],
        })
        db.get_daily_bars.return_value = _bars_df(["2026-07-17"])
        loader.incremental_update.return_value = _bars_df(["2026-07-17", "2026-07-18"])
        db.watchlist_get_all.return_value = pd.DataFrame()

        with patch("tasks.bars.PARALLEL_WORKERS", 1), \
             patch("tasks.bars.get_expected_latest_trading_day", return_value="2026-07-20"), \
             patch("tasks.bars.time.sleep") as mock_sleep, \
             patch("tasks.bars.logger"):
            r = update_bars(db, loader)

        assert r["success"] == 2
        # 串行模式下每个非 skipped 股票调用 2 次 sleep（抖动延迟 + 动态限流）
        assert mock_sleep.call_count >= 4


# ===========================================================================
# 边界条件
# ===========================================================================

class TestBarsBoundary:
    """update_bars 其他边界路径。"""

    def test_limit_zero_returns_all(self):
        """limit=0 时无限制。"""
        db = MagicMock()
        loader = MagicMock()
        db.get_stock_list.return_value = pd.DataFrame({
            "code": ["000001.SZ", "000002.SZ", "000003.SZ"],
        })
        db.get_daily_bars.return_value = _bars_df(["2026-07-17"])
        loader.incremental_update.return_value = _bars_df(["2026-07-17", "2026-07-18"])
        db.watchlist_get_all.return_value = pd.DataFrame()

        with patch("tasks.bars.get_expected_latest_trading_day", return_value="2026-07-20"), \
             patch("tasks.bars.time.sleep"), \
             patch("tasks.bars.logger"):
            r = update_bars(db, loader, limit=0)

        assert r["total"] == 3

    def test_symbols_mode_with_bj_filter(self):
        """--symbols 模式过滤北交所。"""
        db = MagicMock()
        loader = MagicMock()
        db.get_daily_bars.return_value = _bars_df(["2026-07-17"])
        loader.incremental_update.return_value = _bars_df(["2026-07-17", "2026-07-18"])
        db.watchlist_get_all.return_value = pd.DataFrame()

        with patch("tasks.bars.get_expected_latest_trading_day", return_value="2026-07-20"), \
             patch("tasks.bars.time.sleep"), \
             patch("tasks.bars.should_skip_beijing",
                    side_effect=lambda s: s.startswith("8")), \
             patch("tasks.bars.logger"):
            r = update_bars(
                db, loader,
                symbols=["000001.SZ", "880001.BJ", "000002.SZ"],
            )

        # 880001.BJ 被过滤，只有 2 只
        assert r["total"] == 2
        assert r["success"] == 2

    def test_symbols_mode_all_bj_filtered(self):
        """--symbols 全部是北交所 → total=0。"""
        db = MagicMock()
        loader = MagicMock()
        db.watchlist_get_all.return_value = pd.DataFrame()

        with patch("tasks.bars.get_expected_latest_trading_day", return_value="2026-07-20"), \
             patch("tasks.bars.time.sleep"), \
             patch("tasks.bars.should_skip_beijing",
                    side_effect=lambda s: True), \
             patch("tasks.bars.logger"):
            r = update_bars(
                db, loader,
                symbols=["830001.BJ", "830002.BJ"],
            )

        assert r["total"] == 0
        assert r["status"] == "no_data"
        assert r["reason"]

    def test_empty_stock_list_logs_error_and_returns(self):
        """股票列表为空 → 立即返回。"""
        db = MagicMock()
        db.get_stock_list.return_value = pd.DataFrame()
        loader = MagicMock()

        with patch("tasks.bars.logger"):
            r = update_bars(db, loader)

        assert r["total"] == 0
        assert r["status"] == "no_data"
        assert r["reason"]

    def test_final_failed_symbols_saved_to_progress(self, tmp_path: Path):
        """部分失败时记录失败队列到 progress.json。"""
        db = MagicMock()
        loader = MagicMock()
        db.get_stock_list.return_value = pd.DataFrame({
            "code": ["000001.SZ", "000002.SZ", "000003.SZ"],
        })
        # _update_single_bar 先调用 get_latest_bar_date，然后 get_daily_bars
        db.get_latest_bar_date.side_effect = Exception("DB error")
        db.watchlist_get_all.return_value = pd.DataFrame()

        progress_file = tmp_path / "progress.json"

        # patch monitor.should_abort 防止监控器中途中止
        with patch("tasks.bars.AkShareMonitor.should_abort", return_value=(False, "")), \
             patch("tasks.bars.ProgressTracker.FILE", progress_file), \
             patch("tasks.bars.MAX_RETRY", 1), \
             patch("tasks.bars.time.sleep"), \
             patch("tasks.bars.logger"):
            r = update_bars(db, loader)

        assert r["failed"] == 3
        # progress.json 应以 retry 任务保存失败队列
        assert progress_file.exists(), "progress.json 应存在"
        saved = json.loads(progress_file.read_text())
        assert saved["task"] == "retry"
        assert len(saved["failed_queue"]) == 3

    def test_all_succeed_clears_progress(self, tmp_path: Path):
        """全部成功时清除进度文件。"""
        db = MagicMock()
        loader = MagicMock()
        db.get_stock_list.return_value = pd.DataFrame({
            "code": ["000001.SZ"],
        })
        db.get_daily_bars.return_value = _bars_df(["2026-07-17"])
        loader.incremental_update.return_value = _bars_df(["2026-07-17", "2026-07-18"])
        db.watchlist_get_all.return_value = pd.DataFrame()

        progress_file = tmp_path / "progress.json"
        progress_file.write_text(json.dumps({"old": "data"}))

        with patch("tasks.bars.ProgressTracker.FILE", progress_file), \
             patch("tasks.bars.get_expected_latest_trading_day", return_value="2026-07-20"), \
             patch("tasks.bars.time.sleep"), \
             patch("tasks.bars.logger"):
            r = update_bars(db, loader)

        assert r["success"] == 1
        assert r["failed"] == 0
        normalised = normalize_task_result("update_bars", r)
        assert normalised.status is TaskStatus.SUCCESS
        # 全部成功后应清除进度文件
        assert not progress_file.exists()

    def test_none_and_friday_progress_cleared(self, tmp_path: Path):
        """resume 时 ProgressTracker.load 返回 None → 从头开始。"""
        db = MagicMock()
        loader = MagicMock()
        db.get_stock_list.return_value = pd.DataFrame({
            "code": ["000001.SZ"],
        })
        db.get_daily_bars.return_value = _bars_df(["2026-07-17"])
        loader.incremental_update.return_value = _bars_df(["2026-07-17", "2026-07-18"])
        db.watchlist_get_all.return_value = pd.DataFrame()

        progress_file = tmp_path / "nonexistent.json"

        with patch("tasks.bars.ProgressTracker.FILE", progress_file), \
             patch("tasks.bars.get_expected_latest_trading_day", return_value="2026-07-20"), \
             patch("tasks.bars.time.sleep"), \
             patch("tasks.bars.logger"):
            r = update_bars(db, loader, resume=True)

        assert r["total"] == 1
        assert r["success"] == 1


# ===========================================================================
# 熔断哨兵验证（Canary Probe）
# ===========================================================================

class TestCanaryCircuitBreaker:
    """连续失败熔断前的哨兵验证逻辑。"""

    def _make_failing_run(self, tmp_path: Path):
        """构造 4 只股票全部失败的场景（db.get_latest_bar_date 抛异常）。"""
        db = MagicMock()
        loader = MagicMock()
        db.get_stock_list.return_value = pd.DataFrame({
            "code": ["000010.SZ", "000020.SZ", "000030.SZ", "000040.SZ"],
        })
        db.get_latest_bar_date.side_effect = Exception("source down")
        db.watchlist_get_all.return_value = pd.DataFrame()
        return db, loader, tmp_path / "progress.json"

    def test_canary_success_prevents_abort(self, tmp_path: Path):
        """连续 3 次失败但哨兵拉取成功 → 不熔断，跑完全部股票。"""
        db, loader, progress_file = self._make_failing_run(tmp_path)
        # 哨兵请求走 loader.get_daily_bars，返回有效数据
        loader.get_daily_bars.return_value = _bars_df(["2026-07-18"])

        with patch("tasks.bars.ProgressTracker.FILE", progress_file), \
             patch("tasks.bars.AkShareMonitor.FILE", tmp_path / "monitor.json"), \
             patch("tasks.bars.PARALLEL_WORKERS", 1), \
             patch("tasks.bars.MAX_RETRY", 1), \
             patch("tasks.bars.time.sleep"), \
             patch("tasks.bars.logger"):
            r = update_bars(db, loader)

        # 4 只全部处理完（未提前熔断退出），失败队列完整
        assert r["failed"] == 4
        assert len(r["failed_symbols"]) == 4
        # 哨兵确实被调用过（第 3 次连续失败时触发）
        assert loader.get_daily_bars.called

    def test_canary_failure_confirms_abort(self, tmp_path: Path):
        """连续 3 次失败且哨兵也失败 → 照常熔断，剩余股票不处理。"""
        db, loader, progress_file = self._make_failing_run(tmp_path)
        loader.get_daily_bars.return_value = pd.DataFrame()  # 哨兵返回空

        with patch("tasks.bars.ProgressTracker.FILE", progress_file), \
             patch("tasks.bars.AkShareMonitor.FILE", tmp_path / "monitor.json"), \
             patch("tasks.bars.PARALLEL_WORKERS", 1), \
             patch("tasks.bars.MAX_RETRY", 1), \
             patch("tasks.bars.time.sleep"), \
             patch("tasks.bars.logger"):
            r = update_bars(db, loader)

        # 第 3 只失败后熔断，第 4 只未处理
        assert r["failed"] == 3
        assert r["status"] == "aborted"
        normalised = normalize_task_result("update_bars", r)
        assert normalised.status is TaskStatus.ABORTED
        assert normalised.saved == r["saved"]
        assert normalised.attempted == r["attempted"]
        assert normalised.error == r["error"]

    def test_abort_sends_notification(self, tmp_path: Path):
        """熔断中止时必须外发 error 级告警（无人值守场景的感知渠道）。"""
        db, loader, progress_file = self._make_failing_run(tmp_path)
        loader.get_daily_bars.return_value = pd.DataFrame()  # 哨兵返回空

        with patch("tasks.bars.ProgressTracker.FILE", progress_file), \
             patch("tasks.bars.AkShareMonitor.FILE", tmp_path / "monitor.json"), \
             patch("tasks.bars.PARALLEL_WORKERS", 1), \
             patch("tasks.bars.MAX_RETRY", 1), \
             patch("tasks.bars.time.sleep"), \
             patch("tasks.bars.logger"), \
             patch("tasks.bars.notify_all") as mock_notify:
            r = update_bars(db, loader)

        assert r["status"] == "aborted"
        mock_notify.assert_called_once()
        assert mock_notify.call_args.args[0] == "error"

    def test_canary_rejects_pure_yfinance_data(self, tmp_path: Path):
        """哨兵返回纯 yfinance 数据 → 视为 AkShare 不可用，照常熔断。"""
        db, loader, progress_file = self._make_failing_run(tmp_path)
        yf_df = _bars_df(["2026-07-18"])
        yf_df["data_source"] = "yfinance"
        loader.get_daily_bars.return_value = yf_df

        with patch("tasks.bars.ProgressTracker.FILE", progress_file), \
             patch("tasks.bars.AkShareMonitor.FILE", tmp_path / "monitor.json"), \
             patch("tasks.bars.PARALLEL_WORKERS", 1), \
             patch("tasks.bars.MAX_RETRY", 1), \
             patch("tasks.bars.time.sleep"), \
             patch("tasks.bars.logger"):
            r = update_bars(db, loader)

        assert r["failed"] == 3

    def test_canary_probe_limit(self, tmp_path: Path):
        """哨兵验证次数耗尽后直接熔断，不再无限探测。"""
        db = MagicMock()
        loader = MagicMock()
        # 13 只全部失败：每 3 次连续失败触发一次哨兵（3 次额度），
        # 第 12 次失败后额度耗尽 → 熔断
        codes = [f"{i:06d}.SZ" for i in range(10, 23)]
        db.get_stock_list.return_value = pd.DataFrame({"code": codes})
        db.get_latest_bar_date.side_effect = Exception("source down")
        db.watchlist_get_all.return_value = pd.DataFrame()
        loader.get_daily_bars.return_value = _bars_df(["2026-07-18"])  # 哨兵永远成功

        with patch("tasks.bars.ProgressTracker.FILE", tmp_path / "progress.json"), \
             patch("tasks.bars.AkShareMonitor.FILE", tmp_path / "monitor.json"), \
             patch("tasks.bars.PARALLEL_WORKERS", 1), \
             patch("tasks.bars.MAX_RETRY", 1), \
             patch("tasks.bars.time.sleep"), \
             patch("tasks.bars.logger"):
            r = update_bars(db, loader)

        # 额度 3 次用完后，第 4 轮连续 3 次失败直接熔断 → 12 只失败，第 13 只未处理
        assert r["failed"] == 12
        # 哨兵正好探测 3 次（loader.get_daily_bars 仅由哨兵调用）
        assert loader.get_daily_bars.call_count == 3


# ===========================================================================
# 全 skip 批次不休息
# ===========================================================================

class TestBatchSleepSkip:
    """批次内零网络请求时跳过批次间休息。"""

    def test_all_skipped_batches_do_not_sleep(self, tmp_path: Path):
        """多批次全部 skipped → 不调用批次间 time.sleep。"""
        db = MagicMock()
        loader = MagicMock()
        codes = [f"{i:06d}.SZ" for i in range(150)]  # 2 个批次
        db.get_stock_list.return_value = pd.DataFrame({"code": codes})
        # 数据已是最新 → 全部 skipped，零网络请求
        db.get_latest_bar_date.return_value = "2026-07-20"
        db.watchlist_get_all.return_value = pd.DataFrame()

        with patch("tasks.bars.ProgressTracker.FILE", tmp_path / "progress.json"), \
             patch("tasks.bars.AkShareMonitor.FILE", tmp_path / "monitor.json"), \
             patch("tasks.bars.PARALLEL_WORKERS", 1), \
             patch("tasks.bars.get_expected_latest_trading_day", return_value="2026-07-20"), \
             patch("tasks.bars.time.sleep") as mock_sleep, \
             patch("tasks.bars.logger"):
            r = update_bars(db, loader)

        assert r["skipped"] == 150
        mock_sleep.assert_not_called()

    def test_batch_with_network_activity_still_sleeps(self, tmp_path: Path):
        """批次内有真实网络请求 → 批次间休息保留。"""
        db = MagicMock()
        loader = MagicMock()
        codes = [f"{i:06d}.SZ" for i in range(101)]  # 2 个批次
        db.get_stock_list.return_value = pd.DataFrame({"code": codes})
        db.get_latest_bar_date.return_value = "2026-07-17"  # 落后 → 需要更新
        db.get_daily_bars.return_value = _bars_df(["2026-07-17"])
        loader.incremental_update.return_value = _bars_df(["2026-07-17", "2026-07-20"])
        db.watchlist_get_all.return_value = pd.DataFrame()

        with patch("tasks.bars.ProgressTracker.FILE", tmp_path / "progress.json"), \
             patch("tasks.bars.AkShareMonitor.FILE", tmp_path / "monitor.json"), \
             patch("tasks.bars.PARALLEL_WORKERS", 1), \
             patch("tasks.bars.get_expected_latest_trading_day", return_value="2026-07-20"), \
             patch("tasks.bars.time.sleep") as mock_sleep, \
             patch("tasks.bars.logger"):
            r = update_bars(db, loader)

        assert r["success"] == 101
        assert mock_sleep.called


# ===========================================================================
# 停牌预检（Suspended Precheck）
# ===========================================================================

def _stub_xueqiu_modules(mock_xq):
    """构造可注入 sys.modules 的 smartmoney_hunter stub。"""
    import types

    pkg = types.ModuleType("smartmoney_hunter")
    pkg.xueqiu = mock_xq
    return {"smartmoney_hunter": pkg, "smartmoney_hunter.xueqiu": mock_xq}


class TestSuspendedPrecheck:
    """运行前停牌预检：雪球 status + 东财停复牌名单。"""

    def _make_db(self, tmp_path: Path, lagging_dates: dict[str, str]):
        """建真实库：指定股票的最新日线日期。"""
        db_path = str(tmp_path / "quant_core.db")
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE daily_bars (ts_code TEXT, trade_date TEXT)")
        for code, d in lagging_dates.items():
            conn.execute("INSERT INTO daily_bars VALUES (?, ?)", (f"{code}.SZ", d))
        conn.commit()
        conn.close()
        db = MagicMock()
        db.db_path = db_path
        return db

    def test_detects_suspended_via_xueqiu_status(self, tmp_path: Path):
        """落后股中雪球 status==2 的被识别为停牌。"""
        import sys

        from tasks.bars import _detect_suspended_symbols

        db = self._make_db(tmp_path, {"002036": "2026-07-22", "000001": "2026-07-24"})
        mock_xq = MagicMock()
        mock_xq.get_batch_quotes.return_value = [
            {"code": "002036", "status": 2},
        ]
        ak = MagicMock()
        ak.stock_tfp_em.return_value = pd.DataFrame()  # 东财无数据
        with patch("tasks.bars.ak", ak), \
             patch.dict(sys.modules, _stub_xueqiu_modules(mock_xq)):
            result = _detect_suspended_symbols(db, "2026-07-24")

        assert result == {"002036"}
        # 只查了落后股（000001 已最新，不在查询列表）
        mock_xq.get_batch_quotes.assert_called_once_with(["002036"])

    def test_tfp_covers_beijing_and_merges(self, tmp_path: Path):
        """东财停复牌名单覆盖北交所，与雪球结果合并。"""
        import sys

        from tasks.bars import _detect_suspended_symbols

        db = self._make_db(tmp_path, {"920685": "2026-07-15", "300242": "2026-07-22"})
        ak = MagicMock()
        ak.stock_tfp_em.return_value = pd.DataFrame({"代码": ["920685"]})
        mock_xq = MagicMock()
        # 带市场前缀的变体也应被归一为 6 位码（防御 [-6:] 截取）
        mock_xq.get_batch_quotes.return_value = [{"code": "SZ300242", "status": 2}]
        with patch("tasks.bars.ak", ak), \
             patch.dict(sys.modules, _stub_xueqiu_modules(mock_xq)):
            result = _detect_suspended_symbols(db, "2026-07-24")

        assert result == {"920685", "300242"}
        # 北交所股不会送入雪球查询
        mock_xq.get_batch_quotes.assert_called_once_with(["300242"])

    def test_skips_when_too_many_lagging(self, tmp_path: Path):
        """落后股超过阈值（正常交易日全市场落后）→ 不发起任何网络请求。"""
        from tasks.bars import _detect_suspended_symbols

        db = self._make_db(tmp_path, {f"{i:06d}": "2026-07-22" for i in range(60)})
        ak = MagicMock()
        with patch("tasks.bars.ak", ak):
            result = _detect_suspended_symbols(db, "2026-07-24")

        assert result == set()
        ak.stock_tfp_em.assert_not_called()

    def test_guards_no_real_db_and_source_errors(self, tmp_path: Path):
        """非真实 db / 两源均异常 → 静默降级不抛错。"""
        import sys

        from tasks.bars import _detect_suspended_symbols

        # 非真实 db_path（MagicMock）→ 直接空集
        assert _detect_suspended_symbols(MagicMock(), "2026-07-24") == set()

        # 两个数据源都抛异常 → 空集，不抛错
        db = self._make_db(tmp_path, {"002036": "2026-07-22"})
        ak = MagicMock()
        ak.stock_tfp_em.side_effect = RuntimeError("net")
        mock_xq = MagicMock()
        mock_xq.get_batch_quotes.side_effect = RuntimeError("net")
        with patch("tasks.bars.ak", ak), \
             patch.dict(sys.modules, _stub_xueqiu_modules(mock_xq)):
            assert _detect_suspended_symbols(db, "2026-07-24") == set()

    def test_update_bars_skips_suspended_not_failed(self, tmp_path: Path):
        """集成：停牌股记 skipped 而非 failed，无失败队列。"""
        db = MagicMock()
        loader = MagicMock()
        db.get_stock_list.return_value = pd.DataFrame({"code": ["002036.SZ", "000001.SZ"]})
        # 两只都落后；002036 停牌，000001 正常更新成功
        db.get_latest_bar_date.return_value = "2026-07-22"
        db.get_daily_bars.return_value = _bars_df(["2026-07-22"])
        loader.incremental_update.return_value = _bars_df(["2026-07-22", "2026-07-24"])
        db.watchlist_get_all.return_value = pd.DataFrame()

        with patch("tasks.bars.ProgressTracker.FILE", tmp_path / "progress.json"), \
             patch("tasks.bars.AkShareMonitor.FILE", tmp_path / "monitor.json"), \
             patch("tasks.bars.PARALLEL_WORKERS", 1), \
             patch("tasks.bars._detect_suspended_symbols", return_value={"002036"}), \
             patch("tasks.bars.get_expected_latest_trading_day", return_value="2026-07-24"), \
             patch("tasks.bars.time.sleep"), \
             patch("tasks.bars.logger"):
            r = update_bars(db, loader)

        assert r["failed"] == 0
        assert r["skipped"] == 1
        assert r["success"] == 1
        assert r["failed_symbols"] == []
        # 停牌股未发起任何数据拉取
        assert all(
            call.args[0] != "002036.SZ" for call in loader.incremental_update.call_args_list
        )


# ===========================================================================
# 收盘刷新 helpers（Task 6）：不落库的抓取 / 归一化
# ===========================================================================

_REFRESH_TARGET = "2026-07-27"


class _RecordingRefreshLoader:
    """记录 get_daily_bars 调用参数并返回预设 DataFrame 的手写 fake。"""

    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.calls: list[tuple[str, str | None, str | None]] = []

    def get_daily_bars(self, symbol, start_date=None, end_date=None):
        self.calls.append((symbol, start_date, end_date))
        return self.df


def _refresh_bar_df(**overrides) -> pd.DataFrame:
    """单行目标日日线 DataFrame，字段可按测试覆盖。"""
    row = {
        "trade_date": _REFRESH_TARGET,
        "open": 10.0,
        "high": 11.0,
        "low": 9.5,
        "close": 10.5,
        "volume": 1000.0,
        "amount": 10500.0,
        "data_source": "akshare",
    }
    row.update(overrides)
    return pd.DataFrame([row])


class TestFetchBarsForRefresh:
    """fetch_bars_for_refresh 只抓目标日、绕过任何完成快捷路径。"""

    def test_requests_exactly_target_date_window(self):
        """抓取窗口 start=end=目标日（YYYYMMDD），不做 latest-date 跳过。"""
        loader = _RecordingRefreshLoader(_refresh_bar_df())
        df = fetch_bars_for_refresh(loader, "000001", _REFRESH_TARGET)

        assert loader.calls == [("000001", "20260727", "20260727")]
        assert len(df) == 1


class TestNormalizeBarRowForRefresh:
    """normalize_bar_row_for_refresh 校验并归一化目标日单行，不写库。"""

    def test_accepts_valid_target_row(self):
        row, reason = normalize_bar_row_for_refresh(
            _refresh_bar_df(), "000001", _REFRESH_TARGET
        )
        assert reason is None
        assert row["ts_code"] == "000001"
        assert row["trade_date"] == _REFRESH_TARGET
        assert row["close"] == 10.5

    def test_normalizes_compact_trade_date(self):
        """YYYYMMDD 形式的 trade_date 归一化为 YYYY-MM-DD。"""
        row, reason = normalize_bar_row_for_refresh(
            _refresh_bar_df(trade_date="20260727"), "000001", _REFRESH_TARGET
        )
        assert reason is None
        assert row["trade_date"] == _REFRESH_TARGET

    def test_rejects_missing_target_date(self):
        """返回数据不含目标日 → 拒绝（源端缺数）。"""
        row, reason = normalize_bar_row_for_refresh(
            _refresh_bar_df(trade_date="2026-07-24"), "000001", _REFRESH_TARGET
        )
        assert row is None
        assert reason

    def test_rejects_empty_dataframe(self):
        row, reason = normalize_bar_row_for_refresh(
            pd.DataFrame(), "000001", _REFRESH_TARGET
        )
        assert row is None
        assert reason

    def test_rejects_yfinance_source(self):
        """yfinance 来源的行绝不入库。"""
        row, reason = normalize_bar_row_for_refresh(
            _refresh_bar_df(data_source="yfinance"), "000001", _REFRESH_TARGET
        )
        assert row is None
        assert "yfinance" in reason

    def test_rejects_invalid_ohlc(self):
        """high < close 违反 OHLC 不变量 → 拒绝。"""
        row, reason = normalize_bar_row_for_refresh(
            _refresh_bar_df(high=10.2, close=10.5), "000001", _REFRESH_TARGET
        )
        assert row is None
        assert reason

    def test_rejects_negative_volume(self):
        row, reason = normalize_bar_row_for_refresh(
            _refresh_bar_df(volume=-1.0), "000001", _REFRESH_TARGET
        )
        assert row is None
        assert reason

    def test_rejects_missing_required_field(self):
        """缺少 amount 等必填字段 → 拒绝。"""
        row, reason = normalize_bar_row_for_refresh(
            _refresh_bar_df(amount=None), "000001", _REFRESH_TARGET
        )
        assert row is None
        assert reason

    def test_rejects_duplicate_target_rows(self):
        """目标日出现重复行 → 拒绝（自然键冲突）。"""
        df = pd.concat([_refresh_bar_df(), _refresh_bar_df()], ignore_index=True)
        row, reason = normalize_bar_row_for_refresh(df, "000001", _REFRESH_TARGET)
        assert row is None
        assert reason

    def test_populates_derived_fields_from_source_row(self):
        """源行带 turnover/pct_change/amplitude → 按 DB 列名带出同源衍生值。

        loader 的 DataFrame 列名为 turnover（换手率）/pct_change（涨跌幅）/
        amplitude（振幅）；归一化行用 DB 列名 turnover_rate/pct_change/amplitude。
        """
        row, reason = normalize_bar_row_for_refresh(
            _refresh_bar_df(turnover=2.5, pct_change=1.8, amplitude=3.2),
            "000001",
            _REFRESH_TARGET,
        )
        assert reason is None
        assert row["turnover_rate"] == 2.5
        assert row["pct_change"] == 1.8
        assert row["amplitude"] == 3.2

    def test_missing_derived_fields_become_none_without_rejection(self):
        """源行缺三个衍生列 → 置 None（写 NULL 可见陈旧），绝不因此拒绝。"""
        row, reason = normalize_bar_row_for_refresh(
            _refresh_bar_df(), "000001", _REFRESH_TARGET
        )
        assert reason is None
        assert row["turnover_rate"] is None
        assert row["pct_change"] is None
        assert row["amplitude"] is None

    def test_nan_derived_fields_become_none(self):
        """NaN 衍生值 → None，不把 NaN 写进库。"""
        row, reason = normalize_bar_row_for_refresh(
            _refresh_bar_df(
                turnover=float("nan"),
                pct_change=float("nan"),
                amplitude=float("nan"),
            ),
            "000001",
            _REFRESH_TARGET,
        )
        assert reason is None
        assert row["turnover_rate"] is None
        assert row["pct_change"] is None
        assert row["amplitude"] is None
