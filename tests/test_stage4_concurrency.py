"""stage4 并发前置验证：多线程并发写库不得出现锁错误/线程亲和错误。

审计结论（2026-09-20）：stage4 十个任务写入表两两不相交；写路径分两类——
providers.py 自带 _write_lock 的共享连接写（check_same_thread=False），
以及委托 smartmoney_hunter DatabaseManager 的批量写（其 _connect_for_write
为每实例缓存单连接）。后者是并发回归的主要风险点，本文件将其固化为测试。
"""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from core.parallel_runner import ParallelTask, run_parallel_tasks


def _make_provider(tmp_path):
    """真临时 SQLite 库上的 SmartMoneyDBProvider（同 test_market_flow fixture）。"""
    from providers import SmartMoneyDBProvider

    db_path = tmp_path / "stage4_concurrency.db"
    provider = SmartMoneyDBProvider(db_path=str(db_path))
    provider._db.db_path = str(db_path)
    provider._ensure_wal_mode()
    provider._ensure_tables()
    provider._run_versioned_migrations()
    return provider


def _make_real_database_manager(tmp_path):
    """加载未被 conftest mock 的真实 smartmoney_hunter DatabaseManager。

    conftest 将 smartmoney_hunter.database 替换为 MagicMock，委托写路径
    （provider._db.save_*_batch）因此无法被压测。这里通过 distribution
    元数据定位真实源码文件（兼容 editable 与普通安装）并独立加载。
    """
    import importlib.util
    import json
    import sys
    import sysconfig
    from importlib.metadata import PackageNotFoundError, distribution
    from pathlib import Path

    try:
        dist = distribution("smartmoney-hunter")
    except PackageNotFoundError:
        # quant_pipeline CI 环境不安装 smartmoney_hunter（conftest 以 mock 替代），
        # 真实写连接的并发回归由 quant_hunter 仓库 TestThreadSafeWriteConn 覆盖
        pytest.skip("smartmoney-hunter not installed in this environment")
    db_py: Path | None = None
    direct = dist.read_text("direct_url.json")
    if direct:
        try:
            url = json.loads(direct)["url"]  # file:///... 指向项目根
            candidate = Path(url.removeprefix("file://")) / "src" / "smartmoney_hunter" / "database.py"
            if candidate.exists():
                db_py = candidate
        except (KeyError, ValueError):
            pass
    if db_py is None:
        for base in (sysconfig.get_paths()["purelib"], *sys.path):
            candidate = Path(base) / "smartmoney_hunter" / "database.py"
            if candidate.exists():
                db_py = candidate
                break
    assert db_py is not None, "未找到 smartmoney_hunter 真实 database.py"
    # conftest 将 smartmoney_hunter 换为 MagicMock（非 package），database.py 的
    # `from smartmoney_hunter import db_schema` / `from smartmoney_hunter.config import ...`
    # 及 __init__ 内的惰性导入需要真实子模块常驻 sys.modules。
    # conftest 只 mock 了 database/data_loader/indicators/market_utils 四个子模块，
    # config/db_schema 换成真实实现不影响其他测试。
    import types
    from unittest.mock import patch

    def _exec_submodule(name: str, path: Path):
        sub_spec = importlib.util.spec_from_file_location(name, path)
        # 显式断言而非静默 None：签名上 spec_from_file_location 可返回 None，
        # 直接往下走只会得到难以定位的 AttributeError
        assert sub_spec is not None, f"无法为 {name} 创建 ModuleSpec"
        assert sub_spec.loader is not None, f"{name} 的 ModuleSpec 缺少 loader"
        sub = importlib.util.module_from_spec(sub_spec)
        sys.modules[name] = sub
        sub_spec.loader.exec_module(sub)
        return sub

    shim = types.ModuleType("smartmoney_hunter")
    shim.__path__ = [str(db_py.parent)]
    with patch.dict(sys.modules, {"smartmoney_hunter": shim}):
        sys.modules.pop("smartmoney_hunter.config", None)
        sys.modules.pop("smartmoney_hunter.db_schema", None)
        db_schema_mod = _exec_submodule("smartmoney_hunter.db_schema", db_py.parent / "db_schema.py")
        config_mod = _exec_submodule("smartmoney_hunter.config", db_py.parent / "config.py")
        shim.db_schema = db_schema_mod
        shim.config = config_mod
        spec = importlib.util.spec_from_file_location("smartmoney_hunter.database_real", db_py)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        # 实例化必须在 patch 上下文内完成：__init__ 有惰性 config 导入；
        # 不带 skip_init，让 db_schema 建出 shareholder_count 等表
        return module.DatabaseManager(db_path=str(tmp_path / "stage4_concurrency.db"))


_ROWS_PER_THREAD = 20


def _north_hold_records(tag: str) -> list[dict]:
    return [
        {
            "ts_code": f"6000{tag}{i:02d}"[-6:],
            "security_name": f"测试{tag}{i}",
            "trade_date": "2026-09-18",
            "hold_shares": 1000.0 + i,
            "data_source": "concurrency-test",
        }
        for i in range(_ROWS_PER_THREAD)
    ]


def _shareholder_records(tag: str) -> list[dict]:
    return [
        {
            "ts_code": f"0000{tag}{i:02d}"[-6:],
            "report_date": "2026-06-30",
            "holder_count": 10000 + i,
            "holder_count_change_pct": -1.5,
            "data_source": "concurrency-test",
        }
        for i in range(_ROWS_PER_THREAD)
    ]


def _block_trade_records(tag: str) -> list[dict]:
    return [
        {
            "ts_code": f"3000{tag}{i:02d}"[-6:],
            "trade_date": "2026-09-18",
            "deal_price": 10.0,
            "close_price": 10.5,
            "discount_rate": -0.05,
            "volume": 1000.0,
            "amount": 10000.0,
            "buyer_branch": "并发买方",
            "seller_branch": "并发卖方",
            "data_source": "concurrency-test",
        }
        for i in range(_ROWS_PER_THREAD)
    ]


def _placement_records(tag: str) -> list[dict]:
    return [
        {
            "ts_code": f"0020{tag}{i:02d}"[-6:],
            "symbol": f"SYM{tag}{i}",
            "name": f"定增{tag}{i}",
            "issue_method": "定向增发",
            "issue_date": "2026-09-01",
            "data_source": "concurrency-test",
            "source_record_key": f"conc-{tag}-{i}",
        }
        for i in range(_ROWS_PER_THREAD)
    ]


class TestConcurrentBatchWrites:
    """stage4 典型写路径并发压测：不同线程写不同表，必须全部成功。"""

    def test_mixed_write_paths_from_worker_threads(self, tmp_path, monkeypatch):
        monkeypatch.setenv("PARALLEL_WORKERS", "3")
        provider = _make_provider(tmp_path)
        # 委托写路径：真实 DatabaseManager（其写连接缓存必须线程隔离，
        # 否则主线程预热后 worker 线程复用会触发 sqlite3 线程亲和检查）
        real_db = _make_real_database_manager(tmp_path)
        # 主线程先写一次预热缓存连接，模拟管道主线程先跑任务的真实时序，
        # 使线程亲和回归确定化（无预热时各线程竞争建连可能偶然通过）
        real_db.save_shareholder_count_batch([
            {"ts_code": "MAIN0", "report_date": "2026-06-30", "holder_count": 1}
        ])
        errors: list[BaseException] = []

        def write_north_hold() -> int:
            return real_db.save_north_hold_batch(_north_hold_records("A"))

        def write_shareholder() -> int:
            return real_db.save_shareholder_count_batch(_shareholder_records("B"))

        def write_block_trade() -> int:
            return provider.save_block_trade_batch(_block_trade_records("C"))

        def write_placement() -> int:
            return provider.save_placement_batch(_placement_records("D"))

        workers = [
            write_north_hold,
            write_shareholder,
            write_block_trade,
            write_placement,
        ]
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(w) for w in workers]
            for f in futures:
                try:
                    assert f.result() == _ROWS_PER_THREAD
                except BaseException as exc:  # noqa: BLE001 — 收集全部并发错误
                    errors.append(exc)

        assert not errors, f"并发写入出现异常: {errors!r}"

        conn = real_db._connect()
        try:
            n_north = conn.execute("SELECT COUNT(*) FROM north_hold").fetchone()[0]
            n_holder = conn.execute("SELECT COUNT(*) FROM shareholder_count").fetchone()[0]
            n_block = conn.execute("SELECT COUNT(*) FROM block_trade").fetchone()[0]
            n_plc = conn.execute("SELECT COUNT(*) FROM placement_announcements").fetchone()[0]
        finally:
            conn.close()
        assert n_north == _ROWS_PER_THREAD
        assert n_holder == 1 + _ROWS_PER_THREAD  # 1 行主线程预热 + B 线程 20 行
        assert n_block == _ROWS_PER_THREAD
        assert n_plc == _ROWS_PER_THREAD


class TestRunParallelTasksRealConcurrency:
    """run_parallel_tasks 在 max_workers>1 时必须真实重叠执行。"""

    def test_tasks_overlap_in_time(self):
        intervals: list[tuple[float, float]] = []
        lock = threading.Lock()

        def task(_i: int) -> dict:
            start = time.monotonic()
            time.sleep(0.3)
            end = time.monotonic()
            with lock:
                intervals.append((start, end))
            return {"status": "ok"}

        ptasks = [
            ParallelTask(name=f"t{i}", fn=task, args=(i,), kwargs={})
            for i in range(4)
        ]
        elapsed_start = time.monotonic()
        results = run_parallel_tasks(ptasks, max_workers=3, runner_fn=lambda _name, fn, *a, **k: fn(*a, **k))
        elapsed = time.monotonic() - elapsed_start

        assert all("error" not in r for r in results.values())
        # 4 个 0.3s 任务用 3 线程：重叠执行应明显快于 4×0.3s 的完全串行
        assert elapsed < 4 * 0.3 * 0.75, f"几乎无重叠（elapsed={elapsed:.2f}s），并发未生效"
        assert len(intervals) == 4

    def test_single_worker_falls_back_serial(self):
        order: list[int] = []

        def task(i: int) -> dict:
            order.append(i)
            return {"status": "ok"}

        ptasks = [
            ParallelTask(name=f"t{i}", fn=task, args=(i,), kwargs={})
            for i in range(3)
        ]
        results = run_parallel_tasks(ptasks, max_workers=1, runner_fn=lambda _name, fn, *a, **k: fn(*a, **k))
        assert all("error" not in r for r in results.values())
        assert order == [0, 1, 2]
