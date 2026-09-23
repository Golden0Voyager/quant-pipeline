"""Test configuration for quant_pipeline."""
from __future__ import annotations

import atexit
import os
import shutil
import sys
import tempfile
from dataclasses import replace
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_TEST_ROOT = Path(tempfile.mkdtemp(prefix="quant_pipeline_tests_"))
_TEST_DB_PATH = _TEST_ROOT / "quant_core.db"
os.environ["QUANT_DB_PATH"] = str(_TEST_DB_PATH)
atexit.register(shutil.rmtree, _TEST_ROOT, ignore_errors=True)

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

_market_utils_mock = MagicMock()
_market_utils_mock.is_beijing_stock = lambda _symbol: False


def _make_database_manager(*_args, **_kwargs):
    mock = MagicMock()
    mock.db_path = str(_TEST_DB_PATH)
    mock.get_distinct_codes.return_value = set()
    mock.count_fundamentals_for_date.return_value = 0
    mock.get_last_task_run.return_value = None
    return mock


_mock_database_module = MagicMock()
_mock_database_module.DatabaseManager.side_effect = _make_database_manager

_mocks = {
    "smartmoney_hunter": MagicMock(),
    "smartmoney_hunter.database": _mock_database_module,
    "smartmoney_hunter.data_loader": MagicMock(),
    "smartmoney_hunter.indicators": MagicMock(),
    "smartmoney_hunter.market_utils": _market_utils_mock,
}
_patcher = patch.dict("sys.modules", _mocks)
_patcher.start()
atexit.register(_patcher.stop)


@pytest.fixture
def pinned_trading_calendar(monkeypatch):
    """将交易日历钉在固定集合上，供 expected 相关用例使用。

    ``get_expected_latest_trading_day`` 优先按交易日历推导，而宿主机是否
    存在缓存（以及缓存是否覆盖目标日）会让同一个用例在本地与 CI 上走出
    不同分支。钉住日历后，被测路径固定为「日历 → expected」。
    日历未覆盖 up_to 时返回 None，以便退化路径可单独测试。
    """
    trade_dates = [
        "2026-06-17",
        "2026-06-18",  # 06-19~06-21 端午休市
        "2026-06-22",
        "2026-07-16",
        "2026-07-17",  # 07-18/07-19 周末
        "2026-07-20",
        "2026-07-21",
        "2026-07-30",
        "2026-07-31",
    ]

    def _covering(up_to: str) -> list[str] | None:
        return trade_dates if max(trade_dates) >= up_to else None

    import core.calendar as calendar

    monkeypatch.setattr(calendar, "_load_calendar_covering", _covering)
    return trade_dates


@pytest.fixture(autouse=True)
def _offline_gate_bypass(monkeypatch):
    """默认让全量管道离线闸门放行，避免 main() 测试发起真实网络探测。

    专测“离线跳过”的用例需在自身测试体内将 daily_pipeline.is_online 覆盖为 False。
    """
    monkeypatch.delenv("QUANT_ALLOW_OFFLINE", raising=False)
    import daily_pipeline

    monkeypatch.setattr(daily_pipeline, "is_online", lambda *a, **k: True)
    yield


@pytest.fixture(autouse=True)
def _fast_default_source_client(monkeypatch):
    """Keep mock-only tests isolated from production source throttling."""
    import core.source_client as source_client

    policies = {
        name: replace(
            policy,
            min_interval_seconds=0,
            base_delay_seconds=0,
            max_delay_seconds=0,
        )
        for name, policy in source_client.POLICIES.items()
    }
    monkeypatch.setattr(source_client, "POLICIES", policies)
    source_client.reset_default_client()
    yield
    source_client.reset_default_client()


@pytest.fixture(autouse=True)
def _skip_watchlist_sync_on_tui_test_mount(monkeypatch, request):
    """Keep Textual mount tests from touching the operator's watchlist database."""
    if request.node.get_closest_marker("allow_startup_watchlist_sync"):
        yield
        return

    from tui.app import PipelineApp
    from tui.widgets.completeness import DataCompletenessWidget

    monkeypatch.setattr(PipelineApp, "_start_watchlist_sync", lambda self: None)
    monkeypatch.setattr(DataCompletenessWidget, "_start_exact_refresh", lambda self: None)
    yield
