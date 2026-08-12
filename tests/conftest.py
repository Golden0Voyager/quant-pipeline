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
