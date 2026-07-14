"""Test configuration for quant_pipeline."""
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

_market_utils_mock = MagicMock()
_market_utils_mock.is_beijing_stock = lambda _symbol: False

# Create a real temp DB path so mocked DatabaseManager().db_path is a string,
# preventing _ensure_wal_mode / _ensure_chip_tables from writing
# MagicMock-named SQLite files into the project root.
_tmp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_tmp_db.close()
_db_path = _tmp_db.name


def _make_database_manager(*args, **kwargs):
    mock = MagicMock()
    mock.db_path = _db_path
    return mock


_mock_database_module = MagicMock()
_mock_database_module.DatabaseManager.side_effect = _make_database_manager

_mock_data_loader_module = MagicMock()
_mock_indicators_module = MagicMock()

mocks = {
    "smartmoney_hunter": MagicMock(),
    "smartmoney_hunter.database": _mock_database_module,
    "smartmoney_hunter.data_loader": _mock_data_loader_module,
    "smartmoney_hunter.indicators": _mock_indicators_module,
    "smartmoney_hunter.market_utils": _market_utils_mock,
}
patcher = patch.dict("sys.modules", mocks)
patcher.start()
