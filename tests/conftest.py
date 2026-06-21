"""Test configuration for quant_pipeline."""
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

mocks = {
    "smartmoney_hunter": MagicMock(),
    "smartmoney_hunter.database": MagicMock(),
    "smartmoney_hunter.data_loader": MagicMock(),
    "smartmoney_hunter.indicators": MagicMock(),
}
patcher = patch.dict("sys.modules", mocks)
patcher.start()
