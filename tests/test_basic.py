"""Smoke tests for quant_pipeline."""
from __future__ import annotations

import os
import pathlib
from unittest.mock import MagicMock

import providers


def test_import_main_modules():
    import interface  # noqa: F401
    from scripts import validate_and_vacuum  # noqa: F401


def test_daemon_script_exists():
    daemon_path = pathlib.Path(__file__).parent.parent / "scripts" / "daemon.py"
    assert daemon_path.exists()


def test_test_environment_uses_temporary_quant_db_path():
    db_path = pathlib.Path(os.environ["QUANT_DB_PATH"])
    production_db = pathlib.Path.home() / "Code/quant_data/quant_core.db"

    assert db_path != production_db
    assert db_path.parent != production_db.parent


def test_external_database_manager_is_mocked_before_test_collection():
    assert isinstance(providers.DatabaseManager, MagicMock)
