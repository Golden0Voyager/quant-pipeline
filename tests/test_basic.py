"""Smoke tests for quant_pipeline."""
from __future__ import annotations

import pathlib


def test_import_main_modules():
    import interface  # noqa: F401
    import validate_and_vacuum  # noqa: F401


def test_manager_script_exists():
    manager_path = pathlib.Path(__file__).parent.parent / "manager.sh"
    assert manager_path.exists()
