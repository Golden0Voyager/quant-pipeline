"""Smoke tests for quant_pipeline."""
from __future__ import annotations

import pathlib


def test_import_main_modules():
    import interface  # noqa: F401
    from scripts import validate_and_vacuum  # noqa: F401


def test_daemon_script_exists():
    daemon_path = pathlib.Path(__file__).parent.parent / "scripts" / "daemon.py"
    assert daemon_path.exists()
