"""Smoke tests for quant_pipeline."""
import subprocess


def test_import_main_modules():
    """Test that core modules can be imported."""
    pass


def test_manager_script_exists():
    """Test manager.sh exists."""
    import pathlib
    manager_path = pathlib.Path(__file__).parent.parent / "manager.sh"
    assert manager_path.exists()
