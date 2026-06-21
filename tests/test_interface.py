"""Tests for interface.py - ProviderFactory and Protocol interfaces."""
from __future__ import annotations

import pytest

from interface import DatabaseInterface, DataLoaderInterface, IndicatorEngineInterface, ProviderFactory


def test_provider_factory_not_configured():
    ProviderFactory._db_provider = None
    ProviderFactory._loader_provider = None
    ProviderFactory._indicator_provider = None

    with pytest.raises(RuntimeError, match="Provider not configured"):
        ProviderFactory.get_db()
    with pytest.raises(RuntimeError, match="Provider not configured"):
        ProviderFactory.get_loader()
    with pytest.raises(RuntimeError, match="Provider not configured"):
        ProviderFactory.get_indicator_engine()


def test_provider_factory_unknown_provider():
    with pytest.raises(ValueError, match="Unknown provider"):
        ProviderFactory.configure(provider="nonexistent")


def test_provider_factory_configure_smartmoney():
    ProviderFactory._db_provider = None
    ProviderFactory._loader_provider = None
    ProviderFactory._indicator_provider = None

    ProviderFactory.configure(provider="smartmoney")

    db = ProviderFactory.get_db()
    assert db is not None

    loader = ProviderFactory.get_loader()
    assert loader is not None

    engine = ProviderFactory.get_indicator_engine()
    assert engine is not None


def test_protocol_imports():
    assert DatabaseInterface is not None
    assert DataLoaderInterface is not None
    assert IndicatorEngineInterface is not None
