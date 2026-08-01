"""Tests for the single task registry."""
from __future__ import annotations

import pytest

from core.task_registry import (
    TASK_REGISTRY,
    Cadence,
    DateStrategy,
    EmptyPolicy,
    RefreshKind,
    lookup_task,
    refreshable_trading_tasks,
    table_date_columns,
    task_names,
)


class TestRegistryInvariants:
    """Structural invariants for TASK_REGISTRY."""

    def test_every_task_has_unique_name(self):
        names = [s.name for s in TASK_REGISTRY]
        assert len(names) == len(set(names)), "duplicate task names found"

    def test_every_task_has_tables_or_utility(self):
        for spec in TASK_REGISTRY:
            # Utility tasks (retry, health_check) can have empty tables
            if spec.name in ("retry", "health_check"):
                assert spec.tables == ()
            else:
                assert len(spec.tables) >= 1, f"{spec.name} has no tables"

    def test_every_task_declares_cadence(self):
        for spec in TASK_REGISTRY:
            assert isinstance(spec.cadence, Cadence), f"{spec.name} missing cadence"

    def test_every_task_declares_empty_policy(self):
        for spec in TASK_REGISTRY:
            assert isinstance(spec.empty_policy, EmptyPolicy), f"{spec.name} missing empty_policy"

    def test_every_task_has_primary_source(self):
        for spec in TASK_REGISTRY:
            assert spec.primary_source, f"{spec.name} missing primary_source"

    def test_every_task_has_display_label(self):
        for spec in TASK_REGISTRY:
            assert spec.display_label, f"{spec.name} missing display_label"

    def test_health_check_registered(self):
        spec = lookup_task("health_check")
        assert spec is not None
        assert spec.cadence is Cadence.ON_DEMAND

    def test_retry_registered(self):
        spec = lookup_task("retry")
        assert spec is not None


class TestLookupTask:
    """lookup_task returns correct specs."""

    def test_lookup_exists(self):
        spec = lookup_task("update_bars")
        assert spec is not None
        assert "daily_bars" in spec.tables

    def test_lookup_missing(self):
        assert lookup_task("nonexistent") is None

    def test_lookup_empty_string(self):
        assert lookup_task("") is None


class TestTaskNames:
    """task_names derived view."""

    def test_returns_sorted(self):
        names = task_names()
        assert names == sorted(names)
        assert len(names) == len(TASK_REGISTRY)

    def test_includes_all(self):
        names = set(task_names())
        for spec in TASK_REGISTRY:
            assert spec.name in names


class TestTableDateColumns:
    """table_date_columns derived view."""

    def test_contains_known_tables(self):
        columns = table_date_columns()
        assert "daily_bars" in columns
        assert columns["daily_bars"] == "trade_date"
        assert "indicators" in columns
        assert columns["indicators"] == "trade_date"

    def test_all_date_columns_are_strings(self):
        for table, col in table_date_columns().items():
            assert isinstance(table, str)
            assert isinstance(col, str)

    def test_utility_tasks_not_in_date_columns(self):
        columns = table_date_columns()
        assert "retry" not in columns
        assert "health_check" not in columns


class TestCadenceEnum:
    """Cadence completeness."""

    def test_all_cadences_used(self):
        used = {spec.cadence for spec in TASK_REGISTRY}
        assert Cadence.TRADING_DAY in used
        assert Cadence.DAILY in used
        assert Cadence.MONTHLY in used
        assert Cadence.QUARTERLY in used
        assert Cadence.ON_DEMAND in used

    def test_trading_day_is_most_common(self):
        trading = [s for s in TASK_REGISTRY if s.cadence is Cadence.TRADING_DAY]
        assert len(trading) > len(TASK_REGISTRY) // 2


class TestEmptyPolicy:
    """Empty-policy coverage."""

    def test_all_policies_used(self):
        used = {spec.empty_policy for spec in TASK_REGISTRY}
        assert EmptyPolicy.ALLOW in used
        assert EmptyPolicy.FAIL in used

    def test_bars_allow_on_non_trading_day(self):
        spec = lookup_task("update_bars")
        assert spec is not None
        assert spec.empty_policy is EmptyPolicy.ALLOW_ON_NON_TRADING_DAY


class TestTaskSpecDataclass:
    """TaskSpec construction and frozenness."""

    def test_frozen(self):
        spec = lookup_task("update_bars")
        assert spec is not None
        with pytest.raises(AttributeError):
            spec.name = "changed"  # type: ignore[misc]

    def test_str_representation(self):
        spec = lookup_task("update_fundamentals")
        assert spec is not None
        assert "update_fundamentals" in str(spec)


class TestRefreshPolicies:
    """Close-refresh metadata is complete and internally consistent."""

    def test_all_trading_day_tasks_have_refresh_policy(self):
        specs = tuple(s for s in TASK_REGISTRY if s.cadence is Cadence.TRADING_DAY)
        # 28：update_north_flow 已下线（北向日度流向 2024-08 起停止披露）
        assert len(specs) == 28
        assert all(s.refresh_policy is not None for s in specs)
        assert {s.name for s in refreshable_trading_tasks()} == {
            s.name for s in specs
        }

    def test_non_trading_day_tasks_are_not_close_refreshable(self):
        assert all(
            s.refresh_policy is None
            for s in TASK_REGISTRY
            if s.cadence is not Cadence.TRADING_DAY
        )

    def test_every_policy_declares_nonempty_contracts_for_all_written_tables(self):
        for spec in refreshable_trading_tasks():
            assert spec.refresh_policy is not None
            assert set(spec.refresh_policy.natural_keys) == set(spec.tables)
            assert set(spec.refresh_policy.required_fields) == set(spec.tables)
            assert all(spec.refresh_policy.natural_keys.values())
            assert all(spec.refresh_policy.required_fields.values())

    def test_every_policy_dependency_names_a_registered_task(self):
        registered_names = {spec.name for spec in TASK_REGISTRY}
        for spec in refreshable_trading_tasks():
            assert spec.refresh_policy is not None
            assert set(spec.refresh_policy.dependencies) <= registered_names

    def test_market_snapshot_depends_on_fundamentals(self):
        spec = lookup_task("update_market_snapshot")
        assert spec is not None
        assert spec.refresh_policy is not None
        assert spec.refresh_policy.dependencies == ("update_fundamentals",)

    def test_market_snapshot_accepts_normal_xueqiu_coverage(self):
        spec = lookup_task("update_market_snapshot")
        assert spec is not None
        assert spec.refresh_policy is not None
        assert spec.refresh_policy.required_fields == {
            "fundamentals": ("ts_code", "trade_date"),
        }
        assert spec.refresh_policy.minimum_coverage == 0.4

    def test_multi_event_tasks_use_stable_source_record_keys(self):
        expected = {
            "update_dragon_tiger": ("trade_date", "ts_code"),
            "update_block_trade": ("trade_date", "ts_code"),
            "update_stock_pledge": ("trade_date", "stock_code"),
        }
        for task_name, business_fields in expected.items():
            spec = lookup_task(task_name)
            assert spec is not None
            assert spec.refresh_policy is not None
            table = spec.tables[0]
            assert spec.refresh_policy.natural_keys[table] == ("source_record_key",)
            assert spec.refresh_policy.required_fields[table] == (
                "source_record_key",
                *business_fields,
            )

    def test_derived_tasks_depend_on_fresh_upstreams(self):
        historical_valuation = lookup_task("update_historical_valuation")
        sector_industry = lookup_task("update_sector_industry")
        assert historical_valuation is not None
        assert sector_industry is not None
        assert historical_valuation.refresh_policy is not None
        assert sector_industry.refresh_policy is not None
        assert historical_valuation.refresh_policy.dependencies == (
            "update_fundamentals",
            "update_market_snapshot",
        )
        assert sector_industry.refresh_policy.dependencies == ("update_fundamentals",)

    def test_high_risk_policy_values_remain_explicit(self):
        expected = {
            "update_bars": {
                "minimum_coverage": 0.8,
                "cache_namespace": "daily_bars",
            },
            "update_margin_trading": {"lookback_days": 3},
            "update_south_flow": {"lookback_days": 3},
            "update_index_daily": {"lookback_days": 3},
            "update_cb_index": {"lookback_days": 3},
            "update_market_valuation": {"lookback_days": 3},
            "update_institution_survey": {"lookback_days": 30},
            "update_stock_pledge": {"lookback_days": 30},
        }

        for task_name, values in expected.items():
            spec = lookup_task(task_name)
            assert spec is not None
            assert spec.refresh_policy is not None
            for field, value in values.items():
                assert getattr(spec.refresh_policy, field) == value

    def test_sector_derivatives_declares_all_written_tables(self):
        spec = lookup_task("update_sector_derivatives")
        assert spec is not None
        assert spec.tables == (
            "sector_daily",
            "sector_valuation",
            "index_futures_basis",
        )

    def test_policies_match_the_approved_strategy_matrix(self):
        expected = {
            "update_bars": (RefreshKind.REMOTE_KEYED_UPSERT, DateStrategy.EXACT_TARGET),
            "update_indicators": (RefreshKind.DERIVED_RECOMPUTE, DateStrategy.EXACT_TARGET),
            "update_fundamentals": (RefreshKind.REMOTE_DATE_SNAPSHOT, DateStrategy.EXACT_TARGET),
            "update_market_snapshot": (RefreshKind.REMOTE_DATE_SNAPSHOT, DateStrategy.EXACT_TARGET),
            "update_fund_flow": (RefreshKind.REMOTE_DATE_SNAPSHOT, DateStrategy.EXACT_TARGET),
            "update_margin_trading": (RefreshKind.REMOTE_DATE_SNAPSHOT, DateStrategy.LATEST_AVAILABLE_WITHIN_LOOKBACK),
            "update_dragon_tiger": (RefreshKind.REMOTE_DATE_SNAPSHOT, DateStrategy.EXACT_TARGET),
            "update_block_trade": (RefreshKind.REMOTE_DATE_SNAPSHOT, DateStrategy.EXACT_TARGET),
            "update_sector_fund_flow": (RefreshKind.REMOTE_RUN_SNAPSHOT, DateStrategy.RUN_SNAPSHOT),
            "update_historical_valuation": (RefreshKind.DERIVED_RECOMPUTE, DateStrategy.EXACT_TARGET),
            "update_sector_industry": (RefreshKind.DERIVED_RECOMPUTE, DateStrategy.EXACT_TARGET),
            "update_south_flow": (RefreshKind.REMOTE_DATE_SNAPSHOT, DateStrategy.LATEST_AVAILABLE_WITHIN_LOOKBACK),
            "update_ah_premium": (RefreshKind.REMOTE_RUN_SNAPSHOT, DateStrategy.RUN_SNAPSHOT),
            "update_etf_daily": (RefreshKind.REMOTE_DATE_SNAPSHOT, DateStrategy.EXACT_TARGET),
            "update_index_daily": (RefreshKind.REMOTE_DATE_SNAPSHOT, DateStrategy.LATEST_AVAILABLE_WITHIN_LOOKBACK),
            "update_cb_quotation": (RefreshKind.REMOTE_RUN_SNAPSHOT, DateStrategy.RUN_SNAPSHOT),
            "update_cb_redeem": (RefreshKind.REMOTE_RUN_SNAPSHOT, DateStrategy.RUN_SNAPSHOT),
            "update_cb_index": (RefreshKind.REMOTE_DATE_SNAPSHOT, DateStrategy.LATEST_AVAILABLE_WITHIN_LOOKBACK),
            "update_limit_up_down": (RefreshKind.REMOTE_DATE_SNAPSHOT, DateStrategy.EXACT_TARGET),
            "update_concept_board": (RefreshKind.REMOTE_RUN_SNAPSHOT, DateStrategy.RUN_SNAPSHOT),
            "update_market_valuation": (RefreshKind.REMOTE_DATE_SNAPSHOT, DateStrategy.LATEST_AVAILABLE_WITHIN_LOOKBACK),
            "update_sector_derivatives": (RefreshKind.COMPOSITE_ATOMIC, DateStrategy.EXACT_TARGET),
            "update_option_sentiment": (RefreshKind.REMOTE_DATE_SNAPSHOT, DateStrategy.EXACT_TARGET),
            "update_stock_repurchase": (RefreshKind.REMOTE_KEYED_UPSERT, DateStrategy.RUN_SNAPSHOT),
            "update_institution_survey": (RefreshKind.REMOTE_KEYED_UPSERT, DateStrategy.LATEST_AVAILABLE_WITHIN_LOOKBACK),
            "update_stock_pledge": (RefreshKind.REMOTE_KEYED_UPSERT, DateStrategy.LATEST_AVAILABLE_WITHIN_LOOKBACK),
            "update_chip_distribution": (RefreshKind.DERIVED_RECOMPUTE, DateStrategy.EXACT_TARGET),
            "update_chip_distribution_em": (RefreshKind.DERIVED_RECOMPUTE, DateStrategy.EXACT_TARGET),
        }

        actual = {
            spec.name: (
                spec.refresh_policy.kind,
                spec.refresh_policy.date_strategy,
            )
            for spec in refreshable_trading_tasks()
            if spec.refresh_policy is not None
        }

        assert actual == expected

    def test_only_supported_tasks_accept_symbol_filters(self):
        assert {
            spec.name
            for spec in refreshable_trading_tasks()
            if spec.refresh_policy is not None
            and spec.refresh_policy.supports_symbols
        } == {
            "update_bars",
            "update_indicators",
            "update_fundamentals",
            "update_fund_flow",
            "update_margin_trading",
            "update_dragon_tiger",
            "update_block_trade",
            "update_historical_valuation",
            "update_chip_distribution",
            "update_chip_distribution_em",
        }
