"""Data-quality tests for close-refresh task audit."""

from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime

import pytest

from core.refresh import RefreshAdapterResult, RefreshContext
from core.refresh_audit import (
    CROSS_SOURCE_BOARDS,
    CrossSourceTolerance,
    RefreshAudit,
    RefreshAuditError,
    classify_board,
    compare_cross_source_quotes,
    cross_source_mismatches,
    stratified_cross_source_sample,
)
from core.task_registry import (
    Cadence,
    DateStrategy,
    EmptyPolicy,
    RefreshKind,
    RefreshPolicy,
    TaskSpec,
)


def _spec(
    *,
    strategy: DateStrategy = DateStrategy.EXACT_TARGET,
    lookback_days: int = 0,
    minimum_coverage: float | None = None,
    required_fields: tuple[str, ...] = ("code", "trade_date"),
    natural_keys: tuple[str, ...] = ("code", "trade_date"),
) -> TaskSpec:
    return TaskSpec(
        name="audit_task",
        callable=None,
        tables=("quotes",),
        cadence=Cadence.TRADING_DAY,
        date_columns={"quotes": "trade_date"},
        empty_policy=EmptyPolicy.ALLOW,
        primary_source="test",
        refresh_policy=RefreshPolicy(
            RefreshKind.REMOTE_DATE_SNAPSHOT,
            strategy,
            {"quotes": natural_keys},
            {"quotes": required_fields},
            minimum_coverage=minimum_coverage,
            lookback_days=lookback_days,
        ),
    )


def _context() -> RefreshContext:
    return RefreshContext(
        target_date="2026-07-27",
        started_at=datetime(2026, 7, 27, 8, tzinfo=UTC),
        run_id="refresh-1",
    )


def _result(
    *,
    as_of_date: str | None = "2026-07-27",
    fetched: int = 1,
    validated: int = 1,
    replaced: int = 1,
    retained: int = 0,
    metadata: dict[str, object] | None = None,
) -> RefreshAdapterResult:
    return RefreshAdapterResult(
        task_name="audit_task",
        as_of_date=as_of_date,
        fetched=fetched,
        validated=validated,
        replaced=replaced,
        retained=retained,
        failed_symbols=(),
        changed_symbols=(),
        metadata=metadata or {},
    )


def test_exact_target_rejects_stale_as_of_date() -> None:
    with pytest.raises(RefreshAuditError, match="target date"):
        RefreshAudit().validate_task(
            _spec(),
            _context(),
            _result(as_of_date="2026-07-24"),
        )


def test_lookback_accepts_latest_available_date_inside_window() -> None:
    report = RefreshAudit().validate_task(
        _spec(
            strategy=DateStrategy.LATEST_AVAILABLE_WITHIN_LOOKBACK,
            lookback_days=3,
        ),
        _context(),
        _result(as_of_date="2026-07-24"),
    )

    assert report.validated == 1
    assert report.degraded is False


@pytest.mark.parametrize("as_of_date", ["2026-07-23", "2026-07-28"])
def test_lookback_rejects_too_old_or_future_date(as_of_date: str) -> None:
    with pytest.raises(RefreshAuditError, match="lookback"):
        RefreshAudit().validate_task(
            _spec(
                strategy=DateStrategy.LATEST_AVAILABLE_WITHIN_LOOKBACK,
                lookback_days=3,
            ),
            _context(),
            _result(as_of_date=as_of_date),
        )


def test_run_snapshot_requires_current_run_identity() -> None:
    spec = _spec(strategy=DateStrategy.RUN_SNAPSHOT)
    with pytest.raises(RefreshAuditError, match="run_id"):
        RefreshAudit().validate_task(
            spec,
            _context(),
            _result(as_of_date=None, metadata={"run_id": "old-run"}),
        )

    report = RefreshAudit().validate_task(
        spec,
        _context(),
        _result(as_of_date=None, metadata={"run_id": "refresh-1"}),
    )
    assert report.validated == 1


def test_duplicate_natural_keys_are_rejected() -> None:
    rows = {
        "quotes": (
            {"code": "000001.SZ", "trade_date": "2026-07-27"},
            {"code": "000001.SZ", "trade_date": "2026-07-27"},
        )
    }
    with pytest.raises(RefreshAuditError, match="duplicate"):
        RefreshAudit().validate_task(
            _spec(),
            _context(),
            _result(fetched=2, validated=2, replaced=2),
            rows_by_table=rows,
        )


@pytest.mark.parametrize("missing_value", [None, "", "   "])
def test_required_fields_reject_null_or_blank(missing_value: object) -> None:
    rows = {
        "quotes": (
            {"code": missing_value, "trade_date": "2026-07-27"},
        )
    }
    with pytest.raises(RefreshAuditError, match="required field"):
        RefreshAudit().validate_task(
            _spec(),
            _context(),
            _result(),
            rows_by_table=rows,
        )


@pytest.mark.parametrize(
    "row",
    [
        {
            "code": "000001.SZ",
            "trade_date": "2026-07-27",
            "open": 10,
            "high": 9,
            "low": 8,
            "close": 9.5,
            "volume": 1,
            "amount": 1,
        },
        {
            "code": "000001.SZ",
            "trade_date": "2026-07-27",
            "open": 10,
            "high": 11,
            "low": 10.5,
            "close": 10,
            "volume": 1,
            "amount": 1,
        },
        {
            "code": "000001.SZ",
            "trade_date": "2026-07-27",
            "open": 10,
            "high": 9,
            "low": 11,
            "close": 10,
            "volume": 1,
            "amount": 1,
        },
    ],
    ids=["high-below-open", "low-above-close", "high-below-low"],
)
def test_ohlc_invariants_are_enforced(row: dict[str, object]) -> None:
    with pytest.raises(RefreshAuditError, match="OHLC"):
        RefreshAudit().validate_task(
            _spec(),
            _context(),
            _result(),
            rows_by_table={"quotes": (row,)},
        )


@pytest.mark.parametrize("field", ["volume", "amount"])
def test_volume_and_amount_must_be_nonnegative(field: str) -> None:
    row = {
        "code": "000001.SZ",
        "trade_date": "2026-07-27",
        "open": 10,
        "high": 11,
        "low": 9,
        "close": 10.5,
        "volume": 1,
        "amount": 1,
    }
    row[field] = -1

    with pytest.raises(RefreshAuditError, match=field):
        RefreshAudit().validate_task(
            _spec(),
            _context(),
            _result(),
            rows_by_table={"quotes": (row,)},
        )


def test_coverage_threshold_uses_declared_baseline() -> None:
    with pytest.raises(RefreshAuditError, match="coverage"):
        RefreshAudit().validate_task(
            _spec(minimum_coverage=0.8),
            _context(),
            _result(fetched=7, validated=7, replaced=7),
            rows_by_table={
                "quotes": tuple(
                    {"code": str(index), "trade_date": "2026-07-27"}
                    for index in range(7)
                )
            },
            baseline_counts={"quotes": 10},
        )


def test_dead_source_is_degraded_metadata_and_cannot_claim_replacement() -> None:
    audit = RefreshAudit()
    report = audit.validate_task(
        _spec(),
        _context(),
        _result(
            as_of_date=None,
            fetched=0,
            validated=0,
            replaced=0,
            retained=10,
            metadata={
                "source_status": "dead_source",
                "reason": "provider endpoint retired",
            },
        ),
    )
    assert report.degraded is True
    assert report.dead_source is True

    with pytest.raises(RefreshAuditError, match="dead source"):
        audit.validate_task(
            _spec(),
            _context(),
            _result(
                replaced=1,
                retained=10,
                metadata={
                    "source_status": "dead_source",
                    "reason": "provider endpoint retired",
                },
            ),
        )


def test_dead_source_accepts_attested_empty_baseline() -> None:
    report = RefreshAudit().validate_task(
        _spec(),
        _context(),
        _result(
            as_of_date=None,
            fetched=0,
            validated=0,
            replaced=0,
            retained=0,
            metadata={
                "source_status": "dead_source",
                "reason": "provider endpoint retired",
                "baseline_empty": True,
            },
        ),
    )

    assert report.degraded is True
    assert report.dead_source is True


@pytest.mark.parametrize("attestation", [{}, {"baseline_empty": 1}], ids=["absent", "truthy-non-bool"])
def test_dead_source_rejects_zero_retained_without_attestation(
    attestation: dict[str, object],
) -> None:
    with pytest.raises(RefreshAuditError, match="dead source"):
        RefreshAudit().validate_task(
            _spec(),
            _context(),
            _result(
                as_of_date=None,
                fetched=0,
                validated=0,
                replaced=0,
                retained=0,
                metadata={
                    "source_status": "dead_source",
                    "reason": "provider endpoint retired",
                    **attestation,
                },
            ),
        )


_ALL_BOARDS = frozenset(CROSS_SOURCE_BOARDS)


def _board_universe(per_board: int = 12) -> tuple[str, ...]:
    symbols: list[str] = []
    for index in range(per_board):
        symbols.append(f"60{index:04d}.SH")
        symbols.append(f"00{index:04d}.SZ")
        symbols.append(f"30{index:04d}.SZ")
        symbols.append(f"68{index:04d}.SH")
        symbols.append(f"83{index:04d}.BJ")
    return tuple(symbols)


@pytest.mark.parametrize(
    ("symbol", "board"),
    [
        ("601398.SH", "shanghai"),
        ("900901.SH", "shanghai"),
        ("000001.SZ", "shenzhen"),
        ("200011.SZ", "shenzhen"),
        ("300750.SZ", "chinext"),
        ("688001.SH", "star"),
        ("430047.BJ", "beijing"),
        ("830799.BJ", "beijing"),
        ("920001.BJ", "beijing"),
        ("T00001", None),
    ],
)
def test_classify_board_follows_market_prefix_rules(
    symbol: str,
    board: str | None,
) -> None:
    assert classify_board(symbol) == board


def test_stratified_sample_is_deterministic_per_target_date() -> None:
    universe = _board_universe()

    first = stratified_cross_source_sample(
        universe,
        target_date="2026-07-27",
        supported_boards=_ALL_BOARDS,
    )
    second = stratified_cross_source_sample(
        universe,
        target_date="2026-07-27",
        supported_boards=_ALL_BOARDS,
    )
    other_day = stratified_cross_source_sample(
        universe,
        target_date="2026-07-28",
        supported_boards=_ALL_BOARDS,
    )

    assert first.symbols == second.symbols
    assert first.symbols != other_day.symbols


def test_stratified_sample_spreads_about_thirty_across_five_boards() -> None:
    universe = _board_universe()

    sample = stratified_cross_source_sample(
        universe,
        target_date="2026-07-27",
        supported_boards=_ALL_BOARDS,
    )

    assert len(sample.symbols) == 30
    assert sample.excluded_boards == ()
    assert set(sample.symbols) <= set(universe)
    counts = Counter(classify_board(symbol) for symbol in sample.symbols)
    assert counts == {
        "shanghai": 6,
        "shenzhen": 6,
        "chinext": 6,
        "star": 6,
        "beijing": 6,
    }


def test_stratified_sample_excludes_unsupported_backup_markets_explicitly() -> None:
    universe = _board_universe()
    supported = frozenset({"shanghai", "shenzhen", "chinext"})

    sample = stratified_cross_source_sample(
        universe,
        target_date="2026-07-27",
        supported_boards=supported,
    )

    assert sample.excluded_boards == ("star", "beijing")
    assert len(sample.symbols) == 30
    assert all(classify_board(symbol) in supported for symbol in sample.symbols)


def test_stratified_sample_takes_short_board_pool_without_padding() -> None:
    universe = tuple(
        symbol
        for symbol in _board_universe()
        if classify_board(symbol) != "beijing"
    ) + ("830001.BJ", "920001.BJ")

    sample = stratified_cross_source_sample(
        universe,
        target_date="2026-07-27",
        supported_boards=_ALL_BOARDS,
    )

    counts = Counter(classify_board(symbol) for symbol in sample.symbols)
    assert counts["beijing"] == 2
    assert len(sample.symbols) == 26


def test_stratified_sample_rejects_unknown_board_names() -> None:
    with pytest.raises(ValueError, match="unknown board"):
        stratified_cross_source_sample(
            _board_universe(),
            target_date="2026-07-27",
            supported_boards=frozenset({"nasdaq"}),
        )


def test_price_and_volume_use_separate_tolerances() -> None:
    tolerance = CrossSourceTolerance(price=0.01, volume=0.10)
    symbols = (
        "600000.SH",
        "000001.SZ",
        "300001.SZ",
        "688001.SH",
        "830799.BJ",
    )
    primary = {
        "600000.SH": {"close": 10.20, "volume": 1000.0},
        "000001.SZ": {"close": 10.05, "volume": 1000.0},
        "300001.SZ": {"close": 10.00, "volume": 1200.0},
        "688001.SH": {"close": 10.00, "volume": 1050.0},
        "830799.BJ": {"close": 10.00, "volume": 1000.0},
    }
    reference = {
        "600000.SH": {"close": 10.00, "volume": 1000.0},
        "000001.SZ": {"close": 10.00, "volume": 1000.0},
        "300001.SZ": {"close": 10.00, "volume": 1000.0},
        "688001.SH": {"close": 10.00, "volume": 1000.0},
    }

    mismatched = cross_source_mismatches(symbols, primary, reference, tolerance)

    # 600000: 2% price diff exceeds the 1% price tolerance.
    # 000001: 0.5% price diff is inside the price tolerance.
    # 300001: 20% volume diff exceeds the 10% volume tolerance.
    # 688001: 5% volume diff is inside the volume tolerance.
    # 830799: missing reference quote always counts as a mismatch.
    assert mismatched == ("600000.SH", "300001.SZ", "830799.BJ")


def test_compare_split_separates_unverifiable_from_real_mismatches() -> None:
    """参考源缺数据（停牌/未覆盖）＝不可校验，绝不能算作真实分歧。"""
    tolerance = CrossSourceTolerance(price=0.01, volume=0.10)
    symbols = ("600000.SH", "000001.SZ", "300001.SZ", "830799.BJ")
    primary = {
        # 2% 价格差超出 1% 容差：真实分歧
        "600000.SH": {"close": 10.20, "volume": 1000.0},
        # 完全一致
        "000001.SZ": {"close": 10.00, "volume": 1000.0},
        # 830799 参考侧缺键：不可校验
        "830799.BJ": {"close": 10.00, "volume": 1000.0},
        # 300001 我方缺行但参考侧有数据：真实分歧（对方有证据我方没发布）
    }
    reference = {
        "600000.SH": {"close": 10.00, "volume": 1000.0},
        "000001.SZ": {"close": 10.00, "volume": 1000.0},
        "300001.SZ": {"close": 10.00, "volume": 1000.0},
    }

    comparison = compare_cross_source_quotes(symbols, primary, reference, tolerance)

    assert comparison.mismatched == ("600000.SH", "300001.SZ")
    assert comparison.unverifiable == ("830799.BJ",)


def test_compare_split_empty_reference_marks_all_unverifiable() -> None:
    """参考源零覆盖：全部样本落入 unverifiable，mismatched 为空。"""
    tolerance = CrossSourceTolerance(price=0.01, volume=0.10)
    symbols = ("600000.SH", "000001.SZ")
    primary = {symbol: {"close": 10.0, "volume": 1000.0} for symbol in symbols}

    comparison = compare_cross_source_quotes(symbols, primary, {}, tolerance)

    assert comparison.mismatched == ()
    assert comparison.unverifiable == symbols


def test_cross_source_mismatches_flat_view_still_flags_both_categories() -> None:
    """兼容的扁平视图保持原契约：缺证据与真实分歧都按样本顺序上报。"""
    tolerance = CrossSourceTolerance(price=0.01, volume=0.10)
    symbols = ("600000.SH", "830799.BJ", "000001.SZ")
    primary = {symbol: {"close": 10.0, "volume": 1000.0} for symbol in symbols}
    reference = {
        "600000.SH": {"close": 10.0, "volume": 1000.0},
        "000001.SZ": {"close": 11.0, "volume": 1000.0},
    }

    mismatched = cross_source_mismatches(symbols, primary, reference, tolerance)

    assert mismatched == ("830799.BJ", "000001.SZ")


def test_zero_reference_volume_requires_zero_primary_volume() -> None:
    tolerance = CrossSourceTolerance(price=0.01, volume=0.10)
    primary = {
        "600000.SH": {"close": 10.0, "volume": 0.0},
        "000001.SZ": {"close": 10.0, "volume": 5.0},
    }
    reference = {
        "600000.SH": {"close": 10.0, "volume": 0.0},
        "000001.SZ": {"close": 10.0, "volume": 0.0},
    }

    mismatched = cross_source_mismatches(
        ("600000.SH", "000001.SZ"),
        primary,
        reference,
        tolerance,
    )

    assert mismatched == ("000001.SZ",)


@pytest.mark.parametrize(
    ("price", "volume"),
    [(-0.01, 0.1), (0.01, -0.1), (float("nan"), 0.1)],
    ids=["negative-price", "negative-volume", "nan-price"],
)
def test_cross_source_tolerance_rejects_invalid_values(
    price: float,
    volume: float,
) -> None:
    with pytest.raises(ValueError, match="tolerance"):
        CrossSourceTolerance(price=price, volume=volume)
