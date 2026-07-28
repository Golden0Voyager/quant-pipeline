"""收盘刷新适配器（Task 6 核心远端 + Task 7 派生）。

实现 RefreshAdapter 协议的四个核心远端任务：
- BarsRefreshAdapter（update_bars）：逐股重抓目标日日线，失败股票保留旧行
- FundamentalsRefreshAdapter（update_fundamentals）：目标日估值分区替换，保留已有 dividend_yield
- MarketSnapshotRefreshAdapter（update_market_snapshot）：雪球报价只补 dividend_yield
- FundFlowRefreshAdapter（update_fund_flow）：资金流 symbol/date → ts_code/trade_date 边界映射

以及五个派生任务（由全量历史计算，只提交目标日行，绝不重写历史）：
- IndicatorsRefreshAdapter（update_indicators）
- LocalChipRefreshAdapter（update_chip_distribution）
- EastmoneyChipRefreshAdapter（update_chip_distribution_em）：显式 scope + 九连败熔断
- HistoricalValuationRefreshAdapter（update_historical_valuation）
- SectorIndustryRefreshAdapter（update_sector_industry）

共同约束：
- symbols=() 表示不刷新任何股票（no-op），symbols=None 表示全市场
- 抓取/校验在正式表之外完成，只经原子 staging store 发布，绝不直接提交破坏性 SQL
- 校验或源端失败 → 上抛异常（编排器重试一次后失败保留旧数据）
"""

from __future__ import annotations

import random
import sqlite3
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, timedelta

from core.refresh import RefreshAdapter, RefreshAdapterResult, RefreshContext
from core.refresh_store import (
    DateSnapshotReplacement,
    KeyedUpsertReplacement,
    RefreshValidationError,
    SQLiteRefreshStore,
)
from core.utils import should_skip_beijing
from interface import DatabaseInterface, DataLoaderInterface, IndicatorEngineInterface
from tasks.bars import fetch_bars_for_refresh, normalize_bar_row_for_refresh
from tasks.convertible_bond import fetch_cb_index_records
from tasks.core_chain import (
    _CHIP_VALUE_COLUMNS,
    _INDICATOR_VALUE_COLUMNS,
    compute_chip_record_for_refresh,
    compute_indicator_record_for_refresh,
)
from tasks.finance_flow import fetch_south_flow_records
from tasks.index_chain import fetch_chip_em_record_for_refresh, fetch_index_daily_records
from tasks.market_flow import (
    _FUND_FLOW_NUMERIC_FIELDS,
    fetch_fund_flow_records,
    fetch_margin_trading_records,
)
from tasks.market_valuation import fetch_market_valuation_records
from tasks.valuation_chain import (
    compute_sector_industry_rows_for_refresh,
    fetch_fundamentals_snapshot,
    fetch_historical_valuation_rows_for_refresh,
    fetch_market_snapshot_quotes,
)


def _noop_result(task_name: str, context: RefreshContext) -> RefreshAdapterResult:
    """symbols=() → 不刷新任何股票；as_of_date 取目标日以通过 EXACT_TARGET 审计。"""
    return RefreshAdapterResult(
        task_name=task_name,
        as_of_date=context.target_date,
        fetched=0,
        validated=0,
        replaced=0,
        retained=0,
        failed_symbols=(),
        changed_symbols=(),
        metadata={"noop": "empty symbol scope"},
    )


def _as_store_rows(rows: list[dict], columns: tuple[str, ...]) -> tuple[tuple, ...]:
    """将字典行按列声明顺序转为 store 要求的位置元组。"""
    return tuple(tuple(row[column] for column in columns) for row in rows)


# ===========================================================================
# update_bars
# ===========================================================================

_BARS_COLUMNS = (
    "ts_code",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "data_source",
)


@dataclass
class BarsRefreshAdapter:
    """逐股重抓目标日日线并 upsert；失败股票不入库（旧行保留）。"""

    store: SQLiteRefreshStore
    db: DatabaseInterface
    loader: DataLoaderInterface

    task_name = "update_bars"

    def refresh(self, context: RefreshContext) -> RefreshAdapterResult:
        if context.symbols == ():
            return _noop_result(self.task_name, context)
        symbols = (
            self._full_market_symbols()
            if context.symbols is None
            else list(context.symbols)
        )

        target = context.target_date
        rows: list[dict] = []
        changed: list[str] = []
        failed: list[str] = []
        for symbol in symbols:
            try:
                frame = fetch_bars_for_refresh(self.loader, symbol, target)
            except Exception:
                failed.append(symbol)
                continue
            row, _reason = normalize_bar_row_for_refresh(frame, symbol, target)
            if row is None:
                failed.append(symbol)
                continue
            rows.append(row)
            changed.append(symbol)

        if rows:
            self.store.upsert_keyed_snapshot(
                KeyedUpsertReplacement(
                    table="daily_bars",
                    columns=_BARS_COLUMNS,
                    rows=_as_store_rows(rows, _BARS_COLUMNS),
                    natural_keys=("ts_code", "trade_date"),
                    required_fields=("open", "high", "low", "close", "volume", "amount"),
                )
            )

        return RefreshAdapterResult(
            task_name=self.task_name,
            as_of_date=target,
            fetched=len(symbols),
            validated=len(rows),
            replaced=len(rows),
            retained=len(failed),
            failed_symbols=tuple(failed),
            changed_symbols=tuple(changed),
            metadata={"expected_count": len(symbols)},
        )

    def _full_market_symbols(self) -> list[str]:
        stock_list = self.db.get_stock_list()
        if stock_list is None or stock_list.empty:
            raise RefreshValidationError("stock_list is empty; cannot resolve full market scope")
        codes = [str(code).strip() for code in stock_list["code"].tolist()]
        return [code for code in codes if code and not should_skip_beijing(code)]


# ===========================================================================
# update_fundamentals
# ===========================================================================

_FUNDAMENTALS_COLUMNS = (
    "ts_code",
    "trade_date",
    "pe_ttm",
    "pb",
    "ps_ttm",
    "dividend_yield",
    "roe",
    "roa",
    "gross_margin",
    "net_margin",
    "debt_ratio",
    "revenue_growth",
    "profit_growth",
    "eps_growth",
    "peg",
    "market_cap",
)


@dataclass
class FundamentalsRefreshAdapter:
    """目标日估值分区整体替换（无 5000 行阈值），保留已有 dividend_yield。"""

    store: SQLiteRefreshStore
    db_path: str
    fetch_snapshot: Callable[[str], list[dict]] = field(default=fetch_fundamentals_snapshot)
    minimum_coverage: float = 0.8

    task_name = "update_fundamentals"

    def refresh(self, context: RefreshContext) -> RefreshAdapterResult:
        if context.symbols == ():
            return _noop_result(self.task_name, context)
        target = context.target_date

        records = self.fetch_snapshot(target)
        old_yields = self._existing_dividend_yields(target)

        rows: list[dict] = []
        seen: set[str] = set()
        for record in records:
            code = str(record.get("ts_code", "")).strip()
            if not code or code in seen:
                continue
            if str(record.get("trade_date", ""))[:10] != target:
                continue
            seen.add(code)
            row = {column: record.get(column) for column in _FUNDAMENTALS_COLUMNS}
            row["trade_date"] = target
            if row.get("dividend_yield") is None:
                row["dividend_yield"] = old_yields.get(code)
            rows.append(row)

        if context.symbols is not None:
            requested = set(context.symbols)
            rows = [row for row in rows if row["ts_code"] in requested]
            self.store.upsert_keyed_snapshot(
                KeyedUpsertReplacement(
                    table="fundamentals",
                    columns=_FUNDAMENTALS_COLUMNS,
                    rows=_as_store_rows(rows, _FUNDAMENTALS_COLUMNS),
                    natural_keys=("ts_code", "trade_date"),
                )
            )
        else:
            self.store.replace_date_snapshot(
                DateSnapshotReplacement(
                    table="fundamentals",
                    columns=_FUNDAMENTALS_COLUMNS,
                    rows=_as_store_rows(rows, _FUNDAMENTALS_COLUMNS),
                    date_column="trade_date",
                    date_value=target,
                    natural_keys=("ts_code", "trade_date"),
                    minimum_coverage=self.minimum_coverage,
                )
            )

        return RefreshAdapterResult(
            task_name=self.task_name,
            as_of_date=target,
            fetched=len(records),
            validated=len(rows),
            replaced=len(rows),
            retained=0,
            failed_symbols=(),
            changed_symbols=tuple(row["ts_code"] for row in rows),
            metadata={},
        )

    def _existing_dividend_yields(self, target_date: str) -> dict[str, float]:
        conn = sqlite3.connect(self.db_path)
        try:
            fetched = conn.execute(
                "SELECT ts_code, dividend_yield FROM fundamentals"
                " WHERE trade_date = ? AND dividend_yield IS NOT NULL",
                (target_date,),
            ).fetchall()
        finally:
            conn.close()
        return {str(code): value for code, value in fetched}


# ===========================================================================
# update_market_snapshot
# ===========================================================================


@dataclass
class MarketSnapshotRefreshAdapter:
    """雪球报价补充目标日分区的 dividend_yield —— 绝不触碰 PE/PB/PEG。"""

    store: SQLiteRefreshStore
    db_path: str
    fetch_quotes: Callable[[list[str]], list[dict]] = field(default=fetch_market_snapshot_quotes)
    minimum_coverage: float = 0.4

    task_name = "update_market_snapshot"

    def refresh(self, context: RefreshContext) -> RefreshAdapterResult:
        if context.symbols == ():
            return _noop_result(self.task_name, context)
        target = context.target_date

        partition = self._partition_codes(target)
        if not partition:
            raise RefreshValidationError(
                f"fundamentals has no {target} partition to enrich"
            )

        quotes = self.fetch_quotes(sorted(partition))

        rows: list[dict] = []
        seen: set[str] = set()
        for quote in quotes:
            code = str(quote.get("code", "")).strip()
            if not code or code in seen or code not in partition:
                continue
            dividend_yield = quote.get("dividend_yield")
            if dividend_yield is None:
                continue
            seen.add(code)
            rows.append({
                "ts_code": code,
                "trade_date": target,
                "dividend_yield": dividend_yield,
            })

        coverage = len(rows) / len(partition)
        if coverage < self.minimum_coverage:
            raise RefreshValidationError(
                f"market snapshot coverage {coverage:.3f} is below minimum "
                f"{self.minimum_coverage:.3f}"
            )

        self.store.upsert_keyed_snapshot(
            KeyedUpsertReplacement(
                table="fundamentals",
                columns=("ts_code", "trade_date", "dividend_yield"),
                rows=_as_store_rows(rows, ("ts_code", "trade_date", "dividend_yield")),
                natural_keys=("ts_code", "trade_date"),
                required_fields=("dividend_yield",),
            )
        )

        return RefreshAdapterResult(
            task_name=self.task_name,
            as_of_date=target,
            fetched=len(quotes),
            validated=len(rows),
            replaced=len(rows),
            retained=len(partition) - len(rows),
            failed_symbols=(),
            changed_symbols=tuple(row["ts_code"] for row in rows),
            metadata={"coverage": coverage},
        )

    def _partition_codes(self, target_date: str) -> set[str]:
        conn = sqlite3.connect(self.db_path)
        try:
            fetched = conn.execute(
                "SELECT ts_code FROM fundamentals WHERE trade_date = ?",
                (target_date,),
            ).fetchall()
        finally:
            conn.close()
        return {str(code) for (code,) in fetched}


# ===========================================================================
# update_fund_flow
# ===========================================================================

_FUND_FLOW_COLUMNS = (
    "ts_code",
    "trade_date",
    *_FUND_FLOW_NUMERIC_FIELDS,
    "is_simulated",
)


@dataclass
class FundFlowRefreshAdapter:
    """全市场资金流目标日分区替换；legacy symbol/date 键在此映射为表列。"""

    store: SQLiteRefreshStore
    loader: DataLoaderInterface
    fetch_records: Callable[[DataLoaderInterface, str], list[dict]] = field(
        default=fetch_fund_flow_records
    )
    minimum_coverage: float = 0.8

    task_name = "update_fund_flow"

    def refresh(self, context: RefreshContext) -> RefreshAdapterResult:
        if context.symbols == ():
            return _noop_result(self.task_name, context)
        target = context.target_date

        records = self.fetch_records(self.loader, target)

        rows: list[dict] = []
        seen: set[str] = set()
        for record in records:
            code = str(record.get("symbol", "")).strip()
            if not code or code in seen:
                continue
            if str(record.get("date", ""))[:10] != target:
                continue
            numeric = {f: record.get(f) for f in _FUND_FLOW_NUMERIC_FIELDS}
            if all(value is None for value in numeric.values()):
                continue
            seen.add(code)
            rows.append({
                "ts_code": code,
                "trade_date": target,
                **numeric,
                "is_simulated": 1 if record.get("simulated") else 0,
            })

        if context.symbols is not None:
            requested = set(context.symbols)
            rows = [row for row in rows if row["ts_code"] in requested]
            self.store.upsert_keyed_snapshot(
                KeyedUpsertReplacement(
                    table="fund_flow",
                    columns=_FUND_FLOW_COLUMNS,
                    rows=_as_store_rows(rows, _FUND_FLOW_COLUMNS),
                    natural_keys=("ts_code", "trade_date"),
                )
            )
        else:
            self.store.replace_date_snapshot(
                DateSnapshotReplacement(
                    table="fund_flow",
                    columns=_FUND_FLOW_COLUMNS,
                    rows=_as_store_rows(rows, _FUND_FLOW_COLUMNS),
                    date_column="trade_date",
                    date_value=target,
                    natural_keys=("trade_date", "ts_code"),
                    minimum_coverage=self.minimum_coverage,
                )
            )

        return RefreshAdapterResult(
            task_name=self.task_name,
            as_of_date=target,
            fetched=len(records),
            validated=len(rows),
            replaced=len(rows),
            retained=0,
            failed_symbols=(),
            changed_symbols=tuple(row["ts_code"] for row in rows),
            metadata={},
        )


# ===========================================================================
# 显式注册表
# ===========================================================================


def build_core_refresh_adapters(
    *,
    db: DatabaseInterface,
    loader: DataLoaderInterface,
    store: SQLiteRefreshStore,
) -> dict[str, RefreshAdapter]:
    """显式注册核心远端任务的刷新适配器（键为注册表任务名）。"""
    db_path = str(db.db_path)
    return {
        "update_bars": BarsRefreshAdapter(store=store, db=db, loader=loader),
        "update_fundamentals": FundamentalsRefreshAdapter(store=store, db_path=db_path),
        "update_market_snapshot": MarketSnapshotRefreshAdapter(store=store, db_path=db_path),
        "update_fund_flow": FundFlowRefreshAdapter(store=store, loader=loader),
    }


# ===========================================================================
# 派生任务公共 helper（Task 7）
# ===========================================================================


def _target_partition_symbols(db_path: str, target_date: str) -> list[str]:
    """symbols=None 时以目标日 daily_bars 分区为派生任务的全市场范围。"""
    conn = sqlite3.connect(db_path)
    try:
        fetched = conn.execute(
            "SELECT DISTINCT ts_code FROM daily_bars WHERE trade_date = ?"
            " ORDER BY ts_code",
            (target_date,),
        ).fetchall()
    finally:
        conn.close()
    return [str(code) for (code,) in fetched]


# ===========================================================================
# update_indicators（派生）
# ===========================================================================

_INDICATORS_COLUMNS = ("ts_code", "trade_date", *_INDICATOR_VALUE_COLUMNS)


@dataclass
class IndicatorsRefreshAdapter:
    """由全量历史计算指标，仅 upsert 目标日一行；失败股票旧行保留。"""

    store: SQLiteRefreshStore
    db: DatabaseInterface
    engine: IndicatorEngineInterface

    task_name = "update_indicators"

    def refresh(self, context: RefreshContext) -> RefreshAdapterResult:
        if context.symbols == ():
            return _noop_result(self.task_name, context)
        target = context.target_date
        symbols = (
            _target_partition_symbols(str(self.db.db_path), target)
            if context.symbols is None
            else list(context.symbols)
        )

        rows: list[dict] = []
        changed: list[str] = []
        failed: list[str] = []
        insufficient = 0
        for symbol in symbols:
            record, reason = compute_indicator_record_for_refresh(
                self.db, self.engine, symbol, target
            )
            if record is not None:
                rows.append(record)
                changed.append(symbol)
            elif reason == "insufficient":
                insufficient += 1
            else:
                failed.append(symbol)

        if rows:
            self.store.upsert_keyed_snapshot(
                KeyedUpsertReplacement(
                    table="indicators",
                    columns=_INDICATORS_COLUMNS,
                    rows=_as_store_rows(rows, _INDICATORS_COLUMNS),
                    natural_keys=("ts_code", "trade_date"),
                )
            )

        return RefreshAdapterResult(
            task_name=self.task_name,
            as_of_date=target,
            fetched=len(symbols),
            validated=len(rows),
            replaced=len(rows),
            retained=len(failed),
            failed_symbols=tuple(failed),
            changed_symbols=tuple(changed),
            metadata={"insufficient": insufficient},
        )


# ===========================================================================
# update_chip_distribution（派生，本地计算）
# ===========================================================================

_CHIP_COLUMNS = ("ts_code", "trade_date", *_CHIP_VALUE_COLUMNS)


@dataclass
class LocalChipRefreshAdapter:
    """由全量历史计算本地筹码分布，仅 upsert 目标日一行。"""

    store: SQLiteRefreshStore
    db: DatabaseInterface

    task_name = "update_chip_distribution"

    def refresh(self, context: RefreshContext) -> RefreshAdapterResult:
        if context.symbols == ():
            return _noop_result(self.task_name, context)
        target = context.target_date
        symbols = (
            _target_partition_symbols(str(self.db.db_path), target)
            if context.symbols is None
            else list(context.symbols)
        )

        rows: list[dict] = []
        changed: list[str] = []
        failed: list[str] = []
        insufficient = 0
        for symbol in symbols:
            record, reason = compute_chip_record_for_refresh(self.db, symbol, target)
            if record is not None:
                rows.append(record)
                changed.append(symbol)
            elif reason == "insufficient":
                insufficient += 1
            else:
                failed.append(symbol)

        if rows:
            self.store.upsert_keyed_snapshot(
                KeyedUpsertReplacement(
                    table="chip_distribution",
                    columns=_CHIP_COLUMNS,
                    rows=_as_store_rows(rows, _CHIP_COLUMNS),
                    natural_keys=("ts_code", "trade_date"),
                )
            )

        return RefreshAdapterResult(
            task_name=self.task_name,
            as_of_date=target,
            fetched=len(symbols),
            validated=len(rows),
            replaced=len(rows),
            retained=len(failed),
            failed_symbols=tuple(failed),
            changed_symbols=tuple(changed),
            metadata={"insufficient": insufficient},
        )


# ===========================================================================
# update_historical_valuation（派生）
# ===========================================================================

_HISTORICAL_VALUATION_COLUMNS = (
    "ts_code",
    "trade_date",
    "pe_ttm",
    "pb",
    "ps_ttm",
    "dividend_yield",
)


@dataclass
class HistoricalValuationRefreshAdapter:
    """把 fundamentals 目标日分区快照到 historical_valuation；空分区拒绝。"""

    store: SQLiteRefreshStore
    db_path: str

    task_name = "update_historical_valuation"

    def refresh(self, context: RefreshContext) -> RefreshAdapterResult:
        if context.symbols == ():
            return _noop_result(self.task_name, context)
        target = context.target_date

        fetched = fetch_historical_valuation_rows_for_refresh(self.db_path, target)
        if not fetched:
            raise RefreshValidationError(
                f"fundamentals has no {target} partition for historical valuation"
            )

        rows = fetched
        if context.symbols is not None:
            requested = set(context.symbols)
            rows = [row for row in rows if row["ts_code"] in requested]

        self.store.upsert_keyed_snapshot(
            KeyedUpsertReplacement(
                table="historical_valuation",
                columns=_HISTORICAL_VALUATION_COLUMNS,
                rows=_as_store_rows(rows, _HISTORICAL_VALUATION_COLUMNS),
                natural_keys=("ts_code", "trade_date"),
            )
        )

        return RefreshAdapterResult(
            task_name=self.task_name,
            as_of_date=target,
            fetched=len(fetched),
            validated=len(rows),
            replaced=len(rows),
            retained=0,
            failed_symbols=(),
            changed_symbols=tuple(row["ts_code"] for row in rows),
            metadata={},
        )


# ===========================================================================
# update_sector_industry（派生）
# ===========================================================================

_SECTOR_INDUSTRY_COLUMNS = (
    "industry_name",
    "trade_date",
    "avg_pe",
    "avg_pb",
    "avg_ps",
    "avg_roe",
    "avg_revenue_growth",
    "avg_profit_growth",
    "total_market_cap",
    "fund_inflow_rank",
    "data_source",
)


@dataclass
class SectorIndustryRefreshAdapter:
    """由目标日 fundamentals 聚合行业对比，整分区替换（清理盘中残留）。"""

    store: SQLiteRefreshStore
    db_path: str

    task_name = "update_sector_industry"

    def refresh(self, context: RefreshContext) -> RefreshAdapterResult:
        if context.symbols == ():
            return _noop_result(self.task_name, context)
        target = context.target_date

        rows = compute_sector_industry_rows_for_refresh(self.db_path, target)
        if not rows:
            raise RefreshValidationError(
                f"fundamentals has no {target} partition for sector industry"
            )

        # 行业行数量级与股票无关，不设覆盖率门槛（空分区已在上方拒绝）
        self.store.replace_date_snapshot(
            DateSnapshotReplacement(
                table="sector_industry",
                columns=_SECTOR_INDUSTRY_COLUMNS,
                rows=_as_store_rows(rows, _SECTOR_INDUSTRY_COLUMNS),
                date_column="trade_date",
                date_value=target,
                natural_keys=("industry_name", "trade_date"),
            )
        )

        return RefreshAdapterResult(
            task_name=self.task_name,
            as_of_date=target,
            fetched=len(rows),
            validated=len(rows),
            replaced=len(rows),
            retained=0,
            failed_symbols=(),
            changed_symbols=(),
            metadata={"industries": len(rows)},
        )


# ===========================================================================
# update_chip_distribution_em（派生，EM 远端 + 硬熔断）
# ===========================================================================

_CHIP_EM_COLUMNS = (
    "ts_code",
    "trade_date",
    "profit_ratio",
    "avg_cost",
    "cost_90_low",
    "cost_90_high",
    "concentration_90",
    "cost_70_low",
    "cost_70_high",
    "concentration_70",
)


def _default_chip_em_throttle() -> None:
    """EM 反爬限速：与 legacy 一致的 1-2 秒随机间隔。"""
    time.sleep(random.uniform(1.0, 2.0))


@dataclass
class EastmoneyChipRefreshAdapter:
    """EM 筹码刷新：显式目标集、只提交目标日行、九连败硬熔断。

    熔断触发时不删旧筹码行；未处理股票全部并入失败队列，metadata
    携带 aborted/abort_reason 供编排器与审计记录。只有源端失败
    （fetch_failed）计入连续失败；成功或其他原因重置计数。
    """

    store: SQLiteRefreshStore
    fetch_record: Callable[[str, str], tuple[dict | None, str | None]] = field(
        default=fetch_chip_em_record_for_refresh
    )
    max_consecutive_failures: int = 9
    throttle: Callable[[], None] = field(default=_default_chip_em_throttle)

    task_name = "update_chip_distribution_em"

    def refresh(self, context: RefreshContext) -> RefreshAdapterResult:
        if context.symbols == ():
            return _noop_result(self.task_name, context)
        if context.symbols is None:
            raise RefreshValidationError(
                "update_chip_distribution_em requires an explicit symbol scope"
                " in refresh mode (random target selector is forbidden)"
            )
        target = context.target_date
        symbols = list(context.symbols)

        rows: list[dict] = []
        changed: list[str] = []
        failed: list[str] = []
        consecutive = 0
        aborted = False
        processed = 0
        for index, symbol in enumerate(symbols):
            if index:
                self.throttle()
            record, reason = self.fetch_record(symbol, target)
            processed = index + 1
            if record is not None:
                rows.append(record)
                changed.append(symbol)
                consecutive = 0
                continue
            failed.append(symbol)
            if reason == "fetch_failed":
                consecutive += 1
                if consecutive >= self.max_consecutive_failures:
                    aborted = True
                    failed.extend(symbols[index + 1:])
                    break
            else:
                consecutive = 0

        if rows:
            self.store.upsert_keyed_snapshot(
                KeyedUpsertReplacement(
                    table="chip_distribution_em",
                    columns=_CHIP_EM_COLUMNS,
                    rows=_as_store_rows(rows, _CHIP_EM_COLUMNS),
                    natural_keys=("ts_code", "trade_date"),
                )
            )

        metadata: dict = {"aborted": aborted}
        if aborted:
            metadata["abort_reason"] = "consecutive_failures"
            metadata["unprocessed"] = len(symbols) - processed
        return RefreshAdapterResult(
            task_name=self.task_name,
            as_of_date=target,
            fetched=processed,
            validated=len(rows),
            replaced=len(rows),
            retained=len(failed),
            failed_symbols=tuple(failed),
            changed_symbols=tuple(changed),
            metadata=metadata,
        )


# ===========================================================================
# 派生任务显式注册表
# ===========================================================================


def build_derived_refresh_adapters(
    *,
    db: DatabaseInterface,
    engine: IndicatorEngineInterface,
    store: SQLiteRefreshStore,
) -> dict[str, RefreshAdapter]:
    """显式注册派生任务的刷新适配器（键为注册表任务名）。"""
    db_path = str(db.db_path)
    return {
        "update_indicators": IndicatorsRefreshAdapter(store=store, db=db, engine=engine),
        "update_chip_distribution": LocalChipRefreshAdapter(store=store, db=db),
        "update_chip_distribution_em": EastmoneyChipRefreshAdapter(store=store),
        "update_historical_valuation": HistoricalValuationRefreshAdapter(
            store=store, db_path=db_path
        ),
        "update_sector_industry": SectorIndustryRefreshAdapter(
            store=store, db_path=db_path
        ),
    }


# ===========================================================================
# 回看接受型公共 helper（Task 9）
# ===========================================================================


def _lookback_candidates(target_date: str, lookback_days: int) -> list[str]:
    """目标日起逐日回退的候选日期序列（含目标日，新→旧）。"""
    anchor = date.fromisoformat(target_date)
    return [
        (anchor - timedelta(days=offset)).isoformat()
        for offset in range(lookback_days + 1)
    ]


def _lookback_partition(
    records: list[dict],
    target_date: str,
    lookback_days: int,
    date_field: str = "trade_date",
) -> tuple[str, list[dict]]:
    """从全历史记录中挑回看窗口内最新的日期分区；窗口内无数据 → 上抛。"""
    window = set(_lookback_candidates(target_date, lookback_days))
    seen_dates = {str(record.get(date_field, ""))[:10] for record in records}
    acceptable = sorted(seen_dates & window)
    if not acceptable:
        raise RefreshValidationError(
            f"no partition within {lookback_days}-day lookback of {target_date}"
        )
    as_of = acceptable[-1]
    partition = [
        record for record in records
        if str(record.get(date_field, ""))[:10] == as_of
    ]
    return as_of, partition


# ===========================================================================
# update_margin_trading（回看接受，逐日探测）
# ===========================================================================

_MARGIN_COLUMNS = (
    "ts_code",
    "trade_date",
    "margin_balance",
    "margin_buy",
    "margin_repay",
    "short_balance",
    "short_sell",
    "short_repay",
    "total_balance",
    "data_source",
)


@dataclass
class MarginTradingRefreshAdapter:
    """融资融券：目标日起逐日探测，接受回看窗口内首个非空日分区。"""

    store: SQLiteRefreshStore
    fetch_records: Callable[[str], list[dict]] = field(default=fetch_margin_trading_records)
    lookback_days: int = 3

    task_name = "update_margin_trading"

    def refresh(self, context: RefreshContext) -> RefreshAdapterResult:
        if context.symbols == ():
            return _noop_result(self.task_name, context)
        target = context.target_date

        as_of: str | None = None
        records: list[dict] = []
        for candidate in _lookback_candidates(target, self.lookback_days):
            fetched = self.fetch_records(candidate)
            if fetched:
                as_of = candidate
                records = fetched
                break
        if as_of is None:
            raise RefreshValidationError(
                f"margin trading has no data within {self.lookback_days}-day"
                f" lookback of {target}"
            )

        rows: list[dict] = []
        seen: set[str] = set()
        for record in records:
            code = str(record.get("ts_code", "")).strip()
            if not code or code in seen:
                continue
            seen.add(code)
            row = {column: record.get(column) for column in _MARGIN_COLUMNS}
            row["ts_code"] = code
            row["trade_date"] = as_of
            rows.append(row)

        if context.symbols is not None:
            requested = set(context.symbols)
            rows = [row for row in rows if row["ts_code"] in requested]
            self.store.upsert_keyed_snapshot(
                KeyedUpsertReplacement(
                    table="margin_trading",
                    columns=_MARGIN_COLUMNS,
                    rows=_as_store_rows(rows, _MARGIN_COLUMNS),
                    natural_keys=("trade_date", "ts_code"),
                )
            )
        else:
            self.store.replace_date_snapshot(
                DateSnapshotReplacement(
                    table="margin_trading",
                    columns=_MARGIN_COLUMNS,
                    rows=_as_store_rows(rows, _MARGIN_COLUMNS),
                    date_column="trade_date",
                    date_value=as_of,
                    natural_keys=("trade_date", "ts_code"),
                )
            )

        return RefreshAdapterResult(
            task_name=self.task_name,
            as_of_date=as_of,
            fetched=len(records),
            validated=len(rows),
            replaced=len(rows),
            retained=0,
            failed_symbols=(),
            changed_symbols=tuple(row["ts_code"] for row in rows),
            metadata={},
        )


# ===========================================================================
# update_south_flow（回看接受，历史型源）
# ===========================================================================

_SOUTH_FLOW_COLUMNS = (
    "trade_date",
    "market",
    "net_buy_amount",
    "buy_amount",
    "sell_amount",
    "cumulative_net_buy",
    "data_source",
)


@dataclass
class SouthFlowRefreshAdapter:
    """南向资金：历史型源只提交回看窗口内最新分区，历史行绝不重写。"""

    store: SQLiteRefreshStore
    fetch_records: Callable[[], list[dict]] = field(default=fetch_south_flow_records)
    lookback_days: int = 3

    task_name = "update_south_flow"

    def refresh(self, context: RefreshContext) -> RefreshAdapterResult:
        if context.symbols == ():
            return _noop_result(self.task_name, context)

        records = self.fetch_records()
        as_of, partition = _lookback_partition(
            records, context.target_date, self.lookback_days
        )

        rows: list[dict] = []
        seen: set[str] = set()
        for record in partition:
            market = str(record.get("market", "")).strip()
            if not market or market in seen:
                continue
            seen.add(market)
            row = {column: record.get(column) for column in _SOUTH_FLOW_COLUMNS}
            row["trade_date"] = as_of
            rows.append(row)

        self.store.replace_date_snapshot(
            DateSnapshotReplacement(
                table="south_flow",
                columns=_SOUTH_FLOW_COLUMNS,
                rows=_as_store_rows(rows, _SOUTH_FLOW_COLUMNS),
                date_column="trade_date",
                date_value=as_of,
                natural_keys=("trade_date", "market"),
            )
        )

        return RefreshAdapterResult(
            task_name=self.task_name,
            as_of_date=as_of,
            fetched=len(records),
            validated=len(rows),
            replaced=len(rows),
            retained=0,
            failed_symbols=(),
            changed_symbols=(),
            metadata={},
        )


# ===========================================================================
# update_index_daily（回看接受，要求完整指数分区）
# ===========================================================================

_INDEX_DAILY_COLUMNS = (
    "index_code",
    "index_name",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "data_source",
)


@dataclass
class IndexDailyRefreshAdapter:
    """四大指数日线：只接受包含全部指数的完整日期分区（防指数滞后）。"""

    store: SQLiteRefreshStore
    fetch_records: Callable[[], list[dict]] = field(default=fetch_index_daily_records)
    lookback_days: int = 3

    task_name = "update_index_daily"

    def refresh(self, context: RefreshContext) -> RefreshAdapterResult:
        if context.symbols == ():
            return _noop_result(self.task_name, context)
        target = context.target_date

        records = self.fetch_records()
        expected = {
            str(record.get("index_code", "")).strip()
            for record in records
            if str(record.get("index_code", "")).strip()
        }
        if not expected:
            raise RefreshValidationError("index daily source returned no index codes")

        codes_by_date: dict[str, set[str]] = {}
        for record in records:
            trade_date = str(record.get("trade_date", ""))[:10]
            code = str(record.get("index_code", "")).strip()
            if trade_date and code:
                codes_by_date.setdefault(trade_date, set()).add(code)

        as_of: str | None = None
        for candidate in _lookback_candidates(target, self.lookback_days):
            if codes_by_date.get(candidate, set()) >= expected:
                as_of = candidate
                break
        if as_of is None:
            raise RefreshValidationError(
                f"no complete index partition within {self.lookback_days}-day"
                f" lookback of {target}"
            )

        rows: list[dict] = []
        seen: set[str] = set()
        for record in records:
            if str(record.get("trade_date", ""))[:10] != as_of:
                continue
            code = str(record.get("index_code", "")).strip()
            if not code or code in seen:
                continue
            seen.add(code)
            row = {column: record.get(column) for column in _INDEX_DAILY_COLUMNS}
            row["trade_date"] = as_of
            rows.append(row)

        self.store.replace_date_snapshot(
            DateSnapshotReplacement(
                table="index_daily",
                columns=_INDEX_DAILY_COLUMNS,
                rows=_as_store_rows(rows, _INDEX_DAILY_COLUMNS),
                date_column="trade_date",
                date_value=as_of,
                natural_keys=("trade_date", "index_code"),
                required_fields=("close",),
            )
        )

        return RefreshAdapterResult(
            task_name=self.task_name,
            as_of_date=as_of,
            fetched=len(records),
            validated=len(rows),
            replaced=len(rows),
            retained=0,
            failed_symbols=(),
            changed_symbols=(),
            metadata={"indices": len(rows)},
        )


# ===========================================================================
# update_market_valuation（回看接受，三源全历史合并）
# ===========================================================================

_MARKET_VALUATION_COLUMNS = (
    "date",
    "pe_median",
    "pe_quantile",
    "pe_lyr_median",
    "pb_median",
    "pb_quantile",
    "equity_bond_spread",
    "ebs_ma",
    "csi300_close",
    "data_source",
    "data_date",
)


@dataclass
class MarketValuationRefreshAdapter:
    """大盘估值：全历史合并源只发布回看窗口内最新一行，历史行不落库。"""

    store: SQLiteRefreshStore
    fetch_records: Callable[[str], list[dict]] = field(default=fetch_market_valuation_records)
    lookback_days: int = 3

    task_name = "update_market_valuation"

    def refresh(self, context: RefreshContext) -> RefreshAdapterResult:
        if context.symbols == ():
            return _noop_result(self.task_name, context)
        target = context.target_date

        records = self.fetch_records(target)
        as_of, partition = _lookback_partition(
            records, target, self.lookback_days, date_field="date"
        )

        # 三源 setdefault 合并可能缺列，统一用 .get 归一化再转位置元组
        rows = [
            {column: record.get(column) for column in _MARKET_VALUATION_COLUMNS}
            for record in partition[:1]
        ]

        self.store.replace_date_snapshot(
            DateSnapshotReplacement(
                table="market_valuation",
                columns=_MARKET_VALUATION_COLUMNS,
                rows=_as_store_rows(rows, _MARKET_VALUATION_COLUMNS),
                date_column="date",
                date_value=as_of,
                natural_keys=("date",),
                required_fields=("date", "data_source"),
            )
        )

        return RefreshAdapterResult(
            task_name=self.task_name,
            as_of_date=as_of,
            fetched=len(records),
            validated=len(rows),
            replaced=len(rows),
            retained=0,
            failed_symbols=(),
            changed_symbols=(),
            metadata={},
        )


# ===========================================================================
# update_cb_index（回看接受，历史型源）
# ===========================================================================

_CB_INDEX_COLUMNS = (
    "trade_date",
    "index_code",
    "index_name",
    "open",
    "close",
    "high",
    "low",
    "volume",
    "data_source",
)


@dataclass
class CbIndexRefreshAdapter:
    """可转债等权指数：全历史源只提交回看窗口内最新分区。"""

    store: SQLiteRefreshStore
    fetch_records: Callable[[], list[dict]] = field(default=fetch_cb_index_records)
    lookback_days: int = 3

    task_name = "update_cb_index"

    def refresh(self, context: RefreshContext) -> RefreshAdapterResult:
        if context.symbols == ():
            return _noop_result(self.task_name, context)

        records = self.fetch_records()
        as_of, partition = _lookback_partition(
            records, context.target_date, self.lookback_days
        )

        rows: list[dict] = []
        seen: set[str] = set()
        for record in partition:
            code = str(record.get("index_code", "")).strip()
            if not code or code in seen:
                continue
            seen.add(code)
            row = {column: record.get(column) for column in _CB_INDEX_COLUMNS}
            row["trade_date"] = as_of
            rows.append(row)

        self.store.replace_date_snapshot(
            DateSnapshotReplacement(
                table="cb_index",
                columns=_CB_INDEX_COLUMNS,
                rows=_as_store_rows(rows, _CB_INDEX_COLUMNS),
                date_column="trade_date",
                date_value=as_of,
                natural_keys=("trade_date", "index_code"),
                required_fields=("close",),
            )
        )

        return RefreshAdapterResult(
            task_name=self.task_name,
            as_of_date=as_of,
            fetched=len(records),
            validated=len(rows),
            replaced=len(rows),
            retained=0,
            failed_symbols=(),
            changed_symbols=(),
            metadata={},
        )


# ===========================================================================
# 运行时适配器总注册表（覆盖测试要求与 refreshable_trading_tasks 一一对应）
# ===========================================================================

REFRESH_ADAPTERS: dict[str, type] = {
    "update_bars": BarsRefreshAdapter,
    "update_fundamentals": FundamentalsRefreshAdapter,
    "update_market_snapshot": MarketSnapshotRefreshAdapter,
    "update_fund_flow": FundFlowRefreshAdapter,
    "update_indicators": IndicatorsRefreshAdapter,
    "update_chip_distribution": LocalChipRefreshAdapter,
    "update_chip_distribution_em": EastmoneyChipRefreshAdapter,
    "update_historical_valuation": HistoricalValuationRefreshAdapter,
    "update_sector_industry": SectorIndustryRefreshAdapter,
    "update_margin_trading": MarginTradingRefreshAdapter,
    "update_south_flow": SouthFlowRefreshAdapter,
    "update_index_daily": IndexDailyRefreshAdapter,
    "update_market_valuation": MarketValuationRefreshAdapter,
    "update_cb_index": CbIndexRefreshAdapter,
}
