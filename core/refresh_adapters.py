"""核心远端刷新适配器（Task 6）。

实现 RefreshAdapter 协议的四个核心远端任务：
- BarsRefreshAdapter（update_bars）：逐股重抓目标日日线，失败股票保留旧行
- FundamentalsRefreshAdapter（update_fundamentals）：目标日估值分区替换，保留已有 dividend_yield
- MarketSnapshotRefreshAdapter（update_market_snapshot）：雪球报价只补 dividend_yield
- FundFlowRefreshAdapter（update_fund_flow）：资金流 symbol/date → ts_code/trade_date 边界映射

共同约束：
- symbols=() 表示不刷新任何股票（no-op），symbols=None 表示全市场
- 抓取/校验在正式表之外完成，只经原子 staging store 发布，绝不直接提交破坏性 SQL
- 校验或源端失败 → 上抛异常（编排器重试一次后失败保留旧数据）
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from dataclasses import dataclass, field

from core.refresh import RefreshAdapter, RefreshAdapterResult, RefreshContext
from core.refresh_store import (
    DateSnapshotReplacement,
    KeyedUpsertReplacement,
    RefreshValidationError,
    SQLiteRefreshStore,
)
from core.utils import should_skip_beijing
from interface import DatabaseInterface, DataLoaderInterface
from tasks.bars import fetch_bars_for_refresh, normalize_bar_row_for_refresh
from tasks.market_flow import _FUND_FLOW_NUMERIC_FIELDS, fetch_fund_flow_records
from tasks.valuation_chain import fetch_fundamentals_snapshot, fetch_market_snapshot_quotes


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
