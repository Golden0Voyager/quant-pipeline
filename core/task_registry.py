"""
Single source of truth for every pipeline task.
══════════════════════════════════════════

Replaces scattered task-name strings, hardcoded if-elif chains, and
duplicate task-to-table mappings throughout ``daily_pipeline.py``,
``tui.py``, and ``tasks/utility.py``.

Usage
─────
    from core.task_registry import TASK_REGISTRY, lookup_task

    for spec in TASK_REGISTRY:
        result = spec.callable(db, ...)

    spec = lookup_task("update_bars")
    print(spec.tables, spec.cadence)
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

logger = logging.getLogger(__name__)


# ── value types ────────────────────────────────────────────────────────


class Cadence(StrEnum):
    """Expected data-production frequency."""

    TRADING_DAY = "trading_day"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    ON_DEMAND = "on_demand"


class EmptyPolicy(StrEnum):
    """What it means when a task produces zero rows."""

    ALLOW = "allow"
    ALLOW_ON_NON_TRADING_DAY = "allow_on_non_trading_day"
    FAIL = "fail"


class RefreshKind(StrEnum):
    """How a task safely replaces its close-refresh output."""

    REMOTE_DATE_SNAPSHOT = "remote_date_snapshot"
    REMOTE_KEYED_UPSERT = "remote_keyed_upsert"
    DERIVED_RECOMPUTE = "derived_recompute"
    COMPOSITE_ATOMIC = "composite_atomic"
    REMOTE_RUN_SNAPSHOT = "remote_run_snapshot"


class DateStrategy(StrEnum):
    """Which date boundary validates a close-refresh result."""

    EXACT_TARGET = "exact_target"
    LATEST_AVAILABLE_WITHIN_LOOKBACK = "latest_available_within_lookback"
    RUN_SNAPSHOT = "run_snapshot"


@dataclass(frozen=True)
class RefreshPolicy:
    """Explicit close-refresh contract for one trading-day task."""

    kind: RefreshKind
    date_strategy: DateStrategy
    natural_keys: Mapping[str, tuple[str, ...]]
    required_fields: Mapping[str, tuple[str, ...]]
    dependencies: tuple[str, ...] = ()
    supports_symbols: bool = False
    minimum_coverage: float | None = None
    cache_namespace: str | None = None
    lookback_days: int = 0


@dataclass(frozen=True)
class TaskSpec:
    """Descriptor for one pipeline task.

    Attributes
    ----------
    name:
        CLI / TUI key (e.g. ``"update_bars"``).
    callable:
        The task function. Signature must be compatible with
        ``core.runner.safe_task``.
    tables:
        Names of the database tables this task writes to.
    cadence:
        Expected production cadence.
    date_columns:
        Mapping ``{table_name: date_column_name}`` for freshness checks.
    empty_policy:
        How zero-row results are interpreted.
    primary_source:
        Primary data source identifier (e.g. ``"akshare"``).
    fallback_sources:
        Alternative sources tried when the primary fails.
    grace_period_days:
        How many days stale data is tolerated before flagging critical.
    display_label:
        Human-readable label for TUI menus (optional).
    """

    name: str
    callable: Callable[..., Any]
    tables: tuple[str, ...]
    cadence: Cadence
    date_columns: Mapping[str, str]
    empty_policy: EmptyPolicy
    primary_source: str
    fallback_sources: tuple[str, ...] = ()
    grace_period_days: int = 3
    display_label: str = ""
    refresh_policy: RefreshPolicy | None = None


# ── derived views (computed from TASK_REGISTRY) ────────────────────────


def lookup_task(name: str) -> TaskSpec | None:
    """Return the ``TaskSpec`` for *name*, or ``None``."""
    for spec in TASK_REGISTRY:
        if spec.name == name:
            return spec
    return None


def table_owners(table: str) -> list[TaskSpec]:
    """Return every task that writes to *table*."""
    return [spec for spec in TASK_REGISTRY if table in spec.tables]


def task_names() -> list[str]:
    """Return all registered task names (alphabetically sorted)."""
    return sorted(spec.name for spec in TASK_REGISTRY)


def table_date_columns() -> dict[str, str]:
    """Derived view: ``{table: date_column}`` for all registered tables."""
    out: dict[str, str] = {}
    for spec in TASK_REGISTRY:
        out.update(spec.date_columns)
    return out


def task_to_table() -> dict[str, list[str]]:
    """Derived view: ``{task_name: [tables]}`` for tasks that write tables."""
    return {spec.name: list(spec.tables) for spec in TASK_REGISTRY if spec.tables}


# ── presentation metadata (single source; consumed by tui.py) ─────────

# 以下常量自 tui.py 一次性搬入（2026-08-01 元数据收敛），键为表名/组名。
TABLE_LABELS: dict[str, str] = {
    # 行情核心
    "daily_bars": "Daily Bars",
    "indicators": "Indicators",
    # 基本面
    "fundamentals": "Fundamentals",
    "historical_valuation": "Valuation",
    "quarterly_financials": "Quarterly Fin.",
    "dividend_summary": "Dividends",
    # 资金面
    "fund_flow": "Fund Flow",
    "margin_trading": "Margin Trading",
    "dragon_tiger": "Dragon Tiger",
    "block_trade": "Block Trade",
    "sector_fund_flow": "Sector Flow",
    "north_hold": "North Hold",
    # 行业/大盘
    "sector_industry": "Industry",
    "index_daily": "Index Daily",
    "limit_up_down": "Limit U/D",
    # 股东
    "shareholder_count": "Shareholders",
    "institutional_holdings": "Inst. Holdings",
    # 筹码分布
    "chip_distribution": "Chip Dist.",
    "chip_distribution_em": "Chip EM",
    # 宏观
    "gold_price": "Gold Price",
    "crude_oil": "Crude Oil",
    "fx_rate": "USD/CNY",
    "global_index": "Global Index",
    "us_treasury": "US Treasury",
    # 期货
    "futures_daily": "Futures",
    # 衍生数据
    "south_flow": "South Flow",
    "ah_premium": "AH Premium",
    "etf_daily": "ETF Daily",
    "cb_quotation": "CB Quotation",
    "cb_redeem": "CB Redeem",
    "cb_index": "CB Index",
    "restricted_share": "Restricted Share",
    "earnings_forecast": "Earnings Forecast",
    "stock_repurchase": "Stock Repurchase",
    "institution_survey": "Institution Survey",
    "stock_pledge": "Stock Pledge",
    "option_sentiment": "Option Sentiment",
    "sector_daily": "Sector Daily",
    "sector_valuation": "Sector Val.",
    "index_futures_basis": "Futures Basis",
    # 宏观
    "macro_monthly": "Macro Monthly",
    "macro_quarterly": "Macro Quarterly",
    # 总览
    "stock_list": "Stock List",
}

TABLE_LABELS_CN: dict[str, str] = {
    # 行情核心
    "daily_bars": "日线行情",
    "indicators": "技术指标",
    # 基本面
    "fundamentals": "基本面数据",
    "historical_valuation": "历史估值",
    "quarterly_financials": "季度财务",
    "dividend_summary": "分红信息",
    # 资金面
    "fund_flow": "资金流向",
    "margin_trading": "融资融券",
    "dragon_tiger": "龙虎榜",
    "block_trade": "大宗交易",
    "sector_fund_flow": "板块资金",
    "north_hold": "北向持仓",
    # 行业/大盘
    "sector_industry": "行业分类",
    "index_daily": "大盘指数",
    "limit_up_down": "涨跌停",
    # 股东
    "shareholder_count": "股东户数",
    "institutional_holdings": "机构持仓",
    # 筹码分布
    "chip_distribution": "筹码分布",
    "chip_distribution_em": "筹码分布(EM)",
    # 宏观
    "gold_price": "黄金价格",
    "crude_oil": "原油价格",
    "fx_rate": "汇率",
    "global_index": "全球指数",
    "us_treasury": "美债收益率",
    # 期货
    "futures_daily": "期货日线",
    # 衍生数据
    "south_flow": "南向资金",
    "ah_premium": "AH溢价",
    "etf_daily": "ETF日线",
    "cb_quotation": "可转债行情",
    "cb_redeem": "可转债强赎",
    "cb_index": "可转债指数",
    "restricted_share": "限售解禁",
    "earnings_forecast": "业绩预告",
    "stock_repurchase": "股票回购",
    "institution_survey": "机构调研",
    "stock_pledge": "股票质押",
    "option_sentiment": "期权情绪",
    "sector_daily": "行业涨跌幅",
    "sector_valuation": "板块估值",
    "index_futures_basis": "基差",
    # 宏观
    "macro_monthly": "宏观(月)",
    "macro_quarterly": "宏观(季)",
    # 总览
    "stock_list": "股票列表",
}

# 任务分组：把相似的 single task 聚合成一键顺序执行的按钮组
TASK_GROUPS: dict[str, list[str]] = {
    "core": [
        "update_bars",
        "update_indicators",
        "update_fundamentals",
        "update_chip_distribution",
        "update_chip_distribution_em",
        "update_chip_distribution_em_fullmarket",
        "update_market_snapshot",
    ],
    "fund": [
        "update_fund_flow",
        "update_sector_fund_flow",
        "update_north_hold",
        "update_margin_trading",
        "update_dragon_tiger",
        "update_block_trade",
    ],
    "valuation": [
        "update_historical_valuation",
        "update_quarterly_financials",
        "update_shareholder_count",
        "update_dividend_summary",
    ],
    "macro": [
        "update_china_macro",
        "update_gold_price",
        "update_crude_oil",
        "update_usd",
        "update_global_index",
        "update_us_treasury",
        "update_futures",
    ],
    "sector_index": [
        "update_sector_industry",
        "update_industry",
        "update_sector_derivatives",
        "update_index_daily",
        "update_limit_up_down",
    ],
    "derivatives": [
        "update_etf_daily",
        "update_cb_quotation",
        "update_cb_redeem",
        "update_cb_index",
        "update_south_flow",
        "update_ah_premium",
    ],
    "events": [
        "update_restricted_share",
        "update_earnings_forecast",
        "update_stock_repurchase",
        "update_institution_survey",
        "update_stock_pledge",
        "update_option_sentiment",
    ],
}

# 补齐缺失按钮的任务执行顺序：与 run_all 的"依赖先行、长尾垫底"一致。
# 一张表可能有多个 owner 任务（如 chip_distribution_em 的日常/全市场版），
# 按此列表先到先得，确保选到日常任务而非 ON_DEMAND 任务。
CATCH_UP_TASK_ORDER: tuple[str, ...] = (
    "update_bars",
    "update_indicators",
    "update_chip_distribution",
    "update_fundamentals",
    "update_market_snapshot",
    "update_historical_valuation",
    "update_sector_fund_flow",
    "update_sector_industry",
    "update_fund_flow",
    "update_margin_trading",
    "update_dragon_tiger",
    "update_block_trade",
    "update_limit_up_down",
    "update_index_daily",
    "update_market_valuation",
    "update_concept_board",
    "update_south_flow",
    "update_ah_premium",
    "update_etf_daily",
    "update_cb_quotation",
    "update_cb_redeem",
    "update_cb_index",
    "update_sector_derivatives",
    "update_option_sentiment",
    "update_stock_repurchase",
    "update_institution_survey",
    "update_stock_pledge",
    "update_restricted_share",
    "update_earnings_forecast",
    "update_dividend_summary",
    "update_gold_price",
    "update_crude_oil",
    "update_usd",
    "update_global_index",
    "update_us_treasury",
    "update_futures",
    "update_china_macro",
    "update_money_market",
    "update_chip_distribution_em",
)


def refreshable_trading_tasks() -> tuple[TaskSpec, ...]:
    """Return the trading-day tasks explicitly eligible for close refresh."""
    return tuple(
        spec
        for spec in TASK_REGISTRY
        if spec.cadence is Cadence.TRADING_DAY and spec.refresh_policy is not None
    )


# ── registry ───────────────────────────────────────────────────────────

# fmt: off
TASK_REGISTRY: tuple[TaskSpec, ...] = (
    # ── Core daily pipeline ────────────────────────────────────────
    TaskSpec(
        name="update_stock_list",
        callable=None,  # filled after module import
        tables=("stock_list",),
        cadence=Cadence.MONTHLY,
        date_columns={"stock_list": "updated_at"},
        empty_policy=EmptyPolicy.FAIL,
        primary_source="akshare",
        display_label="股票列表",
    ),
    TaskSpec(
        name="update_bars",
        callable=None,
        tables=("daily_bars",),
        cadence=Cadence.TRADING_DAY,
        date_columns={"daily_bars": "trade_date"},
        empty_policy=EmptyPolicy.ALLOW_ON_NON_TRADING_DAY,
        primary_source="akshare",
        display_label="日线行情",
        refresh_policy=RefreshPolicy(
            RefreshKind.REMOTE_KEYED_UPSERT,
            DateStrategy.EXACT_TARGET,
            {"daily_bars": ("ts_code", "trade_date")},
            {"daily_bars": ("ts_code", "trade_date", "open", "high", "low", "close", "volume", "amount")},
            supports_symbols=True,
            minimum_coverage=0.8,
            cache_namespace="daily_bars",
        ),
    ),
    TaskSpec(
        name="update_indicators",
        callable=None,
        tables=("indicators",),
        cadence=Cadence.TRADING_DAY,
        date_columns={"indicators": "trade_date"},
        empty_policy=EmptyPolicy.ALLOW_ON_NON_TRADING_DAY,
        primary_source="akshare",
        display_label="技术指标",
        refresh_policy=RefreshPolicy(
            RefreshKind.DERIVED_RECOMPUTE,
            DateStrategy.EXACT_TARGET,
            {"indicators": ("ts_code", "trade_date")},
            {"indicators": ("ts_code", "trade_date")},
            dependencies=("update_bars",),
            supports_symbols=True,
        ),
    ),
    TaskSpec(
        name="update_fundamentals",
        callable=None,
        tables=("fundamentals",),
        cadence=Cadence.TRADING_DAY,
        date_columns={"fundamentals": "trade_date"},
        empty_policy=EmptyPolicy.ALLOW,
        primary_source="akshare",
        display_label="基本面数据",
        refresh_policy=RefreshPolicy(
            RefreshKind.REMOTE_DATE_SNAPSHOT,
            DateStrategy.EXACT_TARGET,
            {"fundamentals": ("ts_code", "trade_date")},
            {"fundamentals": ("ts_code", "trade_date")},
            supports_symbols=True,
            minimum_coverage=0.8,
        ),
    ),
    TaskSpec(
        name="update_market_snapshot",
        callable=None,
        tables=("fundamentals",),
        cadence=Cadence.TRADING_DAY,
        date_columns={"fundamentals": "trade_date"},
        empty_policy=EmptyPolicy.ALLOW,
        primary_source="xueqiu",
        display_label="行情快照",
        refresh_policy=RefreshPolicy(
            RefreshKind.REMOTE_DATE_SNAPSHOT,
            DateStrategy.EXACT_TARGET,
            {"fundamentals": ("ts_code", "trade_date")},
            {"fundamentals": ("ts_code", "trade_date")},
            dependencies=("update_fundamentals",),
            minimum_coverage=0.4,
        ),
    ),
    TaskSpec(
        name="update_fund_flow",
        callable=None,
        tables=("fund_flow",),
        cadence=Cadence.TRADING_DAY,
        date_columns={"fund_flow": "trade_date"},
        empty_policy=EmptyPolicy.ALLOW,
        primary_source="akshare",
        display_label="资金流向",
        refresh_policy=RefreshPolicy(
            RefreshKind.REMOTE_DATE_SNAPSHOT,
            DateStrategy.EXACT_TARGET,
            {"fund_flow": ("trade_date", "ts_code")},
            {"fund_flow": ("trade_date", "ts_code")},
            supports_symbols=True,
            minimum_coverage=0.8,
        ),
    ),
    TaskSpec(
        name="update_margin_trading",
        callable=None,
        tables=("margin_trading",),
        cadence=Cadence.TRADING_DAY,
        date_columns={"margin_trading": "trade_date"},
        empty_policy=EmptyPolicy.ALLOW,
        primary_source="akshare",
        display_label="融资融券",
        refresh_policy=RefreshPolicy(
            RefreshKind.REMOTE_DATE_SNAPSHOT,
            DateStrategy.LATEST_AVAILABLE_WITHIN_LOOKBACK,
            {"margin_trading": ("trade_date", "ts_code")},
            {"margin_trading": ("trade_date", "ts_code")},
            supports_symbols=True,
            lookback_days=3,
        ),
    ),
    TaskSpec(
        name="update_dragon_tiger",
        callable=None,
        tables=("dragon_tiger",),
        cadence=Cadence.TRADING_DAY,
        date_columns={"dragon_tiger": "trade_date"},
        empty_policy=EmptyPolicy.ALLOW,
        primary_source="akshare",
        display_label="龙虎榜",
        refresh_policy=RefreshPolicy(
            RefreshKind.REMOTE_DATE_SNAPSHOT,
            DateStrategy.EXACT_TARGET,
            {"dragon_tiger": ("source_record_key",)},
            {"dragon_tiger": ("source_record_key", "trade_date", "ts_code")},
            supports_symbols=True,
        ),
    ),
    TaskSpec(
        name="update_block_trade",
        callable=None,
        tables=("block_trade",),
        cadence=Cadence.TRADING_DAY,
        date_columns={"block_trade": "trade_date"},
        empty_policy=EmptyPolicy.ALLOW,
        primary_source="akshare",
        display_label="大宗交易",
        refresh_policy=RefreshPolicy(
            RefreshKind.REMOTE_DATE_SNAPSHOT,
            DateStrategy.EXACT_TARGET,
            {"block_trade": ("source_record_key",)},
            {"block_trade": ("source_record_key", "trade_date", "ts_code")},
            supports_symbols=True,
        ),
    ),
    TaskSpec(
        name="update_sector_fund_flow",
        callable=None,
        tables=("sector_fund_flow",),
        cadence=Cadence.TRADING_DAY,
        date_columns={"sector_fund_flow": "trade_date"},
        empty_policy=EmptyPolicy.ALLOW,
        primary_source="akshare",
        display_label="板块资金",
        refresh_policy=RefreshPolicy(
            RefreshKind.REMOTE_RUN_SNAPSHOT,
            DateStrategy.RUN_SNAPSHOT,
            {"sector_fund_flow": ("trade_date", "sector_name")},
            {"sector_fund_flow": ("trade_date", "sector_name")},
        ),
    ),
    TaskSpec(
        name="update_shareholder_count",
        callable=None,
        tables=("shareholder_count",),
        cadence=Cadence.QUARTERLY,
        date_columns={"shareholder_count": "trade_date"},
        empty_policy=EmptyPolicy.ALLOW,
        primary_source="akshare",
        display_label="股东户数",
    ),
    TaskSpec(
        name="update_quarterly_financials",
        callable=None,
        tables=("quarterly_financials",),
        cadence=Cadence.QUARTERLY,
        date_columns={"quarterly_financials": "end_date"},
        empty_policy=EmptyPolicy.ALLOW,
        primary_source="akshare",
        display_label="季度财务",
    ),
    TaskSpec(
        name="update_financial_history",
        callable=None,
        tables=("quarterly_financials_history",),
        cadence=Cadence.DAILY,  # 自动发现缺失报告期，已覆盖时秒级跳过
        date_columns={"quarterly_financials_history": "publish_date"},
        empty_policy=EmptyPolicy.ALLOW,
        primary_source="akshare",
        display_label="财务历史",
    ),
    TaskSpec(
        name="update_historical_valuation",
        callable=None,
        tables=("historical_valuation",),
        cadence=Cadence.TRADING_DAY,
        date_columns={"historical_valuation": "trade_date"},
        empty_policy=EmptyPolicy.ALLOW,
        primary_source="akshare",
        display_label="历史估值",
        refresh_policy=RefreshPolicy(
            RefreshKind.DERIVED_RECOMPUTE,
            DateStrategy.EXACT_TARGET,
            {"historical_valuation": ("ts_code", "trade_date")},
            {"historical_valuation": ("ts_code", "trade_date")},
            dependencies=("update_fundamentals", "update_market_snapshot"),
            supports_symbols=True,
        ),
    ),
    TaskSpec(
        name="update_sector_industry",
        callable=None,
        tables=("sector_industry",),
        cadence=Cadence.TRADING_DAY,
        date_columns={"sector_industry": "trade_date"},
        empty_policy=EmptyPolicy.ALLOW,
        primary_source="akshare",
        display_label="行业对比",
        refresh_policy=RefreshPolicy(
            RefreshKind.DERIVED_RECOMPUTE,
            DateStrategy.EXACT_TARGET,
            {"sector_industry": ("trade_date", "industry_name")},
            {"sector_industry": ("trade_date", "industry_name")},
            dependencies=("update_fundamentals",),
        ),
    ),
    TaskSpec(
        name="update_industry",
        callable=None,
        tables=("industry",),
        cadence=Cadence.MONTHLY,
        date_columns={},
        empty_policy=EmptyPolicy.ALLOW,
        primary_source="akshare",
        display_label="行业分类",
    ),
    # ── Cross-market ──────────────────────────────────────────────
    # update_north_flow 已下线：北向逐日资金流交易所自 2024-08 停止披露，
    # 任务/监控/收盘刷新全部移除；north_flow 表保留在库中（空表无害，
    # 若未来恢复披露可重新接入）。北向持仓 (update_north_hold) 不受影响。
    TaskSpec(
        name="update_north_hold",
        callable=None,
        tables=("north_hold",),
        cadence=Cadence.QUARTERLY,
        date_columns={"north_hold": "trade_date"},
        empty_policy=EmptyPolicy.ALLOW,
        primary_source="akshare",
        display_label="北向持仓",
    ),
    TaskSpec(
        name="update_south_flow",
        callable=None,
        tables=("south_flow",),
        cadence=Cadence.TRADING_DAY,
        date_columns={"south_flow": "trade_date"},
        empty_policy=EmptyPolicy.ALLOW,
        primary_source="akshare",
        display_label="南向资金",
        refresh_policy=RefreshPolicy(
            RefreshKind.REMOTE_DATE_SNAPSHOT,
            DateStrategy.LATEST_AVAILABLE_WITHIN_LOOKBACK,
            {"south_flow": ("trade_date", "market")},
            {"south_flow": ("trade_date", "market")},
            lookback_days=3,
        ),
    ),
    TaskSpec(
        name="update_ah_premium",
        callable=None,
        tables=("ah_premium",),
        cadence=Cadence.TRADING_DAY,
        date_columns={"ah_premium": "trade_date"},
        empty_policy=EmptyPolicy.ALLOW,
        primary_source="akshare",
        display_label="AH溢价",
        refresh_policy=RefreshPolicy(
            RefreshKind.REMOTE_RUN_SNAPSHOT,
            DateStrategy.RUN_SNAPSHOT,
            {"ah_premium": ("trade_date", "ts_code")},
            {"ah_premium": ("trade_date", "ts_code")},
        ),
    ),
    TaskSpec(
        name="update_etf_daily",
        callable=None,
        tables=("etf_daily",),
        cadence=Cadence.TRADING_DAY,
        date_columns={"etf_daily": "trade_date"},
        empty_policy=EmptyPolicy.ALLOW,
        primary_source="akshare",
        display_label="ETF日线",
        refresh_policy=RefreshPolicy(
            RefreshKind.REMOTE_DATE_SNAPSHOT,
            DateStrategy.EXACT_TARGET,
            {"etf_daily": ("ts_code", "trade_date")},
            {"etf_daily": ("ts_code", "trade_date", "close")},
            minimum_coverage=0.8,
        ),
    ),
    TaskSpec(
        name="update_index_daily",
        callable=None,
        tables=("index_daily",),
        cadence=Cadence.TRADING_DAY,
        date_columns={"index_daily": "trade_date"},
        empty_policy=EmptyPolicy.ALLOW,
        primary_source="akshare",
        display_label="指数日线",
        refresh_policy=RefreshPolicy(
            RefreshKind.REMOTE_DATE_SNAPSHOT,
            DateStrategy.LATEST_AVAILABLE_WITHIN_LOOKBACK,
            {"index_daily": ("trade_date", "index_code")},
            {"index_daily": ("trade_date", "index_code", "close")},
            lookback_days=3,
        ),
    ),
    # ── Convertible bonds ──────────────────────────────────────────
    TaskSpec(
        name="update_cb_quotation",
        callable=None,
        tables=("cb_quotation",),
        cadence=Cadence.TRADING_DAY,
        date_columns={"cb_quotation": "updated_at"},
        empty_policy=EmptyPolicy.ALLOW,
        primary_source="akshare",
        display_label="可转债行情",
        refresh_policy=RefreshPolicy(
            RefreshKind.REMOTE_RUN_SNAPSHOT,
            DateStrategy.RUN_SNAPSHOT,
            {"cb_quotation": ("ts_code",)},
            {"cb_quotation": ("ts_code", "price")},
        ),
    ),
    TaskSpec(
        name="update_cb_redeem",
        callable=None,
        tables=("cb_redeem",),
        cadence=Cadence.TRADING_DAY,
        date_columns={"cb_redeem": "updated_at"},
        empty_policy=EmptyPolicy.ALLOW,
        primary_source="akshare",
        display_label="可转债强赎",
        refresh_policy=RefreshPolicy(
            RefreshKind.REMOTE_RUN_SNAPSHOT,
            DateStrategy.RUN_SNAPSHOT,
            {"cb_redeem": ("ts_code",)},
            {"cb_redeem": ("ts_code", "redeem_flag")},
        ),
    ),
    TaskSpec(
        name="update_cb_index",
        callable=None,
        tables=("cb_index",),
        cadence=Cadence.TRADING_DAY,
        date_columns={"cb_index": "trade_date"},
        empty_policy=EmptyPolicy.ALLOW,
        primary_source="akshare",
        display_label="可转债指数",
        refresh_policy=RefreshPolicy(
            RefreshKind.REMOTE_DATE_SNAPSHOT,
            DateStrategy.LATEST_AVAILABLE_WITHIN_LOOKBACK,
            {"cb_index": ("trade_date", "index_code")},
            {"cb_index": ("trade_date", "index_code", "close")},
            lookback_days=3,
        ),
    ),
    # ── Corporate actions ──────────────────────────────────────────
    TaskSpec(
        name="update_restricted_share",
        callable=None,
        tables=("restricted_share",),
        cadence=Cadence.DAILY,
        date_columns={"restricted_share": "release_date"},
        empty_policy=EmptyPolicy.ALLOW,
        primary_source="akshare",
        display_label="限售解禁",
    ),
    TaskSpec(
        name="update_earnings_forecast",
        callable=None,
        tables=("earnings_forecast",),
        cadence=Cadence.DAILY,
        date_columns={"earnings_forecast": "end_date"},
        empty_policy=EmptyPolicy.ALLOW,
        primary_source="akshare",
        display_label="业绩预告",
    ),
    TaskSpec(
        name="update_limit_up_down",
        callable=None,
        tables=("limit_up_down",),
        cadence=Cadence.TRADING_DAY,
        date_columns={"limit_up_down": "trade_date"},
        empty_policy=EmptyPolicy.ALLOW,
        primary_source="akshare",
        display_label="涨跌停",
        refresh_policy=RefreshPolicy(
            RefreshKind.REMOTE_DATE_SNAPSHOT,
            DateStrategy.EXACT_TARGET,
            {"limit_up_down": ("trade_date", "ts_code")},
            {"limit_up_down": ("trade_date", "ts_code")},
        ),
    ),
    TaskSpec(
        name="update_dividend_summary",
        callable=None,
        tables=("dividend_summary",),
        cadence=Cadence.DAILY,
        date_columns={"dividend_summary": "trade_date"},
        empty_policy=EmptyPolicy.ALLOW,
        primary_source="akshare",
        display_label="分红送转",
    ),
    # ── Macro & global ─────────────────────────────────────────────
    TaskSpec(
        name="update_china_macro",
        callable=None,
        # macro_daily 已由 money_market 表接管（update_money_market），
        # 本任务只产出月度/季度，保留 macro_daily 会让健康面板盯一张废表
        tables=("macro_monthly", "macro_quarterly"),
        cadence=Cadence.MONTHLY,
        date_columns={
            "macro_monthly": "date",
            "macro_quarterly": "date",
        },
        empty_policy=EmptyPolicy.ALLOW,
        primary_source="akshare",
        display_label="中国宏观",
    ),
    TaskSpec(
        name="update_money_market",
        callable=None,
        tables=("money_market",),
        cadence=Cadence.DAILY,
        date_columns={"money_market": "date"},
        empty_policy=EmptyPolicy.ALLOW,
        primary_source="akshare",
        display_label="货币市场",
    ),
    TaskSpec(
        name="update_gold_price",
        callable=None,
        tables=("gold_price",),
        cadence=Cadence.DAILY,
        date_columns={"gold_price": "trade_date"},
        empty_policy=EmptyPolicy.ALLOW,
        primary_source="akshare",
        display_label="黄金价格",
    ),
    TaskSpec(
        name="update_crude_oil",
        callable=None,
        tables=("crude_oil",),
        cadence=Cadence.DAILY,
        date_columns={"crude_oil": "trade_date"},
        empty_policy=EmptyPolicy.ALLOW,
        primary_source="akshare",
        display_label="原油价格",
    ),
    TaskSpec(
        name="update_usd",
        callable=None,
        tables=("fx_rate",),
        cadence=Cadence.DAILY,
        date_columns={"fx_rate": "trade_date"},
        empty_policy=EmptyPolicy.ALLOW,
        primary_source="akshare",
        display_label="外汇汇率",
    ),
    TaskSpec(
        name="update_global_index",
        callable=None,
        tables=("global_index",),
        cadence=Cadence.DAILY,
        date_columns={"global_index": "trade_date"},
        empty_policy=EmptyPolicy.ALLOW,
        primary_source="akshare",
        display_label="全球指数",
    ),
    TaskSpec(
        name="update_us_treasury",
        callable=None,
        tables=("us_treasury",),
        cadence=Cadence.DAILY,
        date_columns={"us_treasury": "trade_date"},
        empty_policy=EmptyPolicy.ALLOW,
        primary_source="akshare",
        display_label="国债收益率",
    ),
    TaskSpec(
        name="update_futures",
        callable=None,
        tables=("futures_daily",),
        cadence=Cadence.DAILY,
        date_columns={"futures_daily": "trade_date"},
        empty_policy=EmptyPolicy.ALLOW,
        primary_source="akshare",
        display_label="期货日线",
    ),
    # ── Concept & sector ───────────────────────────────────────────
    TaskSpec(
        name="update_concept_board",
        callable=None,
        tables=("concept_board",),
        cadence=Cadence.TRADING_DAY,
        date_columns={"concept_board": "trade_date"},
        empty_policy=EmptyPolicy.ALLOW,
        primary_source="akshare",
        display_label="概念板块",
        refresh_policy=RefreshPolicy(
            RefreshKind.REMOTE_RUN_SNAPSHOT,
            DateStrategy.RUN_SNAPSHOT,
            {"concept_board": ("trade_date", "concept_code")},
            {"concept_board": ("trade_date", "concept_code")},
        ),
    ),
    TaskSpec(
        name="update_concept_member",
        callable=None,
        tables=("concept_member",),
        cadence=Cadence.MONTHLY,
        date_columns={"concept_member": "updated_at"},
        empty_policy=EmptyPolicy.ALLOW,
        primary_source="akshare",
        display_label="概念成分",
    ),
    TaskSpec(
        name="update_index_membership",
        callable=None,
        tables=("index_member_history",),
        cadence=Cadence.WEEKLY,  # PIT 快照按 interval 模型管理，无须每日全量
        date_columns={"index_member_history": "valid_from"},
        empty_policy=EmptyPolicy.ALLOW,
        primary_source="akshare",
        display_label="指数成分",
    ),
    # ── Market valuation ───────────────────────────────────────────
    TaskSpec(
        name="update_market_valuation",
        callable=None,
        tables=("market_valuation",),
        cadence=Cadence.TRADING_DAY,
        date_columns={"market_valuation": "date"},
        empty_policy=EmptyPolicy.ALLOW,
        primary_source="akshare",
        display_label="大盘估值",
        refresh_policy=RefreshPolicy(
            RefreshKind.REMOTE_DATE_SNAPSHOT,
            DateStrategy.LATEST_AVAILABLE_WITHIN_LOOKBACK,
            {"market_valuation": ("date",)},
            {"market_valuation": ("date", "data_source")},
            lookback_days=3,
        ),
    ),
    # ── Sector derivatives ─────────────────────────────────────────
    TaskSpec(
        name="update_sector_derivatives",
        callable=None,
        tables=("sector_daily", "sector_valuation", "index_futures_basis"),
        cadence=Cadence.TRADING_DAY,
        date_columns={
            "sector_daily": "trade_date",
            "sector_valuation": "trade_date",
            "index_futures_basis": "trade_date",
        },
        empty_policy=EmptyPolicy.ALLOW,
        primary_source="akshare",
        display_label="行业衍生",
        refresh_policy=RefreshPolicy(
            RefreshKind.COMPOSITE_ATOMIC,
            DateStrategy.EXACT_TARGET,
            {
                "sector_daily": ("sector_name", "trade_date"),
                "sector_valuation": ("sector_name", "trade_date"),
                "index_futures_basis": ("trade_date", "futures_code"),
            },
            {
                "sector_daily": ("sector_name", "trade_date", "close"),
                "sector_valuation": ("sector_name", "trade_date"),
                "index_futures_basis": ("trade_date", "futures_code", "basis"),
            },
        ),
    ),
    # ── Option sentiment (Phase 2) ─────────────────────────────────
    TaskSpec(
        name="update_option_sentiment",
        callable=None,
        tables=("option_sentiment",),
        cadence=Cadence.TRADING_DAY,
        date_columns={"option_sentiment": "trade_date"},
        empty_policy=EmptyPolicy.ALLOW,
        primary_source="akshare",
        display_label="期权情绪",
        refresh_policy=RefreshPolicy(
            RefreshKind.REMOTE_DATE_SNAPSHOT,
            DateStrategy.EXACT_TARGET,
            {"option_sentiment": ("trade_date",)},
            {"option_sentiment": ("trade_date",)},
        ),
    ),
    # ── Phase-2 event signals ──────────────────────────────────────
    TaskSpec(
        name="update_stock_repurchase",
        callable=None,
        tables=("stock_repurchase",),
        cadence=Cadence.TRADING_DAY,
        date_columns={"stock_repurchase": "trade_date"},
        empty_policy=EmptyPolicy.ALLOW,
        primary_source="akshare",
        display_label="股票回购",
        refresh_policy=RefreshPolicy(
            RefreshKind.REMOTE_KEYED_UPSERT,
            DateStrategy.RUN_SNAPSHOT,
            {"stock_repurchase": ("source_record_key",)},
            {"stock_repurchase": ("source_record_key", "trade_date", "stock_code")},
        ),
    ),
    TaskSpec(
        name="update_institution_survey",
        callable=None,
        tables=("institution_survey",),
        cadence=Cadence.TRADING_DAY,
        date_columns={"institution_survey": "trade_date"},
        empty_policy=EmptyPolicy.ALLOW,
        primary_source="akshare",
        display_label="机构调研",
        refresh_policy=RefreshPolicy(
            RefreshKind.REMOTE_KEYED_UPSERT,
            DateStrategy.LATEST_AVAILABLE_WITHIN_LOOKBACK,
            {"institution_survey": ("source_record_key",)},
            {"institution_survey": ("source_record_key", "trade_date", "stock_code")},
            lookback_days=30,
        ),
    ),
    TaskSpec(
        name="update_stock_pledge",
        callable=None,
        tables=("stock_pledge",),
        # 中登每周五更新质押比例；cadence 维持 TRADING_DAY 以留在收盘刷新
        # 策略矩阵内（LATEST_AVAILABLE_WITHIN_LOOKBACK 已兼容周度发布），
        # 新鲜度展示由 TUI 的 WEEKLY_TABLES 单独处理
        cadence=Cadence.TRADING_DAY,
        date_columns={"stock_pledge": "trade_date"},
        empty_policy=EmptyPolicy.ALLOW,
        primary_source="akshare",
        display_label="股权质押",
        refresh_policy=RefreshPolicy(
            RefreshKind.REMOTE_KEYED_UPSERT,
            DateStrategy.LATEST_AVAILABLE_WITHIN_LOOKBACK,
            {"stock_pledge": ("source_record_key",)},
            {"stock_pledge": ("source_record_key", "trade_date", "stock_code")},
            lookback_days=30,
        ),
    ),
    # ── Chip distribution ──────────────────────────────────────────
    TaskSpec(
        name="update_chip_distribution",
        callable=None,
        tables=("chip_distribution",),
        cadence=Cadence.TRADING_DAY,
        date_columns={"chip_distribution": "trade_date"},
        empty_policy=EmptyPolicy.ALLOW,
        primary_source="akshare",
        display_label="筹码分布",
        refresh_policy=RefreshPolicy(
            RefreshKind.DERIVED_RECOMPUTE,
            DateStrategy.EXACT_TARGET,
            {"chip_distribution": ("ts_code", "trade_date")},
            {"chip_distribution": ("ts_code", "trade_date")},
            dependencies=("update_bars",),
            supports_symbols=True,
        ),
    ),
    TaskSpec(
        name="update_chip_distribution_em",
        callable=None,
        tables=("chip_distribution_em",),
        cadence=Cadence.TRADING_DAY,
        date_columns={"chip_distribution_em": "trade_date"},
        empty_policy=EmptyPolicy.ALLOW,
        primary_source="akshare",
        display_label="筹码分布线上",
        refresh_policy=RefreshPolicy(
            RefreshKind.DERIVED_RECOMPUTE,
            DateStrategy.EXACT_TARGET,
            {"chip_distribution_em": ("ts_code", "trade_date")},
            {"chip_distribution_em": ("ts_code", "trade_date")},
            dependencies=("update_bars",),
            supports_symbols=True,
        ),
    ),
    TaskSpec(
        name="update_chip_distribution_em_fullmarket",
        callable=None,
        tables=("chip_distribution_em",),
        cadence=Cadence.ON_DEMAND,
        date_columns={"chip_distribution_em": "trade_date"},
        empty_policy=EmptyPolicy.ALLOW,
        primary_source="akshare",
        display_label="全市场筹码分布",
    ),
    # ── Utility ────────────────────────────────────────────────────
    TaskSpec(
        name="retry",
        callable=None,
        tables=(),
        cadence=Cadence.ON_DEMAND,
        date_columns={},
        empty_policy=EmptyPolicy.ALLOW,
        primary_source="akshare",
        display_label="重试失败",
    ),
    TaskSpec(
        name="health_check",
        callable=None,
        tables=(),
        cadence=Cadence.ON_DEMAND,
        date_columns={},
        empty_policy=EmptyPolicy.ALLOW,
        primary_source="akshare",
        display_label="健康检查",
    ),
)
# fmt: on


# ── validate on import ─────────────────────────────────────────────────


def _validate_registry() -> None:
    """Check registry invariants at import time."""
    seen_names: set[str] = set()
    for spec in TASK_REGISTRY:
        if spec.name in seen_names:
            raise ValueError(f"duplicate task name in registry: {spec.name}")
        seen_names.add(spec.name)
        if not spec.callable and spec.name != "update_stock_list":
            # callable is None temporarily; daily_pipeline sets it
            pass


_validate_registry()
