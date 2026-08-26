# quant_pipeline — Agent Knowledge Base

## Overview

A-share quantitative data automation pipeline. Fetches, stores, and maintains daily OHLCV + fundamental data for ~5500 A-share stocks. Uses AkShare (Sina API) as primary data source.

## Key Facts

| Item | Value |
|------|-------|
| Database | `~/Code/quant_data/quant_core.db` (SQLite) |
| Stocks | ~5,530 |
| Daily bars | ~7,237,000 rows |
| Date range | 1996-07-02 ~ current |
| Indicators | ~7,236,000 rows |
| Python | `uv`-managed, `.venv/` in project root |
| TUI | Textual-based (`tui.py`) |

## Architecture

```
quant_pipeline/
├── daily_pipeline.py      # Main pipeline: tasks & CLI entry
├── tui.py                 # Textual TUI monitor panel
├── interface.py           # Abstract interfaces (Protocol classes)
├── providers.py           # SmartMoney provider implementations
│
├── scripts/
│   ├── daemon.py                    # Daemon (start/stop loop)
│   ├── backfill_to_6years.py        # 6-year backfill tool
│   ├── backfill_historical_valuation.py  # Valuation backfill
│   ├── repair_turnover.py           # turnover_rate repair
│   ├── check_data_health.py         # Data health check
│   └── .backfill_skip               # Skip list (1848 stocks)
│
└── tests/                 # pytest tests
```

## Task System (`daily_pipeline.py --task <name>`)

| Task | Description |
|------|-------------|
| `all` | Run full pipeline (all tasks) |
| `update_stock_list` | Refresh stock list from AkShare |
| `update_bars` | Incremental daily bar update (with resume support) |
| `update_indicators` | Calculate technical indicators (macd, rsi, etc.) |
| `update_fundamentals` | Fetch valuation data from Eastmoney |
| `update_market_snapshot` | Xueqiu real-time snapshot (dividend_yield) |
| `update_fund_flow` | Capital flow data |
| `update_margin_trading` | Margin trading data |
| `update_dragon_tiger` | Dragon & tiger list data |
| `update_block_trade` | Block trade data |
| `update_sector_fund_flow` | Sector fund flow |
| `update_shareholder_count` | Quarterly shareholder count |
| `update_quarterly_financials` | Quarterly financial reports |
| `update_historical_valuation` | Valuation snapshot for percentile calc |
| `update_sector_industry` | Industry comparison data |
| `update_industry` | Industry classification (F10 API) |
| `retry` | Retry failed stocks |
| `health_check` | Database health check |

## Commands

- `quant-data` / `pipe-data` — Launch TUI panel
- `uv run python daily_pipeline.py --task <name> [--force] [--resume]` — Direct CLI
- `uv run python scripts/daemon.py [start|stop]` — Daemon management (TUI D/S keys)

## Close Refresh (`--refresh-today`)

Post-close mode that replaces intraday rows for the current trading day with authoritative close data. Covers the 29 refreshable trading-day tasks (`core/task_registry.refreshable_trading_tasks()`), executed in dependency order by `core/refresh.py` (orchestrator) through `core/refresh_store.py` (atomic staging store) and `core/refresh_adapters.py` (per-task adapters).

```bash
rtk uv run python daily_pipeline.py --refresh-today
rtk uv run python daily_pipeline.py --refresh-today --symbols 000001.SZ,600000.SH
```

| Behavior | Detail |
|----------|--------|
| 16:00 gate | Runs before 16:00 Asia/Shanghai are rejected before any write; `--force` maps to `allow_pre_close=True` and lifts only the time gate (data validation still applies) |
| Retained-old-data | A source or validation failure gets one retry; if it still fails, nothing is published — old rows stay untouched, the task ends `failed`/`degraded` with `retained_old_data=true` in its audit metadata, and dependents of a failed task are blocked (also retaining old data) |
| Run records | Each run persists one `refresh_runs` row plus one `refresh_task_runs` row per task (status, fetched/validated/replaced/retained/failed counters, metadata JSON) |
| Failure queue | Per-symbol fetch failures go to `failed_symbols` and keep their old rows; derived tasks (indicators, chip distribution) recompute only symbols whose bars actually changed |
| Rollback | Every publish goes through staging + one transaction (date-partition replace / keyed upsert / run snapshot); composite tasks (`update_sector_derivatives`: `sector_daily`, `sector_valuation`, `index_futures_basis`) commit or roll back all tables together |
| Exit code | Any degraded/failed/aborted task makes the CLI exit nonzero |
| Cross-source check | Opt-in, OFF by default. `REFRESH_CROSS_SOURCE=1` enables a read-only sampled comparison against Xueqiu daily bars (never used for writes; price aligned on qfq, volume normalized by the lot/share convention, Beijing excluded). **Enabling it starts in observe mode**: `REFRESH_CROSS_SOURCE_REPORT_ONLY` defaults to `1` (records/warns disagreements, never degrades); set `0` to enforce after tolerances are validated. Audit metadata `cross_source` splits `mismatched` (real disagreement), `unverifiable` (Xueqiu had no data that day, e.g. suspended) and `reference_dead` (zero hits overall). Tuning knobs: `REFRESH_CROSS_SOURCE_TASK` (default `update_bars`), `REFRESH_CROSS_SOURCE_SAMPLE_SIZE` (default 30), `REFRESH_CROSS_SOURCE_PRICE_TOL`/`REFRESH_CROSS_SOURCE_VOLUME_TOL` (defaults 0.005/0.05 — provisional, must be tuned on real data). Enable/tune 3-step flow: docs/runbooks/cross-source-verification-rollout.md |
| TUI | `u` key ("Close Refresh") launches `daily_pipeline.py --refresh-today` after confirmation |

## Data Flow

```
AkShare (Sina) ──► daily_pipeline.py ──► quant_core.db
                     │
                     ├── stock_list         (stock metadata)
                     ├── daily_bars         (OHLCV)
                     ├── indicators         (technical indicators)
                     ├── fundamentals       (PE/PB/market cap)
                     ├── fund_flow          (capital flow)
                     ├── margin_trading     (margin data)
                     ├── dragon_tiger       (LHB data)
                     ├── block_trade        (block trades)
                     ├── sector_fund_flow   (sector flows)
                     ├── shareholder_count  (holder count)
                     ├── quarterly_financials (financial reports)
                     ├── historical_valuation (PE/PB history)
                     └── sector_industry    (industry aggregates)
```

## Key Conventions

- **Package manager**: `uv` only (no pip)
- **Run scripts**: `uv run python <script>.py`
- **Data source priority**: AkShare > silence (no yfinance fallback for writes)
- **HiThink fallback**: 同花顺官方 API（`core/source_hithink.py`，env `HITHINK_FINANCE_API_KEY`）作为日线最后兜底源，由 `providers.SmartMoneyLoaderProvider` 在 DataLoader 全链失败后触发，写库 `data_source='hithink'`；北交所仅支持 920 前缀。历史回补可用 `scripts/backfill_from_hithink_dump.py`（market-dumps Parquet 批量灌库）
- **Market prefixes**: `6`/`9`→sh, `0`/`2`/`3`→sz, `4`/`8`/`920`→bj
- **Beijing stocks**: Skipped by default (`INCLUDE_BJ=1` to include)
- **Disciplined codebase**: ruff + mypy + pytest CI gate
- **No `as any` / `# type: ignore`** unless absolutely necessary
- **Git workflow**: 开发新功能 (new feature) 时，建议走 `/git-feature` 流程（使用 `/git-feature start` 创建分支，完成开发后使用 `/git-feature done` 完成推送/PR/合入/清理全流程）。

## TUI Architecture

`tui.py` uses Textual framework with 4 widgets:
- **DashboardWidget**: DB size, stock count, daemon/launchd status
- **OperationsWidget**: Keyboard shortcut reference
- **ProgressWidget**: Pipeline progress bar (reads `progress.json`)
- **LogsWidget**: Real-time log tailing with colorized output

## Backfill Skip List

`scripts/.backfill_skip` contains 1848 stock codes confirmed to have no pre-listing data. Used by `backfill_to_6years.py` to skip permanent failures. Format: one stock code per line.
