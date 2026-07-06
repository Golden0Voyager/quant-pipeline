# quant_pipeline — Agent Knowledge Base

## Overview

A-share quantitative data automation pipeline. Fetches, stores, and maintains daily OHLCV + fundamental data for ~5500 A-share stocks. Uses AkShare (Sina API) as primary data source.

## Key Facts

| Item | Value |
|------|-------|
| Database | `~/Code/quant_data/quant_core.db` (SQLite) |
| Stocks | ~5528 |
| Daily bars | ~7,160,000 rows |
| Date range | 1996-07-02 ~ current |
| Indicators | ~4,290,000 rows |
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
- **Market prefixes**: `6`/`9`→sh, `0`/`2`/`3`→sz, `4`/`8`/`920`→bj
- **Beijing stocks**: Skipped by default (`INCLUDE_BJ=1` to include)
- **Disciplined codebase**: ruff + mypy + pytest CI gate
- **No `as any` / `# type: ignore`** unless absolutely necessary

## TUI Architecture

`tui.py` uses Textual framework with 4 widgets:
- **DashboardWidget**: DB size, stock count, daemon/launchd status
- **OperationsWidget**: Keyboard shortcut reference
- **ProgressWidget**: Pipeline progress bar (reads `progress.json`)
- **LogsWidget**: Real-time log tailing with colorized output

## Backfill Skip List

`scripts/.backfill_skip` contains 1848 stock codes confirmed to have no pre-listing data. Used by `backfill_to_6years.py` to skip permanent failures. Format: one stock code per line.
