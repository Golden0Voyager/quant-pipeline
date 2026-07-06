# quant_pipeline — Project-Level Configuration

## 环境约束

- **包管理器**：`uv` 强制（禁止 `pip` / `python -m pip`）
- **运行脚本**：`uv run python <script>.py`（禁止直接 `python`）

## 项目概述

A 股量化数据自动化抓取管道，AkShare 单源，具备断点续传、守护进程与 TUI 监控面板。

## 目录结构

```
quant_pipeline/
├── daily_pipeline.py       # 主管道（CLI + 全部任务）
├── tui.py                  # Textual TUI 监控面板
├── interface.py            # 抽象接口（Protocol）
├── providers.py            # SmartMoney provider 适配层
├── .env                    # 环境配置（INCLUDE_BJ, LOOKBACK_DAYS, 雪球凭证等）
├── scripts/
│   ├── daemon.py                   # 守护进程（start/stop）
│   ├── backfill_to_6years.py
│   ├── backfill_historical_valuation.py
│   ├── repair_turnover.py
│   ├── check_data_health.py
│   └── .backfill_skip
└── tests/
```

## 核心概念

### 数据源策略
- **AkShare（新浪）**：唯一写入源（`ak.stock_zh_a_daily`）
- **yfinance**：仅作为运行时临时 fallback，永不写入 quant_core.db
- **东方财富**：估值数据、龙虎榜、大宗交易等辅助数据

### 交易所前缀
- `6` / `9` → `sh`（沪市主板 / 科创板）
- `0` / `2` / `3` → `sz`（深市主板 / 中小板 / 创业板）
- `4` / `8` / `920` → `bj`（北交所）
- 北交所股票默认跳过（`INCLUDE_BJ=1` 包含）

### TUI 面板
- 基于 Textual 框架
- 快捷键：R=运行, M=续传, D=启动守护, S=停止守护, H=健康检查, Q=退出
- 自动追踪日志、进度、进程状态

### 断点续传
- `progress.json` 记录处理进度
- `progress.json` 的 `failed_queue` 字段记录失败股票
- 重新运行 `--resume` 自动从断点继续

## 快捷命令（~/.zshrc）

| 命令 | 说明 |
|------|------|
| `quant-data` / `pipe-data` | 启动 TUI 面板 |
| `pipe-run` | 完整更新 |
| `pipe-resume` | 断点续传 |
| `pipe-daemon` | 守护进程 |
| `pipe-stop` | 停止守护进程 |
| `pipe-logs` | 查看日志 |
| `pipe-status` | 实时监控 |

## 数据库

- **路径**: `~/Code/quant_data/quant_core.db`
- **表**: `daily_bars`(716万行), `indicators`(429万行), `fundamentals`, `fund_flow`, `margin_trading`, `dragon_tiger`, `block_trade`, `sector_fund_flow`, `shareholder_count`, `quarterly_financials`, `historical_valuation`, `sector_industry`
- **日期范围**: 1996-07-02 ~ 至今
- **股票数**: ~5528

## CI/CD

- GitHub Actions：ruff → mypy → pytest → coverage
- `uv sync --extra dev` 安装开发依赖

## 编码规范

- 中文界面，用户可见文本使用中文
- 遵循 ruff + mypy 检查
- 使用 `from __future__ import annotations`
- 避免 `# type: ignore`
- commit 使用 conventional commits（`feat:` / `fix:` / `style:` / `refactor:`）
- commit 信息中英双语，英文在前，中文在后
