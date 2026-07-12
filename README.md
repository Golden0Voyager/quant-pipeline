# quant_pipeline

A股量化数据自动化抓取管道，支持 AkShare 数据源，具备断点续传、守护进程与终端监控面板能力。

---

## 快捷命令

在终端中直接使用以下命令（已配置在 `~/.zshrc`）：

| 命令 | 作用 |
|------|------|
| `quant-data` 或 `pipe-data` | 启动终端监控面板（TUI） |
| `pipe-run` | 立即执行完整数据更新 |
| `pipe-resume` | 断点续传，从上次中断处继续 |
| `pipe-daemon` | 启动守护进程（崩溃自动重启 + 自动 resume） |
| `pipe-stop` | 停止守护进程 |
| `pipe-logs` | 查看最近日志 |
| `pipe-status` | 实时监控运行中的进程和日志 |

> **推荐方式**：直接运行 `quant-data` 打开 TUI 监控面板，所有操作（运行、续传、守护进程、健康检查、日志查看）均在面板中通过快捷键完成。

---

## TUI 监控面板

终端界面（基于 Textual），可视化监控管道运行状态：

### 快捷键

| 按键 | 操作 |
|------|------|
| `R` | 立即启动完整数据更新 |
| `M` | 断点续传数据更新 |
| `D` | 启动守护进程 |
| `S` | 停止守护进程 |
| `H` | 进行数据健康检查 |
| `Q` | 退出面板 |

### 看板信息

- **状态看板**：数据库大小、有效股票数、守护进程状态、定时任务状态
- **进度看板**：当前任务名称、完成百分比、进度条、正在处理的股票、失败数量
- **实时日志**：彩色高亮显示系统日志，自动追踪最新日志文件

```bash
quant-data    # 启动 TUI
```

---

## 使用方法

### 1. 单次全量更新

```bash
pipe-run
```

等价于：

```bash
uv run python daily_pipeline.py --task all --force
```

### 2. 断点续传

如果之前中断了（Ctrl+C），下次直接继续：

```bash
pipe-resume
```

已成功的股票数据会保留在数据库和缓存中，不会重复抓取。

### 3. 守护进程模式（推荐长期挂机）

```bash
pipe-daemon
```

特性：
- 异常退出后 **30 秒自动重启**
- 每次重启自动 `--resume`
- 正常完成后休眠 1 小时，再继续
- 日志写入 `~/Code/quant_data/logs/daemon.log`

停止守护进程：

```bash
pipe-stop
```

### 4. 查看日志与监控

```bash
pipe-logs      # 查看最近日志
pipe-status    # 实时监控（按 Ctrl+C 退出）
quant-data     # TUI 面板（可视化监控 + 实时日志）
```

---

## 日线数据回填工具

`scripts/backfill_to_6years.py` — 将 A 股日线数据统一回填至 6 年（对上市不足 6 年的新股只取全部历史）。

- 使用 `ak.stock_zh_a_daily`（新浪 API）直调
- 自动识别 sh / sz / bj 交易所前缀
- 手动计算 `pct_change` / `amplitude`
- 空数据不重试，自动记录跳过列表（`scripts/.backfill_skip`）
- 支持分批处理（`--batch-size`）

```bash
uv run python scripts/backfill_to_6years.py --batch-size 500
```

结果：5528 只股票，716 万条日线，日期范围 1996-07-02 ~ 2026-07-06。

---

## 环境变量

配置方式：创建 `.env` 文件（项目根目录），或直接在 shell 中 `export`。

```
# .env 文件示例
INCLUDE_BJ=1              # 包含北交所股票（默认跳过）
LOOKBACK_DAYS=2190        # 全量拉取回溯天数（默认 2190 ≈ 6 年）
XUEQIU_TOKEN=xxx          # 雪球 API Token（用于获取股息率）
XUEQIU_USER_ID=xxx        # 雪球用户 ID
# ULTRA_SAFE=1            # 极致稳定模式（更小批次、更长间隔）
```

> `.env` 文件不会覆盖已存在的环境变量，shell 中 `export` 的优先级更高。
> `.env` 已加入 `.gitignore`。

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `INCLUDE_BJ` | `0` | 设为 `1` 包含北交所股票 |
| `LOOKBACK_DAYS` | `2190` | 全量拉取回溯天数（约 1500+ 个交易日） |
| `DISABLE_YFINANCE_FALLBACK` | `1` | 禁用 yfinance 备用源 |
| `ULTRA_SAFE` | `0` | 极致稳定模式（更小批次、更长间隔） |
| `XUEQIU_TOKEN` | — | 雪球 API Token（`update_market_snapshot` 任务需要） |
| `XUEQIU_USER_ID` | — | 雪球用户 ID |

---

## 🔌 数据中台与项目协同 (Data Hub & Project Integration)

本项目在整个量化系统中扮演着**数据提供者与维护者**的角色，主要与 `quant_data` 中台和 `quant_agents` 进行协同：

### 1. 核心写入中台
* 本项目通过 [providers.py](file:///Users/hainingyu/Code/quant_pipeline/providers.py) 模块作为 Adapter 依赖 `quant_hunter` 的数据库 Schema 定义，向 `~/Code/quant_data/quant_core.db` 核心库中写入 20+ 个表的数据。
* 每天下午 **18:00** 启动全量或增量抓取，同时负责对 `quant_core.db` 中的错误、重影数据进行清洗与 reconcile 修复 (`reconcile_with_akshare.py`)。

### 2. 跨项目 Watchlist 同步
* 本项目内置了自动同步模块，可通过 TUI 运行或在后台将位于 `~/Code/quant_agents/watchlists/` 目录下的所有自选股 `.txt` 文本文件中的股票代码自动提取、格式化（添加 `.SH`/`.SZ`/`.BJ` 后缀）并导入到共享数据库的 `watchlist` 表中，以此实现多 Agent 决策标的与数据管道的无缝结合。

### 3. 数据与日志目录
* **主数据库**：`~/Code/quant_data/quant_core.db` (行情、衍生因子、宏观数据)
* **缓存数据库**：`~/Code/quant_data/quant_cache.db` (网络 API 响应缓存)
* **系统运行日志**：`~/Code/quant_data/logs/` (守护进程日志、错误日志)

---


## 注意事项

- 工作日 9:00~15:00 之间运行全量管道（`R` 键 / `--task all`）会自动跳过，防止盘中半成品数据污染数据库
- `--task update_bars` 或断点续传（`M` 键）不走此检查，盘中强制使用需自行确认数据完整性
- 全量抓取约 5500 只股票约需 **10 ~ 15 小时**（受网络状况影响）
- AkShare 服务端对请求频率敏感，失败率较高时可启用 `ULTRA_SAFE=1`
- 守护进程模式下无需人工盯盘，适合夜间挂机
- TUI 面板依赖 Textual，首次运行前确保已安装：`uv sync`
