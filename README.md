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
| `U` | 收盘刷新（启动 `daily_pipeline.py --refresh-today`，可选输入股票列表） |
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

### 5. 收盘刷新（`--refresh-today`）

收盘后用官方收盘数据整体替换当日盘中抓取的临时数据，覆盖全部 29 个交易日任务，按依赖顺序执行：

```bash
rtk uv run python daily_pipeline.py --refresh-today
rtk uv run python daily_pipeline.py --refresh-today --symbols 000001.SZ,600000.SH
```

- **16:00 时间闸门**：北京时间（Asia/Shanghai）16:00 之前运行会被直接拦截，不产生任何写入；确需提前运行时加 `--force`（仅解除时间闸门，数据校验照常执行）。
- **旧数据保留**：任一任务源端失败或校验失败时，自动重试一次；仍失败则放弃发布，该任务旧数据完整保留，任务标记为 failed/degraded，审计元数据带 `retained_old_data=true`。失败任务的下游任务会被阻断，同样保留旧数据。
- **运行记录**：每次刷新在主数据库写入一行 `refresh_runs`（run 级别）和每任务一行 `refresh_task_runs`（状态、fetched/validated/replaced/retained/failed 计数与元数据 JSON），可随时回查。
- **失败队列**：单只股票抓取失败进入 `failed_symbols` 队列并保留旧行；派生任务（指标、筹码分布）只重算日线实际变化的股票，失败股票不重算。
- **回滚行为**：所有写入先进 staging 校验，再在单个事务内替换目标日分区；复合任务（如 `update_sector_derivatives` 的三张表）任一组件失败则整体回滚，所有表保持原样。
- **退出码**：任一任务 degraded/failed 时进程以非零码退出，便于脚本与定时任务判断。
- **跨源抽样校验（opt-in，默认关闭）**：`REFRESH_CROSS_SOURCE=1` 开启后，刷新完成时抽样对比雪球日线（只读，绝不用于写入；价按 qfq 对齐，成交量按手/股约定归一，北交所不参与）。**开启即先进入观察态**：`REFRESH_CROSS_SOURCE_REPORT_ONLY` 默认 `1`，命中不一致只记录/告警、绝不降级；确认容差后设 `0` 才真降级。审计元数据 `cross_source` 区分 `mismatched`（真不一致）、`unverifiable`（雪球当天无数据，如停牌）与 `reference_dead`（整体零命中）。相关变量：`REFRESH_CROSS_SOURCE_TASK`（默认 `update_bars`）、`REFRESH_CROSS_SOURCE_SAMPLE_SIZE`（默认 30）、`REFRESH_CROSS_SOURCE_PRICE_TOL`/`REFRESH_CROSS_SOURCE_VOLUME_TOL`（默认 0.005/0.05，临时值，须用真实数据调参）。启用与调参三步流程见 [docs/runbooks/cross-source-verification-rollout.md](docs/runbooks/cross-source-verification-rollout.md)。
- **TUI 入口**：面板中按 `U`（Close Refresh）确认后即启动收盘刷新。

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
| `QUANT_WAL_SIZE_LIMIT_MB` | `64` | WAL 文件（`quant_core.db-wal`）体积上限，单位 MiB；每个写连接建立时生效 |

> WAL 文件是**只涨不缩**的高水位线（曾被单个大事务撑到 2.39 GiB），仓库用「体积上限 + 收尾显式截断」两者互补兜住它。机制、观测与 `QUANT_WAL_SIZE_LIMIT_MB` 调参见 [docs/runbooks/wal-size-and-reclamation.md](docs/runbooks/wal-size-and-reclamation.md)。

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
