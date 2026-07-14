# quant_pipeline 管线升级方案

> **For agentic workers:** 使用 `subagent-driven-development` 或 `executing-plans` 按任务分步执行。

**目标**：将 3671 行的 God Object 拆解为核心模块化架构，引入并行调度、APScheduler 定时任务、通知推送、TUI 增强，并加固 CI

**架构方向**：
```
daily_pipeline.py (CLI Orchestration, ~200 lines)
├── core/             — config, lock, monitor, progress, context, runner, scheduler, calendar, notifications
├── tasks/            — bars, core_chain, index_chain, valuation_chain, market_flow, financials, macro, utility
├── interface.py      — cleaned Protocol (dead methods removed)
├── providers.py      — adapter (minor changes)
├── scripts/          — daemon.py (APScheduler rewrite)
└── tui.py            — enhanced widgets
```

**执行策略**：10 波并行，每波完成后管线可运行，不破坏现有功能

---

## Global Constraints

- 中文 UI，用户可见文本用中文；代码注释/CLI docs 英文
- `from __future__ import annotations`
- 避免 `# type: ignore`
- `uv` 包管理器，禁止 pip
- ruff line-length=120，target py312
- mypy 必须通过（CI 中移除 `|| true`）
- coverage fail_under=84
- TUI 使用 Textual 框架
- SQLite 数据库，路径 `~/Code/quant_data/quant_core.db`

---

## 前置确认

在开始执行前，请确认以下默认选项（如有不同可调整）：

1. **调度库**：APScheduler + CronTrigger
2. **交易日历**：`ak.tool_trade_date_hist_sina()` 缓存
3. **守护运行时间**：每周一~五 16:00 上海时区，自动跳过非交易日
4. **并行策略**：独立任务用 ThreadPoolExecutor，默认 max_workers=min(8, 独立任务数)，通过 PARALLEL_WORKERS 环境变量控制
5. **连接安全**：每个并行任务使用独立的 DatabaseInterface 实例（避免 SQLite 连接共享）
6. **通知通道**：Console + Webhook（飞书/钉钉通用文本格式）
7. **通知级别**：error → 飞书/钉钉推送，warning/info → 仅日志
8. **覆盖率阈值**：保持 fail_under=84
9. **分支名**：`feat/pipeline-modular-upgrade`

---

## 依赖关系图

```
Wave 1 (无依赖):
  P1-T1 core/config.py
  P1-T2 core/lock.py
  P1-T3 core/monitor.py
  P1-T4 core/progress.py
  P1-T5 core/utils.py
  P1-T17 interface.py 清理

Wave 2 (依赖 Wave 1):
  P1-T6 core/context.py      ← P1-T1
  P1-T7 core/runner.py       ← P1-T6

Wave 3 (依赖 Wave 2):
  P1-T8  tasks/bars.py           ← P1-T6, P1-T4, P1-T3
  P1-T10 tasks/index_chain.py    ← P1-T6
  P1-T11 tasks/valuation_chain.py ← P1-T6
  P1-T12 tasks/market_flow.py    ← P1-T6
  P1-T13 tasks/financials.py     ← P1-T6
  P1-T14 tasks/macro.py          ← P1-T6

Wave 4 (依赖 Wave 3):
  P1-T9  tasks/core_chain.py  ← P1-T8, P1-T6
  P1-T15 tasks/utility.py     ← P1-T8, P1-T6

Wave 5 (依赖 Wave 4):
  P1-T16 daily_pipeline.py CLI 瘦身 ← P1-T1..P1-T15

Wave 6 (依赖 Wave 5 / 无依赖):
  P2-T1 core/scheduler.py (任务 DAG) ← P1-T16
  P3-T1 core/calendar.py (交易日历)  ← 无
  P4-T1 core/notifications.py (通知) ← 无
  P6-T1 mypy CI 修复               ← P1-T16

Wave 7 (依赖 Wave 6):
  P2-T2 并行 run_all          ← P2-T1, P1-T7
  P3-T2 APScheduler daemon    ← P3-T1, P2-T4
  P3-T3 apscheduler 依赖      ← 无
  P4-T2 通知接入 runner       ← P4-T1, P2-T2

Wave 8 (依赖 Wave 7):
  P2-T3 任务耗时追踪          ← P2-T2
  P2-T4 daily_pipeline new run_all ← P2-T2
  P6-T2 覆盖率阈值 CI         ← 无
  P6-T3 并行 CI job           ← P6-T1

Wave 9 (依赖 Wave 8):
  P5-T1 task_state 扩展       ← P2-T3
  P5-T5 TUI 磁盘告警          ← P1-T1

Wave 10 (依赖 Wave 9):
  P5-T2 TUI AkShare 健康度    ← P1-T3
  P5-T3 TUI 耗时/ETA          ← P5-T1
  P5-T4 TUI 失败股票详情      ← P1-T4
```

---

## 任务列表

### Wave 1 — 核心基础设施提取（6 个并行任务）

#### P1-T1: `core/config.py` — 类型化配置

**文件**：
- Create: `core/__init__.py`, `core/config.py`
- Modify: `daily_pipeline.py`（后续 P1-T16 移除常量）
**接口**：
- Consumes: `.env`, 环境变量
- Produces: `PipelineConfig` dataclass + `from_env()` factory
**内容**：
- 定义 `PipelineConfig` 包含：db_path, shared_data_dir, log_dir, lookback_days, include_bj, ultra_safe, parallel_workers, batch_size, batch_sleep, max_retry, retry_delay, progress_flush_interval, min_fundamentals_count, chip_bins, min_chip_days, chip_max_turnover
- 将 `_load_env_file()`、所有全局常量从 daily_pipeline.py 移入

#### P1-T2: `core/lock.py` — 进程锁

**文件**：
- Create: `core/lock.py`
**接口**：
- Consumes: `/tmp/daily_pipeline.pid`
- Produces: `ProcessLock` context manager
**内容**：
- 封装 `_acquire_lock`、`_release_lock`、pidfile 管理
- 保留 `sys.exit(1)` 冲突退出逻辑

#### P1-T3: `core/monitor.py` — AkShare 稳定性监控

**文件**：
- Create: `core/monitor.py`
**接口**：
- Consumes: `akshare_monitor.json`
- Produces: `AkShareMonitor` class
**内容**：
- 直接搬移 class AkShareMonitor

#### P1-T4: `core/progress.py` — 断点续传

**文件**：
- Create: `core/progress.py`
**接口**：
- Consumes: `progress.json`
- Produces: `ProgressTracker` class

#### P1-T5: `core/utils.py` — 工具函数

**文件**：
- Create: `core/utils.py`
**接口**：
- Consumes: `is_beijing_stock`
- Produces: `should_skip_beijing`, `_infer_market`, `_lower_process_priority`, `_should_update`, `_sleep_with_progress`
**内容**：
- 搬移 `_infer_market`(516-540), `_should_update`(486-500), `_lower_process_priority`(236-241), `_sleep_with_progress`(504-509), `should_skip_beijing`(144-150)

#### P1-T17: `interface.py` Protocol 清理

**文件**：
- Modify: `interface.py`
**内容**：
- 添加 `save_stock_list(self, df: pd.DataFrame) -> None` 到 DatabaseInterface
- 移除未使用的 per-row save 方法（只保留 batch 变体）
- 更新 `tests/test_interface.py` mock

---

### Wave 2 — Context + Runner

#### P1-T6: `core/context.py` — 任务上下文

**文件**：
- Create: `core/context.py`
**接口**：
- Consumes: `PipelineConfig`, `DatabaseInterface`, `DataLoaderInterface`, `IndicatorEngineInterface`
- Produces: `TaskContext` dataclass + `create()` factory + `fresh_db()` 方法
**内容**：
- 封装 ProviderFactory 调用，为并行任务创建独立 DB 连接

#### P1-T7: `core/runner.py` — 任务安全包装器

**文件**：
- Create: `core/runner.py`
**接口**：
- Consumes: `TaskContext`
- Produces: `safe_task(ctx, name, fn, ...)` + `TaskTimer`
**内容**：
- 搬移 `_safe_task` 逻辑（L3457-3470）
- 添加 `TaskTimer` context manager（记录 start/end/duration，为后续 P5-T1 准备）

---

### Wave 3 — 任务模块提取（6 个并行任务）

#### P1-T8: `tasks/bars.py`
- `update_bars` (L584-877), `_update_single_bar` (L878-1020)
- 替换全局常量 → `ctx.config`

#### P1-T9: `tasks/core_chain.py`
- `update_stock_list` (L543-578), `update_indicators` (L1021-1248), `update_chip_distribution` (L1249-1434)

#### P1-T10: `tasks/index_chain.py`
- `update_index_daily` (L2749-2865), `update_chip_distribution_em` (L1335-1518)

#### P1-T11: `tasks/valuation_chain.py`
- `update_fundamentals` (L1519-1629), `update_market_snapshot` (L1630-1738), `update_historical_valuation` (L2046-2086), `update_sector_industry` (L2087-2315)

#### P1-T12: `tasks/market_flow.py`
- `update_fund_flow` (L1739-1811), `update_margin_trading` (L1812-1875), `update_dragon_tiger` (L1876-1926), `update_block_trade` (L1927-2004), `update_sector_fund_flow` (L2005-2045)

#### P1-T13: `tasks/financials.py`
- `update_shareholder_count` (L2316-2373), `update_quarterly_financials` (L2374-2468), `update_industry` (L2469-2721)

#### P1-T14: `tasks/macro.py`
- `update_north_flow`, `update_limit_up_down`, `update_dividend_summary`, `update_gold_price`, `update_crude_oil`, `update_usd`, `update_global_index`, `update_us_treasury`

---

### Wave 4

#### P1-T15: `tasks/utility.py`
- `retry_failed` (L3238-3299), `health_check` (L3300-3438)
- 导入 `_update_single_bar` from `tasks/bars`

---

### Wave 5 — CLI 瘦身

#### P1-T16: 重构 `daily_pipeline.py`
- 移除所有已搬移函数
- 从 `core.*` 和 `tasks.*` 导入再导出
- 保留 `main()`, `argparse`, 进程锁, ProviderFactory 初始化
- 确保向后兼容（所有现有 import 路径仍然可用）

---

### Wave 6 — 调度 + 日历 + 通知 + CI（4 个并行）

#### P2-T1: `core/scheduler.py` — 任务 DAG
- 定义 `TaskNode(name, fn, deps, parallel_group)`
- 拓扑排序 + level 分组
- `build_default_graph()` 返回完整依赖图

#### P3-T1: `core/calendar.py` — 交易日历
- `ak.tool_trade_date_hist_sina()` 获取交易日历，缓存 90 天
- `is_trading_day(date)` 函数
- AkShare 不可用时 fallback 到周末检查

#### P4-T1: `core/notifications.py` — 通知适配器
- 抽象 `NotificationChannel` + `ConsoleChannel` + `WebhookChannel`
- 飞书/钉钉通用文本格式
- `notify_all(level, title, message)`
- 环境变量：`NOTIFICATION_WEBHOOK_URL`, `NOTIFICATION_TYPE`, `NOTIFICATION_LEVEL`

#### P6-T1: mypy CI 修复
- 移除 CI 中 `|| true`（改为 `uv run mypy .`）
- 修复全部 mypy 错误
- 禁止 `# type: ignore`

---

### Wave 7 — 并行执行 + 新守护 + 通知集成（4 个并行）

#### P2-T2: `core/runner.py` 并行 run_all
- 按 level 逐层执行，level 内独立任务用 ThreadPoolExecutor
- 每个任务使用 `ctx.fresh_db()`
- 保留 `_should_update()` 早退和进程优先级降低

#### P3-T2: `scripts/daemon.py` APScheduler 重写
- BlockingScheduler + CronTrigger(hour=16, minute=0, day_of_week="mon-fri", tz="Asia/Shanghai")
- job 内检查 `is_trading_day()`
- 非零退出码重试机制（最多 3 次，带 --resume）
- 保留 pidfile + start/stop 命令

#### P3-T3: 添加 apscheduler 依赖
- `pyproject.toml` 添加 `"apscheduler>=3.10.0"`

#### P4-T2: 通知接入 runner
- `safe_task` 异常时 → `notify_all("error", ...)`
- AkShare 中止时 → `notify_all("warning", ...)`
- run_all 完成时 → `notify_all("info", ...)`

---

### Wave 8 — 追踪 + CI 强化（4 个并行）

#### P2-T3: `core/task_state.py` — 任务耗时追踪
- 持久化 JSON：`task_state.json`
- 记录每次运行的 duration/success/failure
- `get_average_duration(task_name)` 用于 ETA 计算

#### P2-T4: daily_pipeline new run_all
- 替换旧 run_all → 调用 `scheduler.build_default_graph()` + `runner.run_all()`

#### P6-T2: 覆盖率阈值 CI
- CI 中 `uv run coverage report --fail-under=84`

#### P6-T3: 并行 CI jobs
- lint job: ruff check + mypy
- test job: pytest --cov + coverage report
- fail-fast: false

---

### Wave 9-10 — TUI 增强（5 个任务）

#### P5-T1: `core/task_state.py` 扩展
- 增加 success_count / failure_count / average_duration

#### P5-T2: TUI AkShare 健康度
- 读取 `akshare_monitor.json`
- Dashboard 显示成功率 + sleep multiplier
- 红/黄/绿颜色标识

#### P5-T3: TUI 耗时/ETA
- ProgressWidget 显示已耗时 + 预计剩余
- 从 task_state.json 读取平均耗时

#### P5-T4: TUI 失败股票详情
- ProgressWidget 显示前 N 只失败股票代码

#### P5-T5: TUI 磁盘告警
- `shutil.disk_usage()` 检查，<5GB 显示警告

---

## 验收标准

1. `daily_pipeline.py` ≤ 400 行，仅含 CLI 编排
2. `uv run ruff check` 通过
3. `uv run mypy .` 通过（无 `|| true`）
4. `uv run pytest --cov` 通过，coverage ≥ 84%
5. `uv run python daily_pipeline.py --task all --force` 按 DAG 顺序执行
6. 独立任务并行运行，总耗时大幅下降
7. `uv run python scripts/daemon.py start` 在交易日 16:00 启动全量更新
8. 任务失败自动推送通知
9. TUI 显示 AkShare 健康度、任务耗时/ETA、失败股票、磁盘告警
10. CI lint 和 test 并行运行，mypy 和覆盖率强制检查

---

## 提交策略

- 每波结束后管线可运行、测试通过
- conventional commits：`refactor: split core config/lock modules` / `feat: parallel task scheduler`
- 分支名：`feat/pipeline-modular-upgrade`
