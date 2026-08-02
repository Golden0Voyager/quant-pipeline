# 三层次数据管道 + 任务元数据收敛设计

日期：2026-08-01
状态：已批准（用户确认）
分支：feat/three-tier-pipeline

## 背景与目标

当前管道只有"全量运行"（`--task all`，约 50 个任务串行）一种批量形态：
低频任务（周/月/季更）每天陪跑，失败后的补全依赖人工判断，深度修复
（备份/对账/ vacuum）散落在独立脚本里没有编排。同时任务↔表↔标签↔分组
元数据在 `tui.py` 中以 6 份平行映射手工维护，靠防漂移测试兜底。

本设计将管道重构为三个各有侧重的层次，并把元数据收敛到
`core/task_registry` 单一来源：

| 层次 | 入口 | 侧重 | 触发 |
|------|------|------|------|
| 每日抓取 | `--task daily`（`all` 为其别名） | 速度优先：交易日增量 | launchd 20:30（现状不变）+ TUI |
| 每周补全 | `--task weekly_backfill` | 完整性优先：缺漏兜底 | 仅手动（TUI 按钮 / CLI） |
| 每月修复 | `--task monthly_repair` | 正确性优先：校验修复 | 仅手动（TUI 按钮 / CLI） |

非目标：不拆分 `tui.py` 为多模块；不引入配置文件驱动的调度；
不改变 `safe_task` / refresh / 熔断等现有基建语义。

## 1. 三层次入口

### 1.1 每日层（`daily`）

- 即 `update_daily_core` 的正名化：按 registry cadence 过滤执行
  TRADING_DAY + DAILY + ON_DEMAND 任务（过滤机制已存在于 `run_all`
  的 `target_cadences` 参数）。
- `--task all` 成为 `daily` 的别名，launchd（`com.smartmoney.update`，
  每日 20:30）与 TUI「全量更新」按钮零配置改动；WEEKLY/MONTHLY/QUARTERLY
  任务不再每日陪跑（行为收窄即本轮目标，用户已确认）。
- `--task update_daily_core` 保留为兼容别名。

### 1.2 每周补全层（`weekly_backfill`）

顺序执行，全部经 `safe_task` 隔离：

1. WEEKLY cadence 任务（registry 过滤，同 `run_all` 机制）。
2. 补齐缺漏：`compute_catch_up_tasks`（从 `tui.py` 收敛到 core，按
   数据完整度面板同一套 stale 语义驱动，不限 cadence——月度任务变
   stale 也在此兜底，保证低频任务不依赖人工记忆）。
3. `retry`（失败股票重抓，ON_DEMAND）。
4. `health_check` 报告。

### 1.3 每月修复层（`monthly_repair`）

1. MONTHLY + QUARTERLY cadence 任务。
2. 修复链（编排现有脚本能力，不新写修复逻辑）：
   `backup_database` → `reconcile_with_akshare` → `validate_and_vacuum`。
   - backup 失败 → 中止后续修复步骤（不允许无备份修复），层结果 failed。
   - reconcile / vacuum 失败 → 记录并继续，最终汇总呈现。
3. `health_check` 报告。

### 1.4 锁与并发

三层均为批量全量型，共用全局 `ProcessLock`（与现有 `all` /
`--refresh-today` 一致）；运行期间单任务路径经第二轮的
`global_lock_held` 探测自动拒绝，无需新机制。

### 1.5 TUI 入口

新增「每周补全」「每月修复」两个动作，复用 `_run_or_schedule` 弹窗
（立即/稍后/取消）与 `_run_and_report` 完成报告；按钮放置于现有
任务下拉/分组区，映射数据来自收敛后的 registry（见第 2 节）。

## 2. 效率与鲁棒性

- 每日层收窄省掉约 10 个低频任务的每日进程内开销（provider 初始化、
  审计写、日志），是本轮最大的运行效率收益。
- `health_check` 在三层批量入口内使用 page_count 估算行数（`tui.py`
  `get_all_table_counts(fast=True)` 同款模式，秒级 → 亚秒级）；
  精确 `COUNT(*)` 保留给手动 `--task health_check`。
- 层结束复用 `run_all` 的 crashed 汇总 + `notify_all` 外发
  （失败附任务名），与 PR #54 的告警链路一致。
- 层内任务 `safe_task` 隔离，单任务异常不中断后续。

## 3. 元数据收敛（core/task_registry.py）

`tui.py` 现有 6 份平行映射全部改为 registry 派生，registry 新增
少量派生视图，不引入新抽象层：

| tui.py 现状 | 收敛后 |
|-------------|--------|
| `TABLE_DATE_COLUMNS` | `table_date_columns()`（已有派生视图） |
| `get_all_table_counts` 内联表清单 | registry `spec.tables` 汇总 |
| `DataCompletenessWidget.TASK_TO_TABLE` | `{spec.name: spec.tables}` 派生 |
| `TABLE_LABELS` / `TABLE_LABELS_CN` | 一次性搬入 registry `TABLE_LABELS` |
| `TASK_GROUPS` / `_SINGLE_TASK_GROUPS`（两份） | registry 单份 `TASK_GROUPS` |
| `_CATCH_UP_TASK_ORDER` + `compute_catch_up_tasks` | 移入 core，TUI 与每周层共用 |
| `daily_pipeline._TASK_CALLABLES`（手工维护） | 从 registry `callable` 派生 |

`TaskSpec` 不新增字段；分组与标签以模块级常量存于 registry。
`tui.py` 净减约 300 行映射定义；既有防漂移测试改为验证
「TUI 消费的就是 registry 派生值」。

## 4. 错误处理

- 层内：`safe_task` 既有语义（异常 → failed 结果 + error 告警，继续后续）。
- 层间：每层独立进程、独立全局锁获取，互不影响。
- 修复链：见 1.3（backup 中止 / 其余继续并汇总）。
- 每周层补全步骤对 stale 判定失败（DB 不可读等）：记录 warning 并跳过
  补全，不影响 WEEKLY 任务主体。

## 5. 测试

全部 TDD（先失败测试后实现）：

- 每层入口：cadence 过滤正确性、执行顺序（每周层：WEEKLY 任务 →
  补全 → retry → health；每月层：MONTHLY/QUARTERLY 任务 → 修复链 →
  health）、失败隔离、crashed 汇总与通知
  （仿 `TestRunAll` / PR #54 通知测试模式）。
- 修复链：backup 失败中止、reconcile 失败继续（mock 脚本入口）。
- registry 派生视图：与既有 `test_phase2_event_tables_are_registered_everywhere`
  等防漂移测试对齐更新。
- TUI：两个新按钮的弹窗路由冒烟（仿现有 action 测试）。
- 回归：全量 pytest + ruff + mypy 无新增错误。

## 6. 兼容与迁移

- `--task all` / `update_daily_core` 为 `daily` 别名，现有 launchd、
  TUI、runbook 命令不变。
- 低频任务的兜底路径：每日层不再覆盖 → 变 stale 后面板标黄 →
  每周层补全（stale 驱动不限 cadence）。用户已确认接受该语义。
- TUI 面板显示语义不变（同样的表、同样的新鲜度口径），仅数据源
  从本地常量切换为 registry 派生。
