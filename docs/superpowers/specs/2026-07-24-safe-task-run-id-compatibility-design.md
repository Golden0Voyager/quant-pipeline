# `safe_task` Run ID 兼容性修复设计

## 背景

2026-07-24 的提交 `001d54f6` 修改了 `core.runner.safe_task`，开始向每个任务回调无条件传入
`_task_run_id`。绝大多数既有任务使用固定函数签名，不接受这个关键字参数，因此 `pipe-tui`
启动全量抓取后，这些任务会在执行任何业务逻辑前立即以 `TypeError` 失败。

`update_index_membership` 和 `update_concept_member` 需要接收调度器生成的 run ID，以便 PIT
历史记录和 `ingestion_runs` 审计记录共享同一个标识。因此，简单删除 run ID 注入会恢复旧任务，
但会破坏新增的审计关联能力。

## 目标

- 恢复 `pipe-tui` 全量抓取中所有固定签名任务的正常调用。
- 对明确支持 `_task_run_id` 的任务继续注入同一个调度 run ID。
- 对接受 `**kwargs` 的通用回调保持现有注入行为。
- 正确处理使用 `functools.wraps` 的任务装饰器。
- 当回调签名无法被安全检查时，优先保证任务可运行，不注入可选内部参数。
- 不修改任务业务逻辑、数据抓取顺序、数据源策略或数据库迁移。

## 方案选择

### 采用：基于回调签名的条件注入

在 `core.runner` 内增加一个私有判断函数，使用 `inspect.signature` 检查回调是否：

1. 显式声明 `_task_run_id`；或
2. 声明 `**kwargs`。

仅满足其中之一时，`safe_task` 才通过 `kwargs.setdefault` 注入生成的 run ID。调用方显式传入
`_task_run_id` 时保留调用方值。

`inspect.signature` 默认沿 `__wrapped__` 解析，因此当前使用 `functools.wraps` 的
`skip_if_task_locked` 装饰器仍能暴露原任务签名。若签名检查抛出 `TypeError` 或 `ValueError`，
判断函数返回 `False`，避免一个可选审计参数阻断核心抓取任务。

### 未采用：完全停止注入

改动最小，但 `update_index_membership` 和 `update_concept_member` 会自行生成不同的 run ID，
使 PIT 快照与调度审计记录失去稳定关联。

### 未采用：修改所有任务签名

需要改动几十个任务，扩大回归面，而且让不需要审计 run ID 的业务函数依赖调度器内部参数。

## 数据流

1. `safe_task` 为本次任务生成 UUID。
2. 调度器检查回调的公开签名。
3. 支持 `_task_run_id` 的回调收到该 UUID；不支持的回调按原参数调用。
4. 回调结果被标准化。
5. `safe_task` 使用同一个 UUID 写入 `ingestion_runs`。

## 回归测试

在 `tests/test_core_infrastructure.py` 增加以下行为测试：

- 固定零参数回调可以成功执行，且不会收到 `_task_run_id`。
- 显式声明 `_task_run_id` 的回调收到 UUID。
- 接受 `**kwargs` 的回调继续收到 UUID。
- 使用 `functools.wraps` 包装、原始签名不接受该参数的回调可以成功执行。
- 调用方显式提供 `_task_run_id` 时不会被覆盖。
- 无法检查签名的可调用对象不因检查失败而阻断执行。

现有 `tests/test_pipeline_resilience.py` 中关于 `**kwargs` 注入的测试应继续通过。

## 验证

1. 先观察新增固定签名回归测试在当前代码上失败。
2. 实施条件注入后运行 `core.runner` 相关定向测试。
3. 运行 `tests/test_daily_pipeline.py` 和 `tests/test_pipeline_resilience.py`。
4. 运行全量 `rtk uv run python -m pytest -q`。
5. 运行 `rtk uv run ruff check .`。
6. 运行 `rtk git diff --check` 并检查仓库根目录是否出现测试副作用文件。

## 非目标

- 不处理当前工作区中 `providers.py` 与 `migrations/006_reconcile_ingestion_audit.py` 的既有修改。
- 不更改 AkShare 请求、重试、限流或任务并发策略。
- 不自动启动生产全量抓取；修复完成后提供经过验证的启动命令。
