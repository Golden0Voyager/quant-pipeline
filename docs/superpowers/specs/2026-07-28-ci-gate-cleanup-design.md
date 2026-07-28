# CI Gate Cleanup Design

日期：2026-07-28

## 问题

断点续传修复本身的定向回归已通过，但完整 CI 仍被四个独立问题阻塞：

1. `tests/test_data_audit.py` 使用固定日期 `2026-07-23` 构造“健康”数据，
   随当前日期推进后会被正确判定为过期，导致测试漂移。
2. `tests/test_reconcile.py` 的 CLI 测试会获取真实 `ProcessLock`，因此可能与正在
   运行的生产管道冲突。
3. `tasks/valuation_chain.py` 使用大写局部变量 `_MIN_FILLED`，触发 Ruff N806。
4. `tests/test_stock_cyq_em.py` 使用可合并的嵌套 `with`，触发 Ruff SIM117。

## 目标

- 让测试不依赖执行日期或机器上真实进程锁。
- 清除两处 Ruff 告警。
- 不改变数据审计、估值补充或筹码计算的业务行为。
- 不终止、干扰或修改正在运行的生产管道。

## 方案

### 数据审计测试

先创建表规格，再通过 `_expected_date_for(spec)` 得到当前预期交易日，将该日期写入
fixture，并严格断言状态为 `healthy`。这验证的是“预期日期数据健康”，而不是一个
会随时间失效的历史日期。

### Reconcile CLI 测试

在 `TestMain` 的每个 `rwa.main()` 测试中隔离 `ProcessLock.acquire()` 和
`ProcessLock.release()`。测试仍覆盖参数解析和任务路由，但不会读取或修改真实锁。

### Ruff 修复

- 将函数内 `_MIN_FILLED` 改为 `min_filled`。
- 将三个数据源补丁和 `pytest.raises` 合并到同一个多上下文 `with`。

## 验证

依次运行：

1. 两个原始失败测试和相关测试文件；
2. 两个 Ruff 目标文件及全仓 Ruff；
3. 完整 pytest；
4. `git diff --check` 与工作树检查。
