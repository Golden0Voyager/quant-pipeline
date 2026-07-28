# 日线任务断点续传与结果契约修复设计

日期：2026-07-28

## 问题

`update_bars` 仍返回旧式的 `success`、`failed`、`skipped` 计数字段。
统一结果层只把正数 `saved` 或显式 `status` 识别为成功，因此一次实际成功的
日线更新会被误判为 `zero rows without explanation`，CLI 随后以状态码 1 退出。

日线全量扫描结束但存在失败股票时，同一个进度文件会被改写为
`task="retry"`。当前 `--resume` 不检查任务类型，仍使用其中的
`last_symbol` 计算全市场扫描位置；当断点位于最后一只股票时，恢复运行会空跑，
输出 0/0/0 并再次退出失败。

## 目标行为

1. `update_bars` 和 `retry_failed` 返回兼容旧消费者的计数字段，同时提供明确的
   `status`、`saved`、`attempted` 和必要的错误信息。
2. `update_bars --resume` 遇到当日 `task="update_bars"` 时继续全市场扫描。
3. `update_bars --resume` 遇到当日 `task="retry"` 时只处理
   `failed_queue`，不重新扫描全市场，也不使用旧的全市场 `last_symbol`。
4. 重试全部成功后清除进度文件；仍有失败时保留去重后的失败队列。
5. 成功、全部已最新、没有剩余工作均不得产生错误退出码；存在失败时返回
   `degraded`，熔断中止继续保持非零退出语义。

## 实现范围

在 `tasks/bars.py` 中集中构造兼容的新式结果：

- 正常且无失败：`status="success"`；
- 有部分失败：`status="degraded"`，并带 `error`；
- 股票列表为空等合法空结果：`status="no_data"`；
- `saved` 暂以成功更新的股票数量表示，保持与现有 `success` 计数一致；
- `attempted` 表示本次实际检查的股票数量。

恢复初始化阶段按进度记录的 `task` 分流：

- 扫描断点使用现有的 `find_resume_index`；
- 重试断点把本次目标集合替换为失败队列，并将本次计数从零开始；
- 未知任务类型不作为日线扫描断点使用。

`tasks/utility.py` 的 `retry_failed` 同步补齐结果契约，避免独立调用重试任务时
再次触发相同的零行误判。

不修改 `normalize_task_result` 的“无说明零行即失败”规则，因为该规则用于发现
尚未迁移的静默空结果；修复应发生在任务生产者边界。

## 错误与兼容性

- 保留 `success`、`failed`、`skipped`、`total`、`failed_symbols`，避免破坏现有
  日志、测试和调用者。
- 失败队列过滤掉不在当前股票列表中的代码，避免退市或代码变化造成无效重试。
- 旧进度文件若没有 `task` 字段，继续按原有扫描断点兼容处理。
- 不改变 AkShare/yfinance 数据源策略、重试次数或限流参数。

## 验证

先增加并观察失败的回归测试：

1. 成功更新一只股票时，归一化结果为 `success`。
2. `task="retry"` 且断点位于全市场末尾时，只重试失败队列。
3. 重试成功后进度文件被清除且结果不触发错误退出。
4. 重试仍失败时返回 `degraded` 并保留失败队列。
5. 独立 CLI 根据成功/降级结果返回正确退出码。

然后运行相关测试、完整 pytest、Ruff、`git diff --check`，并检查仓库根目录没有
新增 MagicMock 路径副作用文件。
