# 自选股同步可靠性与测试隔离设计

## 目标

补齐自选股全量对账实现中的失败可见性、审计时间戳和测试隔离问题，使同步失败不会被误报为成功，并保证测试不访问真实网络或写入正式日志。

## 同步结果

`WatchlistSyncResult` 在现有计数字段之外增加：

- `success: bool`：同步是否完整成功；
- `error: str | None`：失败原因，成功时为 `None`。

目录不存在是安全跳过，不修改数据库，但视为 `success=False`，错误信息说明来源目录不存在。目录存在但为空仍是一次成功同步，并按既定规则停用所有 `watchlist_sync` 记录。

## 错误边界

- TXT 文件枚举、读取、解析和 SQLite 对账处于同一个异常处理边界内。
- 文件读取或数据库操作失败时回滚事务并返回失败结果。
- TUI 对失败结果显示 warning 通知，包含错误原因；不得显示“同步完成”。
- `_sync_watchlists()` 不再使用无提示的 `contextlib.suppress(Exception)` 隐藏意外异常，而是记录并通知。

## 状态审计

将记录恢复为 `tracking` 或停用为 `inactive` 时，同时将 `updated_at` 更新为当前时间，与 `DatabaseManager.watchlist_update_status()` 的既有行为保持一致。新增记录继续使用数据库默认的 `updated_at`。

## 测试隔离

- `tests/conftest.py` 在项目模块导入前将 `QUANT_DB_PATH` 指向临时数据库，使日志目录位于临时目录。
- `run_all` 编排测试 mock `_safe_task`，只验证编排结果，不执行实际 AkShare、东方财富或其他网络任务。
- mock `asyncio.create_task` 或 `_create_background_task` 后，测试必须关闭未调度的 coroutine。
- 删除 pytest 中针对 `PipelineApp` coroutine RuntimeWarning 的忽略规则；完整测试应在不屏蔽警告的情况下保持干净。

## 验收标准

- 数据库或 TXT 读取失败时，结果为 `success=False`，TUI 显示失败通知。
- 恢复和停用操作更新 `updated_at`。
- 完整测试不访问真实网络、不写 `~/Code/quant_data/logs`、不生成 MagicMock 文件。
- pytest 无 coroutine 未 await 警告。
- Ruff、`git diff --check` 和覆盖率门槛通过。
