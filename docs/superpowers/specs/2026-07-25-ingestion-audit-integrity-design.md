# 摄取审计与 PIT 外键完整性设计

## 背景

`safe_task` 已能按回调签名有条件地注入统一 run ID，但最终分支审查发现两类完整性风险：

1. migration 006 已经发布并被数据库记录 checksum，不能继续原地追加回填逻辑。旧数据库记录
   原始 checksum，新生产库记录修改后 checksum；直接提交修改后的 006 会阻断旧数据库启动。
2. PIT 子记录在任务回调内提交，`ingestion_runs` 父记录在回调完成后才写入。审计写入失败时，
   会留下新的孤立 `snapshot_run_id`。

另外，签名检查仍需排除名为 `_task_run_id` 的 `*args` 参数；当前工作区还有一项未提交的
带锁装饰器回归测试，以及一项显式 `inspect.unwrap` 修改。回归场景应保留，但签名解析由统一
的严格参数规则实现。

## 目标

- 保持已发布 migration 006 内容不可变。
- 让记录旧版或临时修改版 006 checksum 的数据库都能安全升级。
- 在任何 PIT 子记录写入前持久化对应的审计父记录。
- 最终任务结果更新同一条审计记录，而不是重复插入。
- 对运行时写连接启用 SQLite 外键约束。
- 审计父记录预写失败时，不执行可能产生 PIT 子记录的回调。
- 为真实 SQLite 持久化、迁移升级、幂等性和外键完整性建立回归测试。
- 保持普通固定签名任务和非 PIT 任务的现有执行行为。

## 迁移设计

### 恢复 migration 006

将 `migrations/006_reconcile_ingestion_audit.py` 恢复到发布版本，不再包含孤立 run ID 回填。
发布版本的 SHA-256 为：

```text
a7830c97017b503c6d7b10412c901a4a4d365210704f8582521dfbe395e1e4eb
```

### 新增 migration 008

新增 `008_reconcile_orphan_ingestion_runs.py`：

1. 读取 `schema_migrations` 中 version 006 的 checksum。
2. 仅接受两个已知 checksum：
   - 发布版 `a7830c…`
   - 当前生产库曾记录的临时修改版 `8273ec…`
3. 遇到其他未知 checksum 时明确失败，不掩盖迁移漂移。
4. 从 `index_member_history` 和 `concept_member_history` 收集不存在于
   `ingestion_runs` 的非空 `snapshot_run_id`。
5. 使用 `INSERT OR IGNORE` 创建状态为 `reconciled` 的占位父记录。
6. 将 version 006 的记录 checksum 更新为磁盘上已恢复的 006 checksum。

该迁移可重复执行其核心回填函数而不生成重复父记录。MigrationEngine 正常只应用 version 008
一次；008 在最终 checksum 校验前修正 version 006 记录，因此两类既有数据库都能通过校验。

## 运行时写入顺序

### `safe_task`

对显式支持 `_task_run_id` 或 `**kwargs` 的回调：

1. 计算 effective run ID。
2. 若首个参数提供 `record_ingestion_run`，先写入 `status="running"` 的父记录。
3. 父记录写入成功后才执行回调。
4. 回调成功或失败后，使用同一 run ID upsert 最终状态。
5. 父记录预写失败时返回数据库类失败结果，不调用回调。

不支持 run ID 的普通回调保持原有行为：正常执行后记录最终结果，不增加预写步骤。

任务开始时间在调度开始时固定；预写记录的 `finished_at` 暂等于 `started_at`，最终 upsert 时更新。

### Provider

`record_ingestion_run` 使用：

```sql
INSERT ... ON CONFLICT(run_id) DO UPDATE SET ...
```

这样预写的 `running` 父记录会被最终状态原位更新。共享写连接创建时执行：

```sql
PRAGMA foreign_keys = ON
```

因此不存在审计父记录的 PIT 子写入会被 SQLite 拒绝。

## 签名兼容性

显式 `_task_run_id` 仅在参数类型为以下两类时视为可按关键字传入：

- `POSITIONAL_OR_KEYWORD`
- `KEYWORD_ONLY`

`VAR_KEYWORD` (`**kwargs`) 仍然支持注入；`POSITIONAL_ONLY` 和 `VAR_POSITIONAL`
(`*args`) 不支持。

带 `functools.wraps` 的 `skip_if_task_locked` 回调继续由 `inspect.signature` 的标准
wrapped-signature 解析覆盖。保留带锁装饰器回归测试；不依赖额外 `inspect.unwrap`。

## 测试策略

- `tests/test_core_infrastructure.py`
  - PIT 回调执行前已存在 `running` 父记录。
  - 最终记录与预写记录使用同一 run ID。
  - 预写失败时回调未执行。
  - `*_task_run_id` 不被误判为关键字参数。
  - 带锁装饰器的固定签名任务不收到内部关键字。
- `tests/test_providers_extended2.py`
  - 临时 SQLite 中同一 run ID 先插入、后更新且只有一行。
  - metadata 中的 run ID/时间戳真实持久化。
  - 共享写连接启用外键。
  - 无父记录的 PIT 子写入触发外键错误。
- `tests/test_migrations.py`
  - 旧 006 checksum 数据库升级到 008。
  - 临时修改版 006 checksum 数据库升级到 008。
  - 孤立 run ID 被补齐。
  - 重复执行回填保持幂等。
  - 未知 006 checksum 明确失败。
  - `PRAGMA foreign_key_check` 为空。

## 非目标

- 不自动启动生产管道。
- 不修改数据抓取顺序、AkShare 调用、重试或限流策略。
- 不处理当前分支之外的既有 mypy 类型债务。
