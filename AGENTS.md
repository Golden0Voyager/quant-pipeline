# AGENTS.md — quant_pipeline 项目规范

## ⚠️ 环境约束（强制）

- **包管理器**：`uv pip install <pkg>`（仅限 `uv`，禁止 `pip`）
- **运行脚本**：`uv run python <script>.py`

---

## 🛠️ Key Conventions

- **数据库路径**: `~/Code/quant_data/quant_core.db` (SQLite)
- **数据源优先级**: AkShare > 静默（写操作禁用 yfinance fallback）
- **市场前缀规则**: `6`/`9` → sh, `0`/`2`/`3` → sz, `4`/`8`/`920` → bj
- **质量保障**: ruff + mypy + pytest CI 校验
- **三层次入口**: `--task daily`（=`all`）每日层；`weekly_backfill`/`monthly_repair` 每周/每月层仅手动触发（CLI 或 TUI W/M 键）

---

## Git Workflow 与规范

- **一文件一提交**: 严禁将多文件打包在一个 commit 中。Commit message 采用中英双语，英文块在前，中文在后。
- **New Feature 流程**: 开发新功能 (new feature) 时，建议走 `/git-feature` 流程（使用 `/git-feature start` 创建分支，完成开发后使用 `/git-feature done` 完成推送/PR/合入/清理全流程）。
- **分支规范**: 禁止直接在 `main` 上开发，必须在 `feat/*`、`fix/*`、`refactor/*` 等分支上进行。

- **多 agent 协作**: 仓库可能同时由 Codebuff / OpenCode / 其他 agent 编辑。
  - 每个 agent 各自在自己的 `feat/*`/`fix/*` 分支开发;
  - 本地 `main` 只用于 `git fetch --prune origin/main` + merge/rebase 上游变更,不写业务代码;
  - 合入必须经 PR + CI 绿,之后删远端分支。
  - 若某次提交因工具差异误落 main,应尽快 cherry-pick 到对应分支并回退 main,避免分叉长期存在。

---

## 🔒 不可违反的硬规则(来自历史审计)

以下规则有真实生产事故证据,**违反会引入数据损坏或静默失败**,不允许以"为了方便"为由绕过:

### 1. schema DDL 的真相在 `_ensure_tables`
- `providers.py:SmartMoneyDBProvider._ensure_tables` 创建 **28 张基线表**,其中 **26 张不被任何 `migrations/` 文件创建** — migration 目录只负责增量(加列/索引)。
- **禁止**删除 `_ensure_tables` 或假设 migration 能替代它:全新安装会缺表,生产库保持空数据但测试变绿。
- `_ensure_tables` 失败必须 `raise`(当前已是 hard fail);仅当 `Path(db_path).parent` 不存在时静默返回(与 `_ensure_wal_mode` 一致)。
- `_old_migrate_phase2_tables` 是已删除的死代码,引用它的测试已移走 — 不要让它复活。

### 2. 锁文件永远不能 `unlink`
- `core/lock.py` 的 `ProcessLock.acquire` / `release` 以及 `TaskLock.release`:锁文件(锚点)必须保留,只能清空 PID 内容。
- 删除文件会让新进程在**新 inode** 上加锁成功,两个 pipeline 同时认为持有全局锁、并发写同一张 SQLite 数据库(`quant_core.db` 的 WAL 模式不防这个),且 `global_lock_held()` 对其它进程返回 False — 单任务互斥保护整体失效。
- flock 失败一律 **fail-closed**(打印持有者 `exit(1)`),不在"PID 写窗口"做自愈。
- 回归测试(`tests/test_lock.py::test_release_keeps_lockfile_with_empty_pid` 等)会断言文件保留 — 还原旧实现会让这些测试全红,任何改动前需先跑它们验绿。

### 3. 交易日推导必须走缓存日历
- `core/calendar.get_expected_latest_trading_day` 基于 `_load_cached_calendar()` 的最近交易日,不是简单"周末回退"。
- 节假日(国庆/春节/端午等)走周末逻辑会返回非交易日 → health_check 假告警 `status=degraded` → `run_all` 认为 crashed → `sys.exit(1)` + ERROR 告警。
- 日历缓存不可用时退化周末逻辑并打一次 WARNING (`_CALENDAR_FALLBACK_WARNED` 单例)。不要把这个警告逻辑删掉。

### 4. Protocol 契约必须与实现同步
- `interface.py` 的 `DatabaseInterface` Protocol 成员签名是 CI 级别契约。新增/修改实现时同步改 Protocol 签名。
- 已知坑:`def foo(..., date: str = None)` 必须写成 `date: str | None = None` — 前者会让 mypy 报 7 处冲突。
- `tests/test_interface.py` 用 `MockDatabase` 绕过了真实实现校验 — 这是已知的测试盲区,真正的契约验证靠 mypy 全量跑和手写的 `ProviderFactory` 类型对齐。

### 5. 测试隔离:不要依赖操作系统锁状态
- `daily_pipeline.main()` 的进程锁与全局锁探测作用于真实文件 `/tmp/daily_pipeline.pid`(非 mock):`_acquire_lock()` 失败 fail-closed `sys.exit(1)`,`global_lock_held()` 为真时单任务路径同样退出。
- 根 `tests/conftest.py` 的 autouse fixture `_isolate_pipeline_lock`(PR #110)已同时 patch `daily_pipeline._acquire_lock` 与 `daily_pipeline.global_lock_held`,调用 `main()` 的两个测试模块(`test_daily_pipeline.py`/`test_parallel_pipeline.py`)不再因外部持锁(launchd daemon/TUI/其他 agent)假失败。
- 约束:该 fixture 只替换 `daily_pipeline` 命名空间入口——验证真实锁语义的 `tests/test_lock.py`(走 `core.lock`)不受影响;测试体内自行覆盖探测的用例(如 `test_single_task_refused_when_global_lock_held`)优先级更高。
- 历史:漏口曾有两处——旧 fixture 只 patch `_acquire_lock` 漏了 `global_lock_held`,且 `test_parallel_pipeline.py` 完全无隔离,外部持锁时共 6 个测试假失败(`SystemExit: 1`)。新增调 `main()` 的测试模块无需再自行 patch。

---

## 🧪 回归测试规范(red-proof)

每项 P0/P1 修复必须附带**会因旧代码变红**的测试(不能只在"新状态"下断言绿):

- 例:P0-2 锁 unlink 修复 → `test_release_keeps_lockfile_with_empty_pid` 断言旧实现会丢失文件。
- 例:P0-3 schema 硬失败 → `test_schema_baseline.py::TestBaselineDDL` 断言 28 表存在 + DDL 失败抛异常。
- 提交时 commit message 写明"还原旧实现会让 X 测试全红" → 审计方据此反证钉住。

---

## 📐 已知待清理项(按 P0/P1/P2 分级,本文件即权威清单;`docs/todo.md` 仅记录数据源 TODO)

| 编号 | 级别 | 现状 |
|---|---|---|
| P1-5 mypy CI 空转(`\|\| true` + 缺 `[tool.mypy]`) | P1 | ✅ 已修复 (PR #110):CI 跑无参数 `uv run mypy`,范围由 `[tool.mypy] files` 决定,且已含 `tests/` |
| P1-6 `date: str = None` ×7 协议违规 | P1 | ✅ 已修复 (PR #110):`SmartMoneyDBProvider` 现已满足 `DatabaseInterface` |
| P1-7 `TaskSpec.callable` 全是 None,registry 非单一真相 | P1 | ✅ 已修复 (PR #110):字段改为可选,自述"别处会填"的 `pass` 分支换成真实不变量校验 |
| P2-8 10 个任务被 cadence 永久跳过但仍在 stage4 wiring | P2 | 已知设计取舍 |
| P2-9 `PARALLEL_WORKERS` 默认 1 vs help 写 4 | P2 | 未处理 |
| P2-10 `_to_float` 在 13 个模块重复 | P2 | 未处理,计划抽 `core/ak_utils.py` |
| P2-11 `get_*_latest_date` 吞 `Exception` | P2 | 未处理 |
