# AGENTS.md — quant_pipeline 项目规范

## ⚠️ 环境约束（强制）

- **包管理器**：只用 `uv`（禁止 `pip`）。新增依赖用 `uv add <pkg>` —— 它同时写入 `pyproject.toml` 与 `uv.lock`；`uv pip install <pkg>` 只能用于一次性试验，**不能**当作依赖的记录方式（它不改锁定文件，于是 CI 的 `uv sync --extra dev` 与其他 checkout 都不会有该依赖）。
- **运行脚本**：`uv run python <script>.py`
- **worktree 里跑门禁**：先 `uv sync --extra dev`，否则缺 dev 依赖会让 `mypy`/`pytest` 报出与代码无关的假红。

---

## 🛠️ Key Conventions

- **数据库路径**: `~/Code/quant_data/quant_core.db` (SQLite)，默认值在 `core/config.py:DEFAULT_DB_PATH`；可用环境变量 `QUANT_DB_PATH` 覆盖（测试与临时运行靠它隔离，避免误写生产库）
- **数据源优先级**: AkShare 是唯一的**写库**来源；yfinance 仅作运行时临时 fallback，其来源的行一律拒绝落库（`tasks/bars.py` 的拒绝规则）——库内不应出现 `data_source='yfinance'`
- **市场前缀规则**: 权威实现是 `core/utils.py:infer_market`：`688`→`star`，`4`/`8`/`920`→`bj`，`6`/`90`→`sh`，`300`/`301`→`gem`，`002`/`003`→`sme`，`000`/`001`/其余→`sz`。**顺序敏感：`920` 必须先于 `9`→`sh` 判断**（920 是北交所新号段），`core/source_hithink.py` 有同名注释
- **质量保障**: CI 四道门禁 —— `ruff check`、`mypy`（全仓，范围由 `[tool.mypy] files` 决定，**已含 `tests/`**）、`pytest --cov`、`coverage report`（阈值 `fail_under = 85`，覆盖率不达标同样会红）
- **三层次入口**: `--task daily`（=`all`）每日层；`weekly_backfill`/`monthly_repair` 每周/每月层仅手动触发（CLI 或 TUI W/M 键）。后两层都是**注册表驱动**（遍历 `TASK_REGISTRY` 按 cadence 取任务），不是手写清单
- **跨仓库依赖 `smartmoney_hunter`**: 它**不是**声明依赖（`pyproject.toml` 与 venv 里都没有），只存在于兄弟 checkout `~/Code/quant_hunter/src`，所以必须往 `sys.path` 注入。注入**只有一个来源**：`core/_bootstrap.py`——包级 choke point 在 `core/__init__.py`/`tasks/__init__.py`（覆盖各自包下所有子模块），顶层模块 `daily_pipeline.py`/`providers.py` 各显式调用一次，`scripts/*.py` 先就地注入仓库根再调用（脚本直跑时 `sys.path[0]` 是 `scripts/`，此刻还 import 不到 `core`）。位置可用 `QUANT_HUNTER_PATH` 指向别的兄弟 checkout **根目录**（与 `tests/test_stage2_write_disjointness.py` 同一约定）。**不要再在别处内联 `sys.path` 操作**：规则门禁 `tests/test_sys_path_bootstrap.py` 会扫出所有带模块级跨仓库 import 的模块，逐个在全新解释器里只带仓库根导入一次（用桩目录，因此在没有兄弟仓库的 CI 上同样有效）

---

## Git Workflow 与规范

- **一文件一提交**: 严禁将多文件打包在一个 commit 中。Commit message 采用中英双语，英文块在前，中文在后。
- **New Feature 流程**: 开发新功能 (new feature) 时，建议走 `/git-feature` 流程（使用 `/git-feature start` 创建分支，完成开发后使用 `/git-feature done` 完成推送/PR/合入/清理全流程）。
- **分支规范**: 禁止直接在 `main` 上开发，必须在 `feat/*`、`fix/*`、`refactor/*` 等分支上进行。

- **多 agent 协作**: 仓库可能同时由 Codebuff / OpenCode / 其他 agent 编辑。
  - 每个 agent 各自在自己的 `feat/*`/`fix/*` 分支开发;
  - 本地 `main` 只用于同步上游(`git fetch --prune origin` 后 `git merge --ff-only origin/main`)与建分支,不写业务代码;
  - 合入必须经 PR + CI 绿,之后删远端分支。
  - 若某次提交因工具差异误落 main,应尽快 cherry-pick 到对应分支并回退 main,避免分叉长期存在。

---

## 🔒 不可违反的硬规则(来自历史审计)

以下规则有真实事故或踩坑证据,**违反会引入数据损坏、静默失败或假失败**,不允许以"为了方便"为由绕过:

### 1. schema DDL 的真相在 `_ensure_tables`
- `providers.py:SmartMoneyDBProvider._ensure_tables` 创建 **28 张基线表**,其中 **26 张不被任何 `migrations/` 文件创建**（两边都定义的是 `cb_quotation`/`cb_redeem`,由 migration 002 重建）。注意 `migrations/` **本身也建表**（如 `fund_holdings`/`top10_shareholders`/`refresh_runs` 等新表由 migration 创建）——不要把它理解成「只加列/索引」。
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
- 根 `tests/conftest.py` 的 autouse fixture `_isolate_pipeline_lock`(PR #110)已同时 patch `daily_pipeline._acquire_lock` 与 `daily_pipeline.global_lock_held`,调用 `main()` 的两个测试模块(`tests/test_daily_pipeline.py`/`tests/test_parallel_pipeline.py`)不再因外部持锁(launchd daemon/TUI/其他 agent)假失败。
- 约束:该 fixture 只替换 `daily_pipeline` 命名空间入口——验证真实锁语义的 `tests/test_lock.py`(走 `core/lock.py`)不受影响;测试体内自行覆盖探测的用例(如 `tests/test_daily_pipeline.py::test_single_task_refused_when_global_lock_held`)优先级更高。
- 历史:漏口曾有两处——旧 fixture 只 patch `_acquire_lock` 漏了 `global_lock_held`,且 `tests/test_parallel_pipeline.py` 完全无隔离,外部持锁时共 6 个测试假失败(`SystemExit: 1`)。新增调 `main()` 的测试模块无需再自行 patch。

---

## 🔔 无人值守运行的已核实事实

夜跑无人值守且次日 09:30 才需要用数据，因此下面三条直接决定「出问题时你能否知道」。

### 1. 通知默认**不外发**，必须显式配置
- `core/notifications.py` 的 `NOTIFICATION_TYPE` 默认 `"console"` —— **只写日志，不推送**。
  要真正收到通知：`.env` 里设 `NOTIFICATION_TYPE=bark`（可多通道，逗号分隔 `webhook,bark`）
  并填 `BARK_DEVICE_KEY`；`webhook` 通道另需 `NOTIFICATION_WEBHOOK_URL`（飞书/钉钉/企业微信通用文本格式）。
- `NOTIFICATION_LEVEL` 默认 `error`，会抑制 `warning`（保留旧数据）与 `info`（全部完成）。
- Bark 推送**失败时仍返回 HTTP 200**，成败看响应体 `code` —— 通道实现已处理，不要改回只看 HTTP 状态。
- 配置项一律**调用时**读环境变量（`_env()`），**不要改回导入期模块常量**：那样导入之后的一切改动
  （运行期切换配置、测试的 `monkeypatch`）都被静默忽略，`monkeypatch.delenv` 也清不掉它，于是
  本机配了凭据就会让「缺凭据」用例假红、而 CI 假绿。
- `.env` 由 `core.config` 在导入时加载，故 `core/notifications.py` 主动保证它已加载
  （那行 import 不要删，否则入口导入顺序会静默让配置读成空值）。

### 2. 空洞必须登记，否则告警会失去意义
- `core/known_gaps.py` 是**已声明、已接受、且经核实不可回补**的空洞登记册；
  `health_check` 只对**不在册**的空洞告警。
- 增删条目要过 `tests/test_known_gaps.py`（钉住 6 个股息率空洞）——为了让报告变绿而删条目会直接红。
- 已登记的 6 个 `fundamentals.dividend_yield` 空洞为何回补不了：雪球接口只给实时值、
  `historical_valuation` 是 `fundamentals` 的副本（同样为空）、`dividend_summary` 无 `trade_date`。

### 3. 「这一轮没跑完」是可编程信号
- `core/run_state.py` 在 `task_runs` 写 `in-progress:<date>` / `complete:<date>`；
  `run_all` 下一轮开始时发现遗留 `in-progress` 即告警，`health_check` 是第二道防线。
- 造洞机制正是部分运行：09-17 只跑 31 个任务（有 `update_fundamentals`、无 `update_market_snapshot`），
  而这 31 个任务**全部 success**，所以当时没有任何告警。不要把它当成「看一眼日志就好」。

### 4. 写路径必须区分「抓到」与「写入」
- `update_market_snapshot` 的 `saved` 是**真实写入行数**，且抓到报价但 `updated == 0` 时返回
  `failed/data_quality`。旧版返回 `saved=len(all_quotes)` + 硬写 `success`，于是
  `ingestion_runs` 记下 `saved_rows=5203` 而表里整列为 NULL（2026-08-12），且因作用域是
  `MAX(trade_date)`，该日期此后永不被回访 → 静默永久空洞。新增写库任务请沿用同一原则。

### 5. 测试必须 hermetic：不允许真实外发通知
- `tests/conftest.py::_block_external_notifications`（autouse）把 `core.notifications.urlopen`
  换成记录后拒绝调用的桩。通知就挂在真实故障路径上（`safe_task` 异常终止、AkShare 熔断中止、
  整轮汇总），而测试刻意覆盖这些分支——**实测：本机配好通道后跑一次全量 `pytest`，守卫共拦下
  14 次外发尝试（其中 13 次来自那些覆盖失败路径的用例，另 1 次来自守卫用例自身），全部指向
  `api.day.app/push`，内容是夹具名与假失败**。
- 这层副作用**只存在于配了通道的机器上**：CI 没有 `.env`，所以门禁会在本地红、在 CI 绿。
  守卫把测试结果与「本机是否配了通知」彻底解耦。
- 断言真实外发行为的用例自行 patch `notifications.urlopen`（测试体内优先级更高）；反证
  「确实尝试外发但被拦下」的用例直接请求该 fixture 拿拦截列表，见
  `tests/test_notifications.py::test_external_notification_is_blocked_in_tests`。

---

## 🧪 回归测试规范(red-proof)

每项 P0/P1 修复必须附带**会因旧代码变红**的测试(不能只在"新状态"下断言绿):

- 例:P0-2 锁 unlink 修复 → `tests/test_lock.py::test_release_keeps_lockfile_with_empty_pid` 断言旧实现会丢失文件。
- 例:P0-3 schema 硬失败 → `tests/test_schema_baseline.py::test_baseline_tables_all_created`(断言 28 张基线表都存在)与 `tests/test_schema_baseline.py::test_ensure_tables_fails_closed_on_ddl_error`(断言 DDL 失败抛异常,而非只打 WARNING)。
- 提交时 commit message 写明"还原旧实现会让 X 测试全红" → 审计方据此反证钉住。

---

## 📐 历次审计发现与修复记录(已全部闭环)

编号沿用原始审计报告,因此**不连续**:本文件只登记 P1-5 起的条目,更早的 P0/P1 项未在此重列。下表是状态与理由存档,不是待办清单;`docs/todo.md` 另记数据源 TODO。

| 编号 | 级别 | 现状 |
|---|---|---|
| P1-5 mypy CI 空转(`\|\| true` + 缺 `[tool.mypy]`) | P1 | ✅ 已修复 (PR #110):CI 跑无参数 `uv run mypy`,范围由 `[tool.mypy] files` 决定,且已含 `tests/` |
| P1-6 `date: str = None` ×7 协议违规 | P1 | ✅ 已修复 (PR #110):`SmartMoneyDBProvider` 现已满足 `DatabaseInterface` |
| P1-7 `TaskSpec.callable` 全是 None,registry 非单一真相 | P1 | ✅ 已修复 (PR #110):字段改为可选,自述"别处会填"的 `pass` 分支换成真实不变量校验 |
| P2-8 非日频任务仍在 `run_all` wiring（原述:「10 个任务被 cadence 永久跳过但仍在 stage4 wiring」） | P2 | ✅ 已核实为**不实观察** (PR #118):10 个是整个 `run_all` wiring 的计数(stage4 占 8 个,另两个是 stage1 `update_stock_list` 与 stage2 `update_china_macro`),且各自都在 weekly/monthly 层执行——全注册表没有落在三层之外的 cadence。wiring 保留:它是「改 cadence 即生效」的单一生效点 |
| P2-8b 上述排查中发现：`core/freshness.py` 的非日频表集合漏 2 张表；`update_industry` 声明了不存在的表 `industry` | P2 | ✅ 已修复 (PR #118):补 `fund_holdings`/`top10_shareholders` 进 `QUARTERLY_TABLES`(漏掉会在面板上永久显示「滞后」),并加规则型门禁 `tests/test_freshness.py::test_non_daily_task_tables_must_be_classified_as_non_daily`;`update_industry` 的 `tables` 改为它真正写的 `stock_list` |
| P2-9 `PARALLEL_WORKERS` 默认 1 vs help 写 4 | P2 | ✅ 已修复 (PR #114):代码默认对齐生产生效值 3(`.env` 实测),help 修正为真实作用域(仅 stage4 与 bars 内部池),ULTRA_SAFE 钉回 1 |
| P2-10 `_to_float` 在 13 个模块重复 | P2 | ✅ 已修复 (PR #115):抽到 `core/utils.py` 的 `to_float`(非新建 `ak_utils.py`);`strip_percent` 开关只给 `stock_pledge`(其接口返回 `"3.5%"`),其余 12 个模块行为逐字不变(其中 `tasks/hkscc_holder.py` 已于 PR #117 删除;现存 12 个 `tasks/` 模块复用该实现,含 `stock_pledge` 的适配器) |
| P2-11 `get_*_latest_date` 吞 `Exception` | P2 | ✅ 已修复 (PR #114):6 处收窄至 `(sqlite3.Error, OSError)` 并 WARNING 告警,`None` 仅代表「表空/无行」 |
| P2-12 `update_market_snapshot` 静默空写:`saved` 记的是抓取数且硬写 `success` | P2 | ✅ 已修复 (PR #121):`saved` 改为真实写入行数,抓取非空但 `updated == 0` 时返回 `failed/data_quality`,`error` 里区分「源没给字段」与「日期对不上」;仅真正达标才落「已完成」标记。红证:还原旧实现会让 `tests/test_valuation_chain_integration.py::TestUpdateMarketSnapshotWithToken` 三个用例变红(含 `assert 2 == 1`) |
| P2-12b 上条造成的 6 个不可回补空洞 + 缺检测/修复环节 | P2 | ✅ 已处理 (PR #121):新增空洞登记册 `core/known_gaps.py`(6 个日期,已核实不可回补),`health_check` 改为扫描审计期全部日期并只对**新增**空洞告警;登记册条目被删会红(`tests/test_known_gaps.py`) |
| P2-12c 部分运行无告警、无自愈(09-17 只跑 31 个任务且全 success) | P2 | ✅ 已修复 (PR #121):新增 `core/run_state.py` 运行完整性标记,`run_all` 下一轮开始即检测上一轮遗留的 `in-progress` 并告警,`health_check` 作第二道防线 |
| P2-13 「无人值守告警」实际只写日志(`NOTIFICATION_TYPE` 默认 `console`) | P2 | ✅ 已修复 (PR #121):新增 Bark 通道(按响应体 `code` 判成败,不只看 HTTP 200)、`NOTIFICATION_TYPE` 支持逗号分隔多通道、缺凭据明确告警;`.env` 加载顺序隐患一并消除 |
| P2-16 跨仓库路径注入在 4 处各抄一遍(`core/config.py`/`daily_pipeline.py`/`tasks/bars.py`/顶层 `__init__.py`),而 `core/utils.py`/`providers.py`/`tasks/valuation_chain.py` 有模块级跨仓库 import 却完全不注入 | P2 | ✅ 已修复 (PR #124):注入收敛到 `core/_bootstrap.py`,包级 choke point 置于 `core/__init__.py`/`tasks/__init__.py`,顶层模块各显式调一次。排查中修掉另外两处真缺陷:①顶层 `__init__.py` 只注入 `~/Code`——它不是仓库根也不是 `smartmoney_hunter` 的所在,故以包身份导入一直是「import 得进、子模块用不了」(`cd ~/Code && import quant_pipeline.providers` 实测 ModuleNotFoundError);②`scripts/audit_data_contracts.py` 缺仓库根注入,`uv run python scripts/audit_data_contracts.py --db <path> --json` 连 `--help` 都跑不起来——而 `tasks/utility.py` 正是把这条命令打印给运维的。顺带删除 `~/Code` 注入(两项独立证据表明无消费者:本仓库不 import 该目录下任何一级名字,`smartmoney_hunter` 自身只 import 标准库与第三方;`Trading_Agents` 甚至不存在),并加门禁阻止它回来。红证:还原旧实现会让 `tests/test_sys_path_bootstrap.py` 5 个用例变红(`core.utils`/`providers`/`tasks.valuation_chain` 的独立导入、包身份导入、`audit_data_contracts.py` 的脚本规则) |
| P2-15 测试套件不 hermetic:配好通知通道后跑测试会真的外发推送,且该副作用只在本地存在(CI 无 `.env` 故恒绿) | P2 | ✅ 已修复 (PR #123):`core/notifications.py` 的配置由导入期常量改为**调用时**读环境变量(常量会让 `monkeypatch.delenv` 失效,本机配了凭据时「缺凭据」用例静默变成「有凭据」);新增 autouse 守卫 `tests/conftest.py::_block_external_notifications`,把模块唯一出网点 `urlopen` 换成记录后拒绝调用的桩。红证:还原旧实现会让 `tests/test_notifications.py::test_level_is_read_at_call_time_not_cached_at_import` 与 `::test_channel_credentials_are_read_at_call_time` 变红,且在配好通道的条件下 `::test_bark_channel_without_device_key_skips_network`/`::test_channels_warn_when_type_requested_without_credentials` 一并变红(正是本机实测的两个失败);把守卫生效行改为空操作则 `::test_external_notification_is_blocked_in_tests` 变红(`assert 0 == 1`,日志里出现真实 `HTTP Error 400`——请求确已离开本机) |
| P2-14 `ingestion_runs.attempts` 列装的是记录条数而非重试次数(`update_bars`=5565 / `update_index_membership`=3850) | P2 | ✅ 已修复 (PR #122):该列真实语义是重试次数(`core.refresh` 写入的 `metadata["attempts"]`),而「本轮检查了多少条记录」在本表**没有对应列**。因 `ingestion_runs` 是两仓库共享的 canonical 契约(`quant_hunter` 的 `db_schema.py` 有逐字相同的 DDL),**不改列名**、只修正写入方:列改取 `metadata["attempts"]`(未跟踪重试写 0),记录条数并入 `metadata_json` 不静默丢弃。红证:还原旧实现会让 `tests/test_providers_extended2.py::test_attempts_column_records_retry_count_not_record_count` 与 `::test_attempts_column_zero_when_retry_count_not_reported` 变红(`assert 5565 == 2` / `assert 5565 == 0`) |
