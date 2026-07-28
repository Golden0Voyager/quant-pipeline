# 收盘刷新全部交易日任务设计

日期：2026-07-28

## 背景

当前 `--force` 的主要语义是绕过交易日或运行时间检查，并不保证覆盖已经存在的
当日数据。部分任务还有自己的增量跳过条件：

- `update_bars` 发现目标交易日已经存在时逐只跳过；
- `update_indicators` 发现指标日期已追平日线时跳过；
- `update_fundamentals` 在当日记录数达到阈值时跳过；
- `update_market_snapshot` 在股息率覆盖率达到阈值时跳过。

因此，盘中运行全量管道后，收盘再执行 `--force` 仍可能保留盘中快照。直接把
`--force` 改成“删除并覆盖”又会扩大现有参数的破坏性，并在数据源失败时造成数据
空洞。

## 目标

新增显式的 `--refresh-today` 收盘刷新模式：

1. 覆盖任务注册表内全部 `Cadence.TRADING_DAY` 任务；
2. 绕过当日完成标记、增量跳过和网络缓存；
3. 新数据通过校验后才替换旧数据；
4. 单个任务失败不得删除或破坏该任务的旧数据；
5. 刷新后执行统一的日期、新鲜度、覆盖率和字段质量审计；
6. TUI 提供明确的“收盘刷新全部交易日任务”入口；
7. 保持 `--force` 原有语义，不让它隐式变成覆盖操作。

## 非目标

- 不刷新 `DAILY`、`MONTHLY`、`QUARTERLY` 或 `ON_DEMAND` 任务；
- 不包含 `update_chip_distribution_em_fullmarket`；
- 不重写历史交易日；
- 不在数据源失败时先删除旧数据；
- 不改变 AkShare 优先、禁用 yfinance 写入黄金数据库的策略；
- 不把所有任务强行改造成相同的日期删除逻辑。

## CLI 与 TUI 契约

新增命令：

```bash
rtk uv run python daily_pipeline.py --refresh-today
```

行为：

- 目标日期由 `get_expected_latest_trading_day()` 计算；
- 时间判断显式使用 `ZoneInfo("Asia/Shanghai")`，不依赖运行机器本地时区；
- 默认仅在北京时间 16:00 后允许；
- 16:00 前必须同时提供 `--force`，并输出高可见度风险警告；
- CLI 将 `--refresh-today` 与显式 `--task` 设为互斥顶层模式；
- `--task` 的解析默认值改为 `None`，两种模式均未指定时仍解析为原有 `all`，
  保持无参数启动兼容；
- `--refresh-today` 与 `--resume` 互斥；
- 允许 `--symbols`，但只对支持股票范围过滤的任务生效；
- 不支持范围过滤的任务仍按全市场执行，并在日志中明确说明；
- `--refresh-today` 自身已表达覆盖意图，收盘后不需要再带 `--force`。

TUI 新增独立操作“收盘刷新”：

- 显示目标交易日、任务数量和刷新范围；
- 要求用户确认；
- 与普通“全量更新”和“断点续传”分开；
- 显示各任务的 `pending/running/success/degraded/failed` 状态。

## 覆盖范围

规格覆盖当前注册表中的 29 个 `TRADING_DAY` 任务。

### 核心行情与衍生数据

1. `update_bars`
2. `update_indicators`
3. `update_fundamentals`
4. `update_market_snapshot`
5. `update_fund_flow`
6. `update_chip_distribution`
7. `update_chip_distribution_em`

### 市场交易与榜单

8. `update_margin_trading`
9. `update_dragon_tiger`
10. `update_block_trade`
11. `update_sector_fund_flow`
12. `update_limit_up_down`
13. `update_option_sentiment`

### 跨市场、指数与估值

14. `update_historical_valuation`
15. `update_sector_industry`
16. `update_north_flow`
17. `update_south_flow`
18. `update_ah_premium`
19. `update_etf_daily`
20. `update_index_daily`
21. `update_market_valuation`
22. `update_sector_derivatives`

### 可转债

23. `update_cb_quotation`
24. `update_cb_redeem`
25. `update_cb_index`

### 板块与事件信号

26. `update_concept_board`
27. `update_stock_repurchase`
28. `update_institution_survey`
29. `update_stock_pledge`

## 架构

### RefreshPolicy

在任务注册元数据中为每个 `TRADING_DAY` 任务声明刷新策略。建议使用显式枚举，而
不是根据表名或日期列进行猜测：

- `REMOTE_DATE_SNAPSHOT`：远端目标日期快照；
- `REMOTE_KEYED_UPSERT`：远端按自然键返回的历史或事件数据；
- `DERIVED_RECOMPUTE`：由刷新后的本地数据重新计算；
- `COMPOSITE_ATOMIC`：一个任务同时维护多张必须一致提交的表；
- `REMOTE_RUN_SNAPSHOT`：以抓取时刻为版本、没有可靠业务日期的快照。

每个策略同时声明：

- 涉及的表；
- 业务日期列或自然键；
- 日期策略 `EXACT_TARGET`、`LATEST_AVAILABLE_WITHIN_LOOKBACK` 或
  `RUN_SNAPSHOT`；
- 是否支持 `--symbols`；
- 最低覆盖率；
- 必填字段；
- 缓存命名空间；
- 依赖任务。

CI 增加完整性测试：任何新增 `TRADING_DAY` 任务如果没有刷新策略，测试必须失败。

### RefreshOrchestrator

新增独立收盘刷新编排器，职责仅包括：

1. 解析目标日期并执行时间门禁；
2. 按依赖顺序生成刷新计划；
3. 为任务创建刷新上下文；
4. 调用任务适配器；
5. 执行任务级审计；
6. 聚合结构化结果和失败队列。

它不直接包含具体数据源解析逻辑，也不通过通用 SQL 猜测如何删除任务数据。

### RefreshContext

刷新上下文至少包含：

- `target_date`；
- `started_at`；
- `bypass_cache=True`；
- 可选 `symbols`；
- 唯一 `run_id`；
- `refresh_mode="close_refresh"`。

需要支持刷新行为的任务以显式参数或专用适配器消费该上下文。禁止依赖全局环境变量
隐式改变普通管道行为。

## 各类任务的刷新语义

### 目标日期快照

适用于日线、资金流、龙虎榜、融资融券、指数、ETF、涨跌停、板块行情等具有可靠
业务日期的数据。

流程：

1. 绕过缓存并从远端获取目标日期数据；
2. 写入临时 staging 表或同连接临时表；
3. 校验日期、行数、必填字段和自然键唯一性；
4. 在一个事务中删除正式表目标日期旧行并插入 staging 新行；
5. 提交后清理 staging。

使用“目标日期整体替换”而不是单纯 UPSERT，避免盘中存在、收盘已消失的旧行残留。

并非所有交易日来源都在收盘后立即发布当日数据。融资融券等任务使用
`LATEST_AVAILABLE_WITHIN_LOOKBACK`：任务先确定源端实际可用日期，只有该日期位于
策略允许的回看窗口内才提交，并把实际日期记录为 `as_of_date`。审计不得把合规的
源端延迟误报成目标日期缺失。

### 自然键 UPSERT

适用于回购、机构调研、质押、强赎等事件型数据。这些任务可能返回跨日期数据，不能
仅删除 `trade_date=target_date`。

流程：

1. 获取远端完整结果；
2. 校验自然键唯一性和最低覆盖要求；
3. 在事务内按自然键 UPSERT；
4. 仅在任务本身定义了完整快照边界时清理远端已不存在的记录。

不得根据注册表日期列进行通用日期删除。

### 派生重算

`update_indicators`、`update_chip_distribution` 和
`update_chip_distribution_em` 在日线刷新完成后运行：

- 只处理日线实际发生变化的股票；
- 目标日期结果通过质量校验后覆盖；
- 日线刷新失败的股票不重算，并继承到失败队列；
- 不因为单只股票失败而删除其他股票的有效派生数据。

### 复合原子任务

`update_sector_derivatives` 同时维护 `sector_daily` 和
`sector_valuation`。两张表必须全部抓取并校验成功后，在同一事务中替换目标日期。

### 运行时快照

`update_cb_quotation` 和 `update_cb_redeem` 使用 `updated_at`，不能把
`updated_at` 当成稳定业务日期进行删除。刷新以本次 `run_id` 暂存新快照，校验后
按任务定义的自然键替换当前快照。

## 核心任务特殊规则

### 日线

- 忽略“最新日期已达到目标日期”的快速跳过；
- 网络请求不得先命中 `quant_core.db`；
- 跳过 `daily_bars` 缓存，并在成功后更新缓存；
- 新行必须通过 OHLC、成交量、成交额和数据源校验；
- 使用 `(ts_code, trade_date)` 替换目标日期；
- 拉取失败时保留原盘中行，但将其标记为未刷新并进入失败队列。

### 基本面与雪球快照

- 基本面忽略当日 5000 行的完成阈值；
- 收盘结果按目标日期整体替换基本面基础字段；
- 雪球快照必须允许覆盖已有非空 `dividend_yield`；
- 雪球只更新自己负责的字段，不覆盖基本面的 PE、PB、PEG 等字段；
- 任一来源失败时保留对应旧字段。

### 资金流

- 重新抓取全市场收盘资金流；
- 对目标日期进行 staging 后整体替换；
- 全空数值行不得进入正式表；
- 覆盖率低于策略阈值时不替换旧数据。

## 执行顺序

依赖顺序固定为：

1. 远端基础行情：日线、指数、ETF、可转债行情；
2. 基础快照：基本面、雪球快照、资金流及交易榜单；
3. 跨市场、板块、估值、期权和事件信号；
4. 本地派生：指标和两类筹码分布；
5. 统一收盘审计与失败重试。

同一层内没有共享写表的任务可并行；共享表或存在明确依赖的任务必须串行。特别是
`update_fundamentals` 和 `update_market_snapshot` 共同写入 `fundamentals`，
必须按顺序执行。

## 校验与抽检

### 每任务强制校验

- `EXACT_TARGET` 任务必须命中目标日期；
- `LATEST_AVAILABLE_WITHIN_LOOKBACK` 任务的 `as_of_date` 必须位于允许窗口；
- `RUN_SNAPSHOT` 任务必须属于本次 `run_id`；
- 自然键无重复；
- 必填字段非空；
- 行数或覆盖率达到任务策略阈值；
- 存在 `updated_at` 列时，本轮写入行不得早于 `started_at`；没有该列时使用
  ingestion run 记录证明本轮提交；
- 数值范围和表级不变量合法。

### 日线质量

- `high >= max(open, close)`；
- `low <= min(open, close)`；
- `high >= low`；
- 成交量和成交额非负；
- 活跃非停牌股票覆盖率达到配置阈值；
- 数据源不是纯 yfinance。

### 跨源抽检

- 从沪、深、创、科、北交所中按备用源实际支持范围分层抽取约 30 只股票；
- 使用与主抓取不同的数据源进行收盘价和成交量对比；
- 价格和成交量使用分别配置的容差；
- 超出容差的股票进入一次定向重试；
- 抽检失败不触发盲目全市场删除或重抓。

## 错误处理

- 获取或校验失败：不触碰正式表旧数据；
- staging 替换失败：事务回滚；
- 部分股票失败：成功股票正常提交，失败股票保留旧值并进入队列；
- 复合任务任一表失败：整个复合事务回滚；
- 自动定向重试最多一次；
- 重试后仍失败：任务状态为 `degraded`；
- 关键基础任务完全失败：状态为 `failed`，依赖任务标记 `blocked`；
- 编排器继续执行不依赖失败任务的其他任务；
- 最终退出码对 `degraded/failed/aborted` 保持非零。

## 运行记录与可观测性

每次收盘刷新记录：

- `run_id`、目标日期、开始和结束时间；
- 每个任务的策略、状态、抓取数、校验数、替换数和失败数；
- 旧数据保留数；
- 缓存绕过情况；
- 抽检样本及差异摘要；
- 自动重试结果；
- 失败队列。

TUI 和日志必须区分：

- 未运行；
- 已抓取但未通过校验；
- 已提交覆盖；
- 保留旧数据；
- 部分降级；
- 被依赖任务阻塞。

## 兼容性

- 普通 `--task`、`--task all`、`--resume` 和 `--force` 行为保持不变；
- 现有任务无需刷新时继续沿用当前增量逻辑；
- 任务旧式计数字段继续保留，并补充统一 `TaskResult` 状态；
- 数据库迁移必须幂等；
- staging 和运行记录不得改变现有业务表主键；
- 刷新模式不允许 yfinance 数据写入 `quant_core.db`。

## 测试策略

### 注册表契约

- 所有 29 个 `TRADING_DAY` 任务均有刷新策略；
- 其他 cadence 不会被刷新编排器选择；
- 新增未声明策略的交易日任务触发测试失败。

### CLI 与 TUI

- 收盘后允许执行；
- 收盘前拒绝，除非同时传入 `--force`；
- 与 `--resume`、`--task all` 冲突时明确失败；
- `--symbols` 仅传递给支持范围过滤的任务；
- TUI 确认后启动正确命令，取消时不启动。

### 数据安全

- 抓取失败时旧数据保持不变；
- 校验失败时旧数据保持不变；
- staging 替换异常时完整回滚；
- 目标日期整体替换会移除盘中残留行；
- 事件型任务不会被目标日期通用删除；
- 复合任务保持跨表原子性。

### 集成验收

使用临时 SQLite 数据库预置盘中数据，执行收盘刷新后验证：

- 目标日期数据被收盘数据替换；
- 历史日期未变化；
- 失败任务保留盘中数据并报告降级；
- 指标和筹码只重算日线变化股票；
- 审计能够识别覆盖不足、过期更新时间和非法 OHLC；
- 完整 pytest、Ruff、mypy 和 `git diff --check` 达到项目门禁。

## 分阶段交付

### 阶段一：基础设施

- RefreshPolicy 和注册表覆盖契约；
- RefreshContext 与 RefreshOrchestrator；
- staging、事务替换、运行记录和通用审计；
- CLI 时间门禁与结果聚合。

### 阶段二：核心行情

- 日线、指标、基本面、雪球快照、资金流；
- 指数、ETF、市场榜单；
- 失败队列和跨源抽检。

### 阶段三：完整交易日覆盖

- 可转债、板块、跨市场、估值、期权和事件信号；
- 筹码派生任务；
- TUI 操作、状态展示和最终端到端验收。
