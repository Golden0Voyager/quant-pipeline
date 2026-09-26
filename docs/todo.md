# TODO

## 1. col_map 全局统一规范（下一阶段）

当前 AkShare 调用点散落在各个 `tasks/*.py` 中，列名直接硬编码为中文，无保护层。

### 现状问题

- 约 40+ 个 AkShare 调用点通过 `row.get("中文字段名")` 直接访问原始 DataFrame 列，**无任何翻译/兜底层**
- 调用模式不统一：有的有 col_map，有的直接硬编码，有的用 safe filter
- 如果 AkShare 版本升级改了列名（如 `"ON"` → `"O/N-定价"`），数据静默丢失，无告警

### 目标

建立一套统一规范，覆盖所有 AkShare 调用：

1. **标准调用模式**
   - 每个 AkShare API 调用入口有 col_map 定义
   - col_map 支持**多别名**（同一字段的不同历史列名）
   - 执行 rename 后只保留已映射的列
   - 如果某列完全无匹配，发出 warning 日志

2. **列名漂移检测**
   - 运行时检查：AkShare 返回的原始列名中，是否有大量列未命中 col_map 的 key
   - 如果是，打印 warning 并列出"未知列名"
   - 这样 AkShare 版本升级后第一时间就能发现，而不是静默丢数据

3. **统一模式推广到所有调用点**
   - `macro.py`（12 个调用）
   - `market_flow.py`（4 个）
   - `convertible_bond.py`（3 个）
   - `corporate_actions.py`（2 个）
   - `financials.py`（2 个）
   - `money_market.py`（3 个已有硬编码，需添加 col_map）

### 参考实现

`tasks/money_market.py` 中 `_fetch_shibor()` 的 safe filter 模式：

```python
col_map = {
    "O/N-定价": "shibor_on",
    "1W-定价": "shibor_1w",
    # ...
}
df = df.rename(columns=col_map)
available = [c for c in keep if c in df.columns]
df = df[available]
```

可在此基础上提取一个公共工具函数（如 `ak_utils.safe_rename(df, col_map, keep)`），统一所有调用点的行为。

---

## 2. etf_daily / sector_daily 开盘后重试

当前阻塞的两个表：

| 表 | 后端 | 阻塞原因 |
|----|------|----------|
| `sector_daily` / 行业涨跌幅 | East Money `push2.eastmoney.com` | 服务器在非交易时段阻断连接 |
| `etf_daily` | East Money `push2his.eastmoney.com` | 同上 |

**恢复方式**：在 A 股交易时段（9:30-15:00）重新运行即可自动恢复。

```bash
# 单独重试
uv run python daily_pipeline.py --task update_etf_daily
uv run python daily_pipeline.py --task update_sector_industry  # 实际为 sector_daily

# 或等下次全量运行（pipe-run）自动重试
```

---

## 3. 高价值数据接入清单（2026-09 复核）

> **复核结论（2026-09-22）**：原 10 项中 **7 项已接入**，仅 3 项仍未做。已接入项标记 ✅ 并指向实际任务；未做项保留待办。

### ✅ 3.1 沪深港通持股个股明细 — 已接入

| 项目 | 内容 |
|------|------|
| **实际任务** | `update_north_hold`（`tasks/macro.py`） |
| **数据源** | 东财数据中心分页 API `RPT_MUTUAL_HOLDSTOCKNORTH_STA`（季度快照，全市场 ~3900 只） |
| **表** | `north_hold`（ts_code, trade_date, hold_shares, hold_market_cap, hold_shares_ratio, …） |
| **说明** | HKEX 自 2024-08-19 停止每日个股北向披露，故为**季度快照**（非日频）。旧方案 `tasks/hkscc_holder.py`（逐股轮询 5500 只）已于 2026-09-24 删除：它写同一张 `north_hold` 表，但机制与数据源已被本任务整体取代，且 `26c081e` 早已将其 de-wire（同类处置先例：`insider_trading` 的孤儿文件删除 `bb097ec`） |

### 3.2 股东增减持 — 未做

| 项目 | 内容 |
|------|------|
| **AkShare API** | `stock_gdfx_free_holding_change_em` / `stock_gdfx_holding_change_em`（东财股东增减持，按季度） |
| **当前状态** | 无；akshare 无 `stock_share_holder_change`（todo 旧文有误） |
| **量化价值** | 董监高/大股东增减持是内部人交易信号，公告级别数据 |
| **实现建议** | 事件驱动型任务，存 `shareholder_change` 表，字段：ts_code, date, name, change_type, volume, price, ratio |
| **阻塞** | 东财 datacenter API 当前 SSL 不稳定（2026-09-22 实测失败）；恢复后再接入 |

### ✅ 3.3 十大流通股东 / 十大股东 — 已接入

| 项目 | 内容 |
|------|------|
| **实际任务** | `update_top10_shareholders`（`tasks/top10_shareholders.py`） |
| **数据源** | 新浪财经 `stock_main_stock_holder()`（Sina，非东财；东财 API 已失效） |
| **表** | `top10_shareholders`（ts_code, report_date, holder_rank, holder_name, shares_held, share_ratio, share_nature, announcement_date） |
| **Cadence** | QUARTERLY（挂入 monthly_repair，20 线程并行） |
| **说明** | Sina API 返回全量历史，按"截至日期"取最新报告期；~5500 只 × 10 股东 ≈ 5.5 万行/期 |

### ✅ 3.4 机构调研 — 已接入

| 项目 | 内容 |
|------|------|
| **实际任务** | `update_institution_survey`（`tasks/institution_survey.py`） |
| **表** | `institution_survey`（TRADING_DAY cadence） |

### ✅ 3.5 概念板块 — 已接入（东财源）

| 项目 | 内容 |
|------|------|
| **实际任务** | `update_concept_board`（`tasks/concept_board.py`）+ `update_concept_member` |
| **表** | `concept_board` / `concept_member` / `concept_member_history` |
| **说明** | 东财 push2 直调（双主机 failover）。**同花顺第二源未接**（THS 375 概念 vs EM 504，code 格式纯数字不兼容 EM 的 BK 前缀，逐条调用开销大）——评估后放弃，详见 2026-09 调研 |

### ✅ 3.6 股票质押 — 已接入

| 项目 | 内容 |
|------|------|
| **实际任务** | `update_stock_pledge`（`tasks/stock_pledge.py`） |
| **表** | `stock_pledge`（WEEKLY cadence） |

### ✅ 3.7 股票回购 — 已接入

| 项目 | 内容 |
|------|------|
| **实际任务** | `update_stock_repurchase`（`tasks/stock_repurchase.py`） |
| **表** | `stock_repurchase`（TRADING_DAY cadence） |

### ✅ 3.8 基金持股 — 已接入

| 项目 | 内容 |
|------|------|
| **实际任务** | `update_fund_holdings`（`tasks/fund_holdings.py`） |
| **数据源** | 东财主力数据中心 `RPT_FUND_HOLD_STOCK`（6 类机构：基金/QFII/社保/券商/保险/信托） |
| **表** | `fund_holdings`（ts_code, stock_name, report_date, institution_type, fund_count, total_shares, hold_value, hold_ratio, update_kind, update_shares, update_ratio, data_source） |
| **Cadence** | QUARTERLY（挂入 monthly_repair） |
| **数据量** | ~5000 只 × 6 类机构 × 4 季/年 ≈ 12 万行/年 |

### ✅ 3.9 大盘估值指标 — 已接入（乐咕）

| 项目 | 内容 |
|------|------|
| **实际任务** | `update_market_valuation`（`tasks/market_valuation.py`） |
| **数据源** | 乐咕乐咕：`stock_a_ttm_lyr`（全A PE）、`stock_a_all_pb`（全A PB）、`stock_ebs_lg`（股债利差） |
| **表** | `market_valuation`（date, pe_median, pb_median, equity_bond_spread, csi300_close, data_source='legu'） |
| **状态** | TRADING_DAY cadence，最新数据已到当日 |

### ✅ 3.10 期权数据 — 已接入

| 项目 | 内容 |
|------|------|
| **实际任务** | `update_option_sentiment`（`tasks/option_sentiment.py`） |
| **表** | `option_sentiment`（TRADING_DAY cadence） |

---

## 4. 新数据源调研

### ✅ 4.1 乐咕乐咕 (legulegu.com) — 部分接入

**估值指标类（已接入 → `tasks/market_valuation.py`）：**
- ✅ `stock_a_ttm_lyr()` — 全A PE（LYR+TTM、等权+中位数）→ `market_valuation.pe_median/pe_quantile`
- ✅ `stock_a_all_pb()` — 全A PB（等权+中位数）→ `market_valuation.pb_median/pb_quantile`
- ✅ `stock_ebs_lg()` — 股债利差（FED spread）→ `market_valuation.equity_bond_spread/ebs_ma`
- ⏳ `stock_buffett_index_lg()` — 巴菲特指标（总市值/GDP + 分位数）→ `csi300_close` 已存，GDP 分位数未存
- ⏳ `stock_a_congestion_lg()` — 大盘拥挤度 → 未接入
- ⏳ `stock_market_pe_lg()` / `stock_market_pb_lg()` — 上证/深证/创业板/科创板 PE+PB → 未接入
- ⏳ `stock_index_pe_lg()` / `stock_index_pb_lg()` — 12 个主流指数 PE+PB（等权/加权/中位数）→ 未接入
- ⏳ `stock_a_gxl_lg()` / `stock_hk_gxl_lg()` — 市场级股息率时间序列 → 未接入

**市场宽度类（另类数据信号）：**
- `stock_a_high_low_statistics()` — 20/60/120 日新高新低计数
- `stock_a_below_net_asset_statistics()` — 破净股数量 + 占比历史
- `stock_market_activity_legu()` — 赚钱效应（涨跌/涨停/跌停家数）

**基金仓位类：**
- `fund_stock_position_lg()` — 股票型基金仓位
- `fund_balance_position_lg()` — 平衡混合型基金仓位
- `fund_linghuo_position_lg()` — 灵活配置型基金仓位

**接入状态（2026-09 复核）**：
- ✅ 高优先级：策略信号类（股债利差、全A PE/PB）→ 已在 `tasks/market_valuation.py`，TRADING_DAY 每日运行
- ⏳ 中优先级：市场宽度类（新高新低/破净/赚钱效应）→ 未接入，可并入 limit_up_down 或新建任务
- ⏳ 低优先级：基金仓位 / 巴菲特分位数 / 指数 PE+PB → 等需求明确后再做

### 4.2 同花顺 (10jqka) — 30+ 个函数

**核心差异化：技术选股系列（东财无）：**

| 函数 | 说明 |
|------|------|
| `stock_rank_cxg_ths` / `stock_rank_cxd_ths` | 创新高/新低（月/半年/一年/历史） |
| `stock_rank_lxsz_ths` / `stock_rank_lxxd_ths` | 连续上涨/下跌 |
| `stock_rank_cxfl_ths` / `stock_rank_cxsl_ths` | 持续放量/缩量 |
| `stock_rank_xstp_ths` / `stock_rank_xxtp_ths` | 向上/向下突破（5~500日均线） |
| `stock_rank_ljqs_ths` / `stock_rank_ljqd_ths` | 量价齐升/齐跌 |
| `stock_rank_xzjp_ths` | 险资举牌 |

**板块类（不同分类体系）：**
- `stock_board_concept_name_ths()` — 同花顺概念板块列表（分类与东财不同）
- `stock_board_concept_summary_ths()` — 概念板块涨跌概况
- `stock_board_industry_summary_ths()` — 行业板块涨跌概况
- `stock_fund_flow_concept()` — **概念维度资金流**（东财无）

**财务报表类（新版格式）：**
- `stock_financial_abstract_new_ths()` / `stock_financial_debt_new_ths()` / `stock_financial_benefit_new_ths()` / `stock_financial_cash_new_ths()`
- 可在旧版东财报表不稳定时作为备份

**接入建议**：
- 技术选股系列可作为下游因子计算模块的输入，不直接入库
- 概念板块可在将来接入概念板块时与东财概念交叉使用
- 新版财务报表作为东财报表的 fallback

### 4.3 腾讯财经 (QQ) — 7 个函数

| 函数 | 用途 | 评估 |
|------|------|------|
| `stock_zh_a_hist_tx()` | A股日频K线（支持复权） | **Sina 备用源** ⚠️ 缺 volume 列 |
| `stock_zh_index_daily_tx()` | 指数日频K线 | 补充 Sina 缺失的指数数据 |
| `stock_zh_a_tick_tx_js()` | A股分笔(tick)数据 | AkShare 中唯一的分笔源 |
| `stock_zh_ah_spot()` / `stock_zh_ah_daily()` / `stock_zh_ah_name()` | A+H 股 | 已有东财版本，优先级低 |
| `stock_zh_a_spot_tx()` | A股实时行情 | ⚠️ 未导出，不建议用 |

**接入建议**：
- 如果 Sina 日线 API 不稳定，可将腾讯作为 bars 的 fallback 源
- 分笔数据可按需集成到 tick 级分析模块

---

## 5. 数据源优先级总排序（2026-09 复核）

| 优先级 | 来源 | 数据 | 状态 |
|--------|------|------|------|
| 🔴 P0 | 乐咕 | 股债利差、巴菲特指标、全A PE | ✅ 已接入（`update_market_valuation`） |
| 🔴 P0 | 东财 | 沪深港通持股个股明细 | ✅ 已接入（`update_north_hold` 季度快照） |
| 🔴 P0 | 东财 | 股东增减持 | ⏳ 未做（东财 API 暂不稳定） |
| 🟡 P1 | 乐咕 | 市场宽度（新高新低/破净） | ⏳ 未接入 |
| 🟡 P1 | 东财 | 概念板块 | ✅ 已接入（`update_concept_board` 东财源） |
| 🟡 P1 | 东财 | 机构调研 | ✅ 已接入（`update_institution_survey`） |
| 🟢 P2 | 同花顺 | 技术选股系列 | 未接入（因子输入，不直接入库） |
| 🟢 P2 | 东财 | 股票质押/回购/基金持股 | 质押✅ 回购✅ 基金持股✅ |
| ⚪ P3 | 腾讯 | 日线 fallback / 分笔 | 未接入（备用/高级分析） |
| ⚪ P3 | 乐咕 | 基金仓位 | 未接入（低更新频率） |
| ⚪ P3 | 东财/同花顺 | 期权数据 | ✅ 已接入（`update_option_sentiment`） |

---

## ✅ 6. update_financial_history 报告期处理无重试（2026-08-04 观察 → 2026-09-22 已修复）

### 现象

`update_financial_history`（按报告期更新财务历史）6 个报告期全部失败：

| 报告期 | 失败原因 | 耗时 |
|--------|----------|------|
| 20260630 | akshare 瞬时异常（裸字符串 `'20260630'`） | 6s |
| 20251231 | `Response ended prematurely`（HTTP 流截断） | 97s |
| 20250930 | `Response ended prematurely` | 159s |
| 20250630 | `Response ended prematurely` | 48s |
| 20250331 | `Response ended prematurely` | 104s |
| 20241231 | akshare 瞬时异常 | 153s |

任务整体 `[failed]` 耗时 566.9s，下游 `update_quarterly_financials` 等季度/月度任务被跳过。

### 根因

- **非代码 bug**：手动复现 `ak.stock_yjbb_em(date=...)` 两个失败报告期均正常返回（249 / 11644 行）——上游东财接口瞬时限流/断流
- **结构性弱点**：`tasks/financial_history.py:276-278` 每个报告期**单次尝试**，异常即 `continue` 放弃，无重试机制（对比 `update_bars` 有 3 次重试）
- 单报告期连续拉 4 个全量接口（yjbb/lrb/zcfz/xjll + 披露日期表，各 1-2 分钟），上游任一断流即失败

### 修复状态（PR #104，2026-09-22 ✅）

1. ✅ 报告期处理已加 3 次指数退避重试（`_retry` helper，照抄 `sector_derivatives.py`）
2. ✅ 全部报告期失败 → 返回 `retained`（保留旧数据、exit 0），不再 failed 阻塞下游
3. ✅ 失败报告期持久化到待重试队列（PR #138，2026-09-26）：`tasks/financial_history.py` 新增
   文件队列（语义同 `core.progress.ProgressTracker`：flock + 原子替换），失败期次自动记入、
   成功自动清出；`periods=None` 的自动发现路径会把队列期次**并回**待抓取列表——
   补上「覆盖率已达标但不完整」时发现逻辑永远不再回访的盲区。测试隔离见
   `tests/conftest.py::_isolate_financial_period_queue`。

---

## 🟡 7. 单次尝试任务遇上游瞬断即失败（2026-08-04 观察 → 2026-09-22 部分修复）

### 现象

同日日志另有 2 个任务因上游瞬时断连单次失败（与第 6 节同类）：

| 任务 | 失败原因 | 耗时 |
|------|----------|------|
| `update_concept_board` | `ConnectionError: curl (56) Connection closed abruptly` | 1.9s |
| `update_stock_repurchase` | curl 类瞬时断连（Subprocess exit 1） | — |

另：13:24 `update_bars --resume` 5 只停牌股失败、13:29 `retry` 被 SIGTERM（exit -15）——非本次记录范围。

### 根因

- 上游东财/同花顺接口限流或网络抖动时，curl 连接被服务端关闭
- 这些任务与 `update_financial_history` 一样：**单次尝试，失败即放弃**，无重试/退避

### 修复状态（2026-09-22，全部完成）

1. ✅ `update_concept_board` → PR #103 已改 `retained`（源不可用保留旧数据，exit 0）
2. ✅ **8 个高频 daily 任务补 `error_kind=network`**（PR #105）：hk_tech_index、us_macro、cftc_cot、eia_petroleum、lithium_spot、macro.py 的 index_daily/limit_up_down/dividend_summary/gold_price/usd/global_index/us_treasury
3. ✅ **17 个低频任务补 `error_kind=network`**（PR #106 + 直接提交）：convertible_bond×3、futures、china_macro、hkscc_holder、money_market、stock_pledge、stock_repurchase、finance_flow×3、core_chain、corporate_actions×2、financials、index_chain、market_flow×2、valuation_chain×2
4. ✅ **统一重试装饰器**（`tasks/retry_utils.py` 的 `@retry_on_network`）→ 已创建，hkscc_holder 已迁移

---

## 🟡 8. `refresh_runs` / `refresh_task_runs` 近乎死表（close-refresh 从未在生产完成过）

### 现状（2026-09-25 只读复核生产库）

| 表 | 行数 | 内容 |
|----|------|------|
| `refresh_runs` | **3** | 全部 `status='aborted'`，全在 2026-07-30（2 条）与 07-31（1 条） |
| `refresh_task_runs` | **0** | **从未写入过任何一行** |

写入方是 `core/refresh.py::RefreshOrchestrator`（经 `core/refresh_store.py`），入口只有
`daily_pipeline.py --refresh-today`（TUI `U` 键，或单任务下拉里的「收盘刷新」）。读取方只有
`tui/services/refresh_state.py`（收盘刷新状态面板）——因此那个面板设计的「六态」实际上**永远只会显示
「无任务审计记录」**。

### 为什么是 0 行（日志有据）

`refresh_task_runs` 是**每完成一个任务就写一行**（`RefreshOrchestrator._record_task`），所以 0 行意味着
每次尝试都在 29 个任务里的**第一个**完成之前就结束了。当日日志：

```
2026-07-30  --refresh-today exited with code 1     # 14:58，16:00 闸门之前，属正常拒绝（不建 run 行）
2026-07-30  --refresh-today exited with code 143   # SIGTERM（从 TUI 停止）
2026-07-30  --refresh-today exited with code 143
2026-07-31  --refresh-today exited with code 143
```

即 07-30/07-31 共试了 4 次、每次都在第一个任务跑完前被终止，之后**再没运行过**（近两个月）。看上去是
「第一个任务就要几十分钟（全市场、29 个任务、绕过缓存）+ 手动触发 + 得有人守着」的组合劝退了使用者，
而不是某处代码缺陷。

### 两个已核实的“不是”，避免误判

- **不是跨仓库契约**：`~/Code/quant_hunter` 全仓库不引用这两张表（对照：`ingestion_runs.attempts` 是两仓库
  逐字共享的 DDL，改它要两边一起动）。所以这里没有共享 schema 的约束。
- **不是时间戳 bug**：`started_at` 是上海 aware、`finished_at` 是 UTC，格式不一致但**绝对时间一致**
  （已逐行校对）；唯一读取方只拿 `started_at` 排序，而它恒为 `+08:00`，文本序即时间序。真要统一格式可另开小项。

### 决策（2026-09-25）：选 1 —— 修好并让它可用

复核确认「第一个任务要几十分钟」是**结构性、可接受的**，真正的缺陷是**可观测性与可恢复性**，而不是
close-refresh 这个功能本身不成立：

- `update_bars`（全市场、强制绕过缓存）在拓扑序里**排在第一个**，日常管道中它单独就要 **1,214–3,318 s**
  （20–55 分钟，`smartmoney_2026*.log` 实测）；所以「跑很久」是任务本身的性质，不是 bug。
- 但**编排器此前一个任务级日志都没有**（对照 `core/runner.py` 会打 `▶ 开始任务` / `✅ 任务 … 耗时`），
  而 TUI 起刷新子进程时 stdout/stderr 走 `DEVNULL`、只在退出后汇总一次 ⇒ 运行期间界面**完全不动**。
- 再加上**中断即全损**：SIGTERM → run 落 `aborted`，SIGKILL → 运行行永远停在 `running`；重跑要从头再来。

即 07-30 那三次 SIGTERM 的直接原因是「**看着像卡死 + 中断后一无所获**」，而非代码缺陷或表设计有问题。
因此**不删表、不删功能**，改为补齐这两项能力：

| 缺陷 | 修复 | PR |
|------|------|----|
| 运行期间无任何任务级输出 | 每任务落 `▶ [i/N]` / `✅ ⚠️ ⏭️ ❌ … 耗时` 日志（TUI 的 Live Logs 面板 tail 同一日志文件，因此自动可见）；16:00 闸门拒绝也落日志 | #134 |
| 中断后已完成任务全部作废 | `--refresh-today --resume` 续跑：沿用已落定任务（success/no_data/degraded，见 `RESUMABLE_TASK_STATUSES`），只重跑 failed / 被依赖阻塞的任务；旧 run 行保持原样 | #135 |

**因此本节关闭**：两张表继续保留，`refresh_runs`/`refresh_task_runs` 仍是收盘刷新的审计存储；
`tui/services/refresh_state.py` 的六态面板在续跑完成后能拿到完整的每任务记录。

**已收尾（小项，2026-09-26）**：`started_at` 与 `finished_at` 的格式不一致已修复——编排器
落库时把 `context.started_at`（上海 aware，供闸门与目标交易日使用）归一化为 UTC，审计行两个
时间戳现同为 `+00:00`（`core/refresh.py`；回归 `tests/test_refresh.py::test_started_at_is_normalized_to_utc_in_the_audit_row`）。
历史三行仍是 `+08:00`（未回填），但唯一读取方只按 `started_at` 文本排序，且新行偏移恒为 `+00:00`，无实际影响。
