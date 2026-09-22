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

---

## 3. 高价值数据接入清单（2026-09 复核）

> **复核结论（2026-09-22）**：原 10 项中 **7 项已接入**，仅 3 项仍未做。已接入项标记 ✅ 并指向实际任务；未做项保留待办。

### ✅ 3.1 沪深港通持股个股明细 — 已接入

| 项目 | 内容 |
|------|------|
| **实际任务** | `update_north_hold`（`tasks/macro.py`） |
| **数据源** | 东财数据中心分页 API `RPT_MUTUAL_HOLDSTOCKNORTH_STA`（季度快照，全市场 ~3900 只） |
| **表** | `north_hold`（ts_code, trade_date, hold_shares, hold_market_cap, hold_shares_ratio, …） |
| **说明** | HKEX 自 2024-08-19 停止每日个股北向披露，故为**季度快照**（非日频）。`tasks/hkscc_holder.py` 是旧的逐股轮询方案（遍历 5500 只），**已废弃，勿注册** |

### 3.2 股东增减持 — 未做

| 项目 | 内容 |
|------|------|
| **AkShare API** | `stock_gdfx_free_holding_change_em` / `stock_gdfx_holding_change_em`（东财股东增减持，按季度） |
| **当前状态** | 无；akshare 无 `stock_share_holder_change`（todo 旧文有误） |
| **量化价值** | 董监高/大股东增减持是内部人交易信号，公告级别数据 |
| **实现建议** | 事件驱动型任务，存 `shareholder_change` 表，字段：ts_code, date, name, change_type, volume, price, ratio |
| **阻塞** | 东财 datacenter API 当前 SSL 不稳定（2026-09-22 实测失败）；恢复后再接入 |

### 3.3 十大流通股东 / 十大股东 — 未做

| 项目 | 内容 |
|------|------|
| **AkShare API** | `stock_top10_holders`（十大股东）、`stock_top10_flow_holders`（十大流通股东） |
| **当前状态** | 无（`update_shareholder_count` 是股东户数，非十大股东） |
| **量化价值** | 季报级别的大机构持仓变化，补全基本面拼图 |
| **实现建议** | 季频任务，存 `top10_holders` 表 |
| **说明** | `stock_circulate_stock_holder` 可单股取历史但极慢（~67 页/只），批量接入需评估 |

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

### 3.8 基金持股 — 未做

| 项目 | 内容 |
|------|------|
| **AkShare API** | `stock_fund_hold_em` |
| **当前状态** | 无 |
| **量化价值** | 每季度公募基金重仓股变化，跟踪"聪明钱" |
| **实现建议** | 季频任务，存 `fund_holdings` 表 |
| **数据量预估** | ~2000 只 × 每季 |

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
| 🟢 P2 | 东财 | 股票质押/回购/基金持股 | 质押✅ 回购✅ / 基金持股⏳ |
| ⚪ P3 | 腾讯 | 日线 fallback / 分笔 | 未接入（备用/高级分析） |
| ⚪ P3 | 乐咕 | 基金仓位 | 未接入（低更新频率） |
| ⚪ P3 | 东财/同花顺 | 期权数据 | ✅ 已接入（`update_option_sentiment`） |
```

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
3. ⏳ 失败报告期持久化到待重试列表（类似 `failed_symbols` 队列）→ 未做，可后续补

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

### 修复状态（2026-09-22，部分）

1. ✅ `update_concept_board` → PR #103 已改 `retained`（源不可用保留旧数据，exit 0）
2. ✅ **8 个高频 daily 任务补 `error_kind=network`**（PR #105）：hk_tech_index、us_macro、cftc_cot、eia_petroleum、lithium_spot、macro.py 的 index_daily/limit_up_down/dividend_summary/gold_price/usd/global_index/us_treasury
3. ⏳ 剩余 15 个低频任务补 `error_kind=network`（convertible_bond×3、south_flow、ah_premium、etf_daily、restricted_share、earnings_forecast、historical_valuation、sector_industry、block_trade、sector_fund_flow 等）→ 未做
4. ⏳ 统一重试装饰器（`tasks/retry_utils.py` 的 `@retry(attempts=3, backoff=...)`）→ 未做，长期方案
