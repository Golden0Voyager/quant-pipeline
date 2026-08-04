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

## 3. 高价值 AkShare 数据接入（未立项）

与已有 41 个数据任务对比，以下 10 项是 AkShare 提供但尚未接入的**最高价值数据**，按量化分析价值排序。

### 3.1 沪深港通持股个股明细

| 项目 | 内容 |
|------|------|
| **AkShare API** | `stock_hsgt_hold_stock_em`（北向持股）、`stock_hsgt_history_stock`（历史持仓） |
| **当前状态** | 只有北向资金每日净买入总额（`north_flow`），无个股级别 |
| **量化价值** | 北向资金是 A 股最重要的外部资金，个股持仓变化是强信号 |
| **实现建议** | 日频任务，存 `north_bound_hold` 表，字段：ts_code, date, hold_market_value, hold_ratio, change_ratio |
| **数据量预估** | ~1000 只标的 × 每日 ≈ 20 万行/年 |

### 3.2 股东增减持

| 项目 | 内容 |
|------|------|
| **AkShare API** | `stock_share_holder_change`（东财股东增减持） |
| **当前状态** | 只有股东户数（数量变化），缺实际买卖记录 |
| **量化价值** | 董监高/大股东增减持是内部人交易信号，公告级别数据 |
| **实现建议** | 事件驱动型任务，存 `shareholder_change` 表，字段：ts_code, date, name, change_type, volume, price, ratio |
| **数据量预估** | 几千条/月 |

### 3.3 十大流通股东 / 十大股东

| 项目 | 内容 |
|------|------|
| **AkShare API** | `stock_top10_holders`（十大股东）、`stock_top10_flow_holders`（十大流通股东） |
| **当前状态** | 无 |
| **量化价值** | 季报级别的大机构持仓变化，补全基本面拼图 |
| **实现建议** | 季频任务，存 `top10_holders` 表 |
| **数据量预估** | 5000+ 只 × 10 位 × 4 季/年 ≈ 20 万行/年 |

### 3.4 机构调研

| 项目 | 内容 |
|------|------|
| **AkShare API** | `stock_jgdy_tj_em`（机构调研统计）、`stock_jgdy_detail_em`（调研详细） |
| **当前状态** | 无 |
| **量化价值** | 机构调研频率与被调研公司后续表现存在正相关，是另类数据信号 |
| **实现建议** | 日频任务，存 `institution_survey` 表 |
| **数据量预估** | 每日几十条 |

### 3.5 概念板块

| 项目 | 内容 |
|------|------|
| **AkShare API** | `stock_board_concept_*_em`（东财概念板块）/ `stock_board_concept_*_ths`（同花顺） |
| **当前状态** | 只有行业板块（申万分类），无概念板块 |
| **量化价值** | AI、新能源、芯片等概念板块是 A 股热点交易的驱动力 |
| **实现建议** | 日频任务，存 `concept_sector` 和 `concept_sector_constituents` 表 |
| **数据量预估** | ~500 个概念板块 × 每日行情 |

### 3.6 股票质押

| 项目 | 内容 |
|------|------|
| **AkShare API** | `stock_pledge_statistics_em`（质押统计）、`stock_pledge_detail_em`（质押明细） |
| **当前状态** | 无 |
| **量化价值** | 高质押比例是风险指标，质押爆仓是系统性风险源 |
| **实现建议** | 日/周频任务，存 `stock_pledge` 表 |
| **数据量预估** | 几千只股票 |

### 3.7 股票回购

| 项目 | 内容 |
|------|------|
| **AkShare API** | `stock_repurchase_em` |
| **当前状态** | 无 |
| **量化价值** | 回购是公司认为股价被低估的信号 |
| **实现建议** | 日频任务，存 `stock_repurchase` 表 |
| **数据量预估** | 每日几条到几十条 |

### 3.8 基金持股

| 项目 | 内容 |
|------|------|
| **AkShare API** | `stock_fund_hold_em` |
| **当前状态** | 无 |
| **量化价值** | 每季度公募基金重仓股变化，跟踪"聪明钱" |
| **实现建议** | 季频任务，存 `fund_holdings` 表 |
| **数据量预估** | ~2000 只 × 每季 |

### 3.9 大盘估值指标

| 项目 | 内容 |
|------|------|
| **AkShare API** | `stock_a_all_pb`（全市场PB）、`stock_a_all_pe`（全市场PE）、`stock_bond_spread`（股债利差） |
| **当前状态** | 无 |
| **量化价值** | 全市场 PE/PB 中位数、股债利差是判断市场整体估值水位的关键指标 |
| **实现建议** | 日频任务，存 `market_valuation` 表 |
| **数据量预估** | 每日 1 行 |

### 3.10 期权数据

| 项目 | 内容 |
|------|------|
| **AkShare API** | `option_50etf_*`（50ETF 期权）、`option_300etf_*`（300ETF 期权） |
| **当前状态** | 无任何期权数据 |
| **量化价值** | 期权持仓 PCR、隐含波动率是市场情绪和风险偏好的实时指标 |
| **实现建议** | 日频任务，存 `option_daily` 表，字段：date, etf_type, pcr, iv, put_volume, call_volume |
| **数据量预估** | 每日几条 |

---

## 4. 新数据源调研

### 4.1 乐咕乐咕 (legulegu.com) — 21 个函数，基本独占

**估值指标类（已列入 3.9 大盘估值指标）：**
- `stock_a_ttm_lyr()` — 全A PE（LYR+TTM、等权+中位数）
- `stock_a_all_pb()` — 全A PB（等权+中位数）
- `stock_ebs_lg()` — 股债利差（FED spread）
- `stock_buffett_index_lg()` — 巴菲特指标（总市值/GDP + 分位数）
- `stock_a_congestion_lg()` — 大盘拥挤度
- `stock_market_pe_lg()` / `stock_market_pb_lg()` — 上证/深证/创业板/科创板 PE+PB
- `stock_index_pe_lg()` / `stock_index_pb_lg()` — 12 个主流指数 PE+PB（等权/加权/中位数）
- `stock_a_gxl_lg()` / `stock_hk_gxl_lg()` — 市场级股息率时间序列

**市场宽度类（另类数据信号）：**
- `stock_a_high_low_statistics()` — 20/60/120 日新高新低计数
- `stock_a_below_net_asset_statistics()` — 破净股数量 + 占比历史
- `stock_market_activity_legu()` — 赚钱效应（涨跌/涨停/跌停家数）

**基金仓位类：**
- `fund_stock_position_lg()` — 股票型基金仓位
- `fund_balance_position_lg()` — 平衡混合型基金仓位
- `fund_linghuo_position_lg()` — 灵活配置型基金仓位

**接入建议**：
- 高优先级：策略信号类（股债利差、巴菲特指标、全A PE）→ 可合并到 `tasks/macro.py` 或新建 `tasks/market_valuation.py`
- 中优先级：市场宽度类 → 与已有 limit_up_down 任务合并
- 低优先级：基金仓位 → 等仓位数据需求明确后再做

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

## 5. 数据源优先级总排序

| 优先级 | 来源 | 数据 | 原因 |
|--------|------|------|------|
| 🔴 P0 | 乐咕 | 股债利差、巴菲特指标、全A PE | 独占、策略核心、日频、量小易接入 |
| 🔴 P0 | 东财 | 沪深港通持股个股明细 | 北向资金个股级、量化核心信号 |
| 🔴 P0 | 东财 | 股东增减持 | 内部人交易强信号 |
| 🟡 P1 | 乐咕 | 市场宽度（新高新低/破净） | 独占、辅助判断市场状态 |
| 🟡 P1 | 东财 | 概念板块 | 热点交易驱动力，分类与现有行业不同 |
| 🟡 P1 | 东财 | 机构调研 | 另类数据信号 |
| 🟢 P2 | 同花顺 | 技术选股系列 | 因子输入，不直接入库 |
| 🟢 P2 | 东财 | 股票质押/回购/基金持股 | 辅助信号 |
| ⚪ P3 | 腾讯 | 日线 fallback / 分笔 | 备用/高级分析 |
| ⚪ P3 | 乐咕 | 基金仓位 | 低更新频率 |
| ⚪ P3 | 东财/同花顺 | 期权数据 | 市场较小 |
```

---

## 6. update_financial_history 报告期处理无重试（2026-08-04 观察）

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

### 建议方案

1. 报告期处理加 1-2 次指数退避重试（对齐 `update_bars` 的重试标准）
2. 区分「接口失败」与「空数据」日志级别，空数据不应计入失败
3. 失败报告期持久化到待重试列表，跨运行保留（类似 `failed_symbols` 队列），避免下次运行重新发现时重复拉全部

---

## 7. 单次尝试任务遇上游瞬断即失败（2026-08-04 观察）

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

### 建议方案

与第 6 节统一处理：

1. 提炼统一的重试装饰器（如 `tasks/retry_utils.py` 的 `@retry(attempts=3, backoff=...)`），覆盖所有单次尝试的抓取任务
2. 统一将「接口失败」标为可重试错误，空数据/校验失败不重试
3. 任务级失败与 per-symbol 失败队列统一记录，便于跨运行恢复
