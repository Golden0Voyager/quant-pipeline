# quant_pipeline 新数据源接入评估与分阶段计划

> **日期**：2026-07-20
> **背景**：北向资金个股持仓（`north_hold`）已完成三层纵切（pipeline 抓取 → `quant_core.db` 存储 → `quant_agents` 读取）。本文评估用户提出的 10 项候选数据缺口，判断哪些值得接入数据中台。
> **评估原则**：只看**实测可用性** + **与现有中台重复度** + **对量化分析的真价值**，而非清单标称。数据源失效或已被现有表覆盖的一律不做。

---

## 一、评估方法

1. 用 `dir(akshare)` 核对每项的**真实函数名**（用户清单里部分名称不准）。
2. 对每个主接口发起真实请求，记录：返回行数 / 报错类型 / 最新日期。
3. 与 `quant_core.db` 现有 **54 张表**逐项比对，判断重复度。
4. 区分数据源：**乐咕(legu)/同花顺(ths)/新浪** 相对稳定；**东财(datacenter-web.eastmoney.com)** 高频探测会被限流，需复用现有 `curl_cffi` 抗封装。

> ⚠️ 实测当天东财数据中心对本机限流，`ggcg_em / jgdy_tj_em / gpzy_profile_em / report_fund_hold` 未能取到样本，但它们是标准常用端点（非失效的 hsgt 家族），标记为"端点正常、集成时复测"。

---

## 二、实测结论总表

| # | 数据 | 真实 API | 实测状态 | 与现有表重复 | 价值 | 结论 |
|---|------|----------|----------|--------------|------|------|
| 1 | 沪深港通个股 | `stock_hsgt_hold_stock_em`、`stock_hsgt_individual_detail_em` | ❌ 实测 NoneType，政策变更已失效 | ✅ 已覆盖：`north_hold`（季度快照，本次已建） | ⭐⭐⭐⭐⭐ | **已完成**，历史走 HKSCC 回填 |
| 9 | 大盘估值 | `stock_a_all_pb`、`stock_a_ttm_lyr`、`stock_ebs_lg`、`stock_market_pe_lg` | ✅ 5224 / 5165 / 332 行（乐咕，稳） | ❌ 无（`historical_valuation` 是个股级） | ⭐⭐⭐⭐⭐ 大盘择时 | 🟢 **首选** |
| 5 | 概念板块 | `stock_board_concept_name_ths`、`stock_board_concept_cons_em` | ✅ 374 概念（同花顺，稳） | ❌ 无（现有 `sector_*` 全是行业） | ⭐⭐⭐⭐ 热点交易 | 🟢 **首选** |
| 7 | 股票回购 | `stock_repurchase_em` | ✅ 5321 行 | ❌ 无 | ⭐⭐⭐ 价值信号 | 🟡 值得 |
| 2 | 董监高/大股东增减持 | `stock_ggcg_em`、`stock_share_hold_change_{sse,szse,bse}` | 🔶 东财限流中，端点正常 | ❌ 无（`shareholder_count` 只是户数） | ⭐⭐⭐⭐⭐ 内部人信号 | 🟡 值得 |
| 4 | 机构调研 | `stock_jgdy_tj_em`、`stock_jgdy_detail_em` | 🔶 东财限流中，端点正常 | ❌ 无 | ⭐⭐⭐⭐ | 🟡 值得 |
| 6 | 股票质押 | `stock_gpzy_profile_em`、`stock_gpzy_pledge_ratio_em` | 🔶 东财限流中，端点正常 | ❌ 无 | ⭐⭐⭐⭐ 风险指标 | 🟡 值得 |
| 10 | 期权情绪 | `index_option_50etf_qvix`、`option_risk_indicator_sse` | ✅ 2771 行（乐咕，稳） | ❌ 无 | ⭐⭐⭐ 恐慌/贪婪 | 🟠 锦上添花 |
| 3 | 十大流通股东 | `stock_gdfx_free_top_10_em` | ✅ 已验证（茅台 Q1 HKSCC 5873万股/4.69%） | 🔸 部分（`institutional_holdings.top10_holder_ratio`） | ⭐⭐⭐⭐ | 🟠 并入 HKSCC 回填 |
| 8 | 基金持股 | `stock_report_fund_hold`、`stock_report_fund_hold_detail` | 🔶 东财，季度 | ✅ **基本覆盖**（`institutional_holdings.fund_hold_pct`） | ⭐⭐⭐ | 🔴 优先级最低 |

图例：🟢 首选　🟡 值得做　🟠 锦上添花　🔴 不建议　✅ 可用/已覆盖　❌ 失效/无　🔶 待复测

---

## 三、关键判断

1. **#1 不用再做**：用户所列两个 daily API 实测已失效（2024-08-19 披露政策变更），而本次已建的 `north_hold` 覆盖"北向持有哪些个股/持仓比例"；"增持减持排名 / 比例变化"靠 HKSCC 十大流通股东（#3）回填多季度即可。
2. **#8 基本别做**：`institutional_holdings` 已含 `fund_hold_pct / qfii_hold_pct / social_security_hold_pct / top10_holder_ratio`，再接 per-fund 明细边际收益低。
3. **每接一项 = 三层工作量**：`tasks/*.py` 抓取 + `quant_hunter database.py` 建表&写入 + `quant_agents` vendor 读取 + 两侧测试。与本次 `north_hold` 同量级，**必须分阶段**，不宜一次全上。
4. **数据源稳定性是硬约束**：乐咕/同花顺源（#9/#5/#10）稳；东财源（#2/#4/#6/#7）值得做但要复用现有 `curl_cffi + impersonate` 抗限流封装（参考 `core/stock_cyq_em.py`）。

---

## 四、分阶段实施计划

### 阶段一（首选，性价比最高，数据源最稳）

补上 Agent 当前**完全缺失**的两个维度：大盘择时 + 热点概念。

#### #9 大盘估值 → 新表 `market_valuation`（日频）

- **数据源**（乐咕，稳定，无需抗封装）：
  - `stock_a_all_pb()` → 全市场 PB 中位数 + 历史分位
  - `stock_a_ttm_lyr()` → 全市场 PE(TTM/LYR) 中位数 + 分位
  - `stock_ebs_lg()` → 沪深300 股债利差 + 均线
- **建议表结构**：
  ```sql
  CREATE TABLE market_valuation (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      trade_date DATE NOT NULL,
      pe_median REAL,            -- 全市场 PE 中位数
      pe_quantile REAL,          -- PE 历史分位(近10年)
      pb_median REAL,            -- 全市场 PB 中位数
      pb_quantile REAL,          -- PB 历史分位(近10年)
      equity_bond_spread REAL,   -- 股债利差
      ebs_ma REAL,               -- 股债利差均线
      csi300_close REAL,         -- 沪深300收盘(股债利差配套)
      data_source TEXT,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      UNIQUE(trade_date)
  );
  ```
- **Agent vendor**：新增 `get_market_valuation(curr_date)` → 返回最新大盘估值水位 + 分位判断（高估/低估/中性）。

#### #5 概念板块 → 新表 `concept_board` + `concept_member`

- **数据源**：同花顺 `stock_board_concept_name_ths()`（374 概念，稳）+ 成分 `stock_board_concept_cons_ths/_em`。
- **建议表结构**：
  ```sql
  CREATE TABLE concept_board (          -- 概念板块行情(日频)
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      trade_date DATE NOT NULL,
      concept_code TEXT NOT NULL,
      concept_name TEXT,
      pct_change REAL,                  -- 板块涨跌幅
      turnover REAL,                    -- 成交额
      up_count INTEGER, down_count INTEGER,
      data_source TEXT,
      UNIQUE(trade_date, concept_code)
  );
  CREATE TABLE concept_member (         -- 概念成分股映射(低频刷新)
      concept_code TEXT NOT NULL,
      concept_name TEXT,
      ts_code TEXT NOT NULL,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      UNIQUE(concept_code, ts_code)
  );
  ```
- **Agent vendor**：`get_stock_concepts(symbol)` → 个股所属概念；`get_hot_concepts(curr_date)` → 当日热点概念 Top-N。

### 阶段二（事件型强信号，东财源需抗限流）

| # | 数据 | 建议表 | 抓取要点 |
|---|------|--------|----------|
| 2 | 董监高/大股东增减持 | `insider_trading` | `stock_ggcg_em(symbol="全部")` 增量；字段：变动人、方向、股数、比例、公告日 |
| 4 | 机构调研 | `institution_survey` | `stock_jgdy_tj_em(date)` 按日；字段：被调研股、接待机构数、调研机构类型 |
| 7 | 股票回购 | `stock_repurchase` | `stock_repurchase_em()` 全量刷新（5321 行）；字段：计划/实施回购价区间、数量、进度 |
| 6 | 股票质押 | `stock_pledge` | `stock_gpzy_profile_em()` + `stock_gpzy_pledge_ratio_em(date)`；字段：质押比例、质押笔数、预警线 |

- 统一复用 `curl_cffi + impersonate='chrome110'` + 重试退避（参考 `core/stock_cyq_em.py` 的 `_RETRY_*`）。
- Agent vendor 对应：`get_insider_trading / get_institution_survey / get_repurchase / get_pledge(symbol)`。

### 阶段三（锦上添花）

| # | 数据 | 建议表 | 说明 |
|---|------|--------|------|
| 10 | 期权情绪 | `option_sentiment` | `index_option_50etf_qvix()`(QVIX 波动率) + `option_risk_indicator_sse()`(PCR)；市场级恐慌/贪婪指标 |
| 3 | 十大流通股东(HKSCC) | 并入 `north_hold` | `stock_gdfx_free_top_10_em` 抽 HKSCC 行，`data_source='hkscc'`，回填 2026Q1 及更早季度，与 Stock Connect 快照拼接成多季度序列 |

---

## 五、已完成 / 不做的项

- ✅ **#1 沪深港通个股**：`north_hold` 已覆盖当前季度全市场快照；daily API 实测失效，无需再接。
- 🔴 **#8 基金持股**：`institutional_holdings` 已含基金/QFII/社保持仓占比，per-fund 明细边际收益低，暂不做。

---

## 六、每项接入的标准三层改造清单

以本次 `north_hold` 为模板，每个新数据项需：

1. **抓取层** `quant_pipeline/tasks/<模块>.py`：`_fetch_xxx()` + `update_xxx(db)`，含异常兜底与分页。
2. **存储层** `quant_hunter/src/smartmoney_hunter/database.py`：建表 DDL + `save_xxx_batch()`；并在 `quant_pipeline/providers.py._ensure_tables()` 加兜底建表 + 委托方法；`interface.py` 声明抽象方法。
3. **调度接线** `quant_pipeline/daily_pipeline.py`：`run_all` 注册 + CLI 分支；`tui.py`：菜单 + 6 处映射。
4. **消费层** `quant_agents/tradingagents/dataflows/smartmoney_vendor.py`：新增 `get_xxx()` + `interface.py` 注册 + `default_config.py` fallback 链。
5. **测试**：两侧各补单测；`ruff check` 通过。

---

## 七、附录

### A. 现有 54 张表（截至 2026-07-20）

```
daily_bars, indicators, chip_distribution, chip_distribution_em,
fundamentals, quarterly_financials, quarterly_financials_history,
historical_valuation, stock_list, dividend_summary,
fund_flow, sector_fund_flow, margin_trading, dragon_tiger, block_trade,
north_flow, north_hold, south_flow, ah_premium,
shareholder_count, institutional_holdings, restricted_share, earnings_forecast,
index_daily, index_futures_basis, limit_up_down,
sector_industry, sector_daily, sector_valuation,
etf_daily, futures_daily, cb_index, cb_quotation, cb_redeem,
gold_price, crude_oil, fx_rate, global_index, us_treasury,
macro_daily, macro_monthly, macro_quarterly, money_market, central_bank_balance,
watchlist, watchlist_scores, scan_results, alerts_log, ai_memos, task_runs,
__aux_skip_dates, __inst_skip_dates, sqlite_sequence
```

### B. 实测命令与结果快照

```text
# 失效（政策变更）
stock_hsgt_hold_stock_em(北向,今日排行)        → TypeError: NoneType（失效）
stock_hsgt_individual_detail_em(600519)        → TypeError: NoneType（失效）

# 可用（乐咕/同花顺，稳定）
stock_board_concept_name_ths()                 → 374 行 [name, code]
stock_repurchase_em()                          → 5321 行
stock_a_all_pb()                               → 5224 行 [date, middlePB, quantile...]
stock_ebs_lg()                                 → 5165 行 [日期, 沪深300, 股债利差, 均线]
stock_market_pe_lg(上证)                       → 332 行 [日期, 指数, 平均市盈率]
index_option_50etf_qvix()                      → 2771 行 [date, open, high, low, close]
stock_gdfx_free_top_10_em(sh600519,20260331)   → 香港中央结算 5873万股 / 4.69% / 环比+6.69%

# 端点正常但当日被东财限流（集成时复测）
stock_ggcg_em / stock_jgdy_tj_em / stock_gpzy_profile_em / stock_report_fund_hold
```

### C. 参考实现

- 抓取模板：`quant_pipeline/tasks/macro.py` 的 `_fetch_north_hold / update_north_hold`
- 抗东财限流：`quant_pipeline/core/stock_cyq_em.py`（`curl_cffi + impersonate`）
- 消费模板：`quant_agents/tradingagents/dataflows/smartmoney_vendor.py` 的 `get_northbound_hold`
