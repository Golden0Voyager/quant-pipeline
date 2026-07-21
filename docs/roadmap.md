# quant_pipeline 数据源接入路线图

> **维护者**：Golden0Voyager
> **最后更新**：2026-07-21
> **源文档**：`docs/todo.md`、`docs/2026-07-20-new-data-sources-assessment.md`

---

## 一、项目架构速览

```
quant_pipeline/
├── daily_pipeline.py      # 主管道：CLI + 全部任务注册 + run_all()
├── tui.py                 # Textual TUI 监控面板
├── interface.py           # DatabaseInterface Protocol（数据契约）
├── providers.py           # SmartMoney 适配层（DDL + save 方法）
│
├── tasks/                 # 数据抓取任务（~25 个模块）
│   ├── bars.py            # 日线 OHLCV
│   ├── indicators.py      # 技术指标
│   ├── fundamentals.py    # 基本面估值
│   ├── financials.py      # 财报/股东户数
│   ├── macro.py           # 宏观/全球指数/北向
│   ├── money_market.py    # 货币市场（SHIBOR/回购/PBOC）
│   ├── market_flow.py     # 资金流向/龙虎榜/融资融券
│   ├── sector_*.py        # 行业板块
│   ├── convertible_bond.py# 可转债
│   ├── corporate_actions.py# 限售解禁/业绩预告
│   └── ...                # ~15 个更多模块
│
├── scripts/               # 独立工具脚本
└── tests/                 # pytest 测试
```

| 关键指标 | 值 |
|---------|-----|
| 数据库 | `~/Code/quant_data/quant_core.db` |
| 股票数 | ~5528 |
| 日线行数 | ~716 万 |
| 现有表数 | 54 |
| 包管理器 | `uv`（禁止 pip） |

---

## 二、已完成的里程碑

| 时间 | 内容 | PR |
|------|------|----|
| 2026-07-20 | 货币市场模块：SHIBOR/回购利率/PBOC利率/央行资产负债表从 china_macro 拆分到独立模块 `tasks/money_market.py`；col_map 修复 14 个月度宏观fetcher；north_hold 北向资金季度持仓快照 | #26 |
| 2026-07-15 | 扩展市场数据任务：南向资金、A/H溢价、可转债、限售解禁、业绩预告、行业衍生指标 | #25 |
| 2026-07 | 日线回填 6 年 + 断点续传 + 守护进程 | — |

---

## 三、分阶段实施计划

### 阶段一（首选 — 本周优先级最高）

补上 Agent 当前**完全缺失**的两个维度：大盘择时 + 热点概念。数据源稳定（乐咕/同花顺），无需抗限流封装。

| # | 数据 | 源 | 表 | 状态 |
|---|------|----|----|------|
| 9 | 大盘估值（PE/PB/股债利差） | 乐咕 legulegu.com | `market_valuation` | 🟢 待开发 |
| 5 | 概念板块行情 + 成分股映射 | 同花顺 10jqka | `concept_board` + `concept_member` | 🟢 待开发 |

#### #9 大盘估值 → `market_valuation`

- **API**：`stock_a_all_pb()`、`stock_a_ttm_lyr()`、`stock_ebs_lg()`
- **频次**：日频，每天 1 行
- **DDL**：

```sql
CREATE TABLE market_valuation (
    date TEXT PRIMARY KEY,
    pe_median REAL,            -- 全市场 PE(TTM) 中位数
    pe_quantile REAL,          -- PE 历史分位(近10年)
    pe_lyr_median REAL,        -- 全市场 PE(LYR) 中位数
    pb_median REAL,            -- 全市场 PB 中位数
    pb_quantile REAL,          -- PB 历史分位(近10年)
    equity_bond_spread REAL,   -- 股债利差(FED spread)
    ebs_ma REAL,               -- 股债利差 5 日均线
    csi300_close REAL,         -- 沪深300 收盘价
    data_source TEXT DEFAULT 'legu',
    data_date TEXT,
    UNIQUE(date)
);
```

#### #5 概念板块 → `concept_board` + `concept_member`

- **API**：`stock_board_concept_name_ths()`、`stock_board_concept_cons_ths()`、`stock_board_concept_summary_ths()`
- **频次**：日频（行情）+ 周频（成分映射）
- **DDL**：

```sql
CREATE TABLE concept_board (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date DATE NOT NULL,
    concept_code TEXT NOT NULL,
    concept_name TEXT,
    pct_change REAL,           -- 板块涨跌幅
    turnover REAL,             -- 成交额
    up_count INTEGER,          -- 上涨家数
    down_count INTEGER,        -- 下跌家数
    data_source TEXT DEFAULT 'ths',
    UNIQUE(trade_date, concept_code)
);

CREATE TABLE concept_member (
    concept_code TEXT NOT NULL,
    concept_name TEXT,
    ts_code TEXT NOT NULL,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(concept_code, ts_code)
);
```

---

### 阶段二（事件型强信号 — 东财源需抗限流）

利用东财 `curl_cffi + impersonate` 抗限流封装（参考 `core/stock_cyq_em.py`）。

| # | 数据 | 表 | 抓取策略 | 复杂度 |
|---|------|----|----------|--------|
| 2 | 董监高/大股东增减持 | `insider_trading` | `stock_ggcg_em(symbol="全部")` 增量；字段：变动人、方向、股数、比例、公告日 | 🔶 中 |
| 4 | 机构调研 | `institution_survey` | `stock_jgdy_tj_em(date)` 按日；字段：被调研股、接待机构数、机构类型 | 🔶 中 |
| 7 | 股票回购 | `stock_repurchase` | `stock_repurchase_em()` 全量刷新（5321 行）；字段：计划/实施回购价区间、数量、进度 | 🟢 低 |
| 6 | 股票质押 | `stock_pledge` | `stock_gpzy_profile_em()` + `stock_gpzy_pledge_ratio_em(date)`；字段：质押比例、质押笔数、预警线 | 🔶 中 |

---

### 阶段三（锦上添花）

| # | 数据 | 表 | 说明 |
|---|------|----|------|
| 10 | 期权情绪 | `option_sentiment` | `index_option_50etf_qvix()`(QVIX 波动率) + `option_risk_indicator_sse()`(PCR)；市场级恐慌/贪婪指标 |
| 3 | 十大流通股东(HKSCC) | 并入 `north_hold` | `stock_gdfx_free_top_10_em` 抽 HKSCC 行，`data_source='hkscc'`，回填多季度，与已有快照拼接 |

---

## 四、优先级矩阵

| 优先级 | 来源 | 数据 | 原因 |
|--------|------|------|------|
| 🔴 P0 | 乐咕 | 股债利差、巴菲特指标、全A PE | 独占、策略核心、日频、量小 |
| 🔴 P0 | 东财 | 沪深港通持股个股明细 | 北向资金个股级 ⚠️ 已通过 north_hold 覆盖 |
| 🔴 P0 | 东财 | 股东增减持 | 内部人交易强信号 |
| 🟡 P1 | 乐咕 | 市场宽度（新高新低/破净） | 独占、辅助判断市场状态 |
| 🟡 P1 | 东财 | 概念板块 | 热点交易驱动力 |
| 🟡 P1 | 东财 | 机构调研 | 另类数据信号 |
| 🟢 P2 | 同花顺 | 技术选股系列 | 因子输入，不入库 |
| 🟢 P2 | 东财 | 股票质押/回购/基金持股 | 辅助信号 |
| ⚪ P3 | 腾讯 | 日线 fallback/分笔 | 备用/高级分析 |
| ⚪ P3 | 乐咕 | 基金仓位 | 低更新频率 |

---

## 五、实施标准流程

每个新数据项按**三层改造清单**实施：

### 抓取层 `tasks/<模块>.py`

```python
"""模块描述。"""
from __future__ import annotations
import logging
from datetime import datetime
from typing import Any
import pandas as pd
from interface import DatabaseInterface

try:
    import akshare as ak
except ImportError:
    ak = None

logger = logging.getLogger(__name__)

def _to_float(val: Any) -> float | None:
    """安全转浮点。"""
    if val is None:
        return None
    try:
        v = float(val)
        return None if pd.isna(v) else v
    except (ValueError, TypeError):
        return None

def _try_get_ak_df(func, **kwargs) -> pd.DataFrame | None:
    """安全调用 AkShare 函数，失败返回 None。"""
    if ak is None:
        return None
    try:
        return func(**kwargs)
    except Exception as e:
        logger.warning(f"⚠️ {func.__name__} 获取失败: {e}")
        return None

def _fetch_xxx() -> list[dict]:
    ...

def update_xxx(db: DatabaseInterface) -> dict:
    """主更新函数。"""
    if ak is None:
        return {"saved": 0, "error": "akshare not installed"}
    ...
```

### 存储层

1. `interface.py`：声明 `save_xxx_batch(self, records) -> int`
2. `providers.py`：`_ensure_tables()` 加 DDL + `save_xxx_batch()` 实现

### 调度接线

3. `daily_pipeline.py`：`run_all` 注册 + CLI 分支
4. `tui.py`：菜单映射（如果任务需要独立 TUI 按钮）

### 测试层

5. `tests/test_new_tasks.py`：mock akshare + 验证 fetch/update 逻辑

---

## 六、参考

### 现有 54 张表（截至 2026-07-20）

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

### AkShare API 实测参考

```text
# 乐咕（稳定）
stock_a_all_pb()                 → 5224 行 [date, middlePB, quantile...]
stock_a_ttm_lyr()                → 5165 行 [date, middlePE, quantile, middlePE_LYR...]
stock_ebs_lg()                   → 5165 行 [日期, 沪深300, 股债利差, 均线]

# 同花顺（稳定）
stock_board_concept_name_ths()   → 374 行 [name, code]
stock_board_concept_summary_ths()→ 374 行 [日期, 概念名称, 涨跌幅, 成交额...]

# 东财（稳定但限流敏感）
stock_repurchase_em()            → 5321 行
stock_ggcg_em()                  → 端点正常（当日被限流）
stock_jgdy_tj_em()               → 端点正常（当日被限流）
```

### 实现参考文件

- 抓取模板：`tasks/money_market.py`（`_try_get_ak_df` + `update_*` 模式）
- 存储模板：`providers.py`（`save_money_market_batch` + DDL 模式）
- 调度模板：`daily_pipeline.py`（注册 + CLI 分支）
- 测试模板：`tests/test_new_tasks.py`（`_mock_ak_*` + 测试函数）
- 东财抗限流：`core/stock_cyq_em.py`（`curl_cffi + impersonate`）
