# quant_pipeline 接入同花顺 Financial-API 可行性方案

> 日期：2026-08-26
> 背景：当日定时运行中东财/新浪/腾讯数据源大面积不稳（821 条源级 ERROR、2172 条降级 WARNING），最终全靠雪球单点兜底，构成数据完整性单点风险（P1）。
> 参考：`~/Code/quant_agents/docs/tonghuashun_api.md`（API 能力调研，2026-08-26 实测）

---

## 一、结论

**可行且推荐**。quant_pipeline 已具备原生多源降级基础设施（`core/source_client.py` 的 `SourceClient`：熔断器 + 限流器 + `call_with_fallback` 逐源降级链），接入同花顺无需改动架构，核心工作量集中在一个新 fetcher 模块（约 200 行）。落地后可直接消除「雪球单点兜底」风险。

## 二、现状架构与插入点

> ⚠️ 实施修正（2026-08-26）：原稿假设日线降级链由 `call_with_fallback` 组装，
> 实测不符——A 股日线多源链（东财→新浪→腾讯→雪球）是 quant_hunter
> `DataLoader.get_daily_bars` 内的手写 try/except，`call_with_fallback` 仅被
> `source_client.py` 自身与测试使用。实际插入点为 **providers.py 门面层**：
> `SmartMoneyLoaderProvider.get_daily_bars` / `incremental_update` 在 DataLoader
> 全链失败（空结果/无新行）后追加 hithink 兜底，熔断/限流/退避复用
> `core/source_client.py` 的 `SourceClient`（POLICIES 新增 `hithink` 条目）。
> 同理，`_canary_probe` 只是可用性探针、不含对账容差，复权口径对账改由
> 实施前 spike + 入库前 sanity check 承担（见文末实施记录）。

```
core/source_client.py   ← SourceClient：熔断器(CircuitBreaker) + 限流器(_RateLimiter)
                          + call_with_fallback(primary, fallback_sources=[...])
providers.py            ← SmartMoneyLoaderProvider 门面层（hithink 兜底实际插入点）
tasks/bars.py           ← 日线任务编排：停牌预检 / canary 探测(_canary_probe) / checkpoint / retry 队列
tasks/refresh_adapters  ← 各任务刷新适配器
```

改造后日线降级链：

```
DataLoader 内部链：东财 → 新浪 → 腾讯 → 雪球
        ↓ 全链失败（空结果 / 增量无新行）
providers.py 门面层：🆕 hithink 官方API 兜底（写库 data_source='hithink'）
```

## 三、新模块设计：`core/source_hithink.py`

```python
class HithinkClient:
    """同花顺 Financial-API 客户端（公测期）"""
    BASE = "https://fuyao.aicubes.cn"
    # 认证: X-api-key ← 环境变量 HITHINK_FINANCE_API_KEY（.env 注入，禁止硬编码）

    def fetch_daily_bars(symbol, start, end, adjust="qfq") -> pd.DataFrame:
        # GET /api/a-share/prices/historical
        # ⚠️ start/end 为毫秒时间戳（非日期字符串）
        # 代码映射: "600519" → "600519.SH"，"000001" → "000001.SZ"
        # 响应信封: HTTP 恒 200，业务错误看 code 字段：
        #   0=成功 | 4001=QPS超限(触发退避) | 2003=权限收缩(触发熔断) | 500x=服务端错误
        # 返回前对齐现有列名规范（如 turnover 命名约定，见 tasks/bars.py）
```

### 必须处理的契约细节（来自 API 实测踩坑记录）

1. `prices/historical` 的 `start/end` 是**毫秒时间戳**
2. 批量参数为 `thscodes`（CSV 格式）；传单个 `thscode` 会报 `code=1001`
3. `financials/indicators` 的 `report` 格式为 `{yyyy}-{1|2|3|4}`，数据在 `data.abilities[]`
4. HTTP 恒 200 —— 错误处理必须解析信封 `code` 字段，不能依赖 HTTP 状态码

## 四、覆盖矩阵（哪些任务受益）

| 管线任务 | THS 能力 | 方案 |
|---|---|---|
| **daily_bars 日线**（P1 主战场） | `prices/historical` qfq 直出 | ✅ **Phase 1 核心** |
| 月度修复 / 历史回补 | `market-dumps` 全市场 10 年 K 线 + 复权因子 Parquet 一次拉全 | ✅ Phase 2 |
| update_dragon_tiger | `dragon-tiger-list`（分机构榜/游资榜） | Phase 2 可选 |
| update_limit_up_down | 涨停/跌停/炸板池 + 连板天梯 | Phase 2 可选 |
| update_market_valuation | `valuations/snapshot` 批量 PE/PB/PS/PCF | Phase 3 交叉校验 |
| 筹码分布 / 融资融券 / 大宗交易 / 南向资金流 / 主力资金流 | ❌ 官方明确不提供 | **维持现有链不动** |

## 五、风险与缓解

| 风险 | 缓解措施 |
|---|---|
| QPS 限流（4001） | SourcePolicy 设 `qps=2`；优先批量接口（snapshot 类支持 CSV 多票一次调用）；4001 复用 `_backoff_sleep` 指数退避。最坏情况熔断打开自动跳过该源，**绝不阻塞主管线** |
| 公测期政策变化（收费/权限收缩） | `code=2003` 时熔断并记 WARNING，链路自动落到雪球——行为等同现状，无回退风险 |
| 复权口径差异 | hithink `adjust=forward` 与 AkShare qfq 锚点可能不同 → 复用 bars.py 现成 `_canary_probe`：THS 数据入库前抽样对账，不一致则弃用该源并告警 |
| 北交所覆盖未知 | 文档未提 920xxx/`.BJ` 代号格式，Phase 1 实测后再决定是否参与 BJ 股票降级链 |
| Key 泄露 | 调研用 Key 已暴露于聊天记录 → **实施前先到 admin 页轮换**；新 Key 仅存 `.env` |

## 六、分期实施

| 阶段 | 内容 | 工作量 | 收益 |
|---|---|---|---|
| **P0（本次提案）** | `source_hithink.py` + 日线链接入 + 单测（mock 信封成功/4001/2003 三种响应） | 4~6 小时 | 直接消灭雪球单点风险 |
| P1 | market-dumps Parquet 接入月度修复/回补 | 半天 | 6 年级回补从小时级降到分钟级 |
| P2 | 龙虎榜/涨停池替换 + 估值快照交叉校验 | 半天 | 数据质量与溯源增强 |

## 七、验收标准（Phase 1 DoD）

- [ ] mock 东财/新浪/腾讯全部失败时，hithink 成功兜底返回标准化 DataFrame
- [ ] 入库后 `data_source` 字段可溯源为 `"hithink"`（沿用现有溯源机制）
- [ ] 4001 场景退避重试后恢复；持续 4001 触发熔断跳过且不阻塞主流程
- [ ] 现有测试套件（test_daily_pipeline.py / test_column_fixes.py 等）全绿
- [ ] 真实环境运行一晚：smartmoney 日志中 hithink 兜底次数 > 0 且 health_check 通过
- [ ] 北交所代码行为有明确实测结论（支持/不支持均写入本文件备注）

## 八、实施前置条件

1. **轮换 API Key**（旧 Key 已暴露），新 Key 写入 `.env`：`HITHINK_FINANCE_API_KEY=<new>`
2. 确认公测期账号的数据权限覆盖 `prices/historical` 的 qfq 输出
3. 建议在管线稳定窗口（非交易日）合入，避开每日 17:30 定时运行

---

## 附：实施记录（2026-08-26，P0 + P1 已落地）

### 落地内容

- **P0**：`core/source_hithink.py`（HithinkClient + `fetch_daily_bars`，约 200 行）；
  `core/source_client.py` POLICIES 注册 `hithink`（min_interval 0.5s ≈ QPS 2，3 次尝试）；
  `providers.py` `SmartMoneyLoaderProvider` 门面层兜底（`get_daily_bars` 空结果触发，
  `incremental_update` 无新行时按缺口区间补数，合并前归一化 `trade_date`/`turnover_rate`
  列，避免 2026-07 双列写 NULL 事故复发）；`tests/test_source_hithink.py` 23 用例。
- **P1**：`scripts/backfill_from_hithink_dump.py`（下载 10 年日 K + 复权因子 Parquet，
  本地算前复权，回补历史不足 6 年的股票，`--dry-run`/`--symbols`）+ `pyarrow` 依赖
  + `tests/test_backfill_hithink_dump.py` 11 用例。定位为手动回补工具，
  **未串入** `_REPAIR_CHAIN` 月度修复链（避免无人值守的全量锚点漂移覆写，观察后再定）。

### Spike 实测结论

- **复权口径**：hithink `adjust=forward` 与库内 akshare qfq 最新段完全一致
  （10 票 × 10 日 close/volume/amount 全对）；**除权事件窗口内历史段有偏差**
  （601899 紫金矿业 2026-08-13~20 差 ~1.3%，最新日一致）。根因：hithink 官方
  前复权为**等差（减法）**口径，akshare 为**等比（乘法）**口径。P0 兜底只写增量
  当日数据，风险可控；P1 回补脚本采用等比公式与库内数据同族。
- **北交所**：仅 920 前缀受支持（920001.BJ ✅ code=0）；430047.BJ / 830799.BJ
  均报 `code=1002 Unknown thscode` → 43/83/87 老 BJ 代码在 `to_thscode` 中显式跳过。
- **market-dumps**：三个 download-url 端点（`daily-k` 10 年全量 161MB / `daily-k-10d`
  / `adjustment-factors` 288KB）均可用，预签名 URL 300 秒有效。
- **P1 qfq 验证**：脚本等比复权 vs hithink 官方 forward（等差）近 1 年逐日比对，
  601899/600519/000333 最大偏差 1.17%/0.55%/0.58%（结构性口径差，预期内）；
  最近 2 个月及最新交易日偏差 0.000%。

### DoD 核对

- [x] mock 全链失败时 hithink 兜底返回标准化 DataFrame（test_source_hithink.py）
- [x] 写库 `data_source='hithink'` 可溯源（df 列携带，database.py 自动取列值）
- [x] 4001 → 转 `RuntimeError("HTTP 429")` 走 SourceClient 退避；2003 → 进程内停用
- [x] 现有测试套件全绿（1828 + 新增 34 全过）
- [ ] 真实环境运行一晚验证（合入后观察；hithink 仅在主链故障时触发，正常夜晚可能 0 次，属预期）
- [x] 北交所结论已写入本文件（仅 920）

### 遗留事项

- API Key 轮换后需同步更新 `quant_pipeline/.env`（当前复用 quant_agents 的 Key 已可工作）
- P2（龙虎榜/涨停池/估值交叉校验）中**涨停/跌停池已于 2026-09-27 落地**（见下方补记）；
  龙虎榜/估值交叉校验未实施；quant_agents 实测龙虎榜日期参数有坑
  （非交易日报 1002、`trade_date` 被静默忽略须用 `date_ms`），替换类工作建议另行立项

### 补记（2026-09-27）：涨停/跌停池 P2 部分落地

东财涨跌停池（`stock_zt_pool_em` / `stock_zt_pool_dtgc_em`）只保留约 16 个交易日的滚动
窗口，导致整日回补对更早的历史日无源可取（2026-09-02 等）。本次落地：

- `core/source_hithink.py` 新增 `fetch_limit_pools(trade_date)` / `_fetch_limit_pool()`：
  调 `special-data/limit-up-pool` / `limit-down-pool`，参数为 **`date_ms`** 毫秒戳
  （传 `trade_date` 会被服务端静默忽略并返回 0 行——与 quant_agents 的踩坑记录一致），
  按 `pagination.total` 分页拉全（`size=200`）。
- `tasks/macro.py:update_limit_up_down` 在东财两池皆空时用同花顺兜底，写库
  `data_source='hithink'`；字段映射 `ticker`/`price_change_ratio_pct`/`last_price`/
  `continue_day_cnt`（连板数）/`turnover_ratio_pct`，同花顺不提供 `industry` 与涨停池换手率。
- 实测可补 2026-09-02（涨停 51 / 跌停 8）及 08-03/08-19/08-21 等东财窗口外历史日；
  同花顺保留约近几个月（2026-06-01 有数、2026-01-02 为空）。
- 龙虎榜 / 估值交叉校验仍属未实施。
