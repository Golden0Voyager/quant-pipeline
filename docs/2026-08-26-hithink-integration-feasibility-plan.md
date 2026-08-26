# quant_pipeline 接入同花顺 Financial-API 可行性方案

> 日期：2026-08-26
> 背景：当日定时运行中东财/新浪/腾讯数据源大面积不稳（821 条源级 ERROR、2172 条降级 WARNING），最终全靠雪球单点兜底，构成数据完整性单点风险（P1）。
> 参考：`~/Code/quant_agents/docs/tonghuashun_api.md`（API 能力调研，2026-08-26 实测）

---

## 一、结论

**可行且推荐**。quant_pipeline 已具备原生多源降级基础设施（`core/source_client.py` 的 `SourceClient`：熔断器 + 限流器 + `call_with_fallback` 逐源降级链），接入同花顺无需改动架构，核心工作量集中在一个新 fetcher 模块（约 200 行）。落地后可直接消除「雪球单点兜底」风险。

## 二、现状架构与插入点

```
core/source_client.py   ← SourceClient：熔断器(CircuitBreaker) + 限流器(_RateLimiter)
                          + call_with_fallback(primary, fallback_sources=[...])
tasks/bars.py           ← 日线任务编排：停牌预检 / canary 探测(_canary_probe) / checkpoint / retry 队列
providers.py            ← DataLoader 门面层
tasks/refresh_adapters  ← 各任务刷新适配器
```

改造后日线降级链：

```
东财 → 新浪 → 腾讯 → 🆕 同花顺官方API(hithink) → 雪球(最后兜底)
```

只需在 loader 组装 `call_with_fallback` 的 `fallback_sources` 列表时插入 `(source_name="hithink", operation=fetch_fn)` 一项；熔断、限流、指数退避全部复用现有机制。

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
