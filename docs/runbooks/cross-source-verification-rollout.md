# Runbook：收盘刷新跨源校验的启用与调参

> 面向运维/值守。目标：把 `--refresh-today` 的跨源抽样校验（用雪球对比日线收盘价/成交量）
> 从"默认关闭"安全地推进到"真降级"。**这是运维/观测流程，不需要改代码。**

## 背景（一分钟）

- 跨源校验只做一件事：收盘刷新后，随机抽约 30 只股票，把**我们库里刚写入的收盘价/成交量**
  和**雪球的同日日线**对比，对不上就记录/告警。
- 它是防"主源（AkShare）整体错了但自身看起来自洽"的最后一道保险，**只读，绝不写库**。
- 三个安全默认：
  - `REFRESH_CROSS_SOURCE` 默认 **关闭**；
  - 一旦开启，`REFRESH_CROSS_SOURCE_REPORT_ONLY` 默认 **开启**（只记录、不降级任何任务）；
  - 雪球不支持北交所，北交所票不参与抽样。
- 已知**待实测假设**：库里成交量单位是"手"（AkShare 东财），雪球是"股"，代码里按 **÷100**
  归一。这个假设正是下面第 2 步要用真实数据确认的重点。

## 相关环境变量

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `REFRESH_CROSS_SOURCE` | `0`（关） | 总开关。设 `1` 开启抽样校验 |
| `REFRESH_CROSS_SOURCE_REPORT_ONLY` | `1`（只记录） | `1`=只记录不降级；`0`=真降级（第 3 步才翻） |
| `REFRESH_CROSS_SOURCE_TASK` | `update_bars` | 被抽样校验的任务 |
| `REFRESH_CROSS_SOURCE_SAMPLE_SIZE` | `30` | 抽样股票数（跨五大板块分层） |
| `REFRESH_CROSS_SOURCE_PRICE_TOL` | `0.005` | 价格相对容差（**临时值，须调**） |
| `REFRESH_CROSS_SOURCE_VOLUME_TOL` | `0.05` | 成交量相对容差（**临时值，须调**） |

> 校验源用雪球，需已配置 `XUEQIU_TOKEN`。

## 第 1 步：开启，但只观察（report-only）

在收盘刷新的运行环境（`.env` 或调度任务的环境）设置：

```bash
export REFRESH_CROSS_SOURCE=1
# REFRESH_CROSS_SOURCE_REPORT_ONLY 默认即为 1，可显式写上以防误改：
export REFRESH_CROSS_SOURCE_REPORT_ONLY=1
```

然后照常运行收盘刷新：

```bash
rtk uv run python daily_pipeline.py --refresh-today
```

此阶段：命中不一致**只写日志/审计元数据，绝不降级任务**。哪怕单位没对齐，最坏也只是多几行告警。

## 第 2 步：观察约一周，核实假设并调容差

每天刷新后，从审计表读跨源结果（`refresh_task_runs` 里 `update_bars` 那行的 `metadata` JSON，
键为 `cross_source`），关注三个桶：

- `mismatched`：**真不一致**（价或量超出容差）——这是要调参的对象。
- `unverifiable`：雪球当天无数据（停牌等）——**正常，不代表错**，不要据此调容差。
- `reference_dead`：若为真，说明当天雪球对全部抽样股票零命中（token 失效/接口异常），需排查。

也可直接看日志中的 `cross_source` 警告行（`mismatched` / `unverifiable` 分别独立打印）。

判断与调参：

1. **成交量单位**：若 `mismatched` 几乎全是"量差约 100 倍"，说明 ÷100 假设方向对但边界待定，
   或某些源（如 yfinance 兜底行）单位不同——据实调 `REFRESH_CROSS_SOURCE_VOLUME_TOL`
   或排查数据来源；若量差稳定接近 1（即 ÷100 后吻合），说明假设成立。
2. **价格**：qfq 已对齐，正常应只有极小舍入差；若 `mismatched` 里价差普遍偏大，先查是否复权口径漂移。
3. 把 `PRICE_TOL`/`VOLUME_TOL` 调到"真实的偶发差异不再误报、但明显错误仍能抓出"的水平。

```bash
export REFRESH_CROSS_SOURCE_PRICE_TOL=0.005    # 按观察结果调整
export REFRESH_CROSS_SOURCE_VOLUME_TOL=0.05    # 按观察结果调整
```

**通过标准**：连续数日 `mismatched` 稳定为空或仅剩可解释的个别项，`unverifiable` 都能对应到停牌等合理原因。

## 第 3 步：翻成"真降级"

达标后，把观察模式关掉，让校验真正生效：

```bash
export REFRESH_CROSS_SOURCE_REPORT_ONLY=0
```

此后行为：抽样命中真不一致 → 对这些股票**定向重试一次** → 仍不一致则该任务
**降级、保留旧数据、进程非零退出**；`unverifiable` 永不单独触发降级；若雪球整体零命中（`reference_dead`）则明确降级报错。

## 回退

任何阶段发现误报过多或影响生产，立即回退到最安全态：

```bash
export REFRESH_CROSS_SOURCE=0          # 完全关闭；或
export REFRESH_CROSS_SOURCE_REPORT_ONLY=1   # 保留观察、停止降级
```

跨源校验**从不修改或删除已写入的数据**，回退无数据风险。
