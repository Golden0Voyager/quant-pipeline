# Review: EastMoney 资金流批量快照兜底（2026-10-17 方案）

> 评审时间：2026-10-17 15:00 前后（当次实测，非文档值）
> 评审对象：`docs/superpowers/plans/2026-10-17-eastmoney-fund-flow-snapshot-fallback.md`
> 结论：**方向对，但按现文档实施修不好问题。有 3 个阻断点，其中 2 个与"兜底"本身无关 ——
> 即使兜底抓到了数据，也写不进库。**

> **后续进展（2026-10-17 16:00）**：P0-1 与 P0-2 已由 PR #177 修复并合并（merge commit
> `e296187`）—— 空表也走重建、重建结果归一成 `code`，各带红证用例
> （`TestEmptySourceFallsBackToRebuild` / `TestRebuildRecordShapeMatchesWriter`），
> 规则同步进 `AGENTS.md` #8。**P0-3、P1-1、P1-2 仍未处理**；其中 P0-3 的参数（chunk 上限、
> 熔断阈值）必须等 IP 解封后重测，不能沿用本文引用的文档值。

---

## 零、先说结论

| 判断 | 结论 |
|---|---|
| 字段映射（f62/f66/f69/f72/f75/f184） | ✅ 成立，本次复验恒等式全部通过 |
| `ulist.np` 能批量取 | ⚠️ 单次少量可以，**500/块 × 12 块的规模本次复现失败（0%），且触发 push2* 全域封禁** |
| 快照值 = 最新交易日收盘值 | ✅ 成立（与 push2his 的 2026-10-16 值逐字段完全相等） |
| "只能填最新交易日"的边界 | ✅ 成立，但守卫条件写错了（见 P1-1） |
| 本方案能让 pipeline 在 clist 被封时不停摆 | ❌ **不能**（见 P0-1、P0-2） |

---

## 一、致命问题（P0，不修则本方案无效）

### P0-1 接线点在早退之后：clist 全封时新路径根本不会执行

`tasks/market_flow.py:155-168`：

```python
df = loader.get_market_fund_flow()
if df.empty:
    ...
    return {"status": "retained", ...}      # ← 到这里就返回了
```

`quant_hunter/data_loader.py::get_market_fund_flow` 只有两个 strategy，**都打 clist**
（`push2.eastmoney.com` / `push2delay.eastmoney.com`），全失败就 `return pd.DataFrame()`。
`providers.py:2812` 又刻意没有同花顺兜底。

于是"clist 被封"的实际形态是 **空 DataFrame**，走的是上面这条早退，
`_rebuild_market_day` 一次都不会被调用 —— 而 Task 2 恰恰只改 `_rebuild_market_day`。

Task 2 唯一能生效的场景是"有行但覆盖 < 90%"，那需要 clist **部分**成功。
生产库现状印证了这一点：`fund_flow` 最新日期 `2026-10-13`（5307 行），
10-14 ~ 10-16 三行都没有 —— 那是早退路径的产物，不是重建失败。

**修法**：把兜底上移到 `df.empty` 分支之前（`df.empty` 与"覆盖不足"合并成同一条决策），
并让 `--symbols` 子集运行也能用（现在 `symbols` 分支直接跳过 universe/守卫）。

### P0-2 重建层返回值下游用不了（既有 bug，本方案会原样继承）

`_rebuild_market_day`（`tasks/market_flow.py:141-143`）返回 `pd.DataFrame(records)`，
而 records 的键是 `symbol`：

```python
records = [{"symbol": code, "date": trade_date, ...}]
df = pd.DataFrame(records)          # 列名是 symbol / date / 六个数值
```

但下游两处都读 `code`：

- `_fund_flow_coverage`：`have = {_bare_code(c) for c in df["code"]}`（`"code" not in df.columns` → 空集）
- 写库循环：`code = str(row.get("code", "")).strip()` → `""` → `continue`

本次用 mock 实测（rebuild 返回 1000 行、universe 1000）：

```
result: {'saved': 0, 'total': 1000, 'status': 'degraded',
         'error': '资金流覆盖度 0.0% 低于水位 90%：1000 只应有，仅 0 只拿到...'}
save called with rows: not called
```

**即"重建成功"= 一行不写 + 报一个误导性的 degraded**。AGENTS.md 硬规则 #8 里
"重建后覆盖度通常回到 ~100%"这句与事实不符。

为什么测试没抓到：`tests/test_daily_pipeline.py` 里 mock 的 rebuild 返回的是
带 `code` 列的 `_df()`，而真实 `_rebuild_market_day` 返回的是 `symbol` 列 —— 典型
mock 与实现不同构（与 AGENTS.md 规则 #4 的 MockDatabase 盲区同型）。

本方案 Task 2 的用例只断言 `len(df) == 1`，**同样抓不到**，会给出"已修复"的假绿。

**修法**：`_rebuild_market_day` 里把 `symbol` 归一成 `code`（或在写库循环里兼容两者），
并把 Task 2 的断言改成"`save_fund_flow_batch` 收到 1 行"。

### P0-3 500/块 × 12 块的规模本次复现失败，失败形态是全域封禁而非优雅降级

同一台机器、按文档参数（`chunk=500`、`gap=2.0`、`retries=3`）跑全市场：

```
14:52  单次 5 个 secids        → 200, rc=0, 5 行, 恒等式全成立
14:53  push2his 600519/000001  → 200，返回 10-16 数据
14:53  12 块 × 500 secids 开跑
14:57  elapsed 256.1s, chunks=12, failed_chunks=12, rows=0, coverage 0.00%
15:00  push2 / push2delay / push2his → 全部 ConnectionError
       datacenter-web.eastmoney.com → 200；quote.eastmoney.com → 200
15:05  再试 n=5 → 仍 ConnectionError
```

三点后果：

1. **"30 秒 / 100% 覆盖"复现不出来**（本次 0%，耗时 256s，退避把时间吃满）。
   在没量准单请求上限之前，`SNAPSHOT_CHUNK = 500` 不能当实测值写进代码。
2. **失败形态是 `RemoteDisconnected`，不是 HTTP 错误**，`raise_for_status()` 永远不触发，
   只能靠宽 `except` 接住 —— 文档写的"请求失败按 retries 重试"在语义上没错，
   但没说清失败是"连接被关"，运维会去找 4xx/5xx。
3. **封禁是 push2* 全域的**：`push2his` 也一起挂。这意味着这个"兜底"会连带打死
   `fetch_market_day_records`（push2his）、`bars_snapshot`（`stock_zh_a_spot_em` → push2）、
   `scripts/reconcile_with_akshare.py`（`stock_zh_a_hist` → push2his）。
   pipeline 是并行跑的，等于兜底任务把同轮其他任务的源一起砸掉。

**修法**：先量准上限（100 / 200 / 500 逐档实测，留冷却间隔）；加熔断
（连续 N 块失败即停止并返回已拿到的部分）；退避总时长设上限（现在单块最坏 18s × 12 = 216s）。

> 顺带修正文档的一个前提：**"push2his 当前同样被封"不成立**。本次 14:53 它 200 正常返回
> 10-16 数据；它是被我这轮 ulist 打封之后才不可用的。所以第一级兜底目前是可用的，
> 方案的价值不是"唯一活路"，而是"12 个请求替代 5578 个请求"——这个理由更硬，也更该写进 PR。

---

## 二、重要问题（P1，会写出错数据或让守卫失效）

### P1-1 日期守卫写错了对象：不是周末，是盘中

文档 Review Focus 写"`_rebuild_market_day` 已有 `is_weekend` 守卫"——**事实错误**：
`tasks/market_flow.py:122-143` 没有任何日期/时段守卫，`is_weekend` 只在
`scripts/repair_fund_flow_decomposition.py` 与 `scripts/backfill_fund_flow_gaps.py` 里被用。

而且周末其实不是主要风险：周六跑时 `today = get_expected_latest_trading_day()` = 周五，
快照 = 周五收盘，贴周五是**对的**（本次实测快照与 push2his 的 10-16 值逐字段相等）。

真正的风险是**盘中运行**：当天是交易日，快照是未定型的盘中值，却被贴成"当日最终值"。
仓库已有现成工具：`core/market_time.market_phase()`（`post_close` 边界 16:00），
注释里就记着 2026-07-30「盘中写入的当日行」事故。

**判据应是**：`trade_date` 已收盘（`shanghai_now() >= 16:00` 或 `market_phase() == post_close`），
而不是"是否交易日"、更不是"是否周末"。

### P1-2 另一条写路径（收盘刷新）完全没覆盖

`core/refresh_adapters.py::FundFlowRefreshAdapter` 走
`fetch_fund_flow_records(loader, target)` → `loader.get_market_fund_flow()`，
没有兜底、`minimum_coverage=0.8`。clist 被封时这条路径同样空。
方案只改 daily 路径，PR 描述里要明确声明"刷新路径不在本 PR 范围"，否则会给人"修好了"的错觉。

### P1-3 文档里的"实测/事实"有多处不成立

| 文档说法 | 实际 |
|---|---|
| `_rebuild_market_day` 已有 `is_weekend` 守卫 | 无任何守卫 |
| push2his 当前同样被封 | 本次 14:53 正常；是 ulist 打封后才挂的 |
| 用"与 10-13 库值比值 0.08/-0.46/…"证明快照无日期 | 弱证据（不同日值本就不同）。硬证据是响应体无日期字段 + 与 push2his 当日值逐字段相等 |
| `tests/test_fund_flow_source.py` 原有 12 个用例 | 31 个 `test_` 函数（含 parametrize 更多） |
| Task 2 测试文件 `tests/test_new_tasks.py` | 该文件 2737 行里没有任何 market_flow / rebuild 用例；现有重建用例在 `tests/test_daily_pipeline.py:851/950/1078` |
| 新增硬规则 "#13 接在 #12 之后" | AGENTS.md 里 #10 物理上排在 #12 之后，追加时注意编号与顺序 |
| 覆盖率阈值 85% | 属实 |

这些不是吹毛求疵：实施者会照"已有守卫"去写测试、照"test_new_tasks.py"去找文件，
第一处会让你漏掉真正的守卫，第二处会让人以为测试已存在。

### P1-4 `missing` 必须靠集合差，不能靠计数

快照是**无序**返回、且**静默省略**停牌/退市/无效 secid 的行，返回行数 ≠ 请求行数。
`missing = 请求集合 - 返回集合`，且要按 `f12` 建索引（不能按位置对应）。
文档提到"每行以 f12 校验"，方向对，但没写"按 f12 建索引 + 集合求差"这一步。

另外 `_same_code` 用 `lstrip("0")` 比较：请求 `"1"` 与回显 `"000001"` 会被判为相等
（`tests/test_fund_flow_source.py` 里就有 `fetch_day_klines(session, "1")` 的用例）。
快照的 f12 恒为 6 位，风险低，但沿用同一函数等于把这个宽松比较带进新路径。

### P1-5 "整日替换"的语义要写进 docstring

`_rebuild_market_day` 现在的 docstring 说的是"逐票日 K 整日替换"。加了快照源之后，
"第一级拿到 3000/5563 也整体丢弃、全用快照"是对的（同源同口径，本次已实测一致），
但必须写清楚，否则后来者会以为是合并。

---

## 三、细节（P2）

- `core/fund_flow_source.py` 目前**没有 `import time`**，Task 1 用 `time.sleep(gap)` 要补。
- `SNAPSHOT_FIELDS` 含 `f14` 但不用；建议保留（日志排查有用）或去掉，别留着不明不白。
- 重试退避 `3*(attempt+1)`：3/6/9s，单块最坏 18s，12 块最坏 216s（本次实测 256s）。
  文档预期"30 秒"，差一个数量级。
- `fetch_market_snapshot_batch` 收 `session` 参数是**好设计**（`conftest._block_outbound_network`
  的 docstring 里记着的正是 `fetch_market_day_records` 内部自建会话导致测试打真网的事故）；
  `_rebuild_market_day` 里记得 `make_session()` 用完关闭或复用。
- 覆盖率 85% 的门禁：新增分支（熔断、时段守卫）都要有用例，否则新代码在 CI 里是"绿的空壳"。

---

## 四、建议的实施顺序（替代原 Task 1-3）

1. **修 P0-2**（`symbol` → `code` 归一 + 用例断言真的写库）。单独 commit。
   不修它，后面所有兜底都是白干 —— 这是当前最该先做的事，且与本 PR 主题无关，可以单独发。
2. **重排 `update_fund_flow`**：`df.empty` 也进兜底，`--symbols` 也能用（P0-1）。
3. **重新实测**单请求上限（100/200/500 逐档），用实测值替换 `SNAPSHOT_CHUNK`，加熔断（P0-3）。
4. **加 `market_phase` 时段守卫**（P1-1），删掉"is_weekend 已有"的错误前提。
5. 明确声明刷新适配器不在范围内，或一并覆盖（P1-2）。
6. 最后写 AGENTS.md 规则：写**可复现的数字与失败形态**（连接被关、push2* 全域封禁、
   熔断阈值），而不是"2 秒 + 3 次是实测值"这一句。

---

## 五、附：本次实测原始记录

```
# 字段映射 + 恒等式（5 只）
600519 f62=430061504 f66=-19589760 f72=449651264 f184=8.47  → f66+f72=f62 ✓, f69+f75=f184 ✓
300750 f62=-297433984 f66=-220106048 f72=-77327936 f184=-3.62 ✓
000001 f62=-24767431  f66=26962121  f72=-51729552 f184=-1.93 ✓
920002 f62=2455513    f66=0         f72=2455513  f184=6.35  ✓（北交所走 0. 前缀）

# 快照 vs push2his（2026-10-16 收盘）
600519  his=430061504,-19589760,449651264,8.47   snap 完全相同 ✓
000001  his=-24767431,26962121,-51729552,-1.93   snap 完全相同 ✓

# 生产库（只读）
fund_flow  最新 2026-10-13（5307 行），10-14/15/16 无行
daily_bars 最新 2026-10-16（5563 只），distinct ts_code 5578
→ 10-13 覆盖 5307/5578 = 95.2% > 90%，覆盖度守卫未触发过重建

# 拦截形状（15:00）
push2 / push2delay / push2his → RemoteDisconnected
datacenter-web / quote.eastmoney.com → 200
```

> 提醒：这次探测把本机 IP 在 `push2*` 上打封了（datacenter/quote 仍通）。
> 现在跑 pipeline，`daily_bars` / `bars_snapshot` 可能取不到数，
> 别把这段当成"东财彻底挂了"的证据，过一段时间再验恢复。
