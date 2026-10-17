# EastMoney 资金流批量快照兜底 Implementation Plan

> ⚠️ **本方案尚未实施，且部分实测前提已被推翻 —— 动手前先读同目录的
> `2026-10-17-eastmoney-fund-flow-snapshot-fallback.review.md`。**
>
> 三处必须先纠正：①`_rebuild_market_day` **没有** `is_weekend` 守卫（本文件多处当作已有）；
> ②评审时 `push2his` **是通的**，「同样被封」不成立；③`chunk=500` × 12 块的规模在评审时
> **复现失败**（12/12 块 `RemoteDisconnected`、覆盖 0%、耗时 256s），且失败形态是
> `push2*` 全域封禁（连带 `push2his`），不是可重试的限流。
>
> 另：本文 Task 2 的接线点（`_rebuild_market_day`）已被 PR #177 前移 —— 空表也会走重建，
> 因此「clist 全封 → 空表 → 早退」这条路已经修好。

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 当东财榜单端点 `clist/get` 被封时，改用仍可用的批量快照端点 `ulist.np/get` 取全市场资金流，使 pipeline 不再完全停摆。

**Architecture:** 在 `core/fund_flow_source.py` 新增 `fetch_market_snapshot_batch()`，与既有 `fetch_market_day_records()` 返回同构的 `(records, missing)`，由 `tasks/market_flow.py` 的重建层依次尝试。universe 取自本地 `daily_bars`，不依赖东财，从而绕开被封的 `clist`。

**Tech Stack:** Python 3.12 / `curl_cffi`（经 `core.source_client.get_session`）/ requests / pytest / ruff / mypy

**Spec:** 本文件即规格；下述实测数据是全部决策依据。

## Global Constraints

- 仓库根：`/Users/hainingyu/Code/quant_pipeline`，包管理器只能用 `uv`，运行 `uv run python <script>.py`
- **禁止复制第二份市场前缀实现**：必须调用 `core.fund_flow_source.secid_for`。AGENTS.md 硬规则记录了 `920` 必须先于 `9` 判北交所，自行实现会把 350 只 920 段北交所判成沪市（实测 `secid=1.920002` 返回 `rc:100, data:null`）
- **禁止按 `data_source` 推断语义**（AGENTS.md 规则 #10）：字段映射必须由恒等式实测钉死并有门禁
- 一文件一提交；commit message 中英双语、英文在前
- 分支 `fix/*`；PR 需 CI 绿
- 覆盖率阈值 `fail_under = 85`

## 实测依据（2026-10-17，全部当次实测，非文档值）

**拦截形状**：东财首页 200，但 `push2.eastmoney.com/api/qt/clist/get` **3/3 轮次全败**，`ulist.np/get` **间歇可用**——4.2 秒连打 12 个请求时失败，加 2 秒间隔 + 3 次重试后 **12/12 块全成功**。故 `ulist.np` 是限流而非封禁。

**批量能力**：500 只/请求，12 块覆盖 5,563 只，30 秒，覆盖率 **100.00%**（含 616 只 688 科创板，依赖 `secid_for` 给出 `1.` 前缀）。

**字段映射**，由三条恒等式验证（贵州茅台 / 宁德时代 / 平安银行逐条成立）：

| 我们的字段 | f 码 | 验证 |
|---|---|---|
| `main_net_inflow` | f62 | f62 == f66 + f72 |
| `main_net_inflow_pct` | f184 | f184 == f69 + f75 |
| `super_large_net_inflow` | f66 | f66+f72+f84+f78 == 0 |
| `super_large_net_inflow_pct` | f69 | 同上占比 |
| `large_net_inflow` | f72 | 同上 |
| `large_net_inflow_pct` | f75 | 同上 |

**该端点不带日期**：`ulist.np` 是即时快照，响应中无可用交易日字段。实测其值与库里 2026-10-13 的主力净额毫无关系（比值 0.08 / -0.46 / -0.12 / -1.26 / -0.40 / 0.57）。

## 能力边界（本计划最重要的约束）

`ulist.np` **只能填最新交易日，不能补历史**。把 10-16 的快照贴到 10-14 名下是伪造数据。

因此本计划**不解决** `fund_flow` 在 2026-10-14 ~ 10-16 的缺口——那四天只能由历史日 K 端点 `push2his` 恢复，而该主机当前同样被封。本计划的价值是**向前保护**：下一个交易日 `clist` 仍被封时，pipeline 不会整轮停摆。

## Review Focus

以下输入/情形规格未直接覆盖，各自由对应任务的测试钉住：

- **`ulist.np` 返回的 f12 与请求的代码不符** → 必须丢弃该行。AGENTS.md 记录东财把短于 6 位的代码当前缀匹配（`secid=0.1318` 返回 `code=131800`）。本端点不回显 secid，只能靠 f12 校验；**回显缺失或不一致一律拒绝**，宁可响亮地取不到
- **某块全部失败** → 不中断整轮，其余块照常返回，失败的代码进 `missing`
- **源返回空 `data` 或 `diff` 为空**（停牌退市）→ 视为该块未取到，不写脏数据
- **`codes` 为空或 universe 不支持** → 返回 `([], [])`，不发请求
- **网络守卫**：测试套件有 autouse 出网守卫（`tests/conftest.py::_block_outbound_network`），新测试必须 mock session，任何真出网会抛 `OSError` 并让用例上抛失败
- **非交易日调用** → 快照无日期可验，若 `trade_date` 是周末或非交易日，调用方不得使用本路径；`_rebuild_market_day` 已有 `is_weekend` 守卫，需确认新路径同样受约束

---

### Task 1: 批量快照抓取函数

**Files:**
- Modify: `core/fund_flow_source.py`（在 `fetch_market_day_records` 之后追加）
- Test: `tests/test_fund_flow_source.py`

**Interfaces:**
- Consumes: `secid_for(code) -> str`（同文件既有）、`NUMERIC_COLUMNS`（同文件既有）、`make_session() -> Any`（同文件既有）
- Produces:
  ```python
  SNAPSHOT_URL: str = "https://push2.eastmoney.com/api/qt/ulist.np/get"
  SNAPSHOT_FIELDS: str
  SNAPSHOT_FIELD_MAP: dict[str, str]   # f 码 → 我们的字段名
  SNAPSHOT_CHUNK: int = 500
  SNAPSHOT_GAP_SECONDS: float = 2.0
  SNAPSHOT_RETRIES: int = 3

  def fetch_market_snapshot_batch(
      session: Any,
      codes: Iterable[str],
      trade_date: str,
      chunk: int = SNAPSHOT_CHUNK,
      gap: float = SNAPSHOT_GAP_SECONDS,
      retries: int = SNAPSHOT_RETRIES,
  ) -> tuple[list[dict[str, Any]], list[str]]:
  ```
  返回 `(records, missing)`，`records` 每项为 `{"symbol", "date", <NUMERIC_COLUMNS 六项>}`——与 `fetch_market_day_records` **同构**，使 `_rebuild_market_day` 可互换使用。

- [ ] **Step 1: 写失败测试**

在 `tests/test_fund_flow_source.py` 追加。构造一个假 session，其 `get` 返回 `{"data": {"diff": [{"f12": "000001", "f62": 100.0, "f184": 1.0, "f66": 60.0, "f69": 0.6, "f72": 40.0, "f75": 0.4}]}}`，断言：

```python
def test_snapshot_batch_maps_fields_and_shape():
    records, missing = fetch_market_snapshot_batch(_fake_session(...), ["000001"], "2026-10-19")
    assert missing == []
    assert records == [{"symbol": "000001", "date": "2026-10-19",
                        "main_net_inflow": 100.0, "main_net_inflow_pct": 1.0,
                        "super_large_net_inflow": 60.0, "super_large_net_inflow_pct": 0.6,
                        "large_net_inflow": 40.0, "large_net_inflow_pct": 0.4}]
```

同时写这四个用例：

```python
def test_snapshot_batch_rejects_row_whose_code_differs():
    # f12 回显 000002 而请求 000001 → 该行丢弃、代码进 missing
def test_snapshot_batch_accepts_absent_code_only_when_row_is_absent():
    # diff 里没有该票 → 进 missing，不得静默写 0
def test_snapshot_batch_survives_a_fully_failed_chunk():
    # 第 2 块 raise → 其代码进 missing，第 1 块照常返回
def test_snapshot_batch_sends_no_request_for_empty_universe():
    # codes=[] → session.get 不被调用，返回 ([], [])
```

- [ ] **Step 2: 跑测试确认失败**

Run: `uv run pytest tests/test_fund_flow_source.py -q -k snapshot`
Expected: FAIL —— `NameError: name 'fetch_market_snapshot_batch' is not defined`

- [ ] **Step 3: 实现**

在 `core/fund_flow_source.py` 末尾追加。常量值照抄上面 Interfaces 块，字段映射照抄「实测依据」表格，`SNAPSHOT_FIELDS` 是六个 f 码加 `f12,f14` 逗号拼接。

函数体按此结构：去重排序 `codes` → 空则直接返回 → 按 `chunk` 切块 → 每块用 `",".join(secid_for(c) for c in block)` 拼 `secids` → `session.get(SNAPSHOT_URL, params={"fltt":2,"invt":2,"fields":SNAPSHOT_FIELDS,"secids":...}, timeout=(10,20))` → `raise_for_status()` → 取 `((resp.json().get("data") or {}).get("diff") or [])` → 每行以 `str(item.get("f12"))` 经 `_same_code` 校验，**校验不过或 f12 缺失一律丢弃并计入 missing**，通过的按 `SNAPSHOT_FIELD_MAP` 组装 record → 块之间 `time.sleep(gap)`。

请求失败的块按 `retries` 重试，退避 `3 * (attempt + 1)` 秒；用尽后整块代码进 missing，**不抛异常**（单块失败不得中断整轮）。

docstring 必须写明：该端点**无日期字段**，日期由调用方贴上，因此只能用于最新交易日，补历史必须走 `fetch_day_klines`。

- [ ] **Step 4: 跑测试确认通过**

Run: `uv run pytest tests/test_fund_flow_source.py -q`
Expected: PASS，原有 12 个用例全绿

- [ ] **Step 5: 跑类型与风格门禁**

Run: `uv run ruff check core/fund_flow_source.py tests/test_fund_flow_source.py && uv run mypy`
Expected: 两者均无输出即通过

- [ ] **Step 6: 提交**

```bash
git add core/fund_flow_source.py tests/test_fund_flow_source.py
git commit -m "feat(fund-flow): add batched ulist snapshot fallback

...（中英双语，中文块在后，说明 clist 被封与限流的实测形状、字段映射的
恒等式验证、以及该端点无日期只能填最新交易日的边界）"
```

---

### Task 2: 接线到重建层

**Files:**
- Modify: `tasks/market_flow.py:122-143`（`_rebuild_market_day`）
- Test: `tests/test_new_tasks.py`

**Interfaces:**
- Consumes: Task 1 的 `fetch_market_snapshot_batch(session, codes, trade_date) -> (records, missing)`
- Produces: 无新增公开接口。`_rebuild_market_day(universe, trade_date) -> pd.DataFrame | None` 签名不变，返回 DataFrame 结构不变

- [ ] **Step 1: 写失败测试**

在 `tests/test_new_tasks.py` 追加：patch `core.fund_flow_source.fetch_market_day_records` 返回 `([], ["000001"])`（模拟日 K 端点被封），patch `fetch_market_snapshot_batch` 返回一条正常 record，调用 `market_flow._rebuild_market_day({"000001"}, "2026-10-19")`，断言返回值是 `DataFrame` 且 `len(df) == 1`。

再写一条：两级都失败时返回 `None`（调用方据此不写任何东西）。

- [ ] **Step 2: 跑测试确认失败**

Run: `uv run pytest tests/test_new_tasks.py -q -k rebuild`
Expected: FAIL —— 返回 `None` 而非 DataFrame

- [ ] **Step 3: 实现**

改 `_rebuild_market_day`：先调 `fetch_market_day_records`（保持原样，日 K 路径带日期校验，优先）；**若返回空**，再调 `fetch_market_snapshot_batch`。后者需要一个 session——复用该模块已有的 session 获取方式，不得新建第二套会话构造。

保留原有的「全失败即返回 None、绝不写口径不一致的数据」语义不变。两条路径的数据**不做合并**（理由见该函数现有 docstring：混口径正是要消灭的东西）。

日志需打出用的是哪一级，便于事后判断口径来源。

- [ ] **Step 4: 跑测试确认通过**

Run: `uv run pytest tests/test_new_tasks.py -q`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add tasks/market_flow.py tests/test_new_tasks.py
git commit -m "feat(fund-flow): fall back to batched snapshot when day K is blocked"
```

---

### Task 3: 规则沉淀与全量门禁

**Files:**
- Modify: `AGENTS.md`（新增硬规则 #13，接在 #12 之后）
- Test: 无新增测试

**Interfaces:**
- Consumes: Task 1、2 的最终形态
- Produces: 无代码接口

- [ ] **Step 1: 补 AGENTS.md 硬规则**

写明四件事：① `clist` 持续被封而 `ulist.np` 只是限流，2 秒间隔 + 3 次重试是实测值不是文档值；② 字段映射由三条恒等式钉死，**禁止重新推导**；③ 该端点无日期，**只能填最新交易日，补历史必须走日 K**——贴错日期即伪造数据；④ 科创板 688 必须用 `secid_for` 的 `1.` 前缀，自行实现会静默丢掉 616 只。

- [ ] **Step 2: 跑全量门禁**

Run: `uv run ruff check && uv run mypy && uv run pytest -q --cov --cov-report=term`
Expected: 三项均通过，覆盖率 ≥ 85%，无失败用例

- [ ] **Step 3: 提交并开 PR**

```bash
git add AGENTS.md
git commit -m "docs(agents): record the ulist snapshot fallback and its date boundary"
git push -u origin fix/fund-flow-snapshot-fallback
gh pr create --base main --title "..." --body-file <pr.md>
```

PR 描述须包含：拦截形状的实测证据、字段映射的恒等式验证、**明确声明本 PR 不修复 10-14~10-16 的缺口**（那需要日 K 端点，当前同样被封）、以及 2 秒间隔的实测来源。
