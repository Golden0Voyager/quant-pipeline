# Runbook：WAL 体积上限与空间回收

> 面向运维/值守。目标：解释 `quant_core.db-wal` 为什么**只涨不缩**、仓库用什么机制兜住它，
> 以及 `QUANT_WAL_SIZE_LIMIT_MB` 该设多少、怎么观测、出问题怎么应急。
> **本流程不需要改代码**；唯一需要你决定的就是一个环境变量。

## 背景（一分钟）

SQLite 的 WAL 文件（`~/Code/quant_data/quant_core.db-wal`）是**只涨不缩的高水位线**：
它的尺寸由历史上**最大的那一次事务**决定。单个大事务会把它撑到该事务那样大，此后即使
checkpoint 早就完成、帧被反复复用，文件也不会缩小。

- `PRAGMA wal_autocheckpoint`（默认 1000 帧 ≈ 4 MB）只负责把帧**写回主库**（writeback），
  **不负责缩文件**。
- 实测成因（2026-09-25，scratch 库）：一次 310.6 MB 的单事务提交后，WAL 停在 310.6 MB；
  之后**再多的写入都不缩**。
- 生产实测：`quant_core.db-wal` 曾长期占着 **2.39 GiB**，而同一时刻活跃日志只有
  **22,350 帧（87 MB）** —— `PRAGMA wal_checkpoint(PASSIVE)` 返回 `busy=0`，
  即**没有任何读者在阻塞**，文件里 96% 是活跃日志之外的死区。
- 注意：这**不是**进程泄漏、也不是没人跑 checkpoint。孤儿进程、未 checkpoint 都排查过，
  均不是原因。

## 两个手段（互补，不是二选一）

| 手段 | 作用时机 | 效果 |
| --- | --- | --- |
| `PRAGMA journal_size_limit=<N>`（体积上限） | 「超大 WAL 代被 reset 后的**第一次写入**」 | 把文件缩回 **N 字节**，之后常态占位就是 N |
| `PRAGMA wal_checkpoint(TRUNCATE)`（显式截断） | **调用即生效** | 立即截到 **0**，空间还给操作系统 |

两者各自解决一半问题：

- 上限兜住日常：任何一次超大写入之后，下一次写入就把文件缩回上限。
- 显式截断给出确定性：批量回填的典型形态是「巨型事务之后本轮就结束了」，而上限要等到
  **再有人写入**才生效——那一次写入可能永远不会来。所以 `daily_pipeline` 收尾会显式截断一次。

**上限不阻止尖峰**：巨型事务存在的那一刻，WAL 仍然会占满它那么大；上限管的是「停止写入后
长期保留的尺寸」。要真正避免尖峰，得从源头避免超大单事务（分批提交）。

**两个动作都不丢数据**：checkpoint 是先把 WAL 里的已提交内容落进主库，再删除 WAL。

## 唯一出处与接线点（运维只需知道这里）

- `core/db_pragmas.py` 是本仓库**唯一**允许执行 `PRAGMA journal_mode=WAL` 的地方：
  - `apply_write_pragmas(conn, ...)` —— 开启 WAL 并声明体积上限，所有写连接都走它；
  - `truncate_wal(db_path)` —— 尽力而为的 `wal_checkpoint(TRUNCATE)`，**绝不抛异常**
    （失败只记日志，绝不影响退出码）；
  - `wal_bytes(db_path)` —— 读当前 `-wal` 字节数。
- 显式回收的两个调用点：
  - `daily_pipeline.py` 的 `main()` `finally`（覆盖正常结束/异常/`sys.exit` 各条退出路径）；
  - `scripts/reconcile_with_akshare.py` 的收尾。
- 门禁：`tests/test_db_pragmas.py::test_wal_is_enabled_in_exactly_one_place`
  卡住「别处不得再出现 `PRAGMA journal_mode`」。

> 为什么必须有一个统一函数？`journal_size_limit` 是**每连接**设置、**不写进库头**：
> 实测设完之后关闭连接，新连接读回仍是默认值 `-1`。所以「找一处设一次」在架构上不成立，
> **每个写连接都必须自己设**；历史上散在 7 处（`providers` ×2、`core/migrations`、`scripts` ×3 …）
> 的写法迟早会漏，而**漏掉一处就等于没有上限**——那一处照样「工作正常」（WAL 生效、读写都对），
> 只是悄悄把磁盘吃光。
>
> 如果你新增了一个直接开 WAL 的写脚本：**不要**自己写 `PRAGMA journal_mode=WAL`，
> 调用 `apply_write_pragmas(conn)` 即可，否则门禁会红。

## 环境变量

| 变量 | 默认 | 单位 | 说明 |
| --- | --- | --- | --- |
| `QUANT_WAL_SIZE_LIMIT_MB` | `64` | MiB | WAL 体积上限。**每个写连接在建立时读取**（调用时读 env，不是导入时） |

非法取值的行为（都会**告警并回落默认 64 MiB**，不会静默）：

- 非整数（如 `abc`、空串以外的东西）→ 回落 64 MiB；
- **负数 → 回落 64 MiB**（本仓库不支持用负值表达「无上限」）。

配置位置：项目根 `.env`（已 gitignore，`.env` 不覆盖已存在的 shell 变量，`export` 优先级更高），
或写在调度任务的环境里。

## 取值语义（实测，2026-09-25）

同一个 scratch 库、同一次 50 MB 单事务，只改上限值，观察「巨型事务之后的后续小写入」：

| `journal_size_limit` | 巨型事务后 | 之后第一次小写入后 | 含义 |
| --- | --- | --- | --- |
| `0` | 50.4 MiB | **0 MiB** | 截到 0，最激进、最省磁盘 |
| `-1`（SQLite 默认，本仓库拿不到） | 50.4 MiB | 50.4 MiB | **永不缩**，即高水位线一直留着 |
| `8388608`（8 MiB） | 50.4 MiB | **8.0 MiB** | 缩到上限值，常态占位 = 上限 |

带显式 `TRUNCATE` 时三者都会立刻变成 0——所以**只要收尾截断正常跑，日常看到的就是 0**。

## 怎么调 `QUANT_WAL_SIZE_LIMIT_MB`

默认 **64 MiB** 的理由：主库约 5.9 GB，64 MiB 可以忽略；而日常单日增量只有几千行
（几 MB），远小于上限，所以只在真正的大事务之后才触发一次截断。

调参只有一个权衡：**磁盘占用 vs 缩容频率（写路径上的额外 I/O）**。

| 场景 | 建议 | 代价 |
| --- | --- | --- |
| 默认/不确定 | **留默认 64** | 无需关注 |
| 磁盘紧张，或你希望 WAL 常态极小 | 调小到 `8`，甚至 `0` | 缩容更频繁，落在「reset 后的下一次写入」上 |
| 频繁跑大批量回填（单次写入几十~几百 MB），想少些「缩了又长、长了又缩」的抖动 | 调大到 `128`~`256` | 静止时 WAL 最多占那么大的磁盘 |
| 你以为要设「无上限」 | **做不到**：负值被拒并回落 64；把值调大即可近似 | — |

修改步骤：

```bash
# 1) 在项目根 .env 里加一行（或 export）
echo 'QUANT_WAL_SIZE_LIMIT_MB=128' >> .env

# 2) 下一次写连接建立时即生效（无需重启机器；已在跑的连接沿用旧值）
```

`0` 是**合法且最激进**的取值：每次超大代被 reset 后的第一次写入，文件直接截到 0。

## 观测

```bash
# 当前 WAL 占用（无文件 = SQLite 已自行删除 = 0）
ls -lh ~/Code/quant_data/quant_core.db-wal

# 回收日志（回收成功才有，INFO 级）
grep '🧹 WAL 已回收' ~/Code/quant_data/logs/smartmoney_$(date +%Y%m%d).log
```

`truncate_wal` 返回并打日志的字段：`ok`（checkpoint 完成且无冲突）、`busy`、
`before_bytes` / `after_bytes` / `reclaimed_bytes`（释放字节）、`elapsed_ms`、`error`。
若 `busy=1`，日志是「WAL 回收未完成：… 上有事务占用」，属正常竞争，下一轮会再试。

## 应急：手动回收一次

需要立刻把空间还给磁盘（例如磁盘告急）时，直接跑一次 TRUNCATE——**与其他连接共存**，
TUI 或正在跑的分批写入都不影响（生产库实测 2.39 GiB → 0 用时 5.5 s）：

```bash
sqlite3 ~/Code/quant_data/quant_core.db "PRAGMA busy_timeout=5000; PRAGMA wal_checkpoint(TRUNCATE);"
```

该命令是只读语义上的「落盘 + 删 WAL」，不改动任何已提交数据。

## 回收后自检（可选，磁盘告急或有怀疑时做）

```bash
sqlite3 ~/Code/quant_data/quant_core.db \
  "PRAGMA quick_check; SELECT page_count*page_size FROM pragma_page_count(), pragma_page_size(); PRAGMA freelist_count;"
ls -l ~/Code/quant_data/quant_core.db
```

期望：`quick_check` 为 `ok`；`page_count × page_size` 与主库文件字节精准一致；
`freelist_count` 为 0 表示收缩后没有留下空洞（生产库 PR #129 验收即为此结果）。

## 回退

- 把 `.env` 里的 `QUANT_WAL_SIZE_LIMIT_MB` 删掉或改回 `64` 即回到默认，下次新连接生效。
- 回收/上限机制**从不修改或删除已写入的数据**，回退无数据风险。
- 若发现回收影响写路径（理论上只是偶发一次 TRUNCATE 的 I/O），把上限调大即可减少触发频率。
