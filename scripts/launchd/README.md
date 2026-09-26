# launchd 调度配置

macOS launchd 每日任务的安装模板与说明。纳入仓库以便审查与复现。

## ⚠️ 当前状态（2026-09-04）

**本机两台 launchd 任务均已卸载**（`launchctl bootout` + 删除
`~/Library/LaunchAgents/` 下对应 plist）。仓库内文件仅作**可复用模板**保留，
不代表机器当前在自动运行任何任务。

- `com.smartmoney.update`：曾部署版（8/26 修改）的排期是
  `StartCalendarInterval` **数组**（可见首项为周一 17:30），与仓库模板
  （每日 20:30 单 dict）不一致；部署版已删除且从未同步回仓库，原排期不可恢复。
  → 重新启用时请按当前期望的排期编辑模板，不要假设能"原样恢复"。
- `com.smartmoney.healthcheck`：7 月起一直部署（每天 20:00 健康检查），但
  **从未纳入仓库**；本目录同名 plist 是卸载后按运行痕迹重建的模板
  （依据 `~/Code/quant_data/logs/launchd_health.{out,err}.log` 与 launchctl 状态），
  文件头有标注，逐字段未与原文件核对过。

## 分工

| 入口 | 职责 | 触发方式 |
|------|------|----------|
| `com.smartmoney.update.plist` | 全量管道（`--task all`，模板默认每日 20:30） | launchd 定时（已停用） |
| `com.smartmoney.healthcheck.plist` | 健康检查（`--task health_check`，每日 20:00，模板） | launchd 定时（已停用） |
| `scripts/daemon.py` | 盘中每小时 `update_bars` 增量（非交易日自动跳过） | 手动 `start` 常驻 |
| 每周层（`--task weekly_backfill`） | WEEKLY 任务 + 补齐缺漏 + retry + health | 手动触发 |
| 每月层（`--task monthly_repair`） | MONTHLY/QUARTERLY 任务 + 备份→对账→vacuum 修复链 + health | 手动触发 |

各入口通过同一把全局锁（`core.lock.ProcessLock`）互斥，不会并发写库。

> 注：`--task all` 已收窄为每日层别名（TRADING_DAY+DAILY+ON_DEMAND），不再跑周/月/季任务。
> 每周/每月层仅手动触发：
> `uv run python daily_pipeline.py --task weekly_backfill|monthly_repair`，或 TUI 按 W / M 键。

## 启用前提

- **解释器用本仓库自己的 venv**：模板指向 `<repo>/.venv/bin/python3`（由 `uv sync` 生成）。
  不要把解释器/`PATH`/`PYTHONPATH` 指向兄弟仓库 `quant_hunter` 的 venv —— 那会让本仓库入口
  跑在别的依赖集上。兄弟仓库 `smartmoney_hunter` 的路径由 `core._bootstrap.ensure_sibling_paths()`
  在运行期从 `__file__` 自动注入（默认 `~/Code/quant_hunter/src`，可用 `QUANT_HUNTER_PATH` 覆盖），
  因此**无需**在 plist 里配 `PYTHONPATH`。
- 若本机 checkout 不在 `/Users/hainingyu/Code/quant_pipeline`，连同 `ProgramArguments`、`PATH`、
  `WorkingDirectory`、`QUANT_DB_PATH` 一并改成实际路径。

## 安装（启用自动运行）

```bash
# 1. 拷贝模板并按需编辑：本仓库路径（解释器/PATH）、NOTIFICATION_*、排期
cp scripts/launchd/com.smartmoney.update.plist ~/Library/LaunchAgents/
# 2. 加载
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.smartmoney.update.plist
#   旧版兼容：launchctl load ~/Library/LaunchAgents/com.smartmoney.update.plist
```

需要健康检查任务时，对 `com.smartmoney.healthcheck.plist` 重复同样两步。

## 卸载 / 临时停用

```bash
# 卸载（不删文件）
launchctl bootout gui/$(id -u)/com.smartmoney.update
#   旧版兼容：launchctl unload ~/Library/LaunchAgents/com.smartmoney.update.plist
# 彻底移除：再删掉 plist 文件
rm ~/Library/LaunchAgents/com.smartmoney.update.plist
```

临时停用：先 `bootout`，再把文件改名为 `*.plist.disabled`，
避免后续误执行 `launchctl load`/`bootstrap` 重新启用。

## 与 TUI 的状态耦合

`tui/services/process.py` 的 Dashboard 会查询 launchd 中
`com.smartmoney.update` 是否活跃（决定"定时任务"状态展示）。
- 任务未安装时显示为未激活，不影响其他功能；
- 若改动该 label，需同步 `tui/services/process.py` 的查询目标。

## 离线跳过（2026-09 起）

全量管道（`--task all/daily`）启动前内置联网预检（`core/netcheck.py`）：
机器无网络时本轮**干净跳过**（仅一条 INFO、退出码 0），不产生
ERROR/熔断/失败任务/告警，数据由下次联网运行增量补齐。
- 无需在 plist 里额外配置；
- 仅当希望"离线也照跑"时，在 `EnvironmentVariables` 增加
  `QUANT_ALLOW_OFFLINE=1`（逃生门，与 `--force` 语义解耦）。

## 无人值守告警

管道三处关键事件会经 `core.notifications.notify_all` 外发：

- 单任务异常终止（`core/runner.py`）
- AkShare 熔断中止（`tasks/bars.py`）
- 整轮完成汇总（`daily_pipeline.py run_all`，失败时 error 级并列出失败任务）

默认 `NOTIFICATION_TYPE=console` 只写日志。要真正推送到飞书/钉钉：

1. 在 plist 的 `EnvironmentVariables` 里把 `NOTIFICATION_WEBHOOK_URL` 填上机器人 Webhook 地址
2. `NOTIFICATION_TYPE` 改为 `webhook`
3. `NOTIFICATION_LEVEL` 保持 `error`（只推失败）或改 `info`（含每日完成汇总）

**注意**：webhook URL 是凭据，不要把填好真实地址的 plist 提交回仓库。

`scripts/daemon.py` 连续失败 3 次也会经同一通道告警，其环境继承自启动 shell，
可用 `launchctl setenv` 或在启动命令前 export。
