# launchd 调度配置

macOS launchd 的每日全量管道调度配置，纳入仓库以便审查与复现。

## 分工

| 入口 | 职责 | 触发方式 |
|------|------|----------|
| `com.smartmoney.update.plist` | 每日 20:30 全量管道（`--task all`） | launchd 定时 |
| `scripts/daemon.py` | 盘中每小时 `update_bars` 增量（非交易日自动跳过） | 手动 `start` 常驻 |
| 每周层（`--task weekly_backfill`） | WEEKLY 任务 + 补齐缺漏 + retry + health | 手动触发 |
| 每月层（`--task monthly_repair`） | MONTHLY/QUARTERLY 任务 + 备份→对账→vacuum 修复链 + health | 手动触发 |

三者通过同一把全局锁（`core.lock.ProcessLock`）互斥，不会并发写库。

> 注：`--task all` 现已收窄为每日层别名（TRADING_DAY+DAILY+ON_DEMAND），不再跑周/月/季任务；
> launchd 每日 20:30 的配置无需改动。每周/每月层仅手动触发：
> `uv run python daily_pipeline.py --task weekly_backfill|monthly_repair`，或 TUI 按 W / M 键
> （弹窗确认立即/稍后/取消），TUI 下拉「工具」分组也有对应入口。

## 安装

```bash
cp scripts/launchd/com.smartmoney.update.plist ~/Library/LaunchAgents/
# 按需编辑：python 解释器路径、NOTIFICATION_* 告警配置
launchctl load ~/Library/LaunchAgents/com.smartmoney.update.plist
```

卸载：`launchctl unload ~/Library/LaunchAgents/com.smartmoney.update.plist`

临时停用：把文件改名为 `com.smartmoney.update.plist.disabled`。

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
