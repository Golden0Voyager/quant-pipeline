# quant_pipeline

A股量化数据自动化抓取管道，支持 AkShare / yfinance 双源，具备断点续传与守护进程能力。

---

## 快捷命令

在终端中直接使用以下命令（已配置在 `~/.zshrc`）：

| 命令 | 作用 |
|------|------|
| `pipe-run` | 立即执行完整数据更新 |
| `pipe-resume` | 断点续传，从上次中断处继续 |
| `pipe-daemon` | 启动守护进程（崩溃自动重启 + 自动 resume） |
| `pipe-stop` | 停止守护进程 |
| `pipe-logs` | 查看最近日志 |
| `pipe-mon` | 实时监控运行中的进程和日志 |

---

## 使用方法

### 1. 单次全量更新

```bash
pipe-run
```

等价于：

```bash
cd /Users/hainingyu/Code/quant_pipeline
DISABLE_YFINANCE_FALLBACK=1 FORCE_AKSHARE=1 ./manager.sh run
```

### 2. 断点续传

如果之前中断了（Ctrl+C），下次直接继续：

```bash
pipe-resume
```

已成功的股票数据会保留在数据库和缓存中，不会重复抓取。

### 3. 守护进程模式（推荐长期挂机）

```bash
pipe-daemon
```

特性：
- 异常退出后 **30 秒自动重启**
- 每次重启自动 `--resume`
- 正常完成后休眠 1 小时，再继续
- 日志写入 `~/Code/data/quant_data/logs/daemon.log`

停止守护进程：

```bash
pipe-stop
```

### 4. 查看日志与监控

```bash
pipe-logs    # 查看最近日志
pipe-mon     # 实时监控（按 Ctrl+C 退出）
```

---

## 环境变量

| 变量 | 说明 |
|------|------|
| `FORCE_AKSHARE=1` | 强制使用 AkShare 数据源 |
| `DISABLE_YFINANCE_FALLBACK=1` | 禁用 yfinance 备用源，AkShare 失败时直接跳过 |

---

## 数据库与缓存

- **主数据库**：`~/Code/data/quant_data/quant_core.db`
- **缓存数据库**：`~/Code/data/quant_data/quant_cache.db`
- **日志目录**：`~/Code/data/quant_data/logs/`

---

## 直接调用 manager.sh

如果不使用快捷命令，也可直接进入目录操作：

```bash
cd /Users/hainingyu/Code/quant_pipeline
./manager.sh [status|run|resume|health|logs|monitor|daemon|daemon-resume|daemon-stop]
```

---

## 注意事项

- 全量抓取 5257 只股票约需 **10 ~ 15 小时**（受网络状况影响）
- AkShare 服务端对请求频率敏感，失败率较高时可增大请求间隔
- 守护进程模式下无需人工盯盘，适合夜间挂机
