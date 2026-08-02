# AGENTS.md — quant_pipeline 项目规范

## ⚠️ 环境约束（强制）

- **包管理器**：`uv pip install <pkg>`（仅限 `uv`，禁止 `pip`）
- **运行脚本**：`uv run python <script>.py`

---

## 🛠️ Key Conventions

- **数据库路径**: `~/Code/quant_data/quant_core.db` (SQLite)
- **数据源优先级**: AkShare > 静默（写操作禁用 yfinance fallback）
- **市场前缀规则**: `6`/`9` → sh, `0`/`2`/`3` → sz, `4`/`8`/`920` → bj
- **质量保障**: ruff + mypy + pytest CI 校验
- **三层次入口**: `--task daily`（=`all`）每日层；`weekly_backfill`/`monthly_repair` 每周/每月层仅手动触发（CLI 或 TUI W/M 键）

---

## Git Workflow 与规范

- **一文件一提交**: 严禁将多文件打包在一个 commit 中。Commit message 采用中英双语，英文块在前，中文在后。
- **New Feature 流程**: 开发新功能 (new feature) 时，建议走 `/git-feature` 流程（使用 `/git-feature start` 创建分支，完成开发后使用 `/git-feature done` 完成推送/PR/合入/清理全流程）。
- **分支规范**: 禁止直接在 `main` 上开发，必须在 `feat/*`、`fix/*`、`refactor/*` 等分支上进行。
