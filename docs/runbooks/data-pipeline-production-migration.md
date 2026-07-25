# Data Pipeline 生产迁移操作手册

> 适用于 `quant_core.db` 的版本化 schema 迁移。
> 迁移引擎: `core/migrations.py` · 迁移文件: `migrations/`

---

## 一、迁移基础

### 迁移引擎用法

```python
from core.migrations import MigrationEngine

engine = MigrationEngine(
    db_path="~/Code/quant_data/quant_core.db",
    migrations_dir="migrations/",
)
```

### 安全运行

```bash
# 预览待执行迁移（无副作用）
uv run python -c "
from core.migrations import MigrationEngine
engine = MigrationEngine('~/Code/quant_data/quant_core.db', 'migrations/')
for m in engine.plan():
    print(f\"  v{m['version']}: {m['description']}  [{m['checksum'][:12]}]\")
"

# 实际执行
uv run python -c "
from core.migrations import MigrationEngine
engine = MigrationEngine('~/Code/quant_data/quant_core.db', 'migrations/')
results = engine.apply_pending()
for r in results:
    status = 'OK' if r['applied'] else 'SKIP'
    print(f\"  v{r['version']}: {status} ({r['duration_ms']}ms)\")
"
```

---

## 二、迁移类型说明

### .sql 迁移
纯 SQL 文件，直接对连接执行。

### .py 迁移
导出 `apply(conn)` 函数的 Python 脚本，用于：
- 需要条件判断（IF NOT EXISTS）
- 多步骤事务
- 更新 `schema_migrations` 表的 checksum
- DDL + DML 混合

---

## 三、当前迁移清单 (v1–v9)

| 版本 | 文件 | 类型 | 说明 |
|------|------|------|------|
| 001 | `001_ingestion_audit.sql` | SQL | 初始化 ingestion 审计表 |
| 002 | `002_phase2_data_cleanup.py` | Python | Phase2 数据清洗 |
| 003 | `003_point_in_time_tables.sql` | SQL | PIT 表 (financial_history_pt 等) |
| 006 | `006_reconcile_ingestion_audit.py` | Python | 重建 ingestion_runs + ingestion_rejections，删除旧表，修正 001 checksum |
| 007 | `007_reconcile_pit_tables.py` | Python | 重建 PIT 表 (quarterly_financials_history 等)，删除旧 PIT 表，修正 003 checksum |
| 008 | `008_reconcile_orphan_ingestion_runs.py` | Python | 补齐 PIT 孤儿审计父记录，并规范化 006 checksum |
| 009 | `009_source_record_keys.py` | Python | 为 `stock_repurchase` 和 `institution_survey` 增加来源记录哈希键，移除旧 date+code 唯一约束 |

> 004/005 是试验性迁移，已清理。

---

## 四、生产迁移流程

### 4.1 新数据库首次运行

全新 `quant_core.db` 直接执行全部迁移：

```bash
uv run python -c "
from core.migrations import MigrationEngine
engine = MigrationEngine('~/Code/quant_data/quant_core.db', 'migrations/')
engine.apply_pending()
"
```

### 4.2 已有数据库（v1/v2/v3 已应用）

如果 001/003 的原始 SQL 文件未被篡改，checksum 一致，直接执行：

```bash
uv run python -c "
from core.migrations import MigrationEngine
engine = MigrationEngine('~/Code/quant_data/quant_core.db', 'migrations/')
engine.apply_pending()
"
```

006 和 007 会自动：
1. 创建新表（`ingestion_runs` / `ingestion_rejections` / `quarterly_financials_history` 等）
2. 删除旧表（`task_run_log` / `financial_history_pt` 等）
3. 更新 001/003 的 checksum 为当前文件的实际哈希

008 仅接受以下两个已知的 006 历史 checksum：

- 发布版：`a783c28347a05f415f4f6b4dd15f068cde964194657cea3c1573523085af65e0`
- 临时修订版：`8273ec2643335baacddcd6478d4fab032348e7cfd2346d03669dadf12a6e78b2`

它会先为 PIT 历史表中的孤立 `snapshot_run_id` 补齐 `ingestion_runs`
父记录，再把 version 6 的记录规范化为发布版 checksum。其他 checksum
会硬失败，必须先确认文件和数据库来源。

009 会重建 `stock_repurchase` 和 `institution_survey`，为已有行回填
`source_record_key`，并移除旧的 `trade_date + stock_code` 唯一约束。
部署后第一次启动 Provider 或 `pipe-tui` 时可能连续应用 008 和 009；
启动前建议先备份 `~/Code/quant_data/quant_core.db`。

### 4.3 Checksum 冲突处理

如果引擎报错 `checksum mismatch for migration N`：

```bash
# 查看当前记录的 checksum
sqlite3 ~/Code/quant_data/quant_core.db \
  "SELECT version, checksum, applied_at FROM schema_migrations WHERE version IN (1,3)"

# 查看当前文件的 checksum
sha256sum migrations/001_ingestion_audit.sql
sha256sum migrations/003_point_in_time_tables.sql

# 不要编辑已经发布的迁移文件或手工覆盖 schema_migrations checksum。
# 先用版本控制确认文件来源；合法的历史差异应通过新的对账迁移处理。
```

---

## 五、日常维护命令

### 查看迁移状态
```bash
sqlite3 ~/Code/quant_data/quant_core.db \
  "SELECT version, description, applied_at, printf('%.8s', checksum) AS ck_short, success FROM schema_migrations ORDER BY version"
```

### 强制重新应用某个迁移（开发用）
```bash
sqlite3 ~/Code/quant_data/quant_core.db \
  "DELETE FROM schema_migrations WHERE version = N"
# 然后重新运行引擎
```

---

## 六、回滚策略

> 本迁移引擎不提供反向迁移。单个迁移通过 `BEGIN IMMEDIATE` 原子执行，
> 失败时会回滚；已成功提交的迁移仍需从备份恢复或编写新的前向迁移。

### 回滚方案

| 场景 | 操作 |
|------|------|
| 006 误删 `task_run_log` | 数据未丢失：`CREATE TABLE task_run_log AS SELECT * FROM task_run_log_backup`（备份请另行 dump） |
| 007 误删 `financial_history_pt` | 数据未丢失：重新从源采集或从备份恢复 |
| 新表结构有误 | 手动 DROP TABLE + 修正迁移文件 + 删除 `schema_migrations` 记录 + 重新运行 |

### 迁移前建议
```bash
# 备份数据库
cp ~/Code/quant_data/quant_core.db ~/Code/quant_data/quant_core.db.$(date +%Y%m%d_%H%M%S)
```

---

## 七、编写新迁移

### SQL 迁移
```sql
-- migrations/008_add_new_feature.sql
CREATE TABLE IF NOT EXISTS new_feature (
    id    INTEGER PRIMARY KEY AUTOINCREMENT,
    name  TEXT NOT NULL
);
```

### Python 迁移
```python
"""migrations/008_add_new_feature.py"""

import hashlib
from pathlib import Path

def apply(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS new_feature (
            id    INTEGER PRIMARY KEY AUTOINCREMENT,
            name  TEXT NOT NULL
        )
    """)
```

### 添加迁移后运行
```bash
uv run python -c "
from core.migrations import MigrationEngine
engine = MigrationEngine('~/Code/quant_data/quant_core.db', 'migrations/')
engine.apply_pending()
"
```

---

## 八、故障排查

| 问题 | 原因 | 解决 |
|------|------|------|
| `checksum mismatch` | 迁移文件在应用后被修改 | 运行 006/007 的 reconcile，或手动 UPDATE checksum |
| `duplicate migration version N` | 迁移目录中有两个文件前缀数字相同 | 删除重复文件 |
| `no such table: schema_migrations` | 数据库全新，但引擎正常会自行创建 | 确认 `migrations/` 目录和路径正确 |
| 迁移速度慢 | SQLite 在 WAL 模式下正常 | 检查 `PRAGMA busy_timeout` 是否生效 |

---

## 九、持续集成检查

在 CI 中检查迁移是否可重复执行：

```bash
# 创建临时数据库
cp ~/Code/quant_data/quant_core.db /tmp/ci_check.db

# 执行迁移
uv run python -c "
from core.migrations import MigrationEngine
engine = MigrationEngine('/tmp/ci_check.db', 'migrations/')
r = engine.apply_pending()
for x in r:
    assert x['applied'] or not x['error']
"

# 再次执行应无事可做
uv run python -c "
from core.migrations import MigrationEngine
engine = MigrationEngine('/tmp/ci_check.db', 'migrations/')
assert len(engine.plan()) == 0
"

rm -f /tmp/ci_check.db
```
