"""
管线任务模块
─────────────
存放从 daily_pipeline.py 提取出的各数据更新任务。
"""

from __future__ import annotations

from core._bootstrap import ensure_sibling_paths

# 包级 choke point：`tasks/bars.py`、`tasks/valuation_chain.py`、`tasks/finance_flow.py`
# 都有跨仓库 import（部分是模块级），不该依赖「谁先被导入」。
ensure_sibling_paths()
