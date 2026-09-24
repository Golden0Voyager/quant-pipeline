"""
量化数据管道核心基础设施
────────────────────
提供配置管理、进程锁、断点续传、稳定性监控与工具函数。
"""

from __future__ import annotations

from ._bootstrap import ensure_sibling_paths as _ensure_sibling_paths

# 包级 choke point：任何 `core.*` 子模块的导入都先经过这里，因此 core/ 下那些
# **模块级**的跨仓库 import（如 `core/utils.py`）不再依赖「谁先被导入」。
_ensure_sibling_paths()
