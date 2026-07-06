"""
Quant Data Pipeline — 数据基础设施维护管道
─────────────────────────────────────────────
职责：独立于任何应用项目，维护共享 SQLite 数据库。

用法：
    python daily_pipeline.py --task all
"""
import os
import sys

# 把 ~/Code/ 加入 Python 路径，使 pipeline 能 import smartmoney_hunter/quant_lab/Trading_Agents
_CODE_DIR = os.path.expanduser("~/Code")
if _CODE_DIR not in sys.path:
    sys.path.insert(0, _CODE_DIR)

__all__ = ["interface", "providers", "daily_pipeline"]
