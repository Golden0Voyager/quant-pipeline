"""
Quant Data Pipeline — 数据基础设施维护管道
─────────────────────────────────────────────
职责：独立于任何应用项目，维护共享 SQLite 数据库。

用法：
    python daily_pipeline.py --task all
"""
import sys
from pathlib import Path

# 以包身份导入（`import quant_pipeline.*`，cwd=~/Code）时仓库根**不在** `sys.path` 上，
# 而包内模块用的是 `from core...` / `from providers...` 这类仓库根视角的绝对导入，
# 所以必须先把仓库根补上。
# 旧版这里只注入 `~/Code`：它既已在路径上，又不能让 `smartmoney_hunter` 可导入
# （后者在 `~/Code/quant_hunter/src`），于是这个包一直处于「import 得进、子模块用不了」
# 的状态（实测 `import quant_pipeline.providers` 报 ModuleNotFoundError）。
_REPO_ROOT = str(Path(__file__).resolve().parent)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from core._bootstrap import ensure_sibling_paths  # noqa: E402

ensure_sibling_paths()

__all__ = ["interface", "providers", "daily_pipeline"]
