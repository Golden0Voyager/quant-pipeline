"""
兄弟仓库路径引导
────────────────
本仓库依赖 ``smartmoney_hunter``（兄弟仓库 ``~/Code/quant_hunter/src``），而它**不是**
声明依赖：``pyproject.toml`` 里没有、venv 里也没有。唯一可行的接入方式是把它注入
``sys.path``，因此这条注入必须在**任何模块级** ``from smartmoney_hunter ...`` 之前完成。
（位置可用 ``QUANT_HUNTER_PATH`` 指向别的兄弟 checkout 根目录。）

为什么要集中到这一个模块
────────────────────────
这段注入过去在 4 个文件里各抄一遍（``core/config.py``、``daily_pipeline.py``、
``tasks/bars.py``、顶层 ``__init__.py``），而 ``core/utils.py``、``providers.py``、
``tasks/valuation_chain.py`` 这 3 个模块**有模块级跨仓库 import 却完全不做注入**——
它们能否被导入，取决于「别的模块是否碰巧先被导入过」。

实测（只有仓库根在 ``sys.path`` 的解释器里）：上面那 3 个模块全部
``ModuleNotFoundError: No module named 'smartmoney_hunter'``，而自带注入的
``core.config`` / ``daily_pipeline`` / ``tasks.bars`` 才 import 得进。

调用点（每处一次，幂等）
────────────────────────
- ``core/__init__.py`` / ``tasks/__init__.py``：包级 choke point，覆盖该包下所有子模块；
- ``daily_pipeline.py`` / ``providers.py``：顶层模块，没有包级 choke point，各显式调一次；
- ``scripts/*.py``：先内联注入仓库根（直接执行时 ``sys.path[0]`` 是 ``scripts/``，
  此刻还 import 不到本模块），再调用本函数接管兄弟仓库路径。

一处已核实并删除的历史声明
──────────────────────────
这些注入过去还顺带把 ``~/Code`` 自身加进 ``sys.path``，注释声称「使 pipeline 能 import
smartmoney_hunter/quant_lab/Trading_Agents」。实测：``~/Code`` 下**没有任何一级名字被
本仓库 import**（``Trading_Agents`` 甚至不存在），而 ``smartmoney_hunter`` 在
``quant_hunter/src`` 下、其自身也只 import 标准库与第三方包——那条注入是死的。
``tests/test_sys_path_bootstrap.py`` 会阻止它悄悄回来。
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

# 覆盖用的环境变量：兄弟 checkout 的**根目录**。沿用 ``tests/test_stage2_write_disjointness.py``
# 已有的同名约定（那里也把它当「兄弟 checkout 根目录」用），不另造开关。
# 生产不设置它；测试靠它把注入指向一个桩目录，从而在没有兄弟仓库的 CI 上也能验证
# 「注入必须先于 import」。
SIBLING_REPO_PATH_ENV = "QUANT_HUNTER_PATH"

_DEFAULT_SIBLING_REPO = Path("~/Code/quant_hunter")
# 兄弟仓库里真正需要上 ``sys.path`` 的只是源码根；仓库根只是一个目录容器。
_SIBLING_IMPORT_SUBDIR = Path("src")


def sibling_repo_root() -> Path:
    """兄弟仓库（``quant_hunter``）根目录（可被 ``QUANT_HUNTER_PATH`` 覆盖）。"""
    override = os.getenv(SIBLING_REPO_PATH_ENV, "")
    return Path(override).expanduser() if override else _DEFAULT_SIBLING_REPO.expanduser()


def sibling_import_roots() -> tuple[Path, ...]:
    """兄弟仓库内需要注入 ``sys.path`` 的源码根，按优先级从高到低。"""
    return (sibling_repo_root() / _SIBLING_IMPORT_SUBDIR,)


def ensure_sibling_paths() -> tuple[str, ...]:
    """把兄弟仓库导入根补进 ``sys.path``，返回本次真正插入的路径。

    幂等：已在 ``sys.path`` 里的不重复插入。目录不存在时直接跳过——CI 上没有
    ``~/Code/quant_hunter``，此处必须是无操作，否则本仓库自身都跑不起来。
    """
    added: list[str] = []
    # 反转遍历：逐个 ``insert(0)`` 会把顺序倒过来，反转一次才能让「声明在前 = 优先级更高」。
    for root in reversed(sibling_import_roots()):
        target = str(root)
        if root.is_dir() and target not in sys.path:
            sys.path.insert(0, target)
            added.append(target)
    return tuple(reversed(added))
