"""路径引导门禁
──────────────
跨仓库（``smartmoney_hunter``）的 import 不能依赖「谁先被导入过」。这条门禁做三件事：

1. **规则派生**而非例举：扫出所有带**模块级**跨仓库 import 的仓库模块，逐个在**全新解释器**
   里只带仓库根导入一次，必须成功。曾经 ``core/utils.py``、``providers.py``、
   ``tasks/valuation_chain.py`` 三个模块在这里全部 ``ModuleNotFoundError``。
2. 以**包身份**导入（父目录在 ``sys.path`` 上，如 ``cwd=~/Code`` 时
   ``import quant_pipeline.providers``）时，包内 ``from core...`` 同样要能解析。
3. ``scripts/*.py`` 是直接执行入口（``sys.path[0]`` 是 ``scripts/``），在 import 仓库模块
   之前必须就地把仓库根注入 ``sys.path``，否则重演 2026-08-01 那次
   「TUI F 键秒退 ModuleNotFoundError」。这一句抽不成函数（``import core`` 之前拿不到
   ``core``），所以改由**规则**统一：注入只准一种写法、带模块级跨仓库 import 的脚本必须自己
   先引导兄弟仓库、仓库位置不准硬编码；另有行为探针从**外部 cwd** 真跑一遍 ``--help``。

1 和 2 靠 ``QUANT_HUNTER_PATH`` 指向一个**桩目录**来跑，因此在没有
``~/Code/quant_hunter`` 的 CI 上同样有效——否则这条门禁只会在开发者本机生效。
"""
from __future__ import annotations

import ast
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

import core._bootstrap as bootstrap

REPO_ROOT = Path(__file__).resolve().parent.parent
PACKAGES = ("core", "tasks", "tui")

# 兄弟仓库的顶层包名，以及本仓库自身的顶层包/模块名。
_CROSS_REPO_TOP_LEVEL = "smartmoney_hunter"
_REPO_TOP_LEVEL = {"core", "tasks", "tui", "providers", "interface", "daily_pipeline"}

# 桩目录要提供的模块：`providers.py` 在模块级 import 了前三个符号。
_STUB_MODULES = {
    "__init__.py": "",
    "market_utils.py": "def is_beijing_stock(_symbol):\n    return False\n",
    "database.py": "class DatabaseManager:\n    pass\n",
    "data_loader.py": "class DataLoader:\n    pass\n",
    "indicators.py": "class IndicatorCalculator:\n    pass\n",
    "xueqiu.py": "",
}


# ===========================================================================
# AST 工具：只认模块级语句（含 if/try 体内），函数/类体内的不算
# ===========================================================================

def _module_level_statements(tree: ast.Module) -> list[ast.stmt]:
    out: list[ast.stmt] = []

    def visit(stmts: list[ast.stmt]) -> None:
        for stmt in stmts:
            if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                continue
            out.append(stmt)
            for attr in ("body", "orelse", "finalbody"):
                inner = getattr(stmt, attr, None)
                if isinstance(inner, list) and inner and isinstance(inner[0], ast.stmt):
                    visit(inner)
            for handler in getattr(stmt, "handlers", []) or []:
                visit(handler.body)

    visit(tree.body)
    return out


def _parsed(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def _imported_top_level(node: ast.stmt) -> set[str]:
    if isinstance(node, ast.Import):
        return {alias.name.split(".")[0] for alias in node.names}
    if isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
        return {node.module.split(".")[0]}
    return set()


def _module_level_imports(tree: ast.Module) -> list[ast.stmt]:
    return [
        node
        for node in _module_level_statements(tree)
        if isinstance(node, (ast.Import, ast.ImportFrom))
    ]


def _path_insert_statements(tree: ast.Module) -> list[ast.stmt]:
    """模块级的 ``sys.path.insert(...)`` 语句（含其在 ``if`` 里的形态）。"""
    found = []
    for node in _module_level_statements(tree):
        value = node.value if isinstance(node, ast.Expr) else None
        if not isinstance(value, ast.Call):
            continue
        func = value.func
        if (
            isinstance(func, ast.Attribute)
            and func.attr == "insert"
            and ast.unparse(func.value) == "sys.path"
        ):
            found.append(node)
    return found


# ===========================================================================
# 规则派生
# ===========================================================================

def _importable_repo_modules() -> list[Path]:
    """可被 ``import`` 的仓库模块文件（排除直接执行的 ``scripts/``、``tests/`` 与包 ``__init__``）。"""
    paths = [p for p in REPO_ROOT.glob("*.py") if p.name != "__init__.py"]
    for package in PACKAGES:
        paths += [p for p in (REPO_ROOT / package).glob("*.py") if p.name != "__init__.py"]
    return sorted(paths)


def _module_name(path: Path) -> str:
    return ".".join(path.relative_to(REPO_ROOT).with_suffix("").parts)


def _modules_needing_injected_paths() -> list[str]:
    """带**模块级**跨仓库 import 的仓库模块——它们必须能独立导入。"""
    needed = []
    for path in _importable_repo_modules():
        for node in _module_level_imports(_parsed(path)):
            if _CROSS_REPO_TOP_LEVEL in _imported_top_level(node):
                needed.append(_module_name(path))
                break
    return needed


def _direct_execution_scripts() -> list[Path]:
    return sorted((REPO_ROOT / "scripts").glob("*.py"))


def _run_in_fresh_interpreter(code: str, *, cwd: Path, sibling_root: Path) -> subprocess.CompletedProcess:
    """在干净解释器里跑 `code`：剥掉 PYTHONPATH，兄弟仓库指向桩目录。"""
    env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
    env[bootstrap.SIBLING_REPO_PATH_ENV] = str(sibling_root)
    return subprocess.run(
        [sys.executable, "-c", code],
        cwd=str(cwd),
        env=env,
        capture_output=True,
        text=True,
        timeout=180,
    )


@pytest.fixture(scope="module")
def sibling_stub(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """最小兄弟 checkout 桩：``<root>/src/smartmoney_hunter/...``。"""
    root = tmp_path_factory.mktemp("sibling_checkout")
    package = root / "src" / _CROSS_REPO_TOP_LEVEL
    package.mkdir(parents=True)
    for name, body in _STUB_MODULES.items():
        (package / name).write_text(body, encoding="utf-8")
    return root


# ===========================================================================
# 1. 规则派生：模块级跨仓库 import ⇒ 必须能独立导入
# ===========================================================================

def test_rule_detects_the_known_cross_repo_modules():
    """规则本身要被钉住：不能因为写法变动而悄悄扫成空集。"""
    assert {"core.utils", "providers", "tasks.valuation_chain"} <= set(
        _modules_needing_injected_paths()
    )


@pytest.mark.parametrize("module", _modules_needing_injected_paths())
def test_cross_repo_module_imports_standalone(module, sibling_stub):
    """只带仓库根、不带任何先前导入，这个模块也必须 import 得进。

    回归靶子：去掉包级 choke point（``core/__init__.py`` / ``tasks/__init__.py``）或
    ``providers.py`` 里的显式调用后，`core.utils`、`providers`、`tasks.valuation_chain`
    会一起变红。
    """
    proc = _run_in_fresh_interpreter(f"import {module}", cwd=REPO_ROOT, sibling_root=sibling_stub)
    assert proc.returncode == 0, f"{module} 无法独立导入：\n{proc.stderr[-2000:]}"


def test_package_import_mode_resolves_repo_internal_imports(tmp_path, sibling_stub):
    """以包身份导入时，包内模块的仓库根视角 import 也要能解析。

    ``cwd=~/Code`` 下 ``import quant_pipeline.providers`` 曾因顶层 ``__init__.py``
    只注入 ``~/Code``（仓库根不在 ``sys.path`` 上）而 ``ModuleNotFoundError``。
    这里用软链把仓库暴露成父目录下的 ``quant_pipeline``，复现同一导入形态。
    """
    (tmp_path / "quant_pipeline").symlink_to(REPO_ROOT, target_is_directory=True)
    proc = _run_in_fresh_interpreter(
        "import quant_pipeline.providers", cwd=tmp_path, sibling_root=sibling_stub
    )
    assert proc.returncode == 0, f"包身份导入失败：\n{proc.stderr[-2000:]}"


# ===========================================================================
# 2. 直接执行式脚本：仓库根注入只有一种写法，且必须真的可用
# ===========================================================================

# 唯一被认可的脚本前言。它抽不成函数——`import core` 之前拿不到 `core`——所以「唯一来源」
# 只能是这条逐字比对，而不是某个共享 helper。
_CANONICAL_PREAMBLE = (
    "_REPO_ROOT = str(Path(__file__).resolve().parent.parent)\n"
    "if _REPO_ROOT not in sys.path:\n"
    "    sys.path.insert(0, _REPO_ROOT)"
)

# 硬编码的仓库位置：`quant_pipeline` 作为路径段出现（`~`、`/`、`\\` 或串首在前，`/` 或结尾在后）。
# 这样既不会误伤文档里的提及（如 `… Hunter / quant_pipeline.`），也能抓住 `HOME / "Code/quant_pipeline"`。
_HARDCODED_REPO_PATH = re.compile(r"(?:^|[/\\~])quant_pipeline(?:[/\\]|$)")


def _imports_anywhere(tree: ast.Module) -> list[ast.stmt]:
    """整棵语法树里的 import 语句（**含**函数/类体内）。

    脚本规则看整棵树：``scripts/daemon.py`` 的 ``core.*`` import 就写在函数里，它们同样需要
    仓库根在 ``sys.path`` 上。而第 1 节的模块规则只看**模块级** import——那才是「这个模块
    能否被独立导入」的判据，两者不是同一件事。
    """
    return [
        node
        for node in ast.walk(tree)
        if isinstance(node, (ast.Import, ast.ImportFrom))
    ]


def _scripts_with_repo_imports() -> list[Path]:
    found = []
    for path in _direct_execution_scripts():
        if any(
            _imported_top_level(node) & _REPO_TOP_LEVEL
            for node in _imports_anywhere(_parsed(path))
        ):
            found.append(path)
    return found


def _scripts_with_cross_repo_imports() -> list[Path]:
    found = []
    for path in _direct_execution_scripts():
        if any(
            _imported_top_level(node) == {_CROSS_REPO_TOP_LEVEL}
            for node in _imports_anywhere(_parsed(path))
        ):
            found.append(path)
    return found


def _scripts_with_argparse() -> list[Path]:
    """带 ``argparse`` 的脚本——它们支持 ``--help``，因此可以用作行为探针。

    即跑型脚本（``recompute_chip_em.py`` 等）没有 ``--help``，一执行就开始抓数/写库，
    不能用它探测。探针只施加在**同时需要仓库根注入**的脚本上：对不 import 仓库模块的脚本
    跑 ``--help`` 证明不了这条规则（它们本来就不需要注入），只会白白拖长测试。
    """
    needed = {p.name for p in _scripts_with_repo_imports()}
    found = []
    for path in _direct_execution_scripts():
        if path.name not in needed:
            continue
        if any(
            "argparse" in _imported_top_level(node)
            for node in _module_level_imports(_parsed(path))
        ):
            found.append(path)
    return found


def test_scripts_needing_the_preamble_rule_are_actually_found():
    """规则本身要被钉住：不能因为写法变动而悄悄扫成空集。"""
    assert {
        "audit_data_contracts.py",
        "migrate_database.py",
        "repair_turnover.py",
        "reconcile_with_akshare.py",
        "parallel_backfill.py",
    } <= {p.name for p in _direct_execution_scripts()}
    assert {
        "audit_data_contracts.py",
        "backfill_turnover_rate.py",
        "daemon.py",
        "migrate_database.py",
        "recompute_chip_em.py",
        "repair_turnover.py",
    } <= {p.name for p in _scripts_with_repo_imports()}
    assert {p.name for p in _scripts_with_cross_repo_imports()} >= {"repair_turnover.py"}
    assert {p.name for p in _scripts_with_argparse()} == {
        "audit_data_contracts.py",
        # 本脚本原先不 import 仓库模块，因此不在「需要仓库根注入」的集合里；
        # 接入 core.db_pragmas 之后它自动进入本规则，并从外部 cwd 跑 --help。
        "backfill_historical_valuation.py",
        "backfill_from_hithink_dump.py",
        "migrate_database.py",
        "reconcile_with_akshare.py",
        "repair_turnover.py",
    }


@pytest.mark.parametrize("script", _scripts_with_repo_imports(), ids=lambda p: p.name)
def test_script_uses_the_canonical_repo_root_preamble(script):
    """仓库根注入只准这一种写法，且必须在 import 仓库模块之前。

    此前 9 个脚本里有 4 种写法（``_PROJECT_ROOT`` / ``PIPELINE_DIR`` / ``_REPO_ROOT`` /
    裸 ``sys.path.insert``），其中 3 个连守卫都没有——无条件把仓库根顶到 ``sys.path`` 最前，
    会遮蔽已安装的同名包。写法一分叉，「照抄旁边那个脚本」就会带来语义漂移，所以钉住逐字形态。
    """
    tree = _parsed(script)
    repo_imports = [
        node for node in _imports_anywhere(tree) if _imported_top_level(node) & _REPO_TOP_LEVEL
    ]
    first_import = min(node.lineno for node in repo_imports)
    prefix = "\n".join(script.read_text(encoding="utf-8").splitlines()[: first_import - 1])

    assert _CANONICAL_PREAMBLE in prefix, (
        f"{script.name} 的仓库根注入不是统一写法（2026-08-01 事故形态：直跑时 ModuleNotFoundError）。"
        f"期望：\n{_CANONICAL_PREAMBLE}"
    )
    mutations = _path_insert_statements(tree)
    assert len(mutations) == 1, (
        f"{script.name} 有 {len(mutations)} 处模块级 sys.path 变异，应只有仓库根那一处"
    )


@pytest.mark.parametrize(
    "script", _scripts_with_cross_repo_imports(), ids=lambda p: p.name
)
def test_script_importing_the_sibling_repo_bootstraps_it_first(script):
    """带模块级 ``smartmoney_hunter`` import 的脚本必须自己先引导兄弟仓库。

    只 import ``core.*`` / ``tasks.*`` 的脚本不需要显式调用：那是包内导入，必然先执行
    ``core/__init__.py`` / ``tasks/__init__.py`` 两个包级 choke point。而 ``smartmoney_hunter``
    不是本仓库任何包的子模块，没有那层保险——今天的唯一一例是 ``scripts/repair_turnover.py``。
    """
    tree = _parsed(script)
    ensure_calls = [
        node
        for node in _module_level_statements(tree)
        if isinstance(node, ast.Expr)
        and isinstance(node.value, ast.Call)
        and ast.unparse(node.value.func) == "ensure_sibling_paths"
    ]
    cross_imports = [
        node
        for node in _imports_anywhere(tree)
        if _CROSS_REPO_TOP_LEVEL in _imported_top_level(node)
    ]

    assert ensure_calls, (
        f"{script.name} 在模块级 import 兄弟仓库，却没在本模块级调用 ensure_sibling_paths()"
    )
    assert min(call.lineno for call in ensure_calls) < min(imp.lineno for imp in cross_imports), (
        f"{script.name} 的 ensure_sibling_paths() 必须在跨仓库 import 之前"
    )


def test_no_script_hardcodes_the_repo_location():
    """仓库位置只能从 ``__file__`` 推导。

    ``scripts/parallel_backfill.py`` 曾写死 ``~/Code/quant_pipeline``：从另一个 checkout 运行时，
    worker 的 cwd、脚本路径与进度文件全指向**另一个**仓库副本（改动看着生效、其实没生效）。
    （``scripts/launchd/*.plist`` 里还有同类硬编码，但那属于「无人值守调度」那一项，不在本规则范围。）
    """
    offenders = []
    for path in _direct_execution_scripts():
        for node in ast.walk(_parsed(path)):
            if (
                isinstance(node, ast.Constant)
                and isinstance(node.value, str)
                and _HARDCODED_REPO_PATH.search(node.value)
            ):
                offenders.append(f"{path.name}:{node.lineno}")
    assert offenders == [], f"这些地方硬编码了仓库位置：{offenders}"


@pytest.mark.parametrize("script", _scripts_with_argparse(), ids=lambda p: p.name)
def test_script_help_works_from_a_foreign_cwd(tmp_path, sibling_stub, script):
    """从**别的目录**直跑脚本也必须 import 得进仓库——这才是 ``sys.path[0] == scripts/`` 的形态。

    以 ``cwd=REPO_ROOT`` 跑会让仓库根因为「``sys.path`` 里的 ``''`` = 当前目录」而恰好可见，
    注入写错也照样绿——那样这条门禁就测不到 2026-08-01 那次事故（TUI 从任意 cwd 起子进程）。
    兄弟仓库指向桩目录，因此在没有 ``~/Code/quant_hunter`` 的 CI 上同样有效。
    """
    env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
    env[bootstrap.SIBLING_REPO_PATH_ENV] = str(sibling_stub)
    proc = subprocess.run(
        [sys.executable, str(script), "--help"],
        cwd=str(tmp_path),
        env=env,
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert proc.returncode == 0, (
        f"{script.name} --help 在外部 cwd 下失败（路径引导没生效）：\n{proc.stdout[-1000:]}\n{proc.stderr[-2000:]}"
    )


# ===========================================================================
# 3. `core._bootstrap` 自身的行为
# ===========================================================================

def test_default_root_is_the_sibling_repo_src(monkeypatch):
    monkeypatch.delenv(bootstrap.SIBLING_REPO_PATH_ENV, raising=False)
    assert bootstrap.sibling_repo_root() == Path("~/Code/quant_hunter").expanduser()
    assert bootstrap.sibling_import_roots() == (Path("~/Code/quant_hunter/src").expanduser(),)


def test_override_is_honoured(monkeypatch, tmp_path):
    """测试靠这个开关把注入指向桩目录，使门禁在没有兄弟仓库的 CI 上也能跑。"""
    monkeypatch.setenv(bootstrap.SIBLING_REPO_PATH_ENV, str(tmp_path / "elsewhere"))
    assert bootstrap.sibling_import_roots() == (tmp_path / "elsewhere" / "src",)


def test_missing_root_is_a_noop(monkeypatch, tmp_path):
    """目录不存在时必须静默跳过——CI 上没有兄弟仓库，报错会让整套测试跑不起来。"""
    monkeypatch.setenv(bootstrap.SIBLING_REPO_PATH_ENV, str(tmp_path / "does-not-exist"))
    before = list(sys.path)
    assert bootstrap.ensure_sibling_paths() == ()
    assert sys.path == before


def test_inserted_paths_are_reported_and_idempotent(monkeypatch, tmp_path):
    root = tmp_path / "src"
    root.mkdir()
    monkeypatch.setenv(bootstrap.SIBLING_REPO_PATH_ENV, str(tmp_path))
    monkeypatch.setattr(sys, "path", [p for p in sys.path if p != str(root)])

    assert bootstrap.ensure_sibling_paths() == (str(root),)
    assert sys.path[0] == str(root)
    assert bootstrap.ensure_sibling_paths() == (), "重复调用不得重复插入"


def test_declaration_order_is_priority_order(monkeypatch, tmp_path):
    """声明在前 = ``sys.path`` 里更靠前。

    逐个 ``insert(0)`` 会把顺序倒过来，所以实现里反转了一次遍历；这里把那个反转钉住，
    日后往 ``sibling_import_roots()`` 里加第二项时不会静默改优先级。
    """
    first, second = tmp_path / "first", tmp_path / "second"
    monkeypatch.setattr(bootstrap, "sibling_import_roots", lambda: (first, second))
    monkeypatch.setattr(sys, "path", [p for p in sys.path if p not in (str(first), str(second))])
    first.mkdir()
    second.mkdir()

    assert bootstrap.ensure_sibling_paths() == (str(first), str(second))
    assert sys.path[:2] == [str(first), str(second)]


def test_no_module_injects_the_code_dir():
    """``~/Code`` 注入已核实无任何消费者（``Trading_Agents`` 甚至不存在），必须保持删除。

    这条同时是文档门禁：``core/_bootstrap.py` 里那句「已删除」的说法必须为真。
    """
    offenders = []
    for path in sorted(REPO_ROOT.rglob("*.py")):
        if any(part in {".venv", ".worktrees", ".git"} for part in path.parts):
            continue
        for node in _path_insert_statements(_parsed(path)):
            if '"~/Code"' in ast.unparse(node) or "'~/Code'" in ast.unparse(node):
                offenders.append(f"{path.relative_to(REPO_ROOT)}:{node.lineno}")
    assert offenders == [], f"这些地方仍在把 ~/Code 注入 sys.path：{offenders}"
