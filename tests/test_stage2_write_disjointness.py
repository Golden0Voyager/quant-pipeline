"""stage2 写表不相交审计（2026-09-24）——把「stage2 能不能并行」的证据固化成门禁。

审计结论
--------
stage2 的 34 个任务（``daily_pipeline.run_all`` 里的 ``stage2_raw_tasks``）落在 36 个 db
写方法上，这 34 个方法各自只写一张互不相同的表，**写表两两不相交成立**。因此「写表相交」
不是 stage2 维持串行的理由；``run_all`` 里旧注释声称的阻碍（共享 db provider 不能交给多个
SQLite 线程）也已被 stage4 反证——stage4 同样共享该 provider 并并发 3 个任务。是否启用并发
应由收益决定（耗时占比最高的两个任务是纯本地计算、内部已各自并行），而不是由这条不变量决定。

审计方式
--------
静态 AST 分析：不联网、不执行任务、不需要真实数据源。因为「某任务可能写哪些表」是代码属性，
不是运行时属性。取证四步：

1. 读 ``daily_pipeline.py`` 的 ``stage2_raw_tasks`` 字面量 → 任务名 + callable 名；
2. 由该模块的 import 解析 callable → ``tasks.<mod>``，并在模块内递归收集 ``db.<写方法>``
   调用（含同模块 helper 与嵌套函数，如 ``executor.submit(_process_chip_one, db, ...)``）；
3. 由 ``providers.py`` 的 INSERT/UPDATE 语句得到「写方法 → 表」；委托给 smartmoney_hunter
   的 11 个方法改用其真实源码解析（本地存在兄弟仓库时生效，CI 缺失则跳过该部分）；
4. 与 ``core.task_registry`` 的 ``TaskSpec.tables`` 声明交叉校验。

本审计顺带修正的两处
--------------------
* ``run_all`` 里 stage2 段落的计数写成 33，实际是 32；
* ``update_money_market`` 会写 ``central_bank_balance``（``tasks/money_market.py``），但注册表
  只声明了 ``money_market``，导致 ``table_owners``／完整度面板归属漏掉这张表。已补进
  ``tables`` 声明；**故意不加** ``date_columns``：该表是月频，加进去会让它以「日频」身份进入
  新鲜度面板，正是假告警的来源。

维护提示
--------
``test_stage2_task_set_is_pinned`` 是审计入口断言。它变红意味着 stage2 任务集合变了，必须
重做本审计，并同步 ``daily_pipeline.py`` 里 stage2 段落的计数与结论。
"""

from __future__ import annotations

import ast
import os
import re
from pathlib import Path

import pytest

from core.task_registry import lookup_task

REPO_ROOT = Path(__file__).resolve().parents[1]
DAILY_PIPELINE = REPO_ROOT / "daily_pipeline.py"
PROVIDERS = REPO_ROOT / "providers.py"

# stage2 任务集合快照（审计当日）。改动此集合必须重做审计，见模块 docstring。
_STAGE2_TASK_NAMES: frozenset[str] = frozenset(
    {
        "update_indicators",
        "update_chip_distribution",
        "update_global_assets",
        "update_gold_price",
        "update_usd",
        "update_global_index",
        "update_us_treasury",
        "update_us_macro",
        "update_hk_tech_index",
        "update_cftc_cot",
        "update_eia_petroleum",
        "update_lithium_spot",
        "update_futures",
        "update_china_macro",
        "update_money_market",
        "update_margin_trading",
        "update_dragon_tiger",
        "update_block_trade",
        "update_limit_up_down",
        "update_stock_comment",
        "update_hot_rank",
        "update_index_daily",
        "update_market_valuation",
        "update_south_flow",
        "update_ah_premium",
        "update_etf_daily",
        "update_cb_quotation",
        "update_cb_redeem",
        "update_cb_index",
        "update_option_sentiment",
        "update_stock_repurchase",
        "update_placement_announcements",
        "update_institution_survey",
        "update_stock_pledge",
    }
)

# 写方法的命名前缀：db 上以这些前缀开头的属性调用视为「写」。
_WRITE_METHOD_PREFIXES = ("save_", "upsert_", "replace_", "insert_", "write_")

# 「写方法 → 表」的判据。``UPDATE <tbl> SET`` 要求带 SET，否则 upsert 子句
# ``ON CONFLICT DO UPDATE SET`` 会被误抓成表名 "SET"。
_TABLE_SQL_RE = re.compile(
    r"\bINSERT\s+(?:OR\s+\w+\s+)?INTO\s+([A-Za-z_]\w*)"
    r"|\bREPLACE\s+INTO\s+([A-Za-z_]\w*)"
    r"|\bUPDATE\s+([A-Za-z_]\w*)\s+SET\b"
    r"|\bDELETE\s+FROM\s+([A-Za-z_]\w*)",
    re.IGNORECASE,
)

# 委托写目标（smartmoney_hunter.DatabaseManager）的真实源码位置：
# 环境变量 → 仓库根起上溯三层的兄弟 checkout（兼容在主 checkout 与 worktree 下运行）。
# 找不到就跳过委托部分。
_SIBLING_ENV_VAR = "QUANT_HUNTER_PATH"
_SIBLING_RELATIVE = Path("src") / "smartmoney_hunter" / "database.py"


def _parse(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"))


def _stage2_entries() -> list[tuple[str, str]]:
    """读 ``daily_pipeline.py`` 的字面量，返回 stage2 的 ``(任务名, callable 名)``。

    刻意读源码字面量而不是 import 后反射：``stage2_raw_tasks`` 是 ``run_all`` 内的局部
    变量，只有源码是它的完整事实来源。
    """
    for node in ast.walk(_parse(DAILY_PIPELINE)):
        targets: list[ast.expr] = []
        assigned: ast.expr | None = None
        if isinstance(node, ast.AnnAssign):
            targets = [node.target]
            assigned = node.value
        elif isinstance(node, ast.Assign):
            targets = list(node.targets)
            assigned = node.value
        if not any(isinstance(t, ast.Name) and t.id == "stage2_raw_tasks" for t in targets):
            continue
        assert isinstance(assigned, ast.List), "stage2_raw_tasks 应为字面量列表"
        entries: list[tuple[str, str]] = []
        for elt in assigned.elts:
            assert isinstance(elt, ast.Tuple) and len(elt.elts) >= 2, "stage2 条目应为 4 元组"
            name_node, callable_node = elt.elts[0], elt.elts[1]
            assert isinstance(name_node, ast.Constant) and isinstance(name_node.value, str)
            assert isinstance(callable_node, ast.Name), f"{name_node.value}: callable 应为裸函数名"
            entries.append((name_node.value, callable_node.id))
        return entries
    raise AssertionError("daily_pipeline.py 里找不到 stage2_raw_tasks 字面量")


def _task_modules() -> dict[str, str]:
    """``daily_pipeline.py`` 里 ``from tasks.x import y`` 的映射 ``y → tasks.x``。"""
    mapping: dict[str, str] = {}
    for node in ast.walk(_parse(DAILY_PIPELINE)):
        if isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("tasks."):
            for alias in node.names:
                mapping[alias.asname or alias.name] = node.module
    return mapping


def _stage2_modules() -> dict[str, str]:
    """``任务名 → 定义它的模块``。"""
    modules = _task_modules()
    return {task: modules[callable_name] for task, callable_name in _stage2_entries()}


def _module_functions(module: str) -> dict[str, ast.FunctionDef]:
    tree = _parse(REPO_ROOT / f"{module.replace('.', '/')}.py")
    # 只取模块顶层函数；嵌套函数由 ast.walk 在外层函数体内覆盖，无需单独登记。
    return {node.name: node for node in tree.body if isinstance(node, ast.FunctionDef)}


def _provider_write_methods() -> frozenset[str]:
    """``providers.py`` 上全部写方法名，作为「哪些 ``db.<attr>`` 算写」的判据。"""
    methods = {
        node.name
        for cls in _parse(PROVIDERS).body
        if isinstance(cls, ast.ClassDef)
        for node in cls.body
        if isinstance(node, ast.FunctionDef) and node.name.startswith(_WRITE_METHOD_PREFIXES)
    }
    assert methods, "providers.py 里没找到任何写方法，判据失效"
    return frozenset(methods)


def _is_db_receiver(node: ast.expr) -> bool:
    """写路径的两种合法接收者：``db.`` 与 ``db._db.``

    两者都指向同一个共享 provider；其它接收者（别名 / 局部管理对象）会让静态抽取失效，
    因此由 ``test_stage2_modules_write_only_through_db_receiver`` 显式拦下。
    """
    if isinstance(node, ast.Name):
        return node.id == "db"
    return isinstance(node, ast.Attribute) and node.attr == "_db"


def _write_methods_of(
    fn: ast.FunctionDef,
    funcs: dict[str, ast.FunctionDef],
    write_methods: frozenset[str],
    seen: set[str],
) -> set[str]:
    """递归收集 *fn* body 内经 ``db`` 发起的写方法名。

    递归覆盖两类转发：``helper(db, ...)`` 与 ``executor.submit(worker, db, ...)``
    ——后者是 ``update_chip_distribution`` 的真实形态。
    """
    if fn.name in seen:
        return set()
    seen.add(fn.name)
    found: set[str] = set()
    for node in ast.walk(fn):
        if not isinstance(node, ast.Call):
            continue
        callee = node.func
        if isinstance(callee, ast.Attribute) and callee.attr in write_methods and _is_db_receiver(callee.value):
            found.add(callee.attr)
        args = [*node.args, *(kw.value for kw in node.keywords if kw.value is not None)]
        if not any(isinstance(a, ast.Name) and a.id == "db" for a in args):
            continue
        for arg in args:
            if isinstance(arg, ast.Name) and arg.id in funcs:
                found |= _write_methods_of(funcs[arg.id], funcs, write_methods, seen)
    return found


def _stage2_write_methods() -> dict[str, frozenset[str]]:
    """``stage2 任务名 → 它可能调用的 db 写方法集合``。"""
    write_methods = _provider_write_methods()
    funcs_cache: dict[str, dict[str, ast.FunctionDef]] = {}
    result: dict[str, frozenset[str]] = {}
    for task, module in _stage2_modules().items():
        if module not in funcs_cache:
            funcs_cache[module] = _module_functions(module)
        funcs = funcs_cache[module]
        callable_name = dict(_stage2_entries())[task]
        fn = funcs.get(callable_name)
        assert fn is not None, f"{task}: 在 {module} 里找不到 {callable_name}"
        result[task] = frozenset(_write_methods_of(fn, funcs, write_methods, set()))
    return result


def _method_tables(source: Path) -> dict[str, frozenset[str]]:
    """从某个 db 层源码抽 ``写方法 → 表集合``，并解析类内 ``self.<method>()`` 转发。"""
    text = source.read_text(encoding="utf-8")
    direct: dict[str, set[str]] = {}
    forwards: dict[str, set[str]] = {}
    for cls in _parse(source).body:
        if not isinstance(cls, ast.ClassDef):
            continue
        for fn in cls.body:
            if not isinstance(fn, ast.FunctionDef) or not fn.name.startswith(_WRITE_METHOD_PREFIXES):
                continue
            segment = ast.get_source_segment(text, fn) or ""
            tables = {group for match in _TABLE_SQL_RE.finditer(segment) for group in match.groups() if group}
            direct[fn.name] = tables
            if not tables:
                forwards[fn.name] = {
                    n.func.attr
                    for n in ast.walk(fn)
                    if isinstance(n, ast.Call)
                    and isinstance(n.func, ast.Attribute)
                    and n.func.attr.startswith(_WRITE_METHOD_PREFIXES)
                }
    for name, targets in forwards.items():
        for target in targets:
            if direct.get(target):
                direct[name] |= direct[target]
    return {name: frozenset(tables) for name, tables in direct.items() if tables}


def _sibling_database_source() -> Path | None:
    """定位 smartmoney_hunter 真实 ``database.py``；未安装且无兄弟 checkout 时返回 None。

    CI 不安装该包（conftest 以 mock 替代），因此依赖它的检查在 CI 上跳过——与
    ``tests/test_stage4_concurrency.py`` 的既有做法一致。
    """
    override = os.environ.get(_SIBLING_ENV_VAR)
    if override:
        candidate = Path(override) / _SIBLING_RELATIVE
        return candidate if candidate.exists() else None
    for ancestor in list(REPO_ROOT.parents)[:3]:
        for candidate in sorted(ancestor.glob(f"*/{_SIBLING_RELATIVE}")):
            return candidate
    return None


def _all_method_tables() -> dict[str, frozenset[str]]:
    """``写方法 → 表``：providers.py 直连的 SQL 优先，委托方法由兄弟仓库源码补齐。"""
    tables = dict(_method_tables(PROVIDERS))
    sibling = _sibling_database_source()
    if sibling is not None:
        for name, resolved in _method_tables(sibling).items():
            tables.setdefault(name, resolved)
    return tables


def _stage2_write_tables() -> dict[str, frozenset[str]]:
    """``stage2 任务名 → 可由代码静态解析出的表集合``（委托方法缺源码时不完整）。"""
    method_tables = _all_method_tables()
    return {
        task: frozenset(t for method in methods for t in method_tables.get(method, ()))
        for task, methods in _stage2_write_methods().items()
    }


def _stage2_declared_tables() -> dict[str, frozenset[str]]:
    """``stage2 任务名 → core.task_registry 声明的表集合``。"""
    declared: dict[str, frozenset[str]] = {}
    for task in _stage2_write_methods():
        spec = lookup_task(task)
        assert spec is not None, f"{task} 未在 core.task_registry 注册"
        declared[task] = frozenset(spec.tables)
    return declared


def _pairs_with_two_owners(owners: dict[str, frozenset[str]]) -> dict[str, list[str]]:
    """``表 → 声明/写入它的任务列表``，只保留被两个以上任务触及的表。"""
    owners_by_table: dict[str, list[str]] = {}
    for task, tables in owners.items():
        for table in tables:
            owners_by_table.setdefault(table, []).append(task)
    return {table: tasks for table, tasks in owners_by_table.items() if len(tasks) > 1}


class TestStage2AuditInputs:
    """审计的输入本身必须是可信的：任务集合未漂移、每个任务都解析得到写路径。"""

    def test_stage2_task_set_is_pinned(self) -> None:
        """任务集合与审计快照一致。变红 = 需要重做审计，而非简单的补名字。"""
        names = {name for name, _ in _stage2_entries()}
        assert names == set(_STAGE2_TASK_NAMES), (
            "stage2 任务集合已变化，写表不相交审计需要重做：\n"
            f"  新增: {sorted(names - set(_STAGE2_TASK_NAMES))}\n"
            f"  移除: {sorted(set(_STAGE2_TASK_NAMES) - names)}\n"
            "请复核新任务的写表是否与其它 stage2 任务相交，并更新本文件快照与 "
            "daily_pipeline.py 里 stage2 段落的计数与结论。"
        )

    def test_every_stage2_task_has_a_static_write_path(self) -> None:
        """每个任务都解析出至少一个写方法，否则下面的不相交断言会退化成空集互不相交。"""
        resolved = _stage2_write_methods()
        empty = sorted(task for task, methods in resolved.items() if not methods)
        assert not empty, (
            f"以下 stage2 任务解析不到任何 db 写方法: {empty}\n"
            "要么它们本就不写表（应从 stage2 移出或加入显式白名单并说明），"
            "要么写路径用了 _write_methods_of 未覆盖的形态，请扩展抽取逻辑。"
        )

    def test_stage2_modules_write_only_through_db_receiver(self) -> None:
        """stage2 模块里的写调用接收者必须都是 ``db``（或 ``db._db``）。

        这是抽取器完整性的守卫：若某处改用别名接收者（``provider`` / ``manager`` ...），
        ``_write_methods_of`` 会静默漏掉它，写表不相交的结论也就不再完整。
        """
        write_methods = _provider_write_methods()
        offenders: list[str] = []
        for module in sorted(set(_stage2_modules().values())):
            source = REPO_ROOT / f"{module.replace('.', '/')}.py"
            text = source.read_text(encoding="utf-8")
            for node in ast.walk(_parse(source)):
                if not isinstance(node, ast.Call):
                    continue
                callee = node.func
                if not isinstance(callee, ast.Attribute) or callee.attr not in write_methods:
                    continue
                if not _is_db_receiver(callee.value):
                    snippet = ast.get_source_segment(text, callee) or callee.attr
                    offenders.append(f"{module}:{node.lineno} {snippet}")
        assert not offenders, (
            "stage2 模块出现非 db 接收者的写调用，审计抽取不再完整：\n  "
            + "\n  ".join(offenders)
            + "\n请确认该写路径的目标表，并扩展 _write_methods_of 的识别规则。"
        )


class TestStage2WriteDisjointness:
    """审计结论本身：写方法级与写表级都不相交。"""

    def test_stage2_write_methods_are_pairwise_disjoint(self) -> None:
        """没有任何 db 写方法被两个 stage2 任务共用（方法级不相交）。"""
        conflicts = _pairs_with_two_owners(_stage2_write_methods())
        assert not conflicts, (
            "stage2 存在共用写方法（等价于共用表），任务级并发会互相覆盖：\n"
            + "\n".join(f"  {method}: {tasks}" for method, tasks in sorted(conflicts.items()))
        )

    def test_stage2_write_tables_are_pairwise_disjoint(self) -> None:
        """没有任何表被两个 stage2 任务写入（表级不相交，即并行化的写前提）。

        判据取「注册表声明 ∪ 代码静态解析」的并集：任一来源认定某任务会写该表即计入，
        因此结论偏保守，不会因为某一来源遗漏而放行一个真实的写冲突。
        """
        resolved = _stage2_write_tables()
        declared = _stage2_declared_tables()
        combined = {
            task: declared.get(task, frozenset()) | tables for task, tables in resolved.items()
        }
        empty = sorted(task for task, tables in combined.items() if not tables)
        assert not empty, f"以下任务既没声明写表也解析不到写表: {empty}"

        conflicts = _pairs_with_two_owners(combined)
        assert not conflicts, (
            "stage2 存在写表相交，不能任务级并发：\n"
            + "\n".join(f"  {table}: {tasks}" for table, tasks in sorted(conflicts.items()))
        )

    def test_registry_declares_every_statically_resolved_table(self) -> None:
        """代码里实际写的表都必须在注册表声明，否则归属与新鲜度监控会漏表。

        委托给 smartmoney_hunter 的写方法只有在兄弟仓库源码可见时才参与本断言，
        因此该子集在 CI 上不生效；方法级不相交断言（上一测试）不受此影响。
        """
        resolved = _stage2_write_tables()
        declared = _stage2_declared_tables()
        gaps = {task: sorted(tables - declared[task]) for task, tables in resolved.items() if tables - declared[task]}
        assert not gaps, (
            "以下任务写了未在 core.task_registry 声明的表（TaskSpec.tables 有遗漏）：\n"
            + "\n".join(f"  {task}: {tables}" for task, tables in sorted(gaps.items()))
            + "\n请补进 tables 声明；是否同时补 date_columns 需判断该表的真实频率——"
            "把月频表按日频纳入新鲜度面板会产生假告警。"
        )


class TestStage2DelegatedWrites:
    """委托给 smartmoney_hunter 的写方法（11 个）在源码可见时的完整性。"""

    def test_delegated_write_targets_are_resolved(self) -> None:
        """委托写方法在兄弟仓库源码可见时应当全部解析出表（本地完整证据）。

        CI 不安装 smartmoney_hunter，此处跳过；跳过时表级结论由注册表声明与
        providers.py 直连 SQL 两部分支撑。
        """
        if _sibling_database_source() is None:
            pytest.skip("smartmoney_hunter 源码不可见（CI 环境），跳过委托写目标解析")

        methods = {method for methods in _stage2_write_methods().values() for method in methods}
        resolved = _all_method_tables()
        unresolved = sorted(method for method in methods if not resolved.get(method))
        assert not unresolved, (
            f"以下 stage2 写方法解析不出目标表，表级审计存在空洞: {unresolved}\n"
            "请确认它们是否仍是委托写，并同步 _sibling_database_source 的定位方式。"
        )
