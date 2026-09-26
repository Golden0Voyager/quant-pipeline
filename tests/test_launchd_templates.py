"""launchd 模板门禁
──────────────────
``scripts/launchd/*.plist`` 是「无人值守调度」的可复用模板。它们不可避免要写绝对路径
（launchd 不做 ``~`` 展开），但**解释器必须是本仓库自己的 venv**，不能指向兄弟仓库
``quant_hunter`` —— 那样本仓库入口会跑在别人的依赖集上（曾如此：``ProgramArguments`` 用
``quant_hunter/.venv/bin/python3``，``PATH``/``PYTHONPATH`` 也都指向 quant_hunter）。

兄弟仓库 ``smartmoney_hunter`` 的路径由 ``core._bootstrap.ensure_sibling_paths()`` 在运行期
从 ``__file__`` 自动注入，故 plist 里**不该**再配 ``PYTHONPATH``。

断言刻意不硬编码本机 checkout 路径（CI 上路径不同），只要求模板**内部自洽**：
解释器就落在与脚本同一个 checkout 的 ``.venv/bin`` 下。
"""
from __future__ import annotations

import plistlib
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
LAUNCHD_DIR = REPO_ROOT / "scripts" / "launchd"
PLISTS = sorted(LAUNCHD_DIR.glob("com.smartmoney.*.plist"))
SIBLING_MARKER = "quant_hunter"


def _load(path: Path) -> dict:
    with path.open("rb") as fh:
        return plistlib.load(fh)


def test_plist_templates_are_present() -> None:
    names = {p.name for p in PLISTS}
    assert names == {"com.smartmoney.update.plist", "com.smartmoney.healthcheck.plist"}


@pytest.mark.parametrize("path", PLISTS, ids=lambda p: p.name)
def test_program_arguments_run_the_pipeline_with_the_project_venv(path: Path) -> None:
    """解释器必须位于与 ``daily_pipeline.py`` 同一个 checkout 的 ``.venv/bin`` 下。"""
    cfg = _load(path)
    args = [str(a) for a in cfg["ProgramArguments"]]
    assert len(args) >= 2, f"{path.name}: ProgramArguments 至少要含解释器与脚本"

    interpreter = Path(args[0])
    script = Path(args[1])

    assert script.name == "daily_pipeline.py"
    assert interpreter.name.startswith("python"), f"{path.name}: 解释器不像 python：{interpreter}"
    assert interpreter.parent == script.parent / ".venv" / "bin", (
        f"{path.name}: 解释器 {interpreter} 不在入口脚本所属 checkout 的 .venv/bin 下"
    )


@pytest.mark.parametrize("path", PLISTS, ids=lambda p: p.name)
def test_no_reference_to_the_sibling_project_venv(path: Path) -> None:
    """PATH / PYTHONPATH / 解释器都不得再指向兄弟仓库。"""
    cfg = _load(path)
    args = [str(a) for a in cfg["ProgramArguments"]]
    env = cfg.get("EnvironmentVariables", {})

    assert SIBLING_MARKER not in args[0], f"{path.name}: 解释器仍指向兄弟仓库：{args[0]}"
    assert SIBLING_MARKER not in env.get("PATH", ""), f"{path.name}: PATH 仍含兄弟仓库"
    assert "PYTHONPATH" not in env, (
        f"{path.name}: 不该配 PYTHONPATH —— 兄弟仓库路径由 core._bootstrap 运行期注入"
    )


@pytest.mark.parametrize("path", PLISTS, ids=lambda p: p.name)
def test_working_directory_matches_the_entry_checkout(path: Path) -> None:
    cfg = _load(path)
    script = Path(str(cfg["ProgramArguments"][1]))
    assert Path(str(cfg["WorkingDirectory"])) == script.parent


def test_both_templates_share_one_interpreter() -> None:
    interpreters = {str(_load(p)["ProgramArguments"][0]) for p in PLISTS}
    assert len(interpreters) == 1, f"两个模板的解释器应一致：{interpreters}"
