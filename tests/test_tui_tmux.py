"""TUI 的端到端交互测试（真 tmux 会话 + 真按键 + 真抓屏）。

这两个用例的价值就在于「真」：真的 Textual 进程、真的伪终端、真的按键事件。它们**只在装了
tmux 的机器上跑**（CI 现已安装 tmux，见 ``.github/workflows/ci.yml``），也正因为如此，它们
不能对**本机状态**敏感——否则「本机必红、干净机器上必绿」的红噪声会一直挂着。

## 曾经的时序假设（已修）

改动前两条用例都在「启动后主界面立即可交互」这个假设上：

* ``_wait_for_text(session, "SmartMoney Pipeline Manager")`` 通过后就直接 ``send-keys t/c``。
  但 ``tui/app.py::on_mount`` 会先跑 ``find_running_pipeline_processes()``——它既读
  ``/tmp/daily_pipeline.pid``，也 ``pgrep -f 'daily_pipeline\\.py'`` **全机扫描**。只要本机
  **任何地方**有 ``daily_pipeline.py`` 在跑（操作者长期挂着 ``monthly_repair``/回补是常态），
  启动就会推一个**模态**的 ``ConfirmStopScreen``（「检测到后台进程」）。模态屏吃掉所有按键，
  于是 ``t``/``c`` 永远到不了主界面：主题不切换、复制面板不打开。
  → 现在由 ``_wait_until_interactive`` 显式把「有弹窗/无弹窗」两种启动形态都走完再发按键。

* 按键之后用固定 ``time.sleep(1.0)`` 等配置文件落盘、``sleep(0.5)`` 等弹窗消失。
  这两处都是在赌渲染/落盘比 sleep 快。→ 现在一律轮询到条件成立（``_wait_for_theme_change``、
  ``_wait_for_text_gone``），既不再赌时间，也不再白等。

顺带把会话名加上 pid 并先清同名残留：固定会话名在上一轮异常退出（留下同名会话）时会直接
``new-session`` 失败，而失败信息与真实原因（TUI 行为）完全无关。
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
from pathlib import Path

import pytest

_MAIN_TITLE = "SmartMoney Pipeline Manager"
# 启动时「检测到后台进程」模态框的标题片段（见 tui/screens/confirm_stop.py）
_STARTUP_DIALOG = "检测到后台进程"
# 复制面板标题（见 tui/screens/copy_panel.py）
_COPY_PANEL_TITLE = "选择要复制的面板"

_POLL_INTERVAL = 0.25


@pytest.mark.skipif(not shutil.which("tmux"), reason="tmux not installed")
class TestTUIEndToEnd:
    """使用 tmux 对 TUI 进行端到端交互测试。

    这些测试启动真实的 Textual TUI 进程，并通过 tmux 发送按键、
    捕获屏幕内容，验证 CopyPanelScreen 弹窗和主题切换能正常工作。
    """

    @pytest.fixture
    def tui_config(self, tmp_path: Path) -> Path:
        """为测试提供隔离的 TUI 配置文件。"""
        config = tmp_path / "tui.json"
        config.write_text("{}", encoding="utf-8")
        return config

    # ── tmux 原语 ──────────────────────────────────────────────────────

    def _tmux_send(self, session: str, keys: str) -> None:
        """向 tmux 会话发送按键。"""
        subprocess.run(
            ["tmux", "send-keys", "-t", session, keys],
            check=True,
            capture_output=True,
        )

    def _tmux_capture(self, session: str) -> str:
        """捕获 tmux 会话当前 pane 的屏幕内容。"""
        result = subprocess.run(
            ["tmux", "capture-pane", "-t", session, "-p"],
            check=True,
            capture_output=True,
            # 显式 utf-8：断言里是中文面板标题，而 `text=True` 默认用 locale 编码解码——
            # 在 LANG 未设的 Linux runner 上那是 ascii，抓到中文直接 UnicodeDecodeError。
            text=True,
            encoding="utf-8",
        )
        return result.stdout

    def _wait_for_text(self, session: str, text: str, timeout: float = 10.0) -> bool:
        """等待屏幕内容中出现指定文本。"""
        deadline = time.time() + timeout
        while time.time() < deadline:
            if text in self._tmux_capture(session):
                return True
            time.sleep(_POLL_INTERVAL)
        return False

    def _wait_for_text_gone(self, session: str, text: str, timeout: float = 5.0) -> bool:
        """等待屏幕内容中不再出现指定文本。"""
        deadline = time.time() + timeout
        while time.time() < deadline:
            if text not in self._tmux_capture(session):
                return True
            time.sleep(_POLL_INTERVAL)
        return False

    # ── 启动 ───────────────────────────────────────────────────────────

    def _start_tui(self, session: str, tui_config: Path) -> None:
        """在后台 tmux 会话里启动 TUI。"""
        # 上一轮异常退出可能留下同名会话，而 new-session 遇到同名会直接失败
        subprocess.run(
            ["tmux", "kill-session", "-t", session],
            check=False,
            capture_output=True,
        )
        env = {**os.environ, "QUANT_TUI_CONFIG_PATH": str(tui_config)}
        subprocess.run(
            ["tmux", "new-session", "-d", "-s", session, "uv", "run", "python", "tui.py"],
            check=True,
            capture_output=True,
            env=env,
        )

    def _dismiss_startup_dialog_if_present(self, session: str) -> bool:
        """若启动时弹出「检测到后台进程」模态框，按 ``n``（保留运行）关掉它。

        这个弹窗取决于**本机**是否有 ``daily_pipeline.py`` 在跑，所以两种形态都要能走：
        机器上挂着管道时它必出现，而它是模态的——不关掉的话后面所有按键都被它吃掉。

        刻意用 ``n`` 而不是 ``y``：``y`` 会把操作者正在跑的管道**真的杀掉**，
        测试绝不能有这种副作用。

        Returns:
            是否真的出现过弹窗。
        """
        if not self._wait_for_text(session, _STARTUP_DIALOG, timeout=2.0):
            return False
        self._tmux_send(session, "n")
        assert self._wait_for_text_gone(session, _STARTUP_DIALOG, timeout=5.0), (
            f"启动弹窗没被关掉，后续按键会继续被它吃掉；当前屏：\n{self._tmux_capture(session)}"
        )
        return True

    def _wait_until_interactive(self, session: str) -> None:
        """等到主界面渲染**且**启动弹窗（若有）已处理，此时按键才会送到主界面。"""
        assert self._wait_for_text(session, _MAIN_TITLE, timeout=15.0), (
            f"TUI 主界面未在 15 秒内渲染；当前屏：\n{self._tmux_capture(session)}"
        )
        self._dismiss_startup_dialog_if_present(session)

    # ── 配置读写 ───────────────────────────────────────────────────────

    @staticmethod
    def _read_theme(config: Path) -> str | None:
        """读配置文件里的主题；文件缺失/半写/无该键时返回 ``None``。"""
        try:
            data = json.loads(config.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return None
        theme = data.get("theme")
        return str(theme) if theme is not None else None

    def _wait_for_theme_change(self, config: Path, previous: str, timeout: float = 5.0) -> str | None:
        """轮询配置文件直到主题变成与 ``previous`` 不同（取代固定 ``sleep`` 的时序假设）。"""
        deadline = time.time() + timeout
        latest: str | None = previous
        while time.time() < deadline:
            latest = self._read_theme(config)
            if latest is not None and latest != previous:
                return latest
            time.sleep(_POLL_INTERVAL)
        return latest

    # ── 用例 ───────────────────────────────────────────────────────────

    def test_copy_panel_opens_and_closes(self, tui_config: Path) -> None:
        """按 'c' 打开复制面板弹窗，按 Esc 关闭。"""
        session = f"quant_tui_copy_panel_{os.getpid()}"
        self._start_tui(session, tui_config)

        try:
            self._wait_until_interactive(session)

            # 按 'c' 打开复制面板
            self._tmux_send(session, "c")
            assert self._wait_for_text(session, _COPY_PANEL_TITLE, timeout=5.0), (
                f"按 'c' 后复制面板未出现；当前屏：\n{self._tmux_capture(session)}"
            )

            # 按 Esc 关闭弹窗
            self._tmux_send(session, "Escape")
            assert self._wait_for_text_gone(session, _COPY_PANEL_TITLE, timeout=5.0), (
                f"按 Esc 后复制面板未关闭；当前屏：\n{self._tmux_capture(session)}"
            )

            # 关掉弹窗后仍在主界面（Esc 只关弹窗，不该退出应用）。
            # 断言主界面标题仍然在，而不是断言 "Dashboard"：Dashboard 面板在弹窗打开时
            # 本来就露在弹窗两侧，用它判断「回到主界面」等于恒真。
            assert self._wait_for_text(session, _MAIN_TITLE, timeout=5.0)
        finally:
            subprocess.run(["tmux", "kill-session", "-t", session], check=False, capture_output=True)

    def test_theme_toggle_persists(self, tui_config: Path) -> None:
        """按 't' 切换主题，并持久化到配置文件。"""
        session = f"quant_tui_theme_{os.getpid()}"
        self._start_tui(session, tui_config)

        try:
            self._wait_until_interactive(session)

            # 初始主题应为 dark（config 为空 {} 时默认 dark，且启动不会改写配置）
            initial = self._read_theme(tui_config)
            assert initial in (None, "textual-dark"), f"空配置下应停留在默认主题，实得 {initial!r}"
            initial = initial or "textual-dark"

            # 按 't' 切换主题并持久化到配置文件
            self._tmux_send(session, "t")
            after1 = self._wait_for_theme_change(tui_config, initial)
            assert after1 is not None and after1 != initial, (
                f"按 't' 后主题未写入配置文件（仍为 {after1!r}）；当前屏：\n{self._tmux_capture(session)}"
            )

            # 再按一次 't' 切换主题，验证每次切换都会改变并持久化
            self._tmux_send(session, "t")
            after2 = self._wait_for_theme_change(tui_config, after1)
            assert after2 is not None and after2 != after1, (
                f"第二次按 't' 后主题未再变（仍为 {after2!r}）；当前屏：\n{self._tmux_capture(session)}"
            )
        finally:
            subprocess.run(["tmux", "kill-session", "-t", session], check=False, capture_output=True)
