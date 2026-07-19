import json
import shutil
import subprocess
import time
from pathlib import Path

import pytest


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
            text=True,
        )
        return result.stdout

    def _wait_for_text(self, session: str, text: str, timeout: float = 5.0) -> bool:
        """等待屏幕内容中出现指定文本。"""
        deadline = time.time() + timeout
        while time.time() < deadline:
            if text in self._tmux_capture(session):
                return True
            time.sleep(0.5)
        return False

    def test_copy_panel_opens_and_closes(self, tui_config: Path) -> None:
        """按 'c' 打开复制面板弹窗，按 Esc 关闭。"""
        session = "quant_tui_copy_panel"
        env = {
            **dict(subprocess.os.environ),  # type: ignore[attr-defined]
            "QUANT_TUI_CONFIG_PATH": str(tui_config),
        }

        # 启动 TUI
        subprocess.run(
            ["tmux", "new-session", "-d", "-s", session, "uv", "run", "python", "tui.py"],
            check=True,
            env=env,
        )

        try:
            # 等待 TUI 主界面渲染
            assert self._wait_for_text(session, "SmartMoney Pipeline Manager", timeout=5.0)

            # 按 'c' 打开复制面板
            self._tmux_send(session, "c")
            assert self._wait_for_text(session, "选择要复制的面板", timeout=3.0)

            # 按 Esc 关闭弹窗
            self._tmux_send(session, "Escape")
            time.sleep(0.5)

            # 弹窗关闭后应回到主界面
            assert self._wait_for_text(session, "Dashboard", timeout=3.0)
        finally:
            subprocess.run(["tmux", "kill-session", "-t", session], check=False)

    def test_theme_toggle_persists(self, tui_config: Path) -> None:
        """按 't' 切换主题，并持久化到配置文件。"""
        session = "quant_tui_theme"
        env = {
            **dict(subprocess.os.environ),  # type: ignore[attr-defined]
            "QUANT_TUI_CONFIG_PATH": str(tui_config),
        }

        # 启动 TUI
        subprocess.run(
            ["tmux", "new-session", "-d", "-s", session, "uv", "run", "python", "tui.py"],
            check=True,
            env=env,
        )

        try:
            # 等待 TUI 主界面渲染
            assert self._wait_for_text(session, "SmartMoney Pipeline Manager", timeout=5.0)

            # 初始主题应为 dark（config 为空 {} 时默认 dark）
            config = json.loads(tui_config.read_text(encoding="utf-8"))
            assert config.get("theme", "textual-dark") == "textual-dark"
            initial = config.get("theme", "textual-dark")

            # 按 't' 切换主题并持久化到配置文件
            self._tmux_send(session, "t")
            time.sleep(1.0)
            after1 = json.loads(tui_config.read_text(encoding="utf-8")).get("theme")
            assert after1 is not None and after1 != initial

            # 再按一次 't' 切换主题，验证每次切换都会改变并持久化
            self._tmux_send(session, "t")
            time.sleep(1.0)
            after2 = json.loads(tui_config.read_text(encoding="utf-8")).get("theme")
            assert after2 is not None and after2 != after1
        finally:
            subprocess.run(["tmux", "kill-session", "-t", session], check=False)
