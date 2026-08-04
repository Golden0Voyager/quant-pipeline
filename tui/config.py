"""TUI 配置常量与持久化设置。"""

from __future__ import annotations

import json
import os
from pathlib import Path
from zoneinfo import ZoneInfo

# 共享路径常量
DEFAULT_DB_PATH = Path.home() / "Code/quant_data/quant_core.db"
DAEMON_PID_PATH = "/tmp/smartmoney_daemon.pid"
PIPELINE_PID_PATH = "/tmp/daily_pipeline.pid"
PROGRESS_JSON_PATH = Path.home() / "Code/quant_data/progress.json"
LOGS_DIR_PATH = Path.home() / "Code/quant_data/logs"
WATCHLIST_DIR = Path.home() / "Code/quant_agents/watchlists"
TUI_CONFIG_PATH = Path(
    os.environ.get(
        "QUANT_TUI_CONFIG_PATH", Path.home() / ".config/quant_pipeline/tui.json"
    )
)

# 收盘刷新的目标交易日按上海时区计算，与 CLI 保持一致
_SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")

DEFAULT_THEME = "textual-dark"


def load_theme() -> str:
    """从配置文件加载保存的主题，失败时回退到默认主题。"""
    try:
        if TUI_CONFIG_PATH.exists():
            data = json.loads(TUI_CONFIG_PATH.read_text(encoding="utf-8"))
            theme = data.get("theme", DEFAULT_THEME)
            from textual.theme import BUILTIN_THEMES

            if theme in BUILTIN_THEMES:
                return theme
    except Exception:
        pass
    return DEFAULT_THEME


def save_theme(theme_name: str) -> None:
    """持久化保存主题名称。"""
    try:
        TUI_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
        data: dict[str, str] = {}
        if TUI_CONFIG_PATH.exists():
            try:
                data = json.loads(TUI_CONFIG_PATH.read_text(encoding="utf-8"))
            except Exception:
                data = {}
        data["theme"] = theme_name
        TUI_CONFIG_PATH.write_text(json.dumps(data, indent=2), encoding="utf-8")
    except Exception:
        pass
