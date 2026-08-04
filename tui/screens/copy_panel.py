"""面板内容复制选择弹窗。"""

from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import Vertical
from textual.screen import ModalScreen
from textual.widgets import Label, ListItem, ListView


class CopyPanelScreen(ModalScreen[str]):
    """弹窗：选择要复制的面板。"""

    PANELS: list[tuple[str, str, str]] = [
        ("data-completeness", "1", "📀 数据完整度"),
        ("status-dashboard", "2", "📊 Dashboard"),
        ("scraping-progress", "3", "📈 Progress"),
        ("live-logs", "4", "📋 日志"),
    ]

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._panel_ids: set[str] = {panel_id for panel_id, _key, _label in self.PANELS}
        self._key_map = {key: panel_id for panel_id, key, _label in self.PANELS}

    def compose(self) -> ComposeResult:
        with Vertical(id="copy-dialog"):
            yield Label("选择要复制的面板")
            with Vertical(id="copy-list"):
                list_items = []
                for panel_id, key, label in self.PANELS:
                    list_items.append(
                        ListItem(Label(f"[dim][{key}][/dim] {label}"), id=panel_id)
                    )
                yield ListView(*list_items, id="copy-list-view")
            yield Label("按 1-4 / Enter 选择 · Esc 取消", id="copy-hint")

    def on_list_view_selected(self, event: ListView.Selected) -> None:
        """处理 ListView 项选择。"""
        if event.item and event.item.id in self._panel_ids:
            self.dismiss(event.item.id)

    def on_key(self, event) -> None:
        if event.key in ("escape", "q"):
            self.dismiss(None)
            return
        if event.key in self._key_map:
            self.dismiss(self._key_map[event.key])
