"""数据完整度与新鲜度展示面板 (DataCompletenessWidget)。"""

from __future__ import annotations

import asyncio
import os
import time

from textual.containers import VerticalScroll
from textual.widgets import Static

import tui
from core.calendar import get_expected_latest_trading_day
from core.freshness import (
    get_daily_bars_coverage,
    get_latest_dates,
    status_for_table,
)
from core.task_registry import (
    TABLE_LABELS,
    TABLE_LABELS_CN,
    task_to_table,
)
from tui.config import DEFAULT_DB_PATH, PROGRESS_JSON_PATH
from tui.services.db_queries import (
    _HEALTHY_STATUSES,
    NO_DATE_TABLES,
    get_all_table_counts,
)
from tui.services.formatting import (
    _ljust_vis,
    _vis_width,
    format_chinese_magnitude,
    format_count,
    get_db_size,
)
from tui.services.process import parse_progress


class DataCompletenessWidget(VerticalScroll):
    """数据完整度与新鲜度面板：展示各表记录数、最新日期与健康度评级。"""

    # 任务名 → 表名列表的映射（用于判断哪些表正在更新），由 core.task_registry 派生
    TASK_TO_TABLE: dict[str, list[str]] = task_to_table()

    # 表标签元数据由 core.task_registry 统一供给
    TABLE_LABELS: dict[str, str] = TABLE_LABELS
    TABLE_LABELS_CN: dict[str, str] = TABLE_LABELS_CN

    # 数据新鲜度排序权重：数字越小越靠前。
    # 用户指定顺序：最新 → T+1 → 略滞后 → 滞后 → 按周更新 → 按月更新 → 按季更新 → 无数据
    _STATUS_ORDER: dict[str, int] = {
        "更新中": 0,
        "最新": 1,
        "T+1": 2,
        "略滞后": 3,
        "滞后": 4,
        "按周更新": 5,
        "按月更新": 6,
        "按季更新": 7,
        "无数据": 8,
    }

    # 状态 → (图标, 颜色)。使用高对比度 hex 色，确保在深色主题下清晰可辨。
    STATUS_STYLES: dict[str, tuple[str, str]] = {
        "更新中": ("↻", "#22d3ee"),
        "最新": ("●", "#10b981"),
        "T+1": ("◐", "#3b82f6"),
        "按周更新": ("◇", "#a3e635"),
        "按月更新": ("◈", "#8b5cf6"),
        "按季更新": ("◆", "#d946ef"),
        "略滞后": ("▲", "#f59e0b"),
        "滞后": ("▼", "#ef4444"),
        "无数据": ("○", "#737373"),
    }

    async def on_mount(self) -> None:
        self.border_title = "Data Completeness"
        self._counts: dict[str, int] = {}
        self._latest_dates: dict[str, str | None] = {}
        self._daily_coverage: tuple[int, int] = (0, 0)
        self._bg_tasks: set[asyncio.Task] = set()
        self._last_updating_table: list[str] | None = None
        # 挂载内容子组件
        self._content = Static(id="dc-content")
        await self.mount(self._content)
        # 轻量计时器：仅用缓存重建显示（读 progress.json 判断更新中，无 DB 查询）
        self.set_interval(30.0, self._rebuild_from_cache)
        # 重量计时器：后台并行重算行数/最新日期/覆盖率（查询较重，低频执行）
        self.set_interval(90.0, self._refresh_exact)
        # 先快速加载（瞬间完成），再后台精确更新
        db_path = str(getattr(tui, "DEFAULT_DB_PATH", DEFAULT_DB_PATH))
        counts_fn = getattr(tui, "get_all_table_counts", get_all_table_counts)
        self._counts = await asyncio.to_thread(counts_fn, db_path, fast=True)
        self._rebuild_content()
        task = asyncio.create_task(self._refresh_exact())
        self._bg_tasks.add(task)
        task.add_done_callback(self._bg_tasks.discard)

    async def _refresh_exact(self) -> None:
        """后台重算行数、最新日期与覆盖率（查询较重，低频执行）。

        串行执行以避免多查询同时抢占同一块磁盘 I/O 导致争用变慢。
        """
        exp_fn = getattr(
            tui, "get_expected_latest_trading_day", get_expected_latest_trading_day
        )
        expected = exp_fn()
        db_path = str(getattr(tui, "DEFAULT_DB_PATH", DEFAULT_DB_PATH))
        counts_fn = getattr(tui, "get_all_table_counts", get_all_table_counts)
        counts = await asyncio.to_thread(counts_fn, db_path, fast=False)
        latest_fn = getattr(tui, "get_latest_dates", get_latest_dates)
        latest = await asyncio.to_thread(latest_fn, db_path)
        cov_fn = getattr(tui, "get_daily_bars_coverage", get_daily_bars_coverage)
        cov = await asyncio.to_thread(cov_fn, db_path, expected)
        if counts:
            self._counts = counts
        if latest:
            self._latest_dates = latest
        self._daily_coverage = cov
        self._rebuild_content()

    def _rebuild_from_cache(self) -> None:
        """仅用已缓存数据重建显示（无 DB 查询，轻量）。"""
        self._rebuild_content()

    @staticmethod
    def _get_updating_table() -> list[str] | None:
        """读取 progress.json，返回当前正在更新的表名列表，若无活跃任务则返回 None。"""
        try:
            mtime = os.path.getmtime(str(PROGRESS_JSON_PATH))
            # 如果 progress.json 超过 90 秒未更新，认为已无活跃任务
            if time.time() - mtime > 90:
                return None
            parser = getattr(tui, "parse_progress", parse_progress)
            progress = parser(str(PROGRESS_JSON_PATH))
            if not progress:
                return None
            task = progress.get("task", "")
            return DataCompletenessWidget.TASK_TO_TABLE.get(task)
        except OSError:
            return None

    @classmethod
    def _get_status_for_table(
        cls,
        tbl: str,
        latest: str | None,
        expected_date: str,
        updating_tables: list[str] | None,
    ) -> str:
        """返回指定表的新鲜度状态标签（纯文本）。实现已收敛至 core.freshness.status_for_table。"""
        return status_for_table(tbl, latest, expected_date, updating_tables)

    def _rebuild_content(self) -> None:
        counts = self._counts
        latest_dates = self._latest_dates
        updating_tables = self._get_updating_table()
        if updating_tables != self._last_updating_table:
            self._last_updating_table = updating_tables
            if updating_tables:
                self.add_class("active-task")
            else:
                self.remove_class("active-task")
        if not counts:
            self._content.update(" 等待数据库连接...")
            return

        # 快速模式：显示估算值，等待后台精确更新
        estimated_total = counts.get("_estimated_total", 0)
        if estimated_total and sum(counts.get(k, 0) for k in self.TABLE_LABELS) == 0:
            db_size = get_db_size(str(DEFAULT_DB_PATH))
            lines = [
                f" - [bold]数据库：[/bold][cyan]{db_size}[/cyan]  [dim]行数加载中...[/dim]",
                "",
            ]
            for _tbl, label in self.TABLE_LABELS.items():
                lines.append(f" - [bold gray]{label}：[/bold gray][dim]计算中...[/dim]")
            self._content.update("\n".join(lines))
            return

        daily_bars = counts.get("daily_bars", 0) or 1
        exp_fn = getattr(
            tui, "get_expected_latest_trading_day", get_expected_latest_trading_day
        )
        expected_date = exp_fn()
        daily_up_to_date, daily_total = self._daily_coverage

        total_rows = sum(v for k, v in counts.items() if not k.startswith("_"))
        stock_count = counts.get("stock_list", 0)

        lines = [
            f" [cyan]{stock_count}[/cyan] [bold]只股票[/bold]    "
            f"[bold]总数据 [/bold][cyan]{format_chinese_magnitude(total_rows)}[/cyan]  "
            f"[bold]期望 [/bold][cyan]{expected_date}[/cyan]",
        ]

        # 先计算每个表的状态与排序键，再按新鲜度排序（滞后/无数据沉底）
        items: list[tuple[int, int, str, str, int, str | None, str]] = []
        health_counts: dict[str, int] = {"healthy": 0, "degraded": 0, "critical": 0}
        for idx, (tbl, label) in enumerate(self.TABLE_LABELS.items()):
            n = counts.get(tbl, 0)
            latest = latest_dates.get(tbl)
            status = self._get_status_for_table(
                tbl, latest, expected_date, updating_tables
            )

            order = self._STATUS_ORDER.get(status, 3)
            items.append((order, idx, tbl, label, n, latest, status))

            if status in _HEALTHY_STATUSES:
                health_counts["healthy"] += 1
            elif status == "略滞后":
                health_counts["degraded"] += 1
            elif status in ("滞后", "无数据"):
                health_counts["critical"] += 1

        health_str = (
            f"[bold #10b981]●[/bold #10b981] {health_counts['healthy']}  "
            f"[bold #f59e0b]▲[/bold #f59e0b] {health_counts['degraded']}  "
            f"[bold #ef4444]▼[/bold #ef4444] {health_counts['critical']}"
        )
        lines.append(f"  健康度: {health_str}")

        items.sort(key=lambda x: (x[0], x[1]))

        max_label_w = max(_vis_width(cn) for cn in self.TABLE_LABELS_CN.values())
        max_count_w = max(
            len(format_count(counts.get(tbl, 0))) for tbl in self.TABLE_LABELS
        )

        for _, _, tbl, label, n, latest, status in items:
            if tbl == "stock_list":
                continue
            formatted = format_count(n)
            rjust_count = formatted.rjust(max_count_w)
            date_str = f"[gray]{latest or '—'}[/gray]"
            icon, color = self.STATUS_STYLES.get(status, ("●", "white"))
            status_str = f"[{color}]{icon} {status}[/{color}]"
            label_cn = self.TABLE_LABELS_CN.get(tbl, label)
            padded_label = _ljust_vis(label_cn, max_label_w)

            if tbl in ("daily_bars", "indicators"):
                if tbl == "daily_bars":
                    pct = (daily_up_to_date / daily_total * 100) if daily_total else 0
                else:
                    pct = n / daily_bars * 100 if daily_bars else 0
                bar, pct_int = self._mini_bar(pct)
                lines.append(
                    f" [dim]{padded_label}[/dim]  {status_str} [cyan]{rjust_count}[/cyan]  {date_str}  {bar} [dim]{pct_int}%[/dim]"
                )
            elif tbl in NO_DATE_TABLES:
                lines.append(
                    f" [dim]{padded_label}[/dim]  [cyan]{rjust_count}[/cyan]  [dim]无日期列[/dim]"
                )
            else:
                lines.append(
                    f" [dim]{padded_label}[/dim]  {status_str} [cyan]{rjust_count}[/cyan]  {date_str}"
                )

        self._content.update("\n".join(lines))

    @staticmethod
    def _mini_bar(pct: float) -> tuple[str, int]:
        length = 10
        pct_int = max(0, min(100, int(round(pct))))
        filled = max(0, min(length, round(length * pct_int / 100)))
        bar = "█" * filled + "░" * (length - filled)
        return f"[bold #22c55e]{bar}[/bold #22c55e]", pct_int
