"""SmartMoney Pipeline Textual TUI 主应用程序。"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
import sys
from datetime import datetime
from pathlib import Path
from typing import Literal

from rich.markup import escape
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Grid
from textual.events import Resize
from textual.widgets import (
    Footer,
    Header,
    Static,
    TabbedContent,
    TabPane,
)

import tui
from core import calendar as _calendar
from core import market_time as _market_time
from core.log_cleanup import cleanup_logs
from core.task_registry import TASK_GROUPS
from tui.config import (
    DAEMON_PID_PATH,
    DEFAULT_DB_PATH,
    LOGS_DIR_PATH,
    load_theme,
    save_theme,
)
from tui.screens.confirm_run import ConfirmRunScreen
from tui.screens.confirm_stop import ConfirmStopScreen
from tui.screens.copy_panel import CopyPanelScreen
from tui.screens.help import HelpScreen
from tui.screens.log_cleanup import LogCleanupScreen
from tui.screens.refresh_today import ConfirmRefreshTodayScreen
from tui.services.formatting import _seconds_until_safe
from tui.services.process import (
    find_latest_log_file,
    get_subprocess_env,
)
from tui.widgets.completeness import DataCompletenessWidget
from tui.widgets.dashboard import DashboardWidget
from tui.widgets.logs import LogsWidget
from tui.widgets.progress import ProgressWidget
from tui.widgets.single_task import SingleTaskWidget
from tui.widgets.task_group import TaskGroupWidget

logger = logging.getLogger("quant_pipeline.tui")
_PROJECT_ROOT = Path(__file__).resolve().parent.parent


class PipelineApp(App):
    """SmartMoney 管道管理终端界面应用。"""

    TITLE = "SmartMoney Pipeline Manager"
    CSS_PATH = "styles.tcss"

    BINDINGS = [
        Binding("s", "run_pipeline", "Daily Update"),
        Binding("r", "resume_pipeline", "Resume"),
        Binding("u", "refresh_today", "Close Refresh"),
        Binding("x", "stop_pipeline", "Stop"),
        Binding("d", "start_daemon", "Daemon"),
        Binding("h", "run_health", "Health"),
        Binding("?", "show_help", "More"),
        Binding("z", "stop_daemon", "Stop Daemon", show=False),
        Binding("f", "run_reconcile", "Data Repair", show=False),
        Binding("c", "copy_panel", "Copy Panel", show=False),
        Binding("t", "toggle_theme", "Toggle Theme", show=False),
        Binding("l", "clean_logs", "Clean Logs", show=False),
        Binding("w", "weekly_backfill", "Weekly Backfill", show=False),
        Binding("m", "monthly_repair", "Monthly Repair", show=False),
        Binding("f5", "refresh_data", "Refresh", show=False),
        Binding("ctrl+c", "quit", "Quit", priority=True, show=False),
        Binding("q", "quit", "Quit", show=False),
    ]

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._background_tasks: set[asyncio.Task] = set()
        self._current_process: asyncio.subprocess.Process | None = None
        # 全局唯一执行槽：任何时刻只允许一个子任务在跑。
        # 分组队列用 wait=True 在槽上排队；手动按键默认拒绝，防手滑重复启动
        self._task_slot = asyncio.Lock()
        self._theme_name = load_theme()
        # 已排入延迟队列的动作名：防止连按重复调度（排队状态对操作者可见）
        self._pending_scheduled: set[str] = set()

    def _notify_and_log(
        self,
        message: str,
        *,
        severity: Literal["information", "warning", "error"] = "information",
        timeout: float = 6.0,
    ) -> None:
        """弹通知的同时写入 Logs 面板：关键结果可回看（通知几秒后即消失）。"""
        self.notify(message, severity=severity, timeout=timeout)
        try:
            logs = self.query_one("#live-logs", LogsWidget)
            stamp = datetime.now().strftime("%H:%M:%S")
            logs.write(f"[dim]{stamp}[/dim] {escape(message)}")
        except Exception:
            pass

    async def _run_and_report(self, action_name: str, *args: str) -> int | None:
        """运行子进程并报告结果：完成/失败既弹通知也写日志面板，失败列出任务名。"""
        started_at = datetime.now().isoformat(timespec="seconds")
        rc = await self._run_in_background(*args)
        if rc == 0:
            self._notify_and_log(f"✅ 「{action_name}」运行完成", severity="information")
        else:
            db_path = str(getattr(tui, "DEFAULT_DB_PATH", DEFAULT_DB_PATH))
            failed_getter = getattr(tui, "get_recent_failed_tasks", None)
            failed = (
                failed_getter(db_path, limit=5, finished_after=started_at)
                if failed_getter else []
            )
            detail = ""
            if failed:
                names = "、".join(f["task_name"] for f in failed)
                detail = f"；失败任务: {names}"
            self._notify_and_log(
                f"❌ 「{action_name}」运行失败 (code={rc}){detail}，详见日志",
                severity="error",
            )
        return rc

    async def on_mount(self) -> None:
        """启动时应用保存的主题、同步自选股，然后检测后台进程询问是否终止。"""
        self.theme = self._theme_name
        self._apply_narrow_class(self.size.width)
        self._start_watchlist_sync()

        proc_finder = getattr(tui, "find_running_pipeline_processes", None)
        processes = proc_finder() if proc_finder else []
        if processes:
            self.push_screen(
                ConfirmStopScreen(processes),
                callback=lambda should_stop: self._on_stop_confirm(
                    should_stop, processes
                ),
            )

    def on_resize(self, event: Resize) -> None:
        """按终端宽度切换 .narrow（TCSS 无 @media，窄屏样式全挂在该类下）。"""
        self._apply_narrow_class(event.size.width)

    @staticmethod
    def _narrow_threshold(width: int) -> bool:
        # 100 列以下左栏 35% ≈ 35 列，Dashboard/Select/分组按钮均会腰斩
        return width < 100

    def _apply_narrow_class(self, width: int) -> None:
        if self._narrow_threshold(width):
            self.add_class("narrow")
        else:
            self.remove_class("narrow")

    def _start_watchlist_sync(self) -> None:
        """Schedule startup watchlist sync outside the Textual event handler."""
        self._create_background_task(self._sync_watchlists())

    def _on_stop_confirm(self, should_stop: bool, processes: list[dict]) -> None:
        if should_stop:
            for p in processes:
                with contextlib.suppress(OSError):
                    os.kill(int(p["pid"]), signal.SIGTERM)
            self.notify(
                f"Sent SIGTERM to {len(processes)} process(es)",
                severity="information",
                timeout=3.0,
            )
        else:
            self.notify(
                "Background processes kept running",
                severity="information",
                timeout=3.0,
            )

    async def _sync_watchlists(self) -> None:
        try:
            db_path = str(getattr(tui, "DEFAULT_DB_PATH", DEFAULT_DB_PATH))
            syncer = getattr(tui, "sync_watchlists_from_files", None)
            if syncer:
                result = syncer(db_path)
            else:
                return
            if not result.success:
                self.notify(
                    f"自选股同步失败: {result.error or '未知错误'}",
                    severity="warning",
                    timeout=6.0,
                )
            elif result.files or result.deactivated:
                self.notify(
                    f"自选股同步完成: 新增 {result.added}, 恢复 {result.reactivated}, "
                    f"停用 {result.deactivated}, 来源 {result.files} 个文件",
                    timeout=4.0,
                )
        except Exception as exc:
            logger.exception("自选股同步发生未处理异常")
            self.notify(
                f"自选股同步失败: {exc}",
                severity="warning",
                timeout=6.0,
            )

    def _create_background_task(self, coro) -> None:
        task = asyncio.create_task(coro)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    def _run_or_schedule(self, action_name: str, *args: str) -> None:
        """弹窗：立即运行、稍后调度到 16:00 或取消。"""

        def _on_dismiss(choice: str | None) -> None:
            if choice == "run-now":
                self._create_background_task(self._run_and_report(action_name, *args))
            elif choice == "run-later":
                if action_name in self._pending_scheduled:
                    self.notify(
                        f"「{action_name}」已在延迟队列中，请勿重复调度",
                        severity="warning",
                        timeout=4.0,
                    )
                    return
                delay_fn = getattr(tui, "_seconds_until_safe", _seconds_until_safe)
                delay = delay_fn()
                self._pending_scheduled.add(action_name)
                self.notify(
                    f"「{action_name}」已调度到安全时间后自动运行（剩余 {delay//60} 分钟）",
                    timeout=6.0,
                )

                async def _delayed():
                    try:
                        await asyncio.sleep(delay)
                        await self._run_and_report(action_name, *args)
                    finally:
                        self._pending_scheduled.discard(action_name)

                # 统一走 _create_background_task：带 done_callback 回收，任务不泄漏
                self._create_background_task(_delayed())

        self.push_screen(ConfirmRunScreen(action_name), _on_dismiss)

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Grid(id="main-grid"):
            yield DashboardWidget(id="status-dashboard")
            yield LogsWidget(id="live-logs")
            with TabbedContent(id="task-tabs"):
                with TabPane("Single", id="tab-single"):
                    yield SingleTaskWidget(id="single-task")
                with TabPane("Groups", id="tab-groups"):
                    yield TaskGroupWidget(id="task-groups")
            yield ProgressWidget(id="scraping-progress")
            yield DataCompletenessWidget(id="data-completeness")
        yield Footer(show_command_palette=False)

    async def _stop_current_process(self) -> None:
        """终止当前正在运行的子进程及其整个进程组。"""
        proc = self._current_process
        if proc is None or proc.returncode is not None:
            self.notify("No running task to stop", severity="warning", timeout=3.0)
            return
        try:
            logger.info("⛔ TUI 停止当前任务子进程 (PID %s)", proc.pid)
            # 杀整个进程组（start_new_session=True 创建的）
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            except (ProcessLookupError, PermissionError):
                proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), timeout=5.0)
            except TimeoutError:
                logger.warning(
                    "⚠️ 子进程 (PID %s) SIGTERM 超时，升级为 SIGKILL", proc.pid
                )
                try:
                    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                except (ProcessLookupError, PermissionError):
                    proc.kill()
                await proc.wait()
            logger.info("✅ 子进程 (PID %s) 已退出", proc.pid)
            self.notify("Task stopped", severity="information", timeout=3.0)
        except ProcessLookupError:
            self.notify("Process already exited", severity="information", timeout=3.0)
        finally:
            self._current_process = None

    async def _run_in_background(self, *args: str, wait: bool = False) -> int | None:
        """在后台运行子进程，返回其退出码（被外部中断时返回 None）。

        wait=False（手动按键）：执行槽被占用时拒绝启动，防手滑重复触发；
        wait=True（分组队列）：在执行槽上排队，等当前任务结束后自动执行。
        """
        env = get_subprocess_env()

        # 有任务在跑就拒绝启动，X 键是唯一的停止入口——
        # 隐式顶掉运行中的任务曾把手动全量更新 SIGKILL 掉（2026-07-29 事故）
        busy = self._task_slot.locked() or (
            self._current_process is not None
            and self._current_process.returncode is None
        )
        if busy and not wait:
            pid = self._current_process.pid if self._current_process else "?"
            self.notify(
                f"已有任务在运行 (PID {pid})，请先按 X 停止",
                severity="warning",
                timeout=5.0,
            )
            return None

        async with self._task_slot:
            return await self._spawn_and_wait(args, env)

    async def _spawn_and_wait(
        self,
        args: tuple[str, ...],
        env: dict[str, str],
    ) -> int | None:
        proc: asyncio.subprocess.Process | None = None
        log = logging.getLogger(__name__)
        try:
            proc = await asyncio.create_subprocess_exec(
                *args,
                env=env,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
                start_new_session=True,
            )
            self._current_process = proc
            await proc.wait()
            if proc.returncode != 0:
                log.error(
                    f"Subprocess {' '.join(args)} exited with code {proc.returncode}"
                )
            return proc.returncode
        except Exception:
            log.exception(f"Exception running subprocess {' '.join(args)}")
            return None
        finally:
            self._current_process = None

    async def _stop_daemon_process(self) -> None:
        """调用 daemon.py stop 干净地停止守护进程（不破坏 _stop_current_process 的副作用）。"""
        daemon_path = str(_PROJECT_ROOT / "scripts" / "daemon.py")
        env = get_subprocess_env()
        try:
            proc = await asyncio.create_subprocess_exec(
                sys.executable,
                daemon_path,
                "stop",
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=10.0)
            if proc.returncode != 0:
                output = stdout.decode("utf-8", errors="replace").strip()
                logger.warning(
                    "停止守护进程失败 (code=%s): %s", proc.returncode, output
                )
        except TimeoutError:
            logger.warning("停止守护进程超时")
        except Exception:
            logger.exception("停止守护进程异常")

    async def action_run_pipeline(self) -> None:
        pipeline_path = str(_PROJECT_ROOT / "daily_pipeline.py")
        self._run_or_schedule(
            "每日更新",
            sys.executable,
            pipeline_path,
            "--task",
            "all",
            "--force",
        )

    async def action_resume_pipeline(self) -> None:
        pipeline_path = str(_PROJECT_ROOT / "daily_pipeline.py")
        self._run_or_schedule(
            "断点续传",
            sys.executable,
            pipeline_path,
            "--task",
            "all",
            "--resume",
            "--force",
        )

    async def action_refresh_today(self) -> None:
        """收盘刷新：确认后启动 --refresh-today。

        16:00 安全闸门与 --force 由 CLI 拥有，这里不重复实现，
        也不复用普通全量更新的延迟调度（_run_or_schedule）。
        """
        pipeline_path = str(_PROJECT_ROOT / "daily_pipeline.py")

        def _on_dismiss(symbols: str | None) -> None:
            if symbols is None:
                # 取消：不启动任何子进程
                return
            args = [sys.executable, pipeline_path, "--refresh-today"]
            if symbols:
                args.extend(["--symbols", symbols])
            self._create_background_task(self._run_refresh_today(*args))

        self.push_screen(ConfirmRefreshTodayScreen(), _on_dismiss)

    async def _run_refresh_today(self, *args: str) -> None:
        """后台执行收盘刷新，结束后用审计记录汇总通知（区分覆盖/保留）。

        启动前先捕获审计库最新 run 的边界，结束后只汇总严格晚于该边界
        的新 run；若子进程在写入自身 refresh_runs 行之前崩溃，则退回纯
        退出码告警，避免把上一次运行的结果误报为本次。
        """
        db_path = str(getattr(tui, "DEFAULT_DB_PATH", DEFAULT_DB_PATH))
        get_run_id_fn = getattr(tui, "get_latest_refresh_run_id", None)
        boundary_run_id = get_run_id_fn(db_path) if get_run_id_fn else None

        returncode = await self._run_in_background(*args)

        get_states_fn = getattr(tui, "get_latest_refresh_task_states", None)
        records = (
            get_states_fn(db_path, boundary_run_id=boundary_run_id)
            if get_states_fn
            else []
        )
        if records:
            format_fn = getattr(tui, "format_refresh_summary", None)
            summary = format_fn(records) if format_fn else str(records)
            severity: Literal["information", "warning"] = (
                "information" if returncode == 0 else "warning"
            )
            self.notify(summary, severity=severity, timeout=8.0)
        elif returncode is not None and returncode != 0:
            self.notify(f"收盘刷新退出码 {returncode}", severity="warning", timeout=6.0)

    async def action_start_daemon(self) -> None:
        daemon_path = str(_PROJECT_ROOT / "scripts" / "daemon.py")
        self._create_background_task(
            self._run_in_background(sys.executable, daemon_path, "start", "--resume")
        )

    async def action_stop_daemon(self) -> None:
        daemon_path = str(_PROJECT_ROOT / "scripts" / "daemon.py")
        self._create_background_task(
            self._run_in_background(sys.executable, daemon_path, "stop")
        )

    async def action_stop_pipeline(self) -> None:
        """停止当前正在运行的 pipeline / resume / health 子进程，
        以及任何后台（含 daemon 启动、或在其它终端启动）正在运行的 daily_pipeline.py 进程。
        若守护进程正在运行，也会一并停止，避免任务被重新拉起。"""
        stopped_pids: list[int] = []

        # 1. 停止 TUI 直接启动的子进程（R / S / H 键启动的任务）
        if (
            self._current_process is not None
            and self._current_process.returncode is None
        ):
            proc_pid = self._current_process.pid
            await self._stop_current_process()
            stopped_pids.append(proc_pid)

        # 2. 停止通过 pgrep 发现的其它后台 daily_pipeline.py 进程
        #    （含 daemon 启动的孙进程、TUI 子进程、在其它终端启动的进程）
        proc_finder = getattr(tui, "find_running_pipeline_processes", None)
        running_procs = (
            proc_finder(skip_ppid_check=True)
            if proc_finder
            else []
        )
        for p in running_procs:
            pid = int(p["pid"])
            try:
                os.kill(pid, signal.SIGTERM)
                stopped_pids.append(pid)
            except OSError:
                pass

        # 3. 若守护进程仍在运行，一并停止（否则会重新拉起任务）
        daemon_getter = getattr(tui, "get_daemon_status", None)
        daemon_status, daemon_pid = (
            daemon_getter(DAEMON_PID_PATH)
            if daemon_getter
            else ("Stopped", None)
        )
        if daemon_status == "Running" and daemon_pid is not None:
            self._create_background_task(self._stop_daemon_process())
            stopped_pids.append(daemon_pid)

        log = logging.getLogger(__name__)
        if stopped_pids:
            self.notify(
                f"已发送停止信号给 {len(set(stopped_pids))} 个进程",
                severity="information",
                timeout=3.0,
            )
            log.info("已停止进程: %s", stopped_pids)
        else:
            self.notify("没有正在运行的任务可停止", severity="warning", timeout=3.0)
            log.info("没有正在运行的任务可停止")

    async def action_run_health(self) -> None:
        pipeline_path = str(_PROJECT_ROOT / "daily_pipeline.py")
        self._run_or_schedule(
            "健康检查",
            sys.executable,
            pipeline_path,
            "--task",
            "health_check",
            "--force",
        )

    async def action_weekly_backfill(self) -> None:
        """每周补全层：完整性优先的缺漏兜底。"""
        pipeline_path = str(_PROJECT_ROOT / "daily_pipeline.py")
        self._run_or_schedule(
            "每周补全",
            sys.executable,
            pipeline_path,
            "--task",
            "weekly_backfill",
        )

    async def action_monthly_repair(self) -> None:
        """每月修复层：正确性优先的校验修复。"""
        pipeline_path = str(_PROJECT_ROOT / "daily_pipeline.py")
        self._run_or_schedule(
            "每月修复",
            sys.executable,
            pipeline_path,
            "--task",
            "monthly_repair",
        )

    async def action_run_reconcile(self) -> None:
        """全量清洗：对比 AkShare 并修复差异。

        统一走 _run_or_schedule 执行槽保护，与其他 action 一致。
        """
        reconcile_path = str(_PROJECT_ROOT / "scripts" / "reconcile_with_akshare.py")
        self._run_or_schedule(
            "全量数据清洗",
            sys.executable,
            reconcile_path,
            "--workers",
            "3",
        )

    async def action_run_single_task(self, task: str) -> None:
        """运行单个数据拉取任务。"""
        pipeline_path = str(_PROJECT_ROOT / "daily_pipeline.py")
        self._run_or_schedule(
            f"单任务: {task}",
            sys.executable,
            pipeline_path,
            "--task",
            task,
            "--force",
        )

    async def action_run_task_group(self, group_key: str) -> None:
        """顺序执行某一任务分组内的所有 single task。"""
        tasks = TASK_GROUPS.get(group_key)
        if not tasks:
            self.notify(f"未知任务分组: {group_key}", severity="error", timeout=3.0)
            return

        label = TaskGroupWidget.GROUP_LABELS.get(group_key, group_key)
        self.notify(
            f"开始执行分组「{label}」，共 {len(tasks)} 个任务，按顺序运行",
            timeout=4.0,
        )
        self._create_background_task(self._run_task_group(label, tasks))

    async def action_run_catch_up(self) -> None:
        """补齐缺失：检测截至最近交易日的滞后表，只补缺的任务。

        目标日 = get_expected_latest_trading_day()（交易日收盘定型后 → 今天，
        否则 → 上一交易日）。分组队列以 --force 运行会绕过盘中门禁，
        因此交易日的盘中/结算窗口直接拒绝，避免把实时快照写成终值。
        """
        sh_now = _market_time.shanghai_now()
        if _calendar.is_trading_day(sh_now.date()) and _market_time.market_phase(sh_now) not in (
            _market_time.PHASE_PRE_OPEN,
            _market_time.PHASE_POST_CLOSE,
        ):
            self.notify(
                "盘中/结算窗口不可补数（上海 16:00 后数据定型再试）",
                severity="warning",
                timeout=5.0,
            )
            return

        exp_fn = getattr(tui, "get_expected_latest_trading_day", None)
        expected = exp_fn() if exp_fn else None

        db_path = str(getattr(tui, "DEFAULT_DB_PATH", DEFAULT_DB_PATH))
        get_dates_fn = getattr(tui, "get_latest_dates", None)
        latest_dates = (
            await asyncio.to_thread(get_dates_fn, db_path)
            if get_dates_fn
            else {}
        )

        compute_fn = getattr(tui, "compute_catch_up_tasks", None)
        tasks = compute_fn(latest_dates, expected) if compute_fn else []

        if not tasks:
            self.notify(f"✅ 无缺失：全部数据已更新到 {expected}", timeout=4.0)
            return

        self.notify(
            f"检测到 {len(tasks)} 项滞后于 {expected}，按依赖顺序补齐: "
            + "、".join(t.removeprefix("update_") for t in tasks[:6])
            + ("…" if len(tasks) > 6 else ""),
            timeout=6.0,
        )
        self._create_background_task(self._run_task_group("补齐缺失", tasks))

    async def _run_task_group(self, label: str, tasks: list[str]) -> None:
        """在后台协程中依次执行分组任务。"""
        pipeline_path = str(_PROJECT_ROOT / "daily_pipeline.py")
        failed: list[str] = []
        for task in tasks:
            if self._task_slot.locked():
                self.notify(
                    f"[{label}] {task} 排队等待当前任务结束...",
                    severity="information",
                    timeout=3.0,
                )
            self.notify(
                f"[{label}] 正在运行: {task}",
                severity="information",
                timeout=2.0,
            )
            # wait=True：在执行槽上排队而非拒绝，多个分组并发点击时自动串行
            returncode = await self._run_in_background(
                sys.executable, pipeline_path, "--task", task, "--force", wait=True
            )
            # returncode 为 None 表示进程被外部中断或发生异常
            if returncode is None or returncode < 0:
                self.notify(
                    f"[{label}] 任务 {task} 被中断，分组执行停止",
                    severity="warning",
                    timeout=4.0,
                )
                failed.append(task)
                break
            if returncode != 0:
                self.notify(
                    f"[{label}] 任务 {task} 退出码 {returncode}，继续执行下一任务",
                    severity="warning",
                    timeout=3.0,
                )
                failed.append(task)
                # 单个任务失败不阻塞同组其他任务
                continue
        else:
            self.notify(
                f"[{label}] 分组全部完成（共 {len(tasks)} 个任务）",
                severity="information",
                timeout=4.0,
            )
            return

        if failed:
            self.notify(
                f"[{label}] 分组执行结束，失败/中断 {len(failed)} 个任务",
                severity="warning",
                timeout=4.0,
            )

    async def action_toggle_theme(self) -> None:
        """轮换主题并持久化。"""
        from textual.theme import BUILTIN_THEMES

        themes = sorted(BUILTIN_THEMES)
        idx = themes.index(self._theme_name) if self._theme_name in themes else -1
        new_idx = (idx + 1) % len(themes)
        new_theme = themes[new_idx]
        self._theme_name = new_theme
        self.theme = new_theme
        save_fn = getattr(tui, "save_theme", save_theme)
        save_fn(new_theme)
        self.notify(
            f"主题已切换: {new_theme} ({new_idx + 1}/{len(themes)})", timeout=3.0
        )

    async def action_show_help(self) -> None:
        """显示快捷键帮助弹窗。"""
        self.push_screen(HelpScreen())

    async def action_refresh_data(self) -> None:
        """手动刷新数据完整度面板。"""
        dc = self.query_one("#data-completeness", DataCompletenessWidget)
        self._create_background_task(dc._refresh_exact())
        self.notify("数据完整度刷新中...", timeout=2.0)

    def action_copy_panel(self) -> None:
        from rich.text import Text

        def _static_plain(w: Static) -> str:
            raw = str(getattr(w, "_Static__content", ""))
            return Text.from_markup(raw).plain if raw else ""

        def _on_dismiss(choice: str | None) -> None:
            if not choice:
                return

            panel_text = ""
            label = ""
            if choice == "data-completeness":
                dc = self.query_one("#data-completeness", DataCompletenessWidget)
                panel_text = _static_plain(dc._content).strip()
                label = "📀 数据完整度"
            elif choice == "status-dashboard":
                dash = self.query_one("#status-dashboard", DashboardWidget)
                panel_text = _static_plain(dash).strip()
                label = "📊 Dashboard"
            elif choice == "scraping-progress":
                prog = self.query_one("#scraping-progress", ProgressWidget)
                panel_text = _static_plain(prog).strip()
                label = "📈 Progress"
            elif choice == "live-logs":
                logs = self.query_one("#live-logs", LogsWidget)
                panel_text = logs.copy_recent_logs(line_count=500).strip()
                label = "📋 日志"

            if not panel_text:
                self.notify(f"{label} — 内容为空", timeout=2.0)
                return

            full = f"=== {label} ===\n{panel_text}"
            self.copy_to_clipboard(full)
            self.notify(f"✓ {label} 已复制到剪贴板", timeout=3.0)

        self.push_screen(CopyPanelScreen(), _on_dismiss)

    async def action_clean_logs(self) -> None:
        """清理日志文件：弹窗选择「全部 / 保留 7 天 / 保留 30 天」。"""
        active_log = find_latest_log_file(str(LOGS_DIR_PATH))

        def _on_dismiss(choice: str | None) -> None:
            if not choice or choice == "cancel":
                return
            keep_days = 0 if choice == "all" else int(choice.removeprefix("keep-"))
            self._create_background_task(self._clean_logs(keep_days, active_log))

        self.push_screen(LogCleanupScreen(), _on_dismiss)

    async def _clean_logs(self, keep_days: int, active_log: str | None) -> None:
        """后台执行日志清理并通过通知反馈结果。"""
        exclude = {active_log} if active_log else set()
        try:
            result = await asyncio.to_thread(
                cleanup_logs, str(LOGS_DIR_PATH), keep_days, exclude
            )
        except Exception as exc:  # pragma: no cover - 防御性兜底
            self.notify(f"日志清理失败: {exc}", severity="error", timeout=5.0)
            return

        for err in result.errors:
            self.notify(f"⚠️  {err}", severity="warning", timeout=4.0)
        if result.deleted_count == 0:
            self.notify("没有需要清理的日志文件", severity="information", timeout=3.0)
            return
        size_mb = result.freed_bytes / (1024 * 1024)
        size_str = (
            f"{size_mb:.1f} MB" if size_mb < 1024 else f"{size_mb / 1024:.2f} GB"
        )
        self.notify(
            f"已清理 {result.deleted_count} 个日志文件，释放 {size_str}",
            severity="information",
            timeout=4.0,
        )
