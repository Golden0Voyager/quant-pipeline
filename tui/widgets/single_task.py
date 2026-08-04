"""单任务选择下拉组件 (SingleTaskWidget)。"""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from textual.widgets import Select, Static

from core.task_registry import TASK_GROUPS, lookup_task

if TYPE_CHECKING:
    from tui.app import PipelineApp

# ── 单任务下拉：组名与双语标签（成员/组序一律来自 TASK_GROUPS 单一来源）──
_SINGLE_TASK_GROUP_NAMES: dict[str, str] = {
    "core": "核心行情",
    "fund": "资金面",
    "valuation": "估值/财务",
    "macro": "宏观/全球",
    "sector_index": "行业/大盘",
    "derivatives": "ETF/可转债/港通",
    "events": "事件信号",
}

_SINGLE_TASK_LABELS: dict[str, str] = {
    "update_bars": "日线行情 (Daily Bars)",
    "update_indicators": "技术指标 (Indicators)",
    "update_chip_distribution": "筹码分布 (Chip Dist.)",
    "update_chip_distribution_em": "筹码分布线上 (Chip EM)",
    "update_chip_distribution_em_fullmarket": "筹码分布全市场 (Full Market Chip EM)",
    "update_fundamentals": "基本面数据 (Fundamentals)",
    "update_market_snapshot": "行情快照 (Market Snapshot)",
    "update_fund_flow": "资金流向 (Fund Flow)",
    "update_sector_fund_flow": "板块资金 (Sector Fund Flow)",
    "update_north_hold": "北向持仓 (North Hold)",
    "update_margin_trading": "融资融券 (Margin Trading)",
    "update_dragon_tiger": "龙虎榜 (Dragon Tiger)",
    "update_block_trade": "大宗交易 (Block Trade)",
    "update_historical_valuation": "历史估值 (Valuation)",
    "update_quarterly_financials": "季度财务 (Quarterly Fin.)",
    "update_shareholder_count": "股东户数 (Shareholders)",
    "update_dividend_summary": "分红信息 (Dividends)",
    "update_market_valuation": "大盘估值 (Market Valuation)",
    "update_financial_history": "财务历史 (Financial History)",
    "update_china_macro": "中国宏观 (China Macro)",
    "update_gold_price": "黄金价格 (Gold Price)",
    "update_crude_oil": "原油价格 (Crude Oil)",
    "update_usd": "汇率 (USD/CNY)",
    "update_global_index": "全球指数 (Global Index)",
    "update_us_treasury": "美债收益率 (US Treasury)",
    "update_futures": "期货日线 (Futures)",
    "update_money_market": "货币市场 (Money Market)",
    "update_sector_industry": "行业分类 (Sector Industry)",
    "update_industry": "行业更新 (Industry)",
    "update_sector_derivatives": "行业板块 (Sector Derivatives)",
    "update_index_daily": "大盘指数 (Index Daily)",
    "update_limit_up_down": "涨跌停 (Limit U/D)",
    "update_concept_board": "概念板块 (Concept Board)",
    "update_concept_member": "概念成分 (Concept Members)",
    "update_index_membership": "指数成分 (Index Membership)",
    "update_etf_daily": "ETF日线 (ETF Daily)",
    "update_cb_quotation": "可转债行情 (CB Quotation)",
    "update_cb_redeem": "可转债强赎 (CB Redeem)",
    "update_cb_index": "可转债指数 (CB Index)",
    "update_south_flow": "南向资金 (South Flow)",
    "update_ah_premium": "AH溢价 (AH Premium)",
    "update_restricted_share": "限售解禁 (Restricted Share)",
    "update_earnings_forecast": "业绩预告 (Earnings Forecast)",
    "update_stock_repurchase": "股票回购 (Stock Repurchase)",
    "update_institution_survey": "机构调研 (Institution Survey)",
    "update_stock_pledge": "股票质押 (Stock Pledge)",
    "update_option_sentiment": "期权情绪 (Option Sentiment)",
}


def _build_single_task_groups() -> list[tuple[str, list[tuple[str, str]]]]:
    """从 TASK_GROUPS 派生单任务下拉分组（成员与组序的唯一来源在 registry）。"""
    def _label(task: str) -> str:
        if task in _SINGLE_TASK_LABELS:
            return _SINGLE_TASK_LABELS[task]
        spec = lookup_task(task)
        return spec.display_label if spec and spec.display_label else task

    return [
        (
            _SINGLE_TASK_GROUP_NAMES.get(group_key, group_key),
            [(_label(task), task) for task in tasks],
        )
        for group_key, tasks in TASK_GROUPS.items()
    ]


class SingleTaskWidget(Static):
    """Single task selector with a dropdown, organized by task groups."""

    # 成员与组序以 core.task_registry.TASK_GROUPS 为单一来源（防漂移测试锁定），
    # 下拉结构由模块级 _build_single_task_groups 派生
    _SINGLE_TASK_GROUPS: list[tuple[str, list[tuple[str, str]]]] = (
        _build_single_task_groups()
    )

    _SINGLE_TASK_UTILS: list[tuple[str, str]] = [
        ("股票列表 (Stock List)", "update_stock_list"),
        ("重试失败 (Retry Failed)", "retry"),
        ("健康检查 (Health Check)", "health_check"),
    ]

    SINGLE_TASKS: list[tuple[str, str]] = []

    @classmethod
    def _validate_against_registry(cls) -> None:
        """Assert every referenced task name exists in TASK_REGISTRY.

        Keeps TUI display labels flexible while preventing drift from
        the single source of truth for task identity.
        """
        try:
            from core.task_registry import lookup_task

            all_task_names: set[str] = set()
            for _, tasks in cls._SINGLE_TASK_GROUPS:
                for _, name in tasks:
                    all_task_names.add(name)
            for _, name in cls._SINGLE_TASK_UTILS:
                all_task_names.add(name)

            missing = [n for n in sorted(all_task_names) if lookup_task(n) is None]
            if missing:
                import logging
                logging.getLogger(__name__).warning(
                    "TUI references unregistered tasks: %s", missing
                )
        except ImportError:
            pass  # registry not available (e.g. test environment)

    @classmethod
    def _build_single_tasks(cls) -> list[tuple[str, str]]:
        """把分组定义展开为带分隔符的下拉选项列表。"""
        cls._validate_against_registry()
        # 收盘刷新是哨兵项（仿 __sep__ 惯例），不进 _SINGLE_TASK_GROUPS/_UTILS，
        # 避免被 _validate_against_registry 当作未注册任务警告；
        # 每周补全/每月修复同理（它们是 tier 入口，不在 TASK_REGISTRY）
        options: list[tuple[str, str]] = [
            ("每日更新 (Daily Update)", "all"),
            ("收盘刷新 (Close Refresh)", "__refresh_today__"),
        ]
        for group_name, tasks in cls._SINGLE_TASK_GROUPS:
            options.append((f"[dim]── {group_name} ──[/dim]", f"__sep__{group_name}"))
            options.extend(tasks)
        options.append(("[dim]── 工具 ──[/dim]", "__sep__tools"))
        options.extend(cls._SINGLE_TASK_UTILS)
        options.append(("每周补全 (Weekly Backfill)", "__weekly_backfill__"))
        options.append(("每月修复 (Monthly Repair)", "__monthly_repair__"))
        return options

    def on_mount(self) -> None:
        self.border_title = "Single Task"
        self.SINGLE_TASKS = self._build_single_tasks()
        select = Select(
            options=self.SINGLE_TASKS,
            prompt="选择一项任务...",
            id="task-select",
        )
        self.mount(select)

    async def on_select_changed(self, event: Select.Changed) -> None:
        # event.value 在 clear() 后为 Select.NULL（NoSelection 对象），
        # 只有 str 类型才是真实任务名，避免误触发导致杀进程
        value = event.value
        select = self.query_one("#task-select", Select)
        app = cast("PipelineApp", self.app)
        if value == "__refresh_today__":
            # 收盘刷新是独立 CLI 模式（--refresh-today 与 --task 互斥），
            # 走专属确认流程而非 _run_or_schedule 延迟调度
            await app.action_refresh_today()
        elif value == "__weekly_backfill__":
            # 每周补全层哨兵项：路由到专属 action（不带 --force）
            await app.action_weekly_backfill()
        elif value == "__monthly_repair__":
            # 每月修复层哨兵项：路由到专属 action（不带 --force）
            await app.action_monthly_repair()
        elif isinstance(value, str) and value and not value.startswith("__sep__"):
            await app.action_run_single_task(value)
        # 无论选中真实任务还是分组分隔符，都重置回提示状态
        # （会触发新的 Select.Changed 但被上面过滤掉）
        select.clear()
