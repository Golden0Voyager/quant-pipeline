"""core.freshness 的测试（自 test_tui.py 平移并补充边界断言）。

期望值以 tui.py 原函数搬运前的真实行为为准，锁定搬运零语义变化。
"""

import sqlite3

from core.freshness import compute_catch_up_tasks
from core.task_registry import table_date_columns


def _fresh_latest_dates(expected: str) -> dict:
    """所有表都更新到 expected 的基准状态。"""
    return dict.fromkeys(table_date_columns(), expected)


class TestComputeCatchUpTasks:
    EXPECTED = "2026-07-31"
    STALE = "2026-07-30"

    def test_all_fresh_returns_empty(self):
        assert compute_catch_up_tasks(_fresh_latest_dates(self.EXPECTED), self.EXPECTED) == []

    def test_stale_bars_selects_dependency_chain_in_order(self):
        latest = _fresh_latest_dates(self.EXPECTED)
        latest["daily_bars"] = self.STALE
        latest["indicators"] = self.STALE
        tasks = compute_catch_up_tasks(latest, self.EXPECTED)
        assert tasks == ["update_bars", "update_indicators"]

    def test_chip_em_table_maps_to_daily_task(self):
        """chip_distribution_em 表有两个 owner，必须选日常任务而非 ON_DEMAND 全市场版。"""
        latest = _fresh_latest_dates(self.EXPECTED)
        latest["chip_distribution_em"] = self.STALE
        tasks = compute_catch_up_tasks(latest, self.EXPECTED)
        assert tasks == ["update_chip_distribution_em"]
        assert "update_chip_distribution_em_fullmarket" not in tasks

    def test_expected_semantics_excluded_tables_not_selected(self):
        """T+1 / 周更 / 月更 / 季更 / 无数据不视为缺失（沿用面板语义）。"""
        latest = _fresh_latest_dates(self.EXPECTED)
        latest["margin_trading"] = self.STALE  # T+1
        latest["stock_pledge"] = "2026-07-24"  # 周更
        latest["macro_monthly"] = "2026-07-20"  # 月更
        latest["quarterly_financials"] = "2026-06-30"  # 季更
        assert compute_catch_up_tasks(latest, self.EXPECTED) == []

    def test_long_tail_ordered_last(self):
        latest = _fresh_latest_dates(self.EXPECTED)
        latest["chip_distribution_em"] = self.STALE
        latest["daily_bars"] = self.STALE
        latest["fund_flow"] = self.STALE
        tasks = compute_catch_up_tasks(latest, self.EXPECTED)
        assert tasks[0] == "update_bars"
        assert tasks[-1] == "update_chip_distribution_em"


def test_date_status():
    from core.freshness import date_status
    assert date_status("2026-07-07", "2026-07-07") == "最新"
    assert date_status(None, "2026-07-07") == "无数据"
    assert date_status("2026-07-06", "2026-07-07") == "略滞后"
    assert date_status("2026-07-01", "2026-07-07") == "滞后"


def test_date_status_boundaries():
    from core.freshness import date_status
    assert date_status(None, "2026-07-31") == "无数据"
    assert date_status("2026-07-31", "2026-07-31") == "最新"
    # 数据日期超过期望日（非交易日也有数据）仍视为最新
    assert date_status("2026-08-01", "2026-07-31") == "最新"
    # 2 个自然日以内（含第 2 天）为「略滞后」
    assert date_status("2026-07-30", "2026-07-31") == "略滞后"
    assert date_status("2026-07-29", "2026-07-31") == "略滞后"
    # 超过 2 个自然日为「滞后」
    assert date_status("2026-07-28", "2026-07-31") == "滞后"
    # 无法解析的日期按「滞后」处理
    assert date_status("not-a-date", "2026-07-31") == "滞后"


def test_normalize_date():
    from core.freshness import normalize_date
    assert normalize_date(None) is None
    assert normalize_date("2026-07-07") == "2026-07-07"
    assert normalize_date("20260630") == "2026-06-30"
    assert normalize_date("20260331") == "2026-03-31"
    # chip_distribution 等表以 DATE 类型存储，SQLite 返回带时间戳的字符串
    assert normalize_date("2026-07-13 00:00:00") == "2026-07-13"
    assert normalize_date("not-a-date") == "not-a-date"


def test_status_for_table_weekly_and_delayed():
    from core.freshness import status_for_table
    # stock_pledge 为按周更新表：10 个自然日内返回「按周更新」
    assert status_for_table("stock_pledge", "2026-07-24", "2026-07-31", None) == "按周更新"
    # 超过 10 个自然日未更新时回退到日期判定（滞后）
    assert status_for_table("stock_pledge", "2020-01-01", "2026-07-31", None) == "滞后"
    # T+1 表在只差一天时返回「T+1」而非滞后
    assert status_for_table("fx_rate", "2026-07-30", "2026-07-31", None) == "T+1"
    # 月更/季更表有数据即返回周期性标记
    assert status_for_table("macro_monthly", "2026-07-20", "2026-07-31", None) == "按月更新"
    assert status_for_table("quarterly_financials", "2026-06-30", "2026-07-31", None) == "按季更新"
    # 更新中的表优先返回「更新中」
    assert status_for_table("daily_bars", "2026-07-31", "2026-07-31", ["daily_bars"]) == "更新中"
    # 普通表回退到日期判定
    assert status_for_table("daily_bars", "2026-07-29", "2026-07-31", None) == "略滞后"


def test_status_for_table_newly_paneled_periodic_tables():
    """裁决 B 新上面板的周期表必须按周期判定，不得按交易日误报滞后。"""
    from core.freshness import status_for_table
    assert status_for_table("stock_list", "2026-06-01", "2026-07-31", None) == "按月更新"
    assert status_for_table("concept_member", "2026-06-01", "2026-07-31", None) == "按月更新"
    # index_member_history 在按周 10 天窗口内返回「按周更新」
    assert status_for_table("index_member_history", "2026-07-29", "2026-07-31", None) == "按周更新"


def test_non_daily_task_tables_must_be_classified_as_non_daily():
    """非日频任务写的表必须落在非日频集合里，否则会被按交易日误判。

    这是一条**规则**，而不是例子清单（上面那个测试只盖了当时注意到的三张表）：
    ``status_for_table`` 的周/月/季分支都以「表在集合里」为前提，一旦某张周期表漏进
    集合，判定就会 fallthrough 成按日频比较，在完整度面板上永久显示「滞后」且永不自愈。
    ``fund_holdings`` 与 ``top10_shareholders`` 就是这样漏掉的——它们当时分别因为
    「表是空的」和「report_date 恰好是未来季末」而没暴露出来。

    只对**有 date_columns 的表**断言：没有日期列的表不进 ``table_date_columns()``，
    也就不参与任何新鲜度判定，不存在 fallthrough 风险。
    """
    from core.freshness import MONTHLY_TABLES, QUARTERLY_TABLES, WEEKLY_TABLES
    from core.task_registry import TASK_REGISTRY, Cadence

    non_daily_cadences = {Cadence.WEEKLY, Cadence.MONTHLY, Cadence.QUARTERLY}
    classified = WEEKLY_TABLES | MONTHLY_TABLES | QUARTERLY_TABLES
    date_columns = table_date_columns()
    misclassified = sorted(
        (spec.name, spec.cadence.name, table)
        for spec in TASK_REGISTRY
        if spec.cadence in non_daily_cadences
        for table in spec.tables
        if table in date_columns and table not in classified
    )
    assert not misclassified, (
        "以下非日频表不在 freshness 的非日频集合里，会被按交易日判定为永久「滞后」：\n"
        + "\n".join(f"  {task} ({cadence}) -> {table}" for task, cadence, table in misclassified)
        + "\n请补进 core/freshness.py 的 WEEKLY_TABLES / MONTHLY_TABLES / QUARTERLY_TABLES；"
        "若该表并非周期发布，而是当日发布较晚，则 DELAYED_PUBLISH_TABLES 才是对应规则。"
    )


def test_get_daily_bars_coverage(tmp_path):
    from core.freshness import get_daily_bars_coverage
    db_file = tmp_path / "test.db"
    conn = sqlite3.connect(db_file)
    conn.execute("CREATE TABLE daily_bars (ts_code TEXT, trade_date TEXT)")
    conn.execute("INSERT INTO daily_bars VALUES ('000001.SZ', '2026-07-09')")
    conn.execute("INSERT INTO daily_bars VALUES ('600000.SH', '2026-07-08')")
    conn.execute("INSERT INTO daily_bars VALUES ('000002.SZ', '2026-07-01')")
    conn.commit()
    conn.close()

    up_to_date, total = get_daily_bars_coverage(str(db_file), "2026-07-10")
    assert total == 3
    # 2026-07-09 >= 2026-07-08 (expect -2) → up to date
    # 2026-07-08 >= 2026-07-08 → up to date
    # 2026-07-01 <  2026-07-08 → lagging
    assert up_to_date == 2

    # non-existent DB
    assert get_daily_bars_coverage("/nonexistent/test.db", "2026-07-10") == (0, 0)


def test_get_latest_dates(tmp_path):
    from core.freshness import get_latest_dates
    db_file = tmp_path / "test.db"
    conn = sqlite3.connect(db_file)
    conn.execute("CREATE TABLE daily_bars (trade_date TEXT)")
    conn.execute("CREATE TABLE indicators (trade_date TEXT)")
    conn.execute("INSERT INTO daily_bars (trade_date) VALUES ('2026-07-07')")
    conn.execute("INSERT INTO indicators (trade_date) VALUES ('2026-07-06')")
    conn.commit()
    conn.close()

    result = get_latest_dates(str(db_file))
    assert result.get("daily_bars") == "2026-07-07"
    assert result.get("indicators") == "2026-07-06"


def test_get_latest_dates_covers_legacy_panel_tables(tmp_path):
    """面板查询必须覆盖无注册任务的历史遗留表（institutional_holdings）。"""
    import sqlite3

    from core.freshness import get_latest_dates
    db_file = tmp_path / "t.db"
    conn = sqlite3.connect(str(db_file))
    conn.execute("CREATE TABLE institutional_holdings (report_date TEXT)")
    conn.execute("INSERT INTO institutional_holdings VALUES ('2026-06-30')")
    conn.commit()
    conn.close()
    result = get_latest_dates(str(db_file))
    assert result["institutional_holdings"] == "2026-06-30"


def test_freshness_cache_and_invalidation(tmp_path):
    import time

    from core.freshness import (
        clear_freshness_cache,
        get_daily_bars_coverage,
        get_latest_dates,
    )

    clear_freshness_cache()
    db_file = tmp_path / "cache_test.db"
    conn = sqlite3.connect(str(db_file))
    conn.execute("CREATE TABLE daily_bars (ts_code TEXT, trade_date TEXT)")
    conn.execute("INSERT INTO daily_bars VALUES ('000001.SZ', '2026-07-09')")
    conn.commit()
    conn.close()

    # First call - cache miss & population
    res1 = get_latest_dates(str(db_file))
    assert res1.get("daily_bars") == "2026-07-09"

    cov1 = get_daily_bars_coverage(str(db_file), "2026-07-10")
    assert cov1 == (1, 1)

    # Cached hit check
    assert get_latest_dates(str(db_file)).get("daily_bars") == "2026-07-09"

    # Modify DB directly (changing mtime)
    time.sleep(0.01)
    conn = sqlite3.connect(str(db_file))
    conn.execute("INSERT INTO daily_bars VALUES ('600000.SH', '2026-07-10')")
    conn.commit()
    conn.close()

    # Second call - mtime changed -> auto cache invalidation & updated result
    res2 = get_latest_dates(str(db_file))
    assert res2.get("daily_bars") == "2026-07-10"

    cov2 = get_daily_bars_coverage(str(db_file), "2026-07-10")
    assert cov2 == (2, 2)


class TestCheckTaskFreshness:
    EXPECTED = "2026-09-21"

    def test_empty_on_trading_day_is_stale(self, monkeypatch):
        from core.freshness import FreshnessVerdict, check_task_freshness

        monkeypatch.setattr("core.freshness.is_trading_day", lambda d: True)
        monkeypatch.setattr(
            "core.freshness.get_expected_latest_trading_day",
            lambda *a, **k: self.EXPECTED,
        )
        v = check_task_freshness(None, expected=self.EXPECTED)
        assert v.is_stale is True
        assert "empty" in v.reason.lower() or "空" in v.reason
        assert "trading day" in v.reason.lower() or "交易日" in v.reason
        assert v.max_date is None
        assert v.expected == self.EXPECTED
        assert isinstance(v, FreshnessVerdict)

    def test_empty_on_holiday_not_stale(self, monkeypatch):
        from core.freshness import check_task_freshness

        monkeypatch.setattr("core.freshness.is_trading_day", lambda d: False)
        monkeypatch.setattr(
            "core.freshness.get_expected_latest_trading_day",
            lambda *a, **k: self.EXPECTED,
        )
        v = check_task_freshness([], expected=self.EXPECTED)
        assert v.is_stale is False
        assert v.max_date is None
        assert v.expected == self.EXPECTED

    def test_stale_max_date(self, monkeypatch):
        from core.freshness import check_task_freshness

        records = [{"trade_date": "2026-09-18"}]
        monkeypatch.setattr(
            "core.freshness.get_recent_trading_days",
            lambda *a, **k: ["2026-09-21"],
        )
        v = check_task_freshness(
            records, date_field="trade_date", expected=self.EXPECTED
        )
        assert v.is_stale is True
        assert "2026-09-18" in v.reason
        assert "2026-09-21" in v.reason

    def test_fresh_max_date(self, monkeypatch):
        from core.freshness import check_task_freshness

        records = [{"trade_date": "2026-09-21"}]
        monkeypatch.setattr(
            "core.freshness.get_recent_trading_days",
            lambda *a, **k: ["2026-09-21"],
        )
        v = check_task_freshness(
            records, date_field="trade_date", expected=self.EXPECTED
        )
        assert v.is_stale is False

    def test_holiday_no_false_positive(self, monkeypatch):
        """期望日为周二节假日，日历回退至上周五；记录 max = 周五 → 不报 stale。"""
        from core.freshness import check_task_freshness

        records = [{"trade_date": "2026-09-18"}]
        monkeypatch.setattr(
            "core.freshness.get_recent_trading_days",
            lambda end, count: ["2026-09-18"] if end == "2026-09-22" else ["2026-09-21"],
        )
        v = check_task_freshness(
            records, date_field="trade_date", expected="2026-09-22"
        )
        assert v.is_stale is False

    def test_calendar_unavailable_fallback_tolerance(self, monkeypatch):
        from core.freshness import check_task_freshness

        monkeypatch.setattr(
            "core.freshness.get_recent_trading_days", lambda *a, **k: []
        )

        records_near = [{"trade_date": "2026-09-20"}]
        v_near = check_task_freshness(
            records_near, date_field="trade_date", expected=self.EXPECTED
        )
        assert v_near.is_stale is False

        records_old = [{"trade_date": "2026-09-17"}]
        v_old = check_task_freshness(
            records_old, date_field="trade_date", expected=self.EXPECTED
        )
        assert v_old.is_stale is True

    def test_no_date_field_empty_only(self, monkeypatch):
        from core.freshness import check_task_freshness

        monkeypatch.setattr(
            "core.freshness.get_expected_latest_trading_day",
            lambda *a, **k: self.EXPECTED,
        )
        monkeypatch.setattr("core.freshness.is_trading_day", lambda d: False)

        v = check_task_freshness([{"any": "data"}], date_field=None, expected=self.EXPECTED)
        assert v.is_stale is False

        monkeypatch.setattr("core.freshness.is_trading_day", lambda d: True)
        v_empty = check_task_freshness(
            [], date_field=None, expected=self.EXPECTED
        )
        assert v_empty.is_stale is True

    def test_accepts_dataframe(self, monkeypatch, tmp_path):
        import pandas as pd

        from core.freshness import check_task_freshness

        df = pd.DataFrame({"trade_date": ["2026-09-18"]})
        monkeypatch.setattr(
            "core.freshness.get_recent_trading_days",
            lambda *a, **k: ["2026-09-21"],
        )
        v = check_task_freshness(df, date_field="trade_date", expected=self.EXPECTED)
        assert v.is_stale is True
        assert "2026-09-18" in v.reason

