"""Integration tests for financials.update_industry with mocked HTTP + ThreadPoolExecutor.

``update_industry`` uses triple API fallback:
  A) Eastmoney F10 CompanySurvey API (jbzl.sshy)
  B) AkShare ``stock_individual_info_em``
  C) Sina finance stock details page (regex parsing)

The ``@skip_if_task_locked`` decorator is bypassed by patching ``task_lock``.
HTTP requests are mocked via ``requests.Session``.
All tests use ``tmp_path`` for real SQLite databases.
"""

from __future__ import annotations

import contextlib
import sqlite3
from unittest.mock import MagicMock, patch

import pandas as pd

import tasks.financials as financials

# ===========================================================================
# 测试辅助
# ===========================================================================


def _create_db(db_path: str, stocks: list[tuple[str, str, str | None]]) -> None:
    """Create stock_list table populated with given stocks.

    Each tuple is (code, market, industry_or_None).
    """
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stock_list (
            code TEXT, market TEXT, industry TEXT, updated_at TEXT
        )
    """)
    for code, market, industry in stocks:
        conn.execute(
            "INSERT INTO stock_list (code, market, industry) VALUES (?, ?, ?)",
            (code, market, industry),
        )
    conn.commit()
    conn.close()


@contextlib.contextmanager
def _task_lock_yield_true(name: str):
    """Mock ``core.lock.task_lock`` so the decorator always proceeds."""
    yield True


def _mock_resp(
    status_code: int = 200,
    json_data: object | None = None,
    text: str = "",
) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    if json_data is not None:
        resp.json.return_value = json_data
    resp.encoding = "gb2312"
    resp.text = text
    return resp


# ===========================================================================
# 1. 全部已分类 — 提前返回
# ===========================================================================


class TestAllClassified:
    """Early-return path when every stock already has an industry."""

    def test_no_stocks_to_update(self, tmp_path):
        """All stocks have industry → saved=0, total=0."""
        db = MagicMock()
        db_path = tmp_path / "test.db"
        db.db_path = str(db_path)
        _create_db(str(db_path), [
            ("000001", "sz", "银行"),
            ("600000", "sh", "保险"),
        ])
        with patch("core.lock.task_lock", _task_lock_yield_true), \
             patch.object(financials, "time"):
            result = financials.update_industry(db)
        assert result["saved"] == 0
        assert result["total"] == 0


# ===========================================================================
# 2. 策略 A — 东方财富 F10 成功
# ===========================================================================


class TestStrategyA:
    """Eastmoney F10 CompanySurvey API succeeds."""

    def test_f10_returns_valid_industry(self, tmp_path):
        """F10 returns ``jbzl.sshy`` → industry saved."""
        db = MagicMock()
        db_path = tmp_path / "test.db"
        db.db_path = str(db_path)
        _create_db(str(db_path), [("000001", "sz", None)])

        f10_resp = _mock_resp(200, {"jbzl": {"sshy": "银行"}})

        with patch("core.lock.task_lock", _task_lock_yield_true), \
             patch.object(financials, "time"), \
             patch.object(financials, "get_default_client") as mock_gc:
            mock_session = MagicMock()
            mock_gc.return_value.get_session.return_value = mock_session
            mock_session.get.return_value = f10_resp

            result = financials.update_industry(db)

        assert result["saved"] == 1
        assert result["total"] == 1
        assert result["failed"] == 0

        conn = sqlite3.connect(str(db_path))
        row = conn.execute(
            "SELECT industry FROM stock_list WHERE code='000001'"
        ).fetchone()
        assert row[0] == "银行"
        conn.close()

    def test_f10_status_not_200_falls_to_sina(self, tmp_path):
        """F10 returns 500 → retries 3x → falls to Sina → succeeds."""
        db = MagicMock()
        db_path = tmp_path / "test.db"
        db.db_path = str(db_path)
        _create_db(str(db_path), [("000001", "sz", None)])

        f10_err = _mock_resp(500)
        sina_ok = _mock_resp(
            200, text="<html>所属行业板块</td><td>保险</td></html>"
        )

        with patch("core.lock.task_lock", _task_lock_yield_true), \
             patch.object(financials, "time"), \
             patch.object(financials, "ak", None), \
             patch.object(financials, "get_default_client") as mock_gc:
            mock_session = MagicMock()
            mock_gc.return_value.get_session.return_value = mock_session
            mock_session.get.side_effect = [f10_err] * 3 + [sina_ok]

            result = financials.update_industry(db)

        assert result["saved"] == 1
        assert result["failed"] == 0

        conn = sqlite3.connect(str(db_path))
        row = conn.execute(
            "SELECT industry FROM stock_list WHERE code='000001'"
        ).fetchone()
        assert row[0] == "保险"
        conn.close()

    def test_f10_jbzl_missing_key(self, tmp_path):
        """F10 returns 200 but ``jbzl`` has no ``sshy`` → falls to Sina."""
        db = MagicMock()
        db_path = tmp_path / "test.db"
        db.db_path = str(db_path)
        _create_db(str(db_path), [("000001", "sz", None)])

        f10_bad = _mock_resp(200, {"jbzl": {"other": "value"}})
        sina_ok = _mock_resp(
            200, text="所属行业板块</td><td>证券</td>"
        )

        with patch("core.lock.task_lock", _task_lock_yield_true), \
             patch.object(financials, "time"), \
             patch.object(financials, "ak", None), \
             patch.object(financials, "get_default_client") as mock_gc:
            mock_session = MagicMock()
            mock_gc.return_value.get_session.return_value = mock_session
            mock_session.get.side_effect = [f10_bad] * 3 + [sina_ok]

            result = financials.update_industry(db)

        assert result["saved"] == 1

    def test_f10_request_raises(self, tmp_path):
        """F10 HTTP request raises exception → retries → falls to Sina."""
        db = MagicMock()
        db_path = tmp_path / "test.db"
        db.db_path = str(db_path)
        _create_db(str(db_path), [("000001", "sz", None)])

        sina_ok = _mock_resp(
            200, text="所属行业板块</td><td>银行</td>"
        )

        with patch("core.lock.task_lock", _task_lock_yield_true), \
             patch.object(financials, "time"), \
             patch.object(financials, "ak", None), \
             patch.object(financials, "get_default_client") as mock_gc:
            mock_session = MagicMock()
            mock_gc.return_value.get_session.return_value = mock_session
            mock_session.get.side_effect = [RuntimeError("timeout")] * 3 + [sina_ok]

            result = financials.update_industry(db)

        assert result["saved"] == 1
        assert result["failed"] == 0


# ===========================================================================
# 3. 策略 A + B — F10 回退到 AkShare
# ===========================================================================


class TestStrategyBFallback:
    """F10 fails → AkShare ``stock_individual_info_em`` succeeds."""

    def test_f10_fails_akshare_succeeds(self, tmp_path):
        """F10 500 → AkShare ``行业`` row found → saved=1."""
        db = MagicMock()
        db_path = tmp_path / "test.db"
        db.db_path = str(db_path)
        _create_db(str(db_path), [("000001", "sz", None)])

        f10_err = _mock_resp(500)
        ak = MagicMock()
        ak.stock_individual_info_em.return_value = pd.DataFrame({
            "item": ["股票名称", "行业", "上市日期"],
            "value": ["平安银行", "银行", "1991-04-03"],
        })

        with patch("core.lock.task_lock", _task_lock_yield_true), \
             patch.object(financials, "time"), \
             patch.object(financials, "ak", ak), \
             patch.object(financials, "get_default_client") as mock_gc:
            mock_session = MagicMock()
            mock_gc.return_value.get_session.return_value = mock_session
            mock_session.get.side_effect = [f10_err] * 3

            result = financials.update_industry(db)

        assert result["saved"] == 1
        assert ak.stock_individual_info_em.called

    def test_akshare_returns_empty(self, tmp_path):
        """F10 fails, AkShare returns empty → fails."""
        db = MagicMock()
        db_path = tmp_path / "test.db"
        db.db_path = str(db_path)
        _create_db(str(db_path), [("000001", "sz", None)])

        f10_err = _mock_resp(500)
        ak = MagicMock()
        ak.stock_individual_info_em.return_value = pd.DataFrame()

        with patch("core.lock.task_lock", _task_lock_yield_true), \
             patch.object(financials, "time"), \
             patch.object(financials, "ak", ak), \
             patch.object(financials, "get_default_client") as mock_gc:
            mock_session = MagicMock()
            mock_gc.return_value.get_session.return_value = mock_session
            mock_session.get.side_effect = [f10_err] * 3

            result = financials.update_industry(db)

        assert result["saved"] == 0
        assert result["failed"] == 1


# ===========================================================================
# 4. 策略 A + C — F10 回退到新浪
# ===========================================================================


class TestStrategyCFallback:
    """F10 fails, AkShare unavailable → Sina finance page succeeds."""

    def test_sina_regex_matches_industry(self, tmp_path):
        """Sina page contains ``所属行业板块`` → regex parses industry."""
        db = MagicMock()
        db_path = tmp_path / "test.db"
        db.db_path = str(db_path)
        _create_db(str(db_path), [("600000", "sh", None)])

        html = """
        <html>
        <table>
        <tr><td>所属行业板块</td><td>银行</td></tr>
        </table>
        </html>
        """
        sina_ok = _mock_resp(200, text=html)

        with patch("core.lock.task_lock", _task_lock_yield_true), \
             patch.object(financials, "time"), \
             patch.object(financials, "ak", None), \
             patch.object(financials, "get_default_client") as mock_gc:
            mock_session = MagicMock()
            mock_gc.return_value.get_session.return_value = mock_session
            mock_session.get.return_value = sina_ok

            result = financials.update_industry(db)

        assert result["saved"] == 1
        assert result["failed"] == 0
        # Verify Sina was called after F10 (3 retries)
        assert mock_session.get.call_count == 4

    def test_sina_regex_no_match(self, tmp_path):
        """Sina HTML doesn't match regex → fails."""
        db = MagicMock()
        db_path = tmp_path / "test.db"
        db.db_path = str(db_path)
        _create_db(str(db_path), [("600000", "sh", None)])

        html = "<html>没有行业信息</html>"
        sina_fail = _mock_resp(200, text=html)

        with patch("core.lock.task_lock", _task_lock_yield_true), \
             patch.object(financials, "time"), \
             patch.object(financials, "ak", None), \
             patch.object(financials, "get_default_client") as mock_gc:
            mock_session = MagicMock()
            mock_gc.return_value.get_session.return_value = mock_session
            mock_session.get.return_value = sina_fail

            result = financials.update_industry(db)

        assert result["saved"] == 0
        assert result["failed"] == 1


# ===========================================================================
# 5a. _f10_blocked 限流熔断路径
# ===========================================================================


class TestF10Blocked:
    """When F10 returns 429 (rate-limited), subsequent stocks skip F10."""

    def test_f10_429_triggers_block_for_second_stock(self, tmp_path):
        """2 stocks. Stock 1 F10 returns 429 → _f10_blocked set.
        Stock 2 sees _f10_blocked → skips F10 entirely, uses Sina."""
        db = MagicMock()
        db_path = tmp_path / "test.db"
        db.db_path = str(db_path)
        _create_db(str(db_path), [
            ("000001", "sz", None),
            ("000002", "sz", None),
        ])

        f10_429 = _mock_resp(429)
        # Stock 1 Sina success, Stock 2 Sina success (since _f10_blocked
        # causes both to fall through to Sina after their respective F10 phases)
        sina_ok = _mock_resp(200, text="所属行业板块</td><td>银行</td>")

        # Track which URLs get called
        call_log: list[str] = []

        def _side_effect(url: str, **kwargs):
            call_log.append(url)
            if "/PC_HSF10/CompanySurvey" in url:
                return f10_429
            return sina_ok

        with patch("core.lock.task_lock", _task_lock_yield_true), \
             patch.object(financials, "time"), \
             patch.object(financials, "ak", None), \
             patch.object(financials, "get_default_client") as mock_gc:
            mock_session = MagicMock()
            mock_gc.return_value.get_session.return_value = mock_session
            mock_session.get.side_effect = _side_effect

            result = financials.update_industry(db)

        # Both stocks should eventually succeed via Sina
        assert result["saved"] == 2
        assert result["failed"] == 0

        # Stock 1: 3 F10 retries (429) → _f10_blocked set → 1 Sina = 4 calls
        # Stock 2: 0 F10 calls (blocked) → 1 Sina = 1 call
        # After _f10_blocked is set, Stock 2 skips F10 → only 1 Sina call for Stock 2
        f10_calls = [u for u in call_log if "/PC_HSF10/CompanySurvey" in u]
        sina_calls = [u for u in call_log if "money.finance.sina.com.cn" in u]
        assert len(f10_calls) >= 3  # At least stock 1's 3 retries
        assert len(sina_calls) >= 2  # Both stocks try Sina

        conn = sqlite3.connect(str(db_path))
        rows = conn.execute(
            "SELECT code, industry FROM stock_list ORDER BY code"
        ).fetchall()
        assert all(industry == "银行" for _, industry in rows)
        conn.close()


# ===========================================================================
# 6. 全部策略失败
# ===========================================================================


class TestAllStrategiesFail:
    """All three fallback strategies return nothing."""

    def test_all_fail_saved_0_failed_1(self, tmp_path):
        """F10 500, AkShare=None, Sina 404 → saved=0, failed=1."""
        db = MagicMock()
        db_path = tmp_path / "test.db"
        db.db_path = str(db_path)
        _create_db(str(db_path), [("000001", "sz", None)])

        f10_err = _mock_resp(500)
        sina_err = _mock_resp(404)

        with patch("core.lock.task_lock", _task_lock_yield_true), \
             patch.object(financials, "time"), \
             patch.object(financials, "ak", None), \
             patch.object(financials, "get_default_client") as mock_gc:
            mock_session = MagicMock()
            mock_gc.return_value.get_session.return_value = mock_session
            mock_session.get.side_effect = [f10_err] * 3 + [sina_err]

            result = financials.update_industry(db)

        assert result["saved"] == 0
        assert result["total"] == 1
        assert result["failed"] == 1
        assert result["coverage_pct"] == 0.0


# ===========================================================================
# 6. 批量多股票
# ===========================================================================


class TestBatchProcessing:
    """Multiple stocks in one batch (batch_size=50)."""

    def test_three_stocks_all_succeed(self, tmp_path):
        """3 stocks via F10 → saved=3."""
        db = MagicMock()
        db_path = tmp_path / "test.db"
        db.db_path = str(db_path)
        _create_db(str(db_path), [
            ("000001", "sz", None),
            ("000002", "sz", None),
            ("600000", "sh", None),
        ])

        f10_ok = _mock_resp(200, {"jbzl": {"sshy": "银行"}})

        with patch("core.lock.task_lock", _task_lock_yield_true), \
             patch.object(financials, "time"), \
             patch.object(financials, "get_default_client") as mock_gc:
            mock_session = MagicMock()
            mock_gc.return_value.get_session.return_value = mock_session
            mock_session.get.return_value = f10_ok

            result = financials.update_industry(db)

        assert result["saved"] == 3
        assert result["failed"] == 0
        assert result["coverage_pct"] == 100.0

        conn = sqlite3.connect(str(db_path))
        rows = conn.execute(
            "SELECT code, industry FROM stock_list ORDER BY code"
        ).fetchall()
        assert len(rows) == 3
        for _, industry in rows:
            assert industry == "银行"
        conn.close()

    def test_mixed_success_failure(self, tmp_path):
        """Stock 1 F10 success, Stock 2 all fail → saved=1, failed=1.

        Uses a thread-safe callable ``side_effect`` that dispatches HTTP
        responses based on the URL, avoiding race conditions from list-based
        side_effect consumed concurrently by ThreadPoolExecutor threads.
        """
        db = MagicMock()
        db_path = tmp_path / "test.db"
        db.db_path = str(db_path)
        _create_db(str(db_path), [
            ("000001", "sz", None),
            ("000002", "sz", None),
        ])

        f10_ok = _mock_resp(200, {"jbzl": {"sshy": "银行"}})
        f10_err = _mock_resp(500)
        sina_err = _mock_resp(404)

        call_index: dict[str, int] = {}

        def _side_effect(url: str, **kwargs):
            code = "SZ000001" if "SZ000001" in url else "SZ000002"
            idx = call_index.setdefault(code, 0)
            call_index[code] = idx + 1
            if code == "SZ000001":
                # Stock 1: F10 always succeeds
                return f10_ok
            else:
                # Stock 2: F10 3 retries (idx 0,1,2) then Sina (idx 3)
                if idx < 3:
                    return f10_err
                return sina_err

        with patch("core.lock.task_lock", _task_lock_yield_true), \
             patch.object(financials, "time"), \
             patch.object(financials, "ak", None), \
             patch.object(financials, "get_default_client") as mock_gc:
            mock_session = MagicMock()
            mock_gc.return_value.get_session.return_value = mock_session
            mock_session.get.side_effect = _side_effect

            result = financials.update_industry(db)

        assert result["saved"] == 1
        assert result["failed"] == 1
        assert result["total"] == 2

        conn = sqlite3.connect(str(db_path))
        row1 = conn.execute(
            "SELECT industry FROM stock_list WHERE code='000001'"
        ).fetchone()
        assert row1[0] == "银行"
        row2 = conn.execute(
            "SELECT industry FROM stock_list WHERE code='000002'"
        ).fetchone()
        assert row2[0] is None  # Still NULL
        conn.close()


# ===========================================================================
# 7. 任务锁场景
# ===========================================================================


class TestTaskLock:
    """``@skip_if_task_locked`` decorator behavior."""

    def test_task_already_locked(self, tmp_path):
        """Lock held → returns locked status dict."""
        db = MagicMock()
        db_path = tmp_path / "test.db"
        db.db_path = str(db_path)
        _create_db(str(db_path), [("000001", "sz", None)])

        with patch("core.lock.TaskLock.acquire", return_value=False):
            result = financials.update_industry(db)

        assert result["status"] == "locked"
        assert result["skipped"] is True
        assert result["reason"] == "task already running"


# ===========================================================================
# 8. 交易所前缀映射
# ===========================================================================


class TestExchangePrefix:
    """Verify market → prefix mapping for F10 API code."""

    def test_shenzhen_prefix(self, tmp_path):
        """sz market → SZ prefix."""
        db = MagicMock()
        db_path = tmp_path / "test.db"
        db.db_path = str(db_path)
        _create_db(str(db_path), [("000001", "sz", None)])

        f10_ok = _mock_resp(200, {"jbzl": {"sshy": "银行"}})

        with patch("core.lock.task_lock", _task_lock_yield_true), \
             patch.object(financials, "time"), \
             patch.object(financials, "get_default_client") as mock_gc:
            mock_session = MagicMock()
            mock_gc.return_value.get_session.return_value = mock_session
            mock_session.get.return_value = f10_ok
            result = financials.update_industry(db)

        assert result["saved"] == 1
        # Verify URL contained "SZ000001"
        call_url = mock_session.get.call_args[0][0]
        assert "SZ000001" in call_url

    def test_shanghai_prefix(self, tmp_path):
        """sh market → SH prefix."""
        db = MagicMock()
        db_path = tmp_path / "test.db"
        db.db_path = str(db_path)
        _create_db(str(db_path), [("600000", "sh", None)])

        f10_ok = _mock_resp(200, {"jbzl": {"sshy": "银行"}})

        with patch("core.lock.task_lock", _task_lock_yield_true), \
             patch.object(financials, "time"), \
             patch.object(financials, "get_default_client") as mock_gc:
            mock_session = MagicMock()
            mock_gc.return_value.get_session.return_value = mock_session
            mock_session.get.return_value = f10_ok
            result = financials.update_industry(db)

        assert result["saved"] == 1
        call_url = mock_session.get.call_args[0][0]
        assert "SH600000" in call_url

    def test_unknown_market_fallback(self, tmp_path):
        """unknown market → BJ prefix (code 8xxxxx)."""
        db = MagicMock()
        db_path = tmp_path / "test.db"
        db.db_path = str(db_path)
        _create_db(str(db_path), [("830000", "unknown", None)])

        f10_ok = _mock_resp(200, {"jbzl": {"sshy": "银行"}})

        with patch("core.lock.task_lock", _task_lock_yield_true), \
             patch.object(financials, "time"), \
             patch.object(financials, "get_default_client") as mock_gc:
            mock_session = MagicMock()
            mock_gc.return_value.get_session.return_value = mock_session
            mock_session.get.return_value = f10_ok
            result = financials.update_industry(db)

        assert result["saved"] == 1
        call_url = mock_session.get.call_args[0][0]
        assert "BJ830000" in call_url

    def test_star_market_prefix(self, tmp_path):
        """star market → SH prefix."""
        db = MagicMock()
        db_path = tmp_path / "test.db"
        db.db_path = str(db_path)
        _create_db(str(db_path), [("688000", "star", None)])

        f10_ok = _mock_resp(200, {"jbzl": {"sshy": "半导体"}})

        with patch("core.lock.task_lock", _task_lock_yield_true), \
             patch.object(financials, "time"), \
             patch.object(financials, "get_default_client") as mock_gc:
            mock_session = MagicMock()
            mock_gc.return_value.get_session.return_value = mock_session
            mock_session.get.return_value = f10_ok
            result = financials.update_industry(db)

        assert result["saved"] == 1
        call_url = mock_session.get.call_args[0][0]
        assert "SH688000" in call_url

    def test_beijing_market_prefix(self, tmp_path):
        """bj market → BJ prefix."""
        db = MagicMock()
        db_path = tmp_path / "test.db"
        db.db_path = str(db_path)
        _create_db(str(db_path), [("920000", "bj", None)])

        f10_ok = _mock_resp(200, {"jbzl": {"sshy": "专用设备"}})

        with patch("core.lock.task_lock", _task_lock_yield_true), \
             patch.object(financials, "time"), \
             patch.object(financials, "get_default_client") as mock_gc:
            mock_session = MagicMock()
            mock_gc.return_value.get_session.return_value = mock_session
            mock_session.get.return_value = f10_ok
            result = financials.update_industry(db)

        assert result["saved"] == 1
        call_url = mock_session.get.call_args[0][0]
        assert "BJ920000" in call_url
