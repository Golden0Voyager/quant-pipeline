"""测试 core/stock_cyq_em.py — 筹码分布五源级联。

测试策略
--------
- 纯函数（_calculate_chip_numpy / _parse_kline_raw_em / _js_code）优先以真实数据测试
- 网络依赖（EM / 雪球 / 新浪）通过 mock 测试
- DB 读取通过 tmp_path 真实 SQLite 验证
- 主级联 stock_cyq_em 模拟各源路径
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from core.stock_cyq_em import (
    _calc_one_day,
    _calculate_chip_numpy,
    _fetch_kline_db,
    _fetch_kline_em,
    _fetch_kline_sina,
    _fetch_kline_xueqiu,
    _get_js_runtime,
    _js_code,
    _parse_kline_raw_em,
    stock_cyq_em,
)

# ═══════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════


def _kline(close: float, turnover: float = 0.5, **kw) -> dict:
    """生成单条 K 线数据，未指定字段自动从 close 推导。"""
    c = float(close)
    o = float(kw.get("open", c * 0.99))
    h = float(kw.get("high", c * 1.02))
    lv = float(kw.get("low", c * 0.97))
    v = float(kw.get("volume", 10000))
    return {
        "date": kw.get("date", "2024-01-01"),
        "open": o,
        "close": c,
        "high": h,
        "low": lv,
        "volume": v,
        "turnover_rate": turnover,
    }


def _kline_range(prices: list[float], tr: float = 0.5) -> list[dict]:
    """从价格序列生成 K 线列表，递增日期。"""
    return [
        _kline(p, turnover=tr, date=f"2024-01-{i+1:02d}")
        for i, p in enumerate(prices)
    ]


# ═══════════════════════════════════════════════════════════
# 1. 纯函数：_calculate_chip_numpy
# ═══════════════════════════════════════════════════════════


class TestCalculateChipNumpy:
    """_calculate_chip_numpy 纯向量化计算测试。"""

    def test_empty_input_returns_empty(self):
        """空输入返回列正确的空 DataFrame。"""
        result = _calculate_chip_numpy([])
        assert isinstance(result, pd.DataFrame)
        assert result.empty
        expected_cols = [
            "日期", "获利比例", "平均成本", "90成本-低", "90成本-高",
            "90集中度", "70成本-低", "70成本-高", "70集中度",
        ]
        assert list(result.columns) == expected_cols

    def test_single_bar_zero_turnover_all_nan(self):
        """单根 K 线 + 零换手率 → 所有指标为 NaN。"""
        data = [_kline(10.0, turnover=0.0)]
        result = _calculate_chip_numpy(data)
        assert len(result) == 1
        assert pd.isna(result["获利比例"].iloc[0])
        assert pd.isna(result["平均成本"].iloc[0])

    def test_single_bar_with_turnover_produces_values(self):
        """单根 K 线 + 有换手率 → 产生合理数值。"""
        data = [_kline(10.0, turnover=5.0)]
        result = _calculate_chip_numpy(data)
        assert len(result) == 1
        # 获利比例应为 0~1 之间
        pct = result["获利比例"].iloc[0]
        assert not pd.isna(pct)
        assert 0.0 <= pct <= 1.0
        # 平均成本应为正
        ac = result["平均成本"].iloc[0]
        assert not pd.isna(ac)
        assert ac > 0

    def test_multiple_bars_rising_prices(self):
        """上涨行情：获利比例应接近 1。"""
        prices = [10 + i * 0.5 for i in range(30)]  # 30 天上涨
        data = _kline_range(prices, tr=2.0)
        result = _calculate_chip_numpy(data)
        # 最后一天获利比例应接近 1（一直在涨）
        final_pct = result["获利比例"].iloc[-1]
        assert not pd.isna(final_pct)
        assert final_pct > 0.5  # 大部分筹码获利

    def test_multiple_bars_falling_prices(self):
        """下跌行情：获利比例应接近 0。"""
        base = 50.0
        prices = [base - i * 0.5 for i in range(30)]
        data = _kline_range(prices, tr=2.0)
        result = _calculate_chip_numpy(data)
        final_pct = result["获利比例"].iloc[-1]
        assert not pd.isna(final_pct)
        assert final_pct < 0.5  # 大部分筹码亏损

    def test_column_types(self):
        """输出列类型正确：获利比例/集中度为 float，日期为 datetime.date。"""
        data = _kline_range([10 + i * 0.2 for i in range(130)], tr=1.0)
        result = _calculate_chip_numpy(data)
        for col in ["获利比例", "平均成本", "90集中度", "70集中度"]:
            assert result[col].dtype in (np.float64, np.float32, float)

    def test_date_column_is_datetime_date(self):
        """日期列转换为 datetime.date 类型。"""
        data = [_kline(10.0, turnover=5.0, date="2024-06-15")]
        result = _calculate_chip_numpy(data)
        assert result["日期"].iloc[0] == pd.Timestamp("2024-06-15").date()

    def test_all_prices_identical_skips(self):
        """全部同价 K 线 → 跳过（maxprice-minprice < 1e-8）。"""
        data = [_kline(10.0, open=10.0, high=10.0, low=10.0, turnover=5.0)]
        result = _calculate_chip_numpy(data)
        # 由于 maxprice == minprice，continue 跳过，所有值为 NaN
        assert pd.isna(result["获利比例"].iloc[0])

    def test_large_dataset_120_bars(self):
        """120+ 根 K 线 → 滑窗正常工作，不崩溃。"""
        prices = [10 + i * 0.1 for i in range(150)]
        data = _kline_range(prices, tr=1.0)
        result = _calculate_chip_numpy(data)
        assert len(result) == 150
        assert result["获利比例"].notna().sum() > 0

    def test_concentration_between_0_and_1(self):
        """集中度应在 0-1 范围内。"""
        data = _kline_range([10 + i * 0.3 for i in range(130)], tr=2.0)
        result = _calculate_chip_numpy(data)
        valid = result.dropna()
        if not valid.empty:
            assert (valid["90集中度"] >= 0).all()
            assert (valid["70集中度"] >= 0).all()
            # 集中度不可能大于 1（除非 CPU 精度，但应 <= 1.0）
            assert (valid["90集中度"] <= 1.0).all()

    def test_turnover_rate_none_handled(self):
        """turnover_rate 为 None → 视为 0。"""
        data = [
            {"date": "2024-01-01", "open": 10.0, "close": 10.5,
             "high": 11.0, "low": 9.5, "volume": 10000, "turnover_rate": None},
        ]
        result = _calculate_chip_numpy(data)
        assert pd.isna(result["获利比例"].iloc[0])

    def test_turnover_rate_missing_key(self):
        """缺少 turnover_rate 键 → 视为 0。"""
        data = [
            {"date": "2024-01-01", "open": 10.0, "close": 10.5,
             "high": 11.0, "low": 9.5, "volume": 10000},
        ]
        result = _calculate_chip_numpy(data)
        assert pd.isna(result["获利比例"].iloc[0])


# ═══════════════════════════════════════════════════════════
# 2. 纯函数：_parse_kline_raw_em
# ═══════════════════════════════════════════════════════════


class TestParseKlineRawEm:
    """_parse_kline_raw_em EM CSV 解析测试。"""

    def test_parses_default_csv(self):
        """标准 11 列 CSV 行 → 正确解析。"""
        raw = [
            "2024-01-02,10.0,10.5,11.0,9.5,100000,1000000,2.0,5.0,0.5,3.0",
        ]
        records = _parse_kline_raw_em(raw)
        assert len(records) == 1
        r = records[0]
        assert r["date"] == "2024-01-02"
        assert r["open"] == 10.0
        assert r["close"] == 10.5
        assert r["high"] == 11.0
        assert r["low"] == 9.5
        assert r["volume"] == 100000
        assert r["turnover_rate"] == 3.0  # hsl → turnover_rate

    def test_multiple_lines(self):
        """多行 CSV → 全部解析。"""
        raw = [
            "2024-01-02,10.0,10.5,11.0,9.5,100000,1000000,2.0,5.0,0.5,3.0",
            "2024-01-03,10.5,11.0,11.5,10.0,200000,2000000,1.5,4.8,0.4,2.5",
        ]
        records = _parse_kline_raw_em(raw)
        assert len(records) == 2
        assert records[1]["date"] == "2024-01-03"
        assert records[1]["turnover_rate"] == 2.5

    def test_empty_list(self):
        """空列表 → 空列表。"""
        assert _parse_kline_raw_em([]) == []

    def test_missing_fields_default_zero(self):
        """字段不足 → 缺失字段默认为 0.0 / ''。"""
        raw = [
            "2024-01-02,10.0",  # 仅 2 列
        ]
        records = _parse_kline_raw_em(raw)
        assert len(records) == 1
        r = records[0]
        assert r["date"] == "2024-01-02"
        assert r["open"] == 10.0
        # 缺失字段默认 0.0 或空字符串
        for key in ("close", "high", "low", "volume"):
            assert r[key] == 0.0
        assert r["turnover_rate"] == 0.0

    def test_hsl_mapped_to_turnover_rate(self):
        """hsl 列映射到 turnover_rate。"""
        raw = [
            "2024-01-02,10.0,10.5,11.0,9.5,100000,1000000,2.0,5.0,0.5,5.5",
        ]
        records = _parse_kline_raw_em(raw)
        assert records[0]["turnover_rate"] == 5.5
        assert "hsl" not in records[0]  # hsl 已被 pop


# ═══════════════════════════════════════════════════════════
# 3. _js_code
# ═══════════════════════════════════════════════════════════


class TestJsCode:
    """_js_code 测试。"""

    def test_returns_non_empty_string(self):
        code = _js_code()
        assert isinstance(code, str)
        assert len(code) > 100

    def test_contains_cyq_calculator(self):
        assert "CYQCalculator" in _js_code()

    def test_contains_create_number_array(self):
        assert "createNumberArray" in _js_code()


# ═══════════════════════════════════════════════════════════
# 4. _get_js_runtime / _calc_one_day
# ═══════════════════════════════════════════════════════════


class TestGetJsRuntime:
    """_get_js_runtime 延迟初始化测试。"""

    def test_miniracer_import_fail_returns_none(self):
        """MiniRacer 导入失败 → 返回 None。"""
        with (
            patch.dict("sys.modules", {"py_mini_racer": None}),
            patch("core.stock_cyq_em.MiniRacer", None),
            patch("core.stock_cyq_em._JS_INITIALIZED", False),
        ):
            result = _get_js_runtime()
            assert result is None

    def test_js_runtime_fails_returns_none(self):
        """MiniRacer init 正常但 eval 失败 → 返回 None。"""
        def _raise(*args, **kwargs):
            raise RuntimeError("JS eval failed")

        mock_racer = MagicMock()
        mock_racer.eval.side_effect = _raise

        with patch("core.stock_cyq_em._JS_INITIALIZED", False), \
             patch("core.stock_cyq_em.MiniRacer", return_value=mock_racer):
            result = _get_js_runtime()
            assert result is None


class TestCalcOneDay:
    """_calc_one_day 测试。"""

    def test_js_runtime_none_raises(self):
        """JS runtime 为 None → 抛出 RuntimeError。"""
        with (
            patch("core.stock_cyq_em._get_js_runtime", return_value=None),
            pytest.raises(RuntimeError, match="MiniRacer 未初始化"),
        ):
            _calc_one_day([{"date": "2024-01-01", "open": 10.0, "close": 11.0, "high": 12.0, "low": 9.0, "volume": 1000}], 0)

    def test_calls_js_runtime(self):
        """正常路径 → 调用 js_env.call('CYQCalculator', ...)。"""
        mock_js = MagicMock()
        mock_js.call.return_value = {
            "bp": 0.5, "ac": "10.50", "c90l": "9.00", "c90h": "11.00",
            "cn90": 0.1, "c70l": "9.50", "c70h": "10.50", "cn70": 0.05,
        }
        with patch("core.stock_cyq_em._get_js_runtime", return_value=mock_js):
            kline = [{"date": "2024-01-01", "open": 10.0, "close": 11.0, "high": 12.0, "low": 9.0, "volume": 1000}]
            result = _calc_one_day(kline, 0)
            assert result["bp"] == 0.5
            mock_js.call.assert_called_once_with("CYQCalculator", 0, kline)


# ═══════════════════════════════════════════════════════════
# 5. _fetch_kline_em
# ═══════════════════════════════════════════════════════════


class TestFetchKlineEm:
    """_fetch_kline_em EM API 测试（mock curl_cffi）。"""

    def test_successful_fetch(self):
        """curl_get 成功返回有效 klines → 解析后返回。"""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "data": {
                "klines": [
                    "2024-01-02,10.0,10.5,11.0,9.5,100000,1000000,2.0,5.0,0.5,3.0",
                ]
            }
        }
        with patch("core.stock_cyq_em.curl_get", return_value=mock_response):
            result = _fetch_kline_em("000001")
            assert result is not None
            assert len(result) == 1
            assert result[0]["date"] == "2024-01-02"
            assert result[0]["turnover_rate"] == 3.0

    def test_response_missing_data(self):
        """响应中缺少 data 或 klines → 返回 None。"""
        mock_response = MagicMock()
        mock_response.json.return_value = {"data": {}}
        with patch("core.stock_cyq_em.curl_get", return_value=mock_response):
            assert _fetch_kline_em("000001") is None

    def test_response_no_data_key(self):
        """json 不含 data 键 → 返回 None。"""
        mock_response = MagicMock()
        mock_response.json.return_value = {"rc": -1, "rt": 1}
        with patch("core.stock_cyq_em.curl_get", return_value=mock_response):
            assert _fetch_kline_em("000001") is None

    def test_all_retries_fail(self):
        """所有重试均失败 → 返回 None。"""
        with patch("core.stock_cyq_em.curl_get", side_effect=ConnectionError("timeout")):
            result = _fetch_kline_em("000001")
            assert result is None

    def test_sh_market_code(self):
        """6 开头股票 → market_code = 1。"""
        mock_response = MagicMock()
        mock_response.json.return_value = {"data": {"klines": []}}
        with patch("core.stock_cyq_em.curl_get", return_value=mock_response) as mock_get:
            _fetch_kline_em("600000")
            # 验证 params 中 secid 以 1 开头
            call_kwargs = mock_get.call_args[1]
            assert "secid=1.600000" in str(call_kwargs["params"]) or \
                   call_kwargs["params"].get("secid") == "1.600000"

    def test_sz_market_code(self):
        """非 6 开头股票 → market_code = 0。"""
        mock_response = MagicMock()
        mock_response.json.return_value = {"data": {"klines": []}}
        with patch("core.stock_cyq_em.curl_get", return_value=mock_response) as mock_get:
            _fetch_kline_em("000001")
            call_kwargs = mock_get.call_args[1]
            assert "secid=0.000001" in str(call_kwargs["params"]) or \
                   call_kwargs["params"].get("secid") == "0.000001"


# ═══════════════════════════════════════════════════════════
# 6. _fetch_kline_xueqiu
# ═══════════════════════════════════════════════════════════


class TestFetchKlineXueqiu:
    """_fetch_kline_xueqiu 雪球 API 测试。"""

    def test_import_fail_returns_none(self):
        """smartmoney_hunter 未安装 → 返回 None。"""
        with patch.dict("sys.modules", {"smartmoney_hunter": None}):
            result = _fetch_kline_xueqiu("000001")
            assert result is None

    def test_api_fail_returns_none(self):
        """雪球 API 请求异常 → 返回 None。"""
        with patch("smartmoney_hunter.xueqiu.get_daily_bars", side_effect=RuntimeError("API error")):
            result = _fetch_kline_xueqiu("000001")
            assert result is None

    def test_empty_df_returns_none(self):
        """雪球返回空 DataFrame → 返回 None。"""
        with patch("smartmoney_hunter.xueqiu.get_daily_bars", return_value=pd.DataFrame()):
            result = _fetch_kline_xueqiu("000001")
            assert result is None

    def test_successful_fetch(self):
        """雪球返回有效数据 → 解析为 K 线列表。"""
        df = pd.DataFrame({
            "date": ["2024-01-02", "2024-01-03"],
            "open": [10.0, 10.5],
            "close": [10.5, 11.0],
            "high": [11.0, 11.5],
            "low": [9.5, 10.0],
            "volume": [100000, 200000],
            "turnover": [1.5, 2.0],
        })
        with patch("smartmoney_hunter.xueqiu.get_daily_bars", return_value=df):
            result = _fetch_kline_xueqiu("000001")
            assert result is not None
            assert len(result) == 2
            assert result[0]["date"] == "2024-01-02"
            assert result[0]["turnover_rate"] == 1.5


# ═══════════════════════════════════════════════════════════
# 7. _fetch_kline_sina
# ═══════════════════════════════════════════════════════════


class TestFetchKlineSina:
    """_fetch_kline_sina 新浪 API 测试（mock akshare）。"""

    def test_akshare_fail_returns_none(self):
        """akshare 请求异常 → 返回 None。"""
        with patch("akshare.stock_zh_a_daily", side_effect=RuntimeError("API error")):
            result = _fetch_kline_sina("000001")
            assert result is None

    def test_empty_df_returns_none(self):
        """akshare 返回空 DataFrame → 返回 None。"""
        with patch("akshare.stock_zh_a_daily", return_value=pd.DataFrame()):
            result = _fetch_kline_sina("000001")
            assert result is None

    def test_df_without_turnover_column(self):
        """DataFrame 缺少 turnover 列 → 返回 None。"""
        df = pd.DataFrame({
            "date": ["2024-01-02"],
            "open": [10.0],
            "close": [10.5],
        })
        with patch("akshare.stock_zh_a_daily", return_value=df):
            result = _fetch_kline_sina("000001")
            assert result is None

    def test_successful_fetch_with_turnover_percent(self):
        """新浪换手率已为百分数 → 不转换。"""
        df = pd.DataFrame({
            "date": ["2024-01-02"],
            "open": [10.0],
            "close": [10.5],
            "high": [11.0],
            "low": [9.5],
            "volume": [100000],
            "turnover": [3.5],  # 已经是百分数
        })
        with patch("akshare.stock_zh_a_daily", return_value=df):
            result = _fetch_kline_sina("000001")
            assert result is not None
            assert result[0]["turnover_rate"] == 3.5  # 保持原值

    def test_turnover_ratio_converted_to_percent(self):
        """新浪换手率为小数（<1.0）→ 转换为百分数。"""
        df = pd.DataFrame({
            "date": ["2024-01-02"],
            "open": [10.0],
            "close": [10.5],
            "high": [11.0],
            "low": [9.5],
            "volume": [100000],
            "turnover": [0.035],  # 小数比率
        })
        with patch("akshare.stock_zh_a_daily", return_value=df):
            result = _fetch_kline_sina("000001")
            assert result is not None
            assert result[0]["turnover_rate"] == pytest.approx(3.5, rel=1e-6)  # 0.035 * 100

    def test_sh_symbol_prefix(self):
        """6 开头 → sh 前缀。"""
        df = pd.DataFrame({
            "date": ["2024-01-02"], "open": [10.0], "close": [10.5],
            "high": [11.0], "low": [9.5], "volume": [100000],
            "turnover": [1.0],
        })
        with patch("akshare.stock_zh_a_daily", return_value=df) as mock_fn:
            _fetch_kline_sina("600000")
            mock_fn.assert_called_with(symbol="sh600000", adjust="")

    def test_sz_symbol_prefix(self):
        """0/3 开头 → sz 前缀。"""
        df = pd.DataFrame({
            "date": ["2024-01-02"], "open": [10.0], "close": [10.5],
            "high": [11.0], "low": [9.5], "volume": [100000],
            "turnover": [1.0],
        })
        with patch("akshare.stock_zh_a_daily", return_value=df) as mock_fn:
            _fetch_kline_sina("000001")
            mock_fn.assert_called_with(symbol="sz000001", adjust="")

    def test_bj_symbol_prefix(self):
        """8/4/920 开头 → bj 前缀。"""
        df = pd.DataFrame({
            "date": ["2024-01-02"], "open": [10.0], "close": [10.5],
            "high": [11.0], "low": [9.5], "volume": [100000],
            "turnover": [1.0],
        })
        with patch("akshare.stock_zh_a_daily", return_value=df) as mock_fn:
            _fetch_kline_sina("830000")
            mock_fn.assert_called_with(symbol="bj830000", adjust="")


# ═══════════════════════════════════════════════════════════
# 8. _fetch_kline_db
# ═══════════════════════════════════════════════════════════


class TestFetchKlineDb:
    """_fetch_kline_db 本地 DB 读取测试。"""

    def test_fetch_from_real_db(self, tmp_path: Path):
        """真实 SQLite 数据库 → 返回 K 线列表。"""
        db_path = tmp_path / "test.db"
        conn = __import__("sqlite3").connect(str(db_path))
        conn.execute(
            "CREATE TABLE daily_bars ("
            "  ts_code TEXT, trade_date TEXT, open REAL, close REAL, "
            "  high REAL, low REAL, volume REAL, turnover_rate REAL"
            ")"
        )
        conn.execute(
            "INSERT INTO daily_bars VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("000001", "2024-01-02", 10.0, 10.5, 11.0, 9.5, 100000, 3.0),
        )
        conn.execute(
            "INSERT INTO daily_bars VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("000001", "2024-01-03", 10.5, 11.0, 11.5, 10.0, 200000, 2.5),
        )
        conn.commit()
        conn.close()

        result = _fetch_kline_db("000001", str(db_path))
        assert result is not None
        assert len(result) == 2
        assert result[0]["date"] == "2024-01-02"
        assert result[0]["turnover_rate"] == 3.0
        assert result[1]["turnover_rate"] == 2.5

    def test_no_data_returns_none(self, tmp_path: Path):
        """查询无结果 → 返回 None。"""
        db_path = tmp_path / "empty.db"
        conn = __import__("sqlite3").connect(str(db_path))
        conn.execute(
            "CREATE TABLE daily_bars ("
            "  ts_code TEXT, trade_date TEXT, open REAL, close REAL, "
            "  high REAL, low REAL, volume REAL, turnover_rate REAL"
            ")"
        )
        conn.commit()
        conn.close()

        result = _fetch_kline_db("000001", str(db_path))
        assert result is None

    def test_db_not_found_returns_none(self):
        """数据库文件不存在 → 返回 None。"""
        result = _fetch_kline_db("000001", "/nonexistent/path/test.db")
        assert result is None

    def test_symbol_suffix_stripped(self, tmp_path: Path):
        """ts_code 后缀 (.SH/.SZ/.BJ) 被正确剥离。"""
        db_path = tmp_path / "test_suffix.db"
        try:
            conn = __import__("sqlite3").connect(str(db_path))
            conn.execute(
                "CREATE TABLE daily_bars ("
                "  ts_code TEXT, trade_date TEXT, open REAL, close REAL, "
                "  high REAL, low REAL, volume REAL, turnover_rate REAL"
                ")"
            )
            conn.execute(
                "INSERT INTO daily_bars VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                ("000001", "2024-01-02", 10.0, 10.5, 11.0, 9.5, 100000, 3.0),
            )
            conn.commit()
            conn.close()
            # 用 .SZ 后缀查询
            result = _fetch_kline_db("000001.SZ", str(db_path))
            assert result is not None
            assert len(result) == 1
        finally:
            if db_path.exists():
                db_path.unlink()

    def test_null_turnover_rate_defaults_zero(self, tmp_path: Path):
        """turnover_rate 为 NULL → 默认为 0.0。"""
        db_path = tmp_path / "test_null.db"
        conn = __import__("sqlite3").connect(str(db_path))
        conn.execute(
            "CREATE TABLE daily_bars ("
            "  ts_code TEXT, trade_date TEXT, open REAL, close REAL, "
            "  high REAL, low REAL, volume REAL, turnover_rate REAL"
            ")"
        )
        conn.execute(
            "INSERT INTO daily_bars VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("000001", "2024-01-02", 10.0, 10.5, 11.0, 9.5, 100000, None),
        )
        conn.commit()
        conn.close()

        result = _fetch_kline_db("000001", str(db_path))
        assert result is not None
        assert result[0]["turnover_rate"] == 0.0


# ═══════════════════════════════════════════════════════════
# 9. stock_cyq_em 主级联
# ═══════════════════════════════════════════════════════════


class TestStockCyqEm:
    """stock_cyq_em 五源级联测试。"""

    def test_db_numpy_path(self, tmp_path: Path):
        """本地 DB → numpy 路径产生有效结果。"""
        db_path = tmp_path / "test.db"
        conn = __import__("sqlite3").connect(str(db_path))
        conn.execute(
            "CREATE TABLE daily_bars ("
            "  ts_code TEXT, trade_date TEXT, open REAL, close REAL, "
            "  high REAL, low REAL, volume REAL, turnover_rate REAL"
            ")"
        )
        # 插入 130 条 K 线（>120 满足 numpy 路径）
        for i in range(130):
            close = 10.0 + i * 0.1
            conn.execute(
                "INSERT INTO daily_bars VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                ("000001", f"2024-01-{i+1:02d}", close * 0.99, close,
                 close * 1.02, close * 0.97, 100000 + i * 100, 1.0),
            )
        conn.commit()
        conn.close()

        # mock EM 返回 None 以触发 DB 路径
        with patch("core.stock_cyq_em._fetch_kline_em", return_value=None):
            result = stock_cyq_em("000001", use_local_db=True, db_path=db_path)
        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        assert "获利比例" in result.columns
        assert "平均成本" in result.columns
        assert len(result) <= 90  # 最多最近 90 天

    def test_em_path(self):
        """EM API 路径产生有效结果。"""
        mock_kline = [
            {"date": f"2024-01-{i+1:02d}", "open": 10.0 + i * 0.1,
             "close": 10.5 + i * 0.1, "high": 11.0 + i * 0.1,
             "low": 9.5 + i * 0.1, "volume": 100000, "turnover_rate": 1.0}
            for i in range(130)
        ]
        with patch("core.stock_cyq_em._fetch_kline_em", return_value=mock_kline), \
             patch("core.stock_cyq_em._get_js_runtime") as mock_js:
            mock_js.return_value = MagicMock()
            mock_js.return_value.call.return_value = {
                "bp": 0.5, "ac": "12.0", "c90l": "10.0", "c90h": "14.0",
                "cn90": 0.15, "c70l": "11.0", "c70h": "13.0", "cn70": 0.08,
            }
            result = stock_cyq_em("000001", use_local_db=False)
        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        assert len(result) <= 90

    def test_fallback_to_db_js_when_numpy_invalid(self, tmp_path: Path):
        """换手率全 0 → 拒绝计算（防全零伪数据），不再降级 JS。"""
        db_path = tmp_path / "test.db"
        conn = __import__("sqlite3").connect(str(db_path))
        conn.execute(
            "CREATE TABLE daily_bars ("
            "  ts_code TEXT, trade_date TEXT, open REAL, close REAL, "
            "  high REAL, low REAL, volume REAL, turnover_rate REAL"
            ")"
        )
        # 插入数据但 turnover_rate=0 → numpy 结果 NaN → 触发 JS 降级
        for i in range(130):
            close = 10.0 + i * 0.1
            conn.execute(
                "INSERT INTO daily_bars VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                ("000001", f"2024-01-{i+1:02d}", close * 0.99, close,
                 close * 1.02, close * 0.97, 100000 + i * 100, 0.0),
            )
        conn.commit()
        conn.close()

        with patch("core.stock_cyq_em._fetch_kline_em", return_value=None), \
             patch("core.stock_cyq_em._fetch_kline_xueqiu", return_value=None), \
             patch("core.stock_cyq_em._fetch_kline_sina", return_value=None):
            # 2026-07 全零筹码事故修复后：换手率全缺失时必须显式报错，
            # 而非降级 JS 静默产出全 0 行
            with pytest.raises(ValueError, match="换手率"):
                stock_cyq_em("000001", use_local_db=True, db_path=db_path)

    def test_xueqiu_path(self):
        """雪球 API 路径 → JS 计算。"""
        mock_xueqiu = [
            {"date": f"2024-01-{i+1:02d}", "open": 10.0 + i * 0.1,
             "close": 10.5 + i * 0.1, "high": 11.0 + i * 0.1,
             "low": 9.5 + i * 0.1, "volume": 100000, "turnover_rate": 1.0}
            for i in range(130)
        ]
        with patch("core.stock_cyq_em._fetch_kline_em", return_value=None), \
             patch("core.stock_cyq_em._fetch_kline_db", return_value=None), \
             patch("core.stock_cyq_em._fetch_kline_xueqiu", return_value=mock_xueqiu), \
             patch("core.stock_cyq_em._get_js_runtime") as mock_js:
            mock_js.return_value = MagicMock()
            mock_js.return_value.call.return_value = {
                "bp": 0.5, "ac": "12.0", "c90l": "10.0", "c90h": "14.0",
                "cn90": 0.15, "c70l": "11.0", "c70h": "13.0", "cn70": 0.08,
            }
            result = stock_cyq_em("000001")
        assert isinstance(result, pd.DataFrame)
        assert not result.empty

    def test_sina_path(self):
        """新浪 API 路径 → JS 计算。"""
        mock_sina = [
            {"date": f"2024-01-{i+1:02d}", "open": 10.0 + i * 0.1,
             "close": 10.5 + i * 0.1, "high": 11.0 + i * 0.1,
             "low": 9.5 + i * 0.1, "volume": 100000, "turnover_rate": 1.0}
            for i in range(130)
        ]
        with patch("core.stock_cyq_em._fetch_kline_em", return_value=None), \
             patch("core.stock_cyq_em._fetch_kline_db", return_value=None), \
             patch("core.stock_cyq_em._fetch_kline_xueqiu", return_value=None), \
             patch("core.stock_cyq_em._fetch_kline_sina", return_value=mock_sina), \
             patch("core.stock_cyq_em._get_js_runtime") as mock_js:
            mock_js.return_value = MagicMock()
            mock_js.return_value.call.return_value = {
                "bp": 0.5, "ac": "12.0", "c90l": "10.0", "c90h": "14.0",
                "cn90": 0.15, "c70l": "11.0", "c70h": "13.0", "cn70": 0.08,
            }
            result = stock_cyq_em("000001")
        assert isinstance(result, pd.DataFrame)
        assert not result.empty

    def test_all_sources_fail_raises_connection_error(self):
        """全部数据源失败 → 抛出 ConnectionError。"""
        with (
            patch("core.stock_cyq_em._fetch_kline_em", return_value=None),
            patch("core.stock_cyq_em._fetch_kline_db", return_value=None),
            patch("core.stock_cyq_em._fetch_kline_xueqiu", return_value=None),
            patch("core.stock_cyq_em._fetch_kline_sina", return_value=None),
            pytest.raises(ConnectionError, match="均无法获取"),
        ):
            stock_cyq_em("000001", use_local_db=True)

    def test_symbol_suffix_stripped(self):
        """symbol 带 .SZ/.SH 后缀 → 正确剥离传给级联。"""
        with (
            patch("core.stock_cyq_em._fetch_kline_em", return_value=None) as mock_em,
            patch("core.stock_cyq_em._fetch_kline_db", return_value=None),
            patch("core.stock_cyq_em._fetch_kline_xueqiu", return_value=None),
            patch("core.stock_cyq_em._fetch_kline_sina", return_value=None),
            pytest.raises(ConnectionError),
        ):
            stock_cyq_em("000001.SZ", use_local_db=False)
            # 验证传给 _fetch_kline_em 的是纯代码
            call_ts_code = mock_em.call_args[0][0]
            assert call_ts_code == "000001"

    def test_adjust_param_passed_to_em(self):
        """adjust 参数仅影响 EM API。"""
        mock_kline = [
            {"date": "2024-01-01", "open": 10.0, "close": 10.5,
             "high": 11.0, "low": 9.5, "volume": 100000, "turnover_rate": 1.0}
        ]
        with patch("core.stock_cyq_em._fetch_kline_em", return_value=mock_kline) as mock_em, \
             patch("core.stock_cyq_em._get_js_runtime") as mock_js:
            mock_js.return_value = MagicMock()
            mock_js.return_value.call.return_value = {
                "bp": 0.5, "ac": "10.0", "c90l": "9.0", "c90h": "11.0",
                "cn90": 0.1, "c70l": "9.5", "c70h": "10.5", "cn70": 0.05,
            }
            stock_cyq_em("000001", adjust="qfq")
            assert mock_em.call_args[1]["adjust"] == "qfq"

    def test_use_local_db_false_skips_db_paths(self):
        """use_local_db=False → 不尝试 DB 路径。"""
        with patch("core.stock_cyq_em._fetch_kline_em", return_value=None), \
             patch("core.stock_cyq_em._fetch_kline_db", return_value=None) as mock_db, \
             patch("core.stock_cyq_em._fetch_kline_xueqiu", return_value=None), \
             patch("core.stock_cyq_em._fetch_kline_sina", return_value=None):
            with pytest.raises(ConnectionError):
                stock_cyq_em("000001", use_local_db=False)
            # DB 不应被调用
            mock_db.assert_not_called()

    def test_return_last_90_days(self):
        """结果仅含最近 90 天。"""
        mock_sina = [
            {"date": f"2024-{m:02d}-{d:02d}", "open": 10.0, "close": 10.5,
             "high": 11.0, "low": 9.5, "volume": 100000, "turnover_rate": 1.0}
            for m in range(1, 13) for d in [1, 15]  # 24 条
        ]
        with patch("core.stock_cyq_em._fetch_kline_em", return_value=None), \
             patch("core.stock_cyq_em._fetch_kline_db", return_value=None), \
             patch("core.stock_cyq_em._fetch_kline_xueqiu", return_value=None), \
             patch("core.stock_cyq_em._fetch_kline_sina", return_value=mock_sina), \
             patch("core.stock_cyq_em._get_js_runtime") as mock_js:
            mock_js.return_value = MagicMock()
            mock_js.return_value.call.return_value = {
                "bp": 0.5, "ac": "10.0", "c90l": "9.0", "c90h": "11.0",
                "cn90": 0.1, "c70l": "9.5", "c70h": "10.5", "cn70": 0.05,
            }
            result = stock_cyq_em("000001")
        assert len(result) <= 90
        assert len(result) == len(mock_sina)  # 不足 90 则全返
