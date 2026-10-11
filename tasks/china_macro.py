"""
中国宏观经济数据更新任务
─────────────────────
CPI/PPI/PMI/货币供应量/社零/固投/进出口/工业增加值/LPR/SHIBOR等
"""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any

import pandas as pd

from core.utils import to_float as _to_float
from interface import DatabaseInterface

try:
    import akshare as ak
except ImportError:
    ak = None

logger = logging.getLogger(__name__)


# ===========================================================================
# 辅助函数
# ===========================================================================


_QUARTER_CN = {
    "一": "1",
    "二": "2",
    "三": "3",
    "四": "4",
    "第一": "1",
    "第二": "2",
    "第三": "3",
    "第四": "4",
}

# 累计口径："2026年第1-2季度" / "2026年1-2季度"。
# 必须排在序数口径之前，且季度号取**末位**数字——源端写的是「第1-N季度」，
# 旧实现的退化正则抓到「第」后的第一个数字（恒为 1），四个季度全部塌成 Q1。
_CUMULATIVE_QUARTER = re.compile(r"(\d{4})\D*1\s*[-~至]\s*(\d)\s*季度")
_ORDINAL_QUARTER = re.compile(r"(\d{4}).*?(第一|第二|第三|第四|一|二|三|四)\s*季度")
_NUMERIC_QUARTER = re.compile(r"(\d{4})\D*(\d)")


def _parse_quarter(q_str: str) -> str | None:
    """把源端的季度写法归一化为 ``YYYY-Qn``。

    三种口径都要认：累计（``第1-2季度``）、序数（``第一季度``）、数字（``2024-Q1``）。
    """
    if not q_str:
        return None
    m = _CUMULATIVE_QUARTER.search(q_str)
    if m:
        return f"{m.group(1)}-Q{m.group(2)}"
    m = _ORDINAL_QUARTER.search(q_str)
    if m:
        return f"{m.group(1)}-Q{_QUARTER_CN[m.group(2)]}"
    m = _NUMERIC_QUARTER.search(q_str)
    if m:
        return f"{m.group(1)}-Q{m.group(2)}"
    return None


# ===========================================================================
# 月度指标
# ===========================================================================


def _parse_month_col(df: pd.DataFrame) -> pd.DataFrame:
    """将 '月份' 列拆分为 'year' 和 'month' 列（兼容 '2024年01月' 格式）。"""
    if "月份" not in df.columns:
        return df
    parts = df["月份"].astype(str).str.extract(r"(\d{4})[年.]?(\d{1,2})")
    df["year"] = parts[0]
    df["month"] = parts[1].str.zfill(2)
    return df


def _fetch_cpi() -> list[dict]:
    """获取 CPI 数据。"""
    if ak is None:
        return []
    try:
        df = ak.macro_china_cpi()
        if df is None or df.empty:
            return []
        df = _parse_month_col(df)
        col_map = {
            "全国-同比增长": "cpi_yoy",
            "全国-环比增长": "cpi_mom",
        }
        df = df.rename(columns=col_map)
        records = []
        for _, row in df.iterrows():
            year = str(row.get("year", "")).strip()
            month = str(row.get("month", "")).strip()
            if year and month:
                records.append({
                    "date": f"{year}-{month}-01",
                    "cpi_yoy": _to_float(row.get("cpi_yoy")),
                    "cpi_mom": _to_float(row.get("cpi_mom")),
                })
        return records
    except Exception as e:
        logger.warning(f"CPI 获取失败: {e}")
        return []


def _fetch_ppi() -> list[dict]:
    """获取 PPI 数据。"""
    if ak is None:
        return []
    try:
        df = ak.macro_china_ppi()
        if df is None or df.empty:
            return []
        df = _parse_month_col(df)
        col_map = {
            "当月": "ppi_current",
            "当月同比增长": "ppi_yoy",
        }
        df = df.rename(columns=col_map)
        records = []
        for _, row in df.iterrows():
            year = str(row.get("year", "")).strip()
            month = str(row.get("month", "")).strip()
            if year and month:
                records.append({
                    "date": f"{year}-{month}-01",
                    "ppi_yoy": _to_float(row.get("ppi_yoy")),
                    "ppi_current": _to_float(row.get("ppi_current")),
                })
        return records
    except Exception as e:
        logger.warning(f"PPI 获取失败: {e}")
        return []


def _fetch_pmi() -> list[dict]:
    """获取制造业 PMI 数据。"""
    if ak is None:
        return []
    try:
        df = ak.macro_china_pmi()
        if df is None or df.empty:
            return []
        df = _parse_month_col(df)
        col_map = {
            "制造业-指数": "pmi",
            "制造业-同比增长": "pmi_yoy",
        }
        df = df.rename(columns=col_map)
        records = []
        for _, row in df.iterrows():
            year = str(row.get("year", "")).strip()
            month = str(row.get("month", "")).strip()
            if year and month:
                records.append({
                    "date": f"{year}-{month}-01",
                    "pmi": _to_float(row.get("pmi")),
                    "pmi_yoy": _to_float(row.get("pmi_yoy")),
                })
        return records
    except Exception as e:
        logger.warning(f"PMI 获取失败: {e}")
        return []


def _fetch_caixin_pmi() -> list[dict]:
    """获取财新 PMI 数据。"""
    if ak is None:
        return []
    try:
        df = ak.macro_china_cx_pmi_yearly()
        if df is None or df.empty:
            return []
        # Format: ['商品', '日期', '今值', '预测值', '前值']
        col_map = {
            "日期": "date_col",
            "今值": "pmi_caixin",
        }
        df = df.rename(columns=col_map)
        records = []
        for _, row in df.iterrows():
            date_val = row.get("date_col")
            if date_val is None:
                continue
            date_str = str(date_val).strip()[:10]
            if date_str:
                records.append({
                    "date": date_str,
                    "pmi_caixin": _to_float(row.get("pmi_caixin")),
                })
        return records
    except Exception as e:
        logger.warning(f"财新PMI 获取失败: {e}")
        return []


def _fetch_money_supply() -> list[dict]:
    """获取货币供应量（M0/M1/M2）数据。"""
    if ak is None:
        return []
    try:
        df = ak.macro_china_money_supply()
        if df is None or df.empty:
            return []
        df = _parse_month_col(df)
        col_map = {
            "货币和准货币(M2)-数量(亿元)": "m2",
            "货币和准货币(M2)-同比增长": "m2_yoy",
            "货币(M1)-数量(亿元)": "m1",
            "货币(M1)-同比增长": "m1_yoy",
            "流通中的现金(M0)-数量(亿元)": "m0",
            "流通中的现金(M0)-同比增长": "m0_yoy",
        }
        df = df.rename(columns=col_map)
        records = []
        for _, row in df.iterrows():
            year = str(row.get("year", "")).strip()
            month = str(row.get("month", "")).strip()
            if year and month:
                records.append({
                    "date": f"{year}-{month}-01",
                    "m0": _to_float(row.get("m0")),
                    "m1": _to_float(row.get("m1")),
                    "m2": _to_float(row.get("m2")),
                    "m0_yoy": _to_float(row.get("m0_yoy")),
                    "m1_yoy": _to_float(row.get("m1_yoy")),
                    "m2_yoy": _to_float(row.get("m2_yoy")),
                })
        return records
    except Exception as e:
        logger.warning(f"货币供应量 获取失败: {e}")
        return []


def _fetch_new_financial_credit() -> list[dict]:
    """获取新增贷款数据。"""
    if ak is None:
        return []
    try:
        df = ak.macro_china_new_financial_credit()
        if df is None or df.empty:
            return []
        df = _parse_month_col(df)
        col_map = {
            "当月": "new_loans",
            "当月-同比增长": "new_loans_yoy",
        }
        df = df.rename(columns=col_map)
        records = []
        for _, row in df.iterrows():
            year = str(row.get("year", "")).strip()
            month = str(row.get("month", "")).strip()
            if year and month:
                records.append({
                    "date": f"{year}-{month}-01",
                    "new_loans": _to_float(row.get("new_loans")),
                    "new_loans_yoy": _to_float(row.get("new_loans_yoy")),
                })
        return records
    except Exception as e:
        logger.warning(f"新增贷款 获取失败: {e}")
        return []


def _fetch_retail_sales() -> list[dict]:
    """获取社会消费品零售总额数据。"""
    if ak is None:
        return []
    try:
        df = ak.macro_china_consumer_goods_retail()
        if df is None or df.empty:
            return []
        df = _parse_month_col(df)
        col_map = {
            "同比增长": "retail_sales_yoy",
            "累计-同比增长": "retail_sales_ytd_yoy",
        }
        df = df.rename(columns=col_map)
        records = []
        for _, row in df.iterrows():
            year = str(row.get("year", "")).strip()
            month = str(row.get("month", "")).strip()
            if year and month:
                records.append({
                    "date": f"{year}-{month}-01",
                    "retail_sales_yoy": _to_float(row.get("retail_sales_yoy")),
                    "retail_sales_ytd_yoy": _to_float(row.get("retail_sales_ytd_yoy")),
                })
        return records
    except Exception as e:
        logger.warning(f"社会零售 获取失败: {e}")
        return []


def _fetch_fixed_asset_investment() -> list[dict]:
    """获取固定资产投资数据。"""
    if ak is None:
        return []
    try:
        df = ak.macro_china_gdzctz()
        if df is None or df.empty:
            return []
        df = _parse_month_col(df)
        col_map = {
            "同比增长": "fixed_asset_investment_yoy",
            "自年初累计": "fixed_asset_investment_ytd_yoy",
        }
        df = df.rename(columns=col_map)
        records = []
        for _, row in df.iterrows():
            year = str(row.get("year", "")).strip()
            month = str(row.get("month", "")).strip()
            if year and month:
                records.append({
                    "date": f"{year}-{month}-01",
                    "fixed_asset_investment_yoy": _to_float(row.get("fixed_asset_investment_yoy")),
                    "fixed_asset_investment_ytd_yoy": _to_float(row.get("fixed_asset_investment_ytd_yoy")),
                })
        return records
    except Exception as e:
        logger.warning(f"固投 获取失败: {e}")
        return []


def _fetch_trade() -> list[dict]:
    """获取进出口贸易数据。"""
    if ak is None:
        return []
    try:
        df = ak.macro_china_hgjck()
        if df is None or df.empty:
            return []
        df = _parse_month_col(df)
        col_map = {
            "当月出口额-金额": "export_value",
            "当月出口额-同比增长": "export_yoy",
            "当月进口额-金额": "import_value",
            "当月进口额-同比增长": "import_yoy",
        }
        df = df.rename(columns=col_map)
        records = []
        for _, row in df.iterrows():
            year = str(row.get("year", "")).strip()
            month = str(row.get("month", "")).strip()
            if year and month:
                records.append({
                    "date": f"{year}-{month}-01",
                    "export_value": _to_float(row.get("export_value")),
                    "export_yoy": _to_float(row.get("export_yoy")),
                    "import_value": _to_float(row.get("import_value")),
                    "import_yoy": _to_float(row.get("import_yoy")),
                })
        return records
    except Exception as e:
        logger.warning(f"进出口 获取失败: {e}")
        return []


def _fetch_industrial_production() -> list[dict]:
    """获取工业增加值数据。"""
    if ak is None:
        return []
    try:
        df = ak.macro_china_industrial_production_yoy()
        if df is None or df.empty:
            return []
        col_map = {
            "日期": "date_col",
            "今值": "industrial_production_yoy",
        }
        df = df.rename(columns=col_map)
        records = []
        for _, row in df.iterrows():
            date_val = row.get("date_col")
            if date_val is None:
                continue
            date_str = str(date_val).strip()[:10]
            if date_str:
                records.append({
                    "date": date_str,
                    "industrial_production_yoy": _to_float(row.get("industrial_production_yoy")),
                })
        return records
    except Exception as e:
        logger.warning(f"工业增加值 获取失败: {e}")
        return []


def _fetch_electricity_consumption() -> list[dict]:
    """获取全社会用电量数据。"""
    if ak is None:
        return []
    try:
        df = ak.macro_china_society_electricity()
        if df is None or df.empty:
            return []
        # 统计时间格式为 "2003.12"
        parts = df["统计时间"].astype(str).str.extract(r"(\d{4})\.(\d{1,2})")
        df["year"] = parts[0]
        df["month"] = parts[1].str.zfill(2)
        col_map = {
            "全社会用电量": "electricity_consumption_total",
            "全社会用电量同比": "electricity_consumption_yoy",
        }
        df = df.rename(columns=col_map)
        records = []
        for _, row in df.iterrows():
            year = str(row.get("year", "")).strip()
            month = str(row.get("month", "")).strip()
            if year and month:
                records.append({
                    "date": f"{year}-{month}-01",
                    "electricity_consumption_yoy": _to_float(row.get("electricity_consumption_yoy")),
                    "electricity_consumption_total": _to_float(row.get("electricity_consumption_total")),
                })
        return records
    except Exception as e:
        logger.warning(f"用电量 获取失败: {e}")
        return []


def _fetch_enterprise_goods_price() -> list[dict]:
    """获取企业商品价格指数（CGPI）数据。"""
    if ak is None:
        return []
    try:
        df = ak.macro_china_qyspjg()
        if df is None or df.empty:
            return []
        df = _parse_month_col(df)
        col_map = {
            "总指数-指数值": "enterprise_goods_price",
            "总指数-同比增长": "enterprise_goods_price_yoy",
            "总指数-环比增长": "enterprise_goods_price_mom",
        }
        df = df.rename(columns=col_map)
        records = []
        for _, row in df.iterrows():
            year = str(row.get("year", "")).strip()
            month = str(row.get("month", "")).strip()
            if year and month:
                records.append({
                    "date": f"{year}-{month}-01",
                    "enterprise_goods_price": _to_float(row.get("enterprise_goods_price")),
                    "enterprise_goods_price_yoy": _to_float(row.get("enterprise_goods_price_yoy")),
                    "enterprise_goods_price_mom": _to_float(row.get("enterprise_goods_price_mom")),
                })
        return records
    except Exception as e:
        logger.warning(f"企业商品价格 获取失败: {e}")
        return []


def _fetch_consumer_confidence() -> list[dict]:
    """获取消费者信心指数数据。"""
    if ak is None:
        return []
    try:
        df = ak.macro_china_xfzxx()
        if df is None or df.empty:
            return []
        df = _parse_month_col(df)
        col_map = {
            "消费者信心指数-指数值": "consumer_confidence",
            "消费者满意指数-指数值": "consumer_satisfaction",
            "消费者预期指数-指数值": "consumer_expectation",
        }
        df = df.rename(columns=col_map)
        records = []
        for _, row in df.iterrows():
            year = str(row.get("year", "")).strip()
            month = str(row.get("month", "")).strip()
            if year and month:
                records.append({
                    "date": f"{year}-{month}-01",
                    "consumer_confidence": _to_float(row.get("consumer_confidence")),
                    "consumer_satisfaction": _to_float(row.get("consumer_satisfaction")),
                    "consumer_expectation": _to_float(row.get("consumer_expectation")),
                })
        return records
    except Exception as e:
        logger.warning(f"消费者信心 获取失败: {e}")
        return []


def _fetch_lpr() -> list[dict]:
    """获取贷款市场报价利率（LPR）数据。"""
    if ak is None:
        return []
    try:
        df = ak.macro_china_lpr()
        if df is None or df.empty:
            return []
        col_map = {
            "TRADE_DATE": "date",
            "LPR1Y": "lpr_1y",
            "LPR5Y": "lpr_5y",
        }
        df = df.rename(columns=col_map)
        records = []
        for _, row in df.iterrows():
            date_val = row.get("date")
            if date_val is None:
                continue
            date_str = str(date_val).strip()[:10]
            if date_str:
                records.append({
                    "date": date_str,
                    "lpr_1y": _to_float(row.get("lpr_1y")),
                    "lpr_5y": _to_float(row.get("lpr_5y")),
                })
        return records
    except Exception as e:
        logger.warning(f"LPR 获取失败: {e}")
        return []


# ===========================================================================
# 季度指标
# ===========================================================================


# 源端列名 → 本表列名。每个字段给一组候选，按顺序取第一个**真实存在**的列。
#
# akshare 现行的 `macro_china_gdp` 用的是「国内生产总值-绝对值」/「…-同比增长」
# 这一组带后缀的名字（2026-10-11 实测）。旧 `col_map` 只列了无后缀的写法，
# 与实际列名**零重叠**，`row.get("gdp")` 恒为 None —— macro_quarterly 因此
# 21 行全是 NULL，而任务照报 success。
_GDP_COLUMN_CANDIDATES: dict[str, tuple[str, ...]] = {
    "gdp": ("国内生产总值-绝对值", "国内生产总值", "国内生产总值（亿元）", "GDP"),
    "gdp_yoy": ("国内生产总值-同比增长", "同比增长", "GDP同比增长"),
    "gdp_primary": ("第一产业-绝对值", "第一产业", "第一产业（亿元）"),
    "gdp_secondary": ("第二产业-绝对值", "第二产业", "第二产业（亿元）"),
    "gdp_tertiary": ("第三产业-绝对值", "第三产业", "第三产业（亿元）"),
}

# 源端给的是**年内累计值**（第1-2季度 = 1-6 月累计），本表存**单季值**，
# 故这四个字段要差分。`gdp_yoy` 是累计口径的同比，不差分。
_GDP_CUMULATIVE_FIELDS = ("gdp", "gdp_primary", "gdp_secondary", "gdp_tertiary")


def _resolve_gdp_columns(df: pd.DataFrame) -> dict[str, str]:
    """返回 ``{本表字段: 源端列名}``，只含源端真实存在的列。"""
    return {
        field: next((c for c in candidates if c in df.columns), "")
        for field, candidates in _GDP_COLUMN_CANDIDATES.items()
    }


def _to_single_quarter(records: list[dict]) -> list[dict]:
    """把年内累计值还原成单季值。

    Q1 的累计值即单季值；Q2..Q4 由 ``累计(q) - 累计(q-1)`` 得到。**缺前序
    季度的行整行丢弃** —— 差不出单季值时宁可少一行，也不能落一个「看着像
    单季、实则是年初至今累计」的值，那正是本 bug 当初在库里留下的形态。
    """
    by_quarter = {r["date"]: r for r in records}
    out: list[dict] = []
    for quarter in sorted(by_quarter):
        row = dict(by_quarter[quarter])
        q_num = int(quarter.rsplit("Q", 1)[1])
        if q_num == 1:
            out.append(row)
            continue
        prev = by_quarter.get(f"{quarter.rsplit('-', 1)[0]}-Q{q_num - 1}")
        if prev is None:
            logger.warning(f"⚠️ {quarter} 缺前序季度累计值，该季度单季值不可还原，跳过")
            continue
        for field in _GDP_CUMULATIVE_FIELDS:
            cur_val, prev_val = row.get(field), prev.get(field)
            row[field] = (
                None if cur_val is None or prev_val is None else cur_val - prev_val
            )
        out.append(row)
    return out


def _fetch_gdp() -> list[dict]:
    """获取 GDP 数据（年内累计口径转换为单季值）。"""
    if ak is None:
        return []
    try:
        df = ak.macro_china_gdp()
        if df is None or df.empty:
            return []
        if "季度" not in df.columns:
            logger.warning("⚠️ GDP 数据无季度列，跳过")
            return []

        resolved = _resolve_gdp_columns(df)
        if not resolved.get("gdp"):
            # 一个字段都没对上 = 源端改列名了。返回空让调用方报 no_data，
            # 而不是落一批全 NULL 的行再报 success（AGENTS.md 硬规则 #4）。
            logger.warning(
                f"⚠️ GDP 数据列名全部无法识别（实际列: {list(df.columns)}），"
                "跳过以免写入全空行"
            )
            return []

        records = []
        for _, row in df.iterrows():
            date = _parse_quarter(str(row.get("季度", "")).strip())
            if not date:
                continue
            records.append(
                {
                    "date": date,
                    **{
                        field: _to_float(row.get(col)) if col else None
                        for field, col in resolved.items()
                    },
                }
            )
        return _to_single_quarter(records)
    except Exception as e:
        logger.warning(f"GDP 获取失败: {e}")
        return []


# ===========================================================================
# 主更新函数
# ===========================================================================


def update_china_macro(db: DatabaseInterface) -> dict:
    """获取全部中国宏观经济数据并保存。"""
    logger.info("\n" + "=" * 60)
    logger.info("🏛️ 任务: 更新中国宏观经济数据")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "error": "akshare not installed"}

    results: dict[str, Any] = {}
    data_date = datetime.now().strftime("%Y-%m-%d")

    # --- Monthly indicators: merge by date ---
    monthly_merge: dict[str, dict] = {}
    monthly_calls = [
        ("CPI", _fetch_cpi),
        ("PPI", _fetch_ppi),
        ("PMI", _fetch_pmi),
        ("财新PMI", _fetch_caixin_pmi),
        ("货币供应量", _fetch_money_supply),
        ("新增贷款", _fetch_new_financial_credit),
        ("社会零售", _fetch_retail_sales),
        ("固投", _fetch_fixed_asset_investment),
        ("进出口", _fetch_trade),
        ("工业增加值", _fetch_industrial_production),
        ("用电量", _fetch_electricity_consumption),
        ("企业商品价格", _fetch_enterprise_goods_price),
        ("消费者信心", _fetch_consumer_confidence),
        ("LPR", _fetch_lpr),
    ]
    for name, fn in monthly_calls:
        try:
            records = fn()
            for r in records:
                d = r.pop("date")
                if d not in monthly_merge:
                    monthly_merge[d] = {"date": d}
                monthly_merge[d].update(r)
                monthly_merge[d]["data_date"] = data_date
            results[name] = len(records)
        except Exception as e:
            logger.warning(f"⚠️ {name} 获取失败: {e}")
            results[name] = f"error: {e}"

    if monthly_merge:
        monthly_records = list(monthly_merge.values())
        saved = db.save_macro_monthly_batch(monthly_records)
        logger.info(f"✅ 月度宏观数据保存完成: {saved} 条 / {len(monthly_records)} 个月")
    else:
        saved = 0
    results["monthly_saved"] = saved

    # --- Quarterly ---
    try:
        quarterly = _fetch_gdp()
        if quarterly:
            for r in quarterly:
                r["data_date"] = data_date
            saved_q = db.save_macro_quarterly_batch(quarterly)
            logger.info(f"✅ 季度宏观(GDP)保存完成: {saved_q} 条")
        else:
            saved_q = 0
            logger.warning("⚠️ GDP 无数据")
    except Exception as e:
        saved_q = 0
        logger.warning(f"⚠️ GDP 获取失败: {e}")
        results["error_kind"] = "network"
    results["quarterly_saved"] = saved_q

    # If we got here without an exception, the task ran to completion.
    # Decide status based on whether any data was actually saved.
    monthly_saved = results.get("monthly_saved", 0)
    quarterly_saved = results.get("quarterly_saved", 0)
    results["saved"] = monthly_saved + quarterly_saved
    if results["saved"] == 0:
        results["status"] = "no_data"
    else:
        results["status"] = "success"

    return dict(results)
