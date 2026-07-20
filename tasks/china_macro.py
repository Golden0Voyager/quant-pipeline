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

from interface import DatabaseInterface

try:
    import akshare as ak
except ImportError:
    ak = None

logger = logging.getLogger(__name__)


# ===========================================================================
# 辅助函数
# ===========================================================================


def _to_float(val: Any) -> float | None:
    """将值转换为 float，失败返回 None。"""
    if val is None:
        return None
    try:
        v = float(val)
        return None if pd.isna(v) else v
    except (ValueError, TypeError):
        return None


def _parse_quarter(q_str: str) -> str | None:
    """将 '2024年第一季度' 转换为 '2024-Q1'。"""
    if not q_str:
        return None
    chinese_quarters = {
        "一": "1",
        "二": "2",
        "三": "3",
        "四": "4",
        "第一": "1",
        "第二": "2",
        "第三": "3",
        "第四": "4",
    }
    m_cn = re.search(r"(\d{4}).*?(第一|第二|第三|第四|一|二|三|四)\s*季度", q_str)
    if m_cn:
        return f"{m_cn.group(1)}-Q{chinese_quarters[m_cn.group(2)]}"
    m = re.search(r"(\d{4})\D*(\d)", q_str)
    if m:
        year, q = m.group(1), m.group(2)
        return f"{year}-Q{q}"
    return None


# ===========================================================================
# 月度指标
# ===========================================================================


def _fetch_cpi() -> list[dict]:
    """获取 CPI 数据。"""
    if ak is None:
        return []
    try:
        df = ak.macro_china_cpi()
        if df is None or df.empty:
            return []
        col_map = {
            "年": "year",
            "月": "month",
            "cpi当月同比": "cpi_yoy",
            "CPI当月同比": "cpi_yoy",
            "当月同比": "cpi_yoy",
            "cpi当月环比": "cpi_mom",
            "CPI当月环比": "cpi_mom",
            "当月环比": "cpi_mom",
            "cpi核心当月同比": "cpi_core_yoy",
            "CPI核心当月同比": "cpi_core_yoy",
            "核心当月同比": "cpi_core_yoy",
        }
        df = df.rename(columns=col_map)
        keep = {"year", "month", "cpi_yoy", "cpi_mom", "cpi_core_yoy"}
        available = [c for c in keep if c in df.columns]
        df = df[available]
        records = []
        for _, row in df.iterrows():
            year = str(row.get("year", "")).strip()
            month = str(row.get("month", "")).strip()
            if year and month:
                records.append({
                    "date": f"{year}-{month.zfill(2)}-01",
                    "cpi_yoy": _to_float(row.get("cpi_yoy")),
                    "cpi_mom": _to_float(row.get("cpi_mom")),
                    "cpi_core_yoy": _to_float(row.get("cpi_core_yoy")),
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
        col_map = {
            "年": "year",
            "月": "month",
            "ppi当月同比": "ppi_yoy",
            "PPI当月同比": "ppi_yoy",
            "当月同比": "ppi_yoy",
            "ppi当月环比": "ppi_mom",
            "PPI当月环比": "ppi_mom",
            "当月环比": "ppi_mom",
        }
        df = df.rename(columns=col_map)
        keep = {"year", "month", "ppi_yoy", "ppi_mom"}
        available = [c for c in keep if c in df.columns]
        df = df[available]
        records = []
        for _, row in df.iterrows():
            year = str(row.get("year", "")).strip()
            month = str(row.get("month", "")).strip()
            if year and month:
                records.append({
                    "date": f"{year}-{month.zfill(2)}-01",
                    "ppi_yoy": _to_float(row.get("ppi_yoy")),
                    "ppi_mom": _to_float(row.get("ppi_mom")),
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
        col_map = {
            "年": "year",
            "月": "month",
            "制造业PMI": "pmi",
            "制造业PMI同比增长": "pmi_yoy",
            "制造业PMI同比增长率": "pmi_yoy",
            "同比增长": "pmi_yoy",
            "制造业PMI环比变化": "pmi_monthly_change",
            "环比变化": "pmi_monthly_change",
            "制造业PMI环比": "pmi_mom",
            "环比": "pmi_mom",
        }
        df = df.rename(columns=col_map)
        keep = {"year", "month", "pmi", "pmi_yoy", "pmi_monthly_change", "pmi_mom"}
        available = [c for c in keep if c in df.columns]
        df = df[available]
        records = []
        for _, row in df.iterrows():
            year = str(row.get("year", "")).strip()
            month = str(row.get("month", "")).strip()
            if year and month:
                records.append({
                    "date": f"{year}-{month.zfill(2)}-01",
                    "pmi": _to_float(row.get("pmi")),
                    "pmi_yoy": _to_float(row.get("pmi_yoy")),
                    "pmi_monthly_change": _to_float(row.get("pmi_monthly_change")),
                    "pmi_mom": _to_float(row.get("pmi_mom")),
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
        col_map = {
            "年": "year",
            "月": "month",
            "财新制造业PMI": "pmi_caixin",
            "PMI": "pmi_caixin",
        }
        df = df.rename(columns=col_map)
        keep = {"year", "month", "pmi_caixin"}
        available = [c for c in keep if c in df.columns]
        df = df[available]
        records = []
        for _, row in df.iterrows():
            year = str(row.get("year", "")).strip()
            month = str(row.get("month", "")).strip()
            if year and month:
                records.append({
                    "date": f"{year}-{month.zfill(2)}-01",
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
        col_map = {
            "年": "year",
            "月": "month",
            "M0": "m0",
            "M0数量": "m0",
            "M0同比": "m0_yoy",
            "M1": "m1",
            "M1数量": "m1",
            "M1同比": "m1_yoy",
            "M2": "m2",
            "M2数量": "m2",
            "M2同比": "m2_yoy",
        }
        df = df.rename(columns=col_map)
        keep = {"year", "month", "m0", "m1", "m2", "m0_yoy", "m1_yoy", "m2_yoy"}
        available = [c for c in keep if c in df.columns]
        df = df[available]
        records = []
        for _, row in df.iterrows():
            year = str(row.get("year", "")).strip()
            month = str(row.get("month", "")).strip()
            if year and month:
                records.append({
                    "date": f"{year}-{month.zfill(2)}-01",
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
        col_map = {
            "年": "year",
            "月": "month",
            "新增贷款": "new_loans",
            "新增人民币贷款": "new_loans",
            "新增贷款同比": "new_loans_yoy",
            "新增人民币贷款同比": "new_loans_yoy",
            "同比": "new_loans_yoy",
        }
        df = df.rename(columns=col_map)
        keep = {"year", "month", "new_loans", "new_loans_yoy"}
        available = [c for c in keep if c in df.columns]
        df = df[available]
        records = []
        for _, row in df.iterrows():
            year = str(row.get("year", "")).strip()
            month = str(row.get("month", "")).strip()
            if year and month:
                records.append({
                    "date": f"{year}-{month.zfill(2)}-01",
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
        col_map = {
            "年": "year",
            "月": "month",
            "社会消费品零售总额同比增长": "retail_sales_yoy",
            "同比增长率": "retail_sales_yoy",
            "同比增长": "retail_sales_yoy",
            "当月同比": "retail_sales_yoy",
            "社会消费品零售总额累计增长": "retail_sales_ytd_yoy",
            "累计增长": "retail_sales_ytd_yoy",
            "累计同比": "retail_sales_ytd_yoy",
        }
        df = df.rename(columns=col_map)
        keep = {"year", "month", "retail_sales_yoy", "retail_sales_ytd_yoy"}
        available = [c for c in keep if c in df.columns]
        df = df[available]
        records = []
        for _, row in df.iterrows():
            year = str(row.get("year", "")).strip()
            month = str(row.get("month", "")).strip()
            if year and month:
                records.append({
                    "date": f"{year}-{month.zfill(2)}-01",
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
        col_map = {
            "年": "year",
            "月": "month",
            "固定资产投资完成额同比增长": "fixed_asset_investment_yoy",
            "同比增长": "fixed_asset_investment_yoy",
            "当月同比": "fixed_asset_investment_yoy",
            "固定资产投资完成额累计增长": "fixed_asset_investment_ytd_yoy",
            "累计增长": "fixed_asset_investment_ytd_yoy",
            "累计同比": "fixed_asset_investment_ytd_yoy",
        }
        df = df.rename(columns=col_map)
        keep = {"year", "month", "fixed_asset_investment_yoy", "fixed_asset_investment_ytd_yoy"}
        available = [c for c in keep if c in df.columns]
        df = df[available]
        records = []
        for _, row in df.iterrows():
            year = str(row.get("year", "")).strip()
            month = str(row.get("month", "")).strip()
            if year and month:
                records.append({
                    "date": f"{year}-{month.zfill(2)}-01",
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
        col_map = {
            "年": "year",
            "月": "month",
            "出口总额": "export_value",
            "出口总额（亿元）": "export_value",
            "出口值": "export_value",
            "出口总额同比增长": "export_yoy",
            "出口总额同比增长（%）": "export_yoy",
            "出口同比增长": "export_yoy",
            "进口总额": "import_value",
            "进口总额（亿元）": "import_value",
            "进口值": "import_value",
            "进口总额同比增长": "import_yoy",
            "进口总额同比增长（%）": "import_yoy",
            "进口同比增长": "import_yoy",
        }
        df = df.rename(columns=col_map)
        keep = {"year", "month", "export_value", "export_yoy", "import_value", "import_yoy"}
        available = [c for c in keep if c in df.columns]
        df = df[available]
        records = []
        for _, row in df.iterrows():
            year = str(row.get("year", "")).strip()
            month = str(row.get("month", "")).strip()
            if year and month:
                records.append({
                    "date": f"{year}-{month.zfill(2)}-01",
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
            "年": "year",
            "月": "month",
            "工业增加值同比增长": "industrial_production_yoy",
            "同比增长": "industrial_production_yoy",
            "当月同比": "industrial_production_yoy",
            "工业增加值累计增长": "industrial_production_ytd_yoy",
            "累计增长": "industrial_production_ytd_yoy",
            "累计同比": "industrial_production_ytd_yoy",
        }
        df = df.rename(columns=col_map)
        keep = {"year", "month", "industrial_production_yoy", "industrial_production_ytd_yoy"}
        available = [c for c in keep if c in df.columns]
        df = df[available]
        records = []
        for _, row in df.iterrows():
            year = str(row.get("year", "")).strip()
            month = str(row.get("month", "")).strip()
            if year and month:
                records.append({
                    "date": f"{year}-{month.zfill(2)}-01",
                    "industrial_production_yoy": _to_float(row.get("industrial_production_yoy")),
                    "industrial_production_ytd_yoy": _to_float(row.get("industrial_production_ytd_yoy")),
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
        col_map = {
            "年": "year",
            "月": "month",
            "全社会用电量同比增长": "electricity_consumption_yoy",
            "同比增长": "electricity_consumption_yoy",
            "当月同比": "electricity_consumption_yoy",
            "全社会用电量": "electricity_consumption_total",
            "用电量": "electricity_consumption_total",
        }
        df = df.rename(columns=col_map)
        keep = {"year", "month", "electricity_consumption_yoy", "electricity_consumption_total"}
        available = [c for c in keep if c in df.columns]
        df = df[available]
        records = []
        for _, row in df.iterrows():
            year = str(row.get("year", "")).strip()
            month = str(row.get("month", "")).strip()
            if year and month:
                records.append({
                    "date": f"{year}-{month.zfill(2)}-01",
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
        col_map = {
            "年": "year",
            "月": "month",
            "企业商品价格同比": "enterprise_goods_price_yoy",
            "同比": "enterprise_goods_price_yoy",
            "企业商品价格环比": "enterprise_goods_price_mom",
            "环比": "enterprise_goods_price_mom",
        }
        df = df.rename(columns=col_map)
        keep = {"year", "month", "enterprise_goods_price_yoy", "enterprise_goods_price_mom"}
        available = [c for c in keep if c in df.columns]
        df = df[available]
        records = []
        for _, row in df.iterrows():
            year = str(row.get("year", "")).strip()
            month = str(row.get("month", "")).strip()
            if year and month:
                records.append({
                    "date": f"{year}-{month.zfill(2)}-01",
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
        col_map = {
            "年": "year",
            "月": "month",
            "消费者信心指数": "consumer_confidence",
            "消费者信心": "consumer_confidence",
            "消费者满意指数": "consumer_satisfaction",
            "消费者满意": "consumer_satisfaction",
            "消费者预期指数": "consumer_expectation",
            "消费者预期": "consumer_expectation",
        }
        df = df.rename(columns=col_map)
        keep = {"year", "month", "consumer_confidence", "consumer_satisfaction", "consumer_expectation"}
        available = [c for c in keep if c in df.columns]
        df = df[available]
        records = []
        for _, row in df.iterrows():
            year = str(row.get("year", "")).strip()
            month = str(row.get("month", "")).strip()
            if year and month:
                records.append({
                    "date": f"{year}-{month.zfill(2)}-01",
                    "consumer_confidence": _to_float(row.get("consumer_confidence")),
                    "consumer_satisfaction": _to_float(row.get("consumer_satisfaction")),
                    "consumer_expectation": _to_float(row.get("consumer_expectation")),
                })
        return records
    except Exception as e:
        logger.warning(f"消费者信心 获取失败: {e}")
        return []


def _fetch_lpr() -> list[dict]:
    """获取贷款市场报价利率（LPR）数据。

    akshare 返回的 LPR 数据为长格式（每行一个利率品种），
    需要按日期透视成宽格式。
    """
    if ak is None:
        return []
    try:
        df = ak.macro_china_lpr()
        if df is None or df.empty:
            return []
        # 确定列名
        col_map = {
            "日期": "date",
            "利率名称": "rate_name",
            "利率": "rate",
            "LPR品种": "rate_name",
            "品种": "rate_name",
            "LPR": "rate",
        }
        df = df.rename(columns=col_map)
        # 确定可用列
        has_date = "date" in df.columns
        has_rate_name = "rate_name" in df.columns
        has_rate = "rate" in df.columns

        if has_date and has_rate_name and has_rate:
            records: dict[str, dict] = {}
            for _, row in df.iterrows():
                date_val = str(row.get("date", "")).strip()[:10]
                name = str(row.get("rate_name", "")).strip()
                rate = _to_float(row.get("rate"))
                if not date_val or not name or rate is None:
                    continue
                if date_val not in records:
                    records[date_val] = {"date": date_val}
                if "1年" in name or "1Y" in name.upper():
                    records[date_val]["lpr_1y"] = rate
                elif "5年" in name or "5Y" in name.upper():
                    records[date_val]["lpr_5y"] = rate
            return list(records.values())
        else:
            # 降级：尝试直接用年/月格式
            cm = {
                "年": "year",
                "月": "month",
                "1年期LPR": "lpr_1y",
                "5年期LPR": "lpr_5y",
                "1年期": "lpr_1y",
                "5年期": "lpr_5y",
            }
            df = df.rename(columns=cm)
            keep = {"year", "month", "lpr_1y", "lpr_5y"}
            available = [c for c in keep if c in df.columns]
            if "year" in available and "month" in available:
                df = df[available]
                result = []
                for _, row in df.iterrows():
                    year = str(row.get("year", "")).strip()
                    month = str(row.get("month", "")).strip()
                    if year and month:
                        result.append({
                            "date": f"{year}-{month.zfill(2)}-01",
                            "lpr_1y": _to_float(row.get("lpr_1y")),
                            "lpr_5y": _to_float(row.get("lpr_5y")),
                        })
                return result
            return []
    except Exception as e:
        logger.warning(f"LPR 获取失败: {e}")
        return []


# ===========================================================================
# 季度指标
# ===========================================================================


def _fetch_gdp() -> list[dict]:
    """获取 GDP 数据。"""
    if ak is None:
        return []
    try:
        df = ak.macro_china_gdp()
        if df is None or df.empty:
            return []
        col_map = {
            "季度": "quarter",
            "国内生产总值": "gdp",
            "国内生产总值（亿元）": "gdp",
            "GDP": "gdp",
            "同比增长": "gdp_yoy",
            "GDP同比增长": "gdp_yoy",
            "环比增长": "gdp_qoq",
            "GDP环比增长": "gdp_qoq",
            "第一产业": "gdp_primary",
            "第一产业（亿元）": "gdp_primary",
            "第二产业": "gdp_secondary",
            "第二产业（亿元）": "gdp_secondary",
            "第三产业": "gdp_tertiary",
            "第三产业（亿元）": "gdp_tertiary",
        }
        df = df.rename(columns=col_map)
        keep = {"quarter", "gdp", "gdp_yoy", "gdp_qoq", "gdp_primary", "gdp_secondary", "gdp_tertiary"}
        available = [c for c in keep if c in df.columns]
        df = df[available]
        records = []
        for _, row in df.iterrows():
            q_str = str(row.get("quarter", "")).strip()
            date = _parse_quarter(q_str)
            if date:
                records.append({
                    "date": date,
                    "gdp": _to_float(row.get("gdp")),
                    "gdp_yoy": _to_float(row.get("gdp_yoy")),
                    "gdp_qoq": _to_float(row.get("gdp_qoq")),
                    "gdp_primary": _to_float(row.get("gdp_primary")),
                    "gdp_secondary": _to_float(row.get("gdp_secondary")),
                    "gdp_tertiary": _to_float(row.get("gdp_tertiary")),
                })
        return records
    except Exception as e:
        logger.warning(f"GDP 获取失败: {e}")
        return []


# ===========================================================================
# 日度指标
# ===========================================================================


def _fetch_shibor() -> list[dict]:
    """获取 SHIBOR 利率数据。"""
    if ak is None:
        return []
    try:
        df = ak.macro_china_shibor_all()
        if df is None or df.empty:
            return []
        col_map = {
            "日期": "date",
            "ON": "shibor_on",
            "隔夜": "shibor_on",
            "1W": "shibor_1w",
            "1周": "shibor_1w",
            "2W": "shibor_2w",
            "2周": "shibor_2w",
            "1M": "shibor_1m",
            "1个月": "shibor_1m",
            "3M": "shibor_3m",
            "3个月": "shibor_3m",
            "6M": "shibor_6m",
            "6个月": "shibor_6m",
            "9M": "shibor_9m",
            "9个月": "shibor_9m",
            "1Y": "shibor_1y",
            "1年": "shibor_1y",
        }
        df = df.rename(columns=col_map)
        keep = {
            "date", "shibor_on", "shibor_1w", "shibor_2w",
            "shibor_1m", "shibor_3m", "shibor_6m", "shibor_9m", "shibor_1y",
        }
        available = [c for c in keep if c in df.columns]
        df = df[available]
        records = []
        for _, row in df.iterrows():
            date_val = str(row.get("date", "")).strip()[:10]
            if date_val:
                records.append({
                    "date": date_val,
                    "shibor_on": _to_float(row.get("shibor_on")),
                    "shibor_1w": _to_float(row.get("shibor_1w")),
                    "shibor_2w": _to_float(row.get("shibor_2w")),
                    "shibor_1m": _to_float(row.get("shibor_1m")),
                    "shibor_3m": _to_float(row.get("shibor_3m")),
                    "shibor_6m": _to_float(row.get("shibor_6m")),
                    "shibor_9m": _to_float(row.get("shibor_9m")),
                    "shibor_1y": _to_float(row.get("shibor_1y")),
                })
        return records
    except Exception as e:
        logger.warning(f"SHIBOR 获取失败: {e}")
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
    results["quarterly_saved"] = saved_q

    # --- Daily ---
    try:
        daily = _fetch_shibor()
        if daily:
            for r in daily:
                r["data_date"] = data_date
            saved_d = db.save_macro_daily_batch(daily)
            logger.info(f"✅ 日度宏观(SHIBOR)保存完成: {saved_d} 条")
        else:
            saved_d = 0
            logger.warning("⚠️ SHIBOR 无数据")
    except Exception as e:
        saved_d = 0
        logger.warning(f"⚠️ SHIBOR 获取失败: {e}")
    results["daily_saved"] = saved_d

    return dict(results)
