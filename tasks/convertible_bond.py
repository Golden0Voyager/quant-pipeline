"""
可转债数据更新任务
────────────────
从 akshare 获取：可转债行情、可转债强赎、可转债指数。
"""

from __future__ import annotations

import logging

from core.utils import warn_if_all_empty
from interface import DatabaseInterface

try:
    import akshare as ak
except ImportError:
    ak = None

logger = logging.getLogger(__name__)


# ===========================================================================
# 辅助函数
# ===========================================================================


def _to_float(value: object) -> float | None:
    """安全转换数值字段，非数字 / None / 空字符串 返回 None。"""
    if value is None:
        return None
    try:
        v = float(value)
        if v != v:  # NaN
            return None
        return v
    except (ValueError, TypeError):
        return None


# ===========================================================================
# 可转债行情
# ===========================================================================


def _fetch_cb_quotation() -> list[dict]:
    """获取可转债实时行情（集思录）。"""
    if ak is None:
        return []
    try:
        df = ak.bond_cb_jsl()
        if df is None or df.empty:
            return []
        records: list[dict] = []
        for _, row in df.iterrows():
            records.append(
                {
                    "ts_code": str(row.get("代码", "")).strip(),
                    "bond_name": str(row.get("转债名称", "")).strip(),
                    "price": _to_float(row.get("现价")),
                    "premium": _to_float(row.get("转股溢价率")),
                    "double_low": _to_float(row.get("双低")),
                    "expire_date": str(row.get("到期时间", ""))[:10],
                    "data_source": "akshare",
                }
            )
        return records
    except Exception as e:
        logger.warning(f"⚠️ 可转债行情获取失败: {e}")
        return []


def update_cb_quotation(db: DatabaseInterface) -> dict:
    """获取可转债行情并保存。"""
    logger.info("\n" + "=" * 60)
    logger.info("📜 任务: 更新可转债行情")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "error": "akshare not installed"}

    try:
        records = _fetch_cb_quotation()
        warn_if_all_empty(records, key_cols=["ts_code", "price"], task_name="convertible_bond_quotation")
        if not records:
            logger.warning("⚠️ 可转债行情无数据")
            # 显式 skipped：非交易日/上游无数据属正常结果，避免被结果契约误判为 failed
            return {"skipped": True, "reason": "no convertible bond quotation data", "total": 0}
        saved = db.save_cb_quotation_batch(records)
        logger.info(f"✅ 可转债行情保存完成: {saved} 条")
        return {"saved": saved, "total": len(records)}
    except Exception as e:
        logger.error(f"❌ 可转债行情更新失败: {e}")
        return {"saved": 0, "error": str(e)}


# ===========================================================================
# 可转债强赎
# ===========================================================================


def _fetch_cb_redeem() -> list[dict]:
    """获取可转债强赎信息（集思录）。"""
    if ak is None:
        return []
    try:
        df = ak.bond_cb_redeem_jsl()
        if df is None or df.empty:
            return []
        records: list[dict] = []
        for _, row in df.iterrows():
            records.append(
                {
                    "ts_code": str(row.get("代码", "")).strip(),
                    "bond_name": str(row.get("名称", "")).strip(),
                    "redeem_flag": str(row.get("强赎状态", "")).strip(),
                    "redeem_price": _to_float(row.get("强赎价")),
                    "redeem_date": str(row.get("最后交易日", ""))[:10],
                    "data_source": "akshare",
                }
            )
        return records
    except Exception as e:
        logger.warning(f"⚠️ 可转债强赎获取失败: {e}")
        return []


def update_cb_redeem(db: DatabaseInterface) -> dict:
    """获取可转债强赎信息并保存。"""
    logger.info("\n" + "=" * 60)
    logger.info("🔥 任务: 更新可转债强赎")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "error": "akshare not installed"}

    try:
        records = _fetch_cb_redeem()
        warn_if_all_empty(records, key_cols=["ts_code", "redeem_flag"], task_name="convertible_bond_redeem")
        if not records:
            logger.warning("⚠️ 可转债强赎无数据")
            # 显式 skipped：无强赎公告属正常结果，避免被结果契约误判为 failed
            return {"skipped": True, "reason": "no convertible bond redeem data", "total": 0}
        saved = db.save_cb_redeem_batch(records)
        logger.info(f"✅ 可转债强赎保存完成: {saved} 条")
        return {"saved": saved, "total": len(records)}
    except Exception as e:
        logger.error(f"❌ 可转债强赎更新失败: {e}")
        return {"saved": 0, "error": str(e)}


# ===========================================================================
# 可转债指数
# ===========================================================================


def _fetch_cb_index() -> list[dict]:
    """获取可转债指数行情（集思录等权指数）。

    ``ak.bond_cb_index_jsl`` 返回单一时间序列，列名为英文
    （``price_dt``/``price``/``volume``/``amount`` 等），无 OHLC 与指数代码/名称，
    故 open/high/low 置空，index_code/index_name 使用常量。
    """
    if ak is None:
        return []
    try:
        df = ak.bond_cb_index_jsl()
        if df is None or df.empty:
            return []
        records: list[dict] = []
        for _, row in df.iterrows():
            records.append(
                {
                    "trade_date": str(row.get("price_dt", ""))[:10],
                    "index_code": "JSL_EW",
                    "index_name": "集思录可转债等权指数",
                    "open": None,
                    "close": _to_float(row.get("price")),
                    "high": None,
                    "low": None,
                    "volume": _to_float(row.get("volume")),
                    "data_source": "akshare",
                }
            )
        return records
    except Exception as e:
        logger.warning(f"⚠️ 可转债指数获取失败: {e}")
        return []


def update_cb_index(db: DatabaseInterface) -> dict:
    """获取可转债指数行情并保存。"""
    logger.info("\n" + "=" * 60)
    logger.info("📊 任务: 更新可转债指数")
    logger.info("=" * 60)

    if ak is None:
        logger.error("❌ akshare 未安装")
        return {"saved": 0, "error": "akshare not installed"}

    try:
        records = _fetch_cb_index()
        warn_if_all_empty(records, key_cols=["trade_date", "close"], task_name="convertible_bond_index")
        if not records:
            logger.warning("⚠️ 可转债指数无数据")
            # 显式 skipped：非交易日/上游无数据属正常结果，避免被结果契约误判为 failed
            return {"skipped": True, "reason": "no convertible bond index data", "total": 0}
        saved = db.save_cb_index_batch(records)
        logger.info(f"✅ 可转债指数保存完成: {saved} 条")
        return {"saved": saved, "total": len(records)}
    except Exception as e:
        logger.error(f"❌ 可转债指数更新失败: {e}")
        return {"saved": 0, "error": str(e)}


# ===========================================================================
# 统一入口
# ===========================================================================


def update_convertible_bond(db: DatabaseInterface) -> dict:
    """获取可转债行情、强赎、指数数据并保存。"""
    logger.info("\n" + "=" * 60)
    logger.info("📜 任务: 更新可转债数据")
    logger.info("=" * 60)

    if ak is None:
        return {"error": "akshare not installed"}

    results: dict[str, object] = {}

    try:
        records = _fetch_cb_quotation()
        if records:
            saved = db.save_cb_quotation_batch(records)
            results["quotation"] = saved
            logger.info(f"✅ 可转债行情: {saved} 条")
    except Exception as e:
        logger.warning(f"可转债行情失败: {e}")

    try:
        records = _fetch_cb_redeem()
        if records:
            saved = db.save_cb_redeem_batch(records)
            results["redeem"] = saved
            logger.info(f"✅ 可转债强赎: {saved} 条")
    except Exception as e:
        logger.warning(f"可转债强赎失败: {e}")

    try:
        records = _fetch_cb_index()
        if records:
            saved = db.save_cb_index_batch(records)
            results["index"] = saved
            logger.info(f"✅ 可转债指数: {saved} 条")
    except Exception as e:
        logger.warning(f"可转债指数失败: {e}")

    return results


# ===========================================================================
# 收盘刷新 helper（Task 9）：只抓取/归一化，不写库，源异常直接上抛
# ===========================================================================


def fetch_cb_quotation_records(updated_at: str) -> list[dict]:
    """收盘刷新专用：抓取可转债实时行情快照并附 updated_at。

    权威空返回 []；源异常直接上抛，由适配器/编排器落实保留旧数据。
    """
    df = ak.bond_cb_jsl()
    if df is None or df.empty:
        return []
    records: list[dict] = []
    for _, row in df.iterrows():
        records.append(
            {
                "ts_code": str(row.get("代码", "")).strip(),
                "bond_name": str(row.get("转债名称", "")).strip(),
                "price": _to_float(row.get("现价")),
                "premium": _to_float(row.get("转股溢价率")),
                "double_low": _to_float(row.get("双低")),
                "expire_date": str(row.get("到期时间", ""))[:10],
                "data_source": "akshare",
                "updated_at": updated_at,
            }
        )
    return records


def fetch_cb_redeem_records(updated_at: str) -> list[dict]:
    """收盘刷新专用：抓取可转债强赎快照并附 updated_at。

    权威空返回 []；源异常直接上抛。
    """
    df = ak.bond_cb_redeem_jsl()
    if df is None or df.empty:
        return []
    records: list[dict] = []
    for _, row in df.iterrows():
        records.append(
            {
                "ts_code": str(row.get("代码", "")).strip(),
                "bond_name": str(row.get("名称", "")).strip(),
                "redeem_flag": str(row.get("强赎状态", "")).strip(),
                "redeem_price": _to_float(row.get("强赎价")),
                "redeem_date": str(row.get("最后交易日", ""))[:10],
                "data_source": "akshare",
                "updated_at": updated_at,
            }
        )
    return records


def fetch_cb_index_records() -> list[dict]:
    """收盘刷新专用：抓取可转债等权指数全历史并归一化（历史型源）。

    由适配器挑选回看窗口内可接受的目标分区；源异常直接上抛。
    """
    df = ak.bond_cb_index_jsl()
    if df is None or df.empty:
        return []
    records: list[dict] = []
    for _, row in df.iterrows():
        records.append(
            {
                "trade_date": str(row.get("price_dt", ""))[:10],
                "index_code": "JSL_EW",
                "index_name": "集思录可转债等权指数",
                "open": None,
                "close": _to_float(row.get("price")),
                "high": None,
                "low": None,
                "volume": _to_float(row.get("volume")),
                "data_source": "akshare",
            }
        )
    return records
