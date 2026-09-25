"""
EIA 周度石油 status 指标更新任务
─────────────────────────────
数据源：美国能源信息署（EIA）Petroleum Status Report，经 APIv2
向后兼容端点 /v2/seriesid/<v1 series id> 抓取（APIv1 已于 2023-03
停用）。API key 从环境变量 EIA_API_KEY 读取（core.config 启动时
自动加载项目根 .env，该文件已 gitignore，不得提交）。

五个已验证的周度序列：
- PET.WCESTUS1.W  全美商业原油库存（除 SPR），千桶
- PET.WCSSTUS1.W  战略储备（SPR）库存，千桶
- PET.WGTSTUS1.W  车用汽油总库存，千桶
- PET.WCRFPUS2.W  原油产量，千桶/日
- PET.WPULEUS3.W  炼厂开工率，%

EIA 每周三/四发布并对近周修订，任务每次重抓尾部窗口（近 8 周）
幂等 UPSERT，天然自愈缺跑与修订。
"""

from __future__ import annotations

import logging
import os
from types import ModuleType

from interface import DatabaseInterface

# 显式声明使 `except ImportError: requests = None` 类型可收窄（原为 ignore[assignment]）
requests: ModuleType | None

try:
    import requests
except ImportError:
    requests = None

logger = logging.getLogger(__name__)

_EIA_SERIESID_URL = "https://api.eia.gov/v2/seriesid"
# v1 series id -> (中文名, 单位)。全部经 2026-09-20 实测验证。
_SERIES: dict[str, tuple[str, str]] = {
    "PET.WCESTUS1.W": ("全美商业原油库存(除SPR)", "MBBL"),
    "PET.WCSSTUS1.W": ("战略储备(SPR)库存", "MBBL"),
    "PET.WGTSTUS1.W": ("车用汽油总库存", "MBBL"),
    "PET.WCRFPUS2.W": ("原油产量", "MBBL/D"),
    "PET.WPULEUS3.W": ("炼厂开工率", "%"),
}
# 尾部重抓窗口：覆盖 EIA 对近周的修订
_TRAILING_WEEKS = 8
_TIMEOUT = 30.0


def _fetch_series(series_id: str, api_key: str) -> list[dict]:
    """抓取单序列尾部窗口，返回规范化记录列表。"""
    if requests is None:
        return []
    try:
        resp = requests.get(
            f"{_EIA_SERIESID_URL}/{series_id}",
            params={"api_key": api_key, "length": str(_TRAILING_WEEKS)},
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        payload = resp.json()
    except (requests.RequestException, ValueError) as e:
        logger.warning(f"⚠️ EIA {series_id} 获取失败: {e}")
        return []
    rows = (payload.get("response") or {}).get("data") or []
    name_cn, units = _SERIES[series_id]
    records = []
    for row in rows:
        period = str(row.get("period", ""))[:10]
        if not period:
            continue
        try:
            value = float(row.get("value"))
        except (TypeError, ValueError):
            value = None
        records.append(
            {
                "week_date": period,
                "series_id": series_id,
                "series_name": name_cn,
                "value": value,
                "units": units,
                "data_source": "eia",
            }
        )
    return records


def update_eia_petroleum(db: DatabaseInterface) -> dict:
    """获取 EIA 周度石油指标并保存。"""
    logger.info("\n" + "=" * 60)
    logger.info("🛢️ 任务: 更新 EIA 周度石油指标")
    logger.info("=" * 60)

    if requests is None:
        logger.error("❌ requests 未安装")
        return {"saved": 0, "error": "requests not installed"}

    api_key = os.getenv("EIA_API_KEY", "").strip()
    if not api_key:
        logger.error("❌ EIA_API_KEY 未配置（写入项目根 .env 或环境变量）")
        return {"saved": 0, "error": "EIA_API_KEY not configured"}

    try:
        records: list[dict] = []
        failed: list[str] = []
        for series_id in _SERIES:
            got = _fetch_series(series_id, api_key)
            if not got:
                failed.append(series_id)
            records.extend(got)
        if not records:
            logger.warning("⚠️ EIA 全部序列无数据")
            # fetch 内部吞异常，空 records 无法区分合法零行与全失败，保持 failed 语义
            return {"saved": 0, "total": 0}
        saved = db.save_eia_petroleum_batch(records)
        if failed:
            logger.warning(f"⚠️ EIA 部分序列获取失败: {failed}")
        logger.info(f"✅ EIA 周度石油指标保存完成: {saved} 条")
        return {"saved": saved, "total": len(records)}
    except Exception as e:
        logger.error(f"❌ EIA 周度石油指标更新失败: {e}")
        return {"saved": 0, "error": str(e), "error_kind": "network"}
