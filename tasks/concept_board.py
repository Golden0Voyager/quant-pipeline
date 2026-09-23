"""
概念板块数据更新任务
────────────────────
东方财富概念板块日频行情 + 成分股映射。
数据源：东方财富 (push2.eastmoney.com / push2delay.eastmoney.com)。
"""
from __future__ import annotations

import logging
import uuid
from typing import Any

import pandas as pd

from core.data_contract import CONCEPT_BOARD_CONTRACT, validate_records
from core.market_time import shanghai_today
from core.source_client import get_default_client
from interface import DatabaseInterface

logger = logging.getLogger(__name__)


def _to_float(val: Any) -> float | None:
    if val is None:
        return None
    try:
        v = float(val)
        return None if pd.isna(v) else v
    except (ValueError, TypeError):
        return None


def _to_int(val: Any) -> int | None:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


# ===========================================================================
# 东方财富概念板块实时行情（含涨跌幅/成交额/涨跌家数）
# ===========================================================================


def _fetch_em_spot() -> list[dict]:
    """直接从东方财富 push2 接口获取概念板块实时行情（自动分页，支持备用域名）。

    Raises on HTTP/network errors so that ``SourceClient.call()`` can
    handle retry and circuit-breaker logic.
    """
    hosts = ("push2.eastmoney.com", "push2delay.eastmoney.com")
    session = get_default_client().get_session("eastmoney")
    today = shanghai_today()
    last_err: Exception | None = None

    for host in hosts:
        base_url = (
            f"https://{host}/api/qt/clist/get"
            "?pn={page}&pz=100&po=1&np=1"
            "&ut=bd1d9ddb04089700cf9c27f6f7426281"
            "&fltt=2&invt=2&fid=f3"
            "&fs=m:90+t:3"
            "&fields=f3,f4,f12,f14,f104,f105"
        )
        records = []
        page = 1
        success = True
        while True:
            try:
                resp = session.get(base_url.format(page=page), timeout=15)
                resp.raise_for_status()
                data = resp.json()
                if not isinstance(data, dict):
                    raise RuntimeError(f"{host} 返回畸形响应: {type(data).__name__}")
            except Exception as e:
                logger.debug(f"概念板块行情主机 {host} 请求失败 (page {page}): {e}")
                last_err = e
                success = False
                break

            items = ((data.get("data") or {}).get("diff")) or []
            if not items:
                break
            for item in items:
                code = str(item.get("f12", "")).strip()
                name = str(item.get("f14", "")).strip()
                if not code or not name:
                    continue
                records.append({
                    "trade_date": today,
                    "concept_code": code,
                    "concept_name": name,
                    "pct_change": _to_float(item.get("f3")),
                    "turnover": _to_float(item.get("f4")),
                    "up_count": _to_int(item.get("f104")),
                    "down_count": _to_int(item.get("f105")),
                    "data_source": "em",
                })
            if len(items) < 100:
                break
            page += 1

        if success and records:
            return records

    if last_err is not None:
        raise last_err
    return []


# ===========================================================================
# 概念板块成分股映射（使用东方财富个股接口）
# ===========================================================================


def _fetch_concept_list_em() -> list[dict]:
    """Fetch concept board name list from East Money push2 API (host failover).

    The clist API truncates each page to 100 rows regardless of ``pz``, so
    paginate until a short page is returned.
    """
    hosts = ("push2.eastmoney.com", "push2delay.eastmoney.com")
    session = get_default_client().get_session("eastmoney")
    last_err: Exception | None = None

    for host in hosts:
        out: list[dict] = []
        page = 1
        success = True
        while True:
            try:
                resp = session.get(
                    f"https://{host}/api/qt/clist/get",
                    params={
                        "pn": page, "pz": 100, "po": 1, "np": 1,
                        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
                        "fltt": 2, "invt": 2, "fid": "f3",
                        "fs": "m:90+t:3",
                        "fields": "f12,f14",
                    },
                    timeout=15,
                )
                resp.raise_for_status()
                data = resp.json()
                if not isinstance(data, dict):
                    raise RuntimeError(f"{host} 返回畸形响应: {type(data).__name__}")
            except Exception as e:
                logger.debug(f"概念板块列表主机 {host} 请求失败 (page {page}): {e}")
                last_err = e
                success = False
                break

            items = ((data.get("data") or {}).get("diff")) or []
            if not items:
                break
            for item in items:
                code = str(item.get("f12", "")).strip()
                name = str(item.get("f14", "")).strip()
                if code and name:
                    out.append({"concept_code": code, "concept_name": name})
            if len(items) < 100:
                break
            page += 1

        if success and out:
            return out

    if last_err is not None:
        raise last_err
    return []


def _fetch_concept_members_one(code: str) -> list[str]:
    """抓取单个概念板块的成分股代码（push2 / push2delay 双域名 failover）。

    Raises on HTTP/network errors so that ``SourceClient.call()`` can
    handle retry and circuit-breaker logic.
    """
    hosts = ("push2.eastmoney.com", "push2delay.eastmoney.com")
    session = get_default_client().get_session("eastmoney")
    last_err: Exception | None = None

    for host in hosts:
        ts_codes: list[str] = []
        page = 1
        success = True
        while True:
            try:
                resp = session.get(
                    f"https://{host}/api/qt/clist/get",
                    params={
                        "pn": page, "pz": 100, "po": 1, "np": 1,
                        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
                        "fltt": 2, "invt": 2, "fid": "f12",
                        "fs": f"b:{code} f:!50",
                        "fields": "f12",
                    },
                    timeout=15,
                )
                resp.raise_for_status()
                data = resp.json()
                if not isinstance(data, dict):
                    raise RuntimeError(f"{host} 返回畸形响应: {type(data).__name__}")
            except Exception as e:
                logger.debug(f"概念成分请求主机 {host} 失败 (page {page}): {e}")
                last_err = e
                success = False
                break

            items = ((data.get("data") or {}).get("diff")) or []
            if not items:
                break
            for item in items:
                ts_code = str(item.get("f12", "")).strip()
                if ts_code:
                    ts_codes.append(ts_code)
            if len(items) < 100:
                break
            page += 1

        if success and ts_codes:
            return ts_codes

    if last_err is not None:
        raise last_err
    return []


def _fetch_concept_members_em() -> list[dict]:
    """从东方财富获取所有概念板块的成分股映射。

    通过 ``SourceClient`` + curl_cffi 浏览器掩护直调 push2 / push2delay
    双域名，规避 akshare 内部硬编码 ``29.push2.eastmoney.com`` 编号子域名
    被东方财富 WAF 封锁（RemoteDisconnected）的问题。

    概念板块列表获取失败时**上抛异常**（而非返回空列表），把源故障的
    失败语义交给调用方（``update_concept_member`` 返回 retained 保留旧
    数据，见 PR #103 规范），避免把网络故障误报为 ``no_data`` 而静默
    丢失整月数据。
    只有列表成功返回但确实无板块时才返回空列表。
    """
    client = get_default_client()
    resp = client.call("eastmoney", _fetch_concept_list_em)
    if not resp.success:
        error = resp.metadata.error or "network error"
        logger.warning(f"⚠️ 东方财富概念板块列表获取失败: {error}")
        raise RuntimeError(error)
    items = resp.data

    members: list[dict] = []
    for item in items:
        code = item.get("concept_code", "")
        name = item.get("concept_name", "")
        if not code or not name:
            continue
        try:
            resp = client.call("eastmoney", _fetch_concept_members_one, code)
            if not resp.success:
                logger.warning(f"⚠️ 概念 {name}({code}) 成分股获取失败: {resp.metadata.error}")
                continue
            for ts_code in resp.data:
                if ts_code:
                    members.append({
                        "concept_code": code,
                        "concept_name": name,
                        "ts_code": ts_code,
                    })
        except Exception as e:
            logger.warning(f"⚠️ 概念 {name}({code}) 成分股获取失败: {e}")
    return members


# ===========================================================================
# 主更新函数
# ===========================================================================


def update_concept_board(db: DatabaseInterface) -> dict:
    """获取概念板块日频行情并保存。"""
    logger.info("\n" + "=" * 60)
    logger.info("🏷️ 任务: 更新概念板块行情")
    logger.info("=" * 60)

    results: dict[str, Any] = {}

    resp = get_default_client().call("eastmoney", _fetch_em_spot)
    if not resp.success:
        error_msg = str(resp.metadata.error) if resp.metadata.error else "network error"
        logger.warning(f"⚠️ 概念板块行情获取失败: {error_msg}")
        return {
            "status": "retained",
            "error_kind": "network",
            "reason": f"eastmoney concept spot unavailable: {error_msg}",
            "error": error_msg,
            "retained_old_data": True,
            "board_saved": 0,
            "saved": 0,
        }

    spot = resp.data
    if not spot:
        logger.warning("⚠️ 概念板块行情无数据")
        return {"status": "no_data", "board_saved": 0, "saved": 0}

    try:
        # 补充 trade_date 字段（实时行情接口不返回日期，按上海市场日标记）
        today_str = shanghai_today()
        for r in spot:
            r.setdefault("trade_date", today_str)
        validated_spot, violations = validate_records(spot, CONCEPT_BOARD_CONTRACT, logger)
        if violations and not validated_spot:
            logger.error(f"🚫 概念板块行情数据合约校验失败: {violations}")
            return {
                "status": "failed",
                "error_kind": "data_quality",
                "error": f"contract validation failed: {violations}",
                "board_saved": 0,
                "saved": 0,
            }

        if violations:
            logger.warning(f"⚠️ 概念板块行情合约校验过滤 {len(spot) - len(validated_spot)} 条")
        saved_board = db.save_concept_board_batch(validated_spot)
        logger.info(f"✅ 概念板块行情保存完成: {saved_board} 条")
    except Exception as e:
        logger.warning(f"⚠️ 概念板块行情保存失败: {e}")
        return {
            "status": "failed",
            "error_kind": "internal",
            "error": str(e),
            "board_saved": 0,
            "saved": 0,
        }

    results["board_saved"] = saved_board
    results["saved"] = saved_board
    results["status"] = "success" if saved_board > 0 else "no_data"

    return dict(results)


def update_concept_member(
    db: DatabaseInterface,
    _task_run_id: str | None = None,
) -> dict:
    """获取概念板块成分股映射并保存（较慢，建议按需运行而非每日）。

    同时写入 ``concept_member``（快照表）和 ``concept_member_history``（PIT 历史表）。

    Args:
        db: 数据库接口
        _task_run_id: 由 ``safe_task`` 注入的运行 ID。为 None 时自动生成。
    """
    logger.info("\n" + "=" * 60)
    logger.info("🏷️ 任务: 更新概念板块成分股映射 (含 PIT)")
    logger.info("=" * 60)

    results: dict[str, Any] = {}
    run_id = _task_run_id or str(uuid.uuid4())
    # PIT interval 边界使用上海市场日，避免本机时区在午夜前后错切快照区间
    valid_from = shanghai_today()

    try:
        members = _fetch_concept_members_em()
    except Exception as e:
        logger.warning(f"⚠️ 概念板块成分股获取失败: {e}")
        return {
            "status": "retained",
            "error_kind": "network",
            "reason": f"eastmoney concept members unavailable: {e}",
            "error": str(e),
            "retained_old_data": True,
            "member_saved": 0,
            "pit_saved": 0,
            "saved": 0,
        }

    if not members:
        # 仅当源成功响应但确无数据时才视为 no_data（不告警）
        logger.warning("⚠️ 概念板块成分股无数据")
        return {
            "status": "no_data",
            "member_saved": 0,
            "pit_saved": 0,
            "saved": 0,
        }

    try:
        # legacy snapshot table
        saved_member = db.save_concept_member_batch(members)
        # PIT history table
        pit_saved = db.save_concept_member_history_batch(members, run_id, valid_from)
        unique_codes = {m["concept_code"] for m in members}
        logger.info(
            f"✅ 概念板块成分股: 快照 {saved_member} 条 / "
            f"PIT {pit_saved} 条 / {len(unique_codes)} 个板块"
        )
    except Exception as e:
        logger.warning(f"⚠️ 概念板块成分股保存失败: {e}")
        return {
            "status": "failed",
            "error_kind": "internal",
            "error": str(e),
            "member_saved": 0,
            "pit_saved": 0,
            "saved": 0,
        }
    results["member_saved"] = saved_member
    results["pit_saved"] = pit_saved
    results["saved"] = saved_member + pit_saved
    results["status"] = "success" if (saved_member + pit_saved) > 0 else "no_data"

    return dict(results)


# ===========================================================================
# 收盘刷新 helper（Task 9）：只抓取/归一化，不写库，源异常直接上抛
# ===========================================================================


def fetch_concept_board_records(trade_date: str) -> list[dict]:
    """收盘刷新专用：抓取概念板块实时快照并覆写 trade_date 为目标日。

    ``_fetch_em_spot`` 本身即 raise 版（HTTP 错误直接上抛）；快照的
    自然日戳统一覆写为调用方指定的目标交易日。权威空返回 []。
    """
    records = _fetch_em_spot()
    for record in records:
        record["trade_date"] = trade_date
    return records
