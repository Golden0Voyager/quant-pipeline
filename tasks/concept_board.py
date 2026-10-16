"""
概念板块数据更新任务
────────────────────
东方财富概念板块日频行情 + 成分股映射。
数据源：东方财富 (push2.eastmoney.com / push2delay.eastmoney.com)。
"""
from __future__ import annotations

import logging
import uuid
from typing import Any, NamedTuple

from core.data_contract import CONCEPT_BOARD_CONTRACT, validate_records
from core.market_time import shanghai_today
from core.source_client import get_default_client
from core.utils import to_float as _to_float
from interface import DatabaseInterface

logger = logging.getLogger(__name__)


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


# 概念成分「完整抓到」的板块数 / 板块总数 的下限。
#
# 水位取 90%，与 tasks/market_flow.py 的 fund_flow 覆盖度水位同源（都是
# 「源端部分可用时必须降级上报、不得静默 success」）。板块级请求是逐个概念
# 发的，东财熔断会在任意位置截断整轮 —— 2026-10-11 实测只完成 18/504
# (3.6%)，远低于任何合理下限，故 90% 不会误报；同时高到足以在任何一次
# 熔断/限流发生时立刻把「基本没抓到」暴露成 degraded。
_MEMBER_CONCEPT_COVERAGE_FLOOR = 0.90


class ConceptMemberFetch(NamedTuple):
    """概念成分抓取结果，**完整性必须跟着数据一起返回**。

    只有 ``complete_codes`` 里的板块才算「完整抓到」，只有它们才允许关闭
    自己在 ``concept_member_history`` 里的旧成员区间。

    为什么必须区分：2026-10-11 东财熔断，只抓到 18/504 个板块、681 个
    成员，但那次运行仍返回 success，而保存层当时是「先把所有 open 行关闭、
    再插入本轮抓到的」—— 于是 09-19 的完整快照（71,915 行）被整体标成
    失效，当前有效成员只剩 681 个，与真实值（约 7.2 万）差两个数量级。

    ``expected`` 是概念列表里有效板块的总数，用作覆盖率分母。
    """

    members: list[dict]
    complete_codes: frozenset[str]
    expected: int


def _fetch_concept_members_em() -> ConceptMemberFetch:
    """从东方财富获取所有概念板块的成分股映射。

    通过 ``SourceClient`` + curl_cffi 浏览器掩护直调 push2 / push2delay
    双域名，规避 akshare 内部硬编码 ``29.push2.eastmoney.com`` 编号子域名
    被东方财富 WAF 封锁（RemoteDisconnected）的问题。

    概念板块列表获取失败时**上抛异常**（而非返回空列表），把源故障的
    失败语义交给调用方（``update_concept_member`` 返回 retained 保留旧
    数据，见 PR #103 规范），避免把网络故障误报为 ``no_data`` 而静默
    丢失整月数据。
    只有列表成功返回但确实无板块时才返回空列表。

    单个板块抓取失败 / 返回空成分都不算完整（不进 ``complete_codes``），
    以免保存层把「没抓到」当成「该板块没有成分」而关闭旧区间。
    """
    client = get_default_client()
    resp = client.call("eastmoney", _fetch_concept_list_em)
    if not resp.success:
        error = resp.metadata.error or "network error"
        logger.warning(f"⚠️ 东方财富概念板块列表获取失败: {error}")
        raise RuntimeError(error)
    items = resp.data

    boards = [
        (str(item.get("concept_code", "")), str(item.get("concept_name", "")))
        for item in items
        if item.get("concept_code") and item.get("concept_name")
    ]

    members: list[dict] = []
    complete_codes: set[str] = set()
    for code, name in boards:
        try:
            resp = client.call("eastmoney", _fetch_concept_members_one, code)
            if not resp.success:
                logger.warning(f"⚠️ 概念 {name}({code}) 成分股获取失败: {resp.metadata.error}")
                continue
            codes = [ts for ts in (resp.data or []) if ts]
            if not codes:
                # 源成功但没给出成分 ≠ 该板块确实没有成分，按未完整处理。
                logger.warning(f"⚠️ 概念 {name}({code}) 成分股返回空，不视作完整抓取")
                continue
            for ts_code in codes:
                members.append({
                    "concept_code": code,
                    "concept_name": name,
                    "ts_code": ts_code,
                })
            complete_codes.add(code)
        except Exception as e:
            logger.warning(f"⚠️ 概念 {name}({code}) 成分股获取失败: {e}")
    return ConceptMemberFetch(members, frozenset(complete_codes), len(boards))


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
        fetched = _fetch_concept_members_em()
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

    members = fetched.members
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
        # PIT history table —— 只把「完整抓到」的板块交给保存层去关闭旧区间
        pit_saved = db.save_concept_member_history_batch(
            members, run_id, valid_from, fetched.complete_codes
        )
        expected = fetched.expected
        covered = len(fetched.complete_codes)
        coverage = (covered / expected) if expected else 1.0
        logger.info(
            f"✅ 概念板块成分股: 快照 {saved_member} 条 / "
            f"PIT {pit_saved} 条 / 完整 {covered}/{expected} 个板块 ({coverage:.1%})"
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
    results["member_concepts_covered"] = covered
    results["member_concepts_expected"] = expected
    results["member_concept_coverage"] = round(coverage, 4)

    if saved_member + pit_saved == 0:
        results["status"] = "no_data"
    elif expected and coverage < _MEMBER_CONCEPT_COVERAGE_FLOOR:
        # 覆盖率不足不能报 success：2026-10-11 东财熔断，只抓到 18/504 个板块，
        # 旧实现照样写库并返回 success，``ingestion_runs`` 记下 saved_rows=73277，
        # 于是「基本没抓到」这件事在审计里完全看不见，还顺手作废了上一轮 PIT 快照。
        # 数据仍写入（保存层已按板块隔离，部分写入不再有破坏性），但状态降级。
        results["status"] = "degraded"
        results["error_kind"] = "data_quality"
        results["reason"] = (
            f"only {covered}/{expected} concept boards fetched completely "
            f"({coverage:.1%} < {_MEMBER_CONCEPT_COVERAGE_FLOOR:.0%}); "
            f"PIT intervals closed for the fetched boards only"
        )
        logger.warning(f"⚠️ {results['reason']}")
    else:
        results["status"] = "success"

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
