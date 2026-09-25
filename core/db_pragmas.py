"""SQLite 写连接的 PRAGMA 唯一出处，以及 WAL 文件体积回收
──────────────────────────────────────────────────────────
本仓库所有连接都跑在 WAL 模式，但 **WAL 文件是只涨不缩的高水位线**：单个大事务会把
文件撑到该事务那么大，之后即使 checkpoint 早已完成、帧被反复复用，文件也不会缩小。
``PRAGMA wal_autocheckpoint``（默认 1000 帧 ≈ 4 MB）只负责把帧写回主库，不负责缩文件。

实测（2026-09-25，生产库 ``~/Code/quant_data/quant_core.db`` 与 scratch 库复现）：

* 生产库 ``quant_core.db-wal`` = **2,568,519,272 B（2.39 GiB）**，而同一时刻它的
  **活跃日志只有 22,350 帧（87 MB）** —— ``PRAGMA wal_checkpoint(PASSIVE)`` 返回
  ``busy=0``，即**没有任何读者在阻塞 checkpoint**，文件里 96% 是活跃日志之外的死区。
* scratch 复现成因：单个 310.6 MB 的事务提交后，WAL 停在 310.6 MB；此后**再多的写入
  都不缩**（``journal_size_limit=-1`` 时末值仍 310.6 MB）。
* 两种手段各自的效果（同一实验）：
  - ``journal_size_limit=8MiB``：巨型事务后仍是 310.6 MB，但**下一次写入即缩回 8.4 MB**
    —— 所以上限的生效时机是「超大 WAL 代被 reset 后的第一次写入」，不是提交那一刻。
  - ``PRAGMA wal_checkpoint(TRUNCATE)``：**立即**截到 0（生产库 2.39 GiB → 0，5.5 s），
    且在其他连接（TUI、正在跑的分批写入）持有连接时同样成功。

为什么要单独一个模块（而不是在 7 处各写一遍）
──────────────────────────────────────────────
``journal_size_limit`` **不是磁盘上的持久设置**：实测设完之后关闭连接，新连接读回的仍是
默认值 ``-1``。也就是说「找一处设一次」在架构上不成立，**每个写连接都必须自己设**；
散落在 7 处（``providers`` ×2、``core/migrations``、``scripts`` ×3、…）的写法迟早会漏，
而漏掉一处就等于没有上限。所以这里提供唯一的 ``apply_write_pragmas``，并有门禁
``tests/test_db_pragmas.py::test_wal_is_enabled_in_exactly_one_place`` 卡住「别处不得再
出现 ``PRAGMA journal_mode``」。

上限与 TRUNCATE 是互补的，不是二选一：

* ``journal_size_limit`` 兜住日常——任何一次超大写入之后，下一次写入就把文件缩回上限；
* ``truncate_wal`` 给出确定性——批量回填的典型形态是「巨型事务之后本轮就结束了」，
  上限要等到**再有人写入**才生效，而 ``daily_pipeline`` 收尾显式 checkpoint 一次，
  立刻把空间还给磁盘（只要还有别的连接开着，WAL 就不会被 SQLite 自己删除）。

调用方
──────
``apply_write_pragmas``：所有开启 WAL 的写连接（``providers`` / ``core/migrations`` /
``scripts/*``）。``truncate_wal``：``daily_pipeline.main`` 的 ``finally``（覆盖正常结束、
异常、``sys.exit`` 各条退出路径；失败只记日志，绝不影响退出码）。
"""

from __future__ import annotations

import contextlib
import logging
import os
import sqlite3
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# 上限取 64 MiB：截断是「截到上限」而不是「截到 0」（实测 limit=8MiB 时文件落在 8.4 MB），
# 所以上限同时是 WAL 文件的常态占位。64 MiB 相对 5.9 GB 的主库可以忽略，又远大于日常
# 单日增量（几千行 ≈ 几 MB），只在真正的大事务之后才触发一次截断。
DEFAULT_WAL_SIZE_LIMIT_BYTES = 64 * 1024 * 1024

# 覆盖项（单位 MiB）。调用时读取而不是模块导入时读取——沿用 P2-15 的教训：
# 导入期常量会让 monkeypatch/运行中改 env 静默失效。
WAL_SIZE_LIMIT_ENV = "QUANT_WAL_SIZE_LIMIT_MB"


def wal_size_limit_bytes() -> int:
    """WAL 文件上限（字节）。``QUANT_WAL_SIZE_LIMIT_MB`` 可覆盖，非法值回落默认并告警。"""
    raw = os.environ.get(WAL_SIZE_LIMIT_ENV, "").strip()
    if not raw:
        return DEFAULT_WAL_SIZE_LIMIT_BYTES
    try:
        mib = int(raw)
    except ValueError:
        logger.warning(
            "%s=%r 不是整数，WAL 上限回落默认 %d MiB",
            WAL_SIZE_LIMIT_ENV,
            raw,
            DEFAULT_WAL_SIZE_LIMIT_BYTES // (1024 * 1024),
        )
        return DEFAULT_WAL_SIZE_LIMIT_BYTES
    if mib < 0:
        logger.warning(
            "%s=%d 为负，WAL 上限回落默认 %d MiB（如需「不限制」请显式设 0）",
            WAL_SIZE_LIMIT_ENV,
            mib,
            DEFAULT_WAL_SIZE_LIMIT_BYTES // (1024 * 1024),
        )
        return DEFAULT_WAL_SIZE_LIMIT_BYTES
    return mib * 1024 * 1024


def apply_write_pragmas(
    conn: sqlite3.Connection,
    *,
    busy_timeout_ms: int | None = None,
    foreign_keys: bool = False,
    synchronous: str | None = "NORMAL",
    wal_size_limit: int | None = None,
) -> str:
    """启用 WAL 并声明 WAL 体积上限，返回实测生效的 ``journal_mode``。

    这是本仓库**唯一**允许执行 ``PRAGMA journal_mode=WAL`` 的地方。

    ``busy_timeout_ms`` / ``foreign_keys`` / ``synchronous`` 默认值与 SQLite 自身的默认值
    一致，所以只有确实需要偏离的调用方（如 ``providers`` 的共享写连接要 ``busy_timeout``
    与 ``foreign_keys=ON``）才显式传参，其余站点接入本函数不会改变原有行为。
    """
    limit = wal_size_limit_bytes() if wal_size_limit is None else wal_size_limit

    mode_row = conn.execute("PRAGMA journal_mode=WAL").fetchone()
    observed = str(mode_row[0]).lower() if mode_row else ""
    if observed != "wal":
        # 静默退化是本仓库反复踩的坑：WAL 没生效时上限同样无从生效，必须留下痕迹。
        logger.warning("PRAGMA journal_mode=WAL 未生效（实测 %r），WAL 体积上限不适用", observed or "<空>")
    if synchronous:
        conn.execute(f"PRAGMA synchronous={synchronous}")
    if busy_timeout_ms is not None:
        conn.execute(f"PRAGMA busy_timeout={int(busy_timeout_ms)}")
    conn.execute(f"PRAGMA foreign_keys={'ON' if foreign_keys else 'OFF'}")
    conn.execute(f"PRAGMA journal_size_limit={int(limit)}")
    return observed


def wal_bytes(db_path: str | Path) -> int:
    """当前 ``-wal`` 文件字节数；不存在（已被 SQLite 删除）时为 0。"""
    path = Path(str(db_path) + "-wal")
    try:
        return path.stat().st_size
    except OSError:
        return 0


def truncate_wal(db_path: str | Path, *, timeout: float = 5.0) -> dict[str, Any]:
    """尽力把 WAL 截断为 0 字节并把空间还给操作系统。**绝不抛异常。**

    失败（库被独占写锁占住 → ``busy=1``、文件被删、路径非法）只返回结果字典并告警，
    因为调用点在 ``daily_pipeline`` 的收尾；回收磁盘是尽力而为，不该影响退出码。

    Returns:
        ``{"ok", "busy", "before_bytes", "after_bytes", "reclaimed_bytes",
        "elapsed_ms", "error"}``；``ok`` 表示 checkpoint 完成且无冲突。
    """
    started = time.monotonic()
    before = wal_bytes(db_path)
    result: dict[str, Any] = {
        "ok": False,
        "busy": None,
        "before_bytes": before,
        "after_bytes": before,
        "reclaimed_bytes": 0,
        "elapsed_ms": 0,
        "error": None,
    }

    conn: sqlite3.Connection | None = None
    try:
        conn = sqlite3.connect(str(db_path), timeout=timeout)
        conn.execute(f"PRAGMA busy_timeout={int(timeout * 1000)}")
        row = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        busy = bool(row[0]) if row else True
        result["busy"] = busy
        result["ok"] = not busy
        if busy:
            result["error"] = "wal_checkpoint 返回 busy=1（有读写事务占用）"
            logger.warning("WAL 回收未完成：%s 上有事务占用，残留 %d 字节", db_path, before)
    except (sqlite3.Error, OSError, TypeError, ValueError) as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
        logger.warning("WAL 回收跳过：%s (%s)", db_path, result["error"])
    finally:
        if conn is not None:
            with contextlib.suppress(sqlite3.Error):
                conn.close()

    after = wal_bytes(db_path)
    result["after_bytes"] = after
    result["reclaimed_bytes"] = max(before - after, 0)
    result["elapsed_ms"] = int((time.monotonic() - started) * 1000)
    if result["ok"] and before:
        logger.info("🧹 WAL 已回收：%d → %d 字节（释放 %.1f MiB，%d ms）", before, after, (before - after) / 1048576, result["elapsed_ms"])
    return result
