"""联网预检：全量管道启动前判断机器当前是否有可用网络。

为什么不用 AkShare 请求来判定：
- 入口预检发生在任何真实请求之前，AkShareMonitor 只能等请求失败后累积计数，
  无法在任务开始前快速回答“机器是否离线”；
- 任务层的断网报错会把一次环境问题（如笔记本在包里被 launchd 唤醒）放大成
  上百条 ERROR、熔断、失败任务与告警——这些应在源头被拦下。

本模块用底层 TCP 直连（raw socket，不受系统代理设置影响）只回答一个问题：
“机器当前有没有网”。代理配置损坏、数据源个别故障等仍按真实错误在运行期暴露，
不会被这里误判为离线。
"""
from __future__ import annotations

import logging
import socket
from collections.abc import Iterable

logger = logging.getLogger(__name__)

# 默认探测目标：东财 / 新浪 / 腾讯行情域名（A 股各主数据源）。
# 任一目标可达即视为联网；全部不可达判定为离线。
DEFAULT_PROBE_HOSTS: tuple[tuple[str, int], ...] = (
    ("push2.eastmoney.com", 443),
    ("hq.sinajs.cn", 443),
    ("qt.gtimg.cn", 443),
)

# 单主机连接超时（秒）。离线场景通常立即失败（DNS/路由不可达），
# 该超时只兜底“黑洞网络”，最坏耗时 ≈ 超时 × 主机数。
DEFAULT_CONNECT_TIMEOUT = 3.0


def _host_reachable(host: str, port: int, timeout: float) -> bool:
    """单主机 TCP 连通性探测；任何异常静默视为不可达（探测不产生日志噪音）。"""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def is_online(
    hosts: Iterable[tuple[str, int]] | None = None,
    timeout: float = DEFAULT_CONNECT_TIMEOUT,
) -> bool:
    """判断机器当前是否具备访问行情源的基本网络连通性。

    顺序探测 ``hosts``（默认 DEFAULT_PROBE_HOSTS），任一成功立即返回 True；
    全部失败返回 False。纯 socket 直连、不经系统代理环境变量，
    因此“代理配置损坏”不会被误判为离线（那是运行期真实错误，应如实暴露）。

    Args:
        hosts: (host, port) 探测目标列表；None 使用默认行情域名。
        timeout: 单主机连接超时秒数。
    """
    targets = DEFAULT_PROBE_HOSTS if hosts is None else tuple(hosts)
    return any(_host_reachable(host, port, timeout) for host, port in targets)
