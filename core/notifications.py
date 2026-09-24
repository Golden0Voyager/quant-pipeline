"""
通知推送模块
────────────
提供抽象通知通道和 Console/Webhook/Bark 实现。
Webhook 通道支持飞书/钉钉/企业微信通用文本格式；Bark 通道推送 iOS 通知。

配置（环境变量，需在进程启动前设置）
────────────────────────────────────
``NOTIFICATION_TYPE``
    逗号分隔的通道列表，取值 ``console`` / ``webhook`` / ``bark``。
    默认 ``console``——**只写日志，不外发**。夜跑无人值守时要真正收到通知，
    必须显式改成 ``webhook`` 或 ``bark``。
``NOTIFICATION_LEVEL``
    最低发送级别（``error`` / ``warning`` / ``info``），默认 ``error``，
    即只推送失败类通知，抑制「保留旧数据」与「全部完成」。
``NOTIFICATION_WEBHOOK_URL``
    飞书/钉钉/企业微信群机器人 Webhook 地址（``webhook`` 通道用）。
``BARK_DEVICE_KEY``
    Bark 设备 Key（``bark`` 通道用），必填。
``BARK_SERVER_URL``
    自建 Bark 服务地址，默认 ``https://api.day.app``。
``BARK_GROUP``
    通知分组名，默认 ``quant_pipeline``，用于在 Bark 里归类。
"""

from __future__ import annotations

import json
import logging
import os
from abc import ABC, abstractmethod
from urllib.request import Request, urlopen

# 本模块在**导入时**读取下列环境变量常量，而 .env 是在 core.config 导入时加载的。
# 若某个入口先导入本模块再导入 core.config，配置就会静默读成空值——那正是
# 「配了却收不到通知」最难查的一类故障。因此这里主动保证 .env 已加载。
# （core.config 只依赖标准库，不存在循环导入。）
from core.config import load_env_file as _load_env_file  # noqa: F401

logger = logging.getLogger(__name__)

# 环境变量配置
NOTIFICATION_WEBHOOK_URL = os.getenv("NOTIFICATION_WEBHOOK_URL", "")
NOTIFICATION_LEVEL = os.getenv("NOTIFICATION_LEVEL", "error")  # error, warning, info
NOTIFICATION_TYPE = os.getenv("NOTIFICATION_TYPE", "console")  # console, webhook, bark（可逗号分隔）

BARK_DEVICE_KEY = os.getenv("BARK_DEVICE_KEY", "")
BARK_SERVER_URL = os.getenv("BARK_SERVER_URL", "https://api.day.app")
BARK_GROUP = os.getenv("BARK_GROUP", "quant_pipeline")

# Bark 的 level 取值：critical / active / timeSensitive / passive。
# error 用 timeSensitive 而非 critical：critical 需在 App 内单独授权，且会绕过
# 静音与专注模式，对「夜跑失败」这类通知过重。
_BARK_LEVEL = {"error": "timeSensitive", "warning": "active", "info": "passive"}


class NotificationChannel(ABC):
    """通知通道抽象基类。"""

    @abstractmethod
    def send(self, level: str, title: str, message: str) -> None:
        """发送通知。"""


class ConsoleChannel(NotificationChannel):
    """控制台通知通道：直接通过 logger 输出。"""

    def send(self, level: str, title: str, message: str) -> None:
        log_method = {
            "error": logger.error,
            "warning": logger.warning,
            "info": logger.info,
        }.get(level, logger.info)
        log_method(f"[{level.upper()}] {title}: {message}")


class WebhookChannel(NotificationChannel):
    """Webhook 通知通道：发送到飞书/钉钉通用 Webhook。"""

    def __init__(self, webhook_url: str = ""):
        self.webhook_url = webhook_url or NOTIFICATION_WEBHOOK_URL

    def send(self, level: str, title: str, message: str) -> None:
        if not self.webhook_url:
            logger.warning("Webhook URL 未配置，跳过推送")
            return

        # 飞书/钉钉通用文本消息格式
        payload = json.dumps({
            "msgtype": "text",
            "text": {"content": f"[{level.upper()}] {title}\n{message}"},
        }).encode("utf-8")

        try:
            req = Request(
                self.webhook_url,
                data=payload,
                headers={"Content-Type": "application/json"},
            )
            urlopen(req, timeout=10)
            logger.info(f"Webhook 通知已推送: {title}")
        except Exception as e:
            logger.warning(f"Webhook 推送失败: {e}")


def _bark_response_code(body: str) -> int | None:
    """解析 Bark 响应体里的 ``code``。

    Bark 在推送**失败**时（设备 Key 不存在、参数非法等）仍返回 HTTP 200，
    成败只能看响应体：``{"code": 200, "message": "success"}``。只看 HTTP
    状态会把失败当成功——那正是本仓库反复出现的"报告成功但什么都没发生"。
    """
    try:
        payload = json.loads(body)
    except (ValueError, TypeError):
        return None
    code = payload.get("code") if isinstance(payload, dict) else None
    return code if isinstance(code, int) else None


class BarkChannel(NotificationChannel):
    """Bark 通知通道：推送到 iOS 设备。"""

    def __init__(
        self,
        device_key: str = "",
        server_url: str = "",
        group: str = "",
    ) -> None:
        self.device_key = device_key or os.getenv("BARK_DEVICE_KEY", BARK_DEVICE_KEY)
        self.server_url = (server_url or os.getenv("BARK_SERVER_URL", BARK_SERVER_URL)).rstrip("/")
        self.group = group or os.getenv("BARK_GROUP", BARK_GROUP)

    def send(self, level: str, title: str, message: str) -> None:
        if not self.device_key:
            logger.warning("BARK_DEVICE_KEY 未配置，跳过 Bark 推送")
            return

        payload = json.dumps(
            {
                "device_key": self.device_key,
                "title": title,
                "body": message or title,
                "level": _BARK_LEVEL.get(level, "active"),
                "group": self.group,
            }
        ).encode("utf-8")

        try:
            req = Request(
                f"{self.server_url}/push",
                data=payload,
                headers={"Content-Type": "application/json; charset=utf-8"},
            )
            with urlopen(req, timeout=10) as resp:  # noqa: S310 - 目标地址由本机配置控制
                body = resp.read().decode("utf-8", "replace")
        except Exception as e:
            logger.warning(f"Bark 推送失败: {e}")
            return

        code = _bark_response_code(body)
        if code != 200:
            logger.warning(f"Bark 推送未成功 (code={code}): {body[:200]}")
        else:
            logger.info(f"Bark 通知已推送: {title}")


def _configured_types() -> list[str]:
    """``NOTIFICATION_TYPE`` 支持逗号分隔（如 ``webhook,bark``）。"""
    raw = os.getenv("NOTIFICATION_TYPE", NOTIFICATION_TYPE)
    return [part.strip().lower() for part in raw.split(",") if part.strip()]


def _get_channels() -> list[NotificationChannel]:
    """根据环境变量配置返回通知通道列表。

    Console 通道始终保留：外发通道失败时，日志仍留有轨迹（本模块的外发
    失败只记 warning，不会向上抛）。
    """
    channels: list[NotificationChannel] = [ConsoleChannel()]
    types = _configured_types()

    if "webhook" in types:
        url = os.getenv("NOTIFICATION_WEBHOOK_URL", NOTIFICATION_WEBHOOK_URL)
        if url:
            channels.append(WebhookChannel(url))
        else:
            logger.warning("NOTIFICATION_TYPE 含 webhook 但 NOTIFICATION_WEBHOOK_URL 未配置，跳过")

    if "bark" in types:
        key = os.getenv("BARK_DEVICE_KEY", BARK_DEVICE_KEY)
        if key:
            channels.append(BarkChannel())
        else:
            logger.warning("NOTIFICATION_TYPE 含 bark 但 BARK_DEVICE_KEY 未配置，跳过")

    return channels


def notify_all(level: str, title: str, message: str = "") -> None:
    """
    向所有配置的通道发送通知。

    Args:
        level: 通知级别（error, warning, info）
        title: 通知标题
        message: 通知正文
    """
    level_order = {"error": 0, "warning": 1, "info": 2}
    config_level = NOTIFICATION_LEVEL
    # level_order 越小越严重：仅当通知级别 >= 配置级别（数值 <=）时才发送
    if level_order.get(level, 2) > level_order.get(config_level, 2):
        return  # 低于配置级别，不发送

    for channel in _get_channels():
        try:
            channel.send(level, title, message)
        except Exception as e:
            logger.warning(f"通知通道发送失败: {e}")
