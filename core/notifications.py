"""
通知推送模块
────────────
提供抽象通知通道和 Console/Webhook 实现。
支持飞书/钉钉通用文本格式。
"""

from __future__ import annotations

import json
import logging
import os
from abc import ABC, abstractmethod
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

# 环境变量配置
NOTIFICATION_WEBHOOK_URL = os.getenv("NOTIFICATION_WEBHOOK_URL", "")
NOTIFICATION_LEVEL = os.getenv("NOTIFICATION_LEVEL", "error")  # error, warning, info
NOTIFICATION_TYPE = os.getenv("NOTIFICATION_TYPE", "console")  # console, webhook


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


def _get_channels() -> list[NotificationChannel]:
    """根据环境变量配置返回通知通道列表。"""
    channels: list[NotificationChannel] = [ConsoleChannel()]
    notify_type = os.getenv("NOTIFICATION_TYPE", NOTIFICATION_TYPE)
    if notify_type == "webhook" and NOTIFICATION_WEBHOOK_URL:
        channels.append(WebhookChannel())
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
    if level_order.get(level, 2) < level_order.get(config_level, 2):
        return  # 低于配置级别，不发送

    for channel in _get_channels():
        try:
            channel.send(level, title, message)
        except Exception as e:
            logger.warning(f"通知通道发送失败: {e}")
