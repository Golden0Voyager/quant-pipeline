"""core.notifications 测试：级别过滤与通道分发。"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import core.notifications as notifications


def _patched_channels() -> tuple[MagicMock, MagicMock]:
    channel = MagicMock()
    getter = MagicMock(return_value=[channel])
    return channel, getter


def test_notify_all_suppresses_below_configured_level():
    """NOTIFICATION_LEVEL=error 时，info/warning 不应发送。"""
    channel, getter = _patched_channels()
    with patch.object(notifications, "NOTIFICATION_LEVEL", "error"), \
         patch.object(notifications, "_get_channels", getter):
        notifications.notify_all("info", "t", "m")
        notifications.notify_all("warning", "t", "m")
        channel.send.assert_not_called()
        notifications.notify_all("error", "t", "m")
        channel.send.assert_called_once_with("error", "t", "m")


def test_notify_all_info_config_sends_everything():
    """NOTIFICATION_LEVEL=info 时，所有级别都应发送。"""
    channel, getter = _patched_channels()
    with patch.object(notifications, "NOTIFICATION_LEVEL", "info"), \
         patch.object(notifications, "_get_channels", getter):
        for level in ("error", "warning", "info"):
            notifications.notify_all(level, "t", "m")
        assert channel.send.call_count == 3


def test_notify_all_channel_failure_does_not_raise():
    """单个通道抛异常不影响其它通道，也不向外传播。"""
    bad = MagicMock()
    bad.send.side_effect = RuntimeError("boom")
    good = MagicMock()
    with patch.object(notifications, "NOTIFICATION_LEVEL", "info"), \
         patch.object(notifications, "_get_channels", return_value=[bad, good]), \
         patch.object(notifications, "logger"):
        notifications.notify_all("error", "t", "m")
        good.send.assert_called_once_with("error", "t", "m")
