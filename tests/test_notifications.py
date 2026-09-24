"""core.notifications 测试：级别过滤、通道分发与 Bark 推送。"""
from __future__ import annotations

import json
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


# ===========================================================================
# Bark 通道
# ===========================================================================

def _bark_response(body: bytes) -> MagicMock:
    """构造可作为上下文管理器的 urlopen 返回值。"""
    resp = MagicMock()
    resp.read.return_value = body
    resp.__enter__.return_value = resp
    return resp


def _sent_payload(mock_open: MagicMock) -> dict:
    request = mock_open.call_args.args[0]
    return json.loads(request.data.decode("utf-8"))


def test_bark_channel_posts_expected_payload():
    resp = _bark_response(b'{"code":200,"message":"success"}')
    with patch.object(notifications, "urlopen", return_value=resp) as mock_open:
        notifications.BarkChannel(
            device_key="k1", server_url="https://api.day.app", group="g"
        ).send("error", "标题", "正文")

    request = mock_open.call_args.args[0]
    assert request.full_url == "https://api.day.app/push"
    assert _sent_payload(mock_open) == {
        "device_key": "k1",
        "title": "标题",
        "body": "正文",
        "level": "timeSensitive",
        "group": "g",
    }


def test_bark_level_mapping_by_severity():
    """error 用 timeSensitive（而非 critical），info 用 passive。"""
    for level, expected in (("error", "timeSensitive"), ("warning", "active"), ("info", "passive")):
        resp = _bark_response(b'{"code":200}')
        with patch.object(notifications, "urlopen", return_value=resp) as mock_open:
            notifications.BarkChannel(device_key="k1").send(level, "t", "m")
        assert _sent_payload(mock_open)["level"] == expected


def test_bark_channel_empty_message_falls_back_to_title():
    resp = _bark_response(b'{"code":200}')
    with patch.object(notifications, "urlopen", return_value=resp) as mock_open:
        notifications.BarkChannel(device_key="k1").send("info", "只有标题", "")
    assert _sent_payload(mock_open)["body"] == "只有标题"


def test_bark_channel_warns_when_response_code_is_not_200():
    """Bark 推送失败时仍返回 HTTP 200，成败只能看响应体里的 code。"""
    resp = _bark_response(b'{"code":400,"message":"device key not found"}')
    with patch.object(notifications, "urlopen", return_value=resp), \
         patch.object(notifications, "logger") as logger:
        notifications.BarkChannel(device_key="k1").send("error", "t", "m")

    logger.warning.assert_called_once()
    logger.info.assert_not_called()
    assert "400" in logger.warning.call_args.args[0]


def test_bark_channel_unparsable_body_is_treated_as_failure():
    resp = _bark_response(b"<html>not json</html>")
    with patch.object(notifications, "urlopen", return_value=resp), \
         patch.object(notifications, "logger") as logger:
        notifications.BarkChannel(device_key="k1").send("error", "t", "m")
    logger.warning.assert_called_once()
    logger.info.assert_not_called()


def test_bark_channel_logs_success_on_code_200():
    resp = _bark_response(b'{"code":200,"message":"success"}')
    with patch.object(notifications, "urlopen", return_value=resp), \
         patch.object(notifications, "logger") as logger:
        notifications.BarkChannel(device_key="k1").send("info", "t", "m")
    logger.info.assert_called_once()
    logger.warning.assert_not_called()


def test_bark_channel_without_device_key_skips_network(monkeypatch):
    monkeypatch.delenv("BARK_DEVICE_KEY", raising=False)
    with patch.object(notifications, "urlopen") as mock_open, \
         patch.object(notifications, "logger") as logger:
        notifications.BarkChannel().send("error", "t", "m")
    mock_open.assert_not_called()
    logger.warning.assert_called_once()


def test_bark_channel_swallows_network_errors():
    """推送失败只记 warning：告警通道自己不能把管道带崩。"""
    with patch.object(notifications, "urlopen", side_effect=OSError("boom")), \
         patch.object(notifications, "logger") as logger:
        notifications.BarkChannel(device_key="k1").send("error", "t", "m")
    logger.warning.assert_called_once()


# ===========================================================================
# 通道装配（NOTIFICATION_TYPE）
# ===========================================================================

def _channel_names() -> list[str]:
    return [type(c).__name__ for c in notifications._get_channels()]


def test_channels_default_to_console_only(monkeypatch):
    """默认配置只写日志——夜跑要真正收到通知必须显式改 NOTIFICATION_TYPE。"""
    monkeypatch.setenv("NOTIFICATION_TYPE", "console")
    monkeypatch.delenv("BARK_DEVICE_KEY", raising=False)
    monkeypatch.delenv("NOTIFICATION_WEBHOOK_URL", raising=False)
    assert _channel_names() == ["ConsoleChannel"]


def test_channels_add_bark_when_configured(monkeypatch):
    monkeypatch.setenv("NOTIFICATION_TYPE", "bark")
    monkeypatch.setenv("BARK_DEVICE_KEY", "k1")
    assert "BarkChannel" in _channel_names()


def test_channels_support_comma_separated_types(monkeypatch):
    monkeypatch.setenv("NOTIFICATION_TYPE", "webhook, bark")
    monkeypatch.setenv("BARK_DEVICE_KEY", "k1")
    monkeypatch.setenv("NOTIFICATION_WEBHOOK_URL", "https://example.invalid/hook")
    names = _channel_names()
    assert "WebhookChannel" in names and "BarkChannel" in names


def test_channels_warn_when_type_requested_without_credentials(monkeypatch):
    """配了通道但缺凭据 → 明确告警，而不是静默少发。"""
    monkeypatch.setenv("NOTIFICATION_TYPE", "bark")
    monkeypatch.delenv("BARK_DEVICE_KEY", raising=False)
    with patch.object(notifications, "logger") as logger:
        names = _channel_names()
    assert names == ["ConsoleChannel"]
    logger.warning.assert_called_once()
