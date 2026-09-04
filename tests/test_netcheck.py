"""Tests for core.netcheck - TCP-based offline detection."""
from __future__ import annotations

import socket
from unittest.mock import patch

import pytest

from core import netcheck


class _FakeConnectedSocket:
    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False


def test_online_when_first_host_reachable(monkeypatch: pytest.MonkeyPatch):
    """任一目标可达即在线，且不再探测后续主机。"""
    calls: list[tuple[str, int]] = []

    def fake_create_connection(target, timeout):
        calls.append(target)
        return _FakeConnectedSocket()

    monkeypatch.setattr(socket, "create_connection", fake_create_connection)
    hosts = [("host-a.com", 443), ("host-b.com", 443)]
    assert netcheck.is_online(hosts=hosts, timeout=1.0) is True
    assert calls == [("host-a.com", 443)]


def test_online_when_second_host_reachable(monkeypatch: pytest.MonkeyPatch):
    """首个目标不可达时继续探测下一个。"""
    attempts: list[tuple[str, int]] = []

    def fake_create_connection(target, timeout):
        attempts.append(target)
        if target[0] == "dead.example":
            raise OSError("no route to host")
        return _FakeConnectedSocket()

    monkeypatch.setattr(socket, "create_connection", fake_create_connection)
    hosts = [("dead.example", 443), ("alive.example", 443)]
    assert netcheck.is_online(hosts=hosts, timeout=1.0) is True
    assert attempts == [("dead.example", 443), ("alive.example", 443)]


def test_offline_when_all_hosts_unreachable(monkeypatch: pytest.MonkeyPatch):
    """全部目标不可达（DNS/路由/超时）判定为离线。"""
    attempts: list[tuple[str, int]] = []

    def fake_create_connection(target, timeout):
        attempts.append(target)
        raise OSError("temporary failure in name resolution")

    monkeypatch.setattr(socket, "create_connection", fake_create_connection)
    hosts = [("a.example", 443), ("b.example", 443), ("c.example", 443)]
    assert netcheck.is_online(hosts=hosts, timeout=1.0) is False
    assert len(attempts) == 3


def test_default_hosts_are_data_sources():
    """默认探测目标应覆盖 A 股主数据源域名（东财/新浪/腾讯），端口为 443。"""
    hosts = {host for host, _port in netcheck.DEFAULT_PROBE_HOSTS}
    assert "push2.eastmoney.com" in hosts
    assert "hq.sinajs.cn" in hosts
    assert "qt.gtimg.cn" in hosts
    assert all(port == 443 for _host, port in netcheck.DEFAULT_PROBE_HOSTS)


def test_probe_failures_do_not_log():
    """探测失败必须静默：不产生任何 error/warning 日志（入口预检的去噪要求）。"""
    with patch.object(
        socket, "create_connection", side_effect=OSError("offline")
    ), patch("core.netcheck.logger") as mock_logger:
        assert netcheck.is_online(hosts=[("a.example", 443)]) is False
        mock_logger.error.assert_not_called()
        mock_logger.warning.assert_not_called()
