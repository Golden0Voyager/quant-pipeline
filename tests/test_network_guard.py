"""
出网守卫自身的门禁。

``tests/conftest.py::_block_outbound_network`` 是 autouse 的，没有测试断言它
有效的话，把守卫写坏（例如 patch 点写错、判定条件写反）会**静默地变成一个
什么都不拦的 fixture**——套件照样全绿，但依赖实时网络的用例又回来了，而且照样
可能「通过」（正是 2026-10-13 踩到的那种假绿）。

因此这里既验证「拦得住」，也验证「放得过」：只测前者的话，一个把所有连接
（含回环）一律拦掉的实现同样能骗过门禁。
"""
from __future__ import annotations

import os
import socket
from pathlib import Path

import pytest


class TestOutboundNetworkIsBlocked:
    def test_public_address_is_refused(self, _block_outbound_network):
        sock = socket.socket()
        sock.settimeout(1.0)
        try:
            with pytest.raises(OSError, match="禁止外网连接"):
                sock.connect(("1.2.3.4", 80))
        finally:
            sock.close()
        assert _block_outbound_network == [("1.2.3.4", 80)]

    def test_real_data_source_host_is_refused(self, _block_outbound_network):
        """点名真实数据源：守卫不是只对随手写的示例地址生效。"""
        for host in ("push2his.eastmoney.com", "push2.eastmoney.com", "api.day.app"):
            sock = socket.socket()
            sock.settimeout(1.0)
            try:
                with pytest.raises(OSError):
                    sock.connect((host, 443))
            finally:
                sock.close()
        assert len(_block_outbound_network) == 3

    def test_connect_ex_is_refused_too(self, _block_outbound_network):
        """``connect_ex`` 返回错误码而不抛异常，只堵 connect 会漏掉它。"""
        sock = socket.socket()
        sock.settimeout(1.0)
        try:
            with pytest.raises(OSError):
                sock.connect_ex(("1.2.3.4", 80))
        finally:
            sock.close()

    def test_create_connection_is_refused(self, _block_outbound_network):
        """``socket.create_connection`` 内部走 connect，应被同一个守卫覆盖。"""
        with pytest.raises(OSError):
            socket.create_connection(("1.2.3.4", 80), timeout=1.0)


class TestLoopbackIsAllowed:
    """守卫不能把回环也拦掉——否则会误伤本地临时服务类用例。"""

    @staticmethod
    def _listener():
        srv = socket.socket()
        srv.bind(("127.0.0.1", 0))
        srv.listen(1)
        return srv, srv.getsockname()

    def test_loopback_connection_succeeds(self, _block_outbound_network):
        srv, addr = self._listener()
        client = socket.socket()
        client.settimeout(2.0)
        try:
            client.connect(addr)
        finally:
            client.close()
            srv.close()
        assert _block_outbound_network == []

    def test_ipv6_loopback_is_allowed(self, _block_outbound_network):
        try:
            srv = socket.socket(socket.AF_INET6)
        except OSError:
            pytest.skip("本机无 IPv6")
        try:
            srv.bind(("::1", 0))
            srv.listen(1)
            client = socket.socket(socket.AF_INET6)
            client.settimeout(2.0)
            try:
                client.connect(srv.getsockname()[:2])
            finally:
                client.close()
        finally:
            srv.close()
        assert _block_outbound_network == []

    def test_unix_socket_address_is_allowed(self, _block_outbound_network):
        """AF_UNIX 传字符串地址，不该被当成 IP 判定。

        路径必须够短：macOS 的 sockaddr_un 上限约 104 字节，而 pytest 的
        ``tmp_path`` 在 macOS 下就可能超限（会报 "AF_UNIX path too long"，
        与守卫无关）。故用 /tmp 下的短名并自行清理。
        """
        path = f"/tmp/qpg_guard_{os.getpid()}.sock"
        srv = socket.socket(socket.AF_UNIX)
        try:
            srv.bind(path)
            srv.listen(1)
            client = socket.socket(socket.AF_UNIX)
            client.settimeout(2.0)
            try:
                client.connect(path)
            finally:
                client.close()
        finally:
            srv.close()
            Path(path).unlink(missing_ok=True)
        assert _block_outbound_network == []


class TestRealisticFailureMode:
    def test_refusal_looks_like_a_network_error_to_callers(self, _block_outbound_network):
        """抛 OSError 而非 AssertionError：数据源代码的 ``except OSError`` 要能接住，
        这样「网络故障路径」的用例照常成立，不会因为守卫换了异常类型而改变行为。"""
        sock = socket.socket()
        sock.settimeout(1.0)
        try:
            with pytest.raises(OSError) as exc:
                sock.connect(("1.2.3.4", 80))
        finally:
            sock.close()
        assert isinstance(exc.value, ConnectionError | OSError)
        assert not isinstance(exc.value, AssertionError)
