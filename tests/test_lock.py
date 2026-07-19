"""core.lock 覆盖率测试：ProcessLock 加锁/释放/冲突清理路径。

fcntl / os / sys / atexit / signal 全部 mock，避免真实写 PID 文件或退出进程。
"""
from __future__ import annotations

import io
from unittest.mock import MagicMock, patch

import core.lock as lock_mod
from core.lock import ProcessLock


def _make_pidfile() -> MagicMock:
    """模拟 PID 文件路径对象，unlink 安全可调用。"""
    p = MagicMock()
    p.unlink = MagicMock()
    return p


def _reset():
    ProcessLock._lock_file_fd = None


def test_acquire_success_and_idempotent():
    _reset()
    pidfile = _make_pidfile()
    fake_fd = MagicMock(spec=io.TextIOWrapper)
    with patch.object(lock_mod, "_PIDFILE", pidfile), patch(
        "core.lock.open", return_value=fake_fd
    ) as mock_open, patch("core.lock.fcntl") as mock_fcntl, patch(
        "core.lock.atexit"
    ) as mock_atexit, patch("core.lock.signal") as mock_signal:
        # 第一次加锁成功
        ProcessLock.acquire()
        mock_open.assert_called_once()
        mock_fcntl.flock.assert_called_once()
        mock_atexit.register.assert_called_once()
        mock_signal.signal.assert_called()
        # 第二次加锁应为幂等（已加锁直接返回），不再打开文件
        ProcessLock.acquire()
        assert mock_open.call_count == 1
        # 释放
        ProcessLock.release()
        mock_fcntl.flock.assert_called_with(fake_fd, mock_fcntl.LOCK_UN)
        fake_fd.close.assert_called_once()
        pidfile.unlink.assert_called_once_with(missing_ok=True)


def test_acquire_stale_lock_auto_cleanup():
    _reset()
    pidfile = _make_pidfile()
    stale = MagicMock(spec=io.TextIOWrapper)
    stale.read.return_value = "999999"
    fresh = MagicMock(spec=io.TextIOWrapper)

    with patch.object(lock_mod, "_PIDFILE", pidfile), patch(
        "core.lock.open", side_effect=[stale, fresh]
    ), patch("core.lock.fcntl") as mock_fcntl, patch(
        "core.lock.os"
    ) as mock_os, patch("core.lock.atexit"), patch("core.lock.signal"), patch(
        "core.lock.sys"
    ) as mock_sys:
        # 第一次 flock 抛 OSError 进入冲突分支；残留 pid 不存在 → alive=False
        # → stale 清理后递归 acquire；锁冲突分支末尾会 sys.exit(1)，mock 掉避免真退出
        calls = {"n": 0}

        def _flock_effect(*args, **kwargs):
            calls["n"] += 1
            if calls["n"] == 1:
                raise OSError("locked")
            return None

        mock_fcntl.flock.side_effect = _flock_effect
        mock_os.kill.side_effect = OSError("no such process")
        mock_sys.exit.return_value = None
        ProcessLock.acquire()
        # 冲突分支 flock 抛错 + 清理后重加锁 flock 成功，至少两次调用
        assert mock_fcntl.flock.call_count >= 2
        mock_os.kill.assert_called_once_with(999999, 0)
        ProcessLock.release()
        pidfile.unlink.assert_called_with(missing_ok=True)


def test_acquire_alive_lock_exits():
    _reset()
    pidfile = _make_pidfile()
    fake_fd = MagicMock(spec=io.TextIOWrapper)

    class _Exit(Exception):
        pass

    with patch.object(lock_mod, "_PIDFILE", pidfile), patch(
        "core.lock.open", return_value=fake_fd
    ), patch("core.lock.fcntl") as mock_fcntl, patch("core.lock.os") as mock_os, patch(
        "core.lock.atexit"
    ), patch("core.lock.signal"), patch("core.lock.sys") as mock_sys:
        # 第一个锁由别的存活进程持有：flock 抛 OSError，pid 存活
        mock_fcntl.flock.side_effect = OSError("locked")
        mock_os.kill.return_value = None  # 不抛异常 → alive=True
        mock_sys.exit.side_effect = _Exit()
        try:
            ProcessLock.acquire()
        except _Exit:
            pass
        mock_sys.exit.assert_called_once_with(1)


def test_release_when_not_locked():
    _reset()
    # 未加锁时 release 应为安全 no-op
    with patch.object(lock_mod, "_PIDFILE", _make_pidfile()), patch(
        "core.lock.fcntl"
    ), patch("core.lock.os"):
        ProcessLock.release()  # 不应抛异常
