"""core.lock 覆盖率测试：ProcessLock 加锁/释放/冲突拒绝路径。

fcntl / os / sys / atexit / signal 全部 mock，避免真实写 PID 文件或退出进程；
另有若干用例使用真实 flock + 真实文件，专测「锁文件不得 unlink」这一互斥前提。
"""
from __future__ import annotations

import contextlib
import io
from unittest.mock import MagicMock, patch

import pytest

import core.lock as lock_mod
from core.lock import ProcessLock, TaskLock, skip_if_task_locked, task_lock


def _make_pidfile() -> MagicMock:
    """模拟 PID 文件路径对象，unlink 安全可调用。"""
    p = MagicMock()
    p.unlink = MagicMock()
    return p


def _reset():
    ProcessLock._lock_file_fd = None
    TaskLock._fds.clear()


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
        # 锁文件是稳定的锁锚点：release 只清空内容，绝不 unlink
        pidfile.unlink.assert_not_called()
        fake_fd.truncate.assert_called_with(0)


def test_acquire_conflict_refuses_when_pid_not_yet_written():
    """flock 被持有但 PID 内容为空（持有者刚加锁、尚未写入）→ 必须拒绝启动。

    旧实现在此处 unlink 锁文件后递归加锁，本进程于是在**新 inode** 上加锁
    成功，与真正的持有者形成双持有：两个 pipeline 并发写同一个库。
    """
    _reset()
    pidfile = _make_pidfile()
    fd = MagicMock(spec=io.TextIOWrapper)
    fd.read.return_value = ""

    class _ExitError(Exception):
        pass

    with patch.object(lock_mod, "_PIDFILE", pidfile), patch(
        "core.lock.open", return_value=fd
    ), patch("core.lock.fcntl") as mock_fcntl, patch("core.lock.os") as mock_os, patch(
        "core.lock.atexit"
    ), patch("core.lock.signal"), patch("core.lock.sys") as mock_sys:
        mock_fcntl.flock.side_effect = OSError("locked")
        mock_sys.exit.side_effect = _ExitError()
        with contextlib.suppress(_ExitError):
            ProcessLock.acquire()
        mock_sys.exit.assert_called_once_with(1)
        # 不再递归重试：只尝试过一次 flock、也只 open 过一次
        assert mock_fcntl.flock.call_count == 1
        # 存活判定完全交给 flock，PID 与否不影响结果
        mock_os.kill.assert_not_called()
    pidfile.unlink.assert_not_called()
    assert ProcessLock._lock_file_fd is None
    _reset()


def test_acquire_conflict_refuses_even_when_pid_is_dead():
    """残留 PID（进程已退出）但 flock 仍被持有 → 仍然拒绝，不自愈。

    flock 随持有进程退出由内核释放，所以「flock 失败」必然意味着持有者活着；
    此时把文件当成残留、删掉它重加锁，就是双持有的来源。
    """
    _reset()
    pidfile = _make_pidfile()
    fd = MagicMock(spec=io.TextIOWrapper)
    fd.read.return_value = "999999"

    class _ExitError(Exception):
        pass

    with patch.object(lock_mod, "_PIDFILE", pidfile), patch(
        "core.lock.open", return_value=fd
    ), patch("core.lock.fcntl") as mock_fcntl, patch("core.lock.os") as mock_os, patch(
        "core.lock.atexit"
    ), patch("core.lock.signal"), patch("core.lock.sys") as mock_sys:
        mock_fcntl.flock.side_effect = OSError("locked")
        mock_os.kill.side_effect = OSError("no such process")
        mock_sys.exit.side_effect = _ExitError()
        with contextlib.suppress(_ExitError):
            ProcessLock.acquire()
        mock_sys.exit.assert_called_once_with(1)
        assert mock_fcntl.flock.call_count == 1
    pidfile.unlink.assert_not_called()
    _reset()


def test_acquire_refuses_real_lock_held_by_other_description(tmp_path):
    """真实 flock 场景：持有者锁仍在时，第二个 acquire 不得破坏锚点。

    这是旧实现的真实后果回归测试 —— 旧代码会 unlink + 递归加锁成功，
    随后 global_lock_held() 对仍在持有锁的进程返回 False（互斥完全失效）。
    """
    import fcntl as real_fcntl

    _reset()
    pidfile = tmp_path / "daily_pipeline.pid"
    holder = open(pidfile, "a+", buffering=1)  # noqa: SIM115
    real_fcntl.flock(holder, real_fcntl.LOCK_EX | real_fcntl.LOCK_NB)
    holder.seek(0)
    holder.truncate(0)
    holder.flush()  # 持有者尚未写入 PID 的窗口
    try:
        with patch.object(lock_mod, "_PIDFILE", pidfile), patch(
            "core.lock.atexit"
        ), patch("core.lock.signal"):
            with pytest.raises(SystemExit) as exc:
                ProcessLock.acquire()
            assert exc.value.code == 1
            # 锚点未被破坏（旧实现会 unlink），且真实持有者的锁依然被探测到
            assert pidfile.exists()
            assert lock_mod.global_lock_held() is True
    finally:
        real_fcntl.flock(holder, real_fcntl.LOCK_UN)
        holder.close()
        _reset()


def test_release_keeps_lockfile_with_empty_pid(tmp_path):
    """真实文件：release 后锁文件仍在（否则并发启动可双持有），内容清空。"""
    _reset()
    pidfile = tmp_path / "daily_pipeline.pid"
    with patch.object(lock_mod, "_PIDFILE", pidfile), patch(
        "core.lock.atexit"
    ), patch("core.lock.signal"):
        ProcessLock.acquire()
        assert pidfile.read_text().strip().isdigit()
        ProcessLock.release()
        assert pidfile.exists()
        assert pidfile.read_text().strip() == ""
        # 锁确实已释放
        assert lock_mod.global_lock_held() is False
        # 释放后可重新加锁（锚点复用同一 inode）
        ProcessLock.acquire()
        assert pidfile.read_text().strip().isdigit()
        ProcessLock.release()
        _reset()


def test_acquire_alive_lock_exits():
    _reset()
    pidfile = _make_pidfile()
    fake_fd = MagicMock(spec=io.TextIOWrapper)

    class _ExitError(Exception):
        pass

    with patch.object(lock_mod, "_PIDFILE", pidfile), patch(
        "core.lock.open", return_value=fake_fd
    ), patch("core.lock.fcntl") as mock_fcntl, patch("core.lock.os") as mock_os, patch(
        "core.lock.atexit"
    ), patch("core.lock.signal"), patch("core.lock.sys") as mock_sys:
        # 第一个锁由别的存活进程持有：flock 抛 OSError，pid 存活
        mock_fcntl.flock.side_effect = OSError("locked")
        mock_os.kill.return_value = None  # 不抛异常 → alive=True
        mock_sys.exit.side_effect = _ExitError()
        with contextlib.suppress(_ExitError):
            ProcessLock.acquire()
        mock_sys.exit.assert_called_once_with(1)


def test_release_when_not_locked():
    _reset()
    # 未加锁时 release 应为安全 no-op
    with patch.object(lock_mod, "_PIDFILE", _make_pidfile()), patch(
        "core.lock.fcntl"
    ), patch("core.lock.os"):
        ProcessLock.release()  # 不应抛异常


def test_global_lock_held_probes_other_process_lock(tmp_path):
    """global_lock_held 非阻塞试探：被持有时 True，空闲时 False，且不泄露锁。"""
    import fcntl as real_fcntl

    pidfile = tmp_path / "daily_pipeline.pid"
    with patch.object(lock_mod, "_PIDFILE", pidfile):
        assert lock_mod.global_lock_held() is False
        fd = open(pidfile, "a+")  # noqa: SIM115
        real_fcntl.flock(fd, real_fcntl.LOCK_EX | real_fcntl.LOCK_NB)
        try:
            assert lock_mod.global_lock_held() is True
        finally:
            real_fcntl.flock(fd, real_fcntl.LOCK_UN)
            fd.close()
        # 试探本身不得残留锁
        assert lock_mod.global_lock_held() is False


def test_task_lock_context_acquires_and_releases():
    _reset()
    fake_fd = MagicMock(spec=io.TextIOWrapper)

    with patch("core.lock.open", return_value=fake_fd) as mock_open, \
         patch("core.lock.fcntl") as mock_fcntl:
        with task_lock("update_chip_distribution_em") as acquired:
            assert acquired is True
            assert "update_chip_distribution_em" in TaskLock._fds
        mock_open.assert_called_once()
        mock_fcntl.flock.assert_any_call(fake_fd, mock_fcntl.LOCK_EX | mock_fcntl.LOCK_NB)
        mock_fcntl.flock.assert_any_call(fake_fd, mock_fcntl.LOCK_UN)
        fake_fd.close.assert_called_once()


def test_task_lock_keeps_lockfile_and_blocks_second_holder(tmp_path):
    """真实文件：任务锁释放后不 unlink，并发同名任务该锁仍能正确互斥。"""
    _reset()
    lock_path = tmp_path / "daily_pipeline_update_industry.lock"
    with patch.object(lock_mod, "_task_lock_path", return_value=lock_path):
        assert TaskLock.acquire("update_industry") is True
        assert lock_path.read_text().strip().isdigit()
        # 同一进程重复 acquire 幂等（不重建文件）
        assert TaskLock.acquire("update_industry") is True
        TaskLock.release("update_industry")
        # 文件保留、内容清空
        assert lock_path.exists()
        assert lock_path.read_text().strip() == ""
        # 释放后可再次获取
        assert TaskLock.acquire("update_industry") is True
        TaskLock.release("update_industry")
    _reset()


def test_task_lock_context_reports_conflict():
    _reset()
    fake_fd = MagicMock(spec=io.TextIOWrapper)

    with patch("core.lock.open", return_value=fake_fd), \
         patch("core.lock.fcntl") as mock_fcntl:
        mock_fcntl.flock.side_effect = OSError("locked")
        with task_lock("update_industry") as acquired:
            assert acquired is False
        fake_fd.close.assert_called_once()


def test_skip_if_task_locked_returns_locked_result_on_conflict():
    _reset()

    @skip_if_task_locked("update_industry")
    def _task() -> dict[str, object]:
        return {"saved": 1}

    with patch("core.lock.TaskLock.acquire", return_value=False), \
         patch("core.lock.TaskLock.release") as release, \
         patch("core.lock.logger"):
        result = _task()

    assert result["status"] == "locked"
    assert result["skipped"] is True
    release.assert_not_called()
