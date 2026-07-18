import os
import time
from pathlib import Path

from core.log_cleanup import CleanupResult, cleanup_logs


def test_cleanup_logs_all_excludes_active(tmp_path: Path) -> None:
    (tmp_path / "a.log").write_text("x")
    (tmp_path / "b.log").write_text("y" * 100)
    (tmp_path / "c.txt").write_text("z")
    result = cleanup_logs(tmp_path, keep_days=0, exclude={str(tmp_path / "a.log")})
    assert isinstance(result, CleanupResult)
    assert result.deleted_count == 1
    assert result.freed_bytes == 100
    assert (tmp_path / "a.log").exists()  # 排除项保留
    assert not (tmp_path / "b.log").exists()  # 已删除
    assert (tmp_path / "c.txt").exists()  # 非 .log 跳过


def test_cleanup_logs_keep_days_removes_older(tmp_path: Path) -> None:
    old = tmp_path / "old.log"
    old.write_text("old")
    new = tmp_path / "new.log"
    new.write_text("new")
    past = time.time() - 10 * 86400
    os.utime(old, (past, past))
    result = cleanup_logs(tmp_path, keep_days=7)
    assert result.deleted_count == 1
    assert not old.exists()
    assert new.exists()


def test_cleanup_logs_keep_zero_deletes_everything(tmp_path: Path) -> None:
    (tmp_path / "x.log").write_text("x")
    (tmp_path / "y.log").write_text("y")
    result = cleanup_logs(tmp_path, keep_days=0)
    assert result.deleted_count == 2
    assert not (tmp_path / "x.log").exists()


def test_cleanup_logs_missing_dir_records_error() -> None:
    result = cleanup_logs("/nonexistent/quant_data_path_xyz", keep_days=0)
    assert result.deleted_count == 0
    assert result.freed_bytes == 0
    assert result.errors  # 目录不存在应记录错误
