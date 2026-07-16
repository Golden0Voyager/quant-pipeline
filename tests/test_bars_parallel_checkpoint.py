"""Tests for parallel checkpoint in tasks/bars.py - batch order vs completion order."""
from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

import daily_pipeline
from core.progress import ProgressTracker


def test_parallel_checkpoint_saves_batch_boundary():
    """Parallel checkpoint must use batch-ordered last_symbol, not as_completed order."""
    db = MagicMock()
    loader = MagicMock()
    loader.incremental_update.return_value = pd.DataFrame()
    db.get_daily_bars.return_value = pd.DataFrame()

    stocks_df = pd.DataFrame({
        "code": ["000001", "000002", "000003", "000004"],
        "name": ["A", "B", "C", "D"],
    })
    db.get_stock_list.return_value = stocks_df

    # Create pre-resolved futures for each symbol
    futures = {
        sym: Future() for sym in ["000001", "000002", "000003", "000004"]
    }
    for f in futures.values():
        f.set_result("success")

    # ThreadPoolExecutor.submit returns the matching future
    def mock_submit(_fn, _db, _loader, symbol, **_kw):
        return futures[symbol]

    # as_completed returns futures in REVERSED order: D, C, B, A
    reversed_order = [futures[sym] for sym in ("000004", "000003", "000002", "000001")]

    save_calls: list[dict] = []

    def capture_save(**kw: object) -> None:
        save_calls.append(kw)

    with patch("core.utils.is_beijing_stock", return_value=False), \
         patch("daily_pipeline.logger"), \
         patch("tasks.bars.logger"), \
         patch("tasks.bars.PARALLEL_WORKERS", 2), \
         patch("tasks.bars.BATCH_SIZE", 4), \
         patch("tasks.bars.PROGRESS_FLUSH_INTERVAL", 1), \
         patch.object(ThreadPoolExecutor, "submit", side_effect=mock_submit), \
         patch("tasks.bars.as_completed", return_value=reversed_order):
        with patch.object(ProgressTracker, "save", side_effect=capture_save):
            ProgressTracker.clear()
            daily_pipeline.update_bars(db, loader)

    # After fix: batch-end save must use batch[-1] ("000004"), not "000001"
    # (which would be the last symbol from reversed as_completed order)
    if save_calls:
        last_save = save_calls[-1]
        assert last_save["last_symbol"] in ("000004",), \
            f"Expected batch[-1]='000004', got '{last_save['last_symbol']}'"
    ProgressTracker.clear()
