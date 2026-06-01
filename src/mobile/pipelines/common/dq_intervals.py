"""DQ helpers for temporal binding intervals (MSISDN ↔ IMSI/IMEI)."""

from __future__ import annotations

import pandas as pd


def count_overlapping_interval_rows(frame: pd.DataFrame, group_cols: tuple[str, ...]) -> int:
    """Rows participating in overlapping intervals within the same group."""
    if frame.empty:
        return 0
    subset = list(group_cols) + ["valid_from", "valid_to"]
    work = frame.dropna(subset=subset).copy()
    if work.empty:
        return 0
    overlap_rows = 0
    for _, group in work.groupby(list(group_cols), sort=False):
        ordered = group.sort_values("valid_from", kind="mergesort")
        prev_end: pd.Timestamp | None = None
        for row in ordered.itertuples(index=False):
            start = pd.Timestamp(row.valid_from)
            end = pd.Timestamp(row.valid_to)
            if prev_end is not None and start <= prev_end:
                overlap_rows += 2
            prev_end = end if prev_end is None else max(prev_end, end)
    return overlap_rows


def count_mergeable_adjacent_interval_rows(
    frame: pd.DataFrame,
    group_cols: tuple[str, ...],
    *,
    gap_seconds: int = 1,
) -> int:
    """Adjacent segments with gap ≤ ``gap_seconds`` that ETL should have merged."""
    if frame.empty:
        return 0
    subset = list(group_cols) + ["valid_from", "valid_to"]
    work = frame.dropna(subset=subset).copy()
    if work.empty:
        return 0
    mergeable = 0
    gap = pd.Timedelta(seconds=gap_seconds)
    for _, group in work.groupby(list(group_cols), sort=False):
        ordered = group.sort_values("valid_from", kind="mergesort")
        prev_end: pd.Timestamp | None = None
        for row in ordered.itertuples(index=False):
            start = pd.Timestamp(row.valid_from)
            end = pd.Timestamp(row.valid_to)
            if prev_end is not None and start <= prev_end + gap:
                mergeable += 1
            prev_end = end if prev_end is None else max(prev_end, end)
    return mergeable
