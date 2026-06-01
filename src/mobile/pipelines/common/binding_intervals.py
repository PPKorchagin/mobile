"""Shared MSISDN binding interval logic (IMEI / IMSI from stg_geo_all)."""

from __future__ import annotations

import logging
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from mobile.pipelines.fct.subscriber_ids import normalize_msisdn
from mobile.project_paths import resolve_project_path, resolve_stg_daily_parquet_path, stg_geo_all_output_path

logger = logging.getLogger(__name__)


def drop_intervals_overlapping_day(
    frame: pd.DataFrame,
    *,
    day_start: datetime | pd.Timestamp,
    day_end: datetime | pd.Timestamp,
) -> pd.DataFrame:
    if frame.empty:
        return frame
    work = frame.copy()
    work["valid_from"] = pd.to_datetime(work["valid_from"], errors="coerce")
    work["valid_to"] = pd.to_datetime(work["valid_to"], errors="coerce")
    start = pd.Timestamp(day_start)
    end = pd.Timestamp(day_end)
    overlap = (
        work["valid_from"].notna()
        & work["valid_to"].notna()
        & (work["valid_from"] <= end)
        & (work["valid_to"] >= start)
    )
    return work.loc[~overlap].reset_index(drop=True)


def prepare_pair_events(
    raw: pd.DataFrame,
    *,
    value_col: str,
    normalize_value: Callable[[pd.Series | None], pd.Series],
) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame(columns=["msisdn", value_col, "event_ts"])

    work = raw.copy()
    work["event_ts"] = pd.to_datetime(work.get("start_time_utc"), errors="coerce")
    work["msisdn"] = normalize_msisdn(work.get("msisdn"))
    work[value_col] = normalize_value(work.get(value_col))
    work = work[work["msisdn"].notna() & work[value_col].notna() & work["event_ts"].notna()]
    return work[["msisdn", value_col, "event_ts"]].reset_index(drop=True)


def build_temporal_intervals(
    events: pd.DataFrame,
    *,
    value_col: str,
    period_start: datetime,
    period_end: datetime,
) -> pd.DataFrame:
    if events.empty:
        return pd.DataFrame(columns=["msisdn", value_col, "valid_from", "valid_to"])

    rows: list[dict[str, Any]] = []
    sorted_events = events.sort_values(["msisdn", "event_ts"], kind="mergesort")

    for msisdn, group in sorted_events.groupby("msisdn", sort=False):
        current_val: str | None = None
        seg_start: pd.Timestamp | None = None
        seg_end: pd.Timestamp | None = None

        for row in group.itertuples(index=False):
            val = str(getattr(row, value_col))
            ts = getattr(row, "event_ts")
            if current_val is None:
                current_val, seg_start, seg_end = val, ts, ts
                continue
            if val != current_val:
                rows.append(
                    {
                        "msisdn": msisdn,
                        value_col: current_val,
                        "valid_from": seg_start,
                        "valid_to": seg_end,
                    }
                )
                current_val, seg_start, seg_end = val, ts, ts
            else:
                seg_end = ts

        if current_val is not None and seg_start is not None and seg_end is not None:
            rows.append(
                {
                    "msisdn": msisdn,
                    value_col: current_val,
                    "valid_from": seg_start,
                    "valid_to": seg_end,
                }
            )

    if not rows:
        return pd.DataFrame(columns=["msisdn", value_col, "valid_from", "valid_to"])

    out = pd.DataFrame(rows)
    out["valid_from"] = pd.to_datetime(out["valid_from"], errors="coerce").clip(lower=period_start)
    out["valid_to"] = pd.to_datetime(out["valid_to"], errors="coerce").clip(upper=period_end)
    return out.loc[out["valid_from"] <= out["valid_to"]].reset_index(drop=True)


def merge_adjacent_intervals(
    frame: pd.DataFrame,
    *,
    group_cols: list[str],
    gap_seconds: int = 1,
) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=group_cols + ["valid_from", "valid_to"])

    sort_cols = list(group_cols) + ["valid_from"]
    work = frame.sort_values(sort_cols, kind="mergesort").reset_index(drop=True)
    rows: list[dict[str, Any]] = []
    gap = pd.Timedelta(seconds=gap_seconds)

    for key, group in work.groupby(group_cols, sort=False, dropna=False):
        if not isinstance(key, tuple):
            key = (key,)
        seg_start: pd.Timestamp | None = None
        seg_end: pd.Timestamp | None = None
        row_base: dict[str, Any] = dict(zip(group_cols, key, strict=True))

        for row in group.itertuples(index=False):
            start = pd.Timestamp(getattr(row, "valid_from"))
            end = pd.Timestamp(getattr(row, "valid_to"))
            if seg_start is None:
                seg_start, seg_end = start, end
                continue
            if start <= seg_end + gap:
                seg_end = max(seg_end, end)
            else:
                rows.append({**row_base, "valid_from": seg_start, "valid_to": seg_end})
                seg_start, seg_end = start, end
        if seg_start is not None and seg_end is not None:
            rows.append({**row_base, "valid_from": seg_start, "valid_to": seg_end})

    return pd.DataFrame(rows)


def resolve_geo_all_source_path(report_date: date, source_path: str | Path | None) -> Path:
    if source_path is None:
        return stg_geo_all_output_path(report_date)
    return resolve_stg_daily_parquet_path(source_path, report_date)


def read_geo_all_day(
    report_date: date,
    source_path: Path,
    *,
    log_prefix: str,
) -> pd.DataFrame:
    columns = ["msisdn", "imsi", "imei", "start_time_utc"]
    if not source_path.exists():
        logger.warning("%s: stg_geo_all not found for %s at %s", log_prefix, report_date, source_path)
        return pd.DataFrame(columns=columns)
    try:
        return pd.read_parquet(source_path, columns=columns)
    except Exception:
        logger.exception("%s: failed to read stg_geo_all at %s", log_prefix, source_path)
        return pd.DataFrame(columns=columns)
