"""Склейка интервалов MSISDN↔IMSI/IMEI и инкрементальное обновление месячного parquet."""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from mobile.cli_defaults import DEFAULT_PARQUET_COMPRESSION
from mobile.pipelines.stg.subscriber_ids import normalize_msisdn
from mobile.project_paths import stg_geo_all_output_path

logger = logging.getLogger(__name__)


def drop_intervals_overlapping_day(
    frame: pd.DataFrame,
    *,
    day_start: datetime | pd.Timestamp,
    day_end: datetime | pd.Timestamp,
) -> pd.DataFrame:
    """Убрать строки, пересекающие календарный день (идемпотентный пересчёт дня)."""
    if frame.empty:
        return frame
    work = frame.copy()
    work["valid_from"] = pd.to_datetime(work["valid_from"], errors="coerce")
    work["valid_to"] = pd.to_datetime(work["valid_to"], errors="coerce")
    start = pd.Timestamp(day_start)
    end = pd.Timestamp(day_end)
    overlap = work["valid_from"].notna() & work["valid_to"].notna() & (work["valid_from"] <= end) & (work["valid_to"] >= start)
    return work.loc[~overlap].reset_index(drop=True)


def merge_binding_intervals(frame: pd.DataFrame, *, value_col: str) -> pd.DataFrame:
    """Склеить смежные интервалы с одним (msisdn, value_col)."""
    if frame.empty:
        return pd.DataFrame(columns=["msisdn", value_col, "valid_from", "valid_to"])

    work = frame.sort_values(["msisdn", value_col, "valid_from"], kind="mergesort").reset_index(drop=True)
    rows: list[dict[str, Any]] = []
    for (msisdn, value), group in work.groupby(["msisdn", value_col], sort=False):
        seg_start: pd.Timestamp | None = None
        seg_end: pd.Timestamp | None = None
        for row in group.itertuples(index=False):
            start = pd.Timestamp(getattr(row, "valid_from"))
            end = pd.Timestamp(getattr(row, "valid_to"))
            if seg_start is None:
                seg_start, seg_end = start, end
                continue
            if start <= seg_end + pd.Timedelta(seconds=1):
                seg_end = max(seg_end, end)
            else:
                rows.append(
                    {
                        "msisdn": msisdn,
                        value_col: value,
                        "valid_from": seg_start,
                        "valid_to": seg_end,
                    }
                )
                seg_start, seg_end = start, end
        if seg_start is not None and seg_end is not None:
            rows.append(
                {
                    "msisdn": msisdn,
                    value_col: value,
                    "valid_from": seg_start,
                    "valid_to": seg_end,
                }
            )
    return pd.DataFrame(rows)


def upsert_daily_into_month_parquet(
    *,
    month_path: Path,
    day_intervals: pd.DataFrame,
    value_col: str,
    day_start: datetime,
    day_end: datetime,
    field_names: list[str],
    normalize_value: Callable[[pd.Series | None], pd.Series],
) -> pd.DataFrame:
    """Добавить суточные интервалы в месячный файл: снять старый вклад дня, merge, записать."""
    day_part = _coerce_binding_frame(day_intervals, field_names=field_names, value_col=value_col, normalize_value=normalize_value)
    existing = pd.DataFrame(columns=field_names)
    if month_path.exists():
        existing = pd.read_parquet(month_path, columns=field_names)
        existing = drop_intervals_overlapping_day(existing, day_start=day_start, day_end=day_end)
    combined = pd.concat([existing, day_part], ignore_index=True)
    merged = merge_binding_intervals(combined, value_col=value_col)
    result = _coerce_binding_frame(merged, field_names=field_names, value_col=value_col, normalize_value=normalize_value)
    month_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_parquet(month_path, compression=DEFAULT_PARQUET_COMPRESSION, index=False)
    return result


def _coerce_binding_frame(
    df: pd.DataFrame,
    *,
    field_names: list[str],
    value_col: str,
    normalize_value: Callable[[pd.Series | None], pd.Series],
) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=field_names)
    out = df.copy()
    out["msisdn"] = normalize_msisdn(out["msisdn"])
    out[value_col] = normalize_value(out[value_col])
    out["valid_from"] = pd.to_datetime(out["valid_from"], errors="coerce")
    out["valid_to"] = pd.to_datetime(out["valid_to"], errors="coerce")
    out = out.dropna(subset=field_names)
    return out[field_names].reset_index(drop=True)


def month_days(report_month: date) -> list[date]:
    """Календарные дни месяца (``report_month`` = 1-е число)."""
    start = report_month.replace(day=1)
    end = (pd.Timestamp(start) + pd.offsets.MonthEnd(0)).date()
    days: list[date] = []
    cursor = start
    while cursor <= end:
        days.append(cursor)
        cursor += timedelta(days=1)
    return days


def refresh_month_bindings_from_geo(report_month: date) -> dict[str, int]:
    """Пересобрать месячные binding из всех ``stg_geo_all`` за дни месяца (по одному дню)."""
    from mobile.pipelines.stg import msisdn_imei, msisdn_imsi

    days_run = 0
    for day in month_days(report_month):
        if not stg_geo_all_output_path(day).exists():
            continue
        msisdn_imsi.run_build(day)
        msisdn_imei.run_build(day)
        days_run += 1
    logger.info("refresh_month_bindings_from_geo: %s days updated for %s", days_run, report_month.isoformat())
    return {"binding_days_refreshed": days_run}
