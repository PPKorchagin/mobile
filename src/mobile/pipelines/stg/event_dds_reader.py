"""Чтение ``event_dds`` за отчётную дату (общий вход для binding и DQ)."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Final

import pandas as pd

from mobile.project_paths import (
    DEFAULT_STG_EVENT_DDS_ROOT,
    resolve_project_path,
    started_parseable_mask,
    stg_event_dds_day_key_from_path,
)


def discover_event_dds_parquet_paths(path: Path, report_date: date) -> list[Path]:
    day_key = report_date.isoformat()
    if path.is_file():
        if path.suffix.lower() != ".parquet":
            return []
        key = stg_event_dds_day_key_from_path(path)
        if key is not None and key != day_key:
            return []
        return [path]
    if path.is_dir():
        day_dir = path / day_key
        if day_dir.is_dir():
            return sorted(day_dir.glob("*.parquet"))
        out: list[Path] = []
        for p in sorted(path.rglob("*.parquet")):
            key = stg_event_dds_day_key_from_path(p)
            if key is None or key == day_key:
                out.append(p)
        return out
    return []

BINDING_READ_COLUMNS: Final[list[str]] = [
    "event_timestamp",
    "imsi",
    "imei",
    "msisdn",
]


def read_event_dds_for_report_date(
    report_date: date,
    event_dds_path: str | Path | None = None,
) -> pd.DataFrame:
    """События за локальные сутки ``report_date`` из всех parquet дня."""
    root = resolve_project_path(event_dds_path or DEFAULT_STG_EVENT_DDS_ROOT)
    paths = discover_event_dds_parquet_paths(root, report_date)
    if not paths:
        return pd.DataFrame(columns=list(BINDING_READ_COLUMNS))

    parts: list[pd.DataFrame] = []
    for p in paths:
        try:
            parts.append(pd.read_parquet(p, columns=list(BINDING_READ_COLUMNS)))
        except Exception:
            continue
    if not parts:
        return pd.DataFrame(columns=list(BINDING_READ_COLUMNS))

    merged = pd.concat(parts, ignore_index=True)
    return _filter_by_local_report_date(merged, report_date)


def _filter_by_local_report_date(df: pd.DataFrame, report_date: date) -> pd.DataFrame:
    if df.empty or "event_timestamp" not in df.columns:
        return df
    day_str = report_date.strftime("%Y%m%d")
    s = df["event_timestamp"].astype("string").str.strip()
    mask = started_parseable_mask(s) & (s.str[:8] == day_str)
    if not bool(mask.any()):
        return df.iloc[0:0].copy()
    return df.loc[mask].copy()


def parse_event_timestamps(series: pd.Series) -> pd.Series:
    """``event_timestamp`` (YYYYMMDDhhmmss, локальное) → naive datetime."""
    s = series.astype("string").str.strip()
    return pd.to_datetime(s, format="%Y%m%d%H%M%S", errors="coerce")
