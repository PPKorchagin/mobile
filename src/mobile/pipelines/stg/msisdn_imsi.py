"""Сборка ``stg_msisdn_imsi``: интервалы MSISDN–IMSI из ``event_dds`` за отчётный день."""

from __future__ import annotations

import json
import logging
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from mobile.cli_defaults import DEFAULT_PARQUET_COMPRESSION
from mobile.command_timing import append_command_metrics, timed_stage
from mobile.pipelines.stg.event_dds_reader import parse_event_timestamps, read_event_dds_for_report_date
from mobile.pipelines.stg.subscriber_ids import normalize_imsi, normalize_msisdn
from mobile.project_paths import (
    DEFAULT_STG_EVENT_DDS_ROOT,
    DEFAULT_STG_MSISDN_IMSI_SCHEMA_PATH,
    resolve_project_path,
    stg_msisdn_imsi_output_path,
)

logger = logging.getLogger(__name__)

STG_MSISDN_IMSI_TABLE = "stg_msisdn_imsi"
STG_MSISDN_IMSI_FIELDS: list[dict[str, str]] = [
    {"name": "msisdn", "type": "string"},
    {"name": "imsi", "type": "string"},
    {"name": "valid_from", "type": "timestamp"},
    {"name": "valid_to", "type": "timestamp"},
]
_PAIR_VALUE_COL = "imsi"


def _load_schema_contract(schema_path: Path) -> None:
    global STG_MSISDN_IMSI_TABLE, STG_MSISDN_IMSI_FIELDS
    with schema_path.open(encoding="utf-8") as file:
        cfg = json.load(file)
    STG_MSISDN_IMSI_TABLE = str(cfg.get("table", STG_MSISDN_IMSI_TABLE))
    STG_MSISDN_IMSI_FIELDS = [
        {"name": str(f["name"]), "type": str(f["type"])} for f in cfg.get("fields", STG_MSISDN_IMSI_FIELDS)
    ]


_load_schema_contract(DEFAULT_STG_MSISDN_IMSI_SCHEMA_PATH)


def run_build(
    report_date: date,
    *,
    event_dds_path: str | Path | None = None,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    """Собрать ``stg_msisdn_imsi`` за ``report_date``."""
    out = (
        resolve_project_path(output_path)
        if output_path is not None
        else stg_msisdn_imsi_output_path(report_date)
    )
    return _run_build(
        command="build-stg-msisdn-imsi",
        report_date=report_date,
        event_dds_path=event_dds_path,
        output_path=out,
        value_col=_PAIR_VALUE_COL,
        normalize_value=normalize_imsi,
    )


def _run_build(
    *,
    command: str,
    report_date: date,
    event_dds_path: str | Path | None,
    output_path: Path,
    value_col: str,
    normalize_value: Callable[[pd.Series | None], pd.Series],
) -> dict[str, Any]:
    perf: dict[str, Any] = {}
    started = time.perf_counter()
    day_start = datetime.combine(report_date, datetime.min.time())
    day_end = datetime.combine(report_date, datetime.max.time())
    field_names = [f["name"] for f in STG_MSISDN_IMSI_FIELDS]

    with timed_stage("read_events_sec", perf):
        raw = read_event_dds_for_report_date(report_date, event_dds_path)

    with timed_stage("prepare_events_sec", perf):
        events = _prepare_pair_events(raw, value_col=value_col, normalize_value=normalize_value)

    with timed_stage("build_intervals_sec", perf):
        intervals = _build_temporal_intervals(
            events,
            value_col=value_col,
            period_start=day_start,
            period_end=day_end,
        )

    with timed_stage("write_sec", perf):
        result = _coerce_output(intervals, field_names, value_col=value_col)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        result.to_parquet(output_path, compression=DEFAULT_PARQUET_COMPRESSION, index=False)

    stats: dict[str, Any] = {
        "command": command,
        "table": STG_MSISDN_IMSI_TABLE,
        "report_date": report_date.isoformat(),
        "event_dds_path": str(resolve_project_path(event_dds_path or DEFAULT_STG_EVENT_DDS_ROOT)),
        "output_path": str(output_path),
        "event_rows_read": int(len(raw)),
        "event_rows_with_pair": int(len(events)),
        "interval_rows": int(len(result)),
        "distinct_msisdn": int(result["msisdn"].nunique()) if not result.empty else 0,
    }
    perf["elapsed_total_sec"] = round(time.perf_counter() - started, 4)
    append_command_metrics(command=command, metrics={**stats, **perf})
    logger.info("%s completed: %s", command, stats)
    return {**stats, **perf}


def _prepare_pair_events(
    raw: pd.DataFrame,
    *,
    value_col: str,
    normalize_value: Callable[[pd.Series | None], pd.Series],
) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame(columns=["msisdn", value_col, "event_ts"])

    work = raw.copy()
    work["event_ts"] = parse_event_timestamps(work["event_timestamp"])
    work["msisdn"] = normalize_msisdn(work.get("msisdn"))
    work[value_col] = normalize_value(work.get(value_col))
    work = work[work["msisdn"].notna() & work[value_col].notna() & work["event_ts"].notna()]
    return work[["msisdn", value_col, "event_ts"]].reset_index(drop=True)


def _build_temporal_intervals(
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


def _coerce_output(df: pd.DataFrame, field_names: list[str], *, value_col: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=field_names)
    out = df.copy()
    out["msisdn"] = normalize_msisdn(out["msisdn"])
    out[value_col] = normalize_imsi(out[value_col])
    out["valid_from"] = pd.to_datetime(out["valid_from"], errors="coerce")
    out["valid_to"] = pd.to_datetime(out["valid_to"], errors="coerce")
    out = out.dropna(subset=field_names)
    return out[field_names].reset_index(drop=True)
