"""Сборка ``fct_msisdn_imei``: интервалы MSISDN–IMEI из ``stg_geo_all`` за отчётный день."""

from __future__ import annotations

import logging
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from mobile.cli_defaults import DEFAULT_PARQUET_COMPRESSION
from mobile.command_timing import append_command_metrics, timed_stage
from mobile.pipelines.common.binding_intervals import (
    build_temporal_intervals,
    drop_intervals_overlapping_day,
    merge_adjacent_intervals,
    prepare_pair_events,
    read_geo_all_day,
    resolve_geo_all_source_path,
)
from mobile.pipelines.common.schema_contract import apply_table_fields_to_module
from mobile.pipelines.fct.subscriber_ids import normalize_imei, normalize_msisdn
from mobile.project_paths import (
    DEFAULT_FCT_MSISDN_IMEI_SCHEMA_PATH,
    report_month_start,
    resolve_project_path,
)

logger = logging.getLogger(__name__)

STG_MSISDN_IMEI_TABLE = "fct_msisdn_imei"
FCT_MSISDN_IMEI_FIELDS: list[dict[str, str]] = [
    {"name": "msisdn", "type": "string"},
    {"name": "imei", "type": "string"},
    {"name": "valid_from", "type": "timestamp"},
    {"name": "valid_to", "type": "timestamp"},
]
_PAIR_VALUE_COL = "imei"

apply_table_fields_to_module(
    DEFAULT_FCT_MSISDN_IMEI_SCHEMA_PATH,
    table_name="STG_MSISDN_IMEI_TABLE",
    fields_name="FCT_MSISDN_IMEI_FIELDS",
    module_globals=globals(),
    default_table=STG_MSISDN_IMEI_TABLE,
    default_fields=FCT_MSISDN_IMEI_FIELDS,
)


def _merge_imei_intervals(frame: pd.DataFrame) -> pd.DataFrame:
    return merge_adjacent_intervals(frame, group_cols=["msisdn", _PAIR_VALUE_COL])


def _upsert_daily_into_month_parquet(
    *,
    month_path: Path,
    day_intervals: pd.DataFrame,
    day_start: datetime,
    day_end: datetime,
    field_names: list[str],
) -> pd.DataFrame:
    day_part = _coerce_output(day_intervals, field_names, value_col=_PAIR_VALUE_COL, normalize_value=normalize_imei)
    existing = pd.DataFrame(columns=field_names)
    if month_path.exists():
        existing = pd.read_parquet(month_path, columns=field_names)
        existing = drop_intervals_overlapping_day(existing, day_start=day_start, day_end=day_end)
    combined = pd.concat([existing, day_part], ignore_index=True)
    merged = _merge_imei_intervals(combined)
    result = _coerce_output(merged, field_names, value_col=_PAIR_VALUE_COL, normalize_value=normalize_imei)
    month_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_parquet(month_path, compression=DEFAULT_PARQUET_COMPRESSION, index=False)
    return result


def run_build(
    report_date: date,
    *,
    stg_geo_all_path: str | Path,
    output_path: str | Path,
    command: str = "build-fct-msisdn-imei",
) -> dict[str, Any]:
    """Собрать ``fct_msisdn_imei`` за ``report_date``."""
    out = resolve_project_path(output_path)
    geo = resolve_project_path(stg_geo_all_path)
    return _run_build(
        command=command,
        report_date=report_date,
        stg_geo_all_path=geo,
        output_path=out,
        value_col=_PAIR_VALUE_COL,
        normalize_value=normalize_imei,
    )


def _run_build(
    *,
    command: str,
    report_date: date,
    stg_geo_all_path: str | Path | None,
    output_path: Path,
    value_col: str,
    normalize_value: Callable[[pd.Series | None], pd.Series],
) -> dict[str, Any]:
    perf: dict[str, Any] = {}
    started = time.perf_counter()
    day_start = datetime.combine(report_date, datetime.min.time())
    day_end = datetime.combine(report_date, datetime.max.time())
    field_names = [f["name"] for f in FCT_MSISDN_IMEI_FIELDS]
    source_path = resolve_geo_all_source_path(report_date, stg_geo_all_path)

    with timed_stage("read_events_sec", perf):
        raw = read_geo_all_day(report_date, source_path, log_prefix=command)

    with timed_stage("prepare_events_sec", perf):
        events = prepare_pair_events(raw, value_col=value_col, normalize_value=normalize_value)

    with timed_stage("build_intervals_sec", perf):
        intervals = build_temporal_intervals(
            events,
            value_col=value_col,
            period_start=day_start,
            period_end=day_end,
        )

    with timed_stage("merge_month_sec", perf):
        day_rows = _coerce_output(intervals, field_names, value_col=value_col, normalize_value=normalize_imei)
        result = _upsert_daily_into_month_parquet(
            month_path=output_path,
            day_intervals=day_rows,
            day_start=day_start,
            day_end=day_end,
            field_names=field_names,
        )

    stats: dict[str, Any] = {
        "command": command,
        "table": STG_MSISDN_IMEI_TABLE,
        "report_date": report_date.isoformat(),
        "report_month": report_month_start(report_date).isoformat(),
        "stg_geo_all_path": str(source_path),
        "output_path": str(output_path),
        "geo_rows_read": int(len(raw)),
        "event_rows_with_pair": int(len(events)),
        "day_interval_rows": int(len(day_rows)),
        "month_interval_rows": int(len(result)),
        "distinct_msisdn": int(result["msisdn"].nunique()) if not result.empty else 0,
    }
    perf["elapsed_total_sec"] = round(time.perf_counter() - started, 4)
    append_command_metrics(command=command, metrics={**stats, **perf})
    logger.info("%s completed: %s", command, stats)
    return {**stats, **perf}


def _coerce_output(
    df: pd.DataFrame,
    field_names: list[str],
    *,
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
