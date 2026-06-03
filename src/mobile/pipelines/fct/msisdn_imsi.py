"""Сборка ``fct_msisdn_imsi``: MSISDN–IMSI + ``operator_id`` из наблюдений ``stg_geo_all``."""

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
from mobile.pipelines.fct.subscriber_ids import normalize_imsi, normalize_msisdn, to_digit_string_series
from mobile.project_paths import (
    DEFAULT_FCT_MSISDN_IMSI_SCHEMA_PATH,
    resolve_project_path,
)

logger = logging.getLogger(__name__)

_OPEN_TO = pd.Timestamp("2999-12-31 23:59:59")
_PAIR_VALUE_COL = "imsi"

STG_MSISDN_IMSI_TABLE = "fct_msisdn_imsi"
FCT_MSISDN_IMSI_FIELDS: list[dict[str, str]] = [
    {"name": "msisdn", "type": "string"},
    {"name": "imsi", "type": "string"},
    {"name": "operator_id", "type": "long"},
    {"name": "valid_from", "type": "timestamp"},
    {"name": "valid_to", "type": "timestamp"},
]


apply_table_fields_to_module(
    DEFAULT_FCT_MSISDN_IMSI_SCHEMA_PATH,
    table_name="STG_MSISDN_IMSI_TABLE",
    fields_name="FCT_MSISDN_IMSI_FIELDS",
    module_globals=globals(),
    default_table=STG_MSISDN_IMSI_TABLE,
    default_fields=FCT_MSISDN_IMSI_FIELDS,
)
_FIELD_NAMES = [f["name"] for f in FCT_MSISDN_IMSI_FIELDS]


def operator_id_from_imsi_series(imsi: pd.Series) -> pd.Series:
    """MNC из IMSI при MCC=250 (цифры 4–5); для иностранных IMSI — ``NA``."""
    digits = imsi.astype("string")
    mnc = pd.to_numeric(digits.str.slice(3, 5), errors="coerce")
    ru = digits.str.startswith("250", na=False)
    return mnc.where(ru).astype("Int64")


_BINDING_REQUIRED_COLS = ("msisdn", "imsi", "valid_from", "valid_to")


def build_imsi_day_intervals(
    report_date: date,
    *,
    stg_geo_all_path: str | Path,
) -> pd.DataFrame:
    """Суточные интервалы MSISDN–IMSI из ``stg_geo_all`` (без ``operator_id``)."""
    day_start = datetime.combine(report_date, datetime.min.time())
    day_end = datetime.combine(report_date, datetime.max.time())
    binding_fields = ["msisdn", "imsi", "valid_from", "valid_to"]
    source_path = resolve_geo_all_source_path(report_date, stg_geo_all_path)
    raw = read_geo_all_day(report_date, source_path, log_prefix="build-fct-msisdn-imsi")
    events = prepare_pair_events(raw, value_col=_PAIR_VALUE_COL, normalize_value=normalize_imsi)
    intervals = build_temporal_intervals(
        events,
        value_col=_PAIR_VALUE_COL,
        period_start=day_start,
        period_end=day_end,
    )
    return _coerce_binding_output(intervals, binding_fields, value_col=_PAIR_VALUE_COL, normalize_value=normalize_imsi)


def build_imsi_intervals_with_operator(imsi_intervals: pd.DataFrame) -> pd.DataFrame:
    """Добавить ``operator_id`` к интервалам MSISDN–IMSI."""
    if imsi_intervals.empty:
        return pd.DataFrame(columns=_FIELD_NAMES)

    work = imsi_intervals.copy()
    work["operator_id"] = operator_id_from_imsi_series(work["imsi"])
    work = work.dropna(subset=list(_BINDING_REQUIRED_COLS))
    return _coerce_imsi_frame(work)


def build_imsi_intervals_from_src(raw: pd.DataFrame, *, report_month: date) -> pd.DataFrame:
    """Интервалы из ``src_person`` (MNP) для ``build-fct-person``; ``operator_id`` из IMSI."""
    if raw.empty:
        return pd.DataFrame(columns=_FIELD_NAMES)

    work = raw.copy()
    client_type = pd.to_numeric(work.get("client_type"), errors="coerce")
    work = work.loc[client_type == 0].copy()
    month_start = pd.Timestamp(report_month)
    month_end = pd.Timestamp(report_month) + pd.offsets.MonthEnd(0)
    work["actually_from"] = pd.to_datetime(work.get("actually_from"), errors="coerce")
    work["actually_to"] = pd.to_datetime(work.get("actually_to"), errors="coerce").fillna(_OPEN_TO)
    work = work.loc[
        work["actually_from"].notna()
        & (work["actually_from"] <= month_end)
        & (work["actually_to"] >= month_start)
    ].copy()

    work["msisdn"] = normalize_msisdn(to_digit_string_series(work.get("isdn")))
    work["imsi"] = normalize_imsi(to_digit_string_series(work.get("imsi")))
    work["operator_id"] = operator_id_from_imsi_series(work["imsi"])
    work = work.dropna(subset=["msisdn", "operator_id", "imsi", "actually_from", "actually_to"])

    grouped = (
        work.groupby(["msisdn", "operator_id", "imsi"], dropna=False)
        .agg(valid_from=("actually_from", "min"), valid_to=("actually_to", "max"))
        .reset_index()
    )
    return _coerce_imsi_frame(grouped)


build_operator_intervals_from_src = build_imsi_intervals_from_src


def run_build(
    report_date: date,
    *,
    stg_geo_all_path: str | Path,
    output_path: str | Path,
) -> dict[str, Any]:
    """Собрать ``fct_msisdn_imsi`` за день из ``stg_geo_all`` (IMSI + operator_id из MNC)."""
    command = "build-fct-msisdn-imsi-operator"
    perf: dict[str, Any] = {}
    started = time.perf_counter()
    day_start = datetime.combine(report_date, datetime.min.time())
    day_end = datetime.combine(report_date, datetime.max.time())
    geo = resolve_project_path(stg_geo_all_path)
    out = resolve_project_path(output_path)

    day_without_operator = 0
    with timed_stage("build_imsi_day_sec", perf):
        imsi_day = build_imsi_day_intervals(report_date, stg_geo_all_path=geo)

    with timed_stage("build_operator_id_sec", perf):
        day_rows = build_imsi_intervals_with_operator(imsi_day)
        if not day_rows.empty:
            day_without_operator = int(day_rows["operator_id"].isna().sum())

    with timed_stage("upsert_imsi_month_sec", perf):
        result = upsert_imsi_daily_into_month_parquet(
            month_path=out,
            day_intervals=day_rows,
            day_start=day_start,
            day_end=day_end,
        )

    stats: dict[str, Any] = {
        "command": command,
        "table": STG_MSISDN_IMSI_TABLE,
        "report_date": report_date.isoformat(),
        "stg_geo_all_path": str(geo),
        "output_path": str(out),
        "day_imsi_interval_rows": int(len(imsi_day)),
        "day_binding_rows": int(len(day_rows)),
        "day_rows_without_operator_id": day_without_operator,
        "day_rows_with_operator": int(len(day_rows) - day_without_operator),
        "month_interval_rows": int(len(result)),
        "distinct_msisdn": int(result["msisdn"].nunique()) if not result.empty else 0,
    }
    perf["elapsed_total_sec"] = round(time.perf_counter() - started, 4)
    append_command_metrics(command=command, metrics={**stats, **perf})
    logger.info("%s completed: %s", command, stats)
    return {**stats, **perf}


def _coerce_imsi_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=_FIELD_NAMES)
    out = df.copy()
    out["msisdn"] = normalize_msisdn(out["msisdn"])
    out["imsi"] = normalize_imsi(out["imsi"])
    out["operator_id"] = pd.to_numeric(out["operator_id"], errors="coerce").astype("Int64")
    out["valid_from"] = pd.to_datetime(out["valid_from"], errors="coerce")
    out["valid_to"] = pd.to_datetime(out["valid_to"], errors="coerce")
    out = out.dropna(subset=list(_BINDING_REQUIRED_COLS))
    return out[_FIELD_NAMES].reset_index(drop=True)


def _merge_imsi_intervals(frame: pd.DataFrame) -> pd.DataFrame:
    return merge_adjacent_intervals(frame, group_cols=["msisdn", "operator_id", "imsi"])


def upsert_imsi_daily_into_month_parquet(
    *,
    month_path: Path,
    day_intervals: pd.DataFrame,
    day_start: datetime,
    day_end: datetime,
) -> pd.DataFrame:
    day_part = _coerce_imsi_frame(day_intervals)
    existing = pd.DataFrame(columns=_FIELD_NAMES)
    if month_path.exists():
        existing = pd.read_parquet(month_path, columns=_FIELD_NAMES)
        existing = drop_intervals_overlapping_day(existing, day_start=day_start, day_end=day_end)
    combined = pd.concat([existing, day_part], ignore_index=True)
    merged = _merge_imsi_intervals(combined)
    if not day_part.empty and merged.empty:
        logger.warning(
            "build-fct-msisdn-imsi-operator: merge dropped %s day rows (check groupby null keys)",
            len(day_part),
        )
    result = _coerce_imsi_frame(merged)
    month_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_parquet(month_path, compression=DEFAULT_PARQUET_COMPRESSION, index=False)
    return result


def _coerce_binding_output(
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
