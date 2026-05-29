"""Сборка ``stg_msisdn_operator``: интервалы MSISDN + operator_id (+ imsi) из ``src_person``."""

from __future__ import annotations

import logging
import time
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from mobile.cli_defaults import DEFAULT_PARQUET_COMPRESSION
from mobile.command_timing import append_command_metrics, timed_stage
from mobile.pipelines.stg.person_identity import to_digit_string_series
from mobile.pipelines.stg.src_person_month import read_src_person_month
from mobile.pipelines.stg.subscriber_ids import normalize_imsi, normalize_msisdn
from mobile.project_paths import stg_msisdn_operator_output_path

logger = logging.getLogger(__name__)

_OPEN_TO = pd.Timestamp("2999-12-31 23:59:59")

STG_MSISDN_OPERATOR_FIELDS = [
    {"name": "msisdn", "type": "string"},
    {"name": "operator_id", "type": "long"},
    {"name": "imsi", "type": "string"},
    {"name": "valid_from", "type": "timestamp"},
    {"name": "valid_to", "type": "timestamp"},
]


def _validate_report_month(report_date: date) -> date:
    if report_date.day != 1:
        raise ValueError(f"build-stg-msisdn-operator: report_date must be YYYY-MM-01, got {report_date.isoformat()}")
    return report_date


def _month_period(report_month: date) -> tuple[date, date]:
    month_end = (pd.Timestamp(report_month) + pd.offsets.MonthEnd(0)).date()
    return report_month, month_end


def build_operator_intervals_from_src(raw: pd.DataFrame, *, report_month: date) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame(columns=[f["name"] for f in STG_MSISDN_OPERATOR_FIELDS])

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
    work["operator_id"] = pd.to_numeric(work.get("operator_Id"), errors="coerce").astype("Int64")
    work = work.dropna(subset=["msisdn", "operator_id", "actually_from", "actually_to"])

    grouped = (
        work.groupby(["msisdn", "operator_id", "imsi"], dropna=False)
        .agg(valid_from=("actually_from", "min"), valid_to=("actually_to", "max"))
        .reset_index()
    )
    return grouped[["msisdn", "operator_id", "imsi", "valid_from", "valid_to"]]


def run_build(
    report_date: date,
    *,
    src_person_path: str | Path | None = None,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    command = "build-stg-msisdn-operator"
    perf: dict[str, Any] = {}
    started = time.perf_counter()
    report_month = _validate_report_month(report_date)
    period_start, period_end = _month_period(report_month)
    out = output_path if output_path is not None else stg_msisdn_operator_output_path(report_month)
    if not isinstance(out, Path):
        from mobile.project_paths import resolve_project_path

        out = resolve_project_path(out)

    with timed_stage("read_src_person_sec", perf):
        raw, load_days = read_src_person_month(
            report_month=report_month,
            period_start=period_start,
            period_end=period_end,
            src_person_path=src_person_path,
            mode="all_snapshots",
        )

    with timed_stage("build_intervals_sec", perf):
        result = build_operator_intervals_from_src(raw, report_month=report_month)

    with timed_stage("write_sec", perf):
        out.parent.mkdir(parents=True, exist_ok=True)
        result.to_parquet(out, compression=DEFAULT_PARQUET_COMPRESSION, index=False)

    stats = {
        "command": command,
        "report_date": report_month.isoformat(),
        "output_path": str(out),
        "src_load_days": len(load_days),
        "interval_rows": int(len(result)),
        "distinct_msisdn": int(result["msisdn"].nunique()) if not result.empty else 0,
    }
    perf["elapsed_total_sec"] = round(time.perf_counter() - started, 4)
    append_command_metrics(command=command, metrics={**stats, **perf})
    logger.info("%s completed: %s", command, stats)
    return {**stats, **perf}

