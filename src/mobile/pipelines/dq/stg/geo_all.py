"""DQ витрины ``stg_geo_all`` за отчётный день."""

from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from mobile.project_paths import resolve_project_path, stg_geo_all_output_path

logger = logging.getLogger(__name__)
LOG_TAG = "DQ_STG_GEO_ALL"
_EVENT_TYPES = frozenset({"cdr", "sms", "gprs", "location"})

_EXPECTED_COLUMNS: tuple[str, ...] = (
    "msisdn",
    "imsi",
    "imei",
    "start_time_utc",
    "end_time_utc",
    "utc_offset",
    "lat",
    "lon",
    "bs_type",
    "cgi",
    "event_count",
    "source_event_type",
    "oktmo_code_1",
    "oktmo_code_2",
)


def run_dq(*, report_date: date, stg_geo_all_path: str | Path | None = None) -> dict[str, Any]:
    source_path = _resolve_source_path(report_date=report_date, stg_geo_all_path=stg_geo_all_path)
    checks = 0
    warnings = 0
    failed = 0

    def emit(check: str, status: str, metrics: dict[str, Any]) -> None:
        nonlocal checks, warnings, failed
        checks += 1
        if status == "warning":
            warnings += 1
        elif status == "failed":
            failed += 1
        _emit_log(check, status, metrics)

    if not source_path.exists():
        emit(
            "dataset_presence",
            "failed",
            {
                "reason": "parquet_not_found",
                "report_date": report_date.isoformat(),
                "stg_geo_all_path": str(source_path),
            },
        )
        _emit_summary(total_checks=checks, warnings=warnings, failed=failed)
        return {
            "status": "failed",
            "total_checks": checks,
            "warning_checks": warnings,
            "failed_checks": failed,
            "report_date": report_date.isoformat(),
            "stg_geo_all_path": str(source_path),
        }

    data = pd.read_parquet(source_path)
    emit(
        "dataset_basic",
        "ok",
        {
            "report_date": report_date.isoformat(),
            "stg_geo_all_path": str(source_path),
            "row_count": int(len(data)),
            "column_count": int(len(data.columns)),
        },
    )

    missing_columns = [col for col in _EXPECTED_COLUMNS if col not in data.columns]
    emit(
        "schema_columns",
        "failed" if missing_columns else "ok",
        {
            "expected_columns": list(_EXPECTED_COLUMNS),
            "missing_columns": missing_columns,
        },
    )

    for col in _EXPECTED_COLUMNS:
        if col not in data.columns:
            continue
        series = data[col]
        emit(
            f"nulls.{col}",
            "ok",
            {
                "null_count": int(series.isna().sum()),
                "null_ratio": round(float(series.isna().mean()), 6),
            },
        )
        emit(
            f"cardinality.{col}",
            "ok",
            {"nunique": int(series.nunique(dropna=True))},
        )

    if {"msisdn", "cgi", "start_time_utc"}.issubset(data.columns):
        required = data["msisdn"].notna() & data["cgi"].notna() & data["start_time_utc"].notna()
        rate = float(required.mean()) if len(required) else 1.0
        emit(
            "required_fields_presence",
            "failed" if rate < 1.0 else "ok",
            {
                "required_rate": round(rate, 6),
                "invalid_rows": int((~required).sum()),
            },
        )

    if {"lat", "lon"}.issubset(data.columns):
        lat = pd.to_numeric(data["lat"], errors="coerce")
        lon = pd.to_numeric(data["lon"], errors="coerce")
        invalid_lat = int((~lat.between(-90, 90) & lat.notna()).sum())
        invalid_lon = int((~lon.between(-180, 180) & lon.notna()).sum())
        emit(
            "coords_range",
            "warning" if (invalid_lat > 0 or invalid_lon > 0) else "ok",
            {
                "invalid_lat_count": invalid_lat,
                "invalid_lon_count": invalid_lon,
            },
        )

    if {"start_time_utc", "end_time_utc"}.issubset(data.columns):
        start = pd.to_datetime(data["start_time_utc"], errors="coerce")
        end = pd.to_datetime(data["end_time_utc"], errors="coerce")
        invalid_order = int((end.notna() & start.notna() & (end < start)).sum())
        emit(
            "temporal_order",
            "failed" if invalid_order > 0 else "ok",
            {"invalid_order_count": invalid_order},
        )

    if "event_count" in data.columns:
        cnt = pd.to_numeric(data["event_count"], errors="coerce")
        invalid = int((cnt.isna() | (cnt < 1)).sum())
        emit(
            "event_count_valid",
            "failed" if invalid > 0 else "ok",
            {"invalid_event_count_rows": invalid},
        )

    if "source_event_type" in data.columns:
        src_type = data["source_event_type"].astype("string").str.strip().str.lower()
        invalid = int((src_type.notna() & ~src_type.isin(_EVENT_TYPES)).sum())
        emit(
            "source_event_type_vocab",
            "failed" if invalid > 0 else "ok",
            {"invalid_source_event_type_rows": invalid, "allowed_values": sorted(_EVENT_TYPES)},
        )
        emit(
            "distribution.source_event_type",
            "ok",
            {
                "counts": {str(k): int(v) for k, v in src_type.value_counts(dropna=False).to_dict().items()},
            },
        )

    if "utc_offset" in data.columns:
        utc_offset = pd.to_numeric(data["utc_offset"], errors="coerce")
        invalid = int((utc_offset.notna() & ~utc_offset.between(-12, 14)).sum())
        emit(
            "utc_offset_range",
            "warning" if invalid > 0 else "ok",
            {"invalid_utc_offset_rows": invalid},
        )

    if {"msisdn", "start_time_utc", "source_event_type", "cgi"}.issubset(data.columns):
        dup = int(data.duplicated(subset=["msisdn", "start_time_utc", "source_event_type", "cgi"], keep=False).sum())
        emit(
            "duplicate_event_key",
            "warning" if dup > 0 else "ok",
            {"duplicate_rows": dup},
        )

    _emit_summary(total_checks=checks, warnings=warnings, failed=failed)
    return {
        "status": "failed" if failed else ("warning" if warnings else "ok"),
        "total_checks": checks,
        "warning_checks": warnings,
        "failed_checks": failed,
        "report_date": report_date.isoformat(),
        "stg_geo_all_path": str(source_path),
        "row_count": int(len(data)),
    }


def _resolve_source_path(*, report_date: date, stg_geo_all_path: str | Path | None) -> Path:
    if stg_geo_all_path is None:
        return stg_geo_all_output_path(report_date)
    resolved = resolve_project_path(stg_geo_all_path)
    if resolved.is_dir():
        return resolved / f"{report_date.isoformat()}.parquet"
    return resolved


def _emit_log(check: str, status: str, metrics: dict[str, Any]) -> None:
    payload = {"tag": LOG_TAG, "check": check, "status": status, "metrics": metrics}
    message = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    if status == "failed":
        logger.error(message)
    elif status == "warning":
        logger.warning(message)
    else:
        logger.info(message)


def _emit_summary(*, total_checks: int, warnings: int, failed: int) -> None:
    status = "failed" if failed else ("warning" if warnings else "ok")
    _emit_log(
        "summary",
        status,
        {
            "total_checks": int(total_checks),
            "warning_checks": int(warnings),
            "failed_checks": int(failed),
        },
    )
