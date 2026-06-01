"""DQ витрины ``stg_geo_intervals`` за отчётный день."""

from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from mobile.project_paths import resolve_stg_daily_parquet_path

logger = logging.getLogger(__name__)
LOG_TAG = "DQ_STG_GEO_INTERVALS"
_BS_TYPES = frozenset({"m", "f", "i", "x", "o"})

_EXPECTED_COLUMNS: tuple[str, ...] = (
    "msisdn",
    "imsi",
    "imei",
    "start_time_utc",
    "end_time_utc",
    "cgi_list",
    "sub_lat",
    "sub_lon",
    "bs_type",
    "timezone",
    "oktmo_code_1",
    "oktmo_code_2",
    "time_key",
)


def run_dq(*, report_date: date, stg_geo_intervals_path: str | Path) -> dict[str, Any]:
    """DQ ``stg_geo_intervals``; ``report_date`` и ``stg_geo_intervals_path`` обязательны (пути задаёт CLI)."""
    source_path = _resolve_source_path(report_date=report_date, stg_geo_intervals_path=stg_geo_intervals_path)
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
                "stg_geo_intervals_path": str(source_path),
            },
        )
        _emit_summary(total_checks=checks, warnings=warnings, failed=failed)
        return {
            "status": "failed",
            "total_checks": checks,
            "warning_checks": warnings,
            "failed_checks": failed,
            "report_date": report_date.isoformat(),
            "stg_geo_intervals_path": str(source_path),
        }

    data = pd.read_parquet(source_path)
    emit(
        "dataset_basic",
        "ok",
        {
            "report_date": report_date.isoformat(),
            "stg_geo_intervals_path": str(source_path),
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
            {"null_count": int(series.isna().sum()), "null_ratio": round(float(series.isna().mean()), 6)},
        )
        emit(f"cardinality.{col}", "ok", {"nunique": _safe_nunique(series)})

    if {"msisdn", "start_time_utc", "end_time_utc"}.issubset(data.columns):
        required = data["msisdn"].notna() & data["start_time_utc"].notna() & data["end_time_utc"].notna()
        emit(
            "required_fields_presence",
            "failed" if float(required.mean()) < 1.0 else "ok",
            {"invalid_rows": int((~required).sum())},
        )

    if {"start_time_utc", "end_time_utc"}.issubset(data.columns):
        start = pd.to_datetime(data["start_time_utc"], errors="coerce")
        end = pd.to_datetime(data["end_time_utc"], errors="coerce")
        invalid_order = int((end.notna() & start.notna() & (end < start)).sum())
        emit("temporal_order", "failed" if invalid_order > 0 else "ok", {"invalid_order_count": invalid_order})

    if {"sub_lat", "sub_lon"}.issubset(data.columns):
        lat = pd.to_numeric(data["sub_lat"], errors="coerce")
        lon = pd.to_numeric(data["sub_lon"], errors="coerce")
        invalid_lat = int((~lat.between(-90, 90) & lat.notna()).sum())
        invalid_lon = int((~lon.between(-180, 180) & lon.notna()).sum())
        emit(
            "coords_range",
            "warning" if (invalid_lat > 0 or invalid_lon > 0) else "ok",
            {"invalid_sub_lat_count": invalid_lat, "invalid_sub_lon_count": invalid_lon},
        )

    if "bs_type" in data.columns:
        bs_type = data["bs_type"].astype("string").str.strip().str.lower()
        invalid = int((bs_type.notna() & ~bs_type.isin(_BS_TYPES)).sum())
        emit("bs_type_vocab", "warning" if invalid > 0 else "ok", {"invalid_bs_type_rows": invalid})

    if "timezone" in data.columns:
        timezone = pd.to_numeric(data["timezone"], errors="coerce")
        invalid = int((timezone.notna() & ~timezone.between(-12, 14)).sum())
        emit("timezone_range", "warning" if invalid > 0 else "ok", {"invalid_timezone_rows": invalid})

    if "cgi_list" in data.columns:
        cgi_len = data["cgi_list"].map(lambda v: len(v) if isinstance(v, (list, tuple)) or hasattr(v, "__len__") else 0)
        empty = int((cgi_len <= 0).sum())
        emit("cgi_list_non_empty", "failed" if empty > 0 else "ok", {"empty_cgi_list_rows": empty})
        emit(
            "distribution.cgi_list_len",
            "ok",
            {"counts": {str(k): int(v) for k, v in cgi_len.value_counts(dropna=False).to_dict().items()}},
        )

    if {"msisdn", "imsi", "imei", "start_time_utc", "end_time_utc", "bs_type"}.issubset(data.columns):
        dup = int(
            data.duplicated(
                subset=["msisdn", "imsi", "imei", "start_time_utc", "end_time_utc", "bs_type"],
                keep=False,
            ).sum()
        )
        emit("duplicate_interval_key", "warning" if dup > 0 else "ok", {"duplicate_rows": dup})

    _emit_summary(total_checks=checks, warnings=warnings, failed=failed)
    return {
        "status": "failed" if failed else ("warning" if warnings else "ok"),
        "total_checks": checks,
        "warning_checks": warnings,
        "failed_checks": failed,
        "report_date": report_date.isoformat(),
        "stg_geo_intervals_path": str(source_path),
        "row_count": int(len(data)),
    }


def _resolve_source_path(*, report_date: date, stg_geo_intervals_path: str | Path) -> Path:
    return resolve_stg_daily_parquet_path(stg_geo_intervals_path, report_date)


def _safe_nunique(series: pd.Series) -> int:
    def _norm(v: Any) -> Any:
        if isinstance(v, (list, tuple)):
            return tuple(v)
        if hasattr(v, "tolist"):
            try:
                vv = v.tolist()
                if isinstance(vv, list):
                    return tuple(vv)
                return vv
            except Exception:
                return str(v)
        return v

    normalized = series.map(_norm)
    return int(normalized.nunique(dropna=True))


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
        {"total_checks": int(total_checks), "warning_checks": int(warnings), "failed_checks": int(failed)},
    )
