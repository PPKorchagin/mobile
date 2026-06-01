from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd
from shapely import wkt
from shapely.errors import GEOSException

from mobile.pipelines.dim.time_zones import DIM_TIME_ZONES_FIELDS
from mobile.project_paths import PROJECT_ROOT

logger = logging.getLogger(__name__)
LOG_TAG = "DQ_DIM_TIME_ZONES"
ALLOWED_GEOM_TYPES = {"POLYGON", "MULTIPOLYGON"}


def run_dq(time_zones_path: str | Path) -> dict[str, Any]:
    """DQ витрины ``dim_time_zones`` по пути parquet (поля — ``DIM_TIME_ZONES_FIELDS`` в ETL)."""
    resolved = _resolve_time_zones_path(time_zones_path)
    expected_columns = [field["name"] for field in DIM_TIME_ZONES_FIELDS]

    if not resolved.exists():
        summary = {"status": "failed", "reason": "parquet_not_found", "time_zones_path": str(resolved)}
        _emit_log("dataset_presence", "failed", summary)
        _emit_summary(total_checks=1, warnings=0, failed=1)
        return summary

    data = pd.read_parquet(resolved)
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

    emit(
        "dataset_basic",
        "ok",
        {
            "row_count": int(len(data)),
            "column_count": int(len(data.columns)),
            "time_zones_path": str(resolved),
        },
    )

    missing_columns = [col for col in expected_columns if col not in data.columns]
    emit(
        "schema_columns",
        "failed" if missing_columns else "ok",
        {
            "expected_columns": expected_columns,
            "missing_columns": missing_columns,
        },
    )

    for field in expected_columns:
        if field not in data.columns:
            continue
        series = data[field]
        emit(
            f"nulls.{field}",
            "ok",
            {
                "null_count": int(series.isna().sum()),
                "null_ratio": float(series.isna().mean()),
            },
        )
        emit(
            f"cardinality.{field}",
            "ok",
            {"nunique": int(series.nunique(dropna=True))},
        )

    if "code" in data.columns:
        code = pd.to_numeric(data["code"], errors="coerce")
        duplicate_codes = int(code.duplicated(keep=False).sum())
        invalid_codes = int(code.isna().sum())
        emit(
            "code_quality",
            "warning" if invalid_codes > 0 else "ok",
            {
                "duplicate_code_count": duplicate_codes,
                "invalid_code_count": invalid_codes,
            },
        )

    if "timezone" in data.columns:
        timezone = pd.to_numeric(data["timezone"], errors="coerce")
        invalid_timezone = int((~timezone.between(-12, 14) & timezone.notna()).sum())
        emit(
            "timezone_range",
            "warning" if invalid_timezone > 0 else "ok",
            {
                "invalid_timezone_count": invalid_timezone,
                "timezone_min": float(timezone.min()) if timezone.notna().any() else None,
                "timezone_max": float(timezone.max()) if timezone.notna().any() else None,
                "distribution": _distribution_pct(timezone),
            },
        )

    if "geometry" in data.columns:
        geometry_metrics = _collect_wkt_metrics(data["geometry"])
        has_geom_warnings = (
            geometry_metrics["parse_error_count"] > 0
            or geometry_metrics["invalid_topology_count"] > 0
            or geometry_metrics["empty_geometry_count"] > 0
            or geometry_metrics["unsupported_geom_type_count"] > 0
        )
        emit("geometry_quality", "warning" if has_geom_warnings else "ok", geometry_metrics)

    _emit_summary(total_checks=checks, warnings=warnings, failed=failed)
    return {
        "status": "ok",
        "time_zones_path": str(resolved),
        "total_checks": checks,
        "warning_checks": warnings,
        "failed_checks": failed,
    }


def _distribution_pct(series: pd.Series) -> dict[str, float]:
    value_counts = (series.astype("string").fillna("<NA>").value_counts(normalize=True) * 100).round(4).to_dict()
    return {str(key): float(val) for key, val in value_counts.items()}


def _collect_wkt_metrics(values: pd.Series) -> dict[str, Any]:
    parse_error_count = 0
    invalid_topology_count = 0
    unsupported_geom_type_count = 0
    empty_geometry_count = 0
    valid_geometry_count = 0
    geom_type_counts: dict[str, int] = {}

    for value in values:
        if value is None or pd.isna(value) or not str(value).strip():
            parse_error_count += 1
            continue
        try:
            geom = wkt.loads(str(value))
        except (GEOSException, ValueError):
            parse_error_count += 1
            continue

        geom_type = geom.geom_type.upper()
        geom_type_counts[geom_type] = geom_type_counts.get(geom_type, 0) + 1
        if geom_type not in ALLOWED_GEOM_TYPES:
            unsupported_geom_type_count += 1
        if geom.is_empty:
            empty_geometry_count += 1
        if not geom.is_valid:
            invalid_topology_count += 1
        if geom_type in ALLOWED_GEOM_TYPES and geom.is_valid and not geom.is_empty:
            valid_geometry_count += 1

    total = int(len(values))
    return {
        "total_geometry_count": total,
        "valid_geometry_count": valid_geometry_count,
        "parse_error_count": parse_error_count,
        "unsupported_geom_type_count": unsupported_geom_type_count,
        "empty_geometry_count": empty_geometry_count,
        "invalid_topology_count": invalid_topology_count,
        "geom_type_counts": geom_type_counts,
    }


def _resolve_time_zones_path(path: str | Path) -> Path:
    candidate = Path(path)
    return candidate if candidate.is_absolute() else PROJECT_ROOT / candidate


def _emit_log(check: str, status: str, metrics: dict[str, Any]) -> None:
    payload = {"tag": LOG_TAG, "check": check, "status": status, "metrics": metrics}
    message = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    if status == "failed":
        logger.error(message)
    elif status == "warning":
        logger.warning(message)
    else:
        logger.info(message)


def _emit_summary(total_checks: int, warnings: int, failed: int) -> None:
    payload = {
        "tag": LOG_TAG,
        "check": "summary",
        "status": "ok",
        "metrics": {
            "total_checks": total_checks,
            "warning_checks": warnings,
            "failed_checks": failed,
        },
    }
    logger.info(json.dumps(payload, ensure_ascii=False, sort_keys=True))
