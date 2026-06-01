from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd

from mobile.pipelines.common.dq_logging import emit_dq_log, emit_dq_summary
from mobile.pipelines.common.dq_wkt import collect_wkt_metrics
from mobile.pipelines.fct.bs import FCT_BS_FIELDS
from mobile.project_paths import PROJECT_ROOT

logger = logging.getLogger(__name__)

LOG_TAG = "DQ_FCT_BS"
_OPEN_END_TS = pd.Timestamp("2262-04-11 00:00:00")
_BS_TYPES = frozenset({"m", "f", "i", "x", "o"})
_TELECOMSTANDARD = frozenset({"2G", "3G", "4G"})


def run_dq(parquet_path: str | Path) -> dict[str, Any]:
    """DQ витрины ``fct_bs`` по пути parquet (контракт полей — ``FCT_BS_FIELDS``)."""
    resolved = _resolve_parquet_path(parquet_path)
    expected_columns = [field["name"] for field in FCT_BS_FIELDS]

    if not resolved.exists():
        summary = {"status": "failed", "reason": "parquet_not_found", "parquet_path": str(resolved)}
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
            "parquet_path": str(resolved),
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

    if {"mcc", "mnc", "lac", "cell_id"}.issubset(data.columns):
        key_null = int(
            data[["mcc", "mnc", "lac", "cell_id"]]
            .isna()
            .any(axis=1)
            .sum()
        )
        emit(
            "key_presence",
            "failed" if key_null > 0 else "ok",
            {"null_key_rows": key_null},
        )

    if {"mcc", "mnc", "lac", "cell_id", "date_on"}.issubset(data.columns):
        key_cols = ["mcc", "mnc", "lac", "cell_id", "date_on"]
        dup = int(data.duplicated(subset=key_cols, keep=False).sum())
        emit(
            "key_uniqueness_per_snapshot",
            "warning" if dup > 0 else "ok",
            {"duplicate_rows": dup, "key_columns": key_cols},
        )

    if {"date_on", "date_off"}.issubset(data.columns):
        date_on = pd.to_datetime(data["date_on"], errors="coerce")
        date_off = pd.to_datetime(data["date_off"], errors="coerce")
        invalid_order = int(((date_off < date_on) & date_on.notna() & date_off.notna()).sum())
        open_rows = int(date_off.eq(_OPEN_END_TS).sum())
        emit(
            "temporal_consistency",
            "failed" if invalid_order > 0 else "ok",
            {
                "invalid_date_order_count": invalid_order,
                "open_rows": open_rows,
                "open_ratio": round(open_rows / max(len(data), 1), 4),
                "open_sentinel": str(_OPEN_END_TS),
            },
        )

    if {"lon", "lat"}.issubset(data.columns):
        lon = pd.to_numeric(data["lon"], errors="coerce")
        lat = pd.to_numeric(data["lat"], errors="coerce")
        invalid_lon = int((~lon.between(-180, 180) & lon.notna()).sum())
        invalid_lat = int((~lat.between(-90, 90) & lat.notna()).sum())
        emit(
            "coords_range",
            "warning" if invalid_lon > 0 or invalid_lat > 0 else "ok",
            {
                "invalid_lon_count": invalid_lon,
                "invalid_lat_count": invalid_lat,
            },
        )

    if "bs_type" in data.columns:
        bs_type = data["bs_type"].astype("string").str.strip().str.lower()
        invalid = int((~bs_type.isin(_BS_TYPES) & bs_type.notna()).sum())
        emit(
            "bs_type_vocab",
            "warning" if invalid > 0 else "ok",
            {"invalid_bs_type_count": invalid, "allowed_values": sorted(_BS_TYPES)},
        )

    if "telecomstandard" in data.columns:
        std = data["telecomstandard"].astype("string").str.strip().str.upper()
        invalid = int((~std.isin(_TELECOMSTANDARD) & std.notna()).sum())
        emit(
            "telecomstandard_vocab",
            "warning" if invalid > 0 else "ok",
            {"invalid_telecomstandard_count": invalid, "allowed_values": sorted(_TELECOMSTANDARD)},
        )

    for geom_col in ("sector_wkt", "mapinfo_wkt"):
        if geom_col not in data.columns:
            continue
        metrics = collect_wkt_metrics(data[geom_col])
        warn = (
            metrics["parse_error_count"] > 0
            or metrics["invalid_topology_count"] > 0
            or metrics["empty_geometry_count"] > 0
            or metrics["unsupported_geom_type_count"] > 0
        )
        emit(f"geometry.{geom_col}", "warning" if warn else "ok", metrics)

    _emit_summary(total_checks=checks, warnings=warnings, failed=failed)
    return {
        "status": "failed" if failed else ("warning" if warnings else "ok"),
        "parquet_path": str(resolved),
        "total_checks": checks,
        "warning_checks": warnings,
        "failed_checks": failed,
    }


def _resolve_parquet_path(path: str | Path) -> Path:
    candidate = Path(path)
    return candidate if candidate.is_absolute() else PROJECT_ROOT / candidate


def _emit_log(check: str, status: str, metrics: dict[str, Any]) -> None:
    emit_dq_log(LOG_TAG, check, status, metrics, logger=logger)

def _emit_summary(total_checks: int, warnings: int, failed: int) -> None:
    emit_dq_summary(
        LOG_TAG,
        total_checks=total_checks,
        warnings=warnings,
        failed=failed,
        logger=logger,
        derive_status=False,
        clean_status="ok",
    )

