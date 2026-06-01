from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd

from mobile.pipelines.common.dq_logging import emit_dq_log, emit_dq_summary
from mobile.pipelines.common.dq_wkt import DEFAULT_ALLOWED_GEOM_TYPES, collect_wkt_metrics
from mobile.pipelines.dim.oktmo import DIM_OKTMO_FIELDS
from mobile.project_paths import PROJECT_ROOT, resolve_project_path

logger = logging.getLogger(__name__)

LOG_TAG = "DQ_DIM_OKTMO"
ALLOWED_GEOM_TYPES = DEFAULT_ALLOWED_GEOM_TYPES


def run_dq(oktmo_path: str | Path) -> dict[str, Any]:
    """DQ витрины ``dim_oktmo`` по пути parquet (схема полей — ``DIM_OKTMO_FIELDS`` в ETL)."""
    resolved = _resolve_oktmo_path(oktmo_path)
    expected_columns = [field["name"] for field in DIM_OKTMO_FIELDS]

    if not resolved.exists():
        summary = {"status": "failed", "reason": "parquet_not_found", "oktmo_path": str(resolved)}
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
            "oktmo_path": str(resolved),
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
            {
                "nunique": int(series.nunique(dropna=True)),
            },
        )

    if "level" in data.columns:
        level_series = pd.to_numeric(data["level"], errors="coerce")
        level_counts = (
            level_series.fillna(-1).astype("int64").value_counts(dropna=False).sort_index().to_dict()
        )
        invalid_level_count = int((~level_series.isin([1, 2]) & level_series.notna()).sum())
        emit(
            "level_distribution",
            "warning" if invalid_level_count > 0 else "ok",
            {
                "level_counts": {str(k): int(v) for k, v in level_counts.items()},
                "invalid_level_count": invalid_level_count,
            },
        )

    if "code" in data.columns:
        code = _normalize_code_series(data["code"])
        duplicate_codes = int(code.duplicated(keep=False).sum())
        non_numeric_code = int((~code.fillna("").str.fullmatch(r"\d+") & code.notna()).sum())
        emit(
            "code_quality",
            "warning" if duplicate_codes > 0 or non_numeric_code > 0 else "ok",
            {
                "duplicate_code_count": duplicate_codes,
                "non_numeric_code_count": non_numeric_code,
            },
        )

    if "parent_code" in data.columns:
        parent_code = _normalize_code_series(data["parent_code"])
        non_numeric_parent = int((~parent_code.fillna("").str.fullmatch(r"\d+") & parent_code.notna()).sum())
        emit(
            "parent_code_quality",
            "warning" if non_numeric_parent > 0 else "ok",
            {"non_numeric_parent_code_count": non_numeric_parent},
        )

    if {"level", "code", "parent_code"}.issubset(data.columns):
        level_num = pd.to_numeric(data["level"], errors="coerce")
        level_1 = data[level_num == 1].copy()
        level_2 = data[level_num == 2].copy()

        level1_parent = _normalize_code_series(level_1["parent_code"])
        level2_parent = _normalize_code_series(level_2["parent_code"])
        level1_with_parent = int(level1_parent.notna().sum())
        level2_without_parent = int(level2_parent.isna().sum())

        parent_codes = set(_normalize_code_series(level_1["code"]).dropna())
        child_parent_codes = set(level2_parent.dropna())
        children_without_parent = int(len(sorted(child_parent_codes - parent_codes)))
        parents_without_children = int(len(sorted(parent_codes - child_parent_codes)))

        hierarchy_warn = (
            level1_with_parent > 0
            or level2_without_parent > 0
            or children_without_parent > 0
            or parents_without_children > 0
        )
        emit(
            "hierarchy_integrity",
            "warning" if hierarchy_warn else "ok",
            {
                "level1_with_parent_count": level1_with_parent,
                "level2_without_parent_count": level2_without_parent,
                "children_without_parent_count": children_without_parent,
                "parents_without_children_count": parents_without_children,
                "rows_level_1": int((level_num == 1).sum()),
                "rows_level_2": int((level_num == 2).sum()),
            },
        )

    if "name" in data.columns:
        names = data["name"].astype("string").str.strip()
        invalid_names = int(names.isna().sum() + names.isin({"", "-", "null", "NULL"}).sum())
        emit(
            "name_quality",
            "warning" if invalid_names > 0 else "ok",
            {
                "invalid_name_count": invalid_names,
            },
        )

    if "WKT" in data.columns:
        wkt_metrics = collect_wkt_metrics(data["WKT"], total_count_key="total_wkt_count")
        geom_warn = (
            wkt_metrics["parse_error_count"] > 0
            or wkt_metrics["invalid_topology_count"] > 0
            or wkt_metrics["empty_geometry_count"] > 0
            or wkt_metrics["unsupported_geom_type_count"] > 0
        )
        emit("wkt_geometry", "warning" if geom_warn else "ok", wkt_metrics)

    _emit_summary(total_checks=checks, warnings=warnings, failed=failed)
    return {
        "status": "ok",
        "oktmo_path": str(resolved),
        "total_checks": checks,
        "warning_checks": warnings,
        "failed_checks": failed,
    }


def _resolve_oktmo_path(path: str | Path) -> Path:
    candidate = Path(path)
    return candidate if candidate.is_absolute() else PROJECT_ROOT / candidate


def _normalize_code_series(series: pd.Series) -> pd.Series:
    normalized = series.astype("string").str.strip()
    normalized = normalized.str.replace(r"\.0+$", "", regex=True)
    normalized = normalized.mask(normalized == "", pd.NA)
    return normalized

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

