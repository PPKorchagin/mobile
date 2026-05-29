from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any

import pandas as pd

from mobile.pipelines.stg.tac import M2M_EQUIPMENT_TYPES, STG_TAC_FIELDS
from mobile.project_paths import PROJECT_ROOT

logger = logging.getLogger(__name__)
LOG_TAG = "DQ_STG_TAC"
_TAC_RE = re.compile(r"^\d{8}$")
_MIN_M2M_RATIO = 0.05


def run_dq(tac_path: str | Path) -> dict[str, Any]:
    """DQ витрины ``stg_tac`` по пути parquet (поля и M2M — константы ETL ``stg/tac.py``)."""
    resolved = _resolve_tac_path(tac_path)
    expected_columns = [field["name"] for field in STG_TAC_FIELDS]
    m2m_types = set(M2M_EQUIPMENT_TYPES)

    if not resolved.exists():
        summary = {"status": "failed", "reason": "parquet_not_found", "tac_path": str(resolved)}
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
            "tac_path": str(resolved),
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
        if field != "is_m2m":
            emit(
                f"cardinality.{field}",
                "ok",
                {"nunique": int(series.nunique(dropna=True))},
            )

    if "tac" in data.columns:
        tac = data["tac"].astype("string").str.strip()
        invalid_tac = int((~tac.str.fullmatch(_TAC_RE.pattern) & tac.notna()).sum())
        duplicate_tac = int(tac.duplicated(keep=False).sum())
        emit(
            "tac_integrity",
            "failed" if invalid_tac > 0 or duplicate_tac > 0 else "ok",
            {
                "invalid_tac_count": invalid_tac,
                "duplicate_tac_count": duplicate_tac,
            },
        )

    if "is_m2m" in data.columns:
        is_m2m = data["is_m2m"].astype("boolean")
        m2m_count = int(is_m2m.sum())
        m2m_ratio = float(m2m_count / len(data)) if len(data) else 0.0
        emit(
            "m2m_coverage",
            "warning" if m2m_count == 0 or m2m_ratio < _MIN_M2M_RATIO else "ok",
            {
                "m2m_row_count": m2m_count,
                "m2m_ratio": round(m2m_ratio, 4),
                "non_m2m_row_count": int((~is_m2m.fillna(False)).sum()),
            },
        )

    if {"equipment_type", "is_m2m"}.issubset(data.columns):
        equipment = data["equipment_type"].astype("string").str.strip()
        expected_m2m = equipment.isin(m2m_types)
        actual_m2m = data["is_m2m"].fillna(False)
        mismatch = int((expected_m2m != actual_m2m).sum())
        type_counts = equipment.value_counts(dropna=False).head(20).to_dict()
        emit(
            "m2m_equipment_type_consistency",
            "failed" if mismatch > 0 else "ok",
            {
                "mismatch_count": mismatch,
                "equipment_type_counts": {str(k): int(v) for k, v in type_counts.items()},
                "configured_m2m_types": sorted(m2m_types),
            },
        )

    if "allocation_date" in data.columns:
        dates = data["allocation_date"].astype("string").str.strip()
        parsed = pd.to_datetime(dates, format="%Y-%m-%d", errors="coerce")
        invalid_dates = int((parsed.isna() & dates.notna() & (dates != "")).sum())
        emit(
            "allocation_date_format",
            "warning" if invalid_dates > 0 else "ok",
            {
                "invalid_date_count": invalid_dates,
                "min_date": str(parsed.min()) if parsed.notna().any() else None,
                "max_date": str(parsed.max()) if parsed.notna().any() else None,
            },
        )

    if "manufacturer" in data.columns:
        manufacturer = data["manufacturer"].astype("string").str.strip()
        empty_manufacturer = int(manufacturer.isna().sum() + manufacturer.isin({"", "-"}).sum())
        emit(
            "manufacturer_quality",
            "warning" if empty_manufacturer > 0 else "ok",
            {"empty_manufacturer_count": empty_manufacturer},
        )

    _emit_summary(total_checks=checks, warnings=warnings, failed=failed)
    return {
        "status": "ok",
        "tac_path": str(resolved),
        "total_checks": checks,
        "warning_checks": warnings,
        "failed_checks": failed,
    }


def _resolve_tac_path(path: str | Path) -> Path:
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
