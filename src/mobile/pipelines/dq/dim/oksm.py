from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any

import pandas as pd

from mobile.pipelines.dim.oksm import DIM_OKSM_FIELDS
from mobile.project_paths import PROJECT_ROOT

logger = logging.getLogger(__name__)

from mobile.pipelines.common.dq_logging import emit_dq_log, emit_dq_summary
LOG_TAG = "DQ_DIM_OKSM"
_NUMERIC_CODE_RE = re.compile(r"^\d{3}$")
_ALPHA2_RE = re.compile(r"^[A-Z]{2}$")
_ALPHA3_RE = re.compile(r"^[A-Z]{3}$")
_RUSSIA_NUMERIC_CODE = "643"


def run_dq(oksm_path: str | Path) -> dict[str, Any]:
    """DQ витрины ``dim_oksm`` по пути parquet (поля — константы ETL ``dim/oksm.py``)."""
    resolved = _resolve_oksm_path(oksm_path)
    expected_columns = [field["name"] for field in DIM_OKSM_FIELDS]

    if not resolved.exists():
        summary = {"status": "failed", "reason": "parquet_not_found", "oksm_path": str(resolved)}
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
            "oksm_path": str(resolved),
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

    if "numeric_code" in data.columns:
        code = data["numeric_code"].astype("string").str.strip()
        invalid_code = int((~code.str.fullmatch(_NUMERIC_CODE_RE.pattern) & code.notna()).sum())
        duplicate_code = int(code.duplicated(keep=False).sum())
        emit(
            "numeric_code_integrity",
            "failed" if invalid_code > 0 or duplicate_code > 0 else "ok",
            {
                "invalid_numeric_code_count": invalid_code,
                "duplicate_numeric_code_count": duplicate_code,
            },
        )
        has_russia = bool((code == _RUSSIA_NUMERIC_CODE).any())
        emit(
            "russia_presence",
            "warning" if not has_russia else "ok",
            {"has_numeric_code_643": has_russia},
        )

    if "alpha2" in data.columns:
        alpha2 = data["alpha2"].astype("string").str.strip()
        nonempty = alpha2.notna() & (alpha2 != "")
        invalid_alpha2 = int((nonempty & ~alpha2.str.fullmatch(_ALPHA2_RE.pattern)).sum())
        duplicate_alpha2 = int(alpha2.duplicated(keep=False).sum())
        emit(
            "alpha2_integrity",
            "failed" if invalid_alpha2 > 0 or duplicate_alpha2 > 0 else "ok",
            {
                "invalid_alpha2_count": invalid_alpha2,
                "duplicate_alpha2_count": duplicate_alpha2,
            },
        )

    if "alpha3" in data.columns:
        alpha3 = data["alpha3"].astype("string").str.strip()
        nonempty = alpha3.notna() & (alpha3 != "")
        invalid_alpha3 = int((nonempty & ~alpha3.str.fullmatch(_ALPHA3_RE.pattern)).sum())
        duplicate_alpha3 = int(alpha3.duplicated(keep=False).sum())
        emit(
            "alpha3_integrity",
            "failed" if invalid_alpha3 > 0 or duplicate_alpha3 > 0 else "ok",
            {
                "invalid_alpha3_count": invalid_alpha3,
                "duplicate_alpha3_count": duplicate_alpha3,
            },
        )

    if {"alpha2", "alpha3"}.issubset(data.columns):
        alpha2 = data["alpha2"].astype("string").str.strip()
        alpha3 = data["alpha3"].astype("string").str.strip()
        both = alpha2.notna() & (alpha2 != "") & alpha3.notna() & (alpha3 != "")
        pairs = data.loc[both, ["alpha2", "alpha3"]].drop_duplicates()
        pair_count = int(len(pairs))
        emit(
            "alpha_pair_cardinality",
            "ok",
            {"distinct_alpha2_alpha3_pairs": pair_count},
        )

    if {"name_short", "name_full"}.issubset(data.columns):
        name_short = data["name_short"].astype("string").str.strip()
        name_full = data["name_full"].astype("string").str.strip()
        empty_short = int(name_short.isna().sum() + name_short.isin({""}).sum())
        empty_full = int(name_full.isna().sum() + name_full.isin({""}).sum())
        emit(
            "name_quality",
            "failed" if empty_short > 0 or empty_full > 0 else "ok",
            {
                "empty_name_short_count": empty_short,
                "empty_name_full_count": empty_full,
            },
        )

    if "autokey" in data.columns:
        autokey = data["autokey"].astype("string").str.strip()
        duplicate_autokey = int(autokey.duplicated(keep=False).sum())
        empty_autokey = int(autokey.isna().sum() + autokey.isin({""}).sum())
        emit(
            "autokey_integrity",
            "failed" if duplicate_autokey > 0 or empty_autokey > 0 else "ok",
            {
                "duplicate_autokey_count": duplicate_autokey,
                "empty_autokey_count": empty_autokey,
            },
        )

    _emit_summary(total_checks=checks, warnings=warnings, failed=failed)
    return {
        "status": "ok",
        "oksm_path": str(resolved),
        "total_checks": checks,
        "warning_checks": warnings,
        "failed_checks": failed,
    }


def _resolve_oksm_path(path: str | Path) -> Path:
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

