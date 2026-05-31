from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd

from mobile.project_paths import resolve_project_path

logger = logging.getLogger(__name__)
LOG_TAG = "DQ_SRC_EXCL"
_VALUE_COLUMN = "value"


def run_dq(
    *,
    src_imsi_path: Path | str,
    src_imei_path: Path | str,
    src_msisdn_path: Path | str,
) -> dict[str, Any]:
    marts = {
        "src_imsi": resolve_project_path(src_imsi_path),
        "src_imei": resolve_project_path(src_imei_path),
        "src_msisdn": resolve_project_path(src_msisdn_path),
    }
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

    output: dict[str, Any] = {}
    for mart_name, parquet_path in marts.items():
        if not parquet_path.exists():
            emit(
                f"{mart_name}.dataset_presence",
                "failed",
                {"parquet_path": str(parquet_path), "reason": "parquet_not_found"},
            )
            output[mart_name] = {"status": "failed", "reason": "parquet_not_found"}
            continue

        data = pd.read_parquet(parquet_path)
        row_count = int(len(data))
        col_count = int(len(data.columns))
        emit(
            f"{mart_name}.dataset_basic",
            "ok",
            {"parquet_path": str(parquet_path), "row_count": row_count, "column_count": col_count},
        )

        if _VALUE_COLUMN not in data.columns:
            emit(
                f"{mart_name}.schema_columns",
                "failed",
                {
                    "expected_column": _VALUE_COLUMN,
                    "actual_columns": [str(c) for c in data.columns],
                },
            )
            output[mart_name] = {
                "status": "failed",
                "row_count": row_count,
                "expected_column": _VALUE_COLUMN,
            }
            continue

        series = data[_VALUE_COLUMN]
        null_count = int(series.isna().sum())
        unique_count = int(series.nunique(dropna=True))
        emit(
            f"{mart_name}.totals",
            "ok",
            {
                "row_count": row_count,
                "unique_count": unique_count,
                "null_count": null_count,
            },
        )
        output[mart_name] = {
            "status": "ok",
            "parquet_path": str(parquet_path),
            "row_count": row_count,
            "unique_count": unique_count,
            "null_count": null_count,
        }

    _emit_summary(total_checks=checks, warnings=warnings, failed=failed)
    return {
        "status": "ok" if failed == 0 else "failed",
        "total_checks": checks,
        "warning_checks": warnings,
        "failed_checks": failed,
        "marts": output,
    }


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
        "status": "ok" if failed == 0 else "failed",
        "metrics": {"total_checks": total_checks, "warning_checks": warnings, "failed_checks": failed},
    }
    logger.info(json.dumps(payload, ensure_ascii=False, sort_keys=True))
