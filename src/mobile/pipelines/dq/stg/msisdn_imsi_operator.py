"""DQ месячной витрины ``fct_msisdn_imsi`` (команда ``dq-fct-msisdn-imsi-operator``)."""

from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from mobile.pipelines.stg.msisdn_imsi import FCT_MSISDN_IMSI_FIELDS, operator_id_from_imsi_series
from mobile.pipelines.stg.subscriber_ids import (
    IMSI_MAX_LEN,
    IMSI_MIN_LEN,
    MSISDN_MAX_LEN,
    MSISDN_MIN_LEN,
    normalize_imsi,
    normalize_msisdn,
)
from mobile.project_paths import report_month_start, resolve_project_path

logger = logging.getLogger(__name__)
LOG_TAG = "DQ_FCT_MSISDN_IMSI_OPERATOR"

_EXPECTED_COLUMNS: tuple[str, ...] = tuple(f["name"] for f in FCT_MSISDN_IMSI_FIELDS)
_REQUIRED_COLUMNS: frozenset[str] = frozenset(_EXPECTED_COLUMNS) - {"operator_id"}
_INTERVAL_GROUP_COLS: tuple[str, ...] = ("msisdn", "operator_id", "imsi")


def run_dq(*, report_date: date, fct_msisdn_imsi_path: str | Path) -> dict[str, Any]:
    """DQ ``fct_msisdn_imsi``; ``report_date`` — любой день месяца, приводится к 1-му числу."""
    report_month = report_month_start(report_date)
    source_path = _resolve_source_path(report_date=report_month, fct_msisdn_imsi_path=fct_msisdn_imsi_path)
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

    base: dict[str, Any] = {
        "report_date": report_month.isoformat(),
        "fct_msisdn_imsi_path": str(source_path),
    }
    if report_date != report_month:
        base["report_date_input"] = report_date.isoformat()

    if not source_path.exists():
        emit("dataset_presence", "failed", {**base, "reason": "parquet_not_found"})
        _emit_summary(total_checks=checks, warnings=warnings, failed=failed)
        return {
            "status": "failed",
            "total_checks": checks,
            "warning_checks": warnings,
            "failed_checks": failed,
            **base,
        }

    data = pd.read_parquet(source_path)
    emit(
        "dataset_basic",
        "ok",
        {
            **base,
            "row_count": int(len(data)),
            "column_count": int(len(data.columns)),
            "distinct_msisdn": int(data["msisdn"].nunique()) if "msisdn" in data.columns and len(data) else 0,
            "distinct_operator_id": int(data["operator_id"].nunique())
            if "operator_id" in data.columns and len(data)
            else 0,
        },
    )

    missing_columns = [col for col in _EXPECTED_COLUMNS if col not in data.columns]
    emit(
        "schema_columns",
        "failed" if missing_columns else "ok",
        {**base, "expected_columns": list(_EXPECTED_COLUMNS), "missing_columns": missing_columns},
    )

    if missing_columns:
        _emit_summary(total_checks=checks, warnings=warnings, failed=failed)
        return {
            "status": "failed",
            "total_checks": checks,
            "warning_checks": warnings,
            "failed_checks": failed,
            **base,
            "row_count": int(len(data)),
        }

    for col in _EXPECTED_COLUMNS:
        series = data[col]
        null_ratio = float(series.isna().mean()) if len(series) else 0.0
        emit(
            f"nulls.{col}",
            "failed" if col in _REQUIRED_COLUMNS and null_ratio > 0 else "ok",
            {**base, "null_count": int(series.isna().sum()), "null_ratio": round(null_ratio, 6)},
        )

    work = data.copy()
    work["valid_from"] = pd.to_datetime(work["valid_from"], errors="coerce")
    work["valid_to"] = pd.to_datetime(work["valid_to"], errors="coerce")
    invalid_order = int(
        (work["valid_from"].notna() & work["valid_to"].notna() & (work["valid_to"] < work["valid_from"])).sum()
    )
    emit(
        "temporal_order",
        "failed" if invalid_order > 0 else "ok",
        {**base, "invalid_order_count": invalid_order},
    )

    msisdn_norm = normalize_msisdn(work["msisdn"])
    imsi_norm = normalize_imsi(work["imsi"])
    msisdn_raw = work["msisdn"].astype("string").str.replace(r"\D+", "", regex=True)
    imsi_raw = work["imsi"].astype("string").str.replace(r"\D+", "", regex=True)

    invalid_msisdn = int(msisdn_norm.isna().sum())
    invalid_imsi = int(imsi_norm.isna().sum())
    emit(
        "msisdn_format",
        "failed" if invalid_msisdn > 0 else "ok",
        {
            **base,
            "invalid_msisdn_rows": invalid_msisdn,
            "allowed_length": f"{MSISDN_MIN_LEN}-{MSISDN_MAX_LEN}",
        },
    )
    emit(
        "imsi_format",
        "failed" if invalid_imsi > 0 else "ok",
        {
            **base,
            "invalid_imsi_rows": invalid_imsi,
            "allowed_length": f"{IMSI_MIN_LEN}-{IMSI_MAX_LEN}",
        },
    )

    unchanged_msisdn = int((msisdn_norm.notna() & (msisdn_norm == msisdn_raw)).sum())
    unchanged_imsi = int((imsi_norm.notna() & (imsi_norm == imsi_raw)).sum())
    emit(
        "normalization_canonical",
        "warning"
        if (unchanged_msisdn < len(work) - invalid_msisdn or unchanged_imsi < len(work) - invalid_imsi)
        else "ok",
        {
            **base,
            "canonical_msisdn_rows": unchanged_msisdn,
            "canonical_imsi_rows": unchanged_imsi,
            "rows": int(len(work)),
        },
    )

    operator_id = pd.to_numeric(work["operator_id"], errors="coerce")
    imsi_s = work["imsi"].astype("string")
    ru = imsi_s.str.startswith("250", na=False)
    ru_missing_operator = int((ru & (operator_id.isna() | (operator_id < 1))).sum())
    emit(
        "operator_id_valid",
        "failed" if ru_missing_operator > 0 else "ok",
        {
            **base,
            "ru_imsi_missing_operator_id_rows": ru_missing_operator,
            "rows_without_operator_id": int(operator_id.isna().sum()),
        },
    )

    if "imsi" in work.columns and "operator_id" in work.columns:
        expected_op = operator_id_from_imsi_series(work["imsi"])
        stored_op = operator_id.astype("Int64")
        comparable = ru & expected_op.notna() & stored_op.notna()
        mismatch = int((comparable & (expected_op != stored_op)).sum())
        emit(
            "operator_id_imsi_alignment",
            "failed" if mismatch > 0 else "ok",
            {
                **base,
                "misaligned_rows": mismatch,
                "ru_imsi_rows": int(ru.sum()),
            },
        )
        non_ru_with_op = int((~ru & stored_op.notna()).sum())
        emit(
            "operator_id_non_ru_imsi",
            "warning" if non_ru_with_op > 0 else "ok",
            {**base, "non_ru_rows_with_operator_id": non_ru_with_op},
        )
        non_ru_rows = int((~ru).sum())
        emit(
            "foreign_imsi_observations",
            "ok",
            {**base, "non_ru_imsi_rows": non_ru_rows},
        )

    dup_exact = int(work.duplicated(subset=list(_EXPECTED_COLUMNS), keep=False).sum())
    emit(
        "duplicate_rows",
        "warning" if dup_exact > 0 else "ok",
        {**base, "duplicate_rows": dup_exact},
    )

    interval_cols = set(_INTERVAL_GROUP_COLS) | {"valid_from", "valid_to"}
    if interval_cols.issubset(work.columns):
        overlap_count = _count_overlapping_intervals(work, group_cols=_INTERVAL_GROUP_COLS)
        emit(
            "interval_overlap_same_triple",
            "failed" if overlap_count > 0 else "ok",
            {**base, "overlapping_interval_rows": overlap_count},
        )
        mergeable = _count_mergeable_adjacent_intervals(work, group_cols=_INTERVAL_GROUP_COLS)
        emit(
            "interval_mergeable_gap",
            "warning" if mergeable > 0 else "ok",
            {**base, "mergeable_adjacent_segments": mergeable},
        )

    _emit_summary(total_checks=checks, warnings=warnings, failed=failed)
    return {
        "status": "failed" if failed else ("warning" if warnings else "ok"),
        "total_checks": checks,
        "warning_checks": warnings,
        "failed_checks": failed,
        **base,
        "row_count": int(len(data)),
    }


def _resolve_source_path(*, report_date: date, fct_msisdn_imsi_path: str | Path) -> Path:
    resolved = resolve_project_path(fct_msisdn_imsi_path)
    if resolved.is_dir():
        month = report_month_start(report_date)
        return resolved / f"{month.isoformat()}.parquet"
    return resolved


def _count_overlapping_intervals(frame: pd.DataFrame, *, group_cols: tuple[str, ...]) -> int:
    if frame.empty:
        return 0
    work = frame.dropna(subset=["msisdn", "imsi", "valid_from", "valid_to"]).copy()
    if work.empty:
        return 0
    overlap_rows = 0
    for _, group in work.groupby(list(group_cols), sort=False, dropna=False):
        ordered = group.sort_values("valid_from", kind="mergesort")
        prev_end: pd.Timestamp | None = None
        for row in ordered.itertuples(index=False):
            start = pd.Timestamp(row.valid_from)
            end = pd.Timestamp(row.valid_to)
            if prev_end is not None and start <= prev_end:
                overlap_rows += 2
            prev_end = end if prev_end is None else max(prev_end, end)
    return overlap_rows


def _count_mergeable_adjacent_intervals(frame: pd.DataFrame, *, group_cols: tuple[str, ...]) -> int:
    if frame.empty:
        return 0
    work = frame.dropna(subset=["msisdn", "imsi", "valid_from", "valid_to"]).copy()
    if work.empty:
        return 0
    mergeable = 0
    for _, group in work.groupby(list(group_cols), sort=False, dropna=False):
        ordered = group.sort_values("valid_from", kind="mergesort")
        prev_end: pd.Timestamp | None = None
        for row in ordered.itertuples(index=False):
            start = pd.Timestamp(row.valid_from)
            end = pd.Timestamp(row.valid_to)
            if prev_end is not None and start <= prev_end + pd.Timedelta(seconds=1):
                mergeable += 1
            prev_end = end if prev_end is None else max(prev_end, end)
    return mergeable


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
