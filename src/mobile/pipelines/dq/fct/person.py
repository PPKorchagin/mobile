"""DQ месячной витрины ``fct_person``."""

from __future__ import annotations

import json
import logging
import re
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from mobile.pipelines.fct.person import FCT_PERSON_FIELDS
from mobile.project_paths import (
    DEFAULT_DIM_OKSM_OUTPUT_PATH,
    report_month_start,
    resolve_project_path,
    resolve_stg_monthly_parquet_path,
)

logger = logging.getLogger(__name__)
LOG_TAG = "DQ_FCT_PERSON"

_PERSON_ID_RE = re.compile(r"^prs_[0-9a-f]{24}$")
_DIGITS_RE = re.compile(r"^\d+$")
_CITIZENSHIP_RE = re.compile(r"^\d{3}$")
_GENDER_VALUES = frozenset({"M", "F", "U"})
_CONFIDENCE_VALUES = frozenset({"high", "medium", "low"})
_LOW_CONFIDENCE_WARN_RATIO = 0.30

_PERSON_COLUMNS = [field["name"] for field in FCT_PERSON_FIELDS]
_PERSON_CRITICAL_NULLS = (
    "person_id",
    "person_cluster_key",
    "report_date",
    "msisdn",
    "imsi",
    "imei",
    "operator_id",
)
_PERSON_DEMO_NULLS = ("gender", "age", "citizenship")


def run_dq(
    *,
    report_date: date,
    fct_person_path: str | Path,
    dim_oksm_path: str | Path | None = None,
) -> dict[str, Any]:
    """Read-only DQ ``fct_person``; ``report_date`` и ``fct_person_path`` обязательны (пути задаёт CLI)."""
    report_month = report_month_start(report_date)
    person_path = _resolve_source_path(report_date=report_month, fct_person_path=fct_person_path)
    oksm_path = resolve_project_path(dim_oksm_path or DEFAULT_DIM_OKSM_OUTPUT_PATH)

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
        "fct_person_path": str(person_path),
    }
    if report_date != report_month:
        base["report_date_input"] = report_date.isoformat()

    if not person_path.exists():
        emit(
            "dataset_presence",
            "failed",
            {**base, "reason": "parquet_not_found"},
        )
        _emit_summary(total_checks=checks, warnings=warnings, failed=failed)
        return {
            "status": "failed",
            "total_checks": checks,
            "warning_checks": warnings,
            "failed_checks": failed,
            **base,
        }

    person = pd.read_parquet(person_path)
    emit(
        "dataset_basic",
        "ok",
        {
            **base,
            "row_count": int(len(person)),
            "column_count": int(len(person.columns)),
            "distinct_person_id": int(person["person_id"].nunique()) if "person_id" in person.columns else 0,
        },
    )

    missing_columns = [col for col in _PERSON_COLUMNS if col not in person.columns]
    extra_columns = [col for col in person.columns if col not in _PERSON_COLUMNS]
    emit(
        "schema_columns",
        "failed" if missing_columns else ("warning" if extra_columns else "ok"),
        {
            "expected_columns": _PERSON_COLUMNS,
            "missing_columns": missing_columns,
            "extra_columns": extra_columns,
        },
    )

    for field in _PERSON_CRITICAL_NULLS:
        if field not in person.columns:
            continue
        null_ratio = float(person[field].isna().mean())
        emit(
            f"nulls.{field}",
            "failed" if null_ratio > 0 else "ok",
            {"null_count": int(person[field].isna().sum()), "null_ratio": round(null_ratio, 6)},
        )

    for field in _PERSON_DEMO_NULLS:
        if field not in person.columns:
            continue
        null_ratio = float(person[field].isna().mean())
        emit(
            f"nulls.{field}",
            "warning" if null_ratio > 0 else "ok",
            {"null_count": int(person[field].isna().sum()), "null_ratio": round(null_ratio, 6)},
        )

    if person.empty:
        _emit_summary(total_checks=checks, warnings=warnings, failed=failed)
        return {
            "status": "failed" if failed else ("warning" if warnings else "ok"),
            "total_checks": checks,
            "warning_checks": warnings,
            "failed_checks": failed,
            **base,
            "row_count": 0,
        }

    if "person_id" in person.columns:
        pid = person["person_id"].astype("string")
        dup_count = int(pid.duplicated(keep=False).sum())
        emit(
            "key.person_id_unique",
            "failed" if dup_count > 0 else "ok",
            {
                "row_count": int(len(person)),
                "distinct_person_id": int(pid.nunique(dropna=True)),
                "duplicate_person_id_count": dup_count,
            },
        )
        invalid_format = int((pid.notna() & ~pid.str.fullmatch(_PERSON_ID_RE.pattern)).sum())
        emit(
            "key.person_id_format",
            "failed" if invalid_format > 0 else "ok",
            {"invalid_person_id_count": invalid_format},
        )

    if "report_date" in person.columns:
        rd = pd.to_datetime(person["report_date"], errors="coerce").dt.date
        distinct = rd.dropna().unique().tolist()
        single = len(distinct) == 1 and distinct[0] == report_month
        emit(
            "key.report_date_single",
            "failed" if not single else "ok",
            {
                "distinct_report_date": [d.isoformat() for d in distinct],
                "expected_report_date": report_month.isoformat(),
            },
        )

    if "gender" in person.columns:
        gender = person["gender"].astype("string").str.strip()
        invalid = int((gender.notna() & ~gender.isin(_GENDER_VALUES)).sum())
        emit("domain.gender", "failed" if invalid > 0 else "ok", {"invalid_gender_count": invalid})

    if "age" in person.columns:
        age = person["age"].astype("string").str.strip()
        invalid = 0
        for value in age.dropna():
            if value == "U":
                continue
            if not value.isdigit():
                invalid += 1
                continue
            n = int(value)
            if n < 0 or n > 120:
                invalid += 1
        emit("domain.age", "failed" if invalid > 0 else "ok", {"invalid_age_count": invalid})

    if "citizenship" in person.columns:
        citizenship = person["citizenship"].astype("string").str.strip()
        empty = int(citizenship.isna().sum() + citizenship.isin({""}).sum())
        invalid_fmt = int(
            (
                citizenship.notna()
                & (citizenship != "")
                & ~citizenship.isin({"U"})
                & ~citizenship.str.fullmatch(_CITIZENSHIP_RE.pattern)
            ).sum()
        )
        emit(
            "domain.citizenship",
            "failed" if empty > 0 or invalid_fmt > 0 else "ok",
            {"empty_citizenship_count": empty, "invalid_citizenship_format_count": invalid_fmt},
        )
        if oksm_path.exists():
            oksm = pd.read_parquet(oksm_path, columns=["numeric_code"])
            valid_codes = set(oksm["numeric_code"].astype("string").str.strip())
            unknown = citizenship[
                citizenship.notna()
                & (citizenship != "")
                & ~citizenship.isin({"U"})
                & ~citizenship.isin(valid_codes)
            ]
            emit(
                "domain.citizenship_oksm",
                "failed" if len(unknown) > 0 else "ok",
                {"unknown_citizenship_code_count": int(len(unknown))},
            )
        else:
            emit(
                "domain.citizenship_oksm",
                "warning",
                {"reason": "oksm_parquet_not_found", "oksm_path": str(oksm_path)},
            )

    if "person_confidence" in person.columns:
        conf = person["person_confidence"].astype("string").str.strip().str.lower()
        invalid = int((conf.notna() & ~conf.isin(_CONFIDENCE_VALUES)).sum())
        emit(
            "domain.person_confidence",
            "failed" if invalid > 0 else "ok",
            {"invalid_person_confidence_count": invalid},
        )
        low_ratio = float((conf == "low").mean()) if len(conf) else 0.0
        emit(
            "distribution.person_confidence",
            "warning" if low_ratio > _LOW_CONFIDENCE_WARN_RATIO else "ok",
            {"low_confidence_ratio": round(low_ratio, 6)},
        )

    if "sim_count" in person.columns:
        sim_count = pd.to_numeric(person["sim_count"], errors="coerce")
        below_one = int((sim_count < 1).sum())
        emit("domain.sim_count", "failed" if below_one > 0 else "ok", {"sim_count_below_one": below_one})

    for col in ("msisdn", "imsi", "imei"):
        if col not in person.columns:
            continue
        series = person[col].astype("string").str.strip()
        nonempty = series.notna() & (series != "")
        invalid = int((nonempty & ~series.str.fullmatch(_DIGITS_RE.pattern)).sum())
        emit(
            f"domain.{col}_digits",
            "warning" if invalid > 0 else "ok",
            {f"invalid_{col}_count": invalid},
        )

    _emit_summary(total_checks=checks, warnings=warnings, failed=failed)
    return {
        "status": "failed" if failed else ("warning" if warnings else "ok"),
        "total_checks": checks,
        "warning_checks": warnings,
        "failed_checks": failed,
        **base,
        "row_count": int(len(person)),
    }


def _resolve_source_path(*, report_date: date, fct_person_path: str | Path) -> Path:
    return resolve_stg_monthly_parquet_path(fct_person_path, report_date)


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
