"""DQ витрин ``stg_person`` и ``stg_person_sim`` за отчётный месяц."""

from __future__ import annotations

import json
import logging
import re
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from mobile.pipelines.stg.person import STG_PERSON_FIELDS, STG_PERSON_SIM_FIELDS
from mobile.project_paths import (
    DEFAULT_STG_OKSM_OUTPUT_PATH,
    resolve_project_path,
    stg_person_id_ledger_output_path,
    stg_person_output_path,
    stg_person_sim_output_path,
)

logger = logging.getLogger(__name__)
LOG_TAG = "DQ_STG_PERSON"

_PERSON_ID_RE = re.compile(r"^prs_[0-9a-f]{24}$")
_DIGITS_RE = re.compile(r"^\d+$")
_CITIZENSHIP_RE = re.compile(r"^\d{3}$")
_GENDER_VALUES = frozenset({"M", "F", "U"})
_CONFIDENCE_VALUES = frozenset({"high", "medium", "low"})
_NODE_PREFIXES = ("bio:", "contract:", "iccid:", "msisdn:", "imsi:", "imei:")
_LOW_CONFIDENCE_WARN_RATIO = 0.30

_PERSON_COLUMNS = [field["name"] for field in STG_PERSON_FIELDS]
_PERSON_SIM_COLUMNS = [field["name"] for field in STG_PERSON_SIM_FIELDS]
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
    stg_person_path: str | Path | None = None,
    stg_person_sim_path: str | Path | None = None,
    stg_oksm_path: str | Path | None = None,
    stg_person_ledger_path: str | Path | None = None,
) -> dict[str, Any]:
    """Read-only DQ ``stg_person`` / ``stg_person_sim`` (и опционально ledger) за ``report_date``."""
    person_path = _resolve_person_path(report_date, stg_person_path)
    sim_path = _resolve_sim_path(report_date, stg_person_sim_path)
    oksm_path = resolve_project_path(stg_oksm_path or DEFAULT_STG_OKSM_OUTPUT_PATH)
    ledger_path = (
        resolve_project_path(stg_person_ledger_path)
        if stg_person_ledger_path is not None
        else stg_person_id_ledger_output_path(report_date)
    )

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

    if not person_path.exists():
        emit(
            "dataset_presence",
            "failed",
            {
                "reason": "parquet_not_found",
                "report_date": report_date.isoformat(),
                "stg_person_path": str(person_path),
            },
        )
        _emit_summary(total_checks=checks, warnings=warnings, failed=failed)
        return _result(
            report_date=report_date,
            person_path=person_path,
            sim_path=sim_path,
            checks=checks,
            warnings=warnings,
            failed=failed,
        )

    person = pd.read_parquet(person_path)
    emit(
        "dataset_basic",
        "ok",
        {
            "report_date": report_date.isoformat(),
            "stg_person_path": str(person_path),
            "row_count": int(len(person)),
            "column_count": int(len(person.columns)),
            "distinct_person_id": int(person["person_id"].nunique()) if "person_id" in person.columns else 0,
        },
    )

    if not sim_path.exists():
        emit(
            "person_sim_presence",
            "failed",
            {
                "reason": "parquet_not_found",
                "stg_person_sim_path": str(sim_path),
            },
        )
        sim = pd.DataFrame(columns=_PERSON_SIM_COLUMNS)
    else:
        sim = pd.read_parquet(sim_path)
        emit(
            "person_sim_basic",
            "ok",
            {
                "stg_person_sim_path": str(sim_path),
                "row_count": int(len(sim)),
                "column_count": int(len(sim.columns)),
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

    sim_missing = [col for col in _PERSON_SIM_COLUMNS if col not in sim.columns]
    sim_extra = [col for col in sim.columns if col not in _PERSON_SIM_COLUMNS] if len(sim.columns) else []
    if sim_path.exists():
        emit(
            "schema_columns_sim",
            "failed" if sim_missing else ("warning" if sim_extra else "ok"),
            {
                "expected_columns": _PERSON_SIM_COLUMNS,
                "missing_columns": sim_missing,
                "extra_columns": sim_extra,
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
        return _result(
            report_date=report_date,
            person_path=person_path,
            sim_path=sim_path,
            checks=checks,
            warnings=warnings,
            failed=failed,
            row_count=0,
        )

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
        single = len(distinct) == 1 and distinct[0] == report_date
        emit(
            "key.report_date_single",
            "failed" if not single else "ok",
            {
                "distinct_report_date": [d.isoformat() for d in distinct],
                "expected_report_date": report_date.isoformat(),
            },
        )

    if "person_id" in person.columns and "person_id" in sim.columns and not sim.empty:
        person_ids = set(person["person_id"].astype("string").dropna())
        sim_ids = set(sim["person_id"].astype("string").dropna())
        orphans = sim_ids - person_ids
        orphan_rows = int(sim["person_id"].astype("string").isin(orphans).sum()) if orphans else 0
        emit(
            "key.person_sim_orphans",
            "failed" if orphan_rows > 0 else "ok",
            {"orphan_sim_rows": orphan_rows, "orphan_person_id_count": len(orphans)},
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

    if (
        not sim.empty
        and {"person_id", "sim_count"}.issubset(person.columns)
        and {"person_id", "imsi", "iccid"}.issubset(sim.columns)
    ):
        sim_keys = sim.copy()
        sim_keys["sim_key"] = sim_keys["imsi"].astype("string") + "|" + sim_keys["iccid"].astype("string").fillna(
            ""
        )
        agg = sim_keys.groupby("person_id", as_index=False).agg(
            sim_rows=("person_id", "size"),
            distinct_sim_keys=("sim_key", "nunique"),
        )
        merged = person[["person_id", "sim_count"]].merge(agg, on="person_id", how="left")
        merged["sim_count"] = pd.to_numeric(merged["sim_count"], errors="coerce")
        mismatch = int((merged["sim_count"] != merged["distinct_sim_keys"]).sum())
        emit(
            "sim_count_consistency",
            "warning" if mismatch > 0 else "ok",
            {"mismatch_person_count": mismatch},
        )

    if not sim.empty and {"person_id", "is_primary"}.issubset(sim.columns):
        primary_counts = sim.groupby("person_id")["is_primary"].apply(
            lambda values: int(values.fillna(False).astype(bool).sum())
        )
        zero_or_many = int((primary_counts != 1).sum())
        emit(
            "primary_sim",
            "failed" if zero_or_many > 0 else "ok",
            {"person_ids_without_exactly_one_primary": zero_or_many},
        )

        if {"msisdn", "imsi", "imei"}.issubset(person.columns) and {"msisdn", "imsi", "imei"}.issubset(sim.columns):
            prim = sim.loc[sim["is_primary"].fillna(False)].copy()
            prim = prim.drop_duplicates(subset=["person_id"], keep="first")
            prof = person[["person_id", "msisdn", "imsi", "imei"]].copy()
            joined = prof.merge(
                prim[["person_id", "msisdn", "imsi", "imei"]],
                on="person_id",
                how="left",
                suffixes=("_person", "_sim"),
            )
            mismatches = 0
            for col in ("msisdn", "imsi", "imei"):
                left = _norm_series(joined[f"{col}_person"])
                right = _norm_series(joined[f"{col}_sim"])
                both = left.notna() & right.notna()
                mismatches += int((both & (left != right)).sum())
            emit(
                "primary_matches_profile",
                "warning" if mismatches > 0 else "ok",
                {"mismatched_identifier_fields": mismatches},
            )

    if ledger_path.exists() and not person.empty:
        ledger = pd.read_parquet(ledger_path)
        emit(
            "ledger_basic",
            "ok",
            {
                "stg_person_ledger_path": str(ledger_path),
                "row_count": int(len(ledger)),
            },
        )
        if {"person_id", "node"}.issubset(ledger.columns):
            ledger_pids = set(ledger["person_id"].astype("string").dropna())
            person_pids = set(person["person_id"].astype("string").dropna())
            missing_ledger = person_pids - ledger_pids
            emit(
                "ledger.person_coverage",
                "warning" if missing_ledger else "ok",
                {"person_ids_without_ledger_nodes": len(missing_ledger)},
            )
            nodes = ledger["node"].astype("string").dropna()
            invalid_nodes = int((~nodes.str.startswith(_NODE_PREFIXES)).sum())
            emit(
                "ledger.node_prefix",
                "warning" if invalid_nodes > 0 else "ok",
                {"invalid_node_prefix_count": invalid_nodes},
            )

    _emit_summary(total_checks=checks, warnings=warnings, failed=failed)
    return _result(
        report_date=report_date,
        person_path=person_path,
        sim_path=sim_path,
        checks=checks,
        warnings=warnings,
        failed=failed,
        row_count=int(len(person)),
    )


def _resolve_person_path(report_date: date, path: str | Path | None) -> Path:
    if path is None:
        return stg_person_output_path(report_date)
    return resolve_project_path(path)


def _resolve_sim_path(report_date: date, path: str | Path | None) -> Path:
    if path is None:
        return stg_person_sim_output_path(report_date)
    return resolve_project_path(path)


def _norm_series(series: pd.Series) -> pd.Series:
    return series.astype("string").str.strip().where(series.notna(), pd.NA)


def _result(
    *,
    report_date: date,
    person_path: Path,
    sim_path: Path,
    checks: int,
    warnings: int,
    failed: int,
    row_count: int | None = None,
) -> dict[str, Any]:
    status = "failed" if failed else ("warning" if warnings else "ok")
    out: dict[str, Any] = {
        "status": status,
        "report_date": report_date.isoformat(),
        "stg_person_path": str(person_path),
        "stg_person_sim_path": str(sim_path),
        "total_checks": checks,
        "warning_checks": warnings,
        "failed_checks": failed,
    }
    if row_count is not None:
        out["row_count"] = row_count
    return out


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
