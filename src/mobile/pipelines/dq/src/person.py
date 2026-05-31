from __future__ import annotations

import json
import logging
import re
from datetime import date, timedelta
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

from mobile.cli_defaults import DEFAULT_DQ_SRC_PERSON_START_DATE
from mobile.pipelines.src.person import ACTUALLY_TO_OPEN
from mobile.project_paths import (
    DEFAULT_SRC_PERSON_OUTPUT_ROOT,
    SRC_PERSON_SUCCESS_FLAG,
    resolve_project_path,
)


logger = logging.getLogger(__name__)
LOG_TAG = "DQ_SRC_PERSON"

_NUMERIC_TYPES = frozenset({"int", "smallint", "long", "float"})
_DISTRIBUTION_TOP_N = 25
_UNIQUE_VALUES_MAX_CARDINALITY = 20
_DISCRETE_NUMERIC_DIST_MAX = 30

_PROFILE_SKIP_DISTRIBUTION = frozenset(
    {
        "service_list",
        "last_geo",
        "geo_json",
        "address",
        "description",
        "dul_department",
        "dul_issued_by",
    }
)

_PERIOD_LIGHT_COLUMNS = (
    "identity_type",
    "client_type",
    "operator_Id",
    "abonent_status",
    "abonent_last_location",
)

_IDENTITY_TYPE_FIELDS: dict[int, tuple[str, ...]] = {
    0: ("network_pager_id",),
    1: ("isdn_world_pstn", "additional_isdn_pstn"),
    2: ("isdn", "imsi", "imei", "iccid"),
    3: ("isdn_cdma", "cdma_imsi_a", "cdma_Imei_a", "cdma_imsi_b", "icc_cdma"),
    4: ("isdn_date_network", "isdn_date_network_imsi", "isdn_date_network_imei"),
    5: ("voip_calling_isdn",),
}

_TIMESTAMP_MONTH_COLUMNS = (
    "actually_from",
    "actually_to",
    "birth_day",
    "start_contract_date",
    "end_contract_date",
    "main_service_end_ts",
    "last_activity_ts",
)

_PERSON_STG_CRITICAL_COLUMNS = [
    "client_type",
    "actually_from",
    "actually_to",
    "isdn",
    "imsi",
    "imei",
    "first_name",
    "second_name",
    "last_name",
    "birth_day",
    "dul_department",
]

_PASSPORT_RE = re.compile(r"^\d{4} \d{6}$")
_MSISDN_LEN = 11


def _filter_days_in_range(
    day_dirs: list[dict[str, Any]],
    *,
    start_date: date,
    end_date: date,
) -> list[dict[str, Any]]:
    return [d for d in day_dirs if start_date <= d["date"] <= end_date]


def run_dq(
    *,
    start_date: date,
    end_date: date,
    person_root: Path | str,
) -> dict[str, Any]:
    success_flag = SRC_PERSON_SUCCESS_FLAG

    root = resolve_project_path(person_root)
    day_dirs_all = _discover_day_dirs(root)
    day_dirs = day_dirs_all

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

    date_filter: dict[str, Any] | None = None
    if start_date > end_date:
        summary = {
            "status": "failed",
            "reason": "invalid_date_range",
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        }
        emit("dataset_filter", "failed", summary)
        _emit_summary(total_checks=checks, warnings=warnings, failed=failed)
        return summary
    day_dirs = _filter_days_in_range(day_dirs_all, start_date=start_date, end_date=end_date)
    date_filter = {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "day_dirs_total": int(len(day_dirs_all)),
        "day_dirs_in_range": int(len(day_dirs)),
    }
    emit(
        "dataset_filter",
        "warning" if not day_dirs else "ok",
        date_filter,
    )
    _emit_period_calendar_checks(emit, start_date=start_date, end_date=end_date, day_dirs=day_dirs)

    _emit_period_volume_metrics(emit, day_dirs, success_flag=success_flag)

    for day_info in day_dirs:
        parquet_path = day_info["path"] / "person.parquet"
        emit(
            "day.coverage",
            "ok",
            {
                "calendar_day": day_info["date"].isoformat(),
                "row_count": _parquet_row_count(parquet_path) if parquet_path.exists() else 0,
                "has_parquet": parquet_path.exists(),
                "has_success": (day_info["path"] / success_flag).exists(),
            },
        )

    success_days = [day for day in day_dirs if (day["path"] / success_flag).exists()]
    success_day_dates = sorted(d["date"].isoformat() for d in success_days)
    latest_day = day_dirs[-1] if day_dirs else None
    latest_success_day = success_days[-1] if success_days else None

    emit(
        "success_days_presence",
        "failed" if len(success_days) == 0 else "ok",
        {
            "root": str(root),
            "day_dirs_count": int(len(day_dirs)),
            "success_days_count": int(len(success_days)),
            **(date_filter or {}),
        },
    )

    emit(
        "success_days_inventory",
        "ok",
        {
            "success_days": success_day_dates,
            "success_days_count": int(len(success_days)),
        },
    )

    success_share = float(len(success_days) / len(day_dirs)) if day_dirs else 0.0
    emit(
        "success_days_share",
        "ok",
        {
            "success_days_share": round(success_share, 6),
            "success_days_count": int(len(success_days)),
            "day_dirs_count": int(len(day_dirs)),
        },
    )

    if latest_success_day is not None:
        emit(
            "latest_success_day",
            "ok",
            {
                "latest_success_day": latest_success_day["date"].isoformat(),
                "success_days_count": int(len(success_days)),
            },
        )
    else:
        emit(
            "latest_success_day",
            "failed",
            {"latest_success_day": None, "reason": "no_success_days"},
        )

    if latest_day is not None:
        latest_day_has_success = (latest_day["path"] / success_flag).exists()
        emit(
            "latest_calendar_day_has_success",
            "ok" if latest_day_has_success else "warning",
            {
                "latest_calendar_day": latest_day["date"].isoformat(),
                "has_success": bool(latest_day_has_success),
            },
        )

    _emit_period_identity_aggregates(emit, day_dirs)

    selected_day = latest_success_day or latest_day
    if selected_day is None:
        _emit_summary(total_checks=checks, warnings=warnings, failed=failed)
        return {
            "status": "failed",
            "reason": "no_person_day_dirs",
            "root": str(root),
            "total_checks": checks,
            "warning_checks": warnings,
            "failed_checks": failed,
        }

    parquet_path = selected_day["path"] / "person.parquet"
    if not parquet_path.exists():
        emit(
            "dataset_presence",
            "failed",
            {"selected_day": selected_day["date"].isoformat(), "parquet_path": str(parquet_path)},
        )
        _emit_summary(total_checks=checks, warnings=warnings, failed=failed)
        return {
            "status": "failed",
            "reason": "parquet_not_found",
            "parquet_path": str(parquet_path),
            "total_checks": checks,
            "warning_checks": warnings,
            "failed_checks": failed,
        }

    data = pd.read_parquet(parquet_path)
    row_count = int(len(data))
    basic_metrics: dict[str, Any] = {
        "row_count": row_count,
        "column_count": int(len(data.columns)),
        "parquet_path": str(parquet_path),
        "selected_day": selected_day["date"].isoformat(),
        "selected_by_success": bool(latest_success_day is not None),
    }
    if date_filter is not None:
        basic_metrics["date_filter"] = date_filter
    emit("dataset_basic", "warning" if row_count == 0 else "ok", basic_metrics)

    column_names = sorted(data.columns.astype(str).tolist())
    emit(
        "dataset_columns",
        "ok",
        {
            "column_count": int(len(column_names)),
            "columns": column_names,
        },
    )

    if data.empty:
        _emit_summary(total_checks=checks, warnings=warnings, failed=failed)
        return {
            "status": "failed" if failed else "warning",
            "parquet_path": str(parquet_path),
            "total_checks": checks,
            "warning_checks": warnings,
            "failed_checks": failed,
        }

    _emit_person_stg_contract_checks(emit, data)

    profile_stats = _emit_field_profiles(emit, data)
    emit(
        "field_profile_coverage",
        "ok",
        {"total_columns": int(len(data.columns)), **profile_stats},
    )

    for col in _TIMESTAMP_MONTH_COLUMNS:
        if col not in data.columns:
            continue
        parsed = pd.to_datetime(data[col], errors="coerce")
        buckets = parsed.dt.to_period("M").astype("string")
        dist = _distribution_bundle(buckets, row_count=row_count, top_n=24)
        emit(f"distribution.{col}_month", "ok", {**dist, "field_type": "timestamp"})

    for col in ("service_list", "last_geo", "geo_json"):
        if col not in data.columns:
            continue
        series = data[col]
        non_null = int(series.notna().sum())
        lengths = series.dropna().astype("string").str.len()
        emit(
            f"string_length.{col}",
            "ok",
            {
                "non_null_count": non_null,
                "len_p50": float(lengths.quantile(0.5)) if len(lengths) else None,
                "len_p95": float(lengths.quantile(0.95)) if len(lengths) else None,
                "len_max": int(lengths.max()) if len(lengths) else None,
            },
        )

    _emit_person_snapshot_checks(emit, data, row_count=row_count)

    status = "failed" if failed else ("warning" if warnings else "ok")
    _emit_summary(total_checks=checks, warnings=warnings, failed=failed)
    result: dict[str, Any] = {
        "status": status,
        "parquet_path": str(parquet_path),
        "total_checks": checks,
        "warning_checks": warnings,
        "failed_checks": failed,
        "success_days_count": int(len(success_days)),
        "day_dirs_in_range": int(len(day_dirs)),
        "field_profile_coverage": profile_stats,
    }
    if date_filter is not None:
        result["start_date"] = date_filter["start_date"]
        result["end_date"] = date_filter["end_date"]
    return result


def _emit_period_calendar_checks(
    emit: Callable[[str, str, dict[str, Any]], None],
    *,
    start_date: date,
    end_date: date,
    day_dirs: list[dict[str, Any]],
) -> None:
    expected_days = (end_date - start_date).days + 1
    present = {d["date"] for d in day_dirs}
    missing: list[str] = []
    cursor = start_date
    while cursor <= end_date:
        if cursor not in present:
            missing.append(cursor.isoformat())
        cursor += timedelta(days=1)
    emit(
        "period.calendar_coverage",
        "warning" if missing else "ok",
        {
            "expected_calendar_days": expected_days,
            "present_day_dirs": int(len(day_dirs)),
            "missing_calendar_days": missing,
            "missing_count": int(len(missing)),
        },
    )


def _emit_period_volume_metrics(
    emit: Callable[[str, str, dict[str, Any]], None],
    day_dirs: list[dict[str, Any]],
    *,
    success_flag: str,
) -> None:
    if not day_dirs:
        emit("period.volume", "warning", {"reason": "no_days"})
        return

    daily: list[dict[str, Any]] = []
    for day in day_dirs:
        path = day["path"] / "person.parquet"
        rows = _parquet_row_count(path) if path.exists() else 0
        daily.append(
            {
                "calendar_day": day["date"].isoformat(),
                "row_count": rows,
                "has_success": (day["path"] / success_flag).exists(),
            }
        )

    counts = pd.Series([d["row_count"] for d in daily], dtype="float64")
    success_counts = [d["row_count"] for d in daily if d["has_success"]]
    partial_counts = [d["row_count"] for d in daily if not d["has_success"]]

    def _summary(series: list[int]) -> dict[str, Any]:
        if not series:
            return {"days": 0}
        s = pd.Series(series, dtype="float64")
        return {
            "days": int(len(s)),
            "min": int(s.min()),
            "p50": float(s.quantile(0.5)),
            "p95": float(s.quantile(0.95)),
            "max": int(s.max()),
            "mean": round(float(s.mean()), 2),
        }

    emit(
        "period.volume",
        "ok",
        {
            "daily_row_counts": daily,
            "all_days": _summary(counts.astype(int).tolist()),
            "success_days": _summary(success_counts),
            "partial_days": _summary(partial_counts),
            "success_to_partial_row_ratio": (
                round(float(pd.Series(success_counts).mean() / max(pd.Series(partial_counts).mean(), 1)), 4)
                if success_counts and partial_counts
                else None
            ),
        },
    )


def _emit_period_identity_aggregates(
    emit: Callable[[str, str, dict[str, Any]], None],
    day_dirs: list[dict[str, Any]],
) -> None:
    frames: list[pd.DataFrame] = []
    for day in day_dirs:
        path = day["path"] / "person.parquet"
        if not path.exists():
            continue
        try:
            schema_cols = pq.ParquetFile(path).schema_arrow.names
        except Exception:
            continue
        cols = [c for c in _PERIOD_LIGHT_COLUMNS if c in schema_cols]
        if not cols:
            continue
        chunk = pd.read_parquet(path, columns=cols)
        chunk["_calendar_day"] = day["date"].isoformat()
        frames.append(chunk)

    if not frames:
        emit("period.identity_aggregates", "warning", {"reason": "no_readable_parquet"})
        return

    period = pd.concat(frames, ignore_index=True)
    row_count = len(period)
    emit(
        "period.identity_aggregates",
        "ok",
        {"rows_scanned": row_count, "days_scanned": int(period["_calendar_day"].nunique())},
    )

    for dim in _PERIOD_LIGHT_COLUMNS:
        if dim not in period.columns:
            continue
        dist = _distribution_bundle(period[dim], row_count=row_count, top_n=25)
        emit(f"period.distribution.{dim}", "ok", dist)

    if {"identity_type", "client_type"}.issubset(period.columns):
        cross = (
            period.assign(
                identity_type=period["identity_type"].astype("string"),
                client_type=period["client_type"].astype("string"),
            )
            .groupby(["identity_type", "client_type"], dropna=False)
            .size()
            .reset_index(name="count")
        )
        rows = [
            {
                "identity_type": str(r.identity_type),
                "client_type": str(r.client_type),
                "count": int(r.count),
                "pct": round(int(r.count) / max(row_count, 1) * 100, 4),
            }
            for r in cross.itertuples(index=False)
        ]
        emit("period.cross.identity_type_x_client_type", "ok", {"rows": rows})


def _emit_person_snapshot_checks(
    emit: Callable[[str, str, dict[str, Any]], None],
    data: pd.DataFrame,
    *,
    row_count: int,
) -> None:
    citizenship_candidates = [
        c for c in data.columns if "citizen" in c.lower() or "grazhd" in c.lower() or "гражд" in c.lower()
    ]
    emit(
        "citizenship_fields_presence",
        "warning" if not citizenship_candidates else "ok",
        {"detected_fields": citizenship_candidates},
    )

    if "identity_type" in data.columns:
        itype = pd.to_numeric(data["identity_type"], errors="coerce")
        for type_id, cols in _IDENTITY_TYPE_FIELDS.items():
            mask = itype == type_id
            scoped = data.loc[mask]
            for col in cols:
                if col not in data.columns:
                    continue
                rate = float(scoped[col].notna().mean()) if len(scoped) else 0.0
                emit(
                    f"identity_type.{type_id}.{col}_fill",
                    _status_from_rate(rate, failed_below=0.90, warn_below=0.98)
                    if type_id in (2, 4, 5)
                    else "ok",
                    {
                        "identity_type": type_id,
                        "non_null_rate": round(rate, 6),
                        "rows": int(len(scoped)),
                    },
                )
        other_mask = itype.isin([0, 1, 3])
        if "isdn" in data.columns and other_mask.any():
            leak = float(data.loc[other_mask, "isdn"].notna().mean())
            emit(
                "identity_type.non_gsm_isdn_leak",
                "warning" if leak > 0.01 else "ok",
                {"non_gsm_rows_with_isdn_rate": round(leak, 6), "rows": int(other_mask.sum())},
            )

    if {"operator_Id", "isdn"}.issubset(data.columns):
        dup = int(data.duplicated(subset=["operator_Id", "isdn"], keep=False).sum())
        emit(
            "key_integrity.operator_isdn",
            "warning" if dup > 0 else "ok",
            {
                "duplicate_rows": dup,
                "distinct_keys": int(data[["operator_Id", "isdn"]].drop_duplicates().shape[0]),
            },
        )

    if {"operator_Id", "isdn", "imsi", "actually_from"}.issubset(data.columns):
        dup = int(
            data.duplicated(subset=["operator_Id", "isdn", "imsi", "actually_from"], keep=False).sum()
        )
        emit(
            "identity_duplicate_keys",
            "warning" if dup > 0 else "ok",
            {
                "duplicate_rows": dup,
                "key_columns": ["operator_Id", "isdn", "imsi", "actually_from"],
            },
        )

    if "isdn" in data.columns:
        isdn = pd.to_numeric(data["isdn"], errors="coerce").astype("Int64").astype("string")
        digits = isdn.str.replace("<NA>", "", regex=False).fillna("")
        present = digits.ne("")
        len_ok = present & (digits.str.len() == _MSISDN_LEN)
        starts_7 = present & digits.str.startswith("7")
        valid = len_ok & starts_7
        rate = float(valid.mean()) if len(digits) else 0.0
        emit(
            "isdn_format",
            _status_from_rate(rate, failed_below=0.95, warn_below=0.99),
            {
                "valid_rate": round(rate, 6),
                "invalid_len": int((present & ~len_ok).sum()),
                "invalid_prefix": int((present & ~starts_7).sum()),
            },
        )

    for col, expected_len in (("imsi", 15), ("imei", 15)):
        if col not in data.columns:
            continue
        raw = pd.to_numeric(data[col], errors="coerce").astype("Int64").astype("string")
        present = raw.notna() & (raw != "<NA>")
        lens = raw[present].str.len()
        ok = lens == expected_len
        rate = float(ok.mean()) if len(lens) else 1.0
        emit(
            f"{col}_format",
            _status_from_rate(rate, failed_below=0.95, warn_below=0.99),
            {"valid_len_rate": round(rate, 6), "expected_len": expected_len},
        )

    if "iccid" in data.columns:
        icc = data["iccid"].astype("string").str.strip()
        present = icc.notna() & icc.ne("")
        lens = icc[present].str.len()
        ok = lens.between(18, 20)
        rate = float(ok.mean()) if len(lens) else 1.0
        emit(
            "iccid_format",
            _status_from_rate(rate, failed_below=0.90, warn_below=0.98),
            {"valid_len_rate": round(rate, 6)},
        )

    if "dul_number" in data.columns:
        passport = data["dul_number"].astype("string").str.strip()
        present = passport.notna() & passport.ne("")
        ok = passport[present].map(lambda v: bool(_PASSPORT_RE.match(str(v))))
        rate = float(ok.mean()) if len(ok) else 1.0
        emit(
            "passport_format",
            _status_from_rate(rate, failed_below=0.90, warn_below=0.98),
            {"valid_format_rate": round(rate, 6), "rows_with_passport": int(present.sum())},
        )

    if "service_list" in data.columns:
        raw = data["service_list"].astype("string").fillna("")
        sep = "\x11"
        with_sep = raw.str.contains(sep, regex=False)
        non_empty = raw.ne("")
        emit(
            "service_list_format",
            "ok",
            {
                "non_empty_rate": round(float(non_empty.mean()), 6),
                "separator_present_rate": round(float(with_sep[non_empty].mean()), 6)
                if non_empty.any()
                else 0.0,
                "avg_service_count": round(
                    float((raw[non_empty].str.count(sep) + 1).mean()), 4
                )
                if non_empty.any()
                else 0.0,
            },
        )

    if {"abonent_last_location", "lac", "cell"}.issubset(data.columns):
        loc = pd.to_numeric(data["abonent_last_location"], errors="coerce")
        lac = data["lac"]
        cell = data["cell"]
        at_bs = loc == 0
        lac_filled = lac.notna() & (lac.astype("string").str.strip() != "")
        cell_filled = cell.notna() & (cell.astype("string").str.strip() != "")
        when_bs = float((lac_filled & cell_filled)[at_bs].mean()) if at_bs.any() else 0.0
        when_not = float((lac_filled | cell_filled)[~at_bs].mean()) if (~at_bs).any() else 0.0
        emit(
            "lac_cell_by_last_location",
            _status_from_rate(when_bs, failed_below=0.90, warn_below=0.98),
            {
                "at_bs_lac_cell_filled_rate": round(when_bs, 6),
                "not_at_bs_any_lac_cell_rate": round(when_not, 6),
                "rows_at_bs": int(at_bs.sum()),
            },
        )

    if {"actually_from", "actually_to"}.issubset(data.columns):
        frm = pd.to_datetime(data["actually_from"], errors="coerce")
        to = pd.to_datetime(data["actually_to"], errors="coerce")
        invalid_order = int(((to < frm) & frm.notna() & to.notna()).sum())
        active_days = (to - frm).dt.total_seconds() / 86400.0
        active_days_valid = active_days[active_days.notna()]
        emit(
            "temporal_consistency",
            "warning" if invalid_order > 0 else "ok",
            {
                "invalid_date_order_count": invalid_order,
                "actually_from_min": str(frm.min()) if frm.notna().any() else None,
                "actually_from_max": str(frm.max()) if frm.notna().any() else None,
                "actually_to_min": str(to.min()) if to.notna().any() else None,
                "actually_to_max": str(to.max()) if to.notna().any() else None,
                "active_days_p50": float(active_days_valid.quantile(0.5))
                if len(active_days_valid)
                else None,
                "active_days_p95": float(active_days_valid.quantile(0.95))
                if len(active_days_valid)
                else None,
            },
        )
        open_interval = to >= ACTUALLY_TO_OPEN
        if "abonent_status" in data.columns:
            active = pd.to_numeric(data["abonent_status"], errors="coerce") == 0
            missing_open = int((active & ~open_interval).sum())
            emit(
                "actually_to_open_interval",
                "warning" if missing_open > 0 else "ok",
                {
                    "expected_open_to": str(ACTUALLY_TO_OPEN),
                    "active_rows_missing_open_to": missing_open,
                    "open_interval_rows": int(open_interval.sum()),
                },
            )

    if {"main_service_end_ts", "end_contract_date"}.issubset(data.columns):
        end_ts = pd.to_datetime(data["main_service_end_ts"], errors="coerce")
        end_contract = pd.to_datetime(data["end_contract_date"], errors="coerce")
        closed = end_ts.notna() | end_contract.notna()
        emit(
            "closed_contract_ratio",
            "ok",
            {
                "ratio": round(float(closed.mean()), 6),
                "closed_rows": int(closed.sum()),
            },
        )

    if "birth_day" in data.columns:
        birth = pd.to_datetime(data["birth_day"], errors="coerce")
        age_years = (pd.Timestamp.utcnow().tz_localize(None) - birth).dt.days / 365.25
        invalid_age = int(((age_years < 14) | (age_years > 110)).fillna(False).sum())
        valid_age = age_years[age_years.notna()]
        profile = _numeric_profile(valid_age) if len(valid_age) else {}
        emit(
            "birth_day_quality",
            "warning" if invalid_age > 0 else "ok",
            {
                "invalid_age_count": invalid_age,
                "age_profile": profile,
            },
        )

    fio_cols = ["last_name", "first_name", "second_name"]
    if all(col in data.columns for col in fio_cols) and "client_type" in data.columns:
        ct = pd.to_numeric(data["client_type"], errors="coerce")
        physical = data.loc[ct == 0]
        corporate = data.loc[ct == 1]
        if not physical.empty:
            fio_ok = (
                physical["last_name"].astype("string").str.strip().ne("")
                & physical["first_name"].astype("string").str.strip().ne("")
            )
            emit(
                "fio_quality_physical",
                _status_from_rate(float(fio_ok.mean()), failed_below=0.95, warn_below=0.98),
                {
                    "fio_present_rate": round(float(fio_ok.mean()), 6),
                    "rows_physical": int(len(physical)),
                },
            )
        if not corporate.empty:
            fio_any = (
                corporate["last_name"].astype("string").str.strip().ne("")
                | corporate["first_name"].astype("string").str.strip().ne("")
            )
            emit(
                "fio_quality_corporate",
                "warning" if float(fio_any.mean()) > 0.05 else "ok",
                {
                    "fio_filled_rate": round(float(fio_any.mean()), 6),
                    "rows_corporate": int(len(corporate)),
                },
            )

    if "operator_Id" in data.columns:
        op = pd.to_numeric(data["operator_Id"], errors="coerce")
        dist = _distribution_bundle(
            op.map(lambda v: str(int(v)) if pd.notna(v) else "<NA>"),
            row_count=row_count,
            top_n=_DISTRIBUTION_TOP_N,
        )
        emit("distribution.operator_Id", "ok", dist)


def _infer_field_type(series: pd.Series) -> str:
    if pd.api.types.is_bool_dtype(series):
        return "boolean"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "timestamp"
    if pd.api.types.is_integer_dtype(series):
        return "long" if series.dtype.itemsize >= 8 else "int"
    if pd.api.types.is_float_dtype(series):
        return "float"
    return "string"


def _emit_field_profiles(
    emit: Callable[[str, str, dict[str, Any]], None],
    data: pd.DataFrame,
) -> dict[str, int]:
    row_count = len(data)
    stats = {
        "profiled_fields": 0,
        "distribution_checks": 0,
        "numeric_profile_checks": 0,
        "unique_values_checks": 0,
    }

    for name in data.columns:
        stats["profiled_fields"] += 1
        ftype = _infer_field_type(data[name])
        series = data[name]

        null_count = int(series.isna().sum())
        non_null = int(series.notna().sum())
        nunique = int(series.nunique(dropna=True))
        emit(
            f"nulls.{name}",
            "ok",
            {
                "null_count": null_count,
                "null_ratio": round(null_count / max(row_count, 1), 4),
                "non_null_count": non_null,
            },
        )
        emit(
            f"cardinality.{name}",
            "ok",
            {
                "nunique": nunique,
                "nunique_ratio": round(nunique / max(non_null, 1), 4) if non_null else 0.0,
                "non_null_count": non_null,
                "field_type": ftype,
            },
        )

        if nunique <= _UNIQUE_VALUES_MAX_CARDINALITY and ftype != "timestamp":
            emit(
                f"unique_values.{name}",
                "ok",
                {
                    "nunique": nunique,
                    "values": _unique_values_table(series, row_count=row_count),
                    "field_type": ftype,
                },
            )
            stats["unique_values_checks"] += 1

        if ftype == "boolean":
            emit(
                f"distribution.{name}",
                "ok",
                {**_boolean_distribution(series, row_count=row_count), "field_type": ftype},
            )
            stats["distribution_checks"] += 1
            continue

        if ftype == "timestamp":
            continue

        if ftype in _NUMERIC_TYPES:
            emit(f"numeric_profile.{name}", "ok", {**_numeric_profile(series), "field_type": ftype})
            stats["numeric_profile_checks"] += 1
            if nunique <= _DISCRETE_NUMERIC_DIST_MAX:
                emit(
                    f"distribution.{name}",
                    "ok",
                    {
                        **_distribution_bundle(
                            _numeric_distribution_labels(series),
                            row_count=row_count,
                            top_n=_DISTRIBUTION_TOP_N,
                        ),
                        "field_type": ftype,
                        "discrete_numeric": True,
                    },
                )
                stats["distribution_checks"] += 1
            continue

        if ftype == "string" and name not in _PROFILE_SKIP_DISTRIBUTION:
            emit(
                f"distribution.{name}",
                "ok",
                {
                    **_distribution_bundle(series, row_count=row_count, top_n=_DISTRIBUTION_TOP_N),
                    "field_type": ftype,
                },
            )
            stats["distribution_checks"] += 1

    return stats


def _distribution_bundle(series: pd.Series, *, row_count: int, top_n: int) -> dict[str, Any]:
    normalized = series.astype("string").fillna("<NA>")
    vc = normalized.value_counts()
    top = vc.head(top_n)
    return {
        "distinct_values": int(vc.shape[0]),
        "top_n": top_n,
        "distribution_counts": {str(k): int(v) for k, v in top.items()},
        "distribution_pct": {
            str(k): round(int(v) / max(row_count, 1) * 100, 4) for k, v in top.items()
        },
    }


def _unique_values_table(series: pd.Series, *, row_count: int) -> list[dict[str, Any]]:
    normalized = series.astype("string").fillna("<NA>")
    return [
        {
            "value": str(value),
            "count": int(count),
            "pct": round(int(count) / max(row_count, 1) * 100, 4),
        }
        for value, count in normalized.value_counts().items()
    ]


def _boolean_distribution(series: pd.Series, *, row_count: int) -> dict[str, Any]:
    normalized = series.map(
        lambda v: (
            pd.NA
            if pd.isna(v)
            else True
            if str(v).strip().lower() in {"true", "1", "t", "yes", "y"}
            else False
            if str(v).strip().lower() in {"false", "0", "f", "no", "n"}
            else "<other>"
        )
    )
    vc = normalized.value_counts()
    return {
        "distinct_values": int(vc.shape[0]),
        "distribution_counts": {str(k): int(v) for k, v in vc.items()},
        "distribution_pct": {
            str(k): round(int(v) / max(row_count, 1) * 100, 4) for k, v in vc.items()
        },
    }


def _numeric_distribution_labels(series: pd.Series) -> pd.Series:
    num = pd.to_numeric(series, errors="coerce")

    def _label(v: Any) -> str:
        if pd.isna(v):
            return "<NA>"
        fv = float(v)
        return str(int(fv)) if fv.is_integer() else str(fv)

    return num.map(_label)


def _numeric_profile(series: pd.Series) -> dict[str, Any]:
    num = pd.to_numeric(series, errors="coerce")
    valid = num.dropna()
    non_numeric = int(series.notna().sum() - valid.count())
    if valid.empty:
        return {
            "non_null_count": 0,
            "non_numeric_count": non_numeric,
            "all_null_or_non_numeric": True,
        }
    return {
        "non_null_count": int(valid.count()),
        "non_numeric_count": non_numeric,
        "min": float(valid.min()),
        "p50": float(valid.quantile(0.5)),
        "p95": float(valid.quantile(0.95)),
        "max": float(valid.max()),
        "mean": round(float(valid.mean()), 4),
        "std": round(float(valid.std()), 4) if len(valid) > 1 else 0.0,
    }


def _status_from_rate(rate: float, *, failed_below: float, warn_below: float) -> str:
    if rate < failed_below:
        return "failed"
    if rate < warn_below:
        return "warning"
    return "ok"


def _emit_person_stg_contract_checks(
    emit: Callable[[str, str, dict[str, Any]], None],
    data: pd.DataFrame,
) -> None:
    missing = [c for c in _PERSON_STG_CRITICAL_COLUMNS if c not in data.columns]
    emit(
        "stg_contract.columns",
        "failed" if missing else "ok",
        {"required_fields": _PERSON_STG_CRITICAL_COLUMNS, "missing_fields": missing},
    )
    if missing or data.empty:
        return

    ct = pd.to_numeric(data["client_type"], errors="coerce")
    physical = data.loc[ct == 0].copy()
    emit(
        "stg_contract.physical_rows",
        "ok" if len(physical) > 0 else "failed",
        {"physical_rows": int(len(physical)), "total_rows": int(len(data))},
    )
    if physical.empty:
        return

    if "identity_type" in physical.columns:
        gsm = physical.loc[pd.to_numeric(physical["identity_type"], errors="coerce") == 2]
        for col in ("isdn", "imsi", "imei"):
            target = gsm if col in ("imsi", "imei") else physical
            non_null = float(target[col].notna().mean()) if len(target) else 0.0
            emit(
                f"stg_contract.physical.{col}_present",
                _status_from_rate(non_null, failed_below=0.99, warn_below=0.995),
                {
                    "non_null_rate": round(non_null, 6),
                    "rows_physical": int(len(target)),
                    "identity_scope": "gsm" if col in ("imsi", "imei") else "all",
                },
            )
    else:
        for col in ("isdn", "imsi", "imei"):
            non_null = float(physical[col].notna().mean())
            emit(
                f"stg_contract.physical.{col}_present",
                _status_from_rate(non_null, failed_below=0.99, warn_below=0.995),
                {"non_null_rate": round(non_null, 6), "rows_physical": int(len(physical))},
            )

    frm = pd.to_datetime(physical["actually_from"], errors="coerce")
    to = pd.to_datetime(physical["actually_to"], errors="coerce")
    order_ok = float(((to >= frm) | to.isna() | frm.isna()).mean())
    emit(
        "stg_contract.physical.interval_order",
        _status_from_rate(order_ok, failed_below=0.995, warn_below=0.999),
        {"valid_order_rate": round(order_ok, 6)},
    )

    fio_ok = float(
        (
            physical["last_name"].astype("string").str.strip().ne("")
            & physical["first_name"].astype("string").str.strip().ne("")
        ).mean()
    )
    emit(
        "stg_contract.physical.fio_present",
        _status_from_rate(fio_ok, failed_below=0.95, warn_below=0.98),
        {"fio_present_rate": round(fio_ok, 6)},
    )


def _discover_day_dirs(root: Path) -> list[dict[str, Any]]:
    if not root.exists():
        return []
    out: list[dict[str, Any]] = []
    for p in sorted(root.glob("load_year=*/load_month=*/load_day=*")):
        y = _extract_int(p, "load_year")
        m = _extract_int(p, "load_month")
        d = _extract_int(p, "load_day")
        if y is None or m is None or d is None:
            continue
        try:
            out.append({"path": p, "date": date(y, m, d)})
        except ValueError:
            continue
    return sorted(out, key=lambda x: x["date"])


def _parquet_row_count(path: Path) -> int:
    try:
        return int(pq.ParquetFile(path).metadata.num_rows)
    except Exception:
        return 0


def _extract_int(path: Path, prefix: str) -> int | None:
    for part in path.parts:
        if part.startswith(prefix + "="):
            tail = part.split("=", 1)[1]
            try:
                return int(tail)
            except ValueError:
                return None
    return None


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
        "metrics": {"total_checks": total_checks, "warning_checks": warnings, "failed_checks": failed},
    }
    logger.info(json.dumps(payload, ensure_ascii=False, sort_keys=True))
