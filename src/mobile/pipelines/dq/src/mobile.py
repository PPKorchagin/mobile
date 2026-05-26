from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from mobile.project_paths import (
    MART_PARQUET_FILES,
    discover_mart_parquet_paths,
    filter_df_by_local_report_date,
    filter_paths_near_report_date,
    read_all_parquets_concat,
    resolve_project_path,
    started_parseable_mask,
)


logger = logging.getLogger(__name__)
LOG_TAG = "DQ_SRC_MOBILE"

_EXPECTED_EVENT_SEG = {"cdr": "10001", "sms": "10002", "gprs": "10003", "location": "10004"}

SRC_CDR_FIELDS: list[dict[str, str]] = [
    {"name": "Started", "type": "string"},
    {"name": "Duration", "type": "uint32"},
    {"name": "Category", "type": "uint8"},
    {"name": "Event", "type": "uint32"},
    {"name": "Service", "type": "uint32"},
    {"name": "CallingNumber", "type": "string"},
    {"name": "CallingSource", "type": "string"},
    {"name": "CallingRegion", "type": "string"},
    {"name": "CalledNumber", "type": "string"},
    {"name": "CalledSource", "type": "string"},
    {"name": "CalledRegion", "type": "string"},
    {"name": "DialedNumber", "type": "string"},
    {"name": "Owner", "type": "int"},
    {"name": "IMSI", "type": "string"},
    {"name": "IMEI", "type": "string"},
    {"name": "BSStartLac", "type": "uint16"},
    {"name": "BSStartCell", "type": "uint32"},
    {"name": "BSEndLac", "type": "uint16"},
    {"name": "BSEndCell", "type": "uint32"},
    {"name": "RouteIn", "type": "string"},
    {"name": "RouteOut", "type": "string"},
    {"name": "RecEntNumber", "type": "string"},
    {"name": "OwnerMCCMNC", "type": "string"},
    {"name": "RecipientMCCMNC", "type": "string"},
    {"name": "RecEntOwnerRegion", "type": "string"},
    {"name": "dateTimeOriginal", "type": "string"},
    {"name": "Custom", "type": "string"},
    {"name": "RecEntType", "type": "string"},
    {"name": "PartyEntType", "type": "string"},
    {"name": "SequenceID", "type": "string"},
    {"name": "CauseDiagnostic", "type": "string"},
    {"name": "OwnerMSRNNumber", "type": "string"},
    {"name": "OtherMSRNNumber", "type": "string"},
    {"name": "Intermediate", "type": "string"},
]

SRC_SMS_FIELDS: list[dict[str, str]] = [
    {"name": "Started", "type": "string"},
    {"name": "Event", "type": "int"},
    {"name": "Calling", "type": "string"},
    {"name": "Called", "type": "string"},
    {"name": "Owner", "type": "int"},
    {"name": "SMSC", "type": "long"},
    {"name": "IMSI", "type": "string"},
    {"name": "IMEI", "type": "string"},
    {"name": "MCC", "type": "int"},
    {"name": "MNC", "type": "int"},
    {"name": "Lac", "type": "int"},
    {"name": "Cell", "type": "int"},
    {"name": "MAC", "type": "string"},
    {"name": "BSID", "type": "int"},
    {"name": "Latitude", "type": "double"},
    {"name": "Longitude", "type": "double"},
    {"name": "PDPAddress.IPV4", "type": "string"},
    {"name": "PDPAddress.IPV6", "type": "string"},
    {"name": "PDPAddress.Port", "type": "string"},
    {"name": "Message", "type": "string"},
    {"name": "Custom", "type": "string"},
]

SRC_GPRS_FIELDS: list[dict[str, str]] = [
    {"name": "Started", "type": "string"},
    {"name": "Duration", "type": "uint32"},
    {"name": "Category", "type": "uint8"},
    {"name": "Upload", "type": "string"},
    {"name": "Download", "type": "string"},
    {"name": "Event", "type": "uint32"},
    {"name": "Service", "type": "uint32"},
    {"name": "CauseDiagnostic", "type": "string"},
    {"name": "CallingNumber", "type": "string"},
    {"name": "CallingSource", "type": "string"},
    {"name": "CallingRegion", "type": "string"},
    {"name": "CalledNumber", "type": "string"},
    {"name": "CalledSource", "type": "string"},
    {"name": "CalledRegion", "type": "string"},
    {"name": "DialedNumber", "type": "string"},
    {"name": "Owner", "type": "int"},
    {"name": "PDPV4Address", "type": "string"},
    {"name": "PDPV6Address", "type": "string"},
    {"name": "IMSI", "type": "string"},
    {"name": "IMEI", "type": "string"},
    {"name": "APN", "type": "string"},
    {"name": "BSStartLac", "type": "uint16"},
    {"name": "BSStartCell", "type": "uint32"},
    {"name": "BSEndLac", "type": "uint16"},
    {"name": "BSEndCell", "type": "uint32"},
    {"name": "RouteIn", "type": "string"},
    {"name": "RouteOut", "type": "string"},
    {"name": "RecEntType", "type": "string"},
    {"name": "RecEntNumber", "type": "string"},
    {"name": "PartyEntType", "type": "string"},
    {"name": "PartyEntNumber", "type": "string"},
    {"name": "OwnerMCCMNC", "type": "string"},
    {"name": "RecipientMCCMNC", "type": "string"},
    {"name": "PartyMCCMNC", "type": "string"},
    {"name": "RecEntOwnerRegion", "type": "string"},
    {"name": "dateTimeOriginal", "type": "string"},
    {"name": "Custom", "type": "string"},
    {"name": "RAT", "type": "string"},
    {"name": "LT", "type": "string"},
]

SRC_LOCATION_FIELDS: list[dict[str, str]] = [
    {"name": "Started", "type": "string"},
    {"name": "Event", "type": "int"},
    {"name": "Served", "type": "string"},
    {"name": "IMSI", "type": "string"},
    {"name": "IMEI", "type": "string"},
    {"name": "MCC", "type": "string"},
    {"name": "MNC", "type": "string"},
    {"name": "Lac", "type": "int"},
    {"name": "Cell", "type": "int"},
    {"name": "MAC", "type": "int"},
    {"name": "BSID", "type": "int"},
    {"name": "Latitude", "type": "double"},
    {"name": "Longitude", "type": "double"},
    {"name": "IP4Address", "type": "string"},
    {"name": "IP6Address", "type": "string"},
    {"name": "Port", "type": "int"},
    {"name": "TA", "type": "int"},
    {"name": "Source", "type": "int"},
    {"name": "Custom", "type": "string"},
]

MOBILE_STG_CRITICAL_BY_MART: dict[str, list[str]] = {
    "cdr": [
        "Started",
        "Duration",
        "Owner",
        "CallingNumber",
        "CalledNumber",
        "IMSI",
        "BSStartLac",
        "BSStartCell",
        "dateTimeOriginal",
    ],
    "gprs": [
        "Started",
        "Duration",
        "Owner",
        "CallingNumber",
        "CalledNumber",
        "IMSI",
        "BSStartLac",
        "BSStartCell",
        "dateTimeOriginal",
    ],
    "sms": ["Started", "Owner", "Calling", "Called", "IMSI", "Lac", "Cell"],
    "location": ["Started", "Served", "IMSI", "Lac", "Cell"],
}

_MOBILE_IMSI_DQ_FAILED_BELOW_BY_MART: dict[str, float] = {
    "cdr": 0.35,
    "gprs": 0.40,
    "sms": 0.20,
    "location": 0.15,
}
_MOBILE_IMSI_DQ_WARN_BELOW_BY_MART: dict[str, float] = {
    "cdr": 0.45,
    "gprs": 0.50,
    "sms": 0.30,
    "location": 0.22,
}
_OWNER_VALID = {1, 2}

_DISTRIBUTION_TOP_N = 12

# Поля для value_counts / numeric summary (понимание профиля витрины за день).
_DISTRIBUTION_COLUMNS_BY_MART: dict[str, tuple[str, ...]] = {
    "cdr": (
        "Owner",
        "Category",
        "Service",
        "Event",
        "Duration",
        "RecEntOwnerRegion",
        "CallingRegion",
        "CalledRegion",
    ),
    "sms": ("Owner", "Event", "MCC", "MNC", "SMSC"),
    "gprs": (
        "Owner",
        "Category",
        "Service",
        "Event",
        "Duration",
        "RAT",
        "RecEntOwnerRegion",
        "APN",
    ),
    "location": ("Event", "MCC", "MNC", "Source", "TA"),
}


def run_dq(
    dc: str,
    report_date: date,
    cdr_path: str | Path,
    sms_path: str | Path,
    gprs_path: str | Path,
    location_path: str | Path,
) -> dict[str, Any]:
    """DQ mobile-витрин одного ЦОД за отчётную дату в локальном времени абонента.

    Строки отбираются по полю ``Started`` (``YYYYMMDDhhmmss`` в локальном времени, как в ``build-src-mobile``),
    а не по сегменту ``YYYY/MM/DD`` в пути parquet. Файлы читаются из каталогов витрин с окном ±1 день
    по календарю в пути, чтобы не пропустить события на границе суток.
    """
    mart_roots = {
        "cdr": resolve_project_path(cdr_path),
        "sms": resolve_project_path(sms_path),
        "gprs": resolve_project_path(gprs_path),
        "location": resolve_project_path(location_path),
    }
    configs = {
        "cdr": {"fields": SRC_CDR_FIELDS},
        "sms": {"fields": SRC_SMS_FIELDS},
        "gprs": {"fields": SRC_GPRS_FIELDS},
        "location": {"fields": SRC_LOCATION_FIELDS},
    }
    report_day = report_date.isoformat()
    date_filter: dict[str, Any] = {
        "report_date": report_day,
        "filter": "Started_local_subscriber_date",
    }

    checks = 0
    warnings = 0
    failed = 0

    def emit_metric(check: str, metrics: dict[str, Any], *, mart: str | None = None) -> None:
        nonlocal checks
        checks += 1
        _emit_log(check, "info", metrics, mart=mart)

    def emit_gate(check: str, status: str, metrics: dict[str, Any], *, mart: str | None = None) -> None:
        nonlocal checks, warnings, failed
        checks += 1
        if status == "warning":
            warnings += 1
        elif status == "failed":
            failed += 1
        _emit_log(check, status, metrics, mart=mart)

    mart_paths: dict[str, list[Path]] = {}
    mart_dfs: dict[str, pd.DataFrame] = {}
    mart_rows: dict[str, int] = {}
    mart_files_scanned: dict[str, int] = {}
    rows_before_filter = 0

    for key, cfg in configs.items():
        paths_all = discover_mart_parquet_paths(mart_roots[key], MART_PARQUET_FILES[key])
        paths = filter_paths_near_report_date(paths_all, report_date=report_date)
        mart_paths[key] = paths
        mart_files_scanned[key] = len(paths)
        df_all = read_all_parquets_concat(paths)
        rows_before_filter += int(len(df_all))
        df = filter_df_by_local_report_date(df_all, report_date)
        mart_dfs[key] = df
        mart_rows[key] = int(len(df))
        cov: dict[str, Any] = {
            "datacenter": dc,
            "mart_root": str(mart_roots[key]),
            "parquet_files_scanned": int(len(paths)),
            "row_count_before_local_filter": int(len(df_all)),
            "row_count_total": int(len(df)),
        }
        cov["date_filter"] = {
            **date_filter,
            "rows_before": int(len(df_all)),
            "rows_after": int(len(df)),
        }
        emit_metric(f"{key}.coverage", cov, mart=key)

    emit_metric(
        "dataset_filter",
        {
            **date_filter,
            "rows_before": int(rows_before_filter),
            "rows_after": int(sum(mart_rows.values())),
            "parquet_files_scanned": int(sum(mart_files_scanned.values())),
        },
        mart="cross_mart",
    )

    total_mobile = sum(mart_rows.values())
    row_totals = {k: int(mart_rows.get(k, 0)) for k in configs}
    if total_mobile > 0:
        gprs_share = mart_rows.get("gprs", 0) / total_mobile
        loc = mart_rows.get("location", 0)
        gprs = max(1, mart_rows.get("gprs", 0))
        loc_ratio = loc / gprs
        emit_metric(
            "cross_mart.traffic_mix",
            {
                "row_totals": row_totals,
                "total_rows": int(total_mobile),
                "gprs_share": round(gprs_share, 6),
                "location_to_gprs_row_ratio": round(loc_ratio, 6),
            },
            mart="cross_mart",
        )
        _emit_cross_mart_day_traffic_mix(report_day, row_totals, emit_metric)
    else:
        emit_metric(
            "cross_mart.traffic_mix",
            {"reason": "no_rows_in_any_mart", "row_totals": row_totals},
            mart="cross_mart",
        )
        _emit_cross_mart_day_traffic_mix(
            report_day,
            {m: 0 for m in configs},
            emit_metric,
        )

    for key, cfg in configs.items():
        paths = mart_paths[key]
        df = mart_dfs[key]
        emit_metric(
            f"{key}.day.coverage",
            {
                "calendar_day": report_day,
                "local_time_field": "Started",
                "parquet_files": len(paths),
                "row_count_total": int(len(df)),
            },
            mart=key,
        )
        if paths or not df.empty:
            _emit_mart_deep_metrics(
                key,
                cfg,
                df,
                paths,
                emit_metric,
                emit_gate,
                calendar_day=report_day,
            )
            _emit_mart_deep_metrics(
                key,
                cfg,
                df,
                paths,
                emit_metric,
                emit_gate,
                calendar_day=None,
            )
        required = MOBILE_STG_CRITICAL_BY_MART.get(key, [])
        missing = [c for c in required if df.empty or c not in df.columns]
        emit_gate(
            f"{key}.stg_contract.columns",
            "failed" if missing else "ok",
            {"required_fields": required, "missing_fields": missing},
            mart=key,
        )

    _emit_summary(total_checks=checks, warnings=warnings, failed=failed)
    return {
        "status": "failed" if failed else ("warning" if warnings else "ok"),
        "total_checks": checks,
        "warning_checks": warnings,
        "failed_checks": failed,
        "mart_rows": row_totals,
        "mart_files_scanned": {k: int(mart_files_scanned.get(k, 0)) for k in configs},
        "report_date": report_day,
        "datacenter": dc,
        "mart_paths": {k: str(mart_roots[k]) for k in configs},
    }


def _emit_cross_mart_day_traffic_mix(
    calendar_day: str,
    rows: dict[str, int],
    emit_metric: Callable[..., None],
) -> None:
    total = int(sum(int(rows.get(m, 0)) for m in ("cdr", "sms", "gprs", "location")))
    if total <= 0:
        emit_metric(
            "cross_mart.day_traffic_mix",
            {
                "calendar_day": calendar_day,
                "reason": "no_rows_for_day",
                "row_totals": {m: int(rows.get(m, 0)) for m in ("cdr", "sms", "gprs", "location")},
                "total_rows": 0,
            },
            mart="cross_mart",
        )
        return
    gprs_share = int(rows.get("gprs", 0)) / total
    loc = int(rows.get("location", 0))
    gprs = max(1, int(rows.get("gprs", 0)))
    loc_ratio = loc / gprs
    emit_metric(
        "cross_mart.day_traffic_mix",
        {
            "calendar_day": calendar_day,
            "row_totals": {m: int(rows.get(m, 0)) for m in ("cdr", "sms", "gprs", "location")},
            "total_rows": total,
            "gprs_share": round(gprs_share, 6),
            "location_to_gprs_row_ratio": round(loc_ratio, 6),
        },
        mart="cross_mart",
    )


def _emit_mart_deep_metrics(
    key: str,
    cfg: dict[str, Any],
    df: pd.DataFrame,
    paths_for_segment: list[Path],
    emit_metric: Callable[..., None],
    emit_gate: Callable[..., None],
    *,
    calendar_day: str | None,
) -> None:
    fields = cfg.get("fields", [])
    expected_columns = [f["name"] for f in fields]
    is_day = calendar_day is not None

    def chk(suffix: str) -> str:
        return f"{key}.day.{suffix}" if is_day else f"{key}.{suffix}"

    base: dict[str, Any] = {}
    if is_day:
        base["calendar_day"] = calendar_day

    n_files = len(paths_for_segment)
    if df.empty:
        emit_metric(
            chk("sample_read"),
            {**base, "reason": "empty_sample", "parquet_files": n_files},
            mart=key,
        )
        return

    emit_metric(
        chk("sample_basic"),
        {
            **base,
            "total_rows": int(len(df)),
            "column_count": int(len(df.columns)),
            "parquet_files": n_files,
            "full_scan": True,
        },
        mart=key,
    )
    missing = [c for c in expected_columns if c not in df.columns]
    emit_metric(
        chk("schema_columns"),
        {**base, "missing_columns": missing, "expected_count": len(expected_columns), "present_count": len(df.columns)},
        mart=key,
    )
    seg = _EXPECTED_EVENT_SEG[key]
    bad_seg_files = sum(1 for p in paths_for_segment if seg not in str(p).replace("\\", "/"))
    emit_metric(
        chk("path_event_segment"),
        {
            **base,
            "expected_segment": seg,
            "files_without_segment_in_path": int(bad_seg_files),
            "total_files": n_files,
        },
        mart=key,
    )
    if "Started" in df.columns:
        parseable = started_parseable_mask(df["Started"])
        ratio = float(parseable.mean()) if len(df) else 1.0
        bad_ratio = 1.0 - ratio
        emit_metric(
            chk("started_parseable"),
            {**base, "parseable_ratio": round(ratio, 6), "unparseable_ratio": round(bad_ratio, 6)},
            mart=key,
        )
    _emit_mart_distributions(key, df, emit_metric, chk, base)
    _emit_null_rate_profile(df, expected_columns, emit_metric, chk, base, mart=key)
    if key == "location" and {"Latitude", "Longitude"}.issubset(df.columns):
        lat = pd.to_numeric(df["Latitude"], errors="coerce")
        lon = pd.to_numeric(df["Longitude"], errors="coerce")
        bad_lat = int((lat.notna() & ~lat.between(-90, 90)).sum())
        bad_lon = int((lon.notna() & ~lon.between(-180, 180)).sum())
        emit_metric(
            chk("spatial_ranges_sample"),
            {**base, "invalid_lat": bad_lat, "invalid_lon": bad_lon},
            mart="location",
        )
    if key in ("cdr", "gprs") and {"IMSI", "Started"}.issubset(df.columns):
        dup = int(df.duplicated(subset=["IMSI", "Started"], keep=False).sum())
        emit_metric(
            chk("imsi_started_duplicates_sample"),
            {**base, "duplicate_rows": dup},
            mart=key,
        )

    _emit_stg_field_checks(
        emit=lambda c, s, m: emit_gate(c, s, m, mart=key),
        check_prefix=chk("mobile"),
        df=df,
        mart=key,
        base=base,
    )


def _distribution_metrics(series: pd.Series) -> dict[str, Any]:
    null_count = int(series.isna().sum())
    non_null = series.notna()
    non_null_count = int(non_null.sum())
    out: dict[str, Any] = {
        "null_count": null_count,
        "non_null_count": non_null_count,
        "unique_count": int(series.nunique(dropna=True)),
    }
    if non_null_count == 0:
        return out

    num = pd.to_numeric(series[non_null], errors="coerce")
    numeric_ok = int(num.notna().sum())
    if numeric_ok >= max(1, int(0.85 * non_null_count)):
        nn = num.dropna()
        out["kind"] = "numeric"
        out["min"] = float(nn.min())
        out["max"] = float(nn.max())
        out["mean"] = round(float(nn.mean()), 4)
        for q_label, q_val in (("p25", 0.25), ("p50", 0.5), ("p75", 0.75), ("p95", 0.95), ("p99", 0.99)):
            out[q_label] = round(float(nn.quantile(q_val)), 4)
        return out

    out["kind"] = "categorical"
    vc = series[non_null].astype("string").str.strip().value_counts().head(_DISTRIBUTION_TOP_N)
    out["value_counts_top"] = {str(k): int(v) for k, v in vc.items()}
    return out


def _emit_mart_distributions(
    mart: str,
    df: pd.DataFrame,
    emit_metric: Callable[..., None],
    chk: Callable[[str], str],
    base: dict[str, Any],
) -> None:
    for col in _DISTRIBUTION_COLUMNS_BY_MART.get(mart, ()):
        if col not in df.columns:
            continue
        metrics = _distribution_metrics(df[col])
        emit_metric(chk(f"distribution.{col}"), {**base, "column": col, **metrics}, mart=mart)

    if "Started" not in df.columns:
        return
    s = df["Started"].astype("string").str.strip()
    ok = started_parseable_mask(s)
    if not bool(ok.any()):
        return
    hours = s.loc[ok].str[8:10]
    hour_vc = hours.value_counts().sort_index().head(24)
    emit_metric(
        chk("distribution.Started_hour"),
        {
            **base,
            "kind": "categorical",
            "column": "Started",
            "value_counts_top": {str(k): int(v) for k, v in hour_vc.items()},
            "unique_count": int(hour_vc.size),
        },
        mart=mart,
    )


def _emit_null_rate_profile(
    df: pd.DataFrame,
    expected_columns: list[str],
    emit_metric: Callable[..., None],
    chk: Callable[[str], str],
    base: dict[str, Any],
    *,
    mart: str,
) -> None:
    rates: dict[str, float] = {}
    present = 0
    for col in expected_columns:
        if col not in df.columns:
            rates[col] = 1.0
            continue
        present += 1
        rates[col] = round(float(df[col].isna().mean()), 6)
    emit_metric(
        chk("null_rates"),
        {
            **base,
            "expected_columns": len(expected_columns),
            "present_columns": present,
            "null_rate_by_column": rates,
        },
        mart=mart,
    )


def _gate_status_from_rate(rate: float, *, failed_below: float, warn_below: float) -> str:
    if rate < failed_below:
        return "failed"
    if rate < warn_below:
        return "warning"
    return "ok"


def _valid_rate(mask: pd.Series) -> float:
    if len(mask) == 0:
        return 1.0
    return float(mask.mean())


def _check_started_series(series: pd.Series | None) -> tuple[float, dict[str, Any]]:
    if series is None or len(series) == 0:
        return 1.0, {"rows": 0}
    ok = started_parseable_mask(series)
    rate = _valid_rate(ok)
    return rate, {"parseable_rate": round(rate, 6), "rows": int(len(series))}


def _check_owner_series(series: pd.Series | None) -> tuple[float, dict[str, Any]]:
    if series is None or len(series) == 0:
        return 1.0, {"rows": 0}
    owner = pd.to_numeric(series, errors="coerce")
    ok = owner.isin(list(_OWNER_VALID))
    rate = _valid_rate(ok)
    return rate, {"valid_owner_rate": round(rate, 6), "rows": int(len(series))}


def _check_lac_cell_series(lac: pd.Series | None, cell: pd.Series | None) -> tuple[float, dict[str, Any]]:
    if lac is None or cell is None or len(lac) == 0:
        return 1.0, {"rows": 0}
    l = pd.to_numeric(lac, errors="coerce")
    c = pd.to_numeric(cell, errors="coerce")
    ok = (
        l.notna()
        & c.notna()
        & (l >= 0)
        & (c >= 0)
        & (l < 10**5)
        & (c < 10**6)
    )
    rate = _valid_rate(ok)
    return rate, {"valid_lac_cell_rate": round(rate, 6), "rows": int(len(lac))}


def _check_imsi_digits(series: pd.Series | None, *, min_len: int = 10) -> tuple[float, dict[str, Any]]:
    if series is None or len(series) == 0:
        return 1.0, {"rows": 0}
    digits = series.astype("string").str.replace(r"\D+", "", regex=True)
    ok = digits.str.len() >= min_len
    rate = _valid_rate(ok & series.notna())
    return rate, {"valid_imsi_rate": round(rate, 6), "rows": int(len(series))}


def _check_msisdn_digits(series: pd.Series | None) -> tuple[float, dict[str, Any]]:
    if series is None or len(series) == 0:
        return 1.0, {"rows": 0}
    digits = series.astype("string").str.replace(r"\D+", "", regex=True)
    ok = digits.str.len().between(10, 15)
    rate = _valid_rate(ok & series.notna())
    return rate, {"valid_msisdn_rate": round(rate, 6), "rows": int(len(series))}


def _check_coords(lon: pd.Series, lat: pd.Series) -> tuple[float, dict[str, Any]]:
    lon_n = pd.to_numeric(lon, errors="coerce")
    lat_n = pd.to_numeric(lat, errors="coerce")
    ok = lon_n.between(-180, 180) & lat_n.between(-90, 90) & lon_n.notna() & lat_n.notna()
    rate = _valid_rate(ok)
    return rate, {
        "valid_coord_rate": round(rate, 6),
        "invalid_lon": int((lon_n.notna() & ~lon_n.between(-180, 180)).sum()),
        "invalid_lat": int((lat_n.notna() & ~lat_n.between(-90, 90)).sum()),
        "rows": int(len(lon)),
    }


def _emit_stg_field_checks(
    *,
    emit: Callable[[str, str, dict[str, Any]], None],
    check_prefix: str,
    df: pd.DataFrame,
    mart: str | None = None,
    base: dict[str, Any] | None = None,
) -> None:
    base = dict(base or {})
    if mart:
        base["mart"] = mart

    def gate(suffix: str, status: str, metrics: dict[str, Any]) -> None:
        emit(f"{check_prefix}.stg_contract.{suffix}", status, {**base, **metrics})

    if df.empty:
        gate("sample", "warning", {"reason": "empty_sample"})
        return

    rate, metrics = _check_started_series(df.get("Started"))
    gate("started", _gate_status_from_rate(rate, failed_below=0.99, warn_below=0.995), metrics)

    if "Owner" in df.columns:
        rate, metrics = _check_owner_series(df["Owner"])
        gate("owner", _gate_status_from_rate(rate, failed_below=0.99, warn_below=0.995), metrics)

    lac_col = "Lac" if "Lac" in df.columns else "BSStartLac" if "BSStartLac" in df.columns else None
    cell_col = "Cell" if "Cell" in df.columns else "BSStartCell" if "BSStartCell" in df.columns else None
    if lac_col and cell_col:
        rate, metrics = _check_lac_cell_series(df[lac_col], df[cell_col])
        gate("lac_cell", _gate_status_from_rate(rate, failed_below=0.99, warn_below=0.995), metrics)

    if "IMSI" in df.columns:
        rate, metrics = _check_imsi_digits(df["IMSI"])
        failed_b = _MOBILE_IMSI_DQ_FAILED_BELOW_BY_MART.get(mart or "", 0.98)
        warn_b = _MOBILE_IMSI_DQ_WARN_BELOW_BY_MART.get(mart or "", 0.99)
        gate("imsi", _gate_status_from_rate(rate, failed_below=failed_b, warn_below=warn_b), metrics)

    msisdn_col = next((c for c in ("CallingNumber", "Calling", "Served") if c in df.columns), None)
    if msisdn_col:
        rate, metrics = _check_msisdn_digits(df[msisdn_col])
        gate("msisdn", _gate_status_from_rate(rate, failed_below=0.98, warn_below=0.99), metrics)

    if {"Latitude", "Longitude"}.issubset(df.columns):
        rate, metrics = _check_coords(df["Longitude"], df["Latitude"])
        gate("coords", _gate_status_from_rate(rate, failed_below=0.99, warn_below=0.995), metrics)


def _emit_log(
    check: str,
    status: str,
    metrics: dict[str, Any],
    *,
    mart: str | None = None,
) -> None:
    payload: dict[str, Any] = {"tag": LOG_TAG, "check": check, "status": status, "metrics": metrics}
    if mart is not None:
        payload["mart"] = mart
    message = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    if status == "failed":
        logger.error(message)
    elif status == "warning":
        logger.warning(message)
    else:
        logger.info(message)


def _emit_summary(total_checks: int, warnings: int = 0, failed: int = 0) -> None:
    status = "failed" if failed else ("warning" if warnings else "info")
    payload = {
        "tag": LOG_TAG,
        "check": "summary",
        "status": status,
        "metrics": {
            "total_checks": total_checks,
            "warning_checks": warnings,
            "failed_checks": failed,
        },
    }
    log_fn = logger.error if failed else (logger.warning if warnings else logger.info)
    log_fn(json.dumps(payload, ensure_ascii=False, sort_keys=True))
