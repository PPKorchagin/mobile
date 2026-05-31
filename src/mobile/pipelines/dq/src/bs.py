from __future__ import annotations

import json
import logging
import re
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pandas as pd

from mobile.project_paths import resolve_project_path

logger = logging.getLogger(__name__)
LOG_TAG = "DQ_SRC_BS"

_NUMERIC_TYPES = frozenset({"int", "smallint", "long", "float"})
_DISTRIBUTION_TOP_N = 25
_UNIQUE_VALUES_MAX_CARDINALITY = 20
_DISCRETE_NUMERIC_DIST_MAX = 30
_SKIP_DISTRIBUTION_FIELDS = frozenset({"date_on", "date_off"})

_BS_STG_CRITICAL_COLUMNS = (
    "date_on",
    "date_off",
    "lac",
    "cell",
    "mcc",
    "mnc",
    "generation",
    "frequency",
    "coord_x",
    "coord_y",
    "bs_type",
    "location",
    "description",
    "azimuth",
    "thickness",
    "address",
    "subject",
)

RF_MCC = 250
GENERATION_VALUES = frozenset({"2G", "3G", "4G", "LTE", "5G"})
LOCATION_INDOOR = frozenset({"indoor", "underground", "small cell", "indoor/small cell"})

_RADIO_PROFILE_FIELDS = (
    "power",
    "height",
    "frequency",
    "tilt",
    "el_tilt",
    "mech_tilt",
    "amplification",
    "polarization",
    "raster",
    "thickness",
)

_RADIO_PRESENCE_FIELDS = (
    "bs_type",
    "location",
    "rad_class",
    "bcch",
    "controllernum",
    "frequency_out",
    "frequency_in",
)

_NUMERIC_RANGES: dict[str, tuple[float, float]] = {
    "azimuth": (-1.0, 998.0),
    "height": (-50.0, 240.0),
    "tilt": (-90.0, 360.0),
    "el_tilt": (-359.0, 46.0),
    "mech_tilt": (-240.0, 25.0),
    "raster": (-360.0, 360.0),
    "thickness": (-360.0, 360.0),
    "frequency": (-1.0, 1e10),
    "power": (-14.0, 2040.0),
    "amplification": (-66.0, 55.0),
    "polarization": (45.0, 360.0),
}

_INT_RANGES: dict[str, tuple[int, int]] = {
    "rac": (0, 255),
    "avtocod": (0, 95),
    "bsic": (0, 77),
    "bsid": (0, 4_294_207_763),
}

_FREQ_LIST_RE = re.compile(r"^\d+(?:\.\d+)?(?:,\d+(?:\.\d+)?)*$")
def _resolve_parquet_path(parquet_path: Path | str) -> Path:
    return resolve_project_path(parquet_path)


def run_dq(*, parquet_path: Path | str) -> dict[str, Any]:
    """DQ по всей витрине ``src_bs`` (без фильтра по дате)."""
    path = _resolve_parquet_path(parquet_path)

    if not path.exists():
        summary = {"status": "failed", "reason": "parquet_not_found", "parquet_path": str(path)}
        _emit_log("dataset_presence", "failed", summary)
        _emit_summary(total_checks=1, warnings=0, failed=1)
        return summary

    source = pd.read_parquet(path)
    source_row_count = int(len(source))
    if not {"date_on", "date_off"}.issubset(source.columns):
        summary = {
            "status": "failed",
            "reason": "missing_temporal_columns",
            "parquet_path": str(path),
        }
        _emit_log("report_scope", "failed", summary)
        _emit_summary(total_checks=1, warnings=0, failed=1)
        return summary

    return _run_dq_checks(
        source,
        parquet_path=path,
        source_row_count=source_row_count,
    )


def _run_dq_checks(
    data: pd.DataFrame,
    *,
    parquet_path: Path,
    source_row_count: int,
) -> dict[str, Any]:
    row_count = int(len(data))

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
        "report_scope",
        "warning" if row_count == 0 else "ok",
        {
            "source_row_count": source_row_count,
            "row_count_total": row_count,
            "parquet_path": str(parquet_path),
        },
    )

    emit(
        "dataset_basic",
        "warning" if row_count == 0 else "ok",
        {
            "row_count": row_count,
            "column_count": int(len(data.columns)),
            "parquet_path": str(parquet_path),
        },
    )

    if data.empty:
        status = "failed" if failed else ("warning" if warnings else "ok")
        _emit_summary(total_checks=checks, warnings=warnings, failed=failed)
        return {
            "status": status,
            "parquet_path": str(parquet_path),
            "row_count": 0,
            "total_checks": checks,
            "warning_checks": warnings,
            "failed_checks": failed,
        }

    profile_stats = _emit_field_profiles(emit, data)
    emit(
        "field_profile_coverage",
        "ok",
        {
            "total_columns": int(len(data.columns)),
            "profiled_fields": profile_stats["profiled_fields"],
            "distribution_checks": profile_stats["distribution_checks"],
            "numeric_profile_checks": profile_stats["numeric_profile_checks"],
            "unique_values_checks": profile_stats["unique_values_checks"],
        },
    )

    key_cols = ["mcc", "mnc", "lac", "cell", "date_on"]
    if all(col in data.columns for col in key_cols):
        key_frame = data[key_cols].copy()
        duplicate_keys = int(key_frame.duplicated(keep=False).sum())
        emit(
            "key_integrity",
            "warning" if duplicate_keys > 0 else "ok",
            {
                "duplicate_key_rows": duplicate_keys,
                "key_columns": key_cols,
                "distinct_keys": int(key_frame.drop_duplicates().shape[0]),
            },
        )

    if {"date_on", "date_off"}.issubset(data.columns):
        date_on = pd.to_datetime(data["date_on"], errors="coerce")
        date_off = pd.to_datetime(data["date_off"], errors="coerce")
        invalid_order = int(((date_off < date_on) & date_on.notna() & date_off.notna()).sum())
        active_days = (date_off - date_on).dt.total_seconds() / 86400.0
        active_days_valid = active_days[active_days.notna()]
        emit(
            "temporal_consistency",
            "warning" if invalid_order > 0 else "ok",
            {
                "invalid_date_order_count": invalid_order,
                "date_on_min": str(date_on.min()) if date_on.notna().any() else None,
                "date_on_max": str(date_on.max()) if date_on.notna().any() else None,
                "date_off_min": str(date_off.min()) if date_off.notna().any() else None,
                "date_off_max": str(date_off.max()) if date_off.notna().any() else None,
                "active_days_p50": float(active_days_valid.quantile(0.5)) if len(active_days_valid) else None,
                "active_days_p95": float(active_days_valid.quantile(0.95)) if len(active_days_valid) else None,
            },
        )
        emit(
            "temporal_date_off_tail",
            "ok",
            _date_off_tail_metrics(date_off, row_count=len(data)),
        )
        for ts_col in ("date_on", "date_off"):
            parsed = pd.to_datetime(data[ts_col], errors="coerce")
            buckets = parsed.dt.to_period("M").astype("string")
            dist = _distribution_bundle(buckets, row_count=len(data), top_n=24)
            emit(
                f"distribution.{ts_col}_month",
                "ok",
                {**dist, "field_type": "timestamp"},
            )

    if {"coord_x", "coord_y"}.issubset(data.columns):
        lon = pd.to_numeric(data["coord_x"], errors="coerce")
        lat = pd.to_numeric(data["coord_y"], errors="coerce")
        invalid_lon = int((~lon.between(-180, 180) & lon.notna()).sum())
        invalid_lat = int((~lat.between(-90, 90) & lat.notna()).sum())
        emit(
            "spatial_ranges",
            "warning" if invalid_lon > 0 or invalid_lat > 0 else "ok",
            {
                "invalid_lon_count": invalid_lon,
                "invalid_lat_count": invalid_lat,
                "lon_min": float(lon.min()) if lon.notna().any() else None,
                "lon_max": float(lon.max()) if lon.notna().any() else None,
                "lat_min": float(lat.min()) if lat.notna().any() else None,
                "lat_max": float(lat.max()) if lat.notna().any() else None,
            },
        )

    _emit_bs_stg_contract_checks(emit, data)
    _emit_domain_contract_checks(emit, data)

    status = "failed" if failed else ("warning" if warnings else "ok")
    _emit_summary(total_checks=checks, warnings=warnings, failed=failed)
    return {
        "status": status,
        "parquet_path": str(parquet_path),
        "row_count": row_count,
        "source_row_count": source_row_count,
        "total_checks": checks,
        "warning_checks": warnings,
        "failed_checks": failed,
    }


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
        ftype = _infer_field_type(data[name], name=name)
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
            values = _unique_values_table(series, row_count=row_count)
            emit(
                f"unique_values.{name}",
                "ok",
                {
                    "nunique": nunique,
                    "values": values,
                    "field_type": ftype,
                },
            )
            stats["unique_values_checks"] += 1

        if ftype == "boolean":
            dist = _boolean_distribution(series, row_count=row_count)
            emit(f"distribution.{name}", "ok", {**dist, "field_type": ftype})
            stats["distribution_checks"] += 1
            continue

        if ftype == "timestamp":
            continue

        if ftype in _NUMERIC_TYPES:
            profile = _numeric_profile(series)
            emit(f"numeric_profile.{name}", "ok", {**profile, "field_type": ftype})
            stats["numeric_profile_checks"] += 1
            if nunique <= _DISCRETE_NUMERIC_DIST_MAX:
                dist = _distribution_bundle(
                    _numeric_distribution_labels(series),
                    row_count=row_count,
                    top_n=_DISTRIBUTION_TOP_N,
                )
                emit(f"distribution.{name}", "ok", {**dist, "field_type": ftype, "discrete_numeric": True})
                stats["distribution_checks"] += 1
            continue

        if ftype == "string" and name not in _SKIP_DISTRIBUTION_FIELDS:
            dist = _distribution_bundle(series, row_count=row_count, top_n=_DISTRIBUTION_TOP_N)
            emit(f"distribution.{name}", "ok", {**dist, "field_type": ftype})
            stats["distribution_checks"] += 1

    return stats


def _infer_field_type(series: pd.Series, *, name: str) -> str:
    if name in {"date_on", "date_off"}:
        return "timestamp"
    if pd.api.types.is_bool_dtype(series):
        return "boolean"
    if pd.api.types.is_numeric_dtype(series):
        return "float" if pd.api.types.is_float_dtype(series) else "int"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "timestamp"
    return "string"


def _distribution_bundle(series: pd.Series, *, row_count: int, top_n: int) -> dict[str, Any]:
    normalized = series.astype("string").fillna("<NA>")
    vc = normalized.value_counts()
    total_distinct = int(vc.shape[0])
    top = vc.head(top_n)
    counts = {str(k): int(v) for k, v in top.items()}
    pct = {str(k): round(int(v) / max(row_count, 1) * 100, 4) for k, v in top.items()}
    return {
        "distinct_values": total_distinct,
        "top_n": top_n,
        "distribution_counts": counts,
        "distribution_pct": pct,
    }


def _unique_values_table(series: pd.Series, *, row_count: int) -> list[dict[str, Any]]:
    normalized = series.astype("string").fillna("<NA>")
    rows: list[dict[str, Any]] = []
    for value, count in normalized.value_counts().items():
        rows.append(
            {
                "value": str(value),
                "count": int(count),
                "pct": round(int(count) / max(row_count, 1) * 100, 4),
            }
        )
    return rows


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
    counts = {str(k): int(v) for k, v in vc.items()}
    pct = {str(k): round(int(v) / max(row_count, 1) * 100, 4) for k, v in vc.items()}
    return {
        "distinct_values": int(vc.shape[0]),
        "distribution_counts": counts,
        "distribution_pct": pct,
    }


def _numeric_distribution_labels(series: pd.Series) -> pd.Series:
    num = pd.to_numeric(series, errors="coerce")

    def _label(v: Any) -> str:
        if pd.isna(v):
            return "<NA>"
        fv = float(v)
        if fv.is_integer():
            return str(int(fv))
        return str(fv)

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


def _valid_rate(mask: pd.Series) -> float:
    if len(mask) == 0:
        return 1.0
    return float(mask.mean())


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


def _emit_bs_stg_contract_checks(
    emit: Callable[[str, str, dict[str, Any]], None],
    data: pd.DataFrame,
) -> None:
    missing = [c for c in _BS_STG_CRITICAL_COLUMNS if c not in data.columns]
    emit(
        "stg_contract.columns",
        "failed" if missing else "ok",
        {"required_fields": list(_BS_STG_CRITICAL_COLUMNS), "missing_fields": missing},
    )
    if missing or data.empty:
        return

    rate, metrics = _check_lac_cell_series(data.get("lac"), data.get("cell"))
    emit(
        "stg_contract.lac_cell",
        _status_from_rate(rate, failed_below=0.995, warn_below=0.999),
        metrics,
    )

    rate, metrics = _check_coords(data.get("coord_x"), data.get("coord_y"))
    emit(
        "stg_contract.coords",
        _status_from_rate(rate, failed_below=0.995, warn_below=0.999),
        metrics,
    )

    date_on = pd.to_datetime(data["date_on"], errors="coerce")
    date_off = pd.to_datetime(data["date_off"], errors="coerce")
    order_ok = float(((date_off >= date_on) | date_off.isna() | date_on.isna()).mean())
    emit(
        "stg_contract.temporal_order",
        _status_from_rate(order_ok, failed_below=0.99, warn_below=0.995),
        {"valid_order_rate": round(order_ok, 6)},
    )

    gen = data["generation"].astype("string").str.strip()
    gen_ok = float((gen.notna() & gen.ne("")).mean())
    emit(
        "stg_contract.generation_present",
        _status_from_rate(gen_ok, failed_below=0.99, warn_below=0.995),
        {"present_rate": round(gen_ok, 6)},
    )


def _emit_domain_contract_checks(
    emit: Callable[[str, str, dict[str, Any]], None],
    data: pd.DataFrame,
) -> None:
    if data.empty:
        return
    _emit_identity_checks(emit, data)
    _emit_temporal_contract_checks(emit, data)
    _emit_spatial_contract_checks(emit, data)
    _emit_radio_contract_checks(emit, data)
    _emit_radio_profile_checks(emit, data)
    _emit_frequency_list_checks(emit, data)


def _emit_identity_checks(
    emit: Callable[[str, str, dict[str, Any]], None],
    data: pd.DataFrame,
) -> None:
    if "mcc" in data.columns:
        mcc = pd.to_numeric(data["mcc"], errors="coerce")
        rf_rate = float((mcc == RF_MCC).mean())
        emit(
            "contract.mcc_rf",
            _status_from_rate(rf_rate, failed_below=0.99, warn_below=0.999),
            {
                "mcc_250_rate": round(rf_rate, 6),
                "mcc_top": _value_counts_top(mcc, top_n=5),
            },
        )

    if "mnc" in data.columns:
        mnc = pd.to_numeric(data["mnc"], errors="coerce")
        non_null = mnc.notna()
        negative = int((mnc < 0).sum())
        emit(
            "contract.mnc_valid",
            "warning" if negative > 0 else "ok",
            {
                "negative_count": negative,
                "null_count": int((~non_null).sum()),
                "distinct_mnc": int(mnc.nunique(dropna=True)),
            },
        )

    if {"lac", "cell"}.issubset(data.columns):
        lac = pd.to_numeric(data["lac"], errors="coerce")
        cell = pd.to_numeric(data["cell"], errors="coerce")
        ok = lac.notna() & cell.notna() & (lac >= 0) & (cell >= 0)
        rate = float(ok.mean())
        emit(
            "contract.lac_cell_non_negative",
            _status_from_rate(rate, failed_below=0.995, warn_below=0.999),
            {
                "valid_rate": round(rate, 6),
                "lac_null_count": int(lac.isna().sum()),
                "cell_null_count": int(cell.isna().sum()),
            },
        )

    if {"mcc", "mnc", "lac", "cell"}.issubset(data.columns):
        cgi = (
            data["mcc"].astype("string")
            + "-"
            + data["mnc"].astype("string")
            + "-"
            + data["lac"].astype("string")
            + "-"
            + data["cell"].astype("string")
        )
        dup_cgi = int(cgi.duplicated(keep=False).sum())
        emit(
            "contract.cgi_duplicate_rows",
            "warning" if dup_cgi > 0 else "ok",
            {
                "duplicate_cgi_rows": dup_cgi,
                "distinct_cgi": int(cgi.nunique(dropna=True)),
            },
        )


def _emit_temporal_contract_checks(
    emit: Callable[[str, str, dict[str, Any]], None],
    data: pd.DataFrame,
) -> None:
    if not {"date_on", "date_off"}.issubset(data.columns):
        return

    date_off = pd.to_datetime(data["date_off"], errors="coerce")
    row_n = max(len(data), 1)

    date_off_null = int(date_off.isna().sum())
    emit(
        "contract.date_off_present",
        "failed" if date_off_null > 0 else "ok",
        {
            "null_date_off_count": date_off_null,
            "null_date_off_ratio": round(date_off_null / row_n, 4),
        },
    )


def _date_off_tail_metrics(date_off: pd.Series, *, row_count: int) -> dict[str, Any]:
    valid = date_off.dropna()
    row_n = max(row_count, 1)
    if valid.empty:
        return {
            "date_off_max": None,
            "date_off_p95": None,
            "rows_at_max": 0,
            "rows_at_max_ratio": 0.0,
            "rows_below_max": 0,
            "rows_below_max_ratio": 0.0,
        }
    max_val = valid.max()
    at_max = int((date_off == max_val).sum())
    below_max = row_count - at_max - int(date_off.isna().sum())
    return {
        "date_off_max": str(max_val),
        "date_off_p95": str(valid.quantile(0.95)),
        "rows_at_max": at_max,
        "rows_at_max_ratio": round(at_max / row_n, 4),
        "rows_below_max": below_max,
        "rows_below_max_ratio": round(below_max / row_n, 4),
    }


def _emit_spatial_contract_checks(
    emit: Callable[[str, str, dict[str, Any]], None],
    data: pd.DataFrame,
) -> None:
    if not {"coord_x", "coord_y"}.issubset(data.columns):
        return

    lon = pd.to_numeric(data["coord_x"], errors="coerce")
    lat = pd.to_numeric(data["coord_y"], errors="coerce")
    null_coords = int((lon.isna() | lat.isna()).sum())
    zero_coords = int(((lon == 0) & (lat == 0)).sum())
    row_n = max(len(data), 1)

    emit(
        "contract.coords_present",
        _status_from_rate(1.0 - null_coords / row_n, failed_below=0.995, warn_below=0.999),
        {
            "null_coord_rows": null_coords,
            "zero_zero_rows": zero_coords,
        },
    )

    site_keys = data[["coord_x", "coord_y"]].astype("string").agg(",".join, axis=1)
    cells_per_site = site_keys.value_counts()
    emit(
        "contract.cells_per_coordinate",
        "ok",
        {
            "distinct_sites": int(cells_per_site.shape[0]),
            "cells_per_site_p50": _safe_float(cells_per_site.quantile(0.5)),
            "cells_per_site_p95": _safe_float(cells_per_site.quantile(0.95)),
            "cells_per_site_max": int(cells_per_site.max()),
        },
    )


def _emit_radio_contract_checks(
    emit: Callable[[str, str, dict[str, Any]], None],
    data: pd.DataFrame,
) -> None:
    if "generation" in data.columns:
        gen = data["generation"].astype("string").str.strip()
        known = gen.isin(GENERATION_VALUES)
        rate = float(known.mean())
        unknown = gen[~known & gen.notna() & (gen != "")]
        emit(
            "contract.generation_vocab",
            _status_from_rate(rate, failed_below=0.95, warn_below=0.99),
            {
                "known_generation_rate": round(rate, 6),
                "unknown_count": int(unknown.shape[0]),
                "unknown_ratio": round(int(unknown.shape[0]) / max(len(data), 1), 4),
                "distinct_unknown": int(unknown.nunique()) if len(unknown) else 0,
            },
        )

    if {"azimuth", "location"}.issubset(data.columns):
        az = pd.to_numeric(data["azimuth"], errors="coerce")
        loc = data["location"].astype("string").str.strip().str.lower()
        indoor_loc = loc.isin(LOCATION_INDOOR)
        omnidirectional = az == -1
        directional_ok = az.between(0, 360, inclusive="both")
        az_valid = omnidirectional | directional_ok | az.between(361, 998, inclusive="both")
        az_rate = float(az_valid.mean()) if az.notna().any() else 1.0
        indoor_with_dir = int((indoor_loc & ~omnidirectional & az.notna()).sum())
        emit(
            "contract.azimuth_semantics",
            _status_from_rate(az_rate, failed_below=0.98, warn_below=0.995),
            {
                "valid_azimuth_rate": round(az_rate, 6),
                "omnidirectional_rows": int(omnidirectional.sum()),
                "omnidirectional_ratio": round(float(omnidirectional.mean()), 4),
                "indoor_not_omni_count": indoor_with_dir,
            },
        )

    for field, (lo, hi) in _NUMERIC_RANGES.items():
        if field not in data.columns:
            continue
        num = pd.to_numeric(data[field], errors="coerce")
        if not num.notna().any():
            continue
        in_range = num.between(lo, hi) | num.isna()
        rate = float(in_range.mean())
        emit(
            f"contract.range.{field}",
            _status_from_rate(rate, failed_below=0.98, warn_below=0.995),
            {
                "min_allowed": lo,
                "max_allowed": hi,
                "in_range_rate": round(rate, 6),
                "below_min": int((num < lo).sum()),
                "above_max": int((num > hi).sum()),
            },
        )

    for field, (lo, hi) in _INT_RANGES.items():
        if field not in data.columns:
            continue
        num = pd.to_numeric(data[field], errors="coerce")
        if not num.notna().any():
            continue
        in_range = num.between(lo, hi) | num.isna()
        rate = float(in_range.mean())
        emit(
            f"contract.range.{field}",
            _status_from_rate(rate, failed_below=0.98, warn_below=0.995),
            {
                "min_allowed": lo,
                "max_allowed": hi,
                "in_range_rate": round(rate, 6),
            },
        )

    if "border" in data.columns:
        normalized = _normalize_boolean_series(data["border"])
        other = int((normalized == "<other>").sum())
        emit(
            "contract.border_boolean",
            "warning" if other > 0 else "ok",
            {
                "non_boolean_count": other,
                "distribution_counts": _value_counts_dict(normalized, top_n=5),
            },
        )


def _numeric_radio_profile(series: pd.Series, *, row_count: int) -> dict[str, Any]:
    num = pd.to_numeric(series, errors="coerce")
    null_count = int(num.isna().sum())
    valid = num.dropna()
    out: dict[str, Any] = {
        "null_count": null_count,
        "null_ratio": round(null_count / max(row_count, 1), 4),
        "non_null_count": int(valid.count()),
    }
    if valid.empty:
        return out
    return {
        **out,
        "min": _safe_float(valid.min()),
        "p50": _safe_float(valid.quantile(0.5)),
        "p95": _safe_float(valid.quantile(0.95)),
        "max": _safe_float(valid.max()),
        "mean": _safe_float(valid.mean()),
    }


def _categorical_presence_metrics(series: pd.Series, *, row_count: int) -> dict[str, Any]:
    text = series.astype("string").str.strip()
    empty = text.isna() | (text == "")
    empty_count = int(empty.sum())
    present = text[~empty]
    return {
        "empty_count": empty_count,
        "empty_ratio": round(empty_count / max(row_count, 1), 4),
        "present_count": int(present.shape[0]),
        "distinct_values": int(present.nunique()) if len(present) else 0,
    }


def _emit_radio_profile_checks(
    emit: Callable[[str, str, dict[str, Any]], None],
    data: pd.DataFrame,
) -> None:
    if data.empty:
        return

    row_count = len(data)

    for field in _RADIO_PROFILE_FIELDS:
        if field not in data.columns:
            continue
        metrics = _numeric_radio_profile(data[field], row_count=row_count)
        if field == "frequency":
            freq = pd.to_numeric(data[field], errors="coerce")
            metrics["sentinel_minus_one_count"] = int((freq == -1).sum())
            metrics["positive_count"] = int((freq > 0).sum())
        status = "warning" if metrics.get("non_null_count", 0) == 0 else "ok"
        emit(f"radio.profile.{field}", status, metrics)

    if "azimuth" in data.columns:
        az = pd.to_numeric(data["azimuth"], errors="coerce")
        omnidirectional = int((az == -1).sum())
        directional = int(az.between(0, 360, inclusive="both").sum())
        emit(
            "radio.profile.azimuth",
            "ok",
            {
                **_numeric_radio_profile(data["azimuth"], row_count=row_count),
                "omnidirectional_rows": omnidirectional,
                "omnidirectional_ratio": round(omnidirectional / max(row_count, 1), 4),
                "directional_rows": directional,
                "directional_ratio": round(directional / max(row_count, 1), 4),
            },
        )

    for field in _RADIO_PRESENCE_FIELDS:
        if field not in data.columns:
            continue
        metrics = _categorical_presence_metrics(data[field], row_count=row_count)
        emit(
            f"radio.presence.{field}",
            "ok",
            metrics,
        )

    if {"power", "height"}.issubset(data.columns):
        power = pd.to_numeric(data["power"], errors="coerce")
        height = pd.to_numeric(data["height"], errors="coerce")
        both = power.notna() & height.notna()
        emit(
            "radio.profile.power_height",
            "ok",
            {
                "both_present_rows": int(both.sum()),
                "both_present_ratio": round(float(both.mean()), 4),
                "power_p50": _safe_float(power.quantile(0.5)) if power.notna().any() else None,
                "height_p50": _safe_float(height.quantile(0.5)) if height.notna().any() else None,
            },
        )

    if {"generation", "bs_type"}.issubset(data.columns):
        gen = data["generation"].astype("string").str.strip()
        bs_type = data["bs_type"].astype("string").str.strip()
        both = gen.notna() & gen.ne("") & bs_type.notna() & bs_type.ne("")
        emit(
            "radio.profile.generation_bs_type",
            "ok",
            {
                "both_present_rows": int(both.sum()),
                "both_present_ratio": round(float(both.mean()), 4),
                "distinct_generation": int(gen[both].nunique()) if both.any() else 0,
                "distinct_bs_type": int(bs_type[both].nunique()) if both.any() else 0,
            },
        )


def _emit_frequency_list_checks(
    emit: Callable[[str, str, dict[str, Any]], None],
    data: pd.DataFrame,
) -> None:
    for col in ("frequency_out", "frequency_in"):
        if col not in data.columns:
            continue
        raw = data[col].astype("string").str.strip()
        empty = raw.isna() | (raw == "")
        not_empty = ~empty
        valid = raw.str.fullmatch(_FREQ_LIST_RE.pattern, na=False)
        malformed = int((not_empty & ~valid).sum())
        band_counts = raw[not_empty & valid].str.count(",") + 1
        emit(
            f"contract.frequency_list.{col}",
            "warning" if malformed > 0 else "ok",
            {
                "empty_count": int(empty.sum()),
                "malformed_count": malformed,
                "valid_rows": int((not_empty & valid).sum()),
                "bands_per_row_p50": _safe_float(band_counts.quantile(0.5)) if len(band_counts) else None,
                "bands_per_row_p95": _safe_float(band_counts.quantile(0.95)) if len(band_counts) else None,
            },
        )


def _value_counts_top(series: pd.Series, *, top_n: int) -> dict[str, int]:
    vc = series.value_counts(dropna=False).head(top_n)
    return {str(k): int(v) for k, v in vc.items()}


def _string_value_counts_top(series: pd.Series, *, top_n: int) -> dict[str, int]:
    if series.empty:
        return {}
    vc = series.value_counts().head(top_n)
    return {str(k): int(v) for k, v in vc.items()}


def _value_counts_dict(series: pd.Series, *, top_n: int) -> dict[str, int]:
    vc = series.value_counts().head(top_n)
    return {str(k): int(v) for k, v in vc.items()}


def _normalize_boolean_series(series: pd.Series) -> pd.Series:
    return series.map(
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


def _safe_float(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    return round(float(value), 6)


def _emit_log(check: str, status: str, metrics: dict[str, Any]) -> None:
    payload = {"tag": LOG_TAG, "check": check, "status": status, "metrics": metrics}
    message = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    if status == "failed":
        logger.error(message)
    elif status == "warning":
        logger.warning(message)
    else:
        logger.info(message)


def _emit_summary(
    *,
    total_checks: int,
    warnings: int,
    failed: int,
) -> None:
    payload = {
        "tag": LOG_TAG,
        "check": "summary",
        "status": "failed" if failed else ("warning" if warnings else "ok"),
        "metrics": {
            "total_checks": total_checks,
            "warning_checks": warnings,
            "failed_checks": failed,
        },
    }
    logger.info(json.dumps(payload, ensure_ascii=False, sort_keys=True))
