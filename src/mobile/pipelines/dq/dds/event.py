"""DQ DDS-слоя ``event_dds`` (``{source_id}.parquet``) за отчётную дату."""

from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path
from typing import Any, Callable

import pandas as pd
import pyarrow.parquet as pq

from mobile.pipelines.dds.event import EVENT_CODES, DDS_EVENT_FIELDS
from mobile.project_paths import (
    resolve_project_path,
    started_parseable_mask,
    dds_event_dds_day_key_from_path,
    dds_event_dds_source_id_from_path,
)

logger = logging.getLogger(__name__)
LOG_TAG = "DQ_DDS_EVENT"

STG_EVENT_CRITICAL_COLUMNS: tuple[str, ...] = tuple(f["name"] for f in DDS_EVENT_FIELDS)

_EVENT_TYPES = tuple(EVENT_CODES.values())
_EVENT_NAMES_VALID = frozenset(EVENT_CODES.keys())
_EVENT_CODE_TO_NAME = {int(v): k for k, v in EVENT_CODES.items()}

_DISTRIBUTION_TOP_N = 12
# Без PII: не логируем value_counts по imsi / imei / msisdn (только длины и TAC).
_DISTRIBUTION_SCALAR_COLUMNS: tuple[str, ...] = (
    "event",
    "event_name",
    "event_count",
)


def run_dq(report_date: date, event_dds_root: str | Path) -> dict[str, Any]:
    """DQ ``event_dds`` за отчётную дату и корень каталога layout.

    ``event_dds_root`` — каталог ``data/dds/event_dds/`` (или аналог). Обход всех
    ``*.parquet`` за ``report_date`` (сегмент ``YYYY-MM-DD`` в пути); строки фильтруются
    по ``event_timestamp[:8]`` (локальные сутки).
    """
    root = resolve_project_path(event_dds_root)
    if not root.is_dir():
        raise ValueError(f"event_dds_root must be a directory: {root}")
    paths = _discover_event_dds_parquet_paths(root, report_date)
    report_day = report_date.isoformat()

    checks = 0
    warnings = 0
    failed = 0

    def emit_metric(check: str, metrics: dict[str, Any]) -> None:
        nonlocal checks
        checks += 1
        _emit_log(check, "info", metrics)

    def emit_gate(check: str, status: str, metrics: dict[str, Any]) -> None:
        nonlocal checks, warnings, failed
        checks += 1
        if status == "warning":
            warnings += 1
        elif status == "failed":
            failed += 1
        _emit_log(check, status, metrics)

    if not paths:
        emit_gate(
            "dataset_presence",
            "failed",
            {
                "reason": "no_parquet_for_report_date",
                "report_date": report_day,
                "event_dds_root": str(root),
            },
        )
        _emit_summary(total_checks=checks, warnings=warnings, failed=failed)
        return {
            "status": "failed",
            "total_checks": checks,
            "warning_checks": warnings,
            "failed_checks": failed,
            "report_date": report_day,
            "event_dds_root": str(root),
            "parquet_files": 0,
        }

    row_count_total = int(sum(_parquet_row_count(p) for p in paths))
    emit_metric(
        "coverage",
        {
            "report_date": report_day,
            "event_dds_root": str(root),
            "parquet_files": len(paths),
            "row_count_total": row_count_total,
            "paths": [str(p) for p in paths],
        },
    )

    by_source = _group_paths_by_source_id(paths)
    source_frames: dict[str, pd.DataFrame] = {}

    for source_id, spaths in sorted(by_source.items()):
        srows = int(sum(_parquet_row_count(p) for p in spaths))
        emit_metric(
            "source.coverage",
            {
                "report_date": report_day,
                "source_id": source_id,
                "parquet_files": len(spaths),
                "row_count_total": srows,
            },
        )
        df_s = _read_and_filter_parquets(spaths, report_date)
        if not df_s.empty:
            source_frames[source_id] = df_s
        _emit_deep_metrics(
            df_s,
            spaths,
            emit_metric,
            emit_gate,
            report_date=report_day,
            source_id=source_id,
        )

    sample = (
        pd.concat(list(source_frames.values()), ignore_index=True)
        if source_frames
        else pd.DataFrame()
    )
    _emit_deep_metrics(
        sample,
        paths,
        emit_metric,
        emit_gate,
        report_date=report_day,
        source_id=None,
    )

    missing = [c for c in STG_EVENT_CRITICAL_COLUMNS if sample.empty or c not in sample.columns]
    emit_gate(
        "stg_contract.columns",
        "failed" if missing else "ok",
        {"required_fields": list(STG_EVENT_CRITICAL_COLUMNS), "missing_fields": missing},
    )

    _emit_summary(total_checks=checks, warnings=warnings, failed=failed)
    return {
        "status": "failed" if failed else ("warning" if warnings else "ok"),
        "total_checks": checks,
        "warning_checks": warnings,
        "failed_checks": failed,
        "report_date": report_day,
        "event_dds_root": str(root),
        "parquet_files": len(paths),
        "row_count_total": int(len(sample)),
        "source_ids": sorted(source_frames.keys()),
    }


def _discover_event_dds_parquet_paths(path: Path, report_date: date) -> list[Path]:
    day_key = report_date.isoformat()
    if path.is_file():
        if path.suffix.lower() != ".parquet":
            return []
        key = dds_event_dds_day_key_from_path(path)
        if key is not None and key != day_key:
            return []
        return [path]
    if path.is_dir():
        day_dir = path / day_key
        if day_dir.is_dir():
            return sorted(day_dir.glob("*.parquet"))
        out: list[Path] = []
        for p in sorted(path.rglob("*.parquet")):
            key = dds_event_dds_day_key_from_path(p)
            if key is None or key == day_key:
                out.append(p)
        return out
    return []


def _source_id_from_path(path: Path) -> str | None:
    return dds_event_dds_source_id_from_path(path)


def _group_paths_by_source_id(paths: list[Path]) -> dict[str, list[Path]]:
    buckets: dict[str, list[Path]] = {}
    for p in paths:
        source_id = _source_id_from_path(p) or "_unknown"
        buckets.setdefault(source_id, []).append(p)
    return buckets


def _parquet_row_count(path: Path) -> int:
    try:
        return int(pq.ParquetFile(path).metadata.num_rows)
    except Exception:
        return 0


def _read_and_filter_parquets(paths: list[Path], report_date: date) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []
    for p in paths:
        try:
            parts.append(pd.read_parquet(p))
        except Exception:
            continue
    if not parts:
        return pd.DataFrame()
    merged = pd.concat(parts, ignore_index=True)
    return _filter_df_by_local_report_date(merged, report_date)


def _filter_df_by_local_report_date(df: pd.DataFrame, report_date: date) -> pd.DataFrame:
    if df.empty or "event_timestamp" not in df.columns:
        return df
    day_str = report_date.strftime("%Y%m%d")
    s = df["event_timestamp"].astype("string").str.strip()
    mask = started_parseable_mask(s) & (s.str[:8] == day_str)
    if not bool(mask.any()):
        return df.iloc[0:0].copy()
    return df.loc[mask].copy()


def _emit_deep_metrics(
    df: pd.DataFrame,
    paths_for_segment: list[Path],
    emit_metric: Callable[[str, dict[str, Any]], None],
    emit_gate: Callable[[str, str, dict[str, Any]], None],
    *,
    report_date: str,
    source_id: str | None,
) -> None:
    is_source = source_id is not None

    def chk(suffix: str) -> str:
        if is_source:
            return f"source.{source_id}.{suffix}"
        return suffix

    base: dict[str, Any] = {"report_date": report_date}
    if is_source:
        base["source_id"] = source_id

    if df.empty:
        emit_metric(
            chk("sample_read"),
            {**base, "reason": "empty_sample", "parquet_files": len(paths_for_segment)},
        )
        return

    emit_metric(
        chk("sample_basic"),
        {
            **base,
            "row_count": int(len(df)),
            "parquet_files": len(paths_for_segment),
        },
    )

    if "event" in df.columns:
        vc = df["event"].value_counts(dropna=False).to_dict()
        emit_metric(chk("event_distribution"), {**base, "counts": {str(k): int(v) for k, v in vc.items()}})

    if "event_name" in df.columns:
        vc = df["event_name"].astype("string").str.lower().value_counts(dropna=False).to_dict()
        emit_metric(chk("event_name_distribution"), {**base, "counts": {str(k): int(v) for k, v in vc.items()}})

    _emit_dds_event_distributions(df, emit_metric, chk, base)

    if "event_timestamp" in df.columns:
        ok = started_parseable_mask(df["event_timestamp"])
        rate = float(ok.mean()) if len(ok) else 1.0
        emit_gate(
            chk("event_timestamp_parseable"),
            _gate_status_from_rate(rate, failed_below=0.99, warn_below=0.995),
            {**base, "parseable_rate": round(rate, 6), "rows": int(len(df))},
        )

    if "event_count" in df.columns:
        ec = pd.to_numeric(df["event_count"], errors="coerce")
        invalid = int((ec.isna() | (ec < 1)).sum())
        aggregated = int((ec > 1).sum())
        emit_gate(
            chk("event_count_valid"),
            "failed" if invalid > 0 else "ok",
            {
                **base,
                "invalid_event_count_rows": invalid,
                "aggregated_rows": aggregated,
                "aggregated_share": round(aggregated / max(1, len(df)), 6),
            },
        )

    if {"imsi", "event_timestamp", "event"}.issubset(df.columns):
        dup = int(df.duplicated(subset=["imsi", "event_timestamp", "event"], keep=False).sum())
        emit_metric(
            chk("imsi_event_timestamp_event_duplicates"),
            {**base, "duplicate_rows": dup, "rows": int(len(df))},
        )

    _emit_dds_event_field_checks(emit_gate, chk("event"), df, base={**base, "rows": int(len(df))})


def _emit_dds_event_distributions(
    df: pd.DataFrame,
    emit_metric: Callable[[str, dict[str, Any]], None],
    chk: Callable[[str], str],
    base: dict[str, Any],
) -> None:
    row_count = int(len(df))
    if row_count == 0:
        return

    for col in _DISTRIBUTION_SCALAR_COLUMNS:
        if col not in df.columns:
            continue
        metrics = _distribution_metrics(df[col])
        emit_metric(chk(f"distribution.{col}"), {**base, "column": col, **metrics})

    if "event_count" in df.columns:
        ec = pd.to_numeric(df["event_count"], errors="coerce")
        buckets = pd.cut(
            ec,
            bins=[0, 1, 5, 20, float("inf")],
            labels=["1", "2-5", "6-20", "21+"],
            right=True,
        ).astype("string")
        emit_metric(
            chk("distribution.event_count_bucket"),
            {
                **base,
                "column": "event_count",
                **_distribution_bundle(buckets, row_count=row_count, top_n=8),
            },
        )

    if "event_timestamp" in df.columns:
        s = df["event_timestamp"].astype("string").str.strip()
        ok = started_parseable_mask(s)
        if bool(ok.any()):
            hours = s.loc[ok].str[8:10]
            emit_metric(
                chk("distribution.event_timestamp_hour"),
                {
                    **base,
                    "column": "event_timestamp",
                    **_distribution_bundle(hours, row_count=row_count, top_n=24),
                },
            )

    for col in ("imsi", "imei", "msisdn"):
        if col not in df.columns:
            continue
        lengths = df[col].astype("string").str.strip().str.len()
        emit_metric(
            chk(f"distribution.{col}_length"),
            {
                **base,
                "column": col,
                **_distribution_bundle(lengths.astype("string"), row_count=row_count, top_n=_DISTRIBUTION_TOP_N),
            },
        )

    if "imei" in df.columns:
        imei_digits = df["imei"].astype("string").str.replace(r"\D+", "", regex=True)
        tac = imei_digits.where(imei_digits.str.len() >= 8, pd.NA).str[:8]
        emit_metric(
            chk("distribution.imei_tac"),
            {**base, "column": "imei", **_distribution_bundle(tac, row_count=row_count, top_n=_DISTRIBUTION_TOP_N)},
        )

    if "location" in df.columns:
        mcc, mnc, lac, cell = _location_columns(df["location"])
        for name, series in (
            ("location_mcc", mcc),
            ("location_mnc", mnc),
            ("location_lac", lac),
            ("location_cell", cell),
        ):
            emit_metric(
                chk(f"distribution.{name}"),
                {
                    **base,
                    "column": "location",
                    **_distribution_bundle(series, row_count=row_count, top_n=_DISTRIBUTION_TOP_N),
                },
            )
        compressible = (
            df["location"].notna()
            & mcc.astype("string").str.len().ge(1)
            & mnc.astype("string").str.len().ge(1)
            & lac.notna()
            & cell.notna()
        )
        emit_metric(
            chk("distribution.location_compressible"),
            {
                **base,
                "column": "location",
                **_distribution_bundle(
                    compressible.map({True: "true", False: "false"}).astype("string"),
                    row_count=row_count,
                    top_n=4,
                ),
            },
        )

    rates: dict[str, float] = {}
    for col in STG_EVENT_CRITICAL_COLUMNS:
        if col not in df.columns:
            rates[col] = 1.0
        else:
            rates[col] = round(float(df[col].isna().mean()), 6)
    emit_metric(
        chk("null_rates"),
        {**base, "null_rate_by_column": rates},
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


def _emit_dds_event_field_checks(
    emit: Callable[[str, str, dict[str, Any]], None],
    check_prefix: str,
    df: pd.DataFrame,
    *,
    base: dict[str, Any] | None = None,
) -> None:
    base = dict(base or {})

    def gate(suffix: str, status: str, metrics: dict[str, Any]) -> None:
        emit(f"{check_prefix}.stg_contract.{suffix}", status, {**base, **metrics})

    if df.empty:
        gate("sample", "warning", {"reason": "empty_sample"})
        return

    if "event" in df.columns:
        event = pd.to_numeric(df["event"], errors="coerce")
        ok = event.isin(_EVENT_TYPES)
        rate = _valid_rate(ok)
        gate("event", _gate_status_from_rate(rate, failed_below=0.999, warn_below=1.0), {"valid_event_rate": round(rate, 6)})

    if "event_name" in df.columns:
        names = df["event_name"].astype("string").str.strip().str.lower()
        ok = names.isin(_EVENT_NAMES_VALID)
        rate = _valid_rate(ok)
        gate("event_name", _gate_status_from_rate(rate, failed_below=0.999, warn_below=1.0), {"valid_event_name_rate": round(rate, 6)})

    if {"event", "event_name"}.issubset(df.columns):
        event = pd.to_numeric(df["event"], errors="coerce")
        expected = event.map(_EVENT_CODE_TO_NAME).astype("string")
        actual = df["event_name"].astype("string").str.strip().str.lower()
        ok = expected == actual
        rate = _valid_rate(ok & event.notna())
        gate(
            "event_code_name_alignment",
            _gate_status_from_rate(rate, failed_below=0.999, warn_below=1.0),
            {"aligned_rate": round(rate, 6)},
        )

    if "location" in df.columns:
        rate, metrics = _location_struct_metrics(df)
        gate("location", _gate_status_from_rate(rate, failed_below=0.90, warn_below=0.98), metrics)

    if "location" in df.columns:
        rate, metrics = _location_compressible_metrics(df)
        gate(
            "location_compressible",
            "warning" if rate < 0.5 else "ok",
            metrics,
        )


def _location_struct_metrics(df: pd.DataFrame) -> tuple[float, dict[str, Any]]:
    loc = df["location"]
    mcc, mnc, lac, cell = _location_columns(loc)
    present = loc.notna()
    ok = present & mcc.astype("string").str.len().ge(1) & mnc.astype("string").str.len().ge(1)
    rate = _valid_rate(ok)
    return rate, {
        "location_present_rate": round(_valid_rate(present), 6),
        "location_mcc_mnc_rate": round(rate, 6),
        "rows": int(len(df)),
    }


def _location_compressible_metrics(df: pd.DataFrame) -> tuple[float, dict[str, Any]]:
    loc = df["location"]
    mcc, mnc, lac, cell = _location_columns(loc)
    ok = (
        loc.notna()
        & mcc.astype("string").str.len().ge(1)
        & mnc.astype("string").str.len().ge(1)
        & lac.notna()
        & cell.notna()
    )
    rate = _valid_rate(ok)
    return rate, {
        "compressible_location_rate": round(rate, 6),
        "rows": int(len(df)),
    }


def _location_columns(loc: pd.Series) -> tuple[pd.Series, pd.Series, pd.Series, pd.Series]:
    if isinstance(loc, pd.DataFrame):

        def _col(name: str) -> pd.Series:
            return loc.get(name, pd.Series(pd.NA, index=loc.index))

    else:

        def _col(name: str) -> pd.Series:
            return loc.map(lambda x: x.get(name) if isinstance(x, dict) else pd.NA)

    mcc = _col("mcc").astype("string").str.replace(r"\D+", "", regex=True)
    mnc = _col("mnc").astype("string").str.replace(r"\D+", "", regex=True)
    lac = pd.to_numeric(_col("lac"), errors="coerce")
    cell = pd.to_numeric(_col("cell"), errors="coerce")
    return mcc, mnc, lac, cell


def _valid_rate(mask: pd.Series) -> float:
    if len(mask) == 0:
        return 1.0
    return float(mask.mean())


def _gate_status_from_rate(rate: float, *, failed_below: float, warn_below: float) -> str:
    if rate < failed_below:
        return "failed"
    if rate < warn_below:
        return "warning"
    return "ok"


def _emit_log(check: str, status: str, metrics: dict[str, Any]) -> None:
    payload: dict[str, Any] = {"tag": LOG_TAG, "check": check, "status": status, "metrics": metrics}
    message = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    if status == "failed":
        logger.error(message)
    elif status == "warning":
        logger.warning(message)
    else:
        logger.info(message)


def _emit_summary(*, total_checks: int, warnings: int = 0, failed: int = 0) -> None:
    status = "failed" if failed else ("warning" if warnings else "info")
    _emit_log(
        "summary",
        status,
        {
            "total_checks": int(total_checks),
            "warning_checks": int(warnings),
            "failed_checks": int(failed),
        },
    )
