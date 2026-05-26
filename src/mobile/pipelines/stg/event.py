"""Сборка ``stg_event`` из mobile-витрин за отчётную дату в локальном времени абонента."""

from __future__ import annotations

import logging
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Final

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from mobile.cli_defaults import DEFAULT_PARQUET_COMPRESSION
from mobile.command_timing import append_command_metrics, timed_stage
from mobile.project_paths import (
    MART_PARQUET_FILES,
    discover_mart_parquet_paths,
    filter_df_by_local_report_date,
    filter_paths_near_report_date,
    read_all_parquets_concat,
    resolve_project_path,
    stg_event_output_path,
)

logger = logging.getLogger(__name__)

STG_EVENT_TABLE = "stg_event"

STG_EVENT_FIELDS: list[dict[str, str]] = [
    {"name": "event_timestamp", "type": "string"},
    {"name": "imsi", "type": "string"},
    {"name": "imei", "type": "string"},
    {"name": "msisdn", "type": "string"},
    {"name": "location", "type": "struct"},
    {"name": "event", "type": "uint32"},
    {"name": "event_name", "type": "string"},
    {"name": "event_count", "type": "uint32"},
]

EVENT_CODES: Final[dict[str, int]] = {
    "cdr": 10001,
    "sms": 10002,
    "gprs": 10003,
    "location": 10004,
}

_EVENT_COLUMNS: Final[tuple[str, ...]] = tuple(f["name"] for f in STG_EVENT_FIELDS)

_MART_READ_COLUMNS: Final[dict[str, list[str]]] = {
    "cdr": ["Started", "IMSI", "IMEI", "CallingNumber", "OwnerMCCMNC", "BSStartLac", "BSStartCell"],
    "sms": ["Started", "IMSI", "IMEI", "Calling", "MCC", "MNC", "Lac", "Cell"],
    "gprs": ["Started", "IMSI", "IMEI", "CallingNumber", "OwnerMCCMNC", "BSStartLac", "BSStartCell"],
    "location": ["Started", "IMSI", "IMEI", "Served", "MCC", "MNC", "Lac", "Cell"],
}

_MSISDN_COLUMN: Final[dict[str, str]] = {
    "cdr": "CallingNumber",
    "gprs": "CallingNumber",
    "sms": "Calling",
    "location": "Served",
}

_LOCATION_STRUCT = pa.struct(
    [
        pa.field("mcc", pa.string()),
        pa.field("mnc", pa.string()),
        pa.field("lac", pa.int64()),
        pa.field("cell", pa.int64()),
    ]
)

_COMPRESS_GAP_SECONDS = 300


def run_build(
    dc: str,
    report_date: date,
    cdr_path: str | Path,
    sms_path: str | Path,
    gprs_path: str | Path,
    location_path: str | Path,
) -> dict[str, Any]:
    """Собрать ``events.parquet`` (``stg_event``) для одного ЦОД и отчётной даты.

    Строки отбираются по ``Started`` (локальное время абонента), parquet в пути — окно ±1 день.
    После объединения витрин — сортировка по ``imsi`` / ``event_timestamp``,
    сжатие 5-минутными группами (как ``stg_geo_all._aggregate_events``), запись Parquet ``snappy``.
    """
    mart_roots = {
        "cdr": resolve_project_path(cdr_path),
        "sms": resolve_project_path(sms_path),
        "gprs": resolve_project_path(gprs_path),
        "location": resolve_project_path(location_path),
    }
    perf: dict[str, Any] = {}
    started = time.perf_counter()
    frames: list[pd.DataFrame] = []
    mart_stats: dict[str, dict[str, int]] = {}

    with timed_stage("read_transform_sec", perf):
        for mart, root in mart_roots.items():
            paths_all = discover_mart_parquet_paths(root, MART_PARQUET_FILES[mart])
            paths = filter_paths_near_report_date(paths_all, report_date=report_date)
            raw = read_all_parquets_concat(paths, columns=_MART_READ_COLUMNS[mart])
            filtered = filter_df_by_local_report_date(raw, report_date)
            transformed = _transform_mart_frame(filtered, mart)
            mart_stats[mart] = {
                "parquet_files": len(paths),
                "rows_before_filter": int(len(raw)),
                "rows_after_filter": int(len(filtered)),
                "rows_out": int(len(transformed)),
            }
            if not transformed.empty:
                frames.append(transformed)

    if frames:
        merged = pd.concat(frames, ignore_index=True)
    else:
        merged = pd.DataFrame(columns=_EVENT_COLUMNS)

    rows_merged = int(len(merged))

    with timed_stage("sort_sec", perf):
        if not merged.empty:
            merged = merged.sort_values(
                ["imsi", "event_timestamp", "event_name"],
                kind="mergesort",
            ).reset_index(drop=True)

    compress_stats: dict[str, int] = {"rows_before": rows_merged, "rows_after": rows_merged}

    with timed_stage("compress_sec", perf):
        if not merged.empty:
            merged, compress_stats = _compress_consecutive_events(merged)

    out_path = stg_event_output_path(dc, report_date)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with timed_stage("write_sec", perf):
        job_start = datetime.now(timezone.utc)
        _write_event_parquet(merged, out_path)
        job_end = datetime.now(timezone.utc)

    job_count = int(len(merged))
    logger.info(
        "build-stg-event source_id=%s report_date=%s job_start=%s job_end=%s job_count=%s path=%s",
        dc,
        report_date.isoformat(),
        job_start.isoformat(),
        job_end.isoformat(),
        job_count,
        out_path,
    )

    stats: dict[str, Any] = {
        "datacenter": dc,
        "report_date": report_date.isoformat(),
        "output_path": str(out_path),
        "rows_merged": rows_merged,
        "rows_written": job_count,
        "mart_stats": mart_stats,
        "compress": compress_stats,
        "parquet_compression": DEFAULT_PARQUET_COMPRESSION,
        "job_start": job_start.isoformat(),
        "job_end": job_end.isoformat(),
    }
    perf["elapsed_total_sec"] = round(time.perf_counter() - started, 4)
    append_command_metrics(command="build-stg-event", metrics={**stats, **perf})
    logger.info("build-stg-event completed: %s", stats)
    return stats


def _location_tuples_from_owner_mcc_mnc(df: pd.DataFrame) -> pd.Series:
    mcc_mnc = df["OwnerMCCMNC"].astype("string").str.replace(r"\D+", "", regex=True).fillna("")
    mcc = mcc_mnc.str[:3]
    mnc = mcc_mnc.str[3:]
    lac = pd.to_numeric(df["BSStartLac"], errors="coerce")
    cell = pd.to_numeric(df["BSStartCell"], errors="coerce")
    return pd.Series(zip(mcc, mnc, lac, cell), index=df.index, dtype=object)


def _location_tuples_from_mcc_mnc(df: pd.DataFrame) -> pd.Series:
    mcc = df["MCC"].astype("string").str.replace(r"\D+", "", regex=True).fillna("")
    mnc = df["MNC"].astype("string").str.replace(r"\D+", "", regex=True).fillna("")
    lac = pd.to_numeric(df["Lac"], errors="coerce")
    cell = pd.to_numeric(df["Cell"], errors="coerce")
    return pd.Series(zip(mcc, mnc, lac, cell), index=df.index, dtype=object)


def _location_tuple_valid(value: object) -> bool:
    if not isinstance(value, tuple) or len(value) != 4:
        return False
    mcc, mnc, lac, cell = value
    if pd.isna(mcc) or str(mcc).strip() == "":
        return False
    if pd.isna(mnc) or str(mnc).strip() == "":
        return False
    if pd.isna(lac) or pd.isna(cell):
        return False
    return True


def _transform_mart_frame(df: pd.DataFrame, mart: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=_EVENT_COLUMNS)
    msisdn_col = _MSISDN_COLUMN[mart]
    started = df["Started"].astype("string").str.strip()
    if mart in ("cdr", "gprs"):
        location = _location_tuples_from_owner_mcc_mnc(df)
    else:
        location = _location_tuples_from_mcc_mnc(df)
    out = pd.DataFrame(
        {
            "event_timestamp": started,
            "imsi": df["IMSI"].astype("string").str.strip(),
            "imei": df["IMEI"].astype("string").str.strip(),
            "msisdn": df[msisdn_col].astype("string").str.strip(),
            "location": location,
            "event": pd.Series(EVENT_CODES[mart], index=df.index, dtype="uint32"),
            "event_name": mart,
        }
    )
    valid = out["event_timestamp"].str.fullmatch(r"\d{14}", na=False)
    return out.loc[valid].reset_index(drop=True)


def _unpack_location(location: pd.Series) -> tuple[pd.Series, pd.Series, pd.Series, pd.Series]:
    mcc: list[object] = []
    mnc: list[object] = []
    lac: list[object] = []
    cell: list[object] = []
    for item in location:
        if isinstance(item, tuple) and len(item) == 4:
            mcc.append(item[0])
            mnc.append(item[1])
            lac.append(item[2])
            cell.append(item[3])
        else:
            mcc.append(pd.NA)
            mnc.append(pd.NA)
            lac.append(pd.NA)
            cell.append(pd.NA)
    idx = location.index
    return (
        pd.Series(mcc, index=idx),
        pd.Series(mnc, index=idx),
        pd.Series(lac, index=idx),
        pd.Series(cell, index=idx),
    )


def _build_cgi_series(
    mcc: pd.Series,
    mnc: pd.Series,
    lac: pd.Series,
    cell: pd.Series,
) -> pd.Series:
    mcc_n = pd.to_numeric(mcc, errors="coerce")
    mnc_n = pd.to_numeric(mnc, errors="coerce")
    lac_n = pd.to_numeric(lac, errors="coerce")
    cell_n = pd.to_numeric(cell, errors="coerce")
    return (
        mcc_n.fillna(0).astype("Int64") * 10**13
        + mnc_n.fillna(0).astype("Int64") * 10**11
        + lac_n.fillna(0).astype("Int64") * 10**6
        + cell_n.fillna(0).astype("Int64")
    )


def _location_compressible(value: object) -> bool:
    """Строки с валидным lac/cell (и mcc/mnc) участвуют в 5m-схлопывании."""
    return _location_tuple_valid(value)


def _compress_consecutive_events(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    """Схлопывание событий с валидной location; остальные — как есть, ``event_count=1``."""
    rows_before = int(len(df))
    if df.empty:
        return df, {"rows_before": 0, "rows_after": 0, "rows_passthrough": 0, "rows_compressed_in": 0}

    compressible_mask = df["location"].map(_location_compressible)
    passthrough = df.loc[~compressible_mask].copy()
    if not passthrough.empty:
        passthrough["event_count"] = pd.Series(1, index=passthrough.index, dtype="uint32")

    compressible = df.loc[compressible_mask]
    compressed_parts: list[pd.DataFrame] = []
    if not compressible.empty:
        work = compressible.copy()
        work["_ts"] = pd.to_datetime(work["event_timestamp"], format="%Y%m%d%H%M%S", errors="coerce")
        mcc, mnc, lac, cell = _unpack_location(work["location"])
        work["_cgi"] = _build_cgi_series(mcc, mnc, lac, cell)
        work["_bucket_5m"] = work["_ts"].dt.floor("5min")
        work = work.sort_values(["imsi", "_ts", "event_name", "_cgi"], kind="mergesort")
        gap_sec = (work["_ts"] - work["_ts"].shift(1)).dt.total_seconds()
        new_grp = (
            (work["imsi"] != work["imsi"].shift(1))
            | (work["event_name"] != work["event_name"].shift(1))
            | (work["_cgi"] != work["_cgi"].shift(1))
            | (work["_bucket_5m"] != work["_bucket_5m"].shift(1))
            | (gap_sec > _COMPRESS_GAP_SECONDS)
        ).fillna(True)
        work["_grp"] = new_grp.astype("int64").cumsum()
        data_cols = [c for c in _EVENT_COLUMNS if c != "event_count"]
        grouped = work.groupby("_grp", as_index=False).agg(
            **{col: (col, "first") for col in data_cols},
            event_count=("_grp", "size"),
        )
        grouped["event_count"] = grouped["event_count"].astype("uint32")
        compressed_parts.append(grouped)

    parts: list[pd.DataFrame] = []
    if compressed_parts:
        parts.append(compressed_parts[0])
    if not passthrough.empty:
        parts.append(passthrough)
    if not parts:
        return pd.DataFrame(columns=list(_EVENT_COLUMNS)), {
            "rows_before": rows_before,
            "rows_after": 0,
            "rows_passthrough": 0,
            "rows_compressed_in": 0,
        }

    out = pd.concat(parts, ignore_index=True)
    out = out[list(_EVENT_COLUMNS)].sort_values(
        ["imsi", "event_timestamp", "event_name"],
        kind="mergesort",
    ).reset_index(drop=True)
    rows_after = int(len(out))
    return out, {
        "rows_before": rows_before,
        "rows_after": rows_after,
        "rows_passthrough": int(len(passthrough)),
        "rows_compressed_in": int(len(compressible)),
    }


def _write_event_parquet(df: pd.DataFrame, path: Path) -> None:
    compression = DEFAULT_PARQUET_COMPRESSION
    if df.empty:
        table = pa.table(
            {
                "event_timestamp": pa.array([], type=pa.string()),
                "imsi": pa.array([], type=pa.string()),
                "imei": pa.array([], type=pa.string()),
                "msisdn": pa.array([], type=pa.string()),
                "location": pa.array([], type=_LOCATION_STRUCT),
                "event": pa.array([], type=pa.uint32()),
                "event_name": pa.array([], type=pa.string()),
                "event_count": pa.array([], type=pa.uint32()),
            }
        )
        pq.write_table(table, path, compression=compression)
        return

    loc = df["location"]
    mcc: list[str | None] = []
    mnc: list[str | None] = []
    lac: list[int | None] = []
    cell: list[int | None] = []
    for item in loc:
        if not isinstance(item, tuple) or len(item) != 4:
            mcc.append(None)
            mnc.append(None)
            lac.append(None)
            cell.append(None)
            continue
        m0, n0, l0, c0 = item
        mcc.append(str(m0).strip() if m0 is not None and not pd.isna(m0) else None)
        mnc.append(str(n0).strip() if n0 is not None and not pd.isna(n0) else None)
        lac.append(int(l0) if l0 is not None and not pd.isna(l0) else None)
        cell.append(int(c0) if c0 is not None and not pd.isna(c0) else None)

    location_arr = pa.StructArray.from_arrays(
        [
            pa.array(mcc, type=pa.string()),
            pa.array(mnc, type=pa.string()),
            pa.array(lac, type=pa.int64()),
            pa.array(cell, type=pa.int64()),
        ],
        fields=_LOCATION_STRUCT,
    )
    table = pa.table(
        {
            "event_timestamp": pa.array(df["event_timestamp"].astype("string"), type=pa.string()),
            "imsi": pa.array(df["imsi"].astype("string"), type=pa.string()),
            "imei": pa.array(df["imei"].astype("string"), type=pa.string()),
            "msisdn": pa.array(df["msisdn"].astype("string"), type=pa.string()),
            "location": location_arr,
            "event": pa.array(df["event"], type=pa.uint32()),
            "event_name": pa.array(df["event_name"].astype("string"), type=pa.string()),
            "event_count": pa.array(df["event_count"], type=pa.uint32()),
        }
    )
    pq.write_table(table, path, compression=compression)
