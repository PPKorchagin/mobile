"""Сборка ``stg_geo_all`` из ``event_dds`` + ``stg_bs`` без binding-fill."""

from __future__ import annotations

import logging
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from mobile.cli_defaults import DEFAULT_PARQUET_COMPRESSION
from mobile.command_timing import append_command_metrics, timed_stage
from mobile.project_paths import stg_event_dds_day_key_from_path
from mobile.project_paths import (
    DEFAULT_STG_EVENT_DDS_ROOT,
    resolve_project_path,
    stg_bs_output_path,
    stg_geo_all_output_path,
)

logger = logging.getLogger(__name__)

STG_GEO_ALL_TABLE = "stg_geo_all"

_EVENT_CODE_TO_NAME: dict[int, str] = {
    10001: "cdr",
    10002: "sms",
    10003: "gprs",
    10004: "location",
}
_EVENT_NAMES_VALID = frozenset(_EVENT_CODE_TO_NAME.values())
_AGG_GAP_SECONDS = 300

_READ_COLUMNS = [
    "event_timestamp",
    "imsi",
    "imei",
    "msisdn",
    "location",
    "event",
    "event_name",
    "event_count",
]

_OUTPUT_COLUMNS = [
    "msisdn",
    "imsi",
    "imei",
    "start_time_utc",
    "end_time_utc",
    "utc_offset",
    "lat",
    "lon",
    "bs_type",
    "cgi",
    "event_count",
    "source_event_type",
    "oktmo_code_1",
    "oktmo_code_2",
]


def run_build(
    *,
    report_date: date,
    event_dds_path: str | Path | None = None,
    stg_bs_path: str | Path | None = None,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    """Собрать ``stg_geo_all`` за отчётный день из ``event_dds`` без binding-fill."""
    started = time.perf_counter()
    perf: dict[str, Any] = {}

    dds_root = resolve_project_path(event_dds_path or DEFAULT_STG_EVENT_DDS_ROOT)
    out_path = resolve_project_path(output_path) if output_path is not None else stg_geo_all_output_path(report_date)
    bs_path = resolve_project_path(stg_bs_path) if stg_bs_path is not None else stg_bs_output_path()
    if not bs_path.exists():
        raise FileNotFoundError(f"stg_bs parquet not found: {bs_path}")

    with timed_stage("read_event_dds_sec", perf):
        source_df = _read_event_dds_day(dds_root, report_date)
    with timed_stage("read_bs_sec", perf):
        bs = pd.read_parquet(bs_path)
    with timed_stage("prepare_lookup_sec", perf):
        bs_lookup = _build_bs_lookup(bs)
    with timed_stage("transform_sec", perf):
        merged, transform_metrics = _transform_event_dds(source_df=source_df, bs_lookup=bs_lookup, report_date_utc=report_date)
        validated = _validate_records(merged)
    with timed_stage("aggregate_sec", perf):
        geo = _aggregate_events(validated)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with timed_stage("write_sec", perf):
        geo.to_parquet(out_path, index=False, compression=DEFAULT_PARQUET_COMPRESSION)

    stats: dict[str, Any] = {
        "report_date": report_date.isoformat(),
        "event_dds_path": str(dds_root),
        "stg_bs_path": str(bs_path),
        "output_path": str(out_path),
        "rows_read_event_dds": int(len(source_df)),
        "rows_after_transform": int(len(merged)),
        "rows_after_validate": int(len(validated)),
        "rows_written": int(len(geo)),
        "parquet_compression": DEFAULT_PARQUET_COMPRESSION,
        **transform_metrics,
    }
    perf["elapsed_total_sec"] = round(time.perf_counter() - started, 4)
    append_command_metrics(command="build-stg-geo-all", metrics={**stats, **perf})
    logger.info("build-stg-geo-all completed: %s", stats)
    return stats


def _read_event_dds_day(path: Path, report_date: date) -> pd.DataFrame:
    paths = _discover_event_dds_paths_for_utc_day(path, report_date)
    if not paths:
        return pd.DataFrame(columns=_READ_COLUMNS)
    parts: list[pd.DataFrame] = []
    for p in paths:
        try:
            parts.append(pd.read_parquet(p, columns=_READ_COLUMNS))
        except Exception:
            continue
    if not parts:
        return pd.DataFrame(columns=_READ_COLUMNS)
    merged = pd.concat(parts, ignore_index=True)
    ts = merged["event_timestamp"].astype("string").str.strip()
    mask = ts.str.fullmatch(r"\d{14}", na=False)
    if not bool(mask.any()):
        return merged.iloc[0:0].copy()
    return merged.loc[mask].copy()


def _discover_event_dds_paths_for_utc_day(path: Path, report_date: date) -> list[Path]:
    """Берём периоды, потенциально попадающие в UTC-день: report_date ± 1 день."""
    day_keys = {
        (report_date - timedelta(days=1)).isoformat(),
        report_date.isoformat(),
        (report_date + timedelta(days=1)).isoformat(),
    }
    if path.is_file():
        return [path] if path.suffix.lower() == ".parquet" else []
    if not path.is_dir():
        return []
    out: list[Path] = []
    for p in sorted(path.rglob("*.parquet")):
        key = stg_event_dds_day_key_from_path(p)
        if key is None or key in day_keys:
            out.append(p)
    return out


def _location_arrays_from_event(df: pd.DataFrame) -> tuple[pd.Series, pd.Series, pd.Series, pd.Series]:
    loc = df.get("location")
    if loc is None:
        empty = pd.Series(pd.NA, index=df.index, dtype="Float64")
        return empty, empty, empty, empty

    sample = loc.dropna()
    if sample.empty:
        empty = pd.Series(pd.NA, index=df.index, dtype="Float64")
        return empty, empty, empty, empty

    first = sample.iloc[0]
    if isinstance(first, dict):
        mcc = pd.to_numeric(loc.map(lambda x: x.get("mcc") if isinstance(x, dict) else pd.NA), errors="coerce")
        mnc = pd.to_numeric(loc.map(lambda x: x.get("mnc") if isinstance(x, dict) else pd.NA), errors="coerce")
        lac = pd.to_numeric(loc.map(lambda x: x.get("lac") if isinstance(x, dict) else pd.NA), errors="coerce")
        cell = pd.to_numeric(loc.map(lambda x: x.get("cell") if isinstance(x, dict) else pd.NA), errors="coerce")
        return mcc, mnc, lac, cell

    if isinstance(first, tuple):
        mcc = pd.to_numeric(loc.map(lambda x: x[0] if isinstance(x, tuple) and len(x) > 0 else pd.NA), errors="coerce")
        mnc = pd.to_numeric(loc.map(lambda x: x[1] if isinstance(x, tuple) and len(x) > 1 else pd.NA), errors="coerce")
        lac = pd.to_numeric(loc.map(lambda x: x[2] if isinstance(x, tuple) and len(x) > 2 else pd.NA), errors="coerce")
        cell = pd.to_numeric(loc.map(lambda x: x[3] if isinstance(x, tuple) and len(x) > 3 else pd.NA), errors="coerce")
        return mcc, mnc, lac, cell

    parts = loc.astype("string").str.split("|", expand=True)
    if parts.shape[1] < 3:
        empty = pd.Series(pd.NA, index=df.index, dtype="Float64")
        return empty, empty, empty, empty
    mcc_mnc = parts[0].astype("string").str.replace(r"\D+", "", regex=True)
    mcc = pd.to_numeric(mcc_mnc.str[:3], errors="coerce")
    mnc = pd.to_numeric(mcc_mnc.str[3:], errors="coerce")
    lac = pd.to_numeric(parts[1], errors="coerce")
    cell = pd.to_numeric(parts[2], errors="coerce")
    return mcc, mnc, lac, cell


def _build_cgi_series(mcc: pd.Series, mnc: pd.Series, lac: pd.Series, cell: pd.Series) -> pd.Series:
    return (
        mcc.fillna(0).astype("Int64") * 10**13
        + mnc.fillna(0).astype("Int64") * 10**11
        + lac.fillna(0).astype("Int64") * 10**6
        + cell.fillna(0).astype("Int64")
    )


def _event_timestamps_utc(event_timestamp: pd.Series | None, *, utc_offset_minutes: int = 0) -> pd.DataFrame:
    if event_timestamp is None or len(event_timestamp) == 0:
        return pd.DataFrame(
            {
                "start_time_utc": pd.Series(dtype="datetime64[ns]"),
                "utc_offset": pd.Series(dtype="Int32"),
            }
        )
    index = event_timestamp.index
    local = event_timestamp.astype("string").str.strip()
    local_dt = pd.to_datetime(local, format="%Y%m%d%H%M%S", errors="coerce")
    start_utc = local_dt - pd.to_timedelta(int(utc_offset_minutes), unit="m")
    utc_offset = pd.Series(int(utc_offset_minutes), index=index, dtype="Int32")
    utc_offset = utc_offset.where(start_utc.notna(), 0)
    return pd.DataFrame({"start_time_utc": start_utc, "utc_offset": utc_offset}, index=index)


def _normalize_msisdn_series(series: pd.Series | None) -> pd.Series:
    if series is None:
        return pd.Series(dtype="Int64")
    normalized = series.astype("string").str.replace(r"\D+", "", regex=True)
    normalized = normalized.mask(normalized == "", pd.NA)
    normalized = normalized.where(normalized.isna() | normalized.str.len().isin([10, 11]))
    normalized = normalized.mask(normalized.str.len() == 10, "7" + normalized)
    normalized = normalized.mask(normalized.str.len() == 11, normalized.str.replace(r"^8", "7", n=1, regex=True))
    return pd.to_numeric(normalized, errors="coerce").astype("Int64")


def _normalize_digits_to_int(series: pd.Series | None, min_length: int) -> pd.Series:
    if series is None:
        return pd.Series(dtype="Int64")
    cleaned = series.astype("string").str.replace(r"\D+", "", regex=True)
    cleaned = cleaned.mask(cleaned == "", pd.NA)
    cleaned = cleaned.where(cleaned.isna() | (cleaned.str.len() >= min_length))
    return pd.to_numeric(cleaned, errors="coerce").astype("Int64")


def _build_bs_lookup(bs: pd.DataFrame) -> pd.DataFrame:
    work = bs.copy()
    mcc = pd.to_numeric(work.get("mcc"), errors="coerce")
    mnc = pd.to_numeric(work.get("mnc"), errors="coerce")
    lac = pd.to_numeric(work.get("lac"), errors="coerce")
    cell = pd.to_numeric(work.get("cell_id"), errors="coerce")
    work["cgi"] = _build_cgi_series(mcc, mnc, lac, cell)
    work["lat_bs"] = pd.to_numeric(work.get("lat"), errors="coerce")
    work["lon_bs"] = pd.to_numeric(work.get("lon"), errors="coerce")
    work["date_on_bs"] = pd.to_datetime(work.get("date_on"), errors="coerce")
    work["date_off_bs"] = pd.to_datetime(work.get("date_off"), errors="coerce")
    work["utc_offset"] = pd.to_numeric(work.get("timezone"), errors="coerce").fillna(0).astype("Int32")
    work["bs_type_code"] = work.get("bs_type").astype("string").fillna("o")

    lookup = work[
        [
            "cgi",
            "lat_bs",
            "lon_bs",
            "bs_type_code",
            "oktmo_code_1",
            "oktmo_code_2",
            "date_on_bs",
            "date_off_bs",
            "utc_offset",
        ]
    ].dropna(subset=["cgi", "date_on_bs", "lat_bs", "lon_bs"])
    lookup["cgi"] = lookup["cgi"].astype("Int64")
    return lookup


def _empty_enriched_events() -> pd.DataFrame:
    return pd.DataFrame(columns=_OUTPUT_COLUMNS)


def _transform_event_dds(
    *, source_df: pd.DataFrame, bs_lookup: pd.DataFrame, report_date_utc: date
) -> tuple[pd.DataFrame, dict[str, int]]:
    if source_df.empty:
        return _empty_enriched_events(), _empty_transform_metrics()

    df = source_df.copy()
    mcc, mnc, lac, cell = _location_arrays_from_event(df)
    cgi_raw = _build_cgi_series(mcc, mnc, lac, cell)
    cgi = cgi_raw.where(mcc.notna() & mnc.notna() & lac.notna() & cell.notna())
    event_name = df.get("event_name")
    if event_name is not None and event_name.notna().any():
        source_event_type = event_name.astype("string").str.strip().str.lower()
    else:
        event_type = pd.to_numeric(df.get("event"), errors="coerce")
        source_event_type = event_type.map(_EVENT_CODE_TO_NAME).astype("string")

    normalized = pd.DataFrame(
        {
            "msisdn": _normalize_msisdn_series(df.get("msisdn")),
            "imsi": _normalize_digits_to_int(df.get("imsi"), min_length=10),
            "imei": _normalize_digits_to_int(df.get("imei"), min_length=8),
            "cgi": cgi,
            "source_event_type": source_event_type,
            "event_count": pd.to_numeric(df.get("event_count"), errors="coerce").fillna(1).astype("Int32"),
        }
    )
    normalized["start_time_local"] = pd.to_datetime(
        df.get("event_timestamp").astype("string").str.strip(),
        format="%Y%m%d%H%M%S",
        errors="coerce",
    )
    normalized["end_time_utc"] = pd.NaT
    known_type = normalized["source_event_type"].notna() & normalized["source_event_type"].isin(_EVENT_NAMES_VALID)
    metrics = _collect_transform_metrics(
        source_df=source_df,
        normalized=normalized,
        known_type=known_type,
        mcc=mcc,
        mnc=mnc,
        lac=lac,
        cell=cell,
    )
    normalized = normalized.loc[known_type].reset_index(drop=True)
    normalized, cgi_imputed = _impute_intermediate_cgi(normalized, bs_lookup)
    metrics["rows_cgi_imputed"] = int(cgi_imputed)
    return _enrich_and_filter_day(normalized, bs_lookup, report_date_utc), metrics


def _empty_transform_metrics() -> dict[str, int]:
    return {
        "rows_norm_error_event_timestamp": 0,
        "rows_norm_error_location_parts": 0,
        "rows_norm_error_msisdn": 0,
        "rows_norm_error_imsi": 0,
        "rows_norm_error_imei": 0,
        "rows_norm_error_event_count": 0,
        "rows_norm_error_event_type": 0,
        "rows_norm_error_cgi": 0,
        "rows_cgi_imputed": 0,
    }


def _collect_transform_metrics(
    *,
    source_df: pd.DataFrame,
    normalized: pd.DataFrame,
    known_type: pd.Series,
    mcc: pd.Series,
    mnc: pd.Series,
    lac: pd.Series,
    cell: pd.Series,
) -> dict[str, int]:
    metrics = _empty_transform_metrics()
    event_ts_src = source_df.get("event_timestamp")
    event_ts = (
        event_ts_src.astype("string").str.strip()
        if event_ts_src is not None
        else pd.Series(pd.NA, index=source_df.index, dtype="string")
    )
    event_count_src = source_df.get("event_count")
    event_count_raw = (
        pd.to_numeric(event_count_src, errors="coerce")
        if event_count_src is not None
        else pd.Series(pd.NA, index=source_df.index, dtype="Float64")
    )
    location_src = source_df.get("location")
    location_present = (
        location_src.notna()
        if location_src is not None
        else pd.Series(False, index=source_df.index, dtype="bool")
    )
    valid_cgi_parts = mcc.notna() & mnc.notna() & lac.notna() & cell.notna()
    msisdn_src = source_df.get("msisdn")
    imsi_src = source_df.get("imsi")
    imei_src = source_df.get("imei")

    metrics["rows_norm_error_event_timestamp"] = int(event_ts.notna().sum() - normalized["start_time_local"].notna().sum())
    metrics["rows_norm_error_location_parts"] = int((location_present & ~valid_cgi_parts).sum())
    metrics["rows_norm_error_msisdn"] = int((msisdn_src.notna().sum() if msisdn_src is not None else 0) - normalized["msisdn"].notna().sum())
    metrics["rows_norm_error_imsi"] = int((imsi_src.notna().sum() if imsi_src is not None else 0) - normalized["imsi"].notna().sum())
    metrics["rows_norm_error_imei"] = int((imei_src.notna().sum() if imei_src is not None else 0) - normalized["imei"].notna().sum())
    metrics["rows_norm_error_event_count"] = int((event_count_src.notna().sum() if event_count_src is not None else 0) - event_count_raw.notna().sum())
    metrics["rows_norm_error_event_type"] = int((~known_type).sum())
    metrics["rows_norm_error_cgi"] = int((~valid_cgi_parts).sum())
    return metrics


def _bs_points_by_cgi(bs_lookup: pd.DataFrame) -> pd.DataFrame:
    points = bs_lookup.dropna(subset=["cgi", "lat_bs", "lon_bs"]).copy()
    if points.empty:
        return pd.DataFrame(columns=["cgi", "lat_bs", "lon_bs"]).set_index("cgi")
    points["cgi"] = pd.to_numeric(points["cgi"], errors="coerce").astype("Int64")
    points = points.dropna(subset=["cgi"]).copy()
    points = points.groupby("cgi", as_index=False).agg(lat_bs=("lat_bs", "median"), lon_bs=("lon_bs", "median"))
    return points.set_index("cgi")


def _impute_intermediate_cgi(normalized: pd.DataFrame, bs_lookup: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    if normalized.empty:
        return normalized, 0
    points = _bs_points_by_cgi(bs_lookup)
    if points.empty:
        return normalized, 0

    work = normalized.sort_values(["msisdn", "start_time_local"], kind="mergesort").copy()
    work["cgi"] = pd.to_numeric(work["cgi"], errors="coerce").astype("Int64")
    valid_cgi = work["cgi"].isin(points.index)
    missing_idx = work.index[~valid_cgi]
    if len(missing_idx) == 0:
        return normalized, 0

    cgi_filled = 0
    for _, idxs in work.groupby("msisdn", dropna=True).groups.items():
        grp_idx = list(idxs)
        grp = work.loc[grp_idx]
        valid_mask = grp["cgi"].isin(points.index).to_numpy()
        if valid_mask.sum() < 2:
            continue
        positions = np.arange(len(grp_idx))
        known_pos = positions[valid_mask]
        known_cgi = grp["cgi"].to_numpy()[valid_mask]

        for pos in positions[~valid_mask]:
            prev_candidates = known_pos[known_pos < pos]
            next_candidates = known_pos[known_pos > pos]
            if len(prev_candidates) == 0 or len(next_candidates) == 0:
                continue
            prev_pos = int(prev_candidates.max())
            next_pos = int(next_candidates.min())
            prev_cgi = int(known_cgi[np.where(known_pos == prev_pos)[0][0]])
            next_cgi = int(known_cgi[np.where(known_pos == next_pos)[0][0]])
            chosen = _choose_intermediate_cgi(
                points=points,
                prev_cgi=prev_cgi,
                next_cgi=next_cgi,
                fraction=(pos - prev_pos) / float(next_pos - prev_pos),
            )
            if chosen is None:
                continue
            work.at[grp_idx[pos], "cgi"] = chosen
            cgi_filled += 1

    if cgi_filled:
        logger.info("build-stg-geo-all: imputed cgi for %s events between known BS anchors", cgi_filled)
    return work.sort_index(), int(cgi_filled)


def _choose_intermediate_cgi(*, points: pd.DataFrame, prev_cgi: int, next_cgi: int, fraction: float) -> int | None:
    if prev_cgi not in points.index or next_cgi not in points.index:
        return None
    if prev_cgi == next_cgi:
        return prev_cgi

    prev = points.loc[prev_cgi]
    nxt = points.loc[next_cgi]
    lat0 = float(prev["lat_bs"])
    lon0 = float(prev["lon_bs"])
    lat1 = float(nxt["lat_bs"])
    lon1 = float(nxt["lon_bs"])

    target_lat = lat0 + (lat1 - lat0) * float(fraction)
    target_lon = lon0 + (lon1 - lon0) * float(fraction)

    dlat = lat1 - lat0
    dlon = lon1 - lon0
    denom = dlat * dlat + dlon * dlon

    candidates = points.copy()
    if denom > 0:
        proj = ((candidates["lat_bs"] - lat0) * dlat + (candidates["lon_bs"] - lon0) * dlon) / denom
        corridor = proj.between(-0.2, 1.2, inclusive="both")
        if bool(corridor.any()):
            candidates = candidates[corridor].copy()
            candidates["proj"] = proj[corridor]
        else:
            candidates["proj"] = proj
    else:
        candidates["proj"] = 0.5

    dist2 = (candidates["lat_bs"] - target_lat) ** 2 + (candidates["lon_bs"] - target_lon) ** 2
    score = dist2 + ((candidates["proj"] - float(fraction)).abs() * 0.01)
    if score.empty:
        return None
    return int(score.idxmin())


def _enrich_and_filter_day(normalized: pd.DataFrame, bs_lookup: pd.DataFrame, report_date_utc: date) -> pd.DataFrame:
    if normalized.empty:
        return _empty_enriched_events()
    events = normalized[normalized["start_time_local"].notna()].copy()
    events = events.reset_index(drop=False).rename(columns={"index": "_event_id"})
    merged = events.merge(bs_lookup, on="cgi", how="left")
    if merged.empty:
        return _empty_enriched_events()

    active_mask = (
        merged["date_on_bs"].notna()
        & (merged["start_time_local"] >= merged["date_on_bs"])
        & (merged["date_off_bs"].isna() | (merged["start_time_local"] <= merged["date_off_bs"]))
    )
    active_matches = merged[active_mask].copy()
    active_best = active_matches.sort_values(by=["_event_id", "date_on_bs"], ascending=[True, False]).drop_duplicates(
        subset=["_event_id"], keep="first"
    )

    unresolved = events[~events["_event_id"].isin(active_best["_event_id"])].copy()
    fallback_best = pd.DataFrame()
    if not unresolved.empty:
        fallback = unresolved.merge(bs_lookup, on="cgi", how="left")
        fallback = fallback[fallback["date_on_bs"].notna()].copy()
        if not fallback.empty:
            fallback["date_gap_sec"] = (fallback["start_time_local"] - fallback["date_on_bs"]).abs().dt.total_seconds()
            fallback_best = fallback.sort_values(
                by=["_event_id", "date_gap_sec", "date_on_bs"],
                ascending=[True, True, False],
            ).drop_duplicates(subset=["_event_id"], keep="first")

    selected = pd.concat([active_best, fallback_best], ignore_index=True)
    if selected.empty:
        return _empty_enriched_events()

    selected["lat"] = selected["lat_bs"]
    selected["lon"] = selected["lon_bs"]
    selected["bs_type"] = selected["bs_type_code"]
    selected["oktmo_code_1"] = selected["oktmo_code_1"].fillna("unknown").astype("string")
    selected["oktmo_code_2"] = selected["oktmo_code_2"].astype("string")
    utc_offset_col = "utc_offset"
    if utc_offset_col not in selected.columns:
        if "utc_offset_y" in selected.columns:
            utc_offset_col = "utc_offset_y"
        elif "utc_offset_x" in selected.columns:
            utc_offset_col = "utc_offset_x"
    selected["utc_offset"] = pd.to_numeric(selected.get(utc_offset_col), errors="coerce").fillna(0).astype("Int32")
    selected["start_time_utc"] = selected["start_time_local"] - pd.to_timedelta(selected["utc_offset"], unit="h")
    utc_day_start = datetime.combine(report_date_utc, datetime.min.time())
    utc_day_end = utc_day_start + timedelta(days=1)
    selected = selected[
        selected["start_time_utc"].notna()
        & (selected["start_time_utc"] >= utc_day_start)
        & (selected["start_time_utc"] < utc_day_end)
    ]
    if selected.empty:
        return _empty_enriched_events()

    return selected[_OUTPUT_COLUMNS].copy()


def _validate_records(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    validated = df.copy()
    validated = validated[
        validated["msisdn"].notna()
        & validated["cgi"].notna()
        & validated["start_time_utc"].notna()
    ]
    validated["lat"] = pd.to_numeric(validated["lat"], errors="coerce")
    validated["lon"] = pd.to_numeric(validated["lon"], errors="coerce")
    validated = validated[
        validated["lat"].between(-90, 90, inclusive="both")
        & validated["lon"].between(-180, 180, inclusive="both")
    ]
    validated["lat"] = validated["lat"].round(5)
    validated["lon"] = validated["lon"].round(5)
    validated["msisdn"] = pd.to_numeric(validated["msisdn"], errors="coerce").astype("Int64")
    validated["imsi"] = pd.to_numeric(validated["imsi"], errors="coerce").astype("Int64")
    validated["imei"] = pd.to_numeric(validated["imei"], errors="coerce").astype("Int64")
    validated["cgi"] = pd.to_numeric(validated["cgi"], errors="coerce").astype("Int64")
    validated["event_count"] = pd.to_numeric(validated["event_count"], errors="coerce").fillna(1).astype("Int32")
    validated["bs_type"] = validated["bs_type"].astype("string").fillna("o")
    validated["source_event_type"] = validated["source_event_type"].astype("string")
    validated["oktmo_code_1"] = validated["oktmo_code_1"].astype("string")
    validated["oktmo_code_2"] = validated["oktmo_code_2"].astype("string")
    return validated.reset_index(drop=True)


def _aggregate_events(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=_OUTPUT_COLUMNS)

    work = df.sort_values(["msisdn", "start_time_utc", "source_event_type", "cgi"], kind="mergesort").copy()
    work["bucket_5m"] = work["start_time_utc"].dt.floor("5min")
    gap_sec = (work["start_time_utc"] - work["start_time_utc"].shift(1)).dt.total_seconds()
    changed_msisdn = (work["msisdn"] != work["msisdn"].shift(1)).fillna(True)
    changed_type = (work["source_event_type"] != work["source_event_type"].shift(1)).fillna(True)
    changed_cgi = (work["cgi"] != work["cgi"].shift(1)).fillna(True)
    changed_bucket = (work["bucket_5m"] != work["bucket_5m"].shift(1)).fillna(True)
    gap_break = (gap_sec > _AGG_GAP_SECONDS).fillna(True)
    new_grp = changed_msisdn | changed_type | changed_cgi | changed_bucket | gap_break
    work["_grp"] = new_grp.astype("int64").cumsum()

    grouped = (
        work.groupby("_grp", as_index=False)
        .agg(
            msisdn=("msisdn", "first"),
            imsi=("imsi", "first"),
            imei=("imei", "first"),
            start_time_utc=("start_time_utc", "min"),
            end_time_utc=("end_time_utc", "max"),
            utc_offset=("utc_offset", "first"),
            lat=("lat", "first"),
            lon=("lon", "first"),
            bs_type=("bs_type", "first"),
            cgi=("cgi", "first"),
            event_count=("event_count", "size"),
            source_event_type=("source_event_type", "first"),
            oktmo_code_1=("oktmo_code_1", "first"),
            oktmo_code_2=("oktmo_code_2", "first"),
        )
        .drop(columns=["_grp"])
    )
    grouped["event_count"] = pd.to_numeric(grouped["event_count"], errors="coerce").fillna(1).astype("Int32")
    grouped["end_time_utc"] = grouped["end_time_utc"].where(grouped["end_time_utc"] > grouped["start_time_utc"])
    return grouped[_OUTPUT_COLUMNS].sort_values(["msisdn", "start_time_utc", "source_event_type"], kind="mergesort").reset_index(drop=True)
