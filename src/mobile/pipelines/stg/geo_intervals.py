"""Сборка ``stg_geo_intervals`` из ``stg_geo_all`` + ``stg_bs`` + ``stg_time_zones``."""

from __future__ import annotations

import logging
import math
import time
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from shapely import wkt
from shapely.errors import GEOSException
from shapely.geometry import Point
from shapely.prepared import prep

from mobile.cli_defaults import DEFAULT_PARQUET_COMPRESSION
from mobile.command_timing import append_command_metrics, timed_stage
from mobile.pipelines.stg.subscriber_ids import normalize_imei, normalize_imsi, normalize_msisdn
from mobile.project_paths import (
    resolve_project_path,
    resolve_stg_daily_parquet_path,
    resolve_stg_monthly_parquet_path,
)

logger = logging.getLogger(__name__)

_SECONDS_5MIN = 300
_DIST_THRESHOLD_SINGLE_BS_M = 40_000_000.0
_DIST_THRESHOLD_MULTI_BS_M = 3500.0
_MERGE_GAP_NIGHT_MIN = 30.0
_MERGE_GAP_DAY_MIN = 5.0
_SUB_COORD_TOL = 1e-6
_EARTH_RADIUS_M = 6_371_000.0

_OUTPUT_COLUMNS: tuple[str, ...] = (
    "msisdn",
    "imsi",
    "imei",
    "start_time_utc",
    "end_time_utc",
    "cgi_list",
    "sub_lat",
    "sub_lon",
    "bs_type",
    "timezone",
    "oktmo_code_1",
    "oktmo_code_2",
    "time_key",
)


def run_build(
    *,
    report_date: date,
    stg_geo_all_path: str | Path,
    stg_bs_path: str | Path,
    time_zones_path: str | Path,
    stg_msisdn_imsi_path: str | Path,
    stg_msisdn_imei_path: str | Path,
    output_path: str | Path,
) -> dict[str, Any]:
    started = time.perf_counter()
    perf: dict[str, Any] = {}

    geo_all_file = resolve_stg_daily_parquet_path(stg_geo_all_path, report_date)
    bs_file = resolve_project_path(stg_bs_path)
    tz_file = resolve_project_path(time_zones_path)
    imsi_file = resolve_stg_monthly_parquet_path(stg_msisdn_imsi_path, report_date)
    imei_file = resolve_stg_monthly_parquet_path(stg_msisdn_imei_path, report_date)
    out_file = resolve_stg_daily_parquet_path(output_path, report_date)

    if not geo_all_file.exists():
        raise FileNotFoundError(f"stg_geo_all parquet not found: {geo_all_file}")
    if not bs_file.exists():
        raise FileNotFoundError(f"stg_bs parquet not found: {bs_file}")
    if not tz_file.exists():
        raise FileNotFoundError(f"stg_time_zones parquet not found: {tz_file}")

    with timed_stage("read_inputs_sec", perf):
        geo = pd.read_parquet(geo_all_file)
        bs = pd.read_parquet(bs_file)
        tz_df = pd.read_parquet(tz_file)
        imsi_binding = _read_binding(path=imsi_file, value_col="imsi")
        imei_binding = _read_binding(path=imei_file, value_col="imei")
    with timed_stage("fill_subscriber_ids_sec", perf):
        geo = _fill_subscriber_ids(geo=geo, imsi_binding=imsi_binding, imei_binding=imei_binding)
    with timed_stage("prepare_timezones_sec", perf):
        zones = _prepare_zone_geoms(tz_df)
    with timed_stage("build_intervals_sec", perf):
        result = _build_intervals_for_day(geo=geo, bs=bs, zones=zones, time_key=report_date)

    out_file.parent.mkdir(parents=True, exist_ok=True)
    with timed_stage("write_sec", perf):
        result.to_parquet(out_file, compression=DEFAULT_PARQUET_COMPRESSION, index=False)

    stats: dict[str, Any] = {
        "report_date": report_date.isoformat(),
        "stg_geo_all_path": str(geo_all_file),
        "stg_bs_path": str(bs_file),
        "time_zones_path": str(tz_file),
        "stg_msisdn_imsi_path": str(imsi_file),
        "stg_msisdn_imei_path": str(imei_file),
        "output_path": str(out_file),
        "rows_read_geo_all": int(len(geo)),
        "rows_written": int(len(result)),
    }
    perf["elapsed_total_sec"] = round(time.perf_counter() - started, 4)
    append_command_metrics(command="build-stg-geo-intervals", metrics={**stats, **perf})
    logger.info("build-stg-geo-intervals completed: %s", stats)
    return {**stats, **perf}


def _empty_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_OUTPUT_COLUMNS))


def _read_binding(*, path: Path, value_col: str) -> pd.DataFrame:
    cols = ["msisdn", value_col, "valid_from", "valid_to"]
    if not path.exists():
        logger.warning("build-stg-geo-intervals: binding file not found: %s", path)
        return pd.DataFrame(columns=cols)
    try:
        binding = pd.read_parquet(path, columns=cols)
    except Exception:
        logger.exception("build-stg-geo-intervals: failed to read binding file: %s", path)
        return pd.DataFrame(columns=cols)
    binding["msisdn"] = normalize_msisdn(binding.get("msisdn"))
    if value_col == "imsi":
        binding[value_col] = normalize_imsi(binding.get(value_col))
    else:
        binding[value_col] = normalize_imei(binding.get(value_col))
    binding["valid_from"] = pd.to_datetime(binding.get("valid_from"), errors="coerce")
    binding["valid_to"] = pd.to_datetime(binding.get("valid_to"), errors="coerce")
    return binding.dropna(subset=["msisdn", value_col, "valid_from", "valid_to"]).reset_index(drop=True)


def _fill_subscriber_ids(*, geo: pd.DataFrame, imsi_binding: pd.DataFrame, imei_binding: pd.DataFrame) -> pd.DataFrame:
    if geo.empty:
        return geo
    work = geo.copy()
    work["msisdn"] = normalize_msisdn(work.get("msisdn"))
    work["imsi"] = normalize_imsi(work.get("imsi"))
    work["imei"] = normalize_imei(work.get("imei"))
    work["start_time_utc"] = pd.to_datetime(work.get("start_time_utc"), errors="coerce")
    work = _apply_binding_fill(work=work, binding=imsi_binding, value_col="imsi")
    work = _apply_binding_fill(work=work, binding=imei_binding, value_col="imei")
    return work


def _apply_binding_fill(*, work: pd.DataFrame, binding: pd.DataFrame, value_col: str) -> pd.DataFrame:
    if binding.empty:
        return work
    target_missing = work[value_col].isna()
    if not bool(target_missing.any()):
        return work
    candidates = work.loc[target_missing, ["msisdn", "start_time_utc"]].copy()
    if candidates.empty:
        return work
    candidates = candidates.reset_index().rename(columns={"index": "_row_id"})
    merged = candidates.merge(binding, on="msisdn", how="left")
    if merged.empty:
        return work
    in_interval = (
        merged["start_time_utc"].notna()
        & merged["valid_from"].notna()
        & merged["valid_to"].notna()
        & (merged["start_time_utc"] >= merged["valid_from"])
        & (merged["start_time_utc"] <= merged["valid_to"])
    )
    merged = merged.loc[in_interval].copy()
    if merged.empty:
        return work
    merged = merged.sort_values(["_row_id", "valid_from"], ascending=[True, False]).drop_duplicates(
        subset=["_row_id"], keep="first"
    )
    work.loc[merged["_row_id"].to_numpy(), value_col] = merged[value_col].to_numpy()
    return work


def _cgi_str(cgi: Any) -> str:
    if cgi is None or pd.isna(cgi):
        return ""
    try:
        return str(int(cgi))
    except (TypeError, ValueError):
        return ""


def _haversine_m(lat1: np.ndarray, lon1: np.ndarray, lat2: np.ndarray, lon2: np.ndarray) -> np.ndarray:
    rlat1 = np.radians(lat1)
    rlon1 = np.radians(lon1)
    rlat2 = np.radians(lat2)
    rlon2 = np.radians(lon2)
    dlat = np.sin((rlat2 - rlat1) / 2.0) ** 2
    dlon = np.cos(rlat1) * np.cos(rlat2) * np.sin((rlon2 - rlon1) / 2.0) ** 2
    a = dlat + dlon
    c = 2.0 * np.arcsin(np.minimum(1.0, np.sqrt(np.maximum(0.0, a))))
    return _EARTH_RADIUS_M * c


def _floor_5min_utc(series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(series, errors="coerce")
    origin = pd.Timestamp("1970-01-01 00:00:00")
    secs = ((dt - origin) / pd.Timedelta(seconds=1)).astype("int64")
    floored = (secs // _SECONDS_5MIN) * _SECONDS_5MIN
    return origin + pd.to_timedelta(floored, unit="s")


def _end_5min_from_ts_rounded(ts_rounded: pd.Series) -> pd.Series:
    origin = pd.Timestamp("1970-01-01 00:00:00")
    dt = pd.to_datetime(ts_rounded, errors="coerce")
    secs = ((dt - origin) / pd.Timedelta(seconds=1)).astype("int64")
    return origin + pd.to_timedelta(secs + _SECONDS_5MIN, unit="s")


def _prepare_zone_geoms(tz_df: pd.DataFrame) -> list[tuple[int, int, Any]]:
    out: list[tuple[int, int, Any]] = []
    for _, row in tz_df.iterrows():
        code = int(row["code"])
        tz_h = int(row["timezone"])
        geom_s = row.get("geometry")
        if geom_s is None or (isinstance(geom_s, float) and math.isnan(geom_s)):
            continue
        try:
            geom = wkt.loads(str(geom_s))
        except (GEOSException, ValueError, TypeError):
            continue
        out.append((code, tz_h, prep(geom)))
    out.sort(key=lambda t: t[0])
    return out


def _timezone_from_point(lon: float, lat: float, zones: list[tuple[int, int, Any]]) -> int | None:
    pt = Point(float(lon), float(lat))
    for _code, tz_h, prepared in zones:
        if prepared.covers(pt):
            return tz_h
    return None


def _aggregate_interval_group(key: tuple[Any, ...], df: pd.DataFrame) -> dict[str, Any]:
    imsi_v, imei_v, msisdn_v, st_v, en_v, bt = key
    w = df["total_events_count"].astype(float)
    sw = float(w.sum())
    cgis = sorted(df["cgi_str"].unique().tolist())
    lat_o = float((df["centroid_lat"].fillna(df["bs_lat"]) * w).sum() / sw) if sw else float("nan")
    lon_o = float((df["centroid_lon"].fillna(df["bs_lon"]) * w).sum() / sw) if sw else float("nan")
    lat_i = float((df["bs_lat"] * w).sum() / sw) if sw else float("nan")
    lon_i = float((df["bs_lon"] * w).sum() / sw) if sw else float("nan")
    sub_lat, sub_lon = (lat_o, lon_o) if bt == "o" else (lat_i, lon_i)

    okt = (
        df.groupby(["oktmo_code_1", "oktmo_code_2"], dropna=False)["total_events_count"]
        .sum()
        .reset_index()
        .sort_values(["total_events_count", "oktmo_code_1", "oktmo_code_2"], ascending=[False, False, False])
    )
    o1 = str(okt.iloc[0]["oktmo_code_1"]) if len(okt) else ""
    o2 = str(okt.iloc[0]["oktmo_code_2"]) if len(okt) else ""
    return {
        "imsi": imsi_v,
        "imei": imei_v,
        "msisdn": msisdn_v,
        "start_time_utc": pd.Timestamp(st_v),
        "end_time_utc": pd.Timestamp(en_v),
        "bs_type": bt,
        "cgi_list": cgis,
        "sub_lat": sub_lat,
        "sub_lon": sub_lon,
        "oktmo_code_1": o1,
        "oktmo_code_2": o2,
    }


def _cgi_lists_equal(a: Any, b: Any) -> bool:
    la = list(a) if a is not None else []
    lb = list(b) if b is not None else []
    return la == lb


def _rows_similar_for_merge(cur: pd.Series, prev: pd.Series | None) -> bool:
    if prev is None:
        return False
    if not _cgi_lists_equal(cur["cgi_list"], prev["cgi_list"]):
        return False
    if str(cur["bs_type"]) != str(prev["bs_type"]):
        return False
    if abs(float(cur["sub_lat"]) - float(prev["sub_lat"])) > _SUB_COORD_TOL:
        return False
    if abs(float(cur["sub_lon"]) - float(prev["sub_lon"])) > _SUB_COORD_TOL:
        return False
    return True


def _merge_adjacent_interval_rows(sr9: pd.DataFrame) -> list[dict[str, Any]]:
    rows_out: list[dict[str, Any]] = []
    for _, chunk in sr9.groupby(["msisdn", "imsi", "imei"], sort=False):
        chunk = chunk.sort_values(["start_time_utc", "end_time_utc"]).reset_index(drop=True)
        prev: pd.Series | None = None
        flags: list[int] = []
        for i in range(len(chunk)):
            r = chunk.iloc[i]
            if prev is None:
                sim = False
                gap_min = _MERGE_GAP_DAY_MIN + 1.0
            else:
                sim = _rows_similar_for_merge(r, prev)
                gap_min = (r["start_time_utc"] - prev["end_time_utc"]).total_seconds() / 60.0
            hour = r["start_time_utc"].hour
            thr = _MERGE_GAP_NIGHT_MIN if (hour <= 5 or hour == 23) else _MERGE_GAP_DAY_MIN
            flags.append(0 if (sim and gap_min <= thr) else 1)
            prev = r
        work = chunk.copy()
        work["_flag"] = flags
        work["_grp"] = work["_flag"].cumsum()
        for _, g in work.groupby("_grp", sort=False):
            first_s = g["start_time_utc"].min()
            last_e = g["end_time_utc"].max()
            r0 = g.iloc[0]
            rows_out.append(
                {
                    "msisdn": int(r0["msisdn"]),
                    "imsi": pd.to_numeric(r0["imsi"], errors="coerce"),
                    "imei": pd.to_numeric(r0["imei"], errors="coerce"),
                    "start_time_utc": first_s,
                    "end_time_utc": last_e,
                    "cgi_list": list(r0["cgi_list"]),
                    "sub_lat": float(r0["sub_lat"]),
                    "sub_lon": float(r0["sub_lon"]),
                    "bs_type": str(r0["bs_type"]),
                    "oktmo_code_1": str(r0["oktmo_code_1"]),
                    "oktmo_code_2": str(r0["oktmo_code_2"]),
                }
            )
    return rows_out


def _attach_timezone_and_partition(
    out: pd.DataFrame, b_small: pd.DataFrame, zones: list[tuple[int, int, Any]], time_key: date
) -> pd.DataFrame:
    tzs: list[int | None] = []
    for _, r in out.iterrows():
        t = _timezone_from_point(r["sub_lon"], r["sub_lat"], zones)
        if t is None and len(r["cgi_list"]):
            cg0 = str(r["cgi_list"][0])
            hit = b_small[b_small["cgi"] == cg0]
            if not hit.empty and pd.notna(hit.iloc[0].get("bs_timezone_hours")):
                t = int(hit.iloc[0]["bs_timezone_hours"])
        tzs.append(t)
    out = out.copy()
    out["timezone"] = pd.array(tzs, dtype="Int64")
    out["time_key"] = time_key
    return out


def _ensure_bs_columns(bs: pd.DataFrame) -> pd.DataFrame:
    b = bs.copy()
    for col in (
        "mapinfo_wkt_centroid_lon",
        "mapinfo_wkt_centroid_lat",
        "sector_wkt_centroid_lon",
        "sector_wkt_centroid_lat",
        "lon",
        "lat",
        "timezone",
    ):
        if col not in b.columns:
            b[col] = np.nan
    if "cgi" not in b.columns:
        mcc = pd.to_numeric(b.get("mcc"), errors="coerce")
        mnc = pd.to_numeric(b.get("mnc"), errors="coerce")
        lac = pd.to_numeric(b.get("lac"), errors="coerce")
        cell = pd.to_numeric(b.get("cell_id"), errors="coerce")
        b["cgi"] = (
            mcc.fillna(0).astype("Int64") * 10**13
            + mnc.fillna(0).astype("Int64") * 10**11
            + lac.fillna(0).astype("Int64") * 10**6
            + cell.fillna(0).astype("Int64")
        )
    b["cgi"] = b["cgi"].astype(str)
    b["centroid_lon"] = pd.to_numeric(
        b["mapinfo_wkt_centroid_lon"].fillna(b["sector_wkt_centroid_lon"]).fillna(b["lon"]),
        errors="coerce",
    )
    b["centroid_lat"] = pd.to_numeric(
        b["mapinfo_wkt_centroid_lat"].fillna(b["sector_wkt_centroid_lat"]).fillna(b["lat"]),
        errors="coerce",
    )
    small = b[["cgi", "centroid_lon", "centroid_lat", "timezone"]].copy()
    return small.rename(columns={"timezone": "bs_timezone_hours"})


def _build_intervals_for_day(geo: pd.DataFrame, bs: pd.DataFrame, zones: list[tuple[int, int, Any]], time_key: date) -> pd.DataFrame:
    if geo.empty:
        return _empty_df()

    g = geo.copy()
    g["cgi_str"] = g["cgi"].apply(_cgi_str)
    g["start_time_utc"] = pd.to_datetime(g["start_time_utc"], errors="coerce")
    g["end_time_utc"] = pd.to_datetime(g["end_time_utc"], errors="coerce")
    g.loc[g["end_time_utc"].isna(), "end_time_utc"] = g.loc[g["end_time_utc"].isna(), "start_time_utc"]

    req = ["msisdn", "start_time_utc", "cgi", "lat", "lon"]
    for c in req:
        g = g[g[c].notna()]
    g = g[g["cgi_str"] != ""]
    if g.empty:
        return _empty_df()

    g["event_count"] = pd.to_numeric(g["event_count"], errors="coerce").fillna(1).astype(int)
    g["event_count"] = g["event_count"].clip(lower=1)
    g["bs_lat"] = pd.to_numeric(g["lat"], errors="coerce")
    g["bs_lon"] = pd.to_numeric(g["lon"], errors="coerce")
    g["bs_type"] = g["bs_type"].astype(str).str.strip().str.lower().str[:1]
    g["oktmo_code_1"] = g["oktmo_code_1"].fillna("").astype(str)
    g["oktmo_code_2"] = g["oktmo_code_2"].fillna("").astype(str)
    g["ts_rounded"] = _floor_5min_utc(g["start_time_utc"])

    keys2 = [
        "imsi",
        "imei",
        "msisdn",
        "ts_rounded",
        "start_time_utc",
        "end_time_utc",
        "cgi_str",
        "bs_lat",
        "bs_lon",
        "bs_type",
    ]
    sr2 = (
        g.groupby(keys2, dropna=False)["event_count"]
        .sum()
        .reset_index()
        .rename(columns={"event_count": "total_events_count"})
    )
    idx_max = g.groupby(keys2, dropna=False)["event_count"].idxmax()
    okt_part = g.loc[idx_max, keys2 + ["oktmo_code_1", "oktmo_code_2"]].reset_index(drop=True)
    sr2 = sr2.merge(okt_part, on=keys2, how="left")

    sr2["_indoor"] = (sr2["bs_type"] != "o").astype(int)
    mx = sr2.groupby(["msisdn", "imsi", "imei", "ts_rounded"], dropna=False)["_indoor"].transform("max")
    sr2["is_indoor_in_interval"] = mx > 0
    sr3 = sr2[(~sr2["is_indoor_in_interval"]) | (sr2["is_indoor_in_interval"] & (sr2["bs_type"] != "o"))].copy()
    sr3 = sr3.drop(columns=["_indoor"], errors="ignore")

    ts_r = sr3["ts_rounded"]
    end5 = _end_5min_from_ts_rounded(ts_r)
    not_indoor = ~sr3["is_indoor_in_interval"]
    sr3["start_adj"] = np.where(not_indoor, ts_r, sr3["start_time_utc"])
    sr3["end_adj"] = np.where(not_indoor, end5, sr3["end_time_utc"])
    sr3["start_time_utc"] = sr3["start_adj"]
    sr3["end_time_utc"] = sr3["end_adj"]
    sr4_1 = sr3.drop(columns=["start_adj", "end_adj"], errors="ignore").copy()
    sr4_2 = sr4_1.rename(
        columns={
            "bs_type": "bs_type_top",
            "cgi_str": "cgi_top",
            "bs_lat": "bs_lat_top",
            "bs_lon": "bs_lon_top",
        }
    )

    o_only = sr4_1[sr4_1["bs_type"] == "o"].copy()
    o_top = sr4_2[sr4_2["bs_type_top"] == "o"].copy()
    merge_keys = ["imsi", "imei", "msisdn", "start_time_utc", "end_time_utc"]
    if not o_only.empty and not o_top.empty:
        o_top_m = o_top.rename(columns={"total_events_count": "total_events_count_top"})
        right_cols = merge_keys + ["cgi_top", "bs_lat_top", "bs_lon_top", "bs_type_top", "total_events_count_top"]
        o_top_m = o_top_m[[c for c in right_cols if c in o_top_m.columns]]
        sr5 = o_only.merge(o_top_m, on=merge_keys, how="inner")
    else:
        sr5 = pd.DataFrame()

    if not sr5.empty:
        sr5["dist"] = _haversine_m(
            sr5["bs_lat"].to_numpy(),
            sr5["bs_lon"].to_numpy(),
            sr5["bs_lat_top"].to_numpy(),
            sr5["bs_lon_top"].to_numpy(),
        )
        both_diff = (sr5["bs_lat"] != sr5["bs_lat_top"]) & (sr5["bs_lon"] != sr5["bs_lon_top"])
        sr5["_dist_for_mean"] = np.where(both_diff, sr5["dist"], np.nan)
        grp6 = [
            "imsi",
            "imei",
            "msisdn",
            "start_time_utc",
            "end_time_utc",
            "cgi_str",
            "bs_type",
            "bs_lat",
            "bs_lon",
            "total_events_count",
            "oktmo_code_1",
            "oktmo_code_2",
        ]
        sr6 = sr5.groupby(grp6, dropna=False).agg(dist_mean=("_dist_for_mean", "mean")).reset_index()
        part_keys = ["imsi", "imei", "msisdn", "start_time_utc", "end_time_utc"]
        sr6["rank_lo"] = sr6.groupby(part_keys)["dist_mean"].rank(method="dense", ascending=True)
        sr6["rank_hi"] = sr6.groupby(part_keys)["dist_mean"].rank(method="dense", ascending=False)
        sr6["count_d"] = sr6["rank_lo"] + sr6["rank_hi"] - 1.0
        dm = sr6["dist_mean"].fillna(0.0)
        thr = np.where(sr6["count_d"] == 1.0, _DIST_THRESHOLD_SINGLE_BS_M, _DIST_THRESHOLD_MULTI_BS_M)
        sr6_kept = sr6[dm < thr].drop(columns=["rank_lo", "rank_hi", "count_d", "dist_mean"], errors="ignore")
        sr7_o = sr5.merge(sr6_kept, on=grp6, how="inner")
        drop_cols = [c for c in sr7_o.columns if c.endswith("_top") or c in ("dist", "_dist_for_mean")]
        sr7_o = sr7_o.drop(columns=drop_cols, errors="ignore")
        sr7_o = sr7_o.drop_duplicates(subset=grp6)
    else:
        sr7_o = pd.DataFrame(columns=list(sr4_1.columns))

    non_o = sr4_1[sr4_1["bs_type"] != "o"].copy()
    base_cols = [
        "imsi",
        "imei",
        "msisdn",
        "start_time_utc",
        "end_time_utc",
        "cgi_str",
        "bs_type",
        "bs_lat",
        "bs_lon",
        "total_events_count",
        "oktmo_code_1",
        "oktmo_code_2",
    ]
    non_o = non_o[[c for c in base_cols if c in non_o.columns]]
    if sr7_o.empty:
        sr7 = non_o.copy()
    else:
        sr7_o_sub = sr7_o[[c for c in base_cols if c in sr7_o.columns]]
        sr7 = pd.concat([sr7_o_sub, non_o], ignore_index=True)
    if sr7.empty:
        return _empty_df()

    b_small = _ensure_bs_columns(bs)
    sr7 = sr7.merge(b_small, left_on="cgi_str", right_on="cgi", how="left").drop(columns=["cgi"], errors="ignore")
    sr7.loc[sr7["bs_type"] == "o", "bs_lat"] = sr7.loc[sr7["bs_type"] == "o", "centroid_lat"].fillna(sr7["bs_lat"])
    sr7.loc[sr7["bs_type"] == "o", "bs_lon"] = sr7.loc[sr7["bs_type"] == "o", "centroid_lon"].fillna(sr7["bs_lon"])

    g9keys = ["imsi", "imei", "msisdn", "start_time_utc", "end_time_utc", "bs_type"]
    sr9_rows = [_aggregate_interval_group(k, g) for k, g in sr7.groupby(g9keys, sort=False, dropna=False)]
    sr9 = pd.DataFrame(sr9_rows)
    sr9 = sr9.sort_values(["msisdn", "imsi", "imei", "start_time_utc", "end_time_utc"]).reset_index(drop=True)

    rows_out = _merge_adjacent_interval_rows(sr9)
    out = pd.DataFrame(rows_out)
    if out.empty:
        return _empty_df()
    out = _attach_timezone_and_partition(out=out, b_small=b_small, zones=zones, time_key=time_key)
    return out[list(_OUTPUT_COLUMNS)]
