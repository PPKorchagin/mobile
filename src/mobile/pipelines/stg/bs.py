from __future__ import annotations

import json
import logging
import math
import time
from pathlib import Path
from typing import Any

import h3
import numpy as np
import pandas as pd
from shapely import ops, wkt
from shapely.geometry import MultiPoint, MultiPolygon, Point, Polygon
from shapely.ops import unary_union
from shapely.prepared import prep

from mobile.cli_defaults import DEFAULT_PARQUET_COMPRESSION
from mobile.command_timing import append_command_metrics, timed_stage
from mobile.project_paths import (
    DEFAULT_BS_LAYOUT,
    DEFAULT_SRC_BS_SCHEMA_PATH,
    DEFAULT_FCT_BS_SCHEMA_PATH,
    DEFAULT_DIM_OKTMO_OUTPUT_PATH,
    DEFAULT_DIM_TIME_ZONES_OUTPUT_PATH,
    resolve_project_path,
    fct_bs_output_path,
)

logger = logging.getLogger(__name__)

STG_BS_TABLE = "fct_bs"
FCT_BS_FIELDS: list[dict[str, Any]] = []
SRC_BS_FIELD_TYPES: dict[str, str] = {}

SRC_BS_READ_COLUMNS = (
    "date_on",
    "date_off",
    "mcc",
    "mnc",
    "lac",
    "cell",
    "generation",
    "frequency",
    "coord_x",
    "coord_y",
    "bs_type",
    "location",
    "description",
    "azimuth",
    "thickness",
    "subject",
    "avtocod",
    "power",
    "height",
    "amplification",
    "tilt",
    "el_tilt",
    "mech_tilt",
    "rad_class",
)

_NUMERIC_TYPES = frozenset({"int", "smallint", "long", "float"})
_SRC_BS_NUMERIC_RANGES: dict[str, tuple[float, float]] = {
    "azimuth": (-1.0, 998.0),
    "height": (-50.0, 240.0),
    "tilt": (-90.0, 360.0),
    "el_tilt": (-359.0, 46.0),
    "mech_tilt": (-240.0, 25.0),
    "thickness": (-360.0, 360.0),
    "frequency": (-1.0, 1e10),
    "power": (-14.0, 2040.0),
    "amplification": (-66.0, 55.0),
}
_SRC_BS_INT_RANGES: dict[str, tuple[int, int]] = {}

_OPEN_END_TS = pd.Timestamp("2262-04-11 00:00:00")
_KM_IN_DEG = 111.32
_M_PER_DEG_LAT = 110_540.0
_M_PER_DEG_LON_AT_EQUATOR = 111_320.0


def _load_schema_contract(schema_path: Path) -> None:
    global STG_BS_TABLE, FCT_BS_FIELDS
    with schema_path.open(encoding="utf-8") as file:
        cfg = json.load(file)
    STG_BS_TABLE = str(cfg.get("table", STG_BS_TABLE))
    FCT_BS_FIELDS = list(cfg.get("fields", FCT_BS_FIELDS))


def _load_src_bs_schema(schema_path: Path) -> None:
    global SRC_BS_FIELD_TYPES
    with schema_path.open(encoding="utf-8") as file:
        cfg = json.load(file)
    SRC_BS_FIELD_TYPES = {
        str(field["name"]): str(field["type"]) for field in cfg.get("fields", [])
    }


_load_schema_contract(DEFAULT_FCT_BS_SCHEMA_PATH)
_load_src_bs_schema(DEFAULT_SRC_BS_SCHEMA_PATH)


def run_build(
    *,
    src_bs_path: str | Path | None = None,
    oktmo_path: str | Path | None = None,
    output_path: str | Path | None = None,
    time_zones_path: str | Path | None = None,
) -> dict[str, Any]:
    """Собрать ``fct_bs`` из полного ``src_bs`` с SCD-историей изменений."""
    fields = FCT_BS_FIELDS
    out = (
        resolve_project_path(output_path)
        if output_path is not None
        else fct_bs_output_path()
    )
    src_path = resolve_project_path(src_bs_path or DEFAULT_BS_LAYOUT)
    oktmo_file = resolve_project_path(oktmo_path or DEFAULT_DIM_OKTMO_OUTPUT_PATH)
    tz_path = resolve_project_path(time_zones_path or DEFAULT_DIM_TIME_ZONES_OUTPUT_PATH)

    if not src_path.exists():
        raise FileNotFoundError(f"SRC BS parquet not found: {src_path}")
    if not oktmo_file.exists():
        raise FileNotFoundError(f"OKTMO parquet not found: {oktmo_file}")
    if not tz_path.exists():
        raise FileNotFoundError(f"Time zones parquet not found: {tz_path}")

    perf_metrics: dict[str, Any] = {}
    started = time.perf_counter()
    effective_ts = pd.Timestamp.now().floor("s")
    logger.info(
        "Reading source BS dataset: %s (full snapshot; effective_ts=%s)",
        src_path,
        effective_ts.isoformat(),
    )
    with timed_stage("read_inputs_sec", perf_metrics):
        src = pd.read_parquet(src_path)
        row_count_total = int(len(src))
        oktmo = pd.read_parquet(oktmo_file)
        time_zones = pd.read_parquet(tz_path)

    with timed_stage("prepare_src_bs_sec", perf_metrics):
        src, src_prepare = _prepare_src_bs(src)

    with timed_stage("build_snapshot_sec", perf_metrics):
        source_snapshot = _build_source_snapshot(src, oktmo, time_zones)
    if source_snapshot.empty:
        final_df = _empty_frame(fields)
    else:
        n_cols = [f["name"] for f in fields if f["name"] not in {"date_on", "date_off"}]
        missing = [name for name in n_cols if name not in source_snapshot.columns]
        if missing:
            raise ValueError(f"Source snapshot missing STG BS fields: {missing}")
        final_df = source_snapshot[n_cols].copy()
        final_df["date_on"] = effective_ts
        final_df["date_off"] = _OPEN_END_TS
    final_df = _coerce_types(final_df, fields)
    final_df = _select_fields(final_df, fields)

    with timed_stage("write_parquet_sec", perf_metrics):
        final_df = _merge_history(
            existing_path=out,
            current_snapshot=final_df,
            effective_ts=effective_ts,
            fields=fields,
        )
        out.parent.mkdir(parents=True, exist_ok=True)
        final_df.to_parquet(out, compression=DEFAULT_PARQUET_COMPRESSION, index=False)

    stats = {
        "command": "build-fct-bs",
        "table": STG_BS_TABLE,
        "output_path": str(out),
        "src_bs_path": str(src_path),
        "oktmo_path": str(oktmo_file),
        "time_zones_path": str(tz_path),
        "row_count": int(len(final_df)),
        "source_row_count_total": row_count_total,
        "source_active_rows": int(len(source_snapshot)),
        "effective_ts": effective_ts.isoformat(),
        "src_bs_prepare": src_prepare,
    }
    perf_metrics["elapsed_total_sec"] = round(time.perf_counter() - started, 4)
    append_command_metrics(command="build-fct-bs", metrics={**stats, **perf_metrics})
    logger.info("build-fct-bs completed: %s", stats)
    return {**stats, **perf_metrics}


def _non_empty_mask(series: pd.Series) -> pd.Series:
    if pd.api.types.is_string_dtype(series) or series.dtype == object:
        return series.notna() & series.astype("string").str.strip().ne("")
    return series.notna()


def _processable_src_bs_mask(data: pd.DataFrame) -> pd.Series:
    mcc = pd.to_numeric(data["mcc"], errors="coerce")
    mnc = pd.to_numeric(data["mnc"], errors="coerce")
    lac = pd.to_numeric(data["lac"], errors="coerce")
    cell = pd.to_numeric(data["cell"], errors="coerce")
    return mcc.notna() & mnc.notna() & lac.notna() & cell.notna()


def _build_src_bs_row_keys(data: pd.DataFrame) -> pd.Series:
    def _part(name: str) -> pd.Series:
        if name not in data.columns:
            return pd.Series("<na>", index=data.index, dtype="string")
        return data[name].astype("string").fillna("<na>")

    return (
        "cgi="
        + _part("mcc")
        + "-"
        + _part("mnc")
        + "-"
        + _part("lac")
        + "-"
        + _part("cell")
        + " date_on="
        + _part("date_on")
        + " date_off="
        + _part("date_off")
    )


def _append_src_bs_row_error(
    errors: list[dict[str, Any]],
    *,
    row_key: str,
    field: str,
    value: Any,
    reason: str,
) -> None:
    errors.append(
        {
            "row_key": row_key,
            "field": field,
            "value": None if value is None or pd.isna(value) else str(value),
            "reason": reason,
        }
    )


def _clip_numeric_value(value: float, field: str) -> float:
    bounds = _SRC_BS_NUMERIC_RANGES.get(field)
    if bounds is None:
        int_bounds = _SRC_BS_INT_RANGES.get(field)
        if int_bounds is None:
            return value
        lo, hi = int_bounds
    else:
        lo, hi = bounds
    return float(min(hi, max(lo, value)))


def _normalize_src_bs_numeric_column(
    series: pd.Series,
    *,
    field: str,
    row_keys: pd.Series,
    errors: list[dict[str, Any]],
) -> pd.Series:
    raw = series.copy()
    non_empty = _non_empty_mask(raw)
    parsed = pd.to_numeric(raw, errors="coerce")
    failed = non_empty & parsed.isna()
    for idx in raw.index[failed]:
        _append_src_bs_row_error(
            errors,
            row_key=str(row_keys.loc[idx]),
            field=field,
            value=raw.loc[idx],
            reason="non_numeric",
        )

    out = parsed.copy()
    if field in _SRC_BS_NUMERIC_RANGES or field in _SRC_BS_INT_RANGES:
        clipped = out.map(lambda v: _clip_numeric_value(float(v), field) if pd.notna(v) else v)
        out = pd.to_numeric(clipped, errors="coerce")

    if field == "coord_x":
        out = out.map(lambda v: _normalize_lon(v) if pd.notna(v) else v)
        invalid_lon = out.notna() & ~out.between(-180.0, 180.0)
        for idx in raw.index[invalid_lon]:
            _append_src_bs_row_error(
                errors,
                row_key=str(row_keys.loc[idx]),
                field=field,
                value=raw.loc[idx],
                reason="invalid_lon",
            )
        out = out.where(out.isna() | out.between(-180.0, 180.0))
    elif field == "coord_y":
        invalid_lat = parsed.notna() & ~parsed.between(-90.0, 90.0)
        for idx in raw.index[invalid_lat]:
            _append_src_bs_row_error(
                errors,
                row_key=str(row_keys.loc[idx]),
                field=field,
                value=raw.loc[idx],
                reason="invalid_lat",
            )
        out = out.where(out.isna() | out.between(-90.0, 90.0))
    elif field == "mnc":
        negative = parsed.notna() & (parsed < 0)
        for idx in raw.index[negative]:
            _append_src_bs_row_error(
                errors,
                row_key=str(row_keys.loc[idx]),
                field=field,
                value=raw.loc[idx],
                reason="negative_mnc",
            )
        out = out.where(out.isna() | (out >= 0))
    elif field == "azimuth":
        out = out.map(lambda v: _normalize_azimuth(v) if pd.notna(v) else v)
    elif field == "thickness":
        out = out.map(lambda v: _normalize_sector_angle(v) if pd.notna(v) else v)

    return out


def _normalize_src_bs_timestamp_column(
    series: pd.Series,
    *,
    field: str,
    row_keys: pd.Series,
    errors: list[dict[str, Any]],
) -> pd.Series:
    raw = series.copy()
    non_empty = _non_empty_mask(raw)
    parsed = pd.to_datetime(raw, errors="coerce")
    failed = non_empty & parsed.isna()
    for idx in raw.index[failed]:
        _append_src_bs_row_error(
            errors,
            row_key=str(row_keys.loc[idx]),
            field=field,
            value=raw.loc[idx],
            reason="non_timestamp",
        )
    return parsed


def _normalize_src_bs_string_column(series: pd.Series) -> pd.Series:
    return series.astype("string").str.strip().replace("", pd.NA)


def _log_src_bs_prepare_errors(errors: list[dict[str, Any]]) -> None:
    if not errors:
        return
    for err in errors:
        logger.warning(
            "src_bs prepare: %s field=%s value=%r reason=%s",
            err["row_key"],
            err["field"],
            err["value"],
            err["reason"],
        )


def _prepare_src_bs(data: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Нормализовать поля ``src_bs``; ошибки строк логируются, job не падает."""
    if data.empty:
        return data, {"row_count": 0, "status": "ok", "skipped": True}

    out = data.copy()
    errors: list[dict[str, Any]] = []
    metrics: dict[str, Any] = {"row_count": int(len(out))}

    missing = [col for col in SRC_BS_READ_COLUMNS if col not in out.columns]
    metrics["missing_columns"] = missing
    for col in missing:
        logger.error("src_bs prepare: missing column %s; filling with NA", col)
        out[col] = pd.NA

    row_keys = _build_src_bs_row_keys(out)

    for col in SRC_BS_READ_COLUMNS:
        ftype = SRC_BS_FIELD_TYPES.get(col, "string")
        if ftype in _NUMERIC_TYPES:
            out[col] = _normalize_src_bs_numeric_column(
                out[col],
                field=col,
                row_keys=row_keys,
                errors=errors,
            )
        elif ftype == "timestamp":
            out[col] = _normalize_src_bs_timestamp_column(
                out[col],
                field=col,
                row_keys=row_keys,
                errors=errors,
            )
        else:
            out[col] = _normalize_src_bs_string_column(out[col])

    date_on = pd.to_datetime(out["date_on"], errors="coerce")
    date_off = pd.to_datetime(out["date_off"], errors="coerce")
    invalid_order = date_on.notna() & date_off.notna() & (date_off < date_on)
    for idx in out.index[invalid_order]:
        _append_src_bs_row_error(
            errors,
            row_key=str(row_keys.loc[idx]),
            field="date_off",
            value=out.loc[idx, "date_off"],
            reason="date_off_before_date_on",
        )

    _log_src_bs_prepare_errors(errors)

    processable = _processable_src_bs_mask(out)
    metrics["processable_row_count"] = int(processable.sum())
    metrics["error_count"] = len(errors)
    metrics["error_samples"] = errors
    metrics["status"] = "ok_with_errors" if errors else "ok"
    logger.info(
        "src_bs prepare completed: rows=%s processable=%s errors=%s",
        metrics["row_count"],
        metrics["processable_row_count"],
        metrics["error_count"],
    )
    return out, metrics


def _build_source_snapshot(
    src: pd.DataFrame,
    oktmo: pd.DataFrame,
    time_zones: pd.DataFrame,
) -> pd.DataFrame:
    work = src.copy()
    if work.empty:
        return pd.DataFrame()

    work["mcc"] = pd.to_numeric(work.get("mcc"), errors="coerce").astype("Int16")
    work["mnc"] = pd.to_numeric(work.get("mnc"), errors="coerce").astype("Int16")
    work["lac"] = pd.to_numeric(work.get("lac"), errors="coerce").astype("Int64")
    work["cell_id"] = pd.to_numeric(work.get("cell"), errors="coerce").astype("Int64")
    work["telecomstandard"] = work.get("generation").map(_map_generation)
    work["frequency"] = pd.to_numeric(work.get("frequency"), errors="coerce").round().astype("Int32")

    work["lon_original"] = pd.to_numeric(work.get("coord_x"), errors="coerce").map(_normalize_lon).round(5)
    work["lat_original"] = pd.to_numeric(work.get("coord_y"), errors="coerce").round(5)
    work["lon"] = work["lon_original"]
    work["lat"] = work["lat_original"]

    work["bs_type"] = work.apply(
        lambda row: _map_bs_type(
            bs_type=row.get("bs_type"),
            location=row.get("location"),
            description=row.get("description"),
        ),
        axis=1,
    )
    work["sector_azimuth"] = pd.to_numeric(work.get("azimuth"), errors="coerce").map(_normalize_azimuth)
    work["sector_angle"] = pd.to_numeric(work.get("thickness"), errors="coerce").map(_normalize_sector_angle)
    work["sector_radius"] = work.apply(
        lambda row: _derive_sector_radius(row.get("bs_type")),
        axis=1,
    ).astype("float64")

    work["position_code"] = work.get("avtocod").astype("string")
    work["h3"] = work.apply(lambda row: _to_h3(row.get("lat"), row.get("lon")), axis=1)

    _BS_KEY_COLS = ["mcc", "mnc", "lac", "cell_id"]
    work = work.sort_values([*_BS_KEY_COLS, "date_on", "date_off"], ascending=[True, True, True, True, False, False])
    work = work.drop_duplicates(subset=_BS_KEY_COLS, keep="first")
    work = work[work["mcc"].notna() & work["mnc"].notna() & work["lac"].notna() & work["cell_id"].notna()].copy()
    if work.empty:
        return pd.DataFrame()

    tz_values = _map_timezones(work, time_zones)
    work = pd.concat([work.reset_index(drop=True), tz_values.reset_index(drop=True)], axis=1)

    oktmo_values = _map_oktmo(work, oktmo)
    work = pd.concat([work.reset_index(drop=True), oktmo_values.reset_index(drop=True)], axis=1)

    sector_metrics = work.apply(
        lambda row: _sector_row_metrics(
            lon=row.get("lon"),
            lat=row.get("lat"),
            radius_km=row.get("sector_radius"),
            azimuth=row.get("sector_azimuth"),
            angle=row.get("sector_angle"),
            bs_type=row.get("bs_type"),
        ),
        axis=1,
        result_type="expand",
    )
    sector_metrics.columns = [
        "sector_wkt",
        "sector_wkt_area",
        "sector_wkt_centroid_lon",
        "sector_wkt_centroid_lat",
    ]
    work = pd.concat([work.reset_index(drop=True), sector_metrics.reset_index(drop=True)], axis=1)
    radio_cols = compute_mapinfo_radio_columns(work)
    work = pd.concat([work.reset_index(drop=True), radio_cols.reset_index(drop=True)], axis=1)
    _attach_mapinfo_best_cells(work)
    return work


def _map_oktmo(work: pd.DataFrame, oktmo: pd.DataFrame) -> pd.DataFrame:
    level1 = oktmo[oktmo["level"] == 1].copy()
    level2 = oktmo[oktmo["level"] == 2].copy()
    level1["name"] = level1["name"].astype("string").str.strip()
    level1["code"] = level1["code"].astype("string").str.strip()
    level2["code"] = level2["code"].astype("string").str.strip()
    level2["parent_code"] = level2["parent_code"].astype("string").str.strip()

    lvl1_geoms: list[dict[str, Any]] = []
    for _, row in level1.iterrows():
        try:
            geom = wkt.loads(str(row["WKT"]))
        except Exception:
            continue
        if geom.is_empty or not geom.is_valid:
            continue
        lvl1_geoms.append(
            {
                "code": row["code"],
                "name": row["name"],
                "bounds": geom.bounds,
                "prepared": prep(geom),
            }
        )

    lvl2_geoms: list[dict[str, Any]] = []
    for _, row in level2.iterrows():
        try:
            geom = wkt.loads(str(row["WKT"]))
        except Exception:
            continue
        if geom.is_empty or not geom.is_valid:
            continue
        lvl2_geoms.append(
            {
                "code": row["code"],
                "parent_code": row["parent_code"],
                "bounds": geom.bounds,
                "prepared": prep(geom),
            }
        )

    lvl1_name_map = {
        str(name).strip().lower(): (str(code).strip(), str(name).strip())
        for name, code in zip(level1["name"], level1["code"])
        if pd.notna(name) and pd.notna(code)
    }
    lvl1_name_by_code = {
        str(code).strip(): str(name).strip()
        for name, code in zip(level1["name"], level1["code"])
        if pd.notna(name) and pd.notna(code)
    }

    result = {
        "oktmo_code_1": [],
        "oktmo_code_2": [],
        "oktmo_region_name": [],
    }
    for _, row in work.iterrows():
        lon = row.get("lon")
        lat = row.get("lat")
        subject = _safe_str(row.get("subject")).strip().lower()
        point = None
        if pd.notna(lon) and pd.notna(lat):
            point = Point(float(lon), float(lat))

        code_2 = None
        parent_from_lvl2 = None
        if point is not None:
            for item in lvl2_geoms:
                if _point_in_bounds(point, item["bounds"]) and item["prepared"].covers(point):
                    code_2 = item["code"]
                    parent_from_lvl2 = item["parent_code"]
                    break

        code_1 = None
        name_1 = None
        if parent_from_lvl2:
            code_1 = str(parent_from_lvl2).strip()
            name_1 = lvl1_name_by_code.get(code_1)
        else:
            by_subject = lvl1_name_map.get(subject)
            if by_subject is not None:
                code_1, name_1 = by_subject
            elif point is not None:
                for item in lvl1_geoms:
                    if _point_in_bounds(point, item["bounds"]) and item["prepared"].covers(point):
                        code_1 = item["code"]
                        name_1 = item["name"]
                        break

        result["oktmo_code_1"].append(code_1)
        result["oktmo_code_2"].append(code_2)
        result["oktmo_region_name"].append(name_1)

    return pd.DataFrame(result)


def _map_timezones(work: pd.DataFrame, time_zones: pd.DataFrame) -> pd.DataFrame:
    tz = time_zones.copy()
    tz["timezone"] = pd.to_numeric(tz.get("timezone"), errors="coerce").astype("Int32")

    tz_geoms: list[dict[str, Any]] = []
    for _, row in tz.iterrows():
        try:
            geom = wkt.loads(str(row["geometry"]))
        except Exception:
            continue
        if geom.is_empty or not geom.is_valid:
            continue
        tz_geoms.append(
            {
                "timezone": row.get("timezone"),
                "bounds": geom.bounds,
                "prepared": prep(geom),
            }
        )

    result = {"timezone": []}
    for _, row in work.iterrows():
        timezone = pd.NA
        lon = row.get("lon")
        lat = row.get("lat")
        if pd.notna(lon) and pd.notna(lat):
            point = Point(float(lon), float(lat))
            for item in tz_geoms:
                if _point_in_bounds(point, item["bounds"]) and item["prepared"].covers(point):
                    timezone = item["timezone"]
                    break
        result["timezone"].append(timezone)
    return pd.DataFrame(result)


def _point_in_bounds(point: Point, bounds: tuple[float, float, float, float]) -> bool:
    min_x, min_y, max_x, max_y = bounds
    return min_x <= point.x <= max_x and min_y <= point.y <= max_y


def _map_generation(value: Any) -> str:
    s = str(value).strip().upper()
    if s in {"2G", "3G", "4G"}:
        return s
    if s in {"LTE", "5G"}:
        return "4G"
    return "4G"


def _map_bs_type(bs_type: Any, location: Any, description: Any) -> str:
    text = f"{_safe_str(bs_type)} {_safe_str(location)} {_safe_str(description)}".lower()
    if "metro" in text or "underground" in text:
        return "m"
    if "indoor" in text or "ind " in text:
        return "i"
    if "out/ind" in text or "indoor-outdoor" in text:
        return "x"
    if "femto" in text or "pico" in text:
        return "f"
    return "o"


def _normalize_lon(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    lon = float(value)
    if lon > 180:
        lon -= 360
    if lon < -180:
        lon += 360
    return lon


def _normalize_azimuth(value: Any) -> float:
    if value is None or pd.isna(value):
        return 0.0
    az = float(value) % 360.0
    return round(az, 5)


def _normalize_sector_angle(value: Any) -> float:
    if value is None or pd.isna(value):
        return 120.0
    angle = abs(float(value))
    if angle < 1:
        return 120.0
    return float(min(angle, 359.0))


def _derive_sector_radius(bs_type: Any) -> float:
    if bs_type == "m":
        return 0.2
    if bs_type == "i":
        return 0.1
    if bs_type == "f":
        return 0.05
    return 5.0


def _to_h3(lat: Any, lon: Any) -> str | None:
    if lat is None or lon is None or pd.isna(lat) or pd.isna(lon):
        return None
    try:
        return h3.latlng_to_cell(float(lat), float(lon), 15)
    except Exception:
        return None


def _sector_row_metrics(
    lon: Any,
    lat: Any,
    radius_km: Any,
    azimuth: Any,
    angle: Any,
    bs_type: Any,
) -> tuple[str | None, float | None, float | None, float | None]:
    if lon is None or lat is None or pd.isna(lon) or pd.isna(lat):
        return (None, None, None, None)
    lon_f = float(lon)
    lat_f = float(lat)
    radius = max(0.01, float(radius_km) if pd.notna(radius_km) else 5.0)
    az = float(azimuth) if pd.notna(azimuth) else 0.0
    ang = float(angle) if pd.notna(angle) else 120.0

    sector_geom = _build_sector_geometry(lon_f, lat_f, radius, az, ang, str(bs_type))
    sector_wkt = sector_geom.wkt if sector_geom is not None else None
    sector_area = _geometry_area_km2(sector_geom)
    sector_centroid = sector_geom.centroid if sector_geom is not None else None
    sector_centroid_lon = round(float(sector_centroid.x), 5) if sector_centroid is not None else None
    sector_centroid_lat = round(float(sector_centroid.y), 5) if sector_centroid is not None else None
    return (sector_wkt, sector_area, sector_centroid_lon, sector_centroid_lat)


def _lonlat_to_xy_m(lon: np.ndarray, lat: np.ndarray, ref_lon: float, ref_lat: float) -> tuple[np.ndarray, np.ndarray]:
    cos_lat = max(0.2, math.cos(math.radians(ref_lat)))
    scale_x = _M_PER_DEG_LON_AT_EQUATOR * cos_lat
    x = (lon - ref_lon) * scale_x
    y = (lat - ref_lat) * _M_PER_DEG_LAT
    return x, y


def _xy_m_to_lonlat(x: float, y: float, ref_lon: float, ref_lat: float) -> tuple[float, float]:
    cos_lat = max(0.2, math.cos(math.radians(ref_lat)))
    scale_x = _M_PER_DEG_LON_AT_EQUATOR * cos_lat
    lon = ref_lon + float(x) / scale_x
    lat = ref_lat + float(y) / _M_PER_DEG_LAT
    return lon, lat


def _polygon_xy_to_lonlat(poly: Polygon | MultiPolygon, ref_lon: float, ref_lat: float) -> Polygon | MultiPolygon | None:
    if poly is None or poly.is_empty:
        return None
    try:
        if isinstance(poly, MultiPolygon):
            parts = [_polygon_xy_to_lonlat(g, ref_lon, ref_lat) for g in poly.geoms]
            parts = [g for g in parts if g is not None and not g.is_empty]
            if not parts:
                return None
            if len(parts) == 1:
                out = parts[0]
            else:
                out = MultiPolygon(parts)
        else:
            ext = [_xy_m_to_lonlat(x, y, ref_lon, ref_lat) for x, y in poly.exterior.coords]
            holes = []
            for ring in poly.interiors:
                holes.append([_xy_m_to_lonlat(x, y, ref_lon, ref_lat) for x, y in ring.coords])
            out = Polygon(ext, holes if holes else None)
        if not out.is_valid:
            out = out.buffer(0)
        if out.is_empty:
            return None
        return out
    except Exception:
        return None


def _jitter_duplicate_xy(
    xs: np.ndarray,
    ys: np.ndarray,
    keys: list[str],
    eps: float = 1.5,
) -> tuple[np.ndarray, np.ndarray]:
    xs = np.asarray(xs, dtype=float).copy()
    ys = np.asarray(ys, dtype=float).copy()
    rounded: dict[tuple[int, int], list[int]] = {}
    for i in range(len(xs)):
        key = (int(round(xs[i] * 1e6)), int(round(ys[i] * 1e6)))
        rounded.setdefault(key, []).append(i)
    for idxs in rounded.values():
        if len(idxs) <= 1:
            continue
        for k, i in enumerate(idxs):
            h = (hash(keys[i]) % 1000) / 1_000_000.0
            xs[i] += (k - len(idxs) // 2) * eps + h
            ys[i] -= h * eps * 0.7
    return xs, ys


def _pick_voronoi_cell_for_site(
    site_x: float,
    site_y: float,
    polys: list[Polygon],
) -> Polygon | None:
    pt = Point(site_x, site_y)
    for poly in polys:
        if poly.covers(pt) or poly.contains(pt):
            return poly
    if not polys:
        return None
    return min(polys, key=lambda p: p.distance(pt))


def _voronoi_cells_xy_for_group(xs: np.ndarray, ys: np.ndarray, reach_m: np.ndarray) -> list[Polygon | MultiPolygon]:
    """Одна диаграмма Вороного на группу; ячейки в метрах (локальная плоскость). reach_m — валидированный радиус MAPINFO по БС."""
    n = len(xs)
    if n == 0:
        return []
    if n == 1:
        sx, sy = float(xs[0]), float(ys[0])
        r_m = max(120.0, float(reach_m[0]))
        return [Point(sx, sy).buffer(r_m)]
    pts = MultiPoint([(float(xs[i]), float(ys[i])) for i in range(n)])
    hull = pts.convex_hull
    if hull.is_empty:
        return [Point(float(xs[i]), float(ys[i])).buffer(max(120.0, float(reach_m[i]))) for i in range(n)]
    minx, miny, maxx, maxy = hull.bounds
    span = max(maxx - minx, maxy - miny, 500.0)
    margin = max(span * 0.12, 2500.0)
    env = hull.buffer(margin)
    try:
        diagram = ops.voronoi_diagram(pts, envelope=env, tolerance=0.0)
    except Exception:
        return [Point(float(xs[i]), float(ys[i])).buffer(max(120.0, float(reach_m[i]))) for i in range(n)]
    polys = [g for g in diagram.geoms if g.geom_type == "Polygon" and not g.is_empty]
    if not polys:
        return [Point(float(xs[i]), float(ys[i])).buffer(max(120.0, float(reach_m[i]))) for i in range(n)]
    out: list[Polygon | MultiPolygon] = []
    for i in range(n):
        cell = _pick_voronoi_cell_for_site(float(xs[i]), float(ys[i]), polys)
        if cell is None:
            cell = Point(float(xs[i]), float(ys[i])).buffer(max(120.0, float(reach_m[i])))
        max_reach_m = max(120.0, float(reach_m[i]))
        reach = Point(float(xs[i]), float(ys[i])).buffer(max_reach_m)
        clipped = cell.intersection(reach)
        if clipped.is_empty:
            clipped = reach
        if clipped.geom_type not in {"Polygon", "MultiPolygon"}:
            clipped = reach
        out.append(clipped)
    return out


def _attach_mapinfo_best_cells(work: pd.DataFrame) -> None:
    """MAPINFO: непересекающаяся мозаика best-сервера внутри (telecomstandard, frequency)."""
    n = len(work)
    work["mapinfo_wkt"] = pd.Series([pd.NA] * n, dtype="string")
    nan_col = pd.Series(np.nan, index=work.index, dtype="float64")
    work["mapinfo_wkt_area"] = nan_col.copy()
    work["mapinfo_wkt_centroid_lon"] = nan_col.copy()
    work["mapinfo_wkt_centroid_lat"] = nan_col.copy()

    group_cols = ["telecomstandard", "frequency"]
    for _, sub in work.groupby(group_cols, dropna=False):
        mask = (
            sub["lon"].notna()
            & sub["lat"].notna()
            & sub["lon"].between(-180.0, 180.0)
            & sub["lat"].between(-90.0, 90.0)
        )
        sub = sub.loc[mask]
        if sub.empty:
            continue
        ref_lat = float(sub["lat"].mean())
        ref_lon = float(sub["lon"].mean())
        lons = sub["lon"].to_numpy(dtype=float)
        lats = sub["lat"].to_numpy(dtype=float)
        reach_m = work.loc[sub.index, "mapinfo_reach_m"].to_numpy(dtype=float, copy=False)
        xs, ys = _lonlat_to_xy_m(lons, lats, ref_lon, ref_lat)
        keys = (
            sub["mcc"].astype(str)
            + "_"
            + sub["mnc"].astype(str)
            + "_"
            + sub["lac"].astype(str)
            + "_"
            + sub["cell_id"].astype(str)
        ).tolist()
        xs, ys = _jitter_duplicate_xy(xs, ys, keys)
        cells_xy = _voronoi_cells_xy_for_group(xs, ys, reach_m)
        for idx, cell_xy in zip(sub.index, cells_xy, strict=True):
            cell_ll = _polygon_xy_to_lonlat(cell_xy, ref_lon, ref_lat)
            if cell_ll is None:
                continue
            area = _geometry_area_km2(cell_ll)
            c = cell_ll.centroid
            work.loc[idx, "mapinfo_wkt"] = cell_ll.wkt
            work.loc[idx, "mapinfo_wkt_area"] = area if area is not None else np.nan
            work.loc[idx, "mapinfo_wkt_centroid_lon"] = round(float(c.x), 5)
            work.loc[idx, "mapinfo_wkt_centroid_lat"] = round(float(c.y), 5)


def _build_sector_geometry(
    lon: float,
    lat: float,
    radius_km: float,
    azimuth_deg: float,
    angle_deg: float,
    bs_type: str,
) -> Polygon | MultiPolygon | None:
    if bs_type in {"m", "i", "f"} or angle_deg >= 360:
        return _circle_polygon(lon, lat, radius_km, steps=48)

    poly = _wedge_sector_polygon(lon, lat, radius_km, azimuth_deg, angle_deg)
    if poly is None or poly.is_empty:
        return None
    if bs_type == "o":
        rear_r = max(0.05, min(radius_km * 0.35, radius_km * 0.99))
        rear_ang = min(100.0, max(40.0, angle_deg * 0.65))
        rear = _wedge_sector_polygon(lon, lat, rear_r, (azimuth_deg + 180.0) % 360.0, rear_ang)
        if rear is not None and not rear.is_empty:
            try:
                merged = unary_union([poly, rear])
            except Exception:
                return poly
            if merged.is_empty:
                return poly
            if merged.geom_type in {"Polygon", "MultiPolygon"}:
                return merged  # type: ignore[return-value]
    return poly


def _wedge_sector_polygon(
    lon: float,
    lat: float,
    radius_km: float,
    azimuth_deg: float,
    angle_deg: float,
) -> Polygon | None:
    half = max(1.0, min(179.0, angle_deg / 2.0))
    bearings = [azimuth_deg - half + i * (2 * half / 18.0) for i in range(19)]
    points = [(lon, lat)]
    for b in bearings:
        points.append(_project_point(lon, lat, b, radius_km))
    points.append((lon, lat))
    try:
        poly = Polygon(points)
        if poly.is_valid and not poly.is_empty:
            return poly
    except Exception:
        return None
    return None


def _circle_polygon(lon: float, lat: float, radius_km: float, steps: int = 36) -> Polygon:
    points = []
    for i in range(steps):
        bearing = (360.0 / steps) * i
        points.append(_project_point(lon, lat, bearing, radius_km))
    points.append(points[0])
    return Polygon(points)


def _project_point(lon: float, lat: float, bearing_deg: float, dist_km: float) -> tuple[float, float]:
    br = math.radians(bearing_deg)
    dlat = (dist_km / _KM_IN_DEG) * math.cos(br)
    denom = max(0.01, math.cos(math.radians(lat)))
    dlon = (dist_km / (_KM_IN_DEG * denom)) * math.sin(br)
    return (round(lon + dlon, 6), round(lat + dlat, 6))


def _geometry_area_km2(geom: Polygon | MultiPolygon | None) -> float | None:
    if geom is None or geom.is_empty:
        return None
    if isinstance(geom, MultiPolygon):
        parts = [_geometry_area_km2(g) for g in geom.geoms]
        parts = [p for p in parts if p is not None]
        return round(float(sum(parts)), 5) if parts else None
    c = geom.centroid
    scale = _KM_IN_DEG * _KM_IN_DEG * max(0.01, math.cos(math.radians(float(c.y))))
    return round(float(geom.area) * scale, 5)


def _coerce_types(df: pd.DataFrame, fields: list[dict[str, Any]]) -> pd.DataFrame:
    out = df.copy()
    for field in fields:
        name = field["name"]
        if name not in out.columns:
            out[name] = pd.NA
        kind = field["type"]
        if kind == "int":
            out[name] = pd.to_numeric(out[name], errors="coerce").astype("Int32")
        elif kind == "smallint":
            out[name] = pd.to_numeric(out[name], errors="coerce").astype("Int16")
        elif kind == "long":
            out[name] = pd.to_numeric(out[name], errors="coerce").astype("Int64")
        elif kind == "float":
            out[name] = pd.to_numeric(out[name], errors="coerce").astype("float64")
        elif kind == "timestamp":
            out[name] = pd.to_datetime(out[name], errors="coerce")
        elif kind == "string":
            out[name] = out[name].astype("string")
        elif kind == "boolean":
            out[name] = out[name].astype("boolean")
        else:
            raise ValueError(f"Unsupported field type in fct_bs schema: {kind}")
    return out


def _select_fields(df: pd.DataFrame, fields: list[dict[str, Any]]) -> pd.DataFrame:
    names = [f["name"] for f in fields]
    missing = [name for name in names if name not in df.columns]
    if missing:
        raise ValueError(f"Missing output fields: {missing}")
    return df[names].copy()


def _empty_frame(fields: list[dict[str, Any]]) -> pd.DataFrame:
    return pd.DataFrame(columns=[f["name"] for f in fields])


def _merge_history(
    *,
    existing_path: Path,
    current_snapshot: pd.DataFrame,
    effective_ts: pd.Timestamp,
    fields: list[dict[str, Any]],
) -> pd.DataFrame:
    key_cols = ["mcc", "mnc", "lac", "cell_id"]
    time_cols = {"date_on", "date_off"}
    compare_cols = [f["name"] for f in fields if f["name"] not in time_cols and f["name"] not in key_cols]
    close_ts = effective_ts - pd.Timedelta(microseconds=1)

    if existing_path.exists():
        history = pd.read_parquet(existing_path)
        history = _coerce_types(history, fields)
        history = _select_fields(history, fields)
    else:
        history = _empty_frame(fields)

    if history.empty:
        return current_snapshot.reset_index(drop=True)

    active_prev = history[history["date_off"].eq(_OPEN_END_TS)].copy()
    if active_prev.empty:
        merged = pd.concat([history, current_snapshot], ignore_index=True)
        return merged.sort_values(key_cols + ["date_on", "date_off"], kind="mergesort").reset_index(drop=True)

    prev_sig = _signature_map(active_prev, key_cols=key_cols, compare_cols=compare_cols)
    curr_sig = _signature_map(current_snapshot, key_cols=key_cols, compare_cols=compare_cols)

    unchanged = {key for key in prev_sig.keys() & curr_sig.keys() if prev_sig[key] == curr_sig[key]}
    keys_to_close = set(prev_sig.keys()) - unchanged
    keys_to_insert = set(curr_sig.keys()) - unchanged

    if keys_to_close:
        mask_close = history["date_off"].eq(_OPEN_END_TS) & history[key_cols].apply(tuple, axis=1).isin(keys_to_close)
        history.loc[mask_close, "date_off"] = close_ts
        invalid = mask_close & (history["date_off"] < history["date_on"])
        history.loc[invalid, "date_off"] = history.loc[invalid, "date_on"]

    to_insert = current_snapshot[current_snapshot[key_cols].apply(tuple, axis=1).isin(keys_to_insert)].copy()
    if not to_insert.empty:
        history = pd.concat([history, to_insert], ignore_index=True)

    history = history.sort_values(key_cols + ["date_on", "date_off"], kind="mergesort").reset_index(drop=True)
    return history


def _signature_map(
    frame: pd.DataFrame,
    *,
    key_cols: list[str],
    compare_cols: list[str],
) -> dict[tuple[Any, ...], tuple[Any, ...]]:
    out: dict[tuple[Any, ...], tuple[Any, ...]] = {}
    if frame.empty:
        return out
    for row in frame.itertuples(index=False):
        key = tuple(getattr(row, c) for c in key_cols)
        signature = tuple(_normalize_signature_value(getattr(row, c)) for c in compare_cols)
        out[key] = signature
    return out


def _normalize_signature_value(value: Any) -> Any:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value


def _read_json(path: str | Path) -> dict[str, Any]:
    file = Path(path)
    if not file.exists():
        raise FileNotFoundError(f"Config file not found: {file}")
    return json.loads(file.read_text(encoding="utf-8"))


def _safe_str(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value)


# --- MAPINFO radio field validation (power / height / tilt → reach) ---

_HEIGHT_RANGE = (-50.0, 240.0)
_POWER_CAP = 150.0
_AMPL_RANGE = (-25.0, 80.0)
_DEFAULT_POWER_BY_BS: dict[str, float] = {
    "m": 22.0,
    "i": 6.0,
    "f": 1.5,
    "x": 16.0,
    "o": 42.0,
}
_DEFAULT_HEIGHT_BY_BS: dict[str, float] = {
    "m": 18.0,
    "i": 7.0,
    "f": 4.5,
    "x": 20.0,
    "o": 35.0,
}

_RAD_CLASS_MULT: dict[str, float] = {
    "a": 1.08,
    "b": 1.0,
    "c": 0.94,
    "d": 0.86,
    "s": 0.92,
}


def _default_power_for_bs(bs: str) -> float:
    return float(_DEFAULT_POWER_BY_BS.get(str(bs).strip().lower()[:1] or "o", 35.0))


def _default_height_for_bs(bs: str) -> float:
    return float(_DEFAULT_HEIGHT_BY_BS.get(str(bs).strip().lower()[:1] or "o", 30.0))


def _clip(v: float, lo: float, hi: float) -> float:
    return float(min(hi, max(lo, v)))


def _safe_float(x: Any) -> float | None:
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return None
    try:
        if isinstance(x, str) and not x.strip():
            return None
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    except (TypeError, ValueError):
        return None


def _median_abs_tilts(row: pd.Series) -> float:
    parts: list[float] = []
    for key in ("tilt", "el_tilt", "mech_tilt"):
        v = _safe_float(row.get(key))
        if v is None:
            continue
        av = abs(v)
        if av > 120.0:
            continue
        parts.append(av)
    if not parts:
        return 8.0
    parts.sort()
    mid = len(parts) // 2
    if len(parts) % 2:
        return float(parts[mid])
    return float((parts[mid - 1] + parts[mid]) / 2.0)


def _rad_class_mult(row: pd.Series) -> float:
    raw = row.get("rad_class")
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return 1.0
    key = str(raw).strip().lower()[:1]
    return float(_RAD_CLASS_MULT.get(key, 1.0))


def effective_reach_m(
    *,
    sector_radius_km: float,
    power_w: float,
    height_m: float,
    tilt_down_deg: float,
    bs_type: str,
    rad_mult: float,
) -> float:
    """Максимальный радиус MAPINFO (метры) после Вороного-клипа: из sector_radius + валидированные power/height/tilt."""
    sr = max(0.01, float(sector_radius_km))
    base_m = max(120.0, sr * 1000.0 * 0.85)
    pw = max(0.0, float(power_w))
    pf = 1.0 + 0.18 * math.log1p(pw / 15.0)
    pf = _clip(pf, 0.72, 1.48)
    hm = max(0.0, float(height_m))
    hf = 1.0 + min(0.32, hm / 120.0) * 0.11
    td = max(0.0, min(35.0, float(tilt_down_deg)))
    tf = 1.0 - min(0.22, (td / 28.0) * 0.16)
    tf = max(0.68, tf)
    r = base_m * pf * hf * tf * max(0.75, min(1.2, float(rad_mult)))
    b = str(bs_type).strip().lower()[:1] or "o"
    if b in {"m", "i", "f"}:
        return max(80.0, min(r, 900.0))
    return max(120.0, min(r, 42_000.0))


def _effective_reach_m_vec(
    sr: np.ndarray,
    power_v: np.ndarray,
    height_v: np.ndarray,
    tilt_down: np.ndarray,
    is_small_site: np.ndarray,
    rad_mult: np.ndarray,
) -> np.ndarray:
    sr = np.maximum(0.01, sr.astype(float))
    base_m = np.maximum(120.0, sr * 1000.0 * 0.85)
    pw = np.maximum(0.0, power_v.astype(float))
    pf = 1.0 + 0.18 * np.log1p(pw / 15.0)
    pf = np.clip(pf, 0.72, 1.48)
    hm = np.maximum(0.0, height_v.astype(float))
    hf = 1.0 + np.minimum(0.32, hm / 120.0) * 0.11
    td = np.clip(tilt_down.astype(float), 0.0, 35.0)
    tf = 1.0 - np.minimum(0.22, (td / 28.0) * 0.16)
    tf = np.maximum(0.68, tf)
    rm = np.clip(rad_mult.astype(float), 0.75, 1.2)
    r = base_m * pf * hf * tf * rm
    cap = np.where(is_small_site, 900.0, 42_000.0)
    floor = np.where(is_small_site, 80.0, 120.0)
    return np.maximum(floor, np.minimum(r, cap))


def compute_mapinfo_radio_columns(work: pd.DataFrame) -> pd.DataFrame:
    """Возвращает столбцы с суффиксом _v / mapinfo_reach_m (тот же индекс, что у work)."""
    bs = work["bs_type"].astype(str).str.lower().str.slice(0, 1)
    default_p = bs.map(_DEFAULT_POWER_BY_BS).astype("float64")
    default_h = bs.map(_DEFAULT_HEIGHT_BY_BS).astype("float64")

    power = pd.to_numeric(work.get("power"), errors="coerce")
    ampl = pd.to_numeric(work.get("amplification"), errors="coerce")
    height = pd.to_numeric(work.get("height"), errors="coerce")
    sr = pd.to_numeric(work.get("sector_radius"), errors="coerce").fillna(5.0).astype("float64")

    power_v = power.where(power.notna() & (power >= 0), default_p)
    power_v = power_v.where(power_v <= _POWER_CAP, _POWER_CAP)

    ampl_v = ampl.where(ampl.notna(), 0.0)
    ampl_v = ampl_v.clip(_AMPL_RANGE[0], _AMPL_RANGE[1])

    height_v = height.where(height.notna(), default_h)
    height_v = height_v.clip(_HEIGHT_RANGE[0], _HEIGHT_RANGE[1])

    rad_mult = work.apply(_rad_class_mult, axis=1).astype("float64")
    tilt_down = work.apply(_median_abs_tilts, axis=1).astype("float64")

    is_small = bs.isin(["m", "i", "f"]).to_numpy(dtype=bool)
    reach = _effective_reach_m_vec(
        sr.to_numpy(),
        power_v.to_numpy(),
        height_v.to_numpy(),
        tilt_down.to_numpy(),
        is_small,
        rad_mult.to_numpy(),
    )

    return pd.DataFrame(
        {
            "mapinfo_power_v": power_v.astype("float64"),
            "mapinfo_height_v": height_v.astype("float64"),
            "mapinfo_ampl_v": ampl_v.astype("float64"),
            "mapinfo_tilt_down_v": tilt_down.astype("float64"),
            "mapinfo_rad_mult_v": rad_mult.astype("float64"),
            "mapinfo_reach_m": reach,
        },
        index=work.index,
    )
