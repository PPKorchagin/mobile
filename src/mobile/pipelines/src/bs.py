from __future__ import annotations

import json
import logging
import random
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from shapely import wkt
from shapely.geometry import MultiPolygon, Point, Polygon

from mobile.command_timing import append_command_metrics, timed_stage
from mobile.cli_defaults import OPEN_BS_DATE_OFF, OPERATORS
from mobile.project_paths import PROJECT_ROOT


logger = logging.getLogger(__name__)

SRC_BS_TABLE = "bs"

SRC_BS_FIELDS: list[dict[str, str]] = [
    {"name": "id", "type": "int"},
    {"name": "mcc", "type": "smallint"},
    {"name": "mnc", "type": "smallint"},
    {"name": "lac", "type": "int"},
    {"name": "cell", "type": "long"},
    {"name": "date_on", "type": "timestamp"},
    {"name": "date_off", "type": "timestamp"},
    {"name": "coord_x", "type": "float"},
    {"name": "coord_y", "type": "float"},
    {"name": "bs_type", "type": "string"},
    {"name": "generation", "type": "string"},
    {"name": "address", "type": "string"},
    {"name": "subject", "type": "string"},
    {"name": "location", "type": "string"},
    {"name": "description", "type": "string"},
    {"name": "controllernum", "type": "string"},
    {"name": "frequency_out", "type": "string"},
    {"name": "frequency_in", "type": "string"},
    {"name": "rad_class", "type": "string"},
    {"name": "bcch", "type": "string"},
    {"name": "azimuth", "type": "float"},
    {"name": "height", "type": "float"},
    {"name": "tilt", "type": "float"},
    {"name": "el_tilt", "type": "float"},
    {"name": "mech_tilt", "type": "float"},
    {"name": "raster", "type": "float"},
    {"name": "thickness", "type": "float"},
    {"name": "frequency", "type": "float"},
    {"name": "power", "type": "float"},
    {"name": "amplification", "type": "float"},
    {"name": "polarization", "type": "float"},
    {"name": "rac", "type": "int"},
    {"name": "border", "type": "boolean"},
    {"name": "avtocod", "type": "int"},
    {"name": "bsic", "type": "int"},
    {"name": "bsid", "type": "long"},
]

OPERATOR_PROFILE_ALIASES = {
    "мтс": {"мтс", "mts"},
    "мегафон": {"мегафон", "megafon"},
    "билайн": {"билайн", "beeline"},
    "теле2": {"теле2", "tele2", "т2/tele2"},
}

NOISE_ROW_PROBABILITY = 0.22
NOISE_FIELD_PROBABILITY = 0.35

# OCC-013: неизвестная БС — lac/cell = null или 0.
LAC_CELL_NULL_PROBABILITY = 0.015
LAC_CELL_ZERO_PROBABILITY = 0.01

_MSK = ZoneInfo("Europe/Moscow")
# Порог расстояния до границы полигона субъекта (градусы WGS84, ~1.3 км на широте 55°).
_BORDER_DISTANCE_DEG = 0.012

_PROTECTED_RADIO_COORD_FIELDS = frozenset(
    {
        "power",
        "amplification",
        "height",
        "tilt",
        "el_tilt",
        "mech_tilt",
        "thickness",
        "frequency",
        "coord_x",
        "coord_y",
    }
)

# Согласованные профили развёртывания: тип/локация ↔ высота/мощность/антенна.
_DEPLOY_PROFILES: list[dict[str, Any]] = [
    {
        "kind": "macro_outdoor",
        "weight": 0.36,
        "bs_type": "macro",
        "location": "outdoor",
        "rad_class": "A",
        "height": (22.0, 58.0),
        "power": (38.0, 72.0),
        "amplification": (14.0, 32.0),
        "directional": True,
        "thick": (52.0, 108.0),
    },
    {
        "kind": "macro_rooftop",
        "weight": 0.12,
        "bs_type": "outdoor",
        "location": "outdoor",
        "rad_class": "B",
        "height": (18.0, 45.0),
        "power": (32.0, 62.0),
        "amplification": (10.0, 26.0),
        "directional": True,
        "thick": (48.0, 95.0),
    },
    {
        "kind": "micro",
        "weight": 0.14,
        "bs_type": "micro",
        "location": "outdoor",
        "rad_class": "B",
        "height": (10.0, 28.0),
        "power": (20.0, 40.0),
        "amplification": (6.0, 18.0),
        "directional": True,
        "thick": (55.0, 110.0),
    },
    {
        "kind": "indoor",
        "weight": 0.14,
        "bs_type": "indoor",
        "location": "indoor",
        "rad_class": "C",
        "height": (3.0, 14.0),
        "power": (2.5, 14.0),
        "amplification": (0.5, 8.0),
        "directional": False,
        "thick": None,
    },
    {
        "kind": "femto",
        "weight": 0.07,
        "bs_type": "femto",
        "location": "small cell",
        "rad_class": "S",
        "height": (2.5, 9.0),
        "power": (0.08, 0.85),
        "amplification": (0.0, 4.0),
        "directional": False,
        "thick": None,
    },
    {
        "kind": "metro",
        "weight": 0.09,
        "bs_type": "macro",
        "location": "underground",
        "rad_class": "C",
        "height": (-12.0, 18.0),
        "power": (18.0, 42.0),
        "amplification": (4.0, 16.0),
        "directional": True,
        "thick": (70.0, 130.0),
    },
    {
        "kind": "street_small",
        "weight": 0.08,
        "bs_type": "small cell",
        "location": "outdoor",
        "rad_class": "D",
        "height": (6.0, 18.0),
        "power": (8.0, 22.0),
        "amplification": (2.0, 12.0),
        "directional": True,
        "thick": (42.0, 95.0),
    },
]


def _pick_deploy_profile(rng: random.Random, tech: str) -> dict[str, Any]:
    weights: list[float] = []
    for row in _DEPLOY_PROFILES:
        w = float(row["weight"])
        if tech == "2G" and row["kind"] in {"femto", "street_small"}:
            w *= 0.2
        if tech in {"5G"} and row["kind"] in {"macro_outdoor", "macro_rooftop", "micro"}:
            w *= 1.15
        weights.append(w)
    return rng.choices(_DEPLOY_PROFILES, weights=weights, k=1)[0]


def _coherent_radio_fields(
    rng: random.Random,
    tech: str,
    profile: Any,
    deploy: dict[str, Any],
) -> dict[str, float]:
    h_lo, h_hi = deploy["height"]
    p_lo, p_hi = deploy["power"]
    a_lo, a_hi = deploy["amplification"]
    base_p, base_a = _sample_radio_power(rng, profile)
    height = float(min(h_hi, max(h_lo, rng.uniform(h_lo, h_hi) * rng.uniform(0.94, 1.06))))
    power = float(min(p_hi, max(p_lo, base_p * rng.uniform(0.9, 1.08))))
    power = float(min(p_hi, max(p_lo, power)))
    amplification = float(min(a_hi, max(a_lo, base_a * rng.uniform(0.85, 1.12))))
    amplification = float(min(a_hi, max(a_lo, amplification)))
    tilt = rng.uniform(2.0, 14.0) if deploy["directional"] else rng.uniform(0.0, 6.0)
    el_tilt = rng.uniform(-1.5, 10.0) if deploy["directional"] else rng.uniform(-2.0, 4.0)
    mech_tilt = rng.uniform(-6.0, 8.0) if deploy["directional"] else rng.uniform(-3.0, 5.0)
    if deploy["thick"] is None:
        thickness = float(rng.choice([90.0, 120.0, 180.0, 240.0, 360.0]))
    else:
        t_lo, t_hi = deploy["thick"]
        thickness = float(rng.uniform(t_lo, t_hi))
    azimuth = rng.uniform(0.0, 360.0) if deploy["directional"] else -1.0
    return {
        "height": height,
        "power": power,
        "amplification": amplification,
        "tilt": tilt,
        "el_tilt": el_tilt,
        "mech_tilt": mech_tilt,
        "thickness": thickness,
        "azimuth": azimuth,
    }


@dataclass(frozen=True)
class BuildBsParams:
    start_date: date
    end_date: date
    subjects: list[str]
    operators: list[str]
    seed: int
    profile_path: Path | None = None


@dataclass(frozen=True)
class BuildBsProfile:
    operator_weights: dict[str, float]
    generation_weights: dict[str, float]
    operator_generation_weights: dict[str, dict[str, float]]
    lac_min: int
    lac_max: int
    cell_min: int
    cell_max: int
    samples_p50: float | None
    samples_p95: float | None


def _resolve_path(path: str | Path) -> Path:
    candidate = Path(path)
    return candidate if candidate.is_absolute() else PROJECT_ROOT / candidate


def run(
    *,
    oktmo_parquet_path: str | Path,
    output_path: str | Path,
    compression: str,
    params: BuildBsParams,
) -> dict[str, Any]:
    fields = SRC_BS_FIELDS
    parquet_file = _resolve_path(output_path)

    perf: dict[str, Any] = {}
    started = time.perf_counter()
    rng = random.Random(params.seed)
    with timed_stage("load_oktmo_sec", perf):
        subject_geometries = _load_subject_geometries(oktmo_parquet_path, params.subjects)
    profile = _load_build_profile(params.profile_path, params.operators)
    with timed_stage("generate_rows_sec", perf):
        rows = _generate_rows(fields, subject_geometries, params, rng, profile)
        data = pd.DataFrame(rows)
        data, noise_metrics = _inject_noise(data, fields, rng)
        data = _coerce_types(data, fields)

    stats = _collect_stats(data, params, profile)
    stats.update(noise_metrics)
    _validate_dataset(data, fields, params.subjects)

    with timed_stage("write_parquet_sec", perf):
        parquet_file.parent.mkdir(parents=True, exist_ok=True)
        logger.info("Writing BS parquet (overwrite): %s", parquet_file)
        data.to_parquet(parquet_file, compression=compression, index=False)

    perf["elapsed_total_sec"] = round(time.perf_counter() - started, 4)
    append_command_metrics(command="build-src-bs", metrics={**stats, **perf})
    logger.info(
        "%s build done. rows=%s, output=%s, compression=%s",
        SRC_BS_TABLE,
        stats["row_count"],
        parquet_file,
        compression,
    )
    logger.info("BS validation and stats: %s", json.dumps(stats, ensure_ascii=False))
    return stats


def _load_subject_geometries(
    oktmo_parquet_path: str | Path, subjects: list[str]
) -> dict[str, Polygon | MultiPolygon]:
    oktmo_path = Path(oktmo_parquet_path)
    if not oktmo_path.exists():
        raise FileNotFoundError(f"OKTMO parquet not found: {oktmo_path}")

    logger.info("Reading OKTMO parquet for geometries: %s", oktmo_path)
    oktmo = pd.read_parquet(oktmo_path)
    level_1 = oktmo[oktmo["level"] == 1].copy()
    level_1["name"] = level_1["name"].astype("string").str.strip()
    level_1 = level_1[level_1["name"].isin(subjects)]

    found = sorted(level_1["name"].dropna().unique().tolist())
    missing = sorted(set(subjects) - set(found))
    if missing:
        raise ValueError(f"Subjects not found in OKTMO level=1: {missing}")

    geometries: dict[str, Polygon | MultiPolygon] = {}
    for _, row in level_1.iterrows():
        geom = wkt.loads(str(row["WKT"]))
        if geom.geom_type not in {"Polygon", "MultiPolygon"}:
            raise ValueError(f"Unsupported geometry for {row['name']}: {geom.geom_type}")
        geometries[str(row["name"])] = geom
    return geometries


def _generate_rows(
    fields: list[dict[str, Any]],
    subject_geometries: dict[str, Polygon | MultiPolygon],
    params: BuildBsParams,
    rng: random.Random,
    profile: BuildBsProfile | None,
) -> list[dict[str, Any]]:
    field_names = [field["name"] for field in fields]
    dates = _weighted_dates(params.start_date, params.end_date)

    rows: list[dict[str, Any]] = []
    row_id = 1
    for subject, geom in subject_geometries.items():
        subject_total = rng.randint(2800, 5200)
        per_operator_counts = _split_subject_total_by_operator(
            subject_total=subject_total,
            operators=params.operators,
            rng=rng,
            profile=profile,
        )
        for operator in params.operators:
            target_count = per_operator_counts.get(operator, 0)
            for _ in range(target_count):
                date_on = rng.choice(dates)
                date_off = _sample_date_off(date_on, params.end_date, rng)
                point = _sample_point_in_geometry(geom, rng)
                on_border = _is_border_point(point, geom)

                row = _generate_row(
                    row_id=row_id,
                    operator=operator,
                    subject=subject,
                    coord_x=point.x,
                    coord_y=point.y,
                    date_on=date_on,
                    date_off=date_off,
                    period_end=params.end_date,
                    on_border=on_border,
                    rng=rng,
                    profile=profile,
                )
                rows.append({name: row[name] for name in field_names})
                row_id += 1
    return rows


def _split_subject_total_by_operator(
    *,
    subject_total: int,
    operators: list[str],
    rng: random.Random,
    profile: BuildBsProfile | None,
) -> dict[str, int]:
    if subject_total <= 0:
        return {op: 0 for op in operators}
    if not operators:
        return {}

    if profile is None:
        baseline = [1.0 for _ in operators]
    else:
        baseline = [max(1e-6, profile.operator_weights.get(op, 1.0)) for op in operators]

    total_weight = sum(baseline)
    shares = [w / total_weight for w in baseline]
    raw_counts = [int(subject_total * share) for share in shares]
    assigned = sum(raw_counts)
    remainder = max(0, subject_total - assigned)

    if remainder > 0:
        sampled_idx = rng.choices(range(len(operators)), weights=shares, k=remainder)
        for idx in sampled_idx:
            raw_counts[idx] += 1

    return {op: raw_counts[i] for i, op in enumerate(operators)}


def _weighted_dates(start_date: date, end_date: date) -> list[date]:
    dates: list[date] = []
    current = start_date
    while current <= end_date:
        month_weight = {1: 3, 2: 4, 3: 5}.get(current.month, 2)
        weekday_weight = 1 if current.weekday() >= 5 else 2
        repeat = month_weight * weekday_weight
        dates.extend([current] * repeat)
        current += timedelta(days=1)
    return dates


def _sample_date_off(date_on: date, end_date: date, rng: random.Random) -> date:
    active_days = rng.randint(3, 90)
    date_off = date_on + timedelta(days=active_days)
    if date_off > end_date:
        date_off = end_date
    return date_off


def _msk_wall_datetime(day: date, *, end_of_day: bool = False, minute_jitter: int = 0) -> datetime:
    """Наивный timestamp в поясе Europe/Moscow (контракт BS-006)."""
    if end_of_day:
        base = datetime.combine(day, datetime.max.time())
    else:
        base = datetime.combine(day, datetime.min.time())
    aware = base.replace(tzinfo=_MSK)
    if minute_jitter and not end_of_day:
        aware += timedelta(minutes=minute_jitter)
    return aware.replace(tzinfo=None)


def _is_border_point(point: Point, geom: Polygon | MultiPolygon) -> bool:
    return float(point.distance(geom.boundary)) <= _BORDER_DISTANCE_DEG


def _sample_point_in_geometry(
    geom: Polygon | MultiPolygon, rng: random.Random
) -> Point:
    if isinstance(geom, MultiPolygon):
        polys = list(geom.geoms)
        weights = [max(poly.area, 0.0001) for poly in polys]
        selected = rng.choices(polys, weights=weights, k=1)[0]
        return _sample_point_in_polygon(selected, rng)
    return _sample_point_in_polygon(geom, rng)


def _sample_point_in_polygon(poly: Polygon, rng: random.Random) -> Point:
    min_x, min_y, max_x, max_y = poly.bounds
    for _ in range(5000):
        point = Point(rng.uniform(min_x, max_x), rng.uniform(min_y, max_y))
        if poly.contains(point):
            return point
    return poly.representative_point()


def _generate_row(
    row_id: int,
    operator: str,
    subject: str,
    coord_x: float,
    coord_y: float,
    date_on: date,
    date_off: date,
    period_end: date,
    on_border: bool,
    rng: random.Random,
    profile: BuildBsProfile | None,
) -> dict[str, Any]:
    mcc = 250
    mnc = OPERATORS[operator]
    tech = _sample_generation(operator, rng, profile)
    deploy = _pick_deploy_profile(rng, tech)
    bs_type = str(deploy["bs_type"])
    location = str(deploy["location"])
    rad_class = str(deploy["rad_class"])
    radio = _coherent_radio_fields(rng, tech, profile, deploy)
    desc_pool = [
        "NO COMMENT",
        "legacy node",
        "femto-СЃРѕС‚Р°",
        "TEMP??",
        "modernized in Q1",
        "no-comment",
    ]
    if deploy["kind"] == "femto":
        desc = rng.choices(desc_pool, weights=[1, 1, 4, 1, 1, 1], k=1)[0]
    else:
        desc = rng.choice(desc_pool)

    lac_cell_roll = rng.random()
    if lac_cell_roll < LAC_CELL_NULL_PROBABILITY:
        lac, cell = None, None
    elif lac_cell_roll < LAC_CELL_NULL_PROBABILITY + LAC_CELL_ZERO_PROBABILITY:
        lac, cell = 0, 0
    else:
        lac = _sample_lac(rng, profile)
        cell = _sample_cell(row_id, rng, profile)
    base_band = _sample_base_band_for_generation(tech, rng)
    freq_out = f"{base_band + rng.randint(-3, 3)},{base_band + rng.randint(5, 15)}"
    freq_in = f"{base_band - 45},{base_band - 35}"

    dt_on = _msk_wall_datetime(date_on, minute_jitter=rng.randint(0, 1439))
    end_of_period = _msk_wall_datetime(period_end, end_of_day=True)
    still_active = date_off >= period_end
    if still_active:
        dt_off = OPEN_BS_DATE_OFF.to_pydatetime().replace(tzinfo=None)
    else:
        dt_off = _msk_wall_datetime(date_off, minute_jitter=rng.randint(0, 1439))

    temporal_case = rng.random()
    if still_active:
        pass
    elif temporal_case < 0.015:
        dt_on = datetime(rng.randint(1980, 1999), rng.randint(1, 12), rng.randint(1, 28), rng.randint(0, 23), rng.randint(0, 59))
        dt_off = dt_on + timedelta(days=rng.randint(30, 3650))
    elif temporal_case < 0.03:
        dt_on = datetime(rng.randint(2035, 2060), rng.randint(1, 12), rng.randint(1, 28), rng.randint(0, 23), rng.randint(0, 59))
        dt_off = dt_on + timedelta(days=rng.randint(10, 1200))
    elif temporal_case < 0.04:
        dt_off = dt_on - timedelta(days=rng.randint(1, 120))
    else:
        if dt_off < dt_on:
            dt_off = dt_on + timedelta(minutes=rng.randint(30, 720))
        if dt_off > end_of_period:
            dt_off = end_of_period

    return {
        "id": row_id,
        "mcc": mcc,
        "mnc": mnc,
        "lac": lac,
        "cell": cell,
        "date_on": dt_on,
        "date_off": dt_off,
        "coord_x": coord_x,
        "coord_y": coord_y,
        "bs_type": bs_type,
        "generation": tech,
        "address": f"{subject}, {rng.choice(['ул.', 'пр-т', 'ш.', 'тер.'])} {rng.choice(['Центральная', 'Лесная', 'Промышленная'])}, д.{rng.randint(1, 180)}",
        "subject": subject,
        "location": location,
        "description": desc,
        "controllernum": f"{rng.choice(['RNC', 'BSC', 'UAG'])}{rng.randint(10, 999)}_{operator[:3].upper()}",
        "frequency_out": freq_out,
        "frequency_in": freq_in,
        "rad_class": rad_class,
        "bcch": str(rng.randint(1, 124)),
        "azimuth": float(radio["azimuth"]),
        "height": float(radio["height"]),
        "tilt": float(radio["tilt"]),
        "el_tilt": float(radio["el_tilt"]),
        "mech_tilt": float(radio["mech_tilt"]),
        "raster": rng.uniform(-180, 180),
        "thickness": float(radio["thickness"]),
        "frequency": float(base_band),
        "power": float(radio["power"]),
        "amplification": float(radio["amplification"]),
        "polarization": rng.choice([0.0, 45.0, 90.0, -45.0]),
        "rac": rng.randint(1, 255),
        "border": on_border,
        "avtocod": rng.randint(10000, 99999),
        "bsic": rng.randint(0, 63),
        "bsid": row_id * 1000 + rng.randint(100, 999),
    }


def _sample_generation(operator: str, rng: random.Random, profile: BuildBsProfile | None) -> str:
    default_values = ["2G", "3G", "4G", "LTE", "5G"]
    default_weights = [5, 8, 35, 35, 17]
    if profile is None:
        return rng.choices(default_values, weights=default_weights, k=1)[0]

    op_mix = profile.operator_generation_weights.get(operator, {})
    if op_mix:
        values = []
        weights = []
        for generation in default_values:
            weight = max(1e-6, op_mix.get(generation, 0.0))
            values.append(generation)
            weights.append(weight)
        return rng.choices(values, weights=weights, k=1)[0]

    values = []
    weights = []
    for generation in default_values:
        weight = max(1e-6, profile.generation_weights.get(generation, 0.0))
        values.append(generation)
        weights.append(weight)
    return rng.choices(values, weights=weights, k=1)[0]


def _sample_lac(rng: random.Random, profile: BuildBsProfile | None) -> int:
    if profile is None:
        return rng.randint(1000, 65533)
    low = max(0, min(profile.lac_min, profile.lac_max))
    high = max(low, min(65535, max(profile.lac_min, profile.lac_max)))
    return rng.randint(low, high)


def _sample_cell(row_id: int, rng: random.Random, profile: BuildBsProfile | None) -> int:
    if profile is None:
        return row_id * 100 + rng.randint(1, 99)
    low = max(0, min(profile.cell_min, profile.cell_max))
    high = max(low + 1, min(2_147_483_647, max(profile.cell_min, profile.cell_max)))
    sampled = rng.randint(low, high)
    # Легкий сдвиг снижает риск массовых коллизий при узком диапазоне.
    return int((sampled + row_id) % 2_147_483_647)


def _sample_radio_power(rng: random.Random, profile: BuildBsProfile | None) -> tuple[float, float]:
    if profile is None:
        return rng.uniform(5, 85), rng.uniform(0, 35)

    p50 = profile.samples_p50 if profile.samples_p50 is not None else 2.0
    p95 = profile.samples_p95 if profile.samples_p95 is not None else 57.0
    sample_intensity = rng.triangular(0.0, max(p95, 1.0), max(p50, 0.5))
    power = min(120.0, max(0.0, 8.0 + sample_intensity * 0.35 + rng.uniform(-5.0, 8.0)))
    amplification = min(60.0, max(-10.0, rng.uniform(0.0, 20.0) + sample_intensity * 0.05))
    return float(power), float(amplification)


def _sample_base_band_for_generation(generation: str, rng: random.Random) -> int:
    if generation == "2G":
        return rng.choice([800, 900, 1800])
    if generation == "3G":
        return rng.choice([900, 2100])
    if generation in {"4G", "LTE"}:
        return rng.choice([800, 1800, 2100, 2600])
    if generation == "5G":
        return rng.choice([2600, 3500])
    return rng.choice([800, 900, 1800, 2100, 2600, 3500])


def _load_build_profile(profile_path: Path | None, operators: list[str]) -> BuildBsProfile | None:
    if profile_path is None:
        return None
    path = Path(profile_path)
    if not path.exists():
        raise FileNotFoundError(f"Build BS profile not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        raw = json.load(f)

    operator_weights = _extract_operator_weights(raw.get("operator_distribution_pct", {}), operators)
    generation_weights = _extract_generation_weights(raw.get("generation_distribution_pct", {}))
    operator_generation_weights = _extract_operator_generation_weights(
        raw.get("operator_generation_mix_pct", {}),
        operators,
    )
    id_ranges = raw.get("id_ranges", {}) if isinstance(raw.get("id_ranges", {}), dict) else {}
    samples = raw.get("samples", {}) if isinstance(raw.get("samples", {}), dict) else {}
    profile = BuildBsProfile(
        operator_weights=operator_weights,
        generation_weights=generation_weights,
        operator_generation_weights=operator_generation_weights,
        lac_min=_safe_int(id_ranges.get("area_min"), 1000),
        lac_max=_safe_int(id_ranges.get("area_max"), 65533),
        cell_min=_safe_int(id_ranges.get("cell_min"), 1),
        cell_max=_safe_int(id_ranges.get("cell_max"), 2_147_483_647),
        samples_p50=_safe_float_or_none(samples.get("p50")),
        samples_p95=_safe_float_or_none(samples.get("p95")),
    )
    logger.info("Loaded build-src-bs profile: %s", path)
    return profile


def _extract_operator_weights(raw_distribution: dict[str, Any], operators: list[str]) -> dict[str, float]:
    weights = {op: 1.0 for op in operators}
    if not isinstance(raw_distribution, dict):
        return weights

    for op in operators:
        aliases = OPERATOR_PROFILE_ALIASES.get(op, {op})
        for key, value in raw_distribution.items():
            if str(key).strip().lower() not in aliases:
                continue
            numeric = _safe_float_or_none(value)
            if numeric is not None and numeric > 0:
                weights[op] = numeric
    return weights


def _extract_operator_generation_weights(
    raw_mix: dict[str, Any],
    operators: list[str],
) -> dict[str, dict[str, float]]:
    canonical = {"2g": "2G", "3g": "3G", "4g": "4G", "lte": "LTE", "5g": "5G"}
    out: dict[str, dict[str, float]] = {}
    if not isinstance(raw_mix, dict):
        return out

    for op in operators:
        aliases = OPERATOR_PROFILE_ALIASES.get(op, {op})
        matched: dict[str, float] = {}
        for key, value in raw_mix.items():
            if str(key).strip().lower() not in aliases:
                continue
            if not isinstance(value, dict):
                continue
            for gen_key, weight in value.items():
                mapped = canonical.get(str(gen_key).strip().lower())
                if mapped is None:
                    continue
                numeric = _safe_float_or_none(weight)
                if numeric is not None and numeric > 0:
                    matched[mapped] = numeric
        if matched:
            out[op] = matched
    return out


def _extract_generation_weights(raw_distribution: dict[str, Any]) -> dict[str, float]:
    canonical = {"2g": "2G", "3g": "3G", "4g": "4G", "lte": "LTE", "5g": "5G"}
    weights = {"2G": 5.0, "3G": 8.0, "4G": 35.0, "LTE": 35.0, "5G": 17.0}
    if not isinstance(raw_distribution, dict):
        return weights

    for key, value in raw_distribution.items():
        mapped = canonical.get(str(key).strip().lower())
        if mapped is None:
            continue
        numeric = _safe_float_or_none(value)
        if numeric is not None and numeric > 0:
            weights[mapped] = numeric
    return weights


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _safe_float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _inject_noise(
    data: pd.DataFrame, fields: list[dict[str, Any]], rng: random.Random
) -> tuple[pd.DataFrame, dict[str, Any]]:
    # Временный object-слой нужен, чтобы безопасно вставлять "грязные" значения
    # (строки в числовые колонки, невалидные даты и т.д.) до этапа coercion.
    noisy = data.copy().astype("object")
    type_map = {field["name"]: field["type"] for field in fields}
    rows_touched = 0
    cells_touched = 0

    for idx in noisy.index:
        if rng.random() >= NOISE_ROW_PROBABILITY:
            continue
        row_touched = False
        for field in noisy.columns:
            if field in _PROTECTED_RADIO_COORD_FIELDS:
                continue
            if rng.random() >= NOISE_FIELD_PROBABILITY:
                continue
            noisy.at[idx, field] = _sample_noisy_value(field, type_map[field], rng)
            cells_touched += 1
            row_touched = True
        if row_touched:
            rows_touched += 1

    row_count = max(len(data), 1)
    return noisy, {
        "noise_rows_touched": rows_touched,
        "noise_cells_touched": cells_touched,
        "noise_row_ratio": round(rows_touched / row_count, 4),
    }


def _sample_noisy_value(field: str, field_type: str, rng: random.Random) -> Any:
    if field_type in {"int", "smallint", "long", "float"}:
        # Для чисел: null / 0 / явные выбросы / нечисловой мусор.
        return rng.choice([None, 0, -999999, 999999, "ERR", "NaN?", ""])
    if field_type == "timestamp":
        # Для времени: null / нулевая дата / экзотика / невалидная строка.
        return rng.choice([None, "1970-01-01 00:00:00", "1901-01-01 00:00:00", "2099-12-31 23:59:59", "not-a-date", ""])
    if field_type == "boolean":
        return rng.choice([None, False, True, 0, 1, "UNKNOWN"])
    # string
    return rng.choice([None, "0", "", "-", "N/A", "###ERROR###", "???", "null"])


def _coerce_types(data: pd.DataFrame, fields: list[dict[str, Any]]) -> pd.DataFrame:
    type_map = {
        "int": "Int32",
        "smallint": "Int16",
        "long": "Int64",
        "timestamp": "datetime64[ns]",
        "float": "float64",
        "string": "string",
        "boolean": "boolean",
    }

    for field in fields:
        name = field["name"]
        dtype = type_map.get(field["type"])
        if not dtype:
            raise ValueError(f"Unsupported type in dim_bs config: {field['type']}")

        if field["type"] in {"int", "smallint", "long"}:
            numeric = pd.to_numeric(data[name], errors="coerce")
            numeric = numeric.where(numeric.isna() | ((numeric % 1) == 0))
            bounds = {
                "Int16": np.iinfo(np.int16),
                "Int32": np.iinfo(np.int32),
                "Int64": np.iinfo(np.int64),
            }
            info = bounds[dtype]
            numeric = numeric.where(numeric.isna() | ((numeric >= info.min) & (numeric <= info.max)))
            data[name] = numeric.astype(dtype)
        elif field["type"] == "float":
            data[name] = pd.to_numeric(data[name], errors="coerce").astype(dtype)
        elif field["type"] == "timestamp":
            data[name] = pd.to_datetime(data[name], errors="coerce")
        elif field["type"] == "boolean":
            normalized = data[name].map(
                lambda v: (
                    pd.NA
                    if pd.isna(v)
                    else True
                    if str(v).strip().lower() in {"true", "1", "t", "yes", "y"}
                    else False
                    if str(v).strip().lower() in {"false", "0", "f", "no", "n"}
                    else pd.NA
                )
            )
            data[name] = normalized.astype("boolean")
        else:
            data[name] = data[name].astype("string")
    return data


def _validate_dataset(data: pd.DataFrame, fields: list[dict[str, Any]], subjects: list[str]) -> None:
    required = [field["name"] for field in fields]
    missing = [name for name in required if name not in data.columns]
    if missing:
        raise ValueError(f"Missing required output fields: {missing}")

    if data.empty:
        raise ValueError("Generated dataset is empty")

    # Здесь intentionally permissive: в датасет инжектируется шум для демонстрации DQ.
    non_null_subjects = set(data["subject"].dropna().astype("string").unique().tolist())
    unknown_subjects = sorted(non_null_subjects - set(subjects))
    if unknown_subjects:
        logger.warning("Found unexpected subjects (noise): %s", unknown_subjects[:10])


def _collect_stats(data: pd.DataFrame, params: BuildBsParams, profile: BuildBsProfile | None) -> dict[str, Any]:
    mnc_distribution: dict[str, float] = {}
    if "mnc" in data.columns:
        raw_mnc = (data["mnc"].value_counts(normalize=True) * 100).round(3).to_dict()
        mnc_distribution = {str(key): float(value) for key, value in raw_mnc.items()}

    generation_distribution: dict[str, float] = {}
    if "generation" in data.columns:
        raw_gen = (
            (data["generation"].value_counts(normalize=True) * 100).round(3).to_dict()
        )
        generation_distribution = {str(key): float(value) for key, value in raw_gen.items()}

    border_true_ratio: float | None = None
    if "border" in data.columns:
        border_true_ratio = float(data["border"].fillna(False).astype(bool).mean())

    open_off_rows: int | None = None
    if "date_off" in data.columns:
        date_off = pd.to_datetime(data["date_off"], errors="coerce")
        open_off_rows = int((date_off >= OPEN_BS_DATE_OFF - pd.Timedelta(days=1)).sum())

    return {
        "row_count": int(len(data)),
        "column_count": int(len(data.columns)),
        "date_on_min": str(data["date_on"].min()),
        "date_off_max": str(data["date_off"].max()),
        "open_date_off_rows": open_off_rows,
        "border_true_ratio": border_true_ratio,
        "subjects": params.subjects,
        "operators": params.operators,
        "seed": params.seed,
        "profile_applied": profile is not None,
        "mnc_distribution_pct": mnc_distribution,
        "generation_distribution_pct": generation_distribution,
        "timezone_contract": "Europe/Moscow (naive wall clock)",
    }


def filter_src_bs_active_in_date_range(
    data: pd.DataFrame,
    *,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """Rows overlapping ``[start_date, end_date]`` (``dq-src-bs``, ``build-stg-bs``)."""
    if not {"date_on", "date_off"}.issubset(data.columns):
        return data
    date_on = pd.to_datetime(data["date_on"], errors="coerce")
    date_off = pd.to_datetime(data["date_off"], errors="coerce")
    range_start = pd.Timestamp(datetime.combine(start_date, datetime.min.time()))
    range_end = pd.Timestamp(datetime.combine(end_date, datetime.min.time())) + pd.Timedelta(days=1)
    active = date_on.notna() & (date_on < range_end) & (date_off.isna() | (date_off >= range_start))
    return data.loc[active].copy()


def filter_src_bs_active_on_day(data: pd.DataFrame, *, report_date: date) -> pd.DataFrame:
    """Rows active on a single calendar day (``report_date`` inclusive)."""
    return filter_src_bs_active_in_date_range(
        data,
        start_date=report_date,
        end_date=report_date,
    )
