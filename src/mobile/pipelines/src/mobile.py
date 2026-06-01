"""Mobile OSS vitrines (CDR, SMS, GPRS, location) — build-src-mobile pipeline."""

from __future__ import annotations

import functools
import hashlib
import json
import logging
import math
import random
import re
import sys
import tempfile
import time
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from multiprocessing import Manager
from pathlib import Path
from typing import Any, Final, Iterable, Iterator, Literal, Sequence

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from shapely import wkt as shapely_wkt
from shapely.geometry import Point
from tqdm import tqdm

from mobile.cli_defaults import OPERATORS
from mobile.command_timing import append_command_metrics, timed_stage
from mobile.project_paths import (
    DEFAULT_TIME_ZONES_RAW_PATH,
    PROJECT_ROOT,
    SRC_CDR_LAYOUT_TEMPLATE,
    SRC_GPRS_LAYOUT_TEMPLATE,
    SRC_LOCATION_LAYOUT_TEMPLATE,
    SRC_PERSON_LAYOUT_TEMPLATE,
    SRC_PERSON_SUCCESS_FLAG,
    SRC_SMS_LAYOUT_TEMPLATE,
    mobile_datacenter_ids,
    subject_to_mobile_datacenter,
)

logger = logging.getLogger(__name__)

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


# --- operators ---

OPERATOR_MNC: dict[str, int] = dict(OPERATORS)
MNC_OPERATOR: dict[int, str] = {mnc: op for op, mnc in OPERATOR_MNC.items()}
OPERATORS_ORDER: list[str] = list(OPERATORS.keys())

# Latin slugs for s3_layout {name_operator}
OPERATOR_SLUG: dict[str, str] = {
    "билайн": "beeline",
    "мегафон": "megafon",
    "мтс": "mts",
    "теле2": "tele2",
}


def operator_slug(operator: str) -> str:
    return OPERATOR_SLUG.get(operator, operator)


# --- service types (OCC-018) ---

SERVICE_CDR_WEIGHTS: list[tuple[int, float]] = [
    (1101, 55.0),  # TELEPHONY
    (1102, 3.0),   # EMERGENCY
    (1201, 12.0),  # SMSTPP (mis-filed noise target)
    (1202, 8.0),   # SMSOPP
    (1400, 10.0),  # ALLDATA
    (1000, 7.0),   # ANYTS
    (0, 5.0),      # UNKNOWN
]

SERVICE_GPRS_WEIGHTS: list[tuple[int, float]] = [
    (1400, 40.0),
    (1401, 25.0),
    (1101, 15.0),
    (1000, 12.0),
    (0, 8.0),
]

SERVICE_SMS_EVENT = 10002
SERVICE_CDR_EVENT = 10001
SERVICE_GPRS_EVENT = 10003
SERVICE_LOCATION_EVENT = 10004


def pick_weighted_service(rng: random.Random, mart: str) -> int:
    pool = SERVICE_GPRS_WEIGHTS if mart == "gprs" else SERVICE_CDR_WEIGHTS
    codes = [c for c, _ in pool]
    weights = [w for _, w in pool]
    return int(rng.choices(codes, weights=weights, k=1)[0])


# --- cross-mart (OCC-003) ---

CROSS_MART_ROW_FRACTION = 0.025
WRONG_EVENT_IN_ROW_FRACTION = 0.02


def _wrong_event_for_mart(mart: str, rng: random.Random) -> int:
    choices = [SERVICE_CDR_EVENT, SERVICE_SMS_EVENT, SERVICE_GPRS_EVENT, SERVICE_LOCATION_EVENT]
    canonical = {
        "cdr": SERVICE_CDR_EVENT,
        "sms": SERVICE_SMS_EVENT,
        "gprs": SERVICE_GPRS_EVENT,
        "location": SERVICE_LOCATION_EVENT,
    }[mart]
    foreign = [c for c in choices if c != canonical]
    if rng.random() < 0.85 and foreign:
        return int(rng.choice(foreign))
    return int(rng.choice(choices))


def inject_cross_mart_rows(
    *,
    cdr: list[dict[str, Any]],
    sms: list[dict[str, Any]],
    gprs: list[dict[str, Any]],
    location: list[dict[str, Any]],
    rng: random.Random,
) -> None:
    """Move a small fraction of rows across marts with wrong ``Event`` / ``Service``."""
    pools: dict[str, list[dict[str, Any]]] = {
        "cdr": cdr,
        "sms": sms,
        "gprs": gprs,
        "location": location,
    }
    names = list(pools.keys())
    if not any(pools.values()):
        return

    total = sum(len(v) for v in pools.values())
    n_moves = max(1, int(total * CROSS_MART_ROW_FRACTION))
    for _ in range(n_moves):
        src = rng.choice([n for n in names if pools[n]])
        dst = rng.choice([n for n in names if n != src])
        if not pools[src]:
            continue
        idx = rng.randrange(len(pools[src]))
        row = dict(pools[src].pop(idx))
        row["Event"] = _wrong_event_for_mart(dst, rng)
        if dst in ("cdr", "gprs") and "Service" in row:
            row["Service"] = pick_weighted_service(rng, dst)
        pools[dst].append(row)

    for rows in pools.values():
        for row in rows:
            if rng.random() < WRONG_EVENT_IN_ROW_FRACTION and "Event" in row:
                mart = _infer_mart_from_row(row)
                row["Event"] = _wrong_event_for_mart(mart, rng)
                if mart in ("cdr", "gprs") and "Service" in row:
                    row["Service"] = pick_weighted_service(rng, mart)


def _infer_mart_from_row(row: dict[str, Any]) -> str:
    ev = int(row.get("Event", 0) or 0)
    if ev == SERVICE_SMS_EVENT:
        return "sms"
    if ev == SERVICE_GPRS_EVENT:
        return "gprs"
    if ev == SERVICE_LOCATION_EVENT:
        return "location"
    return "cdr"

# --- I/O ---


def write_mobile_day_parquet_by_datacenter(
    *,
    rows: list[dict[str, Any]],
    fields: list[dict[str, Any]],
    operator: str,
    day: date,
    out_template: str,
    compression: str,
    filename: str,
    coerce_types: Callable[[pd.DataFrame], pd.DataFrame],
    bs_op: pd.DataFrame | None = None,
    region_column: str | None = "RecEntOwnerRegion",
    lac_col: str | None = None,
    cell_col: str | None = None,
    fallback_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """По одному parquet на витрину/день/оператора в каждый ЦОД (каталог ``{dc}``)."""
    buckets = partition_mobile_rows_by_datacenter(
        rows,
        region_column=region_column,
        bs_op=bs_op,
        lac_col=lac_col,
        cell_col=cell_col,
    )
    if not rows and fallback_rows:
        lac_cell_subject = build_bs_lac_cell_to_subject(bs_op) if bs_op is not None else {}
        for fb in fallback_rows:
            dc = datacenter_id_for_mobile_row(
                fb,
                region_column=region_column,
                lac_cell_subject=lac_cell_subject,
                lac_col=lac_col,
                cell_col=cell_col,
            )
            buckets.setdefault(dc, []).append(fb)

    total_rows = 0
    output_paths: list[str] = []
    col_names = [f["name"] for f in fields]

    for dc in mobile_datacenter_ids():
        dc_rows = buckets.get(dc, [])
        if dc_rows:
            data = coerce_types(pd.DataFrame(dc_rows))
        else:
            data = coerce_types(pd.DataFrame(columns=col_names))
        output_path = resolve_mobile_oss_output_path(
            out_template, operator, day, filename, dc=dc
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        data.to_parquet(output_path, compression=compression, index=False)
        total_rows += len(dc_rows)
        output_paths.append(str(output_path))

    return {
        "row_count": total_rows,
        "output_path": output_paths[0] if output_paths else "",
        "output_paths": output_paths,
        "datacenters": list(mobile_datacenter_ids()),
    }

# --- behavior / orchestration ---

import functools
import hashlib
import json
import logging
import math
import random
import re
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import Manager
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
import tempfile
from typing import Any, Callable, Final, Iterable, Iterator, Literal, Sequence

import pandas as pd
from shapely import wkt as shapely_wkt
from shapely.geometry import Point
from tqdm import tqdm

from mobile.project_paths import DEFAULT_TIME_ZONES_RAW_PATH, PROJECT_ROOT

logger = logging.getLogger(__name__)


# Доля «перескоков» на далёкую БС (остальное — локальные соседи по сетке координат).
LONG_JUMP_PROBABILITY = 0.045
# Mobility OSS (калибровка по analyze_oss.xlsx, IMSI, среднее по 3 городам).
MOBILITY_LONG_JUMP_PROBABILITY = 0.03
MOBILITY_INTERPOLATE_ENDPOINT_BIAS = 0.58
MOBILITY_DENSIFY_STEP_MINUTES: dict[str, int] = {"light": 38, "normal": 28, "heavy": 20}
MOBILITY_DENSIFY_MOVING_MULTIPLIER = 0.82
HYPERACTIVE_DAY_PROBABILITY = 0.018
HYPERACTIVE_MULTIPLIER_RANGE = (6.0, 14.0)
PROFILE_MIX_WEIGHTS = [28, 54, 18]
MOBILITY_MODE_WEIGHTS_MOVING = [58, 30, 12]
_ACTIVITY_COUNTS: dict[str, dict[str, dict[str, list[Any]]]] = {
    "light": {
        "cdr": {"choices": [1, 2, 3], "weights": [40, 42, 18]},
        "sms": {"choices": [0, 1, 2], "weights": [30, 50, 20]},
        "gprs": {"choices": [4, 6, 8, 10], "weights": [32, 38, 22, 8]},
    },
    "normal": {
        "cdr": {"choices": [2, 3, 4, 5], "weights": [22, 34, 30, 14]},
        "sms": {"choices": [1, 2, 3, 4], "weights": [18, 32, 34, 16]},
        "gprs": {"choices": [8, 10, 12, 14], "weights": [20, 32, 32, 16]},
    },
    "heavy": {
        "cdr": {"choices": [5, 7, 9, 12, 16, 22], "weights": [10, 18, 24, 22, 16, 10]},
        "sms": {"choices": [3, 5, 7, 9, 12], "weights": [12, 22, 28, 24, 14]},
        "gprs": {"choices": [14, 18, 22, 28, 36, 48], "weights": [12, 18, 24, 22, 16, 8]},
    },
}
_MOVING_ACTIVITY_BONUS = {"cdr": 1, "sms": 1, "gprs": 3}
_NEIGHBOR_GRID_SCALE = 10_000  # ~11 м по широте на единицу сетки
_MAX_NEIGHBORS_PER_BS = 18


DEFAULT_LOCAL_UTC_OFFSET_HOURS: Final[int] = 3
_TZ_SUBJECT_COL = "name"
_TZ_OFFSET_COL = "timezone"
_TZ_GEOMETRY_COL = "geometry"
_BS_LOCAL_OFFSET_COL = "_local_utc_offset_hours"


@functools.lru_cache(maxsize=1)
def _time_zone_rows_by_subject() -> dict[str, list[tuple[int, Any, tuple[float, float, float, float] | None]]]:
    """Load time zone polygons from raw CSV once; group by lower-cased subject name."""
    path = DEFAULT_TIME_ZONES_RAW_PATH
    if not path.exists():
        logger.warning("time_zones.csv not found: %s", path)
        return {}
    try:
        frame = pd.read_csv(path, sep=";", usecols=[_TZ_SUBJECT_COL, _TZ_OFFSET_COL, _TZ_GEOMETRY_COL], encoding="utf-8")
    except Exception as exc:
        logger.warning("Failed to read %s: %s", path, exc)
        return {}
    out: dict[str, list[tuple[int, Any, tuple[float, float, float, float] | None]]] = {}
    for row in frame.itertuples(index=False):
        subject = str(getattr(row, _TZ_SUBJECT_COL, "")).strip().lower()
        if not subject:
            continue
        try:
            offset = int(getattr(row, _TZ_OFFSET_COL))
        except Exception:
            continue
        geom_raw = getattr(row, _TZ_GEOMETRY_COL, None)
        geom = None
        bounds = None
        if geom_raw is not None and not pd.isna(geom_raw):
            try:
                geom = shapely_wkt.loads(str(geom_raw))
                bounds = geom.bounds
            except Exception:
                geom = None
                bounds = None
        out.setdefault(subject, []).append((offset, geom, bounds))
    return out


def _infer_local_utc_offset_hours(subject: Any, lon: Any, lat: Any) -> int:
    if subject is None or (isinstance(subject, float) and pd.isna(subject)) or pd.isna(subject):
        subject_key = ""
    else:
        subject_key = str(subject).strip().lower()
    candidates = _time_zone_rows_by_subject().get(subject_key, [])
    if not candidates:
        return DEFAULT_LOCAL_UTC_OFFSET_HOURS
    if pd.isna(lon) or pd.isna(lat):
        return int(candidates[0][0])
    try:
        lon_f = float(lon)
        lat_f = float(lat)
    except Exception:
        return int(candidates[0][0])
    point = Point(lon_f, lat_f)
    for offset, geom, bounds in candidates:
        if geom is None:
            continue
        if bounds is not None:
            minx, miny, maxx, maxy = bounds
            if not (minx <= lon_f <= maxx and miny <= lat_f <= maxy):
                continue
        try:
            if geom.contains(point) or geom.touches(point):
                return int(offset)
        except Exception:
            continue
    return int(candidates[0][0])


def ensure_bs_local_offset_column(bs_frame: pd.DataFrame) -> pd.DataFrame:
    """Add `_local_utc_offset_hours` to BS frame once for mobile event local time."""
    if bs_frame.empty:
        return bs_frame
    if _BS_LOCAL_OFFSET_COL in bs_frame.columns and bs_frame[_BS_LOCAL_OFFSET_COL].notna().all():
        return bs_frame
    work = bs_frame.copy()
    work[_BS_LOCAL_OFFSET_COL] = [
        _infer_local_utc_offset_hours(row.get("subject"), row.get("coord_x"), row.get("coord_y"))
        for _, row in work.iterrows()
    ]
    work[_BS_LOCAL_OFFSET_COL] = pd.to_numeric(work[_BS_LOCAL_OFFSET_COL], errors="coerce").fillna(DEFAULT_LOCAL_UTC_OFFSET_HOURS).astype(int)
    return work


def bs_local_utc_offset_hours(bs_row: pd.Series) -> int:
    val = bs_row.get(_BS_LOCAL_OFFSET_COL)
    if val is not None and not pd.isna(val):
        try:
            return int(val)
        except Exception:
            pass
    return _infer_local_utc_offset_hours(bs_row.get("subject"), bs_row.get("coord_x"), bs_row.get("coord_y"))


@dataclass(frozen=True)
class SubscriberDayState:
    home_operator: str
    serving_operator: str
    local_id: int
    msisdn: str
    imsi: str
    imei: str
    started_dt: datetime
    duration_sec: int
    moving: bool
    start_bs_idx: int
    end_bs_idx: int
    #: Основной субъект РФ (``src_bs.subject``) — почти все события дня в этом регионе.
    primary_subject: str
    profile_score: float
    mobility_mode: str
    #: Второй регион только для поездки (``end_bs`` / якоря визита); ``None`` — один регион.
    visit_subject: str | None = None
    actually_from: datetime | None = None
    actually_to: datetime | None = None


@dataclass(frozen=True)
class JourneyPoint:
    timestamp: datetime
    bs_idx: int


@dataclass
class BsSpatialContext:
    """Координаты БС (iloc) и списки соседей по грубой пространственной сетке."""

    lon: list[float]
    lat: list[float]
    neighbor_lists: list[list[int]]


@dataclass(frozen=True)
class BsRegionContext:
    """Индексы БС по субъекту (региону) для region-first выбора сот."""

    subjects_by_idx: tuple[str, ...]
    indices_by_subject: dict[str, tuple[int, ...]]


# Вероятность поездки во второй регион при moving (если в справочнике >1 субъекта).
CROSS_REGION_TRIP_PROBABILITY: Final[float] = 0.22


def _build_bs_region_context(bs_op: pd.DataFrame) -> BsRegionContext:
    subjects: list[str] = []
    by_subject: dict[str, list[int]] = {}
    subcol = bs_op.get("subject")
    if subcol is None:
        fallback = "unknown"
        return BsRegionContext(subjects_by_idx=tuple([fallback] * len(bs_op)), indices_by_subject={fallback: tuple(range(len(bs_op)))})
    for i, raw in enumerate(subcol.astype("string").fillna("unknown")):
        name = str(raw).strip() or "unknown"
        subjects.append(name)
        by_subject.setdefault(name, []).append(i)
    return BsRegionContext(
        subjects_by_idx=tuple(subjects),
        indices_by_subject={k: tuple(v) for k, v in by_subject.items()},
    )


def _pick_bs_in_subject(region_ctx: BsRegionContext, subject: str, rng: random.Random) -> int:
    pool = region_ctx.indices_by_subject.get(subject) or ()
    if not pool:
        all_idx = list(range(len(region_ctx.subjects_by_idx)))
        return int(rng.choice(all_idx))
    return int(rng.choice(pool))


def _pick_subscriber_region_plan(
    region_ctx: BsRegionContext,
    rng: random.Random,
    *,
    moving: bool,
    mobility_mode: str,
    subscriber_key: str | None = None,
) -> tuple[str, str | None]:
    """Один ``primary_subject`` на абонента (``subscriber_key``); ``visit_subject`` — редкая поездка."""
    subjects = [s for s, idxs in region_ctx.indices_by_subject.items() if idxs]
    if not subjects:
        primary = region_ctx.subjects_by_idx[0] if region_ctx.subjects_by_idx else "unknown"
        return primary, None
    weights = [len(region_ctx.indices_by_subject[s]) for s in subjects]
    pick_rng = make_rng("primary_subject", subscriber_key) if subscriber_key else rng
    primary = str(pick_rng.choices(subjects, weights=weights, k=1)[0])
    visit: str | None = None
    if (
        moving
        and mobility_mode in ("commuter", "explorer", "hypermobile")
        and len(subjects) > 1
        and rng.random() < CROSS_REGION_TRIP_PROBABILITY
    ):
        others = [s for s in subjects if s != primary]
        if others:
            visit = str(rng.choice(others))
    return primary, visit


def _neighbor_indices_in_subject(
    ctx: BsSpatialContext,
    region_ctx: BsRegionContext,
    from_idx: int,
    subject: str,
) -> list[int]:
    neigh = ctx.neighbor_lists[from_idx] if from_idx < len(ctx.neighbor_lists) else [from_idx]
    filtered = [j for j in neigh if region_ctx.subjects_by_idx[j] == subject]
    if filtered:
        return filtered
    pool = list(region_ctx.indices_by_subject.get(subject, ()))
    return pool if pool else [from_idx]


def _pick_long_jump_in_subject(
    region_ctx: BsRegionContext,
    subject: str,
    from_idx: int,
    rng: random.Random,
) -> int:
    pool = [j for j in region_ctx.indices_by_subject.get(subject, ()) if j != from_idx]
    if pool:
        return int(rng.choice(pool))
    return from_idx


def stable_seed(*parts: object) -> int:
    data = "|".join(str(p) for p in parts).encode("utf-8")
    return int(hashlib.sha256(data).hexdigest()[:16], 16) % (2**32)


def make_rng(*parts: object) -> random.Random:
    return random.Random(stable_seed(*parts))


def _build_bs_spatial_context(bs_op: pd.DataFrame) -> BsSpatialContext | None:
    if bs_op is None or len(bs_op) == 0:
        return None
    lon = pd.to_numeric(bs_op.get("coord_x"), errors="coerce").astype(float).tolist()
    lat = pd.to_numeric(bs_op.get("coord_y"), errors="coerce").astype(float).tolist()
    n = len(lon)
    if n == 0:
        return None
    if all((math.isnan(lon[i]) or math.isnan(lat[i])) for i in range(n)):
        return None

    buckets: dict[tuple[int, int], list[int]] = {}
    for i in range(n):
        lo, la = lon[i], lat[i]
        if math.isnan(lo) or math.isnan(la):
            continue
        key = (int(round(la * _NEIGHBOR_GRID_SCALE)), int(round(lo * _NEIGHBOR_GRID_SCALE)))
        buckets.setdefault(key, []).append(i)

    def _sq(i: int, j: int) -> float:
        dlo = lon[i] - lon[j]
        dla = lat[i] - lat[j]
        return dlo * dlo + dla * dla

    neighbor_lists: list[list[int]] = []
    for i in range(n):
        lo, la = lon[i], lat[i]
        if math.isnan(lo) or math.isnan(la):
            neighbor_lists.append([i])
            continue
        cand: list[int] = []
        for dla in (-1, 0, 1):
            for dlo in (-1, 0, 1):
                key = (
                    int(round(la * _NEIGHBOR_GRID_SCALE)) + dla,
                    int(round(lo * _NEIGHBOR_GRID_SCALE)) + dlo,
                )
                cand.extend(buckets.get(key, []))
        seen: set[int] = set()
        uniq: list[int] = []
        for j in cand:
            if j not in seen:
                seen.add(j)
                uniq.append(j)
        if i not in seen:
            uniq.append(i)
        uniq.sort(key=lambda j: _sq(i, j))
        neighbor_lists.append(uniq[: max(4, min(_MAX_NEIGHBORS_PER_BS, len(uniq)))])
    return BsSpatialContext(lon=lon, lat=lat, neighbor_lists=neighbor_lists)


def _pick_long_jump_idx(bs_count: int, from_idx: int, rng: random.Random) -> int:
    if bs_count <= 1:
        return 0
    for _ in range(8):
        j = int(rng.randrange(bs_count))
        if j != from_idx:
            return j
    return (from_idx + 1) % bs_count


def _pick_neighbor_biased(
    ctx: BsSpatialContext,
    from_idx: int,
    steer_lon: float,
    steer_lat: float,
    rng: random.Random,
    *,
    long_jump_prob: float = LONG_JUMP_PROBABILITY,
    region_ctx: BsRegionContext | None = None,
    region_subject: str | None = None,
) -> int:
    n = len(ctx.lon)
    if n <= 1:
        return 0
    if region_ctx is not None and region_subject:
        if rng.random() < long_jump_prob:
            return _pick_long_jump_in_subject(region_ctx, region_subject, from_idx, rng)
        neigh = _neighbor_indices_in_subject(ctx, region_ctx, from_idx, region_subject)
    else:
        if rng.random() < long_jump_prob:
            return _pick_long_jump_idx(n, from_idx, rng)
        neigh = ctx.neighbor_lists[from_idx] if from_idx < len(ctx.neighbor_lists) else [from_idx]
    if not neigh:
        return from_idx
    weights: list[float] = []
    for j in neigh:
        d = (ctx.lon[j] - steer_lon) ** 2 + (ctx.lat[j] - steer_lat) ** 2
        weights.append(1.0 / (1e-8 + math.sqrt(d)))
    return int(rng.choices(neigh, weights=weights, k=1)[0])


def active_subscribers_for_day(
    *,
    operator: str,
    day: date,
    seed: int,
    bs_op: pd.DataFrame,
    aab_per_operator: int,
    active_ratio: float,
    transition_ratio: float,
    movement_ratio: float,
) -> list[SubscriberDayState]:
    active_ratio = max(0.0, min(1.0, float(active_ratio)))
    transition_ratio = max(0.0, min(0.5, float(transition_ratio)))
    movement_ratio = max(0.0, min(1.0, float(movement_ratio)))
    active_count = max(1, int(aab_per_operator * active_ratio))
    transition_count = int(active_count * transition_ratio)

    day_start = datetime.combine(day, datetime.min.time())
    rng = make_rng("active", operator, day.isoformat(), seed)
    local_ids = rng.sample(range(aab_per_operator), k=active_count)
    spatial_ctx = _build_bs_spatial_context(bs_op)
    region_ctx = _build_bs_region_context(bs_op)

    states: list[SubscriberDayState] = []
    for i, sid in enumerate(local_ids):
        if i < transition_count:
            home_operator = _neighbor_operator(operator, sid)
        else:
            home_operator = operator
        prof_rng = make_rng("profile", home_operator, sid, day.isoformat(), seed)
        moving = prof_rng.random() < movement_ratio
        mobility_mode = _pick_mobility_mode(moving, prof_rng)
        msisdn = _msisdn(home_operator, sid)
        imsi = _imsi(home_operator, sid)
        imei = _imei(home_operator, sid)
        primary_subject, visit_subject = _pick_subscriber_region_plan(
            region_ctx,
            prof_rng,
            moving=moving,
            mobility_mode=mobility_mode,
            subscriber_key=imsi,
        )
        started_dt = _sample_started_dt(day_start, prof_rng)
        duration_sec = prof_rng.choices(
            [5, 10, 20, 30, 60, 120, 300, 600, 1200, 1800, 3600],
            weights=[2, 3, 5, 8, 12, 14, 18, 16, 10, 7, 5],
            k=1,
        )[0]

        start_idx = _pick_bs_in_subject(region_ctx, primary_subject, prof_rng)
        end_idx = _pick_end_bs_idx(
            bs_op,
            start_idx,
            moving,
            prof_rng,
            ctx=spatial_ctx,
            region_ctx=region_ctx,
            primary_subject=primary_subject,
            visit_subject=visit_subject,
        )
        states.append(
            SubscriberDayState(
                home_operator=home_operator,
                serving_operator=operator,
                local_id=sid,
                msisdn=msisdn,
                imsi=imsi,
                imei=imei,
                started_dt=started_dt,
                duration_sec=duration_sec,
                moving=moving,
                start_bs_idx=start_idx,
                end_bs_idx=end_idx,
                primary_subject=primary_subject,
                visit_subject=visit_subject,
                profile_score=prof_rng.random(),
                mobility_mode=mobility_mode,
            )
        )
    return states


def _neighbor_operator(operator: str, sid: int) -> str:
    if operator not in OPERATORS_ORDER:
        return operator
    idx = OPERATORS_ORDER.index(operator)
    shift = 1 if sid % 2 == 0 else -1
    return OPERATORS_ORDER[(idx + shift) % len(OPERATORS_ORDER)]


def _sample_started_dt(day_start: datetime, rng: random.Random) -> datetime:
    hour = rng.choices(
        population=list(range(24)),
        weights=[1, 1, 1, 1, 1, 2, 4, 6, 7, 8, 7, 6, 5, 5, 6, 7, 8, 9, 10, 9, 7, 5, 3, 2],
        k=1,
    )[0]
    return day_start + timedelta(hours=hour, minutes=rng.randint(0, 59), seconds=rng.randint(0, 59))


def _pick_end_bs_idx(
    bs_op: pd.DataFrame,
    start_idx: int,
    moving: bool,
    rng: random.Random,
    *,
    ctx: BsSpatialContext | None = None,
    region_ctx: BsRegionContext | None = None,
    primary_subject: str | None = None,
    visit_subject: str | None = None,
) -> int:
    if not moving or len(bs_op) <= 1:
        return start_idx
    target_subject = visit_subject if visit_subject else primary_subject
    if target_subject and region_ctx is not None:
        if visit_subject:
            return _pick_bs_in_subject(region_ctx, visit_subject, rng)
        if ctx is not None and len(ctx.lon) == len(bs_op):
            anchor = _pick_bs_in_subject(region_ctx, primary_subject or target_subject, rng)
            a = 0.55 + 0.35 * rng.random()
            steer_lon = ctx.lon[start_idx] * a + ctx.lon[anchor] * (1.0 - a)
            steer_lat = ctx.lat[start_idx] * a + ctx.lat[anchor] * (1.0 - a)
            return _pick_neighbor_biased(
                ctx,
                start_idx,
                steer_lon,
                steer_lat,
                rng,
                region_ctx=region_ctx,
                region_subject=primary_subject or target_subject,
            )
    if ctx is not None and len(ctx.lon) == len(bs_op):
        if rng.random() < LONG_JUMP_PROBABILITY:
            if region_ctx and primary_subject:
                return _pick_long_jump_in_subject(region_ctx, primary_subject, start_idx, rng)
            return _pick_long_jump_idx(len(bs_op), start_idx, rng)
        anchor = int(rng.randrange(len(bs_op)))
        a = 0.55 + 0.35 * rng.random()
        steer_lon = ctx.lon[start_idx] * a + ctx.lon[anchor] * (1.0 - a)
        steer_lat = ctx.lat[start_idx] * a + ctx.lat[anchor] * (1.0 - a)
        return _pick_neighbor_biased(
            ctx,
            start_idx,
            steer_lon,
            steer_lat,
            rng,
            region_ctx=region_ctx,
            region_subject=primary_subject,
        )
    start = bs_op.iloc[start_idx]
    scope = bs_op
    if primary_subject and "subject" in bs_op.columns:
        scope = bs_op[bs_op["subject"] == primary_subject]
    candidates = scope[(scope["lac"] != start.get("lac")) | (scope["cell"] != start.get("cell"))]
    if candidates.empty:
        return start_idx
    return int(candidates.sample(1, random_state=rng.randint(0, 2**31 - 1)).index[0])


# (country_code, NSN length) — E.164 без «+», для синтетики роуминга / международных MSISDN
_INTERNATIONAL_MSISDN_PROFILES: tuple[tuple[str, int], ...] = (
    ("49", 10),  # DE
    ("1", 10),  # US/CA
    ("44", 10),  # UK
    ("33", 9),  # FR
    ("380", 9),  # UA
    ("375", 9),  # BY
    ("374", 8),  # AM
    ("996", 9),  # KG
)

# IMSI визитёров (MCC ≠ 250) — доля абонентов в выборке активных за день
_ROAMING_IMSI_SHARE_PCT = 5
_INTERNATIONAL_MSISDN_SHARE_PCT = 18


def _international_msisdn_e164(operator: str, sid: int) -> str:
    profiles = _INTERNATIONAL_MSISDN_PROFILES
    cc, nsn_len = profiles[stable_seed("intl_cc", operator, sid) % len(profiles)]
    floor = 10 ** (nsn_len - 1)
    ceiling = 10**nsn_len - 1
    span = ceiling - floor + 1
    nsn = floor + (stable_seed("intl_nsn", operator, sid) % span)
    return f"+{cc}{nsn}"


def _msisdn(operator: str, sid: int) -> str:
    if stable_seed("msisdn_intl", operator, sid) % 100 < _INTERNATIONAL_MSISDN_SHARE_PCT:
        return _international_msisdn_e164(operator, sid)
    op_code = OPERATORS_ORDER.index(operator) + 1 if operator in OPERATORS_ORDER else 9
    base = 10_000_000 + sid
    return f"+79{op_code}{base:08d}"[:12]


def _roaming_imsi(operator: str, sid: int) -> str:
    mcc_profiles = ("262", "228", "234", "232", "206")
    mcc = mcc_profiles[stable_seed("roam_mcc", operator, sid) % len(mcc_profiles)]
    mnc = 1 + (stable_seed("roam_mnc", operator, sid) % 99)
    msin = (1_000_000_000 + stable_seed("roam_msin", operator, sid)) % 10_000_000_000
    return f"{mcc}{mnc:02d}{msin:010d}"


def _imsi(operator: str, sid: int) -> str:
    if stable_seed("imsi_roam", operator, sid) % 100 < _ROAMING_IMSI_SHARE_PCT:
        return _roaming_imsi(operator, sid)
    mnc = OPERATOR_MNC.get(operator, 99)
    return f"250{mnc:02d}{(1_000_000_000 + sid) % 10_000_000_000:010d}"


def _imei(operator: str, sid: int) -> str:
    op = OPERATOR_MNC.get(operator, 99)
    return f"{35_000_000_000_000 + op * 1_000_000_000 + sid:015d}"[:15]


def choose_states(states: Iterable[SubscriberDayState], threshold: float, shift: float = 0.0) -> list[SubscriberDayState]:
    return [s for s in states if (s.profile_score + shift) % 1.0 < threshold]


MOBILE_OSS_SUBSCRIBER_CHUNK_SIZE: Final[int] = 10_000

# Per subscriber: activity counts, comms journey (CDR/SMS/GPRS), denser mobility journey (location).
SubscriberActivityJourneyBundle = tuple[
    SubscriberDayState,
    dict[str, int],
    list[JourneyPoint],
    list[JourneyPoint],
]


def filter_bundles_by_profile_threshold(
    bundles: Sequence[SubscriberActivityJourneyBundle],
    threshold: float,
    shift: float = 0.0,
) -> list[SubscriberActivityJourneyBundle]:
    return [b for b in bundles if (b[0].profile_score + shift) % 1.0 < threshold]


def build_subscriber_activity_journey_bundles(
    states: Sequence[SubscriberDayState],
    *,
    bs_op: pd.DataFrame,
    day: date,
    seed: int,
    spatial_ctx: BsSpatialContext | None,
) -> list[SubscriberActivityJourneyBundle]:
    """Compute activity + a single comms journey per subscriber (used by mobile OSS fast path)."""
    if not states:
        return []
    bs_op = bs_op.reset_index(drop=True)
    n = len(bs_op)
    out: list[SubscriberActivityJourneyBundle] = []
    for s in states:
        activity = subscriber_daily_activity(s, day=day, seed=seed)
        journey = subscriber_journey_points(
            s,
            day=day,
            seed=seed,
            bs_count=n,
            bs_op=bs_op,
            spatial_ctx=spatial_ctx,
            rng_namespace="journey",
        )
        mobility = subscriber_journey_points(
            s,
            day=day,
            seed=seed,
            bs_count=n,
            bs_op=bs_op,
            spatial_ctx=spatial_ctx,
            rng_namespace="silent_mobility",
        )
        out.append((s, activity, journey, mobility))
    return out


def _pick_activity_count(rng: random.Random, profile: str, key: str) -> int:
    spec = _ACTIVITY_COUNTS[profile][key]
    return int(rng.choices(spec["choices"], weights=spec["weights"], k=1)[0])


def subscriber_daily_activity(
    state: SubscriberDayState,
    *,
    day: date,
    seed: int,
) -> dict[str, int]:
    rng = make_rng("activity", state.imsi, state.msisdn, day.isoformat(), seed)
    profile = _subscriber_profile(state, day=day, seed=seed)
    voice = _pick_activity_count(rng, profile, "cdr")
    sms = _pick_activity_count(rng, profile, "sms")
    gprs = _pick_activity_count(rng, profile, "gprs")
    if state.moving:
        voice += _MOVING_ACTIVITY_BONUS["cdr"]
        sms += _MOVING_ACTIVITY_BONUS["sms"]
        gprs += _MOVING_ACTIVITY_BONUS["gprs"]
    if rng.random() < HYPERACTIVE_DAY_PROBABILITY:
        lo, hi = HYPERACTIVE_MULTIPLIER_RANGE
        mult = rng.uniform(lo, hi)
        voice = int(max(1, voice * mult))
        sms = int(max(0, sms * mult))
        gprs = int(max(1, gprs * mult))
    return {
        "cdr_calls": int(max(1, voice)),
        "sms_msgs": int(max(0, sms)),
        "gprs_sessions": int(max(1, gprs)),
    }


def mobility_dwell_update_minutes(
    *,
    profile_name: str,
    moving: bool,
    rng: random.Random,
) -> int:
    base = LOCATION_DWELL_UPDATE_MINUTES
    if rng.random() < HYPERACTIVE_DAY_PROBABILITY * 2.5:
        return max(8, int(base * rng.uniform(0.35, 0.55)))
    if profile_name == "heavy" and moving:
        return max(10, int(base * 0.85))
    return base


def _mobility_densify_target_step_sec(profile: str, *, moving: bool) -> int:
    minutes = MOBILITY_DENSIFY_STEP_MINUTES[profile]
    if moving:
        minutes = max(8, int(minutes * MOBILITY_DENSIFY_MOVING_MULTIPLIER))
    return max(180, minutes * 60)


def subscriber_journey_points(
    state: SubscriberDayState,
    *,
    day: date,
    seed: int,
    bs_count: int,
    bs_op: pd.DataFrame | None = None,
    spatial_ctx: BsSpatialContext | None = None,
    rng_namespace: str = "journey",
) -> list[JourneyPoint]:
    """``rng_namespace`` separates RNG streams (e.g. ``silent_mobility`` for location vs comms ``journey``)."""
    rng = make_rng(rng_namespace, state.imsi, state.msisdn, day.isoformat(), seed)
    profile = _subscriber_profile(state, day=day, seed=seed)
    if spatial_ctx is not None and len(spatial_ctx.lon) == bs_count:
        ctx = spatial_ctx
    elif bs_op is not None and len(bs_op) == bs_count:
        ctx = _build_bs_spatial_context(bs_op)
    else:
        ctx = None
    if ctx is not None and len(ctx.lon) != bs_count:
        ctx = None
    max_idx = max(1, bs_count) - 1
    home_bs = int(state.start_bs_idx) % max(1, bs_count)
    work_bs = int(state.end_bs_idx) % max(1, bs_count)
    region_ctx = _build_bs_region_context(bs_op) if bs_op is not None and len(bs_op) == bs_count else None
    if work_bs == home_bs and max_idx > 0 and region_ctx is not None:
        alt_subject = state.visit_subject or state.primary_subject
        pool = [j for j in region_ctx.indices_by_subject.get(alt_subject, ()) if j != home_bs]
        if pool:
            work_bs = int(rng.choice(pool))
        else:
            work_bs = (home_bs + 1 + (stable_seed("work_bs", state.imsi, day.isoformat(), seed) % max_idx)) % max(1, bs_count)

    points = _journey_template_by_mode(
        day=day,
        profile=profile,
        mobility_mode=state.mobility_mode,
        home_bs=home_bs,
        work_bs=work_bs,
        bs_count=bs_count,
        rng=rng,
        ctx=ctx,
        region_ctx=region_ctx,
        primary_subject=state.primary_subject,
        visit_subject=state.visit_subject,
    )

    points = sorted(points, key=lambda p: p.timestamp)
    deduped: list[JourneyPoint] = []
    for p in points:
        if deduped and deduped[-1].timestamp == p.timestamp and deduped[-1].bs_idx == p.bs_idx:
            continue
        deduped.append(p)
    density: Literal["comms", "mobility"] = "mobility" if rng_namespace == "silent_mobility" else "comms"
    return _densify_journey_points(
        deduped,
        state=state,
        day=day,
        seed=seed,
        bs_count=bs_count,
        profile=profile,
        ctx=ctx,
        region_ctx=region_ctx,
        rng_namespace=rng_namespace,
        density=density,
    )


def iter_spread_journey_event_segments(
    journey: list[JourneyPoint],
    event_count: int,
    *,
    rng: random.Random,
) -> Iterator[tuple[JourneyPoint, JourneyPoint]]:
    """Spread comms events across the day instead of cycling the first journey anchors."""
    if event_count <= 0 or not journey:
        return
    if len(journey) == 1:
        p = journey[0]
        for _ in range(event_count):
            yield p, p
        return

    t0 = journey[0].timestamp
    span_sec = max(1, int((journey[-1].timestamp - t0).total_seconds()))

    def segment_at(sec: int) -> tuple[JourneyPoint, JourneyPoint]:
        target = t0 + timedelta(seconds=min(max(0, sec), span_sec))
        for i in range(len(journey) - 1):
            left, right = journey[i], journey[i + 1]
            if right.timestamp >= target:
                return left, right
        return journey[-2], journey[-1]

    for k in range(event_count):
        frac = (k + 0.5) / event_count + rng.uniform(-0.035, 0.035)
        frac = max(0.0, min(1.0, frac))
        yield segment_at(int(frac * span_sec))


def spread_journey_points_for_events(
    journey: list[JourneyPoint],
    event_count: int,
    *,
    rng: random.Random,
) -> list[JourneyPoint]:
    """One anchor point per event, evenly spread on the journey timeline."""
    return [left for left, _ in iter_spread_journey_event_segments(journey, event_count, rng=rng)]


@functools.lru_cache(maxsize=400_000)
def _subscriber_profile_cached(imsi: str, msisdn: str, day_iso: str, seed: int) -> str:
    rng = make_rng("profile_bucket", imsi, msisdn, day_iso, seed)
    return rng.choices(
        population=["light", "normal", "heavy"],
        weights=PROFILE_MIX_WEIGHTS,
        k=1,
    )[0]


def _subscriber_profile(state: SubscriberDayState, *, day: date, seed: int) -> str:
    return _subscriber_profile_cached(state.imsi, state.msisdn, day.isoformat(), seed)


def _dt_in_window(day: date, start_h: int, start_m: int, end_h: int, end_m: int, rng: random.Random) -> datetime:
    start = datetime.combine(day, datetime.min.time()) + timedelta(hours=start_h, minutes=start_m)
    end = datetime.combine(day, datetime.min.time()) + timedelta(hours=end_h, minutes=end_m)
    if end <= start:
        return start
    delta_sec = int((end - start).total_seconds())
    return start + timedelta(seconds=rng.randint(0, delta_sec))


def _transition_bs(
    from_idx: int,
    to_idx: int,
    bs_count: int,
    rng: random.Random,
    ctx: BsSpatialContext | None = None,
    *,
    region_ctx: BsRegionContext | None = None,
    from_subject: str | None = None,
    to_subject: str | None = None,
) -> int:
    if bs_count <= 1:
        return 0
    cross_region = (
        region_ctx is not None
        and from_subject
        and to_subject
        and from_subject != to_subject
    )
    if ctx is not None and len(ctx.lon) == bs_count:
        if from_idx == to_idx:
            if cross_region:
                return int(to_idx)
            if rng.random() < LONG_JUMP_PROBABILITY and from_subject:
                if region_ctx:
                    return _pick_long_jump_in_subject(region_ctx, from_subject, from_idx, rng)
                return _pick_long_jump_idx(bs_count, from_idx, rng)
            jiggle_lon = ctx.lon[from_idx] + rng.uniform(-0.015, 0.015)
            jiggle_lat = ctx.lat[from_idx] + rng.uniform(-0.015, 0.015)
            return _pick_neighbor_biased(
                ctx,
                from_idx,
                jiggle_lon,
                jiggle_lat,
                rng,
                region_ctx=region_ctx,
                region_subject=from_subject,
            )
        if cross_region and rng.random() < 0.42:
            return int(to_idx)
        t = 0.18 + 0.55 * rng.random()
        steer_lon = ctx.lon[from_idx] + (ctx.lon[to_idx] - ctx.lon[from_idx]) * t
        steer_lat = ctx.lat[from_idx] + (ctx.lat[to_idx] - ctx.lat[from_idx]) * t
        return _pick_neighbor_biased(
            ctx,
            from_idx,
            steer_lon,
            steer_lat,
            rng,
            region_ctx=region_ctx,
            region_subject=from_subject if cross_region else (from_subject or to_subject),
        )
    if from_idx == to_idx:
        if cross_region and region_ctx and to_subject:
            return _pick_bs_in_subject(region_ctx, to_subject, rng)
        if region_ctx and from_subject:
            return _pick_bs_in_subject(region_ctx, from_subject, rng)
        return int(rng.randrange(bs_count))
    if abs(to_idx - from_idx) <= 2:
        return int(to_idx)
    step = (to_idx - from_idx) // 2
    cand = from_idx + step
    if cand < 0 or cand >= bs_count:
        if region_ctx and from_subject:
            return _pick_bs_in_subject(region_ctx, from_subject, rng)
        return int(from_idx)
    if region_ctx and from_subject and not cross_region:
        subj = region_ctx.subjects_by_idx[cand]
        if subj != from_subject:
            return _pick_bs_in_subject(region_ctx, from_subject, rng)
    return int(cand)


def _densify_journey_points(
    points: list[JourneyPoint],
    *,
    state: SubscriberDayState,
    day: date,
    seed: int,
    bs_count: int,
    profile: str,
    ctx: BsSpatialContext | None = None,
    region_ctx: BsRegionContext | None = None,
    rng_namespace: str = "journey",
    density: Literal["comms", "mobility"] = "comms",
) -> list[JourneyPoint]:
    if len(points) <= 1 or bs_count <= 1:
        return points

    rng = make_rng(f"{rng_namespace}_dense", state.imsi, state.msisdn, day.isoformat(), seed)
    if density == "mobility":
        target_step_sec = _mobility_densify_target_step_sec(profile, moving=state.moving)
        min_step_sec = 180
        ctx_scale = 0.55
        long_jump_prob = MOBILITY_LONG_JUMP_PROBABILITY
        endpoint_bias = MOBILITY_INTERPOLATE_ENDPOINT_BIAS
    else:
        target_step_sec = None
        long_jump_prob = LONG_JUMP_PROBABILITY
        endpoint_bias = 0.0
        target_step_minutes = 28 if profile == "light" else 16 if profile == "normal" else 10
        min_step_sec = 240
        ctx_scale = 0.65
        if state.moving:
            target_step_minutes = max(6, target_step_minutes // 2)
        target_step_sec = max(min_step_sec, target_step_minutes * 60)
    if ctx is not None:
        target_step_sec = max(min_step_sec, int(target_step_sec * ctx_scale))

    dense: list[JourneyPoint] = [points[0]]
    for left, right in zip(points, points[1:], strict=False):
        delta_sec = int((right.timestamp - left.timestamp).total_seconds())
        if delta_sec <= target_step_sec:
            dense.append(right)
            continue

        hop_count = max(1, delta_sec // target_step_sec)
        for hop in range(1, hop_count + 1):
            frac = hop / (hop_count + 1)
            ts = left.timestamp + timedelta(seconds=int(delta_sec * frac))
            bs_idx = _interpolate_bs_idx(
                left.bs_idx,
                right.bs_idx,
                frac=frac,
                bs_count=bs_count,
                rng=rng,
                ctx=ctx,
                region_ctx=region_ctx,
                long_jump_prob=long_jump_prob,
                endpoint_bias=endpoint_bias,
            )
            dense.append(JourneyPoint(timestamp=ts, bs_idx=bs_idx))
        dense.append(right)

    dense = sorted(dense, key=lambda p: p.timestamp)
    compact: list[JourneyPoint] = []
    for p in dense:
        if compact and compact[-1].timestamp == p.timestamp and compact[-1].bs_idx == p.bs_idx:
            continue
        compact.append(p)
    return compact


def _interpolate_bs_idx(
    start_idx: int,
    end_idx: int,
    *,
    frac: float,
    bs_count: int,
    rng: random.Random,
    ctx: BsSpatialContext | None = None,
    region_ctx: BsRegionContext | None = None,
    long_jump_prob: float = LONG_JUMP_PROBABILITY,
    endpoint_bias: float = 0.0,
) -> int:
    if bs_count <= 1:
        return 0
    start_subj = region_ctx.subjects_by_idx[start_idx] if region_ctx else None
    end_subj = region_ctx.subjects_by_idx[end_idx] if region_ctx else None
    cross_region = start_subj and end_subj and start_subj != end_subj
    active_subj = end_subj if cross_region and frac >= 0.72 else start_subj
    if endpoint_bias > 0.0 and rng.random() < endpoint_bias:
        return int(start_idx if rng.random() < 0.5 else end_idx)
    if ctx is not None and len(ctx.lon) == bs_count:
        if cross_region and frac >= 0.72:
            return int(end_idx)
        if rng.random() < long_jump_prob:
            if region_ctx and active_subj:
                return _pick_long_jump_in_subject(region_ctx, active_subj, start_idx, rng)
            return _pick_long_jump_idx(bs_count, start_idx, rng)
        if start_idx == end_idx:
            jiggle_lon = ctx.lon[start_idx] + rng.uniform(-0.02, 0.02)
            jiggle_lat = ctx.lat[start_idx] + rng.uniform(-0.02, 0.02)
            return _pick_neighbor_biased(
                ctx,
                start_idx,
                jiggle_lon,
                jiggle_lat,
                rng,
                long_jump_prob=0.0,
                region_ctx=region_ctx,
                region_subject=active_subj,
            )
        frac = max(0.0, min(1.0, frac))
        tlon = ctx.lon[start_idx] + (ctx.lon[end_idx] - ctx.lon[start_idx]) * frac
        tlat = ctx.lat[start_idx] + (ctx.lat[end_idx] - ctx.lat[start_idx]) * frac
        base = int(round(start_idx + (end_idx - start_idx) * frac))
        base = max(0, min(bs_count - 1, base))
        if region_ctx and active_subj:
            pool = list(
                dict.fromkeys(
                    _neighbor_indices_in_subject(ctx, region_ctx, start_idx, active_subj)
                    + _neighbor_indices_in_subject(ctx, region_ctx, end_idx, active_subj)
                    + [start_idx, end_idx, base]
                )
            )
        else:
            pool = list(
                dict.fromkeys(
                    ctx.neighbor_lists[start_idx]
                    + ctx.neighbor_lists[end_idx]
                    + ctx.neighbor_lists[base]
                    + [start_idx, end_idx, base]
                )
            )
        weights = [1.0 / (1e-8 + math.hypot(ctx.lon[j] - tlon, ctx.lat[j] - tlat)) for j in pool]
        return int(rng.choices(pool, weights=weights, k=1)[0])
    if start_idx == end_idx:
        if rng.random() < 0.08 and region_ctx and active_subj:
            return _pick_bs_in_subject(region_ctx, active_subj, rng)
        return int(start_idx)

    frac = max(0.0, min(1.0, frac))
    cand = int(round(start_idx + (end_idx - start_idx) * frac))
    cand = max(0, min(bs_count - 1, cand))
    if region_ctx and active_subj:
        pool = sorted(region_ctx.indices_by_subject.get(active_subj, ()))
        if pool:
            if region_ctx.subjects_by_idx[cand] != active_subj:
                cand = int(rng.choice(pool))
            jitter = -1 if rng.random() < 0.08 else 1 if rng.random() > 0.92 else 0
            if jitter and cand in pool:
                pos = pool.index(cand)
                cand = pool[max(0, min(len(pool) - 1, pos + jitter))]
            return int(cand)
    jitter = -1 if rng.random() < 0.08 else 1 if rng.random() > 0.92 else 0
    cand += jitter
    return int(max(0, min(bs_count - 1, cand)))


def _subject_for_anchor(
    *,
    anchor_idx: int,
    home_bs: int,
    work_bs: int,
    primary_subject: str,
    visit_subject: str | None,
    region_ctx: BsRegionContext | None,
) -> str:
    if visit_subject and anchor_idx == work_bs:
        return visit_subject
    if region_ctx is not None and 0 <= anchor_idx < len(region_ctx.subjects_by_idx):
        return region_ctx.subjects_by_idx[anchor_idx]
    return primary_subject


def _journey_template_by_mode(
    *,
    day: date,
    profile: str,
    mobility_mode: str,
    home_bs: int,
    work_bs: int,
    bs_count: int,
    rng: random.Random,
    ctx: BsSpatialContext | None = None,
    region_ctx: BsRegionContext | None = None,
    primary_subject: str = "unknown",
    visit_subject: str | None = None,
) -> list[JourneyPoint]:
    home_subj = primary_subject
    work_subj = visit_subject if visit_subject else primary_subject

    def _tx(from_idx: int, to_idx: int) -> int:
        return _transition_bs(
            from_idx,
            to_idx,
            bs_count,
            rng,
            ctx=ctx,
            region_ctx=region_ctx,
            from_subject=_subject_for_anchor(
                anchor_idx=from_idx,
                home_bs=home_bs,
                work_bs=work_bs,
                primary_subject=primary_subject,
                visit_subject=visit_subject,
                region_ctx=region_ctx,
            ),
            to_subject=_subject_for_anchor(
                anchor_idx=to_idx,
                home_bs=home_bs,
                work_bs=work_bs,
                primary_subject=primary_subject,
                visit_subject=visit_subject,
                region_ctx=region_ctx,
            ),
        )

    def _via_in_subject(subject: str, near_idx: int) -> int:
        if ctx is None or region_ctx is None:
            if region_ctx:
                return _pick_bs_in_subject(region_ctx, subject, rng)
            return int(rng.randrange(max(1, bs_count)))
        steer_lon = ctx.lon[near_idx] + rng.uniform(-0.028, 0.028)
        steer_lat = ctx.lat[near_idx] + rng.uniform(-0.028, 0.028)
        return _pick_neighbor_biased(
            ctx,
            near_idx,
            steer_lon,
            steer_lat,
            rng,
            region_ctx=region_ctx,
            region_subject=subject,
        )

    points: list[JourneyPoint] = [JourneyPoint(_dt_in_window(day, 6, 0, 8, 30, rng), home_bs)]
    if mobility_mode == "static":
        points.append(JourneyPoint(_dt_in_window(day, 10, 0, 14, 0, rng), home_bs))
        if profile != "light" and rng.random() < 0.42:
            near = _tx(home_bs, work_bs) if visit_subject else _via_in_subject(home_subj, home_bs)
            points.append(JourneyPoint(_dt_in_window(day, 12, 30, 14, 30, rng), near))
        if rng.random() < 0.35:
            errand = _tx(home_bs, work_bs) if visit_subject else _via_in_subject(home_subj, home_bs)
            points.append(JourneyPoint(_dt_in_window(day, 15, 0, 18, 0, rng), errand))
        points.append(JourneyPoint(_dt_in_window(day, 20, 0, 23, 30, rng), home_bs))
        return points

    if mobility_mode == "commuter":
        points.append(JourneyPoint(_dt_in_window(day, 7, 30, 9, 30, rng), _tx(home_bs, work_bs)))
        points.append(JourneyPoint(_dt_in_window(day, 9, 0, 12, 0, rng), work_bs))
        if rng.random() < 0.55:
            lunch = _tx(work_bs, home_bs)
            points.append(JourneyPoint(_dt_in_window(day, 12, 0, 14, 0, rng), lunch))
        points.append(JourneyPoint(_dt_in_window(day, 13, 0, 16, 30, rng), work_bs))
        points.append(JourneyPoint(_dt_in_window(day, 17, 0, 19, 30, rng), _tx(work_bs, home_bs)))
        if profile != "light" and rng.random() < 0.4:
            evening = _tx(home_bs, work_bs) if visit_subject else _via_in_subject(home_subj, home_bs)
            points.append(JourneyPoint(_dt_in_window(day, 19, 30, 21, 30, rng), evening))
        points.append(JourneyPoint(_dt_in_window(day, 20, 0, 23, 0, rng), home_bs))
        return points

    if mobility_mode == "explorer":
        points.append(JourneyPoint(_dt_in_window(day, 7, 30, 9, 30, rng), _tx(home_bs, work_bs)))
        points.append(JourneyPoint(_dt_in_window(day, 9, 0, 11, 30, rng), work_bs))
        prev = work_bs
        active_subj = work_subj
        for _ in range(3 if profile == "heavy" else 2):
            if ctx is not None and rng.random() < 0.88:
                prev = _via_in_subject(active_subj, prev)
                via = prev
            else:
                via = _tx(work_bs, home_bs) if visit_subject and rng.random() < 0.55 else _via_in_subject(active_subj, prev)
            points.append(JourneyPoint(_dt_in_window(day, 11, 30, 18, 30, rng), via))
        points.append(JourneyPoint(_dt_in_window(day, 18, 0, 21, 0, rng), _tx(work_bs, home_bs)))
        points.append(JourneyPoint(_dt_in_window(day, 20, 0, 23, 30, rng), home_bs))
        return points

    # mobility_mode == "hypermobile"
    points.append(JourneyPoint(_dt_in_window(day, 7, 0, 8, 30, rng), _tx(home_bs, work_bs)))
    points.append(JourneyPoint(_dt_in_window(day, 8, 30, 10, 0, rng), work_bs))
    prev = work_bs
    active_subj = work_subj
    for _ in range(5 if profile == "heavy" else 4):
        if ctx is not None and rng.random() < 0.82:
            prev = _via_in_subject(active_subj, prev)
            via = prev
        else:
            via = _via_in_subject(active_subj, prev)
        points.append(JourneyPoint(_dt_in_window(day, 10, 0, 20, 30, rng), via))
    points.append(JourneyPoint(_dt_in_window(day, 20, 0, 23, 30, rng), home_bs))
    return points


def _pick_mobility_mode(moving: bool, rng: random.Random) -> str:
    if not moving:
        return "static"
    return rng.choices(
        population=["commuter", "explorer", "hypermobile"],
        weights=MOBILITY_MODE_WEIGHTS_MOVING,
        k=1,
    )[0]


def person_subset_after_active_sample_for_day(
    *,
    operator: str,
    day: date,
    seed: int,
    person_day: pd.DataFrame,
    active_ratio: float,
    operator_person_subset: bool = False,
) -> pd.DataFrame:
    """Operator/day person rows after the same active sampling as ``active_subscribers_from_person_for_day``."""
    if person_day.empty:
        return person_day.iloc[0:0]
    if not operator_person_subset:
        operator_id = OPERATOR_MNC.get(operator)
        if operator_id is None:
            return person_day.iloc[0:0]

        subset = person_day[pd.to_numeric(person_day.get("operator_Id"), errors="coerce") == operator_id]
        if subset.empty:
            return person_day.iloc[0:0]

        subset = subset.dropna(subset=["isdn", "imsi", "imei"])
    else:
        subset = person_day.dropna(subset=["isdn", "imsi", "imei"])
    subset = filter_physical_person_rows(subset)
    if subset.empty:
        return person_day.iloc[0:0]

    active_ratio = max(0.0, min(1.0, float(active_ratio)))
    active_count = max(1, int(len(subset) * active_ratio))
    pick_rng = make_rng("person_active", operator, day.isoformat(), seed)
    if active_count < len(subset):
        picked = pick_rng.sample(range(len(subset)), k=active_count)
        subset = subset.iloc[picked]
    return subset.reset_index(drop=True)


def subscriber_states_from_person_rows(
    *,
    operator: str,
    day: date,
    seed: int,
    bs_op: pd.DataFrame,
    person_rows: pd.DataFrame,
    movement_ratio: float,
    spatial_ctx: BsSpatialContext | None = None,
) -> list[SubscriberDayState]:
    """Build ``SubscriberDayState`` for each row of ``person_rows`` (already active-sampled)."""
    if person_rows.empty:
        return []
    movement_ratio = max(0.0, min(1.0, float(movement_ratio)))
    day_start = datetime.combine(day, datetime.min.time())
    ctx = spatial_ctx if spatial_ctx is not None else _build_bs_spatial_context(bs_op)
    region_ctx = _build_bs_region_context(bs_op)
    states: list[SubscriberDayState] = []
    for row in person_rows.itertuples(index=False):
        msisdn_digits = _normalize_digits(getattr(row, "isdn", None), min_len=10, max_len=11)
        imsi_digits = _normalize_digits(getattr(row, "imsi", None), min_len=10, max_len=15)
        imei_digits = _normalize_digits(getattr(row, "imei", None), min_len=10, max_len=15)
        if not (msisdn_digits and imsi_digits and imei_digits):
            continue

        home_operator = _operator_from_imsi(imsi_digits) or operator
        local_id = stable_seed("person_local_id", imsi_digits, msisdn_digits) % 10_000_000
        prof_rng = make_rng("person_profile", imsi_digits, day.isoformat(), seed)
        started_dt = _sample_started_dt(day_start, prof_rng)
        duration_sec = prof_rng.choices(
            [5, 10, 20, 30, 60, 120, 300, 600, 1200, 1800, 3600],
            weights=[2, 3, 5, 8, 12, 14, 18, 16, 10, 7, 5],
            k=1,
        )[0]

        raw_loc = getattr(row, "abonent_last_location", 0)
        _loc = pd.to_numeric(raw_loc, errors="coerce")
        moving_hint = 0 if pd.isna(_loc) else int(_loc)
        moving = (moving_hint != 0) or (prof_rng.random() < movement_ratio)
        mobility_mode = _pick_mobility_mode(moving, prof_rng)
        primary_subject, visit_subject = _pick_subscriber_region_plan(
            region_ctx,
            prof_rng,
            moving=moving,
            mobility_mode=mobility_mode,
            subscriber_key=imsi_digits,
        )
        start_idx = _pick_bs_in_subject(region_ctx, primary_subject, prof_rng)
        end_idx = _pick_end_bs_idx(
            bs_op,
            start_idx,
            moving,
            prof_rng,
            ctx=ctx,
            region_ctx=region_ctx,
            primary_subject=primary_subject,
            visit_subject=visit_subject,
        )
        act_from = pd.to_datetime(getattr(row, "actually_from", None), errors="coerce")
        act_to = pd.to_datetime(getattr(row, "actually_to", None), errors="coerce")
        act_from_dt = act_from.to_pydatetime() if pd.notna(act_from) else datetime.combine(day, datetime.min.time())
        act_to_dt = act_to.to_pydatetime() if pd.notna(act_to) else None

        states.append(
            SubscriberDayState(
                home_operator=home_operator,
                serving_operator=operator,
                local_id=int(local_id),
                msisdn=f"+{msisdn_digits}" if not msisdn_digits.startswith("+") else msisdn_digits,
                imsi=imsi_digits,
                imei=imei_digits,
                started_dt=started_dt,
                duration_sec=duration_sec,
                moving=moving,
                start_bs_idx=start_idx,
                end_bs_idx=end_idx,
                primary_subject=primary_subject,
                visit_subject=visit_subject,
                profile_score=prof_rng.random(),
                mobility_mode=mobility_mode,
                actually_from=act_from_dt,
                actually_to=act_to_dt,
            )
        )
    return states


def active_subscribers_from_person_for_day(
    *,
    operator: str,
    day: date,
    seed: int,
    bs_op: pd.DataFrame,
    person_day: pd.DataFrame,
    active_ratio: float,
    movement_ratio: float,
    operator_person_subset: bool = False,
    spatial_ctx: BsSpatialContext | None = None,
) -> list[SubscriberDayState]:
    sampled = person_subset_after_active_sample_for_day(
        operator=operator,
        day=day,
        seed=seed,
        person_day=person_day,
        active_ratio=active_ratio,
        operator_person_subset=operator_person_subset,
    )
    return subscriber_states_from_person_rows(
        operator=operator,
        day=day,
        seed=seed,
        bs_op=bs_op,
        person_rows=sampled,
        movement_ratio=movement_ratio,
        spatial_ctx=spatial_ctx,
    )


def _normalize_digits(value: object, *, min_len: int, max_len: int) -> str | None:
    if value is None or pd.isna(value):
        return None
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    if not digits:
        return None
    if len(digits) < min_len:
        return None
    if len(digits) > max_len:
        digits = digits[-max_len:]
    if len(digits) == 10:
        digits = "7" + digits
    if len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    return digits


def _operator_from_imsi(imsi_digits: str) -> str | None:
    if len(imsi_digits) < 5 or not imsi_digits.startswith("250"):
        return None
    try:
        mnc = int(imsi_digits[3:5])
    except ValueError:
        return None
    return MNC_OPERATOR.get(mnc)


# ---------------------------------------------------------------------------
# Mobile OSS: supplier Q&A rules (owner OCC-008, person intervals, LAC/cell)
# ---------------------------------------------------------------------------

OPEN_ACTUALLY_TO = pd.Timestamp("2999-12-31 23:59:59")
_OPEN_ACTUALLY_TO = OPEN_ACTUALLY_TO
OPEN_BS_DATE_OFF = pd.Timestamp("2999-12-31 23:59:59")
_STARTED_RE = re.compile(r"^\d{14}$")
_DTO_TZ_RE = re.compile(r"^(?P<dt>\d{8}T\d{6})(?P<tz>Z|[+-]\d{2}(?:\d{2})?)?$")


def filter_physical_person_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Keep natural persons (client_type=0) for OSS / fct_person alignment."""
    if df.empty or "client_type" not in df.columns:
        return df
    ct = pd.to_numeric(df["client_type"], errors="coerce")
    return df.loc[ct == 0].copy()


def person_interval_overlaps_day(df: pd.DataFrame, day: date) -> pd.DataFrame:
    if df.empty:
        return df
    work = df.copy()
    work["actually_from"] = pd.to_datetime(work.get("actually_from"), errors="coerce")
    work["actually_to"] = pd.to_datetime(work.get("actually_to"), errors="coerce")
    work["actually_to"] = work["actually_to"].fillna(_OPEN_ACTUALLY_TO)
    day_start = pd.Timestamp(day)
    day_end = day_start + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    mask = work["actually_from"].notna() & (work["actually_from"] <= day_end) & (work["actually_to"] >= day_start)
    return work.loc[mask].copy()


def event_within_person_interval(
    event_dt: datetime,
    *,
    actually_from: datetime | pd.Timestamp | None,
    actually_to: datetime | pd.Timestamp | None,
) -> bool:
    if actually_from is None or pd.isna(actually_from):
        return True
    if event_dt < pd.Timestamp(actually_from):
        return False
    end = _OPEN_ACTUALLY_TO if actually_to is None or pd.isna(actually_to) else pd.Timestamp(actually_to)
    return event_dt <= end


# Доля строк с IMSI при Owner=1 (A-party). Owner=2 (B-party) всегда без IMSI (OCC-008).
# Калибровка «как в проде»: location/sms слабее, gprs/cdr сильнее.
MOBILE_IMSI_PRESENT_RATE_BY_MART: Final[dict[str, float]] = {
    "cdr": 0.72,
    "gprs": 0.78,
    "sms": 0.48,
    "location": 0.35,
}

# Пороги для ``check_imsi_digits``: доля строк с валидным IMSI от всех записей (~Owner×present_rate).
MOBILE_IMSI_DQ_FAILED_BELOW_BY_MART: Final[dict[str, float]] = {
    "cdr": 0.30,
    "gprs": 0.32,
    "sms": 0.18,
    "location": 0.28,
}

MOBILE_IMSI_DQ_WARN_BELOW_BY_MART: Final[dict[str, float]] = {
    "cdr": 0.34,
    "gprs": 0.36,
    "sms": 0.22,
    "location": 0.32,
}


def resolve_owner_parties(
    *,
    owner: Any,
    subscriber_msisdn: str,
    peer_msisdn: str,
    subscriber_imsi: str,
    subscriber_imei: str,
) -> dict[str, Any]:
    """OCC-008: isdn/peer and calling/called numbering."""
    o = int(owner) if owner in (1, 2) else 1
    if o == 2:
        calling, called = peer_msisdn, subscriber_msisdn
        imsi, imei = None, None
    else:
        calling, called = subscriber_msisdn, peer_msisdn
        imsi, imei = subscriber_imsi, subscriber_imei
    return {
        "owner": o,
        "calling": calling,
        "called": called,
        "imsi": imsi,
        "imei": imei,
    }


def mobile_row_imsi(
    *,
    mart: str,
    owner: int,
    subscriber_imsi: str,
    parties_imsi: str | None,
    rng: random.Random,
) -> str | None:
    """Слабое заполнение IMSI: B-party пусто; A-party — по витрине + редкий брак."""
    if int(owner) == 2:
        return None
    base = parties_imsi or subscriber_imsi
    if not base:
        return None
    rate = MOBILE_IMSI_PRESENT_RATE_BY_MART.get(mart, 0.55)
    if rng.random() >= rate:
        return None
    if rng.random() < 0.006:
        return None
    if rng.random() < 0.010 and len(base) > 9:
        return base[: rng.randint(8, len(base) - 1)]
    return base


def offset_sec_from_datetime_original(value: Any) -> int:
    if value is None or pd.isna(value):
        return 0
    text = str(value).strip()
    if not text:
        return 0
    m = _DTO_TZ_RE.match(text)
    if not m:
        return 0
    tz = m.group("tz")
    if not tz or tz == "Z":
        return 0
    sign = 1 if tz[0] == "+" else -1
    hh = int(tz[1:3])
    mm = int(tz[3:5]) if len(tz) >= 5 else 0
    return sign * (hh * 3600 + mm * 60)


def offset_series_from_started_and_dto(
    started: pd.Series | None,
    date_time_original: pd.Series | None,
) -> pd.Series:
    if started is None:
        return pd.Series(dtype="Int32")
    n = len(started)
    dto = (
        date_time_original
        if date_time_original is not None
        else pd.Series([pd.NA] * n, index=started.index, dtype="string")
    )
    out = pd.Series(0, index=started.index, dtype="Int32")
    for idx in started.index:
        dto_val = dto.loc[idx] if idx in dto.index else pd.NA
        sec = offset_sec_from_datetime_original(dto_val)
        if sec == 0:
            s = started.loc[idx]
            if pd.notna(s) and _STARTED_RE.match(str(s).strip()):
                sec = 0
        out.loc[idx] = int(sec)
    return out.astype("Int32")


def apply_owner_isdn_peer_columns(
    df: pd.DataFrame,
    *,
    owner_col: str = "Owner",
    calling_col: str,
    called_col: str,
    imsi_col: str = "IMSI",
    imei_col: str = "IMEI",
) -> pd.DataFrame:
    """Rewrite calling/called/imsi/imei in place from Owner (for oss transforms)."""
    if df.empty:
        return df
    work = df.copy()
    owner = pd.to_numeric(work.get(owner_col), errors="coerce").fillna(1).astype(int)
    calling = work[calling_col].astype("string")
    called = work[called_col].astype("string")
    isdn = calling.where(owner != 2, called)
    peer = called.where(owner != 2, calling)
    work["isdn"] = pd.to_numeric(isdn.str.replace(r"\D+", "", regex=True), errors="coerce").astype("Int64")
    work["peer"] = pd.to_numeric(peer.str.replace(r"\D+", "", regex=True), errors="coerce").astype("Int64")
    if imsi_col in work.columns:
        work[imsi_col] = work[imsi_col].where(owner == 1, pd.NA)
    if imei_col in work.columns:
        work[imei_col] = work[imei_col].where(owner == 1, pd.NA)
    return work


def valid_lac_cell(lac: Any, cell: Any) -> bool:
    try:
        l = int(lac)
        c = int(cell)
    except (TypeError, ValueError):
        return False
    return l >= 0 and c >= 0 and l < 10**5 and c < 10**6


def coerce_valid_lac_cell(
    lac: Any,
    cell: Any,
    *,
    rng: Any | None = None,
    fallback_lac: int = 1000,
    fallback_cell: int = 10001,
) -> tuple[int, int]:
    if valid_lac_cell(lac, cell):
        return int(lac), int(cell)
    if rng is not None:
        return int(rng.randint(1, 50000)), int(rng.randint(1, 500000))
    return fallback_lac, fallback_cell


# ---------------------------------------------------------------------------
# Mobile OSS (CDR / SMS / GPRS / location): shared params, paths, orchestration
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BuildSrcMobileOssParams:
    """Runtime parameters for mobile OSS vitrines (subscriber identity comes from ``src_person``)."""

    start_date: date
    end_date: date
    operators: list[str]
    seed: int
    max_workers: int
    #: Extra probability of daily mobility on top of ``abonent_last_location`` hint from ``src_person``.
    movement_ratio: float = 0.22
    #: Restrict ``src_bs`` to these ``subject`` values (default: ``DEFAULT_REGION_SUBJECTS``).
    region_subjects: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.start_date > self.end_date:
            raise ValueError(
                f"start_date must be <= end_date, got {self.start_date} > {self.end_date}"
            )


BuildSrcCdrParams = BuildSrcMobileOssParams
BuildSrcSmsParams = BuildSrcMobileOssParams
BuildSrcGprsParams = BuildSrcMobileOssParams
BuildSrcLocationParams = BuildSrcMobileOssParams

PERSON_SNAPSHOT_COLUMNS: Final[list[str]] = [
    "operator_Id",
    "isdn",
    "imsi",
    "imei",
    "abonent_last_location",
    "client_type",
    "actually_from",
    "actually_to",
]

PERSON_ACTIVE_RATIO_ALL: Final[float] = 1.0
# Default when ``BuildSrcMobileOssParams.movement_ratio`` is not passed explicitly.
DEFAULT_MOBILE_OSS_MOVEMENT_RATIO: Final[float] = 0.22
LOCATION_DWELL_UPDATE_MINUTES: Final[int] = 26

# Целевой размер пула абонентов на домашнего оператора (полный дневной срез). Совпадает с
# ``DEFAULT_SRC_PERSON_PARAMS.target_active_subscribers_per_operator`` в ``cli_defaults``.
DEFAULT_ACTIVE_SUBSCRIBERS_PER_OPERATOR: Final[int] = 20_000
SYNTHETIC_FALLBACK_AAB_PER_OPERATOR: Final[int] = DEFAULT_ACTIVE_SUBSCRIBERS_PER_OPERATOR
SYNTHETIC_FALLBACK_ACTIVE_RATIO: Final[float] = 0.1
SYNTHETIC_FALLBACK_TRANSITION_RATIO: Final[float] = 0.03
SYNTHETIC_FALLBACK_MOVEMENT_RATIO: Final[float] = 0.1


def prepare_bs_by_operator(
    bs: pd.DataFrame,
    operators: list[str],
    *,
    region_subjects: tuple[str, ...] | None = None,
) -> dict[str, pd.DataFrame]:
    """Partition BS reference by operator name using ``OPERATOR_MNC``; optionally filter by ``subject``."""
    frame = ensure_bs_local_offset_column(bs)
    if region_subjects is not None and len(region_subjects) > 0:
        subcol = frame.get("subject")
        if subcol is None:
            raise ValueError("BS parquet has no 'subject' column; cannot apply region_subjects filter")
        frame = frame[frame["subject"].isin(region_subjects)]
    prepared: dict[str, pd.DataFrame] = {}
    for operator in operators:
        mnc = OPERATOR_MNC[operator]
        bs_op = frame[frame["mnc"] == mnc].copy().reset_index(drop=True)
        if bs_op.empty:
            raise ValueError(
                f"No BS rows for operator={operator} (mnc={mnc}) after filters "
                f"(region_subjects={list(region_subjects) if region_subjects else None})"
            )
        prepared[operator] = bs_op
    return prepared


def person_rows_for_operator(person_month_df: pd.DataFrame, operator: str) -> pd.DataFrame:
    """Rows of ``src_person`` for one operator (valid isdn/imsi/imei, natural persons only)."""
    operator_id = OPERATOR_MNC.get(operator)
    if operator_id is None:
        return pd.DataFrame()
    ocol = pd.to_numeric(person_month_df.get("operator_Id"), errors="coerce")
    subset = person_month_df.loc[ocol == operator_id]
    subset = filter_physical_person_rows(subset)
    return subset.dropna(subset=["isdn", "imsi", "imei"]).reset_index(drop=True)


def build_person_pool_by_operator_month(
    person_by_month: dict[tuple[int, int], pd.DataFrame],
    operators: list[str],
) -> dict[tuple[str, int, int], pd.DataFrame]:
    """Pre-filter monthly ``src_person`` frames for OSS hot path (one small frame per operator × month)."""
    pool: dict[tuple[str, int, int], pd.DataFrame] = {}
    for (year, month), pdf in person_by_month.items():
        for op in operators:
            pool[(op, year, month)] = person_rows_for_operator(pdf, op)
    return pool


def load_src_person_for_oss_day(
    *,
    person_layout_template: str = SRC_PERSON_LAYOUT_TEMPLATE,
    person_success_flag: str = SRC_PERSON_SUCCESS_FLAG,
    day: date,
    columns: list[str],
) -> pd.DataFrame:
    """Person slice for OSS day: prefer ``load_day=DD`` parquet, else latest monthly ``_SUCCESS``."""
    day_path = resolve_person_snapshot_path(person_layout_template, day)
    if day_path.exists():
        return pd.read_parquet(day_path, columns=columns)
    monthly = load_src_person_latest_success_by_month(
        person_layout_template=person_layout_template,
        person_success_flag=person_success_flag,
        task_dates=[day],
        columns=columns,
    )
    frame = monthly.get((day.year, day.month), pd.DataFrame())
    return person_interval_overlaps_day(frame, day)




def build_person_pool_by_operator_month_slices(
    *,
    person_layout_template: str = SRC_PERSON_LAYOUT_TEMPLATE,
    person_success_flag: str = SRC_PERSON_SUCCESS_FLAG,
    operators: list[str],
    task_dates: Iterable[date],
    columns: list[str],
) -> dict[tuple[str, date], pd.DataFrame]:
    """Person pool from latest monthly ``_SUCCESS`` slice only (geo build-src-mobile)."""
    task_dates_list = sorted({d for d in task_dates})
    if not task_dates_list:
        return {}
    person_by_month = load_src_person_latest_success_by_month(
        person_layout_template=person_layout_template,
        person_success_flag=person_success_flag,
        task_dates=task_dates_list,
        columns=columns,
    )
    pool: dict[tuple[str, date], pd.DataFrame] = {}
    for day in task_dates_list:
        month_frame = person_by_month.get((day.year, day.month), pd.DataFrame())
        day_frame = person_interval_overlaps_day(month_frame, day)
        for op in operators:
            pool[(op, day)] = person_rows_for_operator(day_frame, op)
    return pool


def build_person_pool_by_operator_day(
    *,
    person_layout_template: str = SRC_PERSON_LAYOUT_TEMPLATE,
    person_success_flag: str = SRC_PERSON_SUCCESS_FLAG,
    operators: list[str],
    task_dates: Iterable[date],
    columns: list[str],
) -> dict[tuple[str, date], pd.DataFrame]:
    """Per operator × calendar day person frames (point-in-time ready).

    Loads each calendar month at most once (daily snapshot preferred when present).
    """
    task_dates_list = sorted({d for d in task_dates})
    if not task_dates_list:
        return {}

    person_by_month = load_src_person_latest_success_by_month(
        person_layout_template=person_layout_template,
        person_success_flag=person_success_flag,
        task_dates=task_dates_list,
        columns=columns,
    )

    pool: dict[tuple[str, date], pd.DataFrame] = {}
    for day in task_dates_list:
        day_path = resolve_person_snapshot_path(person_layout_template, day)
        if day_path.exists():
            day_frame = pd.read_parquet(day_path, columns=columns)
            day_frame = person_interval_overlaps_day(day_frame, day)
        else:
            month_frame = person_by_month.get((day.year, day.month), pd.DataFrame())
            day_frame = person_interval_overlaps_day(month_frame, day)
        for op in operators:
            pool[(op, day)] = person_rows_for_operator(day_frame, op)
    return pool


def _mobile_oss_person_staging_dir(staging_root: Path, operator: str) -> Path:
    digest = hashlib.sha256(operator.encode("utf-8")).hexdigest()[:16]
    slug = "".join(ch if ch.isalnum() else "_" for ch in operator)[:32].strip("_") or "op"
    return staging_root / f"person_{slug}_{digest}"


def stage_operator_person_pool(
    person_pool: dict[tuple[str, date], pd.DataFrame],
    operator: str,
    staging_root: Path,
) -> str:
    """Write one operator's day slices for ``build-src-mobile`` worker processes."""
    op_dir = _mobile_oss_person_staging_dir(staging_root, operator)
    op_dir.mkdir(parents=True, exist_ok=True)
    for (op, day), frame in person_pool.items():
        if op != operator:
            continue
        frame.to_parquet(op_dir / f"{day.isoformat()}.parquet", index=False)
    return str(op_dir.resolve())


def load_staged_operator_person_pool(
    operator: str,
    person_pool_dir: str,
    calendar_days: Iterable[date],
) -> dict[tuple[str, date], pd.DataFrame]:
    base = Path(person_pool_dir)
    pool: dict[tuple[str, date], pd.DataFrame] = {}
    for day in calendar_days:
        path = base / f"{day.isoformat()}.parquet"
        pool[(operator, day)] = pd.read_parquet(path) if path.exists() else pd.DataFrame()
    return pool


def resolve_mobile_oss_output_path(
    template: str,
    operator: str,
    day: date,
    filename: str,
    *,
    dc: str | None = None,
) -> Path:
    return resolve_dated_layout_path(
        template,
        day,
        filename=filename,
        name_operator=operator_slug(operator),
        dc=dc or mobile_datacenter_ids()[0],
    )


def resolve_dated_layout_path(
    template: str,
    day: date,
    *,
    filename: str | None = None,
    name_operator: str = "",
    dc: str = "",
    source_id: str = "",
) -> Path:
    """Resolve ``readiness.s3_layout`` with ``{YYYY}/{MM}/{DD}``, optional placeholders."""
    resolved = template.format(
        name_operator=name_operator,
        dc=dc,
        source_id=source_id,
        YYYY=day.strftime("%Y"),
        MM=day.strftime("%m"),
        DD=day.strftime("%d"),
    )
    path = Path(resolved)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    if path.suffix.lower() == ".parquet":
        return path
    if filename:
        return path / filename
    return path


def build_bs_lac_cell_to_subject(bs_op: pd.DataFrame) -> dict[tuple[int, int], str]:
    """LAC/Cell → ``subject`` для маршрутизации mobile-строк по ЦОД."""
    mapping: dict[tuple[int, int], str] = {}
    if bs_op is None or bs_op.empty:
        return mapping
    lac = pd.to_numeric(bs_op.get("lac"), errors="coerce")
    cell = pd.to_numeric(bs_op.get("cell"), errors="coerce")
    subj = bs_op.get("subject")
    if subj is None:
        return mapping
    for lac_v, cell_v, subj_v in zip(lac, cell, subj.astype("string"), strict=False):
        if pd.isna(lac_v) or pd.isna(cell_v):
            continue
        name = str(subj_v).strip()
        if name:
            mapping[(int(lac_v), int(cell_v))] = name
    return mapping


def datacenter_id_for_mobile_row(
    row: dict[str, Any],
    *,
    region_column: str | None,
    lac_cell_subject: dict[tuple[int, int], str],
    lac_col: str | None,
    cell_col: str | None,
) -> str:
    if region_column:
        raw = row.get(region_column)
        if raw is not None and str(raw).strip():
            return subject_to_mobile_datacenter(str(raw))
    if lac_col and cell_col and lac_cell_subject:
        lac = pd.to_numeric(row.get(lac_col), errors="coerce")
        cell = pd.to_numeric(row.get(cell_col), errors="coerce")
        if not pd.isna(lac) and not pd.isna(cell):
            subj = lac_cell_subject.get((int(lac), int(cell)))
            if subj:
                return subject_to_mobile_datacenter(subj)
    return mobile_datacenter_ids()[0]


def partition_mobile_rows_by_datacenter(
    rows: list[dict[str, Any]],
    *,
    region_column: str | None = "RecEntOwnerRegion",
    bs_op: pd.DataFrame | None = None,
    lac_col: str | None = None,
    cell_col: str | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Разнести строки по ЦОД по региону БС **в момент события** (визит в Якутию → ``far-east``)."""
    lac_cell_subject = build_bs_lac_cell_to_subject(bs_op) if bs_op is not None else {}
    buckets: dict[str, list[dict[str, Any]]] = {dc: [] for dc in mobile_datacenter_ids()}
    for row in rows:
        dc = datacenter_id_for_mobile_row(
            row,
            region_column=region_column,
            lac_cell_subject=lac_cell_subject,
            lac_col=lac_col,
            cell_col=cell_col,
        )
        buckets.setdefault(dc, []).append(row)
    return buckets


def resolve_dated_layout_root(layout_template: str) -> Path:
    """Directory to scan under ``readiness.s3_layout`` (prefix before ``{YYYY}``)."""
    if "{YYYY}" in layout_template:
        prefix = layout_template.split("{YYYY}", 1)[0].rstrip("/\\")
        if "{dc}" in prefix:
            prefix = prefix.replace("{dc}", "").rstrip("/\\")
            path = Path(prefix)
            return path if path.is_absolute() else PROJECT_ROOT / path
        if "{source_id}" in prefix:
            prefix = prefix.replace("{source_id}", "").rstrip("/\\")
        path = Path(prefix)
        return path if path.is_absolute() else PROJECT_ROOT / path
    path = Path(layout_template)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path.parent if path.suffix else path


def resolve_person_layout_root(layout: str) -> Path:
    """Root directory above ``load_year=`` / ``load_month=`` / ``load_day=`` segments."""
    path = Path(layout)
    parts = path.parts
    idx = next((i for i, part in enumerate(parts) if "{" in part and "}" in part), None)
    if idx is None:
        return path.parent if path.suffix else path
    return Path(*parts[:idx])


def resolve_person_snapshot_path(template: str, day: date) -> Path:
    """Resolve daily ``src_person`` parquet from person readiness layout."""
    resolved = template.format(
        YYYY=day.strftime("%Y"),
        MM=day.strftime("%m"),
        DD=day.strftime("%d"),
    )
    path = Path(resolved)
    if path.suffix.lower() == ".parquet":
        return path
    return path / "person.parquet"


def resolve_latest_success_person_day_dir_for_month(
    *,
    layout_template: str,
    success_flag: str,
    year: int,
    month: int,
) -> Path:
    """Latest ``load_day=*`` directory under ``year``/``month`` that contains ``success_flag`` (full slice)."""
    root = resolve_person_layout_root(layout_template)
    candidates = sorted(root.glob(f"load_year={year:04d}/load_month={month:02d}/load_day=*"))
    success_dirs = [p for p in candidates if (p / success_flag).exists()]
    if not success_dirs:
        raise FileNotFoundError(
            f"No src_person directory with {success_flag!r} for {year:04d}-{month:02d} under {root}. "
            "Run build-src-person until the monthly snapshot is marked complete."
        )
    return success_dirs[-1]


def load_src_person_latest_success_by_month(
    *,
    person_layout_template: str = SRC_PERSON_LAYOUT_TEMPLATE,
    person_success_flag: str = SRC_PERSON_SUCCESS_FLAG,
    task_dates: Iterable[date],
    columns: list[str],
) -> dict[tuple[int, int], pd.DataFrame]:
    """
    For each calendar month present in ``task_dates``, load ``person.parquet`` from the latest
    day directory with ``person_success_flag`` (full snapshot days from build-src-person).
    """
    layout_template = person_layout_template
    success_flag = person_success_flag

    months = sorted({(d.year, d.month) for d in task_dates})
    out: dict[tuple[int, int], pd.DataFrame] = {}
    for y, m in months:
        day_dir = resolve_latest_success_person_day_dir_for_month(
            layout_template=layout_template,
            success_flag=success_flag,
            year=y,
            month=m,
        )
        parquet_path = day_dir / "person.parquet"
        if not parquet_path.exists():
            raise FileNotFoundError(f"src_person parquet not found: {parquet_path}")
        frame = pd.read_parquet(parquet_path, columns=columns)
        out[(y, m)] = frame
        logger.info(
            "src_person for OSS %04d-%02d: using latest %s slice %s (%s rows, columns=%s)",
            y,
            m,
            success_flag,
            day_dir,
            len(frame),
            columns,
        )
    return out


def calendar_dates_inclusive(start: date, end: date) -> list[date]:
    """Inclusive calendar dates from ``start`` through ``end`` (mobile OSS period span)."""
    out: list[date] = []
    cur = start
    while cur <= end:
        out.append(cur)
        cur += timedelta(days=1)
    return out


def _mobile_vitrine_spec(
    vitrine: Literal["cdr", "sms", "gprs", "location"],
    *,
    compression: str,
) -> tuple[list[dict[str, Any]], str, str]:
    specs: dict[str, tuple[list[dict[str, Any]], str]] = {
        "cdr": (SRC_CDR_FIELDS, SRC_CDR_LAYOUT_TEMPLATE),
        "sms": (SRC_SMS_FIELDS, SRC_SMS_LAYOUT_TEMPLATE),
        "gprs": (SRC_GPRS_FIELDS, SRC_GPRS_LAYOUT_TEMPLATE),
        "location": (SRC_LOCATION_FIELDS, SRC_LOCATION_LAYOUT_TEMPLATE),
    }
    fields, layout = specs[vitrine]
    return fields, layout, compression


def _estimate_mobile_oss_subscriber_chunks(
    *,
    calendar_days: Sequence[date],
    operators: Sequence[str],
    person_pool: dict[tuple[str, date], pd.DataFrame],
    seed: int,
    chunk_size: int,
) -> tuple[int, int]:
    """Return (sum of active-sampled person rows per operator-day, upper-bound chunk count).

    Chunk count uses ``ceil(len(sampled)/chunk_size)`` per task; real runs use ``len(states)``
    (≤ sampled) so the estimate is a safe upper bound for logging.
    """
    total_sampled = 0
    est_chunks = 0
    for day in calendar_days:
        for operator in operators:
            person_for = person_pool.get((operator, day), pd.DataFrame())
            sampled = person_subset_after_active_sample_for_day(
                operator=operator,
                day=day,
                seed=seed,
                person_day=person_for,
                active_ratio=PERSON_ACTIVE_RATIO_ALL,
                operator_person_subset=True,
            )
            n = len(sampled)
            total_sampled += n
            if n:
                est_chunks += (n + chunk_size - 1) // chunk_size
    return total_sampled, est_chunks


def _mobile_oss_operator_bs_staging_name(operator: str) -> str:
    digest = hashlib.sha256(operator.encode("utf-8")).hexdigest()[:20]
    slug = "".join(ch if ch.isalnum() else "_" for ch in operator)[:48].strip("_") or "op"
    return f"{slug}_{digest}.parquet"


def _init_mobile_tqdm_lock(tqdm_lock: Any) -> None:
    tqdm.set_lock(tqdm_lock)


def _run_mobile_oss_for_one_operator(payload: dict[str, Any]) -> dict[str, Any]:
    """``ProcessPoolExecutor`` worker: sequential calendar days for one operator; one ``to_parquet`` per vitrine per day."""
    import logging

    
    log = logging.getLogger(__name__)

    operator = str(payload["operator"])
    params: BuildSrcMobileOssParams = payload["params"]
    calendar_days = [date.fromisoformat(s) for s in payload["calendar_days_iso"]]
    n_days = len(calendar_days)

    bs_op = pd.read_parquet(Path(payload["bs_op_path"])).reset_index(drop=True)
    spatial_ctx = _build_bs_spatial_context(bs_op)

    person_pool = load_staged_operator_person_pool(
        operator,
        str(payload["person_pool_dir"]),
        calendar_days,
    )

    compression = str(payload["compression"])
    fields_cdr, out_cdr, comp_cdr = _mobile_vitrine_spec("cdr", compression=compression)
    fields_sms, out_sms, comp_sms = _mobile_vitrine_spec("sms", compression=compression)
    fields_gprs, out_gprs, comp_gprs = _mobile_vitrine_spec("gprs", compression=compression)
    fields_loc, out_loc, comp_loc = _mobile_vitrine_spec("location", compression=compression)

    agg: dict[str, int] = {"cdr": 0, "sms": 0, "gprs": 0, "location": 0}

    log.info("build-src-mobile worker start operator=%s days=%s", operator, n_days)

    op_label = operator_slug(operator)
    tqdm_position = int(payload.get("tqdm_position", 0))
    day_pbar = tqdm(
        total=n_days,
        desc=f"mobile:{op_label}",
        unit="day",
        position=tqdm_position,
        leave=True,
        file=sys.stderr,
        dynamic_ncols=True,
        mininterval=0.15,
        smoothing=0.1,
    )
    for day_idx, day in enumerate(calendar_days, start=1):
        person_for = person_pool.get((operator, day), pd.DataFrame())
        sampled = person_subset_after_active_sample_for_day(
            operator=operator,
            day=day,
            seed=params.seed,
            person_day=person_for,
            active_ratio=PERSON_ACTIVE_RATIO_ALL,
            operator_person_subset=True,
        )
        day_seed = abs(hash((operator, day.isoformat(), params.seed))) % (2**32)
        rng = random.Random(day_seed)
        log.debug(
            "build-src-mobile task %s %s: bs_rows=%s sampled_subscribers=%s",
            operator,
            day.isoformat(),
            len(bs_op),
            len(sampled),
        )

        all_cdr: list[dict[str, Any]] = []
        all_sms: list[dict[str, Any]] = []
        all_gprs: list[dict[str, Any]] = []
        all_loc: list[dict[str, Any]] = []
        fallback_state: SubscriberDayState | None = None
        states: list[SubscriberDayState] = []

        if sampled.empty:
            log.info(
                "build-src-mobile %s %s: no active sampled rows; empty vitrines or schema-only files",
                operator,
                day.isoformat(),
            )
        else:
            t0 = time.perf_counter()
            states = subscriber_states_from_person_rows(
                operator=operator,
                day=day,
                seed=params.seed,
                bs_op=bs_op,
                person_rows=sampled,
                movement_ratio=float(params.movement_ratio),
                spatial_ctx=spatial_ctx,
            )
            t1 = time.perf_counter()
            if states:
                fallback_state = states[0]
                seq_cdr = 0
                seq_sms = 0
                seq_gprs = 0
                for i in range(0, len(states), MOBILE_OSS_SUBSCRIBER_CHUNK_SIZE):
                    chunk = states[i : i + MOBILE_OSS_SUBSCRIBER_CHUNK_SIZE]
                    bundles = build_subscriber_activity_journey_bundles(
                        chunk,
                        bs_op=bs_op,
                        day=day,
                        seed=params.seed,
                        spatial_ctx=spatial_ctx,
                    )
                    part_cdr = generate_cdr_rows_from_subscriber_states(
                        bs_op=bs_op,
                        operator=operator,
                        day=day,
                        seed=params.seed,
                        rng=rng,
                        states=chunk,
                        spatial_ctx=spatial_ctx,
                        seq_idx_start=seq_cdr,
                        bundles=bundles,
                    )
                    all_cdr.extend(part_cdr)
                    seq_cdr += len(part_cdr)
                    if part_cdr:
                        log.debug(
                            "build-src-mobile vitrine chunk mart=cdr operator=%s day=%s batch_rows=%s",
                            operator,
                            day.isoformat(),
                            len(part_cdr),
                        )
                    part_sms = generate_sms_rows_from_subscriber_states(
                        bs_op=bs_op,
                        operator=operator,
                        day=day,
                        seed=params.seed,
                        rng=rng,
                        states=chunk,
                        spatial_ctx=spatial_ctx,
                        seq_idx_start=seq_sms,
                        bundles=bundles,
                    )
                    all_sms.extend(part_sms)
                    seq_sms += len(part_sms)
                    if part_sms:
                        log.debug(
                            "build-src-mobile vitrine chunk mart=sms operator=%s day=%s batch_rows=%s",
                            operator,
                            day.isoformat(),
                            len(part_sms),
                        )
                    part_gprs = generate_gprs_rows_from_subscriber_states(
                        bs_op=bs_op,
                        operator=operator,
                        day=day,
                        seed=params.seed,
                        rng=rng,
                        states=chunk,
                        spatial_ctx=spatial_ctx,
                        seq_idx_start=seq_gprs,
                        bundles=bundles,
                    )
                    all_gprs.extend(part_gprs)
                    seq_gprs += len(part_gprs)
                    if part_gprs:
                        log.debug(
                            "build-src-mobile vitrine chunk mart=gprs operator=%s day=%s batch_rows=%s",
                            operator,
                            day.isoformat(),
                            len(part_gprs),
                        )
                    part_loc = generate_location_rows_from_subscriber_states(
                        bs_op=bs_op,
                        operator=operator,
                        day=day,
                        seed=params.seed,
                        rng=rng,
                        states=chunk,
                        spatial_ctx=spatial_ctx,
                        bundles=bundles,
                    )
                    all_loc.extend(part_loc)
                    if part_loc:
                        log.debug(
                            "build-src-mobile vitrine chunk mart=location operator=%s day=%s batch_rows=%s",
                            operator,
                            day.isoformat(),
                            len(part_loc),
                        )
                t_gen1 = time.perf_counter()
                log.debug(
                    "build-src-mobile %s %s: person_rows=%s states=%s ms(states/chunks)=%.0f/%.0f chunk_size=%s",
                    operator,
                    day.isoformat(),
                    len(sampled),
                    len(states),
                    (t1 - t0) * 1000.0,
                    (t_gen1 - t1) * 1000.0,
                    MOBILE_OSS_SUBSCRIBER_CHUNK_SIZE,
                )

                inject_cross_mart_rows(
                    cdr=all_cdr,
                    sms=all_sms,
                    gprs=all_gprs,
                    location=all_loc,
                    rng=rng,
                )

                fin_cdr = finalize_cdr_day_parquet_from_rows(
            rows=all_cdr,
            fields=fields_cdr,
            operator=operator,
            day=day,
            out_template=out_cdr,
            compression=comp_cdr,
            bs_op=bs_op,
            rng=rng,
            fallback_state=fallback_state,
        )
        rc = int(fin_cdr["row_count"])
        fin_sms = finalize_sms_day_parquet_from_rows(
            rows=all_sms,
            fields=fields_sms,
            operator=operator,
            day=day,
            out_template=out_sms,
            compression=comp_sms,
            bs_op=bs_op,
            rng=rng,
            fallback_state=fallback_state,
        )
        rs = int(fin_sms["row_count"])
        fin_gprs = finalize_gprs_day_parquet_from_rows(
            rows=all_gprs,
            fields=fields_gprs,
            operator=operator,
            day=day,
            out_template=out_gprs,
            compression=comp_gprs,
            bs_op=bs_op,
            rng=rng,
            fallback_state=fallback_state,
        )
        rg = int(fin_gprs["row_count"])
        fin_loc = finalize_location_day_parquet_from_rows(
            rows=all_loc,
            fields=fields_loc,
            operator=operator,
            day=day,
            seed=params.seed,
            out_template=out_loc,
            compression=comp_loc,
            bs_op=bs_op,
            rng=rng,
            spatial_ctx=spatial_ctx,
            fallback_state=None,
        )
        rl = int(fin_loc["row_count"])

        agg["cdr"] += rc
        agg["sms"] += rs
        agg["gprs"] += rg
        agg["location"] += rl

        log.info(
            "build-src-mobile day %s/%s operator=%s date=%s rows cdr/sms/gprs/loc=%s/%s/%s/%s",
            day_idx,
            n_days,
            operator,
            day.isoformat(),
            rc,
            rs,
            rg,
            rl,
        )
        day_pbar.update(1)
        day_pbar.set_postfix(
            day=day.isoformat(),
            cdr=rc,
            sms=rs,
            gprs=rg,
            loc=rl,
            refresh=False,
        )

    day_pbar.close()
    log.info(
        "build-src-mobile worker done operator=%s total_rows cdr/sms/gprs/loc=%s/%s/%s/%s",
        operator,
        agg["cdr"],
        agg["sms"],
        agg["gprs"],
        agg["location"],
    )

    return {"operator": operator, "row_count": agg, "files_per_mart": n_days}


def run_mobile_oss_all(
    *,
    bs_parquet_path: str | Path,
    params: BuildSrcMobileOssParams,
    compression: str,
    person_layout_template: str = SRC_PERSON_LAYOUT_TEMPLATE,
    person_success_flag: str = SRC_PERSON_SUCCESS_FLAG,
    module_parallelism: int = 4,
) -> dict[str, Any]:
    """
    Generate CDR, SMS, GPRS, and location vitrines.

    IMSI/IMEI/MSISDN pools come from ``src_person`` snapshots (``person_layout_template``;
    latest ``person_success_flag`` slice per month). Cell geometry uses ``src_bs`` at
    ``bs_parquet_path``, optionally restricted to ``params.region_subjects``.

    Work is scheduled as **one OS process per operator** (``ProcessPoolExecutor``). Each process walks
    ``calendar_days`` sequentially, builds ``SubscriberDayState`` in chunks of
    ``MOBILE_OSS_SUBSCRIBER_CHUNK_SIZE``, shares one activity + journey bundle per chunk across the
    four vitrines, then writes **one** parquet file per vitrine per day via ``finalize_*_day_parquet_from_rows``.

    ``module_parallelism`` is ignored for pool sizing (always ``len(operators)`` processes).
    """
    task_dates = calendar_dates_inclusive(params.start_date, params.end_date)
    t_pre = time.perf_counter()
    person_pool = build_person_pool_by_operator_month_slices(
        person_layout_template=person_layout_template,
        person_success_flag=person_success_flag,
        operators=params.operators,
        task_dates=task_dates,
        columns=PERSON_SNAPSHOT_COLUMNS,
    )
    bs_path = Path(bs_parquet_path)
    if not bs_path.exists():
        raise FileNotFoundError(f"BS parquet not found: {bs_path}")
    bs_frame = pd.read_parquet(bs_path)
    n_bs_raw = len(bs_frame)
    bs_for_ops = bs_frame
    if params.region_subjects:
        subcol = bs_for_ops.get("subject")
        if subcol is None:
            raise ValueError("BS parquet has no 'subject' column; cannot apply region_subjects filter")
        bs_for_ops = bs_for_ops[bs_for_ops["subject"].isin(params.region_subjects)].reset_index(drop=True)
    logger.info(
        "build-src-mobile: BS rows raw=%s after_region_subjects=%s filter=%s",
        n_bs_raw,
        len(bs_for_ops),
        list(params.region_subjects) if params.region_subjects else None,
    )
    bs_by_operator = prepare_bs_by_operator(bs_for_ops, params.operators, region_subjects=None)

    calendar_days: list[date] = list(task_dates)
    n_days = len(calendar_days)
    n_ops = len(params.operators)

    preload_sec = round(time.perf_counter() - t_pre, 2)
    pool_rows = sum(len(df) for df in person_pool.values())
    logger.info(
        "Starting build-src-mobile: calendar_days=%s operators=%s process_workers=%s period=%s..%s "
        "(one process per operator, sequential days; one parquet write per vitrine per day; GPRS-heavy)",
        n_days,
        n_ops,
        n_ops,
        params.start_date,
        params.end_date,
    )
    logger.info(
        "build-src-mobile: shared preload done in %ss (person_pool operator-days=%s rows=%s, BS operators=%s)",
        preload_sec,
        len(person_pool),
        pool_rows,
        list(bs_by_operator.keys()),
    )

    sampled_rows_est, est_chunks = _estimate_mobile_oss_subscriber_chunks(
        calendar_days=calendar_days,
        operators=params.operators,
        person_pool=person_pool,
        seed=params.seed,
        chunk_size=MOBILE_OSS_SUBSCRIBER_CHUNK_SIZE,
    )
    logger.info(
        "build-src-mobile: work estimate active_sampled_rows≈%s est_subscriber_chunks≤%s",
        sampled_rows_est,
        est_chunks,
    )
    _ = module_parallelism  # API compatibility; process count is always len(operators).

    calendar_days_iso = [d.isoformat() for d in calendar_days]
    results: dict[str, dict[str, Any]] = {
        "cdr": {"row_count": 0, "file_count": 0},
        "sms": {"row_count": 0, "file_count": 0},
        "gprs": {"row_count": 0, "file_count": 0},
        "location": {"row_count": 0, "file_count": 0},
    }
    t_run = time.perf_counter()
    with tempfile.TemporaryDirectory(prefix="mobile_oss_") as staging:
        staging_path = Path(staging)
        payloads: list[dict[str, Any]] = []
        for op_idx, op in enumerate(params.operators):
            bs_stage = staging_path / _mobile_oss_operator_bs_staging_name(op)
            bs_by_operator[op].to_parquet(bs_stage, index=False)
            person_stage = stage_operator_person_pool(person_pool, op, staging_path)
            payloads.append(
                {
                    "operator": op,
                    "tqdm_position": op_idx,
                    "calendar_days_iso": calendar_days_iso,
                    "bs_op_path": str(bs_stage.resolve()),
                    "person_pool_dir": person_stage,
                    "params": params,
                    "compression": compression,
                }
            )

        n_workers = len(params.operators)
        logger.info(
            "build-src-mobile: starting %s worker processes (BS staging under %s)",
            n_workers,
            staging_path,
        )
        with Manager() as manager:
            tqdm_lock = manager.RLock()
            with ProcessPoolExecutor(
                max_workers=n_workers,
                initializer=_init_mobile_tqdm_lock,
                initargs=(tqdm_lock,),
            ) as ex:
                futures = {ex.submit(_run_mobile_oss_for_one_operator, p): p["operator"] for p in payloads}
                for fut in as_completed(futures):
                    op_done = futures[fut]
                    part = fut.result()
                    rc_part = part["row_count"]
                    files_n = int(part["files_per_mart"])
                    for name in results:
                        results[name]["row_count"] += int(rc_part[name])
                        results[name]["file_count"] += files_n
                    cum = sum(int(results[k]["row_count"]) for k in results)
                    logger.info(
                        "build-src-mobile process done operator=%s total_rows cdr/sms/gprs/loc=%s/%s/%s/%s "
                        "cumulative_rows_all_marts=%s",
                        op_done,
                        rc_part["cdr"],
                        rc_part["sms"],
                        rc_part["gprs"],
                        rc_part["location"],
                        cum,
                    )

    run_sec = round(time.perf_counter() - t_run, 2)
    total_rows = sum(int(results[k]["row_count"]) for k in results)
    logger.info("build-src-mobile completed: total_rows=%s elapsed_sec=%s", total_rows, run_sec)
    append_command_metrics(
        command="build-src-mobile",
        metrics={
            "elapsed_total_sec": run_sec,
            "preload_sec": preload_sec,
            "row_count": int(total_rows),
            "operators": len(params.operators),
            "calendar_days": len(calendar_days),
            "start_date": params.start_date.isoformat(),
            "end_date": params.end_date.isoformat(),
        },
    )
    return {"by_module": results, "row_count": total_rows}

# --- cdr ---

import json
import logging
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from tqdm import tqdm

from mobile.command_timing import append_command_metrics, timed_stage


logger = logging.getLogger(__name__)


def run_cdr(
    *,
    bs_parquet_path: str | Path,
    params: BuildSrcMobileOssParams,
    compression: str,
    person_layout_template: str = SRC_PERSON_LAYOUT_TEMPLATE,
    person_success_flag: str = SRC_PERSON_SUCCESS_FLAG,
    person_by_month: dict[tuple[int, int], pd.DataFrame] | None = None,
    person_pool_by_op_month: dict[tuple[str, int, int], pd.DataFrame] | None = None,
    bs_by_operator: dict[str, pd.DataFrame] | None = None,
) -> dict[str, Any]:
    perf_metrics: dict[str, Any] = {}
    fields, out_template, compression = _mobile_vitrine_spec("cdr", compression=compression)

    if bs_by_operator is not None:
        missing_ops = [op for op in params.operators if op not in bs_by_operator]
        if missing_ops:
            raise ValueError(f"bs_by_operator missing keys for operators: {missing_ops}")
        bs_prep = bs_by_operator
    else:
        bs_path = Path(bs_parquet_path)
        if not bs_path.exists():
            raise FileNotFoundError(f"BS parquet not found: {bs_path}")
        with timed_stage("read_bs_sec", perf_metrics):
            bs = pd.read_parquet(bs_path)
        bs_prep = prepare_bs_by_operator(bs, params.operators)

    task_dates = calendar_dates_inclusive(params.start_date, params.end_date)

    effective_person_by_month = person_by_month if person_by_month is not None else None
    person_pool_by_day: dict[tuple[str, date], pd.DataFrame] | None = None
    person_pool_by_month: dict[tuple[str, int, int], pd.DataFrame] | None = None
    if person_pool_by_op_month is not None:
        person_pool_by_month = person_pool_by_op_month
    elif effective_person_by_month is not None:
        person_pool_by_month = build_person_pool_by_operator_month(effective_person_by_month, params.operators)
    else:
        person_pool_by_day = build_person_pool_by_operator_day(
            person_layout_template=person_layout_template,
            person_success_flag=person_success_flag,
            operators=params.operators,
            task_dates=task_dates,
            columns=PERSON_SNAPSHOT_COLUMNS,
        )

    spatial_by_operator: dict[str, BsSpatialContext | None] = {
        op: _build_bs_spatial_context(bs_prep[op]) for op in params.operators
    }

    started_at = time.perf_counter()
    generated_rows = 0

    def _run_task(operator: str, day: date) -> dict[str, Any]:
        day_seed = abs(hash((operator, day.isoformat(), params.seed))) % (2**32)
        rng = random.Random(day_seed)
        spatial_ctx = spatial_by_operator[operator]
        if person_pool_by_day is not None:
            person_for = person_pool_by_day.get((operator, day), pd.DataFrame())
            operator_subset = True
        elif person_pool_by_month is not None:
            raw = person_pool_by_month.get((operator, day.year, day.month), pd.DataFrame())
            person_for = person_interval_overlaps_day(raw, day)
            operator_subset = True
        else:
            person_for = (
                effective_person_by_month.get((day.year, day.month))
                if effective_person_by_month is not None
                else None
            )
            operator_subset = False
        return _cdr_generate_and_write_day(
            bs_op=bs_prep[operator],
            fields=fields,
            operator=operator,
            day=day,
            seed=params.seed,
            out_template=out_template,
            compression=compression,
            rng=rng,
            person_day=person_for,
            operator_person_subset=operator_subset,
            spatial_ctx=spatial_ctx,
        )

    def _run_calendar_day(day: date) -> dict[str, Any]:
        day_rows = 0
        for op in params.operators:
            r = _run_task(op, day)
            day_rows += int(r["row_count"])
        return {"row_count": day_rows}

    logger.info(
        "Starting build-src-cdr: calendar_days=%s, workers=%s, period=%s..%s (per-day tasks; src_person=latest _SUCCESS per month when person_config set)",
        len(task_dates),
        params.max_workers,
        params.start_date,
        params.end_date,
    )

    with timed_stage("execution_sec", perf_metrics):
        with ThreadPoolExecutor(max_workers=params.max_workers) as executor:
            futures = [executor.submit(_run_calendar_day, day) for day in task_dates]
            with tqdm(total=len(task_dates), desc="build-src-cdr", unit="day") as pbar:
                for future in as_completed(futures):
                    result = future.result()
                    generated_rows += int(result["row_count"])
                    pbar.update(1)
                    pbar.set_postfix(rows=generated_rows, refresh=False)
                    logger.debug(
                        "build-src-cdr progress: %s/%s rows=%s",
                        pbar.n,
                        len(task_dates),
                        generated_rows,
                    )

    elapsed = round(time.perf_counter() - started_at, 2)
    file_count = len(task_dates) * len(params.operators)
    logger.info(
        "build-src-cdr completed: rows=%s, files=%s, elapsed_sec=%s",
        generated_rows,
        file_count,
        elapsed,
    )
    perf_metrics["elapsed_total_sec"] = elapsed
    perf_metrics["rows"] = int(generated_rows)
    perf_metrics["files"] = int(file_count)
    perf_metrics["workers"] = int(params.max_workers)
    append_command_metrics(command="build-src-cdr", metrics=perf_metrics)

    return {
        "row_count": int(generated_rows),
        "file_count": int(file_count),
        "elapsed_sec": elapsed,
        "max_workers": int(params.max_workers),
    }


def generate_cdr_rows_from_subscriber_states(
    *,
    bs_op: pd.DataFrame,
    operator: str,
    day: date,
    seed: int,
    rng: random.Random,
    states: list[SubscriberDayState],
    spatial_ctx: BsSpatialContext | None = None,
    seq_idx_start: int = 0,
    bundles: list[SubscriberActivityJourneyBundle] | None = None,
) -> list[dict[str, Any]]:
    """Build CDR row dicts for one ``states`` batch (slice of subscribers for the day)."""
    mnc = OPERATOR_MNC[operator]
    if bs_op.empty:
        raise ValueError(f"No BS rows for operator={operator} (mnc={mnc})")
    bs_op = bs_op.reset_index(drop=True)
    rows: list[dict[str, Any]] = []
    seq_idx = seq_idx_start
    if bundles is not None:
        if not bundles:
            return []
        for s, activity, journey, _mobility in filter_bundles_by_profile_threshold(bundles, threshold=0.38, shift=0.03):
            if len(journey) == 0:
                continue
            event_count = int(activity["cdr_calls"])
            for point, next_point in iter_spread_journey_event_segments(journey, event_count, rng=rng):
                if not event_within_person_interval(
                    point.timestamp,
                    actually_from=s.actually_from,
                    actually_to=s.actually_to,
                ):
                    continue
                rows.append(
                    _generate_cdr_row(
                        bs_op=bs_op,
                        operator=operator,
                        mnc=mnc,
                        state=s,
                        rng=rng,
                        idx=seq_idx,
                        started_dt=point.timestamp,
                        duration=max(5, int((next_point.timestamp - point.timestamp).total_seconds()) or s.duration_sec),
                        start_bs_idx=point.bs_idx,
                        end_bs_idx=next_point.bs_idx,
                    )
                )
                seq_idx += 1
        return rows
    if not states:
        return []
    selected = choose_states(states, threshold=0.38, shift=0.03)
    for s in selected:
        activity = subscriber_daily_activity(s, day=day, seed=seed)
        journey = subscriber_journey_points(
            s, day=day, seed=seed, bs_count=len(bs_op), bs_op=bs_op, spatial_ctx=spatial_ctx
        )
        if len(journey) == 0:
            continue
        event_count = int(activity["cdr_calls"])
        for point, next_point in iter_spread_journey_event_segments(journey, event_count, rng=rng):
            if not event_within_person_interval(
                point.timestamp,
                actually_from=s.actually_from,
                actually_to=s.actually_to,
            ):
                continue
            rows.append(
                _generate_cdr_row(
                    bs_op=bs_op,
                    operator=operator,
                    mnc=mnc,
                    state=s,
                    rng=rng,
                    idx=seq_idx,
                    started_dt=point.timestamp,
                    duration=max(5, int((next_point.timestamp - point.timestamp).total_seconds()) or s.duration_sec),
                    start_bs_idx=point.bs_idx,
                    end_bs_idx=next_point.bs_idx,
                )
            )
            seq_idx += 1
    return rows


def finalize_cdr_day_parquet_from_rows(
    *,
    rows: list[dict[str, Any]],
    fields: list[dict[str, Any]],
    operator: str,
    day: date,
    out_template: str,
    compression: str,
    bs_op: pd.DataFrame,
    rng: random.Random,
    fallback_state: SubscriberDayState | None,
) -> dict[str, Any]:
    mnc = OPERATOR_MNC[operator]
    bs_op = bs_op.reset_index(drop=True)
    out_rows = list(rows)
    fallback_rows = None
    if not out_rows and fallback_state is not None:
        fallback_rows = [_generate_cdr_row(bs_op, operator, mnc, fallback_state, rng, 0)]
    return write_mobile_day_parquet_by_datacenter(
        rows=out_rows,
        fields=fields,
        operator=operator,
        day=day,
        out_template=out_template,
        compression=compression,
        filename="cdr.parquet",
        coerce_types=lambda df: _cdr_coerce_types(df, fields),
        bs_op=bs_op,
        region_column="RecEntOwnerRegion",
        lac_col="BSStartLac",
        cell_col="BSStartCell",
        fallback_rows=fallback_rows,
    )


def write_cdr_day_from_subscriber_states(
    *,
    bs_op: pd.DataFrame,
    fields: list[dict[str, Any]],
    operator: str,
    day: date,
    seed: int,
    out_template: str,
    compression: str,
    rng: random.Random,
    states: list[SubscriberDayState],
    spatial_ctx: BsSpatialContext | None = None,
) -> dict[str, Any]:
    """Write one CDR parquet given pre-built ``states`` (used by ``run_mobile_oss_all`` to avoid 4× expansion)."""
    if not states:
        return finalize_cdr_day_parquet_from_rows(
            rows=[],
            fields=fields,
            operator=operator,
            day=day,
            out_template=out_template,
            compression=compression,
            bs_op=bs_op,
            rng=rng,
            fallback_state=None,
        )
    rows = generate_cdr_rows_from_subscriber_states(
        bs_op=bs_op,
        operator=operator,
        day=day,
        seed=seed,
        rng=rng,
        states=states,
        spatial_ctx=spatial_ctx,
        seq_idx_start=0,
    )
    return finalize_cdr_day_parquet_from_rows(
        rows=rows,
        fields=fields,
        operator=operator,
        day=day,
        out_template=out_template,
        compression=compression,
        bs_op=bs_op,
        rng=rng,
        fallback_state=states[0],
    )


def _cdr_generate_and_write_day(
    bs_op: pd.DataFrame,
    fields: list[dict[str, Any]],
    operator: str,
    day: date,
    seed: int,
    out_template: str,
    compression: str,
    rng: random.Random,
    person_day: pd.DataFrame | None = None,
    *,
    operator_person_subset: bool = False,
    spatial_ctx: BsSpatialContext | None = None,
) -> dict[str, Any]:
    mnc = OPERATOR_MNC[operator]
    if bs_op.empty:
        raise ValueError(f"No BS rows for operator={operator} (mnc={mnc})")
    bs_op = bs_op.reset_index(drop=True)

    if person_day is not None:
        states = active_subscribers_from_person_for_day(
            operator=operator,
            day=day,
            seed=seed,
            bs_op=bs_op,
            person_day=person_day,
            active_ratio=PERSON_ACTIVE_RATIO_ALL,
            movement_ratio=DEFAULT_MOBILE_OSS_MOVEMENT_RATIO,
            operator_person_subset=operator_person_subset,
            spatial_ctx=spatial_ctx,
        )
    else:
        states = active_subscribers_for_day(
            operator=operator,
            day=day,
            seed=seed,
            bs_op=bs_op,
            aab_per_operator=SYNTHETIC_FALLBACK_AAB_PER_OPERATOR,
            active_ratio=SYNTHETIC_FALLBACK_ACTIVE_RATIO,
            transition_ratio=SYNTHETIC_FALLBACK_TRANSITION_RATIO,
            movement_ratio=SYNTHETIC_FALLBACK_MOVEMENT_RATIO,
        )
    return write_cdr_day_from_subscriber_states(
        bs_op=bs_op,
        fields=fields,
        operator=operator,
        day=day,
        seed=seed,
        out_template=out_template,
        compression=compression,
        rng=rng,
        states=states,
        spatial_ctx=spatial_ctx,
    )


def _generate_cdr_row(
    bs_op: pd.DataFrame,
    operator: str,
    mnc: int,
    state: SubscriberDayState,
    rng: random.Random,
    idx: int,
    started_dt: datetime | None = None,
    duration: int | None = None,
    start_bs_idx: int | None = None,
    end_bs_idx: int | None = None,
) -> dict[str, Any]:
    started_dt = started_dt or state.started_dt
    duration = int(duration if duration is not None else state.duration_sec)
    category = rng.choices([1, 2, 3, 4, 7], weights=[55, 20, 15, 8, 2], k=1)[0]
    service = pick_weighted_service(rng, "cdr")
    start_bs = bs_op.iloc[int(start_bs_idx if start_bs_idx is not None else state.start_bs_idx)]
    end_bs = bs_op.iloc[int(end_bs_idx if end_bs_idx is not None else state.end_bs_idx)]
    peer_num = _cdr_random_msisdn(rng)
    owner = rng.choice([1, 2])
    parties = resolve_owner_parties(
        owner=owner,
        subscriber_msisdn=state.msisdn,
        peer_msisdn=peer_num,
        subscriber_imsi=state.imsi,
        subscriber_imei=state.imei,
    )
    local_offset_h = bs_local_utc_offset_hours(start_bs)
    started_local = started_dt + timedelta(hours=local_offset_h)
    start_lac, start_cell = coerce_valid_lac_cell(start_bs.get("lac"), start_bs.get("cell"), rng=rng)
    end_lac, end_cell = coerce_valid_lac_cell(end_bs.get("lac"), end_bs.get("cell"), rng=rng)
    seq = f"{operator}-{started_local.strftime('%Y%m%d')}-{idx:08d}-{rng.randint(100,999)}"

    row: dict[str, Any] = {
        "Started": started_local.strftime("%Y%m%d%H%M%S"),
        "Duration": duration,
        "Category": category,
        "Event": 10001,
        "Service": service,
        "CallingNumber": parties["calling"],
        "CallingSource": parties["calling"] if rng.random() < 0.7 else f"8{str(parties['calling'])[2:]}",
        "CallingRegion": str(start_bs.get("subject", "unknown")),
        "CalledNumber": parties["called"],
        "CalledSource": parties["called"] if rng.random() < 0.65 else f"8{str(parties['called'])[2:]}",
        "CalledRegion": str(end_bs.get("subject", "unknown")),
        "DialedNumber": peer_num if rng.random() < 0.95 else f"10.{rng.randint(0,255)}.{rng.randint(0,255)}.{rng.randint(1,254)}",
        "Owner": parties["owner"],
        "IMSI": mobile_row_imsi(
            mart="cdr",
            owner=int(parties["owner"]),
            subscriber_imsi=state.imsi,
            parties_imsi=parties["imsi"],
            rng=rng,
        ),
        "IMEI": parties["imei"] or state.imei,
        "BSStartLac": start_lac,
        "BSStartCell": start_cell,
        "BSEndLac": end_lac,
        "BSEndCell": end_cell,
        "RouteIn": rng.choice(["MSC-IN", "SIP-IN", "ROAM-IN", "TRUNK-A"]),
        "RouteOut": rng.choice(["MSC-OUT", "SIP-OUT", "ROAM-OUT", "TRUNK-B"]),
        "RecEntNumber": str(rng.randint(10000, 99999)),
        "OwnerMCCMNC": f"250{mnc:02d}",
        "RecipientMCCMNC": f"250{rng.choice([1,2,20,99]):02d}",
        "RecEntOwnerRegion": str(start_bs.get("subject", "unknown")),
        "dateTimeOriginal": _cdr_random_started_source(started_local, local_offset_h, rng),
        "Custom": rng.choice(["", "src=cdr", "tag=vip", "device=legacy", "route=alt"]),
        "RecEntType": rng.choice(["MSC", "SMSC", "GMSC", "MME"]),
        "PartyEntType": rng.choice(["MSC", "SMSC", "HLR", "VLR"]),
        "SequenceID": seq,
        "CauseDiagnostic": rng.choice(["NORMAL_CLEARING", "BUSY", "NO_ANSWER", "NETWORK_FAILURE"]),
        "OwnerMSRNNumber": _cdr_random_msisdn(rng),
        "OtherMSRNNumber": _cdr_random_msisdn(rng),
        "Intermediate": rng.choice(["", "hop1>hop2", "int-a,int-b", "edge-gw"]),
    }

    _cdr_apply_aggressive_anomalies(row, rng)
    return row


def _random_peer_msisdn(rng: random.Random) -> str:
    if rng.random() * 100 < _INTERNATIONAL_MSISDN_SHARE_PCT:
        cc, nsn_len = rng.choice(_INTERNATIONAL_MSISDN_PROFILES)
        floor = 10 ** (nsn_len - 1)
        nsn = rng.randint(floor, 10**nsn_len - 1)
        return f"+{cc}{nsn}"
    return f"+79{rng.randint(10**8, 10**9 - 1)}"


def _cdr_random_msisdn(rng: random.Random) -> str:
    return _random_peer_msisdn(rng)


def _cdr_random_started_source(started_dt: datetime, offset_hours: int, rng: random.Random) -> str:
    fmt = rng.choice(["basic", "z", "tz_h", "tz_hm"])
    base = started_dt.strftime("%Y%m%dT%H%M%S")
    if fmt == "basic":
        return base
    if fmt == "z" and offset_hours == 0:
        return f"{base}Z"
    sign = "+" if offset_hours >= 0 else "-"
    hh = abs(int(offset_hours))
    if fmt == "tz_h":
        return f"{base}{sign}{hh:02d}"
    return f"{base}{sign}{hh:02d}00"


def _cdr_apply_aggressive_anomalies(row: dict[str, Any], rng: random.Random) -> None:
    """Non-critical fields only — STG path fields stay validated."""
    if rng.random() < 0.004:
        row["Custom"] = rng.choice(["", "???", "bad=kv", None])
    if rng.random() < 0.003:
        row["RouteIn"] = rng.choice([None, "", "???"])
    if rng.random() < 0.003:
        row["Intermediate"] = rng.choice([None, "", "hop?"])
    if rng.random() < 0.002:
        row["CauseDiagnostic"] = rng.choice([None, "", "UNKNOWN_CAUSE"])


def _cdr_coerce_types(data: pd.DataFrame, fields: list[dict[str, Any]]) -> pd.DataFrame:
    for field in fields:
        name = field["name"]
        t = field["type"]
        if name not in data.columns:
            data[name] = pd.NA

        if t == "string":
            data[name] = data[name].astype("string")
            continue

        if t == "int":
            numeric = pd.to_numeric(data[name], errors="coerce")
            numeric = numeric.where(numeric.isna() | ((numeric % 1) == 0))
            data[name] = numeric.astype("Int32")
            continue

        if t in {"uint8", "uint16", "uint32"}:
            numeric = pd.to_numeric(data[name], errors="coerce")
            numeric = numeric.where(numeric.isna() | ((numeric % 1) == 0))
            max_map = {"uint8": np.iinfo(np.uint8).max, "uint16": np.iinfo(np.uint16).max, "uint32": np.iinfo(np.uint32).max}
            max_v = max_map[t]
            numeric = numeric.where(numeric.isna() | ((numeric >= 0) & (numeric <= max_v)))
            dtype_map = {"uint8": "UInt8", "uint16": "UInt16", "uint32": "UInt32"}
            data[name] = numeric.astype(dtype_map[t])
            continue

        # Fallback for unspecified types: keep as string to avoid hard failures.
        data[name] = data[name].astype("string")

    ordered_cols = [field["name"] for field in fields]
    return data[ordered_cols]

# --- sms ---

import json
import logging
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
from tqdm import tqdm

from mobile.command_timing import append_command_metrics, timed_stage


logger = logging.getLogger(__name__)


def run_sms(
    *,
    bs_parquet_path: str | Path,
    params: BuildSrcMobileOssParams,
    compression: str,
    person_layout_template: str = SRC_PERSON_LAYOUT_TEMPLATE,
    person_success_flag: str = SRC_PERSON_SUCCESS_FLAG,
    person_by_month: dict[tuple[int, int], pd.DataFrame] | None = None,
    person_pool_by_op_month: dict[tuple[str, int, int], pd.DataFrame] | None = None,
    bs_by_operator: dict[str, pd.DataFrame] | None = None,
) -> dict[str, Any]:
    perf_metrics: dict[str, Any] = {}
    fields, out_template, compression = _mobile_vitrine_spec("sms", compression=compression)

    if bs_by_operator is not None:
        missing_ops = [op for op in params.operators if op not in bs_by_operator]
        if missing_ops:
            raise ValueError(f"bs_by_operator missing keys for operators: {missing_ops}")
        bs_prep = bs_by_operator
    else:
        bs_path = Path(bs_parquet_path)
        if not bs_path.exists():
            raise FileNotFoundError(f"BS parquet not found: {bs_path}")
        with timed_stage("read_bs_sec", perf_metrics):
            bs = pd.read_parquet(bs_path)
        bs_prep = prepare_bs_by_operator(bs, params.operators)

    task_dates = calendar_dates_inclusive(params.start_date, params.end_date)

    effective_person_by_month = person_by_month if person_by_month is not None else None
    person_pool_by_day: dict[tuple[str, date], pd.DataFrame] | None = None
    person_pool_by_month: dict[tuple[str, int, int], pd.DataFrame] | None = None
    if person_pool_by_op_month is not None:
        person_pool_by_month = person_pool_by_op_month
    elif effective_person_by_month is not None:
        person_pool_by_month = build_person_pool_by_operator_month(effective_person_by_month, params.operators)
    else:
        person_pool_by_day = build_person_pool_by_operator_day(
            person_layout_template=person_layout_template,
            person_success_flag=person_success_flag,
            operators=params.operators,
            task_dates=task_dates,
            columns=PERSON_SNAPSHOT_COLUMNS,
        )

    spatial_by_operator: dict[str, BsSpatialContext | None] = {
        op: _build_bs_spatial_context(bs_prep[op]) for op in params.operators
    }

    def _run_task(operator: str, day: date) -> dict[str, Any]:
        day_seed = abs(hash((operator, day.isoformat(), params.seed))) % (2**32)
        rng = random.Random(day_seed)
        spatial_ctx = spatial_by_operator[operator]
        if person_pool_by_day is not None:
            person_for = person_pool_by_day.get((operator, day), pd.DataFrame())
            operator_subset = True
        elif person_pool_by_month is not None:
            raw = person_pool_by_month.get((operator, day.year, day.month), pd.DataFrame())
            person_for = person_interval_overlaps_day(raw, day)
            operator_subset = True
        else:
            person_for = (
                effective_person_by_month.get((day.year, day.month))
                if effective_person_by_month is not None
                else None
            )
            operator_subset = False
        return _sms_generate_and_write_day(
            bs_op=bs_prep[operator],
            fields=fields,
            operator=operator,
            day=day,
            seed=params.seed,
            out_template=out_template,
            compression=compression,
            rng=rng,
            person_day=person_for,
            operator_person_subset=operator_subset,
            spatial_ctx=spatial_ctx,
        )

    def _run_calendar_day(day: date) -> dict[str, Any]:
        day_rows = 0
        for op in params.operators:
            r = _run_task(op, day)
            day_rows += int(r["row_count"])
        return {"row_count": day_rows}

    logger.info(
        "Starting build-src-sms: calendar_days=%s, workers=%s, period=%s..%s (per-day tasks; src_person=latest _SUCCESS per month when person_config set)",
        len(task_dates),
        params.max_workers,
        params.start_date,
        params.end_date,
    )

    started_at = time.perf_counter()
    generated_rows = 0
    file_count = len(task_dates) * len(params.operators)
    with timed_stage("execution_sec", perf_metrics):
        with ThreadPoolExecutor(max_workers=params.max_workers) as executor:
            futures = [executor.submit(_run_calendar_day, day) for day in task_dates]
            with tqdm(total=len(task_dates), desc="build-src-sms", unit="day") as pbar:
                for future in as_completed(futures):
                    result = future.result()
                    generated_rows += int(result["row_count"])
                    pbar.update(1)
                    pbar.set_postfix(rows=generated_rows, refresh=False)
                    logger.debug(
                        "build-src-sms progress: %s/%s rows=%s",
                        pbar.n,
                        len(task_dates),
                        generated_rows,
                    )

    elapsed = round(time.perf_counter() - started_at, 2)
    logger.info(
        "build-src-sms completed: rows=%s, files=%s, elapsed_sec=%s",
        generated_rows,
        file_count,
        elapsed,
    )
    perf_metrics["elapsed_total_sec"] = elapsed
    perf_metrics["rows"] = int(generated_rows)
    perf_metrics["files"] = int(file_count)
    perf_metrics["workers"] = int(params.max_workers)
    append_command_metrics(command="build-src-sms", metrics=perf_metrics)
    return {
        "row_count": int(generated_rows),
        "file_count": int(file_count),
        "elapsed_sec": elapsed,
        "max_workers": int(params.max_workers),
    }


def generate_sms_rows_from_subscriber_states(
    *,
    bs_op: pd.DataFrame,
    operator: str,
    day: date,
    seed: int,
    rng: random.Random,
    states: list[SubscriberDayState],
    spatial_ctx: BsSpatialContext | None = None,
    seq_idx_start: int = 0,
    bundles: list[SubscriberActivityJourneyBundle] | None = None,
) -> list[dict[str, Any]]:
    mnc = OPERATOR_MNC[operator]
    if bs_op.empty:
        raise ValueError(f"No BS rows for operator={operator} (mnc={mnc})")
    bs_op = bs_op.reset_index(drop=True)
    rows: list[dict[str, Any]] = []
    seq_idx = seq_idx_start
    if bundles is not None:
        if not bundles:
            return []
        for s, activity, journey, _mobility in filter_bundles_by_profile_threshold(bundles, threshold=0.62, shift=0.03):
            if len(journey) == 0:
                continue
            sms_count = int(activity["sms_msgs"])
            for point in spread_journey_points_for_events(journey, sms_count, rng=rng):
                if not event_within_person_interval(
                    point.timestamp,
                    actually_from=s.actually_from,
                    actually_to=s.actually_to,
                ):
                    continue
                rows.append(
                    _generate_sms_row(
                        bs_op=bs_op,
                        mnc=mnc,
                        state=s,
                        rng=rng,
                        idx=seq_idx,
                        started_dt=point.timestamp,
                        start_bs_idx=point.bs_idx,
                    )
                )
                seq_idx += 1
        return rows
    if not states:
        return []
    selected = choose_states(states, threshold=0.62, shift=0.03)
    for s in selected:
        activity = subscriber_daily_activity(s, day=day, seed=seed)
        journey = subscriber_journey_points(
            s, day=day, seed=seed, bs_count=len(bs_op), bs_op=bs_op, spatial_ctx=spatial_ctx
        )
        if len(journey) == 0:
            continue
        sms_count = int(activity["sms_msgs"])
        for point in spread_journey_points_for_events(journey, sms_count, rng=rng):
            if not event_within_person_interval(
                point.timestamp,
                actually_from=s.actually_from,
                actually_to=s.actually_to,
            ):
                continue
            rows.append(
                _generate_sms_row(
                    bs_op=bs_op,
                    mnc=mnc,
                    state=s,
                    rng=rng,
                    idx=seq_idx,
                    started_dt=point.timestamp,
                    start_bs_idx=point.bs_idx,
                )
            )
            seq_idx += 1
    return rows


def finalize_sms_day_parquet_from_rows(
    *,
    rows: list[dict[str, Any]],
    fields: list[dict[str, Any]],
    operator: str,
    day: date,
    out_template: str,
    compression: str,
    bs_op: pd.DataFrame,
    rng: random.Random,
    fallback_state: SubscriberDayState | None,
) -> dict[str, Any]:
    mnc = OPERATOR_MNC[operator]
    bs_op = bs_op.reset_index(drop=True)
    out_rows = list(rows)
    fallback_rows = None
    if not out_rows and fallback_state is not None:
        fallback_rows = [_generate_sms_row(bs_op, mnc, fallback_state, rng, 0)]
    return write_mobile_day_parquet_by_datacenter(
        rows=out_rows,
        fields=fields,
        operator=operator,
        day=day,
        out_template=out_template,
        compression=compression,
        filename="sms.parquet",
        coerce_types=lambda df: _sms_coerce_types(df, fields),
        bs_op=bs_op,
        region_column=None,
        lac_col="Lac",
        cell_col="Cell",
        fallback_rows=fallback_rows,
    )


def write_sms_day_from_subscriber_states(
    *,
    bs_op: pd.DataFrame,
    fields: list[dict[str, Any]],
    operator: str,
    day: date,
    seed: int,
    out_template: str,
    compression: str,
    rng: random.Random,
    states: list[SubscriberDayState],
    spatial_ctx: BsSpatialContext | None = None,
) -> dict[str, Any]:
    if not states:
        return finalize_sms_day_parquet_from_rows(
            rows=[],
            fields=fields,
            operator=operator,
            day=day,
            out_template=out_template,
            compression=compression,
            bs_op=bs_op,
            rng=rng,
            fallback_state=None,
        )
    rows = generate_sms_rows_from_subscriber_states(
        bs_op=bs_op,
        operator=operator,
        day=day,
        seed=seed,
        rng=rng,
        states=states,
        spatial_ctx=spatial_ctx,
        seq_idx_start=0,
    )
    return finalize_sms_day_parquet_from_rows(
        rows=rows,
        fields=fields,
        operator=operator,
        day=day,
        out_template=out_template,
        compression=compression,
        bs_op=bs_op,
        rng=rng,
        fallback_state=states[0],
    )


def _sms_generate_and_write_day(
    bs_op: pd.DataFrame,
    fields: list[dict[str, Any]],
    operator: str,
    day: date,
    seed: int,
    out_template: str,
    compression: str,
    rng: random.Random,
    person_day: pd.DataFrame | None = None,
    *,
    operator_person_subset: bool = False,
    spatial_ctx: BsSpatialContext | None = None,
) -> dict[str, Any]:
    mnc = OPERATOR_MNC[operator]
    if bs_op.empty:
        raise ValueError(f"No BS rows for operator={operator} (mnc={mnc})")
    bs_op = bs_op.reset_index(drop=True)

    if person_day is not None:
        states = active_subscribers_from_person_for_day(
            operator=operator,
            day=day,
            seed=seed,
            bs_op=bs_op,
            person_day=person_day,
            active_ratio=PERSON_ACTIVE_RATIO_ALL,
            movement_ratio=DEFAULT_MOBILE_OSS_MOVEMENT_RATIO,
            operator_person_subset=operator_person_subset,
            spatial_ctx=spatial_ctx,
        )
    else:
        states = active_subscribers_for_day(
            operator=operator,
            day=day,
            seed=seed,
            bs_op=bs_op,
            aab_per_operator=SYNTHETIC_FALLBACK_AAB_PER_OPERATOR,
            active_ratio=SYNTHETIC_FALLBACK_ACTIVE_RATIO,
            transition_ratio=SYNTHETIC_FALLBACK_TRANSITION_RATIO,
            movement_ratio=SYNTHETIC_FALLBACK_MOVEMENT_RATIO,
        )
    return write_sms_day_from_subscriber_states(
        bs_op=bs_op,
        fields=fields,
        operator=operator,
        day=day,
        seed=seed,
        out_template=out_template,
        compression=compression,
        rng=rng,
        states=states,
        spatial_ctx=spatial_ctx,
    )


def _generate_sms_row(
    bs_op: pd.DataFrame,
    mnc: int,
    state: SubscriberDayState,
    rng: random.Random,
    idx: int,
    started_dt: datetime | None = None,
    start_bs_idx: int | None = None,
) -> dict[str, Any]:
    started_dt = (started_dt or state.started_dt) + timedelta(seconds=rng.randint(-180, 180))
    bs_row = bs_op.iloc[int(start_bs_idx if start_bs_idx is not None else state.start_bs_idx)]
    local_offset_h = bs_local_utc_offset_hours(bs_row)
    started_local = started_dt + timedelta(hours=local_offset_h)
    peer_num = _sms_random_msisdn(rng)
    owner = rng.choice([1, 2])
    parties = resolve_owner_parties(
        owner=owner,
        subscriber_msisdn=state.msisdn,
        peer_msisdn=peer_num,
        subscriber_imsi=state.imsi,
        subscriber_imei=state.imei,
    )
    lac, cell = coerce_valid_lac_cell(bs_row.get("lac"), bs_row.get("cell"), rng=rng)

    message = rng.choices(
        population=[
            "Ваш код подтверждения: " + str(rng.randint(1000, 9999)),
            "Списание 199.00 RUB. Баланс: " + str(rng.randint(10, 5000)) + ".00",
            "Ваш заказ принят и передан в доставку.",
            "Привет! Ты сегодня где?",
            "Акция: скидка 30% только сегодня!",
        ],
        weights=[32, 20, 18, 20, 10],
        k=1,
    )[0]

    row: dict[str, Any] = {
        "Started": started_local.strftime("%Y%m%d%H%M%S"),
        "Event": 10002,
        "Calling": parties["calling"],
        "Called": parties["called"],
        "Owner": parties["owner"],
        "SMSC": rng.randint(10**7, 10**9),
        "IMSI": mobile_row_imsi(
            mart="sms",
            owner=int(parties["owner"]),
            subscriber_imsi=state.imsi,
            parties_imsi=parties["imsi"],
            rng=rng,
        ),
        "IMEI": parties["imei"] or state.imei,
        "MCC": 250,
        "MNC": mnc,
        "Lac": lac,
        "Cell": cell,
        "MAC": ":".join(f"{rng.randint(0,255):02x}" for _ in range(6)),
        "BSID": bs_row.get("bsid"),
        "Latitude": float(bs_row.get("coord_y")) if pd.notna(bs_row.get("coord_y")) else None,
        "Longitude": float(bs_row.get("coord_x")) if pd.notna(bs_row.get("coord_x")) else None,
        "PDPAddress.IPV4": f"{rng.randint(1,223)}.{rng.randint(0,255)}.{rng.randint(0,255)}.{rng.randint(1,254)}",
        "PDPAddress.IPV6": "2001:db8:" + ":".join(f"{rng.randint(0,65535):x}" for _ in range(6)),
        "PDPAddress.Port": str(rng.randint(1024, 65535)),
        "Message": message,
        "Custom": rng.choice(["", "src=sms", "tag=otp", "channel=retail", "priority=high"]),
    }

    _sms_apply_aggressive_anomalies(row, rng)
    return row


def _sms_random_msisdn(rng: random.Random) -> str:
    return _random_peer_msisdn(rng)


def _sms_apply_aggressive_anomalies(row: dict[str, Any], rng: random.Random) -> None:
    """Non-critical fields only — STG path fields stay validated."""
    if rng.random() < 0.004:
        row["Custom"] = rng.choice(["", "???", "bad=kv", None])
    if rng.random() < 0.003:
        row["Message"] = rng.choice([None, "", "###", "???"])
    if rng.random() < 0.003:
        row["PDPAddress.IPV4"] = rng.choice([None, "", "999.999.1.1", "bad-ip"])
    if rng.random() < 0.002:
        row["PDPAddress.IPV6"] = rng.choice([None, "", "gggg::1", "bad-ipv6"])


def _sms_coerce_types(data: pd.DataFrame, fields: list[dict[str, Any]]) -> pd.DataFrame:
    for field in fields:
        name = field["name"]
        t = field["type"]
        if name not in data.columns:
            data[name] = pd.NA

        if t == "string":
            data[name] = data[name].astype("string")
        elif t in {"int", "long"}:
            numeric = pd.to_numeric(data[name], errors="coerce")
            numeric = numeric.where(numeric.isna() | ((numeric % 1) == 0))
            data[name] = numeric.astype("Int64")
        elif t == "double":
            data[name] = pd.to_numeric(data[name], errors="coerce").astype("float64")
        else:
            data[name] = data[name].astype("string")

    ordered_cols = [field["name"] for field in fields]
    return data[ordered_cols]

# --- gprs ---

import json
import logging
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from tqdm import tqdm

from mobile.command_timing import append_command_metrics, timed_stage


logger = logging.getLogger(__name__)


def run_gprs(
    *,
    bs_parquet_path: str | Path,
    params: BuildSrcMobileOssParams,
    compression: str,
    person_layout_template: str = SRC_PERSON_LAYOUT_TEMPLATE,
    person_success_flag: str = SRC_PERSON_SUCCESS_FLAG,
    person_by_month: dict[tuple[int, int], pd.DataFrame] | None = None,
    person_pool_by_op_month: dict[tuple[str, int, int], pd.DataFrame] | None = None,
    bs_by_operator: dict[str, pd.DataFrame] | None = None,
) -> dict[str, Any]:
    perf_metrics: dict[str, Any] = {}
    fields, out_template, compression = _mobile_vitrine_spec("gprs", compression=compression)

    if bs_by_operator is not None:
        missing_ops = [op for op in params.operators if op not in bs_by_operator]
        if missing_ops:
            raise ValueError(f"bs_by_operator missing keys for operators: {missing_ops}")
        bs_prep = bs_by_operator
    else:
        bs_path = Path(bs_parquet_path)
        if not bs_path.exists():
            raise FileNotFoundError(f"BS parquet not found: {bs_path}")
        with timed_stage("read_bs_sec", perf_metrics):
            bs = pd.read_parquet(bs_path)
        bs_prep = prepare_bs_by_operator(bs, params.operators)

    task_dates = calendar_dates_inclusive(params.start_date, params.end_date)

    effective_person_by_month = person_by_month if person_by_month is not None else None
    person_pool_by_day: dict[tuple[str, date], pd.DataFrame] | None = None
    person_pool_by_month: dict[tuple[str, int, int], pd.DataFrame] | None = None
    if person_pool_by_op_month is not None:
        person_pool_by_month = person_pool_by_op_month
    elif effective_person_by_month is not None:
        person_pool_by_month = build_person_pool_by_operator_month(effective_person_by_month, params.operators)
    else:
        person_pool_by_day = build_person_pool_by_operator_day(
            person_layout_template=person_layout_template,
            person_success_flag=person_success_flag,
            operators=params.operators,
            task_dates=task_dates,
            columns=PERSON_SNAPSHOT_COLUMNS,
        )

    spatial_by_operator: dict[str, BsSpatialContext | None] = {
        op: _build_bs_spatial_context(bs_prep[op]) for op in params.operators
    }

    def _run_task(operator: str, day: date) -> dict[str, Any]:
        day_seed = abs(hash((operator, day.isoformat(), params.seed))) % (2**32)
        rng = random.Random(day_seed)
        spatial_ctx = spatial_by_operator[operator]
        if person_pool_by_day is not None:
            person_for = person_pool_by_day.get((operator, day), pd.DataFrame())
            operator_subset = True
        elif person_pool_by_month is not None:
            raw = person_pool_by_month.get((operator, day.year, day.month), pd.DataFrame())
            person_for = person_interval_overlaps_day(raw, day)
            operator_subset = True
        else:
            person_for = (
                effective_person_by_month.get((day.year, day.month))
                if effective_person_by_month is not None
                else None
            )
            operator_subset = False
        return _gprs_generate_and_write_day(
            bs_op=bs_prep[operator],
            fields=fields,
            operator=operator,
            day=day,
            seed=params.seed,
            out_template=out_template,
            compression=compression,
            rng=rng,
            person_day=person_for,
            operator_person_subset=operator_subset,
            spatial_ctx=spatial_ctx,
        )

    def _run_calendar_day(day: date) -> dict[str, Any]:
        day_rows = 0
        for op in params.operators:
            r = _run_task(op, day)
            day_rows += int(r["row_count"])
        return {"row_count": day_rows}

    logger.info(
        "Starting build-src-gprs: calendar_days=%s, workers=%s, period=%s..%s (per-day tasks; src_person=latest _SUCCESS per month when person_config set)",
        len(task_dates),
        params.max_workers,
        params.start_date,
        params.end_date,
    )

    started_at = time.perf_counter()
    generated_rows = 0
    file_count = len(task_dates) * len(params.operators)
    with timed_stage("execution_sec", perf_metrics):
        with ThreadPoolExecutor(max_workers=params.max_workers) as executor:
            futures = [executor.submit(_run_calendar_day, day) for day in task_dates]
            with tqdm(total=len(task_dates), desc="build-src-gprs", unit="day") as pbar:
                for future in as_completed(futures):
                    result = future.result()
                    generated_rows += int(result["row_count"])
                    pbar.update(1)
                    pbar.set_postfix(rows=generated_rows, refresh=False)
                    logger.debug(
                        "build-src-gprs progress: %s/%s rows=%s",
                        pbar.n,
                        len(task_dates),
                        generated_rows,
                    )

    elapsed = round(time.perf_counter() - started_at, 2)
    logger.info(
        "build-src-gprs completed: rows=%s, files=%s, elapsed_sec=%s",
        generated_rows,
        file_count,
        elapsed,
    )
    perf_metrics["elapsed_total_sec"] = elapsed
    perf_metrics["rows"] = int(generated_rows)
    perf_metrics["files"] = int(file_count)
    perf_metrics["workers"] = int(params.max_workers)
    append_command_metrics(command="build-src-gprs", metrics=perf_metrics)
    return {
        "row_count": int(generated_rows),
        "file_count": int(file_count),
        "elapsed_sec": elapsed,
        "max_workers": int(params.max_workers),
    }


def generate_gprs_rows_from_subscriber_states(
    *,
    bs_op: pd.DataFrame,
    operator: str,
    day: date,
    seed: int,
    rng: random.Random,
    states: list[SubscriberDayState],
    spatial_ctx: BsSpatialContext | None = None,
    seq_idx_start: int = 0,
    bundles: list[SubscriberActivityJourneyBundle] | None = None,
) -> list[dict[str, Any]]:
    mnc = OPERATOR_MNC[operator]
    if bs_op.empty:
        raise ValueError(f"No BS rows for operator={operator} (mnc={mnc})")
    bs_op = bs_op.reset_index(drop=True)
    rows: list[dict[str, Any]] = []
    seq_idx = seq_idx_start
    if bundles is not None:
        if not bundles:
            return []
        for s, activity, journey, _mobility in filter_bundles_by_profile_threshold(bundles, threshold=0.92, shift=0.02):
            if len(journey) == 0:
                continue
            sessions = int(activity["gprs_sessions"])
            for point, next_point in iter_spread_journey_event_segments(journey, sessions, rng=rng):
                if not event_within_person_interval(
                    point.timestamp,
                    actually_from=s.actually_from,
                    actually_to=s.actually_to,
                ):
                    continue
                rows.append(
                    _generate_gprs_row(
                        bs_op=bs_op,
                        state=s,
                        mnc=mnc,
                        rng=rng,
                        idx=seq_idx,
                        started_dt=point.timestamp,
                        duration=max(30, int((next_point.timestamp - point.timestamp).total_seconds()) or s.duration_sec),
                        start_bs_idx=point.bs_idx,
                        end_bs_idx=next_point.bs_idx,
                    )
                )
                seq_idx += 1
        return rows
    if not states:
        return []
    selected = choose_states(states, threshold=0.92, shift=0.02)
    for s in selected:
        activity = subscriber_daily_activity(s, day=day, seed=seed)
        journey = subscriber_journey_points(
            s, day=day, seed=seed, bs_count=len(bs_op), bs_op=bs_op, spatial_ctx=spatial_ctx
        )
        if len(journey) == 0:
            continue
        sessions = int(activity["gprs_sessions"])
        for point, next_point in iter_spread_journey_event_segments(journey, sessions, rng=rng):
            if not event_within_person_interval(
                point.timestamp,
                actually_from=s.actually_from,
                actually_to=s.actually_to,
            ):
                continue
            rows.append(
                _generate_gprs_row(
                    bs_op=bs_op,
                    state=s,
                    mnc=mnc,
                    rng=rng,
                    idx=seq_idx,
                    started_dt=point.timestamp,
                    duration=max(30, int((next_point.timestamp - point.timestamp).total_seconds()) or s.duration_sec),
                    start_bs_idx=point.bs_idx,
                    end_bs_idx=next_point.bs_idx,
                )
            )
            seq_idx += 1
    return rows


def finalize_gprs_day_parquet_from_rows(
    *,
    rows: list[dict[str, Any]],
    fields: list[dict[str, Any]],
    operator: str,
    day: date,
    out_template: str,
    compression: str,
    bs_op: pd.DataFrame,
    rng: random.Random,
    fallback_state: SubscriberDayState | None,
) -> dict[str, Any]:
    mnc = OPERATOR_MNC[operator]
    bs_op = bs_op.reset_index(drop=True)
    out_rows = list(rows)
    fallback_rows = None
    if not out_rows and fallback_state is not None:
        fallback_rows = [_generate_gprs_row(bs_op, fallback_state, mnc, rng, 0)]
    return write_mobile_day_parquet_by_datacenter(
        rows=out_rows,
        fields=fields,
        operator=operator,
        day=day,
        out_template=out_template,
        compression=compression,
        filename="gprs.parquet",
        coerce_types=lambda df: _gprs_coerce_types(df, fields),
        bs_op=bs_op,
        region_column="RecEntOwnerRegion",
        lac_col="BSStartLac",
        cell_col="BSStartCell",
        fallback_rows=fallback_rows,
    )


def write_gprs_day_from_subscriber_states(
    *,
    bs_op: pd.DataFrame,
    fields: list[dict[str, Any]],
    operator: str,
    day: date,
    seed: int,
    out_template: str,
    compression: str,
    rng: random.Random,
    states: list[SubscriberDayState],
    spatial_ctx: BsSpatialContext | None = None,
) -> dict[str, Any]:
    if not states:
        return finalize_gprs_day_parquet_from_rows(
            rows=[],
            fields=fields,
            operator=operator,
            day=day,
            out_template=out_template,
            compression=compression,
            bs_op=bs_op,
            rng=rng,
            fallback_state=None,
        )
    rows = generate_gprs_rows_from_subscriber_states(
        bs_op=bs_op,
        operator=operator,
        day=day,
        seed=seed,
        rng=rng,
        states=states,
        spatial_ctx=spatial_ctx,
        seq_idx_start=0,
    )
    return finalize_gprs_day_parquet_from_rows(
        rows=rows,
        fields=fields,
        operator=operator,
        day=day,
        out_template=out_template,
        compression=compression,
        bs_op=bs_op,
        rng=rng,
        fallback_state=states[0],
    )


def _gprs_generate_and_write_day(
    bs_op: pd.DataFrame,
    fields: list[dict[str, Any]],
    operator: str,
    day: date,
    seed: int,
    out_template: str,
    compression: str,
    rng: random.Random,
    person_day: pd.DataFrame | None = None,
    *,
    operator_person_subset: bool = False,
    spatial_ctx: BsSpatialContext | None = None,
) -> dict[str, Any]:
    mnc = OPERATOR_MNC[operator]
    if bs_op.empty:
        raise ValueError(f"No BS rows for operator={operator} (mnc={mnc})")
    bs_op = bs_op.reset_index(drop=True)

    if person_day is not None:
        states = active_subscribers_from_person_for_day(
            operator=operator,
            day=day,
            seed=seed,
            bs_op=bs_op,
            person_day=person_day,
            active_ratio=PERSON_ACTIVE_RATIO_ALL,
            movement_ratio=DEFAULT_MOBILE_OSS_MOVEMENT_RATIO,
            operator_person_subset=operator_person_subset,
            spatial_ctx=spatial_ctx,
        )
    else:
        states = active_subscribers_for_day(
            operator=operator,
            day=day,
            seed=seed,
            bs_op=bs_op,
            aab_per_operator=SYNTHETIC_FALLBACK_AAB_PER_OPERATOR,
            active_ratio=SYNTHETIC_FALLBACK_ACTIVE_RATIO,
            transition_ratio=SYNTHETIC_FALLBACK_TRANSITION_RATIO,
            movement_ratio=SYNTHETIC_FALLBACK_MOVEMENT_RATIO,
        )
    return write_gprs_day_from_subscriber_states(
        bs_op=bs_op,
        fields=fields,
        operator=operator,
        day=day,
        seed=seed,
        out_template=out_template,
        compression=compression,
        rng=rng,
        states=states,
        spatial_ctx=spatial_ctx,
    )


def _generate_gprs_row(
    bs_op: pd.DataFrame,
    state: SubscriberDayState,
    mnc: int,
    rng: random.Random,
    idx: int,
    started_dt: datetime | None = None,
    duration: int | None = None,
    start_bs_idx: int | None = None,
    end_bs_idx: int | None = None,
) -> dict[str, Any]:
    started_dt = started_dt or state.started_dt
    duration = int(duration if duration is not None else state.duration_sec)
    upload = rng.randint(5_000, 500_000_000)
    download = rng.randint(10_000, 2_000_000_000)
    start_bs = bs_op.iloc[int(start_bs_idx if start_bs_idx is not None else state.start_bs_idx)]
    end_bs = bs_op.iloc[int(end_bs_idx if end_bs_idx is not None else state.end_bs_idx)]

    peer_num = _gprs_random_msisdn(rng)
    owner = rng.choice([1, 2])
    parties = resolve_owner_parties(
        owner=owner,
        subscriber_msisdn=state.msisdn,
        peer_msisdn=peer_num,
        subscriber_imsi=state.imsi,
        subscriber_imei=state.imei,
    )
    local_offset_h = bs_local_utc_offset_hours(start_bs)
    started_local = started_dt + timedelta(hours=local_offset_h)
    start_lac, start_cell = coerce_valid_lac_cell(start_bs.get("lac"), start_bs.get("cell"), rng=rng)
    end_lac, end_cell = coerce_valid_lac_cell(end_bs.get("lac"), end_bs.get("cell"), rng=rng)
    ipv4 = f"{rng.randint(1,223)}.{rng.randint(0,255)}.{rng.randint(0,255)}.{rng.randint(1,254)}"
    ipv6 = "2001:db8:" + ":".join(f"{rng.randint(0,65535):x}" for _ in range(6))
    if rng.random() < 0.5:
        pdp4, pdp6 = ipv4, ""
    elif rng.random() < 0.9:
        pdp4, pdp6 = "", ipv6
    else:
        pdp4, pdp6 = ipv4, ipv6

    row: dict[str, Any] = {
        "Started": started_local.strftime("%Y%m%d%H%M%S"),
        "Duration": duration,
        "Category": rng.choices([1, 2, 3, 4, 7], weights=[48, 18, 16, 12, 6], k=1)[0],
        "Upload": str(upload),
        "Download": str(download),
        "Event": 10003,
        "Service": pick_weighted_service(rng, "gprs"),
        "CauseDiagnostic": rng.choice(["NORMAL", "VOLUME_LIMIT", "TIME_LIMIT", "NETWORK_RELEASE"]),
        "CallingNumber": parties["calling"],
        "CallingSource": parties["calling"] if rng.random() < 0.75 else f"8{str(parties['calling'])[2:]}",
        "CallingRegion": str(start_bs.get("subject", "unknown")),
        "CalledNumber": parties["called"],
        "CalledSource": parties["called"] if rng.random() < 0.75 else f"8{str(parties['called'])[2:]}",
        "CalledRegion": str(end_bs.get("subject", "unknown")),
        "DialedNumber": ipv4 if rng.random() < 0.6 else f"www.{rng.choice(['ok.ru', 'ya.ru', 'vk.com', 'example.net'])}",
        "Owner": parties["owner"],
        "PDPV4Address": pdp4,
        "PDPV6Address": pdp6,
        "IMSI": mobile_row_imsi(
            mart="gprs",
            owner=int(parties["owner"]),
            subscriber_imsi=state.imsi,
            parties_imsi=parties["imsi"],
            rng=rng,
        ),
        "IMEI": parties["imei"] or state.imei,
        "APN": rng.choice(["internet", "ims", "vpn.corp", "m2m", "public"]),
        "BSStartLac": start_lac,
        "BSStartCell": start_cell,
        "BSEndLac": end_lac,
        "BSEndCell": end_cell,
        "RouteIn": rng.choice(["SGSN-IN", "MME-IN", "ROAM-IN"]),
        "RouteOut": rng.choice(["GGSN-OUT", "PGW-OUT", "ROAM-OUT"]),
        "RecEntType": rng.choice(["SGSN", "GGSN", "PGW", "MME"]),
        "RecEntNumber": str(rng.randint(10000, 99999)),
        "PartyEntType": rng.choice(["SGSN", "GGSN", "PGW", "MME"]),
        "PartyEntNumber": f"10.{rng.randint(0,255)}.{rng.randint(0,255)}.{rng.randint(1,254)}",
        "OwnerMCCMNC": f"250{mnc:02d}",
        "RecipientMCCMNC": f"250{rng.choice([1, 2, 20, 99]):02d}",
        "PartyMCCMNC": f"250{rng.choice([1, 2, 20, 99]):02d}",
        "RecEntOwnerRegion": str(start_bs.get("subject", "unknown")),
        "dateTimeOriginal": _gprs_random_started_source(started_local, local_offset_h, rng),
        "Custom": rng.choice(["", "src=gprs", "rat=lte", "service=data", f"seq={idx}"]),
        "RAT": rng.choice(["UNKNOWN", "UTRAN", "GERAN", "EUTRAN", "NR"]),
        "LT": rng.choice(["CGI", "SAI", "RAI", "TAI", "ECGI"]),
    }

    _gprs_apply_aggressive_anomalies(row, rng)
    return row


def _gprs_random_msisdn(rng: random.Random) -> str:
    return _random_peer_msisdn(rng)


def _gprs_random_started_source(started_dt: datetime, offset_hours: int, rng: random.Random) -> str:
    fmt = rng.choice(["basic", "z", "tz_h", "tz_hm"])
    base = started_dt.strftime("%Y%m%dT%H%M%S")
    if fmt == "basic":
        return base
    if fmt == "z" and offset_hours == 0:
        return f"{base}Z"
    sign = "+" if offset_hours >= 0 else "-"
    hh = abs(int(offset_hours))
    if fmt == "tz_h":
        return f"{base}{sign}{hh:02d}"
    return f"{base}{sign}{hh:02d}00"


def _gprs_apply_aggressive_anomalies(row: dict[str, Any], rng: random.Random) -> None:
    """Non-critical fields only — STG path fields stay validated."""
    if rng.random() < 0.004:
        row["Custom"] = rng.choice(["", "???", "bad=kv", None])
    if rng.random() < 0.003:
        row["RouteIn"] = rng.choice([None, "", "???"])
    if rng.random() < 0.003:
        row["RouteOut"] = rng.choice([None, "", "???"])
    if rng.random() < 0.002:
        row["CauseDiagnostic"] = rng.choice([None, "", "UNKNOWN_CAUSE"])
    # RAT/LT kept by design (supplier Q&A); anomalies only on Custom above.


def _gprs_coerce_types(data: pd.DataFrame, fields: list[dict[str, Any]]) -> pd.DataFrame:
    for field in fields:
        name = field["name"]
        t = field["type"]
        if name not in data.columns:
            data[name] = pd.NA

        if t == "string":
            data[name] = data[name].astype("string")
            continue

        if t == "int":
            numeric = pd.to_numeric(data[name], errors="coerce")
            numeric = numeric.where(numeric.isna() | ((numeric % 1) == 0))
            data[name] = numeric.astype("Int32")
            continue

        if t in {"uint8", "uint16", "uint32"}:
            numeric = pd.to_numeric(data[name], errors="coerce")
            numeric = numeric.where(numeric.isna() | ((numeric % 1) == 0))
            max_map = {"uint8": np.iinfo(np.uint8).max, "uint16": np.iinfo(np.uint16).max, "uint32": np.iinfo(np.uint32).max}
            max_v = max_map[t]
            numeric = numeric.where(numeric.isna() | ((numeric >= 0) & (numeric <= max_v)))
            dtype_map = {"uint8": "UInt8", "uint16": "UInt16", "uint32": "UInt32"}
            data[name] = numeric.astype(dtype_map[t])
            continue

        data[name] = data[name].astype("string")

    ordered_cols = [field["name"] for field in fields]
    return data[ordered_cols]

# --- location ---

import json
import logging
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
from tqdm import tqdm

from mobile.command_timing import append_command_metrics, timed_stage


logger = logging.getLogger(__name__)


def run_location(
    *,
    bs_parquet_path: str | Path,
    params: BuildSrcMobileOssParams,
    compression: str,
    person_layout_template: str = SRC_PERSON_LAYOUT_TEMPLATE,
    person_success_flag: str = SRC_PERSON_SUCCESS_FLAG,
    person_by_month: dict[tuple[int, int], pd.DataFrame] | None = None,
    person_pool_by_op_month: dict[tuple[str, int, int], pd.DataFrame] | None = None,
    bs_by_operator: dict[str, pd.DataFrame] | None = None,
) -> dict[str, Any]:
    perf_metrics: dict[str, Any] = {}
    fields, out_template, compression = _mobile_vitrine_spec("location", compression=compression)
    if bs_by_operator is not None:
        missing_ops = [op for op in params.operators if op not in bs_by_operator]
        if missing_ops:
            raise ValueError(f"bs_by_operator missing keys for operators: {missing_ops}")
        bs_prep = bs_by_operator
    else:
        bs_path = Path(bs_parquet_path)
        if not bs_path.exists():
            raise FileNotFoundError(f"BS parquet not found: {bs_path}")
        with timed_stage("read_bs_sec", perf_metrics):
            bs = pd.read_parquet(bs_path)
        bs_prep = prepare_bs_by_operator(bs, params.operators)

    task_dates = calendar_dates_inclusive(params.start_date, params.end_date)

    effective_person_by_month = person_by_month if person_by_month is not None else None
    person_pool_by_day: dict[tuple[str, date], pd.DataFrame] | None = None
    person_pool_by_month: dict[tuple[str, int, int], pd.DataFrame] | None = None
    if person_pool_by_op_month is not None:
        person_pool_by_month = person_pool_by_op_month
    elif effective_person_by_month is not None:
        person_pool_by_month = build_person_pool_by_operator_month(effective_person_by_month, params.operators)
    else:
        person_pool_by_day = build_person_pool_by_operator_day(
            person_layout_template=person_layout_template,
            person_success_flag=person_success_flag,
            operators=params.operators,
            task_dates=task_dates,
            columns=PERSON_SNAPSHOT_COLUMNS,
        )

    spatial_by_operator: dict[str, BsSpatialContext | None] = {
        op: _build_bs_spatial_context(bs_prep[op]) for op in params.operators
    }

    def _run_task(operator: str, day: date) -> dict[str, Any]:
        day_seed = abs(hash((operator, day.isoformat(), params.seed))) % (2**32)
        rng = random.Random(day_seed)
        spatial_ctx = spatial_by_operator[operator]
        if person_pool_by_day is not None:
            person_for = person_pool_by_day.get((operator, day), pd.DataFrame())
            operator_subset = True
        elif person_pool_by_month is not None:
            raw = person_pool_by_month.get((operator, day.year, day.month), pd.DataFrame())
            person_for = person_interval_overlaps_day(raw, day)
            operator_subset = True
        else:
            person_for = (
                effective_person_by_month.get((day.year, day.month))
                if effective_person_by_month is not None
                else None
            )
            operator_subset = False
        return _location_generate_and_write_day(
            bs_prep[operator],
            fields,
            operator,
            day,
            seed=params.seed,
            out_template=out_template,
            compression=compression,
            rng=rng,
            person_day=person_for,
            operator_person_subset=operator_subset,
            spatial_ctx=spatial_ctx,
        )

    def _run_calendar_day(day: date) -> dict[str, Any]:
        day_rows = 0
        for op in params.operators:
            r = _run_task(op, day)
            day_rows += int(r["row_count"])
        return {"row_count": day_rows}

    logger.info(
        "Starting build-src-location: calendar_days=%s, workers=%s, period=%s..%s (per-day tasks; src_person=latest _SUCCESS per month when person_config set)",
        len(task_dates),
        params.max_workers,
        params.start_date,
        params.end_date,
    )
    started_at = time.perf_counter()
    generated_rows = 0
    with timed_stage("execution_sec", perf_metrics):
        with ThreadPoolExecutor(max_workers=params.max_workers) as executor:
            futures = [executor.submit(_run_calendar_day, day) for day in task_dates]
            with tqdm(total=len(task_dates), desc="build-src-location", unit="day") as pbar:
                for future in as_completed(futures):
                    result = future.result()
                    generated_rows += int(result["row_count"])
                    pbar.update(1)
                    pbar.set_postfix(rows=generated_rows, refresh=False)
    elapsed = round(time.perf_counter() - started_at, 2)
    perf_metrics["elapsed_total_sec"] = elapsed
    perf_metrics["rows"] = int(generated_rows)
    perf_metrics["files"] = int(len(task_dates) * len(params.operators))
    perf_metrics["workers"] = int(params.max_workers)
    append_command_metrics(command="build-src-location", metrics=perf_metrics)
    return {
        "row_count": int(generated_rows),
        "file_count": int(len(task_dates) * len(params.operators)),
        "elapsed_sec": elapsed,
        "max_workers": int(params.max_workers),
    }


def generate_location_rows_from_subscriber_states(
    *,
    bs_op: pd.DataFrame,
    operator: str,
    day: date,
    seed: int,
    rng: random.Random,
    states: list[SubscriberDayState],
    spatial_ctx: BsSpatialContext | None = None,
    bundles: list[SubscriberActivityJourneyBundle] | None = None,
) -> list[dict[str, Any]]:
    mnc = OPERATOR_MNC[operator]
    if bs_op.empty:
        raise ValueError(f"No BS rows for operator={operator} (mnc={mnc})")
    bs_op = bs_op.reset_index(drop=True)
    if bundles is not None:
        if not bundles:
            return []
        rows: list[dict[str, Any]] = []
        for s, _activity, _comms, mobility in bundles:
            rows.extend(
                _location_rows_from_mobility_points(bs_op, s, mobility, mnc, rng, day=day, seed=seed)
            )
        return rows
    if not states:
        return []
    return _location_rows_from_states(bs_op, states, mnc, rng, day=day, seed=seed, spatial_ctx=spatial_ctx)


def finalize_location_day_parquet_from_rows(
    *,
    rows: list[dict[str, Any]],
    fields: list[dict[str, Any]],
    operator: str,
    day: date,
    seed: int,
    out_template: str,
    compression: str,
    bs_op: pd.DataFrame,
    rng: random.Random,
    spatial_ctx: BsSpatialContext | None = None,
    fallback_state: SubscriberDayState | None,
) -> dict[str, Any]:
    mnc = OPERATOR_MNC[operator]
    bs_op = bs_op.reset_index(drop=True)
    out_rows = list(rows)
    fallback_rows = None
    if not out_rows and fallback_state is not None:
        fallback_rows = _location_rows_from_states(
            bs_op, [fallback_state], mnc, rng, day=day, seed=seed, spatial_ctx=spatial_ctx
        )
    return write_mobile_day_parquet_by_datacenter(
        rows=out_rows,
        fields=fields,
        operator=operator,
        day=day,
        out_template=out_template,
        compression=compression,
        filename="location.parquet",
        coerce_types=lambda df: _location_coerce_types(df, fields),
        bs_op=bs_op,
        region_column=None,
        lac_col="Lac",
        cell_col="Cell",
        fallback_rows=fallback_rows or None,
    )


def write_location_day_from_subscriber_states(
    *,
    bs_op: pd.DataFrame,
    fields: list[dict[str, Any]],
    operator: str,
    day: date,
    seed: int,
    out_template: str,
    compression: str,
    rng: random.Random,
    states: list[SubscriberDayState],
    spatial_ctx: BsSpatialContext | None = None,
) -> dict[str, Any]:
    if not states:
        return finalize_location_day_parquet_from_rows(
            rows=[],
            fields=fields,
            operator=operator,
            day=day,
            seed=seed,
            out_template=out_template,
            compression=compression,
            bs_op=bs_op,
            rng=rng,
            spatial_ctx=spatial_ctx,
            fallback_state=None,
        )
    rows = generate_location_rows_from_subscriber_states(
        bs_op=bs_op,
        operator=operator,
        day=day,
        seed=seed,
        rng=rng,
        states=states,
        spatial_ctx=spatial_ctx,
    )
    return finalize_location_day_parquet_from_rows(
        rows=rows,
        fields=fields,
        operator=operator,
        day=day,
        seed=seed,
        out_template=out_template,
        compression=compression,
        bs_op=bs_op,
        rng=rng,
        spatial_ctx=spatial_ctx,
        fallback_state=None,
    )


def _location_generate_and_write_day(
    bs_op: pd.DataFrame,
    fields: list[dict[str, Any]],
    operator: str,
    day: date,
    seed: int,
    out_template: str,
    compression: str,
    rng: random.Random,
    person_day: pd.DataFrame | None = None,
    *,
    operator_person_subset: bool = False,
    spatial_ctx: BsSpatialContext | None = None,
) -> dict[str, Any]:
    mnc = OPERATOR_MNC[operator]
    if bs_op.empty:
        raise ValueError(f"No BS rows for operator={operator} (mnc={mnc})")
    bs_op = bs_op.reset_index(drop=True)

    if person_day is not None:
        states = active_subscribers_from_person_for_day(
            operator=operator,
            day=day,
            seed=seed,
            bs_op=bs_op,
            person_day=person_day,
            active_ratio=PERSON_ACTIVE_RATIO_ALL,
            movement_ratio=DEFAULT_MOBILE_OSS_MOVEMENT_RATIO,
            operator_person_subset=operator_person_subset,
            spatial_ctx=spatial_ctx,
        )
    else:
        states = active_subscribers_for_day(
            operator=operator,
            day=day,
            seed=seed,
            bs_op=bs_op,
            aab_per_operator=SYNTHETIC_FALLBACK_AAB_PER_OPERATOR,
            active_ratio=SYNTHETIC_FALLBACK_ACTIVE_RATIO,
            transition_ratio=SYNTHETIC_FALLBACK_TRANSITION_RATIO,
            movement_ratio=SYNTHETIC_FALLBACK_MOVEMENT_RATIO,
        )
    return write_location_day_from_subscriber_states(
        bs_op=bs_op,
        fields=fields,
        operator=operator,
        day=day,
        seed=seed,
        out_template=out_template,
        compression=compression,
        rng=rng,
        states=states,
        spatial_ctx=spatial_ctx,
    )


def _handover_location_rows_from_points(
    bs_op: pd.DataFrame,
    state: SubscriberDayState,
    points: list[JourneyPoint],
    mnc: int,
    rng: random.Random,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for idx, point in enumerate(points):
        if idx == 0:
            continue
        prev = points[idx - 1]
        if prev.bs_idx == point.bs_idx:
            continue
        if not event_within_person_interval(
            point.timestamp,
            actually_from=state.actually_from,
            actually_to=state.actually_to,
        ):
            continue
        bs_row = bs_op.iloc[point.bs_idx]
        rows.append(_build_location_row(bs_row, mnc, point.timestamp, state.msisdn, state.imsi, state.imei, True, rng))
    return rows


def _location_rows_from_mobility_points(
    bs_op: pd.DataFrame,
    state: SubscriberDayState,
    points: list[JourneyPoint],
    mnc: int,
    rng: random.Random,
    *,
    day: date | None = None,
    seed: int = 0,
) -> list[dict[str, Any]]:
    """Handovers plus periodic updates while camped on the same cell (typical LTE location reports)."""
    if not points:
        return []
    rows = _handover_location_rows_from_points(bs_op, state, points, mnc, rng)
    cal_day = day or points[0].timestamp.date()
    profile = _subscriber_profile(state, day=cal_day, seed=seed)
    dwell_min = mobility_dwell_update_minutes(
        profile_name=profile, moving=state.moving, rng=rng
    )
    for idx in range(len(points)):
        point = points[idx]
        if idx + 1 < len(points):
            next_pt = points[idx + 1]
            dwell_end = next_pt.timestamp
            same_cell = next_pt.bs_idx == point.bs_idx
        else:
            dwell_end = point.timestamp + timedelta(hours=2)
            same_cell = True
        if not same_cell:
            continue
        dwell_sec = int((dwell_end - point.timestamp).total_seconds())
        if dwell_sec < dwell_min * 60:
            continue
        updates = max(1, dwell_sec // (dwell_min * 60))
        bs_row = bs_op.iloc[point.bs_idx]
        for u in range(1, updates + 1):
            ts = point.timestamp + timedelta(minutes=dwell_min * u)
            if ts >= dwell_end:
                break
            if not event_within_person_interval(
                ts,
                actually_from=state.actually_from,
                actually_to=state.actually_to,
            ):
                continue
            rows.append(
                _build_location_row(bs_row, mnc, ts, state.msisdn, state.imsi, state.imei, False, rng)
            )
    return rows


def _location_rows_from_states(
    bs_op: pd.DataFrame,
    states: list[SubscriberDayState],
    mnc: int,
    rng: random.Random,
    *,
    day: date,
    seed: int,
    spatial_ctx: BsSpatialContext | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for s in states:
        points = subscriber_journey_points(
            s,
            day=day,
            seed=seed,
            bs_count=len(bs_op),
            bs_op=bs_op,
            spatial_ctx=spatial_ctx,
            rng_namespace="silent_mobility",
        )
        rows.extend(_location_rows_from_mobility_points(bs_op, s, points, mnc, rng, day=day, seed=seed))
    return rows


def _build_location_row(bs_row: pd.Series, mnc: int, started_dt, served: str, imsi: str, imei: str, handover: bool, rng: random.Random) -> dict[str, Any]:
    ipv4 = f"{rng.randint(1,223)}.{rng.randint(0,255)}.{rng.randint(0,255)}.{rng.randint(1,254)}"
    ipv6 = "2001:db8:" + ":".join(f"{rng.randint(0,65535):x}" for _ in range(6))
    ip4, ip6 = (ipv4, "") if rng.random() < 0.45 else ("", ipv6) if rng.random() < 0.9 else (ipv4, ipv6)
    lac, cell = coerce_valid_lac_cell(bs_row.get("lac"), bs_row.get("cell"), rng=rng)
    lat = float(bs_row["coord_y"]) if pd.notna(bs_row.get("coord_y")) else None
    lon = float(bs_row["coord_x"]) if pd.notna(bs_row.get("coord_x")) else None
    local_offset_h = bs_local_utc_offset_hours(bs_row)
    started_local = started_dt + timedelta(hours=local_offset_h)
    row: dict[str, Any] = {
        "Started": started_local.strftime("%Y%m%d%H%M%S"),
        "Event": 10004,
        "Served": served,
        "IMSI": mobile_row_imsi(
            mart="location",
            owner=1,
            subscriber_imsi=imsi,
            parties_imsi=imsi,
            rng=rng,
        ),
        "IMEI": imei,
        "MCC": "250",
        "MNC": f"{mnc:02d}",
        "Lac": lac,
        "Cell": cell,
        "MAC": rng.randint(0, 2**48 - 1),
        "BSID": bs_row.get("bsid"),
        "Latitude": lat,
        "Longitude": lon,
        "IP4Address": ip4,
        "IP6Address": ip6,
        "Port": rng.randint(1024, 65535),
        "TA": rng.randint(0, 63),
        "Source": rng.randint(1, 16),
        "Custom": rng.choice(["", "src=location", "tag=cell", "tech=lte", "rad=geo", f"handover={1 if handover else 0}"]),
    }
    _location_apply_aggressive_anomalies(row, rng)
    return row


def _location_apply_aggressive_anomalies(row: dict[str, Any], rng: random.Random) -> None:
    """Non-critical fields only — STG path fields stay validated."""
    if rng.random() < 0.004:
        row["Custom"] = rng.choice([None, "", "###", "not=key=value"])
    if rng.random() < 0.003:
        row["IP4Address"] = rng.choice([None, "", "999.999.1.1", "bad-ip"])
    if rng.random() < 0.003:
        row["IP6Address"] = rng.choice([None, "", "gggg::1", "bad-ipv6"])
    if rng.random() < 0.002:
        row["Port"] = rng.choice([None, 0, 70000])
    # Source kept by design (supplier Q&A); no anomalies on Source.


def _location_coerce_types(data: pd.DataFrame, fields: list[dict[str, Any]]) -> pd.DataFrame:
    for field in fields:
        name = field["name"]
        t = field["type"]
        if name not in data.columns:
            data[name] = pd.NA

        if t == "string":
            data[name] = data[name].astype("string")
        elif t in {"int", "long"}:
            numeric = pd.to_numeric(data[name], errors="coerce")
            numeric = numeric.where(numeric.isna() | ((numeric % 1) == 0))
            data[name] = numeric.astype("Int64")
        elif t == "double":
            data[name] = pd.to_numeric(data[name], errors="coerce").astype("float64")
        else:
            data[name] = data[name].astype("string")

    ordered_cols = [field["name"] for field in fields]
    return data[ordered_cols]

# --- public API ---

BuildSrcMobileParams = BuildSrcMobileOssParams


def run_mobile_all(
    *,
    bs_parquet_path: str | Path,
    params: BuildSrcMobileParams,
    compression: str,
    person_layout_template: str = SRC_PERSON_LAYOUT_TEMPLATE,
    person_success_flag: str = SRC_PERSON_SUCCESS_FLAG,
) -> dict[str, Any]:
    return run_mobile_oss_all(
        bs_parquet_path=bs_parquet_path,
        params=params,
        compression=compression,
        person_layout_template=person_layout_template,
        person_success_flag=person_success_flag,
    )


__all__ = ["BuildSrcMobileParams", "run_mobile_all"]
