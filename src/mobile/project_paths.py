"""Resolved paths for JSON schemas, parquet layouts, and raw data."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
_SCHEMA_ROOT = PROJECT_ROOT / "src" / "mobile" / "schema"
_RAW_DATA = PROJECT_ROOT / "src" / "mobile" / "raw_data"

DEFAULT_STG_OKTMO_CSV_PATH = _RAW_DATA / "oktmo_v001.csv"
DEFAULT_STG_OKTMO_OUTPUT_PATH = PROJECT_ROOT / "data" / "stg" / "oktmo.parquet"
DEFAULT_STG_TIME_ZONES_CSV_PATH = _RAW_DATA / "time_zones.csv"
DEFAULT_STG_TIME_ZONES_OUTPUT_PATH = PROJECT_ROOT / "data" / "stg" / "time_zones.parquet"
DEFAULT_STG_TAC_CSV_PATH = _RAW_DATA / "tacdb_v001.csv"
DEFAULT_STG_TAC_OUTPUT_PATH = PROJECT_ROOT / "data" / "stg" / "tac.parquet"

DEFAULT_BS_PROFILE_PATH = _RAW_DATA / "build_bs_profile_from_opencellid.json"
DEFAULT_BS_LAYOUT = PROJECT_ROOT / "data" / "src" / "bs.parquet"
DEFAULT_TIME_ZONES_RAW_PATH = DEFAULT_STG_TIME_ZONES_CSV_PATH

SRC_PERSON_LAYOUT_TEMPLATE = "data/src/person/load_year={YYYY}/load_month={MM}/load_day={DD}"
SRC_PERSON_SUCCESS_FLAG = "_SUCCESS"

DEFAULT_SRC_EXCL_IMSI_OUTPUT = PROJECT_ROOT / "data" / "src" / "excl" / "src_imsi.parquet"
DEFAULT_SRC_EXCL_IMEI_OUTPUT = PROJECT_ROOT / "data" / "src" / "excl" / "src_imei.parquet"
DEFAULT_SRC_EXCL_MSISDN_OUTPUT = PROJECT_ROOT / "data" / "src" / "excl" / "src_msisdn.parquet"

SRC_CDR_LAYOUT_TEMPLATE = "data/src/mobile/{dc}/operator/cdr/{name_operator}/10001/{YYYY}/{MM}/{DD}"
SRC_SMS_LAYOUT_TEMPLATE = "data/src/mobile/{dc}/operator/sms/{name_operator}/10002/{YYYY}/{MM}/{DD}"
SRC_GPRS_LAYOUT_TEMPLATE = "data/src/mobile/{dc}/operator/gprs/{name_operator}/10003/{YYYY}/{MM}/{DD}"
SRC_LOCATION_LAYOUT_TEMPLATE = "data/src/mobile/{dc}/operator/location/{name_operator}/10004/{YYYY}/{MM}/{DD}"


@dataclass(frozen=True)
class MobileDatacenterSpec:
    """Синтетический ЦОД: каталог ``data/src/mobile/{id}/`` и привязка к субъектам РФ."""

    id: str
    title: str
    subjects: frozenset[str]


DEFAULT_MOBILE_DATACENTERS: tuple[MobileDatacenterSpec, ...] = (
    MobileDatacenterSpec(
        "central",
        "Центральный",
        frozenset(
            {
                "Тюменская область",
                "Красноярский край",
            }
        ),
    ),
    MobileDatacenterSpec(
        "far-east",
        "Дальневосточный",
        frozenset({"Республика Саха (Якутия)"}),
    ),
)


def mobile_datacenter_ids() -> tuple[str, ...]:
    return tuple(dc.id for dc in DEFAULT_MOBILE_DATACENTERS)


def subject_to_mobile_datacenter(subject: str) -> str:
    name = str(subject or "").strip()
    for spec in DEFAULT_MOBILE_DATACENTERS:
        if name in spec.subjects:
            return spec.id
    return DEFAULT_MOBILE_DATACENTERS[0].id

_NB = Path(__file__).resolve().parent / "nb"
_DATA_NOTEBOOKS = PROJECT_ROOT / "data" / "notebooks"

DEFAULT_PERF_METRICS_NOTEBOOK_PATH = _NB / "perf_metrics.ipynb"
DEFAULT_PERF_METRICS_EXECUTED_PATH = _DATA_NOTEBOOKS / "perf_metrics.executed.ipynb"
DEFAULT_NOTEBOOK_KERNEL_NAME = "mobile"
DEFAULT_NOTEBOOK_RESOURCES_PATH = PROJECT_ROOT


def stg_load_day_root(day: date) -> Path:
    """Каталог среза STG за календарный день: ``data/stg/load_day=YYYY-MM-DD/``."""
    return PROJECT_ROOT / "data" / "stg" / f"load_day={day.isoformat()}"


def stg_load_day_paths(day: date) -> dict[str, Path]:
    """CSV (общие raw) и parquet-выходы в каталоге ``load_day``."""
    root = stg_load_day_root(day)
    return {
        "oktmo_csv_path": DEFAULT_STG_OKTMO_CSV_PATH,
        "oktmo_output_path": root / "oktmo.parquet",
        "time_zones_csv_path": DEFAULT_STG_TIME_ZONES_CSV_PATH,
        "time_zones_output_path": root / "time_zones.parquet",
        "tac_csv_path": DEFAULT_STG_TAC_CSV_PATH,
        "tac_output_path": root / "tac.parquet",
    }


def resolve_oktmo_layout() -> Path:
    return DEFAULT_STG_OKTMO_OUTPUT_PATH
