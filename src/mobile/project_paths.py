"""Resolved paths for JSON schemas, parquet layouts, and raw data."""

from __future__ import annotations

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
DEFAULT_SRC_BS_CONFIG_PATH = _SCHEMA_ROOT / "src" / "bs.json"
DEFAULT_SRC_PERSON_CONFIG_PATH = _SCHEMA_ROOT / "src" / "person.json"
DEFAULT_SRC_IMSI_CONFIG_PATH = _SCHEMA_ROOT / "src" / "imsi.json"
DEFAULT_SRC_IMEI_CONFIG_PATH = _SCHEMA_ROOT / "src" / "imei.json"
DEFAULT_SRC_MSISDN_CONFIG_PATH = _SCHEMA_ROOT / "src" / "msisdn.json"

DEFAULT_BS_PROFILE_PATH = _RAW_DATA / "build_bs_profile_from_opencellid.json"
DEFAULT_BS_LAYOUT = PROJECT_ROOT / "data" / "src" / "bs.parquet"
DEFAULT_TIME_ZONES_RAW_PATH = DEFAULT_STG_TIME_ZONES_CSV_PATH

DEFAULT_SRC_CDR_CONFIG_PATH = _SCHEMA_ROOT / "src" / "cdr.json"
DEFAULT_SRC_SMS_CONFIG_PATH = _SCHEMA_ROOT / "src" / "sms.json"
DEFAULT_SRC_GPRS_CONFIG_PATH = _SCHEMA_ROOT / "src" / "gprs.json"
DEFAULT_SRC_LOCATION_CONFIG_PATH = _SCHEMA_ROOT / "src" / "location.json"

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
