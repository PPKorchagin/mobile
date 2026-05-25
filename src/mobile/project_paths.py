"""Resolved paths for JSON schemas, parquet layouts, and raw data."""

from __future__ import annotations

import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
_SCHEMA_ROOT = PROJECT_ROOT / "src" / "mobile" / "schema"
_RAW_DATA = PROJECT_ROOT / "src" / "mobile" / "raw_data"

DEFAULT_STG_OKTMO_CONFIG_PATH = _SCHEMA_ROOT / "stg" / "oktmo.json"
DEFAULT_STG_TIME_ZONES_CONFIG_PATH = _SCHEMA_ROOT / "stg" / "time_zones.json"
DEFAULT_STG_TAC_CONFIG_PATH = _SCHEMA_ROOT / "stg" / "tac.json"
DEFAULT_SRC_BS_CONFIG_PATH = _SCHEMA_ROOT / "src" / "bs.json"
DEFAULT_SRC_PERSON_CONFIG_PATH = _SCHEMA_ROOT / "src" / "person.json"
DEFAULT_SRC_IMSI_CONFIG_PATH = _SCHEMA_ROOT / "src" / "imsi.json"
DEFAULT_SRC_IMEI_CONFIG_PATH = _SCHEMA_ROOT / "src" / "imei.json"
DEFAULT_SRC_MSISDN_CONFIG_PATH = _SCHEMA_ROOT / "src" / "msisdn.json"

DEFAULT_BS_PROFILE_PATH = _RAW_DATA / "build_bs_profile_from_opencellid.json"
DEFAULT_BS_LAYOUT = PROJECT_ROOT / "data" / "src" / "bs.parquet"
DEFAULT_TIME_ZONES_RAW_PATH = _RAW_DATA / "time_zones.csv"

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


def resolve_oktmo_layout(config_path: str | Path = DEFAULT_STG_OKTMO_CONFIG_PATH) -> Path:
    cfg_path = Path(config_path)
    with cfg_path.open("r", encoding="utf-8") as file:
        cfg = json.load(file)
    return PROJECT_ROOT / cfg["readiness"]["s3_layout"]
