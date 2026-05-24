"""Resolved paths for JSON schemas, parquet layouts, and raw data."""

from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
_SCHEMA_ROOT = PROJECT_ROOT / "src" / "mobile" / "schema"

DEFAULT_STG_OKTMO_CONFIG_PATH = _SCHEMA_ROOT / "stg" / "oktmo.json"
DEFAULT_STG_TIME_ZONES_CONFIG_PATH = _SCHEMA_ROOT / "stg" / "time_zones.json"
DEFAULT_STG_TAC_CONFIG_PATH = _SCHEMA_ROOT / "stg" / "tac.json"

_NB = Path(__file__).resolve().parent / "nb"
_DATA_NOTEBOOKS = PROJECT_ROOT / "data" / "notebooks"

DEFAULT_PERF_METRICS_NOTEBOOK_PATH = _NB / "perf_metrics.ipynb"
DEFAULT_PERF_METRICS_EXECUTED_PATH = _DATA_NOTEBOOKS / "perf_metrics.executed.ipynb"
DEFAULT_NOTEBOOK_KERNEL_NAME = "mobile"
DEFAULT_NOTEBOOK_RESOURCES_PATH = PROJECT_ROOT
