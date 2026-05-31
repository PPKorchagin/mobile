"""Resolved paths for JSON schemas, parquet layouts, and raw data."""

from __future__ import annotations

import re
from calendar import monthrange
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
_SCHEMA_ROOT = PROJECT_ROOT / "src" / "mobile" / "schema"
_RAW_DATA = PROJECT_ROOT / "src" / "mobile" / "raw_data"

DEFAULT_STG_OKTMO_CSV_PATH = _RAW_DATA / "oktmo_v001.csv"
DEFAULT_STG_OKTMO_OUTPUT_PATH = PROJECT_ROOT / "data" / "stg" / "oktmo.parquet"
DEFAULT_STG_TIME_ZONES_CSV_PATH = _RAW_DATA / "time_zones.csv"
DEFAULT_STG_TIME_ZONES_OUTPUT_PATH = PROJECT_ROOT / "data" / "stg" / "time_zones.parquet"
DEFAULT_STG_TAC_CSV_PATH = _RAW_DATA / "tacdb_v001.csv"
DEFAULT_STG_TAC_OUTPUT_PATH = PROJECT_ROOT / "data" / "stg" / "tac.parquet"
DEFAULT_STG_OKSM_CSV_PATH = _RAW_DATA / "oksm_v001.csv"
DEFAULT_STG_OKSM_OUTPUT_PATH = PROJECT_ROOT / "data" / "stg" / "oksm.parquet"

DEFAULT_BS_PROFILE_PATH = _RAW_DATA / "build_bs_profile_from_opencellid.json"
DEFAULT_BS_LAYOUT = PROJECT_ROOT / "data" / "src" / "bs.parquet"
DEFAULT_TIME_ZONES_RAW_PATH = DEFAULT_STG_TIME_ZONES_CSV_PATH
DEFAULT_SRC_PERSON_SCHEMA_PATH = _SCHEMA_ROOT / "src" / "person.json"

SRC_PERSON_LAYOUT_TEMPLATE = "data/src/person/load_year={YYYY}/load_month={MM}/load_day={DD}"
SRC_PERSON_SUCCESS_FLAG = "_SUCCESS"
DEFAULT_SRC_PERSON_OUTPUT_ROOT = PROJECT_ROOT / "data" / "src" / "person"

DEFAULT_SRC_EXCL_IMSI_OUTPUT = PROJECT_ROOT / "data" / "src" / "excl" / "src_imsi.parquet"
DEFAULT_SRC_EXCL_IMEI_OUTPUT = PROJECT_ROOT / "data" / "src" / "excl" / "src_imei.parquet"
DEFAULT_SRC_EXCL_MSISDN_OUTPUT = PROJECT_ROOT / "data" / "src" / "excl" / "src_msisdn.parquet"

SRC_CDR_LAYOUT_TEMPLATE = "data/src/mobile/{dc}/operator/cdr/{name_operator}/10001/{YYYY}/{MM}/{DD}"
SRC_SMS_LAYOUT_TEMPLATE = "data/src/mobile/{dc}/operator/sms/{name_operator}/10002/{YYYY}/{MM}/{DD}"
SRC_GPRS_LAYOUT_TEMPLATE = "data/src/mobile/{dc}/operator/gprs/{name_operator}/10003/{YYYY}/{MM}/{DD}"
SRC_LOCATION_LAYOUT_TEMPLATE = "data/src/mobile/{dc}/operator/location/{name_operator}/10004/{YYYY}/{MM}/{DD}"

STG_EVENT_LAYOUT_TEMPLATE = "data/stg/event/{YYYY}/{MM}/{DD}/{source_id}/events.parquet"
STG_EVENT_DDS_LAYOUT_TEMPLATE = "data/stg/event_dds/{report_date}/{source_id}.parquet"
STG_MSISDN_IMSI_LAYOUT_TEMPLATE = "data/stg/msisdn_imsi/{report_date}.parquet"
STG_MSISDN_IMEI_LAYOUT_TEMPLATE = "data/stg/msisdn_imei/{report_date}.parquet"
# report_date в шаблоне — всегда YYYY-MM-01 (месячный срез, обновляется ежедневно)
STG_GEO_ALL_LAYOUT_TEMPLATE = "data/stg/geo_all/{report_date}.parquet"
STG_GEO_INTERVALS_LAYOUT_TEMPLATE = "data/stg/geo_intervals/{report_date}.parquet"
STG_PERSON_LAYOUT_TEMPLATE = "data/stg/person/{report_date}.parquet"
STG_PERSON_SIM_LAYOUT_TEMPLATE = "data/stg/person_sim/{report_date}.parquet"
STG_PERSON_ID_LEDGER_LAYOUT_TEMPLATE = "data/stg/person_id_ledger/{report_date}.parquet"
STG_MSISDN_OPERATOR_LAYOUT_TEMPLATE = "data/stg/msisdn_operator/{report_date}.parquet"
STG_BS_LAYOUT_TEMPLATE = "data/stg/bs.parquet"
DEFAULT_STG_EVENT_ROOT = PROJECT_ROOT / "data" / "stg" / "event"
DEFAULT_STG_EVENT_DDS_ROOT = PROJECT_ROOT / "data" / "stg" / "event_dds"
DEFAULT_STG_GEO_ALL_OUTPUT_ROOT = PROJECT_ROOT / "data" / "stg" / "geo_all"
DEFAULT_STG_GEO_INTERVALS_OUTPUT_ROOT = PROJECT_ROOT / "data" / "stg" / "geo_intervals"
DEFAULT_STG_PERSON_OUTPUT_ROOT = PROJECT_ROOT / "data" / "stg" / "person"
DEFAULT_STG_MSISDN_IMSI_SCHEMA_PATH = _SCHEMA_ROOT / "stg" / "msisdn_imsi.json"
DEFAULT_STG_MSISDN_IMEI_SCHEMA_PATH = _SCHEMA_ROOT / "stg" / "msisdn_imei.json"
DEFAULT_STG_PERSON_SCHEMA_PATH = _SCHEMA_ROOT / "stg" / "person.json"
DEFAULT_STG_PERSON_SIM_SCHEMA_PATH = _SCHEMA_ROOT / "stg" / "person_sim.json"
DEFAULT_STG_PERSON_ID_LEDGER_SCHEMA_PATH = _SCHEMA_ROOT / "stg" / "person_id_ledger.json"
DEFAULT_STG_MSISDN_OPERATOR_SCHEMA_PATH = _SCHEMA_ROOT / "stg" / "msisdn_operator.json"
DEFAULT_SRC_BS_SCHEMA_PATH = _SCHEMA_ROOT / "src" / "bs.json"
DEFAULT_STG_BS_SCHEMA_PATH = _SCHEMA_ROOT / "stg" / "bs.json"
DEFAULT_STG_BS_OUTPUT_PATH = PROJECT_ROOT / "data" / "stg" / "bs.parquet"
DEFAULT_STG_EVENT_SCHEMA_PATH = _SCHEMA_ROOT / "stg" / "event.json"

MOBILE_DATA_ROOT = PROJECT_ROOT / "data" / "src" / "mobile"

MART_PARQUET_FILES: dict[str, str] = {
    "cdr": "cdr.parquet",
    "sms": "sms.parquet",
    "gprs": "gprs.parquet",
    "location": "location.parquet",
}

_CALENDAR_DAY_IN_PATH = re.compile(r"/(\d{4})/(\d{2})/(\d{2})/[^/]+\.parquet$", re.IGNORECASE)
_DDS_EVENT_IN_PATH = re.compile(
    r"/event_dds/(\d{4})-(\d{2})-(\d{2})/([^/]+)\.parquet$",
    re.IGNORECASE,
)


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


def mobile_datacenter_root(datacenter_id: str) -> Path:
    """Корень витрин одного ЦОД: ``data/src/mobile/{central|far-east}/``."""
    return MOBILE_DATA_ROOT / datacenter_id


def mobile_mart_paths(
    datacenter_id: str,
    *,
    mobile_root: Path | None = None,
) -> dict[str, Path]:
    """Корни витрин CDR/SMS/GPRS/location под одним ЦОД."""
    root = mobile_root if mobile_root is not None else mobile_datacenter_root(datacenter_id)
    return {
        "cdr": root / "operator" / "cdr",
        "sms": root / "operator" / "sms",
        "gprs": root / "operator" / "gprs",
        "location": root / "operator" / "location",
    }


def calendar_month_end(day: date) -> date:
    """Последний календарный день месяца для ``day``."""
    return date(day.year, day.month, monthrange(day.year, day.month)[1])


def resolve_project_path(path: str | Path) -> Path:
    p = Path(path)
    if not p.is_absolute():
        p = PROJECT_ROOT / p
    return p


def calendar_day_key_from_path(path: Path) -> str | None:
    m = _CALENDAR_DAY_IN_PATH.search(path.as_posix())
    if not m:
        return None
    return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"


def stg_event_dds_day_key_from_path(path: Path) -> str | None:
    """Ключ отчётного дня из DDS-пути: ``…/event_dds/YYYY-MM-DD/{source_id}.parquet``."""
    m = _DDS_EVENT_IN_PATH.search(path.as_posix())
    if not m:
        return None
    return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"


def stg_event_dds_source_id_from_path(path: Path) -> str | None:
    """``source_id`` (ЦОД) из DDS-пути ``…/event_dds/YYYY-MM-DD/{source_id}.parquet``."""
    m = _DDS_EVENT_IN_PATH.search(path.as_posix())
    if not m:
        return None
    return str(m.group(4))


def discover_mart_parquet_paths(mart_root: Path, mart_file: str) -> list[Path]:
    if mart_root.is_file() and mart_root.name == mart_file:
        return [mart_root]
    if mart_root.is_dir():
        return sorted(mart_root.rglob(mart_file))
    return []


def filter_paths_near_report_date(
    paths: list[Path],
    *,
    report_date: date,
    slack_days: int = 1,
) -> list[Path]:
    lo = report_date - timedelta(days=slack_days)
    hi = report_date + timedelta(days=slack_days)
    out: list[Path] = []
    for p in paths:
        day_iso = calendar_day_key_from_path(p)
        if day_iso is None:
            out.append(p)
            continue
        d = date.fromisoformat(day_iso)
        if lo <= d <= hi:
            out.append(p)
    return out


def started_parseable_mask(series: pd.Series | None) -> pd.Series:
    if series is None or len(series) == 0:
        return pd.Series(dtype=bool)
    s = series.astype("string").str.strip()
    return s.notna() & (s.str.len() == 14) & s.str.fullmatch(r"\d{14}", na=False)


def local_report_date_mask(df: pd.DataFrame, report_date: date) -> pd.Series:
    """``Started`` в локальном времени абонента (см. build-src-mobile)."""
    if df.empty or "Started" not in df.columns:
        return pd.Series(False, index=df.index)
    day_str = report_date.strftime("%Y%m%d")
    s = df["Started"].astype("string").str.strip()
    return started_parseable_mask(s) & (s.str[:8] == day_str)


def filter_df_by_local_report_date(df: pd.DataFrame, report_date: date) -> pd.DataFrame:
    if df.empty:
        return df
    mask = local_report_date_mask(df, report_date)
    if not bool(mask.any()):
        return df.iloc[0:0].copy()
    return df.loc[mask].copy()


def read_all_parquets_concat(paths: list[Path], *, columns: list[str] | None = None) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []
    for p in paths:
        try:
            parts.append(pd.read_parquet(p, columns=columns))
        except Exception:
            continue
    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, ignore_index=True)


_NB = Path(__file__).resolve().parent / "pipelines" / "nb"
_DATA_NOTEBOOKS = PROJECT_ROOT / "data" / "notebooks"

DEFAULT_PERF_METRICS_NOTEBOOK_PATH = _NB / "perf_metrics.ipynb"
DEFAULT_PERF_METRICS_EXECUTED_PATH = _DATA_NOTEBOOKS / "perf_metrics.executed.ipynb"
DEFAULT_NB_STG_OKTMO_NOTEBOOK_PATH = _NB / "1_stg_oktmo.ipynb"
DEFAULT_NB_STG_OKTMO_EXECUTED_PATH = _DATA_NOTEBOOKS / "1_stg_oktmo.executed.ipynb"
DEFAULT_NB_STG_TIME_ZONES_NOTEBOOK_PATH = _NB / "2_stg_time_zones.ipynb"
DEFAULT_NB_STG_TIME_ZONES_EXECUTED_PATH = _DATA_NOTEBOOKS / "2_stg_time_zones.executed.ipynb"
DEFAULT_NB_STG_TAC_NOTEBOOK_PATH = _NB / "3_stg_tac.ipynb"
DEFAULT_NB_STG_TAC_EXECUTED_PATH = _DATA_NOTEBOOKS / "3_stg_tac.executed.ipynb"
DEFAULT_NB_STG_OKSM_NOTEBOOK_PATH = _NB / "4_stg_oksm.ipynb"
DEFAULT_NB_STG_OKSM_EXECUTED_PATH = _DATA_NOTEBOOKS / "4_stg_oksm.executed.ipynb"
DEFAULT_NB_SRC_BS_NOTEBOOK_PATH = _NB / "5_src_bs.ipynb"
DEFAULT_NB_SRC_BS_EXECUTED_PATH = _DATA_NOTEBOOKS / "5_src_bs.executed.ipynb"
DEFAULT_NB_SRC_PERSON_NOTEBOOK_PATH = _NB / "6_src_person.ipynb"
DEFAULT_NB_SRC_PERSON_EXECUTED_PATH = _DATA_NOTEBOOKS / "6_src_person.executed.ipynb"
DEFAULT_NB_SRC_EXCL_NOTEBOOK_PATH = _NB / "7_src_excl.ipynb"
DEFAULT_NB_SRC_EXCL_EXECUTED_PATH = _DATA_NOTEBOOKS / "7_src_excl.executed.ipynb"
DEFAULT_NOTEBOOK_KERNEL_NAME = "mobile"
DEFAULT_NOTEBOOK_RESOURCES_PATH = PROJECT_ROOT


def resolve_oktmo_layout() -> Path:
    return DEFAULT_STG_OKTMO_OUTPUT_PATH


def stg_event_output_path(source_id: str, day: date) -> Path:
    """Parquet событий за отчётный день и ЦОД: ``data/stg/event/{YYYY}/{MM}/{DD}/{source_id}/events.parquet``."""
    resolved = STG_EVENT_LAYOUT_TEMPLATE.format(
        YYYY=day.strftime("%Y"),
        MM=day.strftime("%m"),
        DD=day.strftime("%d"),
        source_id=source_id,
    )
    return PROJECT_ROOT / resolved


def stg_event_dds_output_path(source_id: str, day: date) -> Path:
    """DDS-слой: ``data/stg/event_dds/{YYYY-MM-DD}/{source_id}.parquet``."""
    resolved = STG_EVENT_DDS_LAYOUT_TEMPLATE.format(
        report_date=day.isoformat(),
        source_id=source_id,
    )
    return PROJECT_ROOT / resolved


def report_month_start(day: date) -> date:
    """1-е число календарного месяца для ``day``."""
    return day.replace(day=1)


def stg_msisdn_imsi_output_path(day: date) -> Path:
    """Месячный ``stg_msisdn_imsi``: ``data/stg/msisdn_imsi/{YYYY-MM-01}.parquet``."""
    month = report_month_start(day)
    return PROJECT_ROOT / STG_MSISDN_IMSI_LAYOUT_TEMPLATE.format(report_date=month.isoformat())


def stg_msisdn_imei_output_path(day: date) -> Path:
    """Месячный ``stg_msisdn_imei``: ``data/stg/msisdn_imei/{YYYY-MM-01}.parquet``."""
    month = report_month_start(day)
    return PROJECT_ROOT / STG_MSISDN_IMEI_LAYOUT_TEMPLATE.format(report_date=month.isoformat())


def stg_bs_output_path() -> Path:
    """``data/stg/bs.parquet``."""
    return PROJECT_ROOT / STG_BS_LAYOUT_TEMPLATE


def stg_geo_all_output_path(day: date) -> Path:
    """``data/stg/geo_all/{YYYY-MM-DD}.parquet``."""
    return PROJECT_ROOT / STG_GEO_ALL_LAYOUT_TEMPLATE.format(report_date=day.isoformat())


def stg_geo_intervals_output_path(day: date) -> Path:
    """``data/stg/geo_intervals/{YYYY-MM-DD}.parquet``."""
    return PROJECT_ROOT / STG_GEO_INTERVALS_LAYOUT_TEMPLATE.format(report_date=day.isoformat())


def stg_person_output_path(day: date) -> Path:
    """``data/stg/person/{YYYY-MM-DD}.parquet``."""
    return PROJECT_ROOT / STG_PERSON_LAYOUT_TEMPLATE.format(report_date=day.isoformat())


def stg_person_sim_output_path(day: date) -> Path:
    return PROJECT_ROOT / STG_PERSON_SIM_LAYOUT_TEMPLATE.format(report_date=day.isoformat())


def stg_person_id_ledger_output_path(day: date) -> Path:
    return PROJECT_ROOT / STG_PERSON_ID_LEDGER_LAYOUT_TEMPLATE.format(report_date=day.isoformat())


def stg_msisdn_operator_output_path(day: date) -> Path:
    return PROJECT_ROOT / STG_MSISDN_OPERATOR_LAYOUT_TEMPLATE.format(report_date=day.isoformat())


