from __future__ import annotations

import logging
import re
import time
from pathlib import Path
from typing import Any

import pandas as pd

from mobile.command_timing import append_command_metrics, timed_stage
from mobile.project_paths import PROJECT_ROOT

logger = logging.getLogger(__name__)

STG_TAC_TABLE = "stg_tac"

_TAC_RE = re.compile(r"^\d{8}$")

CSV_SEP = ";"
CSV_ENCODING = "utf-8-sig"

SOURCE_MAPPING_COLUMNS: dict[str, str] = {
    "tac": "tac",
    "manufacturer": "manufacturer",
    "model_name": "model_name",
    "marketing_name": "marketing_name",
    "equipment_type": "equipment_type",
    "radio_technology": "radio_technology",
    "sim_form_factor": "sim_form_factor",
    "allocation_date": "allocation_date",
    "reporting_body": "reporting_body",
    "chipset": "chipset",
    "comment": "comment",
}

M2M_EQUIPMENT_TYPES: frozenset[str] = frozenset(
    {
        "Module",
        "WLAN Router",
        "Vehicle Unit",
        "IoT Device",
        "Modem",
        "M2M Module",
    }
)

STG_TAC_FIELDS: list[dict[str, str]] = [
    {"name": "tac", "type": "string"},
    {"name": "manufacturer", "type": "string"},
    {"name": "model_name", "type": "string"},
    {"name": "marketing_name", "type": "string"},
    {"name": "equipment_type", "type": "string"},
    {"name": "radio_technology", "type": "string"},
    {"name": "sim_form_factor", "type": "string"},
    {"name": "allocation_date", "type": "string"},
    {"name": "reporting_body", "type": "string"},
    {"name": "chipset", "type": "string"},
    {"name": "comment", "type": "string"},
    {"name": "is_m2m", "type": "bool"},
]


def run(
    *,
    csv_path: str | Path,
    output_path: str | Path,
    compression: str,
) -> dict[str, Any]:
    csv_file = _resolve_path(csv_path)
    parquet_file = _resolve_path(output_path)

    if not csv_file.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_file}")

    perf: dict[str, Any] = {}
    started = time.perf_counter()

    csv_kwargs: dict[str, Any] = {
        "sep": CSV_SEP,
        "encoding": CSV_ENCODING,
    }

    logger.info("Reading source CSV: %s", csv_file)
    with timed_stage("read_csv_sec", perf):
        raw = pd.read_csv(csv_file, **csv_kwargs)
        data = _prepare_dataset(raw, SOURCE_MAPPING_COLUMNS, STG_TAC_FIELDS, M2M_EQUIPMENT_TYPES)

    with timed_stage("write_parquet_sec", perf):
        parquet_file.parent.mkdir(parents=True, exist_ok=True)
        data.to_parquet(parquet_file, compression=compression, index=False)

    m2m_count = int(data["is_m2m"].sum()) if "is_m2m" in data.columns else 0
    logger.info(
        "%s parquet created: path=%s rows=%s m2m_rows=%s columns=%s compression=%s",
        STG_TAC_TABLE,
        parquet_file,
        len(data),
        m2m_count,
        len(data.columns),
        compression,
    )
    stats = {
        "table": STG_TAC_TABLE,
        "source_csv": str(csv_file),
        "output_parquet": str(parquet_file),
        "row_count": int(len(data)),
        "m2m_row_count": m2m_count,
        "column_count": int(len(data.columns)),
        "parquet_compression": compression,
    }
    perf["elapsed_total_sec"] = round(time.perf_counter() - started, 4)
    append_command_metrics(command="build-stg-tac", metrics={**stats, **perf})
    return stats


def _resolve_path(path: str | Path) -> Path:
    candidate = Path(path)
    return candidate if candidate.is_absolute() else PROJECT_ROOT / candidate


def _prepare_dataset(
    chunk: pd.DataFrame,
    source_mapping: dict[str, str],
    fields: list[dict[str, str]],
    m2m_types: frozenset[str],
) -> pd.DataFrame:
    missing_sources = [src for src in source_mapping.values() if src not in chunk.columns]
    if missing_sources:
        raise ValueError(f"Missing source CSV columns: {missing_sources}")

    rename_map = {src: dst for dst, src in source_mapping.items()}
    renamed = chunk.rename(columns=rename_map)

    target_columns = [field["name"] for field in fields if field["name"] != "is_m2m"]
    missing_targets = [col for col in target_columns if col not in renamed.columns]
    if missing_targets:
        raise ValueError(f"Mapped columns missing in dataset: {missing_targets}")

    selected = renamed[target_columns].copy()

    selected["tac"] = (
        selected["tac"]
        .astype("string")
        .str.strip()
        .str.replace(r"\D", "", regex=True)
        .str.zfill(8)
        .str[-8:]
    )
    invalid_tac = ~selected["tac"].str.fullmatch(_TAC_RE.pattern)
    if int(invalid_tac.sum()) > 0:
        bad = selected.loc[invalid_tac, "tac"].head(5).tolist()
        raise ValueError(f"Invalid TAC values (expected 8 digits): {bad}")

    for col in (
        "manufacturer",
        "model_name",
        "marketing_name",
        "equipment_type",
        "radio_technology",
        "sim_form_factor",
        "reporting_body",
        "chipset",
        "comment",
    ):
        if col in selected.columns:
            selected[col] = selected[col].astype("string").str.strip()

    selected["allocation_date"] = _parse_allocation_dates(selected["allocation_date"])

    equipment = selected["equipment_type"].fillna("")
    selected["is_m2m"] = equipment.isin(m2m_types).astype("boolean")

    ordered = [field["name"] for field in fields]
    out = selected[ordered].copy()
    for field in fields:
        col = field["name"]
        logical_type = field["type"].lower()
        if logical_type == "string":
            out[col] = out[col].astype("string")
        elif logical_type == "bool":
            out[col] = out[col].astype("boolean")
        else:
            raise ValueError(f"Unsupported type '{field['type']}' for field '{col}'")

    duplicate_tac = int(out["tac"].duplicated(keep=False).sum())
    if duplicate_tac > 0:
        dupes = out.loc[out["tac"].duplicated(keep=False), "tac"].unique()[:5].tolist()
        raise ValueError(f"Duplicate TAC in source after normalize: {dupes}")

    return out


def _parse_allocation_dates(series: pd.Series) -> pd.Series:
    raw = series.astype("string").str.strip()
    parsed = pd.to_datetime(raw, format="%d.%m.%Y", errors="coerce")
    needs_fallback = parsed.isna() & raw.notna() & (raw != "")
    if needs_fallback.any():
        parsed = parsed.copy()
        parsed.loc[needs_fallback] = pd.to_datetime(raw.loc[needs_fallback], dayfirst=True, errors="coerce")
    combined = parsed
    invalid = int((combined.isna() & raw.notna() & (raw != "")).sum())
    if invalid > 0:
        samples = raw[combined.isna() & raw.notna()].head(5).tolist()
        raise ValueError(f"Unparseable allocation_date values: {samples}")
    return combined.dt.strftime("%Y-%m-%d").astype("string")
