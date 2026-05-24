from __future__ import annotations

import json
import logging
import re
import time
from pathlib import Path
from typing import Any

import pandas as pd

from mobile.command_timing import append_command_metrics, timed_stage
from mobile.project_paths import PROJECT_ROOT

logger = logging.getLogger(__name__)

_TAC_RE = re.compile(r"^\d{8}$")


def run_from_config(config_path: str | Path) -> dict[str, Any]:
    cfg_path = Path(config_path)
    if not cfg_path.exists():
        raise FileNotFoundError(f"Config file not found: {cfg_path}")

    perf: dict[str, Any] = {}
    started = time.perf_counter()
    with cfg_path.open("r", encoding="utf-8") as file:
        config = json.load(file)

    csv_path = PROJECT_ROOT / config["csv_path"]
    output_path = PROJECT_ROOT / config["readiness"]["s3_layout"]
    compression = config["readiness"].get("parquet_compression", "snappy")
    source_mapping = config["source_mapping_columns"]
    fields = config["fields"]
    m2m_types = {str(v).strip() for v in config.get("m2m_equipment_types", [])}
    source_csv = config.get("source_csv", {})

    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    csv_kwargs: dict[str, Any] = {
        "sep": source_csv.get("sep", ";"),
        "encoding": source_csv.get("encoding", "utf-8-sig"),
    }

    logger.info("Reading TAC source CSV: %s", csv_path)
    with timed_stage("read_csv_sec", perf):
        raw = pd.read_csv(csv_path, **csv_kwargs)
        data = _prepare_dataset(raw, source_mapping, fields, m2m_types)

    with timed_stage("write_parquet_sec", perf):
        output_path.parent.mkdir(parents=True, exist_ok=True)
        data.to_parquet(output_path, compression=compression, index=False)

    m2m_count = int(data["is_m2m"].sum()) if "is_m2m" in data.columns else 0
    logger.info(
        "stg_tac parquet created: path=%s rows=%s m2m_rows=%s compression=%s",
        output_path,
        len(data),
        m2m_count,
        compression,
    )
    stats = {
        "source_csv": str(csv_path),
        "output_parquet": str(output_path),
        "row_count": int(len(data)),
        "m2m_row_count": m2m_count,
        "column_count": int(len(data.columns)),
    }
    perf["elapsed_total_sec"] = round(time.perf_counter() - started, 4)
    append_command_metrics(command="build-stg-tac", metrics={**stats, **perf})
    return stats


def _prepare_dataset(
    chunk: pd.DataFrame,
    source_mapping: dict[str, str],
    fields: list[dict[str, Any]],
    m2m_types: set[str],
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
        logical_type = str(field["type"]).lower()
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
