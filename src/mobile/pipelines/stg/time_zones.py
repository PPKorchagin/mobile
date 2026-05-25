from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

import pandas as pd

from mobile.command_timing import append_command_metrics, timed_stage
from mobile.project_paths import PROJECT_ROOT

logger = logging.getLogger(__name__)

STG_TIME_ZONES_TABLE = "stg_time_zones"

CSV_SEP = ";"
CSV_ENCODING = "utf-8"
CSV_CHUNK_SIZE = 200_000

SOURCE_MAPPING_COLUMNS: dict[str, str] = {
    "code": "code",
    "name": "name",
    "timezone": "timezone",
    "geometry": "geometry",
}

STG_TIME_ZONES_FIELDS: list[dict[str, str]] = [
    {"name": "code", "type": "int32"},
    {"name": "name", "type": "string"},
    {"name": "timezone", "type": "int32"},
    {"name": "geometry", "type": "string"},
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
        "chunksize": CSV_CHUNK_SIZE,
    }

    logger.info("Reading source CSV: %s", csv_file)
    with timed_stage("read_csv_sec", perf):
        prepared_chunks: list[pd.DataFrame] = []
        for chunk in pd.read_csv(csv_file, **csv_kwargs):
            prepared_chunks.append(_prepare_chunk(chunk, SOURCE_MAPPING_COLUMNS, STG_TIME_ZONES_FIELDS))
        data = pd.concat(prepared_chunks, ignore_index=True) if prepared_chunks else pd.DataFrame()

    with timed_stage("write_parquet_sec", perf):
        parquet_file.parent.mkdir(parents=True, exist_ok=True)
        data.to_parquet(parquet_file, compression=compression, index=False)

    logger.info(
        "%s parquet created: path=%s rows=%s columns=%s compression=%s",
        STG_TIME_ZONES_TABLE,
        parquet_file,
        len(data),
        len(data.columns),
        compression,
    )
    stats = {
        "table": STG_TIME_ZONES_TABLE,
        "source_csv": str(csv_file),
        "output_parquet": str(parquet_file),
        "row_count": int(len(data)),
        "column_count": int(len(data.columns)),
        "parquet_compression": compression,
    }
    perf["elapsed_total_sec"] = round(time.perf_counter() - started, 4)
    append_command_metrics(command="build-stg-time-zones", metrics={**stats, **perf})
    return stats


def _resolve_path(path: str | Path) -> Path:
    candidate = Path(path)
    return candidate if candidate.is_absolute() else PROJECT_ROOT / candidate


def _prepare_chunk(
    chunk: pd.DataFrame,
    source_mapping: dict[str, str],
    fields: list[dict[str, str]],
) -> pd.DataFrame:
    missing_sources = [src for src in source_mapping.values() if src not in chunk.columns]
    if missing_sources:
        raise ValueError(f"Missing source CSV columns: {missing_sources}")

    rename_map = {src: dst for dst, src in source_mapping.items()}
    renamed = chunk.rename(columns=rename_map)

    target_columns = [field["name"] for field in fields]
    missing_targets = [col for col in target_columns if col not in renamed.columns]
    if missing_targets:
        raise ValueError(f"Mapped columns missing in dataset: {missing_targets}")

    selected = renamed[target_columns].copy()
    for field in fields:
        col = field["name"]
        logical_type = field["type"].lower()
        if logical_type == "string":
            selected[col] = selected[col].astype("string")
        elif logical_type == "int32":
            selected[col] = pd.to_numeric(selected[col], errors="coerce").astype("Int32")
        elif logical_type == "int64":
            selected[col] = pd.to_numeric(selected[col], errors="coerce").astype("Int64")
        elif logical_type == "float64":
            selected[col] = pd.to_numeric(selected[col], errors="coerce")
        elif logical_type == "bool":
            selected[col] = selected[col].astype("boolean")
        else:
            raise ValueError(f"Unsupported type '{field['type']}' for field '{col}'")
    return selected
