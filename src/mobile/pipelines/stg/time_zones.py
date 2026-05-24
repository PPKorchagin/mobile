from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any

import pandas as pd

from mobile.command_timing import append_command_metrics, timed_stage
from mobile.project_paths import PROJECT_ROOT

logger = logging.getLogger(__name__)


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
    source_csv = config.get("source_csv", {})
    source_mapping = config["source_mapping_columns"]
    fields = config["fields"]
    chunk_size = source_csv.get("chunk_size")

    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    csv_kwargs: dict[str, Any] = {
        "sep": source_csv.get("sep", ","),
        "encoding": source_csv.get("encoding", "utf-8"),
    }
    if chunk_size:
        csv_kwargs["chunksize"] = int(chunk_size)

    logger.info("Reading source CSV from config: %s", csv_path)
    with timed_stage("read_csv_sec", perf):
        source = pd.read_csv(csv_path, **csv_kwargs)
        chunks = source if chunk_size else [source]

        prepared_chunks: list[pd.DataFrame] = []
        for chunk in chunks:
            prepared_chunks.append(_prepare_chunk(chunk, source_mapping, fields))

        data = pd.concat(prepared_chunks, ignore_index=True) if prepared_chunks else pd.DataFrame()

    with timed_stage("write_parquet_sec", perf):
        output_path.parent.mkdir(parents=True, exist_ok=True)
        data.to_parquet(output_path, compression=compression, index=False)

    logger.info(
        "stg_time_zones parquet created: path=%s rows=%s columns=%s compression=%s",
        output_path,
        len(data),
        len(data.columns),
        compression,
    )
    stats = {
        "source_csv": str(csv_path),
        "output_parquet": str(output_path),
        "row_count": int(len(data)),
        "column_count": int(len(data.columns)),
    }
    perf["elapsed_total_sec"] = round(time.perf_counter() - started, 4)
    append_command_metrics(command="build-stg-time-zones", metrics={**stats, **perf})
    return stats


def _prepare_chunk(
    chunk: pd.DataFrame,
    source_mapping: dict[str, str],
    fields: list[dict[str, Any]],
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
        logical_type = str(field["type"]).lower()
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
