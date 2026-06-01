from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

import pandas as pd

from mobile.cli_defaults import DEFAULT_PARQUET_COMPRESSION
from mobile.command_timing import append_command_metrics, timed_stage
from mobile.pipelines.common.dim_csv import prepare_chunk_from_mapping, resolve_csv_input_path

logger = logging.getLogger(__name__)

DIM_TIME_ZONES_TABLE = "dim_time_zones"

CSV_SEP = ";"
CSV_ENCODING = "utf-8"
CSV_CHUNK_SIZE = 200_000

SOURCE_MAPPING_COLUMNS: dict[str, str] = {
    "code": "code",
    "name": "name",
    "timezone": "timezone",
    "geometry": "geometry",
}

DIM_TIME_ZONES_FIELDS: list[dict[str, str]] = [
    {"name": "code", "type": "int32"},
    {"name": "name", "type": "string"},
    {"name": "timezone", "type": "int32"},
    {"name": "geometry", "type": "string"},
]


def run(
    *,
    csv_path: str | Path,
    output_path: str | Path,
) -> dict[str, Any]:
    compression = DEFAULT_PARQUET_COMPRESSION
    csv_file = resolve_csv_input_path(csv_path)
    parquet_file = resolve_csv_input_path(output_path)

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
            prepared_chunks.append(prepare_chunk_from_mapping(chunk, SOURCE_MAPPING_COLUMNS, DIM_TIME_ZONES_FIELDS))
        data = pd.concat(prepared_chunks, ignore_index=True) if prepared_chunks else pd.DataFrame()

    with timed_stage("write_parquet_sec", perf):
        parquet_file.parent.mkdir(parents=True, exist_ok=True)
        data.to_parquet(parquet_file, compression=compression, index=False)

    logger.info(
        "%s parquet created: path=%s rows=%s columns=%s compression=%s",
        DIM_TIME_ZONES_TABLE,
        parquet_file,
        len(data),
        len(data.columns),
        compression,
    )
    stats = {
        "table": DIM_TIME_ZONES_TABLE,
        "source_csv": str(csv_file),
        "output_parquet": str(parquet_file),
        "row_count": int(len(data)),
        "column_count": int(len(data.columns)),
        "parquet_compression": compression,
    }
    perf["elapsed_total_sec"] = round(time.perf_counter() - started, 4)
    append_command_metrics(command="build-dim-time-zones", metrics={**stats, **perf})
    return stats
