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

DIM_OKTMO_TABLE = "dim_oktmo"

CSV_SEP = ";"
CSV_ENCODING = "utf-8"
CSV_CHUNK_SIZE = 200_000

SOURCE_MAPPING_COLUMNS: dict[str, str] = {
    "level": "level",
    "parent_code": "parent_code",
    "code": "code",
    "name": "name",
    "WKT": "WKT",
}

DIM_OKTMO_FIELDS: list[dict[str, str]] = [
    {"name": "WKT", "type": "string"},
    {"name": "level", "type": "int32"},
    {"name": "parent_code", "type": "string"},
    {"name": "code", "type": "string"},
    {"name": "name", "type": "string"},
]


def run(
    *,
    csv_path: str | Path,
    output_path: str | Path,
) -> dict[str, Any]:
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
            prepared_chunks.append(prepare_chunk_from_mapping(chunk, SOURCE_MAPPING_COLUMNS, DIM_OKTMO_FIELDS))
        data = pd.concat(prepared_chunks, ignore_index=True) if prepared_chunks else pd.DataFrame()

    with timed_stage("write_parquet_sec", perf):
        parquet_file.parent.mkdir(parents=True, exist_ok=True)
        data.to_parquet(parquet_file, compression=DEFAULT_PARQUET_COMPRESSION, index=False)

    logger.info(
        "%s parquet created: path=%s rows=%s columns=%s compression=%s",
        DIM_OKTMO_TABLE,
        parquet_file,
        len(data),
        len(data.columns),
        DEFAULT_PARQUET_COMPRESSION,
    )
    stats = {
        "table": DIM_OKTMO_TABLE,
        "source_csv": str(csv_file),
        "output_parquet": str(parquet_file),
        "row_count": int(len(data)),
        "column_count": int(len(data.columns)),
        "parquet_compression": DEFAULT_PARQUET_COMPRESSION,
    }
    perf["elapsed_total_sec"] = round(time.perf_counter() - started, 4)
    append_command_metrics(command="build-dim-oktmo", metrics={**stats, **perf})
    return stats
