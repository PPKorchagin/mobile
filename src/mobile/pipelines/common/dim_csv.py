"""Shared CSV chunk preparation for dim ETL pipelines."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from mobile.project_paths import resolve_project_path


def resolve_csv_input_path(path: str | Path) -> Path:
    """Resolve CSV path relative to project root when not absolute."""
    return resolve_project_path(path)


def prepare_chunk_from_mapping(
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
