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

STG_OKSM_TABLE = "stg_oksm"

_NUMERIC_CODE_RE = re.compile(r"^\d{3}$")
_ALPHA2_RE = re.compile(r"^[A-Z]{2}$")
_ALPHA3_RE = re.compile(r"^[A-Z]{3}$")

CSV_SEP = ";"
CSV_ENCODING = "utf-8-sig"

# Кириллические омоглифы в источнике (напр. «АХ» → AX для Åland).
_CYRILLIC_ISO_LETTERS = str.maketrans(
    {
        "А": "A",
        "В": "B",
        "С": "C",
        "Е": "E",
        "Н": "H",
        "К": "K",
        "М": "M",
        "О": "O",
        "Р": "P",
        "Т": "T",
        "У": "U",
        "Х": "X",
    }
)

SOURCE_MAPPING_COLUMNS: dict[str, str] = {
    "numeric_code": "Цифровой код",
    "name_short": "Наименование краткое",
    "name_full": "Наименование полное",
    "alpha2": "Код альфа-2",
    "alpha3": "Код альфа-3",
    "autokey": "autokey",
}

STG_OKSM_FIELDS: list[dict[str, str]] = [
    {"name": "numeric_code", "type": "string"},
    {"name": "name_short", "type": "string"},
    {"name": "name_full", "type": "string"},
    {"name": "alpha2", "type": "string"},
    {"name": "alpha3", "type": "string"},
    {"name": "autokey", "type": "string"},
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
        "keep_default_na": False,
        "na_values": [""],
    }

    logger.info("Reading source CSV: %s", csv_file)
    with timed_stage("read_csv_sec", perf):
        raw = pd.read_csv(csv_file, **csv_kwargs)
        data = _prepare_dataset(raw, SOURCE_MAPPING_COLUMNS, STG_OKSM_FIELDS)

    with timed_stage("write_parquet_sec", perf):
        parquet_file.parent.mkdir(parents=True, exist_ok=True)
        data.to_parquet(parquet_file, compression=compression, index=False)

    logger.info(
        "%s parquet created: path=%s rows=%s columns=%s compression=%s",
        STG_OKSM_TABLE,
        parquet_file,
        len(data),
        len(data.columns),
        compression,
    )
    stats = {
        "table": STG_OKSM_TABLE,
        "source_csv": str(csv_file),
        "output_parquet": str(parquet_file),
        "row_count": int(len(data)),
        "column_count": int(len(data.columns)),
        "parquet_compression": compression,
    }
    perf["elapsed_total_sec"] = round(time.perf_counter() - started, 4)
    append_command_metrics(command="build-stg-oksm", metrics={**stats, **perf})
    return stats


def _resolve_path(path: str | Path) -> Path:
    candidate = Path(path)
    return candidate if candidate.is_absolute() else PROJECT_ROOT / candidate


def _normalize_iso_code(series: pd.Series, *, width: int) -> pd.Series:
    pattern = _ALPHA2_RE if width == 2 else _ALPHA3_RE
    raw = series.astype("string").str.strip().str.translate(_CYRILLIC_ISO_LETTERS).str.upper()
    invalid = raw.notna() & (raw != "") & ~raw.str.fullmatch(pattern.pattern)
    if int(invalid.sum()) > 0:
        bad = raw.loc[invalid].head(5).tolist()
        raise ValueError(f"Invalid alpha{2 if width == 2 else 3} values (expected {width} Latin letters): {bad}")
    return raw


def _prepare_dataset(
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

    selected["numeric_code"] = (
        selected["numeric_code"]
        .astype("string")
        .str.strip()
        .str.replace(r"\D", "", regex=True)
        .str.zfill(3)
        .str[-3:]
    )
    invalid_numeric = ~selected["numeric_code"].str.fullmatch(_NUMERIC_CODE_RE.pattern)
    if int(invalid_numeric.sum()) > 0:
        bad = selected.loc[invalid_numeric, "numeric_code"].head(5).tolist()
        raise ValueError(f"Invalid numeric_code values (expected 3 digits): {bad}")

    for col in ("name_short", "name_full", "autokey"):
        selected[col] = selected[col].astype("string").str.strip()

    selected["alpha2"] = _normalize_iso_code(selected["alpha2"], width=2)
    selected["alpha3"] = _normalize_iso_code(selected["alpha3"], width=3)

    empty_names = selected["name_short"].isna() | (selected["name_short"] == "") | selected["name_full"].isna() | (
        selected["name_full"] == ""
    )
    if int(empty_names.sum()) > 0:
        bad = selected.loc[empty_names, "numeric_code"].head(5).tolist()
        raise ValueError(f"Empty name_short or name_full for numeric_code: {bad}")

    ordered = [field["name"] for field in fields]
    out = selected[ordered].copy()
    for field in fields:
        col = field["name"]
        logical_type = field["type"].lower()
        if logical_type == "string":
            out[col] = out[col].astype("string")
        else:
            raise ValueError(f"Unsupported type '{field['type']}' for field '{col}'")

    duplicate_numeric = int(out["numeric_code"].duplicated(keep=False).sum())
    if duplicate_numeric > 0:
        dupes = out.loc[out["numeric_code"].duplicated(keep=False), "numeric_code"].unique()[:5].tolist()
        raise ValueError(f"Duplicate numeric_code in source after normalize: {dupes}")

    duplicate_autokey = int(out["autokey"].duplicated(keep=False).sum())
    if duplicate_autokey > 0:
        dupes = out.loc[out["autokey"].duplicated(keep=False), "autokey"].unique()[:5].tolist()
        raise ValueError(f"Duplicate autokey in source after normalize: {dupes}")

    return out
