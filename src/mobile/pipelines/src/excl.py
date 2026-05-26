from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from mobile.command_timing import append_command_metrics
from mobile.project_paths import (
    DEFAULT_SRC_EXCL_IMEI_OUTPUT,
    DEFAULT_SRC_EXCL_IMSI_OUTPUT,
    DEFAULT_SRC_EXCL_MSISDN_OUTPUT,
    PROJECT_ROOT,
    SRC_PERSON_LAYOUT_TEMPLATE,
    SRC_PERSON_SUCCESS_FLAG,
)


logger = logging.getLogger(__name__)

SRC_EXCL_IMSI_TABLE = "src_imsi"
SRC_EXCL_IMEI_TABLE = "src_imei"
SRC_EXCL_MSISDN_TABLE = "src_msisdn"

SRC_IMSI_FIELDS: list[dict[str, str]] = [{"name": "value", "type": "string"}]
SRC_IMEI_FIELDS: list[dict[str, str]] = [{"name": "value", "type": "string"}]
SRC_MSISDN_FIELDS: list[dict[str, str]] = [{"name": "value", "type": "string"}]


@dataclass(frozen=True)
class BuildSrcExclParams:
    """Доля АБ (строк последнего full snapshot person), попадающая в списки исключений."""

    pct_of_ab: float
    seed: int

    def sample_size_for_ab(self, ab_row_count: int, eligible_triple_count: int) -> int:
        if eligible_triple_count <= 0:
            return 0
        target = max(1, int(round(ab_row_count * self.pct_of_ab / 100.0)))
        return min(target, int(eligible_triple_count))


def run(
    *,
    person_layout_template: str = SRC_PERSON_LAYOUT_TEMPLATE,
    person_success_flag: str = SRC_PERSON_SUCCESS_FLAG,
    imsi_output_path: str | Path = DEFAULT_SRC_EXCL_IMSI_OUTPUT,
    imei_output_path: str | Path = DEFAULT_SRC_EXCL_IMEI_OUTPUT,
    msisdn_output_path: str | Path = DEFAULT_SRC_EXCL_MSISDN_OUTPUT,
    compression: str,
    params: BuildSrcExclParams,
) -> dict[str, Any]:
    started = time.perf_counter()

    src_day_dir = _resolve_latest_success_day_dir(person_layout_template, person_success_flag)
    if src_day_dir is None:
        raise FileNotFoundError("No src_person day directory with _SUCCESS found")

    src_parquet = src_day_dir / "person.parquet"
    if not src_parquet.exists():
        raise FileNotFoundError(f"src_person parquet not found: {src_parquet}")

    source = pd.read_parquet(src_parquet)
    eligible = _eligible_exclusion_triples(source)
    ab_rows = int(len(source))
    eligible_count = int(len(eligible))
    sample_size = params.sample_size_for_ab(ab_rows, eligible_count)
    if sample_size <= 0:
        raise ValueError("No eligible rows with non-null isdn/imsi/imei in src_person")

    triples = _sample_triples(eligible, sample_size, params.seed)
    stats: dict[str, Any] = {
        "source_parquet": str(src_parquet),
        "source_day": src_day_dir.name,
        "ab_row_count": ab_rows,
        "eligible_triple_count": eligible_count,
        "pct_of_ab": float(params.pct_of_ab),
        "sample_size": int(len(triples)),
        "seed": int(params.seed),
    }
    stats["src_imsi"] = _write_single_column(
        triples[["imsi"]].rename(columns={"imsi": "value"}),
        SRC_IMSI_FIELDS,
        imsi_output_path,
        compression,
    )
    stats["src_imei"] = _write_single_column(
        triples[["imei"]].rename(columns={"imei": "value"}),
        SRC_IMEI_FIELDS,
        imei_output_path,
        compression,
    )
    stats["src_msisdn"] = _write_single_column(
        triples[["msisdn"]].rename(columns={"msisdn": "value"}),
        SRC_MSISDN_FIELDS,
        msisdn_output_path,
        compression,
    )
    stats["elapsed_total_sec"] = round(time.perf_counter() - started, 4)
    append_command_metrics(command="build-src-excl", metrics=stats)
    logger.info("build-src-excl completed: %s", stats)
    return stats


def _eligible_exclusion_triples(source: pd.DataFrame) -> pd.DataFrame:
    work = source.copy()
    work["imsi"] = _norm_numeric_str(work.get("imsi"))
    work["imei"] = _norm_numeric_str(work.get("imei"))
    work["msisdn"] = _norm_numeric_str(work.get("isdn"))
    work = work.dropna(subset=["imsi", "imei", "msisdn"]).copy()
    if work.empty:
        return pd.DataFrame(columns=["imsi", "imei", "msisdn"])
    return work.drop_duplicates(subset=["imsi", "imei", "msisdn"]).reset_index(drop=True)[
        ["imsi", "imei", "msisdn"]
    ]


def _sample_triples(eligible: pd.DataFrame, sample_size: int, seed: int) -> pd.DataFrame:
    if eligible.empty:
        return pd.DataFrame(columns=["imsi", "imei", "msisdn"])
    n = min(int(sample_size), len(eligible))
    rng = random.Random(seed)
    idx = rng.sample(list(eligible.index), k=n)
    return eligible.loc[idx].reset_index(drop=True)


def _write_single_column(
    data: pd.DataFrame,
    fields: list[dict[str, Any]],
    output_path: str | Path,
    compression: str,
) -> dict[str, Any]:
    target_col = fields[0]["name"] if fields else "value"
    frame = data.rename(columns={"value": target_col}).copy()
    frame = _coerce_types(frame, fields)

    out = Path(output_path)
    if not out.is_absolute():
        out = PROJECT_ROOT / out
    out.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(out, compression=compression, index=False)
    return {"output_path": str(out), "row_count": int(len(frame))}


def _resolve_latest_success_day_dir(layout_template: str, success_flag: str) -> Path | None:
    root = _resolve_person_layout_root(layout_template)
    day_dirs = sorted(root.glob("load_year=*/load_month=*/load_day=*"))
    success_dirs = [p for p in day_dirs if (p / success_flag).exists()]
    if not success_dirs:
        return None
    return success_dirs[-1]


def _resolve_person_layout_root(layout: str) -> Path:
    path = Path(layout)
    candidate = path if path.is_absolute() else PROJECT_ROOT / path
    parts = candidate.parts
    idx = next((i for i, part in enumerate(parts) if "{" in part and "}" in part), None)
    if idx is None:
        return candidate.parent if candidate.suffix else candidate
    return Path(*parts[:idx])


def _coerce_types(df: pd.DataFrame, fields: list[dict[str, Any]]) -> pd.DataFrame:
    out = df.copy()
    for field in fields:
        name = field["name"]
        if name not in out.columns:
            out[name] = pd.NA
        kind = field["type"]
        if kind == "string":
            out[name] = out[name].astype("string")
        elif kind == "long":
            out[name] = pd.to_numeric(out[name], errors="coerce").astype("Int64")
        elif kind == "int":
            out[name] = pd.to_numeric(out[name], errors="coerce").astype("Int32")
        else:
            raise ValueError(f"Unsupported field type in src excl schema: {kind}")
    names = [f["name"] for f in fields]
    return out[names].copy() if names else out


def _norm_numeric_str(series: pd.Series | None) -> pd.Series:
    if series is None:
        return pd.Series(dtype="string")
    numeric = pd.to_numeric(series, errors="coerce").astype("Int64")
    return numeric.astype("string")
