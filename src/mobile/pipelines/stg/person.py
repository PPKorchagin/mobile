"""Сборка ``stg_person``: месячный профиль физлиц из ``src_person`` (ID + демография)."""

from __future__ import annotations

import hashlib
import json
import logging
import re
import time
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from mobile.cli_defaults import DEFAULT_PARQUET_COMPRESSION
from mobile.command_timing import append_command_metrics, timed_stage
from mobile.pipelines.stg.subscriber_ids import normalize_imei, normalize_imsi, normalize_msisdn
from mobile.project_paths import (
    DEFAULT_STG_PERSON_SCHEMA_PATH,
    DEFAULT_STG_TAC_OUTPUT_PATH,
    SRC_PERSON_LAYOUT_TEMPLATE,
    SRC_PERSON_SUCCESS_FLAG,
    resolve_project_path,
    stg_person_output_path,
)

logger = logging.getLogger(__name__)

STG_PERSON_TABLE = "stg_person"
STG_PERSON_FIELDS: list[dict[str, str]] = [
    {"name": "report_date", "type": "date"},
    {"name": "person_id", "type": "string"},
    {"name": "msisdn", "type": "string"},
    {"name": "imsi", "type": "string"},
    {"name": "imei", "type": "string"},
    {"name": "gender", "type": "string"},
    {"name": "age", "type": "string"},
    {"name": "citizenship", "type": "string"},
    {"name": "operator_id", "type": "long"},
    {"name": "actually_from", "type": "timestamp"},
    {"name": "actually_to", "type": "timestamp"},
]

_OPEN_ACTUALLY_TO = pd.Timestamp("2999-12-31 23:59:59")


def _load_schema_contract(schema_path: Path) -> None:
    global STG_PERSON_TABLE, STG_PERSON_FIELDS
    with schema_path.open(encoding="utf-8") as file:
        cfg = json.load(file)
    STG_PERSON_TABLE = str(cfg.get("table", STG_PERSON_TABLE))
    STG_PERSON_FIELDS = [{"name": str(f["name"]), "type": str(f["type"])} for f in cfg.get("fields", STG_PERSON_FIELDS)]


_load_schema_contract(DEFAULT_STG_PERSON_SCHEMA_PATH)


def _validate_report_month(report_date: date) -> date:
    """``report_date`` — 1-е число отчётного месяца (``YYYY-MM-01``)."""
    if report_date.day != 1:
        raise ValueError(f"build-stg-person: report_date must be YYYY-MM-01, got {report_date.isoformat()}")
    return report_date


def _month_period(report_month: date) -> tuple[date, date]:
    """Календарный период месяца: с 1-го по последний день."""
    month_end = (pd.Timestamp(report_month) + pd.offsets.MonthEnd(0)).date()
    return report_month, month_end


def _month_end_ts(report_month: date) -> pd.Timestamp:
    return pd.Timestamp(report_month) + pd.offsets.MonthEnd(0)


def run_build(
    report_date: date,
    *,
    src_person_path: str | Path | None = None,
    stg_msisdn_imsi_path: str | Path | None = None,
    stg_msisdn_imei_path: str | Path | None = None,
    stg_tac_path: str | Path | None = None,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    command = "build-stg-person"
    perf: dict[str, Any] = {}
    started = time.perf_counter()
    report_month = _validate_report_month(report_date)
    period_start, period_end = _month_period(report_month)
    source_path, src_load_day = _resolve_src_person_source_path(
        report_month=report_month,
        period_start=period_start,
        period_end=period_end,
        src_person_path=src_person_path,
    )
    out = resolve_project_path(output_path) if output_path is not None else stg_person_output_path(report_month)
    field_names = [f["name"] for f in STG_PERSON_FIELDS]

    with timed_stage("read_src_person_sec", perf):
        raw = _read_src_person(source_path=source_path)
    src_rows_before_exclusions = int(len(raw))

    tac_path = resolve_project_path(stg_tac_path) if stg_tac_path is not None else DEFAULT_STG_TAC_OUTPUT_PATH
    with timed_stage("exclusions_sec", perf):
        m2m_tacs = _read_m2m_tac_set(tac_path)
        raw, excluded_m2m_tac_rows = _exclude_m2m_by_tac(raw, m2m_tacs=m2m_tacs)

    imsi_binding_path = _resolve_binding_source_path(
        report_month=report_month,
        source_path=stg_msisdn_imsi_path,
        kind="imsi",
    )
    imei_binding_path = _resolve_binding_source_path(
        report_month=report_month,
        source_path=stg_msisdn_imei_path,
        kind="imei",
    )
    with timed_stage("load_bindings_sec", perf):
        imsi_binding = _read_binding(imsi_binding_path, kind="imsi")
        imei_binding = _read_binding(imei_binding_path, kind="imei")

    with timed_stage("transform_sec", perf):
        prepared, binding_fill = _prepare_month_slice(
            raw=raw,
            report_month=report_month,
            imsi_binding=imsi_binding,
            imei_binding=imei_binding,
        )

    with timed_stage("write_sec", perf):
        result = _coerce_output(prepared, field_names, report_month=report_month)
        out.parent.mkdir(parents=True, exist_ok=True)
        result.to_parquet(out, compression=DEFAULT_PARQUET_COMPRESSION, index=False)

    stats: dict[str, Any] = {
        "command": command,
        "table": STG_PERSON_TABLE,
        "report_date": report_month.isoformat(),
        "report_month": report_month.isoformat(),
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "src_load_day": src_load_day.isoformat() if src_load_day else None,
        "src_person_path": str(source_path),
        "output_path": str(out),
        "src_rows_read": src_rows_before_exclusions,
        "src_rows_after_m2m_exclusion": int(len(raw)),
        "excluded_m2m_tac_rows": int(excluded_m2m_tac_rows),
        "stg_tac_path": str(tac_path),
        "distinct_m2m_tac": len(m2m_tacs),
        "stg_rows_written": int(len(result)),
        "distinct_msisdn": int(result["msisdn"].nunique()) if not result.empty else 0,
        "distinct_person_id": int(result["person_id"].nunique()) if not result.empty else 0,
        "stg_msisdn_imsi_path": str(imsi_binding_path),
        "stg_msisdn_imei_path": str(imei_binding_path),
        "stg_msisdn_imsi_rows": int(len(imsi_binding)),
        "stg_msisdn_imei_rows": int(len(imei_binding)),
        "binding_imsi_filled": int(binding_fill.get("imsi", 0)),
        "binding_imei_filled": int(binding_fill.get("imei", 0)),
        "binding_msisdn_filled": int(binding_fill.get("msisdn", 0)),
    }
    perf["elapsed_total_sec"] = round(time.perf_counter() - started, 4)
    append_command_metrics(command=command, metrics={**stats, **perf})
    logger.info("%s completed: %s", command, stats)
    return {**stats, **perf}


def _resolve_src_person_source_path(
    *,
    report_month: date,
    period_start: date,
    period_end: date,
    src_person_path: str | Path | None,
) -> tuple[Path, date | None]:
    """
    Среди ``load_day`` каталогов периода ``[period_start, period_end]`` с ``_SUCCESS``
    выбрать ``person.parquet`` с максимальной датой.
    """
    if src_person_path is not None:
        resolved = resolve_project_path(src_person_path)
        if resolved.is_file():
            return resolved, None

    root = (
        _resolve_person_layout_root(SRC_PERSON_LAYOUT_TEMPLATE)
        if src_person_path is None
        else resolve_project_path(src_person_path)
    )
    return _latest_success_person_parquet_in_period(
        root=root,
        report_month=report_month,
        period_start=period_start,
        period_end=period_end,
    )


def _resolve_person_layout_root(layout: str) -> Path:
    path = resolve_project_path(layout)
    parts = path.parts
    idx = next((i for i, part in enumerate(parts) if "{" in part and "}" in part), None)
    if idx is None:
        return path.parent if path.suffix else path
    return Path(*parts[:idx])


def _parse_load_day(day_dir: Path, *, year: int, month: int) -> date | None:
    prefix = "load_day="
    if not day_dir.name.startswith(prefix):
        return None
    day_part = day_dir.name[len(prefix) :]
    try:
        day_num = int(day_part)
    except ValueError:
        return None
    if day_num < 1 or day_num > 31:
        return None
    try:
        return date(year, month, day_num)
    except ValueError:
        return None


def _latest_success_person_parquet_in_period(
    *,
    root: Path,
    report_month: date,
    period_start: date,
    period_end: date,
) -> tuple[Path, date]:
    year, month = report_month.year, report_month.month
    month_dir = root / f"load_year={year:04d}" / f"load_month={month:02d}"
    if not month_dir.exists():
        raise FileNotFoundError(f"No src_person month directory for {year:04d}-{month:02d} under {root}")

    candidates: list[tuple[date, Path]] = []
    for day_dir in sorted(month_dir.glob("load_day=*")):
        load_day = _parse_load_day(day_dir, year=year, month=month)
        if load_day is None or load_day < period_start or load_day > period_end:
            continue
        if not (day_dir / SRC_PERSON_SUCCESS_FLAG).exists():
            continue
        parquet_path = day_dir / "person.parquet"
        if not parquet_path.exists():
            continue
        candidates.append((load_day, parquet_path))

    if not candidates:
        raise FileNotFoundError(
            f"No src_person snapshot with {SRC_PERSON_SUCCESS_FLAG!r} and person.parquet "
            f"for period {period_start.isoformat()}..{period_end.isoformat()} under {month_dir}"
        )

    chosen_day, chosen_path = max(candidates, key=lambda item: item[0])
    logger.info(
        "build-stg-person: selected src_person load_day=%s (%s) from %s candidates in %04d-%02d",
        chosen_day.isoformat(),
        chosen_path.parent.name,
        len(candidates),
        year,
        month,
    )
    return chosen_path, chosen_day


def _read_src_person(*, source_path: Path) -> pd.DataFrame:
    columns = [
        "operator_Id",
        "isdn",
        "imsi",
        "imei",
        "client_type",
        "actually_from",
        "actually_to",
        "birth_day",
        "first_name",
        "second_name",
        "last_name",
        "dul_department",
        "document",
    ]
    if not source_path.exists():
        logger.warning("build-stg-person: src_person not found at %s", source_path)
        return pd.DataFrame(columns=columns)
    try:
        return pd.read_parquet(source_path, columns=columns)
    except Exception:
        logger.exception("build-stg-person: failed to read src_person at %s", source_path)
        return pd.DataFrame(columns=columns)


def _resolve_binding_source_path(
    *,
    report_month: date,
    source_path: str | Path | None,
    kind: str,
) -> Path:
    from mobile.project_paths import stg_msisdn_imei_output_path, stg_msisdn_imsi_output_path

    if source_path is None:
        if kind == "imsi":
            return stg_msisdn_imsi_output_path(report_month)
        return stg_msisdn_imei_output_path(report_month)
    resolved = resolve_project_path(source_path)
    if resolved.is_dir():
        return resolved / f"{report_month.isoformat()}.parquet"
    return resolved


def _read_binding(path: Path, *, kind: str) -> pd.DataFrame:
    value_col = "imsi" if kind == "imsi" else "imei"
    columns = ["msisdn", value_col, "valid_from", "valid_to"]
    if not path.exists():
        logger.warning("build-stg-person: stg_msisdn_%s binding not found at %s", kind, path)
        return pd.DataFrame(columns=columns)
    try:
        binding = pd.read_parquet(path, columns=columns)
    except Exception:
        logger.exception("build-stg-person: failed to read stg_msisdn_%s binding at %s", kind, path)
        return pd.DataFrame(columns=columns)

    out = binding.copy()
    out["msisdn"] = normalize_msisdn(_to_digit_string_series(out.get("msisdn")))
    if kind == "imsi":
        out["imsi"] = normalize_imsi(_to_digit_string_series(out.get("imsi")))
    else:
        out["imei"] = normalize_imei(_to_digit_string_series(out.get("imei")))
    out["valid_from"] = pd.to_datetime(out.get("valid_from"), errors="coerce")
    out["valid_to"] = pd.to_datetime(out.get("valid_to"), errors="coerce")
    out["valid_to"] = out["valid_to"].fillna(_OPEN_ACTUALLY_TO)
    return out.dropna(subset=["msisdn", value_col, "valid_from", "valid_to"]).reset_index(drop=True)


def _read_m2m_tac_set(tac_path: Path) -> set[str]:
    if not tac_path.exists():
        logger.warning("build-stg-person: stg_tac not found, skipping M2M TAC exclusion: %s", tac_path)
        return set()

    tac_df = pd.read_parquet(tac_path, columns=["tac", "is_m2m"])
    if "tac" not in tac_df.columns or "is_m2m" not in tac_df.columns:
        logger.warning("build-stg-person: stg_tac missing tac/is_m2m columns, skip M2M filter")
        return set()

    m2m = tac_df[tac_df["is_m2m"].fillna(False).astype(bool)]
    return set(m2m["tac"].astype("string").str.strip().dropna())


def _extract_tac_from_imei(imei: pd.Series | None) -> pd.Series:
    """TAC (Type Allocation Code) = first 8 digits of IMEI (GSMA)."""
    digits = _to_digit_string_series(imei).astype("string")
    cleaned = digits.str.replace(r"\D", "", regex=True)
    return cleaned.where(cleaned.str.len() >= 8, pd.NA).str[:8]


def _exclude_m2m_by_tac(raw: pd.DataFrame, *, m2m_tacs: set[str]) -> tuple[pd.DataFrame, int]:
    if raw.empty or not m2m_tacs:
        return raw, 0

    imei_tac = _extract_tac_from_imei(raw.get("imei"))
    m2m_mask = imei_tac.isin(m2m_tacs) & imei_tac.notna()
    excluded_rows = int(m2m_mask.sum())
    if excluded_rows > 0:
        logger.info(
            "build-stg-person: excluded M2M by TAC: excluded_rows=%s (distinct_m2m_tac=%s)",
            excluded_rows,
            len(m2m_tacs),
        )
    return raw.loc[~m2m_mask].copy(), excluded_rows


def _prepare_month_slice(
    *,
    raw: pd.DataFrame,
    report_month: date,
    imsi_binding: pd.DataFrame,
    imei_binding: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, int]]:
    if raw.empty:
        return (
            pd.DataFrame(columns=["msisdn", "imsi", "imei", "operator_id", "actually_from", "actually_to"]),
            {"imsi": 0, "imei": 0, "msisdn": 0},
        )

    work = raw.copy()
    client_type = pd.to_numeric(work.get("client_type"), errors="coerce")
    work = work.loc[client_type == 0].copy()

    work["actually_from"] = pd.to_datetime(work.get("actually_from"), errors="coerce")
    work["actually_to"] = pd.to_datetime(work.get("actually_to"), errors="coerce").fillna(_OPEN_ACTUALLY_TO)
    month_start = pd.Timestamp(report_month)
    month_end = _month_end_ts(report_month)
    overlaps_month = (
        work["actually_from"].notna() & (work["actually_from"] <= month_end) & (work["actually_to"] >= month_start)
    )
    work = work.loc[overlaps_month].copy()

    work["msisdn"] = normalize_msisdn(_to_digit_string_series(work.get("isdn")))
    work["imsi"] = normalize_imsi(_to_digit_string_series(work.get("imsi")))
    work["imei"] = normalize_imei(_to_digit_string_series(work.get("imei")))
    binding_at = month_end.normalize() + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    work, fill_stats = _enrich_identifiers_from_bindings(
        work=work,
        at=binding_at,
        imsi_binding=imsi_binding,
        imei_binding=imei_binding,
    )
    work["operator_id"] = pd.to_numeric(work.get("operator_Id"), errors="coerce").astype("Int64")
    work = work.dropna(subset=["msisdn", "imsi", "imei", "operator_id", "actually_from", "actually_to"])

    work["birth_day"] = pd.to_datetime(work.get("birth_day"), errors="coerce")
    work["first_name"] = _norm_str(work.get("first_name"))
    work["second_name"] = _norm_str(work.get("second_name"))
    work["last_name"] = _norm_str(work.get("last_name"))
    work["dul_department"] = _norm_str(work.get("dul_department"))
    work["document"] = _norm_str(work.get("document"))
    work = _assign_person_cluster_keys(
        work,
        imsi_binding=imsi_binding,
        imei_binding=imei_binding,
        month_start=month_start,
        month_end=month_end,
    )
    work = work[work["person_cluster_key"].notna()].copy()
    if work.empty:
        return pd.DataFrame(), fill_stats

    work = work.sort_values(["person_cluster_key", "actually_from"], ascending=[True, False], na_position="last")
    latest = work.drop_duplicates(subset=["person_cluster_key"], keep="first").copy()
    month_start = pd.Timestamp(report_month)
    latest["person_id"] = latest["person_cluster_key"].map(_build_person_id_from_key)
    latest["gender"] = latest.apply(_derive_gender, axis=1)
    latest["age"] = latest["birth_day"].map(lambda value: _derive_age_as_of_month_start(value, month_start))
    latest["citizenship"] = latest.apply(_derive_citizenship_from_row, axis=1).astype("string")

    return latest.reset_index(drop=True), fill_stats


def _enrich_identifiers_from_bindings(
    *,
    work: pd.DataFrame,
    at: pd.Timestamp,
    imsi_binding: pd.DataFrame,
    imei_binding: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, int]]:
    if work.empty:
        return work, {"imsi": 0, "imei": 0, "msisdn": 0}

    out = work.copy()
    filled = {"imsi": 0, "imei": 0, "msisdn": 0}

    if not imsi_binding.empty:
        out, n = _fill_column_from_binding(
            out, target_col="imsi", lookup_col="msisdn", binding=imsi_binding, value_col="imsi", at=at
        )
        filled["imsi"] += n
        out, n = _fill_column_from_binding(
            out, target_col="msisdn", lookup_col="imsi", binding=imsi_binding, value_col="msisdn", at=at
        )
        filled["msisdn"] += n

    if not imei_binding.empty:
        out, n = _fill_column_from_binding(
            out, target_col="imei", lookup_col="msisdn", binding=imei_binding, value_col="imei", at=at
        )
        filled["imei"] += n
        out, n = _fill_column_from_binding(
            out, target_col="msisdn", lookup_col="imei", binding=imei_binding, value_col="msisdn", at=at
        )
        filled["msisdn"] += n

    if not imsi_binding.empty and not imei_binding.empty:
        out, n = _fill_column_from_binding(
            out, target_col="imei", lookup_col="msisdn", binding=imei_binding, value_col="imei", at=at
        )
        filled["imei"] += n
        out, n = _fill_column_from_binding(
            out, target_col="imsi", lookup_col="msisdn", binding=imsi_binding, value_col="imsi", at=at
        )
        filled["imsi"] += n

    return out, filled


def _fill_column_from_binding(
    work: pd.DataFrame,
    *,
    target_col: str,
    lookup_col: str,
    binding: pd.DataFrame,
    value_col: str,
    at: pd.Timestamp,
) -> tuple[pd.DataFrame, int]:
    if target_col not in work.columns or lookup_col not in work.columns:
        return work, 0
    if binding.empty or lookup_col not in binding.columns or value_col not in binding.columns:
        return work, 0

    out = work.copy()
    missing = out[target_col].isna() & out[lookup_col].notna()
    if not bool(missing.any()):
        return out, 0

    need = out.loc[missing, [lookup_col]].copy()
    need["_row_id"] = need.index
    merged = need.merge(binding[[lookup_col, value_col, "valid_from", "valid_to"]], on=lookup_col, how="inner")
    if merged.empty:
        return out, 0
    in_range = (merged["valid_from"] <= at) & (merged["valid_to"] >= at)
    merged = merged.loc[in_range]
    if merged.empty:
        return out, 0

    merged = merged.sort_values(["_row_id", "valid_from"], ascending=[True, False])
    best = merged.drop_duplicates(subset=["_row_id"], keep="first")
    filled = 0
    for row in best.itertuples(index=False):
        rid = getattr(row, "_row_id")
        if pd.isna(out.at[rid, target_col]):
            out.at[rid, target_col] = getattr(row, value_col)
            filled += 1
    return out, int(filled)


class _UnionFind:
    """Disjoint-set: канонический id кластера = лексикографически минимальный узел."""

    def __init__(self) -> None:
        self._parent: dict[str, str] = {}

    def find(self, node: str) -> str:
        if node not in self._parent:
            self._parent[node] = node
        parent = self._parent[node]
        if parent != node:
            self._parent[node] = self.find(parent)
        return self._parent[node]

    def union(self, left: str, right: str) -> None:
        root_left = self.find(left)
        root_right = self.find(right)
        if root_left == root_right:
            return
        if root_left < root_right:
            self._parent[root_right] = root_left
        else:
            self._parent[root_left] = root_right


def _identifier_nodes_from_row(row: pd.Series) -> list[str]:
    nodes: list[str] = []
    for prefix, col in (("imsi", "imsi"), ("imei", "imei"), ("msisdn", "msisdn")):
        node = _node(prefix, row.get(col))
        if node is not None:
            nodes.append(node)
    bio = _build_bio_fingerprint(row)
    if bio is not None:
        nodes.append(bio)
    return nodes


def _build_bio_fingerprint(row: pd.Series) -> str | None:
    """Стабильный демографический якорь: ФИО + дата рождения (+ цифры документа)."""
    last_name = _str_field(row.get("last_name")).casefold()
    first_name = _str_field(row.get("first_name")).casefold()
    second_name = _str_field(row.get("second_name")).casefold()
    birth_day = pd.to_datetime(row.get("birth_day"), errors="coerce")
    if not last_name or not first_name or pd.isna(birth_day):
        return None
    document_digits = re.sub(r"\D", "", _str_field(row.get("document")))
    return f"bio:{last_name}|{first_name}|{second_name}|{birth_day.date().isoformat()}|{document_digits}"


def _unite_nodes(uf: _UnionFind, nodes: list[str]) -> None:
    if not nodes:
        return
    anchor = nodes[0]
    uf.find(anchor)
    for node in nodes[1:]:
        uf.union(anchor, node)


def _binding_edges_in_month(
    binding: pd.DataFrame,
    *,
    left_kind: str,
    right_kind: str,
    month_start: pd.Timestamp,
    month_end: pd.Timestamp,
) -> list[tuple[str, str]]:
    if binding.empty:
        return []
    left_col = left_kind
    right_col = right_kind
    if left_col not in binding.columns or right_col not in binding.columns:
        return []

    frame = binding[[left_col, right_col, "valid_from", "valid_to"]].copy()
    if left_kind == "msisdn":
        frame[left_col] = normalize_msisdn(_to_digit_string_series(frame[left_col]))
    elif left_kind == "imsi":
        frame[left_col] = normalize_imsi(_to_digit_string_series(frame[left_col]))
    elif left_kind == "imei":
        frame[left_col] = normalize_imei(_to_digit_string_series(frame[left_col]))
    if right_kind == "msisdn":
        frame[right_col] = normalize_msisdn(_to_digit_string_series(frame[right_col]))
    elif right_kind == "imsi":
        frame[right_col] = normalize_imsi(_to_digit_string_series(frame[right_col]))
    elif right_kind == "imei":
        frame[right_col] = normalize_imei(_to_digit_string_series(frame[right_col]))

    frame["valid_from"] = pd.to_datetime(frame["valid_from"], errors="coerce")
    frame["valid_to"] = pd.to_datetime(frame["valid_to"], errors="coerce").fillna(_OPEN_ACTUALLY_TO)
    frame = frame.dropna(subset=[left_col, right_col, "valid_from", "valid_to"])
    overlaps = (frame["valid_from"] <= month_end) & (frame["valid_to"] >= month_start)
    frame = frame.loc[overlaps]
    if frame.empty:
        return []

    edges: list[tuple[str, str]] = []
    for left_value, right_value in zip(frame[left_col], frame[right_col], strict=True):
        left_node = _node(left_kind, left_value)
        right_node = _node(right_kind, right_value)
        if left_node is not None and right_node is not None:
            edges.append((left_node, right_node))
    return edges


def _assign_person_cluster_keys(
    work: pd.DataFrame,
    *,
    imsi_binding: pd.DataFrame,
    imei_binding: pd.DataFrame,
    month_start: pd.Timestamp,
    month_end: pd.Timestamp,
) -> pd.DataFrame:
    """Связать msisdn/imsi/imei одной персоны через co-occurrence, bindings и bio."""
    if work.empty:
        out = work.copy()
        out["person_cluster_key"] = pd.Series(dtype="string")
        return out

    uf = _UnionFind()
    for _, row in work.iterrows():
        _unite_nodes(uf, _identifier_nodes_from_row(row))

    for left_node, right_node in _binding_edges_in_month(
        imsi_binding,
        left_kind="msisdn",
        right_kind="imsi",
        month_start=month_start,
        month_end=month_end,
    ):
        uf.union(left_node, right_node)
    for left_node, right_node in _binding_edges_in_month(
        imei_binding,
        left_kind="msisdn",
        right_kind="imei",
        month_start=month_start,
        month_end=month_end,
    ):
        uf.union(left_node, right_node)

    def cluster_key(row: pd.Series) -> str | None:
        nodes = _identifier_nodes_from_row(row)
        if not nodes:
            return None
        return uf.find(min(nodes))

    out = work.copy()
    out["person_cluster_key"] = out.apply(cluster_key, axis=1)
    return out


def _build_person_id_from_key(cluster_key: str) -> str:
    payload = str(cluster_key).strip()
    if not payload:
        return "prs_unknown"
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]
    return f"prs_{digest}"


def _str_field(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except TypeError:
        pass
    text = str(value).strip()
    return "" if text.lower() in ("<na>", "nan", "none") else text


def _derive_gender(row: pd.Series) -> str:
    first_name = _str_field(row.get("first_name")).lower()
    second_name = _str_field(row.get("second_name")).lower()
    if second_name.endswith(("вна", "ична", "кызы", "оглыкызы")):
        return "F"
    if second_name.endswith(("вич", "оглы")):
        return "M"
    if first_name.endswith(("а", "я")):
        return "F"
    if first_name:
        return "M"
    return "U"


def _derive_age_as_of_month_start(birth_day: pd.Timestamp | Any, month_start: pd.Timestamp) -> str:
    value = pd.to_datetime(birth_day, errors="coerce")
    if pd.isna(value):
        return "U"
    years = int((month_start.date() - value.date()).days // 365.2425)
    if years < 0 or years > 120:
        return "U"
    return str(years)


_DEPARTMENT_TOKEN_TO_CODE: dict[str, str] = {
    "мвд": "RU",
    "овд": "RU",
    "умвд": "RU",
    "гувд": "RU",
    "russia": "RU",
    "росс": "RU",
    "rf": "RU",
    "kaz": "KZ",
    "қаз": "KZ",
    "kazakhstan": "KZ",
    "uzb": "UZ",
    "узб": "UZ",
    "uzbek": "UZ",
    "tjk": "TJ",
    "тадж": "TJ",
    "tajik": "TJ",
    "kgz": "KG",
    "кырг": "KG",
    "kyrgyz": "KG",
    "blr": "BY",
    "белар": "BY",
    "belarus": "BY",
    "arm": "AM",
    "арм": "AM",
    "armenia": "AM",
    "aze": "AZ",
    "азер": "AZ",
    "azerbaijan": "AZ",
    "ukr": "UA",
    "укр": "UA",
    "ukraine": "UA",
    "china": "CN",
    "chinese": "CN",
    "кнр": "CN",
    "german": "DE",
    "deutsch": "DE",
    "auswärtiges": "DE",
    "usa": "US",
    "american": "US",
    "state usa": "US",
}

_DOCUMENT_TOKEN_TO_CODE: dict[str, str] = {
    "паспорт рф": "RU",
    "российск": "RU",
    "загран": "RU",
    "казахстан": "KZ",
    "узбекистан": "UZ",
    "таджикистан": "TJ",
    "кыргыз": "KG",
    "беларус": "BY",
    "армен": "AM",
    "азербайджан": "AZ",
    "украин": "UA",
    "кнр": "CN",
    "chinese": "CN",
    "reisepass": "DE",
    "german": "DE",
    "us passport": "US",
    "american": "US",
}

_NAME_HINT_TO_CODE: tuple[tuple[str, str], ...] = (
    ("қаз", "KZ"),
    ("kaz", "KZ"),
    ("nurlan", "KZ"),
    ("uzb", "UZ"),
    ("узб", "UZ"),
    ("tjk", "TJ"),
    ("тадж", "TJ"),
    ("kgz", "KG"),
    ("кырг", "KG"),
    ("blr", "BY"),
    ("белар", "BY"),
    ("arm", "AM"),
    ("арм", "AM"),
    ("aze", "AZ"),
    ("азер", "AZ"),
    ("укр", "UA"),
    ("ukr", "UA"),
    ("wang", "CN"),
    ("zhang", "CN"),
    ("deutsch", "DE"),
    ("german", "DE"),
    ("usa", "US"),
    ("american", "US"),
    ("smith", "US"),
)


def _match_field_tokens(text: str, mapping: dict[str, str]) -> str | None:
    for token, code in mapping.items():
        if token in text:
            return code
    return None


def _match_name_hints(text: str) -> str | None:
    for token, code in _NAME_HINT_TO_CODE:
        if token in text:
            return code
    return None


def _derive_citizenship(
    *,
    dul_department: Any = None,
    document: Any = None,
    first_name: Any = None,
    second_name: Any = None,
    last_name: Any = None,
) -> str:
    dept = _str_field(dul_department).lower()
    if dept:
        hit = _match_field_tokens(dept, _DEPARTMENT_TOKEN_TO_CODE)
        if hit:
            return hit

    doc = _str_field(document).lower()
    if doc:
        hit = _match_field_tokens(doc, _DOCUMENT_TOKEN_TO_CODE)
        if hit:
            return hit

    name_blob = " ".join(_str_field(x).lower() for x in (last_name, first_name, second_name)).strip()
    if name_blob:
        hit = _match_name_hints(name_blob)
        if hit:
            return hit
        if any(s in name_blob for s in ("вич", "вна", "оглы", "оглу", "улы", "кызы", "ович")):
            if _match_field_tokens(dept, {"мвд": "RU", "овд": "RU"}) or "паспорт рф" in doc:
                return "RU"
            if not dept and not doc:
                return "RU"

    if dept or doc:
        return "U"
    return "U"


def _derive_citizenship_from_row(row: pd.Series) -> str:
    return _derive_citizenship(
        dul_department=row.get("dul_department"),
        document=row.get("document"),
        first_name=row.get("first_name"),
        second_name=row.get("second_name"),
        last_name=row.get("last_name"),
    )


def _norm_str(series: pd.Series | None) -> pd.Series:
    if series is None:
        return pd.Series(dtype="string")
    out = series.astype("string").str.strip()
    return out.where(out.str.len() > 0, pd.NA)


def _node(prefix: str, value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    return f"{prefix}:{text}"


def _to_digit_string_series(series: pd.Series | None) -> pd.Series:
    if series is None:
        return pd.Series(dtype="string")
    num = pd.to_numeric(series, errors="coerce")
    out = num.astype("Int64").astype("string")
    return out.mask(out == "<NA>", pd.NA)


def _coerce_output(df: pd.DataFrame, field_names: list[str], *, report_month: date) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=field_names)
    out = df.copy()
    out["report_date"] = pd.Timestamp(report_month).date()
    out["actually_from"] = pd.to_datetime(out["actually_from"], errors="coerce")
    out["actually_to"] = pd.to_datetime(out["actually_to"], errors="coerce")
    out = out.dropna(subset=field_names)
    out = out.drop_duplicates(subset=["person_id"], keep="first")
    return out[field_names].reset_index(drop=True)
