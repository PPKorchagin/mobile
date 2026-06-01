"""Сборка месячной витрины ``fct_person`` из ``src_person``, binding-витрин и списков исключений."""

from __future__ import annotations

import hashlib
import json
import logging
import time
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from mobile.cli_defaults import DEFAULT_PARQUET_COMPRESSION
from mobile.command_timing import append_command_metrics, timed_stage
from mobile.pipelines.stg import msisdn_imei, msisdn_imsi
from mobile.pipelines.stg.oksm import OksmLookup, load_lookup
from mobile.pipelines.stg.subscriber_ids import (
    normalize_imei,
    normalize_imsi,
    normalize_msisdn,
    to_digit_string_series,
)
from mobile.project_paths import (
    DEFAULT_SRC_EXCL_IMEI_OUTPUT,
    DEFAULT_SRC_EXCL_IMSI_OUTPUT,
    DEFAULT_SRC_EXCL_MSISDN_OUTPUT,
    DEFAULT_DIM_OKSM_OUTPUT_PATH,
    DEFAULT_FCT_PERSON_SCHEMA_PATH,
    DEFAULT_DIM_TAC_OUTPUT_PATH,
    SRC_PERSON_LAYOUT_TEMPLATE,
    SRC_PERSON_SUCCESS_FLAG,
    resolve_project_path,
    resolve_stg_monthly_parquet_path,
    stg_geo_all_output_path,
    fct_msisdn_imei_output_path,
    fct_msisdn_imsi_output_path,
    fct_person_output_path,
)

logger = logging.getLogger(__name__)

_OPEN_ACTUALLY_TO = pd.Timestamp("2999-12-31 23:59:59")

STG_PERSON_TABLE = "fct_person"
FCT_PERSON_FIELDS: list[dict[str, str]] = []


def _identity_node(prefix: str, value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    return f"{prefix}:{text}"


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


class _UnionFind:
    def __init__(self) -> None:
        self._parent: dict[str, str] = {}

    def find(self, item: str) -> str:
        if item not in self._parent:
            self._parent[item] = item
        parent = self._parent[item]
        if parent != item:
            self._parent[item] = self.find(parent)
        return self._parent[item]

    def union(self, left: str, right: str) -> None:
        root_left = self.find(left)
        root_right = self.find(right)
        if root_left == root_right:
            return
        if root_left < root_right:
            self._parent[root_right] = root_left
        else:
            self._parent[root_left] = root_right

    def members(self) -> dict[str, set[str]]:
        clusters: dict[str, set[str]] = {}
        for item in self._parent:
            root = self.find(item)
            clusters.setdefault(root, set()).add(item)
        return clusters


def _canonical_cluster_key(cluster_nodes: set[str]) -> str:
    """Приоритет: bio → iccid → технические id."""
    bios = sorted(n for n in cluster_nodes if n.startswith("bio:"))
    if bios:
        return bios[0]
    iccids = sorted(n for n in cluster_nodes if n.startswith("iccid:"))
    if iccids:
        return iccids[0]
    return min(cluster_nodes)


def _person_confidence_for_nodes(nodes: set[str]) -> str:
    if any(n.startswith("bio:") for n in nodes):
        return "high"
    if any(n.startswith("iccid:") for n in nodes):
        return "medium"
    return "low"


def _build_person_id_from_cluster_key(cluster_key: str) -> str:
    payload = str(cluster_key).strip()
    if not payload:
        return "prs_unknown"
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]
    return f"prs_{digest}"


def _assign_person_ids(
    cluster_keys: pd.Series,
    *,
    previous_person: pd.DataFrame | None = None,
) -> pd.Series:
    key_to_person: dict[str, str] = {}
    if previous_person is not None and not previous_person.empty:
        if {"person_id", "person_cluster_key"}.issubset(previous_person.columns):
            prev = previous_person[["person_id", "person_cluster_key"]].dropna().drop_duplicates(
                subset=["person_cluster_key"], keep="first"
            )
            for row in prev.itertuples(index=False):
                key_to_person[str(row.person_cluster_key)] = str(row.person_id)

    person_ids: list[str] = []
    for cluster_key in cluster_keys:
        key = str(cluster_key)
        assigned = key_to_person.get(key)
        if assigned is None:
            assigned = _build_person_id_from_cluster_key(key)
        key_to_person[key] = assigned
        person_ids.append(assigned)
    return pd.Series(person_ids, index=cluster_keys.index, dtype="string")


def _binding_edges_in_month(
    binding: pd.DataFrame,
    *,
    left_kind: str,
    right_kind: str,
    month_start: pd.Timestamp,
    month_end: pd.Timestamp,
    open_actually_to: pd.Timestamp,
) -> list[tuple[str, str]]:
    if binding.empty:
        return []
    left_col, right_col = left_kind, right_kind
    if left_col not in binding.columns or right_col not in binding.columns:
        return []

    use_cols = [left_col, right_col, "valid_from", "valid_to"]
    frame = binding[use_cols].copy()
    normalizers = {
        "msisdn": normalize_msisdn,
        "imsi": normalize_imsi,
        "imei": normalize_imei,
    }
    frame[left_col] = normalizers[left_kind](frame[left_col].astype("string"))
    frame[right_col] = normalizers[right_kind](frame[right_col].astype("string"))
    frame["valid_from"] = pd.to_datetime(frame["valid_from"], errors="coerce")
    frame["valid_to"] = pd.to_datetime(frame["valid_to"], errors="coerce").fillna(open_actually_to)
    frame = frame.dropna(subset=[left_col, right_col, "valid_from", "valid_to"])
    overlaps = (frame["valid_from"] <= month_end) & (frame["valid_to"] >= month_start)
    frame = frame.loc[overlaps]
    edges: list[tuple[str, str]] = []
    for left_value, right_value in zip(frame[left_col], frame[right_col], strict=True):
        left_node = _identity_node(left_kind, left_value)
        right_node = _identity_node(right_kind, right_value)
        if left_node is not None and right_node is not None:
            edges.append((left_node, right_node))
    return edges


def _operator_observation_edges(
    operator_binding: pd.DataFrame,
    *,
    month_start: pd.Timestamp,
    month_end: pd.Timestamp,
    open_actually_to: pd.Timestamp,
) -> list[tuple[str, str]]:
    if operator_binding.empty:
        return []
    frame = operator_binding.copy()
    frame["msisdn"] = normalize_msisdn(to_digit_string_series(frame.get("msisdn")))
    frame["imsi"] = normalize_imsi(to_digit_string_series(frame.get("imsi")))
    frame["valid_from"] = pd.to_datetime(frame["valid_from"], errors="coerce")
    frame["valid_to"] = pd.to_datetime(frame["valid_to"], errors="coerce").fillna(open_actually_to)
    frame = frame.dropna(subset=["msisdn", "valid_from", "valid_to"])
    overlaps = (frame["valid_from"] <= month_end) & (frame["valid_to"] >= month_start)
    frame = frame.loc[overlaps]
    edges: list[tuple[str, str]] = []
    for row in frame.itertuples(index=False):
        msisdn_node = _identity_node("msisdn", getattr(row, "msisdn", None))
        if msisdn_node is None:
            continue
        imsi_val = getattr(row, "imsi", None)
        if imsi_val is not None and not pd.isna(imsi_val):
            imsi_node = _identity_node("imsi", imsi_val)
            if imsi_node is not None:
                edges.append((msisdn_node, imsi_node))
    return edges


_SRC_PERSON_READ_COLUMNS = [
    "operator_Id",
    "isdn",
    "imsi",
    "imei",
    "iccid",
    "contract_number",
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


def _resolve_person_layout_root(layout: str) -> Path:
    path = resolve_project_path(layout)
    parts = path.parts
    idx = next((i for i, part in enumerate(parts) if "{" in part and "}" in part), None)
    if idx is None:
        return path.parent if path.suffix else path
    return Path(*parts[:idx])


def _parse_src_person_load_day(day_dir: Path, *, year: int, month: int) -> date | None:
    prefix = "load_day="
    if not day_dir.name.startswith(prefix):
        return None
    try:
        day_num = int(day_dir.name[len(prefix) :])
    except ValueError:
        return None
    if day_num < 1 or day_num > 31:
        return None
    try:
        return date(year, month, day_num)
    except ValueError:
        return None


def _list_success_person_parquets_in_period(
    *,
    root: Path,
    period_start: date,
    period_end: date,
) -> list[tuple[date, Path]]:
    year, month = period_start.year, period_start.month
    month_dir = root / f"load_year={year:04d}" / f"load_month={month:02d}"
    if not month_dir.exists():
        return []

    candidates: list[tuple[date, Path]] = []
    for day_dir in sorted(month_dir.glob("load_day=*")):
        load_day = _parse_src_person_load_day(day_dir, year=year, month=month)
        if load_day is None or load_day < period_start or load_day > period_end:
            continue
        if not (day_dir / SRC_PERSON_SUCCESS_FLAG).exists():
            continue
        parquet_path = day_dir / "person.parquet"
        if parquet_path.exists():
            candidates.append((load_day, parquet_path))
    return candidates


def _read_src_person_latest_snapshot(
    *,
    period_start: date,
    period_end: date,
    src_person_path: str | Path | None,
) -> tuple[pd.DataFrame, list[date]]:
    """Последний ``load_day`` с ``_SUCCESS`` за период (или один parquet-файл)."""
    if src_person_path is not None:
        resolved = resolve_project_path(src_person_path)
        if resolved.is_file():
            try:
                frame = pd.read_parquet(resolved, columns=_SRC_PERSON_READ_COLUMNS)
            except Exception:
                logger.exception("build-fct-person: failed to read src_person at %s", resolved)
                return pd.DataFrame(columns=_SRC_PERSON_READ_COLUMNS), []
            return frame, []

    root = (
        _resolve_person_layout_root(SRC_PERSON_LAYOUT_TEMPLATE)
        if src_person_path is None
        else resolve_project_path(src_person_path)
    )
    candidates = _list_success_person_parquets_in_period(
        root=root,
        period_start=period_start,
        period_end=period_end,
    )
    if not candidates:
        raise FileNotFoundError(
            f"No src_person snapshots with {SRC_PERSON_SUCCESS_FLAG!r} for "
            f"{period_start.isoformat()}..{period_end.isoformat()} under {root}"
        )

    latest_day, latest_path = max(candidates, key=lambda item: item[0])
    latest = pd.read_parquet(latest_path, columns=_SRC_PERSON_READ_COLUMNS)
    logger.info(
        "build-fct-person: src_person load_day=%s rows=%s",
        latest_day.isoformat(),
        len(latest),
    )
    return latest, [latest_day]


def _load_schema_contract(schema_path: Path) -> None:
    global STG_PERSON_TABLE, FCT_PERSON_FIELDS
    with schema_path.open(encoding="utf-8") as file:
        cfg = json.load(file)
    STG_PERSON_TABLE = str(cfg.get("table", STG_PERSON_TABLE))
    FCT_PERSON_FIELDS = [{"name": str(f["name"]), "type": str(f["type"])} for f in cfg.get("fields", [])]


_load_schema_contract(DEFAULT_FCT_PERSON_SCHEMA_PATH)


def _validate_report_month(report_date: date) -> date:
    if report_date.day != 1:
        raise ValueError(f"build-fct-person: report_date must be YYYY-MM-01, got {report_date.isoformat()}")
    return report_date


def _month_period(report_month: date) -> tuple[date, date]:
    month_end = (pd.Timestamp(report_month) + pd.offsets.MonthEnd(0)).date()
    return report_month, month_end


def _month_end_ts(report_month: date) -> pd.Timestamp:
    return pd.Timestamp(report_month) + pd.offsets.MonthEnd(0)


def _previous_report_month(report_month: date) -> date:
    return (pd.Timestamp(report_month) - pd.offsets.MonthBegin(1)).date()


def run_build(
    report_date: date,
    *,
    src_person_path: str | Path | None = None,
    fct_msisdn_imsi_path: str | Path | None = None,
    fct_msisdn_imei_path: str | Path | None = None,
    src_excl_imsi_path: str | Path | None = None,
    src_excl_imei_path: str | Path | None = None,
    src_excl_msisdn_path: str | Path | None = None,
    dim_tac_path: str | Path | None = None,
    dim_oksm_path: str | Path | None = None,
    output_path: str | Path | None = None,
    sync_bindings_from_geo: bool = True,
) -> dict[str, Any]:
    command = "build-fct-person"
    perf: dict[str, Any] = {}
    started = time.perf_counter()
    report_month = _validate_report_month(report_date)
    period_start, period_end = _month_period(report_month)
    month_start = pd.Timestamp(report_month)
    month_end = _month_end_ts(report_month)

    person_out = resolve_project_path(output_path) if output_path else fct_person_output_path(report_month)
    field_names = [f["name"] for f in FCT_PERSON_FIELDS]

    imsi_month_path = _resolve_monthly_binding_path(
        report_month=report_month,
        kind="imsi",
        explicit_path=fct_msisdn_imsi_path,
    )
    imei_month_path = _resolve_monthly_binding_path(
        report_month=report_month,
        kind="imei",
        explicit_path=fct_msisdn_imei_path,
    )

    binding_days_synced = 0
    if sync_bindings_from_geo:
        with timed_stage("sync_bindings_sec", perf):
            binding_days_synced = _sync_monthly_bindings_from_geo(
                report_month,
                imsi_month_path=imsi_month_path,
                imei_month_path=imei_month_path,
            )

    with timed_stage("read_src_person_sec", perf):
        raw, src_load_days = _read_src_person_latest_snapshot(
            period_start=period_start,
            period_end=period_end,
            src_person_path=src_person_path,
        )
    src_rows_before_exclusions = int(len(raw))

    tac_path = resolve_project_path(dim_tac_path) if dim_tac_path is not None else DEFAULT_DIM_TAC_OUTPUT_PATH
    oksm_path = resolve_project_path(dim_oksm_path) if dim_oksm_path is not None else DEFAULT_DIM_OKSM_OUTPUT_PATH
    excl_imsi_path = resolve_project_path(src_excl_imsi_path or DEFAULT_SRC_EXCL_IMSI_OUTPUT)
    excl_imei_path = resolve_project_path(src_excl_imei_path or DEFAULT_SRC_EXCL_IMEI_OUTPUT)
    excl_msisdn_path = resolve_project_path(src_excl_msisdn_path or DEFAULT_SRC_EXCL_MSISDN_OUTPUT)

    with timed_stage("load_oksm_sec", perf):
        oksm_lookup = load_lookup(oksm_path)
    with timed_stage("exclusions_sec", perf):
        excl_sets = _load_excl_sets(excl_imsi_path, excl_imei_path, excl_msisdn_path)
        raw, excluded_excl_rows = _exclude_src_person_by_excl(raw, excl_sets=excl_sets)
        m2m_tacs = _read_m2m_tac_set(tac_path)
        raw, excluded_m2m_tac_rows = _exclude_m2m_by_tac(raw, m2m_tacs=m2m_tacs)

    with timed_stage("read_bindings_sec", perf):
        imsi_binding = _filter_binding_excl(
            _read_binding_parquet(imsi_month_path, kind="imsi"),
            kind="imsi",
            excl_sets=excl_sets,
        )
        imei_binding = _filter_binding_excl(
            _read_binding_parquet(imei_month_path, kind="imei"),
            excl_sets=excl_sets,
        )
    operator_binding = imsi_binding

    previous_person = _load_previous_person(_previous_report_month(report_month))

    with timed_stage("transform_sec", perf):
        work, binding_fill, cluster_to_nodes = _prepare_subscriptions(
            raw=raw,
            report_month=report_month,
            month_start=month_start,
            month_end=month_end,
            imsi_binding=imsi_binding,
            imei_binding=imei_binding,
            operator_binding=operator_binding,
            previous_person=previous_person,
        )
        person_df = _build_person_output(
            work,
            report_month=report_month,
            field_names=field_names,
            oksm_lookup=oksm_lookup,
        )

    with timed_stage("write_sec", perf):
        person_out.parent.mkdir(parents=True, exist_ok=True)
        person_df.to_parquet(person_out, compression=DEFAULT_PARQUET_COMPRESSION, index=False)

    stats: dict[str, Any] = {
        "command": command,
        "table": STG_PERSON_TABLE,
        "report_date": report_month.isoformat(),
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "src_load_days": len(src_load_days),
        "src_rows_read": src_rows_before_exclusions,
        "src_rows_after_excl": int(len(raw)),
        "excluded_excl_rows": int(excluded_excl_rows),
        "excluded_m2m_tac_rows": int(excluded_m2m_tac_rows),
        "binding_days_synced": int(binding_days_synced),
        "output_path": str(person_out),
        "fct_msisdn_imsi_path": str(imsi_month_path),
        "fct_msisdn_imei_path": str(imei_month_path),
        "src_excl_imsi_path": str(excl_imsi_path),
        "src_excl_imei_path": str(excl_imei_path),
        "src_excl_msisdn_path": str(excl_msisdn_path),
        "stg_rows_written": int(len(person_df)),
        "distinct_person_id": int(person_df["person_id"].nunique()) if not person_df.empty else 0,
        "binding_imsi_filled": int(binding_fill.get("imsi", 0)),
        "binding_imei_filled": int(binding_fill.get("imei", 0)),
        "binding_msisdn_filled": int(binding_fill.get("msisdn", 0)),
    }
    perf["elapsed_total_sec"] = round(time.perf_counter() - started, 4)
    append_command_metrics(command=command, metrics={**stats, **perf})
    logger.info("%s completed: %s", command, stats)
    return {**stats, **perf}


def _month_days(report_month: date) -> list[date]:
    start = report_month.replace(day=1)
    end = (pd.Timestamp(start) + pd.offsets.MonthEnd(0)).date()
    days: list[date] = []
    cursor = start
    while cursor <= end:
        days.append(cursor)
        cursor += timedelta(days=1)
    return days


def _sync_monthly_bindings_from_geo(
    report_month: date,
    *,
    imsi_month_path: Path,
    imei_month_path: Path,
) -> int:
    """Инкремент месячных ``fct_msisdn_imsi`` / ``fct_msisdn_imei`` из ``stg_geo_all`` по дням месяца."""
    days_run = 0
    for day in _month_days(report_month):
        geo = stg_geo_all_output_path(day)
        if not geo.exists():
            continue
        msisdn_imsi.run_build(
            report_date=day,
            stg_geo_all_path=geo,
            output_path=imsi_month_path,
        )
        msisdn_imei.run_build(
            report_date=day,
            stg_geo_all_path=geo,
            output_path=imei_month_path,
        )
        days_run += 1
    logger.info("build-fct-person: synced bindings for %s days in %s", days_run, report_month.isoformat())
    return days_run


def _resolve_monthly_binding_path(
    *,
    report_month: date,
    kind: str,
    explicit_path: str | Path | None,
) -> Path:
    if explicit_path is not None:
        return resolve_stg_monthly_parquet_path(explicit_path, report_month)
    if kind == "imsi":
        return fct_msisdn_imsi_output_path(report_month)
    return fct_msisdn_imei_output_path(report_month)


def _read_binding_parquet(path: Path, *, kind: str) -> pd.DataFrame:
    value_col = "imsi" if kind == "imsi" else "imei"
    columns = ["msisdn", value_col, "valid_from", "valid_to"]
    if not path.exists():
        logger.warning("build-fct-person: binding not found at %s", path)
        return pd.DataFrame(columns=columns)
    binding = pd.read_parquet(path, columns=columns)
    out = binding.copy()
    out["msisdn"] = normalize_msisdn(to_digit_string_series(out.get("msisdn")))
    if kind == "imsi":
        out["imsi"] = normalize_imsi(to_digit_string_series(out.get("imsi")))
    else:
        out["imei"] = normalize_imei(to_digit_string_series(out.get("imei")))
    out["valid_from"] = pd.to_datetime(out["valid_from"], errors="coerce")
    out["valid_to"] = pd.to_datetime(out["valid_to"], errors="coerce").fillna(_OPEN_ACTUALLY_TO)
    return out.dropna(subset=["msisdn", value_col, "valid_from", "valid_to"]).reset_index(drop=True)


def _load_previous_person(prev_month: date) -> pd.DataFrame | None:
    path = fct_person_output_path(prev_month)
    if not path.exists():
        return None
    return pd.read_parquet(path, columns=["person_id", "person_cluster_key"])


def _read_m2m_tac_set(tac_path: Path) -> set[str]:
    if not tac_path.exists():
        logger.warning("build-fct-person: dim_tac not found, skipping M2M TAC exclusion: %s", tac_path)
        return set()
    tac_df = pd.read_parquet(tac_path, columns=["tac", "is_m2m"])
    m2m = tac_df[tac_df["is_m2m"].fillna(False).astype(bool)]
    return set(m2m["tac"].astype("string").str.strip().dropna())


def _exclude_m2m_by_tac(raw: pd.DataFrame, *, m2m_tacs: set[str]) -> tuple[pd.DataFrame, int]:
    if raw.empty or not m2m_tacs:
        return raw, 0
    digits = to_digit_string_series(raw.get("imei")).astype("string").str.replace(r"\D", "", regex=True)
    imei_tac = digits.where(digits.str.len() >= 8, pd.NA).str[:8]
    m2m_mask = imei_tac.isin(m2m_tacs) & imei_tac.notna()
    excluded_rows = int(m2m_mask.sum())
    return raw.loc[~m2m_mask].copy(), excluded_rows


@dataclass(frozen=True)
class _ExclSets:
    msisdn: set[str]
    imsi: set[str]
    imei: set[str]


def _load_excl_sets(imsi_path: Path, imei_path: Path, msisdn_path: Path) -> _ExclSets:
    return _ExclSets(
        msisdn=_load_excl_value_set(msisdn_path, normalize=normalize_msisdn),
        imsi=_load_excl_value_set(imsi_path, normalize=normalize_imsi),
        imei=_load_excl_value_set(imei_path, normalize=normalize_imei),
    )


def _load_excl_value_set(path: Path, *, normalize: Callable[[pd.Series | None], pd.Series]) -> set[str]:
    if not path.exists():
        logger.warning("build-fct-person: excl list not found at %s", path)
        return set()
    frame = pd.read_parquet(path, columns=["value"])
    values = normalize(to_digit_string_series(frame.get("value")))
    return set(values.dropna().astype("string"))


def _exclude_src_person_by_excl(raw: pd.DataFrame, *, excl_sets: _ExclSets) -> tuple[pd.DataFrame, int]:
    if raw.empty:
        return raw, 0
    msisdn = normalize_msisdn(to_digit_string_series(raw.get("isdn")))
    imsi = normalize_imsi(to_digit_string_series(raw.get("imsi")))
    imei = normalize_imei(to_digit_string_series(raw.get("imei")))
    mask = pd.Series(False, index=raw.index)
    if excl_sets.msisdn:
        mask |= msisdn.isin(excl_sets.msisdn)
    if excl_sets.imsi:
        mask |= imsi.isin(excl_sets.imsi)
    if excl_sets.imei:
        mask |= imei.isin(excl_sets.imei)
    excluded_rows = int(mask.sum())
    return raw.loc[~mask].copy(), excluded_rows


def _filter_binding_excl(binding: pd.DataFrame, *, kind: str, excl_sets: _ExclSets) -> pd.DataFrame:
    if binding.empty:
        return binding
    out = binding.copy()
    if excl_sets.msisdn:
        out = out.loc[~out["msisdn"].isin(excl_sets.msisdn)]
    value_col = "imsi" if kind == "imsi" else "imei"
    excl_values = excl_sets.imsi if kind == "imsi" else excl_sets.imei
    if excl_values and value_col in out.columns:
        out = out.loc[~out[value_col].isin(excl_values)]
    return out.reset_index(drop=True)


def _prepare_subscriptions(
    *,
    raw: pd.DataFrame,
    report_month: date,
    month_start: pd.Timestamp,
    month_end: pd.Timestamp,
    imsi_binding: pd.DataFrame,
    imei_binding: pd.DataFrame,
    operator_binding: pd.DataFrame,
    previous_person: pd.DataFrame | None,
) -> tuple[pd.DataFrame, dict[str, int], dict[str, set[str]]]:
    if raw.empty:
        return pd.DataFrame(), {"imsi": 0, "imei": 0, "msisdn": 0}, {}

    work = raw.copy()
    client_type = pd.to_numeric(work.get("client_type"), errors="coerce")
    work = work.loc[client_type == 0].copy()
    work["actually_from"] = pd.to_datetime(work.get("actually_from"), errors="coerce")
    work["actually_to"] = pd.to_datetime(work.get("actually_to"), errors="coerce").fillna(_OPEN_ACTUALLY_TO)
    work = work.loc[
        work["actually_from"].notna() & (work["actually_from"] <= month_end) & (work["actually_to"] >= month_start)
    ].copy()

    work["msisdn"] = normalize_msisdn(to_digit_string_series(work.get("isdn")))
    work["imsi"] = normalize_imsi(to_digit_string_series(work.get("imsi")))
    work["imei"] = normalize_imei(to_digit_string_series(work.get("imei")))
    work["iccid"] = _norm_str(work.get("iccid"))
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

    work, cluster_to_nodes = _assign_clusters(
        work,
        imsi_binding=imsi_binding,
        imei_binding=imei_binding,
        operator_binding=operator_binding,
        month_start=month_start,
        month_end=month_end,
    )
    work = work[work["person_cluster_key"].notna()].copy()
    if work.empty:
        return work, fill_stats, cluster_to_nodes
    work["person_id"] = _assign_person_ids(
        work["person_cluster_key"],
        previous_person=previous_person,
    )
    return work, fill_stats, cluster_to_nodes


def _vectorized_bio_key(work: pd.DataFrame) -> pd.Series:
    last_name = work["last_name"].astype("string").str.strip().str.casefold()
    first_name = work["first_name"].astype("string").str.strip().str.casefold()
    second_name = work["second_name"].astype("string").str.strip().str.casefold()
    birth = pd.to_datetime(work["birth_day"], errors="coerce")
    document = work["document"].astype("string").str.replace(r"\D", "", regex=True)
    valid = last_name.notna() & first_name.notna() & birth.notna() & (last_name != "") & (first_name != "")
    key = (
        "bio:"
        + last_name.fillna("")
        + "|"
        + first_name.fillna("")
        + "|"
        + second_name.fillna("")
        + "|"
        + birth.dt.date.astype("string").fillna("")
        + "|"
        + document.fillna("")
    )
    return key.where(valid, pd.NA)


def _unite_pair_column(uf: _UnionFind, frame: pd.DataFrame, left_kind: str, right_kind: str) -> None:
    left_col, right_col = left_kind, right_kind
    if left_col not in frame.columns or right_col not in frame.columns:
        return
    pairs = frame[[left_col, right_col]].dropna().drop_duplicates()
    for left_value, right_value in zip(pairs[left_col], pairs[right_col], strict=True):
        left_node = _identity_node(left_kind, left_value)
        right_node = _identity_node(right_kind, right_value)
        if left_node is not None and right_node is not None:
            uf.union(left_node, right_node)


def _assign_clusters(
    work: pd.DataFrame,
    *,
    imsi_binding: pd.DataFrame,
    imei_binding: pd.DataFrame,
    operator_binding: pd.DataFrame,
    month_start: pd.Timestamp,
    month_end: pd.Timestamp,
) -> tuple[pd.DataFrame, dict[str, set[str]]]:
    uf = _UnionFind()
    work = work.copy()
    work["bio_key"] = _vectorized_bio_key(work)
    _unite_pair_column(uf, work, "msisdn", "imsi")
    _unite_pair_column(uf, work, "msisdn", "imei")
    _unite_pair_column(uf, work, "msisdn", "iccid")
    bio_frame = work.loc[work["bio_key"].notna(), ["msisdn", "bio_key"]].rename(columns={"bio_key": "bio"})
    _unite_pair_column(uf, bio_frame, "msisdn", "bio")

    for left, right in _binding_edges_in_month(
        imsi_binding,
        left_kind="msisdn",
        right_kind="imsi",
        month_start=month_start,
        month_end=month_end,
        open_actually_to=_OPEN_ACTUALLY_TO,
    ):
        uf.union(left, right)
    for left, right in _binding_edges_in_month(
        imei_binding,
        left_kind="msisdn",
        right_kind="imei",
        month_start=month_start,
        month_end=month_end,
        open_actually_to=_OPEN_ACTUALLY_TO,
    ):
        uf.union(left, right)
    for left, right in _operator_observation_edges(
        operator_binding,
        month_start=month_start,
        month_end=month_end,
        open_actually_to=_OPEN_ACTUALLY_TO,
    ):
        uf.union(left, right)

    members = uf.members()
    root_to_canonical: dict[str, str] = {}
    root_to_nodes: dict[str, set[str]] = {}
    canonical_to_nodes: dict[str, set[str]] = {}
    for root, nodes in members.items():
        root_to_canonical[root] = _canonical_cluster_key(nodes)
        root_to_nodes[root] = nodes
        canon = root_to_canonical[root]
        canonical_to_nodes[canon] = canonical_to_nodes.get(canon, set()) | nodes

    def roots_for_row(row: pd.Series) -> list[str]:
        nodes = []
        for prefix, col in (("imsi", "imsi"), ("imei", "imei"), ("msisdn", "msisdn")):
            item = _identity_node(prefix, row.get(col))
            if item is not None:
                nodes.append(item)
        bio_val = row.get("bio_key")
        if bio_val is not None and not pd.isna(bio_val):
            nodes.append(str(bio_val))
        iccid_val = row.get("iccid")
        iccid_item = _identity_node("iccid", iccid_val)
        if iccid_item is not None:
            nodes.append(iccid_item)
        return [uf.find(item) for item in nodes]

    cluster_keys: list[str | None] = []
    confidences: list[str] = []
    for tup in work.itertuples(index=False):
        row_series = pd.Series(tup._asdict())
        roots = roots_for_row(row_series)
        if not roots:
            cluster_keys.append(None)
            confidences.append("low")
            continue
        root = min(roots)
        canon = root_to_canonical.get(root, root)
        cluster_keys.append(canon)
        confidences.append(_person_confidence_for_nodes(root_to_nodes.get(root, set())))

    out = work.copy()
    out["person_cluster_key"] = cluster_keys
    out["person_confidence"] = confidences
    return out.drop(columns=["bio_key"], errors="ignore"), canonical_to_nodes


def _build_person_output(
    work: pd.DataFrame,
    *,
    report_month: date,
    field_names: list[str],
    oksm_lookup: OksmLookup,
) -> pd.DataFrame:
    if work.empty:
        return pd.DataFrame(columns=field_names)

    month_start = pd.Timestamp(report_month)
    primary_idx = work.sort_values(["person_id", "actually_from"], ascending=[True, False]).groupby(
        "person_id", sort=False
    ).head(1).index

    work["sim_key"] = work["imsi"].astype("string") + "|" + work["iccid"].astype("string").fillna("")
    sim_counts = work.groupby("person_id")["sim_key"].nunique(dropna=True)

    profile = work.loc[primary_idx].copy()
    profile["report_date"] = pd.Timestamp(report_month).date()
    profile["sim_count"] = profile["person_id"].map(sim_counts).fillna(1).astype("Int64")
    profile["gender"] = profile.apply(_derive_gender, axis=1)
    profile["age"] = profile["birth_day"].map(lambda v: _derive_age_as_of_month_start(v, month_start))
    profile["citizenship"] = profile.apply(
        lambda row: _derive_citizenship_from_row(row, oksm_lookup=oksm_lookup),
        axis=1,
    ).astype("string")

    person_out = profile.drop(columns=["sim_key"], errors="ignore")
    person_out = person_out.drop_duplicates(subset=["person_id"], keep="first")
    for col in field_names:
        if col not in person_out.columns:
            person_out[col] = pd.NA
    return person_out[field_names].reset_index(drop=True)


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
    if binding.empty:
        return work, 0
    out = work.copy()
    missing = out[target_col].isna() & out[lookup_col].notna()
    if not bool(missing.any()):
        return out, 0
    need = out.loc[missing, [lookup_col]].copy()
    need["_row_id"] = need.index
    merged = need.merge(binding[[lookup_col, value_col, "valid_from", "valid_to"]], on=lookup_col, how="inner")
    in_range = (merged["valid_from"] <= at) & (merged["valid_to"] >= at)
    merged = merged.loc[in_range].sort_values(["_row_id", "valid_from"], ascending=[True, False])
    best = merged.drop_duplicates(subset=["_row_id"], keep="first")
    filled = 0
    for row in best.itertuples(index=False):
        rid = getattr(row, "_row_id")
        if pd.isna(out.at[rid, target_col]):
            out.at[rid, target_col] = getattr(row, value_col)
            filled += 1
    return out, int(filled)


def _norm_str(series: pd.Series | None) -> pd.Series:
    if series is None:
        return pd.Series(dtype="string")
    out = series.astype("string").str.strip()
    return out.where(out.str.len() > 0, pd.NA)


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


def _derive_citizenship_from_row(row: pd.Series, *, oksm_lookup: OksmLookup) -> str:
    return _derive_citizenship(
        dul_department=row.get("dul_department"),
        document=row.get("document"),
        first_name=row.get("first_name"),
        second_name=row.get("second_name"),
        last_name=row.get("last_name"),
        oksm_lookup=oksm_lookup,
    )


def _derive_citizenship(
    *,
    dul_department: Any = None,
    document: Any = None,
    first_name: Any = None,
    second_name: Any = None,
    last_name: Any = None,
    oksm_lookup: OksmLookup,
) -> str:
    dept = _str_field(dul_department).lower()
    doc = _str_field(document).lower()
    name_blob = " ".join(_str_field(x).lower() for x in (last_name, first_name, second_name)).strip()
    combined = " ".join(part for part in (dept, doc, name_blob) if part)

    if dept and (hit := oksm_lookup.match_text_tokens(dept, _DEPT_MAP)):
        return hit
    if doc and (hit := oksm_lookup.match_text_tokens(doc, _DOC_MAP)):
        return hit
    if combined and (hit := oksm_lookup.match_text_tokens(combined, _NAME_HINTS)):
        return hit
    if combined and (hit := oksm_lookup.match_country_names(combined)):
        return hit
    if name_blob and any(s in name_blob for s in ("вич", "вна", "оглы", "кызы")):
        if "мвд" in dept or "паспорт рф" in doc:
            return oksm_lookup.default_russia()
        if not dept and not doc:
            return oksm_lookup.default_russia()
    return "U"


# Подстроки в bio → ISO alpha-2; в ``fct_person.citizenship`` — numeric_code из ``dim_oksm``.
_DEPT_MAP = {
    "мвд": "RU",
    "овд": "RU",
    "умвд": "RU",
    "russia": "RU",
    "kaz": "KZ",
    "uzb": "UZ",
    "tajik": "TJ",
    "kyrgyz": "KG",
    "belarus": "BY",
    "armenia": "AM",
    "azerbaijan": "AZ",
    "ukrain": "UA",
    "china": "CN",
    "german": "DE",
    "american": "US",
}
_DOC_MAP = {
    "паспорт рф": "RU",
    "российск": "RU",
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
    "german passport": "DE",
    "american passport": "US",
}
_NAME_HINTS = {
    "kaz": "KZ",
    "uzb": "UZ",
    "ukr": "UA",
    "tj": "TJ",
    "kg": "KG",
    "blr": "BY",
    "arm": "AM",
    "aze": "AZ",
}

