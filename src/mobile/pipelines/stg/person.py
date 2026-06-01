"""Сборка ``stg_person``, ``stg_person_sim``, ``stg_person_id_ledger`` за месяц."""

from __future__ import annotations

import json
import logging
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from mobile.cli_defaults import DEFAULT_PARQUET_COMPRESSION
from mobile.command_timing import append_command_metrics, timed_stage
from mobile.pipelines.stg import msisdn_imei, msisdn_imsi
from mobile.pipelines.stg.person_identity import (
    UnionFind,
    assign_person_ids_with_ledger,
    binding_edges_in_month,
    canonical_cluster_key,
    node,
    operator_observation_edges,
    person_confidence_for_nodes,
    str_field,
    to_digit_string_series,
)
from mobile.pipelines.stg.oksm import OksmLookup, load_lookup
from mobile.pipelines.stg.src_person_month import read_src_person_month
from mobile.pipelines.stg.subscriber_ids import normalize_imei, normalize_imsi, normalize_msisdn
from mobile.project_paths import (
    DEFAULT_STG_OKSM_OUTPUT_PATH,
    DEFAULT_STG_PERSON_SCHEMA_PATH,
    DEFAULT_STG_TAC_OUTPUT_PATH,
    resolve_project_path,
    stg_geo_all_output_path,
    stg_msisdn_imei_output_path,
    stg_msisdn_imsi_output_path,
    stg_person_id_ledger_output_path,
    stg_person_output_path,
    stg_person_sim_output_path,
)

logger = logging.getLogger(__name__)

_OPEN_ACTUALLY_TO = pd.Timestamp("2999-12-31 23:59:59")

STG_PERSON_TABLE = "stg_person"
STG_PERSON_FIELDS: list[dict[str, str]] = []
STG_PERSON_SIM_FIELDS: list[dict[str, str]] = [
    {"name": "report_date", "type": "date"},
    {"name": "person_id", "type": "string"},
    {"name": "msisdn", "type": "string"},
    {"name": "imsi", "type": "string"},
    {"name": "imei", "type": "string"},
    {"name": "iccid", "type": "string"},
    {"name": "operator_id", "type": "long"},
    {"name": "contract_number", "type": "string"},
    {"name": "actually_from", "type": "timestamp"},
    {"name": "actually_to", "type": "timestamp"},
    {"name": "is_primary", "type": "bool"},
]


def _load_schema_contract(schema_path: Path) -> None:
    global STG_PERSON_TABLE, STG_PERSON_FIELDS
    with schema_path.open(encoding="utf-8") as file:
        cfg = json.load(file)
    STG_PERSON_TABLE = str(cfg.get("table", STG_PERSON_TABLE))
    STG_PERSON_FIELDS = [{"name": str(f["name"]), "type": str(f["type"])} for f in cfg.get("fields", [])]


_load_schema_contract(DEFAULT_STG_PERSON_SCHEMA_PATH)


def _validate_report_month(report_date: date) -> date:
    if report_date.day != 1:
        raise ValueError(f"build-stg-person: report_date must be YYYY-MM-01, got {report_date.isoformat()}")
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
    stg_msisdn_imsi_path: str | Path | None = None,
    stg_msisdn_imei_path: str | Path | None = None,
    stg_tac_path: str | Path | None = None,
    stg_oksm_path: str | Path | None = None,
    output_path: str | Path | None = None,
    person_sim_path: str | Path | None = None,
    person_ledger_path: str | Path | None = None,
    build_bindings_month: bool = True,
    build_operator_vitrine: bool = True,
) -> dict[str, Any]:
    command = "build-stg-person"
    perf: dict[str, Any] = {}
    started = time.perf_counter()
    report_month = _validate_report_month(report_date)
    period_start, period_end = _month_period(report_month)
    month_start = pd.Timestamp(report_month)
    month_end = _month_end_ts(report_month)

    person_out = resolve_project_path(output_path) if output_path else stg_person_output_path(report_month)
    sim_out = resolve_project_path(person_sim_path) if person_sim_path else stg_person_sim_output_path(report_month)
    ledger_out = resolve_project_path(person_ledger_path) if person_ledger_path else stg_person_id_ledger_output_path(
        report_month
    )
    field_names = [f["name"] for f in STG_PERSON_FIELDS]

    with timed_stage("read_src_person_sec", perf):
        raw, src_load_days = read_src_person_month(
            report_month=report_month,
            period_start=period_start,
            period_end=period_end,
            src_person_path=src_person_path,
            mode="latest_snapshot",
        )
    src_rows_before_exclusions = int(len(raw))

    tac_path = resolve_project_path(stg_tac_path) if stg_tac_path is not None else DEFAULT_STG_TAC_OUTPUT_PATH
    oksm_path = resolve_project_path(stg_oksm_path) if stg_oksm_path is not None else DEFAULT_STG_OKSM_OUTPUT_PATH
    with timed_stage("load_oksm_sec", perf):
        oksm_lookup = load_lookup(oksm_path)
    with timed_stage("exclusions_sec", perf):
        m2m_tacs = _read_m2m_tac_set(tac_path)
        raw, excluded_m2m_tac_rows = _exclude_m2m_by_tac(raw, m2m_tacs=m2m_tacs)

    imsi_month_path = _resolve_monthly_binding_path(
        report_month=report_month,
        kind="imsi",
        explicit_path=stg_msisdn_imsi_path,
    )
    imei_month_path = _resolve_monthly_binding_path(
        report_month=report_month,
        kind="imei",
        explicit_path=stg_msisdn_imei_path,
    )
    with timed_stage("load_bindings_sec", perf):
        if build_bindings_month and (not imsi_month_path.exists() or not imei_month_path.exists()):
            _refresh_month_bindings_from_geo(report_month)

    with timed_stage("build_msisdn_imsi_mnp_sec", perf):
        if build_operator_vitrine:
            raw_operator, _ = read_src_person_month(
                report_month=report_month,
                period_start=period_start,
                period_end=period_end,
                src_person_path=src_person_path,
                mode="all_snapshots",
            )
            raw_operator, _ = _exclude_m2m_by_tac(raw_operator, m2m_tacs=m2m_tacs)
            imsi_mnp = msisdn_imsi.build_imsi_intervals_from_src(raw_operator, report_month=report_month)
            imsi_month_path.parent.mkdir(parents=True, exist_ok=True)
            imsi_mnp.to_parquet(imsi_month_path, compression=DEFAULT_PARQUET_COMPRESSION, index=False)

    with timed_stage("read_bindings_sec", perf):
        imsi_binding = _read_binding_parquet(imsi_month_path, kind="imsi")
        imei_binding = _read_binding_parquet(imei_month_path, kind="imei")
    operator_binding = imsi_binding

    prev_ledger = _load_previous_ledger(_previous_report_month(report_month))

    with timed_stage("transform_sec", perf):
        work, binding_fill, cluster_to_nodes = _prepare_subscriptions(
            raw=raw,
            report_month=report_month,
            month_start=month_start,
            month_end=month_end,
            imsi_binding=imsi_binding,
            imei_binding=imei_binding,
            operator_binding=operator_binding,
            prev_ledger=prev_ledger,
        )
        person_df, sim_df, ledger_df = _build_outputs(
            work,
            cluster_to_nodes=cluster_to_nodes,
            report_month=report_month,
            field_names=field_names,
            oksm_lookup=oksm_lookup,
        )

    with timed_stage("write_sec", perf):
        person_out.parent.mkdir(parents=True, exist_ok=True)
        person_df.to_parquet(person_out, compression=DEFAULT_PARQUET_COMPRESSION, index=False)
        sim_out.parent.mkdir(parents=True, exist_ok=True)
        sim_df.to_parquet(sim_out, compression=DEFAULT_PARQUET_COMPRESSION, index=False)
        ledger_out.parent.mkdir(parents=True, exist_ok=True)
        ledger_df.to_parquet(ledger_out, compression=DEFAULT_PARQUET_COMPRESSION, index=False)

    stats: dict[str, Any] = {
        "command": command,
        "table": STG_PERSON_TABLE,
        "report_date": report_month.isoformat(),
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "src_load_days": len(src_load_days),
        "src_rows_read": src_rows_before_exclusions,
        "src_rows_after_m2m_exclusion": int(len(raw)),
        "excluded_m2m_tac_rows": int(excluded_m2m_tac_rows),
        "output_path": str(person_out),
        "person_sim_path": str(sim_out),
        "person_ledger_path": str(ledger_out),
        "stg_msisdn_imsi_path": str(imsi_month_path),
        "stg_msisdn_imei_path": str(imei_month_path),
        "stg_rows_written": int(len(person_df)),
        "person_sim_rows": int(len(sim_df)),
        "ledger_node_rows": int(len(ledger_df)),
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


def _refresh_month_bindings_from_geo(report_month: date) -> dict[str, int]:
    """Пересобрать месячные binding из ``stg_geo_all`` по дням месяца (по одному дню)."""
    days_run = 0
    for day in _month_days(report_month):
        geo = stg_geo_all_output_path(day)
        if not geo.exists():
            continue
        msisdn_imsi.run_build(
            report_date=day,
            stg_geo_all_path=geo,
            output_path=stg_msisdn_imsi_output_path(day),
        )
        msisdn_imei.run_build(
            report_date=day,
            stg_geo_all_path=geo,
            output_path=stg_msisdn_imei_output_path(day),
        )
        days_run += 1
    logger.info("build-stg-person: refreshed bindings for %s days in %s", days_run, report_month.isoformat())
    return {"binding_days_refreshed": days_run}


def _resolve_monthly_binding_path(
    *,
    report_month: date,
    kind: str,
    explicit_path: str | Path | None,
) -> Path:
    if explicit_path is not None:
        resolved = resolve_project_path(explicit_path)
        if resolved.is_dir():
            return resolved / f"{report_month.isoformat()}.parquet"
        return resolved
    if kind == "imsi":
        return stg_msisdn_imsi_output_path(report_month)
    return stg_msisdn_imei_output_path(report_month)


def _read_binding_parquet(path: Path, *, kind: str) -> pd.DataFrame:
    value_col = "imsi" if kind == "imsi" else "imei"
    columns = ["msisdn", value_col, "valid_from", "valid_to"]
    if not path.exists():
        logger.warning("build-stg-person: binding not found at %s", path)
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


def _load_previous_ledger(prev_month: date) -> pd.DataFrame | None:
    path = stg_person_id_ledger_output_path(prev_month)
    if not path.exists():
        return None
    return pd.read_parquet(path, columns=["person_id", "person_cluster_key", "node"])


def _read_m2m_tac_set(tac_path: Path) -> set[str]:
    if not tac_path.exists():
        logger.warning("build-stg-person: stg_tac not found, skipping M2M TAC exclusion: %s", tac_path)
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


def _prepare_subscriptions(
    *,
    raw: pd.DataFrame,
    report_month: date,
    month_start: pd.Timestamp,
    month_end: pd.Timestamp,
    imsi_binding: pd.DataFrame,
    imei_binding: pd.DataFrame,
    operator_binding: pd.DataFrame,
    prev_ledger: pd.DataFrame | None,
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
    work["contract_number"] = _norm_str(work.get("contract_number"))
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
    work["person_id"] = assign_person_ids_with_ledger(
        work["person_cluster_key"],
        cluster_to_nodes,
        ledger_nodes=prev_ledger,
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


def _unite_pair_column(uf: UnionFind, frame: pd.DataFrame, left_kind: str, right_kind: str) -> None:
    left_col, right_col = left_kind, right_kind
    if left_col not in frame.columns or right_col not in frame.columns:
        return
    pairs = frame[[left_col, right_col]].dropna().drop_duplicates()
    for left_value, right_value in zip(pairs[left_col], pairs[right_col], strict=True):
        left_node = node(left_kind, left_value)
        right_node = node(right_kind, right_value)
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
    uf = UnionFind()
    work = work.copy()
    work["bio_key"] = _vectorized_bio_key(work)
    _unite_pair_column(uf, work, "msisdn", "imsi")
    _unite_pair_column(uf, work, "msisdn", "imei")
    _unite_pair_column(uf, work, "msisdn", "iccid")
    bio_frame = work.loc[work["bio_key"].notna(), ["msisdn", "bio_key"]].rename(columns={"bio_key": "bio"})
    _unite_pair_column(uf, bio_frame, "msisdn", "bio")
    if "contract_number" in work.columns:
        contract_frame = work.loc[work["contract_number"].notna(), ["msisdn", "contract_number"]].copy()
        contract_frame["contract"] = "contract:" + contract_frame["contract_number"].astype("string")
        _unite_pair_column(uf, contract_frame, "msisdn", "contract")

    for left, right in binding_edges_in_month(
        imsi_binding,
        left_kind="msisdn",
        right_kind="imsi",
        month_start=month_start,
        month_end=month_end,
        open_actually_to=_OPEN_ACTUALLY_TO,
    ):
        uf.union(left, right)
    for left, right in binding_edges_in_month(
        imei_binding,
        left_kind="msisdn",
        right_kind="imei",
        month_start=month_start,
        month_end=month_end,
        open_actually_to=_OPEN_ACTUALLY_TO,
    ):
        uf.union(left, right)
    for left, right in operator_observation_edges(
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
        root_to_canonical[root] = canonical_cluster_key(nodes)
        root_to_nodes[root] = nodes
        canon = root_to_canonical[root]
        canonical_to_nodes[canon] = canonical_to_nodes.get(canon, set()) | nodes

    def roots_for_row(row: pd.Series) -> list[str]:
        nodes = []
        for prefix, col in (("imsi", "imsi"), ("imei", "imei"), ("msisdn", "msisdn")):
            item = node(prefix, row.get(col))
            if item is not None:
                nodes.append(item)
        bio_val = row.get("bio_key")
        if bio_val is not None and not pd.isna(bio_val):
            nodes.append(str(bio_val))
        contract = str_field(row.get("contract_number"))
        if contract:
            nodes.append(f"contract:{contract}")
        iccid_val = row.get("iccid")
        iccid_node = node("iccid", iccid_val)
        if iccid_node is not None:
            nodes.append(iccid_node)
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
        confidences.append(person_confidence_for_nodes(root_to_nodes.get(root, set())))

    out = work.copy()
    out["person_cluster_key"] = cluster_keys
    out["person_confidence"] = confidences
    return out.drop(columns=["bio_key"], errors="ignore"), canonical_to_nodes


def _build_outputs(
    work: pd.DataFrame,
    *,
    cluster_to_nodes: dict[str, set[str]],
    report_month: date,
    field_names: list[str],
    oksm_lookup: OksmLookup,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if work.empty:
        empty_person = pd.DataFrame(columns=field_names)
        empty_sim = pd.DataFrame(columns=[f["name"] for f in STG_PERSON_SIM_FIELDS])
        empty_ledger = pd.DataFrame(columns=["report_date", "person_id", "person_cluster_key", "node"])
        return empty_person, empty_sim, empty_ledger

    month_start = pd.Timestamp(report_month)
    primary_idx = work.sort_values(["person_id", "actually_from"], ascending=[True, False]).groupby(
        "person_id", sort=False
    ).head(1).index

    sim = work.copy()
    sim["report_date"] = pd.Timestamp(report_month).date()
    sim["is_primary"] = sim.index.isin(primary_idx)
    sim_cols = [f["name"] for f in STG_PERSON_SIM_FIELDS]
    sim_out = sim[
        [
            "report_date",
            "person_id",
            "msisdn",
            "imsi",
            "imei",
            "iccid",
            "operator_id",
            "contract_number",
            "actually_from",
            "actually_to",
            "is_primary",
        ]
    ].reset_index(drop=True)

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
    person_out = person_out[field_names].reset_index(drop=True)

    ledger_rows: list[dict[str, str]] = []
    for cluster_key, nodes in cluster_to_nodes.items():
        person_rows = work.loc[work["person_cluster_key"] == cluster_key, "person_id"]
        if person_rows.empty:
            continue
        person_id = str(person_rows.iloc[0])
        for item in sorted(nodes):
            ledger_rows.append(
                {
                    "report_date": report_month.isoformat(),
                    "person_id": person_id,
                    "person_cluster_key": str(cluster_key),
                    "node": item,
                }
            )
    ledger_df = pd.DataFrame(ledger_rows)
    if not ledger_df.empty:
        ledger_df["report_date"] = pd.to_datetime(ledger_df["report_date"]).dt.date

    return person_out, sim_out, ledger_df


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
    from mobile.pipelines.stg.person_identity import str_field

    first_name = str_field(row.get("first_name")).lower()
    second_name = str_field(row.get("second_name")).lower()
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
    from mobile.pipelines.stg.person_identity import str_field

    dept = str_field(dul_department).lower()
    doc = str_field(document).lower()
    name_blob = " ".join(str_field(x).lower() for x in (last_name, first_name, second_name)).strip()
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


# Подстроки в bio → ISO alpha-2; в ``stg_person.citizenship`` — numeric_code из ``stg_oksm``.
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

