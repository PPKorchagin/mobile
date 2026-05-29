"""Общая логика кластеризации персон: union-find, якоря, ledger."""

from __future__ import annotations

import hashlib
import re
from typing import Any

import pandas as pd

from mobile.pipelines.stg.subscriber_ids import normalize_imei, normalize_imsi, normalize_msisdn


def node(prefix: str, value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    return f"{prefix}:{text}"


def str_field(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except TypeError:
        pass
    text = str(value).strip()
    return "" if text.lower() in ("<na>", "nan", "none") else text


def build_bio_fingerprint(row: pd.Series) -> str | None:
    last_name = str_field(row.get("last_name")).casefold()
    first_name = str_field(row.get("first_name")).casefold()
    second_name = str_field(row.get("second_name")).casefold()
    birth_day = pd.to_datetime(row.get("birth_day"), errors="coerce")
    if not last_name or not first_name or pd.isna(birth_day):
        return None
    document_digits = re.sub(r"\D", "", str_field(row.get("document")))
    return f"bio:{last_name}|{first_name}|{second_name}|{birth_day.date().isoformat()}|{document_digits}"


def build_contract_fingerprint(row: pd.Series) -> str | None:
    contract = str_field(row.get("contract_number"))
    if not contract:
        return None
    operator_id = str_field(row.get("operator_id") or row.get("operator_Id"))
    return f"contract:{contract}|op:{operator_id}" if operator_id else f"contract:{contract}"


def build_iccid_node(row: pd.Series) -> str | None:
    return node("iccid", row.get("iccid"))


def identifier_nodes_from_row(row: pd.Series) -> list[str]:
    nodes: list[str] = []
    for prefix, col in (("imsi", "imsi"), ("imei", "imei"), ("msisdn", "msisdn")):
        item = node(prefix, row.get(col))
        if item is not None:
            nodes.append(item)
    for builder in (build_bio_fingerprint, build_contract_fingerprint, build_iccid_node):
        item = builder(row)
        if item is not None:
            nodes.append(item)
    return nodes


class UnionFind:
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


def unite_nodes(uf: UnionFind, nodes: list[str]) -> None:
    if not nodes:
        return
    anchor = nodes[0]
    uf.find(anchor)
    for item in nodes[1:]:
        uf.union(anchor, item)


def canonical_cluster_key(cluster_nodes: set[str]) -> str:
    """Приоритет: bio → contract → iccid → технические id."""
    bios = sorted(n for n in cluster_nodes if n.startswith("bio:"))
    if bios:
        return bios[0]
    contracts = sorted(n for n in cluster_nodes if n.startswith("contract:"))
    if contracts:
        return contracts[0]
    iccids = sorted(n for n in cluster_nodes if n.startswith("iccid:"))
    if iccids:
        return iccids[0]
    return min(cluster_nodes)


def person_confidence_for_nodes(nodes: set[str]) -> str:
    if any(n.startswith("bio:") for n in nodes):
        return "high"
    if any(n.startswith("contract:") for n in nodes):
        return "medium"
    if any(n.startswith("iccid:") for n in nodes):
        return "medium"
    return "low"


def build_person_id_from_cluster_key(cluster_key: str) -> str:
    payload = str(cluster_key).strip()
    if not payload:
        return "prs_unknown"
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]
    return f"prs_{digest}"


def assign_person_ids_with_ledger(
    cluster_keys: pd.Series,
    cluster_to_nodes: dict[str, set[str]],
    *,
    ledger_nodes: pd.DataFrame | None,
) -> pd.Series:
    """Назначить person_id с учётом ledger прошлого месяца (совпадение по узлам)."""
    node_to_person: dict[str, str] = {}
    key_to_person: dict[str, str] = {}
    if ledger_nodes is not None and not ledger_nodes.empty:
        for row in ledger_nodes.itertuples(index=False):
            node_to_person[str(row.node)] = str(row.person_id)
            key_to_person[str(row.person_cluster_key)] = str(row.person_id)

    person_ids: list[str] = []
    for cluster_key in cluster_keys:
        key = str(cluster_key)
        nodes = cluster_to_nodes.get(key, set())
        assigned: str | None = key_to_person.get(key)
        if assigned is None:
            for item in sorted(nodes):
                if item in node_to_person:
                    assigned = node_to_person[item]
                    break
        if assigned is None:
            assigned = build_person_id_from_cluster_key(key)
        key_to_person[key] = assigned
        for item in nodes:
            node_to_person[item] = assigned
        person_ids.append(assigned)
    return pd.Series(person_ids, index=cluster_keys.index, dtype="string")


def to_digit_string_series(series: pd.Series | None) -> pd.Series:
    if series is None:
        return pd.Series(dtype="string")
    num = pd.to_numeric(series, errors="coerce")
    out = num.astype("Int64").astype("string")
    return out.mask(out == "<NA>", pd.NA)


def binding_edges_in_month(
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
        left_node = node(left_kind, left_value)
        right_node = node(right_kind, right_value)
        if left_node is not None and right_node is not None:
            edges.append((left_node, right_node))
    return edges


def operator_observation_edges(
    operator_binding: pd.DataFrame,
    *,
    month_start: pd.Timestamp,
    month_end: pd.Timestamp,
    open_actually_to: pd.Timestamp,
) -> list[tuple[str, str]]:
    """msisdn + imsi на интервале operator (MNP)."""
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
        msisdn_node = node("msisdn", getattr(row, "msisdn", None))
        if msisdn_node is None:
            continue
        imsi_val = getattr(row, "imsi", None)
        if imsi_val is not None and not pd.isna(imsi_val):
            imsi_node = node("imsi", imsi_val)
            if imsi_node is not None:
                edges.append((msisdn_node, imsi_node))
    return edges
