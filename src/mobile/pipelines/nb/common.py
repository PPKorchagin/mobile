"""Утилиты DQ-ноутбуков ``src/mobile/pipelines/nb/``: логи, matplotlib и folium-карты STG."""

from __future__ import annotations

import json
import os
from datetime import date
from pathlib import Path
from typing import Any

import folium
import matplotlib.pyplot as plt
import pandas as pd
from branca.colormap import StepColormap
from IPython.display import HTML, display
from shapely import wkt

from folium.plugins import FastMarkerCluster
from mobile.cli_defaults import DEFAULT_SRC_END_DATE
from mobile.project_paths import (
    DEFAULT_BS_LAYOUT,
    DEFAULT_STG_GEO_ALL_OUTPUT_ROOT,
    DEFAULT_STG_OKSM_OUTPUT_PATH,
    stg_bs_output_path,
    DEFAULT_STG_OKTMO_OUTPUT_PATH,
    DEFAULT_STG_TAC_OUTPUT_PATH,
    DEFAULT_STG_TIME_ZONES_OUTPUT_PATH,
    stg_geo_all_output_path,
    stg_msisdn_imei_output_path,
    stg_msisdn_imsi_output_path,
)

_DQ_META_KEYS = frozenset({"tag", "check", "log_ts", "log_level", "status", "mart", "metrics"})


def normalize_dq_payload(payload: dict[str, Any]) -> dict[str, Any]:
    out = dict(payload)
    raw_metrics = out.get("metrics")
    if not isinstance(raw_metrics, dict):
        flat = {k: v for k, v in out.items() if k not in _DQ_META_KEYS}
        out["metrics"] = flat
    if "status" not in out:
        level = str(out.get("log_level") or "").lower()
        if level == "error":
            out["status"] = "failed"
        elif level == "warning":
            out["status"] = "warning"
        else:
            out["status"] = "ok"
    return out


def parse_log_payload(line: str, *, tag: str) -> dict[str, Any] | None:
    idx = line.find("{")
    if idx < 0:
        return None
    try:
        payload = json.loads(line[idx:])
    except json.JSONDecodeError:
        return None
    if payload.get("tag") != tag:
        return None
    parts = line.split(" | ", 3)
    if len(parts) >= 4:
        payload["log_ts"] = parts[0].strip()
        payload["log_level"] = parts[1].strip()
    else:
        payload["log_ts"] = None
        payload["log_level"] = None
    return payload


def load_dq_logs(log_path: Path, *, tag: str) -> pd.DataFrame:
    if not log_path.exists():
        raise FileNotFoundError(f"Log file not found: {log_path}")
    records: list[dict[str, Any]] = []
    with log_path.open("r", encoding="utf-8") as file:
        for raw in file:
            payload = parse_log_payload(raw.strip(), tag=tag)
            if payload is not None:
                records.append(normalize_dq_payload(payload))
    if not records:
        raise ValueError(f"No {tag} records in {log_path}")
    return pd.DataFrame(records)


def assign_run_ids(checks: pd.Series, boundary_checks: str | list[str]) -> list[int]:
    boundaries = {boundary_checks} if isinstance(boundary_checks, str) else set(boundary_checks)
    run_id = 0
    out: list[int] = []
    for check in checks.astype("string").tolist():
        if check in boundaries:
            run_id += 1
        out.append(run_id)
    return out


def attach_run_ids(dq_logs: pd.DataFrame, boundary_checks: str | list[str]) -> pd.DataFrame:
    out = dq_logs.copy()
    out["run_id"] = assign_run_ids(out["check"], boundary_checks)
    return out


def latest_run_slice(dq_logs: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    latest_run_id = int(dq_logs["run_id"].max())
    latest = dq_logs[dq_logs["run_id"] == latest_run_id].copy()
    return latest, latest_run_id


def run_meta(latest: pd.DataFrame, *, tag: str, latest_run_id: int) -> dict[str, Any]:
    summary_row = latest[latest["check"] == "summary"]
    m: dict[str, Any] = {}
    if not summary_row.empty:
        raw = summary_row.iloc[-1].get("metrics")
        if isinstance(raw, dict):
            m = dict(raw)
    log_ts = latest["log_ts"].dropna()
    return {
        "tag": tag,
        "run_id": latest_run_id,
        "log_ts_start": str(log_ts.min()) if len(log_ts) else None,
        "log_ts_end": str(log_ts.max()) if len(log_ts) else None,
        "total_checks": m.get("total_checks"),
        "warning_checks": m.get("warning_checks"),
        "failed_checks": m.get("failed_checks"),
        "record_count": int(len(latest)),
    }


def load_dq_dashboard(
    root: Path,
    *,
    tag: str,
    boundary_check: str | list[str],
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    log_path = root / "data" / "logs" / "mobile.log"
    dq_logs = load_dq_logs(log_path, tag=tag)
    dq_logs = attach_run_ids(dq_logs, boundary_check)
    latest, run_id = latest_run_slice(dq_logs)
    meta = run_meta(latest, tag=tag, latest_run_id=run_id)
    return dq_logs, latest, meta


def metrics_wide_table(latest: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for _, r in latest.iterrows():
        ck = str(r["check"])
        m = r.get("metrics")
        status = r.get("status")
        if not isinstance(m, dict):
            rows.append({"check": ck, "status": status, "metric": "(empty)", "value": None})
            continue
        for key, val in m.items():
            if isinstance(val, (dict, list)):
                rows.append(
                    {
                        "check": ck,
                        "status": status,
                        "metric": key,
                        "value": json.dumps(val, ensure_ascii=False)[:500],
                    }
                )
            else:
                rows.append({"check": ck, "status": status, "metric": key, "value": val})
    return pd.DataFrame(rows)


def checks_by_status(latest: pd.DataFrame) -> pd.DataFrame:
    sub = latest[latest["check"] != "summary"].copy()
    if sub.empty or "status" not in sub.columns:
        return pd.DataFrame(columns=["status", "count"])
    return (
        sub.groupby("status", as_index=False)
        .agg(count=("check", "count"))
        .sort_values("count", ascending=False)
    )


def failed_warning_table(latest: pd.DataFrame) -> pd.DataFrame:
    sub = latest[(latest["check"] != "summary") & (latest["status"].isin(["failed", "warning"]))].copy()
    cols = [c for c in ("log_ts", "check", "status", "metrics") if c in sub.columns]
    return sub[cols].sort_values(["status", "check"], ascending=[True, True])


def _metrics_for_check(latest: pd.DataFrame, check: str) -> dict[str, Any]:
    rows = latest.loc[latest["check"] == check]
    if rows.empty:
        return {}
    metrics = rows.iloc[-1].get("metrics")
    return dict(metrics) if isinstance(metrics, dict) else {}


def null_ratio_frame(latest: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for _, record in latest.iterrows():
        check = str(record["check"])
        if not check.startswith("nulls."):
            continue
        metrics = record.get("metrics")
        if not isinstance(metrics, dict):
            continue
        rows.append(
            {
                "field": check.removeprefix("nulls."),
                "null_count": int(metrics.get("null_count") or 0),
                "null_ratio": float(metrics.get("null_ratio") or 0),
            }
        )
    return pd.DataFrame(rows)


def level_distribution_frame(latest: pd.DataFrame) -> pd.DataFrame:
    metrics = _metrics_for_check(latest, "level_distribution")
    level_counts = metrics.get("level_counts")
    if not isinstance(level_counts, dict):
        return pd.DataFrame(columns=["level", "count"])
    return pd.DataFrame(
        [{"level": f"level {key}", "count": int(value)} for key, value in level_counts.items()]
    ).sort_values("level")


def wkt_quality_frame(latest: pd.DataFrame) -> pd.DataFrame:
    metrics = _metrics_for_check(latest, "wkt_geometry")
    if not metrics:
        return pd.DataFrame(columns=["metric", "count"])
    labels = {
        "valid_geometry_count": "valid",
        "parse_error_count": "parse errors",
        "invalid_topology_count": "invalid topology",
        "empty_geometry_count": "empty",
        "unsupported_geom_type_count": "unsupported type",
    }
    rows = [
        {"metric": labels[key], "count": int(metrics[key])}
        for key in labels
        if key in metrics
    ]
    return pd.DataFrame(rows)


def integrity_quality_frame(latest: pd.DataFrame) -> pd.DataFrame:
    specs: tuple[tuple[str, str, str], ...] = (
        ("code_quality", "duplicate_code_count", "duplicate codes"),
        ("code_quality", "non_numeric_code_count", "non-numeric codes"),
        ("parent_code_quality", "non_numeric_parent_code_count", "non-numeric parent_code"),
        ("hierarchy_integrity", "children_without_parent_count", "orphan children"),
        ("hierarchy_integrity", "parents_without_children_count", "parents w/o children"),
        ("hierarchy_integrity", "level1_with_parent_count", "level=1 with parent"),
        ("hierarchy_integrity", "level2_without_parent_count", "level=2 without parent"),
        ("name_quality", "invalid_name_count", "invalid names"),
        ("level_distribution", "invalid_level_count", "invalid levels"),
    )
    rows: list[dict[str, Any]] = []
    for check, key, label in specs:
        value = _metrics_for_check(latest, check).get(key)
        if value is None:
            continue
        rows.append({"metric": label, "count": int(value)})
    return pd.DataFrame(rows)


_STATUS_COLORS = {"ok": "#2ca02c", "warning": "#ff7f0e", "failed": "#d62728"}


def plot_check_status(latest: pd.DataFrame, *, ax: plt.Axes | None = None) -> plt.Figure:
    df = checks_by_status(latest)
    if ax is None:
        fig, ax = plt.subplots(figsize=(6, 3.5))
    else:
        fig = ax.figure
    if df.empty:
        ax.set_title("Checks по статусу — нет данных")
        ax.axis("off")
        return fig
    order = [status for status in ("failed", "warning", "ok") if status in df["status"].values]
    df = df.set_index("status").reindex(order).reset_index()
    ax.bar(
        df["status"],
        df["count"],
        color=[_STATUS_COLORS.get(str(status), "#999") for status in df["status"]],
    )
    ax.set_ylabel("checks")
    ax.set_title("DQ checks по статусу")
    for index, value in enumerate(df["count"]):
        ax.text(index, value, str(int(value)), ha="center", va="bottom", fontsize=9)
    fig.tight_layout()
    return fig


def plot_summary_metrics(latest: pd.DataFrame, *, ax: plt.Axes | None = None) -> plt.Figure:
    if ax is None:
        fig, ax = plt.subplots(figsize=(5, 3.5))
    else:
        fig = ax.figure
    metrics = _metrics_for_check(latest, "summary")
    if not metrics:
        ax.set_title("summary — нет в логе")
        ax.axis("off")
        return fig
    labels = ["total_checks", "warning_checks", "failed_checks"]
    values = [int(metrics.get(label) or 0) for label in labels]
    ax.bar(labels, values, color=["#bcbd22", "#ff7f0e", "#d62728"], alpha=0.9)
    ax.set_ylabel("count")
    ax.set_title("Итог прогона (summary)")
    plt.setp(ax.get_xticklabels(), rotation=15, ha="right")
    for index, value in enumerate(values):
        ax.text(index, value, str(value), ha="center", va="bottom", fontsize=9)
    fig.tight_layout()
    return fig


def plot_null_ratios(nulls: pd.DataFrame, *, ax: plt.Axes | None = None) -> plt.Figure:
    if ax is None:
        fig, ax = plt.subplots(figsize=(8, 4))
    else:
        fig = ax.figure
    if nulls.empty:
        ax.set_title("null_ratio — нет данных")
        ax.axis("off")
        return fig
    work = nulls.sort_values("null_ratio", ascending=True)
    ax.barh(work["field"], work["null_ratio"] * 100, color="#ff7f0e", alpha=0.85)
    ax.set_xlabel("null_ratio, %")
    ax.set_title("Доля null по полям")
    fig.tight_layout()
    return fig


def plot_level_distribution(levels: pd.DataFrame, *, ax: plt.Axes | None = None) -> plt.Figure:
    if ax is None:
        fig, ax = plt.subplots(figsize=(6, 4))
    else:
        fig = ax.figure
    if levels.empty:
        ax.set_title("level_distribution — нет данных")
        ax.axis("off")
        return fig
    ax.bar(levels["level"], levels["count"], color="#2563eb", alpha=0.88)
    ax.set_ylabel("polygons")
    ax.set_title("Распределение по level")
    for index, value in enumerate(levels["count"]):
        ax.text(index, value, f"{int(value):,}", ha="center", va="bottom", fontsize=9)
    fig.tight_layout()
    return fig


def plot_count_bars(
    counts: pd.DataFrame,
    *,
    title: str,
    ax: plt.Axes | None = None,
    color: str = "#9467bd",
) -> plt.Figure:
    if ax is None:
        fig, ax = plt.subplots(figsize=(7, 4))
    else:
        fig = ax.figure
    if counts.empty:
        ax.set_title(f"{title}\n(нет данных)")
        ax.axis("off")
        return fig
    work = counts.sort_values("count", ascending=True)
    ax.barh(work["metric"], work["count"], color=color, alpha=0.88)
    ax.set_xlabel("count")
    ax.set_title(title)
    fig.tight_layout()
    return fig


def render_stg_oktmo_dq_overview(latest: pd.DataFrame) -> plt.Figure:
    basic = _metrics_for_check(latest, "dataset_basic")
    fig, axes = plt.subplots(2, 3, figsize=(16, 9))
    plot_check_status(latest, ax=axes[0, 0])
    plot_summary_metrics(latest, ax=axes[0, 1])
    plot_null_ratios(null_ratio_frame(latest), ax=axes[0, 2])
    plot_level_distribution(level_distribution_frame(latest), ax=axes[1, 0])
    plot_count_bars(wkt_quality_frame(latest), title="WKT geometry", ax=axes[1, 1], color="#17becf")
    plot_count_bars(
        integrity_quality_frame(latest),
        title="Качество кодов и иерархии",
        ax=axes[1, 2],
        color="#8c564b",
    )
    if basic:
        fig.suptitle(
            f"DQ STG OKTMO — rows={int(basic.get('row_count') or 0):,}, "
            f"columns={int(basic.get('column_count') or 0)}",
            fontsize=13,
            y=1.02,
        )
    else:
        fig.suptitle("DQ STG OKTMO — обзор метрик", fontsize=13, y=1.02)
    fig.tight_layout()
    return fig


def timezone_distribution_frame(latest: pd.DataFrame) -> pd.DataFrame:
    metrics = _metrics_for_check(latest, "timezone_range")
    distribution = metrics.get("distribution")
    if not isinstance(distribution, dict):
        return pd.DataFrame(columns=["timezone", "pct"])
    return pd.DataFrame(
        [{"timezone": f"UTC+{key}", "pct": float(value)} for key, value in distribution.items()]
    ).sort_values("pct", ascending=False)


def geometry_quality_frame(latest: pd.DataFrame) -> pd.DataFrame:
    metrics = _metrics_for_check(latest, "geometry_quality")
    if not metrics:
        return pd.DataFrame(columns=["metric", "count"])
    labels = {
        "valid_geometry_count": "valid",
        "parse_error_count": "parse errors",
        "invalid_topology_count": "invalid topology",
        "empty_geometry_count": "empty",
        "unsupported_geom_type_count": "unsupported type",
    }
    return pd.DataFrame(
        [{"metric": labels[key], "count": int(metrics[key])} for key in labels if key in metrics]
    )


def time_zones_code_quality_frame(latest: pd.DataFrame) -> pd.DataFrame:
    metrics = _metrics_for_check(latest, "code_quality")
    if not metrics:
        return pd.DataFrame(columns=["metric", "count"])
    rows: list[dict[str, Any]] = []
    for key, label in (
        ("duplicate_code_count", "duplicate codes"),
        ("invalid_code_count", "invalid codes"),
    ):
        if key in metrics:
            rows.append({"metric": label, "count": int(metrics[key])})
    timezone_metrics = _metrics_for_check(latest, "timezone_range")
    if "invalid_timezone_count" in timezone_metrics:
        rows.append(
            {
                "metric": "invalid timezone",
                "count": int(timezone_metrics["invalid_timezone_count"]),
            }
        )
    return pd.DataFrame(rows)


def plot_timezone_distribution(dist: pd.DataFrame, *, ax: plt.Axes | None = None) -> plt.Figure:
    if ax is None:
        fig, ax = plt.subplots(figsize=(7, 4))
    else:
        fig = ax.figure
    if dist.empty:
        ax.set_title("timezone_range — нет данных")
        ax.axis("off")
        return fig
    work = dist.sort_values("pct", ascending=True).tail(15)
    ax.barh(work["timezone"], work["pct"], color="#2563eb", alpha=0.88)
    ax.set_xlabel("доля, %")
    ax.set_title("Распределение UTC offset")
    fig.tight_layout()
    return fig


def render_stg_time_zones_dq_overview(latest: pd.DataFrame) -> plt.Figure:
    basic = _metrics_for_check(latest, "dataset_basic")
    fig, axes = plt.subplots(2, 3, figsize=(16, 9))
    plot_check_status(latest, ax=axes[0, 0])
    plot_summary_metrics(latest, ax=axes[0, 1])
    plot_null_ratios(null_ratio_frame(latest), ax=axes[0, 2])
    plot_timezone_distribution(timezone_distribution_frame(latest), ax=axes[1, 0])
    plot_count_bars(
        geometry_quality_frame(latest),
        title="Geometry quality",
        ax=axes[1, 1],
        color="#17becf",
    )
    plot_count_bars(
        time_zones_code_quality_frame(latest),
        title="Качество code / timezone",
        ax=axes[1, 2],
        color="#8c564b",
    )
    if basic:
        fig.suptitle(
            f"DQ STG TIME ZONES — rows={int(basic.get('row_count') or 0):,}, "
            f"columns={int(basic.get('column_count') or 0)}",
            fontsize=13,
            y=1.02,
        )
    else:
        fig.suptitle("DQ STG TIME ZONES — обзор метрик", fontsize=13, y=1.02)
    fig.tight_layout()
    return fig


def m2m_coverage_frame(latest: pd.DataFrame) -> pd.DataFrame:
    metrics = _metrics_for_check(latest, "m2m_coverage")
    if not metrics:
        return pd.DataFrame(columns=["segment", "count"])
    return pd.DataFrame(
        [
            {"segment": "M2M", "count": int(metrics.get("m2m_row_count") or 0)},
            {"segment": "non-M2M", "count": int(metrics.get("non_m2m_row_count") or 0)},
        ]
    )


def equipment_type_distribution_frame(latest: pd.DataFrame, *, top_n: int = 12) -> pd.DataFrame:
    metrics = _metrics_for_check(latest, "m2m_equipment_type_consistency")
    counts = metrics.get("equipment_type_counts")
    if not isinstance(counts, dict):
        return pd.DataFrame(columns=["equipment_type", "count"])
    rows = [{"equipment_type": str(key), "count": int(value)} for key, value in counts.items()]
    return pd.DataFrame(rows).sort_values("count", ascending=False).head(top_n)


def tac_integrity_frame(latest: pd.DataFrame) -> pd.DataFrame:
    metrics = _metrics_for_check(latest, "tac_integrity")
    if not metrics:
        return pd.DataFrame(columns=["metric", "count"])
    rows: list[dict[str, Any]] = []
    for key, label in (
        ("invalid_tac_count", "invalid TAC"),
        ("duplicate_tac_count", "duplicate TAC"),
    ):
        if key in metrics:
            rows.append({"metric": label, "count": int(metrics[key])})
    return pd.DataFrame(rows)


def tac_quality_frame(latest: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    date_metrics = _metrics_for_check(latest, "allocation_date_format")
    if "invalid_date_count" in date_metrics:
        rows.append({"metric": "invalid allocation_date", "count": int(date_metrics["invalid_date_count"])})
    man_metrics = _metrics_for_check(latest, "manufacturer_quality")
    if "empty_manufacturer_count" in man_metrics:
        rows.append({"metric": "empty manufacturer", "count": int(man_metrics["empty_manufacturer_count"])})
    m2m_metrics = _metrics_for_check(latest, "m2m_equipment_type_consistency")
    if "mismatch_count" in m2m_metrics:
        rows.append({"metric": "is_m2m mismatch", "count": int(m2m_metrics["mismatch_count"])})
    return pd.DataFrame(rows)


def plot_m2m_coverage(coverage: pd.DataFrame, *, ax: plt.Axes | None = None) -> plt.Figure:
    if ax is None:
        fig, ax = plt.subplots(figsize=(6, 4))
    else:
        fig = ax.figure
    if coverage.empty:
        ax.set_title("m2m_coverage — нет данных")
        ax.axis("off")
        return fig
    colors = {"M2M": "#7c3aed", "non-M2M": "#94a3b8"}
    ax.bar(
        coverage["segment"],
        coverage["count"],
        color=[colors.get(str(segment), "#64748b") for segment in coverage["segment"]],
        alpha=0.88,
    )
    ax.set_ylabel("rows")
    ax.set_title("M2M vs non-M2M")
    for index, value in enumerate(coverage["count"]):
        ax.text(index, value, f"{int(value):,}", ha="center", va="bottom", fontsize=9)
    fig.tight_layout()
    return fig


def plot_equipment_type_distribution(dist: pd.DataFrame, *, ax: plt.Axes | None = None) -> plt.Figure:
    if ax is None:
        fig, ax = plt.subplots(figsize=(8, 4))
    else:
        fig = ax.figure
    if dist.empty:
        ax.set_title("equipment_type — нет данных")
        ax.axis("off")
        return fig
    work = dist.sort_values("count", ascending=True)
    ax.barh(work["equipment_type"].astype(str), work["count"], color="#059669", alpha=0.88)
    ax.set_xlabel("rows")
    ax.set_title("Top equipment_type (DQ log)")
    fig.tight_layout()
    return fig


def render_stg_tac_dq_overview(latest: pd.DataFrame) -> plt.Figure:
    basic = _metrics_for_check(latest, "dataset_basic")
    fig, axes = plt.subplots(2, 3, figsize=(16, 9))
    plot_check_status(latest, ax=axes[0, 0])
    plot_summary_metrics(latest, ax=axes[0, 1])
    plot_null_ratios(null_ratio_frame(latest), ax=axes[0, 2])
    plot_m2m_coverage(m2m_coverage_frame(latest), ax=axes[1, 0])
    plot_equipment_type_distribution(equipment_type_distribution_frame(latest), ax=axes[1, 1])
    integrity = tac_integrity_frame(latest)
    plot_count_bars(
        integrity if not integrity.empty else tac_quality_frame(latest),
        title="TAC integrity / quality",
        ax=axes[1, 2],
        color="#8c564b",
    )
    if basic:
        fig.suptitle(
            f"DQ STG TAC — rows={int(basic.get('row_count') or 0):,}, "
            f"columns={int(basic.get('column_count') or 0)}",
            fontsize=13,
            y=1.02,
        )
    else:
        fig.suptitle("DQ STG TAC — обзор метрик", fontsize=13, y=1.02)
    fig.tight_layout()
    return fig


def display_tac_parquet_summary(root: Path) -> None:
    tac_parquet = _resolve_parquet(root, DEFAULT_STG_TAC_OUTPUT_PATH)
    if not tac_parquet.exists():
        raise FileNotFoundError(f"Нет parquet: {tac_parquet}")
    df = pd.read_parquet(tac_parquet)
    try:
        rel = tac_parquet.relative_to(root)
    except ValueError:
        rel = tac_parquet
    print(f"stg_tac rows: {len(df):,} | файл: {rel}")
    if "is_m2m" in df.columns:
        display(df.groupby("is_m2m", dropna=False).size().reset_index(name="rows"))
    if "equipment_type" in df.columns:
        display(df["equipment_type"].value_counts().head(15).to_frame("rows"))


def oksm_code_integrity_frame(latest: pd.DataFrame) -> pd.DataFrame:
    metrics = _metrics_for_check(latest, "numeric_code_integrity")
    if not metrics:
        return pd.DataFrame(columns=["metric", "count"])
    rows: list[dict[str, Any]] = []
    for key, label in (
        ("invalid_numeric_code_count", "invalid numeric_code"),
        ("duplicate_numeric_code_count", "duplicate numeric_code"),
    ):
        if key in metrics:
            rows.append({"metric": label, "count": int(metrics[key])})
    return pd.DataFrame(rows)


def oksm_alpha_integrity_frame(latest: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for check, prefix in (("alpha2_integrity", "alpha2"), ("alpha3_integrity", "alpha3")):
        metrics = _metrics_for_check(latest, check)
        for key, label in (
            (f"invalid_{prefix}_count", f"invalid {prefix}"),
            (f"duplicate_{prefix}_count", f"duplicate {prefix}"),
        ):
            if key in metrics:
                rows.append({"metric": label, "count": int(metrics[key])})
    return pd.DataFrame(rows)


def oksm_key_quality_frame(latest: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    name_metrics = _metrics_for_check(latest, "name_quality")
    for key, label in (
        ("empty_name_short_count", "empty name_short"),
        ("empty_name_full_count", "empty name_full"),
    ):
        if key in name_metrics:
            rows.append({"metric": label, "count": int(name_metrics[key])})
    auto_metrics = _metrics_for_check(latest, "autokey_integrity")
    for key, label in (
        ("duplicate_autokey_count", "duplicate autokey"),
        ("empty_autokey_count", "empty autokey"),
    ):
        if key in auto_metrics:
            rows.append({"metric": label, "count": int(auto_metrics[key])})
    return pd.DataFrame(rows)


def plot_russia_presence(latest: pd.DataFrame, *, ax: plt.Axes | None = None) -> plt.Figure:
    if ax is None:
        fig, ax = plt.subplots(figsize=(6, 4))
    else:
        fig = ax.figure
    metrics = _metrics_for_check(latest, "russia_presence")
    if not metrics:
        ax.set_title("russia_presence — нет данных")
        ax.axis("off")
        return fig
    present = bool(metrics.get("has_numeric_code_643"))
    labels = ["RU (643) missing", "RU (643) present"]
    values = [0 if present else 1, 1 if present else 0]
    colors = ["#ef4444", "#16a34a"]
    ax.bar(labels, values, color=colors, alpha=0.88)
    ax.set_ylim(0, 1.2)
    ax.set_ylabel("flag")
    ax.set_title("Наличие записи RU (643)")
    for index, value in enumerate(values):
        ax.text(index, value, "yes" if value else "no", ha="center", va="bottom", fontsize=10)
    fig.tight_layout()
    return fig


def render_stg_oksm_dq_overview(latest: pd.DataFrame) -> plt.Figure:
    basic = _metrics_for_check(latest, "dataset_basic")
    fig, axes = plt.subplots(2, 3, figsize=(16, 9))
    plot_check_status(latest, ax=axes[0, 0])
    plot_summary_metrics(latest, ax=axes[0, 1])
    plot_null_ratios(null_ratio_frame(latest), ax=axes[0, 2])
    plot_count_bars(
        oksm_code_integrity_frame(latest),
        title="numeric_code integrity",
        ax=axes[1, 0],
        color="#2563eb",
    )
    plot_count_bars(
        oksm_alpha_integrity_frame(latest),
        title="alpha2 / alpha3 integrity",
        ax=axes[1, 1],
        color="#059669",
    )
    quality = oksm_key_quality_frame(latest)
    if quality.empty:
        plot_russia_presence(latest, ax=axes[1, 2])
    else:
        plot_count_bars(
            quality,
            title="name / autokey quality",
            ax=axes[1, 2],
            color="#8c564b",
        )
    if basic:
        pair_metrics = _metrics_for_check(latest, "alpha_pair_cardinality")
        pairs = int(pair_metrics.get("distinct_alpha2_alpha3_pairs") or 0)
        fig.suptitle(
            f"DQ STG OKSM — rows={int(basic.get('row_count') or 0):,}, "
            f"alpha pairs={pairs:,}",
            fontsize=13,
            y=1.02,
        )
    else:
        fig.suptitle("DQ STG OKSM — обзор метрик", fontsize=13, y=1.02)
    fig.tight_layout()
    return fig


def display_oksm_parquet_summary(root: Path) -> None:
    oksm_parquet = _resolve_parquet(root, DEFAULT_STG_OKSM_OUTPUT_PATH)
    if not oksm_parquet.exists():
        raise FileNotFoundError(f"Нет parquet: {oksm_parquet}")
    df = pd.read_parquet(oksm_parquet)
    try:
        rel = oksm_parquet.relative_to(root)
    except ValueError:
        rel = oksm_parquet
    print(f"stg_oksm rows: {len(df):,} | файл: {rel}")
    cols = [col for col in ("numeric_code", "name_short", "alpha2", "alpha3") if col in df.columns]
    if cols:
        display(df[cols].head(20))
    if "alpha2" in df.columns:
        print("\n--- sample alpha2 ---")
        display(df["alpha2"].value_counts().head(15).to_frame("rows"))


def distribution_counts_frame(latest: pd.DataFrame, check: str) -> pd.DataFrame:
    metrics = _metrics_for_check(latest, check)
    counts = metrics.get("distribution_counts")
    if not isinstance(counts, dict):
        return pd.DataFrame(columns=["metric", "count"])
    rows = [{"metric": str(key), "count": int(value)} for key, value in counts.items()]
    return pd.DataFrame(rows).sort_values("count", ascending=False)


def null_ratio_key_fields_frame(latest: pd.DataFrame, fields: tuple[str, ...]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for field in fields:
        metrics = _metrics_for_check(latest, f"nulls.{field}")
        if not metrics:
            continue
        rows.append(
            {
                "field": field,
                "null_count": int(metrics.get("null_count") or 0),
                "null_ratio": float(metrics.get("null_ratio") or 0),
            }
        )
    return pd.DataFrame(rows)


def radio_profile_p50_frame(
    latest: pd.DataFrame,
    fields: tuple[str, ...] = ("power", "height", "frequency", "tilt", "amplification"),
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for field in fields:
        metrics = _metrics_for_check(latest, f"radio.profile.{field}")
        if not metrics or metrics.get("p50") is None:
            continue
        rows.append({"metric": field, "count": float(metrics["p50"])})
    return pd.DataFrame(rows)


def temporal_date_off_tail_frame(latest: pd.DataFrame) -> pd.DataFrame:
    metrics = _metrics_for_check(latest, "temporal_date_off_tail")
    if not metrics:
        return pd.DataFrame(columns=["metric", "count"])
    rows: list[dict[str, Any]] = []
    for key, label in (
        ("rows_at_max", "at max date_off"),
        ("rows_below_max", "below max date_off"),
    ):
        if key in metrics:
            rows.append({"metric": label, "count": int(metrics[key])})
    return pd.DataFrame(rows)


def plot_metric_values(values: pd.DataFrame, *, title: str, ax: plt.Axes | None = None, color: str = "#2563eb") -> plt.Figure:
    if ax is None:
        fig, ax = plt.subplots(figsize=(7, 4))
    else:
        fig = ax.figure
    if values.empty:
        ax.set_title(f"{title}\n(нет данных)")
        ax.axis("off")
        return fig
    work = values.sort_values("count", ascending=True)
    ax.barh(work["metric"].astype(str), work["count"], color=color, alpha=0.88)
    ax.set_xlabel("value")
    ax.set_title(title)
    fig.tight_layout()
    return fig


def render_src_bs_dq_overview(latest: pd.DataFrame) -> plt.Figure:
    basic = _metrics_for_check(latest, "dataset_basic")
    fig, axes = plt.subplots(2, 3, figsize=(16, 9))
    plot_check_status(latest, ax=axes[0, 0])
    plot_summary_metrics(latest, ax=axes[0, 1])
    plot_null_ratios(
        null_ratio_key_fields_frame(
            latest,
            ("generation", "power", "height", "coord_x", "coord_y", "mnc", "azimuth"),
        ),
        ax=axes[0, 2],
    )
    plot_count_bars(
        distribution_counts_frame(latest, "distribution.generation"),
        title="generation (DQ log)",
        ax=axes[1, 0],
        color="#7c3aed",
    )
    plot_metric_values(
        radio_profile_p50_frame(latest),
        title="radio.profile p50",
        ax=axes[1, 1],
        color="#059669",
    )
    plot_count_bars(
        temporal_date_off_tail_frame(latest),
        title="temporal_date_off_tail",
        ax=axes[1, 2],
        color="#8c564b",
    )
    if basic:
        tail = _metrics_for_check(latest, "temporal_date_off_tail")
        date_off_max = tail.get("date_off_max")
        fig.suptitle(
            f"DQ SRC BS — rows={int(basic.get('row_count') or 0):,}, "
            f"date_off_max={date_off_max}",
            fontsize=13,
            y=1.02,
        )
    else:
        fig.suptitle("DQ SRC BS — обзор метрик", fontsize=13, y=1.02)
    fig.tight_layout()
    return fig


def display_src_bs_parquet_summary(root: Path) -> None:
    bs_parquet = _resolve_parquet(root, DEFAULT_BS_LAYOUT)
    if not bs_parquet.exists():
        raise FileNotFoundError(f"Нет parquet: {bs_parquet}")
    df = pd.read_parquet(bs_parquet)
    try:
        rel = bs_parquet.relative_to(root)
    except ValueError:
        rel = bs_parquet
    print(f"src_bs rows: {len(df):,} | файл: {rel}")
    if "generation" in df.columns:
        display(df["generation"].value_counts().head(12).to_frame("rows"))
    if "subject" in df.columns:
        print("\n--- top subjects ---")
        display(df["subject"].value_counts().head(10).to_frame("rows"))


def display_folium_map(m: folium.Map) -> None:
    """Показать folium в ячейке ноутбука (без сохранения HTML на диск)."""
    try:
        display(m)
    except Exception:
        display(HTML(m._repr_html_()))


def render_src_bs_folium_map(root: Path) -> folium.Map:
    """Карта ``src_bs``: кластер точек, слои по generation и контуры ОКТМО level=1."""
    bs_parquet = _resolve_parquet(root, DEFAULT_BS_LAYOUT)
    oktmo_parquet = _resolve_parquet(root, DEFAULT_STG_OKTMO_OUTPUT_PATH)
    if not bs_parquet.exists():
        raise FileNotFoundError(f"Нет parquet: {bs_parquet}")

    bs = pd.read_parquet(bs_parquet)
    required = ["coord_x", "coord_y", "mcc", "mnc", "lac", "cell", "generation", "subject"]
    missing = [col for col in required if col not in bs.columns]
    if missing:
        raise ValueError(f"В src_bs нет ожидаемых колонок: {missing}")

    pts = bs.loc[bs["coord_x"].notna() & bs["coord_y"].notna()].copy()
    pts = pts[pts["coord_x"].between(-180, 180) & pts["coord_y"].between(-90, 90)].copy()
    pts["generation"] = pts["generation"].astype("string").fillna("unknown")
    pts["subject"] = pts["subject"].astype("string").fillna("unknown")

    try:
        rel = bs_parquet.relative_to(root)
    except ValueError:
        rel = bs_parquet
    print(f"src_bs rows: {len(bs):,} | файл: {rel}")
    print(f"valid points: {len(pts):,}")
    if pts.empty:
        raise ValueError("Нет валидных coord_x/coord_y для карты src_bs")

    display(
        pts.groupby("generation", as_index=False)
        .agg(rows=("lac", "count"))
        .sort_values("rows", ascending=False)
    )

    center_lat = float(pts["coord_y"].mean())
    center_lon = float(pts["coord_x"].mean())
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=5,
        tiles="CartoDB positron",
        width="100%",
        height="100%",
    )

    all_points = pts[["coord_y", "coord_x"]].astype(float).values.tolist()
    fg_all = folium.FeatureGroup(name=f"Все БС (FastMarkerCluster): {len(all_points):,}", show=True)
    FastMarkerCluster(data=all_points).add_to(fg_all)
    fg_all.add_to(m)

    gen_colors = {
        "2G": "#1f77b4",
        "3G": "#2ca02c",
        "4G": "#ff7f0e",
        "LTE": "#e377c2",
        "5G": "#d62728",
        "unknown": "#6b7280",
    }
    detail_per_gen_max = 2500
    for gen, group in pts.groupby("generation", dropna=False):
        gen_name = str(gen)
        color = gen_colors.get(gen_name, "#9467bd")
        sample = group if len(group) <= detail_per_gen_max else group.sample(detail_per_gen_max, random_state=42)
        fg_gen = folium.FeatureGroup(
            name=f"generation={gen_name}: {len(sample):,}/{len(group):,}",
            show=False,
        )
        for row in sample.itertuples(index=False):
            tip = (
                f"<b>{row.mcc}-{row.mnc}-{row.lac}-{row.cell}</b><br>"
                f"subject={row.subject}<br>gen={gen_name}"
            )
            folium.CircleMarker(
                location=[float(row.coord_y), float(row.coord_x)],
                radius=2,
                color=color,
                weight=1,
                fill=True,
                fill_opacity=0.7,
                tooltip=folium.Tooltip(tip, sticky=False),
            ).add_to(fg_gen)
        fg_gen.add_to(m)

    subjects = sorted(pts["subject"].dropna().unique().tolist())
    if subjects and oktmo_parquet.exists():
        oktmo_df = pd.read_parquet(oktmo_parquet)
        if {"level", "name", "WKT"}.issubset(oktmo_df.columns):
            oktmo_l1 = oktmo_df.loc[(oktmo_df["level"] == 1) & (oktmo_df["name"].isin(subjects))].copy()
            if not oktmo_l1.empty:
                oktmo_style = {"color": "#b45309", "weight": 2, "fillColor": "#b45309", "fillOpacity": 0.0}
                fg_oktmo = folium.FeatureGroup(
                    name=f"ОКТМО level=1 (субъекты в src_bs): {len(oktmo_l1):,}",
                    show=True,
                )
                bad = 0
                for row in oktmo_l1.itertuples(index=False):
                    try:
                        geom = wkt.loads(str(row.WKT))
                    except Exception:
                        bad += 1
                        continue
                    folium.GeoJson(
                        data=geom.__geo_interface__,
                        style_function=lambda _: oktmo_style,
                        tooltip=folium.Tooltip(f"<b>{row.name}</b><br>ОКТМО {row.code}", sticky=True),
                    ).add_to(fg_oktmo)
                fg_oktmo.add_to(m)
                if bad:
                    print(f"ОКТМО WKT пропущено: {bad}")

    folium.LayerControl(collapsed=False).add_to(m)
    return m


def _collect_centroids(df: pd.DataFrame, wkt_col: str) -> tuple[list[tuple[float, float]], int]:
    pts: list[tuple[float, float]] = []
    bad = 0
    for raw in df[wkt_col].dropna():
        try:
            c = wkt.loads(str(raw)).centroid
            pts.append((float(c.y), float(c.x)))
        except Exception:
            bad += 1
    return pts, bad


def _oktmo_polygon_style(level: int, fill_color: str) -> dict[str, Any]:
    if level == 1:
        return {
            "color": "#374151",
            "weight": 2,
            "fillColor": fill_color,
            "fillOpacity": 0.45,
        }
    return {
        "color": fill_color,
        "weight": 1,
        "fillColor": fill_color,
        "fillOpacity": 0.12,
    }


def _auto_wkt_limit(total_rows: int, base: int, cap: int) -> int:
    if total_rows <= 20_000:
        return min(base, total_rows)
    if total_rows <= 80_000:
        return min(max(base // 2, 1200), total_rows)
    return min(cap, total_rows)


def _add_oktmo_level_layer(
    m: folium.Map,
    df: pd.DataFrame,
    *,
    level: int,
    layer_name: str,
    color: str,
    show: bool,
    max_items: int,
) -> tuple[int, int, int]:
    src = df.loc[df["level"] == level].copy()
    total = len(src)
    if total == 0 or "WKT" not in src.columns:
        return (0, 0, total)
    work = src if total <= max_items else src.sample(max_items, random_state=42)
    fg = folium.FeatureGroup(name=f"{layer_name}: {len(work):,}/{total:,}", show=show)
    ok = bad = 0
    for row in work.itertuples(index=False):
        try:
            geom = wkt.loads(str(row.WKT))
        except Exception:
            bad += 1
            continue
        parent = getattr(row, "parent_code", None)
        parent_s = "" if pd.isna(parent) else str(parent)
        folium.GeoJson(
            data=geom.__geo_interface__,
            style_function=lambda _, c=color, lv=level: _oktmo_polygon_style(lv, c),
            tooltip=folium.Tooltip(
                f"<b>{row.name}</b><br>ОКТМО {row.code}<br>level={level}"
                + (f"<br>parent={parent_s}" if parent_s else ""),
                sticky=True,
            ),
        ).add_to(fg)
        ok += 1
    fg.add_to(m)
    return ok, bad, total


def render_stg_oktmo_folium_map(root: Path) -> folium.Map:
    oktmo_parquet = DEFAULT_STG_OKTMO_OUTPUT_PATH
    if not oktmo_parquet.is_absolute():
        oktmo_parquet = root / oktmo_parquet

    oktmo_df = pd.read_parquet(oktmo_parquet)
    if oktmo_df.empty or "WKT" not in oktmo_df.columns:
        raise ValueError(f"Нет данных для карты: {oktmo_parquet}")

    try:
        rel = oktmo_parquet.relative_to(root)
    except ValueError:
        rel = oktmo_parquet
    print(f"stg_oktmo rows: {len(oktmo_df):,} | файл: {rel}")
    display(
        oktmo_df.groupby("level", as_index=False)
        .agg(polygons=("code", "count"))
        .sort_values("level")
    )

    l1 = oktmo_df.loc[oktmo_df["level"] == 1]
    if l1.empty:
        raise ValueError(f"Нет ОКТМО level=1 в {oktmo_parquet}")

    centroids, bad_centroids = _collect_centroids(l1, "WKT")
    if not centroids:
        raise ValueError("Не удалось распарсить WKT для ОКТМО level=1")

    center_lat = sum(lat for lat, _ in centroids) / len(centroids)
    center_lon = sum(lon for _, lon in centroids) / len(centroids)
    m = folium.Map(location=[center_lat, center_lon], zoom_start=4, tiles="CartoDB positron")

    cmap = plt.get_cmap("tab20", max(len(l1), 1))
    l1_ok = l1_bad = 0
    fg_l1 = folium.FeatureGroup(name=f"ОКТМО level=1: {len(l1):,}", show=True)
    for i, row in enumerate(l1.itertuples(index=False)):
        try:
            geom = wkt.loads(str(row.WKT))
        except Exception:
            l1_bad += 1
            continue
        fill = plt.matplotlib.colors.to_hex(cmap(i % cmap.N))
        folium.GeoJson(
            data=geom.__geo_interface__,
            style_function=lambda _, c=fill: _oktmo_polygon_style(1, c),
            tooltip=folium.Tooltip(
                f"<b>{row.name}</b><br>ОКТМО {row.code}<br>level=1",
                sticky=True,
            ),
        ).add_to(fg_l1)
        l1_ok += 1
    fg_l1.add_to(m)

    l2_max = _auto_wkt_limit(int((oktmo_df["level"] == 2).sum()), base=3000, cap=5000)
    l2_ok, l2_bad, l2_total = _add_oktmo_level_layer(
        m,
        oktmo_df,
        level=2,
        layer_name="ОКТМО level=2",
        color="#2563eb",
        show=False,
        max_items=l2_max,
    )

    folium.LayerControl(collapsed=False).add_to(m)
    print(f"level=1 rendered: {l1_ok:,}/{len(l1):,} (bad: {l1_bad:,})")
    if l2_total:
        print(f"level=2 rendered: {l2_ok:,}/{l2_total:,} (bad: {l2_bad:,}, limit: {l2_max:,})")
    if bad_centroids:
        print(f"centroid WKT errors (level=1): {bad_centroids}")

    return m


def _resolve_parquet(root: Path, path: Path) -> Path:
    if path.is_absolute():
        return path
    return root / path


def render_stg_time_zones_folium_map(root: Path) -> folium.Map:
    tz_parquet = _resolve_parquet(root, DEFAULT_STG_TIME_ZONES_OUTPUT_PATH)
    oktmo_parquet = _resolve_parquet(root, DEFAULT_STG_OKTMO_OUTPUT_PATH)

    tz_df = pd.read_parquet(tz_parquet)
    if tz_df.empty or "geometry" not in tz_df.columns:
        raise ValueError(f"Нет данных для карты: {tz_parquet}")

    oktmo_df = pd.read_parquet(oktmo_parquet)
    oktmo_l1 = oktmo_df.loc[oktmo_df["level"] == 1].copy()
    if oktmo_l1.empty or "WKT" not in oktmo_l1.columns:
        raise ValueError(f"Нет ОКТМО level=1 в {oktmo_parquet}")

    try:
        tz_rel = tz_parquet.relative_to(root)
    except ValueError:
        tz_rel = tz_parquet
    try:
        oktmo_rel = oktmo_parquet.relative_to(root)
    except ValueError:
        oktmo_rel = oktmo_parquet

    print(f"Таймзоны: {len(tz_df):,} | файл: {tz_rel}")
    print(f"ОКТМО level=1: {len(oktmo_l1):,} | файл: {oktmo_rel}")
    display(
        tz_df.groupby("timezone", as_index=False)
        .agg(regions=("code", "count"))
        .sort_values("timezone")
    )

    offsets = sorted(tz_df["timezone"].dropna().astype(int).unique())
    cmap = plt.get_cmap("Spectral", max(len(offsets), 1))
    color_by_tz = {offset: plt.matplotlib.colors.to_hex(cmap(index)) for index, offset in enumerate(offsets)}

    oktmo_l1_style = {
        "color": "#b45309",
        "weight": 3,
        "fillColor": "#ffffff",
        "fillOpacity": 0.0,
        "dashArray": "6 4",
    }

    def region_style(tz: int) -> dict[str, Any]:
        return {
            "color": "#1f2937",
            "weight": 1,
            "fillColor": color_by_tz.get(int(tz), "#9ca3af"),
            "fillOpacity": 0.55,
        }

    centroids, bad_geom = _collect_centroids(tz_df, "geometry")
    oktmo_centroids, bad_oktmo = _collect_centroids(oktmo_l1, "WKT")
    all_centroids = centroids + oktmo_centroids
    if not all_centroids:
        raise ValueError("Не удалось распарсить WKT ни для таймзон, ни для ОКТМО")

    center_lat = sum(lat for lat, _ in all_centroids) / len(all_centroids)
    center_lon = sum(lon for _, lon in all_centroids) / len(all_centroids)
    m = folium.Map(location=[center_lat, center_lon], zoom_start=3, tiles="CartoDB positron")

    regions_fg = folium.FeatureGroup(name="Таймзоны (UTC offset)", show=True)
    for row in tz_df.itertuples(index=False):
        try:
            geom = wkt.loads(str(row.geometry))
        except Exception:
            bad_geom += 1
            continue
        tz = int(row.timezone)
        folium.GeoJson(
            data=geom.__geo_interface__,
            style_function=lambda _, t=tz: region_style(t),
            tooltip=folium.Tooltip(
                f"<b>{row.name}</b><br>code={row.code}<br>UTC+{tz}",
                sticky=True,
            ),
        ).add_to(regions_fg)
    regions_fg.add_to(m)

    oktmo_fg = folium.FeatureGroup(name="ОКТМО level=1 (контуры)", show=True)
    for row in oktmo_l1.itertuples(index=False):
        try:
            geom = wkt.loads(str(row.WKT))
        except Exception:
            bad_oktmo += 1
            continue
        folium.GeoJson(
            data=geom.__geo_interface__,
            style_function=lambda _: oktmo_l1_style,
            tooltip=folium.Tooltip(
                f"<b>{row.name}</b><br>ОКТМО {row.code}<br>level=1",
                sticky=True,
            ),
        ).add_to(oktmo_fg)
    oktmo_fg.add_to(m)

    folium.LayerControl(collapsed=False).add_to(m)

    if offsets:
        step_colors = [color_by_tz[offset] for offset in offsets]
        step_index = offsets + [offsets[-1] + 1] if len(offsets) > 1 else [offsets[0], offsets[0] + 1]
        StepColormap(
            colors=step_colors,
            index=step_index,
            caption="Смещение от UTC, ч",
        ).add_to(m)

    if bad_geom or bad_oktmo:
        print(f"Пропущено WKT: таймзоны={bad_geom}, ОКТМО-1={bad_oktmo}")

    return m


# --- src_person DQ charts ---

_SRC_PERSON_KEY_DISTRIBUTIONS = (
    ("period.distribution.identity_type", "identity_type (period)"),
    ("period.distribution.client_type", "client_type (period)"),
    ("period.distribution.operator_Id", "operator_Id (period)"),
    ("distribution.identity_type", "identity_type (snapshot day)"),
    ("distribution.client_type", "client_type (snapshot day)"),
    ("distribution.operator_Id", "operator_Id (snapshot day)"),
)

_SRC_PERSON_KEY_NULLS = (
    "identity_type",
    "client_type",
    "operator_Id",
    "isdn",
    "imsi",
    "imei",
    "birth_day",
    "actually_from",
    "actually_to",
)

_SRC_PERSON_MONTH_CHECKS = (
    "distribution.actually_from_month",
    "distribution.actually_to_month",
    "distribution.birth_day_month",
    "distribution.start_contract_date_month",
)


def _metric_scalar(latest: pd.DataFrame, check: str, key: str) -> float | int | None:
    metrics = _metrics_for_check(latest, check)
    if not metrics or key not in metrics:
        return None
    value = metrics[key]
    if isinstance(value, (int, float)) and not pd.isna(value):
        return value
    return None


def day_coverage_frame(latest: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for _, row in latest.iterrows():
        if row["check"] != "day.coverage":
            continue
        metrics = row.get("metrics")
        if not isinstance(metrics, dict) or not metrics.get("calendar_day"):
            continue
        rows.append(
            {
                "calendar_day": pd.Timestamp(str(metrics["calendar_day"])),
                "row_count": int(metrics.get("row_count") or 0),
                "has_success": bool(metrics.get("has_success")),
            }
        )
    return pd.DataFrame(rows).sort_values("calendar_day") if rows else pd.DataFrame()


def period_volume_frame(latest: pd.DataFrame) -> pd.DataFrame:
    metrics = _metrics_for_check(latest, "period.volume")
    if not metrics or not isinstance(metrics.get("daily_row_counts"), list):
        return pd.DataFrame()
    return (
        pd.DataFrame(metrics["daily_row_counts"])
        .assign(calendar_day=lambda df: pd.to_datetime(df["calendar_day"]))
        .sort_values("calendar_day")
    )


def distribution_pct_frame(latest: pd.DataFrame, check: str) -> pd.DataFrame:
    metrics = _metrics_for_check(latest, check)
    if not metrics or not isinstance(metrics.get("distribution_pct"), dict):
        return pd.DataFrame(columns=["value", "pct"])
    return pd.DataFrame(
        [{"value": str(key), "pct": float(value)} for key, value in metrics["distribution_pct"].items()]
    ).sort_values("pct", ascending=False)


def identity_fill_frame(latest: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for _, row in latest.iterrows():
        check = str(row["check"])
        if not check.startswith("identity_type.") or not check.endswith("_fill"):
            continue
        metrics = row.get("metrics")
        if not isinstance(metrics, dict):
            continue
        rows.append(
            {
                "check": check,
                "non_null_rate": float(metrics.get("non_null_rate") or 0),
                "rows": int(metrics.get("rows") or 0),
                "status": row.get("status"),
            }
        )
    return pd.DataFrame(rows)


def domain_quality_frame(latest: pd.DataFrame) -> pd.DataFrame:
    specs = (
        ("isdn_format", "valid_rate"),
        ("imsi_format", "valid_len_rate"),
        ("imei_format", "valid_len_rate"),
        ("iccid_format", "valid_len_rate"),
        ("passport_format", "valid_format_rate"),
        ("fio_quality_physical", "fio_present_rate"),
        ("stg_contract.physical.fio_present", "fio_present_rate"),
        ("stg_contract.physical.interval_order", "valid_order_rate"),
    )
    rows: list[dict[str, Any]] = []
    for check, metric_key in specs:
        value = _metric_scalar(latest, check, metric_key)
        if value is None:
            continue
        rows.append({"check": check, "metric": metric_key, "value": float(value)})
    return pd.DataFrame(rows)


def success_days_inventory(latest: pd.DataFrame) -> list[str]:
    metrics = _metrics_for_check(latest, "success_days_inventory")
    if not metrics:
        return []
    raw = metrics.get("success_days")
    return [str(item) for item in raw] if isinstance(raw, list) else []


def plot_daily_volume(
    volume: pd.DataFrame,
    *,
    ax: plt.Axes | None = None,
    title: str | None = None,
) -> plt.Figure:
    if ax is None:
        fig, ax = plt.subplots(figsize=(11, 4))
    else:
        fig = ax.figure
    if volume.empty:
        ax.set_title(title or "Объём по дням — нет метрик day.coverage / period.volume")
        ax.axis("off")
        return fig
    success = volume["has_success"].fillna(False) if "has_success" in volume.columns else pd.Series(False, index=volume.index)
    colors = ["#1f77b4" if flag else "#aec7e8" for flag in success]
    ax.bar(volume["calendar_day"], volume["row_count"], color=colors, width=0.85)
    if success.any():
        ax.bar([], [], color="#1f77b4", label="_SUCCESS")
        ax.bar([], [], color="#aec7e8", label="частичный")
        ax.legend(fontsize=8)
    ax.set_ylabel("row_count (DQ metric)")
    ax.set_title(title or "Строки по дням (DQ)")
    fig.autofmt_xdate(rotation=35, ha="right")
    fig.tight_layout()
    return fig


def plot_distribution_bars(
    dist: pd.DataFrame,
    *,
    title: str,
    ax: plt.Axes | None = None,
    top_n: int = 12,
) -> plt.Figure:
    if ax is None:
        fig, ax = plt.subplots(figsize=(7, 4))
    else:
        fig = ax.figure
    if dist.empty:
        ax.set_title(f"{title}\n(нет distribution_pct в логе)")
        ax.axis("off")
        return fig
    work = dist.head(top_n).iloc[::-1]
    ax.barh(work["value"].astype(str), work["pct"], color="#9467bd", alpha=0.88)
    ax.set_xlabel("%")
    ax.set_title(title)
    fig.tight_layout()
    return fig


def plot_identity_fill(fill: pd.DataFrame, *, ax: plt.Axes | None = None) -> plt.Figure:
    if ax is None:
        fig, ax = plt.subplots(figsize=(9, 5))
    else:
        fig = ax.figure
    if fill.empty:
        ax.set_title("identity_type.*_fill — нет данных")
        ax.axis("off")
        return fig
    work = fill.sort_values("non_null_rate", ascending=True)
    labels = work["check"].str.replace("identity_type.", "", regex=False).str.replace("_fill", "", regex=False)
    colors = [
        "#d62728" if status == "failed" else "#ff7f0e" if status == "warning" else "#2ca02c"
        for status in work["status"]
    ]
    ax.barh(labels, work["non_null_rate"] * 100, color=colors, alpha=0.88)
    ax.set_xlabel("non_null_rate, %")
    ax.set_title("Заполнение полей по identity_type (DQ)")
    ax.axvline(99, color="gray", ls="--", lw=0.8, label="99%")
    ax.legend(fontsize=8)
    fig.tight_layout()
    return fig


def plot_domain_quality(domain: pd.DataFrame, *, ax: plt.Axes | None = None) -> plt.Figure:
    if ax is None:
        fig, ax = plt.subplots(figsize=(8, 4))
    else:
        fig = ax.figure
    if domain.empty:
        ax.set_title("Доменные rate-метрики — нет данных")
        ax.axis("off")
        return fig
    work = domain.sort_values("value", ascending=True)
    ax.barh(work["check"], work["value"] * 100, color="#17becf", alpha=0.88)
    ax.set_xlabel("rate, %")
    ax.set_title("Качество форматов / контракт (DQ)")
    fig.tight_layout()
    return fig


def plot_cross_identity_client(latest: pd.DataFrame, *, ax: plt.Axes | None = None) -> plt.Figure:
    if ax is None:
        fig, ax = plt.subplots(figsize=(8, 4))
    else:
        fig = ax.figure
    metrics = _metrics_for_check(latest, "period.cross.identity_type_x_client_type")
    if not metrics or not isinstance(metrics.get("rows"), list):
        ax.set_title("period.cross — нет данных")
        ax.axis("off")
        return fig
    frame = pd.DataFrame(metrics["rows"])
    frame["label"] = frame["identity_type"].astype(str) + " / ct=" + frame["client_type"].astype(str)
    work = frame.sort_values("pct", ascending=True).tail(12)
    ax.barh(work["label"], work["pct"], color="#8c564b", alpha=0.88)
    ax.set_xlabel("% (period scan)")
    ax.set_title("identity_type × client_type (DQ)")
    fig.tight_layout()
    return fig


def plot_profile_coverage(latest: pd.DataFrame, *, ax: plt.Axes | None = None) -> plt.Figure:
    if ax is None:
        fig, ax = plt.subplots(figsize=(6, 3.5))
    else:
        fig = ax.figure
    metrics = _metrics_for_check(latest, "field_profile_coverage")
    if not metrics:
        ax.set_title("field_profile_coverage — нет данных")
        ax.axis("off")
        return fig
    labels = ["profiled_fields", "distribution_checks", "numeric_profile_checks", "unique_values_checks"]
    values = [int(metrics.get(label) or 0) for label in labels]
    ax.bar(labels, values, color="#bcbd22", alpha=0.9)
    ax.set_ylabel("count")
    ax.set_title("Охват профилирования полей (DQ)")
    plt.setp(ax.get_xticklabels(), rotation=25, ha="right")
    fig.tight_layout()
    return fig


def plot_success_timeline(latest: pd.DataFrame, *, ax: plt.Axes | None = None) -> plt.Figure:
    volume = period_volume_frame(latest)
    if volume.empty:
        volume = day_coverage_frame(latest)
    inventory = set(success_days_inventory(latest))
    if ax is None:
        fig, ax = plt.subplots(figsize=(11, 2.8))
    else:
        fig = ax.figure
    if volume.empty:
        ax.set_title("_SUCCESS timeline — нет day.coverage")
        ax.axis("off")
        return fig
    days = volume["calendar_day"]
    fact = volume["has_success"].fillna(False).astype(int)
    ax.step(days, fact, where="mid", label="факт _SUCCESS", color="#2ca02c")
    if inventory:
        marked = [1 if day.strftime("%Y-%m-%d") in inventory else 0 for day in days]
        ax.step(days, marked, where="mid", label="success_days_inventory", color="#9467bd", alpha=0.7)
    ax.set_ylim(-0.1, 1.2)
    ax.set_yticks([0, 1])
    ax.set_yticklabels(["нет", "да"])
    ax.set_title("Полные срезы (_SUCCESS) по дням (DQ)")
    ax.legend(fontsize=8)
    fig.autofmt_xdate(rotation=35, ha="right")
    fig.tight_layout()
    return fig


def render_src_person_dq_overview(latest: pd.DataFrame) -> plt.Figure:
    fig, axes = plt.subplots(2, 2, figsize=(14, 9))
    plot_check_status(latest, ax=axes[0, 0])
    volume = period_volume_frame(latest)
    title = "period.volume (DQ)"
    if volume.empty:
        volume = day_coverage_frame(latest)
        title = "day.coverage (DQ)"
    plot_daily_volume(volume, ax=axes[0, 1], title=title)
    dist = distribution_pct_frame(latest, "period.distribution.identity_type")
    if dist.empty:
        dist = distribution_pct_frame(latest, "distribution.identity_type")
    plot_distribution_bars(dist, title="identity_type (period → snapshot)", ax=axes[1, 0])
    plot_cross_identity_client(latest, ax=axes[1, 1])
    basic = _metrics_for_check(latest, "dataset_filter")
    period = ""
    if basic:
        period = f"{basic.get('start_date')} .. {basic.get('end_date')}"
    fig.suptitle(f"DQ SRC PERSON — метрики лога ({period})", fontsize=13, y=1.02)
    fig.tight_layout()
    return fig


def render_src_person_dq_distributions(latest: pd.DataFrame) -> plt.Figure:
    count = len(_SRC_PERSON_KEY_DISTRIBUTIONS)
    cols = 2
    rows = (count + cols - 1) // cols
    fig, axes = plt.subplots(rows, cols, figsize=(14, 4 * rows))
    axes_flat = axes.flatten() if count > 1 else [axes]
    for ax, (check, title) in zip(axes_flat, _SRC_PERSON_KEY_DISTRIBUTIONS, strict=False):
        plot_distribution_bars(distribution_pct_frame(latest, check), title=title, ax=ax)
    for ax in axes_flat[len(_SRC_PERSON_KEY_DISTRIBUTIONS) :]:
        ax.axis("off")
    fig.suptitle("distribution_pct из DQ-логов", fontsize=12, y=1.01)
    fig.tight_layout()
    return fig


def render_src_person_dq_quality(latest: pd.DataFrame) -> plt.Figure:
    fig, axes = plt.subplots(2, 2, figsize=(14, 8))
    nulls = null_ratio_frame(latest)
    if not nulls.empty:
        nulls = nulls[nulls["field"].isin(_SRC_PERSON_KEY_NULLS)]
    plot_null_ratios(nulls, ax=axes[0, 0])
    plot_identity_fill(identity_fill_frame(latest), ax=axes[0, 1])
    plot_domain_quality(domain_quality_frame(latest), ax=axes[1, 0])
    plot_profile_coverage(latest, ax=axes[1, 1])
    fig.suptitle("Качество и профили (DQ-метрики)", fontsize=12, y=1.02)
    fig.tight_layout()
    return fig


def render_src_person_dq_timeseries(latest: pd.DataFrame) -> plt.Figure:
    fig, axes = plt.subplots(1, 2, figsize=(14, 4))
    plot_success_timeline(latest, ax=axes[0])
    basic = _metrics_for_check(latest, "dataset_basic")
    row_count = int(basic.get("row_count") or 0) if basic else 0
    axes[1].text(
        0.5,
        0.55,
        f"Выбранный день (DQ):\n{basic.get('selected_day') if basic else '—'}\n"
        f"row_count={row_count:,}\n"
        f"by_success={basic.get('selected_by_success') if basic else '—'}",
        ha="center",
        va="center",
        fontsize=11,
        transform=axes[1].transAxes,
    )
    axes[1].set_title("Контекст snapshot (dataset_basic)")
    axes[1].axis("off")
    fig.suptitle("Календарь и контекст среза", fontsize=12)
    fig.tight_layout()
    return fig


def render_src_person_month_distributions(latest: pd.DataFrame) -> plt.Figure:
    fig, axes = plt.subplots(2, 2, figsize=(14, 8))
    for ax, check in zip(axes.flatten(), _SRC_PERSON_MONTH_CHECKS):
        plot_distribution_bars(distribution_pct_frame(latest, check), title=check, ax=ax)
    for ax in axes.flatten()[len(_SRC_PERSON_MONTH_CHECKS) :]:
        ax.axis("off")
    fig.suptitle("distribution_*_month (snapshot day, DQ)", fontsize=12, y=1.02)
    fig.tight_layout()
    return fig


# --- src excl DQ charts ---

_SRC_EXCL_MARTS = ("src_imsi", "src_imei", "src_msisdn")
_SRC_EXCL_MART_LABELS = {
    "src_imsi": "IMSI",
    "src_imei": "IMEI",
    "src_msisdn": "MSISDN",
}
_SRC_EXCL_MART_CHECKS = ("dataset_presence", "dataset_basic", "schema_columns", "totals")


def src_excl_totals_frame(latest: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for mart in _SRC_EXCL_MARTS:
        metrics = _metrics_for_check(latest, f"{mart}.totals")
        if not metrics:
            continue
        row_count = int(metrics.get("row_count") or 0)
        unique_count = int(metrics.get("unique_count") or 0)
        null_count = int(metrics.get("null_count") or 0)
        rows.append(
            {
                "mart": mart,
                "label": _SRC_EXCL_MART_LABELS[mart],
                "row_count": row_count,
                "unique_count": unique_count,
                "null_count": null_count,
                "unique_ratio": (unique_count / row_count) if row_count else 0.0,
            }
        )
    return pd.DataFrame(rows)


def src_excl_mart_status_frame(latest: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for mart in _SRC_EXCL_MARTS:
        for check in _SRC_EXCL_MART_CHECKS:
            full = f"{mart}.{check}"
            hit = latest[latest["check"] == full]
            status = str(hit.iloc[-1]["status"]) if not hit.empty else "missing"
            rows.append({"mart": mart, "label": _SRC_EXCL_MART_LABELS[mart], "check": check, "status": status})
    return pd.DataFrame(rows)


def _plot_src_excl_totals_bars(totals: pd.DataFrame, *, ax: plt.Axes | None = None) -> plt.Figure:
    if ax is None:
        fig, ax = plt.subplots(figsize=(7, 4))
    else:
        fig = ax.figure
    if totals.empty:
        ax.set_title("totals.* — нет данных в логе")
        ax.axis("off")
        return fig
    x = range(len(totals))
    width = 0.35
    ax.bar([i - width / 2 for i in x], totals["row_count"], width=width, label="row_count", color="#1f77b4")
    ax.bar([i + width / 2 for i in x], totals["unique_count"], width=width, label="unique_count", color="#ff7f0e")
    ax.set_xticks(list(x))
    ax.set_xticklabels(totals["label"])
    ax.set_ylabel("count")
    ax.set_title("Размеры списков (totals)")
    ax.legend(fontsize=8)
    for i, row in enumerate(totals.itertuples()):
        ax.text(i - width / 2, row.row_count, f"{row.row_count:,}", ha="center", va="bottom", fontsize=8)
    fig.tight_layout()
    return fig


def _plot_src_excl_null_counts(totals: pd.DataFrame, *, ax: plt.Axes | None = None) -> plt.Figure:
    if ax is None:
        fig, ax = plt.subplots(figsize=(6, 3.5))
    else:
        fig = ax.figure
    if totals.empty:
        ax.set_title("null_count — нет totals")
        ax.axis("off")
        return fig
    colors = ["#d62728" if n > 0 else "#2ca02c" for n in totals["null_count"]]
    ax.bar(totals["label"], totals["null_count"], color=colors, alpha=0.88)
    ax.set_ylabel("null_count")
    ax.set_title("Пустые значения в колонке value")
    fig.tight_layout()
    return fig


def _plot_src_excl_unique_ratio(totals: pd.DataFrame, *, ax: plt.Axes | None = None) -> plt.Figure:
    if ax is None:
        fig, ax = plt.subplots(figsize=(6, 3.5))
    else:
        fig = ax.figure
    if totals.empty:
        ax.set_title("unique / row — нет totals")
        ax.axis("off")
        return fig
    pct = totals["unique_ratio"] * 100
    ax.bar(totals["label"], pct, color="#9467bd", alpha=0.88)
    ax.axhline(100, color="gray", ls="--", lw=0.8)
    ax.set_ylim(0, 105)
    ax.set_ylabel("unique / row, %")
    ax.set_title("Уникальность значений (ожидается 100%)")
    for i, value in enumerate(pct):
        ax.text(i, value, f"{value:.1f}%", ha="center", va="bottom", fontsize=9)
    fig.tight_layout()
    return fig


def _plot_src_excl_mart_status_grid(status_df: pd.DataFrame, *, ax: plt.Axes | None = None) -> plt.Figure:
    if ax is None:
        fig, ax = plt.subplots(figsize=(8, 3.5))
    else:
        fig = ax.figure
    if status_df.empty:
        ax.set_title("Статусы проверок — нет данных")
        ax.axis("off")
        return fig
    pivot = status_df.pivot(index="label", columns="check", values="status")
    pivot = pivot.reindex(columns=list(_SRC_EXCL_MART_CHECKS))
    codes = {"failed": 0, "warning": 1, "ok": 2, "missing": 3}
    matrix = pivot.map(lambda status: codes.get(str(status), 3)).to_numpy(dtype=float)
    im = ax.imshow(matrix, aspect="auto", cmap=plt.cm.RdYlGn, vmin=0, vmax=2)
    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels(pivot.columns, rotation=25, ha="right")
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels(pivot.index)
    for i in range(matrix.shape[0]):
        for j in range(matrix.shape[1]):
            ax.text(j, i, pivot.iloc[i, j], ha="center", va="center", fontsize=8, color="black")
    ax.set_title("Статус проверок по витринам")
    fig.colorbar(im, ax=ax, fraction=0.03, pad=0.02, ticks=[0, 1, 2], label="failed → ok")
    fig.tight_layout()
    return fig


def _plot_src_excl_summary_metrics(latest: pd.DataFrame, *, ax: plt.Axes | None = None) -> plt.Figure:
    if ax is None:
        fig, ax = plt.subplots(figsize=(5, 3))
    else:
        fig = ax.figure
    metrics = _metrics_for_check(latest, "summary")
    if not metrics:
        ax.set_title("summary — нет в логе")
        ax.axis("off")
        return fig
    labels = ["total_checks", "warning_checks", "failed_checks"]
    values = [int(metrics.get(key) or 0) for key in labels]
    colors = ["#bcbd22", "#ff7f0e", "#d62728"]
    ax.bar(labels, values, color=colors, alpha=0.9)
    ax.set_ylabel("count")
    ax.set_title("Итог прогона (summary)")
    plt.setp(ax.get_xticklabels(), rotation=15, ha="right")
    for i, value in enumerate(values):
        ax.text(i, value, str(value), ha="center", va="bottom", fontsize=9)
    fig.tight_layout()
    return fig


def render_src_excl_dq_overview(latest: pd.DataFrame) -> plt.Figure:
    totals = src_excl_totals_frame(latest)
    fig, axes = plt.subplots(2, 2, figsize=(14, 9))
    plot_check_status(latest, ax=axes[0, 0])
    _plot_src_excl_totals_bars(totals, ax=axes[0, 1])
    _plot_src_excl_unique_ratio(totals, ax=axes[1, 0])
    _plot_src_excl_null_counts(totals, ax=axes[1, 1])
    fig.suptitle("DQ SRC EXCL — обзор (метрики лога)", fontsize=13, y=1.02)
    fig.tight_layout()
    return fig


def render_src_excl_dq_marts(latest: pd.DataFrame) -> plt.Figure:
    totals = src_excl_totals_frame(latest)
    status_df = src_excl_mart_status_frame(latest)
    fig, axes = plt.subplots(1, 2, figsize=(14, 4.5))
    _plot_src_excl_mart_status_grid(status_df, ax=axes[0])
    _plot_src_excl_summary_metrics(latest, ax=axes[1])
    if not totals.empty and totals["row_count"].nunique() == 1:
        row_count = int(totals["row_count"].iloc[0])
        axes[1].text(
            0.5,
            -0.22,
            f"Синхронность размеров: все три списка по {row_count:,} строк",
            transform=axes[1].transAxes,
            ha="center",
            fontsize=9,
        )
    fig.suptitle("Витрины и итог DQ", fontsize=12, y=1.02)
    fig.tight_layout()
    return fig


# --- src_mobile DQ charts ---

_MOBILE_MARTS = ("cdr", "sms", "gprs", "location")
_MOBILE_MART_LABELS = {"cdr": "CDR", "sms": "SMS", "gprs": "GPRS", "location": "LOC"}
_MOBILE_STG_GATE_SUFFIXES = ("started", "owner", "lac_cell", "imsi", "msisdn", "coords", "columns")


def mobile_coverage_frame(latest: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for mart in _MOBILE_MARTS:
        metrics = _metrics_for_check(latest, f"{mart}.coverage")
        if not metrics:
            continue
        rows.append(
            {
                "mart": mart,
                "label": _MOBILE_MART_LABELS[mart],
                "row_count_total": int(metrics.get("row_count_total") or 0),
                "parquet_files_scanned": int(metrics.get("parquet_files_scanned") or 0),
            }
        )
    return pd.DataFrame(rows)


def mobile_traffic_mix_frame(latest: pd.DataFrame, *, day: bool = True) -> pd.DataFrame:
    check = "cross_mart.day_traffic_mix" if day else "cross_mart.traffic_mix"
    metrics = _metrics_for_check(latest, check)
    totals = metrics.get("row_totals")
    if not isinstance(totals, dict):
        return pd.DataFrame(columns=["mart", "label", "rows"])
    rows = [
        {
            "mart": mart,
            "label": _MOBILE_MART_LABELS.get(mart, mart),
            "rows": int(totals.get(mart) or 0),
        }
        for mart in _MOBILE_MARTS
        if mart in totals
    ]
    return pd.DataFrame(rows)


def mobile_stg_gate_frame(latest: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for mart in _MOBILE_MARTS:
        for gate in _MOBILE_STG_GATE_SUFFIXES:
            status = "missing"
            for prefix in (f"{mart}.day.mobile.stg_contract.", f"{mart}.mobile.stg_contract."):
                full = f"{prefix}{gate}"
                hit = latest[latest["check"] == full]
                if not hit.empty:
                    status = str(hit.iloc[-1]["status"])
                    break
            rows.append(
                {
                    "mart": mart,
                    "label": _MOBILE_MART_LABELS[mart],
                    "gate": gate,
                    "status": status,
                }
            )
    return pd.DataFrame(rows)


def _plot_mobile_coverage_bars(coverage: pd.DataFrame, *, ax: plt.Axes | None = None) -> plt.Figure:
    if ax is None:
        fig, ax = plt.subplots(figsize=(7, 4))
    else:
        fig = ax.figure
    if coverage.empty:
        ax.set_title("coverage — нет данных в логе")
        ax.axis("off")
        return fig
    ax.bar(coverage["label"], coverage["row_count_total"], color="#1f77b4", alpha=0.88)
    ax.set_ylabel("row_count_total")
    ax.set_title("Строки после фильтра Started (coverage)")
    for i, row in enumerate(coverage.itertuples()):
        ax.text(i, row.row_count_total, f"{row.row_count_total:,}", ha="center", va="bottom", fontsize=8)
    fig.tight_layout()
    return fig


def _plot_mobile_traffic_mix(mix: pd.DataFrame, *, title: str, ax: plt.Axes | None = None) -> plt.Figure:
    if ax is None:
        fig, ax = plt.subplots(figsize=(6, 4))
    else:
        fig = ax.figure
    if mix.empty:
        ax.set_title(f"{title}\n(нет данных)")
        ax.axis("off")
        return fig
    colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#9467bd"]
    ax.pie(
        mix["rows"],
        labels=mix["label"],
        autopct=lambda pct: f"{pct:.1f}%" if pct >= 3 else "",
        colors=colors[: len(mix)],
        startangle=90,
    )
    ax.set_title(title)
    fig.tight_layout()
    return fig


def _plot_mobile_stg_gate_grid(gates: pd.DataFrame, *, ax: plt.Axes | None = None) -> plt.Figure:
    if ax is None:
        fig, ax = plt.subplots(figsize=(10, 3.5))
    else:
        fig = ax.figure
    if gates.empty:
        ax.set_title("stg_contract — нет данных")
        ax.axis("off")
        return fig
    pivot = gates.pivot(index="label", columns="gate", values="status")
    pivot = pivot.reindex(columns=list(_MOBILE_STG_GATE_SUFFIXES))
    codes = {"failed": 0, "warning": 1, "ok": 2, "missing": 3, "info": 2}
    matrix = pivot.map(lambda status: codes.get(str(status), 3)).to_numpy(dtype=float)
    im = ax.imshow(matrix, aspect="auto", cmap=plt.cm.RdYlGn, vmin=0, vmax=2)
    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels(pivot.columns, rotation=25, ha="right")
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels(pivot.index)
    for i in range(matrix.shape[0]):
        for j in range(matrix.shape[1]):
            ax.text(j, i, pivot.iloc[i, j], ha="center", va="center", fontsize=7, color="black")
    ax.set_title("Gate stg_contract по витринам")
    fig.colorbar(im, ax=ax, fraction=0.03, pad=0.02, ticks=[0, 1, 2], label="failed → ok")
    fig.tight_layout()
    return fig


def render_src_mobile_dq_overview(latest: pd.DataFrame) -> plt.Figure:
    coverage = mobile_coverage_frame(latest)
    mix = mobile_traffic_mix_frame(latest, day=True)
    filt = _metrics_for_check(latest, "dataset_filter")
    report_date = filt.get("report_date") if filt else None
    fig, axes = plt.subplots(2, 2, figsize=(14, 9))
    plot_check_status(latest, ax=axes[0, 0])
    _plot_mobile_coverage_bars(coverage, ax=axes[0, 1])
    _plot_mobile_traffic_mix(mix, title="cross_mart.day_traffic_mix", ax=axes[1, 0])
    plot_summary_metrics(latest, ax=axes[1, 1])
    title = f"DQ SRC MOBILE — report_date={report_date}" if report_date else "DQ SRC MOBILE — обзор"
    fig.suptitle(title, fontsize=13, y=1.02)
    fig.tight_layout()
    return fig


def render_src_mobile_dq_marts(latest: pd.DataFrame) -> plt.Figure:
    coverage = mobile_coverage_frame(latest)
    gates = mobile_stg_gate_frame(latest)
    mix_all = mobile_traffic_mix_frame(latest, day=False)
    fig, axes = plt.subplots(1, 3, figsize=(16, 4.5))
    if not coverage.empty:
        axes[0].bar(
            coverage["label"],
            coverage["parquet_files_scanned"],
            color="#17becf",
            alpha=0.88,
        )
        axes[0].set_ylabel("parquet_files_scanned")
        axes[0].set_title("Файлы в окне ±1 день")
    else:
        axes[0].set_title("coverage — нет данных")
        axes[0].axis("off")
    _plot_mobile_traffic_mix(mix_all, title="cross_mart.traffic_mix (окно чтения)", ax=axes[1])
    _plot_mobile_stg_gate_grid(gates, ax=axes[2])
    fig.suptitle("Витрины и STG-контракт", fontsize=12, y=1.02)
    fig.tight_layout()
    return fig


# --- stg_bs DQ charts ---

_STG_BS_OPEN_END_PREFIX = "2262-04-11"
_STG_BS_CARDINALITY_FOCUS = (
    "mcc",
    "mnc",
    "lac",
    "cell_id",
    "telecomstandard",
    "bs_type",
    "timezone",
    "oktmo_code_1",
    "oktmo_code_2",
)


def stg_bs_cardinality_frame(latest: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for _, record in latest.iterrows():
        check = str(record["check"])
        if not check.startswith("cardinality."):
            continue
        metrics = record.get("metrics")
        if not isinstance(metrics, dict):
            continue
        rows.append(
            {
                "field": check.removeprefix("cardinality."),
                "nunique": int(metrics.get("nunique") or 0),
            }
        )
    if not rows:
        return pd.DataFrame(columns=["field", "nunique"])
    out = pd.DataFrame(rows)
    focus = [f for f in _STG_BS_CARDINALITY_FOCUS if f in set(out["field"])]
    if focus:
        out = pd.concat([out[out["field"].isin(focus)], out[~out["field"].isin(focus)]], ignore_index=True)
    return out.head(20)


def stg_bs_gate_counts_frame(latest: pd.DataFrame) -> pd.DataFrame:
    specs: tuple[tuple[str, str, str], ...] = (
        ("key_presence", "null_key_rows", "null CGI rows"),
        ("key_uniqueness_per_snapshot", "duplicate_rows", "duplicate key rows"),
        ("coords_range", "invalid_lon_count", "invalid lon"),
        ("coords_range", "invalid_lat_count", "invalid lat"),
        ("bs_type_vocab", "invalid_bs_type_count", "invalid bs_type"),
        ("telecomstandard_vocab", "invalid_telecomstandard_count", "invalid telecomstandard"),
    )
    rows: list[dict[str, Any]] = []
    for check, key, label in specs:
        metrics = _metrics_for_check(latest, check)
        if key not in metrics:
            continue
        hit = latest[latest["check"] == check]
        status = str(hit.iloc[-1]["status"]) if not hit.empty else "missing"
        rows.append({"metric": label, "count": int(metrics[key]), "status": status})
    return pd.DataFrame(rows)


def stg_bs_geometry_metrics_frame(latest: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for geom_check in ("geometry.sector_wkt", "geometry.mapinfo_wkt"):
        metrics = _metrics_for_check(latest, geom_check)
        if not metrics:
            continue
        short = geom_check.removeprefix("geometry.")
        for key, label in (
            ("valid_geometry_count", "valid"),
            ("parse_error_count", "parse errors"),
            ("invalid_topology_count", "invalid topology"),
            ("empty_geometry_count", "empty"),
            ("unsupported_geom_type_count", "unsupported type"),
        ):
            if key in metrics:
                rows.append({"geometry": short, "metric": label, "count": int(metrics[key])})
    return pd.DataFrame(rows)


def render_stg_bs_dq_gates(latest: pd.DataFrame) -> plt.Figure:
    gates = stg_bs_gate_counts_frame(latest)
    geom = stg_bs_geometry_quality_frame(latest)
    fig, axes = plt.subplots(1, 2, figsize=(14, 4.5))
    if gates.empty:
        axes[0].set_title("Gate counts — нет данных")
        axes[0].axis("off")
    else:
        colors = [
            "#d62728" if s == "failed" else "#ff7f0e" if s == "warning" else "#2ca02c"
            for s in gates["status"]
        ]
        work = gates.sort_values("count", ascending=True)
        axes[0].barh(work["metric"], work["count"], color=colors, alpha=0.88)
        axes[0].set_xlabel("count")
        axes[0].set_title("Ключи, координаты, словари (DQ)")
    plot_count_bars(geom, title="WKT geometry (DQ)", ax=axes[1], color="#17becf")
    fig.suptitle("Gate-проверки stg_bs", fontsize=12, y=1.02)
    fig.tight_layout()
    return fig


def render_stg_bs_dq_nulls(latest: pd.DataFrame) -> plt.Figure:
    nulls = null_ratio_frame(latest)
    fig, ax = plt.subplots(figsize=(10, 6))
    if nulls.empty:
        ax.set_title("nulls.* — нет данных в логе")
        ax.axis("off")
        return fig
    plot_null_ratios(nulls, ax=ax)
    fig.suptitle("Доля null по полям контракта (DQ)", fontsize=12)
    fig.tight_layout()
    return fig


def render_stg_bs_dq_geometry_detail(latest: pd.DataFrame) -> plt.Figure:
    geom = stg_bs_geometry_metrics_frame(latest)
    fig, ax = plt.subplots(figsize=(10, 4))
    if geom.empty:
        ax.set_title("geometry.* — нет данных")
        ax.axis("off")
        return fig
    pivot = geom.pivot(index="metric", columns="geometry", values="count").fillna(0)
    pivot = pivot.loc[pivot.sum(axis=1).sort_values(ascending=True).index]
    pivot.plot(kind="barh", ax=ax, color=["#17becf", "#9467bd"], alpha=0.88)
    ax.set_xlabel("count (DQ log)")
    ax.set_title("WKT: sector_wkt vs mapinfo_wkt")
    ax.legend(title="geometry", fontsize=8)
    fig.tight_layout()
    return fig


def render_stg_bs_dq_cardinality(latest: pd.DataFrame) -> plt.Figure:
    card = stg_bs_cardinality_frame(latest)
    fig, ax = plt.subplots(figsize=(10, 5))
    if card.empty:
        ax.set_title("cardinality.* — нет данных")
        ax.axis("off")
        return fig
    work = card.sort_values("nunique", ascending=True)
    ax.barh(work["field"], work["nunique"], color="#9467bd", alpha=0.88)
    ax.set_xlabel("nunique (DQ log)")
    ax.set_title("Кардинальность полей (top-20)")
    fig.tight_layout()
    return fig


def render_stg_bs_parquet_scd_mix(root: Path) -> plt.Figure:
    bs_parquet = _resolve_parquet(root, stg_bs_output_path())
    if not bs_parquet.exists():
        raise FileNotFoundError(f"Нет parquet: {bs_parquet}")
    df = pd.read_parquet(bs_parquet, columns=["date_off", "telecomstandard", "bs_type"])
    date_off = pd.to_datetime(df["date_off"], errors="coerce")
    open_mask = date_off.dt.strftime("%Y-%m-%d").eq(_STG_BS_OPEN_END_PREFIX)
    scd = pd.DataFrame(
        [
            {"segment": "open (active)", "rows": int(open_mask.sum())},
            {"segment": "closed (history)", "rows": int((~open_mask).sum())},
        ]
    )
    fig, axes = plt.subplots(1, 3, figsize=(15, 4.5))
    axes[0].pie(
        scd["rows"],
        labels=scd["segment"],
        autopct=lambda pct: f"{pct:.1f}%" if pct >= 2 else "",
        colors=["#2ca02c", "#aec7e8"],
        startangle=90,
    )
    axes[0].set_title("SCD: open vs closed")
    if "telecomstandard" in df.columns:
        tc = df["telecomstandard"].astype("string").fillna("<NA>").value_counts().head(8)
        axes[1].pie(
            tc.values,
            labels=tc.index.astype(str),
            autopct=lambda pct: f"{pct:.1f}%" if pct >= 3 else "",
            startangle=90,
        )
        axes[1].set_title("telecomstandard")
    else:
        axes[1].axis("off")
    if "bs_type" in df.columns:
        bt = df["bs_type"].astype("string").fillna("<NA>").value_counts()
        axes[2].bar(bt.index.astype(str), bt.values, color="#ff7f0e", alpha=0.88)
        axes[2].set_title("bs_type")
        plt.setp(axes[2].get_xticklabels(), rotation=25, ha="right")
    else:
        axes[2].axis("off")
    fig.suptitle("Профиль parquet stg_bs", fontsize=12, y=1.02)
    fig.tight_layout()
    return fig


_STG_BS_MAP_STD_COLORS = {"2G": "#1f77b4", "3G": "#2ca02c", "4G": "#ff7f0e", "unknown": "#6b7280"}
_STG_BS_WKT_SECTOR_STYLE = {
    "color": "#2563eb",
    "weight": 1,
    "fillColor": "#2563eb",
    "fillOpacity": 0.22,
}
_STG_BS_WKT_MAPINFO_STYLE = {
    "color": "#b45309",
    "weight": 1,
    "fillColor": "#b45309",
    "fillOpacity": 0.18,
}
_STG_BS_POINTS_DETAIL_MAX = 2500
_STG_BS_WKT_FEATURES_MAX = 600
_STG_BS_FILTER_ALL = "Все"
_STG_BS_MAP_READ_COLUMNS = (
    "lon",
    "lat",
    "mcc",
    "mnc",
    "lac",
    "cell_id",
    "telecomstandard",
    "bs_type",
    "date_off",
    "oktmo_code_1",
)


def _notebook_batch_mode() -> bool:
    """``nb-*`` через ``notebook_runner`` (без интерактива, облегчённые карты)."""
    return os.environ.get("MOBILE_NOTEBOOK_BATCH") == "1"


def _parquet_columns_subset(parquet: Path, wanted: tuple[str, ...]) -> list[str]:
    import pyarrow.parquet as pq

    available = set(pq.read_schema(parquet).names)
    return [col for col in wanted if col in available]


def _stg_bs_filter_options(series: pd.Series) -> list[str]:
    vals = series.dropna().astype("string").str.strip()
    vals = vals[vals != ""].unique()
    return [_STG_BS_FILTER_ALL] + sorted(vals.tolist(), key=str)


def _apply_stg_bs_filters(
    df: pd.DataFrame,
    *,
    mnc: str,
    telecomstandard: str,
    bs_type: str,
) -> pd.DataFrame:
    out = df
    if mnc != _STG_BS_FILTER_ALL and "mnc" in out.columns:
        out = out.loc[out["mnc"].astype("string") == mnc]
    if telecomstandard != _STG_BS_FILTER_ALL and "telecomstandard" in out.columns:
        out = out.loc[out["telecomstandard"].astype("string") == telecomstandard]
    if bs_type != _STG_BS_FILTER_ALL and "bs_type" in out.columns:
        out = out.loc[out["bs_type"].astype("string") == bs_type]
    return out


def _stg_bs_tooltip(row: Any, *, wkt_kind: str | None = None) -> str:
    std = getattr(row, "telecomstandard", "—")
    bt = getattr(row, "bs_type", "—")
    tip = (
        f"<b>{row.mcc}-{row.mnc}-{row.lac}-{row.cell_id}</b><br>"
        f"mnc={row.mnc}<br>std={std}<br>bs_type={bt}"
    )
    if wkt_kind:
        tip += f"<br>{wkt_kind}"
    return tip


def _load_stg_bs_map_df(root: Path, *, active_only: bool = True) -> tuple[pd.DataFrame, str]:
    """Активные БС с валидными lon/lat и нормализованными полями фильтров."""
    bs_parquet = _resolve_parquet(root, stg_bs_output_path())
    if not bs_parquet.exists():
        raise FileNotFoundError(f"Нет parquet: {bs_parquet}")

    read_cols = _parquet_columns_subset(bs_parquet, _STG_BS_MAP_READ_COLUMNS)
    df = pd.read_parquet(bs_parquet, columns=read_cols or None)
    if active_only and "date_off" in df.columns:
        date_off = pd.to_datetime(df["date_off"], errors="coerce")
        df = df.loc[date_off.dt.strftime("%Y-%m-%d").eq(_STG_BS_OPEN_END_PREFIX)].copy()
        segment_label = "активные (open)"
    else:
        segment_label = "все строки"

    required = ["lon", "lat", "mcc", "mnc", "lac", "cell_id"]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"В stg_bs нет ожидаемых колонок: {missing}")

    pts = df.copy()
    pts["lon"] = pd.to_numeric(pts["lon"], errors="coerce")
    pts["lat"] = pd.to_numeric(pts["lat"], errors="coerce")
    pts = pts.loc[pts["lon"].notna() & pts["lat"].notna()]
    pts = pts[pts["lon"].between(-180, 180) & pts["lat"].between(-90, 90)].copy()
    if "mnc" in pts.columns:
        pts["mnc"] = pts["mnc"].astype("string").str.strip()
    if "telecomstandard" in pts.columns:
        pts["telecomstandard"] = pts["telecomstandard"].astype("string").fillna("unknown")
    if "bs_type" in pts.columns:
        pts["bs_type"] = pts["bs_type"].astype("string").fillna("unknown")

    try:
        rel = bs_parquet.relative_to(root)
    except ValueError:
        rel = bs_parquet
    print(f"stg_bs ({segment_label}): {len(df):,} rows | файл: {rel}")
    print(f"точек lon/lat: {len(pts):,}")
    if pts.empty:
        raise ValueError("Нет валидных lon/lat для карты stg_bs")
    return pts, segment_label


def _stg_bs_map_center(pts: pd.DataFrame) -> tuple[float, float]:
    if pts.empty:
        return 55.75, 37.62
    return float(pts["lat"].mean()), float(pts["lon"].mean())


def _add_stg_bs_oktmo_level1(m: folium.Map, pts: pd.DataFrame, root: Path) -> None:
    if "oktmo_code_1" not in pts.columns:
        return
    oktmo_parquet = _resolve_parquet(root, DEFAULT_STG_OKTMO_OUTPUT_PATH)
    if not oktmo_parquet.exists():
        return
    oktmo_df = pd.read_parquet(oktmo_parquet)
    if not {"level", "code", "WKT"}.issubset(oktmo_df.columns):
        return
    codes = pts["oktmo_code_1"].dropna().astype("string").unique().tolist()
    oktmo_l1 = oktmo_df.loc[
        (oktmo_df["level"] == 1) & (oktmo_df["code"].astype("string").isin(codes))
    ].copy()
    if oktmo_l1.empty:
        return
    oktmo_style = {"color": "#b45309", "weight": 2, "fillColor": "#b45309", "fillOpacity": 0.0}
    fg_oktmo = folium.FeatureGroup(
        name=f"ОКТМО level=1 (коды в stg_bs): {len(oktmo_l1):,}",
        show=True,
    )
    bad = 0
    for row in oktmo_l1.itertuples(index=False):
        try:
            geom = wkt.loads(str(row.WKT))
        except Exception:
            bad += 1
            continue
        name = getattr(row, "name", row.code)
        folium.GeoJson(
            data=geom.__geo_interface__,
            style_function=lambda _: oktmo_style,
            tooltip=folium.Tooltip(f"<b>{name}</b><br>ОКТМО {row.code}", sticky=True),
        ).add_to(fg_oktmo)
    fg_oktmo.add_to(m)
    if bad:
        print(f"ОКТМО WKT пропущено: {bad}")


def build_stg_bs_points_map(
    pts: pd.DataFrame,
    root: Path,
    *,
    segment_label: str,
    lite: bool = False,
) -> folium.Map:
    """Точки БС: кластер; в полном режиме — маркеры по ``telecomstandard`` и ОКТМО."""
    center_lat, center_lon = _stg_bs_map_center(pts)
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=5,
        tiles="CartoDB positron",
        width="100%",
        height="100%",
    )
    all_points = pts[["lat", "lon"]].astype(float).values.tolist()
    fg_all = folium.FeatureGroup(
        name=f"БС {segment_label} (FastMarkerCluster): {len(all_points):,}",
        show=True,
    )
    FastMarkerCluster(data=all_points).add_to(fg_all)
    fg_all.add_to(m)

    if lite:
        folium.LayerControl(collapsed=False).add_to(m)
        return m

    if "telecomstandard" in pts.columns:
        for std, group in pts.groupby("telecomstandard", dropna=False):
            std_name = str(std)
            color = _STG_BS_MAP_STD_COLORS.get(std_name, "#9467bd")
            sample = (
                group
                if len(group) <= _STG_BS_POINTS_DETAIL_MAX
                else group.sample(_STG_BS_POINTS_DETAIL_MAX, random_state=42)
            )
            fg_std = folium.FeatureGroup(
                name=f"telecomstandard={std_name}: {len(sample):,}/{len(group):,}",
                show=False,
            )
            for row in sample.itertuples(index=False):
                folium.CircleMarker(
                    location=[float(row.lat), float(row.lon)],
                    radius=2,
                    color=color,
                    weight=1,
                    fill=True,
                    fill_opacity=0.7,
                    tooltip=folium.Tooltip(_stg_bs_tooltip(row), sticky=False),
                ).add_to(fg_std)
            fg_std.add_to(m)

    _add_stg_bs_oktmo_level1(m, pts, root)
    folium.LayerControl(collapsed=False).add_to(m)
    return m


def build_stg_bs_wkt_map(
    pts: pd.DataFrame,
    root: Path,
    wkt_col: str,
    *,
    layer_title: str,
    style: dict[str, Any],
    segment_label: str,
    max_features: int | None = None,
    add_oktmo: bool = True,
) -> folium.Map | None:
    """Полигоны из ``sector_wkt`` или ``mapinfo_wkt`` (сэмпл при большом объёме)."""
    if wkt_col not in pts.columns:
        return None
    work = pts.loc[
        pts[wkt_col].notna() & (pts[wkt_col].astype("string").str.strip() != "")
    ].copy()
    if work.empty:
        return None

    cap = max_features if max_features is not None else _STG_BS_WKT_FEATURES_MAX
    total = len(work)
    sample = work if total <= cap else work.sample(cap, random_state=42)
    center_lat, center_lon = _stg_bs_map_center(pts)
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=5,
        tiles="CartoDB positron",
        width="100%",
        height="100%",
    )
    fg = folium.FeatureGroup(
        name=f"{layer_title} ({segment_label}): {len(sample):,}/{total:,}",
        show=True,
    )
    ok = bad = 0
    for row in sample.itertuples(index=False):
        raw = getattr(row, wkt_col)
        try:
            geom = wkt.loads(str(raw))
        except Exception:
            bad += 1
            continue
        folium.GeoJson(
            data=geom.__geo_interface__,
            style_function=lambda _, s=style: s,
            tooltip=folium.Tooltip(
                _stg_bs_tooltip(row, wkt_kind=layer_title),
                sticky=True,
            ),
        ).add_to(fg)
        ok += 1
    if ok == 0:
        return None
    fg.add_to(m)
    if add_oktmo:
        _add_stg_bs_oktmo_level1(m, pts, root)
    folium.LayerControl(collapsed=False).add_to(m)
    if bad:
        print(f"{layer_title}: WKT не разобрано: {bad}")
    if len(sample) < total:
        print(f"{layer_title}: показано {len(sample):,} из {total:,} (random sample)")
    return m


def _display_stg_bs_folium_maps_batch(
    pts: pd.DataFrame,
    root: Path,
    *,
    segment_label: str,
) -> None:
    """Быстрый режим для ``uv run mobile nb-stg-bs`` (кластер точек, без ipywidgets)."""
    display_folium_map(
        build_stg_bs_points_map(pts, root, segment_label=segment_label, lite=True),
    )


def display_stg_bs_folium_maps(
    root: Path,
    *,
    active_only: bool = True,
    interactive: bool | None = None,
) -> None:
    """Карта точек lon/lat; в Jupyter — фильтры ipywidgets, в CLI — только кластер."""
    pts, segment = _load_stg_bs_map_df(root, active_only=active_only)
    use_interactive = interactive if interactive is not None else not _notebook_batch_mode()
    if not use_interactive:
        _display_stg_bs_folium_maps_batch(pts, root, segment_label=segment)
        return

    from ipywidgets import Dropdown, interact
    if "telecomstandard" in pts.columns:
        display(
            pts.groupby("telecomstandard", as_index=False)
            .agg(rows=("lac", "count"))
            .sort_values("rows", ascending=False)
        )

    mnc_opts = _stg_bs_filter_options(pts["mnc"]) if "mnc" in pts.columns else [_STG_BS_FILTER_ALL]
    std_opts = (
        _stg_bs_filter_options(pts["telecomstandard"])
        if "telecomstandard" in pts.columns
        else [_STG_BS_FILTER_ALL]
    )
    bt_opts = (
        _stg_bs_filter_options(pts["bs_type"]) if "bs_type" in pts.columns else [_STG_BS_FILTER_ALL]
    )

    @interact(
        mnc=Dropdown(options=mnc_opts, value=mnc_opts[0], description="mnc:"),
        telecomstandard=Dropdown(
            options=std_opts,
            value=std_opts[0],
            description="std:",
        ),
        bs_type=Dropdown(options=bt_opts, value=bt_opts[0], description="bs_type:"),
    )
    def _show_stg_bs_maps(mnc: str, telecomstandard: str, bs_type: str) -> None:
        sub = _apply_stg_bs_filters(
            pts,
            mnc=mnc,
            telecomstandard=telecomstandard,
            bs_type=bs_type,
        )
        print(
            f"Фильтр: mnc={mnc}, telecomstandard={telecomstandard}, bs_type={bs_type} "
            f"| строк: {len(sub):,}"
        )
        if sub.empty:
            print("Нет строк для карты")
            return

        display_folium_map(build_stg_bs_points_map(sub, root, segment_label=segment))


def render_stg_bs_folium_map(root: Path, *, active_only: bool = True) -> folium.Map:
    """Только точки БС без виджетов (обратная совместимость)."""
    pts, segment = _load_stg_bs_map_df(root, active_only=active_only)
    return build_stg_bs_points_map(pts, root, segment_label=segment)


def stg_bs_geometry_quality_frame(latest: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for geom_check in ("geometry.sector_wkt", "geometry.mapinfo_wkt"):
        metrics = _metrics_for_check(latest, geom_check)
        if not metrics:
            continue
        short = geom_check.removeprefix("geometry.")
        for key, label in (
            ("valid_geometry_count", f"{short} valid"),
            ("parse_error_count", f"{short} parse errors"),
            ("invalid_topology_count", f"{short} invalid topology"),
        ):
            if key in metrics:
                rows.append({"metric": label, "count": int(metrics[key])})
    return pd.DataFrame(rows)


def render_stg_bs_dq_overview(latest: pd.DataFrame) -> plt.Figure:
    basic = _metrics_for_check(latest, "dataset_basic")
    temporal = _metrics_for_check(latest, "temporal_consistency")
    fig, axes = plt.subplots(2, 2, figsize=(14, 9))
    plot_check_status(latest, ax=axes[0, 0])
    plot_summary_metrics(latest, ax=axes[0, 1])
    nulls = null_ratio_frame(latest)
    key_nulls = (
        nulls[nulls["field"].isin(("mcc", "mnc", "lac", "cell_id", "lon", "lat"))]
        if not nulls.empty
        else nulls
    )
    plot_null_ratios(key_nulls if not key_nulls.empty else nulls, ax=axes[1, 0])
    if temporal:
        ax = axes[1, 1]
        labels = ["open_rows", "invalid_date_order"]
        values = [
            int(temporal.get("open_rows") or 0),
            int(temporal.get("invalid_date_order_count") or 0),
        ]
        ax.bar(labels, values, color=["#2ca02c", "#d62728"], alpha=0.88)
        ax.set_ylabel("count")
        ax.set_title("temporal_consistency (DQ)")
        for i, value in enumerate(values):
            ax.text(i, value, str(value), ha="center", va="bottom", fontsize=9)
    else:
        plot_count_bars(
            stg_bs_geometry_quality_frame(latest),
            title="WKT geometry (DQ)",
            ax=axes[1, 1],
            color="#17becf",
        )
    if basic:
        fig.suptitle(
            f"DQ STG BS — rows={int(basic.get('row_count') or 0):,}",
            fontsize=13,
            y=1.02,
        )
    else:
        fig.suptitle("DQ STG BS — обзор метрик", fontsize=13, y=1.02)
    fig.tight_layout()
    return fig


def display_stg_bs_parquet_summary(root: Path) -> None:
    bs_parquet = _resolve_parquet(root, stg_bs_output_path())
    if not bs_parquet.exists():
        raise FileNotFoundError(f"Нет parquet: {bs_parquet}")
    df = pd.read_parquet(bs_parquet)
    try:
        rel = bs_parquet.relative_to(root)
    except ValueError:
        rel = bs_parquet
    print(f"stg_bs rows: {len(df):,} | файл: {rel}")
    if "telecomstandard" in df.columns:
        display(df["telecomstandard"].value_counts().head(12).to_frame("rows"))
    if "bs_type" in df.columns:
        print("\n--- bs_type ---")
        display(df["bs_type"].value_counts().head(10).to_frame("rows"))


# --- stg_geo_all DQ charts ---

_STG_GEO_ALL_CARDINALITY_FOCUS = (
    "msisdn",
    "imsi",
    "imei",
    "cgi",
    "source_event_type",
    "bs_type",
    "utc_offset",
    "oktmo_code_1",
    "oktmo_code_2",
)
_STG_GEO_ALL_EVENT_COLORS = {
    "cdr": "#1f77b4",
    "sms": "#ff7f0e",
    "gprs": "#2ca02c",
    "location": "#d62728",
}
_STG_GEO_ALL_MAP_DETAIL_MAX = 2500
_STG_GEO_ALL_MAP_CLUSTER_BATCH_MAX = 8000


def _stg_geo_all_report_date(latest: pd.DataFrame | None) -> date:
    if latest is not None:
        for check in ("dataset_basic", "dataset_presence"):
            metrics = _metrics_for_check(latest, check)
            raw = metrics.get("report_date")
            if raw:
                return date.fromisoformat(str(raw))
    root = DEFAULT_STG_GEO_ALL_OUTPUT_ROOT
    if root.is_dir():
        files = sorted(root.glob("*.parquet"), key=lambda p: p.stat().st_mtime, reverse=True)
        if files:
            try:
                return date.fromisoformat(files[0].stem)
            except ValueError:
                pass
    return DEFAULT_SRC_END_DATE


def stg_geo_all_cardinality_frame(latest: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for _, record in latest.iterrows():
        check = str(record["check"])
        if not check.startswith("cardinality."):
            continue
        metrics = record.get("metrics")
        if not isinstance(metrics, dict):
            continue
        rows.append(
            {
                "field": check.removeprefix("cardinality."),
                "nunique": int(metrics.get("nunique") or 0),
            }
        )
    if not rows:
        return pd.DataFrame(columns=["field", "nunique"])
    out = pd.DataFrame(rows)
    focus = [f for f in _STG_GEO_ALL_CARDINALITY_FOCUS if f in set(out["field"])]
    if focus:
        out = pd.concat([out[out["field"].isin(focus)], out[~out["field"].isin(focus)]], ignore_index=True)
    return out.head(20)


def stg_geo_all_gate_counts_frame(latest: pd.DataFrame) -> pd.DataFrame:
    specs: tuple[tuple[str, str, str], ...] = (
        ("required_fields_presence", "invalid_rows", "invalid required rows"),
        ("coords_range", "invalid_lat_count", "invalid lat"),
        ("coords_range", "invalid_lon_count", "invalid lon"),
        ("temporal_order", "invalid_order_count", "invalid time order"),
        ("event_count_valid", "invalid_event_count_rows", "invalid event_count"),
        ("source_event_type_vocab", "invalid_source_event_type_rows", "invalid event type"),
        ("utc_offset_range", "invalid_utc_offset_rows", "invalid utc_offset"),
        ("bs_type_vocab", "invalid_bs_type_rows", "invalid bs_type"),
        ("duplicate_event_key", "duplicate_rows", "duplicate event key"),
    )
    rows: list[dict[str, Any]] = []
    for check, key, label in specs:
        metrics = _metrics_for_check(latest, check)
        if key not in metrics:
            continue
        hit = latest[latest["check"] == check]
        status = str(hit.iloc[-1]["status"]) if not hit.empty else "missing"
        rows.append({"metric": label, "count": int(metrics[key]), "status": status})
    return pd.DataFrame(rows)


def render_stg_geo_all_dq_overview(latest: pd.DataFrame) -> plt.Figure:
    basic = _metrics_for_check(latest, "dataset_basic")
    required = _metrics_for_check(latest, "required_fields_presence")
    event_mix = stg_event_counts_frame(latest, "distribution.source_event_type")
    report_date = basic.get("report_date") if basic else None
    fig, axes = plt.subplots(2, 2, figsize=(14, 9))
    plot_check_status(latest, ax=axes[0, 0])
    plot_summary_metrics(latest, ax=axes[0, 1])
    if event_mix.empty:
        axes[1, 0].set_title("source_event_type — нет данных")
        axes[1, 0].axis("off")
    else:
        work = event_mix.head(8)
        axes[1, 0].pie(
            work["count"],
            labels=work["label"],
            autopct=lambda pct: f"{pct:.1f}%" if pct >= 3 else "",
            colors=[_STG_GEO_ALL_EVENT_COLORS.get(lbl, "#9467bd") for lbl in work["label"]],
            startangle=90,
        )
        axes[1, 0].set_title("distribution.source_event_type (DQ)")
    ax = axes[1, 1]
    if required:
        rate = float(required.get("required_rate") or 0)
        invalid = int(required.get("invalid_rows") or 0)
        ax.bar(["required_rate", "invalid_rows"], [rate, invalid], color=["#2ca02c", "#d62728"], alpha=0.88)
        ax.set_ylim(0, max(1.05, rate * 1.1))
        ax.set_title("required_fields_presence (DQ)")
        ax.text(0, rate, f"{rate:.4f}", ha="center", va="bottom", fontsize=9)
        ax.text(1, invalid, str(invalid), ha="center", va="bottom", fontsize=9)
    else:
        plot_count_bars(stg_geo_all_gate_counts_frame(latest).head(6), title="Gate counts (DQ)", ax=ax)
    title = (
        f"DQ STG GEO ALL — report_date={report_date}, rows={int(basic.get('row_count') or 0):,}"
        if basic and report_date
        else "DQ STG GEO ALL — обзор метрик"
    )
    fig.suptitle(title, fontsize=13, y=1.02)
    fig.tight_layout()
    return fig


def render_stg_geo_all_dq_gates(latest: pd.DataFrame) -> plt.Figure:
    gates = stg_geo_all_gate_counts_frame(latest)
    fig, ax = plt.subplots(figsize=(10, 5))
    if gates.empty:
        ax.set_title("Gate counts — нет данных")
        ax.axis("off")
        return fig
    colors = [
        "#d62728" if s == "failed" else "#ff7f0e" if s == "warning" else "#2ca02c"
        for s in gates["status"]
    ]
    work = gates.sort_values("count", ascending=True)
    ax.barh(work["metric"], work["count"], color=colors, alpha=0.88)
    ax.set_xlabel("count")
    ax.set_title("Gate-проверки stg_geo_all (DQ)")
    fig.tight_layout()
    return fig


def render_stg_geo_all_dq_nulls(latest: pd.DataFrame) -> plt.Figure:
    nulls = null_ratio_frame(latest)
    fig, ax = plt.subplots(figsize=(10, 6))
    if nulls.empty:
        ax.set_title("nulls.* — нет данных в логе")
        ax.axis("off")
        return fig
    plot_null_ratios(nulls, ax=ax)
    fig.suptitle("Доля null по полям контракта (DQ)", fontsize=12)
    fig.tight_layout()
    return fig


def render_stg_geo_all_dq_cardinality(latest: pd.DataFrame) -> plt.Figure:
    card = stg_geo_all_cardinality_frame(latest)
    fig, ax = plt.subplots(figsize=(10, 5))
    if card.empty:
        ax.set_title("cardinality.* — нет данных")
        ax.axis("off")
        return fig
    work = card.sort_values("nunique", ascending=True)
    ax.barh(work["field"], work["nunique"], color="#9467bd", alpha=0.88)
    ax.set_xlabel("nunique (DQ log)")
    ax.set_title("Кардинальность полей (top-20)")
    fig.tight_layout()
    return fig


def display_stg_geo_all_parquet_summary(root: Path, report_date: date) -> None:
    parquet = _resolve_parquet(root, stg_geo_all_output_path(report_date))
    if not parquet.exists():
        raise FileNotFoundError(f"Нет parquet: {parquet}")
    df = pd.read_parquet(parquet)
    try:
        rel = parquet.relative_to(root)
    except ValueError:
        rel = parquet
    print(f"stg_geo_all ({report_date.isoformat()}): {len(df):,} rows | файл: {rel}")
    if "source_event_type" in df.columns:
        display(
            df["source_event_type"]
            .astype("string")
            .str.lower()
            .value_counts()
            .head(8)
            .to_frame("rows")
        )
    if "utc_offset" in df.columns:
        print("\n--- utc_offset ---")
        display(df["utc_offset"].value_counts().head(12).to_frame("rows"))


def render_stg_geo_all_parquet_profile(root: Path, report_date: date) -> plt.Figure:
    parquet = _resolve_parquet(root, stg_geo_all_output_path(report_date))
    if not parquet.exists():
        raise FileNotFoundError(f"Нет parquet: {parquet}")
    import pyarrow.parquet as pq

    available = set(pq.read_schema(parquet).names)
    profile_cols = [c for c in ("source_event_type", "utc_offset", "event_count", "bs_type") if c in available]
    df = pd.read_parquet(parquet, columns=profile_cols) if profile_cols else pd.read_parquet(parquet)
    fig, axes = plt.subplots(1, 3, figsize=(15, 4.5))
    if "source_event_type" in df.columns:
        mix = df["source_event_type"].astype("string").str.lower().value_counts().head(8)
        axes[0].pie(
            mix.values,
            labels=mix.index.astype(str),
            autopct=lambda pct: f"{pct:.1f}%" if pct >= 3 else "",
            colors=[_STG_GEO_ALL_EVENT_COLORS.get(lbl, "#9467bd") for lbl in mix.index.astype(str)],
            startangle=90,
        )
        axes[0].set_title("source_event_type")
    else:
        axes[0].axis("off")
    if "utc_offset" in df.columns:
        tz = df["utc_offset"].dropna().astype(int).value_counts().sort_index()
        axes[1].bar(tz.index.astype(str), tz.values, color="#2563eb", alpha=0.88)
        axes[1].set_xlabel("utc_offset")
        axes[1].set_title("UTC offset")
        plt.setp(axes[1].get_xticklabels(), rotation=25, ha="right")
    else:
        axes[1].axis("off")
    if "event_count" in df.columns:
        cnt = pd.to_numeric(df["event_count"], errors="coerce").dropna()
        if len(cnt):
            axes[2].hist(cnt.clip(upper=cnt.quantile(0.99)), bins=30, color="#ff7f0e", alpha=0.88)
            axes[2].set_xlabel("event_count (≤ p99)")
            axes[2].set_title("event_count")
        else:
            axes[2].axis("off")
    else:
        axes[2].axis("off")
    fig.suptitle(f"Профиль parquet stg_geo_all ({report_date.isoformat()})", fontsize=12, y=1.02)
    fig.tight_layout()
    return fig


def render_stg_geo_all_folium_map(
    root: Path,
    report_date: date,
    *,
    lite: bool | None = None,
) -> folium.Map:
    """Точки событий lon/lat, кластер и слои по ``source_event_type``."""
    if lite is None:
        lite = _notebook_batch_mode()
    parquet = _resolve_parquet(root, stg_geo_all_output_path(report_date))
    if not parquet.exists():
        raise FileNotFoundError(f"Нет parquet: {parquet}")

    df = pd.read_parquet(parquet, columns=["lat", "lon", "source_event_type", "msisdn", "cgi"])
    pts = df.copy()
    pts["lon"] = pd.to_numeric(pts["lon"], errors="coerce")
    pts["lat"] = pd.to_numeric(pts["lat"], errors="coerce")
    pts = pts.loc[pts["lon"].notna() & pts["lat"].notna()]
    pts = pts[pts["lon"].between(-180, 180) & pts["lat"].between(-90, 90)]
    if pts.empty:
        raise ValueError("Нет валидных lon/lat для карты stg_geo_all")
    if "source_event_type" in pts.columns:
        pts["source_event_type"] = pts["source_event_type"].astype("string").str.lower().fillna("unknown")
    if lite and len(pts) > _STG_GEO_ALL_MAP_CLUSTER_BATCH_MAX:
        pts = pts.sample(_STG_GEO_ALL_MAP_CLUSTER_BATCH_MAX, random_state=42)
        print(f"nb-batch: сэмпл точек для кластера: {len(pts):,}")

    center_lat = float(pts["lat"].mean())
    center_lon = float(pts["lon"].mean())
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=5,
        tiles="CartoDB positron",
        width="100%",
        height="100%",
    )
    all_points = pts[["lat", "lon"]].astype(float).values.tolist()
    fg_all = folium.FeatureGroup(
        name=f"stg_geo_all {report_date.isoformat()} (cluster): {len(all_points):,}",
        show=True,
    )
    FastMarkerCluster(data=all_points).add_to(fg_all)
    fg_all.add_to(m)

    if lite:
        folium.LayerControl(collapsed=False).add_to(m)
        return m

    detail_cap = _STG_GEO_ALL_MAP_DETAIL_MAX
    if "source_event_type" in pts.columns:
        for evt, group in pts.groupby("source_event_type", dropna=False):
            evt_name = str(evt)
            color = _STG_GEO_ALL_EVENT_COLORS.get(evt_name, "#9467bd")
            sample = (
                group
                if len(group) <= detail_cap
                else group.sample(detail_cap, random_state=42)
            )
            fg = folium.FeatureGroup(
                name=f"{evt_name}: {len(sample):,}/{len(group):,}",
                show=False,
            )
            for row in sample.itertuples(index=False):
                tip = (
                    f"<b>{row.msisdn}</b><br>cgi={row.cgi}<br>"
                    f"type={evt_name}"
                )
                folium.CircleMarker(
                    location=[float(row.lat), float(row.lon)],
                    radius=2,
                    color=color,
                    weight=1,
                    fill=True,
                    fill_opacity=0.65,
                    tooltip=folium.Tooltip(tip, sticky=False),
                ).add_to(fg)
            fg.add_to(m)

    folium.LayerControl(collapsed=False).add_to(m)
    return m


# --- stg_event DQ charts ---

_STG_EVENT_SOURCES = ("central", "far-east")
_STG_EVENT_SOURCE_LABELS = {"central": "Central", "far-east": "Far East"}
_STG_EVENT_STG_GATE_SUFFIXES = (
    "event",
    "event_name",
    "event_code_name_alignment",
    "location",
    "location_compressible",
)
_STG_EVENT_RATE_KEYS: tuple[tuple[str, str], ...] = (
    ("event_timestamp_parseable", "parseable_rate"),
    ("event.stg_contract.event", "valid_event_rate"),
    ("event.stg_contract.event_name", "valid_event_name_rate"),
    ("event.stg_contract.event_code_name_alignment", "aligned_rate"),
    ("event.stg_contract.location", "location_mcc_mnc_rate"),
    ("event.stg_contract.location_compressible", "compressible_location_rate"),
)


def stg_event_source_coverage_frame(latest: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for _, record in latest.iterrows():
        if record["check"] != "source.coverage":
            continue
        metrics = record.get("metrics")
        if not isinstance(metrics, dict):
            continue
        source_id = str(metrics.get("source_id") or "_unknown")
        rows.append(
            {
                "source_id": source_id,
                "label": _STG_EVENT_SOURCE_LABELS.get(source_id, source_id),
                "row_count_total": int(metrics.get("row_count_total") or 0),
                "parquet_files": int(metrics.get("parquet_files") or 0),
            }
        )
    return pd.DataFrame(rows)


def stg_event_counts_frame(latest: pd.DataFrame, check: str) -> pd.DataFrame:
    metrics = _metrics_for_check(latest, check)
    counts = metrics.get("counts")
    if not isinstance(counts, dict):
        distribution = metrics.get("distribution_counts")
        if isinstance(distribution, dict):
            counts = distribution
        else:
            return pd.DataFrame(columns=["label", "count"])
    rows = [{"label": str(key), "count": int(value)} for key, value in counts.items()]
    return pd.DataFrame(rows).sort_values("count", ascending=False)


def stg_event_null_rates_frame(latest: pd.DataFrame) -> pd.DataFrame:
    metrics = _metrics_for_check(latest, "null_rates")
    rates = metrics.get("null_rate_by_column")
    if not isinstance(rates, dict):
        return pd.DataFrame(columns=["field", "null_ratio"])
    return pd.DataFrame(
        [{"field": str(key), "null_ratio": float(value)} for key, value in rates.items()]
    ).sort_values("null_ratio", ascending=True)


def stg_event_gate_rates_frame(latest: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for check, metric_key in _STG_EVENT_RATE_KEYS:
        value = _metric_scalar(latest, check, metric_key)
        if value is None:
            continue
        hit = latest[latest["check"] == check]
        status = str(hit.iloc[-1]["status"]) if not hit.empty else "missing"
        rows.append(
            {
                "check": check.split(".")[-1],
                "metric": metric_key,
                "value": float(value),
                "status": status,
            }
        )
    ec_metrics = _metrics_for_check(latest, "event_count_valid")
    if ec_metrics and "aggregated_share" in ec_metrics:
        hit = latest[latest["check"] == "event_count_valid"]
        status = str(hit.iloc[-1]["status"]) if not hit.empty else "missing"
        rows.append(
            {
                "check": "aggregated_share",
                "metric": "aggregated_share",
                "value": float(ec_metrics["aggregated_share"]),
                "status": status,
            }
        )
    return pd.DataFrame(rows)


def stg_event_stg_contract_gate_frame(latest: pd.DataFrame, *, source_id: str | None = None) -> pd.DataFrame:
    prefix = (
        f"source.{source_id}.event.stg_contract."
        if source_id
        else "event.stg_contract."
    )
    rows: list[dict[str, Any]] = []
    for gate in _STG_EVENT_STG_GATE_SUFFIXES:
        full = f"{prefix}{gate}"
        hit = latest[latest["check"] == full]
        status = str(hit.iloc[-1]["status"]) if not hit.empty else "missing"
        rows.append({"gate": gate, "status": status})
    return pd.DataFrame(rows)


def _plot_stg_event_source_coverage(coverage: pd.DataFrame, *, ax: plt.Axes | None = None) -> plt.Figure:
    if ax is None:
        fig, ax = plt.subplots(figsize=(7, 4))
    else:
        fig = ax.figure
    if coverage.empty:
        ax.set_title("source.coverage — нет данных")
        ax.axis("off")
        return fig
    ax.bar(coverage["label"], coverage["row_count_total"], color="#1f77b4", alpha=0.88)
    ax.set_ylabel("row_count_total")
    ax.set_title("Строки по ЦОД (source.coverage)")
    for i, row in enumerate(coverage.itertuples()):
        ax.text(i, row.row_count_total, f"{row.row_count_total:,}", ha="center", va="bottom", fontsize=8)
    fig.tight_layout()
    return fig


def _plot_stg_event_counts_pie(counts: pd.DataFrame, *, title: str, ax: plt.Axes | None = None) -> plt.Figure:
    if ax is None:
        fig, ax = plt.subplots(figsize=(6, 4))
    else:
        fig = ax.figure
    if counts.empty:
        ax.set_title(f"{title}\n(нет данных)")
        ax.axis("off")
        return fig
    colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#9467bd", "#8c564b"]
    ax.pie(
        counts["count"],
        labels=counts["label"],
        autopct=lambda pct: f"{pct:.1f}%" if pct >= 3 else "",
        colors=colors[: len(counts)],
        startangle=90,
    )
    ax.set_title(title)
    fig.tight_layout()
    return fig


def _plot_stg_event_gate_rates(rates: pd.DataFrame, *, ax: plt.Axes | None = None) -> plt.Figure:
    if ax is None:
        fig, ax = plt.subplots(figsize=(8, 4))
    else:
        fig = ax.figure
    if rates.empty:
        ax.set_title("Gate rates — нет данных")
        ax.axis("off")
        return fig
    work = rates.sort_values("value", ascending=True)
    colors = [
        "#d62728" if status == "failed" else "#ff7f0e" if status == "warning" else "#2ca02c"
        for status in work["status"]
    ]
    ax.barh(work["check"], work["value"] * 100, color=colors, alpha=0.88)
    ax.set_xlabel("rate, %")
    ax.set_title("Ключевые gate (агрегат за день)")
    ax.axvline(99, color="gray", ls="--", lw=0.8)
    fig.tight_layout()
    return fig


def _plot_stg_event_stg_gate_grid(gates: pd.DataFrame, *, ax: plt.Axes | None = None) -> plt.Figure:
    if ax is None:
        fig, ax = plt.subplots(figsize=(8, 2.5))
    else:
        fig = ax.figure
    if gates.empty:
        ax.set_title("event.stg_contract — нет данных")
        ax.axis("off")
        return fig
    matrix = gates.set_index("gate")["status"].to_frame().T
    codes = {"failed": 0, "warning": 1, "ok": 2, "missing": 3}
    values = matrix.map(lambda status: codes.get(str(status), 3)).to_numpy(dtype=float)
    im = ax.imshow(values, aspect="auto", cmap=plt.cm.RdYlGn, vmin=0, vmax=2)
    ax.set_xticks(range(len(matrix.columns)))
    ax.set_xticklabels(matrix.columns, rotation=25, ha="right")
    ax.set_yticks([0])
    ax.set_yticklabels(["aggregate"])
    for j, gate in enumerate(matrix.columns):
        ax.text(j, 0, matrix.iloc[0, j], ha="center", va="center", fontsize=8, color="black")
    ax.set_title("event.stg_contract.* (агрегат)")
    fig.colorbar(im, ax=ax, fraction=0.04, pad=0.02, ticks=[0, 1, 2], label="failed → ok")
    fig.tight_layout()
    return fig


def render_stg_event_dq_overview(latest: pd.DataFrame) -> plt.Figure:
    coverage = stg_event_source_coverage_frame(latest)
    agg = _metrics_for_check(latest, "coverage")
    event_mix = stg_event_counts_frame(latest, "event_distribution")
    report_date = agg.get("report_date") if agg else None
    fig, axes = plt.subplots(2, 2, figsize=(14, 9))
    plot_check_status(latest, ax=axes[0, 0])
    _plot_stg_event_source_coverage(coverage, ax=axes[0, 1])
    _plot_stg_event_counts_pie(event_mix, title="event_distribution (код OCC)", ax=axes[1, 0])
    plot_summary_metrics(latest, ax=axes[1, 1])
    title = f"DQ STG EVENT — report_date={report_date}" if report_date else "DQ STG EVENT — обзор"
    fig.suptitle(title, fontsize=13, y=1.02)
    fig.tight_layout()
    return fig


def render_stg_event_dq_quality(latest: pd.DataFrame) -> plt.Figure:
    rates = stg_event_gate_rates_frame(latest)
    gates = stg_event_stg_contract_gate_frame(latest)
    nulls = stg_event_null_rates_frame(latest)
    buckets = stg_event_counts_frame(latest, "distribution.event_count_bucket")
    fig, axes = plt.subplots(2, 2, figsize=(14, 9))
    _plot_stg_event_gate_rates(rates, ax=axes[0, 0])
    _plot_stg_event_stg_gate_grid(gates, ax=axes[0, 1])
    plot_null_ratios(nulls, ax=axes[1, 0])
    _plot_stg_event_counts_pie(buckets, title="event_count_bucket", ax=axes[1, 1])
    fig.suptitle("Контракт, gate и null_rates (агрегат)", fontsize=12, y=1.02)
    fig.tight_layout()
    return fig


def render_stg_event_dq_by_source(latest: pd.DataFrame) -> plt.Figure:
    fig, axes = plt.subplots(1, len(_STG_EVENT_SOURCES), figsize=(14, 4))
    if len(_STG_EVENT_SOURCES) == 1:
        axes = [axes]
    for ax, source_id in zip(axes, _STG_EVENT_SOURCES, strict=True):
        mix = stg_event_counts_frame(latest, f"source.{source_id}.event_distribution")
        label = _STG_EVENT_SOURCE_LABELS.get(source_id, source_id)
        _plot_stg_event_counts_pie(mix, title=f"{label}: event mix", ax=ax)
    fig.suptitle("Микс событий по ЦОД (DQ-лог)", fontsize=12, y=1.02)
    fig.tight_layout()
    return fig


def _stg_binding_report_month(latest: pd.DataFrame | None) -> date:
    if latest is not None:
        for check in ("dataset_basic", "dataset_presence"):
            metrics = _metrics_for_check(latest, check)
            raw = metrics.get("report_date")
            if raw:
                return date.fromisoformat(str(raw))
    for resolver in (stg_msisdn_imei_output_path, stg_msisdn_imsi_output_path):
        path = resolver(DEFAULT_SRC_END_DATE)
        parent = path.parent
        if parent.is_dir():
            files = sorted(parent.glob("*.parquet"), key=lambda p: p.stat().st_mtime, reverse=True)
            if files:
                try:
                    return date.fromisoformat(files[0].stem)
                except ValueError:
                    pass
    return DEFAULT_SRC_END_DATE.replace(day=1)


def _binding_gate_counts_frame(
    latest: pd.DataFrame,
    specs: tuple[tuple[str, str, str], ...],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for check, key, label in specs:
        metrics = _metrics_for_check(latest, check)
        if key not in metrics:
            continue
        hit = latest[latest["check"] == check]
        status = str(hit.iloc[-1]["status"]) if not hit.empty else "missing"
        rows.append({"metric": label, "count": int(metrics[key]), "status": status})
    return pd.DataFrame(rows)


_STG_MSISDN_IMEI_GATE_SPECS: tuple[tuple[str, str, str], ...] = (
    ("temporal_order", "invalid_order_count", "invalid time order"),
    ("msisdn_format", "invalid_msisdn_rows", "invalid msisdn"),
    ("imei_format", "invalid_imei_rows", "invalid imei"),
    ("duplicate_rows", "duplicate_rows", "duplicate rows"),
    ("interval_overlap_same_pair", "overlapping_interval_rows", "overlapping intervals"),
    ("interval_mergeable_gap", "mergeable_adjacent_segments", "mergeable segments"),
)

_STG_MSISDN_IMSI_GATE_SPECS: tuple[tuple[str, str, str], ...] = (
    ("temporal_order", "invalid_order_count", "invalid time order"),
    ("msisdn_format", "invalid_msisdn_rows", "invalid msisdn"),
    ("imsi_format", "invalid_imsi_rows", "invalid imsi"),
    ("operator_id_valid", "invalid_operator_id_rows", "invalid operator_id"),
    ("operator_id_imsi_alignment", "misaligned_rows", "operator_id ≠ IMSI MNC"),
    ("operator_id_non_ru_imsi", "non_ru_rows_with_operator_id", "non-RU with operator_id"),
    ("duplicate_rows", "duplicate_rows", "duplicate rows"),
    ("interval_overlap_same_triple", "overlapping_interval_rows", "overlapping intervals"),
    ("interval_mergeable_gap", "mergeable_adjacent_segments", "mergeable segments"),
)


def render_stg_binding_dq_gates(
    latest: pd.DataFrame,
    gates: pd.DataFrame,
    *,
    title: str,
) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(10, 5))
    if gates.empty:
        ax.set_title(f"{title} — нет данных")
        ax.axis("off")
        return fig
    colors = [
        "#d62728" if s == "failed" else "#ff7f0e" if s == "warning" else "#2ca02c"
        for s in gates["status"]
    ]
    work = gates.sort_values("count", ascending=True)
    ax.barh(work["metric"], work["count"], color=colors, alpha=0.88)
    ax.set_xlabel("count")
    ax.set_title(title)
    fig.tight_layout()
    return fig


def _render_stg_binding_dq_overview(
    latest: pd.DataFrame,
    *,
    title: str,
    gate_specs: tuple[tuple[str, str, str], ...],
    extra_metric: str | None = None,
) -> plt.Figure:
    basic = _metrics_for_check(latest, "dataset_basic")
    fig, axes = plt.subplots(2, 2, figsize=(14, 9))
    plot_check_status(latest, ax=axes[0, 0])
    plot_summary_metrics(latest, ax=axes[0, 1])
    plot_null_ratios(null_ratio_frame(latest), ax=axes[1, 0])
    gates = _binding_gate_counts_frame(latest, gate_specs)
    if gates.empty:
        axes[1, 1].set_title("Gate counts — нет данных")
        axes[1, 1].axis("off")
    else:
        colors = [
            "#d62728" if s == "failed" else "#ff7f0e" if s == "warning" else "#2ca02c"
            for s in gates["status"]
        ]
        work = gates.sort_values("count", ascending=True)
        axes[1, 1].barh(work["metric"], work["count"], color=colors, alpha=0.88)
        axes[1, 1].set_xlabel("count")
        axes[1, 1].set_title("Gate-проверки (DQ)")
    rows = int(basic.get("row_count") or 0)
    msisdn = int(basic.get("distinct_msisdn") or 0)
    suffix = ""
    if extra_metric and extra_metric in basic:
        suffix = f", {extra_metric}={int(basic[extra_metric]):,}"
    month = basic.get("report_date", "")
    fig.suptitle(
        f"{title} — month={month}, rows={rows:,}, msisdn={msisdn:,}{suffix}",
        fontsize=12,
        y=1.02,
    )
    fig.tight_layout()
    return fig


def render_stg_msisdn_imei_dq_overview(latest: pd.DataFrame) -> plt.Figure:
    return _render_stg_binding_dq_overview(
        latest,
        title="DQ STG MSISDN IMEI",
        gate_specs=_STG_MSISDN_IMEI_GATE_SPECS,
    )


def render_stg_msisdn_imsi_operator_dq_overview(latest: pd.DataFrame) -> plt.Figure:
    return _render_stg_binding_dq_overview(
        latest,
        title="DQ STG MSISDN IMSI",
        gate_specs=_STG_MSISDN_IMSI_GATE_SPECS,
        extra_metric="distinct_operator_id",
    )


def _print_per_msisdn_interval_stats(per_msisdn: pd.Series) -> None:
    if per_msisdn.empty:
        print("интервалов на msisdn: нет данных (пустой набор)")
        return
    print(
        f"интервалов на msisdn: min={int(per_msisdn.min())}, "
        f"median={float(per_msisdn.median()):.1f}, max={int(per_msisdn.max())}"
    )


def display_stg_msisdn_imei_parquet_summary(root: Path, report_month: date) -> None:
    parquet = _resolve_parquet(root, stg_msisdn_imei_output_path(report_month))
    if not parquet.exists():
        raise FileNotFoundError(f"Нет parquet: {parquet}")
    df = pd.read_parquet(parquet)
    try:
        rel = parquet.relative_to(root)
    except ValueError:
        rel = parquet
    print(f"stg_msisdn_imei ({report_month.isoformat()}): {len(df):,} rows | файл: {rel}")
    if df.empty:
        print("distinct msisdn: 0, distinct imei: 0")
        _print_per_msisdn_interval_stats(pd.Series(dtype="int64"))
        return
    print(f"distinct msisdn: {df['msisdn'].nunique():,}, distinct imei: {df['imei'].nunique():,}")
    per_msisdn = df.groupby("msisdn", sort=False).size()
    _print_per_msisdn_interval_stats(per_msisdn)
    display(per_msisdn.value_counts().head(10).to_frame("msisdn_count"))


def display_stg_msisdn_imsi_parquet_summary(root: Path, report_month: date) -> None:
    parquet = _resolve_parquet(root, stg_msisdn_imsi_output_path(report_month))
    if not parquet.exists():
        raise FileNotFoundError(f"Нет parquet: {parquet}")
    df = pd.read_parquet(parquet)
    try:
        rel = parquet.relative_to(root)
    except ValueError:
        rel = parquet
    print(f"stg_msisdn_imsi ({report_month.isoformat()}): {len(df):,} rows | файл: {rel}")
    if df.empty:
        print("distinct msisdn: 0, imsi: 0, operator_id: 0")
        _print_per_msisdn_interval_stats(pd.Series(dtype="int64"))
        return
    print(
        f"distinct msisdn: {df['msisdn'].nunique():,}, imsi: {df['imsi'].nunique():,}, "
        f"operator_id: {df['operator_id'].nunique():,}"
    )
    display(df["operator_id"].value_counts().head(12).to_frame("interval_rows"))
    per_msisdn = df.groupby("msisdn", sort=False).size()
    _print_per_msisdn_interval_stats(per_msisdn)


def render_stg_msisdn_imei_parquet_profile(root: Path, report_month: date) -> plt.Figure:
    parquet = _resolve_parquet(root, stg_msisdn_imei_output_path(report_month))
    if not parquet.exists():
        raise FileNotFoundError(f"Нет parquet: {parquet}")
    df = pd.read_parquet(parquet, columns=["msisdn", "imei"])
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    if df.empty:
        for ax in axes:
            ax.set_title("нет данных (пустой parquet)")
            ax.axis("off")
        fig.suptitle(f"stg_msisdn_imei — {report_month.isoformat()}", fontsize=12, y=1.02)
        fig.tight_layout()
        return fig
    per_msisdn = df.groupby("msisdn", sort=False).size()
    tac = df["imei"].astype("string").str.replace(r"\D+", "", regex=True).str.slice(0, 8)
    tac_vc = tac.value_counts().head(12)
    bins = [1, 2, 3, 5, 10, 20, 50, max(int(per_msisdn.max()), 51)]
    axes[0].hist(per_msisdn, bins=sorted(set(bins)), color="#1f77b4", alpha=0.85, edgecolor="white")
    axes[0].set_xlabel("интервалов на msisdn")
    axes[0].set_ylabel("msisdn")
    axes[0].set_title("Распределение числа интервалов на MSISDN")
    if tac_vc.empty:
        axes[1].set_title("IMEI TAC — нет данных")
        axes[1].axis("off")
    else:
        axes[1].barh(tac_vc.index.astype(str), tac_vc.values, color="#ff7f0e", alpha=0.88)
        axes[1].set_xlabel("interval rows")
        axes[1].set_title("Top IMEI TAC (первые 8 цифр)")
    fig.suptitle(f"stg_msisdn_imei — {report_month.isoformat()}", fontsize=12, y=1.02)
    fig.tight_layout()
    return fig


def render_stg_msisdn_imsi_parquet_profile(root: Path, report_month: date) -> plt.Figure:
    parquet = _resolve_parquet(root, stg_msisdn_imsi_output_path(report_month))
    if not parquet.exists():
        raise FileNotFoundError(f"Нет parquet: {parquet}")
    df = pd.read_parquet(parquet, columns=["msisdn", "operator_id", "imsi"])
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    if df.empty:
        for ax in axes:
            ax.set_title("нет данных (пустой parquet)")
            ax.axis("off")
        fig.suptitle(f"stg_msisdn_imsi — {report_month.isoformat()}", fontsize=12, y=1.02)
        fig.tight_layout()
        return fig
    per_msisdn = df.groupby("msisdn", sort=False).size()
    op_vc = df["operator_id"].value_counts().head(12)
    bins = [1, 2, 3, 5, 10, 20, 50, max(int(per_msisdn.max()), 51)]
    axes[0].hist(per_msisdn, bins=sorted(set(bins)), color="#2ca02c", alpha=0.85, edgecolor="white")
    axes[0].set_xlabel("интервалов на msisdn")
    axes[0].set_ylabel("msisdn")
    axes[0].set_title("Распределение числа интервалов на MSISDN")
    if op_vc.empty:
        axes[1].set_title("operator_id — нет данных")
        axes[1].axis("off")
    else:
        axes[1].barh(op_vc.index.astype(str), op_vc.values, color="#9467bd", alpha=0.88)
        axes[1].set_xlabel("interval rows")
        axes[1].set_title("Top operator_id")
    fig.suptitle(f"stg_msisdn_imsi — {report_month.isoformat()}", fontsize=12, y=1.02)
    fig.tight_layout()
    return fig
