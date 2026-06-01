"""Утилиты DQ-ноутбуков ``src/mobile/pipelines/nb/``: логи, matplotlib и folium-карты STG."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import folium
import matplotlib.pyplot as plt
import pandas as pd
from branca.colormap import StepColormap
from IPython.display import display
from shapely import wkt

from folium.plugins import FastMarkerCluster
from mobile.project_paths import (
    DEFAULT_BS_LAYOUT,
    DEFAULT_STG_OKSM_OUTPUT_PATH,
    DEFAULT_STG_OKTMO_OUTPUT_PATH,
    DEFAULT_STG_TAC_OUTPUT_PATH,
    DEFAULT_STG_TIME_ZONES_OUTPUT_PATH,
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
    m = folium.Map(location=[center_lat, center_lon], zoom_start=5, tiles="CartoDB positron")

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
