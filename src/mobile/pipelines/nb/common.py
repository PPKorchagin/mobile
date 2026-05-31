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
