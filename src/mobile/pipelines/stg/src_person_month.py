"""Чтение всех успешных срезов ``src_person`` за календарный месяц."""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import pandas as pd

from mobile.project_paths import SRC_PERSON_LAYOUT_TEMPLATE, SRC_PERSON_SUCCESS_FLAG, resolve_project_path

logger = logging.getLogger(__name__)

SRC_PERSON_READ_COLUMNS = [
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


def resolve_person_layout_root(layout: str) -> Path:
    path = resolve_project_path(layout)
    parts = path.parts
    idx = next((i for i, part in enumerate(parts) if "{" in part and "}" in part), None)
    if idx is None:
        return path.parent if path.suffix else path
    return Path(*parts[:idx])


def parse_load_day(day_dir: Path, *, year: int, month: int) -> date | None:
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


def list_success_person_parquets_in_period(
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
        load_day = parse_load_day(day_dir, year=year, month=month)
        if load_day is None or load_day < period_start or load_day > period_end:
            continue
        if not (day_dir / SRC_PERSON_SUCCESS_FLAG).exists():
            continue
        parquet_path = day_dir / "person.parquet"
        if parquet_path.exists():
            candidates.append((load_day, parquet_path))
    return candidates


def read_src_person_month(
    *,
    report_month: date,
    period_start: date,
    period_end: date,
    src_person_path: str | Path | None,
    mode: str = "all_snapshots",
) -> tuple[pd.DataFrame, list[date]]:
    """Чтение ``src_person`` за месяц.

    ``all_snapshots`` — все срезы (для MNP в ``stg_msisdn_imsi``).
    ``latest_snapshot`` — только последний ``load_day`` с ``_SUCCESS`` (для ``stg_person``).
    """
    if src_person_path is not None:
        resolved = resolve_project_path(src_person_path)
        if resolved.is_file():
            try:
                frame = pd.read_parquet(resolved, columns=SRC_PERSON_READ_COLUMNS)
            except Exception:
                logger.exception("read_src_person_month: failed to read %s", resolved)
                return pd.DataFrame(columns=SRC_PERSON_READ_COLUMNS), []
            return frame, []

    root = (
        resolve_person_layout_root(SRC_PERSON_LAYOUT_TEMPLATE)
        if src_person_path is None
        else resolve_project_path(src_person_path)
    )
    candidates = list_success_person_parquets_in_period(
        root=root,
        period_start=period_start,
        period_end=period_end,
    )
    if not candidates:
        raise FileNotFoundError(
            f"No src_person snapshots with {SRC_PERSON_SUCCESS_FLAG!r} for "
            f"{period_start.isoformat()}..{period_end.isoformat()} under {root}"
        )

    if mode in ("latest_snapshot", "person_union"):
        latest_day, latest_path = max(candidates, key=lambda item: item[0])
        latest = pd.read_parquet(latest_path, columns=SRC_PERSON_READ_COLUMNS)
        logger.info(
            "read_src_person_month %s: load_day=%s rows=%s",
            mode,
            latest_day.isoformat(),
            len(latest),
        )
        return latest, [latest_day]

    parts: list[pd.DataFrame] = []
    load_days: list[date] = []
    for load_day, parquet_path in candidates:
        try:
            parts.append(pd.read_parquet(parquet_path, columns=SRC_PERSON_READ_COLUMNS))
            load_days.append(load_day)
        except Exception:
            logger.warning("read_src_person_month: skip unreadable %s", parquet_path)

    if not parts:
        return pd.DataFrame(columns=SRC_PERSON_READ_COLUMNS), []

    combined = pd.concat(parts, ignore_index=True)
    logger.info(
        "read_src_person_month: %s rows from %s load_day files (%s .. %s)",
        len(combined),
        len(load_days),
        min(load_days).isoformat() if load_days else "?",
        max(load_days).isoformat() if load_days else "?",
    )
    return combined, load_days


def _read_person_union(candidates: list[tuple[date, Path]]) -> tuple[pd.DataFrame, list[date]]:
    latest_day, latest_path = max(candidates, key=lambda item: item[0])
    latest = pd.read_parquet(latest_path, columns=SRC_PERSON_READ_COLUMNS)
    load_days = [latest_day]

    def _pair_set(frame: pd.DataFrame) -> dict[str, set[tuple[str, str]]]:
        out: dict[str, set[tuple[str, str]]] = {}
        subset = frame.dropna(subset=["isdn"])
        for isdn, group in subset.groupby("isdn", sort=False):
            pairs = {
                (str(op), str(imsi))
                for op, imsi in zip(group["operator_Id"], group["imsi"], strict=True)
                if pd.notna(op) and pd.notna(imsi)
            }
            out[str(isdn)] = pairs
        return out

    latest_pairs = _pair_set(latest)

    extras: list[pd.DataFrame] = []
    for load_day, parquet_path in candidates:
        if parquet_path == latest_path:
            continue
        try:
            part = pd.read_parquet(parquet_path, columns=SRC_PERSON_READ_COLUMNS)
        except Exception:
            logger.warning("read_src_person_month: skip unreadable %s", parquet_path)
            continue
        load_days.append(load_day)
        part = part.dropna(subset=["isdn"])
        keep_rows: list[int] = []
        for idx, row in part.iterrows():
            isdn = str(row["isdn"])
            if isdn not in latest_pairs:
                continue
            pair = (str(row["operator_Id"]), str(row["imsi"]))
            if pd.isna(row["operator_Id"]) or pd.isna(row["imsi"]):
                continue
            if pair not in latest_pairs[isdn]:
                keep_rows.append(int(idx))
        if keep_rows:
            extras.append(part.loc[keep_rows])

    combined = latest if not extras else pd.concat([latest, *extras], ignore_index=True)
    logger.info(
        "read_src_person_month person_union: latest=%s rows=%s, extras=%s, total=%s, load_days=%s",
        latest_day.isoformat(),
        len(latest),
        sum(len(part) for part in extras),
        len(combined),
        len(load_days),
    )
    return combined, load_days
