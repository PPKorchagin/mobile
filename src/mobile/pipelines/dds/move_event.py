"""Перенос ``dds_event`` в витрину DDS: ``event/{dc}`` → ``event_dds/{date}/{dc}.parquet``."""

from __future__ import annotations

import logging
import os
import shutil
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path
from typing import Any

from mobile.command_timing import append_command_metrics, timed_stage
from mobile.project_paths import mobile_datacenter_ids, dds_event_dds_output_path, dds_event_output_path

logger = logging.getLogger(__name__)

_COPY_WORKERS = len(mobile_datacenter_ids())


def _fast_copy(src: Path, dst: Path) -> tuple[str, int]:
    """Скопировать parquet: hardlink на том же томе, иначе ``copyfile`` без метаданных."""
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        dst.unlink()
    try:
        if src.stat().st_dev == dst.parent.stat().st_dev:
            os.link(src, dst)
            return "hardlink", int(dst.stat().st_size)
    except OSError:
        pass
    shutil.copyfile(src, dst)
    return "copyfile", int(dst.stat().st_size)


def _move_one_datacenter(report_date: date, dc: str) -> dict[str, Any]:
    src = dds_event_output_path(dc, report_date)
    dst = dds_event_dds_output_path(dc, report_date)
    entry: dict[str, Any] = {
        "source_id": dc,
        "source_path": str(src),
        "output_path": str(dst),
    }
    if not src.exists():
        entry["status"] = "missing_source"
        logger.warning(
            "build-dds-move-event skip: missing source source_id=%s report_date=%s path=%s",
            dc,
            report_date.isoformat(),
            src,
        )
        return entry
    method, nbytes = _fast_copy(src, dst)
    entry["status"] = "ok"
    entry["copy_method"] = method
    entry["bytes"] = nbytes
    logger.info(
        "build-dds-move-event source_id=%s report_date=%s method=%s src=%s dst=%s bytes=%s",
        dc,
        report_date.isoformat(),
        method,
        src,
        dst,
        nbytes,
    )
    return entry


def run_move(report_date: date) -> dict[str, Any]:
    """Скопировать ``events.parquet`` каждого ЦОД за ``report_date`` в ``event_dds``."""
    perf: dict[str, Any] = {}
    started = time.perf_counter()
    dcs = mobile_datacenter_ids()

    with timed_stage("move_sec", perf):
        with ThreadPoolExecutor(max_workers=_COPY_WORKERS) as pool:
            futures = [pool.submit(_move_one_datacenter, report_date, dc) for dc in dcs]
            moves = [fut.result() for fut in as_completed(futures)]
        moves.sort(key=lambda m: str(m["source_id"]))

    files_written = sum(1 for m in moves if m.get("status") == "ok")
    stats: dict[str, Any] = {
        "report_date": report_date.isoformat(),
        "datacenters": list(dcs),
        "files_written": int(files_written),
        "moves": moves,
    }
    perf["elapsed_total_sec"] = round(time.perf_counter() - started, 4)
    append_command_metrics(command="build-dds-move-event", metrics={**stats, **perf})
    logger.info("build-dds-move-event completed: %s", stats)
    return stats
