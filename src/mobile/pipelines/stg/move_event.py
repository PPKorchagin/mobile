"""Перенос ``stg_event`` в витрину DDS: ``event/{dc}`` → ``event_dds/{date}/{dc}.parquet``."""

from __future__ import annotations

import logging
import shutil
import time
from datetime import date
from pathlib import Path
from typing import Any

from mobile.command_timing import append_command_metrics, timed_stage
from mobile.project_paths import mobile_datacenter_ids, stg_event_dds_output_path, stg_event_output_path

logger = logging.getLogger(__name__)


def run_move(report_date: date) -> dict[str, Any]:
    """Скопировать ``events.parquet`` каждого ЦОД за ``report_date`` в ``event_dds``."""
    perf: dict[str, Any] = {}
    started = time.perf_counter()
    moves: list[dict[str, Any]] = []
    files_written = 0

    with timed_stage("move_sec", perf):
        for dc in mobile_datacenter_ids():
            src = stg_event_output_path(dc, report_date)
            dst = stg_event_dds_output_path(dc, report_date)
            entry: dict[str, Any] = {
                "source_id": dc,
                "source_path": str(src),
                "output_path": str(dst),
            }
            if not src.exists():
                entry["status"] = "missing_source"
                logger.warning(
                    "build-move-event skip: missing source source_id=%s report_date=%s path=%s",
                    dc,
                    report_date.isoformat(),
                    src,
                )
                moves.append(entry)
                continue
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)
            entry["status"] = "ok"
            entry["bytes"] = int(dst.stat().st_size)
            files_written += 1
            logger.info(
                "build-move-event source_id=%s report_date=%s src=%s dst=%s bytes=%s",
                dc,
                report_date.isoformat(),
                src,
                dst,
                entry["bytes"],
            )
            moves.append(entry)

    stats: dict[str, Any] = {
        "report_date": report_date.isoformat(),
        "datacenters": list(mobile_datacenter_ids()),
        "files_written": int(files_written),
        "moves": moves,
    }
    perf["elapsed_total_sec"] = round(time.perf_counter() - started, 4)
    append_command_metrics(command="build-move-event", metrics={**stats, **perf})
    logger.info("build-move-event completed: %s", stats)
    return stats
