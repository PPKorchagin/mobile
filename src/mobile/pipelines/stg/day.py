"""Оркестратор STG build + DQ за один календарный день (load_day)."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from mobile.pipelines.dq.stg import oktmo as dq_oktmo, oksm as dq_oksm, tac as dq_tac, time_zones as dq_time_zones
from mobile.notebook_runner import run_nb_stg_oktmo, run_nb_stg_time_zones
from mobile.pipelines.stg import oktmo, oksm, tac, time_zones

logger = logging.getLogger(__name__)

BUILD_STG_DAY_STEPS: tuple[str, ...] = (
    "build-stg-oktmo",
    "dq-stg-oktmo",
    "nb-stg-oktmo",
    "build-stg-time-zones",
    "dq-stg-time-zones",
    "nb-stg-time-zones",
    "build-stg-tac",
    "dq-stg-tac",
    "build-stg-oksm",
    "dq-stg-oksm",
)


@dataclass(frozen=True)
class BuildStgDayParams:
    """Пути и дата для цепочки build-stg-day."""

    day: date
    oktmo_csv_path: Path
    oktmo_output_path: Path
    time_zones_csv_path: Path
    time_zones_output_path: Path
    tac_csv_path: Path
    tac_output_path: Path
    oksm_csv_path: Path
    oksm_output_path: Path
    compression: str


def run(params: BuildStgDayParams) -> dict[str, Any]:
    """build → dq для oktmo, time_zones, tac с путями из ``params``."""
    logger.info(
        "build-stg-day start: day=%s oktmo_out=%s time_zones_out=%s tac_out=%s oksm_out=%s",
        params.day.isoformat(),
        params.oktmo_output_path,
        params.time_zones_output_path,
        params.tac_output_path,
        params.oksm_output_path,
    )
    results: dict[str, Any] = {"day": params.day.isoformat(), "steps": {}}

    results["steps"]["build-stg-oktmo"] = oktmo.run(
        csv_path=params.oktmo_csv_path,
        output_path=params.oktmo_output_path,
    )
    results["steps"]["dq-stg-oktmo"] = dq_oktmo.run_dq(oktmo_path=params.oktmo_output_path)

    run_nb_stg_oktmo()
    results["steps"]["nb-stg-oktmo"] = {"status": "ok"}

    results["steps"]["build-stg-time-zones"] = time_zones.run(
        csv_path=params.time_zones_csv_path,
        output_path=params.time_zones_output_path,
    )
    results["steps"]["dq-stg-time-zones"] = dq_time_zones.run_dq(
        time_zones_path=params.time_zones_output_path,
    )

    run_nb_stg_time_zones()
    results["steps"]["nb-stg-time-zones"] = {"status": "ok"}

    results["steps"]["build-stg-tac"] = tac.run(
        csv_path=params.tac_csv_path,
        output_path=params.tac_output_path,
        compression=params.compression,
    )
    results["steps"]["dq-stg-tac"] = dq_tac.run_dq(params.tac_output_path)

    results["steps"]["build-stg-oksm"] = oksm.run(
        csv_path=params.oksm_csv_path,
        output_path=params.oksm_output_path,
        compression=params.compression,
    )
    results["steps"]["dq-stg-oksm"] = dq_oksm.run_dq(params.oksm_output_path)

    logger.info("build-stg-day completed: day=%s", params.day.isoformat())
    return results
