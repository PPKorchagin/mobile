"""CLI для mobile-пайплайнов."""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
from calendar import monthrange
from collections.abc import Callable
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from mobile.cli_defaults import (
    DEFAULT_PARQUET_COMPRESSION,
    DEFAULT_SRC_END_DATE,
    DEFAULT_SRC_START_DATE,
    default_bs_params,
    default_excl_params,
    default_mobile_params,
    default_person_params,
)
from mobile.command_timing import command_run_scope, run_timed_command
from mobile.logging_config import setup_logging
from mobile.notebook_runner import (
    run_nb_perf_metrics,
    run_nb_src_bs,
    run_nb_src_excl,
    run_nb_src_mobile,
    run_nb_dds_event,
    run_nb_src_person,
    run_nb_fct_bs,
    run_nb_stg_geo_all,
    run_nb_fct_msisdn_imei,
    run_nb_fct_msisdn_imsi_operator,
    run_nb_fct_geo_intervals,
    run_nb_fct_person,
    run_nb_dim_oksm,
    run_nb_dim_oktmo,
    run_nb_dim_tac,
    run_nb_dim_time_zones,
)
from mobile.pipelines.src import bs, excl, mobile as src_mobile, person
from mobile.pipelines.dq.src import bs as dq_src_bs
from mobile.pipelines.dq.src import excl as dq_src_excl
from mobile.pipelines.dq.src import mobile as dq_src_mobile
from mobile.pipelines.dq.src import person as dq_src_person
from mobile.pipelines.dds import event as dds_event
from mobile.pipelines.dds import move_event as stg_move_event
from mobile.pipelines.dim import oktmo, oksm, tac, time_zones
from mobile.pipelines.fct import bs as fct_bs
from mobile.pipelines.fct import geo_intervals as fct_geo_intervals
from mobile.pipelines.fct import msisdn_imei as fct_msisdn_imei
from mobile.pipelines.fct import msisdn_imsi as fct_msisdn_imsi
from mobile.pipelines.fct import person as fct_person
from mobile.pipelines.stg import geo_all as stg_geo_all
from mobile.pipelines.dq.dds import event as dq_dds_event
from mobile.pipelines.dq.dim import oksm as dq_oksm, oktmo as dq_oktmo, tac as dq_tac, time_zones as dq_time_zones
from mobile.pipelines.dq.fct import bs as dq_fct_bs
from mobile.pipelines.dq.fct import geo_intervals as dq_fct_geo_intervals
from mobile.pipelines.dq.fct import msisdn_imei as dq_fct_msisdn_imei
from mobile.pipelines.dq.fct import msisdn_imsi_operator as dq_fct_msisdn_imsi_operator
from mobile.pipelines.dq.fct import person as dq_fct_person
from mobile.pipelines.dq.stg import geo_all as dq_stg_geo_all
from mobile.project_paths import (
    DEFAULT_BS_LAYOUT,
    DEFAULT_SRC_EXCL_IMEI_OUTPUT,
    DEFAULT_SRC_EXCL_IMSI_OUTPUT,
    DEFAULT_SRC_EXCL_MSISDN_OUTPUT,
    DEFAULT_SRC_PERSON_OUTPUT_ROOT,
    DEFAULT_STG_GEO_ALL_OUTPUT_ROOT,
    DEFAULT_FCT_GEO_INTERVALS_OUTPUT_ROOT,
    DEFAULT_DIM_OKTMO_CSV_PATH,
    DEFAULT_DIM_OKTMO_OUTPUT_PATH,
    DEFAULT_DIM_OKSM_CSV_PATH,
    DEFAULT_DIM_OKSM_OUTPUT_PATH,
    DEFAULT_DIM_TAC_CSV_PATH,
    DEFAULT_DDS_EVENT_DDS_ROOT,
    FCT_BS_LAYOUT_TEMPLATE,
    FCT_MSISDN_IMSI_LAYOUT_TEMPLATE,
    FCT_MSISDN_IMEI_LAYOUT_TEMPLATE,
    DEFAULT_DIM_TAC_OUTPUT_PATH,
    DEFAULT_DIM_TIME_ZONES_CSV_PATH,
    DEFAULT_DIM_TIME_ZONES_OUTPUT_PATH,
    mobile_datacenter_ids,
    mobile_datacenter_root,
    mobile_mart_paths,
    resolve_oktmo_layout,
    report_month_start,
    resolve_project_path,
    fct_bs_output_path,
    dds_event_dds_output_path,
    dds_event_output_path,
    stg_geo_all_output_path,
    fct_geo_intervals_output_path,
    fct_msisdn_imei_output_path,
    fct_msisdn_imsi_output_path,
    fct_person_output_path,
    resolve_stg_daily_parquet_path,
    resolve_stg_monthly_parquet_path,
)

_BUILD_STG_EVENT_DC_WORKERS = 2

logger = logging.getLogger(__name__)

_BUILD_COMMANDS: dict[str, tuple[Callable[[], None], str]] = {
    "build-src-bs": (
        lambda compression=DEFAULT_PARQUET_COMPRESSION: bs.run(
            oktmo_parquet_path=resolve_oktmo_layout(),
            output_path=DEFAULT_BS_LAYOUT,
            compression=compression,
            params=default_bs_params(),
        ),
        str(DEFAULT_BS_LAYOUT),
    ),
}

_NB_COMMANDS: dict[str, Callable[[], None]] = {
    "nb-dim-oktmo": run_nb_dim_oktmo,
    "nb-dim-time-zones": run_nb_dim_time_zones,
    "nb-dim-tac": run_nb_dim_tac,
    "nb-dim-oksm": run_nb_dim_oksm,
    "nb-fct-bs": run_nb_fct_bs,
    "nb-stg-geo-all": run_nb_stg_geo_all,
    "nb-fct-msisdn-imei": run_nb_fct_msisdn_imei,
    "nb-fct-msisdn-imsi-operator": run_nb_fct_msisdn_imsi_operator,
    "nb-fct-geo-intervals": run_nb_fct_geo_intervals,
    "nb-fct-person": run_nb_fct_person,
    "nb-src-bs": run_nb_src_bs,
    "nb-src-person": run_nb_src_person,
    "nb-src-excl": run_nb_src_excl,
    "nb-src-mobile": run_nb_src_mobile,
    "nb-dds-event": run_nb_dds_event,
    "nb-perf-metrics": run_nb_perf_metrics,
}

# Порядок как в README.md (команды 1–47).
RUN_ALL_COMMANDS: tuple[str, ...] = (
    "build-dim-oktmo",
    "dq-dim-oktmo",
    "nb-dim-oktmo",
    "build-dim-time-zones",
    "dq-dim-time-zones",
    "nb-dim-time-zones",
    "build-dim-tac",
    "dq-dim-tac",
    "nb-dim-tac",
    "build-dim-oksm",
    "dq-dim-oksm",
    "nb-dim-oksm",
    "build-src-bs",
    "dq-src-bs",
    "nb-src-bs",
    "build-src-person",
    "dq-src-person",
    "nb-src-person",
    "build-src-excl",
    "dq-src-excl",
    "nb-src-excl",
    "build-src-mobile",
    "dq-src-mobile",
    "nb-src-mobile",
    "build-dds-event",
    "build-dds-move-event",
    "dq-dds-event",
    "nb-dds-event",
    "build-fct-bs",
    "dq-fct-bs",
    "nb-fct-bs",
    "build-stg-geo-all",
    "dq-stg-geo-all",
    "nb-stg-geo-all",
    "build-fct-msisdn-imei",
    "dq-fct-msisdn-imei",
    "nb-fct-msisdn-imei",
    "build-fct-msisdn-imsi-operator",
    "dq-fct-msisdn-imsi-operator",
    "nb-fct-msisdn-imsi-operator",
    "build-fct-geo-intervals",
    "dq-fct-geo-intervals",
    "nb-fct-geo-intervals",
    "build-fct-person",
    "dq-fct-person",
    "nb-fct-person",
    "nb-perf-metrics",
)

# Генерация src-витрин (только build; порядок README 1 + 13, 16, 19, 22).
RUN_SRC_COMMANDS: tuple[str, ...] = (
    "build-dim-oktmo",
    "build-src-bs",
    "build-src-person",
    "build-src-excl",
    "build-src-mobile",
)

CLI_COMMANDS: tuple[str, ...] = (
    *RUN_ALL_COMMANDS,
    "run-all",
    "run-src",
)


def _parse_day(value: str) -> date:
    return date.fromisoformat(value)


def _calendar_days_inclusive(start: date, end: date) -> list[date]:
    out: list[date] = []
    cur = start
    while cur <= end:
        out.append(cur)
        cur += timedelta(days=1)
    return out


def _distinct_report_months_in_src_window() -> list[date]:
    months: set[date] = set()
    for day in _calendar_days_inclusive(DEFAULT_SRC_START_DATE, DEFAULT_SRC_END_DATE):
        months.add(report_month_start(day))
    return sorted(months)


def _run_all_argv_steps() -> list[tuple[str, list[str]]]:
    """Шаги run-all: (метка лога, argv для argparse)."""
    steps: list[tuple[str, list[str]]] = []
    for command in RUN_ALL_COMMANDS:
        if command == "build-fct-person":
            for month in _distinct_report_months_in_src_window():
                label = f"{command} ({month.isoformat()})"
                argv = [command, "--report-date", month.isoformat()]
                steps.append((label, argv))
        else:
            steps.append((command, [command]))
    return steps


def _run_src_argv_steps() -> list[tuple[str, list[str]]]:
    """Шаги run-src: (метка лога, argv для argparse)."""
    return [(command, [command]) for command in RUN_SRC_COMMANDS]


def _run_pipeline(
    pipeline_name: str,
    steps: list[tuple[str, list[str]]],
    *,
    target_per_operator: int | None = None,
    excl_pct_of_ab: float | None = None,
    start_message: str,
) -> None:
    parser = _build_parser()

    def _body() -> None:
        total = len(steps)
        logger.info(start_message)
        for index, (label, argv) in enumerate(steps, start=1):
            logger.info("%s [%s/%s] %s", pipeline_name, index, total, label)
            step_args = parser.parse_args(argv)
            if target_per_operator is not None:
                step_args.target_per_operator = target_per_operator
            if excl_pct_of_ab is not None:
                step_args.excl_pct_of_ab = excl_pct_of_ab
            _execute_parsed_args(step_args)
        logger.info("%s completed successfully", pipeline_name)

    run_timed_command(pipeline_name, _body)


def dq_src_mobile_run(
    *,
    report_date: date,
    cdr_path: Path,
    sms_path: Path,
    gprs_path: Path,
    location_path: Path,
) -> dict:
    return dq_src_mobile.run_dq(
        report_date,
        cdr_path,
        sms_path,
        gprs_path,
        location_path,
    )


def _resolve_mobile_mart_paths(
    *,
    datacenter: str,
    mobile_root: Path | None,
    cdr_path: str | None,
    sms_path: str | None,
    gprs_path: str | None,
    location_path: str | None,
) -> dict[str, Path]:
    defaults = mobile_mart_paths(datacenter, mobile_root=mobile_root)
    return {
        "cdr_path": Path(cdr_path) if cdr_path else defaults["cdr"],
        "sms_path": Path(sms_path) if sms_path else defaults["sms"],
        "gprs_path": Path(gprs_path) if gprs_path else defaults["gprs"],
        "location_path": Path(location_path) if location_path else defaults["location"],
    }


def _mobile_mart_run_paths(
    *,
    datacenter: str | None,
    mobile_root: Path | None,
    cdr_path: str | None,
    sms_path: str | None,
    gprs_path: str | None,
    location_path: str | None,
    command: str,
) -> dict[str, Path]:
    if datacenter is not None:
        return _resolve_mobile_mart_paths(
            datacenter=datacenter,
            mobile_root=mobile_root,
            cdr_path=cdr_path,
            sms_path=sms_path,
            gprs_path=gprs_path,
            location_path=location_path,
        )
    missing = [
        name
        for name, value in (
            ("--cdr-path", cdr_path),
            ("--sms-path", sms_path),
            ("--gprs-path", gprs_path),
            ("--location-path", location_path),
        )
        if value is None
    ]
    if missing:
        raise SystemExit(f"{command}: pass --dc or all mart paths: " + ", ".join(missing))
    return {
        "cdr_path": Path(cdr_path),
        "sms_path": Path(sms_path),
        "gprs_path": Path(gprs_path),
        "location_path": Path(location_path),
    }


def build_dds_event_run(
    *,
    report_date: date,
    cdr_path: Path,
    sms_path: Path,
    gprs_path: Path,
    location_path: Path,
    output_path: Path,
) -> dict:
    return dds_event.run_build(
        report_date,
        cdr_path,
        sms_path,
        gprs_path,
        location_path,
        output_path,
    )


def _resolve_build_dds_event_job(
    *,
    datacenter: str | None,
    report_date: date,
    mobile_root: Path | None,
    cdr_path: str | None,
    sms_path: str | None,
    gprs_path: str | None,
    location_path: str | None,
    output_path: str | None,
) -> tuple[dict[str, Path], Path]:
    """Пять аргументов pipeline: дата, 4 корня витрин, выходной parquet."""
    if datacenter is not None:
        paths = _resolve_mobile_mart_paths(
            datacenter=datacenter,
            mobile_root=mobile_root,
            cdr_path=cdr_path,
            sms_path=sms_path,
            gprs_path=gprs_path,
            location_path=location_path,
        )
        out = Path(output_path) if output_path else dds_event_output_path(datacenter, report_date)
        return paths, out

    missing = [
        name
        for name, value in (
            ("--cdr-path", cdr_path),
            ("--sms-path", sms_path),
            ("--gprs-path", gprs_path),
            ("--location-path", location_path),
            ("--output-path", output_path),
        )
        if value is None
    ]
    if missing:
        raise SystemExit(
            "build-dds-event: pass --dc or all of: " + ", ".join(missing)
        )
    return (
        {
            "cdr_path": Path(cdr_path),
            "sms_path": Path(sms_path),
            "gprs_path": Path(gprs_path),
            "location_path": Path(location_path),
        },
        Path(output_path),
    )


def _build_dds_event_subprocess_cmd(
    *,
    datacenter: str,
    report_date: date,
    mobile_root: str | None,
) -> list[str]:
    cmd = [
        sys.executable,
        "-m",
        "mobile",
        "build-dds-event",
        "--dc",
        datacenter,
        "--report-date",
        report_date.isoformat(),
    ]
    if mobile_root is not None:
        cmd.extend(["--mobile-root", mobile_root])
    return cmd


def _run_build_dds_event_dc_subprocesses(
    *,
    datacenter: str,
    days: list[date],
    mobile_root: str | None,
) -> None:
    """До ``_BUILD_STG_EVENT_DC_WORKERS`` параллельных subprocess на один ЦОД."""
    pending = list(days)
    running: dict[subprocess.Popen[bytes], date] = {}

    while pending or running:
        while pending and len(running) < _BUILD_STG_EVENT_DC_WORKERS:
            day = pending.pop(0)
            cmd = _build_dds_event_subprocess_cmd(
                datacenter=datacenter,
                report_date=day,
                mobile_root=mobile_root,
            )
            logger.info("build-dds-event spawn: %s", " ".join(cmd))
            running[subprocess.Popen(cmd)] = day

        if not running:
            break

        done, _ = subprocess.wait(running.keys(), timeout=0.25)
        for proc in done:
            day = running.pop(proc)
            rc = proc.wait()
            if rc != 0:
                raise subprocess.CalledProcessError(rc, proc.args, None, None)
            logger.info(
                "build-dds-event subprocess ok: dc=%s report_date=%s",
                datacenter,
                day.isoformat(),
            )


def run_dq_dim_oktmo(*, oktmo_path: str | None) -> None:
    """DQ ``dim_oktmo`` (read-only проверки)."""
    path = Path(oktmo_path) if oktmo_path else DEFAULT_DIM_OKTMO_OUTPUT_PATH
    run_timed_command(
        "dq-dim-oktmo",
        lambda: dq_oktmo.run_dq(oktmo_path=path),
    )


def run_dq_dim_time_zones(*, time_zones_path: str | None) -> None:
    """DQ ``dim_time_zones`` (read-only проверки)."""
    path = Path(time_zones_path) if time_zones_path else DEFAULT_DIM_TIME_ZONES_OUTPUT_PATH
    run_timed_command(
        "dq-dim-time-zones",
        lambda: dq_time_zones.run_dq(time_zones_path=path),
    )


def run_dq_dim_tac(*, tac_path: str | None) -> None:
    """DQ ``dim_tac`` (read-only проверки)."""
    path = Path(tac_path) if tac_path else DEFAULT_DIM_TAC_OUTPUT_PATH
    run_timed_command(
        "dq-dim-tac",
        lambda: dq_tac.run_dq(tac_path=path),
    )


def run_build_dim_tac(
    *,
    csv_path: str | None,
    output_path: str | None,
) -> None:
    """build-dim-tac: CSV TACDB → Parquet ``dim_tac``."""
    csv = Path(csv_path) if csv_path else DEFAULT_DIM_TAC_CSV_PATH
    out = Path(output_path) if output_path else DEFAULT_DIM_TAC_OUTPUT_PATH
    run_timed_command(
        "build-dim-tac",
        lambda: tac.run(csv_path=csv, output_path=out),
    )


def run_build_dim_oksm(
    *,
    csv_path: str | None,
    output_path: str | None,
) -> None:
    """build-dim-oksm: CSV ОКСМ → Parquet ``dim_oksm``."""
    csv = Path(csv_path) if csv_path else DEFAULT_DIM_OKSM_CSV_PATH
    out = Path(output_path) if output_path else DEFAULT_DIM_OKSM_OUTPUT_PATH
    run_timed_command(
        "build-dim-oksm",
        lambda: oksm.run(csv_path=csv, output_path=out),
    )


def run_dq_dim_oksm(*, oksm_path: str | None) -> None:
    """DQ ``dim_oksm`` (read-only проверки)."""
    path = Path(oksm_path) if oksm_path else DEFAULT_DIM_OKSM_OUTPUT_PATH
    run_timed_command(
        "dq-dim-oksm",
        lambda: dq_oksm.run_dq(oksm_path=path),
    )


def run_dq_fct_bs(*, fct_bs_path: str | None) -> None:
    """DQ ``fct_bs`` (read-only проверки)."""
    path = resolve_project_path(fct_bs_path) if fct_bs_path else fct_bs_output_path()
    run_timed_command(
        "dq-fct-bs",
        lambda: dq_fct_bs.run_dq(path),
    )


def run_build_dim_time_zones(
    *,
    csv_path: str | None,
    output_path: str | None,
) -> None:
    """build-dim-time-zones: CSV таймзон → Parquet ``dim_time_zones``."""
    csv = Path(csv_path) if csv_path else DEFAULT_DIM_TIME_ZONES_CSV_PATH
    out = Path(output_path) if output_path else DEFAULT_DIM_TIME_ZONES_OUTPUT_PATH
    run_timed_command(
        "build-dim-time-zones",
        lambda: time_zones.run(csv_path=csv, output_path=out),
    )


def run_build_dim_oktmo(
    *,
    csv_path: str | None,
    output_path: str | None,
) -> None:
    """build-dim-oktmo: CSV ОКТМО → Parquet ``dim_oktmo``."""
    csv = Path(csv_path) if csv_path else DEFAULT_DIM_OKTMO_CSV_PATH
    out = Path(output_path) if output_path else DEFAULT_DIM_OKTMO_OUTPUT_PATH
    run_timed_command(
        "build-dim-oktmo",
        lambda: oktmo.run(csv_path=csv, output_path=out),
    )


def run_build_fct_bs(
    *,
    src_bs_path: str | None,
    oktmo_path: str | None,
    time_zones_path: str | None,
    output_path: str | None,
) -> None:
    """build-fct-bs: явный прогон (4 параметра) или один прогон с путями по умолчанию."""
    explicit = any((src_bs_path, oktmo_path, time_zones_path, output_path))

    if explicit:
        missing: list[str] = []
        if src_bs_path is None:
            missing.append("--src-bs-path")
        if oktmo_path is None:
            missing.append("--oktmo-path")
        if time_zones_path is None:
            missing.append("--time-zones-path")
        if output_path is None:
            missing.append("--output-path")
        if missing:
            raise SystemExit(
                "build-fct-bs: explicit run requires all parameters; "
                f"missing: {', '.join(missing)}"
            )
        run_timed_command(
            "build-fct-bs",
            lambda: fct_bs.run_build(
                src_bs_path=Path(src_bs_path),
                oktmo_path=Path(oktmo_path),
                time_zones_path=Path(time_zones_path),
                output_path=Path(output_path),
            ),
        )
        return

    run_timed_command(
        "build-fct-bs",
        lambda: fct_bs.run_build(
            src_bs_path=DEFAULT_BS_LAYOUT,
            oktmo_path=DEFAULT_DIM_OKTMO_OUTPUT_PATH,
            time_zones_path=DEFAULT_DIM_TIME_ZONES_OUTPUT_PATH,
            output_path=fct_bs_output_path(),
        ),
    )


def run_build_stg_geo_all(
    *,
    report_date: date | None,
    event_dds_path: str | None,
    fct_bs_path: str | None,
    output_path: str | None,
) -> None:
    """build-stg-geo-all: явный прогон (4 параметра) или цикл DEFAULT_SRC_* по дням."""
    explicit = any((report_date, event_dds_path, fct_bs_path, output_path))

    if explicit:
        missing: list[str] = []
        if report_date is None:
            missing.append("--report-date")
        if event_dds_path is None:
            missing.append("--event-dds-path")
        if fct_bs_path is None:
            missing.append("--fct-bs-path")
        if output_path is None:
            missing.append("--output-path")
        if missing:
            raise SystemExit(
                "build-stg-geo-all: explicit run requires all parameters; "
                f"missing: {', '.join(missing)}"
            )
        run_timed_command(
            f"build-stg-geo-all-{report_date.isoformat()}",
            lambda: stg_geo_all.run_build(
                report_date=report_date,
                event_dds_path=Path(event_dds_path),
                fct_bs_path=Path(fct_bs_path),
                output_path=Path(output_path),
            ),
        )
        return

    lo = DEFAULT_SRC_START_DATE
    hi = DEFAULT_SRC_END_DATE
    days = _calendar_days_inclusive(lo, hi)
    dds = DEFAULT_DDS_EVENT_DDS_ROOT
    bs = fct_bs_output_path()
    logger.info(
        "Starting build-stg-geo-all: days=%s (%s .. %s) event_dds=%s fct_bs=%s",
        len(days),
        lo.isoformat(),
        hi.isoformat(),
        dds,
        bs,
    )
    for day in days:
        out = stg_geo_all_output_path(day)
        run_timed_command(
            f"build-stg-geo-all-{day.isoformat()}",
            lambda d=day, o=out: stg_geo_all.run_build(
                report_date=d,
                event_dds_path=dds,
                fct_bs_path=bs,
                output_path=o,
            ),
        )
    logger.info("build-stg-geo-all completed successfully")


def _geo_intervals_run_build_kwargs(
    report_date: date,
    *,
    stg_geo_all_path: str | Path,
    fct_bs_path: str | Path,
    time_zones_path: str | Path,
    fct_msisdn_imsi_path: str | Path,
    fct_msisdn_imei_path: str | Path,
    output_path: str | Path,
) -> dict[str, Any]:
    return {
        "report_date": report_date,
        "stg_geo_all_path": resolve_stg_daily_parquet_path(stg_geo_all_path, report_date),
        "fct_bs_path": resolve_project_path(fct_bs_path),
        "time_zones_path": resolve_project_path(time_zones_path),
        "fct_msisdn_imsi_path": resolve_stg_monthly_parquet_path(fct_msisdn_imsi_path, report_date),
        "fct_msisdn_imei_path": resolve_stg_monthly_parquet_path(fct_msisdn_imei_path, report_date),
        "output_path": resolve_stg_daily_parquet_path(output_path, report_date),
    }


def run_build_fct_geo_intervals(
    *,
    report_date: date | None,
    stg_geo_all_path: str | None,
    fct_bs_path: str | None,
    time_zones_path: str | None,
    fct_msisdn_imsi_path: str | None,
    fct_msisdn_imei_path: str | None,
    output_path: str | None,
) -> None:
    """build-fct-geo-intervals: явный прогон (7 параметров) или цикл DEFAULT_SRC_* по дням."""
    explicit = any(
        (
            report_date,
            stg_geo_all_path,
            fct_bs_path,
            time_zones_path,
            fct_msisdn_imsi_path,
            fct_msisdn_imei_path,
            output_path,
        )
    )

    if explicit:
        missing: list[str] = []
        if report_date is None:
            missing.append("--report-date")
        if stg_geo_all_path is None:
            missing.append("--stg-geo-all-path")
        if fct_bs_path is None:
            missing.append("--fct-bs-path")
        if time_zones_path is None:
            missing.append("--time-zones-path")
        if fct_msisdn_imsi_path is None:
            missing.append("--fct-msisdn-imsi-path")
        if fct_msisdn_imei_path is None:
            missing.append("--fct-msisdn-imei-path")
        if output_path is None:
            missing.append("--output-path")
        if missing:
            raise SystemExit(
                "build-fct-geo-intervals: explicit run requires all parameters; "
                f"missing: {', '.join(missing)}"
            )
        kwargs = _geo_intervals_run_build_kwargs(
            report_date,
            stg_geo_all_path=stg_geo_all_path,
            fct_bs_path=fct_bs_path,
            time_zones_path=time_zones_path,
            fct_msisdn_imsi_path=fct_msisdn_imsi_path,
            fct_msisdn_imei_path=fct_msisdn_imei_path,
            output_path=output_path,
        )
        run_timed_command(
            f"build-fct-geo-intervals-{report_date.isoformat()}",
            lambda kw=kwargs: fct_geo_intervals.run_build(**kw),
        )
        return

    lo = DEFAULT_SRC_START_DATE
    hi = DEFAULT_SRC_END_DATE
    days = _calendar_days_inclusive(lo, hi)
    bs = fct_bs_output_path()
    tz = DEFAULT_DIM_TIME_ZONES_OUTPUT_PATH
    geo_root = DEFAULT_STG_GEO_ALL_OUTPUT_ROOT
    imsi_root = DEFAULT_STG_GEO_ALL_OUTPUT_ROOT.parent / "msisdn_imsi"
    imei_root = DEFAULT_STG_GEO_ALL_OUTPUT_ROOT.parent / "msisdn_imei"
    out_root = DEFAULT_FCT_GEO_INTERVALS_OUTPUT_ROOT
    if not bs.exists():
        raise SystemExit(f"build-fct-geo-intervals: fct_bs not found: {bs}")
    if not tz.exists():
        raise SystemExit(f"build-fct-geo-intervals: time_zones not found: {tz}")

    logger.info(
        "Starting build-fct-geo-intervals: days=%s (%s .. %s)",
        len(days),
        lo.isoformat(),
        hi.isoformat(),
    )
    for day in days:
        geo_file = stg_geo_all_output_path(day)
        if not geo_file.exists():
            continue
        imsi_file = fct_msisdn_imsi_output_path(day)
        imei_file = fct_msisdn_imei_output_path(day)
        if not imsi_file.exists() or not imei_file.exists():
            logger.info(
                "build-fct-geo-intervals: skip %s (binding missing imsi=%s imei=%s)",
                day.isoformat(),
                imsi_file.exists(),
                imei_file.exists(),
            )
            continue
        kwargs = _geo_intervals_run_build_kwargs(
            day,
            stg_geo_all_path=geo_root,
            fct_bs_path=bs,
            time_zones_path=tz,
            fct_msisdn_imsi_path=imsi_root,
            fct_msisdn_imei_path=imei_root,
            output_path=out_root,
        )
        run_timed_command(
            f"build-fct-geo-intervals-{day.isoformat()}",
            lambda kw=kwargs: fct_geo_intervals.run_build(**kw),
        )
    logger.info("build-fct-geo-intervals completed successfully")


def _run_build_stg_geo_binding(
    command: str,
    *,
    report_date: date | None,
    stg_geo_all_path: str | None,
    output_path: str | None,
    runner: Callable[..., dict],
    default_output: Callable[[date], Path],
) -> None:
    """build-fct-msisdn-imei / build-fct-msisdn-imsi-operator: явный прогон (3 параметра) или цикл по дням."""
    explicit = any((report_date, stg_geo_all_path, output_path))

    if explicit:
        missing: list[str] = []
        if report_date is None:
            missing.append("--report-date")
        if stg_geo_all_path is None:
            missing.append("--stg-geo-all-path")
        if output_path is None:
            missing.append("--output-path")
        if missing:
            raise SystemExit(
                f"{command}: explicit run requires all parameters; missing: {', '.join(missing)}"
            )
        run_timed_command(
            f"{command}-{report_date.isoformat()}",
            lambda: runner(
                report_date=report_date,
                stg_geo_all_path=Path(stg_geo_all_path),
                output_path=Path(output_path),
            ),
        )
        return

    lo = DEFAULT_SRC_START_DATE
    hi = DEFAULT_SRC_END_DATE
    days = _calendar_days_inclusive(lo, hi)
    logger.info("Starting %s: days=%s (%s .. %s)", command, len(days), lo.isoformat(), hi.isoformat())
    for day in days:
        geo = stg_geo_all_output_path(day)
        if not geo.exists():
            continue
        out = default_output(day)
        run_timed_command(
            f"{command}-{day.isoformat()}",
            lambda d=day, g=geo, o=out: runner(
                report_date=d,
                stg_geo_all_path=g,
                output_path=o,
            ),
        )
    logger.info("%s completed successfully", command)


def run_build_fct_msisdn_imei(
    *,
    report_date: date | None,
    stg_geo_all_path: str | None,
    output_path: str | None,
) -> None:
    _run_build_stg_geo_binding(
        "build-fct-msisdn-imei",
        report_date=report_date,
        stg_geo_all_path=stg_geo_all_path,
        output_path=output_path,
        runner=fct_msisdn_imei.run_build,
        default_output=fct_msisdn_imei_output_path,
    )


def run_build_fct_msisdn_imsi_operator(
    *,
    report_date: date | None,
    stg_geo_all_path: str | None,
    output_path: str | None,
) -> None:
    _run_build_stg_geo_binding(
        "build-fct-msisdn-imsi-operator",
        report_date=report_date,
        stg_geo_all_path=stg_geo_all_path,
        output_path=output_path,
        runner=fct_msisdn_imsi.run_build,
        default_output=fct_msisdn_imsi_output_path,
    )


def run_build_fct_person(
    *,
    report_date: date | None,
    src_person_path: str | None,
    fct_msisdn_imsi_path: str | None,
    fct_msisdn_imei_path: str | None,
    src_excl_imsi_path: str | None,
    src_excl_imei_path: str | None,
    src_excl_msisdn_path: str | None,
    dim_tac_path: str | None,
    dim_oksm_path: str | None,
    output_path: str | None,
) -> None:
    """build-fct-person: месячный срез person для физлиц из src_person (``--report-date`` = YYYY-MM-01)."""
    if report_date is None:
        raise SystemExit("build-fct-person: --report-date is required")
    if report_date.day != 1:
        raise SystemExit(f"build-fct-person: --report-date must be YYYY-MM-01, got {report_date.isoformat()}")
    src = Path(src_person_path) if src_person_path else None
    imsi = Path(fct_msisdn_imsi_path) if fct_msisdn_imsi_path else None
    imei = Path(fct_msisdn_imei_path) if fct_msisdn_imei_path else None
    tac = Path(dim_tac_path) if dim_tac_path else None
    oksm = Path(dim_oksm_path) if dim_oksm_path else None
    out = Path(output_path) if output_path else None
    run_timed_command(
        "build-fct-person",
        lambda: fct_person.run_build(
            report_date=report_date,
            src_person_path=src,
            fct_msisdn_imsi_path=imsi,
            fct_msisdn_imei_path=imei,
            src_excl_imsi_path=src_excl_imsi_path,
            src_excl_imei_path=src_excl_imei_path,
            src_excl_msisdn_path=src_excl_msisdn_path,
            dim_tac_path=tac,
            dim_oksm_path=oksm,
            output_path=out,
        ),
    )


def run_build_dds_move_event(*, report_date: date | None) -> None:
    """build-dds-move-event: один день или цикл DEFAULT_SRC_START_DATE..END."""
    if report_date is not None:
        run_timed_command(
            "build-dds-move-event",
            lambda: stg_move_event.run_move(report_date),
        )
        return

    lo = DEFAULT_SRC_START_DATE
    hi = DEFAULT_SRC_END_DATE
    days = _calendar_days_inclusive(lo, hi)
    logger.info(
        "Starting build-dds-move-event: days=%s (%s .. %s)",
        len(days),
        lo.isoformat(),
        hi.isoformat(),
    )
    for day in days:
        run_timed_command(
            f"build-dds-move-event-{day.isoformat()}",
            lambda d=day: stg_move_event.run_move(d),
        )
    logger.info("build-dds-move-event completed successfully")


def run_build_dds_event(
    *,
    datacenter: str | None,
    report_date: date | None,
    mobile_root: str | None,
    cdr_path: str | None,
    sms_path: str | None,
    gprs_path: str | None,
    location_path: str | None,
    output_path: str | None,
) -> None:
    """build-dds-event: явный прогон (5 параметров) или оркестратор DEFAULT_SRC_* × ЦОД (2 subprocess/ЦОД)."""
    root = Path(mobile_root) if mobile_root else None
    explicit = any(
        (
            report_date,
            datacenter,
            cdr_path,
            sms_path,
            gprs_path,
            location_path,
            output_path,
        )
    )

    if explicit:
        if report_date is None:
            raise SystemExit("build-dds-event: --report-date is required for explicit run")
        paths, out = _resolve_build_dds_event_job(
            datacenter=datacenter,
            report_date=report_date,
            mobile_root=root,
            cdr_path=cdr_path,
            sms_path=sms_path,
            gprs_path=gprs_path,
            location_path=location_path,
            output_path=output_path,
        )
        label = (
            f"build-dds-event-{datacenter}-{report_date.isoformat()}"
            if datacenter
            else f"build-dds-event-{report_date.isoformat()}"
        )
        run_timed_command(
            label,
            lambda: build_dds_event_run(
                report_date=report_date,
                output_path=out,
                **paths,
            ),
        )
        return

    lo = DEFAULT_SRC_START_DATE
    hi = DEFAULT_SRC_END_DATE
    days = _calendar_days_inclusive(lo, hi)
    dcs = mobile_datacenter_ids()
    logger.info(
        "Starting build-dds-event: days=%s datacenters=%s processes_per_dc=%s (%s .. %s)",
        len(days),
        ", ".join(dcs),
        _BUILD_STG_EVENT_DC_WORKERS,
        lo.isoformat(),
        hi.isoformat(),
    )
    mobile_root_str = str(root) if root is not None else None
    for dc in dcs:
        logger.info(
            "build-dds-event datacenter=%s: %s days, %s parallel subprocesses",
            dc,
            len(days),
            _BUILD_STG_EVENT_DC_WORKERS,
        )
        _run_build_dds_event_dc_subprocesses(
            datacenter=dc,
            days=days,
            mobile_root=mobile_root_str,
        )
    logger.info("build-dds-event completed successfully")


def dq_dds_event_run(
    *,
    report_date: date,
    event_dds_root: Path,
) -> dict:
    return dq_dds_event.run_dq(report_date, event_dds_root)


def run_dq_dds_event(
    *,
    report_date: date | None,
    event_dds_path: str | None,
) -> None:
    """DQ event_dds: один день (``--report-date``) или цикл DEFAULT_SRC_* по дням."""
    root = Path(event_dds_path) if event_dds_path else DEFAULT_DDS_EVENT_DDS_ROOT

    if report_date is not None:
        run_timed_command(
            f"dq-dds-event-{report_date.isoformat()}",
            lambda: dq_dds_event_run(report_date=report_date, event_dds_root=root),
        )
        return

    lo = DEFAULT_SRC_START_DATE
    hi = DEFAULT_SRC_END_DATE
    days = _calendar_days_inclusive(lo, hi)
    logger.info(
        "Starting dq-dds-event: days=%s (%s .. %s) event_dds_root=%s",
        len(days),
        lo.isoformat(),
        hi.isoformat(),
        root,
    )
    for day in days:
        run_timed_command(
            f"dq-dds-event-{day.isoformat()}",
            lambda d=day: dq_dds_event_run(report_date=d, event_dds_root=root),
        )
    logger.info("dq-dds-event completed successfully")


def run_dq_stg_geo_all(
    *,
    report_date: date | None,
    stg_geo_all_path: str | None,
) -> None:
    """DQ ``stg_geo_all``: явный прогон (2 параметра) или цикл DEFAULT_SRC_* по дням."""
    explicit = any((report_date, stg_geo_all_path))

    if explicit:
        missing: list[str] = []
        if report_date is None:
            missing.append("--report-date")
        if stg_geo_all_path is None:
            missing.append("--stg-geo-all-path")
        if missing:
            raise SystemExit(
                "dq-stg-geo-all: explicit run requires all parameters; "
                f"missing: {', '.join(missing)}"
            )
        path = resolve_project_path(stg_geo_all_path)
        run_timed_command(
            f"dq-stg-geo-all-{report_date.isoformat()}",
            lambda: dq_stg_geo_all.run_dq(report_date=report_date, stg_geo_all_path=path),
        )
        return

    lo = DEFAULT_SRC_START_DATE
    hi = DEFAULT_SRC_END_DATE
    days = _calendar_days_inclusive(lo, hi)
    logger.info(
        "Starting dq-stg-geo-all: days=%s (%s .. %s) stg_geo_all_root=%s",
        len(days),
        lo.isoformat(),
        hi.isoformat(),
        DEFAULT_STG_GEO_ALL_OUTPUT_ROOT,
    )
    for day in days:
        out = stg_geo_all_output_path(day)
        run_timed_command(
            f"dq-stg-geo-all-{day.isoformat()}",
            lambda d=day, p=out: dq_stg_geo_all.run_dq(report_date=d, stg_geo_all_path=p),
        )
    logger.info("dq-stg-geo-all completed successfully")


def run_dq_fct_msisdn_imei(
    *,
    report_date: date | None,
    fct_msisdn_imei_path: str | None,
) -> None:
    """DQ ``fct_msisdn_imei``: явный прогон (2 параметра) или цикл DEFAULT_SRC_* (по месяцам)."""
    explicit = any((report_date, fct_msisdn_imei_path))

    if explicit:
        missing: list[str] = []
        if report_date is None:
            missing.append("--report-date")
        if fct_msisdn_imei_path is None:
            missing.append("--fct-msisdn-imei-path")
        if missing:
            raise SystemExit(
                "dq-fct-msisdn-imei: explicit run requires all parameters; "
                f"missing: {', '.join(missing)}"
            )
        path = resolve_project_path(fct_msisdn_imei_path)
        month = report_month_start(report_date)
        run_timed_command(
            f"dq-fct-msisdn-imei-{month.isoformat()}",
            lambda rd=report_date, p=path: dq_fct_msisdn_imei.run_dq(
                report_date=rd,
                fct_msisdn_imei_path=p,
            ),
        )
        return

    lo = DEFAULT_SRC_START_DATE
    hi = DEFAULT_SRC_END_DATE
    days = _calendar_days_inclusive(lo, hi)
    seen_months: set[date] = set()
    logger.info(
        "Starting dq-fct-msisdn-imei: days=%s (%s .. %s) layout=%s",
        len(days),
        lo.isoformat(),
        hi.isoformat(),
        FCT_MSISDN_IMEI_LAYOUT_TEMPLATE,
    )
    for day in days:
        month = report_month_start(day)
        if month in seen_months:
            continue
        out = fct_msisdn_imei_output_path(day)
        if not out.exists():
            continue
        seen_months.add(month)
        run_timed_command(
            f"dq-fct-msisdn-imei-{month.isoformat()}",
            lambda m=month, p=out: dq_fct_msisdn_imei.run_dq(report_date=m, fct_msisdn_imei_path=p),
        )
    logger.info("dq-fct-msisdn-imei completed successfully")


def run_dq_fct_msisdn_imsi_operator(
    *,
    report_date: date | None,
    fct_msisdn_imsi_path: str | None,
) -> None:
    """DQ ``fct_msisdn_imsi``: явный прогон (2 параметра) или цикл DEFAULT_SRC_* (по месяцам)."""
    explicit = any((report_date, fct_msisdn_imsi_path))

    if explicit:
        missing: list[str] = []
        if report_date is None:
            missing.append("--report-date")
        if fct_msisdn_imsi_path is None:
            missing.append("--fct-msisdn-imsi-path")
        if missing:
            raise SystemExit(
                "dq-fct-msisdn-imsi-operator: explicit run requires all parameters; "
                f"missing: {', '.join(missing)}"
            )
        path = resolve_project_path(fct_msisdn_imsi_path)
        month = report_month_start(report_date)
        run_timed_command(
            f"dq-fct-msisdn-imsi-operator-{month.isoformat()}",
            lambda rd=report_date, p=path: dq_fct_msisdn_imsi_operator.run_dq(
                report_date=rd,
                fct_msisdn_imsi_path=p,
            ),
        )
        return

    lo = DEFAULT_SRC_START_DATE
    hi = DEFAULT_SRC_END_DATE
    days = _calendar_days_inclusive(lo, hi)
    seen_months: set[date] = set()
    logger.info(
        "Starting dq-fct-msisdn-imsi-operator: days=%s (%s .. %s) layout=%s",
        len(days),
        lo.isoformat(),
        hi.isoformat(),
        FCT_MSISDN_IMSI_LAYOUT_TEMPLATE,
    )
    for day in days:
        month = report_month_start(day)
        if month in seen_months:
            continue
        out = fct_msisdn_imsi_output_path(day)
        if not out.exists():
            continue
        seen_months.add(month)
        run_timed_command(
            f"dq-fct-msisdn-imsi-operator-{month.isoformat()}",
            lambda m=month, p=out: dq_fct_msisdn_imsi_operator.run_dq(
                report_date=m,
                fct_msisdn_imsi_path=p,
            ),
        )
    logger.info("dq-fct-msisdn-imsi-operator completed successfully")


def run_dq_fct_geo_intervals(
    *,
    report_date: date | None,
    fct_geo_intervals_path: str | None,
) -> None:
    """DQ ``fct_geo_intervals``: явный прогон (2 параметра) или цикл DEFAULT_SRC_* по дням."""
    explicit = any((report_date, fct_geo_intervals_path))

    if explicit:
        missing: list[str] = []
        if report_date is None:
            missing.append("--report-date")
        if fct_geo_intervals_path is None:
            missing.append("--fct-geo-intervals-path")
        if missing:
            raise SystemExit(
                "dq-fct-geo-intervals: explicit run requires all parameters; "
                f"missing: {', '.join(missing)}"
            )
        path = resolve_stg_daily_parquet_path(fct_geo_intervals_path, report_date)
        run_timed_command(
            f"dq-fct-geo-intervals-{report_date.isoformat()}",
            lambda rd=report_date, p=path: dq_fct_geo_intervals.run_dq(
                report_date=rd,
                fct_geo_intervals_path=p,
            ),
        )
        return

    lo = DEFAULT_SRC_START_DATE
    hi = DEFAULT_SRC_END_DATE
    days = _calendar_days_inclusive(lo, hi)
    root = DEFAULT_FCT_GEO_INTERVALS_OUTPUT_ROOT
    logger.info(
        "Starting dq-fct-geo-intervals: days=%s (%s .. %s) fct_geo_intervals_root=%s",
        len(days),
        lo.isoformat(),
        hi.isoformat(),
        root,
    )
    for day in days:
        out = fct_geo_intervals_output_path(day)
        if not out.exists():
            continue
        run_timed_command(
            f"dq-fct-geo-intervals-{day.isoformat()}",
            lambda d=day, p=out: dq_fct_geo_intervals.run_dq(
                report_date=d,
                fct_geo_intervals_path=p,
            ),
        )
    logger.info("dq-fct-geo-intervals completed successfully")


def run_dq_fct_person(
    *,
    report_date: date | None,
    fct_person_path: str | None,
    dim_oksm_path: str | None,
) -> None:
    """DQ ``fct_person``: явный прогон (2 параметра) или цикл DEFAULT_SRC_* по месяцам."""
    explicit = any((report_date, fct_person_path))

    if explicit:
        missing: list[str] = []
        if report_date is None:
            missing.append("--report-date")
        if fct_person_path is None:
            missing.append("--fct-person-path")
        if missing:
            raise SystemExit(
                "dq-fct-person: explicit run requires all parameters; "
                f"missing: {', '.join(missing)}"
            )
        month = report_month_start(report_date)
        path = resolve_stg_monthly_parquet_path(fct_person_path, month)
        oksm = resolve_project_path(dim_oksm_path) if dim_oksm_path else None
        run_timed_command(
            f"dq-fct-person-{month.isoformat()}",
            lambda m=month, p=path, o=oksm: dq_fct_person.run_dq(
                report_date=m,
                fct_person_path=p,
                dim_oksm_path=o,
            ),
        )
        return

    lo = DEFAULT_SRC_START_DATE
    hi = DEFAULT_SRC_END_DATE
    days = _calendar_days_inclusive(lo, hi)
    seen_months: set[date] = set()
    logger.info(
        "Starting dq-fct-person: days=%s (%s .. %s)",
        len(days),
        lo.isoformat(),
        hi.isoformat(),
    )
    for day in days:
        month = report_month_start(day)
        if month in seen_months:
            continue
        out = fct_person_output_path(month)
        if not out.exists():
            continue
        seen_months.add(month)
        run_timed_command(
            f"dq-fct-person-{month.isoformat()}",
            lambda m=month, p=out: dq_fct_person.run_dq(
                report_date=m,
                fct_person_path=p,
            ),
        )
    logger.info("dq-fct-person completed successfully")


def run_dq_src_mobile(
    *,
    datacenter: str | None,
    report_date: date | None,
    mobile_root: str | None,
    cdr_path: str | None,
    sms_path: str | None,
    gprs_path: str | None,
    location_path: str | None,
) -> None:
    """DQ mobile: один прогон (``--report-date``) или цикл дней × ЦОД."""
    root = Path(mobile_root) if mobile_root else None

    if report_date is not None:
        paths = _mobile_mart_run_paths(
            datacenter=datacenter,
            mobile_root=root,
            cdr_path=cdr_path,
            sms_path=sms_path,
            gprs_path=gprs_path,
            location_path=location_path,
            command="dq-src-mobile",
        )
        label = (
            f"dq-src-mobile-{datacenter}-{report_date.isoformat()}"
            if datacenter
            else f"dq-src-mobile-{report_date.isoformat()}"
        )
        run_timed_command(
            label,
            lambda: dq_src_mobile_run(report_date=report_date, **paths),
        )
        return

    lo = DEFAULT_SRC_START_DATE
    hi = DEFAULT_SRC_END_DATE
    days = _calendar_days_inclusive(lo, hi)
    dcs = mobile_datacenter_ids()
    logger.info(
        "Starting dq-src-mobile: days=%s datacenters=%s (%s .. %s)",
        len(days),
        ", ".join(dcs),
        lo.isoformat(),
        hi.isoformat(),
    )
    for day in days:
        for dc in dcs:
            paths = _resolve_mobile_mart_paths(
                datacenter=dc,
                mobile_root=root,
                cdr_path=cdr_path,
                sms_path=sms_path,
                gprs_path=gprs_path,
                location_path=location_path,
            )
            run_timed_command(
                f"dq-src-mobile-{dc}-{day.isoformat()}",
                lambda d=day, p=paths: dq_src_mobile_run(report_date=d, **p),
            )
    logger.info("dq-src-mobile completed successfully")


def run_dq_src_bs(
    *,
    src_bs_path: str | None,
) -> None:
    """DQ полной витрины src_bs с акцентом на распределения."""
    parquet_path = Path(src_bs_path) if src_bs_path else DEFAULT_BS_LAYOUT
    run_timed_command(
        "dq-src-bs",
        lambda: dq_src_bs.run_dq(parquet_path=parquet_path),
    )


def _dq_src_person_month_passes(lo: date, hi: date) -> list[tuple[date, date]]:
    """Разбить период на проходы по календарным месяцам (пересечение с [lo, hi])."""
    if lo > hi:
        return []
    passes: list[tuple[date, date]] = []
    cursor = lo
    while cursor <= hi:
        last_dom = monthrange(cursor.year, cursor.month)[1]
        month_end = date(cursor.year, cursor.month, last_dom)
        pass_end = min(month_end, hi)
        passes.append((cursor, pass_end))
        if pass_end >= hi:
            break
        cursor = pass_end + timedelta(days=1)
    return passes


def dq_src_person_run(
    *,
    start_date: date,
    end_date: date,
    person_root: Path | None = None,
) -> dict:
    root = Path(person_root) if person_root else DEFAULT_SRC_PERSON_OUTPUT_ROOT
    return dq_src_person.run_dq(
        start_date=start_date,
        end_date=end_date,
        person_root=root,
    )


def run_dq_src_person(
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    src_person_path: str | None = None,
) -> None:
    """DQ src_person: 3 месячных прохода по умолчанию или один период по флагам."""
    person_root = (
        Path(src_person_path) if src_person_path else DEFAULT_SRC_PERSON_OUTPUT_ROOT
    )

    if start_date is not None:
        if src_person_path is None:
            raise SystemExit("dq-src-person: --src-person-path is required with --start-date")
        lo = start_date
        hi = end_date if end_date is not None else start_date
        if lo > hi:
            raise ValueError(f"Invalid date range: {lo} > {hi}")
        passes = [(lo, hi)]
    else:
        if end_date is not None:
            raise SystemExit("dq-src-person: --start-date is required with --end-date")
        passes = _dq_src_person_month_passes(DEFAULT_SRC_START_DATE, DEFAULT_SRC_END_DATE)
        if not passes:
            raise ValueError(
                f"Invalid default SRC period: {DEFAULT_SRC_START_DATE} > {DEFAULT_SRC_END_DATE}"
            )

    logger.info(
        "Starting dq-src-person: passes=%s person_root=%s",
        len(passes),
        person_root,
    )
    for idx, (lo, hi) in enumerate(passes, start=1):
        logger.info(
            "dq-src-person pass %s/%s: %s .. %s",
            idx,
            len(passes),
            lo.isoformat(),
            hi.isoformat(),
        )
        run_timed_command(
            f"dq-src-person-{lo.isoformat()}_{hi.isoformat()}",
            lambda l=lo, h=hi: dq_src_person_run(
                start_date=l,
                end_date=h,
                person_root=person_root,
            ),
        )


def run_dq_src_excl(
    *,
    src_imsi_path: str | None = None,
    src_imei_path: str | None = None,
    src_msisdn_path: str | None = None,
) -> None:
    """DQ витрин src_imsi, src_imei, src_msisdn по parquet-путям."""
    imsi_path = Path(src_imsi_path) if src_imsi_path else DEFAULT_SRC_EXCL_IMSI_OUTPUT
    imei_path = Path(src_imei_path) if src_imei_path else DEFAULT_SRC_EXCL_IMEI_OUTPUT
    msisdn_path = Path(src_msisdn_path) if src_msisdn_path else DEFAULT_SRC_EXCL_MSISDN_OUTPUT
    logger.info(
        "Starting dq-src-excl: imsi=%s imei=%s msisdn=%s",
        imsi_path,
        imei_path,
        msisdn_path,
    )
    run_timed_command(
        "dq-src-excl",
        lambda: dq_src_excl.run_dq(
            src_imsi_path=imsi_path,
            src_imei_path=imei_path,
            src_msisdn_path=msisdn_path,
        ),
    )


def _run_build(command: str) -> None:
    fn, config_path = _BUILD_COMMANDS[command]
    logger.info("Starting %s (config=%s)", command, config_path)
    fn()
    logger.info("%s completed successfully", command)


def _run_command(
    command: str,
    *,
    target_per_operator: int | None = None,
    excl_pct_of_ab: float | None = None,
) -> None:
    if command == "build-dim-oktmo":
        run_build_dim_oktmo(csv_path=None, output_path=None)
        return
    if command == "build-dim-time-zones":
        run_build_dim_time_zones(csv_path=None, output_path=None)
        return
    if command == "build-dim-tac":
        run_build_dim_tac(csv_path=None, output_path=None)
        return
    if command == "build-dim-oksm":
        run_build_dim_oksm(csv_path=None, output_path=None)
        return
    if command == "build-src-person":
        logger.info("Starting %s", command)
        person.run(
            compression=DEFAULT_PARQUET_COMPRESSION,
            params=default_person_params(target_per_operator),
        )
        logger.info("%s completed successfully", command)
        return
    if command == "build-src-excl":
        logger.info("Starting %s", command)
        excl.run(
            compression=DEFAULT_PARQUET_COMPRESSION,
            params=default_excl_params(pct_of_ab=excl_pct_of_ab),
        )
        logger.info("%s completed successfully", command)
        return
    if command == "build-src-mobile":
        logger.info("Starting %s", command)
        src_mobile.run_mobile_all(
            bs_parquet_path=DEFAULT_BS_LAYOUT,
            params=default_mobile_params(),
            compression=DEFAULT_PARQUET_COMPRESSION,
        )
        logger.info("%s completed successfully", command)
        return
    if command in _BUILD_COMMANDS:
        _run_build(command)
        return
    if command in _NB_COMMANDS:
        logger.info("Starting %s", command)
        _NB_COMMANDS[command]()
        logger.info("%s completed successfully", command)
        return
    raise ValueError(f"Unknown command: {command}")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mobile",
        description="Mobile OSS пайплайны.",
    )
    parser.add_argument(
        "command",
        choices=sorted(CLI_COMMANDS),
        help="Шаг пайплайна",
    )
    parser.add_argument(
        "--target-per-operator",
        type=int,
        default=None,
        metavar="N",
        help="build-src-person: абонентов на оператора в полный день (по умолчанию 50000)",
    )
    parser.add_argument(
        "--excl-pct-of-ab",
        type=float,
        default=None,
        metavar="PCT",
        help="build-src-excl: %% строк АБ в списках исключений (по умолчанию 0.7)",
    )
    parser.add_argument(
        "--start-date",
        type=_parse_day,
        default=None,
        metavar="YYYY-MM-DD",
        help=(
            f"dq-src-person: начало периода (обязателен с --src-person-path); "
            f"без флага — {len(_dq_src_person_month_passes(DEFAULT_SRC_START_DATE, DEFAULT_SRC_END_DATE))} "
            f"прохода {DEFAULT_SRC_START_DATE}..{DEFAULT_SRC_END_DATE} по календарным месяцам"
        ),
    )
    parser.add_argument(
        "--end-date",
        type=_parse_day,
        default=None,
        metavar="YYYY-MM-DD",
        help="dq-src-person: конец периода (по умолчанию = --start-date или DEFAULT_SRC_END_DATE)",
    )
    parser.add_argument(
        "--dc",
        choices=list(mobile_datacenter_ids()),
        default=None,
        help="build-dds-event / dq-src-mobile: ЦОД (central / far-east); с --report-date — резолв путей витрин",
    )
    parser.add_argument(
        "--report-date",
        type=_parse_day,
        default=None,
        metavar="YYYY-MM-DD",
        help=(
            "dq-src-mobile / build-dds-event: отчётная дата (с --dc или 4 путями — один прогон; без флага — DEFAULT_SRC_* × все ЦОД); "
            "dq-dds-event / build-dds-move-event / build-fct-msisdn-imei / build-fct-msisdn-imsi-operator / build-stg-geo-all / "
            "build-fct-geo-intervals / dq-stg-geo-all / dq-fct-geo-intervals — календарный день; "
            "dq-fct-msisdn-imei / dq-fct-msisdn-imsi-operator — любой день месяца (→ YYYY-MM-01); "
            "build-fct-person / dq-fct-person — YYYY-MM-01"
        ),
    )
    parser.add_argument(
        "--src-bs-path",
        default=None,
        metavar="PATH",
        help=f"build-fct-bs / dq-src-bs: входной src_bs parquet (по умолчанию {DEFAULT_BS_LAYOUT})",
    )
    parser.add_argument(
        "--src-imsi-path",
        default=None,
        metavar="PATH",
        help=(
            f"build-src-excl / dq-src-excl / build-fct-person: parquet src_imsi "
            f"(по умолчанию {DEFAULT_SRC_EXCL_IMSI_OUTPUT})"
        ),
    )
    parser.add_argument(
        "--src-imei-path",
        default=None,
        metavar="PATH",
        help=(
            f"build-src-excl / dq-src-excl / build-fct-person: parquet src_imei "
            f"(по умолчанию {DEFAULT_SRC_EXCL_IMEI_OUTPUT})"
        ),
    )
    parser.add_argument(
        "--src-msisdn-path",
        default=None,
        metavar="PATH",
        help=(
            f"build-src-excl / dq-src-excl / build-fct-person: parquet src_msisdn "
            f"(по умолчанию {DEFAULT_SRC_EXCL_MSISDN_OUTPUT})"
        ),
    )
    parser.add_argument(
        "--csv-path",
        default=None,
        metavar="PATH",
        help=f"build-dim-oktmo / build-dim-time-zones / build-dim-tac / build-dim-oksm: входной CSV (по умолчанию {DEFAULT_DIM_OKTMO_CSV_PATH}, {DEFAULT_DIM_TIME_ZONES_CSV_PATH}, {DEFAULT_DIM_TAC_CSV_PATH} или {DEFAULT_DIM_OKSM_CSV_PATH})",
    )
    parser.add_argument(
        "--oktmo-path",
        default=None,
        metavar="PATH",
        help=(
            f"build-fct-bs / dq-dim-oktmo: dim_oktmo parquet "
            f"(по умолчанию {DEFAULT_DIM_OKTMO_OUTPUT_PATH})"
        ),
    )
    parser.add_argument(
        "--time-zones-path",
        default=None,
        metavar="PATH",
        help=(
            f"build-fct-bs / build-fct-geo-intervals / dq-dim-time-zones: dim_time_zones parquet "
            f"(по умолчанию {DEFAULT_DIM_TIME_ZONES_OUTPUT_PATH})"
        ),
    )
    parser.add_argument(
        "--tac-path",
        default=None,
        metavar="PATH",
        help=(
            f"dq-dim-tac: dim_tac parquet "
            f"(по умолчанию {DEFAULT_DIM_TAC_OUTPUT_PATH})"
        ),
    )
    parser.add_argument(
        "--oksm-path",
        default=None,
        metavar="PATH",
        help=(
            f"dq-dim-oksm: dim_oksm parquet "
            f"(по умолчанию {DEFAULT_DIM_OKSM_OUTPUT_PATH})"
        ),
    )
    parser.add_argument(
        "--fct-bs-path",
        default=None,
        metavar="PATH",
        help=(
            f"dq-fct-bs / build-stg-geo-all / build-fct-geo-intervals: fct_bs parquet "
            f"(по умолчанию {fct_bs_output_path()})"
        ),
    )
    parser.add_argument(
        "--stg-geo-all-path",
        default=None,
        metavar="PATH",
        help=f"build-fct-msisdn-imei / build-fct-msisdn-imsi-operator / build-fct-geo-intervals / dq-stg-geo-all: входной stg_geo_all parquet (по умолчанию {DEFAULT_STG_GEO_ALL_OUTPUT_ROOT})",
    )
    parser.add_argument(
        "--fct-geo-intervals-path",
        default=None,
        metavar="PATH",
        help=f"dq-fct-geo-intervals: входной fct_geo_intervals parquet или каталог (по умолчанию {DEFAULT_FCT_GEO_INTERVALS_OUTPUT_ROOT})",
    )
    parser.add_argument(
        "--fct-msisdn-imsi-path",
        default=None,
        metavar="PATH",
        help=(
            f"build-fct-msisdn-imsi-operator / dq-fct-msisdn-imsi-operator / build-fct-geo-intervals / build-fct-person: "
            f"fct_msisdn_imsi parquet или каталог (по умолчанию {FCT_MSISDN_IMSI_LAYOUT_TEMPLATE})"
        ),
    )
    parser.add_argument(
        "--fct-msisdn-imei-path",
        default=None,
        metavar="PATH",
        help=(
            f"build-fct-msisdn-imei / dq-fct-msisdn-imei / build-fct-geo-intervals / build-fct-person: "
            f"fct_msisdn_imei parquet или каталог (по умолчанию {FCT_MSISDN_IMEI_LAYOUT_TEMPLATE})"
        ),
    )
    parser.add_argument(
        "--mobile-root",
        default=None,
        metavar="PATH",
        help="dq-src-mobile / build-dds-event: корень витрин ЦОД при --dc (по умолчанию data/src/mobile/{dc})",
    )
    parser.add_argument(
        "--cdr-path",
        default=None,
        metavar="PATH",
        help="dq-src-mobile / build-dds-event: корень CDR (по умолчанию data/src/mobile/{dc}/operator/cdr)",
    )
    parser.add_argument(
        "--sms-path",
        default=None,
        metavar="PATH",
        help="dq-src-mobile / build-dds-event: корень SMS (по умолчанию data/src/mobile/{dc}/operator/sms)",
    )
    parser.add_argument(
        "--gprs-path",
        default=None,
        metavar="PATH",
        help="dq-src-mobile / build-dds-event: корень GPRS (по умолчанию data/src/mobile/{dc}/operator/gprs)",
    )
    parser.add_argument(
        "--location-path",
        default=None,
        metavar="PATH",
        help="dq-src-mobile / build-dds-event: корень location (по умолчанию data/src/mobile/{dc}/operator/location)",
    )
    parser.add_argument(
        "--src-person-path",
        default=None,
        metavar="PATH",
        help=f"dq-src-person: корень src_person (обязателен с --start-date; по умолчанию {DEFAULT_SRC_PERSON_OUTPUT_ROOT})",
    )
    parser.add_argument(
        "--dim-tac-path",
        default=None,
        metavar="PATH",
        help=f"build-fct-person: справочник dim_tac для исключения M2M (по умолчанию {DEFAULT_DIM_TAC_OUTPUT_PATH})",
    )
    parser.add_argument(
        "--dim-oksm-path",
        default=None,
        metavar="PATH",
        help=f"build-fct-person / dq-fct-person: справочник dim_oksm (по умолчанию {DEFAULT_DIM_OKSM_OUTPUT_PATH})",
    )
    parser.add_argument(
        "--fct-person-path",
        default=None,
        metavar="PATH",
        help=(
            "dq-fct-person: входной fct_person parquet или каталог "
            "(явный прогон: обязателен вместе с --report-date)"
        ),
    )
    parser.add_argument(
        "--event-dds-path",
        default=None,
        metavar="PATH",
        help=(
            f"dq-dds-event: корень каталога event_dds (по умолчанию {DEFAULT_DDS_EVENT_DDS_ROOT}); "
            f"build-stg-geo-all: корень event_dds или каталог/файл дня"
        ),
    )
    parser.add_argument(
        "--output-path",
        default=None,
        metavar="PATH",
        help=(
            "build-dds-event / build-dim-oktmo / build-dim-time-zones / build-dim-tac / build-dim-oksm / build-fct-bs / build-stg-geo-all / build-fct-msisdn-imei / build-fct-msisdn-imsi-operator / build-fct-geo-intervals / build-fct-person: выходной parquet "
            f"(по умолчанию {DEFAULT_DIM_OKTMO_OUTPUT_PATH}, {DEFAULT_DIM_TIME_ZONES_OUTPUT_PATH}, {DEFAULT_DIM_TAC_OUTPUT_PATH}, {DEFAULT_DIM_OKSM_OUTPUT_PATH}, {FCT_MSISDN_IMSI_LAYOUT_TEMPLATE}, {FCT_MSISDN_IMEI_LAYOUT_TEMPLATE}, "
            f"{FCT_BS_LAYOUT_TEMPLATE}, data/stg/geo_all/{{report_date}}.parquet, {DEFAULT_FCT_GEO_INTERVALS_OUTPUT_ROOT}/{{report_date}}.parquet, data/fct/person/{{report_date}}.parquet)"
        ),
    )
    return parser


def _execute_parsed_args(args: argparse.Namespace) -> None:
    if args.command == "dq-src-mobile":
        run_dq_src_mobile(
            datacenter=args.dc,
            report_date=args.report_date,
            mobile_root=args.mobile_root,
            cdr_path=args.cdr_path,
            sms_path=args.sms_path,
            gprs_path=args.gprs_path,
            location_path=args.location_path,
        )
    elif args.command == "dq-src-bs":
        run_dq_src_bs(
            src_bs_path=args.src_bs_path,
        )
    elif args.command == "dq-src-person":
        run_dq_src_person(
            start_date=args.start_date,
            end_date=args.end_date,
            src_person_path=args.src_person_path,
        )
    elif args.command == "dq-src-excl":
        run_dq_src_excl(
            src_imsi_path=args.src_imsi_path,
            src_imei_path=args.src_imei_path,
            src_msisdn_path=args.src_msisdn_path,
        )
    elif args.command == "build-dds-move-event":
        run_build_dds_move_event(report_date=args.report_date)
    elif args.command == "build-dds-event":
        run_build_dds_event(
            datacenter=args.dc,
            report_date=args.report_date,
            mobile_root=args.mobile_root,
            cdr_path=args.cdr_path,
            sms_path=args.sms_path,
            gprs_path=args.gprs_path,
            location_path=args.location_path,
            output_path=args.output_path,
        )
    elif args.command == "build-dim-oktmo":
        run_build_dim_oktmo(
            csv_path=args.csv_path,
            output_path=args.output_path,
        )
    elif args.command == "dq-dim-oktmo":
        run_dq_dim_oktmo(oktmo_path=args.oktmo_path)
    elif args.command == "build-dim-time-zones":
        run_build_dim_time_zones(
            csv_path=args.csv_path,
            output_path=args.output_path,
        )
    elif args.command == "dq-dim-time-zones":
        run_dq_dim_time_zones(time_zones_path=args.time_zones_path)
    elif args.command == "build-dim-tac":
        run_build_dim_tac(
            csv_path=args.csv_path,
            output_path=args.output_path,
        )
    elif args.command == "dq-dim-tac":
        run_dq_dim_tac(tac_path=args.tac_path)
    elif args.command == "build-dim-oksm":
        run_build_dim_oksm(
            csv_path=args.csv_path,
            output_path=args.output_path,
        )
    elif args.command == "dq-dim-oksm":
        run_dq_dim_oksm(oksm_path=args.oksm_path)
    elif args.command == "build-stg-geo-all":
        run_build_stg_geo_all(
            report_date=args.report_date,
            event_dds_path=args.event_dds_path,
            fct_bs_path=args.fct_bs_path,
            output_path=args.output_path,
        )
    elif args.command == "build-fct-geo-intervals":
        run_build_fct_geo_intervals(
            report_date=args.report_date,
            stg_geo_all_path=args.stg_geo_all_path,
            fct_bs_path=args.fct_bs_path,
            time_zones_path=args.time_zones_path,
            fct_msisdn_imsi_path=args.fct_msisdn_imsi_path,
            fct_msisdn_imei_path=args.fct_msisdn_imei_path,
            output_path=args.output_path,
        )
    elif args.command == "build-fct-msisdn-imei":
        run_build_fct_msisdn_imei(
            report_date=args.report_date,
            stg_geo_all_path=args.stg_geo_all_path,
            output_path=args.output_path,
        )
    elif args.command == "dq-fct-msisdn-imei":
        run_dq_fct_msisdn_imei(
            report_date=args.report_date,
            fct_msisdn_imei_path=args.fct_msisdn_imei_path,
        )
    elif args.command == "build-fct-msisdn-imsi-operator":
        run_build_fct_msisdn_imsi_operator(
            report_date=args.report_date,
            stg_geo_all_path=args.stg_geo_all_path,
            output_path=args.output_path,
        )
    elif args.command == "dq-fct-msisdn-imsi-operator":
        run_dq_fct_msisdn_imsi_operator(
            report_date=args.report_date,
            fct_msisdn_imsi_path=args.fct_msisdn_imsi_path,
        )
    elif args.command == "build-fct-person":
        run_build_fct_person(
            report_date=args.report_date,
            src_person_path=args.src_person_path,
            fct_msisdn_imsi_path=args.fct_msisdn_imsi_path,
            fct_msisdn_imei_path=args.fct_msisdn_imei_path,
            src_excl_imsi_path=args.src_imsi_path,
            src_excl_imei_path=args.src_imei_path,
            src_excl_msisdn_path=args.src_msisdn_path,
            dim_tac_path=args.dim_tac_path,
            dim_oksm_path=args.dim_oksm_path,
            output_path=args.output_path,
        )
    elif args.command == "dq-dds-event":
        run_dq_dds_event(
            report_date=args.report_date,
            event_dds_path=args.event_dds_path,
        )
    elif args.command == "dq-stg-geo-all":
        run_dq_stg_geo_all(
            report_date=args.report_date,
            stg_geo_all_path=args.stg_geo_all_path,
        )
    elif args.command == "dq-fct-geo-intervals":
        run_dq_fct_geo_intervals(
            report_date=args.report_date,
            fct_geo_intervals_path=args.fct_geo_intervals_path,
        )
    elif args.command == "dq-fct-person":
        run_dq_fct_person(
            report_date=args.report_date,
            fct_person_path=args.fct_person_path,
            dim_oksm_path=args.dim_oksm_path,
        )
    elif args.command == "build-fct-bs":
        run_build_fct_bs(
            src_bs_path=args.src_bs_path,
            oktmo_path=args.oktmo_path,
            time_zones_path=args.time_zones_path,
            output_path=args.output_path,
        )
    elif args.command == "dq-fct-bs":
        run_dq_fct_bs(fct_bs_path=args.fct_bs_path)
    else:
        run_timed_command(
            args.command,
            lambda: _run_command(
                args.command,
                target_per_operator=args.target_per_operator,
                excl_pct_of_ab=args.excl_pct_of_ab,
            ),
        )


def run_all(
    *,
    target_per_operator: int | None = None,
    excl_pct_of_ab: float | None = None,
) -> None:
    """Последовательно выполнить все шаги из RUN_ALL_COMMANDS (порядок README)."""
    steps = _run_all_argv_steps()
    _run_pipeline(
        "run-all",
        steps,
        target_per_operator=target_per_operator,
        excl_pct_of_ab=excl_pct_of_ab,
        start_message=(
            f"Starting run-all: {len(steps)} steps "
            f"({DEFAULT_SRC_START_DATE.isoformat()} .. {DEFAULT_SRC_END_DATE.isoformat()}, "
            f"build-fct-person × {len(_distinct_report_months_in_src_window())} months)"
        ),
    )


def run_src(
    *,
    target_per_operator: int | None = None,
    excl_pct_of_ab: float | None = None,
) -> None:
    """Последовательно выполнить RUN_SRC_COMMANDS (build ОКТМО + src-витрины, без dq/nb)."""
    steps = _run_src_argv_steps()
    _run_pipeline(
        "run-src",
        steps,
        target_per_operator=target_per_operator,
        excl_pct_of_ab=excl_pct_of_ab,
        start_message=(
            f"Starting run-src: {len(steps)} steps "
            f"({', '.join(RUN_SRC_COMMANDS)})"
        ),
    )


def main() -> None:
    setup_logging()
    parser = _build_parser()
    args = parser.parse_args(sys.argv[1:])

    with command_run_scope() as run_id:
        logger.info("run_id=%s (metrics -> data/qa/command_timing.jsonl)", run_id)
        if args.command == "run-all":
            run_all(
                target_per_operator=args.target_per_operator,
                excl_pct_of_ab=args.excl_pct_of_ab,
            )
        elif args.command == "run-src":
            run_src(
                target_per_operator=args.target_per_operator,
                excl_pct_of_ab=args.excl_pct_of_ab,
            )
        else:
            _execute_parsed_args(args)


if __name__ == "__main__":
    main()
