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
    run_nb_stg_event,
    run_nb_src_person,
    run_nb_stg_bs,
    run_nb_stg_oksm,
    run_nb_stg_oktmo,
    run_nb_stg_tac,
    run_nb_stg_time_zones,
)
from mobile.pipelines.src import bs, excl, mobile as src_mobile, person
from mobile.pipelines.dq.src import bs as dq_src_bs
from mobile.pipelines.dq.src import excl as dq_src_excl
from mobile.pipelines.dq.src import mobile as dq_src_mobile
from mobile.pipelines.dq.src import person as dq_src_person
from mobile.pipelines.stg import event as stg_event
from mobile.pipelines.stg import geo_all as stg_geo_all
from mobile.pipelines.stg import geo_intervals as stg_geo_intervals
from mobile.pipelines.stg import person as stg_person
from mobile.pipelines.stg import move_event as stg_move_event
from mobile.pipelines.stg import bs as stg_bs
from mobile.pipelines.stg import binding_intervals as stg_binding_intervals
from mobile.pipelines.stg import msisdn_imsi as stg_msisdn_imsi
from mobile.pipelines.stg import msisdn_imei as stg_msisdn_imei
from mobile.pipelines.stg import msisdn_operator as stg_msisdn_operator
from mobile.pipelines.dq.stg import event as dq_stg_event
from mobile.pipelines.dq.stg import geo_intervals as dq_stg_geo_intervals
from mobile.pipelines.dq.stg import geo_all as dq_stg_geo_all
from mobile.pipelines.dq.stg import person as dq_stg_person
from mobile.pipelines.dq.stg import bs as dq_stg_bs, oksm as dq_oksm, oktmo as dq_oktmo, tac as dq_tac, time_zones as dq_time_zones
from mobile.pipelines.stg import oktmo, oksm, tac, time_zones
from mobile.project_paths import (
    DEFAULT_BS_LAYOUT,
    DEFAULT_SRC_EXCL_IMEI_OUTPUT,
    DEFAULT_SRC_EXCL_IMSI_OUTPUT,
    DEFAULT_SRC_EXCL_MSISDN_OUTPUT,
    DEFAULT_SRC_PERSON_OUTPUT_ROOT,
    DEFAULT_STG_GEO_ALL_OUTPUT_ROOT,
    DEFAULT_STG_GEO_INTERVALS_OUTPUT_ROOT,
    DEFAULT_STG_OKTMO_CSV_PATH,
    DEFAULT_STG_OKTMO_OUTPUT_PATH,
    DEFAULT_STG_OKSM_CSV_PATH,
    DEFAULT_STG_OKSM_OUTPUT_PATH,
    DEFAULT_STG_TAC_CSV_PATH,
    DEFAULT_STG_EVENT_DDS_ROOT,
    STG_BS_LAYOUT_TEMPLATE,
    STG_MSISDN_IMSI_LAYOUT_TEMPLATE,
    STG_MSISDN_IMEI_LAYOUT_TEMPLATE,
    DEFAULT_STG_TAC_OUTPUT_PATH,
    DEFAULT_STG_TIME_ZONES_CSV_PATH,
    DEFAULT_STG_TIME_ZONES_OUTPUT_PATH,
    mobile_datacenter_ids,
    mobile_datacenter_root,
    mobile_mart_paths,
    resolve_oktmo_layout,
    resolve_project_path,
    stg_bs_output_path,
    stg_event_dds_output_path,
    stg_event_output_path,
    stg_geo_all_output_path,
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
    "nb-stg-oktmo": run_nb_stg_oktmo,
    "nb-stg-time-zones": run_nb_stg_time_zones,
    "nb-stg-tac": run_nb_stg_tac,
    "nb-stg-oksm": run_nb_stg_oksm,
    "nb-stg-bs": run_nb_stg_bs,
    "nb-src-bs": run_nb_src_bs,
    "nb-src-person": run_nb_src_person,
    "nb-src-excl": run_nb_src_excl,
    "nb-src-mobile": run_nb_src_mobile,
    "nb-stg-event": run_nb_stg_event,
    "nb-perf-metrics": run_nb_perf_metrics,
}

CLI_COMMANDS: tuple[str, ...] = (
    "build-stg-oktmo",
    "dq-stg-oktmo",
    "build-stg-time-zones",
    "dq-stg-time-zones",
    "build-stg-tac",
    "dq-stg-tac",
    "build-stg-oksm",
    "dq-stg-oksm",
    *tuple(_BUILD_COMMANDS),
    "build-src-person",
    "build-src-excl",
    "build-src-mobile",
    "dq-src-mobile",
    "dq-src-bs",
    "dq-src-person",
    "dq-src-excl",
    "build-stg-event",
    "build-stg-geo-all",
    "build-stg-geo-intervals",
    "build-stg-person",
    "build-stg-msisdn-operator",
    "build-stg-msisdn-imsi-month",
    "build-stg-msisdn-imei-month",
    "build-move-event",
    "dq-stg-event",
    "dq-stg-geo-all",
    "dq-stg-geo-intervals",
    "dq-stg-person",
    "build-stg-msisdn-imsi",
    "build-stg-msisdn-imei",
    "build-stg-bs",
    "dq-stg-bs",
    *tuple(_NB_COMMANDS),
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


def build_stg_event_run(
    *,
    report_date: date,
    cdr_path: Path,
    sms_path: Path,
    gprs_path: Path,
    location_path: Path,
    output_path: Path,
) -> dict:
    return stg_event.run_build(
        report_date,
        cdr_path,
        sms_path,
        gprs_path,
        location_path,
        output_path,
    )


def _resolve_build_stg_event_job(
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
        out = Path(output_path) if output_path else stg_event_output_path(datacenter, report_date)
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
            "build-stg-event: pass --dc or all of: " + ", ".join(missing)
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


def _build_stg_event_subprocess_cmd(
    *,
    datacenter: str,
    report_date: date,
    mobile_root: str | None,
) -> list[str]:
    cmd = [
        sys.executable,
        "-m",
        "mobile",
        "build-stg-event",
        "--dc",
        datacenter,
        "--report-date",
        report_date.isoformat(),
    ]
    if mobile_root is not None:
        cmd.extend(["--mobile-root", mobile_root])
    return cmd


def _run_build_stg_event_dc_subprocesses(
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
            cmd = _build_stg_event_subprocess_cmd(
                datacenter=datacenter,
                report_date=day,
                mobile_root=mobile_root,
            )
            logger.info("build-stg-event spawn: %s", " ".join(cmd))
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
                "build-stg-event subprocess ok: dc=%s report_date=%s",
                datacenter,
                day.isoformat(),
            )


def run_build_stg_binding(
    command: str,
    *,
    report_date: date | None,
    stg_geo_all_path: str | None,
    output_path: str | None,
    runner: Callable[..., dict],
) -> None:
    """build-stg-msisdn-imsi / build-stg-msisdn-imei: один день или цикл DEFAULT_SRC_START_DATE..END."""
    geo_all = Path(stg_geo_all_path) if stg_geo_all_path else None
    out = Path(output_path) if output_path else None

    if report_date is not None:
        run_timed_command(
            command,
            lambda: runner(report_date=report_date, stg_geo_all_path=geo_all, output_path=out),
        )
        return

    lo = DEFAULT_SRC_START_DATE
    hi = DEFAULT_SRC_END_DATE
    days = _calendar_days_inclusive(lo, hi)
    logger.info(
        "Starting %s: days=%s (%s .. %s)",
        command,
        len(days),
        lo.isoformat(),
        hi.isoformat(),
    )
    for day in days:
        run_timed_command(
            f"{command}-{day.isoformat()}",
            lambda d=day: runner(report_date=d, stg_geo_all_path=geo_all, output_path=out),
        )
    logger.info("%s completed successfully", command)


def run_dq_stg_oktmo(*, oktmo_path: str | None) -> None:
    """DQ ``stg_oktmo`` (read-only проверки)."""
    path = Path(oktmo_path) if oktmo_path else DEFAULT_STG_OKTMO_OUTPUT_PATH
    run_timed_command(
        "dq-stg-oktmo",
        lambda: dq_oktmo.run_dq(oktmo_path=path),
    )


def run_dq_stg_time_zones(*, time_zones_path: str | None) -> None:
    """DQ ``stg_time_zones`` (read-only проверки)."""
    path = Path(time_zones_path) if time_zones_path else DEFAULT_STG_TIME_ZONES_OUTPUT_PATH
    run_timed_command(
        "dq-stg-time-zones",
        lambda: dq_time_zones.run_dq(time_zones_path=path),
    )


def run_dq_stg_tac(*, tac_path: str | None) -> None:
    """DQ ``stg_tac`` (read-only проверки)."""
    path = Path(tac_path) if tac_path else DEFAULT_STG_TAC_OUTPUT_PATH
    run_timed_command(
        "dq-stg-tac",
        lambda: dq_tac.run_dq(tac_path=path),
    )


def run_build_stg_tac(
    *,
    csv_path: str | None,
    output_path: str | None,
) -> None:
    """build-stg-tac: CSV TACDB → Parquet ``stg_tac``."""
    csv = Path(csv_path) if csv_path else DEFAULT_STG_TAC_CSV_PATH
    out = Path(output_path) if output_path else DEFAULT_STG_TAC_OUTPUT_PATH
    run_timed_command(
        "build-stg-tac",
        lambda: tac.run(csv_path=csv, output_path=out),
    )


def run_build_stg_oksm(
    *,
    csv_path: str | None,
    output_path: str | None,
) -> None:
    """build-stg-oksm: CSV ОКСМ → Parquet ``stg_oksm``."""
    csv = Path(csv_path) if csv_path else DEFAULT_STG_OKSM_CSV_PATH
    out = Path(output_path) if output_path else DEFAULT_STG_OKSM_OUTPUT_PATH
    run_timed_command(
        "build-stg-oksm",
        lambda: oksm.run(csv_path=csv, output_path=out),
    )


def run_dq_stg_oksm(*, oksm_path: str | None) -> None:
    """DQ ``stg_oksm`` (read-only проверки)."""
    path = Path(oksm_path) if oksm_path else DEFAULT_STG_OKSM_OUTPUT_PATH
    run_timed_command(
        "dq-stg-oksm",
        lambda: dq_oksm.run_dq(oksm_path=path),
    )


def run_dq_stg_bs(*, stg_bs_path: str | None) -> None:
    """DQ ``stg_bs`` (read-only проверки)."""
    path = resolve_project_path(stg_bs_path) if stg_bs_path else stg_bs_output_path()
    run_timed_command(
        "dq-stg-bs",
        lambda: dq_stg_bs.run_dq(path),
    )


def run_build_stg_time_zones(
    *,
    csv_path: str | None,
    output_path: str | None,
) -> None:
    """build-stg-time-zones: CSV таймзон → Parquet ``stg_time_zones``."""
    csv = Path(csv_path) if csv_path else DEFAULT_STG_TIME_ZONES_CSV_PATH
    out = Path(output_path) if output_path else DEFAULT_STG_TIME_ZONES_OUTPUT_PATH
    run_timed_command(
        "build-stg-time-zones",
        lambda: time_zones.run(csv_path=csv, output_path=out),
    )


def run_build_stg_oktmo(
    *,
    csv_path: str | None,
    output_path: str | None,
) -> None:
    """build-stg-oktmo: CSV ОКТМО → Parquet ``stg_oktmo``."""
    csv = Path(csv_path) if csv_path else DEFAULT_STG_OKTMO_CSV_PATH
    out = Path(output_path) if output_path else DEFAULT_STG_OKTMO_OUTPUT_PATH
    run_timed_command(
        "build-stg-oktmo",
        lambda: oktmo.run(csv_path=csv, output_path=out),
    )


def run_build_stg_bs(
    *,
    src_bs_path: str | None,
    oktmo_path: str | None,
    time_zones_path: str | None,
    output_path: str | None,
) -> None:
    """build-stg-bs: явный прогон (4 параметра) или один прогон с путями по умолчанию."""
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
                "build-stg-bs: explicit run requires all parameters; "
                f"missing: {', '.join(missing)}"
            )
        run_timed_command(
            "build-stg-bs",
            lambda: stg_bs.run_build(
                src_bs_path=Path(src_bs_path),
                oktmo_path=Path(oktmo_path),
                time_zones_path=Path(time_zones_path),
                output_path=Path(output_path),
            ),
        )
        return

    run_timed_command(
        "build-stg-bs",
        lambda: stg_bs.run_build(
            src_bs_path=DEFAULT_BS_LAYOUT,
            oktmo_path=DEFAULT_STG_OKTMO_OUTPUT_PATH,
            time_zones_path=DEFAULT_STG_TIME_ZONES_OUTPUT_PATH,
            output_path=stg_bs_output_path(),
        ),
    )


def run_build_stg_geo_all(
    *,
    report_date: date | None,
    event_dds_path: str | None,
    stg_bs_path: str | None,
    output_path: str | None,
) -> None:
    """build-stg-geo-all: явный прогон (4 параметра) или цикл DEFAULT_SRC_* по дням."""
    explicit = any((report_date, event_dds_path, stg_bs_path, output_path))

    if explicit:
        missing: list[str] = []
        if report_date is None:
            missing.append("--report-date")
        if event_dds_path is None:
            missing.append("--event-dds-path")
        if stg_bs_path is None:
            missing.append("--stg-bs-path")
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
                stg_bs_path=Path(stg_bs_path),
                output_path=Path(output_path),
            ),
        )
        return

    lo = DEFAULT_SRC_START_DATE
    hi = DEFAULT_SRC_END_DATE
    days = _calendar_days_inclusive(lo, hi)
    dds = DEFAULT_STG_EVENT_DDS_ROOT
    bs = stg_bs_output_path()
    logger.info(
        "Starting build-stg-geo-all: days=%s (%s .. %s) event_dds=%s stg_bs=%s",
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
                stg_bs_path=bs,
                output_path=o,
            ),
        )
    logger.info("build-stg-geo-all completed successfully")


def run_build_stg_geo_intervals(
    *,
    report_date: date | None,
    stg_geo_all_path: str | None,
    stg_bs_path: str | None,
    time_zones_path: str | None,
    stg_msisdn_imsi_path: str | None,
    stg_msisdn_imei_path: str | None,
    output_path: str | None,
) -> None:
    """build-stg-geo-intervals: интервалы пребывания из stg_geo_all."""
    if report_date is None:
        raise SystemExit("build-stg-geo-intervals: --report-date is required")
    geo_all = Path(stg_geo_all_path) if stg_geo_all_path else None
    bs = Path(stg_bs_path) if stg_bs_path else None
    tz = Path(time_zones_path) if time_zones_path else None
    imsi = Path(stg_msisdn_imsi_path) if stg_msisdn_imsi_path else None
    imei = Path(stg_msisdn_imei_path) if stg_msisdn_imei_path else None
    out = Path(output_path) if output_path else None
    run_timed_command(
        "build-stg-geo-intervals",
        lambda: stg_geo_intervals.run_build(
            report_date=report_date,
            stg_geo_all_path=geo_all,
            stg_bs_path=bs,
            time_zones_path=tz,
            stg_msisdn_imsi_path=imsi,
            stg_msisdn_imei_path=imei,
            output_path=out,
        ),
    )


def run_build_stg_msisdn_operator(*, report_date: date | None, src_person_path: str | None, output_path: str | None) -> None:
    if report_date is None:
        raise SystemExit("build-stg-msisdn-operator: --report-date is required")
    if report_date.day != 1:
        raise SystemExit(f"build-stg-msisdn-operator: --report-date must be YYYY-MM-01, got {report_date.isoformat()}")
    run_timed_command(
        "build-stg-msisdn-operator",
        lambda: stg_msisdn_operator.run_build(
            report_date=report_date,
            src_person_path=Path(src_person_path) if src_person_path else None,
            output_path=Path(output_path) if output_path else None,
        ),
    )


def run_build_stg_msisdn_imsi_month(*, report_date: date | None, output_path: str | None) -> None:
    """Устаревшее имя: пересборка месячного ``stg_msisdn_imsi`` из всех ``stg_geo_all`` за месяц."""
    if report_date is None:
        raise SystemExit("build-stg-msisdn-imsi-month: --report-date is required")
    if report_date.day != 1:
        raise SystemExit(f"build-stg-msisdn-imsi-month: --report-date must be YYYY-MM-01, got {report_date.isoformat()}")
    if output_path is not None:
        logger.warning("build-stg-msisdn-imsi-month: --output-path ignored; use build-stg-msisdn-imsi --report-date <day>")
    run_timed_command(
        "build-stg-msisdn-imsi-month",
        lambda: stg_binding_intervals.refresh_month_bindings_from_geo(report_date),
    )


def run_build_stg_msisdn_imei_month(*, report_date: date | None, output_path: str | None) -> None:
    """Устаревшее имя: то же, что ``build-stg-msisdn-imsi-month`` (обновляет оба binding)."""
    run_build_stg_msisdn_imsi_month(report_date=report_date, output_path=output_path)


def run_build_stg_person(
    *,
    report_date: date | None,
    src_person_path: str | None,
    stg_msisdn_imsi_path: str | None,
    stg_msisdn_imei_path: str | None,
    stg_tac_path: str | None,
    stg_oksm_path: str | None,
    output_path: str | None,
) -> None:
    """build-stg-person: месячный срез person для физлиц из src_person (``--report-date`` = YYYY-MM-01)."""
    if report_date is None:
        raise SystemExit("build-stg-person: --report-date is required")
    if report_date.day != 1:
        raise SystemExit(f"build-stg-person: --report-date must be YYYY-MM-01, got {report_date.isoformat()}")
    src = Path(src_person_path) if src_person_path else None
    imsi = Path(stg_msisdn_imsi_path) if stg_msisdn_imsi_path else None
    imei = Path(stg_msisdn_imei_path) if stg_msisdn_imei_path else None
    tac = Path(stg_tac_path) if stg_tac_path else None
    oksm = Path(stg_oksm_path) if stg_oksm_path else None
    out = Path(output_path) if output_path else None
    run_timed_command(
        "build-stg-person",
        lambda: stg_person.run_build(
            report_date=report_date,
            src_person_path=src,
            stg_msisdn_imsi_path=imsi,
            stg_msisdn_imei_path=imei,
            stg_tac_path=tac,
            stg_oksm_path=oksm,
            output_path=out,
        ),
    )


def run_build_move_event(*, report_date: date | None) -> None:
    """build-move-event: один день или цикл DEFAULT_SRC_START_DATE..END."""
    if report_date is not None:
        run_timed_command(
            "build-move-event",
            lambda: stg_move_event.run_move(report_date),
        )
        return

    lo = DEFAULT_SRC_START_DATE
    hi = DEFAULT_SRC_END_DATE
    days = _calendar_days_inclusive(lo, hi)
    logger.info(
        "Starting build-move-event: days=%s (%s .. %s)",
        len(days),
        lo.isoformat(),
        hi.isoformat(),
    )
    for day in days:
        run_timed_command(
            f"build-move-event-{day.isoformat()}",
            lambda d=day: stg_move_event.run_move(d),
        )
    logger.info("build-move-event completed successfully")


def run_build_stg_event(
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
    """build-stg-event: явный прогон (5 параметров) или оркестратор DEFAULT_SRC_* × ЦОД (2 subprocess/ЦОД)."""
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
            raise SystemExit("build-stg-event: --report-date is required for explicit run")
        paths, out = _resolve_build_stg_event_job(
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
            f"build-stg-event-{datacenter}-{report_date.isoformat()}"
            if datacenter
            else f"build-stg-event-{report_date.isoformat()}"
        )
        run_timed_command(
            label,
            lambda: build_stg_event_run(
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
        "Starting build-stg-event: days=%s datacenters=%s processes_per_dc=%s (%s .. %s)",
        len(days),
        ", ".join(dcs),
        _BUILD_STG_EVENT_DC_WORKERS,
        lo.isoformat(),
        hi.isoformat(),
    )
    mobile_root_str = str(root) if root is not None else None
    for dc in dcs:
        logger.info(
            "build-stg-event datacenter=%s: %s days, %s parallel subprocesses",
            dc,
            len(days),
            _BUILD_STG_EVENT_DC_WORKERS,
        )
        _run_build_stg_event_dc_subprocesses(
            datacenter=dc,
            days=days,
            mobile_root=mobile_root_str,
        )
    logger.info("build-stg-event completed successfully")


def dq_stg_event_run(
    *,
    report_date: date,
    event_dds_root: Path,
) -> dict:
    return dq_stg_event.run_dq(report_date, event_dds_root)


def run_dq_stg_event(
    *,
    report_date: date | None,
    event_dds_path: str | None,
) -> None:
    """DQ event_dds: один день (``--report-date``) или цикл DEFAULT_SRC_* по дням."""
    root = Path(event_dds_path) if event_dds_path else DEFAULT_STG_EVENT_DDS_ROOT

    if report_date is not None:
        run_timed_command(
            f"dq-stg-event-{report_date.isoformat()}",
            lambda: dq_stg_event_run(report_date=report_date, event_dds_root=root),
        )
        return

    lo = DEFAULT_SRC_START_DATE
    hi = DEFAULT_SRC_END_DATE
    days = _calendar_days_inclusive(lo, hi)
    logger.info(
        "Starting dq-stg-event: days=%s (%s .. %s) event_dds_root=%s",
        len(days),
        lo.isoformat(),
        hi.isoformat(),
        root,
    )
    for day in days:
        run_timed_command(
            f"dq-stg-event-{day.isoformat()}",
            lambda d=day: dq_stg_event_run(report_date=d, event_dds_root=root),
        )
    logger.info("dq-stg-event completed successfully")


def run_dq_stg_geo_all(
    *,
    report_date: date | None,
    stg_geo_all_path: str | None,
) -> None:
    """DQ ``stg_geo_all`` за день (read-only проверки)."""
    if report_date is None:
        raise SystemExit("dq-stg-geo-all: --report-date is required")
    path = Path(stg_geo_all_path) if stg_geo_all_path else None
    run_timed_command(
        "dq-stg-geo-all",
        lambda: dq_stg_geo_all.run_dq(report_date=report_date, stg_geo_all_path=path),
    )


def run_dq_stg_geo_intervals(
    *,
    report_date: date | None,
    stg_geo_intervals_path: str | None,
) -> None:
    """DQ ``stg_geo_intervals`` за день (read-only проверки)."""
    if report_date is None:
        raise SystemExit("dq-stg-geo-intervals: --report-date is required")
    path = Path(stg_geo_intervals_path) if stg_geo_intervals_path else None
    run_timed_command(
        "dq-stg-geo-intervals",
        lambda: dq_stg_geo_intervals.run_dq(report_date=report_date, stg_geo_intervals_path=path),
    )


def run_dq_stg_person(
    *,
    report_date: date | None,
    stg_person_path: str | None,
    stg_person_sim_path: str | None,
    stg_oksm_path: str | None,
    stg_person_ledger_path: str | None,
) -> None:
    """DQ ``stg_person`` / ``stg_person_sim`` за месяц (read-only проверки)."""
    if report_date is None:
        raise SystemExit("dq-stg-person: --report-date is required")
    if report_date.day != 1:
        raise SystemExit(f"dq-stg-person: --report-date must be YYYY-MM-01, got {report_date.isoformat()}")
    person_path = Path(stg_person_path) if stg_person_path else None
    sim_path = Path(stg_person_sim_path) if stg_person_sim_path else None
    oksm_path = Path(stg_oksm_path) if stg_oksm_path else None
    ledger_path = Path(stg_person_ledger_path) if stg_person_ledger_path else None
    run_timed_command(
        "dq-stg-person",
        lambda: dq_stg_person.run_dq(
            report_date=report_date,
            stg_person_path=person_path,
            stg_person_sim_path=sim_path,
            stg_oksm_path=oksm_path,
            stg_person_ledger_path=ledger_path,
        ),
    )


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
    if command == "build-stg-oktmo":
        run_build_stg_oktmo(csv_path=None, output_path=None)
        return
    if command == "build-stg-time-zones":
        run_build_stg_time_zones(csv_path=None, output_path=None)
        return
    if command == "build-stg-tac":
        run_build_stg_tac(csv_path=None, output_path=None)
        return
    if command == "build-stg-oksm":
        run_build_stg_oksm(csv_path=None, output_path=None)
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
        help="build-stg-event / dq-src-mobile: ЦОД (central / far-east); с --report-date — резолв путей витрин",
    )
    parser.add_argument(
        "--report-date",
        type=_parse_day,
        default=None,
        metavar="YYYY-MM-DD",
        help="dq-src-mobile / build-stg-event: отчётная дата (с --dc или 4 путями — один прогон; без флага — DEFAULT_SRC_* × все ЦОД); dq-stg-event / build-move-event / build-stg-msisdn-* / build-stg-geo-all / build-stg-geo-intervals / build-stg-person / dq-stg-person / dq-stg-geo-all / dq-stg-geo-intervals — день или YYYY-MM-01 для person",
    )
    parser.add_argument(
        "--src-bs-path",
        default=None,
        metavar="PATH",
        help=f"build-stg-bs / dq-src-bs: входной src_bs parquet (по умолчанию {DEFAULT_BS_LAYOUT})",
    )
    parser.add_argument(
        "--src-imsi-path",
        default=None,
        metavar="PATH",
        help=f"dq-src-excl: parquet src_imsi (по умолчанию {DEFAULT_SRC_EXCL_IMSI_OUTPUT})",
    )
    parser.add_argument(
        "--src-imei-path",
        default=None,
        metavar="PATH",
        help=f"dq-src-excl: parquet src_imei (по умолчанию {DEFAULT_SRC_EXCL_IMEI_OUTPUT})",
    )
    parser.add_argument(
        "--src-msisdn-path",
        default=None,
        metavar="PATH",
        help=f"dq-src-excl: parquet src_msisdn (по умолчанию {DEFAULT_SRC_EXCL_MSISDN_OUTPUT})",
    )
    parser.add_argument(
        "--csv-path",
        default=None,
        metavar="PATH",
        help=f"build-stg-oktmo / build-stg-time-zones / build-stg-tac / build-stg-oksm: входной CSV (по умолчанию {DEFAULT_STG_OKTMO_CSV_PATH}, {DEFAULT_STG_TIME_ZONES_CSV_PATH}, {DEFAULT_STG_TAC_CSV_PATH} или {DEFAULT_STG_OKSM_CSV_PATH})",
    )
    parser.add_argument(
        "--oktmo-path",
        default=None,
        metavar="PATH",
        help=(
            f"build-stg-bs / dq-stg-oktmo: stg_oktmo parquet "
            f"(по умолчанию {DEFAULT_STG_OKTMO_OUTPUT_PATH})"
        ),
    )
    parser.add_argument(
        "--time-zones-path",
        default=None,
        metavar="PATH",
        help=(
            f"build-stg-bs / build-stg-geo-intervals / dq-stg-time-zones: stg_time_zones parquet "
            f"(по умолчанию {DEFAULT_STG_TIME_ZONES_OUTPUT_PATH})"
        ),
    )
    parser.add_argument(
        "--tac-path",
        default=None,
        metavar="PATH",
        help=(
            f"dq-stg-tac: stg_tac parquet "
            f"(по умолчанию {DEFAULT_STG_TAC_OUTPUT_PATH})"
        ),
    )
    parser.add_argument(
        "--oksm-path",
        default=None,
        metavar="PATH",
        help=(
            f"dq-stg-oksm: stg_oksm parquet "
            f"(по умолчанию {DEFAULT_STG_OKSM_OUTPUT_PATH})"
        ),
    )
    parser.add_argument(
        "--stg-bs-path",
        default=None,
        metavar="PATH",
        help=(
            f"dq-stg-bs / build-stg-geo-all / build-stg-geo-intervals: stg_bs parquet "
            f"(по умолчанию {stg_bs_output_path()})"
        ),
    )
    parser.add_argument(
        "--stg-geo-all-path",
        default=None,
        metavar="PATH",
        help=f"build-stg-msisdn-imsi / build-stg-msisdn-imei / build-stg-geo-intervals / dq-stg-geo-all: входной stg_geo_all parquet или каталог (по умолчанию {DEFAULT_STG_GEO_ALL_OUTPUT_ROOT})",
    )
    parser.add_argument(
        "--stg-geo-intervals-path",
        default=None,
        metavar="PATH",
        help=f"dq-stg-geo-intervals: входной stg_geo_intervals parquet или каталог (по умолчанию {DEFAULT_STG_GEO_INTERVALS_OUTPUT_ROOT})",
    )
    parser.add_argument(
        "--stg-msisdn-imsi-path",
        default=None,
        metavar="PATH",
        help=f"build-stg-geo-intervals / build-stg-person: входной stg_msisdn_imsi parquet (по умолчанию {STG_MSISDN_IMSI_LAYOUT_TEMPLATE})",
    )
    parser.add_argument(
        "--stg-msisdn-imei-path",
        default=None,
        metavar="PATH",
        help=f"build-stg-geo-intervals / build-stg-person: входной stg_msisdn_imei parquet (по умолчанию {STG_MSISDN_IMEI_LAYOUT_TEMPLATE})",
    )
    parser.add_argument(
        "--mobile-root",
        default=None,
        metavar="PATH",
        help="dq-src-mobile / build-stg-event: корень витрин ЦОД при --dc (по умолчанию data/src/mobile/{dc})",
    )
    parser.add_argument(
        "--cdr-path",
        default=None,
        metavar="PATH",
        help="dq-src-mobile / build-stg-event: корень CDR (по умолчанию data/src/mobile/{dc}/operator/cdr)",
    )
    parser.add_argument(
        "--sms-path",
        default=None,
        metavar="PATH",
        help="dq-src-mobile / build-stg-event: корень SMS (по умолчанию data/src/mobile/{dc}/operator/sms)",
    )
    parser.add_argument(
        "--gprs-path",
        default=None,
        metavar="PATH",
        help="dq-src-mobile / build-stg-event: корень GPRS (по умолчанию data/src/mobile/{dc}/operator/gprs)",
    )
    parser.add_argument(
        "--location-path",
        default=None,
        metavar="PATH",
        help="dq-src-mobile / build-stg-event: корень location (по умолчанию data/src/mobile/{dc}/operator/location)",
    )
    parser.add_argument(
        "--src-person-path",
        default=None,
        metavar="PATH",
        help=f"dq-src-person: корень src_person (обязателен с --start-date; по умолчанию {DEFAULT_SRC_PERSON_OUTPUT_ROOT})",
    )
    parser.add_argument(
        "--stg-tac-path",
        default=None,
        metavar="PATH",
        help=f"build-stg-person: справочник stg_tac для исключения M2M (по умолчанию {DEFAULT_STG_TAC_OUTPUT_PATH})",
    )
    parser.add_argument(
        "--stg-oksm-path",
        default=None,
        metavar="PATH",
        help=f"build-stg-person / dq-stg-person: справочник stg_oksm (по умолчанию {DEFAULT_STG_OKSM_OUTPUT_PATH})",
    )
    parser.add_argument(
        "--stg-person-path",
        default=None,
        metavar="PATH",
        help="dq-stg-person: входной stg_person parquet (по умолчанию data/stg/person/{YYYY-MM-01}.parquet)",
    )
    parser.add_argument(
        "--stg-person-sim-path",
        default=None,
        metavar="PATH",
        help="dq-stg-person: входной stg_person_sim parquet (по умолчанию data/stg/person_sim/{YYYY-MM-01}.parquet)",
    )
    parser.add_argument(
        "--stg-person-ledger-path",
        default=None,
        metavar="PATH",
        help="dq-stg-person: входной stg_person_id_ledger parquet (по умолчанию data/stg/person_id_ledger/{YYYY-MM-01}.parquet)",
    )
    parser.add_argument(
        "--event-dds-path",
        default=None,
        metavar="PATH",
        help=(
            f"dq-stg-event: корень каталога event_dds (по умолчанию {DEFAULT_STG_EVENT_DDS_ROOT}); "
            f"build-stg-geo-all: корень event_dds или каталог/файл дня"
        ),
    )
    parser.add_argument(
        "--output-path",
        default=None,
        metavar="PATH",
        help=(
            "build-stg-event / build-stg-oktmo / build-stg-time-zones / build-stg-tac / build-stg-oksm / build-stg-msisdn-imsi / build-stg-msisdn-imei / build-stg-bs / build-stg-geo-all / build-stg-geo-intervals / build-stg-person: выходной parquet "
            f"(по умолчанию {DEFAULT_STG_OKTMO_OUTPUT_PATH}, {DEFAULT_STG_TIME_ZONES_OUTPUT_PATH}, {DEFAULT_STG_TAC_OUTPUT_PATH}, {DEFAULT_STG_OKSM_OUTPUT_PATH}, {STG_MSISDN_IMSI_LAYOUT_TEMPLATE}, {STG_MSISDN_IMEI_LAYOUT_TEMPLATE}, "
            f"{STG_BS_LAYOUT_TEMPLATE}, data/stg/geo_all/{{report_date}}.parquet, {DEFAULT_STG_GEO_INTERVALS_OUTPUT_ROOT}/{{report_date}}.parquet, data/stg/person/{{report_date}}.parquet)"
        ),
    )
    return parser


def main() -> None:
    setup_logging()
    parser = _build_parser()
    args = parser.parse_args(sys.argv[1:])

    with command_run_scope() as run_id:
        logger.info("run_id=%s (metrics -> data/qa/command_timing.jsonl)", run_id)
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
        elif args.command == "build-move-event":
            run_build_move_event(report_date=args.report_date)
        elif args.command == "build-stg-event":
            run_build_stg_event(
                datacenter=args.dc,
                report_date=args.report_date,
                mobile_root=args.mobile_root,
                cdr_path=args.cdr_path,
                sms_path=args.sms_path,
                gprs_path=args.gprs_path,
                location_path=args.location_path,
                output_path=args.output_path,
            )
        elif args.command == "build-stg-oktmo":
            run_build_stg_oktmo(
                csv_path=args.csv_path,
                output_path=args.output_path,
            )
        elif args.command == "dq-stg-oktmo":
            run_dq_stg_oktmo(oktmo_path=args.oktmo_path)
        elif args.command == "build-stg-time-zones":
            run_build_stg_time_zones(
                csv_path=args.csv_path,
                output_path=args.output_path,
            )
        elif args.command == "dq-stg-time-zones":
            run_dq_stg_time_zones(time_zones_path=args.time_zones_path)
        elif args.command == "build-stg-tac":
            run_build_stg_tac(
                csv_path=args.csv_path,
                output_path=args.output_path,
            )
        elif args.command == "dq-stg-tac":
            run_dq_stg_tac(tac_path=args.tac_path)
        elif args.command == "build-stg-oksm":
            run_build_stg_oksm(
                csv_path=args.csv_path,
                output_path=args.output_path,
            )
        elif args.command == "dq-stg-oksm":
            run_dq_stg_oksm(oksm_path=args.oksm_path)
        elif args.command == "build-stg-geo-all":
            run_build_stg_geo_all(
                report_date=args.report_date,
                event_dds_path=args.event_dds_path,
                stg_bs_path=args.stg_bs_path,
                output_path=args.output_path,
            )
        elif args.command == "build-stg-geo-intervals":
            run_build_stg_geo_intervals(
                report_date=args.report_date,
                stg_geo_all_path=args.stg_geo_all_path,
                stg_bs_path=args.stg_bs_path,
                time_zones_path=args.time_zones_path,
                stg_msisdn_imsi_path=args.stg_msisdn_imsi_path,
                stg_msisdn_imei_path=args.stg_msisdn_imei_path,
                output_path=args.output_path,
            )
        elif args.command == "build-stg-msisdn-operator":
            run_build_stg_msisdn_operator(
                report_date=args.report_date,
                src_person_path=args.src_person_path,
                output_path=args.output_path,
            )
        elif args.command == "build-stg-msisdn-imsi-month":
            run_build_stg_msisdn_imsi_month(
                report_date=args.report_date,
                output_path=args.output_path,
            )
        elif args.command == "build-stg-msisdn-imei-month":
            run_build_stg_msisdn_imei_month(
                report_date=args.report_date,
                output_path=args.output_path,
            )
        elif args.command == "build-stg-person":
            run_build_stg_person(
                report_date=args.report_date,
                src_person_path=args.src_person_path,
                stg_msisdn_imsi_path=args.stg_msisdn_imsi_path,
                stg_msisdn_imei_path=args.stg_msisdn_imei_path,
                stg_tac_path=args.stg_tac_path,
                stg_oksm_path=args.stg_oksm_path,
                output_path=args.output_path,
            )
        elif args.command == "dq-stg-event":
            run_dq_stg_event(
                report_date=args.report_date,
                event_dds_path=args.event_dds_path,
            )
        elif args.command == "dq-stg-geo-all":
            run_dq_stg_geo_all(
                report_date=args.report_date,
                stg_geo_all_path=args.stg_geo_all_path,
            )
        elif args.command == "dq-stg-geo-intervals":
            run_dq_stg_geo_intervals(
                report_date=args.report_date,
                stg_geo_intervals_path=args.stg_geo_intervals_path,
            )
        elif args.command == "dq-stg-person":
            run_dq_stg_person(
                report_date=args.report_date,
                stg_person_path=args.stg_person_path,
                stg_person_sim_path=args.stg_person_sim_path,
                stg_oksm_path=args.stg_oksm_path,
                stg_person_ledger_path=args.stg_person_ledger_path,
            )
        elif args.command == "build-stg-msisdn-imsi":
            run_build_stg_binding(
                "build-stg-msisdn-imsi",
                report_date=args.report_date,
                stg_geo_all_path=args.stg_geo_all_path,
                output_path=args.output_path,
                runner=stg_msisdn_imsi.run_build,
            )
        elif args.command == "build-stg-msisdn-imei":
            run_build_stg_binding(
                "build-stg-msisdn-imei",
                report_date=args.report_date,
                stg_geo_all_path=args.stg_geo_all_path,
                output_path=args.output_path,
                runner=stg_msisdn_imei.run_build,
            )
        elif args.command == "build-stg-bs":
            run_build_stg_bs(
                src_bs_path=args.src_bs_path,
                oktmo_path=args.oktmo_path,
                time_zones_path=args.time_zones_path,
                output_path=args.output_path,
            )
        elif args.command == "dq-stg-bs":
            run_dq_stg_bs(stg_bs_path=args.stg_bs_path)
        else:
            run_timed_command(
                args.command,
                lambda: _run_command(
                    args.command,
                    target_per_operator=args.target_per_operator,
                    excl_pct_of_ab=args.excl_pct_of_ab,
                ),
            )


if __name__ == "__main__":
    main()
