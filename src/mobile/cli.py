"""CLI для mobile-пайплайнов."""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Callable
from datetime import date

from mobile.cli_defaults import (
    DEFAULT_PARQUET_COMPRESSION,
    DEFAULT_STG_DAY,
    default_bs_params,
    default_build_stg_day_params,
    default_excl_params,
    default_mobile_params,
    default_person_params,
)
from mobile.command_timing import command_run_scope, run_timed_command
from mobile.logging_config import setup_logging
from mobile.pipelines.nb import perf_metrics as nb_perf_metrics
from mobile.pipelines.src import bs, excl, mobile as src_mobile, person
from mobile.pipelines.dq.stg import oktmo as dq_oktmo, tac as dq_tac, time_zones as dq_time_zones
from mobile.pipelines.stg import day as stg_day
from mobile.pipelines.stg import oktmo, tac, time_zones
from mobile.pipelines.stg.day import BUILD_STG_DAY_STEPS
from mobile.project_paths import (
    DEFAULT_SRC_CDR_CONFIG_PATH,
    DEFAULT_SRC_GPRS_CONFIG_PATH,
    DEFAULT_SRC_IMEI_CONFIG_PATH,
    DEFAULT_SRC_IMSI_CONFIG_PATH,
    DEFAULT_SRC_LOCATION_CONFIG_PATH,
    DEFAULT_SRC_MSISDN_CONFIG_PATH,
    DEFAULT_SRC_PERSON_CONFIG_PATH,
    DEFAULT_SRC_SMS_CONFIG_PATH,
    DEFAULT_BS_LAYOUT,
    DEFAULT_STG_OKTMO_CSV_PATH,
    DEFAULT_STG_OKTMO_OUTPUT_PATH,
    DEFAULT_STG_TAC_CSV_PATH,
    DEFAULT_STG_TAC_OUTPUT_PATH,
    DEFAULT_STG_TIME_ZONES_CSV_PATH,
    DEFAULT_STG_TIME_ZONES_OUTPUT_PATH,
    resolve_oktmo_layout,
)

logger = logging.getLogger(__name__)

_BUILD_COMMANDS: dict[str, tuple[Callable[[], None], str]] = {
    "build-stg-oktmo": (
        lambda compression=DEFAULT_PARQUET_COMPRESSION: oktmo.run(
            csv_path=DEFAULT_STG_OKTMO_CSV_PATH,
            output_path=DEFAULT_STG_OKTMO_OUTPUT_PATH,
            compression=compression,
        ),
        str(DEFAULT_STG_OKTMO_CSV_PATH),
    ),
    "build-stg-time-zones": (
        lambda compression=DEFAULT_PARQUET_COMPRESSION: time_zones.run(
            csv_path=DEFAULT_STG_TIME_ZONES_CSV_PATH,
            output_path=DEFAULT_STG_TIME_ZONES_OUTPUT_PATH,
            compression=compression,
        ),
        str(DEFAULT_STG_TIME_ZONES_CSV_PATH),
    ),
    "build-stg-tac": (
        lambda compression=DEFAULT_PARQUET_COMPRESSION: tac.run(
            csv_path=DEFAULT_STG_TAC_CSV_PATH,
            output_path=DEFAULT_STG_TAC_OUTPUT_PATH,
            compression=compression,
        ),
        str(DEFAULT_STG_TAC_CSV_PATH),
    ),
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

_DQ_COMMANDS: dict[str, tuple[Callable[[], dict], str]] = {
    "dq-stg-oktmo": (
        lambda: dq_oktmo.run_dq(DEFAULT_STG_OKTMO_OUTPUT_PATH),
        str(DEFAULT_STG_OKTMO_OUTPUT_PATH),
    ),
    "dq-stg-time-zones": (
        lambda: dq_time_zones.run_dq(DEFAULT_STG_TIME_ZONES_OUTPUT_PATH),
        str(DEFAULT_STG_TIME_ZONES_OUTPUT_PATH),
    ),
    "dq-stg-tac": (
        lambda: dq_tac.run_dq(DEFAULT_STG_TAC_OUTPUT_PATH),
        str(DEFAULT_STG_TAC_OUTPUT_PATH),
    ),
}

_NB_COMMANDS: dict[str, Callable[[], None]] = {
    "nb-perf-metrics": nb_perf_metrics.run,
}

CLI_COMMANDS: tuple[str, ...] = (
    "build-stg-day",
    *tuple(_BUILD_COMMANDS),
    "build-src-person",
    "build-src-excl",
    "build-src-mobile",
    *tuple(_DQ_COMMANDS),
    *tuple(_NB_COMMANDS),
)


def _parse_day(value: str) -> date:
    return date.fromisoformat(value)


def build_stg_day(*, day: date | None = None) -> None:
    params = default_build_stg_day_params(day)
    logger.info(
        "Starting build-stg-day: day=%s steps=%s",
        params.day.isoformat(),
        ", ".join(BUILD_STG_DAY_STEPS),
    )
    for step in BUILD_STG_DAY_STEPS:
        run_timed_command(
            step,
            lambda s=step, p=params: _run_build_stg_day_step(s, p),
        )
    logger.info("build-stg-day completed successfully")


def _run_build_stg_day_step(step: str, params: stg_day.BuildStgDayParams) -> None:
    if step == "build-stg-oktmo":
        oktmo.run(
            csv_path=params.oktmo_csv_path,
            output_path=params.oktmo_output_path,
            compression=params.compression,
        )
        return
    if step == "dq-stg-oktmo":
        dq_oktmo.run_dq(params.oktmo_output_path)
        return
    if step == "build-stg-time-zones":
        time_zones.run(
            csv_path=params.time_zones_csv_path,
            output_path=params.time_zones_output_path,
            compression=params.compression,
        )
        return
    if step == "dq-stg-time-zones":
        dq_time_zones.run_dq(params.time_zones_output_path)
        return
    if step == "build-stg-tac":
        tac.run(
            csv_path=params.tac_csv_path,
            output_path=params.tac_output_path,
            compression=params.compression,
        )
        return
    if step == "dq-stg-tac":
        dq_tac.run_dq(params.tac_output_path)
        return
    raise ValueError(f"Unknown build-stg-day step: {step}")


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
    if command == "build-src-person":
        logger.info("Starting %s (config=%s)", command, DEFAULT_SRC_PERSON_CONFIG_PATH)
        person.run_from_config(
            DEFAULT_SRC_PERSON_CONFIG_PATH,
            default_person_params(target_per_operator),
        )
        logger.info("%s completed successfully", command)
        return
    if command == "build-src-excl":
        logger.info("Starting %s", command)
        excl.run_from_config(
            DEFAULT_SRC_PERSON_CONFIG_PATH,
            DEFAULT_SRC_IMSI_CONFIG_PATH,
            DEFAULT_SRC_IMEI_CONFIG_PATH,
            DEFAULT_SRC_MSISDN_CONFIG_PATH,
            default_excl_params(pct_of_ab=excl_pct_of_ab),
        )
        logger.info("%s completed successfully", command)
        return
    if command == "build-src-mobile":
        logger.info("Starting %s", command)
        src_mobile.run_mobile_all(
            bs_parquet_path=DEFAULT_BS_LAYOUT,
            person_config_path=DEFAULT_SRC_PERSON_CONFIG_PATH,
            params=default_mobile_params(),
            cdr_config_path=DEFAULT_SRC_CDR_CONFIG_PATH,
            sms_config_path=DEFAULT_SRC_SMS_CONFIG_PATH,
            gprs_config_path=DEFAULT_SRC_GPRS_CONFIG_PATH,
            location_config_path=DEFAULT_SRC_LOCATION_CONFIG_PATH,
        )
        logger.info("%s completed successfully", command)
        return
    if command in _BUILD_COMMANDS:
        _run_build(command)
        return
    if command in _DQ_COMMANDS:
        fn, parquet_path = _DQ_COMMANDS[command]
        logger.info("Starting %s (parquet=%s)", command, parquet_path)
        fn()
        logger.info("%s completed successfully", command)
        return
    if command in _NB_COMMANDS:
        logger.info("Starting %s", command)
        _NB_COMMANDS[command]()
        logger.info("%s completed successfully", command)
        return
    raise ValueError(f"Unknown command: {command}")


BUILD_STEPS: tuple[str, ...] = (
    *tuple(_BUILD_COMMANDS),
    "build-src-person",
    "build-src-excl",
    "build-src-mobile",
)
BUILD_SRC_STEPS: tuple[str, ...] = BUILD_STEPS + ("nb-perf-metrics",)


def build_src(
    *,
    target_per_operator: int | None = None,
    excl_pct_of_ab: float | None = None,
) -> None:
    logger.info("Starting build-src: %s", ", ".join(BUILD_SRC_STEPS))
    for command in BUILD_SRC_STEPS:
        run_timed_command(
            command,
            lambda cmd=command: _run_command(
                cmd,
                target_per_operator=target_per_operator,
                excl_pct_of_ab=excl_pct_of_ab,
            ),
        )
    logger.info("build-src completed successfully")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mobile",
        description="Mobile OSS пайплайны.",
    )
    parser.add_argument(
        "command",
        choices=sorted({*CLI_COMMANDS, "build-src"}),
        help="Шаг пайплайна, build-stg-day или build-src",
    )
    parser.add_argument(
        "--day",
        type=_parse_day,
        default=None,
        metavar="YYYY-MM-DD",
        help=f"build-stg-day: календарный срез STG (по умолчанию {DEFAULT_STG_DAY.isoformat()})",
    )
    parser.add_argument(
        "--target-per-operator",
        type=int,
        default=None,
        metavar="N",
        help="build-src-person / build-src: абонентов на оператора в полный день (по умолчанию 50000)",
    )
    parser.add_argument(
        "--excl-pct-of-ab",
        type=float,
        default=None,
        metavar="PCT",
        help="build-src-excl / build-src: %% строк АБ в списках исключений (по умолчанию 0.7)",
    )
    return parser


def main() -> None:
    setup_logging()
    args = _build_parser().parse_args(sys.argv[1:])

    with command_run_scope() as run_id:
        logger.info("run_id=%s (metrics -> data/qa/command_timing.jsonl)", run_id)
        if args.command == "build-stg-day":
            run_timed_command(
                "build-stg-day",
                lambda: build_stg_day(day=args.day),
            )
        elif args.command == "build-src":
            run_timed_command(
                "build-src",
                lambda: build_src(
                    target_per_operator=args.target_per_operator,
                    excl_pct_of_ab=args.excl_pct_of_ab,
                ),
            )
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
