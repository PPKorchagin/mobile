"""CLI для mobile-пайплайнов."""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Callable

from mobile.cli_defaults import default_bs_params, default_person_params
from mobile.command_timing import command_run_scope, run_timed_command
from mobile.logging_config import setup_logging
from mobile.pipelines.nb import perf_metrics as nb_perf_metrics
from mobile.pipelines.src import bs, person
from mobile.pipelines.stg import oktmo, tac, time_zones
from mobile.project_paths import (
    DEFAULT_SRC_BS_CONFIG_PATH,
    DEFAULT_SRC_PERSON_CONFIG_PATH,
    DEFAULT_STG_OKTMO_CONFIG_PATH,
    DEFAULT_STG_TAC_CONFIG_PATH,
    DEFAULT_STG_TIME_ZONES_CONFIG_PATH,
    resolve_oktmo_layout,
)

logger = logging.getLogger(__name__)

_BUILD_COMMANDS: dict[str, tuple[Callable[[], None], str]] = {
    "build-stg-oktmo": (
        lambda: oktmo.run_from_config(DEFAULT_STG_OKTMO_CONFIG_PATH),
        str(DEFAULT_STG_OKTMO_CONFIG_PATH),
    ),
    "build-stg-time-zones": (
        lambda: time_zones.run_from_config(DEFAULT_STG_TIME_ZONES_CONFIG_PATH),
        str(DEFAULT_STG_TIME_ZONES_CONFIG_PATH),
    ),
    "build-stg-tac": (
        lambda: tac.run_from_config(DEFAULT_STG_TAC_CONFIG_PATH),
        str(DEFAULT_STG_TAC_CONFIG_PATH),
    ),
    "build-src-bs": (
        lambda: bs.run_from_config(
            DEFAULT_SRC_BS_CONFIG_PATH,
            resolve_oktmo_layout(),
            default_bs_params(),
        ),
        str(DEFAULT_SRC_BS_CONFIG_PATH),
    ),
}

_NB_COMMANDS: dict[str, Callable[[], None]] = {
    "nb-perf-metrics": nb_perf_metrics.run,
}


def _run_build(command: str) -> None:
    fn, config_path = _BUILD_COMMANDS[command]
    logger.info("Starting %s (config=%s)", command, config_path)
    fn()
    logger.info("%s completed successfully", command)


def _run_command(command: str, *, target_per_operator: int | None = None) -> None:
    if command == "build-src-person":
        logger.info("Starting %s (config=%s)", command, DEFAULT_SRC_PERSON_CONFIG_PATH)
        person.run_from_config(
            DEFAULT_SRC_PERSON_CONFIG_PATH,
            default_person_params(target_per_operator),
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


BUILD_STEPS: tuple[str, ...] = (
    *tuple(_BUILD_COMMANDS),
    "build-src-person",
)
RUN_ALL_STEPS: tuple[str, ...] = BUILD_STEPS + ("nb-perf-metrics",)


def run_all(*, target_per_operator: int | None = None) -> None:
    logger.info("Starting run-all: %s", ", ".join(RUN_ALL_STEPS))
    for command in RUN_ALL_STEPS:
        run_timed_command(
            command,
            lambda cmd=command: _run_command(cmd, target_per_operator=target_per_operator),
        )
    logger.info("run-all completed successfully")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mobile",
        description="Mobile OSS пайплайны.",
    )
    parser.add_argument(
        "command",
        choices=sorted({*RUN_ALL_STEPS, "run-all"}),
        help="Шаг пайплайна или обёртка run-all",
    )
    parser.add_argument(
        "--target-per-operator",
        type=int,
        default=None,
        metavar="N",
        help="build-src-person / run-all: абонентов на оператора в полный день (по умолчанию 50000)",
    )
    return parser


def main() -> None:
    setup_logging()
    args = _build_parser().parse_args(sys.argv[1:])

    with command_run_scope() as run_id:
        logger.info("run_id=%s (metrics -> data/qa/command_timing.jsonl)", run_id)
        if args.command == "run-all":
            run_timed_command(
                "run-all",
                lambda: run_all(target_per_operator=args.target_per_operator),
            )
        else:
            run_timed_command(
                args.command,
                lambda: _run_command(args.command, target_per_operator=args.target_per_operator),
            )


if __name__ == "__main__":
    main()
