"""CLI для mobile-пайплайнов."""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Callable

from mobile.cli_defaults import default_bs_params
from mobile.command_timing import command_run_scope, run_timed_command
from mobile.logging_config import setup_logging
from mobile.pipelines.nb import perf_metrics as nb_perf_metrics
from mobile.pipelines.src import bs
from mobile.pipelines.stg import oktmo, tac, time_zones
from mobile.project_paths import (
    DEFAULT_SRC_BS_CONFIG_PATH,
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

BUILD_STEPS: tuple[str, ...] = tuple(_BUILD_COMMANDS)
RUN_ALL_STEPS: tuple[str, ...] = BUILD_STEPS + ("nb-perf-metrics",)


def _run_build(command: str) -> None:
    fn, config_path = _BUILD_COMMANDS[command]
    logger.info("Starting %s (config=%s)", command, config_path)
    fn()
    logger.info("%s completed successfully", command)


def _run_command(command: str) -> None:
    if command in _BUILD_COMMANDS:
        _run_build(command)
        return
    if command in _NB_COMMANDS:
        logger.info("Starting %s", command)
        _NB_COMMANDS[command]()
        logger.info("%s completed successfully", command)
        return
    raise ValueError(f"Unknown command: {command}")


def run_all() -> None:
    logger.info("Starting run-all: %s", ", ".join(RUN_ALL_STEPS))
    for command in RUN_ALL_STEPS:
        run_timed_command(command, lambda cmd=command: _run_command(cmd))
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
    return parser


def main() -> None:
    setup_logging()
    args = _build_parser().parse_args(sys.argv[1:])

    with command_run_scope() as run_id:
        logger.info("run_id=%s (metrics -> data/qa/command_timing.jsonl)", run_id)
        if args.command == "run-all":
            run_timed_command("run-all", run_all)
        else:
            run_timed_command(args.command, lambda: _run_command(args.command))


if __name__ == "__main__":
    main()
