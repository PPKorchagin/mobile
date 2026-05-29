"""Execute notebooks and nb-* CLI reports."""

from __future__ import annotations

import asyncio
import logging
import subprocess
import sys
from pathlib import Path

import nbformat
from jupyter_client.kernelspec import KernelSpecManager, NoSuchKernel
from nbclient import NotebookClient
from nbformat.validator import normalize

from mobile.project_paths import (
    DEFAULT_NB_STG_OKTMO_EXECUTED_PATH,
    DEFAULT_NB_STG_OKTMO_NOTEBOOK_PATH,
    DEFAULT_NOTEBOOK_KERNEL_NAME,
    DEFAULT_NOTEBOOK_RESOURCES_PATH,
    DEFAULT_PERF_METRICS_EXECUTED_PATH,
    DEFAULT_PERF_METRICS_NOTEBOOK_PATH,
    PROJECT_ROOT,
)

logger = logging.getLogger(__name__)

_LEGACY_DQ_STG_OKTMO_HTML = PROJECT_ROOT / "data" / "notebooks" / "dq_stg_oktmo.html"


def run_notebook(
    source_notebook: Path,
    executed_notebook: Path,
    timeout_seconds: int = 900,
) -> None:
    if not source_notebook.exists():
        raise FileNotFoundError(f"Notebook not found: {source_notebook}")

    logger.info("Executing notebook: %s", source_notebook)
    with source_notebook.open("r", encoding="utf-8") as file:
        nb = nbformat.read(file, as_version=4)
    normalize(nb)

    resources_dir = DEFAULT_NOTEBOOK_RESOURCES_PATH
    if resources_dir.exists() and not resources_dir.is_dir():
        raise NotADirectoryError(f"Notebook resources path is not a directory: {resources_dir}")
    resources_dir.mkdir(parents=True, exist_ok=True)

    kernel_name = _ensure_notebook_kernel()
    client = NotebookClient(
        nb=nb,
        timeout=timeout_seconds,
        kernel_name=kernel_name,
        resources={"metadata": {"path": str(resources_dir)}},
    )
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    client.execute()

    executed_notebook.parent.mkdir(parents=True, exist_ok=True)
    with executed_notebook.open("w", encoding="utf-8") as file:
        nbformat.write(nb, file)
    logger.info("Notebook executed: %s", executed_notebook)


def run_nb_perf_metrics() -> None:
    if DEFAULT_PERF_METRICS_EXECUTED_PATH.exists():
        DEFAULT_PERF_METRICS_EXECUTED_PATH.unlink()
    run_notebook(
        source_notebook=DEFAULT_PERF_METRICS_NOTEBOOK_PATH,
        executed_notebook=DEFAULT_PERF_METRICS_EXECUTED_PATH,
        timeout_seconds=300,
    )


def run_nb_stg_oktmo() -> None:
    if _LEGACY_DQ_STG_OKTMO_HTML.exists():
        _LEGACY_DQ_STG_OKTMO_HTML.unlink()
    if DEFAULT_NB_STG_OKTMO_EXECUTED_PATH.exists():
        DEFAULT_NB_STG_OKTMO_EXECUTED_PATH.unlink()
    run_notebook(
        source_notebook=DEFAULT_NB_STG_OKTMO_NOTEBOOK_PATH,
        executed_notebook=DEFAULT_NB_STG_OKTMO_EXECUTED_PATH,
    )


def _ensure_notebook_kernel() -> str:
    manager = KernelSpecManager()
    try:
        manager.get_kernel_spec(DEFAULT_NOTEBOOK_KERNEL_NAME)
        return DEFAULT_NOTEBOOK_KERNEL_NAME
    except NoSuchKernel:
        logger.info("Notebook kernel '%s' not found, installing it", DEFAULT_NOTEBOOK_KERNEL_NAME)
        subprocess.run(
            [
                sys.executable,
                "-m",
                "ipykernel",
                "install",
                "--name",
                DEFAULT_NOTEBOOK_KERNEL_NAME,
                "--display-name",
                "Python (mobile)",
                "--user",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        return DEFAULT_NOTEBOOK_KERNEL_NAME
