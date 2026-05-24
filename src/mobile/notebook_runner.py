"""Execute notebooks from CLI (``nb-perf-metrics``)."""

from __future__ import annotations

import asyncio
import logging
import subprocess
import sys
from pathlib import Path

import nbformat
from jupyter_client.kernelspec import KernelSpecManager, NoSuchKernel
from nbclient import NotebookClient

from mobile.project_paths import (
    DEFAULT_NOTEBOOK_KERNEL_NAME,
    DEFAULT_NOTEBOOK_RESOURCES_PATH,
)

logger = logging.getLogger(__name__)


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
