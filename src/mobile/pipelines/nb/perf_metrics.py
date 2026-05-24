from __future__ import annotations

from mobile.notebook_runner import run_notebook
from mobile.project_paths import (
    DEFAULT_PERF_METRICS_EXECUTED_PATH,
    DEFAULT_PERF_METRICS_NOTEBOOK_PATH,
)


def run() -> None:
    if DEFAULT_PERF_METRICS_EXECUTED_PATH.exists():
        DEFAULT_PERF_METRICS_EXECUTED_PATH.unlink()
    run_notebook(
        source_notebook=DEFAULT_PERF_METRICS_NOTEBOOK_PATH,
        executed_notebook=DEFAULT_PERF_METRICS_EXECUTED_PATH,
        timeout_seconds=300,
    )
