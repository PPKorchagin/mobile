"""Замер wall-time CLI-команд и запись метрик в JSONL."""

from __future__ import annotations

import json
import logging
import time
import uuid
from collections.abc import Callable
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TypeVar

import pandas as pd

logger = logging.getLogger(__name__)

DEFAULT_METRICS_PATH = Path("data/qa/command_timing.jsonl")
T = TypeVar("T")

_command_run_id: ContextVar[str | None] = ContextVar("command_run_id", default=None)

COMMANDS_WITH_DETAILED_TIMING: frozenset[str] = frozenset(
    {
        "build-stg-oktmo",
        "build-stg-time-zones",
        "build-stg-tac",
        "build-src-bs",
        "build-src-person",
        "build-src-excl",
        "build-src-mobile",
        "build-stg-event",
        "build-move-event",
        "build-stg-msisdn-imsi",
        "build-stg-msisdn-imei",
    }
)


def new_command_run_id() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:8]


def get_command_run_id() -> str | None:
    return _command_run_id.get()


@contextmanager
def command_run_scope(run_id: str | None = None):
    """Привязывает метрики одного CLI-запуска к общему run_id."""
    token = _command_run_id.set(run_id or new_command_run_id())
    try:
        yield _command_run_id.get()
    finally:
        _command_run_id.reset(token)


@contextmanager
def timed_stage(stage_name: str, metrics: dict[str, float]):
    started = time.perf_counter()
    try:
        yield
    finally:
        metrics[stage_name] = round(time.perf_counter() - started, 4)


@contextmanager
def measure_command(
    command: str,
    *,
    output_path: str | Path = DEFAULT_METRICS_PATH,
    log_result: bool = True,
):
    """Замер полного wall-time команды; пишет одну строку в command_timing.jsonl."""
    started = time.perf_counter()
    extra: dict[str, Any] = {}
    error: BaseException | None = None
    try:
        yield extra
    except BaseException as exc:
        error = exc
        raise
    finally:
        elapsed = round(time.perf_counter() - started, 4)
        metrics: dict[str, Any] = {
            "elapsed_total_sec": elapsed,
            "status": "error" if error is not None else "ok",
            **extra,
        }
        append_command_metrics(command=command, metrics=metrics, output_path=output_path)
        if log_result:
            run_id = get_command_run_id()
            logger.info(
                "command=%s elapsed_sec=%s status=%s run_id=%s",
                command,
                elapsed,
                metrics["status"],
                run_id or "-",
            )


def run_timed_command(
    command: str,
    fn: Callable[[], T],
    *,
    force: bool = False,
    output_path: str | Path = DEFAULT_METRICS_PATH,
) -> T:
    """Вызов fn с записью метрик, если команда не пишет их сама."""
    if not force and command in COMMANDS_WITH_DETAILED_TIMING:
        return fn()
    with measure_command(command, output_path=output_path):
        return fn()


def append_command_metrics(
    *,
    command: str,
    metrics: dict[str, Any],
    output_path: str | Path = DEFAULT_METRICS_PATH,
) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    record: dict[str, Any] = {
        "ts_utc": datetime.now(UTC).isoformat(),
        "command": command,
        **metrics,
    }
    run_id = _command_run_id.get()
    if run_id is not None:
        record["run_id"] = run_id
    with path.open("a", encoding="utf-8") as file:
        file.write(json.dumps(record, ensure_ascii=False) + "\n")


def load_command_metrics_df(path: str | Path = DEFAULT_METRICS_PATH) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        return pd.DataFrame()
    rows: list[dict[str, Any]] = []
    with p.open("r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True, errors="coerce")
    return df


def infer_latest_run_id(df: pd.DataFrame) -> str | None:
    if df.empty:
        return None
    if "run_id" in df.columns and df["run_id"].notna().any():
        sub = df[df["run_id"].notna()].copy()
        last_ts = sub.groupby("run_id")["ts_utc"].max()
        return str(last_ts.idxmax())
    return None
