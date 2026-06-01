"""Structured JSON logging for DQ pipeline checks."""

from __future__ import annotations

import json
import logging
from typing import Any


def emit_dq_log(
    tag: str,
    check: str,
    status: str,
    metrics: dict[str, Any],
    *,
    logger: logging.Logger | None = None,
    **extra: Any,
) -> None:
    log = logger or logging.getLogger(__name__)
    payload: dict[str, Any] = {"tag": tag, "check": check, "status": status, "metrics": metrics}
    if extra:
        payload.update(extra)
    message = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    if status == "failed":
        log.error(message)
    elif status == "warning":
        log.warning(message)
    else:
        log.info(message)


def emit_dq_summary(
    tag: str,
    *,
    total_checks: int,
    warnings: int = 0,
    failed: int = 0,
    logger: logging.Logger | None = None,
    derive_status: bool = True,
    clean_status: str = "ok",
) -> None:
    """Log DQ run summary. ``derive_status=False`` keeps ``status`` in payload as ``clean_status`` (legacy dim DQ)."""
    log = logger or logging.getLogger(__name__)
    metrics = {
        "total_checks": int(total_checks),
        "warning_checks": int(warnings),
        "failed_checks": int(failed),
    }
    if derive_status:
        status = "failed" if failed else ("warning" if warnings else clean_status)
        emit_dq_log(tag, "summary", status, metrics, logger=log)
        return
    payload = {
        "tag": tag,
        "check": "summary",
        "status": clean_status,
        "metrics": metrics,
    }
    log.info(json.dumps(payload, ensure_ascii=False, sort_keys=True))


class DqCheckEmitter:
    """Counts checks and emits structured DQ log lines."""

    def __init__(self, tag: str, *, logger: logging.Logger | None = None) -> None:
        self.tag = tag
        self.logger = logger or logging.getLogger(__name__)
        self.total_checks = 0
        self.warnings = 0
        self.failed = 0

    def emit(self, check: str, status: str, metrics: dict[str, Any], **extra: Any) -> None:
        self.total_checks += 1
        if status == "warning":
            self.warnings += 1
        elif status == "failed":
            self.failed += 1
        emit_dq_log(self.tag, check, status, metrics, logger=self.logger, **extra)

    def emit_summary(self, *, derive_status: bool = True, clean_status: str = "ok") -> None:
        emit_dq_summary(
            self.tag,
            total_checks=self.total_checks,
            warnings=self.warnings,
            failed=self.failed,
            logger=self.logger,
            derive_status=derive_status,
            clean_status=clean_status,
        )
