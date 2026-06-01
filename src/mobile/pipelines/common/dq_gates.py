"""Rate-based DQ gate helpers."""

from __future__ import annotations

import pandas as pd


def valid_rate(mask: pd.Series) -> float:
    if len(mask) == 0:
        return 1.0
    return float(mask.mean())


def gate_status_from_rate(rate: float, *, failed_below: float, warn_below: float) -> str:
    if rate < failed_below:
        return "failed"
    if rate < warn_below:
        return "warning"
    return "ok"
