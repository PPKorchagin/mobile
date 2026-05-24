"""Default CLI constants and parameter objects for mobile pipelines."""

from __future__ import annotations

import hashlib
from datetime import date
from functools import lru_cache

import pandas as pd

from mobile.project_paths import DEFAULT_BS_PROFILE_PATH

OPEN_BS_DATE_OFF = pd.Timestamp("2999-12-31 23:59:59")

OPERATORS: dict[str, int] = {
    "билайн": 99,
    "мегафон": 2,
    "мтс": 1,
    "теле2": 20,
}

DEFAULT_REGION_SUBJECTS: tuple[str, ...] = (
    "Тюменская область",
    "Красноярский край",
    "Республика Саха (Якутия)",
)

DEFAULT_BS_SEED = 20250407

DEFAULT_SRC_START_DATE = date(2024, 12, 25)
DEFAULT_SRC_END_DATE = date(2025, 2, 5)


@lru_cache(maxsize=1)
def default_bs_params():
    from mobile.pipelines.src.bs import BuildBsParams

    return BuildBsParams(
        start_date=DEFAULT_SRC_START_DATE,
        end_date=DEFAULT_SRC_END_DATE,
        subjects=list(DEFAULT_REGION_SUBJECTS),
        operators=["билайн", "мегафон", "мтс", "теле2"],
        seed=DEFAULT_BS_SEED,
        profile_path=DEFAULT_BS_PROFILE_PATH,
    )


def stable_seed(*parts: object) -> int:
    data = "|".join(str(p) for p in parts).encode("utf-8")
    return int(hashlib.sha256(data).hexdigest()[:16], 16) % (2**32)
