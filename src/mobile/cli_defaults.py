"""Default CLI constants and parameter objects for mobile pipelines."""

from __future__ import annotations

import hashlib
import os
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
DEFAULT_PARQUET_COMPRESSION = "snappy"

DEFAULT_SRC_START_DATE = date(2024, 12, 25)
DEFAULT_SRC_END_DATE = date(2025, 2, 5)

DEFAULT_SRC_PERSON_TARGET_PER_OPERATOR = 50_000
DEFAULT_SRC_PERSON_EXTRA_FULL_SNAPSHOT_RANDOM_DAYS = 7
DEFAULT_SRC_EXCL_PCT_OF_AB = 0.7
DEFAULT_SRC_MOBILE_MOVEMENT_RATIO = 0.22


def default_max_workers(*, reserve_cores: int = 2, cap: int = 8) -> int:
    return max(1, min(cap, (os.cpu_count() or 2) - reserve_cores))


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


def default_person_params(target_per_operator: int | None = None):
    from mobile.pipelines.src.person import BuildSrcPersonParams

    return BuildSrcPersonParams(
        start_date=DEFAULT_SRC_START_DATE,
        end_date=DEFAULT_SRC_END_DATE,
        operators=["билайн", "мегафон", "мтс", "теле2"],
        target_active_subscribers_per_operator=(
            int(target_per_operator)
            if target_per_operator is not None
            else DEFAULT_SRC_PERSON_TARGET_PER_OPERATOR
        ),
        daily_active_ratio_min=0.55,
        daily_active_ratio_max=0.95,
        closed_contract_ratio=0.18,
        inactive_ratio=0.12,
        corporate_ratio=0.14,
        inter_operator_transition_ratio=0.10,
        movement_ratio=0.22,
        mnp_portability_ratio=0.02,
        multi_sim_per_contract_ratio=0.015,
        foreign_subscriber_ratio=0.10,
        extra_random_full_snapshot_days=DEFAULT_SRC_PERSON_EXTRA_FULL_SNAPSHOT_RANDOM_DAYS,
        seed=DEFAULT_BS_SEED,
        max_workers=default_max_workers(),
    )


def default_excl_params(*, pct_of_ab: float | None = None):
    from mobile.pipelines.src.excl import BuildSrcExclParams

    return BuildSrcExclParams(
        pct_of_ab=float(pct_of_ab if pct_of_ab is not None else DEFAULT_SRC_EXCL_PCT_OF_AB),
        seed=DEFAULT_BS_SEED,
    )


def default_mobile_params():
    from mobile.pipelines.src.mobile import BuildSrcMobileParams

    return BuildSrcMobileParams(
        start_date=DEFAULT_SRC_START_DATE,
        end_date=DEFAULT_SRC_END_DATE,
        operators=["билайн", "мегафон", "мтс", "теле2"],
        seed=DEFAULT_BS_SEED,
        max_workers=len(OPERATORS),
        movement_ratio=DEFAULT_SRC_MOBILE_MOVEMENT_RATIO,
        region_subjects=(),
    )


def stable_seed(*parts: object) -> int:
    data = "|".join(str(p) for p in parts).encode("utf-8")
    return int(hashlib.sha256(data).hexdigest()[:16], 16) % (2**32)
