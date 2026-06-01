"""WKT geometry metrics for dim DQ pipelines."""

from __future__ import annotations

from typing import Any

import pandas as pd
from shapely import wkt
from shapely.errors import GEOSException

DEFAULT_ALLOWED_GEOM_TYPES = frozenset({"POLYGON", "MULTIPOLYGON"})


def collect_wkt_metrics(
    values: pd.Series,
    *,
    allowed_types: frozenset[str] | set[str] = DEFAULT_ALLOWED_GEOM_TYPES,
    total_count_key: str = "total_geometry_count",
) -> dict[str, Any]:
    parse_error_count = 0
    invalid_topology_count = 0
    unsupported_geom_type_count = 0
    empty_geometry_count = 0
    valid_geometry_count = 0
    geom_type_counts: dict[str, int] = {}
    allowed = {g.upper() for g in allowed_types}

    for value in values:
        if value is None or pd.isna(value) or not str(value).strip():
            parse_error_count += 1
            continue
        try:
            geom = wkt.loads(str(value))
        except (GEOSException, ValueError):
            parse_error_count += 1
            continue

        geom_type = geom.geom_type.upper()
        geom_type_counts[geom_type] = geom_type_counts.get(geom_type, 0) + 1
        if geom_type not in allowed:
            unsupported_geom_type_count += 1
        if geom.is_empty:
            empty_geometry_count += 1
        if not geom.is_valid:
            invalid_topology_count += 1
        if geom_type in allowed and geom.is_valid and not geom.is_empty:
            valid_geometry_count += 1

    return {
        total_count_key: int(len(values)),
        "valid_geometry_count": valid_geometry_count,
        "parse_error_count": parse_error_count,
        "unsupported_geom_type_count": unsupported_geom_type_count,
        "empty_geometry_count": empty_geometry_count,
        "invalid_topology_count": invalid_topology_count,
        "geom_type_counts": geom_type_counts,
    }
