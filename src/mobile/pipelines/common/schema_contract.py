"""Load table name and field list from JSON schema files."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def load_table_fields(schema_path: Path) -> tuple[str, list[dict[str, str]]]:
    with schema_path.open(encoding="utf-8") as file:
        cfg: dict[str, Any] = json.load(file)
    table = str(cfg.get("table", ""))
    fields = [
        {"name": str(f["name"]), "type": str(f["type"])} for f in cfg.get("fields", [])
    ]
    return table, fields


def apply_table_fields_to_module(
    schema_path: Path,
    *,
    table_name: str,
    fields_name: str,
    module_globals: dict[str, Any],
    default_table: str,
    default_fields: list[dict[str, str]],
) -> None:
    """Update module-level ``table`` / ``fields`` globals from a schema JSON file."""
    table, fields = load_table_fields(schema_path)
    module_globals[table_name] = table or default_table
    module_globals[fields_name] = fields or default_fields
