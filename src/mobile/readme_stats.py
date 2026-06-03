"""Сбор метрик репозитория и обновление раздела «Статистика» в README."""

from __future__ import annotations

import ast
import json
import re
from collections import Counter
from pathlib import Path

from mobile.cli_defaults import DEFAULT_SRC_END_DATE, DEFAULT_SRC_START_DATE, OPERATORS
from mobile.project_paths import PROJECT_ROOT, mobile_datacenter_ids

_STATS_BEGIN = "<!-- readme-stats:begin -->"
_STATS_END = "<!-- readme-stats:end -->"


def _format_count(value: int) -> str:
    """Человекочитаемое число; для крупных — округление до сотен с префиксом ~."""
    if value < 1000:
        return str(value)
    rounded = round(value / 100) * 100
    s = str(rounded)
    chunks: list[str] = []
    while len(s) > 3:
        chunks.insert(0, s[-3:])
        s = s[:-3]
    if s:
        chunks.insert(0, s)
    body = " ".join(chunks)
    return f"~{body}" if rounded != value else body


def _py_files(base: Path) -> list[Path]:
    return sorted(
        p
        for p in base.rglob("*.py")
        if "__pycache__" not in p.parts and ".venv" not in p.parts
    )


def _count_lines(paths: list[Path]) -> tuple[int, int]:
    total = non_empty = 0
    for path in paths:
        lines = path.read_text(encoding="utf-8-sig", errors="replace").splitlines()
        total += len(lines)
        non_empty += sum(1 for line in lines if line.strip())
    return total, non_empty


def _ast_counts(paths: list[Path]) -> tuple[int, int]:
    funcs = classes = 0
    for path in paths:
        tree = ast.parse(path.read_text(encoding="utf-8-sig", errors="replace"))
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                funcs += 1
            elif isinstance(node, ast.ClassDef):
                classes += 1
    return funcs, classes


def _layer_stats(rel: str, *, exclude_init: bool = True) -> tuple[int, int]:
    root = PROJECT_ROOT / "src" / "mobile" / rel
    if not root.exists():
        return 0, 0
    files = _py_files(root)
    if exclude_init:
        files = [p for p in files if p.name != "__init__.py"]
    return len(files), _count_lines(files)[0] if files else 0


def _mobile_py_lines() -> int:
    path = PROJECT_ROOT / "src" / "mobile" / "pipelines" / "src" / "mobile.py"
    return len(path.read_text(encoding="utf-8-sig", errors="replace").splitlines()) if path.exists() else 0


def _infra_lines() -> int:
    names = (
        "cli.py",
        "project_paths.py",
        "command_timing.py",
        "cli_defaults.py",
        "logging_config.py",
        "notebook_runner.py",
        "readme_stats.py",
        "__init__.py",
        "__main__.py",
    )
    root = PROJECT_ROOT / "src" / "mobile"
    return sum(
        len((root / name).read_text(encoding="utf-8-sig", errors="replace").splitlines())
        for name in names
        if (root / name).exists()
    )


def _doc_breakdown() -> dict[str, int]:
    counts: Counter[str] = Counter()
    for path in sorted((PROJECT_ROOT / "documents").rglob("*.md")):
        parts = path.relative_to(PROJECT_ROOT / "documents").parts
        if len(parts) >= 2 and parts[0] == "dq":
            counts[f"dq/{parts[1]}"] += 1
        elif parts:
            counts[parts[0]] += 1
    return dict(sorted(counts.items()))


def _doc_build_dq_src_line(breakdown: dict[str, int]) -> tuple[str, str, str]:
    return (
        f"{breakdown.get('dim', 0)} / {breakdown.get('dds', 0)} / {breakdown.get('fct', 0)} / {breakdown.get('stg', 0)}",
        f"{breakdown.get('dq/dim', 0)} / {breakdown.get('dq/dds', 0)} / {breakdown.get('dq/fct', 0)} / {breakdown.get('dq/stg', 0)}",
        f"{breakdown.get('src', 0)} / {breakdown.get('dq/src', 0)}",
    )


def _wrapper_command_names() -> tuple[str, ...]:
    from mobile.cli import CLI_COMMANDS, RUN_ALL_COMMANDS

    return tuple(c for c in CLI_COMMANDS if c not in RUN_ALL_COMMANDS)


def collect_readme_stats(*, data_root: Path | None = None) -> dict[str, object]:
    """Собрать метрики для раздела README (без записи файла)."""
    from mobile.cli import (
        CLI_COMMANDS,
        RUN_ALL_COMMANDS,
        RUN_SRC_COMMANDS,
        _distinct_report_months_in_src_window,
        _run_all_argv_steps,
    )

    data = data_root or (PROJECT_ROOT / "data")
    mobile_root = PROJECT_ROOT / "src" / "mobile"
    all_py = _py_files(mobile_root)
    py_total, py_non_empty = _count_lines(all_py)
    funcs, classes = _ast_counts(all_py)

    dim_n, dim_lines = _layer_stats("pipelines/dim")
    dds_n, dds_lines = _layer_stats("pipelines/dds")
    fct_n, fct_lines = _layer_stats("pipelines/fct")
    stg_n, stg_lines = _layer_stats("pipelines/stg")
    etl_lines = dim_lines + dds_lines + fct_lines + stg_lines

    common_n, common_lines = _layer_stats("pipelines/common")
    src_n, src_lines = _layer_stats("pipelines/src")
    dq_total_n, dq_total_lines = 0, 0
    for layer in ("dim", "dds", "fct", "stg", "src"):
        n, lines = _layer_stats(f"pipelines/dq/{layer}")
        dq_total_n += n
        dq_total_lines += lines

    nb_common = PROJECT_ROOT / "src" / "mobile" / "pipelines" / "nb" / "common.py"
    nb_common_lines = (
        len(nb_common.read_text(encoding="utf-8-sig", errors="replace").splitlines())
        if nb_common.exists()
        else 0
    )

    docs = sorted((PROJECT_ROOT / "documents").rglob("*.md"))
    doc_total, doc_non_empty = _count_lines(docs)
    doc_breakdown = _doc_breakdown()
    build_layers, dq_layers, src_layers = _doc_build_dq_src_line(doc_breakdown)

    nb_ipynb = sorted((PROJECT_ROOT / "src/mobile/pipelines/nb").glob("*.ipynb"))
    nb_lines = sum(
        len(p.read_text(encoding="utf-8", errors="replace").splitlines()) for p in nb_ipynb
    )

    schemas = list((PROJECT_ROOT / "src/mobile/schema").rglob("*.json"))
    parquet_files = list(data.rglob("*.parquet")) if data.exists() else []
    executed_nb = list((data / "notebooks").glob("*.ipynb")) if (data / "notebooks").exists() else []

    wrappers = _wrapper_command_names()
    run_all_subprocess = len(_run_all_argv_steps())
    person_months = len(_distinct_report_months_in_src_window())

    return {
        "py_files": len(all_py),
        "py_lines": py_total,
        "py_non_empty": py_non_empty,
        "funcs": funcs,
        "classes": classes,
        "dim_n": dim_n,
        "dds_n": dds_n,
        "fct_n": fct_n,
        "stg_n": stg_n,
        "etl_lines": etl_lines,
        "common_n": common_n,
        "common_lines": common_lines,
        "src_n": src_n,
        "src_lines": src_lines,
        "mobile_py_lines": _mobile_py_lines(),
        "dq_n": dq_total_n,
        "dq_lines": dq_total_lines,
        "nb_common_lines": nb_common_lines,
        "infra_lines": _infra_lines(),
        "schema_n": len(schemas),
        "doc_md_n": len(docs),
        "doc_lines": doc_total,
        "doc_non_empty": doc_non_empty,
        "doc_build_layers": build_layers,
        "doc_dq_layers": dq_layers,
        "doc_src_layers": src_layers,
        "nb_ipynb_n": len(nb_ipynb),
        "nb_ipynb_lines": nb_lines,
        "cli_n": len(CLI_COMMANDS),
        "table_n": len(RUN_ALL_COMMANDS),
        "wrappers": wrappers,
        "run_all_steps": len(RUN_ALL_COMMANDS),
        "run_all_subprocess": run_all_subprocess,
        "run_src_steps": len(RUN_SRC_COMMANDS),
        "person_months": person_months,
        "src_start": DEFAULT_SRC_START_DATE.isoformat(),
        "src_end": DEFAULT_SRC_END_DATE.isoformat(),
        "operators_n": len(OPERATORS),
        "datacenters": list(mobile_datacenter_ids()),
        "parquet_n": len(parquet_files),
        "executed_nb_n": len(executed_nb),
    }


def render_readme_stats_section(stats: dict[str, object] | None = None) -> str:
    """Сгенерировать markdown раздела «Статистика» (без заголовка ##)."""
    s = stats or collect_readme_stats()
    wrappers = s["wrappers"]
    wrapper_list = " + ".join(f"`{name}`" for name in wrappers)

    lines = [
        _STATS_BEGIN,
        "",
        "Оценка по дереву `src/mobile/`, `documents/` и `README.md` (без `data/`, `__pycache__`, `.git`). "
        "Числа округлены; после локальных прогонов меняются артефакты в `data/`. "
        f"Обновить: `uv run mobile update-readme-stats`.",
        "",
        "### Код",
        "",
        "| Метрика | Значение |",
        "| -------- | -------- |",
        f"| Python-модули (`src/mobile`) | **{s['py_files']}** файлов |",
        f"| Строки Python (всего) | **{_format_count(s['py_lines'])}** ({_format_count(s['py_non_empty'])} непустых) |",
        f"| Функции / классы (AST) | **{s['funcs']}** / **{s['classes']}** |",
        f"| ETL `pipelines/{{dim,dds,fct,stg}}` | dim {s['dim_n']} · dds {s['dds_n']} · fct {s['fct_n']} · stg {s['stg_n']}, "
        f"{_format_count(s['etl_lines'])} строк |",
        f"| Общее `pipelines/common` | {s['common_n']} модулей, {_format_count(s['common_lines'])} строк "
        f"(DQ-логи, gates, WKT, CSV, схемы, binding-интервалы) |",
        f"| Синтез `pipelines/src` | {s['src_n']} модуля, {_format_count(s['src_lines'])} строк "
        f"(крупнейший — `mobile.py`, {_format_count(s['mobile_py_lines'])} строк) |",
        f"| DQ `pipelines/dq/{{dim,dds,fct,stg,src}}` | {s['dq_n']} модулей, {_format_count(s['dq_lines'])} строк |",
        f"| Ноутбуки `pipelines/nb/common.py` | {_format_count(s['nb_common_lines'])} строк (DQ-дашборды, folium) |",
        f"| CLI, пути, timing | `cli.py`, `project_paths.py`, … — {_format_count(s['infra_lines'])} строк |",
        f"| JSON-схемы витрин | **{s['schema_n']}** файлов в `schema/` |",
        "",
        "### Документация",
        "",
        "| Метрика | Значение |",
        "| -------- | -------- |",
        f"| Markdown-спеки | **{s['doc_md_n']}** файл в `documents/` + **README** |",
        f"| Строки документации | **{_format_count(s['doc_lines'])}** ({_format_count(s['doc_non_empty'])} непустых) |",
        f"| `documents/{{dim,dds,fct,stg}}` — build | {s['doc_build_layers']} |",
        f"| `documents/dq/{{dim,dds,fct,stg}}` — dq | {s['doc_dq_layers']} |",
        f"| `documents/src` — build / dq | {s['doc_src_layers']} |",
        f"| Исходные DQ-ноутбуки | **{s['nb_ipynb_n']}** `.ipynb` в `pipelines/nb/` ({_format_count(s['nb_ipynb_lines'])} строк JSON) |",
        "",
        "### CLI и пайплайны",
        "",
        "| Метрика | Значение |",
        "| -------- | -------- |",
        f"| Зарегистрированных команд | **{s['cli_n']}** (`{s['table_n']}` по таблице + {wrapper_list}) |",
        f"| Шагов в `run-all` | **{s['run_all_steps']}** (+ до {s['person_months']}× `build-fct-person` по месяцам → до **{s['run_all_subprocess']}** subprocess) |",
        f"| Шагов в `run-src` | **{s['run_src_steps']}** (только build: ОКТМО + 4 src-витрины) |",
        f"| Календарное окно синтеза | `DEFAULT_SRC_*`: **{s['src_start']} … {s['src_end']}** |",
        f"| Операторы / ЦОД в синтезе | **{s['operators_n']}** MNO, **{len(s['datacenters'])}** ЦОД "
        f"(`{'`, `'.join(s['datacenters'])}`) |",
        "",
        "### Локальные артефакты (после прогонов)",
        "",
        "| Метрика | Значение |",
        "| -------- | -------- |",
        f"| Parquet в `data/` | **{s['parquet_n']:,}** файлов (зависит от полноты `run-all` / `run-src`) |".replace(",", " "),
        f"| Executed notebooks | **{s['executed_nb_n']}+** в `data/notebooks/` |",
        "| Метрики времени | `data/qa/command_timing.jsonl` |",
        "| Логи | `data/logs/mobile.log` |",
        "",
        _STATS_END,
        "",
    ]
    return "\n".join(lines)


def update_readme_stats(*, readme_path: Path | None = None) -> Path:
    """Перезаписать раздел «Статистика» в README.md; вернуть путь к файлу."""
    path = readme_path or (PROJECT_ROOT / "README.md")
    text = path.read_text(encoding="utf-8")
    section_body = render_readme_stats_section()

    if _STATS_BEGIN in text and _STATS_END in text:
        pattern = re.compile(
            rf"{re.escape(_STATS_BEGIN)}.*?{re.escape(_STATS_END)}",
            re.DOTALL,
        )
        new_text = pattern.sub(section_body.strip(), text, count=1)
    else:
        marker = "\n## Статистика\n"
        idx = text.find(marker)
        if idx < 0:
            raise ValueError(f"README missing section header: {marker.strip()}")
        new_text = text[: idx + len(marker)] + "\n\n" + section_body + "\n"

    if new_text != text:
        path.write_text(new_text, encoding="utf-8", newline="\n")
    return path
