# mobile

CLI и пайплайны для mobile OSS-витрин. Схемы — `src/mobile/schema/`, документация — `documents/`.

## Запуск

```bash
uv sync
uv run mobile run-all
```

`run-all` выполняет build STG/SRC-справочников и в конце — `nb-perf-metrics`.

Или по отдельности:

```bash
uv run mobile build-stg-oktmo
uv run mobile build-stg-time-zones
uv run mobile build-stg-tac
uv run mobile build-src-bs
uv run mobile build-src-person
uv run mobile nb-perf-metrics
```

Логи: `data/logs/mobile.log` (console + rotating file).  
Метрики времени: `data/qa/command_timing.jsonl`.  
Дашборд метрик: `data/notebooks/perf_metrics.executed.ipynb`.

## CLI

| Команда | Описание |
|---------|----------|
| `run-all` | STG → `build-src-bs` → `build-src-person` → `nb-perf-metrics` |
| `build-stg-oktmo` | CSV → `data/stg/oktmo.parquet` |
| `build-stg-time-zones` | CSV → `data/stg/time_zones.parquet` |
| `build-stg-tac` | CSV → `data/stg/tac.parquet` |
| `build-src-bs` | ОКТМО + профиль → `data/src/bs.parquet` |
| `build-src-person` | Суточные срезы → `data/src/person/...` |
| `nb-perf-metrics` | Notebook-дашборд по `command_timing.jsonl` |

Флаг **`--target-per-operator N`** — для `build-src-person` и `run-all` (по умолчанию `50000`).

| Команда | Конфиг / источник | Вход | Выход |
|---------|-------------------|------|-------|
| `build-stg-oktmo` | `src/mobile/schema/stg/oktmo.json` | `src/mobile/raw_data/oktmo_v001.csv` | `data/stg/oktmo.parquet` |
| `build-stg-time-zones` | `src/mobile/schema/stg/time_zones.json` | `src/mobile/raw_data/time_zones.csv` | `data/stg/time_zones.parquet` |
| `build-stg-tac` | `src/mobile/schema/stg/tac.json` | `src/mobile/raw_data/tacdb_v001.csv` | `data/stg/tac.parquet` |
| `build-src-bs` | `src/mobile/schema/src/bs.json` | `data/stg/oktmo.parquet`, профиль OpenCellID | `data/src/bs.parquet` |
| `build-src-person` | `src/mobile/schema/src/person.json` | — | `data/src/person/load_year=…/person.parquet`, `_SUCCESS` |
| `nb-perf-metrics` | `src/mobile/nb/perf_metrics.ipynb` | `data/qa/command_timing.jsonl` | `data/notebooks/perf_metrics.executed.ipynb` |

Документация:

- [`documents/stg/build_stg_oktmo.md`](documents/stg/build_stg_oktmo.md)
- [`documents/stg/build_stg_time_zones.md`](documents/stg/build_stg_time_zones.md)
- [`documents/stg/build_stg_tac.md`](documents/stg/build_stg_tac.md)
- [`documents/src/build_src_bs.md`](documents/src/build_src_bs.md)
- [`documents/src/build_src_person.md`](documents/src/build_src_person.md)

## Пайплайны (код)

- `src/mobile/pipelines/stg/oktmo.py` — `run_from_config()`
- `src/mobile/pipelines/stg/time_zones.py` — `run_from_config()`
- `src/mobile/pipelines/stg/tac.py` — `run_from_config()`
- `src/mobile/pipelines/src/bs.py` — `run_from_config()`
- `src/mobile/pipelines/src/person.py` — `run_from_config()`
- `src/mobile/pipelines/nb/perf_metrics.py` — `run()`
