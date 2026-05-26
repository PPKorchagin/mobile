# build-stg-day

**Срез:** STG за календарный день · **Команда:** `build-stg-day` · **Режим:** build + DQ трёх справочников в каталог `load_day={YYYY-MM-DD}`.

Референс: [`pipelines/stg/day.py`](../../../src/mobile/pipelines/stg/day.py), [`project_paths.py`](../../../src/mobile/project_paths.py) → `stg_load_day_paths`.

---

## Задачи pipeline

| # | Задача | Результат |
|---|--------|-----------|
| 1 | Собрать `BuildStgDayParams` (дата + пути CSV/parquet) | Параметры цепочки |
| 2 | Выполнить 6 шагов по порядку | Parquet в `data/stg/load_day=…/` + логи DQ |
| 3 | Записать метрики каждого шага | `command_timing.jsonl` |

**Шаги (строго по порядку):**

1. `build-stg-oktmo`
2. `dq-stg-oktmo`
3. `build-stg-time-zones`
4. `dq-stg-time-zones`
5. `build-stg-tac`
6. `dq-stg-tac`

**В scope:** те же ETL/DQ, что у одиночных команд; отличие — каталог выхода привязан к `day`.

---

## TODO

1. При необходимости — отдельные raw CSV на дату (сейчас общие файлы из `raw_data`).

---

## Параметры запуска

Вызов: `build_stg_day(day)` / `default_build_stg_day_params(day)` ([`cli.py`](../../../src/mobile/cli.py)).

| Переменная | Тип | Обязательность | Значение по умолчанию | Описание |
|------------|-----|----------------|----------------------|----------|
| `day` | date | Да | `2025-01-01` | Календарный срез (`DEFAULT_STG_DAY`, CLI `--day`) |
| `oktmo_csv_path` | path | Да | `src/mobile/raw_data/oktmo_v001.csv` | Вход ОКТМО |
| `oktmo_output_path` | path | Да | `data/stg/load_day={day}/oktmo.parquet` | Выход + DQ |
| `time_zones_csv_path` | path | Да | `src/mobile/raw_data/time_zones.csv` | Вход TZ |
| `time_zones_output_path` | path | Да | `data/stg/load_day={day}/time_zones.parquet` | Выход + DQ |
| `tac_csv_path` | path | Да | `src/mobile/raw_data/tacdb_v001.csv` | Вход TAC |
| `tac_output_path` | path | Да | `data/stg/load_day={day}/tac.parquet` | Выход + DQ |
| `compression` | string | Да | `snappy` | Parquet |

| Переменная CLI | Тип | По умолчанию | Описание |
|----------------|-----|--------------|----------|
| `--day` | `YYYY-MM-DD` | `2025-01-01` | Только для `build-stg-day` |

Локальный запуск:

```bash
uv run mobile build-stg-day
uv run mobile build-stg-day --day 2025-01-15
```

Пути **относительные к корню** `mobile` (в коде: `PROJECT_ROOT`).

---

## Структура выходных данных

| Файл | Витрина |
|------|---------|
| `data/stg/load_day={YYYY-MM-DD}/oktmo.parquet` | `stg_oktmo` |
| `data/stg/load_day={YYYY-MM-DD}/time_zones.parquet` | `stg_time_zones` |
| `data/stg/load_day={YYYY-MM-DD}/tac.parquet` | `stg_tac` |

Корневые `data/stg/oktmo.parquet` (без `load_day`) **не обновляются** этой командой.

---

## Источники

| # | Источник | Путь |
|---|----------|------|
| 1 | ОКТМО CSV | `oktmo_csv_path` |
| 2 | Time zones CSV | `time_zones_csv_path` |
| 3 | TAC CSV | `tac_csv_path` |

---

## Алгоритм обработки данных

### Шаг 0. Инициализация

`params = default_build_stg_day_params(day)` → `stg_load_day_paths(day)` формирует каталог `data/stg/load_day={iso}/` и шесть путей.

### Шаги 1–6

Для каждого имени из `BUILD_STG_DAY_STEPS` — `run_timed_command(step, …)`:

- build: `oktmo.run` / `time_zones.run` / `tac.run` с `csv_path`, `output_path`, `compression` из `params`;
- dq: `run_dq(params.*_output_path)` сразу после соответствующего build.

Альтернатива в коде без пошагового timing: `stg_day.run(params)` одним вызовом.

### Проверки DQ в цепочке

После каждого build вызывается тот же `run_dq`, что у одиночных команд; путь parquet — из `load_day={day}/`.

| Шаг | DQ-команда | Документация checks |
|-----|------------|---------------------|
| 2 | `dq-stg-oktmo` | [`dq_stg_oktmo.md`](../dq/stg/dq_stg_oktmo.md#проверки) — схема, иерархия ОКТМО, WKT |
| 4 | `dq-stg-time-zones` | [`dq_stg_time_zones.md`](../dq/stg/dq_stg_time_zones.md#проверки) — timezone, распределение TZ, geometry |
| 6 | `dq-stg-tac` | [`dq_stg_tac.md`](../dq/stg/dq_stg_tac.md#проверки) — TAC, M2M, даты |

Обзор всех DQ-команд: [`documents/dq/README.md`](../dq/README.md).

### Типовые ошибки

| Ошибка | Причина |
|--------|---------|
| `FileNotFoundError` | Нет входного CSV |
| DQ `dataset_presence` failed | Build не создал parquet (процесс не падает) |
| Неверный `--day` | Не ISO-дата |

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| Оркестратор | [`pipelines/stg/day.py`](../../../src/mobile/pipelines/stg/day.py) |
| Пути | [`project_paths.py`](../../../src/mobile/project_paths.py) |
| Дефолты | [`cli_defaults.py`](../../../src/mobile/cli_defaults.py) |
