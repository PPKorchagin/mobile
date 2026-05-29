# build-stg-day

**Срез:** STG за календарный день · **Команда:** `build-stg-day` · **Режим:** build + DQ четырёх справочников в каталог `load_day={YYYY-MM-DD}`.

Референс: [`pipelines/stg/day.py`](../../../src/mobile/pipelines/stg/day.py), [`project_paths.py`](../../../src/mobile/project_paths.py) → `stg_load_day_paths`.

---

## Задачи pipeline

| # | Задача | Результат |
|---|--------|-----------|
| 1 | Собрать `BuildStgDayParams` (дата + пути CSV/parquet) | Параметры цепочки |
| 2 | Выполнить 8 шагов по порядку | Parquet в `data/stg/load_day=…/` + логи DQ |
| 3 | Записать метрики каждого шага | `command_timing.jsonl` |

**Шаги (строго по порядку):**

1. `build-stg-oktmo`
2. `dq-stg-oktmo`
3. `build-stg-time-zones`
4. `dq-stg-time-zones`
5. `build-stg-tac`
6. `dq-stg-tac`
7. `build-stg-oksm`
8. `dq-stg-oksm`

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
| `oksm_csv_path` | path | Да | `src/mobile/raw_data/oksm_v001.csv` | Вход ОКСМ |
| `oksm_output_path` | path | Да | `data/stg/load_day={day}/oksm.parquet` | Выход + DQ |
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
| `data/stg/load_day={YYYY-MM-DD}/oksm.parquet` | `stg_oksm` |

Корневые `data/stg/*.parquet` (без `load_day`) **не обновляются** этой командой — для продакшн-справочников используйте одиночные `build-stg-*`.

---

## Источники

| # | Источник | Путь |
|---|----------|------|
| 1 | ОКТМО CSV | `oktmo_csv_path` |
| 2 | Time zones CSV | `time_zones_csv_path` |
| 3 | TAC CSV | `tac_csv_path` |
| 4 | ОКСМ CSV | `oksm_csv_path` |

---

## Алгоритм обработки данных

### Шаг 0. Инициализация

`params = default_build_stg_day_params(day)` → `stg_load_day_paths(day)` формирует каталог `data/stg/load_day={iso}/` и восемь путей (четыре CSV + четыре parquet).

### Шаг 1. Build `stg_oktmo`

1. `run_timed_command("build-stg-oktmo", …)`.
2. Вход: `params.oktmo_csv_path` (по умолчанию `src/mobile/raw_data/oktmo_v001.csv`).
3. Выход: `params.oktmo_output_path` → `data/stg/load_day={day}/oktmo.parquet`.
4. Вызов `oktmo.run(csv_path, output_path, compression)` — см. [`build_stg_oktmo.md`](./build_stg_oktmo.md).

### Шаг 2. DQ `stg_oktmo`

1. `dq_stg_oktmo.run_dq(params.oktmo_output_path)`.
2. Read-only проверки: схема, иерархия ОКТМО, WKT — [`dq_stg_oktmo.md`](../dq/stg/dq_stg_oktmo.md).
3. Процесс **не падает** при failed checks; статус в логах.

### Шаг 3. Build `stg_time_zones`

1. Аналогично шагу 1: CSV `time_zones.csv` → `load_day={day}/time_zones.parquet`.

### Шаг 4. DQ `stg_time_zones`

1. `dq_stg_time_zones.run_dq` на выход шага 3.

### Шаг 5. Build `stg_tac`

1. CSV `tacdb_v001.csv` → `load_day={day}/tac.parquet`.

### Шаг 6. DQ `stg_tac`

1. `dq_stg_tac.run_dq` на выход шага 5.

### Шаг 7. Build `stg_oksm`

1. CSV `oksm_v001.csv` → `load_day={day}/oksm.parquet` — см. [`build_stg_oksm.md`](./build_stg_oksm.md).

### Шаг 8. DQ `stg_oksm`

1. `dq_stg_oksm.run_dq` на выход шага 7.

**Оркестрация:** последовательность зашита в `BUILD_STG_DAY_STEPS` ([`day.py`](../../src/mobile/pipelines/stg/day.py)); каждый шаг пишет отдельную запись в `command_timing.jsonl`.

**Альтернатива:** `stg_day.run(params)` — те же шаги без отдельного `run_timed_command` на каждый (одна метрика на весь день).

### Проверки DQ в цепочке

После каждого build вызывается тот же `run_dq`, что у одиночных команд; путь parquet — из `load_day={day}/`.

| Шаг | DQ-команда | Документация checks |
|-----|------------|---------------------|
| 2 | `dq-stg-oktmo` | [`dq_stg_oktmo.md`](../dq/stg/dq_stg_oktmo.md#проверки) — схема, иерархия ОКТМО, WKT |
| 4 | `dq-stg-time-zones` | [`dq_stg_time_zones.md`](../dq/stg/dq_stg_time_zones.md#проверки) — timezone, распределение TZ, geometry |
| 6 | `dq-stg-tac` | [`dq_stg_tac.md`](../dq/stg/dq_stg_tac.md#проверки) — TAC, M2M, даты |
| 8 | `dq-stg-oksm` | [`dq_stg_oksm.md`](../dq/stg/dq_stg_oksm.md#проверки) — коды стран, имена |

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
