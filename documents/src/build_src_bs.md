# build-src-bs

Команда читает [`bs.json`](../../src/mobile/schema/src/bs.json), генерирует синтетическую витрину **`src_bs`** (БС в полигонах субъектов) и перезаписывает [`data/src/bs.parquet`](../../data/src/bs.parquet). Календарные окна `date_on` / `date_off`, опциональный профиль OpenCellID.

**Запуск** (из корня репозитория):

```bash
uv run mobile build-src-bs
```

Период, субъекты, операторы и seed — константы в [`cli_defaults.py`](../../src/mobile/cli_defaults.py). Профиль: `DEFAULT_BS_PROFILE_PATH` в [`project_paths.py`](../../src/mobile/project_paths.py). Флагов CLI нет. Entry point: `mobile = "mobile.cli:main"` в [`pyproject.toml`](../../pyproject.toml).

**Предварительно:** `mobile build-stg-oktmo` (нужен `data/stg/oktmo.parquet`).

---

## На вход

| № | Артефакт | Формат | Путь (по умолчанию) | Назначение |
|---|----------|--------|---------------------|------------|
| 1 | [`bs.json`](../../src/mobile/schema/src/bs.json) | JSON | `src/mobile/schema/src/bs.json` | Схема `fields`, `readiness.s3_layout` |
| 2 | Полигоны субъектов | Parquet | `data/stg/oktmo.parquet` | `level=1`, колонки `name`, `WKT` |
| 3 | Профиль генерации | JSON | `src/mobile/raw_data/build_bs_profile_from_opencellid.json` | Доли операторов/поколений, LAC/Cell, p50/p95 мощности |

Путь к ОКТМО в коде: `oktmo.json` → `readiness.s3_layout`.

---

## На выходе

| № | Артефакт | Формат | Путь (по умолчанию) | Назначение |
|---|----------|--------|---------------------|------------|
| 1 | `src_bs` | Parquet (snappy) | `data/src/bs.parquet` | Справочник БС (перезапись) |

Бизнес-ключ: `mcc` + `mnc` + `lac` + `cell` + `date_on`. Активная БС на конец периода: `date_off` = `2999-12-31 23:59:59` (`OPEN_BS_DATE_OFF` в `cli_defaults.py`). Timestamps в parquet — `datetime64[us]`.

---

## Параметры → `BuildBsParams`

Задаются в [`cli_defaults.py`](../../src/mobile/cli_defaults.py) → `default_bs_params()`, не в JSON.

| Параметр | Источник | По умолчанию | Смысл |
|----------|----------|--------------|-------|
| `start_date` / `end_date` | `DEFAULT_SRC_START_DATE` / `END` | `2024-12-25` … `2025-02-05` | Период генерации |
| `subjects` | `DEFAULT_REGION_SUBJECTS` | 3 субъекта | Фильтр ОКТМО `level=1` |
| `operators` | `OPERATORS` | 4 оператора | MNC в `mnc` |
| `seed` | `DEFAULT_BS_SEED` | `20250407` | Случайность генерации |
| `profile_path` | `DEFAULT_BS_PROFILE_PATH` | см. `project_paths` | JSON-профиль; опционально |

---

## Конфиг → код

[`bs.json`](../../src/mobile/schema/src/bs.json). JSON Schema не проверяется.

| Ключ | Использование |
|------|----------------|
| `readiness.s3_layout` | Выходной parquet |
| `readiness.parquet_compression` | `snappy` |
| `fields` | Порядок колонок, cast в `_coerce_types` |

Код: [`pipelines/src/bs.py`](../../src/mobile/pipelines/src/bs.py) — `run_from_config(config_path, oktmo_parquet_path, params)`.

---

## Логика сборки

1. Загрузка полигонов субъектов из ОКТМО (`level=1`, WKT → `Polygon` / `MultiPolygon`).
2. Опционально — профиль (`operator_distribution_pct`, `generation_distribution_pct`, `id_ranges`, `samples.p50/p95`).
3. Генерация строк: на субъект **2800–5200** строк, распределение по операторам; координаты rejection sampling в полигоне; `border` у границы (~1.3 км).
4. Шум ~22% строк (`_inject_noise`), приведение типов, валидация, запись parquet.
5. Метрики — `append_command_metrics(command="build-src-bs", ...)`.

---

## Строка: `_generate_row`

| Поле | Логика |
|------|--------|
| `lac` / `cell` | По умолчанию из профиля (`_sample_lac`, `_sample_cell`); см. **OCC-013** |
| `date_on` / `date_off` | MSK wall clock (`Europe/Moscow`, naive); активные на конец периода — `OPEN_BS_DATE_OFF` |
| Радио / координаты | Согласованный профиль поколения (`_coherent_radio_fields`); часть полей защищена от шума (`_PROTECTED_RADIO_COORD_FIELDS`) |

---

## Синтетические отклонения (Q&A / DQ)

Константы в [`bs.py`](../../src/mobile/pipelines/src/bs.py) (после `NOISE_FIELD_PROBABILITY`). Задаются в `_generate_row` **до** `_inject_noise`.

| ID | Доля / правило | Реализация |
|----|----------------|------------|
| **OCC-013** lac/cell | **~1.5%** null (`LAC_CELL_NULL_PROBABILITY`), **~1%** нули (`LAC_CELL_ZERO_PROBABILITY`) | `lac` и `cell` одновременно `null` или `0` — «неизвестная» БС; остальные строки — положительные значения из диапазона профиля |

Шум (`_inject_noise`) дополнительно портит произвольные поля, в т.ч. `lac`/`cell` (они **не** в `_PROTECTED_RADIO_COORD_FIELDS`), поэтому итоговая доля null/0 в parquet выше номинала (~6% / ~2% при дефолтном seed).

---

## Результат (дефолтный CLI)

- Порядка **8–16 тыс.** строк (3 субъекта × 2800–5200, seed `20250407`).
- **36** колонок по `fields` в `bs.json`.
- JSONL: `load_oktmo_sec`, `generate_rows_sec`, `write_parquet_sec`, `elapsed_total_sec`.

---

## Ошибки

| Исключение | Когда |
|------------|-------|
| `FileNotFoundError` | Нет `bs.json`, ОКТМО parquet или профиля |
| `ValueError` | Субъект не найден в ОКТМО; неподдерживаемая геометрия; пустой датасет |
| pandas / pyarrow | Запись parquet |

---

## TODO

1. Обновить профиль генерации после сверки с prod/DQ.
2. При необходимости вынести период и субъекты в аргументы CLI.
