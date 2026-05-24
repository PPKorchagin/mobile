# build-src-person

Команда читает [`person.json`](../../src/mobile/schema/src/person.json), генерирует синтетическую витрину **src_person** (профиль/принадлежность абонента по календарным дням) и пишет parquet по суточным каталогам. Дни **полного среза** помечаются `_SUCCESS`.

**Запуск** (из корня репозитория):

```bash
uv run mobile build-src-person
uv run mobile build-src-person --target-per-operator 5000
```

Период, операторы, seed и правила полного среза — константы в [`cli_defaults.py`](../../src/mobile/cli_defaults.py). Объём на оператора в полный день задаётся флагом **`--target-per-operator`** (по умолчанию `DEFAULT_SRC_PERSON_TARGET_PER_OPERATOR` = `50_000`). Entry point: `mobile = "mobile.cli:main"` в [`pyproject.toml`](../../pyproject.toml).

---

## На вход

| № | Артефакт | Формат | Путь (по умолчанию) | Назначение |
|---|----------|--------|---------------------|------------|
| 1 | [`person.json`](../../src/mobile/schema/src/person.json) | JSON | `src/mobile/schema/src/person.json` | Схема `fields`, `readiness` (`s3_layout`, `success_flag`) |

Внешние справочники для сборки **не требуются**.

---

## На выходе

| № | Артефакт | Формат | Путь (по умолчанию) | Назначение |
|---|----------|--------|---------------------|------------|
| 1 | `src_person` | Parquet (snappy) | `data/src/person/load_year={YYYY}/load_month={MM}/load_day={DD}/person.parquet` | Срез на календарный день |
| 2 | Маркер готовности | пустой файл | `.../load_day={DD}/_SUCCESS` | Только на днях **полного среза** |

Шаблон каталога — `readiness.s3_layout` в JSON; имя `person.parquet` добавляется в [`person.py`](../../src/mobile/pipelines/src/person.py) (`_resolve_output_path`), если в шаблоне нет `.parquet`.

**Ключ для матчинга с ОСС:** `isdn` + интервал `actually_from` / `actually_to`. `operator_Id` = MNC из `OPERATORS` (имена в CLI: `билайн`, `мегафон`, `мтс`, `теле2`).

**Открытый интервал** у активных: `actually_to` = `ACTUALLY_TO_OPEN` (`2261-12-31 23:59:59`) — предел Parquet `timestamp[ns]`; в Q&A по схеме — `9999-12-31`.

---

## Полные срезы и `_SUCCESS`

Функция [`select_full_snapshot_days`](../../src/mobile/pipelines/src/person.py) формирует множество дней с полным объёмом и маркером. Составляется **объединение** двух правил:

| Правило | Описание |
|---------|----------|
| **Конец месяца** | Для каждого месяца в `[start_date, end_date]`: последний календарный день месяца, но не позже `end_date` |
| **Случайные дни** | Ещё `extra_random_full_snapshot_days` дней из остатка периода (без дублей с month-end), детерминированно по `seed` |

Параметры: `extra_random_full_snapshot_days` ← **7** (`DEFAULT_SRC_PERSON_EXTRA_FULL_SNAPSHOT_RANDOM_DAYS` в [`cli_defaults.py`](../../src/mobile/cli_defaults.py)); `seed` ← `DEFAULT_BS_SEED` (`20250407`).

**Пример** для `2024-12-25` … `2025-02-05` при дефолтном seed (10 дней):

| Тип | Даты |
|-----|------|
| month-end | `2024-12-31`, `2025-01-31`, `2025-02-05` |
| random (+7) | `2025-01-03`, `2025-01-05`, `2025-01-07`, `2025-01-08`, `2025-01-09`, `2025-01-12`, `2025-01-15` |

На полном срезе: `per_operator_count = target_active_subscribers_per_operator`; на остальных днях — доля пула `daily_active_ratio_min` … `max`.

---

## Параметры CLI → `BuildSrcPersonParams`

Задаются в [`cli_defaults.py`](../../src/mobile/cli_defaults.py) → `default_person_params()`, не в JSON. Из CLI передаётся только **`--target-per-operator`**.

| Параметр | Источник | По умолчанию | Смысл |
|----------|----------|--------------|-------|
| `start_date` / `end_date` | `DEFAULT_SRC_START_DATE` / `END` | `2024-12-25` … `2025-02-05` | **43** календарных дня |
| `operators` | `OPERATORS` | 4 оператора | MNC в `operator_Id` |
| `target_active_subscribers_per_operator` | `--target-per-operator N` | `50_000` | Пул на оператора в **полный** день |
| `daily_active_ratio_min` / `max` | константы | `0.55` / `0.95` | Доля пула в частичные дни |
| `closed_contract_ratio` | константа | `0.18` | Закрытые договоры |
| `inactive_ratio` | константа | `0.12` | Неактивные |
| `corporate_ratio` | константа | `0.14` | `client_type=1` |
| `inter_operator_transition_ratio` | константа | `0.10` | Смена оператора |
| `movement_ratio` | константа | `0.22` | «Переезд» home operator |
| `foreign_subscriber_ratio` | константа | `0.10` | Иностранные ФЛ |
| `extra_random_full_snapshot_days` | `DEFAULT_SRC_PERSON_EXTRA_FULL_SNAPSHOT_RANDOM_DAYS` | **7** | Случайные полные дни поверх month-end |
| `seed` | `DEFAULT_BS_SEED` | `20250407` | Faker, генерация, выбор random full-дней |
| `max_workers` | `default_max_workers()` | `1…8` | Параллелизм по **дням** (потоки) |

---

## Конфиг → код

[`person.json`](../../src/mobile/schema/src/person.json). JSON Schema не проверяется.

| Ключ | Использование |
|------|----------------|
| `readiness.s3_layout` | Шаблон каталога дня |
| `readiness.success_flag` | Имя `_SUCCESS` |
| `readiness.parquet_compression` | `snappy` |
| `fields` | Порядок колонок, Arrow-схема |

---

## `run_from_config` ([`person.py`](../../src/mobile/pipelines/src/person.py))

`run_from_config(config_path, params) -> dict` — оркестратор + метрики.

1. **Конфиг** — `fields`, `readiness`; иначе `FileNotFoundError`.
2. **Faker** — локаль `ru_RU` (`_build_faker_pool`).
3. **Календарь** — дни `[start_date … end_date]`; `select_full_snapshot_days(...)`.
4. **Параллелизм** — по дням `ThreadPoolExecutor`; внутри дня операторы в **4 процессах** (`ProcessPoolExecutor`, `OPERATOR_PROCESS_WORKERS=4`).
5. **Метрики** — `append_command_metrics(command="build-src-person", ...)`.

---

## Ошибки

| Исключение | Когда |
|------------|-------|
| `FileNotFoundError` | Нет `person.json` |
| `ValueError` | Пустой период; `extra_random_full_snapshot_days < 0` |
| `KeyError` | Оператор не из `OPERATORS` |
| pyarrow | Запись parquet / несовместимость схемы |

---

## TODO

1. Вынести `extra_random_full_snapshot_days` и период в аргументы CLI при необходимости.
2. Сверить `abonent_status` с поставщиком (PER-016).
