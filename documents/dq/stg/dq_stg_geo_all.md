# dq-stg-geo-all

**Витрина:** `stg_geo_all` · **Команда:** `dq-stg-geo-all` · **Режим:** read-only DQ (не изменяет данные, не падает при failed checks).

Референс: [`pipelines/dq/stg/geo_all.py`](../../../src/mobile/pipelines/dq/stg/geo_all.py). Сборка: [`build_stg_geo_all.md`](../../stg/build_stg_geo_all.md). Схема: [`geo_all.json`](../../../src/mobile/schema/stg/geo_all.json).

---

## Задачи pipeline

| # | Задача | Результат |
|---|--------|-----------|
| 1 | Найти parquet `stg_geo_all` за `report_date` | Путь к дневному срезу |
| 2 | Проверить контракт колонок и профили null/cardinality | Логи `DQ_STG_GEO_ALL` |
| 3 | Проверить доменные и временные правила | Gate-статусы `ok/warning/failed` |
| 4 | Выдать `summary` | Счётчики checks |

**Бизнес-назначение:** контроль качества дневной гео-витрины после [`build-stg-geo-all`](../../stg/build_stg_geo_all.md) перед `build-stg-geo-intervals` и связками MSISDN↔IMSI/IMEI.

**В scope:** наличие файла, контракт `_OUTPUT_COLUMNS`, координаты, время, словари, дубликаты ключа события.

---

## TODO

1. Добавить динамические пороги для `warning/failed` по историческим baseline.
2. Расширить распределения по `utc_offset` и `event_count` bucket-профилем.

---

## Параметры запуска

Вызов: `run_dq(report_date, stg_geo_all_path)` ([`cli.py`](../../../src/mobile/cli.py) → `dq-stg-geo-all`). **Оба параметра обязательны** — pipeline не подставляет пути по умолчанию; их резолвит CLI-оркестратор или явный вызов.

| Переменная | Тип | Обязательность | Описание |
|------------|-----|----------------|----------|
| `report_date` | date | **Да** | Отчётный день |
| `stg_geo_all_path` | path | **Да** | Входной parquet или каталог `data/stg/geo_all` (для каталога — файл `{report_date}.parquet`) |

### CLI

| Режим | Поведение |
|-------|-----------|
| Без флагов | Цикл `DEFAULT_SRC_START_DATE` … `DEFAULT_SRC_END_DATE` ([`cli_defaults.py`](../../../src/mobile/cli_defaults.py)); на каждый день — `dq-stg-geo-all-{YYYY-MM-DD}` с `stg_geo_all_output_path(day)` |
| Оба явно | `--report-date` и `--stg-geo-all-path` (один прогон) |

**Константы DQ в коде** ([`geo_all.py`](../../../src/mobile/pipelines/dq/stg/geo_all.py)):

| Константа | Значение |
|-----------|----------|
| `LOG_TAG` | `DQ_STG_GEO_ALL` |
| `_EVENT_TYPES` | `cdr`, `sms`, `gprs`, `location` |
| `_BS_TYPES` | `m`, `f`, `i`, `x`, `o` |
| `_EXPECTED_COLUMNS` | `_OUTPUT_COLUMNS` из ETL [`stg/geo_all.py`](../../../src/mobile/pipelines/stg/geo_all.py) |

**Предусловие:** `uv run mobile build-stg-geo-all` за ту же `report_date`.

Локальный запуск:

```bash
uv run mobile build-stg-geo-all
uv run mobile dq-stg-geo-all
uv run mobile dq-stg-geo-all --report-date 2025-01-01 \
  --stg-geo-all-path data/stg/geo_all/2025-01-01.parquet
uv run mobile dq-stg-geo-all --report-date 2025-01-01 --stg-geo-all-path data/stg/geo_all
uv run mobile nb-stg-geo-all
```

Логи: `data/logs/mobile.log` (тег `DQ_STG_GEO_ALL`). Метрики времени: `data/qa/command_timing.jsonl`, `command=dq-stg-geo-all` или `dq-stg-geo-all-{date}`. Визуализация: `nb-stg-geo-all` → `data/notebooks/11_stg_geo_all.executed.ipynb`.

---

## Структура проверяемой витрины

| Свойство | Значение |
|----------|----------|
| Имя таблицы | `stg_geo_all` — [`geo_all.json`](../../../src/mobile/schema/stg/geo_all.json) |
| Путь по умолчанию | `data/stg/geo_all/{YYYY-MM-DD}.parquet` |
| Формат | Parquet |
| Контракт полей | `_OUTPUT_COLUMNS` из [`pipelines/stg/geo_all.py`](../../../src/mobile/pipelines/stg/geo_all.py) |

---

## Источники

| # | Источник | Путь | Назначение |
|---|----------|------|------------|
| 1 | `stg_geo_all` | `data/stg/geo_all/{YYYY-MM-DD}.parquet` | Дневной срез после `build-stg-geo-all` |

---

## Алгоритм обработки данных

### Шаг 0. Инициализация

1. `_resolve_source_path(report_date, stg_geo_all_path)` — оба аргумента обязательны; каталог → `{report_date}.parquet`, иначе файл как есть.
2. Счётчики `total_checks`, `warning_checks`, `failed_checks`.

### Шаг 1. Наличие набора

Нет файла → `dataset_presence` (**failed**), `summary`, return.  
Иначе `pd.read_parquet` → `dataset_basic` (**ok**).

### Шаг 2. Схема и профиль

1. `schema_columns` — все поля `_EXPECTED_COLUMNS` (**failed** при пропусках).
2. Для каждой колонки контракта: `nulls.{field}`, `cardinality.{field}` (**ok**).

### Шаг 3. Gate-проверки

1. `required_fields_presence` — `msisdn`, `cgi`, `start_time_utc` без null (**failed**).
2. `coords_range` — диапазоны lat/lon (**warning**).
3. `temporal_order` — `end_time_utc >= start_time_utc` (**failed**).
4. `event_count_valid` — `event_count >= 1` (**failed**).
5. `source_event_type_vocab` — только `_EVENT_TYPES` (**failed**).
6. `distribution.source_event_type` — counts (**ok**).
7. `utc_offset_range` — UTC offset в [-12, 14] (**warning**).
8. `bs_type_vocab` — `bs_type ∈ _BS_TYPES` (**warning**).
9. `duplicate_event_key` — дубли `(msisdn, start_time_utc, source_event_type, cgi)` (**warning**).

### Шаг 4. Итог

`summary` и return dict со статусом прогона. CLI не падает при failed checks.

---

## Проверки

Формат лога: `{"tag":"DQ_STG_GEO_ALL","check":"...","status":"...","metrics":{...}}`.

| Check | Статус при сбое | Смысл | Обоснование |
|-------|-----------------|-------|-------------|
| `dataset_presence` | **failed** | Parquet за день не найден | Нет среза после [`build-stg-geo-all`](../../stg/build_stg_geo_all.md) |
| `dataset_basic` | **ok** | `row_count`, `column_count`, путь | Базовый объём для сравнения прогонов |
| `schema_columns` | **failed** | `missing_columns` | Контракт совпадает с ETL и [`geo_all.json`](../../../src/mobile/schema/stg/geo_all.json) |
| `nulls.*` / `cardinality.*` | **ok** | профиль полей | Полнота и кардинальность без выгрузки PII |
| `required_fields_presence` | **failed** | null в ключевых полях события | Без MSISDN/CGI/времени нельзя строить интервалы |
| `coords_range` | **warning** | координаты вне диапазона | Гео-аналитика и карты |
| `temporal_order` | **failed** | `end < start` | Интервал события некорректен |
| `event_count_valid` | **failed** | `event_count < 1` | После 5m-агрегации в группе минимум одно событие |
| `source_event_type_vocab` | **failed** | неизвестный тип | Согласованность с `event_dds` |
| `utc_offset_range` | **warning** | offset вне [-12, 14] | Согласованность с `stg_bs.timezone` / `stg_time_zones` |
| `bs_type_vocab` | **warning** | неизвестный `bs_type` | Тип БС из enrich `stg_bs` |
| `duplicate_event_key` | **warning** | дубли ключа события | Риск двойного учёта в downstream |
| `summary` | **ok** | счётчики checks | Сводка прогона |

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| DQ pipeline | [`pipelines/dq/stg/geo_all.py`](../../../src/mobile/pipelines/dq/stg/geo_all.py) |
| DQ notebook | [`pipelines/nb/11_stg_geo_all.ipynb`](../../../src/mobile/pipelines/nb/11_stg_geo_all.ipynb) |
| ETL build | [`pipelines/stg/geo_all.py`](../../../src/mobile/pipelines/stg/geo_all.py) |
| CLI | [`cli.py`](../../../src/mobile/cli.py) |
| Схема | [`geo_all.json`](../../../src/mobile/schema/stg/geo_all.json) |

Сквозная цепочка: `build-stg-event` → `build-move-event` → `build-stg-bs` → `build-stg-geo-all` → **`dq-stg-geo-all`** → **`nb-stg-geo-all`** → `build-stg-geo-intervals` → downstream.
