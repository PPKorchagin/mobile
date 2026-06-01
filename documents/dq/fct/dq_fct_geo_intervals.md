# dq-fct-geo-intervals

**Витрина:** `fct_geo_intervals` · **Команда:** `dq-fct-geo-intervals` · **Режим:** read-only DQ (не изменяет данные, не падает при failed checks).

Референс: [`pipelines/dq/fct/geo_intervals.py`](../../../src/mobile/pipelines/dq/fct/geo_intervals.py). Сборка: [`build_fct_geo_intervals.md`](../../fct/build_fct_geo_intervals.md). Схема: [`geo_intervals.json`](../../../src/mobile/schema/fct/geo_intervals.json).

---

## Задачи pipeline

| # | Задача | Результат |
|---|--------|-----------|
| 1 | Найти parquet `fct_geo_intervals` за `report_date` | Путь к дневному срезу |
| 2 | Проверить контракт колонок и профили null/cardinality | Логи `DQ_FCT_GEO_INTERVALS` |
| 3 | Проверить временные, географические и ключевые ограничения | Gate-статусы `ok/warning/failed` |
| 4 | Выдать `summary` | Счётчики checks |

**Бизнес-назначение:** контроль качества дневных интервалов пребывания после [`build-fct-geo-intervals`](../../fct/build_fct_geo_intervals.md) перед downstream-анализом треков и поведенческими признаками.

**В scope:** наличие файла, контракт `_EXPECTED_COLUMNS`, обязательные поля интервала, координаты, `bs_type`/`timezone`, непустой `cgi_list`, дубликаты ключа интервала.

---

## TODO

1. Добавить динамические пороги `warning/failed` по историческим baseline (доля null в `timezone`/`imsi`, длина `cgi_list`).
2. Расширить профиль распределениями длительности интервала (`end_time_utc - start_time_utc`) и числа CGI в интервале.

---

## Параметры запуска

Вызов: `run_dq(report_date, fct_geo_intervals_path)` ([`cli.py`](../../../src/mobile/cli.py) → `dq-fct-geo-intervals`). **Оба параметра обязательны** — pipeline не подставляет пути по умолчанию; их резолвит CLI-оркестратор или явный вызов.

| Переменная | Тип | Обязательность | Описание |
|------------|-----|----------------|----------|
| `report_date` | date | **Да** | Отчётный день |
| `fct_geo_intervals_path` | path | **Да** | Входной parquet или каталог `data/fct/geo_intervals` (для каталога — файл `{report_date}.parquet`) |

### CLI

| Режим | Поведение |
|-------|-----------|
| Без флагов | Цикл `DEFAULT_SRC_START_DATE` … `DEFAULT_SRC_END_DATE` ([`cli_defaults.py`](../../../src/mobile/cli_defaults.py)); на каждый день с существующим `fct_geo_intervals_output_path(day)` — `dq-fct-geo-intervals-{YYYY-MM-DD}` |
| Оба явно | `--report-date` и `--fct-geo-intervals-path` (один прогон) |

**Константы DQ в коде** ([`geo_intervals.py`](../../../src/mobile/pipelines/dq/fct/geo_intervals.py), на вход job **не передаются**):

| Константа | Значение |
|-----------|----------|
| `LOG_TAG` | `DQ_FCT_GEO_INTERVALS` |
| `_BS_TYPES` | `m`, `f`, `i`, `x`, `o` |
| `_EXPECTED_COLUMNS` | `_OUTPUT_COLUMNS` из ETL [`stg/geo_intervals.py`](../../../src/mobile/pipelines/fct/geo_intervals.py) |

**Предусловие:** `uv run mobile build-fct-geo-intervals` за ту же `report_date` (и binding-витрины за месяц этого дня).

Локальный запуск:

```bash
uv run mobile build-fct-msisdn-imei
uv run mobile build-fct-msisdn-imsi-operator
uv run mobile build-fct-geo-intervals
uv run mobile dq-fct-geo-intervals
uv run mobile dq-fct-geo-intervals --report-date 2025-01-15 \
  --fct-geo-intervals-path data/fct/geo_intervals/2025-01-15.parquet
uv run mobile dq-fct-geo-intervals --report-date 2025-01-15 \
  --fct-geo-intervals-path data/fct/geo_intervals
uv run mobile nb-fct-geo-intervals
```

Логи: `data/logs/mobile.log` (тег `DQ_FCT_GEO_INTERVALS`). Метрики времени: `data/qa/command_timing.jsonl`, `command=dq-fct-geo-intervals` или `dq-fct-geo-intervals-{date}`. Визуализация: `nb-fct-geo-intervals` → `data/notebooks/14_fct_geo_intervals.executed.ipynb`.

---

## Структура проверяемой витрины

| Свойство | Значение |
|----------|----------|
| Имя таблицы | `fct_geo_intervals` — [`geo_intervals.json`](../../../src/mobile/schema/fct/geo_intervals.json) |
| Путь по умолчанию | `data/fct/geo_intervals/{YYYY-MM-DD}.parquet` |
| Формат | Parquet (`snappy`) |
| Календарный срез | `report_date` (поле `time_key`) |
| Контракт полей | `_OUTPUT_COLUMNS` из [`pipelines/fct/geo_intervals.py`](../../../src/mobile/pipelines/fct/geo_intervals.py) |

### Поля (контракт)

| # | Поле | Тип | Смысл |
|---|------|-----|-------|
| 1 | `msisdn` | long | MSISDN абонента |
| 2 | `imsi` | long | IMSI (с дозаполнением из `fct_msisdn_imsi`) |
| 3 | `imei` | long | IMEI (с дозаполнением из `fct_msisdn_imei`) |
| 4 | `start_time_utc` | timestamp | Начало интервала в UTC |
| 5 | `end_time_utc` | timestamp | Конец интервала в UTC |
| 6 | `cgi_list` | list | Уникальные CGI в интервале |
| 7 | `sub_lat` | double | Оценочная широта |
| 8 | `sub_lon` | double | Оценочная долгота |
| 9 | `bs_type` | string | Тип БС (`m`/`f`/`i`/`x`/`o`) |
| 10 | `timezone` | int | Смещение от UTC, часы |
| 11 | `oktmo_code_1` | string | Доминирующий ОКТМО уровня 1 |
| 12 | `oktmo_code_2` | string | Доминирующий ОКТМО уровня 2 |
| 13 | `time_key` | date | Календарный день среза |

---

## Источники

| # | Источник | Путь | Назначение |
|---|----------|------|------------|
| 1 | `fct_geo_intervals` | `data/fct/geo_intervals/{YYYY-MM-DD}.parquet` | Дневной срез после `build-fct-geo-intervals` |

---

## Алгоритм обработки данных

### Шаг 0. Инициализация

1. `_resolve_source_path(report_date, fct_geo_intervals_path)` — оба аргумента обязательны; `resolve_stg_daily_parquet_path`: каталог → `{report_date}.parquet`, иначе файл как есть.
2. Счётчики `total_checks`, `warning_checks`, `failed_checks`.

### Шаг 1. Наличие набора

Нет файла → `dataset_presence` (**failed**), `summary`, return.  
Иначе `pd.read_parquet` → `dataset_basic` (**ok**).

### Шаг 2. Схема и профиль

1. `schema_columns` — все поля `_EXPECTED_COLUMNS` (**failed** при пропусках).
2. Для каждой колонки контракта: `nulls.{field}`, `cardinality.{field}` (**ok**).

### Шаг 3. Gate-проверки

1. `required_fields_presence` — `msisdn`, `start_time_utc`, `end_time_utc` без null (**failed**).
2. `temporal_order` — `end_time_utc >= start_time_utc` (**failed**).
3. `coords_range` — `sub_lat` ∈ [-90, 90], `sub_lon` ∈ [-180, 180] (**warning**).
4. `bs_type_vocab` — `bs_type ∈ _BS_TYPES` (**warning**).
5. `timezone_range` — `timezone` в [-12, 14] (**warning**).
6. `cgi_list_non_empty` — у каждой строки непустой `cgi_list` (**failed**).
7. `distribution.cgi_list_len` — распределение длины списка CGI (**ok**).
8. `duplicate_interval_key` — дубли `(msisdn, imsi, imei, start_time_utc, end_time_utc, bs_type)` (**warning**).

### Шаг 4. Итог

`summary` и return dict со статусом прогона. CLI не падает при failed checks.

---

## Проверки

Формат лога: `{"tag":"DQ_FCT_GEO_INTERVALS","check":"...","status":"...","metrics":{...}}`.

| Check | Статус при сбое | Смысл | Обоснование |
|-------|-----------------|-------|-------------|
| `dataset_presence` | **failed** | Parquet за день не найден | Нет среза после [`build-fct-geo-intervals`](../../fct/build_fct_geo_intervals.md) |
| `dataset_basic` | **ok** | `row_count`, `column_count`, путь | Базовый объём для сравнения прогонов |
| `schema_columns` | **failed** | `missing_columns` | Контракт совпадает с ETL и [`geo_intervals.json`](../../../src/mobile/schema/fct/geo_intervals.json) |
| `nulls.*` / `cardinality.*` | **ok** | профиль полей | Полнота и кардинальность без выгрузки PII |
| `required_fields_presence` | **failed** | null в `msisdn` / границах интервала | Без MSISDN и времени интервал бесполезен |
| `temporal_order` | **failed** | `end < start` | Некорректный интервал пребывания |
| `coords_range` | **warning** | координаты вне диапазона | Гео-точка интервала и карты |
| `bs_type_vocab` | **warning** | неизвестный `bs_type` | Согласованность с enrich `fct_bs` |
| `timezone_range` | **warning** | offset вне [-12, 14] | Согласованность с `dim_time_zones` / fallback из БС |
| `cgi_list_non_empty` | **failed** | пустой `cgi_list` | Интервал без сот — нарушение AGG_GEO_INTERVALS |
| `distribution.cgi_list_len` | **ok** | counts по длине `cgi_list` | Профиль мобильности / handover |
| `duplicate_interval_key` | **warning** | дубли ключа интервала | Риск двойного учёта в downstream |
| `summary` | **ok** | счётчики checks | Сводка прогона |

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| DQ pipeline | [`pipelines/dq/fct/geo_intervals.py`](../../../src/mobile/pipelines/dq/fct/geo_intervals.py) |
| DQ notebook | [`pipelines/nb/14_fct_geo_intervals.ipynb`](../../../src/mobile/pipelines/nb/14_fct_geo_intervals.ipynb) |
| ETL build | [`pipelines/fct/geo_intervals.py`](../../../src/mobile/pipelines/fct/geo_intervals.py) |
| Пути layout | [`project_paths.py`](../../../src/mobile/project_paths.py) |
| CLI | [`cli.py`](../../../src/mobile/cli.py) |
| Схема | [`geo_intervals.json`](../../../src/mobile/schema/fct/geo_intervals.json) |
| Вход geo | [`build_stg_geo_all.md`](../../stg/build_stg_geo_all.md) |
| Binding IMEI | [`dq_fct_msisdn_imei.md`](./dq_fct_msisdn_imei.md) |
| Binding IMSI | [`dq_fct_msisdn_imsi_operator.md`](./dq_fct_msisdn_imsi_operator.md) |

Сквозная цепочка: `build-stg-geo-all` → `build-fct-msisdn-imei` → `build-fct-msisdn-imsi-operator` → **`build-fct-geo-intervals`** → **`dq-fct-geo-intervals`** → **`nb-fct-geo-intervals`** → downstream.
