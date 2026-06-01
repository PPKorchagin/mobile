# dq-stg-msisdn-imei

**Витрина:** `stg_msisdn_imei` · **Команда:** `dq-stg-msisdn-imei` · **Режим:** read-only DQ (не изменяет данные, не падает при failed checks).

Референс: [`pipelines/dq/stg/msisdn_imei.py`](../../../src/mobile/pipelines/dq/stg/msisdn_imei.py). Сборка: [`build_stg_msisdn_imei.md`](../../stg/build_stg_msisdn_imei.md). Схема: [`msisdn_imei.json`](../../../src/mobile/schema/stg/msisdn_imei.json).

---

## Задачи pipeline

| # | Задача | Результат |
|---|--------|-----------|
| 1 | Найти месячный parquet `stg_msisdn_imei` за `report_date` | Путь `{YYYY-MM-01}.parquet` |
| 2 | Проверить контракт колонок и null-профиль | Логи `DQ_STG_MSISDN_IMEI` |
| 3 | Проверить форматы MSISDN/IMEI и целостность интервалов | Gate-статусы `ok/warning/failed` |
| 4 | Выдать `summary` | Счётчики checks |

**Бизнес-назначение:** контроль качества месячной витрины MSISDN↔IMEI после [`build-stg-msisdn-imei`](../../stg/build_stg_msisdn_imei.md) перед [`build-stg-msisdn-imsi-operator`](../../stg/build_stg_msisdn_imsi_operator.md), [`build-stg-person`](../../stg/build_stg_person.md) и [`build-stg-geo-intervals`](../../stg/build_stg_geo_intervals.md).

**В scope:** наличие файла, контракт `STG_MSISDN_IMEI_FIELDS`, null-профиль, порядок `valid_from`/`valid_to`, нормализация идентификаторов, дубликаты строк, пересечения и несклеенные сегменты по `(msisdn, imei)`.

---

## TODO

1. Добавить динамические пороги `warning/failed` по историческим baseline (объём интервалов, доля смен IMEI).

---

## Параметры запуска

Вызов: `run_dq(report_date, stg_msisdn_imei_path)` ([`cli.py`](../../../src/mobile/cli.py) → `dq-stg-msisdn-imei`). **Оба параметра обязательны** при явном прогоне — pipeline не подставляет пути по умолчанию; их резолвит CLI-оркестратор или явный вызов.

| Переменная | Тип | Обязательность | Описание |
|------------|-----|----------------|----------|
| `report_date` | date | **Да** | Любой календарный день; pipeline приводит к **1-му числу месяца** (`report_month_start`) |
| `stg_msisdn_imei_path` | path | **Да** | Месячный parquet или каталог `data/stg/msisdn_imei` (для каталога — файл `{YYYY-MM-01}.parquet`) |

### CLI

| Режим | Поведение |
|-------|-----------|
| Без флагов | Цикл `DEFAULT_SRC_START_DATE` … `DEFAULT_SRC_END_DATE` ([`cli_defaults.py`](../../../src/mobile/cli_defaults.py)); **один прогон на календарный месяц**, если `stg_msisdn_imei_output_path(day)` существует; timed-run `dq-stg-msisdn-imei-{YYYY-MM-01}` |
| Оба явно | `--report-date` (любой день, например `2025-01-15` → месяц `2025-01-01`) и `--stg-msisdn-imei-path` |

**Константы DQ в коде** ([`msisdn_imei.py`](../../../src/mobile/pipelines/dq/stg/msisdn_imei.py), на вход job **не передаются**):

| Константа | Значение |
|-----------|----------|
| `LOG_TAG` | `DQ_STG_MSISDN_IMEI` |
| `_EXPECTED_COLUMNS` | `msisdn`, `imei`, `valid_from`, `valid_to` из [`STG_MSISDN_IMEI_FIELDS`](../../../src/mobile/pipelines/stg/msisdn_imei.py) |
| `_REQUIRED_COLUMNS` | все поля контракта (null → **failed**) |
| Длины MSISDN | `MSISDN_MIN_LEN`–`MSISDN_MAX_LEN` ([`subscriber_ids.py`](../../../src/mobile/pipelines/stg/subscriber_ids.py)) |
| Длины IMEI | `IMEI_MIN_LEN`–`IMEI_MAX_LEN` |

**Предусловие:** `uv run mobile build-stg-msisdn-imei` за дни месяца с `stg_geo_all`.

Локальный запуск:

```bash
uv run mobile build-stg-msisdn-imei
uv run mobile dq-stg-msisdn-imei
uv run mobile dq-stg-msisdn-imei --report-date 2025-01-15 \
  --stg-msisdn-imei-path data/stg/msisdn_imei/2025-01-01.parquet
uv run mobile dq-stg-msisdn-imei --report-date 2025-01-01 --stg-msisdn-imei-path data/stg/msisdn_imei
uv run mobile nb-stg-msisdn-imei
```

Логи: `data/logs/mobile.log` (тег `DQ_STG_MSISDN_IMEI`). Метрики времени: `data/qa/command_timing.jsonl`, `command=dq-stg-msisdn-imei` или `dq-stg-msisdn-imei-{YYYY-MM-01}`. Визуализация: `nb-stg-msisdn-imei` → `data/notebooks/12_stg_msisdn_imei.executed.ipynb`.

---

## Структура проверяемой витрины

| Свойство | Значение |
|----------|----------|
| Имя таблицы | `stg_msisdn_imei` — [`msisdn_imei.json`](../../../src/mobile/schema/stg/msisdn_imei.json) |
| Путь по умолчанию | `data/stg/msisdn_imei/{YYYY-MM-01}.parquet` |
| Формат | Parquet (`snappy`) |
| Гранулярность | Месячный файл, пополняется ежедневно из `stg_geo_all` |
| Контракт полей | `STG_MSISDN_IMEI_FIELDS` из [`pipelines/stg/msisdn_imei.py`](../../../src/mobile/pipelines/stg/msisdn_imei.py) |

### Поля (контракт)

| # | Поле | Тип | Смысл |
|---|------|-----|-------|
| 1 | `msisdn` | string | MSISDN, E.164 (RU и иностранные номера) |
| 2 | `imei` | string | IMEI, 14–16 цифр |
| 3 | `valid_from` | timestamp | Первое событие интервала |
| 4 | `valid_to` | timestamp | Последнее событие интервала |

---

## Источники

| # | Источник | Путь | Назначение |
|---|----------|------|------------|
| 1 | `stg_msisdn_imei` | `data/stg/msisdn_imei/{YYYY-MM-01}.parquet` | Месячные интервалы MSISDN↔IMEI после `build-stg-msisdn-imei` |

---

## Алгоритм обработки данных

### Шаг 0. Инициализация

1. `report_month_start(report_date)` — входная дата → 1-е число месяца (в метриках при отличии — `report_date_input`).
2. `_resolve_source_path(report_month, stg_msisdn_imei_path)` — каталог → `{YYYY-MM-01}.parquet`, иначе файл как есть.
3. Счётчики `total_checks`, `warning_checks`, `failed_checks`.

### Шаг 1. Наличие набора

Нет файла → `dataset_presence` (**failed**), `summary`, return.  
Иначе `pd.read_parquet` → `dataset_basic` (**ok**: `row_count`, `column_count`, `distinct_msisdn`).

### Шаг 2. Схема и профиль

1. `schema_columns` — все поля `_EXPECTED_COLUMNS` (**failed** при пропусках; при пропусках — early return).
2. Для каждой колонки контракта: `nulls.{field}` (**failed** при null в обязательных полях).

### Шаг 3. Gate-проверки

1. `temporal_order` — `valid_to >= valid_from` (**failed**).
2. `msisdn_format` — нормализация `normalize_msisdn`, длина 7–15 (**failed**).
3. `imei_format` — нормализация `normalize_imei`, длина 14–16 (**failed**).
4. `normalization_canonical` — значения уже в каноническом виде ETL (**warning**).
5. `duplicate_rows` — полные дубликаты по всем колонкам (**warning**).
6. `interval_overlap_same_pair` — пересечение интервалов с одним `(msisdn, imei)` (**failed**).
7. `interval_mergeable_gap` — смежные сегменты с gap ≤ 1 с, не склеенные ETL (**warning**).

### Шаг 4. Итог

`summary` и return dict со статусом прогона. CLI не падает при failed checks.

---

## Проверки

Формат лога: `{"tag":"DQ_STG_MSISDN_IMEI","check":"...","status":"...","metrics":{...}}`.

| Check | Статус при сбое | Смысл | Обоснование |
|-------|-----------------|-------|-------------|
| `dataset_presence` | **failed** | Parquet за месяц не найден | Нет витрины после [`build-stg-msisdn-imei`](../../stg/build_stg_msisdn_imei.md) |
| `dataset_basic` | **ok** | `row_count`, `column_count`, `distinct_msisdn`, путь | Базовый объём для сравнения прогонов |
| `schema_columns` | **failed** | `missing_columns` | Контракт совпадает с ETL и [`msisdn_imei.json`](../../../src/mobile/schema/stg/msisdn_imei.json) |
| `nulls.*` | **failed** | null в обязательном поле | Интервал без MSISDN/IMEI/границ бесполезен для binding |
| `temporal_order` | **failed** | `valid_to < valid_from` | Некорректный интервал привязки |
| `msisdn_format` | **failed** | MSISDN вне допустимой длины/формата | Согласованность с [`subscriber_ids.py`](../../../src/mobile/pipelines/stg/subscriber_ids.py) |
| `imei_format` | **failed** | IMEI вне 14–16 цифр | Согласованность с ETL и TAC downstream |
| `normalization_canonical` | **warning** | не канонический MSISDN/IMEI в файле | ETL должен писать уже нормализованные значения |
| `duplicate_rows` | **warning** | полные дубликаты строк | Риск двойного учёта в person / geo-intervals |
| `interval_overlap_same_pair` | **failed** | пересечение интервалов `(msisdn, imei)` | Нарушение инварианта merge в ETL |
| `interval_mergeable_gap` | **warning** | сегменты с gap ≤ 1 с не склеены | Должны быть объединены `_merge_imei_intervals` |
| `summary` | **ok** | счётчики checks | Сводка прогона |

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| DQ pipeline | [`pipelines/dq/stg/msisdn_imei.py`](../../../src/mobile/pipelines/dq/stg/msisdn_imei.py) |
| DQ notebook | [`pipelines/nb/12_stg_msisdn_imei.ipynb`](../../../src/mobile/pipelines/nb/12_stg_msisdn_imei.ipynb) |
| ETL build | [`pipelines/stg/msisdn_imei.py`](../../../src/mobile/pipelines/stg/msisdn_imei.py) |
| Нормализация ID | [`pipelines/stg/subscriber_ids.py`](../../../src/mobile/pipelines/stg/subscriber_ids.py) |
| Пути layout | [`project_paths.py`](../../../src/mobile/project_paths.py) |
| CLI | [`cli.py`](../../../src/mobile/cli.py) |
| Схема | [`msisdn_imei.json`](../../../src/mobile/schema/stg/msisdn_imei.json) |
| Вход geo | [`build_stg_geo_all.md`](../../stg/build_stg_geo_all.md) |

| DQ IMSI+operator | [`dq_stg_msisdn_imsi_operator.md`](./dq_stg_msisdn_imsi_operator.md) |

Сквозная цепочка: `build-stg-geo-all` → **`build-stg-msisdn-imei`** → **`dq-stg-msisdn-imei`** → **`nb-stg-msisdn-imei`** → `build-stg-msisdn-imsi-operator` → **`dq-stg-msisdn-imsi-operator`** → `build-stg-person` / `build-stg-geo-intervals` → downstream.
