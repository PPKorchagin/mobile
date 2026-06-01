# dq-fct-msisdn-imsi-operator

**Витрина:** `fct_msisdn_imsi` · **Команда:** `dq-fct-msisdn-imsi-operator` · **Режим:** read-only DQ (не изменяет данные, не падает при failed checks).

Референс: [`pipelines/dq/stg/msisdn_imsi_operator.py`](../../../src/mobile/pipelines/dq/stg/msisdn_imsi_operator.py). Сборка: [`build_fct_msisdn_imsi_operator.md`](../../fct/build_fct_msisdn_imsi_operator.md). Схема: [`msisdn_imsi.json`](../../../src/mobile/schema/fct/msisdn_imsi.json).

---

## Задачи pipeline

| # | Задача | Результат |
|---|--------|-----------|
| 1 | Найти месячный parquet `fct_msisdn_imsi` за `report_date` | Путь `{YYYY-MM-01}.parquet` |
| 2 | Проверить контракт колонок и null-профиль | Логи `DQ_FCT_MSISDN_IMSI_OPERATOR` |
| 3 | Проверить MSISDN/IMSI/`operator_id` и интервалы | Gate-статусы `ok/warning/failed` |
| 4 | Выдать `summary` | Счётчики checks |

**Бизнес-назначение:** контроль качества месячной витрины MSISDN↔IMSI↔`operator_id` после [`build-fct-msisdn-imsi-operator`](../../fct/build_fct_msisdn_imsi_operator.md) перед [`build-fct-person`](../../fct/build_fct_person.md) и [`build-fct-geo-intervals`](../../fct/build_fct_geo_intervals.md).

**В scope:** наличие файла, контракт `FCT_MSISDN_IMSI_FIELDS`, null-профиль, порядок `valid_from`/`valid_to`, нормализация идентификаторов, согласованность `operator_id` с IMSI (MCC=250), дубликаты, пересечения и несклеенные сегменты по `(msisdn, operator_id, imsi)`.

---

## TODO

1. Добавить динамические пороги `warning/failed` по baseline (доля MNP, смен IMSI).

---

## Параметры запуска

Вызов: `run_dq(report_date, fct_msisdn_imsi_path)` ([`cli.py`](../../../src/mobile/cli.py) → `dq-fct-msisdn-imsi-operator`). **Оба параметра обязательны** при явном прогоне — pipeline не подставляет пути по умолчанию; их резолвит CLI-оркестратор или явный вызов.

| Переменная | Тип | Обязательность | Описание |
|------------|-----|----------------|----------|
| `report_date` | date | **Да** | Любой календарный день; pipeline приводит к **1-му числу месяца** (`report_month_start`) |
| `fct_msisdn_imsi_path` | path | **Да** | Месячный parquet или каталог `data/fct/msisdn_imsi` (для каталога — файл `{YYYY-MM-01}.parquet`) |

### CLI

| Режим | Поведение |
|-------|-----------|
| Без флагов | Цикл `DEFAULT_SRC_START_DATE` … `DEFAULT_SRC_END_DATE` ([`cli_defaults.py`](../../../src/mobile/cli_defaults.py)); **один прогон на календарный месяц**, если `fct_msisdn_imsi_output_path(day)` существует; timed-run `dq-fct-msisdn-imsi-operator-{YYYY-MM-01}` |
| Оба явно | `--report-date` (любой день, например `2025-01-15` → месяц `2025-01-01`) и `--fct-msisdn-imsi-path` |

**Константы DQ в коде** ([`msisdn_imsi_operator.py`](../../../src/mobile/pipelines/dq/stg/msisdn_imsi_operator.py), на вход job **не передаются**):

| Константа | Значение |
|-----------|----------|
| `LOG_TAG` | `DQ_FCT_MSISDN_IMSI_OPERATOR` |
| `_EXPECTED_COLUMNS` | `msisdn`, `imsi`, `operator_id`, `valid_from`, `valid_to` |
| `_INTERVAL_GROUP_COLS` | `(msisdn, operator_id, imsi)` — ключ merge ETL |
| Правило `operator_id` | `operator_id_from_imsi_series` — MNC = цифры 4–5 при MCC `250` |

**Предусловие:** `uv run mobile build-fct-msisdn-imsi-operator` за дни месяца с `stg_geo_all`.

Локальный запуск:

```bash
uv run mobile build-fct-msisdn-imsi-operator
uv run mobile dq-fct-msisdn-imsi-operator
uv run mobile dq-fct-msisdn-imsi-operator --report-date 2025-01-15 \
  --fct-msisdn-imsi-path data/fct/msisdn_imsi/2025-01-01.parquet
uv run mobile dq-fct-msisdn-imsi-operator --report-date 2025-01-01 --fct-msisdn-imsi-path data/fct/msisdn_imsi
uv run mobile nb-fct-msisdn-imsi-operator
```

Логи: `data/logs/mobile.log` (тег `DQ_FCT_MSISDN_IMSI_OPERATOR`). Метрики времени: `data/qa/command_timing.jsonl`, `command=dq-fct-msisdn-imsi-operator` или `dq-fct-msisdn-imsi-operator-{YYYY-MM-01}`. Визуализация: `nb-fct-msisdn-imsi-operator` → `data/notebooks/13_fct_msisdn_imsi_operator.executed.ipynb`.

---

## Структура проверяемой витрины

| Свойство | Значение |
|----------|----------|
| Имя таблицы | `fct_msisdn_imsi` — [`msisdn_imsi.json`](../../../src/mobile/schema/fct/msisdn_imsi.json) |
| Путь по умолчанию | `data/fct/msisdn_imsi/{YYYY-MM-01}.parquet` |
| Формат | Parquet (`snappy`) |
| Гранулярность | Месячный файл, пополняется ежедневно из `stg_geo_all` |
| Контракт полей | `FCT_MSISDN_IMSI_FIELDS` из [`pipelines/stg/msisdn_imsi.py`](../../../src/mobile/pipelines/stg/msisdn_imsi.py) |

### Поля (контракт)

| # | Поле | Тип | Смысл |
|---|------|-----|-------|
| 1 | `msisdn` | string | MSISDN, E.164 |
| 2 | `imsi` | string | IMSI, 14–15 цифр |
| 3 | `operator_id` | long | MNC из IMSI при MCC=250 (наблюдения) |
| 4 | `valid_from` | timestamp | Начало интервала |
| 5 | `valid_to` | timestamp | Конец интервала |

---

## Источники

| # | Источник | Путь | Назначение |
|---|----------|------|------------|
| 1 | `fct_msisdn_imsi` | `data/fct/msisdn_imsi/{YYYY-MM-01}.parquet` | Месячные интервалы MSISDN↔IMSI↔operator после `build-fct-msisdn-imsi-operator` |

---

## Алгоритм обработки данных

### Шаг 0. Инициализация

1. `report_month_start(report_date)` — входная дата → 1-е число месяца (в метриках при отличии — `report_date_input`).
2. `_resolve_source_path(report_month, fct_msisdn_imsi_path)` — каталог → `{YYYY-MM-01}.parquet`, иначе файл как есть.
3. Счётчики `total_checks`, `warning_checks`, `failed_checks`.

### Шаг 1. Наличие набора

Нет файла → `dataset_presence` (**failed**), `summary`, return.  
Иначе `pd.read_parquet` → `dataset_basic` (**ok**: `row_count`, `distinct_msisdn`, `distinct_operator_id`).

### Шаг 2. Схема и профиль

1. `schema_columns` — все поля `_EXPECTED_COLUMNS` (**failed** при пропусках; early return).
2. Для каждой колонки: `nulls.{field}` (**failed** при null в обязательных полях).

### Шаг 3. Gate-проверки

1. `temporal_order` — `valid_to >= valid_from` (**failed**).
2. `msisdn_format` / `imsi_format` — нормализация и длины (**failed**).
3. `normalization_canonical` — канонический вид ETL (**warning**).
4. `operator_id_valid` — для IMSI `250…` обязателен `operator_id >= 1`; для иностранных IMSI допускается null (**failed** только при RU без operator).
5. `operator_id_imsi_alignment` — для IMSI `250…` совпадение с `operator_id_from_imsi_series` (**failed**).
6. `operator_id_non_ru_imsi` — не-RU IMSI с заполненным `operator_id` (**warning**).
7. `duplicate_rows` — полные дубликаты (**warning**).
8. `interval_overlap_same_triple` — пересечение по `(msisdn, operator_id, imsi)` (**failed**).
9. `interval_mergeable_gap` — смежные сегменты gap ≤ 1 с (**warning**).

### Шаг 4. Итог

`summary` и return dict. CLI не падает при failed checks.

---

## Проверки

Формат лога: `{"tag":"DQ_FCT_MSISDN_IMSI_OPERATOR","check":"...","status":"...","metrics":{...}}`.

| Check | Статус при сбое | Смысл | Обоснование |
|-------|-----------------|-------|-------------|
| `dataset_presence` | **failed** | Parquet за месяц не найден | Нет витрины после [`build-fct-msisdn-imsi-operator`](../../fct/build_fct_msisdn_imsi_operator.md) |
| `dataset_basic` | **ok** | объём, `distinct_msisdn`, `distinct_operator_id` | Базовый профиль месяца |
| `schema_columns` | **failed** | `missing_columns` | Контракт ETL / [`msisdn_imsi.json`](../../../src/mobile/schema/fct/msisdn_imsi.json) |
| `nulls.*` | **failed** | null в обязательном поле | Интервал без ключевых полей бесполезен |
| `temporal_order` | **failed** | `valid_to < valid_from` | Некорректный интервал |
| `msisdn_format` | **failed** | невалидный MSISDN | [`subscriber_ids.py`](../../../src/mobile/pipelines/stg/subscriber_ids.py) |
| `imsi_format` | **failed** | невалидный IMSI | Согласованность с ETL |
| `normalization_canonical` | **warning** | не канонические значения | ETL должен нормализовать при записи |
| `operator_id_valid` | **failed** | У IMSI `250…` нет `operator_id` | Для иностранных IMSI null допустим |
| `operator_id_imsi_alignment` | **failed** | MNC в файле ≠ MNC из IMSI | Правило наблюдений MCC=250 |
| `operator_id_non_ru_imsi` | **warning** | operator_id у не-RU IMSI | Geo-ETL отбрасывает такие строки |
| `duplicate_rows` | **warning** | полные дубликаты | Риск двойного учёта в person |
| `interval_overlap_same_triple` | **failed** | пересечение интервалов | Нарушение merge ETL |
| `interval_mergeable_gap` | **warning** | не склеены сегменты ≤ 1 с | `_merge_imsi_intervals` |
| `summary` | **ok** | счётчики checks | Сводка прогона |

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| DQ pipeline | [`pipelines/dq/stg/msisdn_imsi_operator.py`](../../../src/mobile/pipelines/dq/stg/msisdn_imsi_operator.py) |
| DQ notebook | [`pipelines/nb/13_fct_msisdn_imsi_operator.ipynb`](../../../src/mobile/pipelines/nb/13_fct_msisdn_imsi_operator.ipynb) |
| ETL build | [`pipelines/stg/msisdn_imsi.py`](../../../src/mobile/pipelines/stg/msisdn_imsi.py) |
| Нормализация ID | [`pipelines/stg/subscriber_ids.py`](../../../src/mobile/pipelines/stg/subscriber_ids.py) |
| Пути layout | [`project_paths.py`](../../../src/mobile/project_paths.py) |
| CLI | [`cli.py`](../../../src/mobile/cli.py) |
| Схема | [`msisdn_imsi.json`](../../../src/mobile/schema/fct/msisdn_imsi.json) |
| DQ IMEI | [`dq_fct_msisdn_imei.md`](./dq_fct_msisdn_imei.md) |

Сквозная цепочка: `build-stg-geo-all` → `build-fct-msisdn-imei` → `dq-fct-msisdn-imei` → **`build-fct-msisdn-imsi-operator`** → **`dq-fct-msisdn-imsi-operator`** → **`nb-fct-msisdn-imsi-operator`** → `build-fct-person` / `build-fct-geo-intervals` → downstream.
