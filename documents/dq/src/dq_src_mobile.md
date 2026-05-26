# dq-src-mobile

**Витрины:** CDR, SMS, GPRS, location · **Команда:** `dq-src-mobile` · **Режим:** read-only DQ (процесс не падает при failed checks).

Референс: [`pipelines/dq/src/mobile.py`](../../../src/mobile/pipelines/dq/src/mobile.py). Сборка витрин: [`build_src_mobile.md`](../../src/build_src_mobile.md). Схемы: [`cdr.json`](../../../src/mobile/schema/src/cdr.json), [`sms.json`](../../../src/mobile/schema/src/sms.json), [`gprs.json`](../../../src/mobile/schema/src/gprs.json), [`location.json`](../../../src/mobile/schema/src/location.json).

---

## Задачи pipeline

| # | Задача | Результат |
|---|--------|-----------|
| 1 | Найти parquet витрин ЦОД за **отчётную дату** | Списки путей по mart |
| 2 | Метрики покрытия и cross-mart микса | JSON в лог `DQ_SRC_MOBILE` |
| 3 | Профиль полей и gate `stg_contract.*` | info / warning / failed |
| 4 | Итог `summary` | Счётчики checks |

**Бизнес-назначение:** контроль качества синтетических mobile-витрин после `build-src-mobile` (полнота дня, схема, сегменты путей, микс трафика).

**В scope задач:** покрытие по файлам и строкам, фильтр по `Started` (локальная дата абонента), профиль полей (`SRC_*_FIELDS`), gate обязательных колонок для STG-контракта.

---

## TODO

1. При необходимости ужесточить пороги (failed вместо warning) gate `stg_contract.*` в [`mobile.py`](../../../src/mobile/pipelines/dq/src/mobile.py).
2. Notebook-визуализация DQ mobile (если перенесём nb из geo).

---

## Параметры запуска

Вызов: `run_dq(dc, report_date, cdr_path, sms_path, gprs_path, location_path)` ([`cli.py`](../../../src/mobile/cli.py) → `dq-src-mobile`).

| Переменная | Тип | Обязательность | Значение по умолчанию | Описание |
|------------|-----|----------------|----------------------|----------|
| `dc` | string | Да | — | `central` или `far-east` |
| `report_date` | date | Да | — | Отчётный день в **локальном** времени абонента (поле `Started`) |
| `cdr_path` | path | Да | `{dc}/operator/cdr` | Корень витрины CDR |
| `sms_path` | path | Да | `{dc}/operator/sms` | Корень витрины SMS |
| `gprs_path` | path | Да | `{dc}/operator/gprs` | Корень витрины GPRS |
| `location_path` | path | Да | `{dc}/operator/location` | Корень витрины location |

CLI worker (`--dc` + `--report-date`): пути строятся из `mobile_mart_paths` / [`project_paths.py`](../../../src/mobile/project_paths.py). Опционально `--mobile-root` переопределяет корень ЦОД.

Оркестратор (без `--dc`): цикл `DEFAULT_SRC_START_DATE` … `DEFAULT_SRC_END_DATE` из [`cli_defaults.py`](../../../src/mobile/cli_defaults.py); на каждый день — **два subprocess** (`central`, `far-east`).

**Схема полей в runtime:** `SRC_*_FIELDS` в [`pipelines/dq/src/mobile.py`](../../../src/mobile/pipelines/dq/src/mobile.py); JSON в `schema/src/` — контракт документации.

**Предусловие:** `uv run mobile build-src-mobile`.

Локальный запуск:

```bash
# Все дни периода build-src-mobile × оба ЦОД (отдельный subprocess на пару день+ЦОД)
uv run mobile dq-src-mobile

# Один день, оба ЦОД
uv run mobile dq-src-mobile --report-date 2025-01-01

# Один ЦОД и одна отчётная дата (worker)
uv run mobile dq-src-mobile --dc central --report-date 2025-01-01
```

Логи: `data/logs/mobile.log` (тег `DQ_SRC_MOBILE`). Метрики времени: `data/qa/command_timing.jsonl`, `command=dq-src-mobile` или `dq-src-mobile-{dc}`.

---

## Структура проверяемых витрин

| Свойство | Значение |
|----------|----------|
| Имена | `cdr`, `sms`, `gprs`, `location` |
| Формат | Parquet |
| Layout | `data/src/mobile/{dc}/operator/{mart}/{operator}/{10001\|10002\|10003\|10004}/{YYYY}/{MM}/{DD}/*.parquet` |
| ЦОД | `central`, `far-east` — отдельный `mobile_root` на ЦОД |

---

## Источники

| # | Источник | Путь | Назначение |
|---|----------|------|------------|
| 1 | CDR | `.../operator/cdr/.../10001/.../cdr.parquet` | Витрина DQ |
| 2 | SMS | `.../operator/sms/.../10002/.../sms.parquet` | Витрина DQ |
| 3 | GPRS | `.../operator/gprs/.../10003/.../gprs.parquet` | Витрина DQ |
| 4 | location | `.../operator/location/.../10004/.../location.parquet` | Витрина DQ |

Фильтр строк: `Started` (локальное время абонента, `YYYYMMDD…`) попадает в `report_date`. Parquet-файлы сканируются с окном ±1 день по сегменту `YYYY/MM/DD` в пути.

---

## Алгоритм обработки данных

### Шаг 0. Инициализация

1. Пути витрин → абсолютные от `PROJECT_ROOT`.
2. Ожидаемые колонки — из `SRC_*_FIELDS` в [`mobile.py`](../../../src/mobile/pipelines/dq/src/mobile.py).

### Шаг 1. Покрытие

1. `rglob` `{mart}.parquet` под корнем витрины; окно путей ±1 день от `report_date`.
2. Полное чтение parquet → фильтр строк по `Started` (локальная дата = `report_date`).
3. Метрики `{mart}.coverage`, сводный `dataset_filter`.

### Шаг 2. Cross-mart и профиль витрин

1. `cross_mart.traffic_mix`, `cross_mart.day_traffic_mix`.
2. `{mart}.day.coverage`.
3. Два прогона `_emit_mart_deep_metrics`: префикс `{mart}.day.*` и `{mart}.*` (распределения по полям, `Started_hour`, `null_rates`).
4. Gate `{mart}.stg_contract.columns`.

### Шаг 3. Итог

`summary` с агрегатами; return dict со `status`, `report_date`, `datacenter`, счётчиками checks.

Каждый check — JSON в лог: `{"tag":"DQ_SRC_MOBILE","mart":"...","check":"...","status":"...","metrics":{...}}`.

Полный перечень checks — в разделе [Проверки](#проверки) ниже.

### Типовые ошибки

| Ошибка | Причина |
|--------|---------|
| Нет parquet за день | `coverage` с нулевыми строками; `traffic_mix` — `no_rows_in_any_mart` |
| Нет обязательных колонок | `{mart}.stg_contract.columns` **failed** |
| pandas / pyarrow | Повреждённый parquet (пропуск при чтении среза) |

---

## Проверки

Статусы: **info** — метрика (`emit_metric`); **ok** / **warning** / **failed** — gate (`emit_gate`).  
Префикс `{mart}`: `cdr` | `sms` | `gprs` | `location`. Для глубокого профиля каждая проверка пишется **дважды**: `{mart}.day.*` (с `calendar_day` в metrics) и `{mart}.*` (тот же срез, без `.day`).

### Покрытие и cross-mart (info)

| Check | `mart` | Смысл |
|-------|--------|--------|
| `{mart}.coverage` | витрина | Файлов в окне ±1 день, строк до/после фильтра по `Started`, корень витрины |
| `dataset_filter` | `cross_mart` | Сводный фильтр: `report_date`, `Started_local_subscriber_date`, строки и файлы по всем витринам |
| `cross_mart.traffic_mix` | `cross_mart` | Доли строк cdr/sms/gprs/location, `gprs_share`, `location_to_gprs_row_ratio`; при пустых данных — `no_rows_in_any_mart` |
| `cross_mart.day_traffic_mix` | `cross_mart` | То же по отчётному дню (`calendar_day` = `report_date`) |
| `{mart}.day.coverage` | витрина | Parquet-файлов и строк за день после локального фильтра |

### Профиль витрины (info)

Выполняются в `_emit_mart_deep_metrics`, если есть пути или непустой DataFrame после фильтра. Иначе только `{mart}.day.sample_read` / `{mart}.sample_read` с `reason: empty_sample`.

| Check | Витрины | Смысл |
|-------|---------|--------|
| `{mart}.day.sample_read` / `{mart}.sample_read` | все | Пустой срез после фильтра |
| `{mart}.day.sample_basic` / `{mart}.sample_basic` | все | `total_rows`, `column_count`, `parquet_files`, `full_scan: true` |
| `{mart}.day.schema_columns` / `{mart}.schema_columns` | все | Сверка с `SRC_*_FIELDS`: `missing_columns`, `expected_count`, `present_count` (только info) |
| `{mart}.day.path_event_segment` / `{mart}.path_event_segment` | все | Сегмент в пути: `10001` (cdr), `10002` (sms), `10003` (gprs), `10004` (location) |
| `{mart}.day.started_parseable` / `{mart}.started_parseable` | все | Доля `Started` в формате `YYYYMMDDhhmmss` (14 цифр) |
| `{mart}.day.distribution.{col}` / `{mart}.distribution.{col}` | см. ниже | Распределение по колонке: `null_count`, `unique_count`, для чисел — min/max/mean/квантили, для категорий — `value_counts_top` (12) |
| `{mart}.day.distribution.Started_hour` / `{mart}.distribution.Started_hour` | все | Распределение часа суток (локальное время) из `Started` |
| `{mart}.day.null_rates` / `{mart}.null_rates` | все | Доля null по каждой колонке из `SRC_*_FIELDS` (отсутствующая колонка → `1.0`) |
| `{mart}.day.spatial_ranges_sample` / `{mart}.spatial_ranges_sample` | `location` | Число строк с `Latitude`/`Longitude` вне [-90,90] / [-180,180] |
| `{mart}.day.imsi_started_duplicates_sample` / `{mart}.imsi_started_duplicates_sample` | `cdr`, `gprs` | Число строк-дубликатов по (`IMSI`, `Started`) |

Колонки для `distribution.{col}` (`_DISTRIBUTION_COLUMNS_BY_MART`):

| Витрина | Поля |
|---------|------|
| `cdr` | `Owner`, `Category`, `Service`, `Event`, `Duration`, `RecEntOwnerRegion`, `CallingRegion`, `CalledRegion` |
| `sms` | `Owner`, `Event`, `MCC`, `MNC`, `SMSC` |
| `gprs` | `Owner`, `Category`, `Service`, `Event`, `Duration`, `RAT`, `RecEntOwnerRegion`, `APN` |
| `location` | `Event`, `MCC`, `MNC`, `Source`, `TA` |

Тип поля в metrics: `kind` = `numeric` | `categorical` (авто: если ≥85% непустых значений приводятся к числу — numeric).

### Gate STG-контракт (`{mart}.day.mobile.stg_contract.*` и `{mart}.mobile.stg_contract.*`)

| Check | Условие | Пороги |
|-------|---------|--------|
| `.stg_contract.sample` | Пустой DataFrame | **warning** |
| `.stg_contract.started` | `Started` parseable | **failed** ниже 99%, **warning** ниже 99.5% |
| `.stg_contract.owner` | `Owner` ∈ {1, 2} | **failed** ниже 99%, **warning** ниже 99.5% |
| `.stg_contract.lac_cell` | `Lac`/`Cell` или `BSStartLac`/`BSStartCell`: неотрицательные, lac < 10⁵, cell < 10⁶ | **failed** ниже 99%, **warning** ниже 99.5% |
| `.stg_contract.imsi` | IMSI: не менее 10 цифр после очистки | Пороги по витрине (доля валидных IMSI): |
| | `cdr` | **failed** ниже 35%, **warning** ниже 45% |
| | `gprs` | **failed** ниже 40%, **warning** ниже 50% |
| | `sms` | **failed** ниже 20%, **warning** ниже 30% |
| | `location` | **failed** ниже 15%, **warning** ниже 22% |
| `.stg_contract.msisdn` | `CallingNumber` / `Calling` / `Served`: 10–15 цифр | **failed** ниже 98%, **warning** ниже 99% |
| `.stg_contract.coords` | `Latitude`, `Longitude` в допустимых диапазонах | **failed** ниже 99%, **warning** ниже 99.5% (если колонки есть; у `sms` тоже) |

### Gate обязательных колонок

| Check | Условие |
|-------|---------|
| `{mart}.stg_contract.columns` | **failed**, если в срезе нет колонки из `MOBILE_STG_CRITICAL_BY_MART` |

Обязательные поля (`MOBILE_STG_CRITICAL_BY_MART`):

| Витрина | Поля |
|---------|------|
| `cdr` | `Started`, `Duration`, `Owner`, `CallingNumber`, `CalledNumber`, `IMSI`, `BSStartLac`, `BSStartCell`, `dateTimeOriginal` |
| `gprs` | то же, что cdr |
| `sms` | `Started`, `Owner`, `Calling`, `Called`, `IMSI`, `Lac`, `Cell` |
| `location` | `Started`, `Served`, `IMSI`, `Lac`, `Cell` |

### Итог

| Check | Смысл |
|-------|--------|
| `summary` | `total_checks`, `warning_checks`, `failed_checks`; статус run: `ok` / `warning` / `failed` |

CLI не завершается с ненулевым exit code при failed checks (read-only DQ).

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| Обзор DQ | [`../README.md`](../README.md) |
| DQ pipeline | [`pipelines/dq/src/mobile.py`](../../../src/mobile/pipelines/dq/src/mobile.py) |
| STG gate | константы и `_emit_stg_field_checks` в [`pipelines/dq/src/mobile.py`](../../../src/mobile/pipelines/dq/src/mobile.py) |
| Поля SRC (DQ) | `SRC_*_FIELDS` в [`pipelines/dq/src/mobile.py`](../../../src/mobile/pipelines/dq/src/mobile.py) |
| ETL build | [`pipelines/src/mobile.py`](../../../src/mobile/pipelines/src/mobile.py) |
| Пути | [`project_paths.py`](../../../src/mobile/project_paths.py) |
