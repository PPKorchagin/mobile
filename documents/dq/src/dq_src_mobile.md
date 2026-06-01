# dq-src-mobile

**Витрины:** CDR, SMS, GPRS, location · **Команда:** `dq-src-mobile` · **Режим:** read-only DQ по отчётным дням (процесс не падает при failed checks).

Референс: [`pipelines/dq/src/mobile.py`](../../../src/mobile/pipelines/dq/src/mobile.py). Сборка витрин: [`build_src_mobile.md`](../../src/build_src_mobile.md).

---

## Задачи pipeline

| # | Задача | Результат |
|---|--------|-----------|
| 1 | Найти parquet витрин за отчётную дату | Списки путей по mart |
| 2 | Отфильтровать строки по локальной дате `Started` | Срез отчётного дня абонента |
| 3 | Посчитать покрытие, cross-mart микс и профили полей | JSON-метрики в лог `DQ_SRC_MOBILE` |
| 4 | Выполнить gate `stg_contract.*` | **ok** / **warning** / **failed** |
| 5 | Сформировать `summary` | Счётчики checks и итоговый статус |

**Бизнес-назначение:** контроль качества синтетических mobile-витрин после `build-src-mobile` (полнота дня, схема, сегменты путей, микс трафика).

**В scope задач:** покрытие по файлам и строкам, фильтр по `Started` (локальная дата абонента), профиль полей (`SRC_*_FIELDS`), gate обязательных колонок для STG-контракта.

---

## TODO

1. При необходимости ужесточить пороги (failed вместо warning) gate `stg_contract.*`.

---

## Параметры запуска

Вызов pipeline: `run_dq(report_date, cdr_path, sms_path, gprs_path, location_path)` ([`cli.py`](../../../src/mobile/cli.py) → `dq-src-mobile`).

| Переменная | Тип | Обязательность | Значение по умолчанию | Описание |
|------------|-----|----------------|----------------------|----------|
| `report_date` | date | **Да** | `2024-12-25` … `2025-02-05` (оркестратор) | Отчётный день в локальном времени абонента (`--report-date`) |
| `cdr_path` | path | **Да** | `data/src/mobile/{dc}/operator/cdr` | Корень CDR (`--cdr-path`) |
| `sms_path` | path | **Да** | `data/src/mobile/{dc}/operator/sms` | Корень SMS (`--sms-path`) |
| `gprs_path` | path | **Да** | `data/src/mobile/{dc}/operator/gprs` | Корень GPRS (`--gprs-path`) |
| `location_path` | path | **Да** | `data/src/mobile/{dc}/operator/location` | Корень location (`--location-path`) |

**CLI:** оркестратор перебирает календарные дни `DEFAULT_SRC_START_DATE` … `DEFAULT_SRC_END_DATE` ([`cli_defaults.py`](../../../src/mobile/cli_defaults.py)) и ЦОД (`central`, `far-east`); на каждую пару (день, ЦОД) — отдельный timed-run с явными путями витрин. Pipeline ЦОД не знает — пути резолвятся через [`mobile_mart_paths`](../../../src/mobile/project_paths.py).

**Без флагов** — **43** календарных дня × **2** ЦОД = **86** прогонов (период `build-src-mobile`).

**С `--report-date`** — один прогон; `--dc` (резолв путей) или все четыре `--*-path`. Опционально `--mobile-root` переопределяет корень `data/src/mobile/{dc}`.

**Предусловие:** `uv run mobile build-src-mobile`.

Локальный запуск:

```bash
uv run mobile build-src-mobile
uv run mobile dq-src-mobile
uv run mobile dq-src-mobile --dc central --report-date 2025-01-01
uv run mobile dq-src-mobile --report-date 2025-01-01 \
  --cdr-path data/src/mobile/central/operator/cdr \
  --sms-path data/src/mobile/central/operator/sms \
  --gprs-path data/src/mobile/central/operator/gprs \
  --location-path data/src/mobile/central/operator/location
uv run mobile nb-src-mobile
```

Логи: `data/logs/mobile.log` (тег `DQ_SRC_MOBILE`). Метрики времени: `data/qa/command_timing.jsonl`, `command=dq-src-mobile-{dc}-{date}`.

---

## Структура проверяемых витрин

| Витрина | Event | JSON | Layout (фрагмент) |
|---------|-------|------|-------------------|
| `cdr` | `10001` | [`cdr.json`](../../../src/mobile/schema/src/cdr.json) | `.../operator/cdr/{operator}/10001/{YYYY}/{MM}/{DD}/` |
| `sms` | `10002` | [`sms.json`](../../../src/mobile/schema/src/sms.json) | `.../operator/sms/{operator}/10002/{YYYY}/{MM}/{DD}/` |
| `gprs` | `10003` | [`gprs.json`](../../../src/mobile/schema/src/gprs.json) | `.../operator/gprs/{operator}/10003/{YYYY}/{MM}/{DD}/` |
| `location` | `10004` | [`location.json`](../../../src/mobile/schema/src/location.json) | `.../operator/location/{operator}/10004/{YYYY}/{MM}/{DD}/` |

| Свойство | Значение |
|----------|----------|
| Формат | Parquet |
| ЦОД | `central`, `far-east` — отдельный набор каталогов на прогон (резолв на CLI) |
| Фильтр строк | `Started` (`YYYYMMDDhhmmss`, локальное время абонента) = `report_date` |
| Окно чтения файлов | Календарь в пути ±1 день от `report_date` |
| Поля runtime | `SRC_*_FIELDS` в [`mobile.py`](../../../src/mobile/pipelines/dq/src/mobile.py) |

---

## Источники

| # | Источник | Путь | Назначение |
|---|----------|------|------------|
| 1 | CDR | `data/src/mobile/{dc}/operator/cdr/.../10001/.../*.parquet` | Витрина звонков |
| 2 | SMS | `data/src/mobile/{dc}/operator/sms/.../10002/.../*.parquet` | Витрина SMS |
| 3 | GPRS | `data/src/mobile/{dc}/operator/gprs/.../10003/.../*.parquet` | Витрина GPRS |
| 4 | location | `data/src/mobile/{dc}/operator/location/.../10004/.../*.parquet` | Витрина location |

Parquet **не передаётся в pipeline напрямую** — CLI резолвит корни витрин; pipeline обходит каталоги и читает файлы в окне ±1 день по сегменту `YYYY/MM/DD` в пути.

---

## Алгоритм обработки данных

### Шаг 0. Инициализация

1. Резолв `report_date` и путей четырёх витрин (без флагов CLI — цикл дней × ЦОД; с `--report-date` — один прогон).
2. Абсолютные пути от `PROJECT_ROOT`; ожидаемые колонки — из `SRC_*_FIELDS` и `MOBILE_STG_CRITICAL_BY_MART`.

### Шаг 1. Покрытие и фильтр строк

Для каждой витрины `cdr|sms|gprs|location`:

1. `discover_mart_parquet_paths` → `filter_paths_near_report_date` (окно ±1 день).
2. Чтение parquet; фильтр `_filter_df_by_local_report_date` по полю `Started`.
3. `{mart}.coverage` — файлов в окне, строк до/после фильтра, `mart_root`.
4. `dataset_filter` — сводка по всем витринам (`Started_local_subscriber_date`).

### Шаг 2. Cross-mart и профиль витрин

1. **`cross_mart.traffic_mix`**, **`cross_mart.day_traffic_mix`** — доли строк по витринам, `gprs_share`, `location_to_gprs_row_ratio`.
2. **`{mart}.day.coverage`** — parquet-файлов и строк за отчётный день.
3. **`_emit_mart_deep_metrics`** (префиксы `{mart}.day.*` и `{mart}.*`):
   - `sample_basic`, `schema_columns`, `path_event_segment`, `started_parseable`;
   - `distribution.{col}`, `distribution.Started_hour`, `null_rates`;
   - для `location` — `spatial_ranges_sample`; для `cdr`/`gprs` — `imsi_started_duplicates_sample`.
4. Gate **`{mart}.stg_contract.*`** и **`{mart}.stg_contract.columns`** — см. [Проверки](#проверки).
5. Каждый check логируется: `{"tag":"DQ_SRC_MOBILE","mart":"...","check":"...","status":"...","metrics":{...}}`.

### Шаг 3. Итог

`summary` с `total_checks`, `warning_checks`, `failed_checks`; return dict со `status`, `report_date`, `mart_paths`, `mart_rows`.

### Типовые ошибки

| Ошибка | Причина |
|--------|---------|
| Нет parquet за день | `{mart}.coverage` с нулевыми строками; `traffic_mix` — `no_rows_in_any_mart` |
| `{mart}.stg_contract.columns` **failed** | Нет обязательной колонки из `MOBILE_STG_CRITICAL_BY_MART` |
| Битый parquet | исключение pandas/pyarrow при чтении |

---

## Проверки

Статусы: **info** — метрика (`emit_metric`); **ok** / **warning** / **failed** — gate (`emit_gate`).  
Префикс `{mart}`: `cdr` | `sms` | `gprs` | `location`. Глубокий профиль дублируется: `{mart}.day.*` (с `calendar_day`) и `{mart}.*`.

### Покрытие и cross-mart

| Check | Статус при сбое | Смысл | Обоснование |
|-------|-----------------|-------|-------------|
| `{mart}.coverage` | info | Файлов в окне ±1 день, строк до/после фильтра по `Started` | Контроль полноты суточного среза по витрине |
| `dataset_filter` | info | Сводный фильтр по всем витринам | Фиксация объёма прогона до gate-проверок |
| `cross_mart.traffic_mix` | info | Доли cdr/sms/gprs/location за окно чтения | Sanity микса трафика между витринами |
| `cross_mart.day_traffic_mix` | info | Микс только за отчётный день | Контроль баланса событий в целевых сутках |
| `{mart}.day.coverage` | info | Parquet-файлов и строк за день | Покрытие отчётной даты после локального фильтра |

### Профиль витрины (info)

| Check | Витрины | Смысл | Обоснование |
|-------|---------|-------|-------------|
| `{mart}.day.sample_read` / `{mart}.sample_read` | все | Пустой срез после фильтра | Ранний выход из глубокого профиля |
| `{mart}.day.sample_basic` / `{mart}.sample_basic` | все | `total_rows`, `column_count`, `parquet_files` | Базовый объём среза |
| `{mart}.day.schema_columns` / `{mart}.schema_columns` | все | Сверка с `SRC_*_FIELDS` | Контракт полей runtime |
| `{mart}.day.path_event_segment` / `{mart}.path_event_segment` | все | Event в пути: `10001`…`10004` | Согласованность layout и типа витрины |
| `{mart}.day.started_parseable` / `{mart}.started_parseable` | все | Доля `Started` формата `YYYYMMDDhhmmss` | Базовый temporal-контракт |
| `{mart}.day.distribution.{col}` / `{mart}.distribution.{col}` | см. ниже | Профиль колонки (numeric/categorical) | Калибровка генератора по доменам |
| `{mart}.day.distribution.Started_hour` / `{mart}.distribution.Started_hour` | все | Распределение часа суток | Профиль активности абонента |
| `{mart}.day.null_rates` / `{mart}.null_rates` | все | Доля null по `SRC_*_FIELDS` | Полнота полей в срезе |
| `{mart}.day.spatial_ranges_sample` / `{mart}.spatial_ranges_sample` | `location` | Координаты вне допустимых диапазонов | Sanity геоданных |
| `{mart}.day.imsi_started_duplicates_sample` / `{mart}.imsi_started_duplicates_sample` | `cdr`, `gprs` | Дубликаты (`IMSI`, `Started`) | Контроль уникальности событий |

Колонки для `distribution.{col}` (`_DISTRIBUTION_COLUMNS_BY_MART`):

| Витрина | Поля |
|---------|------|
| `cdr` | `Owner`, `Category`, `Service`, `Event`, `Duration`, `RecEntOwnerRegion`, `CallingRegion`, `CalledRegion` |
| `sms` | `Owner`, `Event`, `MCC`, `MNC`, `SMSC` |
| `gprs` | `Owner`, `Category`, `Service`, `Event`, `Duration`, `RAT`, `RecEntOwnerRegion`, `APN` |
| `location` | `Event`, `MCC`, `MNC`, `Source`, `TA` |

### Gate STG-контракт (`{mart}.day.mobile.stg_contract.*` / `{mart}.mobile.stg_contract.*`)

| Check | Статус при сбое | Смысл | Обоснование |
|-------|-----------------|-------|-------------|
| `.stg_contract.sample` | **warning** | Пустой DataFrame | Нет данных для gate |
| `.stg_contract.started` | **failed** / **warning** | `Started` parseable (<99% / <99.5%) | Temporal-контракт для `build-dds-event` |
| `.stg_contract.owner` | **failed** / **warning** | `Owner` ∈ {1, 2} | Оператор события |
| `.stg_contract.lac_cell` | **failed** / **warning** | Lac/Cell или BSStartLac/BSStartCell в диапазонах | Геопривязка к БС |
| `.stg_contract.imsi` | **failed** / **warning** | Доля валидных IMSI (пороги по витрине) | Идентификатор абонента для STG |
| `.stg_contract.msisdn` | **failed** / **warning** | MSISDN 10–15 цифр (<98% / <99%) | Номер для OSS-матчинга |
| `.stg_contract.coords` | **failed** / **warning** | `Latitude`/`Longitude` в диапазонах | Координаты location/sms |

Пороги `.stg_contract.imsi` по витринам: `cdr` 35%/45%, `gprs` 40%/50%, `sms` 20%/30%, `location` 15%/22% (failed/warning).

### Gate обязательных колонок

| Check | Статус при сбое | Смысл | Обоснование |
|-------|-----------------|-------|-------------|
| `{mart}.stg_contract.columns` | **failed** | Нет колонки из `MOBILE_STG_CRITICAL_BY_MART` | Минимальный набор для трансформации в STG |

Обязательные поля (`MOBILE_STG_CRITICAL_BY_MART`):

| Витрина | Поля |
|---------|------|
| `cdr` | `Started`, `Duration`, `Owner`, `CallingNumber`, `CalledNumber`, `IMSI`, `BSStartLac`, `BSStartCell`, `dateTimeOriginal` |
| `gprs` | то же, что `cdr` |
| `sms` | `Started`, `Owner`, `Calling`, `Called`, `IMSI`, `Lac`, `Cell` |
| `location` | `Started`, `Served`, `IMSI`, `Lac`, `Cell` |

### Итог

| Check | Смысл | Обоснование |
|-------|-------|-------------|
| `summary` | `total_checks`, `warning_checks`, `failed_checks`; итоговый статус run | Сводка прогона для мониторинга и CI |

CLI не завершается с ненулевым exit code при failed checks (read-only DQ).

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| DQ pipeline | [`pipelines/dq/src/mobile.py`](../../../src/mobile/pipelines/dq/src/mobile.py) |
| DQ notebook | [`pipelines/nb/8_src_mobile.ipynb`](../../../src/mobile/pipelines/nb/8_src_mobile.ipynb) |
| ETL build mobile | [`pipelines/src/mobile.py`](../../../src/mobile/pipelines/src/mobile.py) |
| CLI wiring | [`cli.py`](../../../src/mobile/cli.py) |
| Пути и helpers | [`project_paths.py`](../../../src/mobile/project_paths.py) |
| JSON-схемы | [`cdr.json`](../../../src/mobile/schema/src/cdr.json), [`sms.json`](../../../src/mobile/schema/src/sms.json), [`gprs.json`](../../../src/mobile/schema/src/gprs.json), [`location.json`](../../../src/mobile/schema/src/location.json) |
| STG event | [`build_dds_event.md`](../../dds/build_dds_event.md) |
