# build-stg-event

**Витрина:** `stg_event` · **Команда:** `build-stg-event` · **Режим:** сборка дневного Parquet по ЦОД и отчётной дате.

Референс: [`pipelines/stg/event.py`](../../src/mobile/pipelines/stg/event.py). Схема витрины: [`event.json`](../../src/mobile/schema/stg/event.json).

---

## Задачи pipeline

| # | Задача | Результат |
|---|--------|-----------|
| 1 | Прочитать CDR/SMS/GPRS/location за отчётную дату (локальное время `Started`) | DataFrame по каждой витрине |
| 2 | Нормализовать в канон `stg_event`, объединить | Единый поток событий |
| 3 | Сортировка; 5m-схлопывание только при валидном lac/cell; `event_count` | Упорядоченный срез |
| 4 | Записать `events.parquet` | Файл на ЦОД × отчётный день |

**Бизнес-назначение:** единый слой mobile-событий по абоненту для последующего geo-пайплайна (аналог `build-stg-event` / `src_event` в synthetic_data).

**В scope задач:** чтение четырёх mobile-витрин, маппинг полей, фильтр по локальной отчётной дате, сортировка, частичное сжатие как в `stg_geo_all._aggregate_events` (без join с BS). Списки exclusion на этом этапе **не** применяются.

---

## TODO

1. Команда `dq-stg-event` и notebook DQ (по аналогии с geo).
2. Включить `build-stg-event` в цепочку `build-src` / `run-all`, если потребуется сквозной прогон.

---

## Параметры запуска

Вызов: `run_build(dc, report_date, cdr_path, sms_path, gprs_path, location_path)` ([`cli.py`](../../src/mobile/cli.py) → `build-stg-event`).

| Переменная | Тип | Обязательность | Значение по умолчанию | Описание |
|------------|-----|----------------|----------------------|----------|
| `dc` | string | Да | — | `central` или `far-east` (`source_id` в пути выхода) |
| `report_date` | date | Да | — | Отчётный день в **локальном** времени абонента (`Started`, `YYYYMMDD`) |
| `cdr_path` | path | Да | `{dc}/operator/cdr` | Корень витрины CDR |
| `sms_path` | path | Да | `{dc}/operator/sms` | Корень витрины SMS |
| `gprs_path` | path | Да | `{dc}/operator/gprs` | Корень витрины GPRS |
| `location_path` | path | Да | `{dc}/operator/location` | Корень витрины location |

Parquet всегда пишется со сжатием **`snappy`** (`DEFAULT_PARQUET_COMPRESSION`). Сжатие подряд идущих событий (5m bucket) выполняется **всегда**.

CLI worker (`--dc` + `--report-date`): пути из `mobile_mart_paths` / [`project_paths.py`](../../src/mobile/project_paths.py). Опционально `--mobile-root` переопределяет корень ЦОД.

Оркестратор (без `--dc`): цикл `DEFAULT_SRC_START_DATE` … `DEFAULT_SRC_END_DATE` из [`cli_defaults.py`](../../src/mobile/cli_defaults.py); на каждый день — **два subprocess** (`central`, `far-east`).

**Схема полей в runtime:** `STG_EVENT_FIELDS` в [`event.py`](../../src/mobile/pipelines/stg/event.py); JSON [`event.json`](../../src/mobile/schema/stg/event.json) — контракт документации.

**Предусловие:** `build-src-mobile` за период, покрывающий `report_date`.

Локальный запуск:

```bash
# Все дни периода × оба ЦОД
uv run mobile build-stg-event

# Один день, оба ЦОД
uv run mobile build-stg-event --report-date 2025-01-01

# Один ЦОД и одна отчётная дата (worker)
uv run mobile build-stg-event --dc central --report-date 2025-01-01
```

Логи: `data/logs/mobile.log` (строка `build-stg-event source_id=…`). Метрики: `data/qa/command_timing.jsonl`, `command=build-stg-event` или `build-stg-event-{dc}`.

---

## Структура генерируемой витрины

| Свойство | Значение |
|----------|----------|
| Имя таблицы | `stg_event` — [`event.json`](../../src/mobile/schema/stg/event.json) → `table` |
| Описание | Единая витрина mobile-событий — `description` в JSON |
| Формат хранения | Parquet |
| Партиционирование | Календарный день × ЦОД |
| Календарный срез | `report_date` (локальное время в `event_timestamp`) |
| Сжатие | `snappy` (`DEFAULT_PARQUET_COMPRESSION`, без опций CLI) |

### Путь выхода

Шаблон: `STG_EVENT_LAYOUT_TEMPLATE` в [`project_paths.py`](../../src/mobile/project_paths.py):

`data/stg/event/{YYYY}/{MM}/{DD}/{source_id}/events.parquet`

Пример: `data/stg/event/2025/01/01/central/events.parquet`.

### Поля витрины

Контракт — [`event.json`](../../src/mobile/schema/stg/event.json) → `fields`; в ETL — `STG_EVENT_FIELDS` ([`event.py`](../../src/mobile/pipelines/stg/event.py)).

| # | Поле | Тип | Смысл |
|---|------|-----|-------|
| 1 | `event_timestamp` | string | `Started` из витрины, `YYYYMMDDhhmmss` (локальное время абонента) |
| 2 | `imsi` | string | IMSI как в источнике |
| 3 | `imei` | string | IMEI |
| 4 | `msisdn` | string | MSISDN: `CallingNumber` (cdr/gprs), `Calling` (sms), `Served` (location) |
| 5 | `location` | struct | `mcc`, `mnc`, `lac`, `cell` |
| 6 | `event` | uint32 | Код OCC: 10001 CDR, 10002 SMS, 10003 GPRS, 10004 LOCATION |
| 7 | `event_name` | string | `cdr`, `sms`, `gprs`, `location` |
| 8 | `event_count` | uint32 | Число исходных событий в 5m-группе; `1` — без схлопывания или невалидный lac/cell |

Struct `location`: cdr/gprs — из `OwnerMCCMNC` + `BSStartLac` + `BSStartCell`; sms/location — из `MCC`, `MNC`, `Lac`, `Cell`.

---

## Источники витрины

Четыре mobile-витрины одного ЦОД (после `build-src-mobile`).

| Витрина | Файл | Сегмент в пути | Код `event` |
|---------|------|----------------|-------------|
| CDR | `cdr.parquet` | `.../10001/{YYYY}/{MM}/{DD}/` | 10001 |
| SMS | `sms.parquet` | `.../10002/...` | 10002 |
| GPRS | `gprs.parquet` | `.../10003/...` | 10003 |
| location | `location.parquet` | `.../10004/...` | 10004 |

Корни по умолчанию: `data/src/mobile/{dc}/operator/{mart}/…` — см. [`build_src_mobile.md`](../src/build_src_mobile.md).

---

## Алгоритм обработки данных

### Шаг 0. Инициализация

1. Разрешить пути витрин (`resolve_project_path`).
2. Целевая схема — `STG_EVENT_FIELDS` (JSON в runtime не читается).

### Шаг 1. Чтение и фильтр по дате

Для каждой витрины (`cdr`, `sms`, `gprs`, `location`):

1. `discover_mart_parquet_paths` — все `*.parquet` под корнем витрины.
2. `filter_paths_near_report_date` — окно **±1 день** по календарю в пути (граница суток).
3. `read_all_parquets_concat` — только нужные колонки (`_MART_READ_COLUMNS`).
4. `filter_df_by_local_report_date` — строки, где `Started[:8] == report_date` (локальное время абонента).

### Шаг 2. Трансформация и объединение

1. `_transform_mart_frame`: отбор только по `event_timestamp` (14 цифр); `imsi`, `msisdn`, `imei` и `location` — как в источнике (битый lac/cell допускается).
2. `pd.concat` по всем витринам.

### Шаг 3. Сортировка и сжатие

1. Сортировка: `imsi`, `event_timestamp`, `event_name`.
2. Строки с **валидным** lac/cell (и mcc/mnc): группы по `imsi`, `event_name`, CGI, 5m bucket, пауза >300 с → одна строка, `event_count` = размер группы.
3. Строки с **невалидным** lac/cell: **без** 5m-схлопывания, каждая исходная строка на выходе, `event_count=1`.

### Шаг 4. Запись

1. `stg_event_output_path(dc, report_date)` → `events.parquet`.
2. Лог: `source_id`, `report_date`, `job_start`, `job_end`, `job_count`.
3. `append_command_metrics(command="build-stg-event", …)`.

### Типовые ошибки

| Ошибка | Причина |
|--------|---------|
| `SystemExit` | CLI: `--dc` без `--report-date` |
| Пустой выход | Нет mobile-витрин за день / все строки отфильтрованы |
| Пропуск файла при чтении | Битый parquet (warning в логе не пишется — файл молча пропускается) |

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| Схема витрины | [`src/mobile/schema/stg/event.json`](../../src/mobile/schema/stg/event.json) |
| ETL | [`src/mobile/pipelines/stg/event.py`](../../src/mobile/pipelines/stg/event.py) |
| Пути, чтение витрин | [`src/mobile/project_paths.py`](../../src/mobile/project_paths.py) |
| CLI | [`src/mobile/cli.py`](../../src/mobile/cli.py) |
| Сборка mobile | [`build_src_mobile.md`](../src/build_src_mobile.md) |
| DQ mobile (входные витрины) | [`dq_src_mobile.md`](../dq/src/dq_src_mobile.md) |
