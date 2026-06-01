# build-dds-event

**Витрина:** `dds_event` · **Команда:** `build-dds-event` · **Режим:** сборка дневного Parquet из mobile-витрин одного ЦОД.

Референс: [`pipelines/stg/event.py`](../../src/mobile/pipelines/stg/event.py). Схема витрины: [`event.json`](../../src/mobile/schema/dds/event.json).

> **ЦОД:** pipeline обрабатывает **один** набор входных витрин и пишет **один** выходной файл. Для `central` и `far-east` нужны **отдельные** вызовы (разные корни SRC и разные `output_path`). Оркестратор CLI без флагов выполняет полный цикл по каждому ЦОД последовательно; внутри ЦОД — до **2** параллельных **subprocess** (`uv run mobile build-dds-event --dc … --report-date …`).

---

## Задачи pipeline

| # | Задача | Результат |
|---|--------|-----------|
| 1 | Прочитать CDR/SMS/GPRS/location за отчётную дату (локальное время `Started`) | DataFrame по каждой витрине |
| 2 | Нормализовать в канон `dds_event`, объединить | Единый поток событий |
| 3 | Сортировка; 5m-схлопывание только при валидном lac/cell; `event_count` | Упорядоченный срез |
| 4 | Записать Parquet в `output_path` | `events.parquet` за отчётный день |

**Бизнес-назначение:** единый слой mobile-событий по абоненту для последующего geo-пайплайна.

**В scope задач:** чтение четырёх mobile-витрин, маппинг полей, фильтр по локальной отчётной дате, сортировка, частичное сжатие как в `stg_geo_all._aggregate_events` (без join с BS). Списки exclusion на этом этапе **не** применяются.

**Предусловие:** [`build-src-mobile`](../src/build_src_mobile.md) за период, покрывающий `report_date`, **для того же ЦОД**, что и входные витрины.

---

## TODO

1. Notebook DQ (по аналогии с geo).

---

## Параметры запуска

Переменные, передаваемые в job (`run_build()` из [`event.py`](../../src/mobile/pipelines/stg/event.py)). **Все пять обязательны** — pipeline не резолвит ЦОД и шаблоны путей.

| Переменная | Тип | Обязательность | Описание |
|------------|-----|----------------|----------|
| `report_date` | date | **Да** | Отчётный день в локальном времени абонента (`Started`, `YYYYMMDD`) |
| `cdr_path` | path | **Да** | Корень витрины CDR |
| `sms_path` | path | **Да** | Корень витрины SMS |
| `gprs_path` | path | **Да** | Корень витрины GPRS |
| `location_path` | path | **Да** | Корень витрины location |
| `output_path` | path | **Да** | Выходной `events.parquet` (CLI `--output-path`) |

Parquet всегда пишется со сжатием **`snappy`** (`DEFAULT_PARQUET_COMPRESSION`). Сжатие подряд идущих событий (5m bucket) выполняется **всегда**.

**Константы ETL в коде** (на вход job **не передаются**): `DDS_EVENT_FIELDS`, `_MART_READ_COLUMNS`, `_COMPRESS_GAP_SECONDS` — см. [`event.py`](../../src/mobile/pipelines/stg/event.py).

### CLI

| Режим | Поведение |
|-------|-----------|
| Без флагов | `DEFAULT_SRC_START_DATE` … `DEFAULT_SRC_END_DATE` ([`cli_defaults.py`](../../src/mobile/cli_defaults.py)); для **каждого** ЦОД (`central`, `far-east`) — цикл дней в **2 параллельных subprocess**; пути витрин и `output_path` резолвятся в worker-CLI |
| `--report-date` + `--dc` | Один прогон: 4 корня из `mobile_mart_paths`, выход `data/dds/event/{YYYY}/{MM}/{DD}/{dc}/events.parquet` |
| Все 5 явно | `--report-date`, четыре `--*-path`, `--output-path` (без `--dc`) |

Опционально `--mobile-root` переопределяет корень `data/src/mobile/{dc}` при резолве через `--dc`.

Локальный запуск:

```bash
uv run mobile build-src-mobile
uv run mobile build-dds-event
uv run mobile build-dds-event --dc central --report-date 2025-01-01
uv run mobile build-dds-event --report-date 2025-01-01 \
  --cdr-path data/src/mobile/central/operator/cdr \
  --sms-path data/src/mobile/central/operator/sms \
  --gprs-path data/src/mobile/central/operator/gprs \
  --location-path data/src/mobile/central/operator/location \
  --output-path data/dds/event/2025/01/01/central/events.parquet
```

Логи: `data/logs/mobile.log` (`build-dds-event report_date=… path=…`). Метрики: `data/qa/command_timing.jsonl`, `command=build-dds-event-{dc}-{date}`.

---

## Структура генерируемой витрины

| Свойство | Значение |
|----------|----------|
| Имя таблицы | `dds_event` — [`event.json`](../../src/mobile/schema/dds/event.json) → `table` |
| Описание | Единая витрина mobile-событий — `description` в JSON |
| Формат хранения | Parquet |
| Партиционирование | Календарный день × `source_id` (ЦОД) в каталоге |
| Календарный срез | `report_date` (локальное время в `event_timestamp`) |
| Сжатие | `snappy` (`DEFAULT_PARQUET_COMPRESSION`, не параметр job) |

### Путь выхода (по умолчанию в CLI)

Шаблон `DDS_EVENT_LAYOUT_TEMPLATE` в [`project_paths.py`](../../src/mobile/project_paths.py):

`data/dds/event/{YYYY}/{MM}/{DD}/{source_id}/events.parquet`

Пример: `data/dds/event/2025/01/01/central/events.parquet`.

### Поля витрины

Контракт — [`event.json`](../../src/mobile/schema/dds/event.json) → `fields`; в ETL — `DDS_EVENT_FIELDS` ([`event.py`](../../src/mobile/pipelines/stg/event.py)).

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

Четыре mobile-витрины **одного** ЦОД (после `build-src-mobile`).

| Витрина | Сегмент в пути | Код `event` |
|---------|----------------|-------------|
| CDR | `.../operator/cdr/.../10001/{YYYY}/{MM}/{DD}/` | 10001 |
| SMS | `.../10002/...` | 10002 |
| GPRS | `.../10003/...` | 10003 |
| location | `.../10004/...` | 10004 |

Корни по умолчанию: `data/src/mobile/{dc}/operator/{mart}/…` — см. [`build_src_mobile.md`](../src/build_src_mobile.md).

---

## Алгоритм обработки данных

### Шаг 0. Инициализация

1. Разрешить пути витрин и `output_path` (`resolve_project_path`).
2. Целевая схема — `DDS_EVENT_FIELDS` (JSON в runtime не читается).

### Шаг 1. Чтение и фильтр по дате

Для каждой витрины (`cdr`, `sms`, `gprs`, `location`):

1. `discover_mart_parquet_paths` — все `*.parquet` под корнем витрины.
2. `filter_paths_near_report_date` — окно **±1 день** по календарю в пути.
3. `read_all_parquets_concat` — только нужные колонки (`_MART_READ_COLUMNS`).
4. `filter_df_by_local_report_date` — строки, где `Started[:8] == report_date`.

### Шаг 2. Трансформация и объединение

1. `_transform_mart_frame` → канон `dds_event`.
2. `pd.concat` по всем витринам.

### Шаг 3. Сортировка и сжатие

1. Сортировка: `imsi`, `event_timestamp`, `event_name`.
2. Поток A (валидный CGI): 5m-схлопывание, `event_count = len(group)`.
3. Поток B (невалидный CGI): без схлопывания, `event_count = 1`.
4. `pd.concat` потоков A и B.

### Шаг 4. Запись

1. Запись в `output_path` (`snappy`).
2. Лог: `report_date`, `job_start`, `job_end`, `job_count`, `path`.
3. `append_command_metrics(command="build-dds-event", …)`.

### Типовые ошибки

| Ошибка | Причина |
|--------|---------|
| `SystemExit` | CLI: не переданы все 5 параметров (без `--dc`) |
| `SystemExit` | CLI: явный прогон без `--report-date` |
| Пустой выход | Нет mobile-витрин за день / все строки отфильтрованы |

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| Схема витрины | [`src/mobile/schema/dds/event.json`](../../src/mobile/schema/dds/event.json) |
| ETL | [`src/mobile/pipelines/stg/event.py`](../../src/mobile/pipelines/stg/event.py) |
| Пути, чтение витрин | [`src/mobile/project_paths.py`](../../src/mobile/project_paths.py) |
| CLI | [`src/mobile/cli.py`](../../src/mobile/cli.py) |
| Сборка mobile | [`build_src_mobile.md`](../src/build_src_mobile.md) |
| DQ mobile (входные витрины) | [`dq_src_mobile.md`](../dq/src/dq_src_mobile.md) |
| DDS перенос | [`build_dds_move_event.md`](./build_dds_move_event.md) |
| DQ DDS | [`dq_dds_event.md`](../dq/dds/dq_dds_event.md) |
