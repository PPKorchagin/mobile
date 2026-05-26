# dq-stg-event

**Витрина:** `stg_event` (DDS-слой `event_dds`) · **Команда:** `dq-stg-event` · **Режим:** read-only DQ (процесс не падает при failed checks).

Референс: [`pipelines/dq/stg/event.py`](../../../src/mobile/pipelines/dq/stg/event.py). Сборка DDS: [`build_move_event.md`](../../stg/build_move_event.md). Схема полей: [`event.json`](../../../src/mobile/schema/stg/event.json).

---

## Задачи pipeline

| # | Задача | Результат |
|---|--------|-----------|
| 1 | Найти `{dc}.parquet` за отчётную дату в `event_dds` | Список файлов по ЦОД |
| 2 | Профиль полей, микс событий, gate `stg_contract.*` | Логи `DQ_STG_EVENT` |
| 3 | Итог `summary` | Счётчики checks |

**Бизнес-назначение:** контроль качества дневного DDS-среза событий после `build-move-event` перед потребителями geo-слоя.

**В scope:** чтение Parquet из `data/stg/event_dds/`, фильтр строк по локальным суткам (`event_timestamp[:8]`), проверки контракта `STG_EVENT_FIELDS`.

---

## TODO

1. Notebook DQ (по аналогии с geo).

---

## Параметры запуска

Вызов: `run_dq(report_date, event_dds_path)` ([`cli.py`](../../../src/mobile/cli.py) → `dq-stg-event`).

| Переменная | Тип | Обязательность | Значение по умолчанию | Описание |
|------------|-----|----------------|----------------------|----------|
| `report_date` | date | Да | — | Отчётный день (`event_timestamp[:8]`, локальное время) |
| `event_dds_path` | path | Да* | `data/stg/event_dds` | Файл `{dc}.parquet`, каталог `YYYY-MM-DD` или корень layout |

\* С `--dc` путь по умолчанию: `data/stg/event_dds/{YYYY-MM-DD}/{dc}.parquet` (`stg_event_dds_output_path`).

**Константы DQ в коде** ([`event.py`](../../../src/mobile/pipelines/dq/stg/event.py)):

| Константа | Значение |
|-----------|----------|
| `LOG_TAG` | `DQ_STG_EVENT` |
| `STG_EVENT_CRITICAL_COLUMNS` | поля из `STG_EVENT_FIELDS` |
| `EVENT_CODES` | коды/имена событий (из ETL `stg/event.py`) |

CLI worker (`--dc` + `--report-date`):

```bash
uv run mobile dq-stg-event --dc central --report-date 2025-01-01
```

Один прогон с явным путём:

```bash
uv run mobile dq-stg-event --report-date 2025-01-01 --event-dds-path data/stg/event_dds
uv run mobile dq-stg-event --report-date 2025-01-01 --event-dds-path data/stg/event_dds/2025-01-01/central.parquet
```

Оркестратор (без `--dc`): цикл `DEFAULT_SRC_START_DATE` … `DEFAULT_SRC_END_DATE` × оба ЦОД (subprocess на день × ЦОД).

```bash
uv run mobile dq-stg-event
uv run mobile dq-stg-event --report-date 2025-01-01
```

Логи: `data/logs/mobile.log` (тег `DQ_STG_EVENT`). Метрики: `data/qa/command_timing.jsonl`, `command=dq-stg-event` или `dq-stg-event-{dc}`.

**Предусловие:** `uv run mobile build-move-event` (или `build-stg-event` + `build-move-event`) за ту же дату.

---

## Структура проверяемой витрины

| Свойство | Значение |
|----------|----------|
| Слой хранения | `data/stg/event_dds/{YYYY-MM-DD}/{source_id}.parquet` |
| Схема | [`event.json`](../../../src/mobile/schema/stg/event.json) → `stg_event` |
| Партиционирование | Отчётный день × ЦОД |

### Поля (контракт)

`event_timestamp`, `imsi`, `imei`, `msisdn`, `location`, `event`, `event_name`, `event_count` — см. [`build_stg_event.md`](../../stg/build_stg_event.md).

---

## Источники

| # | Источник | Путь |
|---|----------|------|
| 1 | DDS Parquet | `data/stg/event_dds/{YYYY-MM-DD}/central.parquet` |
| 2 | DDS Parquet | `data/stg/event_dds/{YYYY-MM-DD}/far-east.parquet` |

Отбор файлов: `stg_event_dds_day_key_from_path` / каталог `report_date` в [`project_paths.py`](../../../src/mobile/project_paths.py).

---

## Алгоритм обработки данных

### Шаг 0. Инициализация

1. `resolve_project_path(event_dds_path)`.
2. `_discover_event_dds_parquet_paths` — parquet за `report_date`.
3. Группировка по `source_id` (`central` / `far-east`).

### Шаг 1. Наличие и покрытие

Нет файлов → `dataset_presence` (**failed**). Иначе `coverage`, `source.coverage` по ЦОД.

### Шаг 2. Чтение и фильтр

1. `read_parquet` по файлам сегмента.
2. `_filter_df_by_local_report_date` — `event_timestamp[:8] == report_date`.

### Шаг 3. Профиль и gates

Распределения `event` / `event_name`, `event_timestamp_parseable`, `event_count_valid`, дубликаты ключа, `event.stg_contract.*`, `stg_contract.columns`.

### Шаг 4. Итог

`summary` — `total_checks`, `warning_checks`, `failed_checks`.

### Типовые ошибки

| Ситуация | Поведение |
|----------|-----------|
| Нет parquet за день | `dataset_presence` failed |
| Пустой срез после фильтра | warning на `sample_read` |
| CLI: `--dc` без `--report-date` | `SystemExit` |

---

## Проверки

| Check | Уровень | Смысл |
|-------|---------|--------|
| `dataset_presence` | failed | Нет parquet за `report_date` |
| `coverage` | info | Файлы, строки, пути |
| `source.coverage` | info | Покрытие по ЦОД (`central` / `far-east`) |
| `source.{dc}.sample_basic` | info | Строки в срезе ЦОД |
| `event_distribution` / `event_name_distribution` | info | Микс типов событий (legacy, counts) |
| `distribution.{event,event_name,event_count}` | info | Numeric (квантили) или categorical top-N |
| `distribution.event_timestamp_hour` | info | Час суток (`event_timestamp[8:10]`) |
| `distribution.event_count_bucket` | info | Корзины `1` / `2-5` / `6-20` / `21+` |
| `distribution.{imsi,imei,msisdn}_length` | info | Длина строки идентификатора (без значений) |
| `distribution.imei_tac` | info | Top-N TAC (первые 8 цифр IMEI) |
| `distribution.location_{mcc,mnc,lac,cell}` | info | Top-N по полям struct `location` |
| `distribution.location_compressible` | info | Доля строк с валидным lac/cell |
| `null_rates` | info | Доля null по колонкам контракта |
| `event_timestamp_parseable` | gate | Формат `YYYYMMDDhhmmss` |
| `event_count_valid` | gate | `event_count >= 1`, доля схлопнутых (`>1`) |
| `imsi_event_timestamp_event_duplicates` | info | Дубликаты ключа |
| `event.stg_contract.*` | gate | Код/имя события, location, согласованность |
| `stg_contract.columns` | gate | Все поля `STG_EVENT_FIELDS` |

---

## Место в пайплайне

```text
build-stg-event → build-move-event → dq-stg-event → (потребители DDS)
```

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| DQ ETL | [`pipelines/dq/stg/event.py`](../../../src/mobile/pipelines/dq/stg/event.py) |
| Перенос DDS | [`pipelines/stg/move_event.py`](../../../src/mobile/pipelines/stg/move_event.py) |
| Сборка событий | [`build_stg_event.md`](../../stg/build_stg_event.md) |
| Перенос (doc) | [`build_move_event.md`](../../stg/build_move_event.md) |
| Пути | [`project_paths.py`](../../../src/mobile/project_paths.py) |
| CLI | [`cli.py`](../../../src/mobile/cli.py) |
| Схема | [`event.json`](../../../src/mobile/schema/stg/event.json) |
| DQ mobile (вход) | [`dq_src_mobile.md`](../src/dq_src_mobile.md) |
